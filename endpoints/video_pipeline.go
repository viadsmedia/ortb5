// Package endpoints — video_pipeline.go
//
// Implements the full programmatic video ad pipeline:
//
//  Stage 1  Player/Publisher Request   — Player hits the VAST tag URL
//  Stage 2  Ad Server Config           — Resolve ad-server config for the placement
//  Stage 3  OpenRTB Bid Request        — Build and forward OpenRTB bid request to demand (SSP/DSP)
//  Stage 4  Bid Response               — Receive bid response with VAST/adm
//  Stage 5  Build/Wrap VAST Response   — Ad server wraps VAST with impression trackers
//  Stage 6  VAST Returned to Player    — Serialise final VAST XML and write HTTP response
//  Stage 7  Playback + Tracking        — Record impression/quartile/complete events
//
// HTTP endpoints exposed:
//
//	GET  /video/vast              — runs stages 1-6, returns VAST XML to calling player
//	POST /video/vast              — same as GET but accepts extended request body
//	POST /video/tracking          — stage-7 event beacon (impression / quartile / complete)

package endpoints

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	mrand "math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/julienschmidt/httprouter"
	adcom1 "github.com/prebid/openrtb/v20/adcom1"
	"github.com/prebid/openrtb/v20/openrtb2"
	"github.com/prebid/prebid-server/v4/config"
	"github.com/prebid/prebid-server/v4/exchange"
	"github.com/prebid/prebid-server/v4/hooks/hookexecution"
	"github.com/prebid/prebid-server/v4/metrics"
	cleanrtb "github.com/prebid/prebid-server/v4/openrtb"
	"github.com/prebid/prebid-server/v4/openrtb_ext"
	"github.com/prebid/prebid-server/v4/util/fasthttpclient"
	"github.com/prebid/prebid-server/v4/util/jsonutil"
	"runtime/debug"
)

// logSampled emits a log line only once per N calls (per unique caller site).
// Used on hot paths to prevent log flooding under production QPS.
var logSampleCounters sync.Map

func shouldLogSampled(format string, n int64) (int64, bool) {
	val, _ := logSampleCounters.LoadOrStore(format, new(int64))
	counter := val.(*int64)
	cur := atomic.AddInt64(counter, 1)
	return cur, cur == 1 || cur%n == 0
}

func logSampled(n int64, format string, args ...interface{}) {
	cur, emit := shouldLogSampled(format, n)
	if emit {
		log.Printf(format+" [sampled 1/%d, count=%d]", append(args, n, cur)...)
	}
}

// maskIP redacts the last octet of an IPv4 address or the last 80 bits of an
// IPv6 address for PII protection in logs.
func maskIP(ip string) string {
	if i := strings.LastIndex(ip, "."); i >= 0 {
		return ip[:i] + ".xxx"
	}
	if i := strings.LastIndex(ip, ":"); i >= 0 {
		return ip[:i] + ":xxxx"
	}
	return "redacted"
}

// safeGo launches fn in a goroutine with a deferred recover so that a panic
// in any background task is logged instead of crashing the process.
func safeGo(fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC recovered in goroutine: %v\n%s", r, debug.Stack())
			}
		}()
		fn()
	}()
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 2 — Ad Server Config types
// ─────────────────────────────────────────────────────────────────────────────

// AdServerConfig holds placement-level configuration resolved before every auction.
// ExtraDemandCfg holds a secondary/fallback demand endpoint used in waterfall
// when the primary demand source (DemandVASTURL / DemandOrtbURL) returns no fill.
type ExtraDemandCfg struct {
	VASTTagURL   string   `json:"vast_tag_url,omitempty"`
	OrtbURL      string   `json:"ortb_url,omitempty"`
	FloorCPM     float64  `json:"floor_cpm,omitempty"`
	CampaignID   string   `json:"campaign_id,omitempty"`
	AdvertiserID string   `json:"advertiser_id,omitempty"`
	BCat         []string `json:"bcat,omitempty"`
	BAdv         []string `json:"badv,omitempty"`
}

type AdServerConfig struct {
	PlacementID    string   `json:"placement_id"`
	PublisherID    string   `json:"publisher_id"`
	ContentURL     string   `json:"content_url,omitempty"`
	DomainOrApp    string   `json:"domain_or_app,omitempty"`
	MinDuration    int      `json:"min_duration"`
	MaxDuration    int      `json:"max_duration"`
	Protocols      []int    `json:"protocols,omitempty"`
	APIs           []int    `json:"apis,omitempty"`
	AllowedBidders []string `json:"allowed_bidders,omitempty"`
	FloorCPM       float64  `json:"floor_cpm,omitempty"`
	DemandFloorCPM float64  `json:"demand_floor_cpm,omitempty"`
	// Targeting keys forwarded into OpenRTB ext.
	TargetingExt map[string]interface{} `json:"targeting_ext,omitempty"`
	// Direct demand routing — populated when the Ad Unit is linked to a Campaign.
	// When DemandVASTURL is set the pipeline returns a VAST wrapper around it.
	// When DemandOrtbURL is set the pipeline POSTs an OpenRTB request to that URL.
	DemandVASTURL string `json:"demand_vast_url,omitempty"`
	DemandOrtbURL string `json:"demand_ortb_url,omitempty"`
	// Blocked advertisers and categories forwarded to demand (from campaign).
	BAdv []string `json:"badv,omitempty"`
	BCat []string `json:"bcat,omitempty"`
	// ExtraDemand holds additional demand sources tried in waterfall order
	// when the primary source returns no fill.
	ExtraDemand []ExtraDemandCfg `json:"extra_demand,omitempty"`
	// SeatWeights maps seat/bidder name to a weight used in tie-breaking.
	// Higher weight wins when bids have equal price.
	SeatWeights map[string]float64 `json:"seat_weights,omitempty"`
	// MimeTypes lists accepted video MIME types for the OpenRTB imp.video.mimes
	// field. Defaults to ["video/mp4"] when empty.
	MimeTypes []string `json:"mime_types,omitempty"`
	// Active indicates whether this placement is enabled. When false, the
	// pipeline returns no-fill immediately without contacting demand.
	Active bool `json:"active"`
	// TimeoutMS is the ad-unit-level auction timeout in milliseconds.
	// Overrides the default 500 ms when > 0; player-level TMax still takes
	// precedence if supplied.
	TimeoutMS int `json:"timeout_ms,omitempty"`
	// RequestBaseURL is set per-request to the scheme+host seen by the player
	// (e.g. "http://adzrvr.com"). Used to build self-referencing tracking URLs
	// instead of the static external_url config value.
	RequestBaseURL string `json:"-"`
	// OrtbVersion sets the X-OpenRTB-Version header value on outbound ORTB
	// requests to demand. Defaults to "2.5" when empty.
	OrtbVersion string `json:"ortb_version,omitempty"`

	// VideoPlacementType holds the ad unit's placement type string
	// ("instream"|"outstream"|"interstitial"|"rewarded") used to set
	// imp.video.placement in outbound ORTB requests.
	VideoPlacementType string `json:"-"`
	// CampaignID is the dashboard campaign linked to this ad unit (informational,
	// used for bid reporting).
	CampaignID string `json:"-"`
	// AdvertiserID is the dashboard advertiser owning the linked campaign.
	// Used to route per-advertiser stats into videoStatsStore.ByAdvertiser.
	AdvertiserID string `json:"-"`

	// SellerDomain is this exchange's domain used in the supply chain
	// (schain) node. Set via config to identify the exchange in the chain.
	SellerDomain string `json:"seller_domain,omitempty"`

	// CTV-specific fields
	// PodSequence controls multi-pod ad break ordering (1 = first, 2 = second, …).
	// Maps to imp.video.podseq in ORTB 2.6. 0 means single-ad (no pod).
	PodSequence int `json:"pod_sequence,omitempty"`
	// PodDuration limits the total duration (seconds) of a pod ad break.
	// Maps to imp.video.poddur. 0 means no pod.
	PodDuration int `json:"pod_duration,omitempty"`
	// MaxSeq limits the number of ads in a pod (imp.video.maxseq).
	MaxSeq int `json:"max_seq,omitempty"`
	// CompanionType specifies accepted companion ad types for CTV overlay inventory.
	// 1=Static, 2=HTML, 3=iframe (imp.video.companiontype).
	CompanionType []int `json:"companion_type,omitempty"`
	// CatTax sets the category taxonomy version (imp.cattax / bidrequest.cattax).
	// 1=IAB 1.0, 2=IAB 2.0, 7=IAB 3.0 Content Taxonomy (default for CTV).
	CatTax int `json:"cattax,omitempty"`
}

func (cfg *AdServerConfig) outboundBidFloorCPM() float64 {
	if cfg == nil {
		return 0
	}
	if cfg.DemandFloorCPM > 0 && (cfg.DemandOrtbURL != "" || cfg.DemandVASTURL != "") {
		return cfg.DemandFloorCPM
	}
	return cfg.FloorCPM
}

func (cfg *AdServerConfig) winnerMinPriceCPM() float64 {
	if cfg == nil {
		return 0
	}
	if cfg.DemandFloorCPM > 0 && (cfg.DemandOrtbURL != "" || cfg.DemandVASTURL != "") {
		return cfg.DemandFloorCPM
	}
	return cfg.FloorCPM
}

// adServerConfigStore is a thread-safe registry of AdServerConfig keyed by PlacementID.
// Configs are persisted to disk so placement settings survive server restarts.
type adServerConfigStore struct {
	mu        sync.RWMutex
	configs   map[string]*AdServerConfig
	filePath  string
	label     string
	saveMu    sync.Mutex  // serialises disk writes so concurrent set() calls don't race
	saveTimer *time.Timer // debounce: coalesces rapid set() bursts into a single write
}

func newAdServerConfigStore(filePath string) *adServerConfigStore {
	s := &adServerConfigStore{configs: make(map[string]*AdServerConfig), filePath: filePath, label: "ad_server_configs"}
	s.load()
	return s
}

// load reads previously-saved configs from PostgreSQL when configured,
// otherwise from disk as a fallback.
func (s *adServerConfigStore) load() {
	if db := getDashDB(); db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		rows, err := db.QueryContext(ctx, `SELECT payload FROM dashboard_entities WHERE kind=$1`, s.label)
		if err != nil {
			log.Printf("config store: load db: %v", err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var payload []byte
			if err := rows.Scan(&payload); err != nil {
				log.Printf("config store: scan db: %v", err)
				return
			}
			var cfg AdServerConfig
			if err := json.Unmarshal(payload, &cfg); err != nil {
				log.Printf("config store: parse db: %v", err)
				continue
			}
			cp := cfg
			s.configs[cfg.PlacementID] = &cp
		}
		return
	}
	if s.filePath == "" {
		return
	}
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("config store: load: %v", err)
		}
		return
	}
	var m map[string]AdServerConfig
	if err := json.Unmarshal(data, &m); err != nil {
		log.Printf("config store: parse: %v", err)
		return
	}
	for k, v := range m {
		cp := v
		s.configs[k] = &cp
	}
}

// save writes current configs to PostgreSQL when configured, otherwise to disk.
// Serialised by saveMu so concurrent calls never race on the storage backend.
func (s *adServerConfigStore) save() {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	if db := getDashDB(); db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Printf("config store: begin tx: %v", err)
			return
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM dashboard_entities WHERE kind=$1`, s.label); err != nil {
			log.Printf("config store: purge db: %v", err)
			_ = tx.Rollback()
			return
		}
		s.mu.RLock()
		for placementID, cfg := range s.configs {
			payload, err := json.Marshal(cfg)
			if err != nil {
				log.Printf("config store: marshal db: %v", err)
				continue
			}
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO dashboard_entities(kind,id,payload) VALUES ($1,$2,$3)`,
				s.label, placementID, payload,
			); err != nil {
				log.Printf("config store: insert db: %v", err)
			}
		}
		s.mu.RUnlock()
		if err := tx.Commit(); err != nil {
			log.Printf("config store: commit db: %v", err)
		}
		return
	}
	if s.filePath == "" {
		return
	}
	s.mu.RLock()
	out := make(map[string]AdServerConfig, len(s.configs))
	for k, v := range s.configs {
		out[k] = *v
	}
	s.mu.RUnlock()
	if err := os.MkdirAll(filepath.Dir(s.filePath), 0755); err != nil {
		log.Printf("config store: mkdir: %v", err)
		return
	}
	data, err := json.Marshal(out)
	if err != nil {
		log.Printf("config store: marshal: %v", err)
		return
	}
	tmp := s.filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		log.Printf("config store: write: %v", err)
		return
	}
	if err := os.Rename(tmp, s.filePath); err != nil {
		log.Printf("config store: rename: %v", err)
	}
}

// set registers or replaces a config entry and schedules a debounced persist.
// Rapid successive calls are coalesced into a single disk write (500 ms delay).
func (s *adServerConfigStore) set(c *AdServerConfig) {
	s.mu.Lock()
	s.configs[c.PlacementID] = c
	if s.saveTimer != nil {
		s.saveTimer.Stop()
	}
	s.saveTimer = time.AfterFunc(500*time.Millisecond, s.safeSave)
	s.mu.Unlock()
}

// get returns the config for placementID, or nil if not found.
func (s *adServerConfigStore) get(placementID string) *AdServerConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.configs[placementID]
}

// remove deletes a config entry and schedules a debounced persist.
func (s *adServerConfigStore) remove(placementID string) {
	s.mu.Lock()
	delete(s.configs, placementID)
	if s.saveTimer != nil {
		s.saveTimer.Stop()
	}
	s.saveTimer = time.AfterFunc(500*time.Millisecond, s.safeSave)
	s.mu.Unlock()
}

func (s *adServerConfigStore) safeSave() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC recovered in config store save: %v", r)
		}
	}()
	s.save()
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 7 — Tracking event store
// ─────────────────────────────────────────────────────────────────────────────

// EventType enumerates VAST tracking events.
type EventType string

const (
	EventImpression    EventType = "impression"
	EventStart         EventType = "start"
	EventFirstQuartile EventType = "firstQuartile"
	EventMidpoint      EventType = "midpoint"
	EventThirdQuartile EventType = "thirdQuartile"
	EventComplete      EventType = "complete"
	EventClick         EventType = "click"
)

// TrackingEvent records a single player-fired tracking beacon.
type TrackingEvent struct {
	AuctionID   string    `json:"auction_id"`
	BidID       string    `json:"bid_id"`
	Bidder      string    `json:"bidder"`
	PlacementID string    `json:"placement_id"`
	Event       EventType `json:"event"`
	CrID        string    `json:"crid,omitempty"`
	Price       float64   `json:"price,omitempty"`
	ADomain     string    `json:"adom,omitempty"`
	ReceivedAt  time.Time `json:"received_at"`
}

type trackingEventCode uint8

const (
	trackingEventCustom trackingEventCode = iota
	trackingEventImpression
	trackingEventStart
	trackingEventFirstQuartile
	trackingEventMidpoint
	trackingEventThirdQuartile
	trackingEventComplete
	trackingEventClick
)

type trackingEventRecord struct {
	seq              int64
	AuctionID        string
	BidID            string
	Bidder           string
	PlacementID      string
	Event            trackingEventCode
	Price            float64
	ReceivedAtUnixNs int64
}

type trackingEventExtras struct {
	CrID        string
	ADomain     string
	CustomEvent EventType
}

// trackingStore persists tracking events in a fixed-size ring buffer.
// The ring avoids slice shifts and keeps the hot-path record() O(1)
// with minimal lock hold time.
type trackingStore struct {
	mu     sync.RWMutex
	ring   []trackingEventRecord
	extras map[int64]trackingEventExtras
	pos    int
	full   bool
	count  int64
}

const trackingStoreMaxEvents = 100_000

func (t *trackingStore) record(ev TrackingEvent) {
	t.mu.Lock()
	if t.ring == nil {
		t.ring = make([]trackingEventRecord, trackingStoreMaxEvents)
	}
	t.count++
	seq := t.count
	if oldSeq := t.ring[t.pos].seq; oldSeq != 0 && t.extras != nil {
		delete(t.extras, oldSeq)
	}
	code, customEvent := trackingEventCodeFromEventType(ev.Event)
	t.ring[t.pos] = trackingEventRecord{
		seq:              seq,
		AuctionID:        ev.AuctionID,
		BidID:            ev.BidID,
		Bidder:           ev.Bidder,
		PlacementID:      ev.PlacementID,
		Event:            code,
		Price:            ev.Price,
		ReceivedAtUnixNs: ev.ReceivedAt.UnixNano(),
	}
	if ev.CrID != "" || ev.ADomain != "" || customEvent != "" {
		if t.extras == nil {
			t.extras = make(map[int64]trackingEventExtras)
		}
		t.extras[seq] = trackingEventExtras{
			CrID:        ev.CrID,
			ADomain:     ev.ADomain,
			CustomEvent: customEvent,
		}
	}
	t.pos++
	if t.pos >= trackingStoreMaxEvents {
		t.pos = 0
		t.full = true
	}
	t.mu.Unlock()
}

func (t *trackingStore) all() []TrackingEvent {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.ring == nil {
		return []TrackingEvent{}
	}
	var n int
	if t.full {
		n = trackingStoreMaxEvents
	} else {
		n = t.pos
	}
	if n == 0 {
		return []TrackingEvent{}
	}
	cp := make([]TrackingEvent, n)
	if t.full {
		for i := 0; i < trackingStoreMaxEvents-t.pos; i++ {
			cp[i] = t.expand(t.ring[t.pos+i])
		}
		for i := 0; i < t.pos; i++ {
			cp[trackingStoreMaxEvents-t.pos+i] = t.expand(t.ring[i])
		}
	} else {
		for i := 0; i < t.pos; i++ {
			cp[i] = t.expand(t.ring[i])
		}
	}
	return cp
}

func (t *trackingStore) expand(rec trackingEventRecord) TrackingEvent {
	ev := TrackingEvent{
		AuctionID:   rec.AuctionID,
		BidID:       rec.BidID,
		Bidder:      rec.Bidder,
		PlacementID: rec.PlacementID,
		Event:       trackingEventType(rec.Event),
		Price:       rec.Price,
		ReceivedAt:  time.Unix(0, rec.ReceivedAtUnixNs),
	}
	if rec.seq == 0 || t.extras == nil {
		return ev
	}
	if extra, ok := t.extras[rec.seq]; ok {
		ev.CrID = extra.CrID
		ev.ADomain = extra.ADomain
		if extra.CustomEvent != "" {
			ev.Event = extra.CustomEvent
		}
	}
	return ev
}

func trackingEventCodeFromEventType(event EventType) (trackingEventCode, EventType) {
	switch event {
	case EventImpression:
		return trackingEventImpression, ""
	case EventStart:
		return trackingEventStart, ""
	case EventFirstQuartile:
		return trackingEventFirstQuartile, ""
	case EventMidpoint:
		return trackingEventMidpoint, ""
	case EventThirdQuartile:
		return trackingEventThirdQuartile, ""
	case EventComplete:
		return trackingEventComplete, ""
	case EventClick:
		return trackingEventClick, ""
	default:
		return trackingEventCustom, event
	}
}

func trackingEventType(code trackingEventCode) EventType {
	switch code {
	case trackingEventImpression:
		return EventImpression
	case trackingEventStart:
		return EventStart
	case trackingEventFirstQuartile:
		return EventFirstQuartile
	case trackingEventMidpoint:
		return EventMidpoint
	case trackingEventThirdQuartile:
		return EventThirdQuartile
	case trackingEventComplete:
		return EventComplete
	case trackingEventClick:
		return EventClick
	default:
		return ""
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-publisher real-time ad statistics
// ─────────────────────────────────────────────────────────────────────────────

// VideoStats holds live counters for a single publisher.
type VideoStats struct {
	AdRequests       int64   `json:"ad_requests"`
	Opportunities    int64   `json:"opportunities"` // VAST served (adapter returned a creative)
	Impressions      int64   `json:"impressions"`   // player-confirmed: /video/tracking?event=impression
	Starts           int64   `json:"starts"`        // player-confirmed: /video/tracking?event=start
	Completes        int64   `json:"completes"`     // player-confirmed: /video/tracking?event=complete
	VCR              float64 `json:"vcr_pct"`       // video completion rate: completes ÷ starts × 100
	Revenue          float64 `json:"revenue"`       // advertiser revenue in USD (demand win CPM ÷ 1000 per impression)
	PublisherRevenue float64 `json:"publisher_revenue,omitempty"`
}

// VideoStatsPayload is the JSON payload returned by /dashboard/stats/video.
type VideoStatsPayload struct {
	ByPublisher                map[string]*VideoStats `json:"by_publisher"`
	ByAdvertiser               map[string]*VideoStats `json:"by_advertiser"`
	ByBidder                   map[string]*VideoStats `json:"by_bidder"`
	ByApp                      map[string]*VideoStats `json:"by_app"`
	ByPlacement                map[string]*VideoStats `json:"by_placement"`
	ByCountry                  map[string]*VideoStats `json:"by_country"`
	ByDevice                   map[string]*VideoStats `json:"by_device"`
	ByFormat                   map[string]*VideoStats `json:"by_format"`
	ByDemandChannel            map[string]*VideoStats `json:"by_demand_channel"`
	PublisherRequestsLastDay   map[string]int64       `json:"publisher_requests_last_day,omitempty"`
	PublisherRequestsLastHour  map[string]int64       `json:"publisher_requests_last_hour,omitempty"`
	AdvertiserRequestsLastDay  map[string]int64       `json:"advertiser_requests_last_day,omitempty"`
	AdvertiserRequestsLastHour map[string]int64       `json:"advertiser_requests_last_hour,omitempty"`
	Total                      VideoStats             `json:"total"`
	StartedAt                  int64                  `json:"started_at"` // Unix timestamp of server start; used for QPS calculations
}

type VideoOverviewSummary struct {
	AdRequests       int64   `json:"ad_requests"`
	Opportunities    int64   `json:"opportunities"`
	Impressions      int64   `json:"impressions"`
	Starts           int64   `json:"starts"`
	Completes        int64   `json:"completes"`
	Revenue          float64 `json:"revenue"`
	PublisherRevenue float64 `json:"publisher_revenue"`
	Margin           float64 `json:"margin"`
	ECPM             float64 `json:"ecpm"`
	FillRate         float64 `json:"fill_rate"`
	ResponseRate     float64 `json:"response_rate"`
	VCR              float64 `json:"vcr"`
	Viewability      float64 `json:"viewability"`
}

type VideoOverviewChart struct {
	Labels               []string `json:"labels"`
	CurrentRequests      []int64  `json:"current_requests"`
	CurrentOpportunities []int64  `json:"current_opportunities"`
	CurrentImpressions   []int64  `json:"current_impressions"`
	PreviousRequests     []int64  `json:"previous_requests"`
}

type VideoOverviewPayload struct {
	Period        string               `json:"period"`
	Label         string               `json:"label"`
	PreviousLabel string               `json:"previous_label"`
	Timezone      string               `json:"timezone"`
	Current       VideoOverviewSummary `json:"current"`
	Previous      VideoOverviewSummary `json:"previous"`
	Chart         VideoOverviewChart   `json:"chart"`
	UpdatedAt     string               `json:"updated_at"`
	Source        string               `json:"source"`
}

// videoStatsDisk is the on-disk format (v3, backward-compatible with v2/legacy).
// Older files with just by_pub/by_adv are detected and loaded automatically.
type videoStatsDisk struct {
	ByPub       map[string]VideoStats `json:"by_pub"`
	ByAdv       map[string]VideoStats `json:"by_adv"`
	ByBidder    map[string]VideoStats `json:"by_bidder,omitempty"`
	ByApp       map[string]VideoStats `json:"by_app,omitempty"`
	ByPlacement map[string]VideoStats `json:"by_placement,omitempty"`
	ByCountry   map[string]VideoStats `json:"by_country,omitempty"`
	ByDevice    map[string]VideoStats `json:"by_device,omitempty"`
	ByFormat    map[string]VideoStats `json:"by_format,omitempty"`
	ByDemandCh  map[string]VideoStats `json:"by_demand_ch,omitempty"`
}

// auctionDimKey stores dimension labels for one auction so impression/complete
// beacons fired later can credit the right per-dimension stat buckets.
type auctionDimKey struct {
	Bidder            string
	App               string
	Placement         string
	Country           string
	Device            string
	Format            string
	DemandCh          string
	PriceCPM          float64 // bid price in CPM — used to attribute revenue at impression time
	PublisherPriceCPM float64
	PublisherID       string // cached so impression beacon can credit the right publisher
	AdvertiserID      string // cached so impression beacon can credit the right advertiser
	born              int64  // unix seconds — used for TTL eviction
}

// videoStatsStore tracks per-publisher, per-advertiser, and per-dimension stats
// under a single mutex.
type videoStatsStore struct {
	mu              sync.Mutex
	byPub           map[string]*VideoStats
	byAdvertiser    map[string]*VideoStats
	byBidder        map[string]*VideoStats
	byApp           map[string]*VideoStats
	byPlacement     map[string]*VideoStats
	byCountry       map[string]*VideoStats
	byDevice        map[string]*VideoStats
	byFormat        map[string]*VideoStats
	byDemandChannel map[string]*VideoStats
	auctionDims     map[string]*auctionDimKey // ephemeral — not persisted
	filePath        string
	startedAt       time.Time // set once at creation; used for QPS calculations
}

func newVideoStatsStore(filePath string) *videoStatsStore {
	s := &videoStatsStore{
		byPub:           make(map[string]*VideoStats),
		byAdvertiser:    make(map[string]*VideoStats),
		byBidder:        make(map[string]*VideoStats),
		byApp:           make(map[string]*VideoStats),
		byPlacement:     make(map[string]*VideoStats),
		byCountry:       make(map[string]*VideoStats),
		byDevice:        make(map[string]*VideoStats),
		byFormat:        make(map[string]*VideoStats),
		byDemandChannel: make(map[string]*VideoStats),
		auctionDims:     make(map[string]*auctionDimKey),
		filePath:        filePath,
		startedAt:       time.Now(),
	}
	s.load()
	return s
}

func videoCompletionRate(stats VideoStats) float64 {
	if stats.Starts > 0 {
		return float64(stats.Completes) / float64(stats.Starts) * 100
	}
	if stats.Impressions > 0 {
		return float64(stats.Completes) / float64(stats.Impressions) * 100
	}
	return 0
}

// load reads previously-saved stats from disk (best effort; skips if file absent).
// Supports v3 format (all dimension maps), v2 {by_pub,by_adv}, and legacy flat map.
func (s *videoStatsStore) load() {
	if s.filePath == "" {
		return
	}
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("video stats: load: %v", err)
		}
		return
	}
	// Try v2/v3 disk format first.
	var disk videoStatsDisk
	if err := json.Unmarshal(data, &disk); err == nil && disk.ByPub != nil {
		loadMap := func(dst map[string]*VideoStats, src map[string]VideoStats) {
			for k, v := range src {
				cp := v
				dst[k] = &cp
			}
		}
		loadMap(s.byPub, disk.ByPub)
		loadMap(s.byAdvertiser, disk.ByAdv)
		loadMap(s.byBidder, disk.ByBidder)
		loadMap(s.byApp, disk.ByApp)
		loadMap(s.byPlacement, disk.ByPlacement)
		loadMap(s.byCountry, disk.ByCountry)
		loadMap(s.byDevice, disk.ByDevice)
		loadMap(s.byFormat, disk.ByFormat)
		loadMap(s.byDemandChannel, disk.ByDemandCh)
		return
	}
	// Fall back to legacy flat map (publisher stats only).
	var legacy map[string]VideoStats
	if err := json.Unmarshal(data, &legacy); err != nil {
		log.Printf("video stats: parse: %v", err)
		return
	}
	for k, v := range legacy {
		cp := v
		s.byPub[k] = &cp
	}
}

// save writes current stats to disk atomically (v3 format).
// Also evicts expired entries from the in-memory auctionDims cache.
func (s *videoStatsStore) save() {
	if s.filePath == "" {
		return
	}
	s.mu.Lock()
	snapMap := func(src map[string]*VideoStats) map[string]VideoStats {
		m := make(map[string]VideoStats, len(src))
		for k, v := range src {
			m[k] = *v
		}
		return m
	}
	disk := videoStatsDisk{
		ByPub:       snapMap(s.byPub),
		ByAdv:       snapMap(s.byAdvertiser),
		ByBidder:    snapMap(s.byBidder),
		ByApp:       snapMap(s.byApp),
		ByPlacement: snapMap(s.byPlacement),
		ByCountry:   snapMap(s.byCountry),
		ByDevice:    snapMap(s.byDevice),
		ByFormat:    snapMap(s.byFormat),
		ByDemandCh:  snapMap(s.byDemandChannel),
	}
	// Evict auction dim cache entries older than 30 minutes.
	cutoff := time.Now().Unix() - 30*60
	for k, v := range s.auctionDims {
		if v.born < cutoff {
			delete(s.auctionDims, k)
		}
	}
	s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.filePath), 0755); err != nil {
		log.Printf("video stats: mkdir: %v", err)
		return
	}
	data, err := json.Marshal(disk)
	if err != nil {
		log.Printf("video stats: marshal: %v", err)
		return
	}
	tmp := s.filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		log.Printf("video stats: write: %v", err)
		return
	}
	if err := os.Rename(tmp, s.filePath); err != nil {
		log.Printf("video stats: rename: %v", err)
	}
}

// getOrCreate returns (or creates) the VideoStats entry for pubID.
// Must be called while holding s.mu.
func (s *videoStatsStore) getOrCreate(pubID string) *VideoStats {
	v := s.byPub[pubID]
	if v == nil {
		v = &VideoStats{}
		s.byPub[pubID] = v
	}
	return v
}

// getOrCreateAdv returns (or creates) the VideoStats entry for advID.
// Must be called while holding s.mu.
func (s *videoStatsStore) getOrCreateAdv(advID string) *VideoStats {
	v := s.byAdvertiser[advID]
	if v == nil {
		v = &VideoStats{}
		s.byAdvertiser[advID] = v
	}
	return v
}

// getOrCreateDim returns (or creates) the VideoStats entry for key in m.
// Must be called while holding s.mu.
func getOrCreateDim(m map[string]*VideoStats, key string) *VideoStats {
	v := m[key]
	if v == nil {
		v = &VideoStats{}
		m[key] = v
	}
	return v
}

// deviceTypeLabel maps an IAB OpenRTB device-type integer to a readable label.
func deviceTypeLabel(dt int) string {
	switch dt {
	case 1:
		return "Mobile/Tablet"
	case 2:
		return "Desktop"
	case 3:
		return "CTV"
	case 4:
		return "Phone"
	case 5:
		return "Tablet"
	case 6:
		return "Connected Device"
	case 7:
		return "Set-Top Box"
	default:
		return "Unknown"
	}
}

// demandChannelLabel maps a DemandType to a human-readable channel name.
func demandChannelLabel(dt DemandType) string {
	switch dt {
	case DemandTypeVAST:
		return "VAST Tag"
	case DemandTypeORTB:
		return "OpenRTB"
	case DemandTypePrebid:
		return "Prebid"
	default:
		return "Unknown"
	}
}

// incDimFill credits all 7 programmatic dimension buckets for one fill event
// and caches the auction's dimension keys for later impression/complete attribution.
func (s *videoStatsStore) incDimFill(cfg *AdServerConfig, pr *PlayerRequest, resp *DemandResponse, demandType DemandType) {
	if resp == nil || resp.AuctionID == "" {
		return
	}
	bidder := resp.Bidder
	if bidder == "" {
		bidder = "Unknown"
	}
	app := pr.AppBundle
	if app == "" {
		app = pr.Domain
	}
	if app == "" {
		app = "(unknown)"
	}
	placement := cfg.PlacementID
	if placement == "" {
		placement = "(unknown)"
	}
	country := pr.CountryCode
	if country == "" {
		country = "(unknown)"
	}
	device := deviceTypeLabel(pr.DeviceType)
	format := cfg.VideoPlacementType
	if format == "" {
		format = "(unknown)"
	}
	demandCh := demandChannelLabel(demandType)
	s.mu.Lock()
	incD := func(m map[string]*VideoStats, key string) {
		v := getOrCreateDim(m, key)
		v.Opportunities++
	}
	incD(s.byBidder, bidder)
	incD(s.byApp, app)
	incD(s.byPlacement, placement)
	incD(s.byCountry, country)
	incD(s.byDevice, device)
	incD(s.byFormat, format)
	incD(s.byDemandChannel, demandCh)
	s.auctionDims[resp.AuctionID] = &auctionDimKey{
		Bidder:            bidder,
		App:               app,
		Placement:         placement,
		Country:           country,
		Device:            device,
		Format:            format,
		DemandCh:          demandCh,
		PriceCPM:          resp.WinPrice,
		PublisherPriceCPM: cfg.FloorCPM,
		PublisherID:       cfg.PublisherID,
		AdvertiserID:      cfg.AdvertiserID,
		born:              time.Now().Unix(),
	}
	s.mu.Unlock()
}

// incDimImpression credits all 7 dimension buckets for the given auctionID's impression.
// Revenue is also attributed here using PriceCPM cached at fill time (CPM / 1000 = USD per impression).
// Returns the cached auctionDimKey (or nil if not found) so callers can use the
// authoritative price/pubID/advID without relying on beacon URL parameters.
func (s *videoStatsStore) incDimImpression(auctionID string) *auctionDimKey {
	if auctionID == "" {
		return nil
	}
	s.mu.Lock()
	dk := s.auctionDims[auctionID]
	if dk != nil {
		revenueUSD := dk.PriceCPM / 1000
		publisherRevenueUSD := dk.PublisherPriceCPM / 1000
		incDim := func(m map[string]*VideoStats, key string) {
			if v := m[key]; v != nil {
				v.Impressions++
				v.Revenue += revenueUSD
				v.PublisherRevenue += publisherRevenueUSD
			}
		}
		incDim(s.byBidder, dk.Bidder)
		incDim(s.byApp, dk.App)
		incDim(s.byPlacement, dk.Placement)
		incDim(s.byCountry, dk.Country)
		incDim(s.byDevice, dk.Device)
		incDim(s.byFormat, dk.Format)
		incDim(s.byDemandChannel, dk.DemandCh)
	}
	s.mu.Unlock()
	return dk
}

// incDimComplete credits all 7 dimension buckets for the given auctionID's complete.
func (s *videoStatsStore) incDimComplete(auctionID string) {
	if auctionID == "" {
		return
	}
	s.mu.Lock()
	dk := s.auctionDims[auctionID]
	if dk != nil {
		if v := s.byBidder[dk.Bidder]; v != nil {
			v.Completes++
		}
		if v := s.byApp[dk.App]; v != nil {
			v.Completes++
		}
		if v := s.byPlacement[dk.Placement]; v != nil {
			v.Completes++
		}
		if v := s.byCountry[dk.Country]; v != nil {
			v.Completes++
		}
		if v := s.byDevice[dk.Device]; v != nil {
			v.Completes++
		}
		if v := s.byFormat[dk.Format]; v != nil {
			v.Completes++
		}
		if v := s.byDemandChannel[dk.DemandCh]; v != nil {
			v.Completes++
		}
	}
	s.mu.Unlock()
}

// incDimStart credits all 7 dimension buckets for the given auctionID's start.
func (s *videoStatsStore) incDimStart(auctionID string) {
	if auctionID == "" {
		return
	}
	s.mu.Lock()
	dk := s.auctionDims[auctionID]
	if dk != nil {
		if v := s.byBidder[dk.Bidder]; v != nil {
			v.Starts++
		}
		if v := s.byApp[dk.App]; v != nil {
			v.Starts++
		}
		if v := s.byPlacement[dk.Placement]; v != nil {
			v.Starts++
		}
		if v := s.byCountry[dk.Country]; v != nil {
			v.Starts++
		}
		if v := s.byDevice[dk.Device]; v != nil {
			v.Starts++
		}
		if v := s.byFormat[dk.Format]; v != nil {
			v.Starts++
		}
		if v := s.byDemandChannel[dk.DemandCh]; v != nil {
			v.Starts++
		}
	}
	s.mu.Unlock()
}

// incRequestBatch batches both publisher and advertiser request increments
// under a single lock acquisition — 1 lock instead of 2 per request.
func (s *videoStatsStore) incRequestBatch(pubID, advID string) {
	if pubID == "" {
		pubID = "unknown"
	}
	s.mu.Lock()
	s.getOrCreate(pubID).AdRequests++
	if advID != "" {
		s.getOrCreateAdv(advID).AdRequests++
	}
	s.mu.Unlock()
}

// incFillBatch batches both publisher and advertiser fill increments
// under a single lock acquisition.
func (s *videoStatsStore) incFillBatch(pubID, advID string) {
	if pubID == "" {
		pubID = "unknown"
	}
	s.mu.Lock()
	s.getOrCreate(pubID).Opportunities++
	if advID != "" {
		s.getOrCreateAdv(advID).Opportunities++
	}
	s.mu.Unlock()
}

func (s *videoStatsStore) incRequest(pubID string) {
	if pubID == "" {
		pubID = "unknown"
	}
	s.mu.Lock()
	s.getOrCreate(pubID).AdRequests++
	s.mu.Unlock()
}

func (s *videoStatsStore) incFill(pubID string) {
	if pubID == "" {
		pubID = "unknown"
	}
	s.mu.Lock()
	s.getOrCreate(pubID).Opportunities++
	s.mu.Unlock()
}

func (s *videoStatsStore) incAdvertiserRequest(advID string) {
	if advID == "" {
		return
	}
	s.mu.Lock()
	s.getOrCreateAdv(advID).AdRequests++
	s.mu.Unlock()
}

func (s *videoStatsStore) incAdvertiserFill(advID string) {
	if advID == "" {
		return
	}
	s.mu.Lock()
	s.getOrCreateAdv(advID).Opportunities++
	s.mu.Unlock()
}

func (s *videoStatsStore) incStart(pubID string) {
	if pubID == "" {
		pubID = "unknown"
	}
	s.mu.Lock()
	s.getOrCreate(pubID).Starts++
	s.mu.Unlock()
}

func (s *videoStatsStore) incAdvertiserStart(advID string) {
	if advID == "" {
		return
	}
	s.mu.Lock()
	s.getOrCreateAdv(advID).Starts++
	s.mu.Unlock()
}

// incImpressionBatch batches publisher + advertiser impression under one lock.
func (s *videoStatsStore) incImpressionBatch(pubID, advID string, advertiserPriceCPM, publisherPriceCPM float64) {
	if pubID == "" {
		pubID = "unknown"
	}
	advertiserRevenueUSD := advertiserPriceCPM / 1000
	publisherRevenueUSD := publisherPriceCPM / 1000
	s.mu.Lock()
	v := s.getOrCreate(pubID)
	v.Impressions++
	v.Revenue += advertiserRevenueUSD
	v.PublisherRevenue += publisherRevenueUSD
	if advID != "" {
		a := s.getOrCreateAdv(advID)
		a.Impressions++
		a.Revenue += advertiserRevenueUSD
		a.PublisherRevenue += publisherRevenueUSD
	}
	s.mu.Unlock()
}

func (s *videoStatsStore) incImpression(pubID string, price float64) {
	if pubID == "" {
		pubID = "unknown"
	}
	s.mu.Lock()
	v := s.getOrCreate(pubID)
	v.Impressions++
	v.Revenue += price / 1000
	s.mu.Unlock()
}

func (s *videoStatsStore) incAdvertiserImpression(advID string, price float64) {
	if advID == "" {
		return
	}
	s.mu.Lock()
	v := s.getOrCreateAdv(advID)
	v.Impressions++
	v.Revenue += price / 1000
	s.mu.Unlock()
}

func (s *videoStatsStore) incComplete(pubID string) {
	if pubID == "" {
		pubID = "unknown"
	}
	s.mu.Lock()
	s.getOrCreate(pubID).Completes++
	s.mu.Unlock()
}

func (s *videoStatsStore) incAdvertiserComplete(advID string) {
	if advID == "" {
		return
	}
	s.mu.Lock()
	s.getOrCreateAdv(advID).Completes++
	s.mu.Unlock()
}

func (s *videoStatsStore) snapshot() VideoStatsPayload {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapDim := func(src map[string]*VideoStats) map[string]*VideoStats {
		out := make(map[string]*VideoStats, len(src))
		for k, v := range src {
			cp := *v
			cp.VCR = videoCompletionRate(cp)
			out[k] = &cp
		}
		return out
	}
	out := VideoStatsPayload{
		ByPublisher:     make(map[string]*VideoStats, len(s.byPub)),
		ByAdvertiser:    make(map[string]*VideoStats, len(s.byAdvertiser)),
		ByBidder:        snapDim(s.byBidder),
		ByApp:           snapDim(s.byApp),
		ByPlacement:     snapDim(s.byPlacement),
		ByCountry:       snapDim(s.byCountry),
		ByDevice:        snapDim(s.byDevice),
		ByFormat:        snapDim(s.byFormat),
		ByDemandChannel: snapDim(s.byDemandChannel),
	}
	for k, v := range s.byPub {
		cp := *v
		cp.VCR = videoCompletionRate(cp)
		out.ByPublisher[k] = &cp
		out.Total.AdRequests += cp.AdRequests
		out.Total.Opportunities += cp.Opportunities
		out.Total.Impressions += cp.Impressions
		out.Total.Starts += cp.Starts
		out.Total.Completes += cp.Completes
		out.Total.Revenue += cp.Revenue
	}
	for k, v := range s.byAdvertiser {
		cp := *v
		cp.VCR = videoCompletionRate(cp)
		out.ByAdvertiser[k] = &cp
	}
	out.Total.VCR = videoCompletionRate(out.Total)
	out.StartedAt = s.startedAt.Unix()
	return out
}

// reset clears all stats from memory and removes the on-disk persistence file.
func (s *videoStatsStore) reset() {
	s.mu.Lock()
	s.byPub = make(map[string]*VideoStats)
	s.byAdvertiser = make(map[string]*VideoStats)
	s.byBidder = make(map[string]*VideoStats)
	s.byApp = make(map[string]*VideoStats)
	s.byPlacement = make(map[string]*VideoStats)
	s.byCountry = make(map[string]*VideoStats)
	s.byDevice = make(map[string]*VideoStats)
	s.byFormat = make(map[string]*VideoStats)
	s.byDemandChannel = make(map[string]*VideoStats)
	s.auctionDims = make(map[string]*auctionDimKey)
	s.mu.Unlock()
	if s.filePath != "" {
		_ = os.Remove(s.filePath)
		_ = os.Remove(s.filePath + ".tmp")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// VAST XML types — minimal subset sufficient for wrapping
// ─────────────────────────────────────────────────────────────────────────────

// vastRoot is the top-level VAST document.
type vastRoot struct {
	XMLName xml.Name `xml:"VAST"`
	Version string   `xml:"version,attr"`
	Ad      []vastAd `xml:"Ad"`
}

// vastAd represents one <Ad> element.
type vastAd struct {
	ID      string       `xml:"id,attr,omitempty"`
	Inline  *vastInline  `xml:"InLine,omitempty"`
	Wrapper *vastWrapper `xml:"Wrapper,omitempty"`
}

// vastInline carries the actual creative.
type vastInline struct {
	AdSystem   string           `xml:"AdSystem"`
	AdTitle    string           `xml:"AdTitle"`
	Impression []vastImpression `xml:"Impression"`
	Creatives  vastCreatives    `xml:"Creatives"`
}

// vastWrapper wraps an upstream VAST URI.
type vastWrapper struct {
	AdSystem     string           `xml:"AdSystem"`
	VASTAdTagURI vastCDATA        `xml:"VASTAdTagURI"`
	Impression   []vastImpression `xml:"Impression"`
	// TrackingEvents allows BURL and other server-side trackers to be fired
	// from a Wrapper without waiting for the chained InLine to load.
	TrackingEvents *vastTrackingEvents `xml:"TrackingEvents,omitempty"`
}

type vastImpression struct {
	ID    string    `xml:"id,attr,omitempty"`
	Inner vastCDATA `xml:",innerxml"`
}

type vastCreatives struct {
	Creative []vastCreative `xml:"Creative"`
}

type vastCreative struct {
	ID     string      `xml:"id,attr,omitempty"`
	Linear *vastLinear `xml:"Linear,omitempty"`
}

type vastLinear struct {
	Duration       string              `xml:"Duration"`
	TrackingEvents *vastTrackingEvents `xml:"TrackingEvents,omitempty"`
	MediaFiles     vastMediaFiles      `xml:"MediaFiles"`
	VideoClicks    *vastVideoClicks    `xml:"VideoClicks,omitempty"`
}

type vastTracking struct {
	Event string    `xml:"event,attr"`
	Inner vastCDATA `xml:",innerxml"`
}

type vastMediaFiles struct {
	MediaFile []vastMediaFile `xml:"MediaFile"`
}

type vastMediaFile struct {
	Delivery string    `xml:"delivery,attr"`
	Type     string    `xml:"type,attr"`
	Width    int       `xml:"width,attr,omitempty"`
	Height   int       `xml:"height,attr,omitempty"`
	Inner    vastCDATA `xml:",innerxml"`
}

type vastVideoClicks struct {
	ClickThrough *vastCDATA `xml:"ClickThrough,omitempty"`
}

// vastTrackingEvents groups all <Tracking> elements under a single <TrackingEvents> parent.
// Using the nested path tag xml:"TrackingEvents>Tracking" on a slice field would produce
// a separate <TrackingEvents> wrapper per element, which breaks the VAST spec.
type vastTrackingEvents struct {
	Tracking []vastTracking `xml:"Tracking"`
}

// vastCDATA is serialised as a CDATA section.
type vastCDATA struct {
	Text string `xml:",cdata"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline handler
// ─────────────────────────────────────────────────────────────────────────────

// VideoPipelineHandler orchestrates all 7 pipeline stages.
// pendingBURL holds a resolved billing notice URL until the impression
// beacon fires, at which point it is sent server-side per OpenRTB 2.5 §7.2.
type pendingBURL struct {
	URL       string
	ExpiresAt time.Time
}

type impressionKey struct {
	AuctionID string
	BidID     string
}

const impressionDeduperShardCount = 64

type impressionDeduperShard struct {
	mu   sync.Mutex
	seen map[impressionKey]int64
}

type impressionDeduper struct {
	shards [impressionDeduperShardCount]impressionDeduperShard
}

func newImpressionDeduper() *impressionDeduper {
	d := &impressionDeduper{}
	for i := range d.shards {
		d.shards[i].seen = make(map[impressionKey]int64)
	}
	return d
}

func (d *impressionDeduper) SeenOrAdd(key impressionKey, seenAtUnix int64) bool {
	shard := d.shard(key)
	shard.mu.Lock()
	_, exists := shard.seen[key]
	if !exists {
		shard.seen[key] = seenAtUnix
	}
	shard.mu.Unlock()
	return exists
}

func (d *impressionDeduper) CleanupBefore(cutoffUnix int64) {
	for i := range d.shards {
		shard := &d.shards[i]
		shard.mu.Lock()
		for key, seenAt := range shard.seen {
			if seenAt < cutoffUnix {
				delete(shard.seen, key)
			}
		}
		shard.mu.Unlock()
	}
}

func (d *impressionDeduper) shard(key impressionKey) *impressionDeduperShard {
	return &d.shards[hashImpressionKey(key)&(impressionDeduperShardCount-1)]
}

func hashImpressionKey(key impressionKey) uint64 {
	const (
		offset64 = 1469598103934665603
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(key.AuctionID); i++ {
		h ^= uint64(key.AuctionID[i])
		h *= prime64
	}
	h ^= ':'
	h *= prime64
	for i := 0; i < len(key.BidID); i++ {
		h ^= uint64(key.BidID[i])
		h *= prime64
	}
	return h
}

type VideoPipelineHandler struct {
	exchange          exchange.Exchange
	cfg               *config.Configuration
	metricsEng        metrics.MetricsEngine
	configStore       *adServerConfigStore
	tracking          *trackingStore
	videoStats        *videoStatsStore
	clickHouseMetrics *clickHouseVideoMetricsStore
	bidReport         *BidReportHandler
	externalURL       string
	// demandClient is a shared, pool-backed HTTP client for all outbound calls
	// to demand partners. Tuned for high QPS: 256 keepalive conns per host,
	// 1 s dial timeout, HTTP/2 enabled.
	demandClient *http.Client
	// bufPool reuses byte buffers across requests to eliminate per-request
	// heap allocations for VAST XML and JSON response serialization.
	bufPool         sync.Pool
	adapterRouterFn func(RouterKey) DemandAdapter
	// firedNURLs deduplicates NURL win-notice fires to prevent double-counting.
	firedNURLs sync.Map
	// pendingBURLs caches resolved BURL (billing notice) URLs so they can be
	// fired server-side when the player confirms ad render via ImpressionEndpoint.
	// Key: impressionKey, Value: pendingBURL.
	pendingBURLs sync.Map
	// firedImpressions deduplicates impression beacon fires so the same
	// auction_id:bid_id pair is only counted once (prevents double-count from
	// VAST wrapper chains or player retries).
	firedImpressions *impressionDeduper
	// done is closed by Shutdown to stop background goroutines (stats persistence).
	done chan struct{}
}

// NewVideoPipelineHandler constructs the handler and returns HTTP handles for
// the router. dataDir is the directory used for persistent stats storage;
// pass "" to disable persistence.
func NewVideoPipelineHandler(
	ex exchange.Exchange,
	cfg *config.Configuration,
	me metrics.MetricsEngine,
	dataDir string,
) *VideoPipelineHandler {
	var statsFile string
	if dataDir != "" {
		statsFile = filepath.Join(dataDir, "video_stats.json")
	}
	var configFile string
	if dataDir != "" {
		configFile = filepath.Join(dataDir, "ad_server_configs.json")
	}
	vs := newVideoStatsStore(statsFile)
	h := &VideoPipelineHandler{
		exchange:          ex,
		cfg:               cfg,
		metricsEng:        me,
		configStore:       newAdServerConfigStore(configFile),
		tracking:          &trackingStore{},
		videoStats:        vs,
		clickHouseMetrics: newClickHouseVideoMetricsStoreFromEnv(),
		bidReport:         NewBidReportHandler(dataDir),
		externalURL:       cfg.ExternalURL,
		bufPool: sync.Pool{
			New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 4096)) },
		},
		demandClient: fasthttpclient.NewClient(8*time.Second, fasthttpclient.TransportConfig{
			Name:                "video-demand",
			DialTimeout:         800 * time.Millisecond,
			KeepAlive:           30 * time.Second,
			MaxConnsPerHost:     1024,
			MaxIdleConnDuration: 90 * time.Second,
			ReadTimeout:         5 * time.Second,
			WriteTimeout:        2 * time.Second,
			TLSConfig:           &tls.Config{InsecureSkipVerify: os.Getenv("PBS_INSECURE_TLS") == "1"}, //nolint:gosec
			ReadBufferSize:      8192,
			WriteBufferSize:     8192,
		}),
		firedImpressions: newImpressionDeduper(),
		done:             make(chan struct{}),
	}
	// Persist stats to disk every 30 seconds; stops on Shutdown.
	if statsFile != "" {
		safeGo(func() {
			t := time.NewTicker(30 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					vs.save()
					now := time.Now()
					h.pendingBURLs.Range(func(key, val any) bool {
						if pb, ok := val.(pendingBURL); ok && now.After(pb.ExpiresAt) {
							h.pendingBURLs.Delete(key)
						}
						return true
					})
					h.firedImpressions.CleanupBefore(now.Add(-10 * time.Minute).Unix())
					h.firedNURLs.Range(func(key, val any) bool {
						if t, ok := val.(time.Time); ok && now.Sub(t) > 10*time.Minute {
							h.firedNURLs.Delete(key)
						}
						return true
					})
				case <-h.done:
					vs.save()
					return
				}
			}
		})
	}
	return h
}

// Shutdown stops background goroutines and flushes pending data to disk.
// Safe to call multiple times.
func (h *VideoPipelineHandler) Shutdown() {
	select {
	case <-h.done:
		// already closed
	default:
		close(h.done)
	}
	if h.clickHouseMetrics != nil {
		h.clickHouseMetrics.Close()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared parallel demand — fires all demand sources simultaneously and picks
// the highest bidder.
// ─────────────────────────────────────────────────────────────────────────────

// demandSource groups a demand adapter with the config it should use.
type demandSource struct {
	cfg     *AdServerConfig
	adapter DemandAdapter
	label   string
}

// demandResult carries the outcome of a single parallel demand call.
type demandResult struct {
	resp  *DemandResponse
	err   error
	label string
	cfg   *AdServerConfig
}

func winnerTelemetryConfig(primary *AdServerConfig, resp *DemandResponse) *AdServerConfig {
	if resp != nil && resp.WinnerConfig != nil {
		return resp.WinnerConfig
	}
	return primary
}

// runDemandWaterfall fires the primary demand adapter AND all ExtraDemand
// sources in parallel, then selects the response with the highest WinPrice.
// When only one source is configured the call is direct (no goroutine overhead).
func (h *VideoPipelineHandler) runDemandWaterfall(
	ctx context.Context,
	req *PlayerRequest,
	adsCfg *AdServerConfig,
	inbound InboundProtocol,
) (*DemandResponse, error) {
	if len(adsCfg.ExtraDemand) == 0 {
		primaryAdapter := h.adapterRouter(RouterKey{inbound, resolveDemandType(adsCfg)})
		resp, err := primaryAdapter.Execute(ctx, req, adsCfg)
		if resp != nil && resp.WinnerConfig == nil {
			resp.WinnerConfig = adsCfg
		}
		if err != nil {
			log.Printf("runDemandWaterfall: primary adapter error for placement %s: %v",
				req.PlacementID, err)
		}
		return resp, err
	}

	sources := h.buildDemandSources(adsCfg, inbound)

	// Fire all demand sources in parallel.
	ch := make(chan demandResult, len(sources))
	for _, src := range sources {
		s := src
		safeGo(func() {
			resp, err := s.adapter.Execute(ctx, req, s.cfg)
			ch <- demandResult{resp: resp, err: err, label: s.label, cfg: s.cfg}
		})
	}

	// Collect all responses and pick the best bid.
	var best *DemandResponse
	var bestLabel string
	var lastErr error
	var noFillCount int
	for range sources {
		r := <-ch
		if r.err != nil {
			log.Printf("runDemandWaterfall: %s adapter error for placement %s: %v",
				r.label, req.PlacementID, r.err)
			lastErr = r.err
			continue
		}
		if r.resp == nil || r.resp.NoFill {
			noFillCount++
			continue
		}
		if r.resp.WinnerConfig == nil {
			r.resp.WinnerConfig = r.cfg
		}
		// Highest WinPrice wins.
		if best == nil || r.resp.WinPrice > best.WinPrice {
			best = r.resp
			bestLabel = r.label
		}
	}

	if best != nil {
		logSampled(100, "runDemandWaterfall: parallel winner=%s price=%.4f for placement %s (%d sources, %d no-fill)",
			bestLabel, best.WinPrice, req.PlacementID, len(sources), noFillCount)
		return best, nil
	}

	// All sources returned no-fill or errors. Distinguish between:
	// - All no-fill (valid demand, just nothing available) → return error so
	//   endpoint serves empty VAST/204 gracefully.
	// - At least one hard error → propagate the error for logging.
	log.Printf("runDemandWaterfall: no fill for placement %s — %d sources, %d no-fill, %d errors",
		req.PlacementID, len(sources), noFillCount, len(sources)-noFillCount)
	if lastErr == nil {
		lastErr = fmt.Errorf("no fill from %d demand sources (%d responded with no-fill)", len(sources), noFillCount)
	}
	return nil, lastErr
}

// buildDemandSources assembles the list of demand sources to fire in parallel.
// The primary demand endpoint is always included; ExtraDemand entries are added
// as independent competing sources (each gets its own config copy).
func (h *VideoPipelineHandler) buildDemandSources(
	adsCfg *AdServerConfig,
	inbound InboundProtocol,
) []demandSource {
	sources := make([]demandSource, 0, 1+len(adsCfg.ExtraDemand))

	// Primary demand.
	primaryAdapter := h.adapterRouter(RouterKey{inbound, resolveDemandType(adsCfg)})
	sources = append(sources, demandSource{
		cfg: adsCfg, adapter: primaryAdapter, label: "primary",
	})

	// ExtraDemand — each entry becomes a separate parallel source.
	for i, extra := range adsCfg.ExtraDemand {
		extraCfg := *adsCfg
		extraCfg.DemandVASTURL = extra.VASTTagURL
		extraCfg.DemandOrtbURL = extra.OrtbURL
		if extra.FloorCPM > 0 {
			extraCfg.DemandFloorCPM = extra.FloorCPM
		}
		if extra.CampaignID != "" {
			extraCfg.CampaignID = extra.CampaignID
		}
		if extra.AdvertiserID != "" {
			extraCfg.AdvertiserID = extra.AdvertiserID
		}
		if len(extra.BCat) > 0 {
			extraCfg.BCat = extra.BCat
		}
		if len(extra.BAdv) > 0 {
			extraCfg.BAdv = extra.BAdv
		}
		extraCfg.ExtraDemand = nil // prevent recursion
		cfgCopy := extraCfg        // heap-allocate for goroutine safety
		extraAdapter := h.adapterRouter(RouterKey{inbound, resolveDemandType(&cfgCopy)})
		sources = append(sources, demandSource{
			cfg: &cfgCopy, adapter: extraAdapter, label: "extra-" + strconv.Itoa(i),
		})
	}
	return sources
}

// resolveEndpointTimeout calculates the per-request HTTP context timeout.
// TimeoutMS / TMax represent how long the demand partner has to *decide*
// (bid-level). The HTTP round-trip (DNS + TLS + connect + response transfer)
// requires additional headroom on top of that, so we add a 2 s buffer and
// enforce a 3 s minimum to prevent premature context cancellation.
func resolveEndpointTimeout(adsCfg *AdServerConfig, pr *PlayerRequest) time.Duration {
	var tmax time.Duration
	if adsCfg != nil && adsCfg.TimeoutMS > 0 {
		tmax = time.Duration(adsCfg.TimeoutMS) * time.Millisecond
	} else if pr != nil && pr.TMax > 0 {
		tmax = time.Duration(pr.TMax) * time.Millisecond
	} else {
		return 5 * time.Second // default unchanged
	}
	total := tmax + 2*time.Second
	if total < 3*time.Second {
		total = 3 * time.Second
	}
	return total
}

func withResolvedTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if timeout <= 0 {
		return parent, func() {}
	}
	if deadline, ok := parent.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 && remaining <= timeout {
			return parent, func() {}
		}
	}
	return context.WithTimeout(parent, timeout)
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 1 — Player/Publisher Request handler
// ─────────────────────────────────────────────────────────────────────────────

// VASTEndpoint is the HTTP entry-point (GET or POST).
// The player calls this URL with at minimum ?placement_id=<id>&app_bundle=<bundle>
// (or a JSON body for POST requests).
//
// Route: GET  /video/vast
//
//	POST /video/vast
func (h *VideoPipelineHandler) VASTEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		start := time.Now()

		// ── Stage 1: parse player/publisher request ──────────────────────
		req, err := h.parsePlayerRequest(r)
		if err != nil {
			h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusBadInput})
			log.Printf("VASTEndpoint parsePlayerRequest: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		// ── Stage 2: resolve ad server config ────────────────────────────
		adsCfg, err := h.resolveAdServerConfig(req.PlacementID)
		if err != nil {
			h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusBadInput})
			http.Error(w, "placement not found", http.StatusNotFound)
			return
		}
		if !adsCfg.Active {
			h.writeVASTResponse(w, emptyVAST())
			return
		}
		cfgCopy := *adsCfg
		cfgCopy.RequestBaseURL = requestBaseURL(r)
		adsCfg = &cfgCopy

		ctx, cancel := withResolvedTimeout(r.Context(), resolveEndpointTimeout(adsCfg, req))
		defer cancel()

		// ── Stages 3-6: demand routing via waterfall ─────────────────────
		h.videoStats.incRequestBatch(adsCfg.PublisherID, adsCfg.AdvertiserID)
		h.recordRequestMetric(req, adsCfg)
		resp, err := h.runDemandWaterfall(ctx, req, adsCfg, InboundVAST)
		if err != nil {
			log.Printf("VASTEndpoint: no fill for placement %s after all demand sources", req.PlacementID)
			h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, PubID: adsCfg.PublisherID, RequestStatus: metrics.RequestStatusOK})
			h.metricsEng.RecordRequestTime(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusOK}, time.Since(start))
			h.writeVASTResponse(w, emptyVAST())
			return
		}

		if !resp.NoFill {
			winnerCfg := winnerTelemetryConfig(adsCfg, resp)
			h.videoStats.incFillBatch(winnerCfg.PublisherID, winnerCfg.AdvertiserID)
			h.videoStats.incDimFill(winnerCfg, req, resp, resolveDemandType(winnerCfg))
			h.recordOpportunityMetric(req, winnerCfg, resp)
			h.recordBidReport(winnerCfg, req, resp, "win")
		}
		h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, PubID: adsCfg.PublisherID, RequestStatus: metrics.RequestStatusOK})
		h.metricsEng.RecordRequestTime(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusOK}, time.Since(start))

		h.writeVASTResponse(w, resp.VASTXml)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// OpenRTB inbound endpoint
// ─────────────────────────────────────────────────────────────────────────────

// ORTBEndpoint is the HTTP entry-point for players / publishers that natively
// speak OpenRTB 2.5.  It mirrors VASTEndpoint but returns a JSON BidResponse
// instead of VAST XML.
//
// The demand type configured on the linked Campaign determines which adapter
// schema runs: ORTB→ORTB, ORTB→VAST, or the Prebid fallback.
//
// Route: GET  /video/ortb
//
//	POST /video/ortb
func (h *VideoPipelineHandler) ORTBEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		start := time.Now()

		// ── Stage 1: parse player/publisher request ──────────────────────
		req, err := h.parsePlayerRequest(r)
		if err != nil {
			h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusBadInput})
			log.Printf("ORTBEndpoint parsePlayerRequest: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		// ── Stage 2: resolve ad server config ────────────────────────────
		adsCfg, err := h.resolveAdServerConfig(req.PlacementID)
		if err != nil {
			h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusBadInput})
			http.Error(w, "placement not found", http.StatusNotFound)
			return
		}
		if !adsCfg.Active {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		cfgCopy := *adsCfg
		cfgCopy.RequestBaseURL = requestBaseURL(r)
		adsCfg = &cfgCopy

		ctx, cancel := withResolvedTimeout(r.Context(), resolveEndpointTimeout(adsCfg, req))
		defer cancel()

		// ── Stages 3-5: demand routing via waterfall ─────────────────────
		h.videoStats.incRequestBatch(adsCfg.PublisherID, adsCfg.AdvertiserID)
		h.recordRequestMetric(req, adsCfg)
		resp, err := h.runDemandWaterfall(ctx, req, adsCfg, InboundORTB)
		if err != nil {
			log.Printf("ORTBEndpoint: no fill for placement %s after all demand sources", req.PlacementID)
			h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, PubID: adsCfg.PublisherID, RequestStatus: metrics.RequestStatusOK})
			h.metricsEng.RecordRequestTime(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusOK}, time.Since(start))
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Resolve and fire NURL/BURL for the winning bid on server side so that
		// auction macros are applied and the client never double-fires win notices.
		if !resp.NoFill && resp.BidResp != nil {
			winnerCfg := winnerTelemetryConfig(adsCfg, resp)
			if win, bidder, werr := h.extractWinningBid(resp.BidResp, winnerCfg); werr == nil && win != nil {
				auctionID := resp.AuctionID
				if auctionID == "" && resp.BidResp != nil {
					auctionID = resp.BidResp.ID
				}
				resolvedNURL := resolveAuctionMacros(win.NURL, win, auctionID, bidder)
				resolvedBURL := resolveAuctionMacros(win.BURL, win, auctionID, bidder)

				hasAdM := win.AdM != ""

				// NURL: fire server-side ONLY when AdM is present.
				// When AdM is absent, NURL doubles as the creative URI
				// (OpenRTB 2.5 §4.2.3) — the downstream caller must
				// fetch it, so we must NOT consume it here.
				if resolvedNURL != "" && hasAdM {
					h.fireWinNotice(resolvedNURL)
				}

				// BURL: cache for server-side fire on impression.
				// The exchange fires BURL (OpenRTB 2.5 §7.2), not the
				// downstream caller, so it is blanked from the response.
				if resolvedBURL != "" {
					h.pendingBURLs.Store(impressionKey{AuctionID: auctionID, BidID: win.BidID}, pendingBURL{
						URL:       resolvedBURL,
						ExpiresAt: time.Now().Add(5 * time.Minute),
					})
				}

				for si := range resp.BidResp.SeatBid {
					sb := &resp.BidResp.SeatBid[si]
					for bi := range sb.Bid {
						b := &sb.Bid[bi]
						if isEmptyAdM(b.AdM) {
							b.AdM = ""
						}
						if b.ID == win.BidID && (bidder == "" || sb.Seat == bidder) {
							// Blank BURL — exchange fires it server-side;
							// exposing the resolved URL to downstream
							// would cause a double-fire.
							b.BURL = ""
							resp.BURL = resolvedBURL

							// Blank NURL only when AdM is present (already
							// fired server-side above).  When AdM is empty,
							// keep NURL so the downstream caller can use it
							// as the creative URI.
							if hasAdM {
								b.NURL = ""
							}
						}
					}
				}
			}
		}

		if !resp.NoFill {
			winnerCfg := winnerTelemetryConfig(adsCfg, resp)
			h.videoStats.incFillBatch(winnerCfg.PublisherID, winnerCfg.AdvertiserID)
			h.videoStats.incDimFill(winnerCfg, req, resp, resolveDemandType(winnerCfg))
			h.recordOpportunityMetric(req, winnerCfg, resp)
			h.recordBidReport(winnerCfg, req, resp, "win")
		}
		h.metricsEng.RecordRequest(metrics.Labels{RType: metrics.ReqTypeVideo, PubID: adsCfg.PublisherID, RequestStatus: metrics.RequestStatusOK})
		h.metricsEng.RecordRequestTime(metrics.Labels{RType: metrics.ReqTypeVideo, RequestStatus: metrics.RequestStatusOK}, time.Since(start))

		// ── Stage 6: return BidResponse to player ────────────────────────
		buf := h.bufPool.Get().(*bytes.Buffer)
		buf.Reset()
		if data, merr := json.Marshal(resp.BidResp); merr == nil {
			buf.Write(data)
		}
		hdr := w.Header()
		hdr.Set("Content-Type", "application/json")
		hdr.Set("Content-Length", strconv.Itoa(buf.Len()))
		w.WriteHeader(http.StatusOK)
		w.Write(buf.Bytes()) //nolint:errcheck
		h.bufPool.Put(buf)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 1 — parsePlayerRequest
// ─────────────────────────────────────────────────────────────────────────────

// PlayerRequest captures parameters arriving from the player / publisher.
type PlayerRequest struct {
	PlacementID string `json:"placement_id,omitempty"`
	PublisherID string `json:"publisher_id,omitempty"`
	AppBundle   string `json:"app_bundle,omitempty"`
	Domain      string `json:"domain,omitempty"`
	PageURL     string `json:"page_url,omitempty"`
	GDPR        string `json:"gdpr,omitempty"`
	Consent     string `json:"gdpr_consent,omitempty"`
	CCPA        string `json:"us_privacy,omitempty"`
	COPPA       int8   `json:"coppa,omitempty"`
	GPP         string `json:"gpp,omitempty"`
	GPPSID      string `json:"gpp_sid,omitempty"`
	Width       int64  `json:"w,omitempty"`
	Height      int64  `json:"h,omitempty"`
	// Optional overrides for duration
	MinDuration int `json:"min_duration,omitempty"`
	MaxDuration int `json:"max_duration,omitempty"`

	// Device signals — forwarded into the OpenRTB Device object and used for
	// macro substitution in demand VAST tag URLs.
	IP          string `json:"ip,omitempty"` // client IP (overrides auto-detect)
	IPv6        string `json:"ip6,omitempty"`
	UA          string `json:"ua,omitempty"` // user-agent (overrides header)
	AppName     string `json:"app_name,omitempty"`
	AppStoreURL string `json:"app_store_url,omitempty"`
	DeviceMake  string `json:"device_make,omitempty"`
	DeviceModel string `json:"device_model,omitempty"`
	// DeviceType is the IAB OpenRTB device-type integer:
	// 1=mobile/tablet, 2=PC, 3=CTV, 4=phone, 5=tablet, 6=connected device, 7=set-top box.
	DeviceType    int    `json:"device_type,omitempty"`
	DeviceOS      string `json:"os,omitempty"`
	OSVersion     string `json:"osv,omitempty"`      // device OS version (e.g. "11")
	IFA           string `json:"ifa,omitempty"`      // IDFA (iOS) or GAID (Android)
	IFAType       string `json:"ifa_type,omitempty"` // "aaid", "idfa", "ppid", etc.
	LMT           int8   `json:"lmt,omitempty"`      // Limit Ad Tracking: 0=unrestricted, 1=limited
	Language      string `json:"language,omitempty"` // device language ISO-639-1
	CountryCode   string `json:"country_code,omitempty"`
	DNT           int8   `json:"dnt,omitempty"`
	ContentGenre  string `json:"ct_genre,omitempty"`
	ContentLang   string `json:"ct_lang,omitempty"`
	ContentRating string `json:"ct_rating,omitempty"`     // content rating (e.g. "TV-PG")
	LiveStream    int8   `json:"ct_livestream,omitempty"` // 1=live, 0=on-demand
	ContentLen    int64  `json:"ct_len,omitempty"`        // content duration in seconds
	ContentTitle  string `json:"ct_title,omitempty"`      // episode/video title
	ContentSeries string `json:"ct_series,omitempty"`     // show/series name (CTV)
	ContentSeason string `json:"ct_season,omitempty"`     // season identifier (e.g. "2")
	ContentURL    string `json:"ct_url,omitempty"`        // canonical content URL
	ContentCat    string `json:"ct_cat,omitempty"`        // comma-sep IAB content categories
	ContentProdQ  int    `json:"ct_prodq,omitempty"`      // IAB production quality: 0=unknown,1=professionally produced,2=prosumer,3=UGC
	SiteName      string `json:"site_name,omitempty"`     // human-readable site/app name
	SiteCat       string `json:"site_cat,omitempty"`      // comma-sep IAB content categories for site/app
	SiteKeywords  string `json:"site_keywords,omitempty"` // comma-sep keywords for site/app
	PageRef       string `json:"page_ref,omitempty"`      // referring URL (retargeting signal)
	AppVer        string `json:"app_ver,omitempty"`       // app version string
	// App context
	AppID string `json:"app_id,omitempty"` // app.id for app-traffic requests
	// Video impression controls
	Skip       int8 `json:"skip,omitempty"`        // 0=non-skippable (default), 1=skippable
	StartDelay int  `json:"start_delay,omitempty"` // 0=pre-roll (default), -1=mid-roll, -2=post-roll
	Secure     int8 `json:"secure,omitempty"`      // 0=HTTP (default), 1=HTTPS
	// Request-level controls
	TMax int64  `json:"tmax,omitempty"` // auction timeout ms; default 500
	BCat string `json:"bcat,omitempty"` // comma-separated blocked IAB categories
	BAdv string `json:"badv,omitempty"` // comma-separated blocked advertiser domains
	// User identity
	UserID string `json:"user_id,omitempty"` // exchange user ID
}

// parsePlayerRequest decodes a PlayerRequest from URL query-params (GET) or
// a JSON body (POST).  Query params always take precedence.
func (h *VideoPipelineHandler) parsePlayerRequest(r *http.Request) (*PlayerRequest, error) {
	pr := &PlayerRequest{}

	// JSON body (POST)
	if r.Method == http.MethodPost && r.Body != nil {
		defer r.Body.Close()
		limited := &io.LimitedReader{R: r.Body, N: 65536}
		dec := json.NewDecoder(limited)
		if err := dec.Decode(pr); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("decode body: %w", err)
		}
	}

	// Query-param overrides
	q := r.URL.Query()

	if v := q.Get("placement_id"); v != "" {
		pr.PlacementID = v
	} else if v := q.Get("sid"); v != "" {
		// sid is accepted as a common alias for placement_id in VAST tag URLs
		pr.PlacementID = v
	}
	if v := q.Get("publisher_id"); v != "" {
		pr.PublisherID = v
	}
	if v := q.Get("app_bundle"); v != "" {
		pr.AppBundle = v
	}
	if v := q.Get("domain"); v != "" {
		pr.Domain = v
	}
	if v := q.Get("page_url"); v != "" {
		if dec, err := url.PathUnescape(v); err == nil {
			v = dec
		}
		pr.PageURL = v
	}
	if v := q.Get("gdpr"); v != "" {
		pr.GDPR = v
	}
	if v := q.Get("gdpr_consent"); v != "" {
		pr.Consent = v
	}
	if v := q.Get("us_privacy"); v != "" {
		pr.CCPA = v
	}
	if v := q.Get("gpp"); v != "" {
		pr.GPP = v
	}
	if v := q.Get("gpp_sid"); v != "" {
		pr.GPPSID = v
	}

	// Viewport dimensions
	if v := q.Get("w"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 64); e == nil {
			pr.Width = n
		}
	}
	if v := q.Get("h"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 64); e == nil {
			pr.Height = n
		}
	}

	// Duration overrides
	if v := q.Get("min_dur"); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			pr.MinDuration = n
		}
	}
	if v := q.Get("max_dur"); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			pr.MaxDuration = n
		}
	}

	// Device signals
	if v := q.Get("ip"); v != "" {
		pr.IP = v
	}
	if v := q.Get("ip6"); v != "" {
		pr.IPv6 = v
	}
	if v := q.Get("ua"); v != "" {
		pr.UA = v
	}
	if v := q.Get("app_name"); v != "" {
		pr.AppName = v
	}
	if v := q.Get("app_store_url"); v != "" {
		if dec, err := url.PathUnescape(v); err == nil {
			v = dec
		}
		pr.AppStoreURL = v
	}
	if v := q.Get("device_make"); v != "" {
		pr.DeviceMake = v
	}
	if v := q.Get("device_model"); v != "" {
		pr.DeviceModel = v
	}
	if v := q.Get("device_type"); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			pr.DeviceType = n
		}
	}
	if v := q.Get("os"); v != "" {
		pr.DeviceOS = v
	}
	if v := q.Get("ifa"); v != "" {
		pr.IFA = v
	}
	if v := q.Get("ifa_type"); v != "" {
		pr.IFAType = v
	}
	if v := q.Get("country_code"); v != "" {
		pr.CountryCode = v
	}
	if v := q.Get("dnt"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 8); e == nil {
			pr.DNT = int8(n)
		}
	}
	if v := q.Get("ct_genre"); v != "" {
		pr.ContentGenre = v
	}
	if v := q.Get("ct_lang"); v != "" {
		pr.ContentLang = v
	}
	if v := q.Get("ct_rating"); v != "" {
		pr.ContentRating = v
	}
	if v := q.Get("ct_livestream"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 8); e == nil {
			pr.LiveStream = int8(n)
		}
	}
	if v := q.Get("ct_len"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 64); e == nil {
			pr.ContentLen = n
		}
	}
	if v := q.Get("app_id"); v != "" {
		pr.AppID = v
	}
	if v := q.Get("osv"); v != "" {
		pr.OSVersion = v
	}
	if v := q.Get("lmt"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 8); e == nil {
			pr.LMT = int8(n)
		}
	}
	if v := q.Get("language"); v != "" {
		pr.Language = v
	}
	if v := q.Get("user_id"); v != "" {
		pr.UserID = v
	}
	if v := q.Get("ct_title"); v != "" {
		pr.ContentTitle = v
	}
	if v := q.Get("ct_series"); v != "" {
		pr.ContentSeries = v
	}
	if v := q.Get("ct_season"); v != "" {
		pr.ContentSeason = v
	}
	if v := q.Get("ct_url"); v != "" {
		pr.ContentURL = v
	}
	if v := q.Get("ct_cat"); v != "" {
		pr.ContentCat = v
	}
	if v := q.Get("ct_prodq"); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			pr.ContentProdQ = n
		}
	}
	if v := q.Get("site_name"); v != "" {
		pr.SiteName = v
	}
	if v := q.Get("site_cat"); v != "" {
		pr.SiteCat = v
	}
	if v := q.Get("site_keywords"); v != "" {
		pr.SiteKeywords = v
	}
	if v := q.Get("page_ref"); v != "" {
		pr.PageRef = v
	}
	if v := q.Get("app_ver"); v != "" {
		pr.AppVer = v
	}
	if v := q.Get("skip"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 8); e == nil {
			pr.Skip = int8(n)
		}
	}
	if v := q.Get("start_delay"); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			pr.StartDelay = n
		}
	}
	if v := q.Get("secure"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 8); e == nil {
			pr.Secure = int8(n)
		}
	} else if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		pr.Secure = 1
	}
	if v := q.Get("tmax"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 64); e == nil {
			pr.TMax = n
		}
	}

	// Auto-detect client IPs when not explicitly provided.
	if pr.IP == "" || pr.IPv6 == "" {
		if xfwd := r.Header.Get("X-Forwarded-For"); xfwd != "" {
			ip := strings.TrimSpace(strings.Split(xfwd, ",")[0])
			if pr.IP == "" {
				pr.IP = ip
			}
			if pr.IPv6 == "" && strings.Contains(ip, ":") {
				pr.IPv6 = ip
			}
		} else if rip, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			if pr.IP == "" {
				pr.IP = rip
			}
			if pr.IPv6 == "" && strings.Contains(rip, ":") {
				pr.IPv6 = rip
			}
		}
	}
	if pr.IP == "" && pr.IPv6 != "" {
		pr.IP = pr.IPv6
	}
	if v := q.Get("bcat"); v != "" {
		pr.BCat = v
	}
	if v := q.Get("badv"); v != "" {
		pr.BAdv = v
	}
	if pr.IP == "" {
		pr.IP = extractClientIP(r)
	}
	if pr.UA == "" {
		pr.UA = r.Header.Get("User-Agent")
	}

	if pr.PlacementID == "" {
		return nil, fmt.Errorf("placement_id is required")
	}

	return pr, nil
}

// extractClientIP returns the best-effort client IP from the request.
// It checks X-Forwarded-For, then X-Real-IP, then falls back to RemoteAddr.
func extractClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if idx := strings.Index(xff, ","); idx > 0 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 2 — resolveAdServerConfig
// ─────────────────────────────────────────────────────────────────────────────

// resolveAdServerConfig looks up the ad server configuration for a placement.
// Falls back to a sensible default so the pipeline works even without an
// explicit registration.
func (h *VideoPipelineHandler) resolveAdServerConfig(placementID string) (*AdServerConfig, error) {
	if cfg := h.configStore.get(placementID); cfg != nil {
		return cfg, nil
	}
	// Default permissive config — allows all bidders to compete.
	return &AdServerConfig{
		PlacementID: placementID,
		Active:      true,
		MinDuration: 5,
		MaxDuration: 30,
		Protocols:   []int{2, 3, 5, 6, 7, 8}, // VAST 2.0, 3.0, 2.0W, 3.0W, 4.0, 4.0W
		APIs:        []int{1, 2, 7},          // VPAID 1.0, VPAID 2.0, OMID 1.0
	}, nil
}

// RegisterAdServerConfig allows external callers (e.g. the dashboard CRUD) to
// install or update placement-level ad server configuration at runtime.
func (h *VideoPipelineHandler) RegisterAdServerConfig(cfg *AdServerConfig) {
	h.configStore.set(cfg)
}

// LookupAdServerConfig returns the runtime config for a placement when one is
// currently registered. It does not synthesize the permissive default used by
// serving, because dashboard recovery only wants explicitly managed placements.
func (h *VideoPipelineHandler) LookupAdServerConfig(placementID string) *AdServerConfig {
	if cfg := h.configStore.get(placementID); cfg != nil {
		cp := *cfg
		if len(cfg.Protocols) > 0 {
			cp.Protocols = append([]int(nil), cfg.Protocols...)
		}
		if len(cfg.APIs) > 0 {
			cp.APIs = append([]int(nil), cfg.APIs...)
		}
		if len(cfg.AllowedBidders) > 0 {
			cp.AllowedBidders = append([]string(nil), cfg.AllowedBidders...)
		}
		if len(cfg.BAdv) > 0 {
			cp.BAdv = append([]string(nil), cfg.BAdv...)
		}
		if len(cfg.BCat) > 0 {
			cp.BCat = append([]string(nil), cfg.BCat...)
		}
		if len(cfg.MimeTypes) > 0 {
			cp.MimeTypes = append([]string(nil), cfg.MimeTypes...)
		}
		if len(cfg.CompanionType) > 0 {
			cp.CompanionType = append([]int(nil), cfg.CompanionType...)
		}
		if len(cfg.ExtraDemand) > 0 {
			cp.ExtraDemand = append([]ExtraDemandCfg(nil), cfg.ExtraDemand...)
		}
		if len(cfg.SeatWeights) > 0 {
			cp.SeatWeights = make(map[string]float64, len(cfg.SeatWeights))
			for key, value := range cfg.SeatWeights {
				cp.SeatWeights[key] = value
			}
		}
		if len(cfg.TargetingExt) > 0 {
			cp.TargetingExt = make(map[string]interface{}, len(cfg.TargetingExt))
			for key, value := range cfg.TargetingExt {
				cp.TargetingExt[key] = value
			}
		}
		return &cp
	}
	return nil
}

// UnregisterAdServerConfig removes a placement from the pipeline config store
// so deleted ad units stop serving immediately (no zombie placements).
func (h *VideoPipelineHandler) UnregisterAdServerConfig(placementID string) {
	h.configStore.remove(placementID)
}

// detectIFAType infers the IAB ifa_type string from OS, make, model, and UA.
// Priority: iOS/tvOS → Roku → Samsung (Tizen) → Amazon FireTV → LG →
//
//	Vizio → Xiaomi (Mi Box/TV) → Sony Bravia → Android → dpid.
func detectIFAType(osStr, make, model, ua string) string {
	osL := strings.ToLower(osStr)
	makeL := strings.ToLower(make)
	modelL := strings.ToLower(model)
	uaL := strings.ToLower(ua)

	// ── iOS / tvOS (Apple IDFA) ───────────────────────────────────────────
	if osL == "ios" || osL == "tvos" {
		return "idfa"
	}

	// ── Roku (RIDA — Roku ID for Advertising) ────────────────────────────
	if strings.Contains(makeL, "roku") ||
		strings.Contains(modelL, "roku") ||
		strings.Contains(uaL, "roku") {
		return "rida"
	}

	// ── Samsung Smart TV — Tizen (TIFA) ──────────────────────────────────
	if strings.Contains(makeL, "samsung") ||
		strings.Contains(osL, "tizen") ||
		strings.Contains(uaL, "tizen") {
		return "tifa"
	}

	// ── Amazon Fire TV (AFAI) ─────────────────────────────────────────────
	if strings.Contains(makeL, "amazon") ||
		strings.Contains(modelL, "firetv") ||
		strings.Contains(modelL, "fire tv") ||
		strings.Contains(modelL, "aftm") || strings.Contains(modelL, "aftb") ||
		strings.Contains(modelL, "aftt") || strings.Contains(modelL, "afts") ||
		strings.Contains(uaL, "silk/") || strings.Contains(uaL, "fire tv") {
		return "afai"
	}

	// ── LG Smart TV — webOS (LGUDID) ──────────────────────────────────────
	if strings.Contains(makeL, "lg") ||
		strings.Contains(osL, "webos") ||
		strings.Contains(uaL, "webos") || strings.Contains(uaL, "netcast") {
		return "lgudid"
	}

	// ── Vizio SmartCast (VIDA) ─────────────────────────────────────────────
	if strings.Contains(makeL, "vizio") ||
		strings.Contains(uaL, "vizio") {
		return "vida"
	}

	// ── Xiaomi Mi Box / Mi TV (AAID — runs AOSP Android) ─────────────────
	if strings.Contains(makeL, "xiaomi") ||
		strings.Contains(modelL, "mibox") || strings.Contains(modelL, "mi box") ||
		strings.Contains(modelL, "mitv") || strings.Contains(modelL, "mi tv") {
		return "aaid"
	}

	// ── Sony Bravia (AAID — runs Android TV) ─────────────────────────────
	if strings.Contains(makeL, "sony") ||
		strings.Contains(modelL, "bravia") ||
		strings.Contains(uaL, "bravia") {
		return "aaid"
	}

	// ── Generic Android (AAID) ────────────────────────────────────────────
	if strings.Contains(osL, "android") {
		return "aaid"
	}

	// ── Unknown / fallback ────────────────────────────────────────────────
	return "dpid"
}

// buildSUA constructs an openrtb2.UserAgent (Structured User Agent) from the
// raw UA string, device OS, and device make. Demand partners use SUA for
// deterministic device classification without fragile UA string parsing.
func buildSUA(ua, osStr, make string) *openrtb2.UserAgent {
	if ua == "" {
		return nil
	}

	sua := &openrtb2.UserAgent{
		Source: adcom1.UASourceParsed,
	}

	// ── Platform from OS string ──────────────────────────────────────────
	if osStr != "" {
		sua.Platform = &openrtb2.BrandVersion{Brand: osStr}
	}

	// ── Mobile flag: CTV / desktop = 0, mobile/tablet = 1 ───────────────
	mobile := int8(0)
	uaL := strings.ToLower(ua)
	if strings.Contains(uaL, "mobile") || strings.Contains(uaL, "android") && !strings.Contains(uaL, "tv") {
		mobile = 1
	}
	sua.Mobile = &mobile

	// ── Extract browser brand + version from UA string ───────────────────
	// Matches patterns like "Chrome/108.0.5359.124", "Safari/605.1.15",
	// "CrKey/1.56", "SmartCast/2.0", etc.
	var browsers []openrtb2.BrandVersion

	// Known browser/runtime tokens to look for in the UA, ordered by
	// specificity (CTV runtimes first, then mainstream browsers).
	tokens := []string{
		"CrKey", "Chromecast", "SmartCast", "Roku", "Silk", "BRAVIA",
		"Web0S", "Tizen", "Edge", "OPR", "Chrome", "Firefox", "Safari",
	}
	for _, tok := range tokens {
		idx := strings.Index(ua, tok+"/")
		if idx < 0 {
			continue
		}
		verStart := idx + len(tok) + 1 // skip "Token/"
		verEnd := verStart
		for verEnd < len(ua) && ua[verEnd] != ' ' && ua[verEnd] != ')' {
			verEnd++
		}
		verStr := ua[verStart:verEnd]
		// Take only the major version number.
		if dot := strings.IndexByte(verStr, '.'); dot > 0 {
			verStr = verStr[:dot]
		}
		browsers = append(browsers, openrtb2.BrandVersion{
			Brand:   tok,
			Version: []string{verStr},
		})
		break // Use the first (most specific) match.
	}

	// Fallback: use device make as brand when no browser token matched.
	if len(browsers) == 0 && make != "" {
		browsers = append(browsers, openrtb2.BrandVersion{Brand: make, Version: []string{"1"}})
	}

	if len(browsers) > 0 {
		sua.Browsers = browsers
	}

	return sua
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 3 — forwardToExchange
// ─────────────────────────────────────────────────────────────────────────────

// forwardToExchange builds an OpenRTB bid request from the player request and
// ad server config, then submits it to the exchange for competitive bidding.
func (h *VideoPipelineHandler) forwardToExchange(
	ctx context.Context,
	pr *PlayerRequest,
	adsCfg *AdServerConfig,
) (*openrtb2.BidResponse, error) {
	// ── Build OpenRTB Bid Request ─────────────────────────────────────────
	bidReq := h.buildOpenRTBRequest(pr, adsCfg)

	// ── Wrap in RequestWrapper ────────────────────────────────────────────
	reqWrapper := &openrtb_ext.RequestWrapper{BidRequest: bidReq}

	// ── Resolve account (best-effort) ────────────────────────────────────
	account := config.Account{ID: adsCfg.PublisherID}

	// ── Build AuctionRequest ──────────────────────────────────────────────
	impExtInfoMap := make(map[string]exchange.ImpExtInfo, len(bidReq.Imp))
	for _, imp := range bidReq.Imp {
		impExtInfoMap[imp.ID] = exchange.ImpExtInfo{}
	}
	auctionReq := &exchange.AuctionRequest{
		BidRequestWrapper: reqWrapper,
		Account:           account,
		StartTime:         time.Now(),
		RequestType:       metrics.ReqTypeVideo,
		ImpExtInfoMap:     impExtInfoMap,
		HookExecutor:      &hookexecution.EmptyHookExecutor{},
	}

	// ── Run auction ───────────────────────────────────────────────────────
	auctionResp, err := h.exchange.HoldAuction(ctx, auctionReq, &exchange.DebugLog{})
	if err != nil {
		return nil, fmt.Errorf("HoldAuction: %w", err)
	}

	return auctionResp.BidResponse, nil
}

// buildOpenRTBRequest constructs a fully-populated OpenRTB 2.5 bid request.
// All fields are sourced from the incoming PlayerRequest; sensible defaults
// are applied when the publisher does not supply a value.
func (h *VideoPipelineHandler) buildOpenRTBRequest(
	pr *PlayerRequest,
	adsCfg *AdServerConfig,
) *openrtb2.BidRequest {
	auctionID := fastGenerateID()
	impID := fastGenerateID()

	// ── Duration (publisher override > ad-server config > hard default) ───
	minDur := adsCfg.MinDuration
	if minDur == 0 {
		minDur = 5
	}
	if pr.MinDuration > 0 {
		minDur = pr.MinDuration
	}
	maxDur := adsCfg.MaxDuration
	if maxDur == 0 {
		maxDur = 30
	}
	if pr.MaxDuration > 0 {
		maxDur = pr.MaxDuration
	}

	// ── Protocols — demand capability from campaign, else runtime defaults ──
	var protocols []adcom1.MediaCreativeSubtype
	if len(adsCfg.Protocols) > 0 {
		protocols = make([]adcom1.MediaCreativeSubtype, len(adsCfg.Protocols))
		for i, p := range adsCfg.Protocols {
			protocols[i] = adcom1.MediaCreativeSubtype(p)
		}
	} else {
		// Fallback: VAST 2.0, 3.0, 2.0 Wrapper, 3.0 Wrapper, 4.0, 4.0 Wrapper.
		protocols = []adcom1.MediaCreativeSubtype{2, 3, 5, 6, 7, 8}
	}

	// ── API frameworks (VPAID, MRAID, etc.) ──────────────────────────────
	var apis []adcom1.APIFramework
	if len(adsCfg.APIs) > 0 {
		apis = make([]adcom1.APIFramework, len(adsCfg.APIs))
		for i, a := range adsCfg.APIs {
			apis[i] = adcom1.APIFramework(a)
		}
	}

	// ── Video player dimensions (publisher > default 1920×1080) ──────────
	vidW := int64(1920)
	if pr.Width > 0 {
		vidW = pr.Width
	}
	vidH := int64(1080)
	if pr.Height > 0 {
		vidH = pr.Height
	}

	// ── Video control flags ───────────────────────────────────────────────
	skipVal := pr.Skip                             // 0=non-skippable (default)
	startDelay := adcom1.StartDelay(pr.StartDelay) // 0=pre-roll (default)
	boxingVal := int8(1)
	seqVal := int8(1)

	// ── Imp.Secure: from publisher; auto-detected in parsePlayerRequest ───
	secureVal := pr.Secure
	if secureVal == 0 {
		secureVal = 1 // default to HTTPS if publisher omitted
	}

	bidFloor := adsCfg.outboundBidFloorCPM()
	video := &openrtb2.Video{
		MIMEs:         resolveMimeTypes(adsCfg.MimeTypes),
		Linearity:     adcom1.LinearityLinear,
		MinDuration:   int64(minDur),
		MaxDuration:   int64(maxDur),
		Protocols:     protocols,
		API:           apis,
		W:             &vidW,
		H:             &vidH,
		StartDelay:    startDelay.Ptr(),
		Skip:          &skipVal,
		Sequence:      seqVal,
		BoxingAllowed: &boxingVal,
		Placement:     videoPlacementSubtype(adsCfg.VideoPlacementType),
		Plcmt:         videoPlcmt(adsCfg.VideoPlacementType),
		Pos:           videoAdPosition(adsCfg.VideoPlacementType),
	}

	// CTV pod / ad break controls (ORTB 2.6).
	if adsCfg.PodDuration > 0 {
		podDur := int64(adsCfg.PodDuration)
		video.PodDur = podDur
	}
	if adsCfg.MaxSeq > 0 {
		video.MaxSeq = int64(adsCfg.MaxSeq)
	}
	if adsCfg.PodSequence > 0 {
		video.PodSeq = adcom1.PodSequence(adsCfg.PodSequence)
	}
	if len(adsCfg.CompanionType) > 0 {
		ct := make([]adcom1.CompanionType, len(adsCfg.CompanionType))
		for i, c := range adsCfg.CompanionType {
			ct[i] = adcom1.CompanionType(c)
		}
		video.CompanionType = ct
	}

	imp := openrtb2.Imp{
		ID:                impID,
		DisplayManager:    "GoAds",
		DisplayManagerVer: "1.0",
		BidFloor:          bidFloor,
		BidFloorCur:       "USD",
		Secure:            &secureVal,
		Video:             video,
	}

	bidReq := &openrtb2.BidRequest{
		ID:      auctionID,
		Imp:     []openrtb2.Imp{imp},
		AT:      1,
		Cur:     []string{"USD"},
		AllImps: 0,
		Ext:     json.RawMessage(`{}`),
	}
	if adsCfg.CatTax > 0 {
		bidReq.CatTax = adcom1.CategoryTaxonomy(adsCfg.CatTax)
	}

	// ── Request-level targeting ext ──────────────────────────────────────
	if len(adsCfg.TargetingExt) > 0 {
		if raw, err := json.Marshal(adsCfg.TargetingExt); err == nil {
			bidReq.Ext = raw
		}
	}

	// ── Request-level auction parameters ─────────────────────────────────
	var tmax int64 = 500 // fallback
	if adsCfg.TimeoutMS > 0 {
		tmax = int64(adsCfg.TimeoutMS)
	}
	if pr.TMax > 0 {
		tmax = pr.TMax // player-level override takes precedence
	}

	bcatSeen := make(map[string]struct{})
	var bcat []string
	for _, c := range splitCSVTrim(pr.BCat) {
		if c != "" {
			if _, dup := bcatSeen[c]; !dup {
				bcatSeen[c] = struct{}{}
				bcat = append(bcat, c)
			}
		}
	}
	// Merge campaign-level blocked categories (deduplicated).
	for _, c := range adsCfg.BCat {
		if c != "" {
			if _, dup := bcatSeen[c]; !dup {
				bcatSeen[c] = struct{}{}
				bcat = append(bcat, c)
			}
		}
	}
	badvSeen := make(map[string]struct{})
	var badv []string
	for _, d := range splitCSVTrim(pr.BAdv) {
		if d != "" {
			if _, dup := badvSeen[d]; !dup {
				badvSeen[d] = struct{}{}
				badv = append(badv, d)
			}
		}
	}
	// Merge campaign-level blocked advertisers (deduplicated).
	for _, d := range adsCfg.BAdv {
		if d != "" {
			if _, dup := badvSeen[d]; !dup {
				badvSeen[d] = struct{}{}
				badv = append(badv, d)
			}
		}
	}

	bidReq.TMax = tmax
	bidReq.BCat = bcat
	bidReq.BAdv = badv

	// ── App or Site context ───────────────────────────────────────────────
	// Use adsCfg.ContentURL as fallback when the player didn't supply one.
	contentURL := pr.ContentURL
	if contentURL == "" {
		contentURL = adsCfg.ContentURL
	}
	buildContent := func() *openrtb2.Content {
		if pr.ContentGenre == "" && pr.ContentLang == "" &&
			pr.ContentRating == "" && pr.ContentLen == 0 &&
			pr.ContentTitle == "" && pr.ContentSeries == "" &&
			contentURL == "" && pr.ContentCat == "" {
			return nil
		}
		lsVal := pr.LiveStream
		c := &openrtb2.Content{
			Genre:         pr.ContentGenre,
			Language:      pr.ContentLang,
			ContentRating: pr.ContentRating,
			LiveStream:    &lsVal,
			Title:         pr.ContentTitle,
			Series:        pr.ContentSeries,
			Season:        pr.ContentSeason,
			URL:           contentURL,
			ProdQ:         prodQPtr(pr.ContentProdQ),
		}
		if pr.ContentLen > 0 {
			c.Len = pr.ContentLen
		}
		for _, cat := range splitCSVTrim(pr.ContentCat) {
			if cat != "" {
				c.Cat = append(c.Cat, cat)
			}
		}
		if adsCfg.CatTax > 0 {
			c.CatTax = adcom1.CategoryTaxonomy(adsCfg.CatTax)
		}
		return c
	}

	// parseSiteCat converts a comma-separated IAB category string to a []string.
	parseCatList := func(csv string) []string { return splitCSVTrim(csv) }

	// Publisher identity — shared across site and app contexts.
	pub := &openrtb2.Publisher{
		ID:     adsCfg.PublisherID,
		Domain: adsCfg.DomainOrApp,
	}
	// Populate publisher IAB categories from site_cat when present.
	if pr.SiteCat != "" {
		pub.Cat = parseCatList(pr.SiteCat)
	}

	if pr.AppBundle != "" {
		appID := pr.AppID
		if appID == "" {
			appID = adsCfg.PublisherID
		}
		app := &openrtb2.App{
			ID:        appID,
			Bundle:    pr.AppBundle,
			Name:      pr.AppName,
			StoreURL:  pr.AppStoreURL,
			Ver:       pr.AppVer,
			Publisher: pub,
			Content:   buildContent(),
		}
		if pr.SiteName != "" {
			app.Name = pr.SiteName
		}
		if pr.SiteCat != "" {
			app.Cat = parseCatList(pr.SiteCat)
		}
		if pr.SiteKeywords != "" {
			app.Keywords = pr.SiteKeywords
		}
		bidReq.App = app
	} else if adsCfg.DomainOrApp != "" && !strings.Contains(adsCfg.DomainOrApp, " ") && !strings.HasPrefix(adsCfg.DomainOrApp, "http") {
		// adsCfg.DomainOrApp is a bundle ID (e.g. com.samsung.tv, 12345 for Roku).
		// Use app context so demand partners can identify CTV inventory.
		appID := pr.AppID
		if appID == "" {
			appID = adsCfg.PublisherID
		}
		app := &openrtb2.App{
			ID:        appID,
			Bundle:    adsCfg.DomainOrApp,
			Name:      pr.SiteName,
			Ver:       pr.AppVer,
			Publisher: pub,
			Content:   buildContent(),
		}
		if pr.SiteCat != "" {
			app.Cat = parseCatList(pr.SiteCat)
		}
		if pr.SiteKeywords != "" {
			app.Keywords = pr.SiteKeywords
		}
		bidReq.App = app
	} else {
		domain := pr.Domain
		if domain == "" {
			domain = adsCfg.DomainOrApp
		}
		site := &openrtb2.Site{
			Page:      pr.PageURL,
			Ref:       pr.PageRef,
			Domain:    domain,
			Name:      pr.SiteName,
			Publisher: pub,
			Content:   buildContent(),
		}
		if pr.SiteCat != "" {
			site.Cat = parseCatList(pr.SiteCat)
		}
		if pr.SiteKeywords != "" {
			site.Keywords = pr.SiteKeywords
		}
		bidReq.Site = site
	}

	// ── Device (always included for video/CTV) ────────────────────────────
	{
		dntVal := int8(pr.DNT)
		lmtVal := pr.LMT
		devType := pr.DeviceType
		if devType == 0 && bidReq.App != nil {
			devType = inferCTVDeviceType(pr.UA, pr.DeviceOS, pr.DeviceMake)
		}
		dev := &openrtb2.Device{
			UA:         pr.UA,
			Make:       pr.DeviceMake,
			Model:      pr.DeviceModel,
			OS:         pr.DeviceOS,
			OSV:        pr.OSVersion,
			IFA:        pr.IFA,
			DeviceType: adcom1.DeviceType(devType),
			Language:   pr.Language,
			DNT:        &dntVal,
			Lmt:        &lmtVal,
		}
		isCTV := bidReq.App != nil || isCTVDeviceType(devType)
		if isCTV || pr.IP != "" {
			dev.IP = pr.IP
		}
		if isCTV || pr.IPv6 != "" {
			dev.IPv6 = pr.IPv6
		}
		if pr.CountryCode != "" {
			dev.Geo = &openrtb2.Geo{Country: iso2ToISO3(pr.CountryCode)}
		}
		ifaType := pr.IFAType
		if ifaType == "" && pr.IFA != "" {
			ifaType = detectIFAType(pr.DeviceOS, pr.DeviceMake, pr.DeviceModel, pr.UA)
		}
		if ifaType != "" {
			if extRaw, err := json.Marshal(map[string]string{"ifa_type": ifaType}); err == nil {
				dev.Ext = extRaw
			}
		}
		// ── SUA (Structured User Agent) ──────────────────────────────────
		// Build from UA parsing when available. Demand partners use SUA for
		// accurate device classification instead of manually parsing UA.
		dev.SUA = buildSUA(pr.UA, pr.DeviceOS, pr.DeviceMake)
		bidReq.Device = dev
	}

	// ── Regs (always included, default no restrictions) ───────────────────
	regs := &openrtb2.Regs{COPPA: pr.COPPA}
	if pr.CCPA != "" {
		regs.USPrivacy = pr.CCPA
	}
	if pr.GDPR != "" {
		if gdprVal, err := strconv.ParseInt(pr.GDPR, 10, 8); err == nil {
			v := int8(gdprVal)
			regs.GDPR = &v
		}
	}
	if pr.GPP != "" {
		regs.GPP = pr.GPP
	}
	if pr.GPPSID != "" {
		var gppSIDs []int8
		for _, s := range splitCSVTrim(pr.GPPSID) {
			if v, err := strconv.ParseInt(s, 10, 8); err == nil {
				gppSIDs = append(gppSIDs, int8(v))
			}
		}
		regs.GPPSID = gppSIDs
	}
	// Default GPPSID to [0] when not supplied — signals no active GPP section.
	if len(regs.GPPSID) == 0 {
		regs.GPPSID = []int8{0}
	}
	bidReq.Regs = regs

	// ── User (always included — demand partners expect it) ───────────────
	user := &openrtb2.User{
		ID:  pr.UserID,
		Ext: json.RawMessage(`{}`),
	}
	if pr.Consent != "" {
		extUser := openrtb_ext.ExtUser{Consent: pr.Consent}
		if raw, err := jsonutil.Marshal(extUser); err == nil {
			user.Ext = raw
		}
	}
	bidReq.User = user

	// ── Source (transaction ID for bid dedup + supply chain) ──────────────
	source := &openrtb2.Source{
		TID: auctionID,
	}
	// Supply Chain (schain) — identifies this exchange as the first node in
	// the reseller chain per IAB SupplyChain Object spec.
	sellerDomain := adsCfg.SellerDomain
	if sellerDomain == "" {
		sellerDomain = "goads.io"
	}
	schain := openrtb2.SupplyChain{
		Complete: 1,
		Ver:      "1.0",
		Nodes: []openrtb2.SupplyChainNode{{
			ASI: sellerDomain,
			SID: adsCfg.PublisherID,
			HP:  openrtb2.Int8Ptr(1),
			RID: auctionID,
		}},
	}
	if raw, err := json.Marshal(map[string]interface{}{"schain": schain}); err == nil {
		source.Ext = raw
	}
	bidReq.Source = source

	return bidReq
}

// ─────────────────────────────────────────────────────────────────────────────
// VAST wrapper helper  (used by demand adapters and buildVASTResponse)
// ─────────────────────────────────────────────────────────────────────────────

// substituteMacros replaces VAST tag macro placeholders in demandURL with actual
// values from the incoming player request and ad server config.
//
// Supported macros:
//
//	{cb}             — random cache-buster integer
//	{uip}            — client IP address
//	{ua}             — URL-encoded User-Agent string
//	{app_bundle}     — app bundle identifier
//	{app_name}       — URL-encoded application name
//	{app_store_url}  — URL-encoded app store URL
//	{device_make}    — device manufacturer
//	{device_model}   — device model
//	{device_type}    — IAB OpenRTB device-type integer
//	{idfa} / {ifa}   — IDFA (iOS) or GAID (Android)
//	{device_os}/{os} — device operating system
//	{country_code}   — ISO-3166-1 alpha-3 country code (as sent to SSPs in geo.country)
//	{dnt}            — do-not-track flag (0 or 1)
//	{us_privacy}     — CCPA us_privacy string
//	{gdpr}           — GDPR flag (0 or 1)
//	{gdpr_consent}   — GDPR TCF consent string
//	{w}              — creative width in pixels
//	{h}              — creative height in pixels
//	{min_dur}        — minimum video duration (seconds)
//	{max_dur}        — maximum video duration (seconds)
func substituteMacros(demandURL string, pr *PlayerRequest, adsCfg *AdServerConfig) string {
	// Random cache-buster — math/rand is ~10x faster than crypto/rand for
	// non-security values; Go 1.20+ global source is goroutine-safe.
	cbStr := strconv.FormatUint(uint64(mrand.Uint32()), 10)

	minDur := adsCfg.MinDuration
	if pr.MinDuration > 0 {
		minDur = pr.MinDuration
	}
	maxDur := adsCfg.MaxDuration
	if pr.MaxDuration > 0 {
		maxDur = pr.MaxDuration
	}

	wStr := strconv.FormatInt(pr.Width, 10)
	hStr := strconv.FormatInt(pr.Height, 10)
	minStr := strconv.Itoa(minDur)
	maxStr := strconv.Itoa(maxDur)

	r := strings.NewReplacer(
		"{cb}", cbStr,
		"{uip}", pr.IP,
		"{ua}", url.QueryEscape(pr.UA),
		"{app_bundle}", pr.AppBundle,
		"{app_name}", url.QueryEscape(pr.AppName),
		"{app_store_url}", url.QueryEscape(pr.AppStoreURL),
		"{device_make}", url.QueryEscape(pr.DeviceMake),
		"{device_model}", url.QueryEscape(pr.DeviceModel),
		"{device_type}", strconv.Itoa(pr.DeviceType),
		"{idfa}", pr.IFA,
		"{ifa}", pr.IFA,
		"{ifa_type}", pr.IFAType,
		"{device_os}", url.QueryEscape(pr.DeviceOS),
		"{os}", url.QueryEscape(pr.DeviceOS),
		"{device_osv}", url.QueryEscape(pr.OSVersion),
		"{osv}", url.QueryEscape(pr.OSVersion),
		"{version_os}", url.QueryEscape(pr.OSVersion),
		"{lang}", pr.Language,
		"{language}", pr.Language,
		"{country_code}", pr.CountryCode,
		"{dnt}", strconv.Itoa(int(pr.DNT)),
		"{lmt}", strconv.Itoa(int(pr.LMT)),
		"{skip}", strconv.Itoa(int(pr.Skip)),
		"{coppa}", strconv.Itoa(int(pr.COPPA)),
		"{us_privacy}", pr.CCPA,
		"{gdpr}", pr.GDPR,
		"{gdpr_consent}", pr.Consent,
		"{content_rating}", url.QueryEscape(pr.ContentRating),
		"{content_length}", strconv.FormatInt(pr.ContentLen, 10),
		// Dimension aliases (width/height are more common than w/h in tag URLs)
		"{w}", wStr,
		"{h}", hStr,
		"{width}", wStr,
		"{height}", hStr,
		// Duration aliases
		"{min_dur}", minStr,
		"{max_dur}", maxStr,
		"{min_duration}", minStr,
		"{max_duration}", maxStr,
	)
	return r.Replace(demandURL)
}

// resolveMimeTypes returns the MIME type list for imp.video.mimes.
// Falls back to ["video/mp4"] when the config has no entries.
func resolveMimeTypes(configured []string) []string {
	if len(configured) > 0 {
		return configured
	}
	return []string{"video/mp4"}
}

// splitCSVTrim splits a comma-separated string, trims spaces, and drops empties.
func splitCSVTrim(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// postToDemandORTB — shared ORTB HTTP transport (used by VAST→ORTB and ORTB→ORTB)
// ─────────────────────────────────────────────────────────────────────────────

// postToDemandORTB builds an enriched OpenRTB 2.5 BidRequest from the player
// request, POSTs it to the campaign's demand ORTB endpoint, and returns the
// applyOutboundHeaders sets outbound OpenRTB request headers directly on httpReq.
// Writing directly avoids allocating the intermediate http.Header map.
func applyOutboundHeaders(httpReq *http.Request, bidReq *openrtb2.BidRequest, ortbVersion string) {
	httpReq.Header.Set("Content-Type", "application/json;charset=utf-8")
	httpReq.Header.Set("Accept-Encoding", "gzip")
	httpReq.Header.Set("Connection", "keep-alive")
	if ortbVersion == "" {
		ortbVersion = "2.5"
	}
	httpReq.Header.Set("X-OpenRTB-Version", ortbVersion)
	if bidReq.Device != nil {
		if bidReq.Device.UA != "" {
			httpReq.Header.Set("User-Agent", bidReq.Device.UA)
			httpReq.Header.Set("X-Device-User-Agent", bidReq.Device.UA)
		}
		if bidReq.Device.IP != "" {
			httpReq.Header.Set("X-Forwarded-For", bidReq.Device.IP)
			httpReq.Header.Set("X-Device-IP", bidReq.Device.IP)
		} else if bidReq.Device.IPv6 != "" {
			httpReq.Header.Set("X-Forwarded-For", bidReq.Device.IPv6)
		}
	}
}

// isCTVDeviceType returns true for Connected TV, Set Top Box, and OTT device types
// (adcom1.DeviceType 3=Connected TV, 6=Set Top Box, 7=OTT Device).
func isCTVDeviceType(dt int) bool {
	return dt == 3 || dt == 6 || dt == 7
}

// inferCTVDeviceType guesses DeviceType from UA/OS/Make when the player omits it.
// Returns 3 (Connected TV) for known CTV platforms, 7 (OTT) for streaming sticks,
// or 1 (Mobile/Tablet) for mobile OS. Falls back to 3 for app context since
// CTV is the most common video/app inventory source.
func inferCTVDeviceType(ua, osStr, make string) int {
	uaL := strings.ToLower(ua)
	osL := strings.ToLower(osStr)
	makeL := strings.ToLower(make)

	switch {
	case strings.Contains(uaL, "roku") || osL == "roku":
		return 3 // Connected TV
	case strings.Contains(uaL, "tizen") || strings.Contains(makeL, "samsung"):
		return 3
	case strings.Contains(uaL, "webos") || strings.Contains(makeL, "lg"):
		return 3
	case strings.Contains(uaL, "vizio") || strings.Contains(makeL, "vizio"):
		return 3
	case strings.Contains(uaL, "bravia") || strings.Contains(makeL, "sony"):
		return 3
	case strings.Contains(uaL, "firetv") || strings.Contains(uaL, "aftm") || strings.Contains(uaL, "aftt"):
		return 7 // OTT device (Fire Stick)
	case osL == "tvos" || strings.Contains(uaL, "apple tv"):
		return 3
	case strings.Contains(uaL, "chromecast") || strings.Contains(uaL, "android tv"):
		return 3
	case osL == "ios" || osL == "android":
		return 1 // Mobile/Tablet
	default:
		return 3 // default for app context: CTV
	}
}

// postToDemandORTB builds an enriched OpenRTB 2.5 BidRequest from the player
// request, POSTs it to the campaign's demand ORTB endpoint, and returns the
// raw BidResponse.  The caller decides whether to convert the response to VAST
// (vastToORTBAdapter) or proxy it directly (ortbToORTBAdapter).
func (h *VideoPipelineHandler) postToDemandORTB(
	ctx context.Context,
	pr *PlayerRequest,
	adsCfg *AdServerConfig,
) (*openrtb2.BidResponse, error) {
	if err := validateOutboundPublicURL(adsCfg.DemandOrtbURL); err != nil {
		return nil, fmt.Errorf("invalid demand ORTB url: %w", err)
	}
	bidReq := h.buildOpenRTBRequest(pr, adsCfg)
	cleanReq := cleanrtb.FromPrebidRequest(bidReq)
	body, err := json.Marshal(cleanReq)
	if err != nil {
		return nil, fmt.Errorf("marshal OpenRTB request: %w", err)
	}

	// Debug: log outbound request summary for demand fill-rate diagnostics.
	if bidReq.Device != nil {
		bundle := ""
		if bidReq.App != nil {
			bundle = bidReq.App.Bundle
		}
		logSampled(100, "postToDemandORTB: url=%s id=%s ua_len=%d bundle=%s source_floor=%.2f demand_floor=%.2f effective_floor=%.2f tmax=%d body_len=%d",
			adsCfg.DemandOrtbURL, bidReq.ID, len(bidReq.Device.UA),
			bundle, adsCfg.FloorCPM, adsCfg.DemandFloorCPM, bidReq.Imp[0].BidFloor, bidReq.TMax, len(body))
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, adsCfg.DemandOrtbURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build HTTP request: %w", err)
	}
	httpReq.ContentLength = int64(len(body))
	applyOutboundHeaders(httpReq, bidReq, adsCfg.OrtbVersion)

	resp, err := h.demandClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("POST to demand ORTB endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, fmt.Errorf("no fill from demand (204)")
	}
	// Reject non-2xx statuses from demand — the body is unlikely to be a valid
	// BidResponse and attempting to parse it wastes CPU / creates confusing errors.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("demand ORTB returned HTTP %d", resp.StatusCode)
	}

	// Handle gzip-compressed responses.  applyOutboundHeaders explicitly sends
	// Accept-Encoding: gzip which disables Go's automatic decompression, so we
	// must decompress manually when the response is gzipped.
	var bodyReader io.Reader = resp.Body
	if strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		gr := gzipReaderPool.Get().(*gzip.Reader)
		if gerr := gr.Reset(resp.Body); gerr != nil {
			gzipReaderPool.Put(gr)
			gr, gerr = gzip.NewReader(resp.Body)
			if gerr != nil {
				return nil, fmt.Errorf("gzip reader for bid response: %w", gerr)
			}
		}
		defer func() {
			_ = gr.Close()
			gzipReaderPool.Put(gr)
		}()
		bodyReader = gr
	}

	// Read body into a pooled buffer, then unmarshal.
	buf := h.bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	_, copyErr := io.Copy(buf, io.LimitReader(bodyReader, 1<<20)) // 1 MB cap
	if copyErr != nil {
		h.bufPool.Put(buf)
		return nil, fmt.Errorf("read bid response: %w", copyErr)
	}
	rawBody := buf.Bytes()
	if len(rawBody) == 0 {
		h.bufPool.Put(buf)
		return nil, fmt.Errorf("demand ORTB returned empty body (HTTP %d)", resp.StatusCode)
	}
	var bidResp openrtb2.BidResponse
	if err := json.Unmarshal(sanitizeBidResponse(rawBody), &bidResp); err != nil {
		preview := string(rawBody)
		if len(preview) > 200 {
			preview = preview[:200] + "..."
		}
		h.bufPool.Put(buf)
		log.Printf("postToDemandORTB: decode error: %v — body preview: %s", err, preview)
		return nil, fmt.Errorf("decode bid response: %w", err)
	}
	h.bufPool.Put(buf)
	return &bidResp, nil
}

// bidRespIntFieldRe matches OpenRTB integer fields that some demand partners
// erroneously send as JSON strings (e.g. "mtype":"1" instead of "mtype":1).
// The sanitizer converts them to bare numbers before unmarshaling so that the
// strict openrtb2 types (MarkupType, CreativeAttribute, etc.) don't reject them.
//
// Fields covered: mtype, attr items, dur, w, h, wratio, hratio, expdir
var bidRespIntFieldRe = regexp.MustCompile(`("(?:mtype|dur|w|h|wratio|hratio|expdir)"\s*:\s*)"(\d+)"`)

// mtypeNameRe matches named mtype constants used by some demand partners
// instead of the numeric values defined by OpenRTB 2.5 §5.1.
// Fully case-insensitive to handle all DSP variants:
//
//	"CREATIVE_MARKUP_VIDEO", "creative_markup_video", "Video", "video",
//	"VIDEO_VAST", "VAST", "DISPLAY", etc.
var mtypeNameRe = regexp.MustCompile(`(?i)"mtype"\s*:\s*"(?:CREATIVE_MARKUP_)?([A-Z_]+)"`)

var mtypeNameToInt = map[string]string{
	"BANNER":  "1",
	"DISPLAY": "1",
	"VIDEO":   "2",
	"VAST":    "2",
	"AUDIO":   "3",
	"NATIVE":  "4",
}

// resolveMtypeName maps a DSP mtype string to the OpenRTB numeric value.
// Handles compound names like "VIDEO_VAST", "VIDEO_HTML" by checking if
// any known keyword is contained within the value.
func resolveMtypeName(raw string) string {
	upper := strings.ToUpper(raw)
	// Exact match first (most common case).
	if num, ok := mtypeNameToInt[upper]; ok {
		return num
	}
	// Substring match for compound names (VIDEO_VAST, VIDEO_HTML, etc.).
	for name, num := range mtypeNameToInt {
		if strings.Contains(upper, name) {
			return num
		}
	}
	return ""
}

// sanitizeBidResponse normalises a raw BidResponse body from demand before
// JSON unmarshaling.  It is a no-op (returns the original slice) when no
// stringified integer fields are found, keeping the hot path allocation-free.
func sanitizeBidResponse(data []byte) []byte {
	if !bytes.Contains(data, []byte(`":"`)) {
		return data
	}
	// Fix named mtype constants (CREATIVE_MARKUP_VIDEO, video, VAST, DISPLAY, etc.)
	data = mtypeNameRe.ReplaceAllFunc(data, func(match []byte) []byte {
		subs := mtypeNameRe.FindSubmatch(match)
		if len(subs) < 2 {
			return match
		}
		num := resolveMtypeName(string(subs[1]))
		if num == "" {
			return match
		}
		return []byte(`"mtype":` + num)
	})
	// Fix numeric-as-string fields ("mtype":"2" → "mtype":2)
	return bidRespIntFieldRe.ReplaceAll(data, []byte(`${1}${2}`))
}

// fastGenerateID returns a pseudo-random 16-hex-character ID using the global
// math/rand source. It is ~10x faster than crypto/rand and sufficient for
// non-security auction/impression IDs. Go 1.20+ global PRNG is goroutine-safe.
func fastGenerateID() string {
	return strconv.FormatUint(mrand.Uint64(), 16)
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 4 — extractWinningBid
// ─────────────────────────────────────────────────────────────────────────────

// WinningBid holds the raw bid together with its creative and bidder identity.
type WinningBid struct {
	BidID   string
	AdM     string // VAST XML, VAST Wrapper tag URI, or inline media URL
	NURL    string // Win-notification URL — fire when bid wins (before serving)
	BURL    string // Billing URL — fire when impression is rendered (on start)
	Price   float64
	Width   int64
	Height  int64
	CrID    string
	DealID  string
	ADomain []string // Advertiser domains
}

// extractWinningBid picks the best bid from the BidResponse using a
// 4-level tie-break rule:
//
//  1. Top price    — highest bid.Price wins.
//  2. Seat weight  — when prices are equal, higher weight (adsCfg.SeatWeights) wins.
//  3. Latency      — when weight is equal, the bid that appeared earliest in the
//     response wins (earlier index = faster seat).
//  4. Stable key   — when all else is equal, lexicographic seat+":"+bid.ID
//     ensures repeatability across requests.
func (h *VideoPipelineHandler) extractWinningBid(
	resp *openrtb2.BidResponse,
	adsCfg *AdServerConfig,
) (*WinningBid, string, error) {
	if resp == nil {
		return nil, "", fmt.Errorf("nil bid response")
	}

	var bestBid *openrtb2.Bid
	var bestAdM string
	var bestSeat string
	var bestWeight float64
	var bestPos int
	hasBest := false
	pos := 0
	// Auction transparency counters
	totalBids := 0
	emptyAdM := 0
	belowFloor := 0

	for _, seatBid := range resp.SeatBid {
		seatWeight := 1.0
		if adsCfg != nil && adsCfg.SeatWeights != nil {
			if w, ok := adsCfg.SeatWeights[seatBid.Seat]; ok {
				seatWeight = w
			}
		}
		for i := range seatBid.Bid {
			bid := &seatBid.Bid[i]
			adm := normalizeAdM(bid.AdM)
			pos++
			totalBids++
			if adm == "" && bid.NURL == "" {
				emptyAdM++
				continue
			}
			// Enforce the minimum sell price for the active demand path.
			minPrice := adsCfg.winnerMinPriceCPM()
			if minPrice > 0 && bid.Price < minPrice {
				belowFloor++
				continue
			}
			if !hasBest || bidBetter(bid.Price, seatWeight, pos, bestBid.Price, bestWeight, bestPos) {
				bestBid = bid
				bestAdM = adm
				bestSeat = seatBid.Seat
				bestWeight = seatWeight
				bestPos = pos
				hasBest = true
			}
		}
	}

	// Structured auction log for transparency: shows all candidates + outcome.
	if hasBest {
		logSampled(100, "auction id=%s bids=%d empty=%d floor_reject=%d source_floor=%.2f demand_floor=%.2f effective_floor=%.2f winner=%s seat=%s price=%.4f",
			resp.ID, totalBids, emptyAdM, belowFloor, adsCfg.FloorCPM, adsCfg.DemandFloorCPM, adsCfg.winnerMinPriceCPM(), bestBid.ID, bestSeat, bestBid.Price)
	} else {
		logSampled(100, "auction id=%s bids=%d empty=%d floor_reject=%d source_floor=%.2f demand_floor=%.2f effective_floor=%.2f winner=none",
			resp.ID, totalBids, emptyAdM, belowFloor, adsCfg.FloorCPM, adsCfg.DemandFloorCPM, adsCfg.winnerMinPriceCPM())
	}

	if !hasBest {
		return nil, "", fmt.Errorf("no fill: %d bids, %d empty AdM, %d below floor", totalBids, emptyAdM, belowFloor)
	}
	win := &WinningBid{
		BidID:   bestBid.ID,
		AdM:     bestAdM,
		NURL:    bestBid.NURL,
		BURL:    bestBid.BURL,
		Price:   bestBid.Price,
		Width:   bestBid.W,
		Height:  bestBid.H,
		CrID:    bestBid.CrID,
		DealID:  bestBid.DealID,
		ADomain: bestBid.ADomain,
	}
	return win, bestSeat, nil
}

func bidBetter(price, weight float64, pos int, bestPrice, bestWeight float64, bestPos int) bool {
	if price != bestPrice {
		return price > bestPrice
	}
	if weight != bestWeight {
		return weight > bestWeight
	}
	return pos < bestPos
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 5 — buildVASTResponse
// ─────────────────────────────────────────────────────────────────────────────

// vastCreativeType classifies what is inside WinningBid.AdM.
type vastCreativeType uint8

const (
	adMIsVAST  vastCreativeType = iota // AdM contains a VAST XML document (Inline or Wrapper)
	adMIsURI                           // AdM is a bare URL → use as Wrapper VASTAdTagURI
	adMIsMedia                         // AdM is a direct media URL → build Inline around it
	adMEmpty                           // AdM is empty; fall back to NURL/BURL
)

func isEmptyAdM(adm string) bool {
	trim := strings.TrimSpace(adm)
	if trim == "" {
		return true
	}
	switch {
	case trim == "0":
		return true
	case strings.EqualFold(trim, "null"):
		return true
	case strings.EqualFold(trim, "none"):
		return true
	case strings.EqualFold(trim, "empty"):
		return true
	case strings.EqualFold(trim, "n/a"):
		return true
	case strings.EqualFold(trim, "na"):
		return true
	default:
		return false
	}
}

func normalizeAdM(adm string) string {
	if isEmptyAdM(adm) {
		return ""
	}
	return adm
}

// mediaTypeForURL returns the MIME type and VAST delivery attribute for a media
// URL, inferred from the URL path extension (query/fragment stripped).
// Defaults to video/mp4 + progressive for unknown extensions.
func mediaTypeForURL(rawURL string) (mimeType, delivery string) {
	// strip query / fragment so filepath.Ext sees only the path
	if u, err := url.Parse(rawURL); err == nil {
		rawURL = u.Path
	}
	switch strings.ToLower(filepath.Ext(rawURL)) {
	case ".m3u8":
		return "application/x-mpegURL", "streaming"
	case ".mpd":
		return "application/dash+xml", "streaming"
	case ".webm":
		return "video/webm", "progressive"
	case ".mov":
		return "video/quicktime", "progressive"
	case ".ts":
		return "video/mp2t", "progressive"
	default:
		return "video/mp4", "progressive"
	}
}

// classifyAdM determines how the bid AdM field should be interpreted.
func classifyAdM(adm string) vastCreativeType {
	if isEmptyAdM(adm) {
		// Treat whitespace/placeholder AdM (e.g. "adm":" ", "adm":"kosong")
		// the same as an absent one.
		return adMEmpty
	}
	probe := adm
	if len(probe) > 512 {
		probe = probe[:512]
	}
	upper := strings.ToUpper(probe)
	// A valid VAST document always contains a <VAST element. The <?xml prolog alone
	// is not sufficient — any XML document would match it, including error responses.
	if strings.Contains(upper, "<VAST") {
		return adMIsVAST
	}
	if strings.HasPrefix(strings.TrimSpace(adm), "http") {
		return adMIsURI
	}
	return adMIsMedia
}

// validateVASTAdM performs a lightweight structural check on an AdM value that
// has already been classified as adMIsVAST.  Returns a non-nil error when the
// document is malformed so the bid is rejected rather than forwarded to the player.
//
// Checks performed (in order):
//  1. The XML must be parseable — tokens up to (and including) the first <Ad>
//     child are decoded so we confirm the document is not truncated garbage.
//  2. The root element name must be "VAST" (case-insensitive).
//  3. The first <Ad> child must itself contain either <InLine> or <Wrapper>.
//     — Wrapper: accepted immediately after the XML structure check.  The
//     document deliberately contains no self-contained creative; the player
//     chain-fetches the next VAST URL, so we must not reject it.
//     — InLine:  accepted; it carries the actual creative.
//     — Neither: rejected (e.g. <VAST><Error>…</Error></VAST> error docs).
func validateVASTAdM(adm string) error {
	dec := xml.NewDecoder(strings.NewReader(adm))
	dec.Strict = false

	// ── Step 1: confirm root is <VAST> ───────────────────────────────────────
	foundVAST := false
	for {
		tok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("VAST AdM is malformed XML: %w", err)
		}
		se, ok := tok.(xml.StartElement)
		if !ok {
			continue // skip <?xml …?>, comments, whitespace
		}
		if !strings.EqualFold(se.Name.Local, "VAST") {
			return fmt.Errorf("VAST AdM root element is <%s>, expected <VAST>", se.Name.Local)
		}
		foundVAST = true
		break
	}
	if !foundVAST {
		return fmt.Errorf("VAST AdM contains no root element")
	}

	// ── Step 2: walk to the first <Ad> child ─────────────────────────────────
	// A <VAST> document that contains only <Error> (no <Ad>) is an error-only
	// response and must not be forwarded as if it were a valid creative.
	for {
		tok, err := dec.Token()
		if err != nil {
			// EOF without finding any <Ad> — treat as error-only / empty.
			return fmt.Errorf("VAST AdM contains no <Ad> element")
		}
		se, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		if !strings.EqualFold(se.Name.Local, "Ad") {
			// <Error>, <Extensions>, etc. at VAST level — skip whole subtree.
			if err := dec.Skip(); err != nil {
				return fmt.Errorf("VAST AdM is malformed XML in <%s>: %w", se.Name.Local, err)
			}
			continue
		}
		// Found the first <Ad> element. Now look for <Wrapper> or <InLine>.
		for {
			inner, err := dec.Token()
			if err != nil {
				return fmt.Errorf("VAST AdM <Ad> element is incomplete: %w", err)
			}
			ise, ok := inner.(xml.StartElement)
			if !ok {
				if _, isEnd := inner.(xml.EndElement); isEnd {
					// </Ad> with no recognised child — treat as empty.
					return fmt.Errorf("VAST AdM <Ad> element contains neither <InLine> nor <Wrapper>")
				}
				continue
			}
			switch strings.ToLower(ise.Name.Local) {
			case "wrapper":
				// Wrapper is valid by definition — the player resolves the chain.
				// Do NOT apply any further content checks; return success.
				return nil
			case "inline":
				// InLine carries the actual creative — accepted.
				return nil
			default:
				// Unexpected child element inside <Ad>; skip it and keep looking.
				if err := dec.Skip(); err != nil {
					return fmt.Errorf("VAST AdM <Ad> child <%s> is malformed: %w", ise.Name.Local, err)
				}
			}
		}
	}
}

// buildVASTResponse constructs a VAST 3.0 response from the winning bid.
//
// NURL / BURL semantics (OpenRTB 2.5 §7.2):
//
//	NURL (win notice)     — fired server-side immediately when the bid wins.
//	                        MUST NOT appear in the VAST document; embedding it
//	                        would expose the ${AUCTION_PRICE} macro to the player
//	                        and cause incorrect fire timing.
//	BURL (billing notice) — player-fired on ad render.  Embedded as
//	                        <Impression id="burl"> so all VAST 2.0+ players fire
//	                        it at the same time as other impression pixels.
//	                        Auction macros are resolved before embedding.
//
// Creative-type routing:
//  1. AdM is VAST XML  — inject PBS <Impression> + resolved BURL <Impression> + tracking events
//  2. AdM is a tag URI — build VAST Wrapper with PBS tracker + BURL + tracking events
//  3. AdM is media URL — build VAST InLine with PBS tracking + BURL
//  4. AdM is empty     — NURL NOT pre-fired; wrap NURL as VASTAdTagURI so player fires it once
func (h *VideoPipelineHandler) buildVASTResponse(
	ctx context.Context,
	pr *PlayerRequest,
	adsCfg *AdServerConfig,
	win *WinningBid,
	bidder string,
	auctionID string,
) (string, error) {
	// ── NURL — server-side win notice ────────────────────────────────────────
	// Fire NURL server-side ONLY when the bid has a real AdM so the URL is not
	// also wrapped as VASTAdTagURI (which would cause the player to fire it a
	// second time, double-counting the win notice with SSPs that track it).
	nurlForWrap := ""
	if win.NURL != "" {
		resolved := resolveAuctionMacros(win.NURL, win, auctionID, bidder)
		if win.AdM != "" {
			// AdM present — fire immediately, no need to wrap NURL.
			h.fireWinNotice(resolved)
		} else {
			// No AdM — NURL doubles as the creative URI; do NOT fire it here
			// so the player fires it exactly once when it fetches the creative.
			nurlForWrap = resolved
		}
	}

	// ── BURL — billing notice ─────────────────────────────────────────────────
	// Resolve macros once; pass the resolved URL to all VAST builders.
	// The resolved BURL is embedded as <Impression id="burl"> in the VAST
	// document so the player fires it directly to the DSP at ad-render time.
	// Do NOT also cache it in pendingBURLs — that would cause a double-fire
	// (once by the player via <Impression>, once by the server via
	// ImpressionEndpoint).  Server-side BURL caching is only used by the
	// ORTB endpoint path, where no VAST document is served.
	resolvedBURL := resolveAuctionMacros(win.BURL, win, auctionID, bidder)

	reqBaseURL := adsCfg.RequestBaseURL
	pbsImpURL := h.buildImpressionURL(reqBaseURL, auctionID, win.BidID, bidder, pr.PlacementID, win.CrID, win.Price, win.ADomain)

	// Build PBS tracking beacons (start/quartile/complete) once; reused across all paths.
	trackingEvents := h.buildTrackingEventList(reqBaseURL, auctionID, win.BidID, bidder, pr.PlacementID, win.CrID, win.Price, win.ADomain)

	switch classifyAdM(win.AdM) {

	case adMIsVAST:
		if err := validateVASTAdM(win.AdM); err != nil {
			log.Printf("buildVASTResponse: rejecting bid %s — %v", win.BidID, err)
			return "", fmt.Errorf("invalid VAST AdM: %w", err)
		}
		// If the bid returned a VAST Wrapper (chain to another DSP), resolve
		// the full chain server-side so the player gets the final MediaFile.
		adm := win.AdM
		if extractVASTAdTagURI(adm) != "" {
			resolved, rerr := h.resolveVASTWrapperChain(ctx, adm, pr.UA, 5)
			if rerr == nil && strings.TrimSpace(resolved) != "" {
				adm = resolved
			}
		}
		vast := injectVASTImpression(adm, pbsImpURL)
		if resolvedBURL != "" {
			vast = injectVASTImpressionWithID(vast, resolvedBURL, "burl")
		}
		vast = injectVASTTracking(vast, trackingEvents)
		return vast, nil

	case adMIsURI:
		// AdM is a bare VAST tag URL → fetch and resolve the chain server-side
		// instead of wrapping it (CTV players can't follow wrapper chains).
		fetched, ferr := h.fetchVAST(ctx, win.AdM, pr.UA)
		if ferr == nil && strings.TrimSpace(fetched) != "" {
			resolved, rerr := h.resolveVASTWrapperChain(ctx, fetched, pr.UA, 5)
			if rerr == nil && strings.TrimSpace(resolved) != "" {
				vast := injectVASTImpression(resolved, pbsImpURL)
				if resolvedBURL != "" {
					vast = injectVASTImpressionWithID(vast, resolvedBURL, "burl")
				}
				vast = injectVASTTracking(vast, trackingEvents)
				return vast, nil
			}
		}
		// Fallback: build a wrapper if server-side resolution failed.
		return h.buildVASTWrapperDoc(win, pbsImpURL, win.AdM, resolvedBURL, trackingEvents)

	case adMIsMedia:
		// AdM holds a direct media URL → build an InLine document.
		return h.buildVASTInlineDoc(pr, win, bidder, reqBaseURL, pbsImpURL, win.AdM, resolvedBURL, auctionID, adsCfg)

	default: // adMEmpty
		if nurlForWrap != "" {
			// No AdM. NURL not yet fired — wrap it so the player chain-fetches
			// the downstream creative and fires the win notice in the process
			// (OpenRTB pre-2.0 backwards compatibility).
			return h.buildVASTWrapperDoc(win, pbsImpURL, nurlForWrap, resolvedBURL, trackingEvents)
		}
		return "", fmt.Errorf("bid has no AdM and no NURL — cannot build VAST")
	}
}

// buildVASTWrapperDoc creates a VAST 3.0 Wrapper document.
//
// Impressions emitted:
//
//	<Impression> — PBS server-side tracker (always present)
//	<Impression> — Player-fired billing notification (when resolvedBURL != "")
//
// NURL is intentionally absent — it has already been fired server-side by
// buildVASTResponse before this function is called.
func (h *VideoPipelineHandler) buildVASTWrapperDoc(
	win *WinningBid,
	pbsImpURL string,
	tagURI string,
	resolvedBURL string,
	trackingEvents []vastTracking,
) (string, error) {
	impressions := []vastImpression{
		{Inner: vastCDATA{Text: pbsImpURL}},
	}
	if resolvedBURL != "" {
		// id="burl" marks this pixel as the OpenRTB billing notice URL
		// so it can be distinguished from PBS and DSP impression pixels.
		impressions = append(impressions, vastImpression{
			ID:    "burl",
			Inner: vastCDATA{Text: resolvedBURL},
		})
	}

	var te *vastTrackingEvents
	if len(trackingEvents) > 0 {
		te = &vastTrackingEvents{Tracking: trackingEvents}
	}

	doc := vastRoot{
		Version: "3.0",
		Ad: []vastAd{{
			ID: win.BidID,
			Wrapper: &vastWrapper{
				AdSystem:       "AdZrvr",
				VASTAdTagURI:   vastCDATA{Text: tagURI},
				Impression:     impressions,
				TrackingEvents: te,
			},
		}},
	}
	xmlBytes, err := xml.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "", fmt.Errorf("xml marshal: %w", err)
	}
	return xml.Header + string(xmlBytes), nil
}

// buildVASTInlineDoc creates a VAST 3.0 InLine document with a MediaFile.
//
// Impressions emitted:
//
//	<Impression> — PBS server-side tracker (always present)
//	<Impression> — Player-fired billing notification (when resolvedBURL != "")
//
// NURL is intentionally absent — it has already been fired server-side by
// buildVASTResponse before this function is called.
func (h *VideoPipelineHandler) buildVASTInlineDoc(
	pr *PlayerRequest,
	win *WinningBid,
	bidder string,
	reqBaseURL string,
	pbsImpURL string,
	mediaURL string,
	resolvedBURL string,
	auctionID string,
	adsCfg *AdServerConfig,
) (string, error) {
	impressions := []vastImpression{
		{Inner: vastCDATA{Text: pbsImpURL}},
	}
	if resolvedBURL != "" {
		// id="burl" marks this pixel as the OpenRTB billing notice URL.
		impressions = append(impressions, vastImpression{
			ID:    "burl",
			Inner: vastCDATA{Text: resolvedBURL},
		})
	}

	events := []vastTracking{
		{Event: "start", Inner: vastCDATA{Text: h.buildTrackingURL(reqBaseURL, auctionID, win.BidID, bidder, pr.PlacementID, "start", win.CrID, win.Price, win.ADomain)}},
		{Event: "firstQuartile", Inner: vastCDATA{Text: h.buildTrackingURL(reqBaseURL, auctionID, win.BidID, bidder, pr.PlacementID, "firstQuartile", win.CrID, win.Price, win.ADomain)}},
		{Event: "midpoint", Inner: vastCDATA{Text: h.buildTrackingURL(reqBaseURL, auctionID, win.BidID, bidder, pr.PlacementID, "midpoint", win.CrID, win.Price, win.ADomain)}},
		{Event: "thirdQuartile", Inner: vastCDATA{Text: h.buildTrackingURL(reqBaseURL, auctionID, win.BidID, bidder, pr.PlacementID, "thirdQuartile", win.CrID, win.Price, win.ADomain)}},
		{Event: "complete", Inner: vastCDATA{Text: h.buildTrackingURL(reqBaseURL, auctionID, win.BidID, bidder, pr.PlacementID, "complete", win.CrID, win.Price, win.ADomain)}},
	}

	mimeType, deliveryMode := mediaTypeForURL(mediaURL)
	doc := vastRoot{
		Version: "3.0",
		Ad: []vastAd{{
			ID: win.BidID,
			Inline: &vastInline{
				AdSystem:   "AdZrvr",
				AdTitle:    "Video Ad",
				Impression: impressions,
				Creatives: vastCreatives{
					Creative: []vastCreative{{
						ID: win.CrID,
						Linear: &vastLinear{
							Duration:       formatDuration(adsCfg.MaxDuration),
							TrackingEvents: &vastTrackingEvents{Tracking: events},
							MediaFiles: vastMediaFiles{
								MediaFile: []vastMediaFile{{
									Delivery: deliveryMode,
									Type:     mimeType,
									Width:    int(win.Width),
									Height:   int(win.Height),
									Inner:    vastCDATA{Text: mediaURL},
								}},
							},
						},
					}},
				},
			},
		}},
	}
	xmlBytes, err := xml.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "", fmt.Errorf("xml marshal: %w", err)
	}
	return xml.Header + string(xmlBytes), nil
}

// injectVASTImpression inserts a <Impression> CDATA element into an
// existing VAST XML string at the most appropriate position.
//
// We probe a small set of known capitalizations for each close-tag rather than
// uppercasing the entire document (which would allocate a copy of potentially
// 100 KB+ of VAST XML just to scan it).
//
// Insertion order:
//  1. After the last </Impression> — keeps all impression pixels together.
//  2. Before </InLine> — when no Impression element exists yet.
//  3. Before </Wrapper> — last resort for Wrapper-only documents.
func injectVASTImpression(vast, trackURL string) string {
	return injectVASTImpressionWithID(vast, trackURL, "")
}

// injectVASTImpressionWithID is like injectVASTImpression but also sets
// an optional id attribute on the <Impression> element (e.g. id="burl"),
// which is useful for distinguishing PBS, DSP, and billing pixels server-side.
// indexCaseFold finds the first occurrence of needle in haystack (case-insensitive)
// and returns its byte index, or -1 if not found.
func indexCaseFold(haystack, needle string) int {
	lower := strings.ToLower(haystack)
	return strings.Index(lower, strings.ToLower(needle))
}

// lastIndexCaseFold finds the last occurrence of needle in haystack (case-insensitive).
func lastIndexCaseFold(haystack, needle string) int {
	lower := strings.ToLower(haystack)
	return strings.LastIndex(lower, strings.ToLower(needle))
}

func injectVASTImpressionWithID(vast, trackURL, id string) string {
	if trackURL == "" {
		return vast
	}
	// Deduplicate: if the exact tracking URL already exists inside any Impression,
	// skip injection to avoid over-counting.
	if strings.Contains(vast, trackURL) {
		return vast
	}
	var tag string
	if id != "" {
		tag = `<Impression id="` + id + `"><![CDATA[` + trackURL + `]]></Impression>`
	} else {
		tag = `<Impression><![CDATA[` + trackURL + `]]></Impression>`
	}

	if idx := lastIndexCaseFold(vast, "</impression>"); idx != -1 {
		insertAt := idx + len("</impression>")
		return vast[:insertAt] + tag + vast[insertAt:]
	}
	if idx := indexCaseFold(vast, "</inline>"); idx != -1 {
		return vast[:idx] + tag + vast[idx:]
	}
	if idx := indexCaseFold(vast, "</wrapper>"); idx != -1 {
		return vast[:idx] + tag + vast[idx:]
	}
	return vast
}

// injectVASTTracking injects PBS <Tracking> pixel elements into an existing VAST
// XML document so that our quartile/complete beacons are always recorded even
// when demand returns its own fully-formed VAST XML.
//
// Strategy (in preference order):
//  1. Append after the last </Tracking> — extends an existing <TrackingEvents>.
//  2. Insert a fresh <TrackingEvents> block before </Linear>.
//  3. Insert a fresh <TrackingEvents> block before </Wrapper>.
func injectVASTTracking(vast string, events []vastTracking) string {
	if len(events) == 0 {
		return vast
	}
	// Build only tracking pixels that are not already present in the document
	// to prevent duplicate beacon fires (discrepancy vs. DSP billing).
	var filtered []vastTracking
	for _, ev := range events {
		if ev.Inner.Text == "" {
			continue
		}
		if strings.Contains(vast, ev.Inner.Text) {
			continue
		}
		filtered = append(filtered, ev)
	}
	if len(filtered) == 0 {
		return vast
	}
	var sb strings.Builder
	for _, ev := range filtered {
		fmt.Fprintf(&sb, `<Tracking event="%s"><![CDATA[%s]]></Tracking>`, ev.Event, ev.Inner.Text)
	}
	block := sb.String()

	// Strategy 1: append after the last existing </Tracking>
	if idx := lastIndexCaseFold(vast, "</tracking>"); idx != -1 {
		insertAt := idx + len("</tracking>")
		return vast[:insertAt] + block + vast[insertAt:]
	}
	wrapped := "<TrackingEvents>" + block + "</TrackingEvents>"
	// Strategy 2: inject before </Linear>
	if idx := indexCaseFold(vast, "</linear>"); idx != -1 {
		return vast[:idx] + wrapped + vast[idx:]
	}
	// Strategy 3: inject before </Wrapper>
	if idx := indexCaseFold(vast, "</wrapper>"); idx != -1 {
		return vast[:idx] + wrapped + vast[idx:]
	}
	return vast
}

// buildTrackingEventList returns PBS start/quartile/complete tracking events as
// a []vastTracking slice ready to embed in any VAST document type.
func (h *VideoPipelineHandler) buildTrackingEventList(reqBaseURL, auctionID, bidID, bidder, placementID, crid string, price float64, adom []string) []vastTracking {
	evNames := []string{"start", "firstQuartile", "midpoint", "thirdQuartile", "complete"}
	out := make([]vastTracking, len(evNames))
	for i, ev := range evNames {
		out[i] = vastTracking{
			Event: ev,
			Inner: vastCDATA{Text: h.buildTrackingURL(reqBaseURL, auctionID, bidID, bidder, placementID, ev, crid, price, adom)},
		}
	}
	return out
}

// requestBaseURL derives the scheme+host from an inbound request.
// It prefers X-Forwarded-Proto for reverse-proxy deployments.
// Falls back to "http" when TLS is not in use.
func requestBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	} else if p := r.Header.Get("X-Forwarded-Proto"); p != "" {
		scheme = p
	}
	host := r.Host
	if host == "" {
		host = r.URL.Host
	}
	return scheme + "://" + host
}

// buildImpressionURL constructs the PBS /video/impression beacon URL that is
// injected into the VAST <Impression> tag. When the player fires this URL the
// dedicated ImpressionEndpoint records the impression directly.
func (h *VideoPipelineHandler) buildImpressionURL(reqBaseURL, auctionID, bidID, bidder, placementID, crid string, price float64, adom []string) string {
	base := reqBaseURL
	if base == "" {
		base = h.externalURL
	}
	q := url.Values{}
	q.Set("auction_id", auctionID)
	q.Set("bid_id", bidID)
	q.Set("bidder", bidder)
	q.Set("placement_id", placementID)
	if crid != "" {
		q.Set("crid", crid)
	}
	if price > 0 {
		q.Set("price", strconv.FormatFloat(price, 'f', -1, 64))
	}
	if len(adom) > 0 {
		q.Set("adom", strings.Join(adom, ","))
	}
	return base + "/video/impression?" + q.Encode()
}

// buildTrackingURL constructs the PBS /video/tracking beacon URL.
// reqBaseURL, when non-empty, overrides the configured external_url so that
// the tracking pixel always refers back to the same host that served the VAST
// (e.g. the player's custom domain). All parameter values are URL-encoded.
func (h *VideoPipelineHandler) buildTrackingURL(reqBaseURL, auctionID, bidID, bidder, placementID, event, crid string, price float64, adom []string) string {
	base := reqBaseURL
	if base == "" {
		base = h.externalURL
	}
	q := url.Values{}
	q.Set("auction_id", auctionID)
	q.Set("bid_id", bidID)
	q.Set("bidder", bidder)
	q.Set("placement_id", placementID)
	q.Set("event", event)
	if crid != "" {
		q.Set("crid", crid)
	}
	if price > 0 {
		q.Set("price", strconv.FormatFloat(price, 'f', -1, 64))
	}
	if len(adom) > 0 {
		q.Set("adom", strings.Join(adom, ","))
	}
	return base + "/video/tracking?" + q.Encode()
}

// iso2ToISO3 converts an ISO 3166-1 alpha-2 country code (e.g. "US") to the
// corresponding alpha-3 code (e.g. "USA") required by OpenRTB 2.5 geo.country.
// If the code is already 3 characters or is unrecognised the input is returned
// unchanged so we never silently drop a value.
func iso2ToISO3(alpha2 string) string {
	// Complete UN M.49 / ISO 3166-1 mapping (249 entries).
	const mapping = `` +
		"AD:AND,AE:ARE,AF:AFG,AG:ATG,AI:AIA,AL:ALB,AM:ARM,AO:AGO,AQ:ATA,AR:ARG," +
		"AS:ASM,AT:AUT,AU:AUS,AW:ABW,AX:ALA,AZ:AZE," +
		"BA:BIH,BB:BRB,BD:BGD,BE:BEL,BF:BFA,BG:BGR,BH:BHR,BI:BDI,BJ:BEN,BL:BLM," +
		"BM:BMU,BN:BRN,BO:BOL,BQ:BES,BR:BRA,BS:BHS,BT:BTN,BV:BVT,BW:BWA,BY:BLR,BZ:BLZ," +
		"CA:CAN,CC:CCK,CD:COD,CF:CAF,CG:COG,CH:CHE,CI:CIV,CK:COK,CL:CHL,CM:CMR," +
		"CN:CHN,CO:COL,CR:CRI,CU:CUB,CV:CPV,CW:CUW,CX:CXR,CY:CYP,CZ:CZE," +
		"DE:DEU,DJ:DJI,DK:DNK,DM:DMA,DO:DOM,DZ:DZA," +
		"EC:ECU,EE:EST,EG:EGY,EH:ESH,ER:ERI,ES:ESP,ET:ETH," +
		"FI:FIN,FJ:FJI,FK:FLK,FM:FSM,FO:FRO,FR:FRA," +
		"GA:GAB,GB:GBR,GD:GRD,GE:GEO,GF:GUF,GG:GGY,GH:GHA,GI:GIB,GL:GRL,GM:GMB," +
		"GN:GIN,GP:GLP,GQ:GNQ,GR:GRC,GS:SGS,GT:GTM,GU:GUM,GW:GNB,GY:GUY," +
		"HK:HKG,HM:HMD,HN:HND,HR:HRV,HT:HTI,HU:HUN," +
		"ID:IDN,IE:IRL,IL:ISR,IM:IMN,IN:IND,IO:IOT,IQ:IRQ,IR:IRN,IS:ISL,IT:ITA," +
		"JE:JEY,JM:JAM,JO:JOR,JP:JPN," +
		"KE:KEN,KG:KGZ,KH:KHM,KI:KIR,KM:COM,KN:KNA,KP:PRK,KR:KOR,KW:KWT,KY:CYM,KZ:KAZ," +
		"LA:LAO,LB:LBN,LC:LCA,LI:LIE,LK:LKA,LR:LBR,LS:LSO,LT:LTU,LU:LUX,LV:LVA,LY:LBY," +
		"MA:MAR,MC:MCO,MD:MDA,ME:MNE,MF:MAF,MG:MDG,MH:MHL,MK:MKD,ML:MLI,MM:MMR," +
		"MN:MNG,MO:MAC,MP:MNP,MQ:MTQ,MR:MRT,MS:MSR,MT:MLT,MU:MUS,MV:MDV,MW:MWI," +
		"MX:MEX,MY:MYS,MZ:MOZ," +
		"NA:NAM,NC:NCL,NE:NER,NF:NFK,NG:NGA,NI:NIC,NL:NLD,NO:NOR,NP:NPL,NR:NRU,NU:NIU,NZ:NZL," +
		"OM:OMN," +
		"PA:PAN,PE:PER,PF:PYF,PG:PNG,PH:PHL,PK:PAK,PL:POL,PM:SPM,PN:PCN,PR:PRI," +
		"PS:PSE,PT:PRT,PW:PLW,PY:PRY," +
		"QA:QAT," +
		"RE:REU,RO:ROU,RS:SRB,RU:RUS,RW:RWA," +
		"SA:SAU,SB:SLB,SC:SYC,SD:SDN,SE:SWE,SG:SGP,SH:SHN,SI:SVN,SJ:SJM,SK:SVK," +
		"SL:SLE,SM:SMR,SN:SEN,SO:SOM,SR:SUR,SS:SSD,ST:STP,SV:SLV,SX:SXM,SY:SYR,SZ:SWZ," +
		"TC:TCA,TD:TCD,TF:ATF,TG:TGO,TH:THA,TJ:TJK,TK:TKL,TL:TLS,TM:TKM,TN:TUN,TO:TON," +
		"TR:TUR,TT:TTO,TV:TUV,TW:TWN,TZ:TZA," +
		"UA:UKR,UG:UGA,UM:UMI,US:USA,UY:URY,UZ:UZB," +
		"VA:VAT,VC:VCT,VE:VEN,VG:VGB,VI:VIR,VN:VNM,VU:VUT," +
		"WF:WLF,WS:WSM," +
		"YE:YEM,YT:MYT," +
		"ZA:ZAF,ZM:ZMB,ZW:ZWE"
	if len(alpha2) == 3 {
		return alpha2 // already alpha-3
	}
	key := strings.ToUpper(alpha2) + ":"
	if idx := strings.Index(mapping, key); idx != -1 {
		return mapping[idx+3 : idx+6]
	}
	return alpha2 // unknown — pass through unchanged
}

// prodQPtr returns the adcom1.ProductionQuality pointer for use in
// openrtb2.Content.ProdQ.  When the caller passes 0 (unknown/unset) we default
// to ProductionProfessional (1) which signals premium/broadcast content and
// attracts higher CPMs from brand-safe buyers.
func prodQPtr(v int) *adcom1.ProductionQuality {
	var pq adcom1.ProductionQuality
	if v > 0 {
		pq = adcom1.ProductionQuality(v)
	} else {
		pq = adcom1.ProductionProfessional
	}
	return &pq
}

// videoAdPosition returns the IAB slot position for the given placement type.
// Used to populate imp.video.pos in the OpenRTB bid request.
//
//	"outstream" → 1 (Above The Fold — in-feed/in-content)
//	 all others → 7 (Full Screen — instream CTV pre/mid/post-roll)
func videoAdPosition(placementType string) *adcom1.PlacementPosition {
	switch placementType {
	case "outstream":
		return adcom1.PositionAboveFold.Ptr()
	default: // instream, interstitial, rewarded, CTV
		return adcom1.PositionFullScreen.Ptr()
	}
}

// videoPlacementSubtype maps the ad unit placement string to the OpenRTB 2.5
// adcom1.VideoPlacementSubtype sent in imp.video.placement.
//
//	"instream"     → 1 (In-Stream   — pre/mid/post-roll alongside streaming content)
//	"outstream"    → 4 (In-Feed     — standalone unit in content/social feeds)
//	"interstitial" → 5 (Interstitial/Slider/Floating — full-screen overlay)
//	"rewarded"     → 5 (treated as full-screen interstitial on the buy side)
//	""  / unknown  → 1 (default: In-Stream)
func videoPlacementSubtype(placementType string) adcom1.VideoPlacementSubtype {
	switch placementType {
	case "outstream":
		return adcom1.VideoPlacementInFeed
	case "interstitial", "rewarded":
		return adcom1.VideoPlacementAlwaysVisible
	default: // "instream" or empty
		return adcom1.VideoPlacementInStream
	}
}

// videoPlcmt maps the ad unit placement string to the OpenRTB 2.6
// adcom1.VideoPlcmt sent in imp.video.plcmt.
//
//	"instream"     → 1 (Instream)
//	"outstream"    → 3 (Interstitial — standalone unit)
//	"interstitial" → 3 (Interstitial)
//	"rewarded"     → 3 (Interstitial)
//	""  / unknown  → 1 (default: Instream)
func videoPlcmt(placementType string) adcom1.VideoPlcmtSubtype {
	switch placementType {
	case "outstream", "interstitial", "rewarded":
		return adcom1.VideoPlcmtInterstitial
	default: // "instream" or empty
		return adcom1.VideoPlcmtInstream
	}
}

// formatDuration converts seconds to VAST HH:MM:SS format.
func formatDuration(seconds int) string {
	h := seconds / 3600
	m := (seconds % 3600) / 60
	s := seconds % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

// ─────────────────────────────────────────────────────────────────────────────
// NURL / BURL helpers — auction macro resolution and win-notice firing
// ─────────────────────────────────────────────────────────────────────────────

// resolveAuctionMacros substitutes standard OpenRTB 2.5 §5.1 auction macros
// in rawURL (typically NURL or BURL) with their runtime values.
//
// Supported macros:
//
//	${AUCTION_ID}       — top-level bid-request ID
//	${AUCTION_BID_ID}   — bid.id of the winning bid
//	${AUCTION_IMP_ID}   — imp.id (first impression; "1" by convention)
//	${AUCTION_SEAT_ID}  — bidder / seat identifier
//	${AUCTION_AD_ID}    — creative ID (bid.adid)
//	${AUCTION_PRICE}    — clearing price, URL-encoded
//	${AUCTION_CURRENCY} — bid currency (USD)
//	${AUCTION_LOSS}     — loss reason code (empty for winning bids)
//
// Returns rawURL unchanged when it contains no "${" prefix (fast path).
func resolveAuctionMacros(rawURL string, win *WinningBid, auctionID, bidder string) string {
	if rawURL == "" || !strings.Contains(rawURL, "${") {
		return rawURL
	}
	priceStr := strconv.FormatFloat(win.Price, 'f', -1, 64)
	r := strings.NewReplacer(
		"${AUCTION_ID}", url.QueryEscape(auctionID),
		"${AUCTION_BID_ID}", url.QueryEscape(win.BidID),
		"${AUCTION_IMP_ID}", "1",
		"${AUCTION_SEAT_ID}", url.QueryEscape(bidder),
		"${AUCTION_AD_ID}", url.QueryEscape(win.CrID),
		"${AUCTION_PRICE}", url.QueryEscape(priceStr),
		"${AUCTION_CURRENCY}", "USD",
		"${AUCTION_LOSS}", "",
	)
	return r.Replace(rawURL)
}

// fireWinNotice fires the NURL win notification asynchronously via HTTP GET.
//
// Per OpenRTB 2.5 §7.2, NURL must be called by the exchange (us) when the bid
// wins — not by the player.  Errors are logged for observability.
// A sync.Map dedup guard prevents the same NURL from being fired twice (e.g.
// if a waterfall retry re-selects the same bid).
func (h *VideoPipelineHandler) fireWinNotice(nurl string) {
	if nurl == "" {
		return
	}
	if err := validateOutboundPublicURL(nurl); err != nil {
		log.Printf("fireWinNotice: skipped invalid NURL: %v", err)
		return
	}
	// Dedup: skip if this exact NURL was already fired during this process lifetime.
	if _, loaded := h.firedNURLs.LoadOrStore(nurl, time.Now()); loaded {
		return
	}
	client := h.demandClient
	safeGo(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, nurl, nil)
		if err != nil {
			log.Printf("fireWinNotice: build request error: %v", err)
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("fireWinNotice: HTTP error for NURL: %v", err)
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			log.Printf("fireWinNotice: HTTP %d for NURL %s", resp.StatusCode, nurl)
		}
	})
}

// fireBillingNotice fires the BURL billing notification asynchronously via HTTP GET.
//
// Per OpenRTB 2.5 §7.2, BURL must be called by the exchange when a billable
// event occurs (ad render confirmed by player impression beacon).
// Errors are logged for observability.
func (h *VideoPipelineHandler) fireBillingNotice(burl string) {
	if burl == "" {
		return
	}
	if err := validateOutboundPublicURL(burl); err != nil {
		log.Printf("fireBillingNotice: skipped invalid BURL: %v", err)
		return
	}
	client := h.demandClient
	safeGo(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, burl, nil)
		if err != nil {
			log.Printf("fireBillingNotice: build request error: %v", err)
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("fireBillingNotice: HTTP error for BURL: %v", err)
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			log.Printf("fireBillingNotice: HTTP %d for BURL %s", resp.StatusCode, burl)
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 6 — writeVASTResponse
// ─────────────────────────────────────────────────────────────────────────────

// recordBidReport saves a BidReportEntry to the persistent bid report store
// whenever a real demand fill occurs.  It is fire-and-forget (best effort).
func (h *VideoPipelineHandler) recordBidReport(cfg *AdServerConfig, pr *PlayerRequest, resp *DemandResponse, eventType string) {
	if h.bidReport == nil {
		return
	}
	entry := &BidReportEntry{
		RequestID:   resp.AuctionID,
		ImpID:       pr.PlacementID,
		PublisherID: cfg.PublisherID,
		AdUnitID:    pr.PlacementID,
		Bidder:      resp.Bidder,
		ADomain:     resp.ADomain,
		CrID:        resp.CrID,
		CampaignID:  cfg.CampaignID,
		DealID:      resp.DealID,
		BURL:        resp.BURL,
		Price:       resp.WinPrice,
		Currency:    "USD",
		EventType:   eventType,
		EventTime:   time.Now().UTC(),
		AppBundle:   pr.AppBundle,
		Domain:      pr.Domain,
		CountryCode: pr.CountryCode,
	}
	safeGo(func() {
		_ = h.bidReport.store.create(entry)
	})
}

// writeVASTResponse serialises the VAST document to the HTTP response writer.
// The empty-VAST fast path uses pre-computed bytes (zero alloc).
// Non-empty responses use a pooled buffer.
func (h *VideoPipelineHandler) writeVASTResponse(w http.ResponseWriter, vastXML string) {
	hdr := w.Header()
	hdr.Set("Content-Type", "application/xml; charset=utf-8")
	hdr.Set("X-Content-Type-Options", "nosniff")
	hdr.Set("Connection", "keep-alive")

	if vastXML == emptyVASTString {
		hdr.Set("Content-Length", emptyVASTLenStr)
		w.WriteHeader(http.StatusOK)
		w.Write(emptyVASTBytes) //nolint:errcheck
		return
	}

	buf := h.bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.WriteString(vastXML)
	hdr.Set("Content-Length", strconv.Itoa(buf.Len()))
	w.WriteHeader(http.StatusOK)
	w.Write(buf.Bytes()) //nolint:errcheck
	h.bufPool.Put(buf)
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 7 — TrackingEndpoint
// ─────────────────────────────────────────────────────────────────────────────

// TrackingEndpoint handles playback + tracking beacons fired by the player.
// Supported events: impression, start, firstQuartile, midpoint, thirdQuartile,
// complete, click.
//
// Route: GET /video/tracking
func (h *VideoPipelineHandler) TrackingEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		q := r.URL.Query()

		priceVal := 0.0
		if ps := q.Get("price"); ps != "" {
			priceVal, _ = strconv.ParseFloat(ps, 64)
		}
		ev := TrackingEvent{
			AuctionID:   q.Get("auction_id"),
			BidID:       q.Get("bid_id"),
			Bidder:      q.Get("bidder"),
			PlacementID: q.Get("placement_id"),
			Event:       EventType(q.Get("event")),
			CrID:        q.Get("crid"),
			Price:       priceVal,
			ADomain:     q.Get("adom"),
			ReceivedAt:  time.Now(),
		}

		if ev.AuctionID == "" || ev.BidID == "" || ev.Event == "" {
			http.Error(w, "auction_id, bid_id, and event are required", http.StatusBadRequest)
			return
		}

		h.tracking.record(ev)
		var cfg *AdServerConfig
		if resolvedCfg, err := h.resolveAdServerConfig(ev.PlacementID); err == nil {
			cfg = resolvedCfg
		}
		var dk *auctionDimKey
		if ev.AuctionID != "" {
			h.videoStats.mu.Lock()
			dk = h.videoStats.auctionDims[ev.AuctionID]
			h.videoStats.mu.Unlock()
		}
		h.recordTrackingMetric(ev, cfg, dk)

		if ev.Event == EventStart {
			if cfg != nil {
				h.videoStats.incStart(cfg.PublisherID)
				h.videoStats.incAdvertiserStart(cfg.AdvertiserID)
			}
			h.videoStats.incDimStart(ev.AuctionID)
		}

		// Count player-confirmed completes (100% viewed).
		if ev.Event == EventComplete {
			if cfg != nil {
				h.videoStats.incComplete(cfg.PublisherID)
				h.videoStats.incAdvertiserComplete(cfg.AdvertiserID)
			}
			h.videoStats.incDimComplete(ev.AuctionID)
		}

		h.servePixelGIF(w)
	}
}

// servePixelGIF writes a 1×1 transparent GIF response to w.
// Used by tracking and impression beacon endpoints.
var pixelGIF = []byte{
	0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00,
	0x01, 0x00, 0x80, 0x00, 0x00, 0xff, 0xff, 0xff,
	0x00, 0x00, 0x00, 0x21, 0xf9, 0x04, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x00,
	0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x44,
	0x01, 0x00, 0x3b,
}

var pixelGIFContentLength = strconv.Itoa(len(pixelGIF))

func (h *VideoPipelineHandler) servePixelGIF(w http.ResponseWriter) {
	hdr := w.Header()
	hdr.Set("Content-Type", "image/gif")
	hdr.Set("Content-Length", pixelGIFContentLength)
	hdr.Set("Cache-Control", "no-store")
	hdr.Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	w.Write(pixelGIF) //nolint:errcheck
}

// ImpressionEndpoint is the dedicated handler for the VAST <Impression> beacon.
// The VAST <Impression> tag points here (not to /video/tracking) so that
// impression counting is cleanly separated from generic playback events.
// This is the ONLY place impressions are counted — a firing beacon confirms
// the player actually rendered the ad.
//
// Route: GET /video/impression
func (h *VideoPipelineHandler) ImpressionEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		q := r.URL.Query()
		placementID := q.Get("placement_id")
		auctionID := q.Get("auction_id")
		bidID := q.Get("bid_id")
		if auctionID == "" || bidID == "" {
			http.Error(w, "auction_id and bid_id are required", http.StatusBadRequest)
			return
		}
		key := impressionKey{AuctionID: auctionID, BidID: bidID}
		now := time.Now()

		// Dedup: only count each impression once per auction_id:bid_id.
		// VAST wrapper chains and player retries can fire the same
		// <Impression> URL multiple times — skip duplicates.
		if h.firedImpressions.SeenOrAdd(key, now.Unix()) {
			// Already counted — still return the GIF pixel but don't double-count.
			logSampled(50, "ImpressionEndpoint: duplicate beacon for auction=%s bid=%s from %s — skipped", auctionID, bidID, maskIP(extractClientIP(r)))
			h.servePixelGIF(w)
			return
		}

		// Credit dimension stats first so cached bidder/placement/price can be
		// reused without re-reading query parameters on the hot path.
		dk := h.videoStats.incDimImpression(auctionID)
		bidder := q.Get("bidder")
		if dk != nil && dk.Bidder != "" {
			bidder = dk.Bidder
		}
		if dk != nil && dk.Placement != "" {
			placementID = dk.Placement
		}
		priceVal := 0.0
		if dk != nil {
			priceVal = dk.PriceCPM
		} else if ps := q.Get("price"); ps != "" {
			priceVal, _ = strconv.ParseFloat(ps, 64)
		}

		clientIP := extractClientIP(r)
		logSampled(200, "ImpressionEndpoint: beacon fired auction=%s bid=%s placement=%s from ip=%s", auctionID, bidID, placementID, maskIP(clientIP))

		// Record a TrackingEvent so the events log stays complete.
		h.tracking.record(TrackingEvent{
			AuctionID:   auctionID,
			BidID:       bidID,
			Bidder:      bidder,
			PlacementID: placementID,
			Event:       EventImpression,
			CrID:        q.Get("crid"),
			Price:       priceVal,
			ADomain:     q.Get("adom"),
			ReceivedAt:  now,
		})

		h.metricsEng.RecordImps(metrics.ImpLabels{VideoImps: true})

		// Use cached auction data for revenue attribution when available;
		// fall back to beacon URL params + config lookup otherwise.
		var cfg *AdServerConfig
		if dk != nil {
			h.videoStats.incImpressionBatch(dk.PublisherID, dk.AdvertiserID, dk.PriceCPM, dk.PublisherPriceCPM)
		} else if resolvedCfg, err := h.resolveAdServerConfig(placementID); err == nil {
			cfg = resolvedCfg
			h.videoStats.incImpressionBatch(resolvedCfg.PublisherID, resolvedCfg.AdvertiserID, priceVal, resolvedCfg.FloorCPM)
		}
		if cfg == nil {
			if resolvedCfg, err := h.resolveAdServerConfig(placementID); err == nil {
				cfg = resolvedCfg
			}
		}
		h.recordImpressionMetric(placementID, auctionID, bidID, bidder, q.Get("crid"), priceVal, cfg, dk)

		// Fire demand BURL server-side (OpenRTB 2.5 §7.2: exchange fires
		// billing notice when a billable event occurs — ad render confirmed).
		if val, ok := h.pendingBURLs.LoadAndDelete(key); ok {
			if pb, ok := val.(pendingBURL); ok && now.Before(pb.ExpiresAt) {
				h.fireBillingNotice(pb.URL)
			}
		}

		h.servePixelGIF(w)
	}
}

// TrackingEventsEndpoint exposes recorded tracking events as JSON (useful for
// the dashboard / debugging). Supports optional pagination via ?limit=N&offset=M.
//
// Route: GET /video/tracking/events
func (h *VideoPipelineHandler) TrackingEventsEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		events := h.tracking.all()

		// Pagination: ?limit=N&offset=M (defaults: offset=0, limit=1000)
		q := r.URL.Query()
		offset := 0
		limit := 1000
		if v := q.Get("offset"); v != "" {
			if n, e := strconv.Atoi(v); e == nil && n >= 0 {
				offset = n
			}
		}
		if v := q.Get("limit"); v != "" {
			if n, e := strconv.Atoi(v); e == nil && n > 0 {
				limit = n
			}
		}
		total := len(events)
		if offset > total {
			offset = total
		}
		end := offset + limit
		if end > total {
			end = total
		}
		page := events[offset:end]

		resp := struct {
			Total  int             `json:"total"`
			Offset int             `json:"offset"`
			Limit  int             `json:"limit"`
			Events []TrackingEvent `json:"events"`
		}{Total: total, Offset: offset, Limit: limit, Events: page}

		data, _ := json.Marshal(resp)
		hdr := w.Header()
		hdr.Set("Content-Type", "application/json")
		hdr.Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		w.Write(data) //nolint:errcheck
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Video Statistics endpoint
// ─────────────────────────────────────────────────────────────────────────────

// Snapshot returns a live snapshot of all video statistics.
// Used by DashboardRegistry.WireVideoStats to enrich supply/demand partner list responses.
func (h *VideoPipelineHandler) Snapshot() VideoStatsPayload {
	return h.snapshotVideoMetrics()
}

// VideoStatsEndpoint exposes per-publisher ad request / opportunity / impression
// / revenue counters accumulated since the server last started.
//
// Route: GET /dashboard/stats/video
func (h *VideoPipelineHandler) VideoStatsEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		payload := h.snapshotVideoMetrics()
		data, _ := json.Marshal(payload)
		hdr := w.Header()
		hdr.Set("Content-Type", "application/json")
		hdr.Set("Cache-Control", "no-cache, no-store, must-revalidate")
		hdr.Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		w.Write(data) //nolint:errcheck
	}
}

// Route: GET /dashboard/stats/video/overview?period=today|yesterday|week|month
func (h *VideoPipelineHandler) VideoOverviewStatsEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		payload, err := h.snapshotOverviewMetrics(r.Context(), r.URL.Query().Get("period"), r.URL.Query().Get("tz"), time.Now().UTC())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data, _ := json.Marshal(payload)
		hdr := w.Header()
		hdr.Set("Content-Type", "application/json")
		hdr.Set("Cache-Control", "no-cache, no-store, must-revalidate")
		hdr.Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		w.Write(data) //nolint:errcheck
	}
}

// Route: POST /dashboard/stats/reset
func (h *VideoPipelineHandler) ResetStatsEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := h.resetVideoMetrics(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`)) //nolint:errcheck
	}
}

// dashboardConfigResponse is the JSON shape returned by GET /dashboard/config.
// It exposes the live server settings that the Exchange Optimisation checklist
// reads to colour-code its status badges.
type dashboardConfigResponse struct {
	AuctionTimeouts struct {
		Default int `json:"default"`
		Max     int `json:"max"`
	} `json:"auction_timeouts_ms"`
	HTTPClient struct {
		MaxIdleConns        int `json:"max_idle_connections"`
		MaxIdleConnsPerHost int `json:"max_idle_connections_per_host"`
		IdleConnTimeout     int `json:"idle_connection_timeout_seconds"`
	} `json:"http_client"`
	Compression struct {
		Response struct {
			EnableGzip bool `json:"enable_gzip"`
		} `json:"response"`
		Request struct {
			EnableGzip bool `json:"enable_gzip"`
		} `json:"request"`
	} `json:"compression"`
}

// DashboardConfigEndpoint returns the live server configuration that the
// dashboard Exchange Optimisation checklist needs to colour its badges.
//
// Route: GET /dashboard/config
func (h *VideoPipelineHandler) DashboardConfigEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		var resp dashboardConfigResponse
		if h.cfg != nil {
			resp.AuctionTimeouts.Default = int(h.cfg.AuctionTimeouts.Default)
			resp.AuctionTimeouts.Max = int(h.cfg.AuctionTimeouts.Max)
			resp.HTTPClient.MaxIdleConns = h.cfg.Client.MaxIdleConns
			resp.HTTPClient.MaxIdleConnsPerHost = h.cfg.Client.MaxIdleConnsPerHost
			resp.HTTPClient.IdleConnTimeout = h.cfg.Client.IdleConnTimeout
			resp.Compression.Response.EnableGzip = h.cfg.Compression.Response.GZIP
			resp.Compression.Request.EnableGzip = h.cfg.Compression.Request.GZIP
		}
		data, _ := json.Marshal(resp)
		hdr := w.Header()
		hdr.Set("Content-Type", "application/json")
		hdr.Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.WriteHeader(http.StatusOK)
		w.Write(data) //nolint:errcheck
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Ad Server Config CRUD endpoints (used by the dashboard)
// ─────────────────────────────────────────────────────────────────────────────

// AdServerConfigEndpoint exposes GET (list) and POST (upsert) for ad server configs.
//
// Route: GET  /video/adserver
//
//	POST /video/adserver
func (h *VideoPipelineHandler) AdServerConfigEndpoint() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		switch r.Method {

		case http.MethodGet:
			h.configStore.mu.RLock()
			configs := make([]AdServerConfig, 0, len(h.configStore.configs))
			for _, c := range h.configStore.configs {
				configs = append(configs, *c) // deep copy — prevent data race on pointer
			}
			h.configStore.mu.RUnlock()

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(configs) //nolint:errcheck

		case http.MethodPost:
			var cfg AdServerConfig
			limited := &io.LimitedReader{R: r.Body, N: 65536}
			if err := json.NewDecoder(limited).Decode(&cfg); err != nil {
				http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
				return
			}
			if cfg.PlacementID == "" {
				http.Error(w, "placement_id is required", http.StatusBadRequest)
				return
			}
			if cfg.DemandVASTURL != "" {
				if err := validateOutboundPublicURL(cfg.DemandVASTURL); err != nil {
					http.Error(w, "invalid demand_vast_url: "+err.Error(), http.StatusBadRequest)
					return
				}
			}
			if cfg.DemandOrtbURL != "" {
				if err := validateOutboundPublicURL(cfg.DemandOrtbURL); err != nil {
					http.Error(w, "invalid demand_ortb_url: "+err.Error(), http.StatusBadRequest)
					return
				}
			}
			for _, extra := range cfg.ExtraDemand {
				if extra.VASTTagURL != "" {
					if err := validateOutboundPublicURL(extra.VASTTagURL); err != nil {
						http.Error(w, "invalid extra_demand.vast_tag_url: "+err.Error(), http.StatusBadRequest)
						return
					}
				}
				if extra.OrtbURL != "" {
					if err := validateOutboundPublicURL(extra.OrtbURL); err != nil {
						http.Error(w, "invalid extra_demand.ortb_url: "+err.Error(), http.StatusBadRequest)
						return
					}
				}
			}
			h.configStore.set(&cfg)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(cfg) //nolint:errcheck

		case http.MethodDelete:
			placementID := ps.ByName("placement_id")
			if placementID == "" {
				http.Error(w, "placement_id is required", http.StatusBadRequest)
				return
			}
			if h.configStore.get(placementID) == nil {
				http.Error(w, "placement not found", http.StatusNotFound)
				return
			}
			h.configStore.remove(placementID)
			w.WriteHeader(http.StatusNoContent)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func validateOutboundPublicURL(rawURL string) error {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return errors.New("url is empty")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("url scheme must be http or https")
	}
	host := strings.TrimSpace(parsed.Hostname())
	if host == "" {
		return errors.New("url host is empty")
	}
	lowerHost := strings.ToLower(host)
	if lowerHost == "localhost" || strings.HasSuffix(lowerHost, ".localhost") || strings.HasSuffix(lowerHost, ".local") {
		return errors.New("url host must be public")
	}
	if ip := net.ParseIP(host); ip != nil {
		if !isPublicOutboundIP(ip) {
			return errors.New("url host must be public")
		}
		return nil
	}
	if !strings.Contains(lowerHost, ".") {
		return errors.New("url host must be public")
	}
	return nil
}

func isPublicOutboundIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalMulticast() || ip.IsLinkLocalUnicast() || ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}
	if ipv4 := ip.To4(); ipv4 != nil {
		if ipv4[0] == 100 && ipv4[1] >= 64 && ipv4[1] <= 127 {
			return false
		}
	}
	return true
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────
