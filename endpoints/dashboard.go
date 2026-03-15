package endpoints

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/prebid/prebid-server/v4/metrics"
	metricsConf "github.com/prebid/prebid-server/v4/metrics/config"
	"github.com/prebid/prebid-server/v4/util/fasthttpclient"
)

var (
	dashDBOnce sync.Once
	dashDB     *sql.DB
)

func getDashDB() *sql.DB {
	dashDBOnce.Do(func() {
		dsn := os.Getenv("DASH_DB_DSN")
		if dsn == "" {
			return
		}
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			log.Printf("dashboard: postgres connect failed, falling back to file store: %v", err)
			return
		}
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(10)
		db.SetConnMaxLifetime(5 * time.Minute)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			log.Printf("dashboard: postgres ping failed, falling back to file store: %v", err)
			_ = db.Close()
			return
		}
		if _, err := db.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS dashboard_entities (
				kind TEXT NOT NULL,
				id   TEXT NOT NULL,
				payload JSONB NOT NULL,
				PRIMARY KEY (kind,id)
			);`); err != nil {
			log.Printf("dashboard: create table failed, falling back to file store: %v", err)
			_ = db.Close()
			return
		}
		dashDB = db
		log.Printf("dashboard: postgres store enabled (table dashboard_entities)")
	})
	return dashDB
}

// DashboardStats is the JSON payload returned by /dashboard/stats
type DashboardStats struct {
	Impressions      DashboardImpStats        `json:"impressions"`
	Requests         DashboardRequestStats    `json:"requests"`
	Connections      DashboardConnectionStats `json:"connections"`
	Privacy          DashboardPrivacyStats    `json:"privacy"`
	Cache            DashboardCacheStats      `json:"cache"`
	Bidders          []DashboardBidderStats   `json:"bidders"`
	TopBiddersByBids []DashboardBidderStats   `json:"top_bidders_by_bids"`
}

type DashboardImpStats struct {
	Total  int64 `json:"total"`
	Banner int64 `json:"banner"`
	Video  int64 `json:"video"`
	Native int64 `json:"native"`
	Audio  int64 `json:"audio"`
}

type DashboardRequestStats struct {
	Auction    int64 `json:"auction"`
	AMP        int64 `json:"amp"`
	Video      int64 `json:"video"`
	CookieSync int64 `json:"cookie_sync"`
	SetUID     int64 `json:"set_uid"`
	NoCookie   int64 `json:"no_cookie"`
}

type DashboardConnectionStats struct {
	Active       int64 `json:"active"`
	AcceptErrors int64 `json:"accept_errors"`
	CloseErrors  int64 `json:"close_errors"`
}

type DashboardPrivacyStats struct {
	CCPARequests  int64 `json:"ccpa_requests"`
	CCPAOptOut    int64 `json:"ccpa_opt_out"`
	COPPARequests int64 `json:"coppa_requests"`
	TCFv2Requests int64 `json:"tcf_v2_requests"`
	LMTRequests   int64 `json:"lmt_requests"`
}

type DashboardCacheStats struct {
	StoredReqHits   int64 `json:"stored_req_hits"`
	StoredReqMisses int64 `json:"stored_req_misses"`
	StoredImpHits   int64 `json:"stored_imp_hits"`
	StoredImpMisses int64 `json:"stored_imp_misses"`
	AccountHits     int64 `json:"account_cache_hits"`
	AccountMisses   int64 `json:"account_cache_misses"`
}

type DashboardBidderStats struct {
	Name          string  `json:"name"`
	BidsReceived  int64   `json:"bids_received"`
	NoBids        int64   `json:"no_bids"`
	Errors        int64   `json:"errors"`
	AvgResponseMs float64 `json:"avg_response_ms"`
}

// NewDashboardHandler returns an httprouter.Handle that serves the dashboard HTML page.
// We read the file into memory and write it directly to avoid ERR_CONTENT_LENGTH_MISMATCH
// that can occur when http.ServeFile interacts with conditional-GET request headers.
func NewDashboardHandler() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		data, err := os.ReadFile("static/dashboard.html")
		if err != nil {
			http.Error(w, "dashboard not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}
}

// NewDashboardStatsHandler returns an httprouter.Handle that produces real-time metrics as JSON.
func NewDashboardStatsHandler(metricsEngine *metricsConf.DetailedMetricsEngine) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		stats := buildDashboardStats(metricsEngine)

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		if err := json.NewEncoder(w).Encode(stats); err != nil {
			http.Error(w, "failed to encode stats", http.StatusInternalServerError)
		}
	}
}

// NewExtStatsFetchHandler proxies a GET request to a third-party analytics API URL.
// The client supplies the fully-constructed URL in the JSON body {"url":"https://..."}.
// Only HTTPS URLs are accepted to limit SSRF exposure on the admin-only dashboard.
func NewExtStatsFetchHandler() httprouter.Handle {
	client := fasthttpclient.NewClient(15*time.Second, fasthttpclient.TransportConfig{
		Name:                "dashboard-ext-stats",
		DialTimeout:         2 * time.Second,
		KeepAlive:           30 * time.Second,
		MaxConnsPerHost:     128,
		MaxIdleConnDuration: 60 * time.Second,
		ReadTimeout:         15 * time.Second,
		WriteTimeout:        15 * time.Second,
		ReadBufferSize:      8192,
		WriteBufferSize:     8192,
	})
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		var proxyRequest struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&proxyRequest); err != nil {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		if !strings.HasPrefix(proxyRequest.URL, "https://") {
			http.Error(w, `{"error":"only https:// URLs are allowed"}`, http.StatusBadRequest)
			return
		}
		resp, err := client.Get(proxyRequest.URL)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		defer resp.Body.Close()
		ct := resp.Header.Get("Content-Type")
		if ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, io.LimitReader(resp.Body, 1<<20)) // 1 MB cap
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Dashboard authentication — session tokens, cookie: dash_session
// Credentials are read from DASH_ADMIN_USER / DASH_ADMIN_PASS environment variables.
// Sessions survive server restarts via data/dash_sessions.json.
// ──────────────────────────────────────────────────────────────────────────────

const (
	dashSessionCookie = "dash_session"
	dashSessionTTL    = 7 * 24 * time.Hour // 7 days — survives a work week
	dashSessionFile   = "./data/dash_sessions.json"
)

// dashAdminUser and dashAdminPass are read from DASH_ADMIN_USER / DASH_ADMIN_PASS
// environment variables. The hard-coded defaults ensure backwards compatibility
// but DASH_ADMIN_PASS must be changed in production via the env var.
var (
	dashAdminUser string
	dashAdminPass string
)

var (
	dashSessions   = map[string]time.Time{} // token → expiry
	dashSessionsMu sync.RWMutex
)

func init() {
	// Credentials — prefer environment variables, fall back to built-in defaults.
	if dashAdminUser = os.Getenv("DASH_ADMIN_USER"); dashAdminUser == "" {
		dashAdminUser = "admin"
	}
	if dashAdminPass = os.Getenv("DASH_ADMIN_PASS"); dashAdminPass == "" {
		log.Fatal("DASH_ADMIN_PASS must be set; refusing to start dashboard with default password")
	}
	// Load persisted sessions from disk on startup.
	data, err := os.ReadFile(dashSessionFile)
	if err == nil {
		var m map[string]time.Time
		if json.Unmarshal(data, &m) == nil {
			now := time.Now()
			for token, exp := range m {
				if now.Before(exp) { // discard already-expired tokens
					dashSessions[token] = exp
				}
			}
		}
	}
}

// pruneExpiredSessions removes expired entries from the in-memory session map.
func pruneExpiredSessions(now time.Time) {
	dashSessionsMu.Lock()
	for token, exp := range dashSessions {
		if now.After(exp) {
			delete(dashSessions, token)
		}
	}
	dashSessionsMu.Unlock()
}

func saveDashSessions() {
	pruneExpiredSessions(time.Now())
	dashSessionsMu.RLock()
	cp := make(map[string]time.Time, len(dashSessions))
	for k, v := range dashSessions {
		cp[k] = v
	}
	dashSessionsMu.RUnlock()
	_ = os.MkdirAll(filepath.Dir(dashSessionFile), 0755)
	data, err := json.Marshal(cp)
	if err != nil {
		return
	}
	tmp := dashSessionFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return
	}
	_ = os.Rename(tmp, dashSessionFile)
}

func isValidDashSession(r *http.Request) bool {
	c, err := r.Cookie(dashSessionCookie)
	if err != nil {
		return false
	}
	return isValidDashSessionToken(c.Value)
}

func isValidDashSessionToken(token string) bool {
	dashSessionsMu.RLock()
	exp, ok := dashSessions[token]
	dashSessionsMu.RUnlock()
	return ok && time.Now().Before(exp)
}

// DashboardAuthMiddleware redirects unauthenticated browser requests to
// /dashboard/login and returns 401 for JSON API callers.
func DashboardAuthMiddleware(next httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		if !isValidDashSession(r) {
			if strings.Contains(r.Header.Get("Accept"), "application/json") ||
				strings.Contains(r.Header.Get("Content-Type"), "application/json") {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
				return
			}
			http.Redirect(w, r, "/dashboard/login", http.StatusFound)
			return
		}
		next(w, r, ps)
	}
}

// NewDashboardLoginGetHandler serves the login page.
// Already-authenticated users are redirected straight to the dashboard.
func NewDashboardLoginGetHandler() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		if isValidDashSession(r) {
			http.Redirect(w, r, "/dashboard", http.StatusFound)
			return
		}
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		http.ServeFile(w, r, "static/login.html")
	}
}

// NewDashboardLoginPostHandler validates credentials and issues a session cookie.
// Accepts both application/json and form-encoded bodies.
func NewDashboardLoginPostHandler() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		var username, password string
		ct := r.Header.Get("Content-Type")
		if strings.Contains(ct, "application/json") {
			var creds struct {
				Username string `json:"username"`
				Password string `json:"password"`
			}
			if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
				http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
				return
			}
			username, password = creds.Username, creds.Password
		} else {
			_ = r.ParseForm()
			username = r.FormValue("username")
			password = r.FormValue("password")
		}
		if !secureStringEquals(username, dashAdminUser) || !secureStringEquals(password, dashAdminPass) {
			if strings.Contains(ct, "application/json") {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"invalid credentials"}`))
			} else {
				http.Redirect(w, r, "/dashboard/login?error=1", http.StatusFound)
			}
			return
		}
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		token := hex.EncodeToString(b)
		dashSessionsMu.Lock()
		dashSessions[token] = time.Now().Add(dashSessionTTL)
		dashSessionsMu.Unlock()
		safeGo(saveDashSessions)
		isSecure := r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https"
		sameSite := http.SameSiteLaxMode
		if isSecure {
			sameSite = http.SameSiteStrictMode
		}
		http.SetCookie(w, &http.Cookie{
			Name:     dashSessionCookie,
			Value:    token,
			Path:     "/",
			HttpOnly: true,
			Secure:   isSecure,
			SameSite: sameSite,
			MaxAge:   int(dashSessionTTL.Seconds()),
		})
		if strings.Contains(ct, "application/json") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true}`))
		} else {
			http.Redirect(w, r, "/dashboard", http.StatusFound)
		}
	}
}

func secureStringEquals(left, right string) bool {
	leftHash := sha256.Sum256([]byte(left))
	rightHash := sha256.Sum256([]byte(right))
	return subtle.ConstantTimeCompare(leftHash[:], rightHash[:]) == 1
}

// NewDashboardLogoutHandler invalidates the session cookie and redirects to login.
func NewDashboardLogoutHandler() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		if c, err := r.Cookie(dashSessionCookie); err == nil {
			dashSessionsMu.Lock()
			delete(dashSessions, c.Value)
			dashSessionsMu.Unlock()
			safeGo(saveDashSessions)
		}
		http.SetCookie(w, &http.Cookie{
			Name:     dashSessionCookie,
			Value:    "",
			Path:     "/",
			Secure:   true,
			SameSite: http.SameSiteStrictMode,
			MaxAge:   -1,
		})
		http.Redirect(w, r, "/dashboard/login", http.StatusFound)
	}
}

// buildDashboardStats orchestrates all section builders and returns the full payload.
func buildDashboardStats(engine *metricsConf.DetailedMetricsEngine) DashboardStats {
	gm := engine.GoMetrics
	if gm == nil {
		return DashboardStats{}
	}

	bidders, topBidders := buildBidderStats(gm)

	return DashboardStats{
		Impressions:      buildImpStats(gm),
		Requests:         buildRequestStats(gm),
		Connections:      buildConnectionStats(gm),
		Privacy:          buildPrivacyStats(gm),
		Cache:            buildCacheStats(gm),
		Bidders:          bidders,
		TopBiddersByBids: topBidders,
	}
}

// buildImpStats collects impression counters broken down by ad format.
func buildImpStats(gm *metrics.Metrics) DashboardImpStats {
	return DashboardImpStats{
		Total:  gm.ImpMeter.Count(),
		Banner: gm.ImpsTypeBanner.Count(),
		Video:  gm.ImpsTypeVideo.Count(),
		Native: gm.ImpsTypeNative.Count(),
		Audio:  gm.ImpsTypeAudio.Count(),
	}
}

// buildRequestStats collects request counters per endpoint type.
func buildRequestStats(gm *metrics.Metrics) DashboardRequestStats {
	reqs := DashboardRequestStats{
		NoCookie:   gm.NoCookieMeter.Count(),
		CookieSync: gm.CookieSyncMeter.Count(),
		SetUID:     gm.SetUidMeter.Count(),
	}

	for reqType, statusMap := range gm.RequestStatuses {
		var total int64
		for _, meter := range statusMap {
			total += meter.Count()
		}
		switch reqType {
		case metrics.ReqTypeORTB2Web, metrics.ReqTypeORTB2App, metrics.ReqTypeORTB2DOOH:
			reqs.Auction += total
		case metrics.ReqTypeAMP:
			reqs.AMP = total
		case metrics.ReqTypeVideo:
			reqs.Video = total
		}
	}

	return reqs
}

// buildConnectionStats collects active connection count and error meters.
func buildConnectionStats(gm *metrics.Metrics) DashboardConnectionStats {
	return DashboardConnectionStats{
		Active:       gm.ConnectionCounter.Count(),
		AcceptErrors: gm.ConnectionAcceptErrorMeter.Count(),
		CloseErrors:  gm.ConnectionCloseErrorMeter.Count(),
	}
}

// buildPrivacyStats collects CCPA, COPPA, TCF, and LMT signal counters.
func buildPrivacyStats(gm *metrics.Metrics) DashboardPrivacyStats {
	var tcfV2Count int64
	if m, ok := gm.PrivacyTCFRequestVersion[metrics.TCFVersionV2]; ok {
		tcfV2Count = m.Count()
	}

	return DashboardPrivacyStats{
		CCPARequests:  gm.PrivacyCCPARequest.Count(),
		CCPAOptOut:    gm.PrivacyCCPARequestOptOut.Count(),
		COPPARequests: gm.PrivacyCOPPARequest.Count(),
		TCFv2Requests: tcfV2Count,
		LMTRequests:   gm.PrivacyLMTRequest.Count(),
	}
}

// buildCacheStats reads hit/miss counts for the stored-request, stored-impression,
// and account caches.
func buildCacheStats(gm *metrics.Metrics) DashboardCacheStats {
	var cs DashboardCacheStats

	for result, meter := range gm.StoredReqCacheMeter {
		switch result {
		case metrics.CacheHit:
			cs.StoredReqHits = meter.Count()
		case metrics.CacheMiss:
			cs.StoredReqMisses = meter.Count()
		}
	}
	for result, meter := range gm.StoredImpCacheMeter {
		switch result {
		case metrics.CacheHit:
			cs.StoredImpHits = meter.Count()
		case metrics.CacheMiss:
			cs.StoredImpMisses = meter.Count()
		}
	}
	for result, meter := range gm.AccountCacheMeter {
		switch result {
		case metrics.CacheHit:
			cs.AccountHits = meter.Count()
		case metrics.CacheMiss:
			cs.AccountMisses = meter.Count()
		}
	}

	return cs
}

// buildBidderStats returns two lists:
//   - all bidders sorted alphabetically
//   - the top-10 bidders by bids received (descending)
func buildBidderStats(gm *metrics.Metrics) (all []DashboardBidderStats, top10 []DashboardBidderStats) {
	all = make([]DashboardBidderStats, 0, len(gm.AdapterMetrics))

	for name, am := range gm.AdapterMetrics {
		var errCount int64
		for _, em := range am.ErrorMeters {
			errCount += em.Count()
		}
		all = append(all, DashboardBidderStats{
			Name:          name,
			BidsReceived:  am.GotBidsMeter.Count(),
			NoBids:        am.NoBidMeter.Count(),
			Errors:        errCount,
			AvgResponseMs: am.RequestTimer.Mean() / 1e6, // nanoseconds → milliseconds
		})
	}

	// Sort descending by bids first so we can cheaply slice the top-10
	// without copying the full adapter list (which can be 200+ entries).
	sort.Slice(all, func(i, j int) bool {
		return all[i].BidsReceived > all[j].BidsReceived
	})
	n := len(all)
	if n > 10 {
		n = 10
	}
	top10 = make([]DashboardBidderStats, n)
	copy(top10, all[:n])

	// Re-sort the full list alphabetically for the bidder table.
	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	return all, top10
}

// ═════════════════════════════════════════════════════════════════════════════
// CTV / In-App Video Exchange CRUD
// ═════════════════════════════════════════════════════════════════════════════

// VideoEnv classifies the inventory environment.
type VideoEnv string

const (
	VideoEnvCTV   VideoEnv = "ctv"
	VideoEnvInApp VideoEnv = "inapp"
)

// VideoPlacement classifies the ad placement within the stream.
type VideoPlacement string

const (
	PlacementInStream     VideoPlacement = "instream"
	PlacementOutStream    VideoPlacement = "outstream"
	PlacementInterstitial VideoPlacement = "interstitial"
	PlacementRewarded     VideoPlacement = "rewarded"
)

// VideoExchangeEntry represents a single programmatic CTV / in-app video
// exchange configuration managed through the dashboard CRUD API.
type VideoExchangeEntry struct {
	ID          string         `json:"id"`
	PublisherID string         `json:"publisher_id,omitempty"`
	Name        string         `json:"name"`
	Environment VideoEnv       `json:"environment"`
	Placement   VideoPlacement `json:"placement"`

	// In-app specific
	BundleID string `json:"bundle_id,omitempty"`
	AppName  string `json:"app_name,omitempty"`

	// CTV specific
	ChannelName string `json:"channel_name,omitempty"`
	NetworkName string `json:"network_name,omitempty"`

	// Video ad parameters
	MinDuration int `json:"min_duration"`
	MaxDuration int `json:"max_duration"`

	// Ad pod configuration (CTV)
	PodDurationSec int `json:"pod_duration_sec,omitempty"`
	MaxPods        int `json:"max_pods,omitempty"`
	PodSequence    int `json:"pod_sequence,omitempty"`

	// CTV companion & taxonomy
	CompanionType []int                  `json:"companion_type,omitempty"` // 1=Static, 2=HTML, 3=iframe
	CatTax        int                    `json:"cattax,omitempty"`         // IAB category taxonomy version
	SellerDomain  string                 `json:"seller_domain,omitempty"`  // schain ASI domain for this exchange
	DomainOrApp   string                 `json:"domain_or_app,omitempty"`
	ContentURL    string                 `json:"content_url,omitempty"`
	TargetingExt  map[string]interface{} `json:"targeting_ext,omitempty"`

	// Integration & source settings
	IntegrationType string `json:"integration_type,omitempty"` // "tag_based"|"open_rtb"

	// Source pricing
	PricingType string  `json:"pricing_type,omitempty"`  // "fixed_cpm"|"floor_price"
	AdvFloorCPM float64 `json:"adv_floor_cpm,omitempty"` // advertiser's floor CPM

	// Exchange controls
	FloorCPM float64  `json:"floor_cpm"`
	Bidders  []string `json:"bidders"`
	Active   bool     `json:"active"`

	// Delivery caps & throttling
	ImpsDailyCap  int `json:"imps_daily_cap,omitempty"`
	ImpsHourlyCap int `json:"imps_hourly_cap,omitempty"`
	QPS           int `json:"qps,omitempty"`
	TimeoutMS     int `json:"timeout_ms,omitempty"`

	// Demand routing — links this Ad Unit to a Campaign (third-party VAST/ORTB demand).
	// When set, the pipeline forwards requests to the Campaign's demand endpoint
	// instead of running a full Prebid auction.
	CampaignID string `json:"campaign_id,omitempty"`
	// Demand links — campaign IDs providing demand for this ad unit
	DemandLinks []string `json:"demand_links,omitempty"`
	// External statistics API config
	ExtStats *ExtStatsConfig `json:"ext_stats,omitempty"`

	// Audience-targeting integration
	SegmentIDs    []string `json:"segment_ids,omitempty"`
	FreqCapImps   int      `json:"freq_cap_imps,omitempty"`
	FreqCapPeriod string   `json:"freq_cap_period,omitempty"` // "hour"|"day"|"week"

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// VideoExchangeListResponse wraps the list payload with a count.
type VideoExchangeListResponse struct {
	Total   int                   `json:"total"`
	Entries []*VideoExchangeEntry `json:"entries"`
}

func normalizeIntegrationType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "open_rtb", "openrtb", "ortb":
		return "open_rtb"
	case "tag_based", "tagbased", "vast", "vast_tag":
		return "tag_based"
	default:
		return ""
	}
}

func normalizeAuctionPriceType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "second", "second_price":
		return "second"
	default:
		return "first"
	}
}

func normalizeYieldPriority(value YieldPriority) YieldPriority {
	switch strings.ToLower(strings.TrimSpace(string(value))) {
	case "house":
		return YieldPriorityHouseAd
	case "programmatic", "premium", "guaranteed":
		return YieldPriorityProgrammatic
	default:
		return YieldPriorityProgrammatic
	}
}

func normalizeDemandPath(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "*", "any", "direct", "pmp":
		return ""
	case "open_auction", "open", "programmatic":
		return "open_auction"
	default:
		return ""
	}
}

// validate returns a human-readable error string if the entry is invalid, or "".
func (e *VideoExchangeEntry) validate() string {
	e.Name = strings.TrimSpace(e.Name)
	e.DomainOrApp = strings.TrimSpace(e.DomainOrApp)
	e.ContentURL = strings.TrimSpace(e.ContentURL)
	e.SellerDomain = strings.TrimSpace(e.SellerDomain)
	e.BundleID = strings.TrimSpace(e.BundleID)
	e.AppName = strings.TrimSpace(e.AppName)
	e.ChannelName = strings.TrimSpace(e.ChannelName)
	e.NetworkName = strings.TrimSpace(e.NetworkName)
	e.IntegrationType = normalizeIntegrationType(e.IntegrationType)
	if e.Name == "" {
		return "name is required"
	}
	if e.IntegrationType == "" {
		return "integration_type must be one of: tag_based, open_rtb"
	}
	switch e.Environment {
	case VideoEnvCTV, VideoEnvInApp:
	default:
		return "environment must be 'ctv' or 'inapp'"
	}
	switch e.Placement {
	case PlacementInStream, PlacementOutStream, PlacementInterstitial, PlacementRewarded:
	default:
		return "placement must be one of: instream, outstream, interstitial, rewarded"
	}
	if e.MinDuration < 0 {
		return "min_duration must be >= 0"
	}
	if e.MaxDuration <= 0 {
		return "max_duration must be > 0"
	}
	if e.MinDuration > e.MaxDuration {
		return "min_duration must be <= max_duration"
	}
	if e.FloorCPM < 0 {
		return "floor_cpm must be >= 0"
	}
	return ""
}

func (e *VideoExchangeEntry) getID() string                { return e.ID }
func (e *VideoExchangeEntry) getCreatedAt() time.Time      { return e.CreatedAt }
func (e *VideoExchangeEntry) setID(id string)              { e.ID = id }
func (e *VideoExchangeEntry) setTimestamps(c, u time.Time) { e.CreatedAt = c; e.UpdatedAt = u }

// VideoExchangeStore is a thread-safe in-memory store for VideoExchangeEntry records.
type VideoExchangeStore = entityStore[*VideoExchangeEntry]

func newVideoExchangeStore(filePath string) *VideoExchangeStore {
	return newEntityStore[*VideoExchangeEntry](filePath, "adunits")
}

func generateID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// atomicWriteJSON serialises v as indented JSON and writes it to path via a
// temp-file-then-rename so the file is never left in a partially-written state.
func atomicWriteJSON(path string, v interface{}) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// dataStorePath returns filepath.Join(dataDir, filename), or "" when dataDir is "".
func dataStorePath(dataDir, filename string) string {
	if dataDir == "" {
		return ""
	}
	return filepath.Join(dataDir, filename)
}

// VideoExchangeHandler owns the store and exposes five httprouter.Handle methods.
type VideoExchangeHandler struct {
	store         *VideoExchangeStore
	campStore     *campaignStore        // resolved lazily via SetCampaignStore
	registerCfg   func(*AdServerConfig) // pipline config hook; set via SetPipelineRegister
	unregisterCfg func(string)          // pipeline config removal hook; set via SetPipelineUnregister
	lookupCfg     func(string) *AdServerConfig
}

// NewVideoExchangeHandler creates a VideoExchangeHandler backed by a persistent store.
// dataDir is the directory where adunits.json is written; pass "" to disable persistence.
func NewVideoExchangeHandler(dataDir string) *VideoExchangeHandler {
	return &VideoExchangeHandler{store: newVideoExchangeStore(dataStorePath(dataDir, "adunits.json"))}
}

// SetCampaignStore injects the campaign store so the handler can resolve demand endpoints.
func (h *VideoExchangeHandler) SetCampaignStore(s *campaignStore) { h.campStore = s }

// SetPipelineRegister injects the callback used to push AdServerConfig into the pipeline.
func (h *VideoExchangeHandler) SetPipelineRegister(fn func(*AdServerConfig)) { h.registerCfg = fn }

// SetPipelineUnregister injects the callback used to remove AdServerConfig from the pipeline.
func (h *VideoExchangeHandler) SetPipelineUnregister(fn func(string)) { h.unregisterCfg = fn }

// SetPipelineLookup injects the callback used to recover runtime placement config
// when the dashboard CRUD store is stale or missing an ad unit row.
func (h *VideoExchangeHandler) SetPipelineLookup(fn func(string) *AdServerConfig) { h.lookupCfg = fn }

// SyncAllToPipeline pushes every ad unit that was loaded from disk into the pipeline
// config store.  Must be called once from router setup after both SetCampaignStore and
// SetPipelineRegister have been injected, so that placements survive server restarts.
func (h *VideoExchangeHandler) SyncAllToPipeline() {
	for _, e := range h.store.list() {
		h.syncPipelineCfg(e)
	}
}

// syncPipelineCfg builds an AdServerConfig from an Ad Unit entry (optionally resolving its
// linked Campaign for campaign demand routing) and registers it with the video pipeline.
func (h *VideoExchangeHandler) syncPipelineCfg(e *VideoExchangeEntry) {
	if h.registerCfg == nil {
		return
	}
	resolvedCampaignID := e.CampaignID
	if resolvedCampaignID == "" {
		for _, campaignID := range e.DemandLinks {
			if campaignID == "" || h.campStore == nil {
				continue
			}
			if _, ok := h.campStore.get(campaignID); ok {
				resolvedCampaignID = campaignID
				break
			}
		}
	}
	if resolvedCampaignID == "" && h.campStore != nil {
		for _, campaign := range h.campStore.list() {
			for _, supplyID := range campaign.SupplyLinks {
				if supplyID == e.ID {
					resolvedCampaignID = campaign.ID
					break
				}
			}
			if resolvedCampaignID != "" {
				break
			}
		}
	}
	cfg := &AdServerConfig{
		PlacementID:    e.ID,
		PublisherID:    e.PublisherID,
		DomainOrApp:    e.DomainOrApp,
		ContentURL:     e.ContentURL,
		MinDuration:    e.MinDuration,
		MaxDuration:    e.MaxDuration,
		AllowedBidders: e.Bidders,
		FloorCPM:       e.FloorCPM,
		CampaignID:     resolvedCampaignID,
		Active:         e.Active,
		TimeoutMS:      e.TimeoutMS,
		PodDuration:    e.PodDurationSec,
		MaxSeq:         e.MaxPods,
		PodSequence:    e.PodSequence,
		CompanionType:  e.CompanionType,
		CatTax:         e.CatTax,
		SellerDomain:   e.SellerDomain,
	}
	if cfg.DomainOrApp == "" {
		cfg.DomainOrApp = e.BundleID
	}
	if len(e.TargetingExt) > 0 {
		cfg.TargetingExt = make(map[string]interface{}, len(e.TargetingExt))
		for key, value := range e.TargetingExt {
			cfg.TargetingExt[key] = value
		}
	}
	cfg.VideoPlacementType = string(e.Placement)

	// Resolve linked Campaign demand endpoint and settings.
	if resolvedCampaignID != "" && h.campStore != nil {
		if camp, ok := h.campStore.get(resolvedCampaignID); ok {
			cfg.DemandVASTURL = camp.VASTTagURL
			cfg.DemandOrtbURL = camp.OrtbEndpointURL
			cfg.AdvertiserID = camp.AdvertiserID
			// Campaign demand floor is distinct from source floor pricing.
			if camp.FloorCPM > 0 {
				cfg.DemandFloorCPM = camp.FloorCPM
			}
			if len(camp.BAdv) > 0 {
				cfg.BAdv = camp.BAdv
			}
			if len(camp.BCat) > 0 {
				cfg.BCat = camp.BCat
			}
			// ORTB version from campaign (e.g. "2.5", "2.6").
			if camp.OrtbVersion != "" {
				cfg.OrtbVersion = camp.OrtbVersion
			}
			// Campaign-level MIME types override the default video/mp4.
			if len(camp.MimeTypes) > 0 {
				cfg.MimeTypes = camp.MimeTypes
			}
			if len(camp.Protocols) > 0 {
				cfg.Protocols = append([]int(nil), camp.Protocols...)
			}
			if len(camp.APIs) > 0 {
				cfg.APIs = append([]int(nil), camp.APIs...)
			}
		}
	}
	// Populate waterfall from DemandLinks — any campaign not already used as
	// the primary CampaignID is added as an ExtraDemandCfg fallback.
	if h.campStore != nil {
		for _, campID := range e.DemandLinks {
			if campID == e.CampaignID {
				continue
			}
			if camp, ok := h.campStore.get(campID); ok {
				extra := ExtraDemandCfg{
					VASTTagURL:   camp.VASTTagURL,
					OrtbURL:      camp.OrtbEndpointURL,
					FloorCPM:     camp.FloorCPM,
					CampaignID:   camp.ID,
					AdvertiserID: camp.AdvertiserID,
					BCat:         camp.BCat,
					BAdv:         camp.BAdv,
				}
				if extra.VASTTagURL != "" || extra.OrtbURL != "" {
					cfg.ExtraDemand = append(cfg.ExtraDemand, extra)
				}
			}
		}
	}
	h.registerCfg(cfg)
}

func normalizeVideoPlacement(value string) VideoPlacement {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case string(PlacementOutStream):
		return PlacementOutStream
	case string(PlacementInterstitial):
		return PlacementInterstitial
	case string(PlacementRewarded):
		return PlacementRewarded
	default:
		return PlacementInStream
	}
}

func inferVideoEnvironment(cfg *AdServerConfig) VideoEnv {
	placement := normalizeVideoPlacement(cfg.VideoPlacementType)
	if placement == PlacementRewarded || placement == PlacementInterstitial {
		return VideoEnvInApp
	}
	if strings.TrimSpace(cfg.ContentURL) != "" {
		return VideoEnvCTV
	}
	return VideoEnvCTV
}

func appendUniqueDemandLink(links []string, campaignID string) []string {
	campaignID = strings.TrimSpace(campaignID)
	if campaignID == "" {
		return links
	}
	for _, existing := range links {
		if existing == campaignID {
			return links
		}
	}
	return append(links, campaignID)
}

func copyTargetingExt(src map[string]interface{}) map[string]interface{} {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}

func (h *VideoExchangeHandler) entryFromRuntimeConfig(cfg *AdServerConfig) *VideoExchangeEntry {
	if cfg == nil || strings.TrimSpace(cfg.PlacementID) == "" {
		return nil
	}
	environment := inferVideoEnvironment(cfg)
	entry := &VideoExchangeEntry{
		ID:              cfg.PlacementID,
		PublisherID:     cfg.PublisherID,
		Name:            cfg.PlacementID,
		Environment:     environment,
		Placement:       normalizeVideoPlacement(cfg.VideoPlacementType),
		MinDuration:     cfg.MinDuration,
		MaxDuration:     cfg.MaxDuration,
		PodDurationSec:  cfg.PodDuration,
		MaxPods:         cfg.MaxSeq,
		PodSequence:     cfg.PodSequence,
		CompanionType:   append([]int(nil), cfg.CompanionType...),
		CatTax:          cfg.CatTax,
		SellerDomain:    cfg.SellerDomain,
		DomainOrApp:     cfg.DomainOrApp,
		ContentURL:      cfg.ContentURL,
		TargetingExt:    copyTargetingExt(cfg.TargetingExt),
		IntegrationType: "open_rtb",
		FloorCPM:        cfg.FloorCPM,
		Bidders:         append([]string(nil), cfg.AllowedBidders...),
		Active:          cfg.Active,
		TimeoutMS:       cfg.TimeoutMS,
		CampaignID:      cfg.CampaignID,
	}
	if environment == VideoEnvInApp {
		entry.BundleID = cfg.DomainOrApp
	}
	if cfg.DemandVASTURL != "" && cfg.DemandOrtbURL == "" {
		entry.IntegrationType = "tag_based"
	}
	entry.DemandLinks = appendUniqueDemandLink(entry.DemandLinks, cfg.CampaignID)
	for _, extra := range cfg.ExtraDemand {
		entry.DemandLinks = appendUniqueDemandLink(entry.DemandLinks, extra.CampaignID)
	}
	return entry
}

func (h *VideoExchangeHandler) recoverEntry(id string) (*VideoExchangeEntry, bool) {
	entry, ok := h.store.get(id)
	if ok {
		return entry, true
	}
	if h.lookupCfg == nil {
		return nil, false
	}
	entry = h.entryFromRuntimeConfig(h.lookupCfg(id))
	if entry == nil {
		return nil, false
	}
	return h.store.put(entry), true
}

// List handles GET /dashboard/video — returns all entries.
func (h *VideoExchangeHandler) List() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		entries := h.store.list()
		writeJSON(w, http.StatusOK, VideoExchangeListResponse{
			Total:   len(entries),
			Entries: entries,
		})
	}
}

// Create handles POST /dashboard/video — creates a new entry.
func (h *VideoExchangeHandler) Create() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		var entry VideoExchangeEntry
		if !decodeBody(w, r, &entry) {
			return
		}
		if msg := entry.validate(); msg != "" {
			writeError(w, http.StatusBadRequest, msg)
			return
		}
		created := h.store.create(&entry)
		h.syncPipelineCfg(created)
		writeJSON(w, http.StatusCreated, created)
	}
}

// Get handles GET /dashboard/video/:id — returns one entry.
func (h *VideoExchangeHandler) Get() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		id := ps.ByName("id")
		entry, ok := h.recoverEntry(id)
		if !ok {
			writeError(w, http.StatusNotFound, "entry not found: "+id)
			return
		}
		writeJSON(w, http.StatusOK, entry)
	}
}

// Update handles PUT /dashboard/video/:id — replaces an existing entry.
func (h *VideoExchangeHandler) Update() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		id := ps.ByName("id")
		var patch VideoExchangeEntry
		if !decodeBody(w, r, &patch) {
			return
		}
		if msg := patch.validate(); msg != "" {
			writeError(w, http.StatusBadRequest, msg)
			return
		}
		if _, ok := h.recoverEntry(id); !ok {
			writeError(w, http.StatusNotFound, "entry not found: "+id)
			return
		}
		updated, ok := h.store.update(id, &patch)
		if !ok {
			writeError(w, http.StatusNotFound, "entry not found: "+id)
			return
		}
		h.syncPipelineCfg(updated)
		writeJSON(w, http.StatusOK, updated)
	}
}

// Delete handles DELETE /dashboard/video/:id — removes an entry.
func (h *VideoExchangeHandler) Delete() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		id := ps.ByName("id")
		deleted := h.store.delete(id)
		if !deleted {
			if h.lookupCfg == nil || h.lookupCfg(id) == nil {
				writeError(w, http.StatusNotFound, "entry not found: "+id)
				return
			}
		}
		if h.unregisterCfg != nil {
			h.unregisterCfg(id)
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst interface{}) bool {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
}

// ── Generic store helpers ─────────────────────────────────────────────────────

// storeable is the minimal interface required by the generic store helpers.
type storeable interface {
	getID() string
	getCreatedAt() time.Time
}

// storeList returns a copy of entries sorted newest-first (by CreatedAt desc).
func storeList[E storeable](mu *sync.RWMutex, entries map[string]E) []E {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]E, 0, len(entries))
	for _, e := range entries {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].getCreatedAt().After(out[j].getCreatedAt()) })
	return out
}

// storeSave serialises entries to filePath atomically.
func storeSave[E storeable](mu *sync.RWMutex, entries map[string]E, filePath, label string) {
	if db := getDashDB(); db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Printf("%s store: begin tx: %v", label, err)
			return
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM dashboard_entities WHERE kind=$1`, label); err != nil {
			log.Printf("%s store: purge: %v", label, err)
			_ = tx.Rollback()
			return
		}
		mu.RLock()
		for _, e := range entries {
			b, err := json.Marshal(e)
			if err != nil {
				log.Printf("%s store: marshal: %v", label, err)
				continue
			}
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO dashboard_entities(kind,id,payload) VALUES ($1,$2,$3)`,
				label, e.getID(), b); err != nil {
				log.Printf("%s store: insert: %v", label, err)
			}
		}
		mu.RUnlock()
		if err := tx.Commit(); err != nil {
			log.Printf("%s store: commit: %v", label, err)
		}
		return
	}
	if filePath == "" {
		return
	}
	mu.RLock()
	out := make([]E, 0, len(entries))
	for _, e := range entries {
		out = append(out, e)
	}
	mu.RUnlock()
	if err := atomicWriteJSON(filePath, out); err != nil {
		log.Printf("%s store: save: %v", label, err)
	}
}

// storeLoad deserialises entries from filePath into the entries map.
func storeLoad[E storeable](mu *sync.RWMutex, entries map[string]E, filePath, label string) {
	if db := getDashDB(); db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		rows, err := db.QueryContext(ctx, `SELECT payload FROM dashboard_entities WHERE kind=$1`, label)
		if err != nil {
			log.Printf("%s store: load db: %v", label, err)
			return
		}
		defer rows.Close()
		var payloads []json.RawMessage
		for rows.Next() {
			var b []byte
			if err := rows.Scan(&b); err != nil {
				log.Printf("%s store: scan: %v", label, err)
				return
			}
			payloads = append(payloads, b)
		}
		data, err := json.Marshal(payloads)
		if err != nil {
			log.Printf("%s store: marshal list: %v", label, err)
			return
		}
		var list []E
		if err := json.Unmarshal(data, &list); err != nil {
			log.Printf("%s store: parse db: %v", label, err)
			return
		}
		mu.Lock()
		for _, e := range list {
			entries[e.getID()] = e
		}
		mu.Unlock()
		return
	}
	if filePath == "" {
		return
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("%s store: load: %v", label, err)
		}
		return
	}
	var list []E
	if err := json.Unmarshal(data, &list); err != nil {
		log.Printf("%s store: parse: %v", label, err)
		return
	}
	mu.Lock()
	for _, e := range list {
		entries[e.getID()] = e
	}
	mu.Unlock()
}

// ── Generic entity store ──────────────────────────────────────────────────────

// entity extends storeable with mutation support and validation.
// Implement on every CRUD entity pointer type to enable the generic store.
type entity interface {
	storeable
	setID(string)
	setTimestamps(created, updated time.Time)
	validate() string
}

// entityStore is a generic, thread-safe map-backed store for any entity type E.
// It handles persistence, sorted listing, and produces HTTP handler closures via
// listHandle / createHandle / getHandle / updateHandle / deleteHandle.
type entityStore[E entity] struct {
	mu       sync.RWMutex
	entries  map[string]E
	filePath string
	label    string
}

func newEntityStore[E entity](filePath, label string) *entityStore[E] {
	s := &entityStore[E]{entries: make(map[string]E), filePath: filePath, label: label}
	s.loadFromFile()
	return s
}

func (s *entityStore[E]) list() []E { return storeList(&s.mu, s.entries) }

func (s *entityStore[E]) get(id string) (E, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[id]
	return e, ok
}

func (s *entityStore[E]) create(e E) E {
	e.setID(generateID())
	now := time.Now().UTC()
	e.setTimestamps(now, now)
	s.mu.Lock()
	s.entries[e.getID()] = e
	s.mu.Unlock()
	safeGo(s.save)
	return e
}

func (s *entityStore[E]) put(e E) E {
	if e.getID() == "" {
		e.setID(generateID())
	}
	now := time.Now().UTC()
	s.mu.Lock()
	createdAt := e.getCreatedAt()
	if existing, ok := s.entries[e.getID()]; ok {
		createdAt = existing.getCreatedAt()
	}
	if createdAt.IsZero() {
		createdAt = now
	}
	e.setTimestamps(createdAt, now)
	s.entries[e.getID()] = e
	s.mu.Unlock()
	safeGo(s.save)
	return e
}

func (s *entityStore[E]) update(id string, patch E) (E, bool) {
	s.mu.Lock()
	existing, ok := s.entries[id]
	if !ok {
		s.mu.Unlock()
		var zero E
		return zero, false
	}
	patch.setID(existing.getID())
	patch.setTimestamps(existing.getCreatedAt(), time.Now().UTC())
	s.entries[id] = patch
	s.mu.Unlock()
	safeGo(s.save)
	return patch, true
}

func (s *entityStore[E]) delete(id string) bool {
	s.mu.Lock()
	_, ok := s.entries[id]
	if ok {
		delete(s.entries, id)
	}
	s.mu.Unlock()
	if ok {
		safeGo(s.save)
	}
	return ok
}

func (s *entityStore[E]) save() { storeSave(&s.mu, s.entries, s.filePath, s.label) }

func (s *entityStore[E]) loadFromFile() { storeLoad(&s.mu, s.entries, s.filePath, s.label) }

// listHandle returns a GET (list all) HTTP handler.
func (s *entityStore[E]) listHandle() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		entries := s.list()
		writeJSON(w, http.StatusOK, map[string]interface{}{"total": len(entries), "entries": entries})
	}
}

// createHandle returns a POST (create) HTTP handler.
// newFn must return a non-nil zero-value E suitable for JSON decoding.
func (s *entityStore[E]) createHandle(newFn func() E) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		e := newFn()
		if !decodeBody(w, r, e) {
			return
		}
		if msg := e.validate(); msg != "" {
			writeError(w, http.StatusBadRequest, msg)
			return
		}
		writeJSON(w, http.StatusCreated, s.create(e))
	}
}

// getHandle returns a GET /:id HTTP handler.
func (s *entityStore[E]) getHandle(label string) httprouter.Handle {
	notFound := label + " not found"
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		e, ok := s.get(ps.ByName("id"))
		if !ok {
			writeError(w, http.StatusNotFound, notFound)
			return
		}
		writeJSON(w, http.StatusOK, e)
	}
}

// updateHandle returns a PUT /:id HTTP handler.
func (s *entityStore[E]) updateHandle(label string, newFn func() E) httprouter.Handle {
	notFound := label + " not found"
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		id := ps.ByName("id")
		patch := newFn()
		if !decodeBody(w, r, patch) {
			return
		}
		if msg := patch.validate(); msg != "" {
			writeError(w, http.StatusBadRequest, msg)
			return
		}
		updated, ok := s.update(id, patch)
		if !ok {
			writeError(w, http.StatusNotFound, notFound)
			return
		}
		writeJSON(w, http.StatusOK, updated)
	}
}

// deleteHandle returns a DELETE /:id HTTP handler.
func (s *entityStore[E]) deleteHandle(label string) httprouter.Handle {
	notFound := label + " not found"
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		if !s.delete(ps.ByName("id")) {
			writeError(w, http.StatusNotFound, notFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// modifyFn fetches the entry by id, calls fn(e) to apply an in-place mutation
// (e.g. toggling a single field), updates the entity's UpdatedAt timestamp, and
// persists the result — all under the write lock to avoid races.
func (s *entityStore[E]) modifyFn(id string, fn func(E)) (E, bool) {
	s.mu.Lock()
	e, ok := s.entries[id]
	if !ok {
		s.mu.Unlock()
		var zero E
		return zero, false
	}
	fn(e)
	e.setTimestamps(e.getCreatedAt(), time.Now().UTC())
	s.mu.Unlock()
	safeGo(s.save)
	return e, true
}

// ─────────────────────────────────────────────────────────────────────────────
// Publisher CRUD
// ─────────────────────────────────────────────────────────────────────────────

// Publisher represents a supply-side publisher.
type Publisher struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Domain       string    `json:"domain"`
	ContactEmail string    `json:"contact_email,omitempty"`
	Status       string    `json:"status"` // "active" | "paused"
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (p *Publisher) validate() string {
	p.Name = strings.TrimSpace(p.Name)
	if p.Name == "" {
		return "name is required"
	}
	p.Domain = strings.TrimSpace(p.Domain)
	if p.Domain == "" {
		return "domain is required"
	}
	if p.Status != "active" && p.Status != "paused" {
		p.Status = "active"
	}
	return ""
}

func (p *Publisher) getID() string                { return p.ID }
func (p *Publisher) getCreatedAt() time.Time      { return p.CreatedAt }
func (p *Publisher) setID(id string)              { p.ID = id }
func (p *Publisher) setTimestamps(c, u time.Time) { p.CreatedAt = c; p.UpdatedAt = u }

type publisherStore = entityStore[*Publisher]

func newPublisherStore(fp string) *publisherStore {
	return newEntityStore[*Publisher](fp, "publishers")
}

// PublisherHandler handles CRUD for Publisher records.
type PublisherHandler struct{ store *publisherStore }

// NewPublisherHandler creates a PublisherHandler backed by a persistent store.
func NewPublisherHandler(dataDir string) *PublisherHandler {
	return &PublisherHandler{store: newPublisherStore(dataStorePath(dataDir, "publishers.json"))}
}

func (h *PublisherHandler) List() httprouter.Handle { return h.store.listHandle() }
func (h *PublisherHandler) Create() httprouter.Handle {
	return h.store.createHandle(func() *Publisher { return &Publisher{} })
}
func (h *PublisherHandler) Get() httprouter.Handle { return h.store.getHandle("publisher") }
func (h *PublisherHandler) Update() httprouter.Handle {
	return h.store.updateHandle("publisher", func() *Publisher { return &Publisher{} })
}
func (h *PublisherHandler) Delete() httprouter.Handle { return h.store.deleteHandle("publisher") }

func (h *PublisherHandler) Store() *publisherStore { return h.store }

// ─────────────────────────────────────────────────────────────────────────────
// Advertiser CRUD
// ─────────────────────────────────────────────────────────────────────────────

// Advertiser represents a demand-side advertiser.
type Advertiser struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Domain       string    `json:"domain"`
	Category     string    `json:"category,omitempty"`
	ContactEmail string    `json:"contact_email,omitempty"`
	Status       string    `json:"status"` // "active" | "paused"
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (a *Advertiser) validate() string {
	a.Name = strings.TrimSpace(a.Name)
	if a.Name == "" {
		return "name is required"
	}
	a.Domain = strings.TrimSpace(a.Domain)
	if a.Domain == "" {
		return "domain is required"
	}
	if a.Status != "active" && a.Status != "paused" {
		a.Status = "active"
	}
	return ""
}

func (a *Advertiser) getID() string                { return a.ID }
func (a *Advertiser) getCreatedAt() time.Time      { return a.CreatedAt }
func (a *Advertiser) setID(id string)              { a.ID = id }
func (a *Advertiser) setTimestamps(c, u time.Time) { a.CreatedAt = c; a.UpdatedAt = u }

type advertiserStore = entityStore[*Advertiser]

func newAdvertiserStore(fp string) *advertiserStore {
	return newEntityStore[*Advertiser](fp, "advertisers")
}

// AdvertiserHandler handles CRUD for Advertiser records.
type AdvertiserHandler struct{ store *advertiserStore }

// NewAdvertiserHandler creates an AdvertiserHandler backed by a persistent store.
func NewAdvertiserHandler(dataDir string) *AdvertiserHandler {
	return &AdvertiserHandler{store: newAdvertiserStore(dataStorePath(dataDir, "advertisers.json"))}
}

func (h *AdvertiserHandler) List() httprouter.Handle { return h.store.listHandle() }
func (h *AdvertiserHandler) Create() httprouter.Handle {
	return h.store.createHandle(func() *Advertiser { return &Advertiser{} })
}
func (h *AdvertiserHandler) Get() httprouter.Handle { return h.store.getHandle("advertiser") }
func (h *AdvertiserHandler) Update() httprouter.Handle {
	return h.store.updateHandle("advertiser", func() *Advertiser { return &Advertiser{} })
}
func (h *AdvertiserHandler) Delete() httprouter.Handle { return h.store.deleteHandle("advertiser") }

func (h *AdvertiserHandler) Store() *advertiserStore { return h.store }

// ─────────────────────────────────────────────────────────────────────────────
// Domain List CRUD
// ─────────────────────────────────────────────────────────────────────────────

// DomainList is a named collection of domains used for whitelist/blocklist filtering.
type DomainList struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	ListType  string    `json:"list_type"` // "whitelist" | "blocklist"
	Entries   []string  `json:"entries"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (d *DomainList) validate() string {
	d.Name = strings.TrimSpace(d.Name)
	if d.Name == "" {
		return "name is required"
	}
	if d.ListType != "whitelist" && d.ListType != "blocklist" {
		d.ListType = "blocklist"
	}
	return ""
}

func (d *DomainList) getID() string                { return d.ID }
func (d *DomainList) getCreatedAt() time.Time      { return d.CreatedAt }
func (d *DomainList) setID(id string)              { d.ID = id }
func (d *DomainList) setTimestamps(c, u time.Time) { d.CreatedAt = c; d.UpdatedAt = u }

type domainListStore = entityStore[*DomainList]

func newDomainListStore(fp string) *domainListStore {
	return newEntityStore[*DomainList](fp, "domain-lists")
}

// DomainListHandler handles CRUD for DomainList records.
type DomainListHandler struct{ store *domainListStore }

func NewDomainListHandler(dataDir string) *DomainListHandler {
	return &DomainListHandler{store: newDomainListStore(dataStorePath(dataDir, "domain_lists.json"))}
}

func (h *DomainListHandler) List() httprouter.Handle { return h.store.listHandle() }
func (h *DomainListHandler) Create() httprouter.Handle {
	return h.store.createHandle(func() *DomainList { return &DomainList{} })
}
func (h *DomainListHandler) Get() httprouter.Handle { return h.store.getHandle("domain list") }
func (h *DomainListHandler) Update() httprouter.Handle {
	return h.store.updateHandle("domain list", func() *DomainList { return &DomainList{} })
}
func (h *DomainListHandler) Delete() httprouter.Handle { return h.store.deleteHandle("domain list") }

// ─────────────────────────────────────────────────────────────────────────────
// Campaign CRUD
// ─────────────────────────────────────────────────────────────────────────────

// TargetingRule represents a single targeting criterion for a Campaign.
type TargetingRule struct {
	Type  string `json:"type"` // geo, device_type, os, app_bundle, domain, day_part
	Op    string `json:"op"`   // is, is_not, contains
	Value string `json:"value"`
}

// ExtStatsConfig holds the connection details for a third-party analytics API.
type ExtStatsConfig struct {
	Provider string `json:"provider,omitempty"` // human-readable label, e.g. "EliteBidder"
	URL      string `json:"url,omitempty"`      // base API URL
	APIKey   string `json:"api_key,omitempty"`
	IDParam  string `json:"id_param,omitempty"` // query param name for the placement/campaign ID
	IDValue  string `json:"id_value,omitempty"` // override value; empty = use record ID
	DateFrom string `json:"date_from,omitempty"`
	DateTo   string `json:"date_to,omitempty"`
}

// Campaign links an Advertiser to a set of third-party ad endpoints.
// VASTTagURL holds the third-party VAST tag URL (optional).
// OrtbEndpointURL holds the third-party OpenRTB endpoint URL (optional).
type Campaign struct {
	ID              string  `json:"id"`
	Name            string  `json:"name"`
	AdvertiserID    string  `json:"advertiser_id"`
	PublisherID     string  `json:"publisher_id,omitempty"`
	VASTTagURL      string  `json:"vast_tag_url,omitempty"`
	OrtbEndpointURL string  `json:"ortb_endpoint_url,omitempty"`
	FloorCPM        float64 `json:"floor_cpm"`
	Status          string  `json:"status"` // "active" | "paused"

	// Integration & OpenRTB settings
	IntegrationType  string   `json:"integration_type,omitempty"` // "tag_based"|"open_rtb"
	OrtbVersion      string   `json:"ortb_version,omitempty"`     // "2.5"|"2.6"|"3.0"
	MimeTypes        []string `json:"mime_types,omitempty"`
	Protocols        []int    `json:"protocols,omitempty"`          // IAB VAST protocol IDs (1–10)
	APIs             []int    `json:"apis,omitempty"`               // API frameworks accepted by the demand endpoint
	DemandTypes      []string `json:"demand_types,omitempty"`       // "video"|"display"|"audio"
	AuctionType      string   `json:"auction_type,omitempty"`       // always "open" for transparent open bidding
	AuctionPriceType string   `json:"auction_price_type,omitempty"` // "first"|"second"

	// Delivery caps
	ImpsDailyCap  int     `json:"imps_daily_cap,omitempty"`
	ImpsHourlyCap int     `json:"imps_hourly_cap,omitempty"`
	ReqsDailyCap  int     `json:"reqs_daily_cap,omitempty"`
	ReqsHourlyCap int     `json:"reqs_hourly_cap,omitempty"`
	OppsDailyCap  int     `json:"opps_daily_cap,omitempty"`
	OppsHourlyCap int     `json:"opps_hourly_cap,omitempty"`
	QPS           int     `json:"qps,omitempty"`
	RevDailyCap   float64 `json:"rev_daily_cap,omitempty"`

	// Targeting & filtering
	FilterImpressions string   `json:"filter_impressions,omitempty"`
	DomainFilterType  string   `json:"domain_filter_type,omitempty"` // "white"|"block"
	DomainLists       []string `json:"domain_lists,omitempty"`

	// Supply chain
	PassSchain bool `json:"pass_schain"`

	// Targeting rules
	TargetingRules []TargetingRule `json:"targeting_rules,omitempty"`
	// Supply links — ad unit IDs routed by this campaign
	SupplyLinks []string `json:"supply_links,omitempty"`

	// Automation & pacing
	BudgetTotal       float64 `json:"budget_total,omitempty"`
	BudgetDaily       float64 `json:"budget_daily,omitempty"`
	PacingType        string  `json:"pacing_type,omitempty"` // "even"|"accelerated"|"front_loaded"
	AutoBidding       bool    `json:"auto_bidding"`
	AutoBiddingGoal   string  `json:"auto_bidding_goal,omitempty"` // "vcr"|"cpm"|"cpcv"|"viewability"
	AutoBiddingTarget float64 `json:"auto_bidding_target,omitempty"`

	// Scheduling — ISO date strings (YYYY-MM-DD)
	StartDate       string   `json:"start_date,omitempty"`
	EndDate         string   `json:"end_date,omitempty"`
	DayPartSchedule []string `json:"day_part_schedule,omitempty"` // ["mon","wed","fri"]

	// Frequency cap & audience targeting
	FreqCapImps   int      `json:"freq_cap_imps,omitempty"`
	FreqCapPeriod string   `json:"freq_cap_period,omitempty"` // "hour"|"day"|"week"
	SegmentIDs    []string `json:"segment_ids,omitempty"`

	// Brand safety — blocked advertiser domains and IAB categories
	BAdv []string `json:"badv,omitempty"`
	BCat []string `json:"bcat,omitempty"`

	// External statistics API config
	ExtStats *ExtStatsConfig `json:"ext_stats,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (c *Campaign) validate() string {
	c.Name = strings.TrimSpace(c.Name)
	c.VASTTagURL = strings.TrimSpace(c.VASTTagURL)
	c.OrtbEndpointURL = strings.TrimSpace(c.OrtbEndpointURL)
	c.IntegrationType = normalizeIntegrationType(c.IntegrationType)
	c.AuctionType = "open"
	c.AuctionPriceType = normalizeAuctionPriceType(c.AuctionPriceType)
	if c.Name == "" {
		return "name is required"
	}
	if c.AdvertiserID == "" {
		return "advertiser_id is required"
	}
	if c.IntegrationType == "" {
		return "integration_type must be one of: tag_based, open_rtb"
	}
	if c.IntegrationType == "tag_based" && c.VASTTagURL == "" {
		return "vast_tag_url is required when integration_type is tag_based"
	}
	if c.IntegrationType == "open_rtb" && c.OrtbEndpointURL == "" {
		return "ortb_endpoint_url is required when integration_type is open_rtb"
	}
	if c.FloorCPM < 0 {
		return "floor_cpm must be >= 0"
	}
	if c.Status != "active" && c.Status != "paused" {
		c.Status = "active"
	}
	return ""
}

func (c *Campaign) getID() string                 { return c.ID }
func (c *Campaign) getCreatedAt() time.Time       { return c.CreatedAt }
func (c *Campaign) setID(id string)               { c.ID = id }
func (c *Campaign) setTimestamps(cr, u time.Time) { c.CreatedAt = cr; c.UpdatedAt = u }

type campaignStore = entityStore[*Campaign]

func newCampaignStore(fp string) *campaignStore {
	return newEntityStore[*Campaign](fp, "campaigns")
}

// CampaignHandler handles CRUD for Campaign records.
type CampaignHandler struct {
	store    *campaignStore
	onChange func() // called after any mutation to re-sync the video pipeline
}

// NewCampaignHandler creates a CampaignHandler backed by a persistent store.
func NewCampaignHandler(dataDir string) *CampaignHandler {
	return &CampaignHandler{store: newCampaignStore(dataStorePath(dataDir, "campaigns.json"))}
}

// Store exposes the underlying campaign store for inter-handler demand routing.
func (h *CampaignHandler) Store() *campaignStore { return h.store }

// SetOnChange registers a callback that is called after any campaign mutation.
func (h *CampaignHandler) SetOnChange(fn func()) { h.onChange = fn }

func (h *CampaignHandler) List() httprouter.Handle { return h.store.listHandle() }
func (h *CampaignHandler) Create() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		campaign := &Campaign{}
		if !decodeBody(w, r, campaign) {
			return
		}
		if msg := campaign.validate(); msg != "" {
			writeError(w, http.StatusBadRequest, msg)
			return
		}
		created := h.store.create(campaign)
		if h.onChange != nil {
			safeGo(h.onChange)
		}
		writeJSON(w, http.StatusCreated, created)
	}
}
func (h *CampaignHandler) Get() httprouter.Handle { return h.store.getHandle("campaign") }

func (h *CampaignHandler) Update() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		id := ps.ByName("id")
		patch := &Campaign{}
		if !decodeBody(w, r, patch) {
			return
		}
		if msg := patch.validate(); msg != "" {
			writeError(w, http.StatusBadRequest, msg)
			return
		}
		updated, ok := h.store.update(id, patch)
		if !ok {
			writeError(w, http.StatusNotFound, "campaign not found")
			return
		}
		if h.onChange != nil {
			safeGo(h.onChange)
		}
		writeJSON(w, http.StatusOK, updated)
	}
}

func (h *CampaignHandler) Delete() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		if !h.store.delete(ps.ByName("id")) {
			writeError(w, http.StatusNotFound, "campaign not found")
			return
		}
		if h.onChange != nil {
			safeGo(h.onChange)
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// ═════════════════════════════════════════════════════════════════════════════
// Audience Segment CRUD
// ═════════════════════════════════════════════════════════════════════════════

// SegmentType classifies the audience targeting methodology.
type SegmentType string

const (
	SegmentTypeDemographic SegmentType = "demographic"
	SegmentTypeBehavioral  SegmentType = "behavioral"
	SegmentTypeContextual  SegmentType = "contextual"
	SegmentTypeRetarget    SegmentType = "retarget"
	SegmentTypeLookalike   SegmentType = "lookalike"
)

// SegmentSource identifies the origin of the audience data.
type SegmentSource string

const (
	SegmentSourceFirstParty SegmentSource = "first_party"
	SegmentSourceCRM        SegmentSource = "crm"
	SegmentSourceThirdParty SegmentSource = "third_party"
)

// SegmentRule is a single targeting condition within an audience segment.
type SegmentRule struct {
	Attribute string `json:"attribute"` // e.g. age_range, gender, interest, device_type, url_pattern
	Op        string `json:"op"`        // is, is_not, contains, in, not_in
	Value     string `json:"value"`
}

// AudienceSegment represents a named, reusable audience built from first-party data
// or CRM synchronisation. Segments can be attached to Campaigns and YieldRules
// for precise targeting and privacy-conscious frequency capping.
type AudienceSegment struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	Type        SegmentType   `json:"type"`
	Source      SegmentSource `json:"source"`
	Description string        `json:"description,omitempty"`

	// CRM sync configuration
	CRMProvider string     `json:"crm_provider,omitempty"` // "salesforce"|"hubspot"|"klaviyo"|"custom"
	CRMEndpoint string     `json:"crm_endpoint,omitempty"`
	CRMAPIKey   string     `json:"crm_api_key,omitempty"`
	CRMSyncedAt *time.Time `json:"crm_synced_at,omitempty"`

	// Targeting rules
	Rules []SegmentRule `json:"rules,omitempty"`

	// Reach & frequency controls
	EstimatedReach int    `json:"estimated_reach,omitempty"` // unique users
	FreqCapImps    int    `json:"freq_cap_imps,omitempty"`   // max impressions per user
	FreqCapPeriod  string `json:"freq_cap_period,omitempty"` // "hour"|"day"|"week"|"month"

	// Privacy / consent
	ConsentRequired bool `json:"consent_required"`
	DataTTLDays     int  `json:"data_ttl_days,omitempty"`
	GDPRApplies     bool `json:"gdpr_applies"`
	CCPAApplies     bool `json:"ccpa_applies"`

	Status    string    `json:"status"` // "active"|"paused"
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (a *AudienceSegment) validate() string {
	a.Name = strings.TrimSpace(a.Name)
	if a.Name == "" {
		return "name is required"
	}
	switch a.Type {
	case SegmentTypeDemographic, SegmentTypeBehavioral, SegmentTypeContextual,
		SegmentTypeRetarget, SegmentTypeLookalike:
	default:
		a.Type = SegmentTypeBehavioral
	}
	switch a.Source {
	case SegmentSourceFirstParty, SegmentSourceCRM, SegmentSourceThirdParty:
	default:
		a.Source = SegmentSourceFirstParty
	}
	if a.Status != "active" && a.Status != "paused" {
		a.Status = "active"
	}
	return ""
}

func (a *AudienceSegment) getID() string                { return a.ID }
func (a *AudienceSegment) getCreatedAt() time.Time      { return a.CreatedAt }
func (a *AudienceSegment) setID(id string)              { a.ID = id }
func (a *AudienceSegment) setTimestamps(c, u time.Time) { a.CreatedAt = c; a.UpdatedAt = u }

type audienceSegmentStore = entityStore[*AudienceSegment]

func newAudienceSegmentStore(filePath string) *audienceSegmentStore {
	return newEntityStore[*AudienceSegment](filePath, "audience-segments")
}

// AudienceSegmentHandler handles CRUD for AudienceSegment records.
type AudienceSegmentHandler struct{ store *audienceSegmentStore }

// NewAudienceSegmentHandler creates an AudienceSegmentHandler backed by a persistent store.
func NewAudienceSegmentHandler(dataDir string) *AudienceSegmentHandler {
	return &AudienceSegmentHandler{store: newAudienceSegmentStore(dataStorePath(dataDir, "audience_segments.json"))}
}

func (h *AudienceSegmentHandler) List() httprouter.Handle { return h.store.listHandle() }
func (h *AudienceSegmentHandler) Create() httprouter.Handle {
	return h.store.createHandle(func() *AudienceSegment { return &AudienceSegment{} })
}
func (h *AudienceSegmentHandler) Get() httprouter.Handle {
	return h.store.getHandle("audience segment")
}
func (h *AudienceSegmentHandler) Update() httprouter.Handle {
	return h.store.updateHandle("audience segment", func() *AudienceSegment { return &AudienceSegment{} })
}
func (h *AudienceSegmentHandler) Delete() httprouter.Handle {
	return h.store.deleteHandle("audience segment")
}

// ═════════════════════════════════════════════════════════════════════════════
// Yield Rule CRUD
// ═════════════════════════════════════════════════════════════════════════════

// YieldPriority defines the serving tier for a yield rule.
type YieldPriority string

const (
	YieldPriorityProgrammatic YieldPriority = "programmatic" // open auction / header bidding
	YieldPriorityHouseAd      YieldPriority = "house"        // fallback / self-promo
)

// YieldRule expresses a floor CPM, priority tier, quality thresholds, and an optional
// audience scope that the ad server uses when selecting demand for an impression.
// It is the canonical mechanism for per-impression yield optimisation on the platform.
type YieldRule struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	PublisherID string        `json:"publisher_id,omitempty"`
	AdUnitID    string        `json:"ad_unit_id,omitempty"`
	Priority    YieldPriority `json:"priority"`

	// CPM controls
	FloorCPM  float64 `json:"floor_cpm"`
	TargetCPM float64 `json:"target_cpm,omitempty"` // soft optimisation target

	// Waterfall / auction controls
	WaterfallPos int    `json:"waterfall_pos,omitempty"`
	AuctionType  string `json:"auction_type,omitempty"` // "first_price"|"second_price"

	// Quality thresholds — only serve when signals meet or exceed these
	ViewabilityThreshold float64 `json:"viewability_threshold,omitempty"` // 0–100 %
	VCRThreshold         float64 `json:"vcr_threshold,omitempty"`         // 0–100 %

	// Audience segment scoping (empty = all traffic)
	SegmentIDs []string `json:"segment_ids,omitempty"`

	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (y *YieldRule) validate() string {
	y.Name = strings.TrimSpace(y.Name)
	y.Priority = normalizeYieldPriority(y.Priority)
	y.AuctionType = normalizeAuctionPriceType(y.AuctionType) + "_price"
	if y.Name == "" {
		return "name is required"
	}
	if y.FloorCPM < 0 {
		return "floor_cpm must be >= 0"
	}
	if y.ViewabilityThreshold < 0 || y.ViewabilityThreshold > 100 {
		return "viewability_threshold must be between 0 and 100"
	}
	if y.VCRThreshold < 0 || y.VCRThreshold > 100 {
		return "vcr_threshold must be between 0 and 100"
	}
	return ""
}

func (y *YieldRule) getID() string                { return y.ID }
func (y *YieldRule) getCreatedAt() time.Time      { return y.CreatedAt }
func (y *YieldRule) setID(id string)              { y.ID = id }
func (y *YieldRule) setTimestamps(c, u time.Time) { y.CreatedAt = c; y.UpdatedAt = u }

type yieldRuleStore = entityStore[*YieldRule]

func newYieldRuleStore(filePath string) *yieldRuleStore {
	return newEntityStore[*YieldRule](filePath, "yield-rules")
}

// YieldRuleHandler handles CRUD for YieldRule records.
type YieldRuleHandler struct{ store *yieldRuleStore }

// NewYieldRuleHandler creates a YieldRuleHandler backed by a persistent store.
func NewYieldRuleHandler(dataDir string) *YieldRuleHandler {
	return &YieldRuleHandler{store: newYieldRuleStore(dataStorePath(dataDir, "yield_rules.json"))}
}

func (h *YieldRuleHandler) List() httprouter.Handle { return h.store.listHandle() }
func (h *YieldRuleHandler) Create() httprouter.Handle {
	return h.store.createHandle(func() *YieldRule { return &YieldRule{} })
}
func (h *YieldRuleHandler) Get() httprouter.Handle { return h.store.getHandle("yield rule") }
func (h *YieldRuleHandler) Update() httprouter.Handle {
	return h.store.updateHandle("yield rule", func() *YieldRule { return &YieldRule{} })
}
func (h *YieldRuleHandler) Delete() httprouter.Handle { return h.store.deleteHandle("yield rule") }

// ═════════════════════════════════════════════════════════════════════════════
// Bidder Scorecard
// Per-bidder quality and reliability tracking for demand routing decisions.
// Score = bid_price × render_success × completion × (1-discrepancy)
// ═════════════════════════════════════════════════════════════════════════════

// BidderScorecard tracks per-bidder reliability metrics used by the
// optimization engine to route high-value inventory to the best-performing demand.
type BidderScorecard struct {
	ID         string `json:"id"`
	BidderName string `json:"bidder_name"`
	// Rate fields are 0–1 fractions
	BidRate           float64    `json:"bid_rate"`            // fraction of requests that received a bid
	WinRate           float64    `json:"win_rate"`            // fraction of bids that won
	RenderSuccessRate float64    `json:"render_success_rate"` // fraction of wins that rendered
	VASTErrorRate     float64    `json:"vast_error_rate"`     // fraction of renders with VAST errors
	TimeoutRate       float64    `json:"timeout_rate"`        // fraction of requests that timed out
	CompletionRate    float64    `json:"completion_rate"`     // fraction of started views that completed
	DiscrepancyRate   float64    `json:"discrepancy_rate"`    // billing vs ad-server count mismatch
	AvgBidCPM         float64    `json:"avg_bid_cpm"`
	EffectiveCPM      float64    `json:"effective_cpm"`  // avg_bid_cpm × (1 - discrepancy_rate)
	ExpectedValue     float64    `json:"expected_value"` // bid × render × completion × (1-discrepancy)
	AvgWrapperDepth   float64    `json:"avg_wrapper_depth"`
	SuppressedUntil   *time.Time `json:"suppressed_until,omitempty"`
	Active            bool       `json:"active"`
	Notes             string     `json:"notes,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

func (b *BidderScorecard) getID() string                { return b.ID }
func (b *BidderScorecard) getCreatedAt() time.Time      { return b.CreatedAt }
func (b *BidderScorecard) setID(id string)              { b.ID = id }
func (b *BidderScorecard) setTimestamps(c, u time.Time) { b.CreatedAt = c; b.UpdatedAt = u }

func (b *BidderScorecard) validate() string {
	b.BidderName = strings.TrimSpace(b.BidderName)
	if b.BidderName == "" {
		return "bidder_name is required"
	}
	for _, f := range []struct {
		v float64
		n string
	}{
		{b.BidRate, "bid_rate"}, {b.WinRate, "win_rate"},
		{b.RenderSuccessRate, "render_success_rate"}, {b.VASTErrorRate, "vast_error_rate"},
		{b.TimeoutRate, "timeout_rate"}, {b.CompletionRate, "completion_rate"},
		{b.DiscrepancyRate, "discrepancy_rate"},
	} {
		if f.v < 0 || f.v > 1 {
			return f.n + " must be between 0 and 1"
		}
	}
	if b.AvgBidCPM < 0 {
		return "avg_bid_cpm must be >= 0"
	}
	return ""
}

type bidderScorecardStore = entityStore[*BidderScorecard]

func newBidderScorecardStore(fp string) *bidderScorecardStore {
	return newEntityStore[*BidderScorecard](fp, "bidder-scorecards")
}

// BidderScorecardHandler handles CRUD for BidderScorecard records.
type BidderScorecardHandler struct{ store *bidderScorecardStore }

// NewBidderScorecardHandler creates a BidderScorecardHandler backed by a persistent store.
func NewBidderScorecardHandler(dataDir string) *BidderScorecardHandler {
	return &BidderScorecardHandler{store: newBidderScorecardStore(dataStorePath(dataDir, "bidder_scorecards.json"))}
}

func (h *BidderScorecardHandler) List() httprouter.Handle { return h.store.listHandle() }
func (h *BidderScorecardHandler) Create() httprouter.Handle {
	return h.store.createHandle(func() *BidderScorecard { return &BidderScorecard{} })
}
func (h *BidderScorecardHandler) Get() httprouter.Handle {
	return h.store.getHandle("bidder scorecard")
}
func (h *BidderScorecardHandler) Update() httprouter.Handle {
	return h.store.updateHandle("bidder scorecard", func() *BidderScorecard { return &BidderScorecard{} })
}
func (h *BidderScorecardHandler) Delete() httprouter.Handle {
	return h.store.deleteHandle("bidder scorecard")
}

// ═════════════════════════════════════════════════════════════════════════════
// Dynamic Floor Rule
// Per-segment floor prices with optional auto-adjustment bands.
// ═════════════════════════════════════════════════════════════════════════════

// FloorSegment identifies the inventory segment that a DynamicFloorRule applies to.
// Use "*" or leave empty to match any value.
type FloorSegment struct {
	Env          string `json:"env,omitempty"`           // "ctv"|"inapp"|"*"
	GeoCountry   string `json:"geo_country,omitempty"`   // ISO-3166-1 alpha-2 or "*"
	DeviceType   string `json:"device_type,omitempty"`   // "ctv"|"mobile"|"desktop"|"tablet"|"*"
	ContentGenre string `json:"content_genre,omitempty"` // IAB genre or "*"
	SlotInPod    int    `json:"slot_in_pod,omitempty"`   // 0=any, 1=first, −1=last
	DaypartHour  int    `json:"daypart_hour"`            // 0–23; −1 = any hour
}

// DynamicFloorRule defines a programmatic floor and optional auto-adjustment
// policy for a specific inventory segment.
type DynamicFloorRule struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	PublisherID string       `json:"publisher_id,omitempty"`
	AdUnitID    string       `json:"ad_unit_id,omitempty"`
	Segment     FloorSegment `json:"segment"`
	// Floor CPM values in USD
	BaseFloor float64 `json:"base_floor"`
	SoftFloor float64 `json:"soft_floor,omitempty"` // fallback if hard floor yields no bids
	MinFloor  float64 `json:"min_floor,omitempty"`
	MaxFloor  float64 `json:"max_floor,omitempty"`
	// Auto-adjustment — raise/lower floor by FloorStepPct each cycle to hit TargetFillRate
	AutoAdjust     bool    `json:"auto_adjust"`
	TargetFillRate float64 `json:"target_fill_rate,omitempty"` // 0–1, e.g. 0.85
	FloorStepPct   float64 `json:"floor_step_pct,omitempty"`   // % change per cycle, e.g. 5.0
	// Demand path scope: "open_auction"|"*"
	DemandPath string    `json:"demand_path,omitempty"`
	Priority   int       `json:"priority"` // higher value wins when segments overlap
	Active     bool      `json:"active"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func (d *DynamicFloorRule) getID() string                { return d.ID }
func (d *DynamicFloorRule) getCreatedAt() time.Time      { return d.CreatedAt }
func (d *DynamicFloorRule) setID(id string)              { d.ID = id }
func (d *DynamicFloorRule) setTimestamps(c, u time.Time) { d.CreatedAt = c; d.UpdatedAt = u }

func (d *DynamicFloorRule) validate() string {
	d.Name = strings.TrimSpace(d.Name)
	d.DemandPath = normalizeDemandPath(d.DemandPath)
	if d.Name == "" {
		return "name is required"
	}
	if d.BaseFloor < 0 {
		return "base_floor must be >= 0"
	}
	if d.MinFloor < 0 {
		return "min_floor must be >= 0"
	}
	if d.MaxFloor < 0 {
		return "max_floor must be >= 0"
	}
	if d.MaxFloor > 0 && d.MinFloor > d.MaxFloor {
		return "min_floor must be <= max_floor"
	}
	if d.AutoAdjust && (d.TargetFillRate < 0 || d.TargetFillRate > 1) {
		return "target_fill_rate must be between 0 and 1 when auto_adjust is enabled"
	}
	if d.AutoAdjust && d.FloorStepPct <= 0 {
		return "floor_step_pct must be > 0 when auto_adjust is enabled"
	}
	return ""
}

type dynamicFloorRuleStore = entityStore[*DynamicFloorRule]

func newDynamicFloorRuleStore(fp string) *dynamicFloorRuleStore {
	return newEntityStore[*DynamicFloorRule](fp, "dynamic-floor-rules")
}

// DynamicFloorRuleHandler handles CRUD for DynamicFloorRule records.
type DynamicFloorRuleHandler struct{ store *dynamicFloorRuleStore }

// NewDynamicFloorRuleHandler creates a DynamicFloorRuleHandler backed by a persistent store.
func NewDynamicFloorRuleHandler(dataDir string) *DynamicFloorRuleHandler {
	return &DynamicFloorRuleHandler{store: newDynamicFloorRuleStore(dataStorePath(dataDir, "dynamic_floor_rules.json"))}
}

func (h *DynamicFloorRuleHandler) List() httprouter.Handle { return h.store.listHandle() }
func (h *DynamicFloorRuleHandler) Create() httprouter.Handle {
	return h.store.createHandle(func() *DynamicFloorRule { return &DynamicFloorRule{} })
}
func (h *DynamicFloorRuleHandler) Get() httprouter.Handle {
	return h.store.getHandle("dynamic floor rule")
}
func (h *DynamicFloorRuleHandler) Update() httprouter.Handle {
	return h.store.updateHandle("dynamic floor rule", func() *DynamicFloorRule { return &DynamicFloorRule{} })
}
func (h *DynamicFloorRuleHandler) Delete() httprouter.Handle {
	return h.store.deleteHandle("dynamic floor rule")
}

// ═════════════════════════════════════════════════════════════════════════════
// Request QA Profile
// Per-publisher validation rules that reject or downgrade malformed requests
// before they reach the auction engine.
// ═════════════════════════════════════════════════════════════════════════════

// QARequiredField defines a single field that the QA profile enforces.
type QARequiredField struct {
	Field     string `json:"field"`               // e.g. "app.bundle", "device.ifa"
	Condition string `json:"condition,omitempty"` // "always"|"ctv_only"|"inapp_only"
	Action    string `json:"action"`              // "reject"|"downgrade"|"warn"
}

// RequestQAProfile specifies the metadata quality gates for a publisher or
// environment. Requests that fail these checks are rejected or CPM-penalised
// before entering the auction.
type RequestQAProfile struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	PublisherID string `json:"publisher_id,omitempty"`
	Environment string `json:"environment,omitempty"` // "ctv"|"inapp"|"*"
	// Required field rules
	RequiredFields []QARequiredField `json:"required_fields,omitempty"`
	// Technical quality gates
	MaxWrapperDepth  int     `json:"max_wrapper_depth,omitempty"` // e.g. 3
	MinFloorCPM      float64 `json:"min_floor_cpm,omitempty"`
	MaxBidderTimeout int     `json:"max_bidder_timeout_ms,omitempty"`
	// Brand safety
	BlockedCategories  []string `json:"blocked_categories,omitempty"`  // IAB content categories
	BlockedAdvertisers []string `json:"blocked_advertisers,omitempty"` // domain list
	// Privacy & supply chain compliance
	RequireConsent   bool `json:"require_consent"`
	RequireAppAdsTxt bool `json:"require_app_ads_txt"`
	RequireSChain    bool `json:"require_schain"`
	// Operational
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (q *RequestQAProfile) getID() string                { return q.ID }
func (q *RequestQAProfile) getCreatedAt() time.Time      { return q.CreatedAt }
func (q *RequestQAProfile) setID(id string)              { q.ID = id }
func (q *RequestQAProfile) setTimestamps(c, u time.Time) { q.CreatedAt = c; q.UpdatedAt = u }

func (q *RequestQAProfile) validate() string {
	q.Name = strings.TrimSpace(q.Name)
	if q.Name == "" {
		return "name is required"
	}
	if q.MaxWrapperDepth < 0 {
		return "max_wrapper_depth must be >= 0"
	}
	if q.MinFloorCPM < 0 {
		return "min_floor_cpm must be >= 0"
	}
	for i, f := range q.RequiredFields {
		if strings.TrimSpace(f.Field) == "" {
			return "required_fields[" + strings.TrimSpace(strings.Repeat("0", i)) + "].field cannot be empty"
		}
		switch f.Action {
		case "reject", "downgrade", "warn":
		default:
			return "required_fields action must be 'reject', 'downgrade', or 'warn'"
		}
	}
	return ""
}

type requestQAProfileStore = entityStore[*RequestQAProfile]

func newRequestQAProfileStore(fp string) *requestQAProfileStore {
	return newEntityStore[*RequestQAProfile](fp, "request-qa-profiles")
}

// RequestQAProfileHandler handles CRUD for RequestQAProfile records.
type RequestQAProfileHandler struct{ store *requestQAProfileStore }

// NewRequestQAProfileHandler creates a RequestQAProfileHandler backed by a persistent store.
func NewRequestQAProfileHandler(dataDir string) *RequestQAProfileHandler {
	return &RequestQAProfileHandler{store: newRequestQAProfileStore(dataStorePath(dataDir, "request_qa_profiles.json"))}
}

func (h *RequestQAProfileHandler) List() httprouter.Handle { return h.store.listHandle() }
func (h *RequestQAProfileHandler) Create() httprouter.Handle {
	return h.store.createHandle(func() *RequestQAProfile { return &RequestQAProfile{} })
}
func (h *RequestQAProfileHandler) Get() httprouter.Handle {
	return h.store.getHandle("request QA profile")
}
func (h *RequestQAProfileHandler) Update() httprouter.Handle {
	return h.store.updateHandle("request QA profile", func() *RequestQAProfile { return &RequestQAProfile{} })
}
func (h *RequestQAProfileHandler) Delete() httprouter.Handle {
	return h.store.deleteHandle("request QA profile")
}

// ═════════════════════════════════════════════════════════════════════════════
// Timeout Profile
// Per-environment timeout budgets to balance fill rate vs latency.
// ═════════════════════════════════════════════════════════════════════════════

// TimeoutProfile defines the time budget for a specific inventory environment.
// Separate profiles allow tighter deadlines for banner/mobile while allowing
// more headroom for longer or higher-latency CTV pods.
type TimeoutProfile struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// Environment — one of: "ctv_client"|"mobile_client"|"ssai"|"longform_pod"|"default"
	Environment     string `json:"environment"`
	TotalBudgetMS   int    `json:"total_budget_ms"`   // wall-clock deadline for the full auction
	BidderTimeoutMS int    `json:"bidder_timeout_ms"` // max time to wait for bidder responses
	VASTTimeoutMS   int    `json:"vast_timeout_ms"`   // per VAST wrapper resolve step
	NoBidGraceMS    int    `json:"no_bid_grace_ms"`   // extra grace after first bid arrives
	// Auto-scaling — multiply total_budget_ms by premium_multiplier for peak traffic windows
	AutoScale         bool    `json:"auto_scale"`
	PremiumMultiplier float64 `json:"premium_multiplier,omitempty"` // 1.0–2.0×
	// Per-bidder timeout overrides (bidder name → timeout ms)
	BidderOverrides map[string]int `json:"bidder_overrides,omitempty"`
	Active          bool           `json:"active"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
}

func (t *TimeoutProfile) getID() string                { return t.ID }
func (t *TimeoutProfile) getCreatedAt() time.Time      { return t.CreatedAt }
func (t *TimeoutProfile) setID(id string)              { t.ID = id }
func (t *TimeoutProfile) setTimestamps(c, u time.Time) { t.CreatedAt = c; t.UpdatedAt = u }

func (t *TimeoutProfile) validate() string {
	t.Name = strings.TrimSpace(t.Name)
	if t.Name == "" {
		return "name is required"
	}
	validEnvs := map[string]bool{
		"ctv_client": true, "mobile_client": true, "ssai": true,
		"longform_pod": true, "default": true,
	}
	if t.Environment != "" && !validEnvs[t.Environment] {
		return "environment must be one of: ctv_client, mobile_client, ssai, longform_pod, default"
	}
	if t.TotalBudgetMS <= 0 {
		return "total_budget_ms must be > 0"
	}
	if t.BidderTimeoutMS <= 0 {
		return "bidder_timeout_ms must be > 0"
	}
	if t.BidderTimeoutMS > t.TotalBudgetMS {
		return "bidder_timeout_ms must be <= total_budget_ms"
	}
	if t.AutoScale && (t.PremiumMultiplier < 1.0 || t.PremiumMultiplier > 3.0) {
		return "premium_multiplier must be between 1.0 and 3.0"
	}
	return ""
}

type timeoutProfileStore = entityStore[*TimeoutProfile]

func newTimeoutProfileStore(fp string) *timeoutProfileStore {
	return newEntityStore[*TimeoutProfile](fp, "timeout-profiles")
}

// TimeoutProfileHandler handles CRUD for TimeoutProfile records.
type TimeoutProfileHandler struct{ store *timeoutProfileStore }

// NewTimeoutProfileHandler creates a TimeoutProfileHandler backed by a persistent store.
func NewTimeoutProfileHandler(dataDir string) *TimeoutProfileHandler {
	return &TimeoutProfileHandler{store: newTimeoutProfileStore(dataStorePath(dataDir, "timeout_profiles.json"))}
}

func (h *TimeoutProfileHandler) List() httprouter.Handle { return h.store.listHandle() }
func (h *TimeoutProfileHandler) Create() httprouter.Handle {
	return h.store.createHandle(func() *TimeoutProfile { return &TimeoutProfile{} })
}
func (h *TimeoutProfileHandler) Get() httprouter.Handle { return h.store.getHandle("timeout profile") }
func (h *TimeoutProfileHandler) Update() httprouter.Handle {
	return h.store.updateHandle("timeout profile", func() *TimeoutProfile { return &TimeoutProfile{} })
}
func (h *TimeoutProfileHandler) Delete() httprouter.Handle {
	return h.store.deleteHandle("timeout profile")
}

// ═════════════════════════════════════════════════════════════════════════════
// Pod Optimization Rule
// CTV-specific pod-level auction and competitive-separation controls.
// ═════════════════════════════════════════════════════════════════════════════

// PodSlotValue sets a relative value multiplier for a specific slot within
// a CTV ad pod (e.g. first slot at 1.3× floor weight).
type PodSlotValue struct {
	SlotIndex       int     `json:"slot_index"`       // 0-based; −1 = last slot
	ValueMultiplier float64 `json:"value_multiplier"` // relative floor multiplier, e.g. 1.3
}

// PodOptimizationRule controls how CTV ad pods are filled including slot
// pricing, competitive separation, and overall yield goal.
type PodOptimizationRule struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	PublisherID string `json:"publisher_id,omitempty"`
	AdUnitID    string `json:"ad_unit_id,omitempty"`
	// Pod structure constraints
	MaxPodDurationSec  int `json:"max_pod_duration_sec"`
	MaxSlotsPerPod     int `json:"max_slots_per_pod"`
	MinSlotDurationSec int `json:"min_slot_duration_sec,omitempty"`
	MaxSlotDurationSec int `json:"max_slot_duration_sec,omitempty"`
	// Slot-level value multipliers
	FirstSlotPremium float64        `json:"first_slot_premium,omitempty"` // e.g. 1.3 = 30% weight increase
	LastSlotPremium  float64        `json:"last_slot_premium,omitempty"`
	SlotValues       []PodSlotValue `json:"slot_values,omitempty"`
	// Competitive separation
	CompSepCategories  []string `json:"comp_sep_categories,omitempty"`  // IAB content categories
	CompSepAdvertisers []string `json:"comp_sep_advertisers,omitempty"` // domain list
	// Pod-level optimization goal
	// "total_yield"|"fill_rate"|"vcr"|"user_experience"
	OptimizeFor string    `json:"optimize_for"`
	MinFillPct  float64   `json:"min_fill_pct,omitempty"` // minimum pod fill % target (0–100)
	Active      bool      `json:"active"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (p *PodOptimizationRule) getID() string                { return p.ID }
func (p *PodOptimizationRule) getCreatedAt() time.Time      { return p.CreatedAt }
func (p *PodOptimizationRule) setID(id string)              { p.ID = id }
func (p *PodOptimizationRule) setTimestamps(c, u time.Time) { p.CreatedAt = c; p.UpdatedAt = u }

func (p *PodOptimizationRule) validate() string {
	p.Name = strings.TrimSpace(p.Name)
	if p.Name == "" {
		return "name is required"
	}
	if p.MaxPodDurationSec <= 0 {
		return "max_pod_duration_sec must be > 0"
	}
	if p.MaxSlotsPerPod <= 0 {
		return "max_slots_per_pod must be > 0"
	}
	if p.MinSlotDurationSec < 0 {
		return "min_slot_duration_sec must be >= 0"
	}
	if p.MaxSlotDurationSec < 0 {
		return "max_slot_duration_sec must be >= 0"
	}
	if p.MaxSlotDurationSec > 0 && p.MinSlotDurationSec > p.MaxSlotDurationSec {
		return "min_slot_duration_sec must be <= max_slot_duration_sec"
	}
	if p.MinFillPct < 0 || p.MinFillPct > 100 {
		return "min_fill_pct must be between 0 and 100"
	}
	validGoals := map[string]bool{
		"total_yield": true, "fill_rate": true, "vcr": true, "user_experience": true, "": true,
	}
	if !validGoals[p.OptimizeFor] {
		return "optimize_for must be one of: total_yield, fill_rate, vcr, user_experience"
	}
	return ""
}

type podOptimizationRuleStore = entityStore[*PodOptimizationRule]

func newPodOptimizationRuleStore(fp string) *podOptimizationRuleStore {
	return newEntityStore[*PodOptimizationRule](fp, "pod-optimization-rules")
}

// PodOptimizationRuleHandler handles CRUD for PodOptimizationRule records.
type PodOptimizationRuleHandler struct{ store *podOptimizationRuleStore }

// NewPodOptimizationRuleHandler creates a PodOptimizationRuleHandler backed by a persistent store.
func NewPodOptimizationRuleHandler(dataDir string) *PodOptimizationRuleHandler {
	return &PodOptimizationRuleHandler{store: newPodOptimizationRuleStore(dataStorePath(dataDir, "pod_optimization_rules.json"))}
}

func (h *PodOptimizationRuleHandler) List() httprouter.Handle { return h.store.listHandle() }
func (h *PodOptimizationRuleHandler) Create() httprouter.Handle {
	return h.store.createHandle(func() *PodOptimizationRule { return &PodOptimizationRule{} })
}
func (h *PodOptimizationRuleHandler) Get() httprouter.Handle {
	return h.store.getHandle("pod optimization rule")
}
func (h *PodOptimizationRuleHandler) Update() httprouter.Handle {
	return h.store.updateHandle("pod optimization rule", func() *PodOptimizationRule { return &PodOptimizationRule{} })
}
func (h *PodOptimizationRuleHandler) Delete() httprouter.Handle {
	return h.store.deleteHandle("pod optimization rule")
}

// ═════════════════════════════════════════════════════════════════════════════
// Supply Partner CRUD
// Represents a supply-side publisher / SSP integration together with its
// operational metrics (opportunities, impressions, revenue, QPS, VCR,
// viewability). Metrics fields are updated by the serving / billing pipeline;
// the create/edit form only requires name, delivery_status, and active.
// ═════════════════════════════════════════════════════════════════════════════

// SupplyPartner is the canonical record for a supply-side revenue console entity.
type SupplyPartner struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// DeliveryStatus: "Live" | "Limited" | "Paused" | "Archived"
	DeliveryStatus string `json:"delivery_status"`
	Active         bool   `json:"active"`

	// Inventory & revenue metrics — written by the serving / billing pipeline.
	Opportunities   int64   `json:"opportunities"`
	GrossRevenue    float64 `json:"gross_revenue"`
	AvgQpsYesterday int64   `json:"avg_qps_yesterday"`
	AvgQpsLastHour  int64   `json:"avg_qps_last_hour"`
	Impressions     int64   `json:"impressions"`
	PublisherPayout float64 `json:"publisher_payout"`
	// VCR / Viewability raw counts — ratios are derived in the UI.
	Completions         int64 `json:"completions"`
	ViewableImpressions int64 `json:"viewable_impressions"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (s *SupplyPartner) getID() string                { return s.ID }
func (s *SupplyPartner) getCreatedAt() time.Time      { return s.CreatedAt }
func (s *SupplyPartner) setID(id string)              { s.ID = id }
func (s *SupplyPartner) setTimestamps(c, u time.Time) { s.CreatedAt = c; s.UpdatedAt = u }

func (s *SupplyPartner) validate() string {
	s.Name = strings.TrimSpace(s.Name)
	if s.Name == "" {
		return "name is required"
	}
	switch s.DeliveryStatus {
	case "Live", "Limited", "Paused", "Archived":
	default:
		s.DeliveryStatus = "Live"
	}
	if s.Opportunities < 0 {
		return "opportunities must be >= 0"
	}
	if s.GrossRevenue < 0 {
		return "gross_revenue must be >= 0"
	}
	if s.Impressions < 0 {
		return "impressions must be >= 0"
	}
	if s.PublisherPayout < 0 {
		return "publisher_payout must be >= 0"
	}
	if s.Completions < 0 {
		return "completions must be >= 0"
	}
	if s.ViewableImpressions < 0 {
		return "viewable_impressions must be >= 0"
	}
	return ""
}

type supplyPartnerStore = entityStore[*SupplyPartner]

func newSupplyPartnerStore(fp string) *supplyPartnerStore {
	return newEntityStore[*SupplyPartner](fp, "supply-partners")
}

// SupplyPartnerHandler manages CRUD + active-toggle for SupplyPartner records.
type SupplyPartnerHandler struct {
	store         *supplyPartnerStore
	statsProvider func() VideoStatsPayload // injected by WireVideoStats; may be nil
	publisherList func() []*Publisher
}

// NewSupplyPartnerHandler creates a SupplyPartnerHandler backed by a persistent store.
func NewSupplyPartnerHandler(dataDir string) *SupplyPartnerHandler {
	return &SupplyPartnerHandler{store: newSupplyPartnerStore(dataStorePath(dataDir, "supply_partners.json"))}
}

// SetStatsProvider injects a live-stats snapshot function so that List() can
// overlay real-time metrics (opportunities, revenue, impressions, QPS, …) onto
// each supply partner record keyed by partner ID == publisher_id.
func (h *SupplyPartnerHandler) SetStatsProvider(fn func() VideoStatsPayload) { h.statsProvider = fn }

func (h *SupplyPartnerHandler) SetPublisherListProvider(fn func() []*Publisher) { h.publisherList = fn }

func statsWindowSeconds(startedAt int64) (dayWindowSeconds, hourWindowSeconds int64) {
	nowUnix := time.Now().Unix()
	uptimeSeconds := nowUnix - startedAt
	if uptimeSeconds < 1 {
		uptimeSeconds = 1
	}
	dayWindowSeconds = uptimeSeconds
	if dayWindowSeconds > 86400 {
		dayWindowSeconds = 86400
	}
	hourWindowSeconds = uptimeSeconds
	if hourWindowSeconds > 3600 {
		hourWindowSeconds = 3600
	}
	return dayWindowSeconds, hourWindowSeconds
}

func qpsFromRecentRequests(recentRequests map[string]int64, entityID string, recentWindowSeconds, fallbackRequests, fallbackWindowSeconds int64) int64 {
	if recentRequests != nil {
		if recent, ok := recentRequests[entityID]; ok {
			if recentWindowSeconds < 1 {
				recentWindowSeconds = 1
			}
			return recent / recentWindowSeconds
		}
	}
	if fallbackWindowSeconds < 1 {
		fallbackWindowSeconds = 1
	}
	return fallbackRequests / fallbackWindowSeconds
}

func deliveryStatusFromMasterStatus(status string) string {
	if strings.EqualFold(strings.TrimSpace(status), "paused") {
		return "Paused"
	}
	return "Live"
}

func (h *SupplyPartnerHandler) listEntries() []*SupplyPartner {
	entries := h.store.list()
	if h.publisherList == nil {
		return entries
	}
	publishers := h.publisherList()
	if len(publishers) == 0 {
		return entries
	}
	entriesByID := make(map[string]*SupplyPartner, len(entries))
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		copyEntry := *entry
		entriesByID[entry.ID] = &copyEntry
	}
	merged := make([]*SupplyPartner, 0, len(publishers)+len(entries))
	seen := make(map[string]struct{}, len(publishers))
	for _, publisher := range publishers {
		if publisher == nil {
			continue
		}
		entry := &SupplyPartner{}
		if existing := entriesByID[publisher.ID]; existing != nil {
			*entry = *existing
		}
		entry.ID = publisher.ID
		entry.Name = publisher.Name
		entry.DeliveryStatus = deliveryStatusFromMasterStatus(publisher.Status)
		entry.Active = strings.EqualFold(strings.TrimSpace(publisher.Status), "active")
		if entry.CreatedAt.IsZero() {
			entry.CreatedAt = publisher.CreatedAt
		}
		entry.UpdatedAt = publisher.UpdatedAt
		merged = append(merged, entry)
		seen[publisher.ID] = struct{}{}
	}
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		if _, ok := seen[entry.ID]; ok {
			continue
		}
		copyEntry := *entry
		merged = append(merged, &copyEntry)
	}
	return merged
}

// List handles GET /dashboard/supply-partners.
// When a statsProvider is wired it overlays live pipeline metrics onto each
// record so the Revenue Console always shows up-to-date numbers without a
// separate stats API call.
func (h *SupplyPartnerHandler) List() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		entries := h.listEntries()
		if h.statsProvider != nil {
			snap := h.statsProvider()
			dayWindowSeconds, hourWindowSeconds := statsWindowSeconds(snap.StartedAt)
			for _, e := range entries {
				if vs := snap.ByPublisher[e.ID]; vs != nil {
					// Overlay live metrics; preserve CRUD-managed fields (name, status, …)
					e.Opportunities = vs.Opportunities
					e.GrossRevenue = vs.Revenue
					e.PublisherPayout = vs.PublisherRevenue
					e.Impressions = vs.Impressions
					e.Completions = vs.Completes
					e.AvgQpsYesterday = qpsFromRecentRequests(snap.PublisherRequestsLastDay, e.ID, 86400, vs.AdRequests, dayWindowSeconds)
					e.AvgQpsLastHour = qpsFromRecentRequests(snap.PublisherRequestsLastHour, e.ID, 3600, vs.AdRequests, hourWindowSeconds)
				}
			}
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"entries": entries})
	}
}
func (h *SupplyPartnerHandler) Create() httprouter.Handle {
	return h.store.createHandle(func() *SupplyPartner { return &SupplyPartner{} })
}
func (h *SupplyPartnerHandler) Get() httprouter.Handle { return h.store.getHandle("supply partner") }
func (h *SupplyPartnerHandler) Update() httprouter.Handle {
	return h.store.updateHandle("supply partner", func() *SupplyPartner { return &SupplyPartner{} })
}

// Patch handles PATCH /dashboard/supply-partners/:id.
// Accepts {"active": bool} to toggle the enabled/disabled state without
// overwriting metric counters (use PUT for a full replace).
func (h *SupplyPartnerHandler) Patch() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		var body struct {
			Active *bool `json:"active"`
		}
		if !decodeBody(w, r, &body) {
			return
		}
		if body.Active == nil {
			writeError(w, http.StatusBadRequest, "active field is required")
			return
		}
		want := *body.Active
		updated, ok := h.store.modifyFn(ps.ByName("id"), func(e *SupplyPartner) { e.Active = want })
		if !ok {
			writeError(w, http.StatusNotFound, "supply partner not found")
			return
		}
		writeJSON(w, http.StatusOK, updated)
	}
}

func (h *SupplyPartnerHandler) Delete() httprouter.Handle {
	return h.store.deleteHandle("supply partner")
}

// ═════════════════════════════════════════════════════════════════════════════
// Demand Partner CRUD
// Represents a demand-side DSP / buyer integration together with its
// operational metrics (bid requests, bids, impressions, revenue, VCR,
// viewability). Metrics are updated by the serving pipeline; the create/edit
// form only requires name, delivery_status, and active.
// ═════════════════════════════════════════════════════════════════════════════

// DemandPartner is the canonical record for a demand-side revenue console entity.
type DemandPartner struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// DeliveryStatus: "Live" | "Limited" | "Paused" | "Archived"
	DeliveryStatus string `json:"delivery_status"`
	Active         bool   `json:"active"`

	// Demand-side metrics — written by the serving / billing pipeline.
	BidRequests     int64   `json:"bid_requests"`
	Bids            int64   `json:"bids"`
	AvgQpsYesterday int64   `json:"avg_qps_yesterday"`
	AvgQpsLastHour  int64   `json:"avg_qps_last_hour"`
	Impressions     int64   `json:"impressions"`
	GrossRevenue    float64 `json:"gross_revenue"`
	Payout          float64 `json:"payout"`
	// VCR / Viewability raw counts — ratios are derived in the UI.
	Completions         int64 `json:"completions"`
	ViewableImpressions int64 `json:"viewable_impressions"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (d *DemandPartner) getID() string                { return d.ID }
func (d *DemandPartner) getCreatedAt() time.Time      { return d.CreatedAt }
func (d *DemandPartner) setID(id string)              { d.ID = id }
func (d *DemandPartner) setTimestamps(c, u time.Time) { d.CreatedAt = c; d.UpdatedAt = u }

func (d *DemandPartner) validate() string {
	d.Name = strings.TrimSpace(d.Name)
	if d.Name == "" {
		return "name is required"
	}
	switch d.DeliveryStatus {
	case "Live", "Limited", "Paused", "Archived":
	default:
		d.DeliveryStatus = "Live"
	}
	if d.BidRequests < 0 {
		return "bid_requests must be >= 0"
	}
	if d.Bids < 0 {
		return "bids must be >= 0"
	}
	if d.Impressions < 0 {
		return "impressions must be >= 0"
	}
	if d.GrossRevenue < 0 {
		return "gross_revenue must be >= 0"
	}
	if d.Payout < 0 {
		return "payout must be >= 0"
	}
	if d.Completions < 0 {
		return "completions must be >= 0"
	}
	if d.ViewableImpressions < 0 {
		return "viewable_impressions must be >= 0"
	}
	return ""
}

type demandPartnerStore = entityStore[*DemandPartner]

func newDemandPartnerStore(fp string) *demandPartnerStore {
	return newEntityStore[*DemandPartner](fp, "demand-partners")
}

// DemandPartnerHandler manages CRUD + active-toggle for DemandPartner records.
type DemandPartnerHandler struct {
	store          *demandPartnerStore
	statsProvider  func() VideoStatsPayload // injected by WireVideoStats; may be nil
	advertiserList func() []*Advertiser
}

// NewDemandPartnerHandler creates a DemandPartnerHandler backed by a persistent store.
func NewDemandPartnerHandler(dataDir string) *DemandPartnerHandler {
	return &DemandPartnerHandler{store: newDemandPartnerStore(dataStorePath(dataDir, "demand_partners.json"))}
}

// SetStatsProvider injects a live-stats snapshot function so that List() can
// overlay real-time demand metrics (bid requests, bids, revenue, QPS, …) onto
// each demand partner record keyed by partner ID == advertiser_id.
func (h *DemandPartnerHandler) SetStatsProvider(fn func() VideoStatsPayload) { h.statsProvider = fn }

func (h *DemandPartnerHandler) SetAdvertiserListProvider(fn func() []*Advertiser) {
	h.advertiserList = fn
}

func (h *DemandPartnerHandler) listEntries() []*DemandPartner {
	entries := h.store.list()
	if h.advertiserList == nil {
		return entries
	}
	advertisers := h.advertiserList()
	if len(advertisers) == 0 {
		return entries
	}
	entriesByID := make(map[string]*DemandPartner, len(entries))
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		copyEntry := *entry
		entriesByID[entry.ID] = &copyEntry
	}
	merged := make([]*DemandPartner, 0, len(advertisers)+len(entries))
	seen := make(map[string]struct{}, len(advertisers))
	for _, advertiser := range advertisers {
		if advertiser == nil {
			continue
		}
		entry := &DemandPartner{}
		if existing := entriesByID[advertiser.ID]; existing != nil {
			*entry = *existing
		}
		entry.ID = advertiser.ID
		entry.Name = advertiser.Name
		entry.DeliveryStatus = deliveryStatusFromMasterStatus(advertiser.Status)
		entry.Active = strings.EqualFold(strings.TrimSpace(advertiser.Status), "active")
		if entry.CreatedAt.IsZero() {
			entry.CreatedAt = advertiser.CreatedAt
		}
		entry.UpdatedAt = advertiser.UpdatedAt
		merged = append(merged, entry)
		seen[advertiser.ID] = struct{}{}
	}
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		if _, ok := seen[entry.ID]; ok {
			continue
		}
		copyEntry := *entry
		merged = append(merged, &copyEntry)
	}
	return merged
}

// List handles GET /dashboard/demand-partners.
// When a statsProvider is wired it overlays live pipeline metrics onto each
// record so the Revenue Console always shows up-to-date numbers.
func (h *DemandPartnerHandler) List() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		entries := h.listEntries()
		if h.statsProvider != nil {
			snap := h.statsProvider()
			dayWindowSeconds, hourWindowSeconds := statsWindowSeconds(snap.StartedAt)
			for _, e := range entries {
				if vs := snap.ByAdvertiser[e.ID]; vs != nil {
					// Bid requests = ad_requests seen by this advertiser/demand partner.
					// Bids (fill) = opportunities (VAST/InLine returned to publisher).
					e.BidRequests = vs.AdRequests
					e.Bids = vs.Opportunities
					e.Impressions = vs.Impressions
					e.GrossRevenue = vs.Revenue
					e.Payout = vs.Revenue // Payout = Gross Revenue
					e.Completions = vs.Completes
					e.AvgQpsYesterday = qpsFromRecentRequests(snap.AdvertiserRequestsLastDay, e.ID, 86400, vs.AdRequests, dayWindowSeconds)
					e.AvgQpsLastHour = qpsFromRecentRequests(snap.AdvertiserRequestsLastHour, e.ID, 3600, vs.AdRequests, hourWindowSeconds)
				}
			}
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"entries": entries})
	}
}
func (h *DemandPartnerHandler) Create() httprouter.Handle {
	return h.store.createHandle(func() *DemandPartner { return &DemandPartner{} })
}
func (h *DemandPartnerHandler) Get() httprouter.Handle { return h.store.getHandle("demand partner") }
func (h *DemandPartnerHandler) Update() httprouter.Handle {
	return h.store.updateHandle("demand partner", func() *DemandPartner { return &DemandPartner{} })
}

// Patch handles PATCH /dashboard/demand-partners/:id.
// Accepts {"active": bool} to toggle the enabled/disabled state without
// overwriting metric counters.
func (h *DemandPartnerHandler) Patch() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		var body struct {
			Active *bool `json:"active"`
		}
		if !decodeBody(w, r, &body) {
			return
		}
		if body.Active == nil {
			writeError(w, http.StatusBadRequest, "active field is required")
			return
		}
		want := *body.Active
		updated, ok := h.store.modifyFn(ps.ByName("id"), func(e *DemandPartner) { e.Active = want })
		if !ok {
			writeError(w, http.StatusNotFound, "demand partner not found")
			return
		}
		writeJSON(w, http.StatusOK, updated)
	}
}

func (h *DemandPartnerHandler) Delete() httprouter.Handle {
	return h.store.deleteHandle("demand partner")
}

// ═════════════════════════════════════════════════════════════════════════════
// Bid Report — push/pull ad-delivery events (adomain, crid, campaign_id, …)
// Used by the serving/billing pipeline to record every auction stage and by
// the revenue console to pull attribution and discrepancy data.
//
//   Push single  →  POST /dashboard/reports
//   Push batch   →  POST /dashboard/reports/bulk
//   Pull / query →  GET  /dashboard/reports[?campaign_id=&crid=&adomain=…]
//   Pull by ID   →  GET  /dashboard/reports/:id
//   Delete       →  DELETE /dashboard/reports/:id
// ═════════════════════════════════════════════════════════════════════════════

// BidReportEntry captures a single ad-delivery event together with the key
// OpenRTB bid-response identifiers used for revenue attribution.
type BidReportEntry struct {
	ID string `json:"id"`

	// Auction context
	RequestID   string `json:"request_id,omitempty"`
	ImpID       string `json:"imp_id,omitempty"`
	PublisherID string `json:"publisher_id,omitempty"`
	AdUnitID    string `json:"ad_unit_id,omitempty"`
	Bidder      string `json:"bidder,omitempty"`

	// OpenRTB bid-response identifiers
	ADomain    []string `json:"adomain,omitempty"`     // advertiser domain(s)
	CrID       string   `json:"crid,omitempty"`        // creative ID
	CampaignID string   `json:"campaign_id,omitempty"` // maps to bid.cid / bid.adid
	DealID     string   `json:"deal_id,omitempty"`
	CID        string   `json:"cid,omitempty"`  // buyer's campaign ID (bid.cid)
	BURL       string   `json:"burl,omitempty"` // billing-notice URL logged on win

	// Pricing
	Price    float64 `json:"price"`
	Currency string  `json:"currency,omitempty"` // ISO-4217; default "USD"

	// Event classification
	// "request"|"bid"|"win"|"impression"|"click"|"complete"|"error"
	EventType string    `json:"event_type"`
	EventTime time.Time `json:"event_time"`

	// Inventory environment
	Env         string `json:"env,omitempty"` // "ctv"|"inapp"|"web"
	AppBundle   string `json:"app_bundle,omitempty"`
	Domain      string `json:"domain,omitempty"`
	CountryCode string `json:"country_code,omitempty"` // ISO-3166-1 alpha-2

	// Error detail (when event_type = "error")
	ErrorCode int    `json:"error_code,omitempty"` // VAST error code or HTTP status
	ErrorMsg  string `json:"error_msg,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (b *BidReportEntry) getID() string                { return b.ID }
func (b *BidReportEntry) getCreatedAt() time.Time      { return b.CreatedAt }
func (b *BidReportEntry) setID(id string)              { b.ID = id }
func (b *BidReportEntry) setTimestamps(c, u time.Time) { b.CreatedAt = c; b.UpdatedAt = u }

var validReportEventTypes = map[string]bool{
	"request": true, "bid": true, "win": true,
	"impression": true, "click": true, "complete": true, "error": true,
}

func (b *BidReportEntry) validate() string {
	if !validReportEventTypes[b.EventType] {
		return "event_type must be one of: request, bid, win, impression, click, complete, error"
	}
	if b.Price < 0 {
		return "price must be >= 0"
	}
	if b.EventTime.IsZero() {
		b.EventTime = time.Now().UTC()
	}
	return ""
}

type bidReportStore struct{ *entityStore[*BidReportEntry] }

func newBidReportStore(fp string) *bidReportStore {
	return &bidReportStore{entityStore: newEntityStore[*BidReportEntry](fp, "bid-reports")}
}

func (s *bidReportStore) create(e *BidReportEntry) *BidReportEntry {
	e.setID(generateID())
	now := time.Now().UTC()
	e.setTimestamps(now, now)
	s.mu.Lock()
	s.entries[e.getID()] = e
	s.mu.Unlock()
	if getDashDB() != nil {
		safeGo(func() { s.persistOne(e) })
		return e
	}
	safeGo(s.save)
	return e
}

func (s *bidReportStore) persistOne(e *BidReportEntry) {
	db := getDashDB()
	if db == nil || e == nil {
		return
	}
	b, err := json.Marshal(e)
	if err != nil {
		log.Printf("%s store: marshal: %v", s.label, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO dashboard_entities(kind,id,payload) VALUES ($1,$2,$3)
		 ON CONFLICT (kind,id) DO UPDATE SET payload=EXCLUDED.payload`,
		s.label, e.getID(), b,
	); err != nil {
		log.Printf("%s store: upsert: %v", s.label, err)
	}
}

// BidReportHandler manages push/pull of BidReportEntry records.
type BidReportHandler struct{ store *bidReportStore }

// NewBidReportHandler creates a BidReportHandler backed by a persistent store.
func NewBidReportHandler(dataDir string) *BidReportHandler {
	return &BidReportHandler{store: newBidReportStore(dataStorePath(dataDir, "bid_reports.json"))}
}

func isAuthorizedBidReportWrite(r *http.Request) bool {
	if key := os.Getenv("DASH_REPORT_API_KEY"); key != "" {
		provided := r.Header.Get("X-Dashboard-Report-Key")
		if provided == "" {
			if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
				provided = strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
			}
		}
		return secureStringEquals(provided, key)
	}
	if isValidDashSession(r) {
		return true
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(strings.TrimSpace(host))
	return ip != nil && ip.IsLoopback()
}

// List handles GET /dashboard/reports.
// Supports server-side filtering via query params:
// campaign_id, adomain, crid, bidder, event_type, publisher_id, ad_unit_id.
func (h *BidReportHandler) List() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		all := h.store.list()
		q := r.URL.Query()
		filterCampaign := q.Get("campaign_id")
		filterAdomain := q.Get("adomain")
		filterCrID := q.Get("crid")
		filterBidder := q.Get("bidder")
		filterEvent := q.Get("event_type")
		filterPub := q.Get("publisher_id")
		filterAdUnit := q.Get("ad_unit_id")
		sortBy := q.Get("sort_by")
		sortDir := q.Get("sort_dir")

		if filterCampaign == "" && filterAdomain == "" && filterCrID == "" &&
			filterBidder == "" && filterEvent == "" && filterPub == "" && filterAdUnit == "" {
			if sortBy != "" {
				sortBidReportEntries(all, sortBy, sortDir)
			}
			writeJSON(w, http.StatusOK, map[string]interface{}{"total": len(all), "entries": all})
			return
		}

		out := make([]*BidReportEntry, 0, len(all))
		for _, e := range all {
			if filterCampaign != "" && e.CampaignID != filterCampaign {
				continue
			}
			if filterCrID != "" && e.CrID != filterCrID {
				continue
			}
			if filterBidder != "" && e.Bidder != filterBidder {
				continue
			}
			if filterEvent != "" && e.EventType != filterEvent {
				continue
			}
			if filterPub != "" && e.PublisherID != filterPub {
				continue
			}
			if filterAdUnit != "" && e.AdUnitID != filterAdUnit {
				continue
			}
			if filterAdomain != "" {
				found := false
				for _, d := range e.ADomain {
					if d == filterAdomain {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}
			out = append(out, e)
		}
		if sortBy != "" {
			sortBidReportEntries(out, sortBy, sortDir)
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"total": len(out), "entries": out})
	}
}

func sortBidReportEntries(entries []*BidReportEntry, sortBy, sortDir string) {
	if len(entries) < 2 {
		return
	}
	field := strings.ToLower(strings.TrimSpace(sortBy))
	desc := !strings.EqualFold(sortDir, "asc")
	sort.SliceStable(entries, func(i, j int) bool {
		cmp := compareBidReportEntries(entries[i], entries[j], field)
		if cmp == 0 {
			cmp = compareTime(entries[i].CreatedAt, entries[j].CreatedAt)
		}
		if desc {
			return cmp > 0
		}
		return cmp < 0
	})
}

func compareBidReportEntries(left, right *BidReportEntry, field string) int {
	switch field {
	case "time", "event_time":
		return compareTime(left.EventTime, right.EventTime)
	case "price":
		return compareFloat64(left.Price, right.Price)
	case "event", "event_type":
		return compareString(left.EventType, right.EventType)
	case "bidder":
		return compareString(left.Bidder, right.Bidder)
	case "adomain":
		return compareString(strings.Join(left.ADomain, ","), strings.Join(right.ADomain, ","))
	case "crid":
		return compareString(left.CrID, right.CrID)
	case "campaign", "campaign_id":
		return compareString(left.CampaignID, right.CampaignID)
	case "publisher", "publisher_id":
		return compareString(left.PublisherID, right.PublisherID)
	case "ad_unit", "ad_unit_id":
		return compareString(left.AdUnitID, right.AdUnitID)
	case "domain":
		return compareString(firstNonEmpty(left.Domain, left.AppBundle), firstNonEmpty(right.Domain, right.AppBundle))
	case "country", "country_code":
		return compareString(left.CountryCode, right.CountryCode)
	case "env":
		return compareString(left.Env, right.Env)
	case "request", "request_id":
		return compareString(left.RequestID, right.RequestID)
	case "created_at":
		return compareTime(left.CreatedAt, right.CreatedAt)
	case "updated_at":
		return compareTime(left.UpdatedAt, right.UpdatedAt)
	default:
		return compareTime(left.EventTime, right.EventTime)
	}
}

func compareString(left, right string) int {
	return strings.Compare(strings.ToLower(left), strings.ToLower(right))
}

func compareFloat64(left, right float64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareTime(left, right time.Time) int {
	switch {
	case left.Before(right):
		return -1
	case left.After(right):
		return 1
	default:
		return 0
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// Push handles POST /dashboard/reports — pushes a single event record.
func (h *BidReportHandler) Push() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		if !isAuthorizedBidReportWrite(r) {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		var e BidReportEntry
		if !decodeBody(w, r, &e) {
			return
		}
		if msg := e.validate(); msg != "" {
			writeError(w, http.StatusBadRequest, msg)
			return
		}
		writeJSON(w, http.StatusCreated, h.store.create(&e))
	}
}

// BulkPush handles POST /dashboard/reports/bulk — pushes a slice of event records.
func (h *BidReportHandler) BulkPush() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		if !isAuthorizedBidReportWrite(r) {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		var entries []*BidReportEntry
		if !decodeBody(w, r, &entries) {
			return
		}
		created := make([]*BidReportEntry, 0, len(entries))
		for i, e := range entries {
			if msg := e.validate(); msg != "" {
				writeError(w, http.StatusBadRequest, strings.Join([]string{"entry", strings.TrimSpace(strings.Repeat("0", i+1)), msg}, " "))
				return
			}
			created = append(created, h.store.create(e))
		}
		writeJSON(w, http.StatusCreated, map[string]interface{}{"total": len(created), "entries": created})
	}
}

// Get handles GET /dashboard/reports/:id — pulls one report entry.
func (h *BidReportHandler) Get() httprouter.Handle { return h.store.getHandle("report") }

// Delete handles DELETE /dashboard/reports/:id — removes one report entry.
func (h *BidReportHandler) Delete() httprouter.Handle { return h.store.deleteHandle("report") }

// ═════════════════════════════════════════════════════════════════════════════
// DashboardRegistry
// Central registry that constructs and wires every dashboard CRUD handler.
// Callers instantiate once with NewDashboardRegistry, optionally inject
// pipeline dependencies via WireVideoExchange, then call Register.
// ═════════════════════════════════════════════════════════════════════════════

// DashboardRegistry holds every dashboard CRUD handler keyed by entity.
// All handlers share the same dataDir for persistent storage.
type DashboardRegistry struct {
	Publisher       *PublisherHandler
	Advertiser      *AdvertiserHandler
	Campaign        *CampaignHandler
	DomainList      *DomainListHandler
	AudienceSegment *AudienceSegmentHandler
	YieldRule       *YieldRuleHandler
	BidderScorecard *BidderScorecardHandler
	DynamicFloor    *DynamicFloorRuleHandler
	RequestQA       *RequestQAProfileHandler
	TimeoutProfile  *TimeoutProfileHandler
	PodOptimization *PodOptimizationRuleHandler
	VideoExchange   *VideoExchangeHandler
	// Revenue Console supply/demand partner tables
	SupplyPartner *SupplyPartnerHandler
	DemandPartner *DemandPartnerHandler
	// Ad-delivery reporting (push/pull adomain, crid, campaign_id, …)
	BidReport *BidReportHandler
}

// NewDashboardRegistry constructs all dashboard CRUD handlers with persistent
// stores rooted at dataDir. Pass "" to disable file persistence (useful in tests).
func NewDashboardRegistry(dataDir string) *DashboardRegistry {
	return &DashboardRegistry{
		Publisher:       NewPublisherHandler(dataDir),
		Advertiser:      NewAdvertiserHandler(dataDir),
		Campaign:        NewCampaignHandler(dataDir),
		DomainList:      NewDomainListHandler(dataDir),
		AudienceSegment: NewAudienceSegmentHandler(dataDir),
		YieldRule:       NewYieldRuleHandler(dataDir),
		BidderScorecard: NewBidderScorecardHandler(dataDir),
		DynamicFloor:    NewDynamicFloorRuleHandler(dataDir),
		RequestQA:       NewRequestQAProfileHandler(dataDir),
		TimeoutProfile:  NewTimeoutProfileHandler(dataDir),
		PodOptimization: NewPodOptimizationRuleHandler(dataDir),
		VideoExchange:   NewVideoExchangeHandler(dataDir),
		SupplyPartner:   NewSupplyPartnerHandler(dataDir),
		DemandPartner:   NewDemandPartnerHandler(dataDir),
		BidReport:       NewBidReportHandler(dataDir),
	}
}

// WireVideoStats injects the video pipeline's live-snapshot function into the
// SupplyPartner and DemandPartner list handlers so that GET /dashboard/supply-partners
// and GET /dashboard/demand-partners return real-time metrics merged from the
// serving pipeline. Call after videoPipeline is constructed and before Register.
func (reg *DashboardRegistry) WireVideoStats(snap func() VideoStatsPayload) {
	reg.SupplyPartner.SetStatsProvider(snap)
	reg.DemandPartner.SetStatsProvider(snap)
}

func (reg *DashboardRegistry) WireRevenueConsoleMasters() {
	reg.SupplyPartner.SetPublisherListProvider(func() []*Publisher { return reg.Publisher.Store().list() })
	reg.DemandPartner.SetAdvertiserListProvider(func() []*Advertiser { return reg.Advertiser.Store().list() })
}

// WireVideoExchange injects the video pipeline's ad-server registration callback
// and the campaign store into the VideoExchange handler, then syncs all
// placements loaded from disk back into the pipeline.
// Call after videoPipeline is constructed and before Register.
func (reg *DashboardRegistry) WireVideoExchange(registerCfg func(*AdServerConfig), unregisterCfg func(string), lookupCfg func(string) *AdServerConfig) {
	reg.VideoExchange.SetCampaignStore(reg.Campaign.Store())
	reg.VideoExchange.SetPipelineRegister(registerCfg)
	reg.VideoExchange.SetPipelineUnregister(unregisterCfg)
	reg.VideoExchange.SetPipelineLookup(lookupCfg)
	reg.VideoExchange.SyncAllToPipeline()
	// Re-sync all placements whenever a campaign URL changes.
	reg.Campaign.SetOnChange(reg.VideoExchange.SyncAllToPipeline)
}

// Register attaches every dashboard CRUD route to r.
// Pass authWrap (e.g. DashboardAuthMiddleware) to guard all routes behind auth.
// Must be called after WireVideoExchange when the video pipeline is in use.
func (reg *DashboardRegistry) Register(r *httprouter.Router, authWrap ...func(httprouter.Handle) httprouter.Handle) {
	auth := func(h httprouter.Handle) httprouter.Handle { return h }
	if len(authWrap) > 0 && authWrap[0] != nil {
		auth = authWrap[0]
	}

	// ── Publishers ────────────────────────────────────────────────────────────
	r.GET("/dashboard/publishers", auth(reg.Publisher.List()))
	r.POST("/dashboard/publishers", auth(reg.Publisher.Create()))
	r.GET("/dashboard/publishers/:id", auth(reg.Publisher.Get()))
	r.PUT("/dashboard/publishers/:id", auth(reg.Publisher.Update()))
	r.DELETE("/dashboard/publishers/:id", auth(reg.Publisher.Delete()))

	// ── Advertisers ───────────────────────────────────────────────────────────
	r.GET("/dashboard/advertisers", auth(reg.Advertiser.List()))
	r.POST("/dashboard/advertisers", auth(reg.Advertiser.Create()))
	r.GET("/dashboard/advertisers/:id", auth(reg.Advertiser.Get()))
	r.PUT("/dashboard/advertisers/:id", auth(reg.Advertiser.Update()))
	r.DELETE("/dashboard/advertisers/:id", auth(reg.Advertiser.Delete()))

	// ── Campaigns ─────────────────────────────────────────────────────────────
	r.GET("/dashboard/campaigns", auth(reg.Campaign.List()))
	r.POST("/dashboard/campaigns", auth(reg.Campaign.Create()))
	r.GET("/dashboard/campaigns/:id", auth(reg.Campaign.Get()))
	r.PUT("/dashboard/campaigns/:id", auth(reg.Campaign.Update()))
	r.DELETE("/dashboard/campaigns/:id", auth(reg.Campaign.Delete()))

	// ── Domain Lists ──────────────────────────────────────────────────────────
	r.GET("/dashboard/domain-lists", auth(reg.DomainList.List()))
	r.POST("/dashboard/domain-lists", auth(reg.DomainList.Create()))
	r.GET("/dashboard/domain-lists/:id", auth(reg.DomainList.Get()))
	r.PUT("/dashboard/domain-lists/:id", auth(reg.DomainList.Update()))
	r.DELETE("/dashboard/domain-lists/:id", auth(reg.DomainList.Delete()))

	// ── Audience Segments ─────────────────────────────────────────────────────
	r.GET("/dashboard/audiences", auth(reg.AudienceSegment.List()))
	r.POST("/dashboard/audiences", auth(reg.AudienceSegment.Create()))
	r.GET("/dashboard/audiences/:id", auth(reg.AudienceSegment.Get()))
	r.PUT("/dashboard/audiences/:id", auth(reg.AudienceSegment.Update()))
	r.DELETE("/dashboard/audiences/:id", auth(reg.AudienceSegment.Delete()))

	// ── Yield Rules ───────────────────────────────────────────────────────────
	r.GET("/dashboard/yield-rules", auth(reg.YieldRule.List()))
	r.POST("/dashboard/yield-rules", auth(reg.YieldRule.Create()))
	r.GET("/dashboard/yield-rules/:id", auth(reg.YieldRule.Get()))
	r.PUT("/dashboard/yield-rules/:id", auth(reg.YieldRule.Update()))
	r.DELETE("/dashboard/yield-rules/:id", auth(reg.YieldRule.Delete()))

	// ── Bidder Scorecards ─────────────────────────────────────────────────────
	r.GET("/dashboard/bidder-scorecards", auth(reg.BidderScorecard.List()))
	r.POST("/dashboard/bidder-scorecards", auth(reg.BidderScorecard.Create()))
	r.GET("/dashboard/bidder-scorecards/:id", auth(reg.BidderScorecard.Get()))
	r.PUT("/dashboard/bidder-scorecards/:id", auth(reg.BidderScorecard.Update()))
	r.DELETE("/dashboard/bidder-scorecards/:id", auth(reg.BidderScorecard.Delete()))

	// ── Dynamic Floor Rules ───────────────────────────────────────────────────
	r.GET("/dashboard/floor-rules", auth(reg.DynamicFloor.List()))
	r.POST("/dashboard/floor-rules", auth(reg.DynamicFloor.Create()))
	r.GET("/dashboard/floor-rules/:id", auth(reg.DynamicFloor.Get()))
	r.PUT("/dashboard/floor-rules/:id", auth(reg.DynamicFloor.Update()))
	r.DELETE("/dashboard/floor-rules/:id", auth(reg.DynamicFloor.Delete()))

	// ── Request QA Profiles ───────────────────────────────────────────────────
	r.GET("/dashboard/qa-profiles", auth(reg.RequestQA.List()))
	r.POST("/dashboard/qa-profiles", auth(reg.RequestQA.Create()))
	r.GET("/dashboard/qa-profiles/:id", auth(reg.RequestQA.Get()))
	r.PUT("/dashboard/qa-profiles/:id", auth(reg.RequestQA.Update()))
	r.DELETE("/dashboard/qa-profiles/:id", auth(reg.RequestQA.Delete()))

	// ── Timeout Profiles ──────────────────────────────────────────────────────
	r.GET("/dashboard/timeout-profiles", auth(reg.TimeoutProfile.List()))
	r.POST("/dashboard/timeout-profiles", auth(reg.TimeoutProfile.Create()))
	r.GET("/dashboard/timeout-profiles/:id", auth(reg.TimeoutProfile.Get()))
	r.PUT("/dashboard/timeout-profiles/:id", auth(reg.TimeoutProfile.Update()))
	r.DELETE("/dashboard/timeout-profiles/:id", auth(reg.TimeoutProfile.Delete()))

	// ── Pod Optimization Rules ────────────────────────────────────────────────
	r.GET("/dashboard/pod-rules", auth(reg.PodOptimization.List()))
	r.POST("/dashboard/pod-rules", auth(reg.PodOptimization.Create()))
	r.GET("/dashboard/pod-rules/:id", auth(reg.PodOptimization.Get()))
	r.PUT("/dashboard/pod-rules/:id", auth(reg.PodOptimization.Update()))
	r.DELETE("/dashboard/pod-rules/:id", auth(reg.PodOptimization.Delete()))

	// ── Video Exchange (Ad Units) ─────────────────────────────────────────────
	r.GET("/dashboard/video", auth(reg.VideoExchange.List()))
	r.POST("/dashboard/video", auth(reg.VideoExchange.Create()))
	r.GET("/dashboard/video/:id", auth(reg.VideoExchange.Get()))
	r.PUT("/dashboard/video/:id", auth(reg.VideoExchange.Update()))
	r.DELETE("/dashboard/video/:id", auth(reg.VideoExchange.Delete()))

	// ── Supply Partners (Revenue Console) ────────────────────────────────────
	r.GET("/dashboard/supply-partners", auth(reg.SupplyPartner.List()))
	r.POST("/dashboard/supply-partners", auth(reg.SupplyPartner.Create()))
	r.GET("/dashboard/supply-partners/:id", auth(reg.SupplyPartner.Get()))
	r.PUT("/dashboard/supply-partners/:id", auth(reg.SupplyPartner.Update()))
	r.PATCH("/dashboard/supply-partners/:id", auth(reg.SupplyPartner.Patch()))
	r.DELETE("/dashboard/supply-partners/:id", auth(reg.SupplyPartner.Delete()))

	// ── Demand Partners (Revenue Console) ────────────────────────────────────
	r.GET("/dashboard/demand-partners", auth(reg.DemandPartner.List()))
	r.POST("/dashboard/demand-partners", auth(reg.DemandPartner.Create()))
	r.GET("/dashboard/demand-partners/:id", auth(reg.DemandPartner.Get()))
	r.PUT("/dashboard/demand-partners/:id", auth(reg.DemandPartner.Update()))
	r.PATCH("/dashboard/demand-partners/:id", auth(reg.DemandPartner.Patch()))
	r.DELETE("/dashboard/demand-partners/:id", auth(reg.DemandPartner.Delete()))

	// ── Bid Reports (push/pull adomain, crid, campaign_id) ───────────────────
	// No auth required on the push endpoints so the serving pipeline can post
	// events without a browser session; the pull endpoints are auth-gated.
	r.GET("/dashboard/reports", auth(reg.BidReport.List()))
	r.GET("/dashboard/reports/:id", auth(reg.BidReport.Get()))
	r.DELETE("/dashboard/reports/:id", auth(reg.BidReport.Delete()))
	r.POST("/dashboard/reports", reg.BidReport.Push())          // unauthenticated: pipeline push
	r.POST("/dashboard/reports/bulk", reg.BidReport.BulkPush()) // unauthenticated: batch pipeline push
}
