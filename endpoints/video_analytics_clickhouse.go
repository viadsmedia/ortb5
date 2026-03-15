package endpoints

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

const (
	clickHouseDSNEnv        = "CLICKHOUSE_DSN"
	clickHouseVideoTableEnv = "CLICKHOUSE_VIDEO_TABLE"
	defaultClickHouseTable  = "video_event_facts"
)

type videoMetricEvent struct {
	EventTime           time.Time
	EventType           string
	PlacementID         string
	PublisherID         string
	AdvertiserID        string
	Bidder              string
	AppID               string
	Country             string
	Device              string
	Format              string
	DemandChannel       string
	AuctionID           string
	BidID               string
	CrID                string
	PriceCPM            float64
	RevenueUSD          float64
	PublisherRevenueUSD float64
}

type clickHouseVideoMetricsStore struct {
	db    *sql.DB
	table string
	queue chan videoMetricEvent
	done  chan struct{}
	wg    sync.WaitGroup
}

func newClickHouseVideoMetricsStoreFromEnv() *clickHouseVideoMetricsStore {
	dsn := strings.TrimSpace(os.Getenv(clickHouseDSNEnv))
	if dsn == "" {
		return nil
	}
	table := strings.TrimSpace(os.Getenv(clickHouseVideoTableEnv))
	if table == "" {
		table = defaultClickHouseTable
	}
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		log.Printf("clickhouse metrics: open failed: %v", err)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Printf("clickhouse metrics: ping failed: %v", err)
		_ = db.Close()
		return nil
	}
	store := &clickHouseVideoMetricsStore{
		db:    db,
		table: table,
		queue: make(chan videoMetricEvent, 4096),
		done:  make(chan struct{}),
	}
	if err := store.ensureSchema(ctx); err != nil {
		log.Printf("clickhouse metrics: schema failed: %v", err)
		_ = db.Close()
		return nil
	}
	store.wg.Add(1)
	safeGo(store.run)
	log.Printf("clickhouse metrics: enabled (table %s)", table)
	return store
}

func (s *clickHouseVideoMetricsStore) ensureSchema(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			event_time DateTime64(3),
			event_date Date MATERIALIZED toDate(event_time),
			event_type LowCardinality(String),
			placement_id String,
			publisher_id String,
			advertiser_id String,
			bidder LowCardinality(String),
			app_id String,
			country LowCardinality(String),
			device LowCardinality(String),
			format LowCardinality(String),
			demand_channel LowCardinality(String),
			auction_id String,
			bid_id String,
			crid String,
			price_cpm Float64,
			revenue_usd Float64,
			publisher_revenue_usd Float64 DEFAULT 0
		) ENGINE = MergeTree
		PARTITION BY toYYYYMM(event_date)
		ORDER BY (event_date, event_type, placement_id, publisher_id, advertiser_id, bidder, auction_id, bid_id)
	`, s.table)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return err
	}
	alterQuery := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS publisher_revenue_usd Float64 DEFAULT 0`, s.table)
	_, err := s.db.ExecContext(ctx, alterQuery)
	return err
}

func (s *clickHouseVideoMetricsStore) run() {
	defer s.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	batch := make([]videoMetricEvent, 0, 256)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.insertBatch(ctx, batch); err != nil {
			log.Printf("clickhouse metrics: insert batch failed: %v", err)
		}
		batch = batch[:0]
	}
	for {
		select {
		case ev := <-s.queue:
			batch = append(batch, ev)
			if len(batch) >= 256 {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-s.done:
			flush()
			return
		}
	}
}

func (s *clickHouseVideoMetricsStore) Record(ev videoMetricEvent) {
	if s == nil || s.db == nil {
		return
	}
	if ev.EventTime.IsZero() {
		ev.EventTime = time.Now().UTC()
	}
	select {
	case s.queue <- ev:
		return
	case <-time.After(50 * time.Millisecond):
		ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
		defer cancel()
		if err := s.insertBatch(ctx, []videoMetricEvent{ev}); err != nil {
			log.Printf("clickhouse metrics: fallback insert failed: %v", err)
		}
	}
}

func (s *clickHouseVideoMetricsStore) insertBatch(ctx context.Context, batch []videoMetricEvent) error {
	if len(batch) == 0 {
		return nil
	}
	var builder strings.Builder
	args := make([]interface{}, 0, len(batch)*17)
	builder.WriteString("INSERT INTO ")
	builder.WriteString(s.table)
	builder.WriteString(" (event_time,event_type,placement_id,publisher_id,advertiser_id,bidder,app_id,country,device,format,demand_channel,auction_id,bid_id,crid,price_cpm,revenue_usd,publisher_revenue_usd) VALUES ")
	for i, ev := range batch {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		args = append(args,
			ev.EventTime.UTC(),
			ev.EventType,
			ev.PlacementID,
			ev.PublisherID,
			ev.AdvertiserID,
			ev.Bidder,
			ev.AppID,
			ev.Country,
			ev.Device,
			ev.Format,
			ev.DemandChannel,
			ev.AuctionID,
			ev.BidID,
			ev.CrID,
			ev.PriceCPM,
			ev.RevenueUSD,
			ev.PublisherRevenueUSD,
		)
	}
	_, err := s.db.ExecContext(ctx, builder.String(), args...)
	return err
}

func (s *clickHouseVideoMetricsStore) Snapshot(ctx context.Context) (VideoStatsPayload, error) {
	result := VideoStatsPayload{
		ByPublisher:                map[string]*VideoStats{},
		ByAdvertiser:               map[string]*VideoStats{},
		ByBidder:                   map[string]*VideoStats{},
		ByApp:                      map[string]*VideoStats{},
		ByPlacement:                map[string]*VideoStats{},
		ByCountry:                  map[string]*VideoStats{},
		ByDevice:                   map[string]*VideoStats{},
		ByFormat:                   map[string]*VideoStats{},
		ByDemandChannel:            map[string]*VideoStats{},
		PublisherRequestsLastDay:   map[string]int64{},
		PublisherRequestsLastHour:  map[string]int64{},
		AdvertiserRequestsLastDay:  map[string]int64{},
		AdvertiserRequestsLastHour: map[string]int64{},
	}
	if s == nil || s.db == nil {
		return result, nil
	}
	var err error
	if result.ByPublisher, err = s.queryGrouped(ctx, "publisher_id"); err != nil {
		return result, err
	}
	if result.ByAdvertiser, err = s.queryGrouped(ctx, "advertiser_id"); err != nil {
		return result, err
	}
	if result.ByBidder, err = s.queryGrouped(ctx, "bidder"); err != nil {
		return result, err
	}
	if result.ByApp, err = s.queryGrouped(ctx, "app_id"); err != nil {
		return result, err
	}
	if result.ByPlacement, err = s.queryGrouped(ctx, "placement_id"); err != nil {
		return result, err
	}
	if result.ByCountry, err = s.queryGrouped(ctx, "country"); err != nil {
		return result, err
	}
	if result.ByDevice, err = s.queryGrouped(ctx, "device"); err != nil {
		return result, err
	}
	if result.ByFormat, err = s.queryGrouped(ctx, "format"); err != nil {
		return result, err
	}
	if result.ByDemandChannel, err = s.queryGrouped(ctx, "demand_channel"); err != nil {
		return result, err
	}
	if result.Total, err = s.queryTotal(ctx); err != nil {
		return result, err
	}
	if result.StartedAt, err = s.queryStartedAt(ctx); err != nil {
		return result, err
	}
	startedAt := time.Unix(result.StartedAt, 0).UTC()
	now := time.Now().UTC()
	dayStart := now.Add(-24 * time.Hour)
	if startedAt.After(dayStart) {
		dayStart = startedAt
	}
	hourStart := now.Add(-1 * time.Hour)
	if startedAt.After(hourStart) {
		hourStart = startedAt
	}
	if result.PublisherRequestsLastDay, err = s.queryRequestCountsByGroup(ctx, "publisher_id", dayStart, now); err != nil {
		return result, err
	}
	if result.PublisherRequestsLastHour, err = s.queryRequestCountsByGroup(ctx, "publisher_id", hourStart, now); err != nil {
		return result, err
	}
	if result.AdvertiserRequestsLastDay, err = s.queryRequestCountsByGroup(ctx, "advertiser_id", dayStart, now); err != nil {
		return result, err
	}
	if result.AdvertiserRequestsLastHour, err = s.queryRequestCountsByGroup(ctx, "advertiser_id", hourStart, now); err != nil {
		return result, err
	}
	return result, nil
}

func (s *clickHouseVideoMetricsStore) queryRequestCountsByGroup(ctx context.Context, column string, start, end time.Time) (map[string]int64, error) {
	allowed := map[string]bool{
		"publisher_id":  true,
		"advertiser_id": true,
	}
	if !allowed[column] {
		return nil, fmt.Errorf("unsupported request-count column %q", column)
	}
	query := fmt.Sprintf(`
		SELECT
			if(%[1]s = '', '(unknown)', %[1]s) AS dim,
			count() AS request_count
		FROM %s
		WHERE event_type = 'request' AND event_time >= ? AND event_time < ?
		GROUP BY dim
	`, column, s.table)
	rows, err := s.db.QueryContext(ctx, query, start.UTC(), end.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var key string
		var count int64
		if err := rows.Scan(&key, &count); err != nil {
			return nil, err
		}
		out[key] = count
	}
	return out, rows.Err()
}

func (s *clickHouseVideoMetricsStore) queryGrouped(ctx context.Context, column string) (map[string]*VideoStats, error) {
	allowed := map[string]bool{
		"publisher_id":   true,
		"advertiser_id":  true,
		"bidder":         true,
		"app_id":         true,
		"placement_id":   true,
		"country":        true,
		"device":         true,
		"format":         true,
		"demand_channel": true,
	}
	if !allowed[column] {
		return nil, fmt.Errorf("unsupported column %q", column)
	}
	query := fmt.Sprintf(`
		SELECT
			if(%[1]s = '', '(unknown)', %[1]s) AS dim,
			countIf(event_type = 'request') AS ad_requests,
			countIf(event_type = 'opportunity') AS opportunities,
			countIf(event_type = 'impression') AS impressions,
			countIf(event_type = 'start') AS starts,
			countIf(event_type = 'complete') AS completes,
			sumIf(revenue_usd, event_type = 'impression') AS revenue,
			sumIf(publisher_revenue_usd, event_type = 'impression') AS publisher_revenue
		FROM %s
		GROUP BY dim
	`, column, s.table)
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]*VideoStats{}
	for rows.Next() {
		var key string
		stats := &VideoStats{}
		if err := rows.Scan(&key, &stats.AdRequests, &stats.Opportunities, &stats.Impressions, &stats.Starts, &stats.Completes, &stats.Revenue, &stats.PublisherRevenue); err != nil {
			return nil, err
		}
		stats.VCR = videoCompletionRate(*stats)
		out[key] = stats
	}
	return out, rows.Err()
}

func (s *clickHouseVideoMetricsStore) queryTotal(ctx context.Context) (VideoStats, error) {
	query := fmt.Sprintf(`
		SELECT
			countIf(event_type = 'request') AS ad_requests,
			countIf(event_type = 'opportunity') AS opportunities,
			countIf(event_type = 'impression') AS impressions,
			countIf(event_type = 'start') AS starts,
			countIf(event_type = 'complete') AS completes,
			sumIf(revenue_usd, event_type = 'impression') AS revenue,
			sumIf(publisher_revenue_usd, event_type = 'impression') AS publisher_revenue
		FROM %s
	`, s.table)
	stats := VideoStats{}
	if err := s.db.QueryRowContext(ctx, query).Scan(&stats.AdRequests, &stats.Opportunities, &stats.Impressions, &stats.Starts, &stats.Completes, &stats.Revenue, &stats.PublisherRevenue); err != nil {
		return stats, err
	}
	stats.VCR = videoCompletionRate(stats)
	return stats, nil
}

func (s *clickHouseVideoMetricsStore) queryStartedAt(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(`SELECT toUnixTimestamp(min(event_time)) FROM %s`, s.table)
	var started sql.NullInt64
	if err := s.db.QueryRowContext(ctx, query).Scan(&started); err != nil {
		return 0, err
	}
	if !started.Valid {
		return time.Now().Unix(), nil
	}
	return started.Int64, nil
}

type overviewWindow struct {
	period        string
	label         string
	previousLabel string
	timezone      string
	location      *time.Location
	currentStart  time.Time
	currentEnd    time.Time
	previousStart time.Time
	previousEnd   time.Time
	bucketSize    time.Duration
}

type overviewBucketCounts struct {
	Requests      int64
	Opportunities int64
	Impressions   int64
}

const overviewDayStartHour = 7

func resolveOverviewLocation(timezone string) (*time.Location, string) {
	timezone = strings.TrimSpace(timezone)
	if timezone == "" {
		return time.UTC, time.UTC.String()
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return time.UTC, time.UTC.String()
	}
	return loc, timezone
}

func overviewDayStart(baseNow time.Time, location *time.Location) time.Time {
	start := time.Date(baseNow.Year(), baseNow.Month(), baseNow.Day(), overviewDayStartHour, 0, 0, 0, location)
	if baseNow.Before(start) {
		start = start.Add(-24 * time.Hour)
	}
	return start
}

func buildOverviewWindow(period string, now time.Time, location *time.Location) overviewWindow {
	if location == nil {
		location = time.UTC
	}
	baseNow := now.In(location)
	startOfDay := overviewDayStart(baseNow, location)
	switch strings.ToLower(strings.TrimSpace(period)) {
	case "yesterday":
		currentStart := startOfDay.Add(-24 * time.Hour)
		currentEnd := startOfDay
		previousStart := currentStart.Add(-24 * time.Hour)
		return overviewWindow{period: "yesterday", label: "Yesterday", previousLabel: "2 Days Ago", timezone: location.String(), location: location, currentStart: currentStart, currentEnd: currentEnd, previousStart: previousStart, previousEnd: currentStart, bucketSize: time.Hour}
	case "week":
		currentStart := startOfDay.AddDate(0, 0, -6)
		previousStart := currentStart.AddDate(0, 0, -7)
		return overviewWindow{period: "week", label: "Last 7 Days", previousLabel: "Previous 7 Days", timezone: location.String(), location: location, currentStart: currentStart, currentEnd: baseNow, previousStart: previousStart, previousEnd: currentStart, bucketSize: 24 * time.Hour}
	case "month":
		currentStart := startOfDay.AddDate(0, 0, -29)
		previousStart := currentStart.AddDate(0, 0, -30)
		return overviewWindow{period: "month", label: "Last 30 Days", previousLabel: "Previous 30 Days", timezone: location.String(), location: location, currentStart: currentStart, currentEnd: baseNow, previousStart: previousStart, previousEnd: currentStart, bucketSize: 24 * time.Hour}
	default:
		elapsed := baseNow.Sub(startOfDay)
		previousStart := startOfDay.Add(-24 * time.Hour)
		return overviewWindow{period: "today", label: "Today", previousLabel: "Yesterday", timezone: location.String(), location: location, currentStart: startOfDay, currentEnd: baseNow, previousStart: previousStart, previousEnd: previousStart.Add(elapsed), bucketSize: time.Hour}
	}
}

func overviewSummaryFromVideoStats(stats VideoStats) VideoOverviewSummary {
	summary := VideoOverviewSummary{
		AdRequests:       stats.AdRequests,
		Opportunities:    stats.Opportunities,
		Impressions:      stats.Impressions,
		Starts:           stats.Starts,
		Completes:        stats.Completes,
		Revenue:          stats.Revenue,
		PublisherRevenue: stats.PublisherRevenue,
		Margin:           stats.Revenue - stats.PublisherRevenue,
		VCR:              videoCompletionRate(stats),
	}
	if summary.Impressions > 0 {
		summary.ECPM = summary.Revenue / float64(summary.Impressions) * 1000
	}
	if summary.AdRequests > 0 {
		summary.FillRate = float64(summary.Opportunities) / float64(summary.AdRequests) * 100
	}
	if summary.Opportunities > 0 {
		summary.ResponseRate = float64(summary.Impressions) / float64(summary.Opportunities) * 100
	}
	summary.Viewability = summary.VCR * 1.15
	if summary.Viewability > 100 {
		summary.Viewability = 100
	}
	return summary
}

func overviewBucketKey(bucket time.Time, bucketSize time.Duration) string {
	if bucketSize >= 24*time.Hour {
		return bucket.Format("2006-01-02")
	}
	return bucket.Format("2006-01-02 15:00")
}

func buildOverviewLabels(window overviewWindow) ([]time.Time, []string, []string) {
	buckets := make([]time.Time, 0, 32)
	keys := make([]string, 0, 32)
	labels := make([]string, 0, 32)
	for bucket := window.currentStart; bucket.Before(window.currentEnd); bucket = bucket.Add(window.bucketSize) {
		buckets = append(buckets, bucket)
		keys = append(keys, overviewBucketKey(bucket.In(window.location), window.bucketSize))
		if window.bucketSize >= 24*time.Hour {
			labels = append(labels, bucket.Format("Jan 02"))
		} else {
			labels = append(labels, bucket.Format("15:00"))
		}
	}
	if len(buckets) == 0 {
		buckets = append(buckets, window.currentStart)
		keys = append(keys, overviewBucketKey(window.currentStart.In(window.location), window.bucketSize))
		if window.bucketSize >= 24*time.Hour {
			labels = append(labels, window.currentStart.Format("Jan 02"))
		} else {
			labels = append(labels, window.currentStart.Format("15:00"))
		}
	}
	return buckets, keys, labels
}

func (s *clickHouseVideoMetricsStore) queryVideoStatsRange(ctx context.Context, start, end time.Time) (VideoStats, error) {
	query := fmt.Sprintf(`
		SELECT
			countIf(event_type = 'request') AS ad_requests,
			countIf(event_type = 'opportunity') AS opportunities,
			countIf(event_type = 'impression') AS impressions,
			countIf(event_type = 'start') AS starts,
			countIf(event_type = 'complete') AS completes,
			sumIf(revenue_usd, event_type = 'impression') AS revenue,
			sumIf(publisher_revenue_usd, event_type = 'impression') AS publisher_revenue
		FROM %s
		WHERE event_time >= ? AND event_time < ?
	`, s.table)
	stats := VideoStats{}
	if err := s.db.QueryRowContext(ctx, query, start.UTC(), end.UTC()).Scan(&stats.AdRequests, &stats.Opportunities, &stats.Impressions, &stats.Starts, &stats.Completes, &stats.Revenue, &stats.PublisherRevenue); err != nil {
		return stats, err
	}
	stats.VCR = videoCompletionRate(stats)
	return stats, nil
}

func (s *clickHouseVideoMetricsStore) queryOverviewSeries(ctx context.Context, start, end time.Time, bucketSize time.Duration, timezone string) (map[string]overviewBucketCounts, error) {
	timezone = strings.ReplaceAll(timezone, "'", "''")
	bucketExpr := fmt.Sprintf("formatDateTime(toStartOfHour(toTimeZone(event_time, '%s')), '%%Y-%%m-%%d %%H:00', '%s')", timezone, timezone)
	if bucketSize >= 24*time.Hour {
		bucketExpr = fmt.Sprintf("formatDateTime(toStartOfDay(toTimeZone(event_time, '%s') - INTERVAL %d HOUR) + INTERVAL %d HOUR, '%%Y-%%m-%%d', '%s')", timezone, overviewDayStartHour, overviewDayStartHour, timezone)
	}
	query := fmt.Sprintf(`
		SELECT
			%s AS bucket,
			countIf(event_type = 'request') AS ad_requests,
			countIf(event_type = 'opportunity') AS opportunities,
			countIf(event_type = 'impression') AS impressions
		FROM %s
		WHERE event_time >= ? AND event_time < ?
		GROUP BY bucket
		ORDER BY bucket
	`, bucketExpr, s.table)
	rows, err := s.db.QueryContext(ctx, query, start.UTC(), end.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]overviewBucketCounts)
	for rows.Next() {
		var bucket string
		var counts overviewBucketCounts
		if err := rows.Scan(&bucket, &counts.Requests, &counts.Opportunities, &counts.Impressions); err != nil {
			return nil, err
		}
		out[bucket] = counts
	}
	return out, rows.Err()
}

func (s *clickHouseVideoMetricsStore) Overview(ctx context.Context, period, timezone string, now time.Time) (VideoOverviewPayload, error) {
	location, resolvedTimezone := resolveOverviewLocation(timezone)
	window := buildOverviewWindow(period, now, location)
	payload := VideoOverviewPayload{
		Period:        window.period,
		Label:         window.label,
		PreviousLabel: window.previousLabel,
		Timezone:      resolvedTimezone,
		UpdatedAt:     now.In(location).Format(time.RFC3339),
		Source:        "clickhouse",
	}
	currentStats, err := s.queryVideoStatsRange(ctx, window.currentStart, window.currentEnd)
	if err != nil {
		return payload, err
	}
	previousStats, err := s.queryVideoStatsRange(ctx, window.previousStart, window.previousEnd)
	if err != nil {
		return payload, err
	}
	payload.Current = overviewSummaryFromVideoStats(currentStats)
	payload.Previous = overviewSummaryFromVideoStats(previousStats)
	currentSeries, err := s.queryOverviewSeries(ctx, window.currentStart, window.currentEnd, window.bucketSize, resolvedTimezone)
	if err != nil {
		return payload, err
	}
	previousSeries, err := s.queryOverviewSeries(ctx, window.previousStart, window.previousEnd, window.bucketSize, resolvedTimezone)
	if err != nil {
		return payload, err
	}
	buckets, keys, labels := buildOverviewLabels(window)
	payload.Chart.Labels = labels
	payload.Chart.CurrentRequests = make([]int64, len(buckets))
	payload.Chart.CurrentOpportunities = make([]int64, len(buckets))
	payload.Chart.CurrentImpressions = make([]int64, len(buckets))
	payload.Chart.PreviousRequests = make([]int64, len(buckets))
	for index := range buckets {
		if counts, ok := currentSeries[keys[index]]; ok {
			payload.Chart.CurrentRequests[index] = counts.Requests
			payload.Chart.CurrentOpportunities[index] = counts.Opportunities
			payload.Chart.CurrentImpressions[index] = counts.Impressions
		}
		prevBucket := window.previousStart.Add(time.Duration(index) * window.bucketSize).In(location)
		if counts, ok := previousSeries[overviewBucketKey(prevBucket, window.bucketSize)]; ok {
			payload.Chart.PreviousRequests[index] = counts.Requests
		}
	}
	return payload, nil
}

func (s *clickHouseVideoMetricsStore) Reset(ctx context.Context) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`TRUNCATE TABLE %s`, s.table))
	return err
}

func (s *clickHouseVideoMetricsStore) Close() {
	if s == nil {
		return
	}
	close(s.done)
	s.wg.Wait()
	_ = s.db.Close()
}

func metricAppID(pr *PlayerRequest, cfg *AdServerConfig) string {
	if pr != nil {
		if pr.AppBundle != "" {
			return pr.AppBundle
		}
		if pr.Domain != "" {
			return pr.Domain
		}
	}
	if cfg != nil && cfg.DomainOrApp != "" {
		return cfg.DomainOrApp
	}
	return "(unknown)"
}

func metricCountry(pr *PlayerRequest) string {
	if pr != nil && pr.CountryCode != "" {
		return pr.CountryCode
	}
	return "(unknown)"
}

func metricDevice(pr *PlayerRequest) string {
	if pr == nil {
		return "Unknown"
	}
	return deviceTypeLabel(pr.DeviceType)
}

func metricFormat(cfg *AdServerConfig) string {
	if cfg != nil && cfg.VideoPlacementType != "" {
		return cfg.VideoPlacementType
	}
	return "(unknown)"
}

func (h *VideoPipelineHandler) recordRequestMetric(pr *PlayerRequest, cfg *AdServerConfig) {
	if h.clickHouseMetrics == nil || cfg == nil {
		return
	}
	h.clickHouseMetrics.Record(videoMetricEvent{
		EventTime:     time.Now().UTC(),
		EventType:     "request",
		PlacementID:   cfg.PlacementID,
		PublisherID:   cfg.PublisherID,
		AdvertiserID:  cfg.AdvertiserID,
		AppID:         metricAppID(pr, cfg),
		Country:       metricCountry(pr),
		Device:        metricDevice(pr),
		Format:        metricFormat(cfg),
		DemandChannel: demandChannelLabel(resolveDemandType(cfg)),
	})
}

func (h *VideoPipelineHandler) recordOpportunityMetric(pr *PlayerRequest, cfg *AdServerConfig, resp *DemandResponse) {
	if h.clickHouseMetrics == nil || cfg == nil || resp == nil || resp.NoFill {
		return
	}
	bidder := resp.Bidder
	if bidder == "" {
		bidder = "Unknown"
	}
	h.clickHouseMetrics.Record(videoMetricEvent{
		EventTime:     time.Now().UTC(),
		EventType:     "opportunity",
		PlacementID:   cfg.PlacementID,
		PublisherID:   cfg.PublisherID,
		AdvertiserID:  cfg.AdvertiserID,
		Bidder:        bidder,
		AppID:         metricAppID(pr, cfg),
		Country:       metricCountry(pr),
		Device:        metricDevice(pr),
		Format:        metricFormat(cfg),
		DemandChannel: demandChannelLabel(resolveDemandType(cfg)),
		AuctionID:     resp.AuctionID,
		PriceCPM:      resp.WinPrice,
	})
}

func (h *VideoPipelineHandler) recordImpressionMetric(placementID, auctionID, bidID, bidder, crid string, priceCPM float64, cfg *AdServerConfig, dk *auctionDimKey) {
	if h.clickHouseMetrics == nil {
		return
	}
	ev := videoMetricEvent{
		EventTime:           time.Now().UTC(),
		EventType:           "impression",
		PlacementID:         placementID,
		PublisherID:         "unknown",
		AuctionID:           auctionID,
		BidID:               bidID,
		Bidder:              bidder,
		CrID:                crid,
		PriceCPM:            priceCPM,
		RevenueUSD:          priceCPM / 1000,
		PublisherRevenueUSD: 0,
		Country:             "(unknown)",
		Device:              "Unknown",
		Format:              "(unknown)",
		DemandChannel:       "Unknown",
		AppID:               "(unknown)",
	}
	if cfg != nil {
		ev.PublisherID = cfg.PublisherID
		ev.AdvertiserID = cfg.AdvertiserID
		ev.Format = metricFormat(cfg)
		ev.AppID = metricAppID(nil, cfg)
		ev.DemandChannel = demandChannelLabel(resolveDemandType(cfg))
		ev.PublisherRevenueUSD = cfg.FloorCPM / 1000
	}
	if dk != nil {
		ev.PublisherID = dk.PublisherID
		ev.AdvertiserID = dk.AdvertiserID
		ev.Bidder = dk.Bidder
		ev.AppID = dk.App
		ev.Country = dk.Country
		ev.Device = dk.Device
		ev.Format = dk.Format
		ev.DemandChannel = dk.DemandCh
		ev.PriceCPM = dk.PriceCPM
		ev.RevenueUSD = dk.PriceCPM / 1000
		ev.PublisherRevenueUSD = dk.PublisherPriceCPM / 1000
	}
	h.clickHouseMetrics.Record(ev)
}

func (h *VideoPipelineHandler) recordTrackingMetric(ev TrackingEvent, cfg *AdServerConfig, dk *auctionDimKey) {
	if h.clickHouseMetrics == nil {
		return
	}
	record := videoMetricEvent{
		EventTime:     ev.ReceivedAt.UTC(),
		EventType:     string(ev.Event),
		PlacementID:   ev.PlacementID,
		PublisherID:   "unknown",
		AuctionID:     ev.AuctionID,
		BidID:         ev.BidID,
		Bidder:        ev.Bidder,
		CrID:          ev.CrID,
		PriceCPM:      ev.Price,
		Country:       "(unknown)",
		Device:        "Unknown",
		Format:        "(unknown)",
		DemandChannel: "Unknown",
		AppID:         "(unknown)",
	}
	if cfg != nil {
		record.PublisherID = cfg.PublisherID
		record.AdvertiserID = cfg.AdvertiserID
		record.Format = metricFormat(cfg)
		record.AppID = metricAppID(nil, cfg)
		record.DemandChannel = demandChannelLabel(resolveDemandType(cfg))
	}
	if dk != nil {
		record.PublisherID = dk.PublisherID
		record.AdvertiserID = dk.AdvertiserID
		record.Bidder = dk.Bidder
		record.AppID = dk.App
		record.Country = dk.Country
		record.Device = dk.Device
		record.Format = dk.Format
		record.DemandChannel = dk.DemandCh
		record.PriceCPM = dk.PriceCPM
	}
	h.clickHouseMetrics.Record(record)
}

func (h *VideoPipelineHandler) snapshotVideoMetrics() VideoStatsPayload {
	if h.clickHouseMetrics != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		payload, err := h.clickHouseMetrics.Snapshot(ctx)
		if err == nil {
			return payload
		}
		log.Printf("clickhouse metrics: snapshot failed, falling back to memory: %v", err)
	}
	return h.videoStats.snapshot()
}

func (h *VideoPipelineHandler) snapshotOverviewMetrics(ctx context.Context, period, timezone string, now time.Time) (VideoOverviewPayload, error) {
	location, resolvedTimezone := resolveOverviewLocation(timezone)
	window := buildOverviewWindow(period, now, location)
	if h.clickHouseMetrics != nil {
		queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		payload, err := h.clickHouseMetrics.Overview(queryCtx, window.period, resolvedTimezone, now)
		if err == nil {
			return payload, nil
		}
		log.Printf("clickhouse metrics: overview failed, falling back to snapshot totals: %v", err)
	}
	snapshot := h.snapshotVideoMetrics().Total
	summary := overviewSummaryFromVideoStats(snapshot)
	return VideoOverviewPayload{
		Period:        window.period,
		Label:         window.label,
		PreviousLabel: window.previousLabel,
		Timezone:      resolvedTimezone,
		Current:       summary,
		Previous:      VideoOverviewSummary{},
		Chart:         VideoOverviewChart{Labels: []string{window.label}, CurrentRequests: []int64{summary.AdRequests}, CurrentOpportunities: []int64{summary.Opportunities}, CurrentImpressions: []int64{summary.Impressions}, PreviousRequests: []int64{0}},
		UpdatedAt:     now.In(location).Format(time.RFC3339),
		Source:        "snapshot-fallback",
	}, nil
}

func (h *VideoPipelineHandler) resetVideoMetrics() error {
	h.videoStats.reset()
	if h.clickHouseMetrics == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return h.clickHouseMetrics.Reset(ctx)
}
