package endpoints

import (
	"testing"
	"time"
)

func TestBuildOverviewWindowToday(t *testing.T) {
	now := time.Date(2026, 3, 15, 16, 45, 0, 0, time.UTC)
	window := buildOverviewWindow("today", now, time.UTC)
	if window.period != "today" {
		t.Fatalf("expected today period, got %q", window.period)
	}
	if window.currentStart.Hour() != 7 || window.currentStart.Minute() != 0 {
		t.Fatalf("expected currentStart at 07:00, got %s", window.currentStart)
	}
	if !window.previousEnd.Equal(window.previousStart.Add(window.currentEnd.Sub(window.currentStart))) {
		t.Fatalf("expected previous window to match current duration")
	}
	if window.bucketSize != time.Hour {
		t.Fatalf("expected hourly buckets, got %s", window.bucketSize)
	}
}

func TestBuildOverviewWindowTodayUsesRequestedTimezone(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		t.Fatalf("failed to load location: %v", err)
	}
	now := time.Date(2026, 3, 15, 17, 8, 0, 0, time.UTC)
	window := buildOverviewWindow("today", now, loc)
	if got, want := window.currentStart.Format(time.RFC3339), "2026-03-15T07:00:00+07:00"; got != want {
		t.Fatalf("expected currentStart %s, got %s", want, got)
	}
	if got, want := window.previousStart.Format(time.RFC3339), "2026-03-14T07:00:00+07:00"; got != want {
		t.Fatalf("expected previousStart %s, got %s", want, got)
	}
}

func TestBuildOverviewLabelsYesterdayUses24BucketsFromSevenAM(t *testing.T) {
	window := overviewWindow{
		period:       "yesterday",
		location:     time.UTC,
		currentStart: time.Date(2026, 3, 15, 7, 0, 0, 0, time.UTC),
		currentEnd:   time.Date(2026, 3, 16, 7, 0, 0, 0, time.UTC),
		bucketSize:   time.Hour,
	}
	buckets, _, labels := buildOverviewLabels(window)
	if len(buckets) != 24 {
		t.Fatalf("expected 24 buckets, got %d", len(buckets))
	}
	if labels[0] != "07:00" {
		t.Fatalf("expected first label 07:00, got %q", labels[0])
	}
	if labels[len(labels)-1] != "06:00" {
		t.Fatalf("expected last label 06:00, got %q", labels[len(labels)-1])
	}
}

func TestResolveOverviewLocationInvalidFallsBackToUTC(t *testing.T) {
	loc, timezone := resolveOverviewLocation("Mars/Olympus")
	if loc != time.UTC {
		t.Fatalf("expected UTC fallback location, got %v", loc)
	}
	if timezone != "UTC" {
		t.Fatalf("expected UTC fallback timezone, got %q", timezone)
	}
}

func TestOverviewSummaryFromVideoStats(t *testing.T) {
	summary := overviewSummaryFromVideoStats(VideoStats{
		AdRequests:       1000,
		Opportunities:    250,
		Impressions:      125,
		Starts:           110,
		Completes:        100,
		Revenue:          12.5,
		PublisherRevenue: 8.0,
	})
	if summary.FillRate != 25 {
		t.Fatalf("expected fill rate 25, got %v", summary.FillRate)
	}
	if summary.ResponseRate != 50 {
		t.Fatalf("expected response rate 50, got %v", summary.ResponseRate)
	}
	if summary.ECPM != 100 {
		t.Fatalf("expected ecpm 100, got %v", summary.ECPM)
	}
	if summary.VCR != float64(100)/float64(110)*100 {
		t.Fatalf("expected vcr %.6f, got %v", float64(100)/float64(110)*100, summary.VCR)
	}
	if summary.Viewability != 100 {
		t.Fatalf("expected viewability 92, got %v", summary.Viewability)
	}
	if summary.Margin != 4.5 {
		t.Fatalf("expected margin 4.5, got %v", summary.Margin)
	}
}

func TestVideoCompletionRateFallsBackToImpressions(t *testing.T) {
	stats := VideoStats{Impressions: 20, Completes: 10}
	if got := videoCompletionRate(stats); got != 50 {
		t.Fatalf("expected fallback vcr 50, got %v", got)
	}
}
