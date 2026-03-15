package endpoints

import (
	"testing"

	"github.com/prebid/prebid-server/v4/config"
)

func TestClickHouseHostPort(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		host string
		port string
	}{
		{
			name: "url with query database",
			dsn:  "clickhouse://127.0.0.1:9000?database=goads",
			host: "127.0.0.1",
			port: "9000",
		},
		{
			name: "url with path database",
			dsn:  "clickhouse://127.0.0.1:9000/goads",
			host: "127.0.0.1",
			port: "9000",
		},
		{
			name: "bare host and port",
			dsn:  "127.0.0.1:9000",
			host: "127.0.0.1",
			port: "9000",
		},
		{
			name: "bare host uses default port",
			dsn:  "clickhouse.internal",
			host: "clickhouse.internal",
			port: "9000",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			host, port := clickHouseHostPort(test.dsn)
			if host != test.host || port != test.port {
				t.Fatalf("expected %s:%s, got %s:%s", test.host, test.port, host, port)
			}
		})
	}
}

func TestBuildDependencyChecksIncludesClickHouseAnalytics(t *testing.T) {
	t.Setenv("DASH_DB_DSN", "postgres://dashboard:secret@127.0.0.1:5432/goads?sslmode=disable")
	t.Setenv("CLICKHOUSE_DSN", "clickhouse://127.0.0.1:9000/goads")

	checks := buildDependencyChecks(&config.Configuration{})
	if len(checks) == 0 {
		t.Fatal("expected dependency checks")
	}

	var foundDashboard bool
	var foundClickHouse bool
	for _, check := range checks {
		switch check.name {
		case "dashboard_db":
			foundDashboard = true
			if check.target != "127.0.0.1:5432" {
				t.Fatalf("expected dashboard target 127.0.0.1:5432, got %s", check.target)
			}
		case "clickhouse_analytics":
			foundClickHouse = true
			if check.kind != "analytics" {
				t.Fatalf("expected clickhouse kind analytics, got %s", check.kind)
			}
			if check.target != "127.0.0.1:9000" {
				t.Fatalf("expected clickhouse target 127.0.0.1:9000, got %s", check.target)
			}
		}
	}
	if !foundDashboard {
		t.Fatal("expected dashboard_db dependency check")
	}
	if !foundClickHouse {
		t.Fatal("expected clickhouse_analytics dependency check")
	}
}