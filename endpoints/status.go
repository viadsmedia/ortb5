package endpoints

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/prebid/prebid-server/v4/config"
)

type dependencyCheck struct {
	name     string
	kind     string
	target   string
	dialAddr string
}

type dependencyStatus struct {
	Name      string `json:"name"`
	Kind      string `json:"kind"`
	Target    string `json:"target"`
	Reachable bool   `json:"reachable"`
	LatencyMS int64  `json:"latency_ms"`
	Error     string `json:"error,omitempty"`
}

type statusPayload struct {
	Status       string             `json:"status"`
	Response     string             `json:"response,omitempty"`
	Timestamp    string             `json:"timestamp"`
	Dependencies []dependencyStatus `json:"dependencies,omitempty"`
}

// NewStatusEndpoint returns a handler which writes the given response when the app is ready to serve requests.
// When dependency endpoints are configured, it also performs lightweight reachability checks and returns JSON.
func NewStatusEndpoint(response string, cfg *config.Configuration) httprouter.Handle {
	checks := buildDependencyChecks(cfg)

	// Today, the app always considers itself ready to serve requests.
	if response == "" && len(checks) == 0 {
		return func(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
			w.WriteHeader(http.StatusNoContent)
		}
	}

	if len(checks) == 0 {
		responseBytes := []byte(response)
		return func(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
			_, _ = w.Write(responseBytes)
		}
	}

	return func(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
		payload, healthy := runDependencyChecks(response, checks)
		w.Header().Set("Content-Type", "application/json")
		if healthy {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(payload)
	}
}

func buildDependencyChecks(cfg *config.Configuration) []dependencyCheck {
	if cfg == nil {
		return nil
	}
	checks := make([]dependencyCheck, 0, 9)
	addEndpointCheck := func(name, kind, scheme, host string) {
		addr := endpointDialAddr(scheme, host)
		if addr == "" {
			return
		}
		checks = append(checks, dependencyCheck{
			name:     name,
			kind:     kind,
			target:   strings.TrimSpace(host),
			dialAddr: addr,
		})
	}
	addDatabaseCheck := func(name string, dbCfg config.DatabaseConnection) {
		addr := databaseDialAddr(dbCfg)
		if addr == "" {
			return
		}
		target := dbCfg.Host
		if dbCfg.Port > 0 {
			target = net.JoinHostPort(dbCfg.Host, strconv.Itoa(dbCfg.Port))
		}
		checks = append(checks, dependencyCheck{
			name:     name,
			kind:     "database",
			target:   target,
			dialAddr: addr,
		})
	}

	addEndpointCheck("prebid_cache", "cache", cfg.CacheURL.Scheme, cfg.CacheURL.Host)
	addEndpointCheck("external_cache", "cache", cfg.ExtCacheURL.Scheme, cfg.ExtCacheURL.Host)
	addDatabaseCheck("stored_requests_db", cfg.StoredRequests.Database.ConnectionInfo)
	addDatabaseCheck("stored_amp_db", cfg.StoredRequestsAMP.Database.ConnectionInfo)
	addDatabaseCheck("stored_video_db", cfg.StoredVideo.Database.ConnectionInfo)
	addDatabaseCheck("accounts_db", cfg.Accounts.Database.ConnectionInfo)
	addDatabaseCheck("stored_responses_db", cfg.StoredResponses.Database.ConnectionInfo)
	if host, port := dashboardDBHostPort(os.Getenv("DASH_DB_DSN")); host != "" {
		checks = append(checks, dependencyCheck{
			name:     "dashboard_db",
			kind:     "database",
			target:   net.JoinHostPort(host, port),
			dialAddr: net.JoinHostPort(host, port),
		})
	}
	if host, port := clickHouseHostPort(os.Getenv("CLICKHOUSE_DSN")); host != "" {
		checks = append(checks, dependencyCheck{
			name:     "clickhouse_analytics",
			kind:     "analytics",
			target:   net.JoinHostPort(host, port),
			dialAddr: net.JoinHostPort(host, port),
		})
	}
	return checks
}

func runDependencyChecks(response string, checks []dependencyCheck) (statusPayload, bool) {
	payload := statusPayload{
		Status:       "ok",
		Response:     response,
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
		Dependencies: make([]dependencyStatus, len(checks)),
	}

	type checkResult struct {
		idx int
		dep dependencyStatus
	}
	ch := make(chan checkResult, len(checks))

	for i, check := range checks {
		idx, c := i, check
		safeGo(func() {
			started := time.Now()
			conn, err := net.DialTimeout("tcp", c.dialAddr, 2*time.Second)
			latencyMS := time.Since(started).Milliseconds()
			dep := dependencyStatus{
				Name:      c.name,
				Kind:      c.kind,
				Target:    c.target,
				Reachable: err == nil,
				LatencyMS: latencyMS,
			}
			if err != nil {
				dep.Error = err.Error()
			} else {
				_ = conn.Close()
			}
			ch <- checkResult{idx: idx, dep: dep}
		})
	}

	// Collect results with a 3s overall timeout cap so /status stays fast
	// for load-balancer health checks even when dependencies are unreachable.
	deadline := time.After(3 * time.Second)
	collected := 0
	healthy := true
	for collected < len(checks) {
		select {
		case r := <-ch:
			payload.Dependencies[r.idx] = r.dep
			if !r.dep.Reachable {
				healthy = false
				payload.Status = "degraded"
				log.Printf("status: dependency check failed name=%s kind=%s target=%s err=%s", r.dep.Name, r.dep.Kind, r.dep.Target, r.dep.Error)
			}
			collected++
		case <-deadline:
			for j := range payload.Dependencies {
				if payload.Dependencies[j].Name == "" {
					payload.Dependencies[j] = dependencyStatus{
						Name:  checks[j].name,
						Kind:  checks[j].kind,
						Target: checks[j].target,
						Error: "health check timed out",
					}
					healthy = false
					payload.Status = "degraded"
				}
			}
			return payload, healthy
		}
	}
	return payload, healthy
}

func endpointDialAddr(scheme, host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	if strings.Contains(host, "://") {
		if parsed, err := url.Parse(host); err == nil && parsed.Host != "" {
			return hostPortWithDefault(parsed.Host, parsed.Scheme)
		}
	}
	return hostPortWithDefault(host, scheme)
}

func hostPortWithDefault(host, scheme string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	if _, _, err := net.SplitHostPort(host); err == nil {
		return host
	}
	defaultPort := "80"
	if strings.EqualFold(strings.TrimSpace(scheme), "https") {
		defaultPort = "443"
	}
	return net.JoinHostPort(host, defaultPort)
}

func databaseDialAddr(cfg config.DatabaseConnection) string {
	if strings.TrimSpace(cfg.Host) == "" {
		return ""
	}
	port := cfg.Port
	if port == 0 {
		switch strings.ToLower(strings.TrimSpace(cfg.Driver)) {
		case "mysql":
			port = 3306
		default:
			port = 5432
		}
	}
	return net.JoinHostPort(strings.TrimSpace(cfg.Host), strconv.Itoa(port))
}

func dashboardDBHostPort(dsn string) (string, string) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return "", ""
	}
	if strings.Contains(dsn, "://") {
		if parsed, err := url.Parse(dsn); err == nil {
			host := parsed.Hostname()
			if host == "" {
				return "", ""
			}
			port := parsed.Port()
			if port == "" {
				port = "5432"
			}
			return host, port
		}
	}
	var host, port string
	for _, part := range strings.Fields(dsn) {
		if strings.HasPrefix(part, "host=") {
			host = strings.Trim(strings.TrimPrefix(part, "host="), "'\"")
		}
		if strings.HasPrefix(part, "port=") {
			port = strings.Trim(strings.TrimPrefix(part, "port="), "'\"")
		}
	}
	if host == "" {
		return "", ""
	}
	if port == "" {
		port = "5432"
	}
	return host, port
}

func clickHouseHostPort(dsn string) (string, string) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return "", ""
	}
	if strings.Contains(dsn, "://") {
		if parsed, err := url.Parse(dsn); err == nil {
			host := parsed.Hostname()
			if host == "" {
				return "", ""
			}
			port := parsed.Port()
			if port == "" {
				port = "9000"
			}
			return host, port
		}
	}
	if host, port, err := net.SplitHostPort(dsn); err == nil {
		if strings.TrimSpace(host) == "" {
			return "", ""
		}
		if strings.TrimSpace(port) == "" {
			port = "9000"
		}
		return host, port
	}
	return strings.TrimSpace(dsn), "9000"
}
