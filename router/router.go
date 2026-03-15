package router

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prebid/go-gdpr/vendorlist"
	openrtb2model "github.com/prebid/openrtb/v20/openrtb2"
	analyticsBuild "github.com/prebid/prebid-server/v4/analytics/build"
	"github.com/prebid/prebid-server/v4/config"
	"github.com/prebid/prebid-server/v4/currency"
	"github.com/prebid/prebid-server/v4/endpoints"
	"github.com/prebid/prebid-server/v4/endpoints/events"
	infoEndpoints "github.com/prebid/prebid-server/v4/endpoints/info"
	"github.com/prebid/prebid-server/v4/endpoints/openrtb2"
	"github.com/prebid/prebid-server/v4/errortypes"
	"github.com/prebid/prebid-server/v4/exchange"
	"github.com/prebid/prebid-server/v4/experiment/adscert"
	"github.com/prebid/prebid-server/v4/floors"
	"github.com/prebid/prebid-server/v4/gdpr"
	"github.com/prebid/prebid-server/v4/hooks"
	"github.com/prebid/prebid-server/v4/logger"
	"github.com/prebid/prebid-server/v4/macros"
	"github.com/prebid/prebid-server/v4/metrics"
	metricsConf "github.com/prebid/prebid-server/v4/metrics/config"
	"github.com/prebid/prebid-server/v4/modules"
	"github.com/prebid/prebid-server/v4/modules/moduledeps"
	"github.com/prebid/prebid-server/v4/openrtb_ext"
	"github.com/prebid/prebid-server/v4/ortb"
	"github.com/prebid/prebid-server/v4/pbs"
	pbc "github.com/prebid/prebid-server/v4/prebid_cache_client"
	"github.com/prebid/prebid-server/v4/router/aspects"
	"github.com/prebid/prebid-server/v4/server/ssl"
	storedRequestsConf "github.com/prebid/prebid-server/v4/stored_requests/config"
	"github.com/prebid/prebid-server/v4/usersync"
	"github.com/prebid/prebid-server/v4/util/fasthttpclient"
	"github.com/prebid/prebid-server/v4/util/jsonutil"
	"github.com/prebid/prebid-server/v4/util/uuidutil"
	"github.com/prebid/prebid-server/v4/version"

	_ "github.com/go-sql-driver/mysql"
	"github.com/julienschmidt/httprouter"
	_ "github.com/lib/pq"
	"github.com/rs/cors"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

// NewJsonDirectoryServer is used to serve .json files from a directory as a single blob. For example,
// given a directory containing the files "a.json" and "b.json", this returns a Handle which serves JSON like:
//
//	{
//	  "a": { ... content from the file a.json ... },
//	  "b": { ... content from the file b.json ... }
//	}
//
// This function stores the file contents in memory, and should not be used on large directories.
// If the root directory, or any of the files in it, cannot be read, then the program will exit.
func NewJsonDirectoryServer(schemaDirectory string, validator openrtb_ext.BidderParamValidator) httprouter.Handle {
	return newJsonDirectoryServer(schemaDirectory, validator, openrtb_ext.GetAliasBidderToParent())
}

func newJsonDirectoryServer(schemaDirectory string, validator openrtb_ext.BidderParamValidator, aliases map[openrtb_ext.BidderName]openrtb_ext.BidderName) httprouter.Handle {
	// Slurp the files into memory first, since they're small and it minimizes request latency.
	files, err := os.ReadDir(schemaDirectory)
	if err != nil {
		logger.Fatalf("Failed to read directory %s: %v", schemaDirectory, err)
	}

	bidderMap := openrtb_ext.BuildBidderMap()

	data := make(map[string]json.RawMessage, len(files))
	for _, file := range files {
		bidder := strings.TrimSuffix(file.Name(), ".json")
		bidderName, isValid := bidderMap[bidder]
		if !isValid {
			logger.Fatalf("Schema exists for an unknown bidder: %s", bidder)
		}
		data[bidder] = json.RawMessage(validator.Schema(bidderName))
	}

	// Add in any aliases
	for aliasName, parentBidder := range aliases {
		data[string(aliasName)] = json.RawMessage(validator.Schema(parentBidder))
	}

	response, err := jsonutil.Marshal(data)
	if err != nil {
		logger.Fatalf("Failed to marshal bidder param JSON-schema: %v", err)
	}

	return func(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
		w.Header().Add("Content-Type", "application/json")
		w.Write(response)
	}
}

func serveIndex(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	http.Redirect(w, r, "/dashboard", http.StatusFound)
}

type NoCache struct {
	Handler http.Handler
}

func (m NoCache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Add("Pragma", "no-cache")
	w.Header().Add("Expires", "0")
	m.Handler.ServeHTTP(w, r)
}

// rateLimiter implements a simple in-process token-bucket rate limiter.
// It caps the number of concurrent in-flight requests to protect the server
// from overload. Rejected requests receive 429 Too Many Requests.
type rateLimiter struct {
	sem  chan struct{}
	next httprouter.Handle
}

func newRateLimiter(maxConcurrent int, next httprouter.Handle) httprouter.Handle {
	if maxConcurrent <= 0 {
		return next
	}
	rl := &rateLimiter{sem: make(chan struct{}, maxConcurrent), next: next}
	return rl.handle
}

func (rl *rateLimiter) handle(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	select {
	case rl.sem <- struct{}{}:
		defer func() { <-rl.sem }()
		rl.next(w, r, ps)
	default:
		w.Header().Set("Retry-After", "1")
		http.Error(w, "too many requests", http.StatusTooManyRequests)
	}
}

type Router struct {
	*httprouter.Router
	MetricsEngine    *metricsConf.DetailedMetricsEngine
	ParamsValidator  openrtb_ext.BidderParamValidator
	cfg              *config.Configuration
	videoPipeline    *endpoints.VideoPipelineHandler
	fastStatus       fasthttp.RequestHandler
	versionResponse  []byte
	fastHTTPFallback fasthttp.RequestHandler
	fastAdRequestSem chan struct{}

	shutdowns []func()
}

// FastHTTPHandler returns the native fasthttp hot-path dispatcher with a
// fallback to the existing httprouter stack for non-hot endpoints.
func (r *Router) FastHTTPHandler() fasthttp.RequestHandler {
	return r.fastHandler()
}

// ListenAndServe starts a fasthttp server with the adapted handler. This keeps
// the existing route definitions while switching the underlying server to the
// faster fasthttp engine.
func (r *Router) ListenAndServe(addr string) error {
	return fasthttp.ListenAndServe(addr, r.FastHTTPHandler())
}

func New(cfg *config.Configuration, rateConverter *currency.RateConverter) (r *Router, err error) {
	const schemaDirectory = "./static/bidder-params"

	r = &Router{
		Router: httprouter.New(),
		cfg:    cfg,
	}

	r.fastStatus = endpoints.NewFastStatusHandler(cfg.StatusResponse, cfg)
	versionValue := version.Ver
	if versionValue == "" {
		versionValue = "not-set"
	}
	revisionValue := version.Rev
	if revisionValue == "" {
		revisionValue = "not-set"
	}
	r.versionResponse, err = jsonutil.Marshal(struct {
		Revision string `json:"revision"`
		Version  string `json:"version"`
	}{
		Revision: revisionValue,
		Version:  versionValue,
	})
	if err != nil {
		logger.Fatalf("error creating /version endpoint response: %v", err)
	}

	certPool, certPoolCreateErr := ssl.CreateCertPool()
	if certPoolCreateErr != nil {
		logger.Infof("Could not load root certificates: %s \n", certPoolCreateErr.Error())
	}

	// load optional PEM certificate files
	var readCertErr error
	certPool, readCertErr = ssl.AppendPEMFileToCertPool(certPool, cfg.PemCertsFile)
	if readCertErr != nil {
		logger.Infof("Could not read certificates file: %s \n", readCertErr.Error())
	}

	generalHttpClient := fasthttpclient.NewClient(0, fasthttpclient.TransportConfig{
		Name:                "pbs-general",
		DialTimeout:         time.Duration(cfg.Client.Dialer.TimeoutSeconds) * time.Second,
		KeepAlive:           time.Duration(cfg.Client.Dialer.KeepAliveSeconds) * time.Second,
		MaxConnsPerHost:     cfg.Client.MaxConnsPerHost,
		MaxIdleConnDuration: time.Duration(cfg.Client.IdleConnTimeout) * time.Second,
		ReadTimeout:         time.Duration(cfg.Client.TLSHandshakeTimeout) * time.Second,
		WriteTimeout:        time.Duration(cfg.Client.ExpectContinueTimeout) * time.Second,
		TLSConfig:           &tls.Config{RootCAs: certPool},
		ReadBufferSize:      8192,
		WriteBufferSize:     8192,
	})

	cacheHttpClient := fasthttpclient.NewClient(0, fasthttpclient.TransportConfig{
		Name:                "pbs-cache",
		DialTimeout:         time.Duration(cfg.CacheClient.Dialer.TimeoutSeconds) * time.Second,
		KeepAlive:           time.Duration(cfg.CacheClient.Dialer.KeepAliveSeconds) * time.Second,
		MaxConnsPerHost:     cfg.CacheClient.MaxConnsPerHost,
		MaxIdleConnDuration: time.Duration(cfg.CacheClient.IdleConnTimeout) * time.Second,
		ReadTimeout:         time.Duration(cfg.CacheClient.TLSHandshakeTimeout) * time.Second,
		WriteTimeout:        time.Duration(cfg.CacheClient.ExpectContinueTimeout) * time.Second,
		ReadBufferSize:      8192,
		WriteBufferSize:     8192,
	})

	floorFetcherHTTPClient := fasthttpclient.NewClient(0, fasthttpclient.TransportConfig{
		Name:                "pbs-floors",
		DialTimeout:         time.Duration(cfg.PriceFloors.Fetcher.HttpClient.Dialer.TimeoutSeconds) * time.Second,
		KeepAlive:           time.Duration(cfg.PriceFloors.Fetcher.HttpClient.Dialer.KeepAliveSeconds) * time.Second,
		MaxConnsPerHost:     cfg.PriceFloors.Fetcher.HttpClient.MaxConnsPerHost,
		MaxIdleConnDuration: time.Duration(cfg.PriceFloors.Fetcher.HttpClient.IdleConnTimeout) * time.Second,
		ReadTimeout:         time.Duration(cfg.PriceFloors.Fetcher.HttpClient.TLSHandshakeTimeout) * time.Second,
		WriteTimeout:        time.Duration(cfg.PriceFloors.Fetcher.HttpClient.ExpectContinueTimeout) * time.Second,
		ReadBufferSize:      8192,
		WriteBufferSize:     8192,
	})

	if err := checkSupportedUserSyncEndpoints(cfg.BidderInfos); err != nil {
		return nil, err
	}

	syncersByBidder, errs := usersync.BuildSyncers(cfg, cfg.BidderInfos)
	if len(errs) > 0 {
		return nil, errortypes.NewAggregateError("user sync", errs)
	}

	syncerKeys := make([]string, 0, len(syncersByBidder))
	syncerKeysHashSet := map[string]struct{}{}
	for _, syncer := range syncersByBidder {
		syncerKeysHashSet[syncer.Key()] = struct{}{}
	}
	for k := range syncerKeysHashSet {
		syncerKeys = append(syncerKeys, k)
	}

	normalizedGeoscopes := getNormalizedGeoscopes(cfg.BidderInfos)
	moduleDeps := moduledeps.ModuleDeps{HTTPClient: generalHttpClient, RateConvertor: rateConverter, Geoscope: normalizedGeoscopes}
	repo, moduleStageNames, shutdownModules, err := modules.NewBuilder().Build(cfg.Hooks.Modules, moduleDeps)
	if err != nil {
		logger.Fatalf("Failed to init hook modules: %v", err)
	}

	// Metrics engine
	r.MetricsEngine = metricsConf.NewMetricsEngine(cfg, openrtb_ext.CoreBidderNames(), syncerKeys, moduleStageNames)
	shutdown, fetcher, ampFetcher, accounts, categoriesFetcher, videoFetcher, storedRespFetcher := storedRequestsConf.NewStoredRequests(cfg, r.MetricsEngine, generalHttpClient, r.Router)

	analyticsRunner := analyticsBuild.New(cfg.Analytics)

	paramsValidator, err := openrtb_ext.NewBidderParamsValidator(schemaDirectory)
	if err != nil {
		logger.Fatalf("Failed to create the bidder params validator. %v", err)
	}

	activeBidders := exchange.GetActiveBidders(cfg.BidderInfos)
	disabledBidders := exchange.GetDisabledBidderWarningMessages(cfg.BidderInfos)

	defReqJSON := readDefaultRequest(cfg.DefReqConfig)

	gvlVendorIDs := cfg.BidderInfos.ToGVLVendorIDMap()
	liveGVLVendorIDs := gdpr.NewLiveGVLVendorIDs()

	var vendorListFetcher gdpr.VendorListFetcher
	if cfg.GDPR.Enabled {
		vendorListFetcher = gdpr.NewVendorListFetcher(context.Background(), cfg.GDPR, generalHttpClient, r.MetricsEngine, gdpr.VendorListURLMaker)
		refreshInterval := time.Duration(cfg.GDPR.LiveGVLRefreshInterval) * time.Second
		gvlVendorIDTask := gdpr.NewGVLVendorIDTickerTask(refreshInterval, generalHttpClient, gdpr.VendorListURLMaker, liveGVLVendorIDs, r.MetricsEngine)
		gvlVendorIDTask.Start()
		r.shutdowns = append(r.shutdowns, gvlVendorIDTask.Stop)
	} else {
		vendorListFetcher = func(ctx context.Context, specVersion uint16, listVersion uint16, me metrics.MetricsEngine) (vendorlist.VendorList, error) {
			return nil, nil
		}
	}
	gdprPermsBuilder := gdpr.NewPermissionsBuilder(cfg.GDPR, gvlVendorIDs, liveGVLVendorIDs, vendorListFetcher, r.MetricsEngine)
	tcf2CfgBuilder := gdpr.NewTCF2Config

	// register the analytics runner, modules for shutdown
	r.shutdowns = append(r.shutdowns, shutdown, analyticsRunner.Shutdown, shutdownModules.Shutdown)

	cacheClient := pbc.NewClient(cacheHttpClient, &cfg.CacheURL, &cfg.ExtCacheURL, r.MetricsEngine)

	adapters, singleFormatAdapters, adaptersErrs := exchange.BuildAdapters(generalHttpClient, cfg, cfg.BidderInfos, r.MetricsEngine)
	if len(adaptersErrs) > 0 {
		errs := errortypes.NewAggregateError("Failed to initialize adapters", adaptersErrs)
		return nil, errs
	}
	adsCertSigner, err := adscert.NewAdCertsSigner(cfg.Experiment.AdCerts)
	if err != nil {
		logger.Fatalf("Failed to create ads cert signer: %v", err)
	}

	requestValidator := ortb.NewRequestValidator(activeBidders, disabledBidders, paramsValidator)
	priceFloorFetcher := floors.NewPriceFloorFetcher(cfg.PriceFloors, floorFetcherHTTPClient, r.MetricsEngine)

	tmaxAdjustments := exchange.ProcessTMaxAdjustments(cfg.TmaxAdjustments)
	planBuilder := hooks.NewExecutionPlanBuilder(cfg.Hooks, repo)
	macroReplacer := macros.NewStringIndexBasedReplacer()
	theExchange := exchange.NewExchange(adapters, cacheClient, cfg, requestValidator, syncersByBidder, r.MetricsEngine, cfg.BidderInfos, gdprPermsBuilder, rateConverter, categoriesFetcher, adsCertSigner, macroReplacer, priceFloorFetcher, singleFormatAdapters)
	var uuidGenerator uuidutil.UUIDRandomGenerator
	openrtbEndpoint, err := openrtb2.NewEndpoint(uuidGenerator, theExchange, requestValidator, fetcher, accounts, cfg, r.MetricsEngine, analyticsRunner, disabledBidders, defReqJSON, activeBidders, storedRespFetcher, planBuilder, tmaxAdjustments)
	if err != nil {
		logger.Fatalf("Failed to create the openrtb2 endpoint handler. %v", err)
	}

	ampEndpoint, err := openrtb2.NewAmpEndpoint(uuidGenerator, theExchange, requestValidator, ampFetcher, accounts, cfg, r.MetricsEngine, analyticsRunner, disabledBidders, defReqJSON, activeBidders, storedRespFetcher, planBuilder, tmaxAdjustments)
	if err != nil {
		logger.Fatalf("Failed to create the amp endpoint handler. %v", err)
	}

	videoEndpoint, err := openrtb2.NewVideoEndpoint(uuidGenerator, theExchange, requestValidator, fetcher, videoFetcher, accounts, cfg, r.MetricsEngine, analyticsRunner, disabledBidders, defReqJSON, activeBidders, cacheClient, tmaxAdjustments)
	if err != nil {
		logger.Fatalf("Failed to create the video endpoint handler. %v", err)
	}

	requestTimeoutHeaders := config.RequestTimeoutHeaders{}
	if cfg.RequestTimeoutHeaders != requestTimeoutHeaders {
		videoEndpoint = aspects.QueuedRequestTimeout(videoEndpoint, cfg.RequestTimeoutHeaders, r.MetricsEngine, metrics.ReqTypeVideo)
	}

	// Rate-limit high-QPS ad-serving endpoints to prevent overload.
	// 500 concurrent in-flight requests per endpoint; excess gets 429.
	const maxConcurrentAdRequests = 500
	r.POST("/openrtb2/auction", newRateLimiter(maxConcurrentAdRequests, openrtbEndpoint))
	r.POST("/openrtb2/video", newRateLimiter(maxConcurrentAdRequests, videoEndpoint))
	r.GET("/openrtb2/amp", newRateLimiter(maxConcurrentAdRequests, ampEndpoint))
	r.GET("/info/bidders", infoEndpoints.NewBiddersEndpoint(cfg.BidderInfos))
	r.GET("/info/bidders/:bidderName", infoEndpoints.NewBiddersDetailEndpoint(cfg.BidderInfos))
	r.GET("/bidders/params", NewJsonDirectoryServer(schemaDirectory, paramsValidator))
	r.POST("/cookie_sync", endpoints.NewCookieSyncEndpoint(syncersByBidder, cfg, gdprPermsBuilder, tcf2CfgBuilder, r.MetricsEngine, analyticsRunner, accounts, activeBidders).Handle)
	r.GET("/status", endpoints.NewStatusEndpoint(cfg.StatusResponse, cfg))
	r.GET("/", serveIndex)
	r.Handler("GET", "/version", endpoints.NewVersionEndpoint(version.Ver, version.Rev))
	r.ServeFiles("/static/*filepath", http.Dir("static"))

	// Dashboard – unauthenticated login/logout endpoints
	r.GET("/dashboard/login", endpoints.NewDashboardLoginGetHandler())
	r.POST("/dashboard/login", endpoints.NewDashboardLoginPostHandler())
	r.POST("/dashboard/logout", endpoints.NewDashboardLogoutHandler())

	// auth wraps a handler behind the dashboard session check.
	auth := endpoints.DashboardAuthMiddleware

	// Dashboard – main SPA + section landing pages (all auth-protected)
	r.GET("/dashboard", auth(endpoints.NewDashboardHandler()))
	r.GET("/dashboard/bidder", auth(endpoints.NewDashboardHandler()))
	r.GET("/dashboard/audience", auth(endpoints.NewDashboardHandler()))
	r.GET("/dashboard/optimization", auth(endpoints.NewDashboardHandler()))
	r.GET("/dashboard/quality", auth(endpoints.NewDashboardHandler()))
	r.GET("/dashboard/backend", auth(endpoints.NewDashboardHandler()))
	r.GET("/dashboard/stats", auth(endpoints.NewDashboardStatsHandler(r.MetricsEngine)))

	// Dashboard CRUD — all entities registered via a single central registry.
	// WireVideoExchange is called after videoPipeline is constructed below so
	// the registry can inject the pipeline's RegisterAdServerConfig callback.
	dashReg := endpoints.NewDashboardRegistry("./data")

	// Video Ad Pipeline (stages 1-7):
	//   GET/POST /video/vast  — VAST inbound: VAST→VAST, VAST→ORTB, or Prebid fallback
	//   GET/POST /video/ortb  — ORTB inbound: ORTB→ORTB, ORTB→VAST, or Prebid fallback
	//   GET      /video/impression       — VAST <Impression> beacon (counts impression)
	//   GET      /video/tracking         — playback tracking beacon (start/quartile/complete)
	//   GET      /video/tracking/events  — list recorded tracking events (debug/dashboard)
	//   GET/POST /video/adserver         — ad server config CRUD
	videoPipeline := endpoints.NewVideoPipelineHandler(theExchange, cfg, r.MetricsEngine, "./data")
	r.shutdowns = append(r.shutdowns, videoPipeline.Shutdown)
	r.videoPipeline = videoPipeline
	r.fastHTTPFallback = fasthttpadaptor.NewFastHTTPHandler(r.Router)
	vastEp := videoPipeline.VASTEndpoint()
	ortbEp := videoPipeline.ORTBEndpoint()
	r.fastAdRequestSem = make(chan struct{}, maxConcurrentAdRequests)
	r.GET("/video/vast", newRateLimiter(maxConcurrentAdRequests, vastEp))
	r.POST("/video/vast", newRateLimiter(maxConcurrentAdRequests, vastEp))
	r.GET("/video/ortb", newRateLimiter(maxConcurrentAdRequests, ortbEp))
	r.POST("/video/ortb", newRateLimiter(maxConcurrentAdRequests, ortbEp))
	r.GET("/video/impression", videoPipeline.ImpressionEndpoint())
	r.GET("/video/tracking", videoPipeline.TrackingEndpoint())
	r.GET("/video/tracking/events", auth(videoPipeline.TrackingEventsEndpoint()))
	r.GET("/video/adserver", auth(videoPipeline.AdServerConfigEndpoint()))
	r.POST("/video/adserver", auth(videoPipeline.AdServerConfigEndpoint()))
	r.DELETE("/video/adserver/:placement_id", auth(videoPipeline.AdServerConfigEndpoint()))
	r.GET("/dashboard/stats/video", auth(videoPipeline.VideoStatsEndpoint()))
	r.GET("/dashboard/stats/video/overview", auth(videoPipeline.VideoOverviewStatsEndpoint()))
	r.POST("/dashboard/stats/reset", auth(videoPipeline.ResetStatsEndpoint()))
	r.GET("/dashboard/config", auth(videoPipeline.DashboardConfigEndpoint()))

	// Wire demand-routing dependencies into the VideoExchange handler, wire live
	// video stats into the supply/demand partner list endpoints, then register
	// all dashboard routes (auth-protected) via the central registry.
	dashReg.WireVideoExchange(videoPipeline.RegisterAdServerConfig, videoPipeline.UnregisterAdServerConfig, videoPipeline.LookupAdServerConfig)
	dashReg.WireVideoStats(videoPipeline.Snapshot)
	dashReg.WireRevenueConsoleMasters()
	dashReg.Register(r.Router, auth)

	// Dashboard – External Statistics API proxy (admin-only, HTTPS only)
	r.POST("/dashboard/ext-stats/fetch", auth(endpoints.NewExtStatsFetchHandler()))
	r.POST("/dashboard/backend/bridge", auth(endpoints.NewBackendBridgeHandler(generalHttpClient, cfg.BackendBridge)))

	// vtrack endpoint
	if cfg.VTrack.Enabled {
		vtrackEndpoint := events.NewVTrackEndpoint(cfg, accounts, cacheClient, cfg.BidderInfos, r.MetricsEngine)
		r.POST("/vtrack", vtrackEndpoint)
	}

	// event endpoint
	eventEndpoint := events.NewEventEndpoint(cfg, accounts, analyticsRunner, r.MetricsEngine)
	r.GET("/event", eventEndpoint)

	userSyncDeps := &pbs.UserSyncDeps{
		HostCookieConfig: &(cfg.HostCookie),
		ExternalUrl:      cfg.ExternalURL,
		RecaptchaSecret:  cfg.RecaptchaSecret,
		PriorityGroups:   cfg.UserSync.PriorityGroups,
		CertPool:         certPool,
	}

	r.GET("/setuid", endpoints.NewSetUIDEndpoint(cfg, syncersByBidder, gdprPermsBuilder, tcf2CfgBuilder, analyticsRunner, accounts, r.MetricsEngine))
	r.GET("/getuids", endpoints.NewGetUIDsEndpoint(cfg.HostCookie))
	r.POST("/optout", userSyncDeps.OptOut)
	r.GET("/optout", userSyncDeps.OptOut)

	return r, nil
}

// defaultTransportDialContext returns the same dialer context as the default transport uses, copied from the library code.
func defaultTransportDialContext(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return dialer.DialContext
}

// Shutdown closes any dependencies of the router that may need closing
func (r *Router) Shutdown() {
	logger.Infof("[PBS Router] shutting down")
	for _, shutdown := range r.shutdowns {
		shutdown()
	}
	logger.Infof("[PBS Router] shut down")
}

func checkSupportedUserSyncEndpoints(bidderInfos config.BidderInfos) error {
	for name, info := range bidderInfos {
		if info.Syncer == nil {
			continue
		}

		for _, endpoint := range info.Syncer.Supports {
			endpointLower := strings.ToLower(endpoint)
			switch endpointLower {
			case "iframe":
				if info.Syncer.IFrame == nil {
					logger.Warnf("bidder %s supports iframe user sync, but doesn't have a default and must be configured by the host", name)
				}
			case "redirect":
				if info.Syncer.Redirect == nil {
					logger.Warnf("bidder %s supports redirect user sync, but doesn't have a default and must be configured by the host", name)
				}
			default:
				return fmt.Errorf("failed to load bidder info for %s, user sync supported endpoint '%s' is unrecognized", name, endpoint)
			}
		}
	}
	return nil
}

// Fixes #648
//
// These CORS options pose a security risk... but it's a calculated one.
// People _must_ call us with "withCredentials" set to "true" because that's how we use the cookie sync info.
// We also must allow all origins because every site on the internet _could_ call us.
//
// This is an inherent security risk. However, PBS doesn't use cookies for authorization--just identification.
// We only store the User's ID for each Bidder, and each Bidder has already exposed a public cookie sync endpoint
// which returns that data anyway.
//
// For more info, see:
//
// - https://github.com/rs/cors/issues/55
// - https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS/Errors/CORSNotSupportingCredentials
// - https://portswigger.net/blog/exploiting-cors-misconfigurations-for-bitcoins-and-bounties
func SupportCORS(handler http.Handler) http.Handler {
	c := cors.New(cors.Options{
		AllowCredentials: true,
		AllowOriginFunc: func(string) bool {
			return true
		},
		AllowedHeaders: []string{"Origin", "X-Requested-With", "Content-Type", "Accept"},
		AllowedMethods: []string{"GET", "POST", "OPTIONS"},
		MaxAge:         600,
	})
	return securityHeaders(c.Handler(handler))
}

// securityHeaders wraps a handler to inject security-related response headers
// on every response, providing defense-in-depth against XSS, clickjacking,
// and MIME-sniffing attacks.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "strict-origin-when-cross-origin")
		h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		next.ServeHTTP(w, r)
	})
}

func readDefaultRequest(defReqConfig config.DefReqConfig) []byte {
	switch defReqConfig.Type {
	case "file":
		return readDefaultRequestFromFile(defReqConfig)
	default:
		return []byte{}
	}
}

func readDefaultRequestFromFile(defReqConfig config.DefReqConfig) []byte {
	if len(defReqConfig.FileSystem.FileName) == 0 {
		return []byte{}
	}

	defaultRequestJSON, err := os.ReadFile(defReqConfig.FileSystem.FileName)
	if err != nil {
		logger.Fatalf("error reading default request from file %s: %v", defReqConfig.FileSystem.FileName, err)
		return []byte{}
	}

	// validate json is valid
	if err := jsonutil.UnmarshalValid(defaultRequestJSON, &openrtb2model.BidRequest{}); err != nil {
		logger.Fatalf("error parsing default request from file %s: %v", defReqConfig.FileSystem.FileName, err)
		return []byte{}
	}

	return defaultRequestJSON
}

func getNormalizedGeoscopes(bidderInfos config.BidderInfos) map[string][]string {
	geoscopes := make(map[string][]string, len(bidderInfos))

	for name, info := range bidderInfos {
		if len(info.Geoscope) > 0 {
			uppercasedGeoscopes := make([]string, len(info.Geoscope))
			for i, scope := range info.Geoscope {
				uppercasedGeoscopes[i] = strings.ToUpper(scope)
			}
			geoscopes[name] = uppercasedGeoscopes
		}
	}
	return geoscopes
}
