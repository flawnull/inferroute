// Command router runs the inferroute control plane.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"inferroute/internal/envconf"
	"inferroute/internal/router"
)

const defaultModels = "phi-3-mini=1024,llama-3-8b=2048,mistral-7b=2048,mixtral-8x7b=6144"

func main() {
	probe := flag.String("healthcheck", "", "probe this URL and exit (for container health checks)")
	flag.Parse()
	if *probe != "" {
		envconf.Probe(*probe)
	}
	log := envconf.Logger("router")
	if err := run(log); err != nil {
		log.Error("router failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	regCfg, rtCfg, disc, err := loadConfig()
	if err != nil {
		return err
	}
	reg := router.NewRegistry(regCfg, disc, log)
	rt := router.New(rtCfg, reg, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	bg, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()
	go reg.Run(bg)
	go evictLoop(bg, rt.Limiter())

	srv := &http.Server{
		Addr:              envconf.String("LISTEN_ADDR", ":8080"),
		Handler:           rt.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	log.Info("router started", "addr", srv.Addr, "models", len(rtCfg.Models), "tenant_overrides", len(rtCfg.TenantLimits))

	select {
	case <-ctx.Done():
	case err := <-errc:
		return err
	}
	// Stop accepting connections and let in-flight requests finish; keep
	// heartbeats running meanwhile so those requests can still fail over.
	log.Info("shutting down; draining in-flight requests")
	sctx, cancel := context.WithTimeout(context.Background(),
		envconf.Duration("SHUTDOWN_TIMEOUT", 15*time.Second, time.Second))
	defer cancel()
	err = srv.Shutdown(sctx)
	stopBackground()
	reg.Close()
	log.Info("router stopped")
	return err
}

// loadConfig reads all router settings from the environment.
func loadConfig() (router.RegistryConfig, router.Config, router.Discoverer, error) {
	var (
		regCfg router.RegistryConfig
		rtCfg  router.Config
	)
	disc, err := discoverer()
	if err != nil {
		return regCfg, rtCfg, nil, err
	}
	models, err := router.ParseModels(envconf.String("MODELS", defaultModels))
	if err != nil {
		return regCfg, rtCfg, nil, err
	}
	tenants, err := router.ParseTenantLimits(envconf.String("TENANT_LIMITS", ""))
	if err != nil {
		return regCfg, rtCfg, nil, err
	}
	host, _ := os.Hostname()
	regCfg = router.RegistryConfig{
		RouterID:          envconf.String("ROUTER_ID", host),
		HeartbeatInterval: envconf.Duration("HEARTBEAT_INTERVAL", 500*time.Millisecond, time.Millisecond),
		HeartbeatTimeout:  envconf.Duration("HEARTBEAT_TIMEOUT", 400*time.Millisecond, time.Millisecond),
		UnhealthyAfter:    int(envconf.Int("UNHEALTHY_AFTER", 2)),
		DiscoveryInterval: envconf.Duration("DISCOVERY_INTERVAL", 5*time.Second, time.Second),
		Breaker: router.BreakerConfig{
			FailureThreshold:  int(envconf.Int("BREAKER_FAILURE_THRESHOLD", 5)),
			OpenTimeout:       envconf.Duration("BREAKER_OPEN_TIMEOUT", 2*time.Second, time.Millisecond),
			MaxOpenTimeout:    envconf.Duration("BREAKER_MAX_OPEN_TIMEOUT", 30*time.Second, time.Millisecond),
			Jitter:            envconf.Float("BREAKER_JITTER", 0.5),
			HalfOpenMaxProbes: int(envconf.Int("BREAKER_HALF_OPEN_PROBES", 1)),
			HalfOpenSuccesses: int(envconf.Int("BREAKER_HALF_OPEN_SUCCESSES", 2)),
		},
	}
	rtCfg = router.Config{
		DefaultTimeout: envconf.Duration("REQUEST_TIMEOUT", 10*time.Second, time.Millisecond),
		MaxTimeout:     envconf.Duration("MAX_REQUEST_TIMEOUT", 60*time.Second, time.Millisecond),
		AttemptTimeout: envconf.Duration("ATTEMPT_TIMEOUT", 3*time.Second, time.Millisecond),
		MaxAttempts:    int(envconf.Int("MAX_ATTEMPTS", 2)),
		QueueTimeout:   envconf.Duration("QUEUE_TIMEOUT", time.Second, time.Millisecond),
		Models:         models,
		RateLimit: router.Limit{
			Rate:  envconf.Float("RATE_LIMIT_RPS", 100),
			Burst: envconf.Float("RATE_LIMIT_BURST", 200),
		},
		TenantLimits: tenants,
	}
	return regCfg, rtCfg, disc, nil
}

// discoverer uses WORKER_DNS (host:port, resolved periodically) when set,
// otherwise the static WORKERS list.
func discoverer() (router.Discoverer, error) {
	if d := envconf.String("WORKER_DNS", ""); d != "" {
		h, p, err := net.SplitHostPort(d)
		if err != nil {
			return nil, err
		}
		port, err := strconv.Atoi(p)
		if err != nil {
			return nil, err
		}
		return router.DNSDiscoverer{Host: h, Port: port}, nil
	}
	addrs, err := router.ParseAddrs(envconf.String("WORKERS", "localhost:50051"))
	if err != nil {
		return nil, err
	}
	return router.StaticDiscoverer(addrs), nil
}

func evictLoop(ctx context.Context, rl *router.RateLimiter) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rl.Evict()
		}
	}
}
