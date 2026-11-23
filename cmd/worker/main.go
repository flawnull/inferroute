// Command worker runs a simulated GPU inference worker.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pb "inferroute/gen/inferencepb"
	"inferroute/internal/envconf"
	"inferroute/internal/worker"
)

func main() {
	probe := flag.String("healthcheck", "", "probe this URL and exit (for container health checks)")
	flag.Parse()
	if *probe != "" {
		envconf.Probe(*probe)
	}

	if err := run(); err != nil {
		envconf.Logger("worker").Error("worker failed", "err", err)
		os.Exit(1)
	}
}

// run serves until SIGTERM or SIGINT, then drains in-flight work.
func run() error {
	host, _ := os.Hostname()
	cfg := worker.Config{
		ID:              envconf.String("WORKER_ID", host),
		TotalVRAMMB:     envconf.Int("TOTAL_VRAM_MB", 24000),
		BaseLatency:     envconf.Duration("BASE_LATENCY_MS", 50*time.Millisecond, time.Millisecond),
		Jitter:          envconf.Duration("JITTER_MS", 20*time.Millisecond, time.Millisecond),
		PerTokenLatency: envconf.Duration("PER_TOKEN_MS", 500*time.Microsecond, time.Millisecond),
		Contention:      envconf.Float("CONTENTION", 0.05),
		Faults: worker.FaultConfig{
			ErrorRate:      envconf.Float("FAULT_ERROR_RATE", 0),
			ExtraLatencyMs: envconf.Int("FAULT_EXTRA_LATENCY_MS", 0),
			Slowdown:       envconf.Float("FAULT_SLOWDOWN", 1),
			Unhealthy:      envconf.Bool("FAULT_UNHEALTHY", false),
		},
	}
	grpcAddr := envconf.String("GRPC_ADDR", ":50051")
	adminAddr := envconf.String("ADMIN_ADDR", ":9091")
	drainTimeout := envconf.Duration("DRAIN_TIMEOUT", 25*time.Second, time.Second)

	log := envconf.Logger("worker")
	reg := prometheus.NewRegistry()
	srv := worker.NewServer(cfg, log, reg)

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", grpcAddr, err)
	}
	gs := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
	)
	pb.RegisterWorkerServiceServer(gs, srv)

	admin := &http.Server{Addr: adminAddr, Handler: srv.AdminMux(reg), ReadHeaderTimeout: 5 * time.Second}

	errc := make(chan error, 2)
	go func() { errc <- gs.Serve(lis) }()
	go func() {
		if err := admin.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	log.Info("worker started", "grpc", grpcAddr, "admin", adminAddr,
		"total_vram_mb", cfg.TotalVRAMMB, "base_latency", cfg.BaseLatency.String(), "jitter", cfg.Jitter.String())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	select {
	case <-ctx.Done():
	case err := <-errc:
		return fmt.Errorf("server failed: %w", err)
	}

	log.Info("shutdown signal received")
	dctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	if err := srv.Drain(dctx); err != nil {
		log.Warn("drain timed out; stopping with requests in flight", "err", err)
	}

	done := make(chan struct{})
	go func() { gs.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-dctx.Done():
		gs.Stop()
	}
	sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer scancel()
	if err := admin.Shutdown(sctx); err != nil {
		return fmt.Errorf("admin shutdown: %w", err)
	}
	log.Info("worker stopped")
	return nil
}
