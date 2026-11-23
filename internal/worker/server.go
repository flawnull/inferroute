// Package worker implements a simulated GPU inference worker that speaks the
// WorkerService gRPC API.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "inferroute/gen/inferencepb"
	"inferroute/internal/ewma"
)

// Config describes a worker's simulated hardware profile.
type Config struct {
	ID          string
	TotalVRAMMB int64
	BaseLatency time.Duration
	Jitter      time.Duration
	// PerTokenLatency is added for each requested output token (decode cost).
	PerTokenLatency time.Duration
	// Contention inflates latency by this fraction per concurrent request,
	// modelling batch interference on a shared GPU.
	Contention float64
	Faults     FaultConfig
}

type Server struct {
	pb.UnimplementedWorkerServiceServer

	cfg      Config
	log      *slog.Logger
	vram     *VRAMPool
	faults   *Faults
	ewma     *ewma.EWMA
	active   atomic.Int64
	draining atomic.Bool
	metrics  *metrics
	sleep    func(ctx context.Context, d time.Duration) error
}

func NewServer(cfg Config, log *slog.Logger, reg prometheus.Registerer) *Server {
	if log == nil {
		log = slog.Default()
	}
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	s := &Server{
		cfg:    cfg,
		log:    log.With("worker_id", cfg.ID),
		vram:   NewVRAMPool(cfg.TotalVRAMMB),
		faults: NewFaults(cfg.Faults),
		ewma:   ewma.New(0.2),
		sleep:  sleepCtx,
	}
	s.metrics = newMetrics(reg, s)
	return s
}

func (s *Server) Faults() *Faults { return s.faults }
func (s *Server) VRAM() *VRAMPool { return s.vram }
func (s *Server) Active() int64   { return s.active.Load() }
func (s *Server) Draining() bool  { return s.draining.Load() }

func (s *Server) Infer(ctx context.Context, req *pb.InferRequest) (*pb.InferResponse, error) {
	resp, err := s.infer(ctx, req)
	s.metrics.requests.WithLabelValues(status.Code(err).String()).Inc()
	return resp, err
}

func (s *Server) infer(ctx context.Context, req *pb.InferRequest) (*pb.InferResponse, error) {
	// Increment before checking draining so Drain never observes zero active
	// requests while one is about to start.
	active := s.active.Add(1)
	defer s.active.Add(-1)
	if s.draining.Load() {
		return nil, status.Error(codes.Unavailable, "worker draining")
	}
	if req.GetRequiredVramMb() <= 0 || req.GetMaxTokens() < 0 || req.GetPromptTokens() < 0 {
		return nil, status.Error(codes.InvalidArgument, "required_vram_mb must be > 0 and token counts >= 0")
	}
	if !s.vram.Reserve(req.GetRequiredVramMb()) {
		return nil, status.Errorf(codes.ResourceExhausted,
			"insufficient VRAM: need %d MB, available %d MB", req.GetRequiredVramMb(), s.vram.Available())
	}
	defer s.vram.Release(req.GetRequiredVramMb())

	f := s.faults.Get()
	if f.ErrorRate > 0 && rand.Float64() < f.ErrorRate {
		return nil, status.Error(codes.Unavailable, "injected fault")
	}

	d := s.computeLatency(req, active, f)
	start := time.Now()
	if err := s.sleep(ctx, d); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	elapsed := time.Since(start)
	ms := float64(elapsed) / float64(time.Millisecond)
	s.ewma.Observe(ms)
	s.metrics.compute.Observe(elapsed.Seconds())

	completion := req.GetMaxTokens()
	if completion > 1 {
		// Generations usually stop before the cap.
		completion = completion/2 + rand.Int32N(completion/2+1)
	}
	return &pb.InferResponse{
		RequestId:        req.GetRequestId(),
		WorkerId:         s.cfg.ID,
		Output:           fmt.Sprintf("[%s] %s: %d tokens", s.cfg.ID, req.GetModelId(), completion),
		CompletionTokens: completion,
		ComputeMs:        ms,
	}, nil
}

func (s *Server) computeLatency(req *pb.InferRequest, active int64, f FaultConfig) time.Duration {
	d := s.cfg.BaseLatency + time.Duration(req.GetMaxTokens())*s.cfg.PerTokenLatency
	if s.cfg.Jitter > 0 {
		d += rand.N(s.cfg.Jitter)
	}
	if active > 1 && s.cfg.Contention > 0 {
		d = time.Duration(float64(d) * (1 + s.cfg.Contention*float64(active-1)))
	}
	d = time.Duration(float64(d) * f.Slowdown)
	return d + time.Duration(f.ExtraLatencyMs)*time.Millisecond
}

func (s *Server) Heartbeat(context.Context, *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{
		WorkerId:        s.cfg.ID,
		AvailableVramMb: s.vram.Available(),
		TotalVramMb:     s.vram.Total(),
		ActiveRequests:  int32(s.active.Load()),
		EwmaLatencyMs:   s.ewma.Value(),
		Healthy:         !s.draining.Load() && !s.faults.Get().Unhealthy,
	}, nil
}

// Drain stops accepting new work and blocks until in-flight requests finish
// or ctx expires. Heartbeats report healthy=false from the first moment, so
// routers stop scheduling here within one heartbeat interval.
func (s *Server) Drain(ctx context.Context) error {
	s.draining.Store(true)
	s.log.Info("draining", "active", s.active.Load())
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		if s.active.Load() == 0 {
			s.log.Info("drained")
			return nil
		}
		select {
		case <-ctx.Done():
			s.log.Warn("drain deadline exceeded", "active", s.active.Load())
			return ctx.Err()
		case <-t.C:
		}
	}
}

// HealthHandler answers 200 while the worker accepts work, 503 otherwise.
func (s *Server) HealthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if s.draining.Load() {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
