package worker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "inferroute/gen/inferencepb"
)

func newTestServer(t *testing.T, vram int64) *Server {
	t.Helper()
	s := NewServer(Config{ID: "w-test", TotalVRAMMB: vram, BaseLatency: time.Millisecond}, nil, prometheus.NewRegistry())
	return s
}

func req(vram int64) *pb.InferRequest {
	return &pb.InferRequest{RequestId: "r", ModelId: "m", PromptTokens: 10, MaxTokens: 16, RequiredVramMb: vram}
}

func TestVRAMPoolNeverOversubscribes(t *testing.T) {
	p := NewVRAMPool(1000)
	var ok atomic.Int64
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if p.Reserve(30) {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := ok.Load(); got != 33 {
		t.Fatalf("successful reservations = %d, want 33", got)
	}
	if p.Used() != 990 || p.Available() != 10 {
		t.Fatalf("used=%d available=%d", p.Used(), p.Available())
	}
	for range 33 {
		p.Release(30)
	}
	if p.Used() != 0 {
		t.Fatalf("used after release = %d", p.Used())
	}
}

func TestInferSuccessReleasesVRAM(t *testing.T) {
	s := newTestServer(t, 1000)
	resp, err := s.Infer(context.Background(), req(400))
	if err != nil {
		t.Fatal(err)
	}
	if resp.WorkerId != "w-test" || resp.RequestId != "r" || resp.CompletionTokens <= 0 {
		t.Fatalf("unexpected response %+v", resp)
	}
	if s.VRAM().Used() != 0 {
		t.Fatalf("VRAM leaked: %d", s.VRAM().Used())
	}
}

func TestInferResourceExhausted(t *testing.T) {
	s := newTestServer(t, 1000)
	release := make(chan struct{})
	s.sleep = func(ctx context.Context, _ time.Duration) error { <-release; return nil }

	errc := make(chan error, 1)
	go func() { _, err := s.Infer(context.Background(), req(800)); errc <- err }()
	waitFor(t, func() bool { return s.VRAM().Used() == 800 })

	_, err := s.Infer(context.Background(), req(300))
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("code = %v, want ResourceExhausted", status.Code(err))
	}
	close(release)
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestInferInvalidArgument(t *testing.T) {
	s := newTestServer(t, 1000)
	_, err := s.Infer(context.Background(), req(0))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v", status.Code(err))
	}
}

func TestInferContextCancelReleasesVRAM(t *testing.T) {
	s := newTestServer(t, 1000)
	s.cfg.BaseLatency = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, err := s.Infer(ctx, req(500)); errc <- err }()
	waitFor(t, func() bool { return s.VRAM().Used() == 500 })
	cancel()
	if code := status.Code(<-errc); code != codes.Canceled {
		t.Fatalf("code = %v, want Canceled", code)
	}
	if s.VRAM().Used() != 0 {
		t.Fatal("VRAM leaked after cancel")
	}
}

func TestFaultInjectionErrorRate(t *testing.T) {
	s := newTestServer(t, 1000)
	s.Faults().Set(FaultConfig{ErrorRate: 1})
	_, err := s.Infer(context.Background(), req(100))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable", status.Code(err))
	}
	if s.VRAM().Used() != 0 {
		t.Fatal("VRAM leaked on injected fault")
	}
}

func TestFaultInjectionLatency(t *testing.T) {
	s := newTestServer(t, 1000)
	var got time.Duration
	s.sleep = func(_ context.Context, d time.Duration) error { got = d; return nil }
	s.Faults().Set(FaultConfig{Slowdown: 3, ExtraLatencyMs: 100})
	if _, err := s.Infer(context.Background(), req(100)); err != nil {
		t.Fatal(err)
	}
	// base 1ms * 3 + 100ms
	if got != 103*time.Millisecond {
		t.Fatalf("latency = %v, want 103ms", got)
	}
}

func TestHeartbeatReflectsState(t *testing.T) {
	s := newTestServer(t, 2000)
	release := make(chan struct{})
	s.sleep = func(context.Context, time.Duration) error { <-release; return nil }
	go func() { _, _ = s.Infer(context.Background(), req(700)) }()
	waitFor(t, func() bool { return s.Active() == 1 })

	hb, _ := s.Heartbeat(context.Background(), &pb.HeartbeatRequest{})
	if hb.AvailableVramMb != 1300 || hb.TotalVramMb != 2000 || hb.ActiveRequests != 1 || !hb.Healthy || hb.WorkerId != "w-test" {
		t.Fatalf("heartbeat = %+v", hb)
	}
	s.Faults().Set(FaultConfig{Unhealthy: true})
	hb, _ = s.Heartbeat(context.Background(), &pb.HeartbeatRequest{})
	if hb.Healthy {
		t.Fatal("expected unhealthy heartbeat under fault")
	}
	close(release)
}

func TestDrainRejectsNewWorkAndWaitsForInflight(t *testing.T) {
	s := newTestServer(t, 1000)
	release := make(chan struct{})
	s.sleep = func(context.Context, time.Duration) error { <-release; return nil }

	errc := make(chan error, 1)
	go func() { _, err := s.Infer(context.Background(), req(100)); errc <- err }()
	waitFor(t, func() bool { return s.Active() == 1 })

	drained := make(chan error, 1)
	go func() { drained <- s.Drain(context.Background()) }()
	waitFor(t, s.Draining)

	if _, err := s.Infer(context.Background(), req(100)); status.Code(err) != codes.Unavailable {
		t.Fatalf("new request during drain: code = %v, want Unavailable", status.Code(err))
	}
	hb, _ := s.Heartbeat(context.Background(), &pb.HeartbeatRequest{})
	if hb.Healthy {
		t.Fatal("draining worker reported healthy")
	}
	select {
	case <-drained:
		t.Fatal("drain returned with a request in flight")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-errc; err != nil {
		t.Fatalf("in-flight request failed during drain: %v", err)
	}
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
}

func TestDrainTimeout(t *testing.T) {
	s := newTestServer(t, 1000)
	release := make(chan struct{})
	defer close(release)
	s.sleep = func(context.Context, time.Duration) error { <-release; return nil }
	go func() { _, _ = s.Infer(context.Background(), req(100)) }()
	waitFor(t, func() bool { return s.Active() == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Drain(ctx); err == nil {
		t.Fatal("expected drain timeout")
	}
}

func TestAdminFaultsEndpoint(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := NewServer(Config{ID: "w", TotalVRAMMB: 100}, nil, reg)
	ts := httptest.NewServer(s.AdminMux(reg))
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/admin/faults", "application/json", strings.NewReader(`{"error_rate":0.5,"extra_latency_ms":20}`))
	if err != nil {
		t.Fatal(err)
	}
	var got FaultConfig
	_ = json.NewDecoder(resp.Body).Decode(&got)
	resp.Body.Close()
	if got.ErrorRate != 0.5 || got.ExtraLatencyMs != 20 || got.Slowdown != 1 {
		t.Fatalf("faults = %+v", got)
	}

	resp, _ = http.Post(ts.URL+"/admin/faults", "application/json", strings.NewReader(`{"bogus":1}`))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	r, _ := http.NewRequest(http.MethodDelete, ts.URL+"/admin/faults", nil)
	resp, _ = http.DefaultClient.Do(r)
	resp.Body.Close()
	if s.Faults().Get().ErrorRate != 0 {
		t.Fatal("faults not cleared")
	}

	resp, _ = http.Get(ts.URL + "/metrics")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "inferworker_vram_total_mb") {
		t.Fatal("metrics missing inferworker_vram_total_mb")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(time.Millisecond)
	}
}
