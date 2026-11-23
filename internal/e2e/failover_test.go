// Package e2e runs the router against real in-process gRPC workers.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	pb "inferroute/gen/inferencepb"
	"inferroute/internal/router"
	"inferroute/internal/worker"
)

type testWorker struct {
	id   string
	addr string
	srv  *worker.Server
	gs   *grpc.Server
}

func startWorker(t *testing.T, id, addr string) *testWorker {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	srv := worker.NewServer(worker.Config{
		ID: id, TotalVRAMMB: 40000, BaseLatency: 5 * time.Millisecond, Jitter: 5 * time.Millisecond,
	}, quietLogger(), prometheus.NewRegistry())
	gs := grpc.NewServer()
	pb.RegisterWorkerServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	return &testWorker{id: id, addr: lis.Addr().String(), srv: srv, gs: gs}
}

// kill simulates a crash: connections drop and in-flight RPCs fail.
func (w *testWorker) kill() { w.gs.Stop() }

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type harness struct {
	url     string
	reg     *router.Registry
	workers []*testWorker
}

func newHarness(t *testing.T, workers ...*testWorker) *harness {
	t.Helper()
	addrs := make([]string, len(workers))
	for i, w := range workers {
		addrs[i] = w.addr
	}
	reg := router.NewRegistry(router.RegistryConfig{
		RouterID:          "test-router",
		HeartbeatInterval: 50 * time.Millisecond,
		HeartbeatTimeout:  40 * time.Millisecond,
		UnhealthyAfter:    2,
		Breaker:           router.BreakerConfig{FailureThreshold: 3, OpenTimeout: 200 * time.Millisecond, Jitter: 0.5, HalfOpenSuccesses: 2},
	}, router.StaticDiscoverer(addrs), quietLogger())
	rt := router.New(router.Config{
		DefaultTimeout: 5 * time.Second,
		AttemptTimeout: 2 * time.Second,
		MaxAttempts:    2,
		QueueTimeout:   time.Second,
		Models:         map[string]int64{"llama-3-8b": 2048, "phi-3-mini": 1024},
		RateLimit:      router.Limit{Rate: 1e6, Burst: 1e6},
	}, reg, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	go reg.Run(ctx)
	ts := httptest.NewServer(rt.Handler())
	t.Cleanup(func() {
		ts.Close()
		cancel()
		reg.Close()
		for _, w := range workers {
			w.gs.Stop()
		}
	})
	h := &harness{url: ts.URL, reg: reg, workers: workers}
	h.waitHealthy(t, len(workers))
	return h
}

func (h *harness) waitHealthy(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		healthy := 0
		for _, w := range h.reg.Workers() {
			if w.Healthy() && w.Status().Breaker == "closed" {
				healthy++
			}
		}
		if healthy == n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%d workers did not become healthy", n)
}

type result struct {
	code     int
	workerID string
	attempts int
}

func (h *harness) infer(client *http.Client, i int) result {
	model := []string{"llama-3-8b", "phi-3-mini"}[i%2]
	body := fmt.Sprintf(`{"model_id":%q,"prompt_tokens":32,"max_tokens":16}`, model)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, h.url+"/v1/infer", strings.NewReader(body))
	if err != nil {
		return result{code: -1}
	}
	req.Header.Set("X-Tenant-ID", fmt.Sprintf("tenant-%d", i%3))
	resp, err := client.Do(req)
	if err != nil {
		return result{code: -1}
	}
	defer func() { _ = resp.Body.Close() }()
	var out router.InferResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return result{code: resp.StatusCode, workerID: out.WorkerID, attempts: out.Attempts}
}

// runLoad drives concurrent traffic until stop closes and returns every result.
func (h *harness) runLoad(concurrency int, stop <-chan struct{}) []result {
	client := &http.Client{Timeout: 10 * time.Second}
	var mu sync.Mutex
	var out []result
	var seq atomic.Int64
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				r := h.infer(client, int(seq.Add(1)))
				mu.Lock()
				out = append(out, r)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return out
}

func summarize(t *testing.T, results []result) (byWorker map[string]int, retried int) {
	t.Helper()
	byWorker = map[string]int{}
	failed := 0
	for _, r := range results {
		if r.code != http.StatusOK {
			failed++
			continue
		}
		byWorker[r.workerID]++
		if r.attempts > 1 {
			retried++
		}
	}
	t.Logf("requests=%d failed=%d retried=%d by_worker=%v", len(results), failed, retried, byWorker)
	if failed > 0 {
		codes := map[int]int{}
		for _, r := range results {
			codes[r.code]++
		}
		t.Fatalf("%d of %d requests dropped; status codes: %v", failed, len(results), codes)
	}
	return byWorker, retried
}

// TestFailoverOnWorkerCrash kills one of two workers under sustained load and
// requires that no request fails.
func TestFailoverOnWorkerCrash(t *testing.T) {
	w1 := startWorker(t, "worker-1", "127.0.0.1:0")
	w2 := startWorker(t, "worker-2", "127.0.0.1:0")
	h := newHarness(t, w1, w2)

	stop := make(chan struct{})
	var results []result
	done := make(chan struct{})
	go func() { results = h.runLoad(24, stop); close(done) }()

	time.Sleep(400 * time.Millisecond)
	w1.kill()
	killedAt := time.Now()
	time.Sleep(800 * time.Millisecond)
	close(stop)
	<-done

	byWorker, _ := summarize(t, results)
	if byWorker["worker-1"] == 0 || byWorker["worker-2"] == 0 {
		t.Fatalf("expected both workers to serve traffic before the crash: %v", byWorker)
	}
	var w1Status router.WorkerStatus
	for _, w := range h.reg.Workers() {
		if w.ID() == "worker-1" {
			w1Status = w.Status()
		}
	}
	if w1Status.Healthy {
		t.Fatalf("worker-1 still healthy %v after crash", time.Since(killedAt))
	}
	if w1Status.Breaker == "closed" {
		t.Fatal("worker-1 breaker should not be closed after crash")
	}
}

// TestGracefulDrainNoDrops sends SIGTERM-equivalent drain to a worker under
// load: it must finish in-flight work and the router must route around it.
func TestGracefulDrainNoDrops(t *testing.T) {
	w1 := startWorker(t, "worker-1", "127.0.0.1:0")
	w2 := startWorker(t, "worker-2", "127.0.0.1:0")
	h := newHarness(t, w1, w2)

	stop := make(chan struct{})
	var results []result
	done := make(chan struct{})
	go func() { results = h.runLoad(16, stop); close(done) }()

	time.Sleep(300 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w1.srv.Drain(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	w1.gs.GracefulStop()
	time.Sleep(500 * time.Millisecond)
	close(stop)
	<-done
	summarize(t, results)
}

// TestRecoveryAfterRestart restarts a crashed worker on the same address and
// checks the router probes it through half-open and sends it traffic again.
func TestRecoveryAfterRestart(t *testing.T) {
	w1 := startWorker(t, "worker-1", "127.0.0.1:0")
	w2 := startWorker(t, "worker-2", "127.0.0.1:0")
	h := newHarness(t, w1, w2)

	stop := make(chan struct{})
	var results []result
	done := make(chan struct{})
	go func() { results = h.runLoad(12, stop); close(done) }()

	time.Sleep(200 * time.Millisecond)
	w1.kill()
	time.Sleep(500 * time.Millisecond)
	restarted := startWorker(t, "worker-1", w1.addr)
	t.Cleanup(restarted.gs.Stop)
	restartAt := time.Now()

	h.waitHealthy(t, 2)
	t.Logf("recovered %v after restart", time.Since(restartAt))
	time.Sleep(300 * time.Millisecond)
	close(stop)
	<-done

	summarize(t, results)
	var recovered int
	for _, r := range results[len(results)/2:] {
		if r.workerID == "worker-1" {
			recovered++
		}
	}
	if recovered == 0 {
		t.Fatal("restarted worker-1 received no traffic after recovery")
	}
}

// TestDuplicateIdentityIsRejected points two registry addresses at the same
// worker process, as happens when a stale DNS answer lands on a reused IP.
// The router must count that worker once and keep /metrics scrapeable.
func TestDuplicateIdentityIsRejected(t *testing.T) {
	w := startWorker(t, "worker-1", "127.0.0.1:0")
	_, port, _ := net.SplitHostPort(w.addr)
	alias := "localhost:" + port
	h := newHarness(t, w)
	h.reg.Sync([]string{w.addr, alias})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		healthy := 0
		for _, x := range h.reg.Workers() {
			if x.Healthy() {
				healthy++
				if x.Addr != w.addr {
					t.Fatalf("alias address %s accepted as healthy", x.Addr)
				}
			}
		}
		if healthy != 1 {
			t.Fatalf("healthy workers = %d, want 1", healthy)
		}
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, h.url+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics = %d with duplicate identities", resp.StatusCode)
	}
}
