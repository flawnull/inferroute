package router

// Routing behavior through the real HTTP handler, scheduler and breaker. The
// workers are scripted gRPC clients (fakes); internal/e2e runs real workers.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "inferroute/gen/inferencepb"
)

// scripted is a WorkerServiceClient whose Infer runs a test-supplied function
// and records every request it receives.
type scripted struct {
	infer func(ctx context.Context, req *pb.InferRequest) (*pb.InferResponse, error)
	mu    sync.Mutex
	got   []*pb.InferRequest
}

func (s *scripted) Infer(ctx context.Context, req *pb.InferRequest, _ ...grpc.CallOption) (*pb.InferResponse, error) {
	s.mu.Lock()
	s.got = append(s.got, req)
	s.mu.Unlock()
	return s.infer(ctx, req)
}

func (s *scripted) Heartbeat(context.Context, *pb.HeartbeatRequest, ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{}, nil
}

func (s *scripted) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.got)
}

func (s *scripted) last() *pb.InferRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.got[len(s.got)-1]
}

func answers(id string) *scripted {
	return &scripted{infer: func(_ context.Context, req *pb.InferRequest) (*pb.InferResponse, error) {
		return &pb.InferResponse{RequestId: req.GetRequestId(), WorkerId: id, Output: "ok", CompletionTokens: req.GetMaxTokens()}, nil
	}}
}

func fails(code codes.Code) *scripted {
	return &scripted{infer: func(context.Context, *pb.InferRequest) (*pb.InferResponse, error) {
		return nil, status.Error(code, "scripted "+code.String())
	}}
}

// hangs blocks until the attempt's context ends, like a wedged GPU.
func hangs() *scripted {
	return &scripted{infer: func(ctx context.Context, _ *pb.InferRequest) (*pb.InferResponse, error) {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}}
}

// pool builds healthy workers with plenty of VRAM. latency orders them: the
// scheduler tries the first one first.
func pool(clients ...*scripted) []*Worker {
	ws := make([]*Worker, len(clients))
	for i, c := range clients {
		w := newTestWorker(fakeWorker{addr: "w" + string(rune('a'+i)), vram: 100000, latency: float64(1 + i*50), healthy: true})
		w.client = c
		ws[i] = w
	}
	return ws
}

type reply struct {
	code    int
	header  http.Header
	body    InferResponse
	errBody errorBody
}

func send(t *testing.T, url string, body string, headers map[string]string) reply {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url+"/v1/infer", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Tenant-ID", "t")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	r := reply{code: resp.StatusCode, header: resp.Header}
	_ = json.Unmarshal(raw, &r.body)
	_ = json.Unmarshal(raw, &r.errBody)
	return r
}

const okBody = `{"model_id":"m","prompt_tokens":8,"max_tokens":4}`

func TestRouting(t *testing.T) {
	t.Run("an unavailable worker fails over to the next worker and the client sees one success", func(t *testing.T) {
		bad, good := fails(codes.Unavailable), answers("good")
		ts := newTestRouter(t, Config{}, pool(bad, good)...)
		r := send(t, ts.URL, okBody, nil)
		if r.code != 200 || r.body.WorkerID != "good" || r.body.Attempts != 2 {
			t.Fatalf("got %d worker=%q attempts=%d, want 200 from good after 2 attempts", r.code, r.body.WorkerID, r.body.Attempts)
		}
		if r.header.Get("X-Worker-ID") != "good" {
			t.Fatalf("X-Worker-ID = %q", r.header.Get("X-Worker-ID"))
		}
	})

	t.Run("a worker that is out of VRAM fails over to another worker", func(t *testing.T) {
		ts := newTestRouter(t, Config{}, pool(fails(codes.ResourceExhausted), answers("good"))...)
		if r := send(t, ts.URL, okBody, nil); r.code != 200 || r.body.Attempts != 2 {
			t.Fatalf("got %d attempts=%d", r.code, r.body.Attempts)
		}
	})

	t.Run("a hanging worker is abandoned after the attempt timeout and another worker answers", func(t *testing.T) {
		ts := newTestRouter(t, Config{AttemptTimeout: 50 * time.Millisecond, DefaultTimeout: 2 * time.Second}, pool(hangs(), answers("good"))...)
		start := time.Now()
		r := send(t, ts.URL, okBody, nil)
		if r.code != 200 || r.body.WorkerID != "good" {
			t.Fatalf("got %d worker=%q, want 200 from good", r.code, r.body.WorkerID)
		}
		if el := time.Since(start); el > time.Second {
			t.Fatalf("took %v; the hung attempt should have been cut at 50ms", el)
		}
	})

	t.Run("a request the worker calls invalid returns 400 and is never retried elsewhere", func(t *testing.T) {
		bad, other := fails(codes.InvalidArgument), answers("other")
		ts := newTestRouter(t, Config{}, pool(bad, other)...)
		if r := send(t, ts.URL, okBody, nil); r.code != 400 {
			t.Fatalf("status %d, want 400", r.code)
		}
		if other.calls() != 0 {
			t.Fatal("an invalid request was retried on another worker")
		}
	})

	t.Run("an internal worker error returns 502 and is not retried", func(t *testing.T) {
		bad, other := fails(codes.Internal), answers("other")
		ts := newTestRouter(t, Config{}, pool(bad, other)...)
		if r := send(t, ts.URL, okBody, nil); r.code != 502 {
			t.Fatalf("status %d, want 502", r.code)
		}
		if other.calls() != 0 {
			t.Fatal("a non-retryable error was retried")
		}
	})

	t.Run("when every attempt fails the client gets 503 with Retry-After", func(t *testing.T) {
		ts := newTestRouter(t, Config{}, pool(fails(codes.Unavailable), fails(codes.Unavailable))...)
		r := send(t, ts.URL, okBody, nil)
		if r.code != 503 || r.header.Get("Retry-After") == "" {
			t.Fatalf("status %d Retry-After=%q, want 503 with Retry-After", r.code, r.header.Get("Retry-After"))
		}
	})

	t.Run("failover stops at MaxAttempts even when more workers are available", func(t *testing.T) {
		a, b, c := fails(codes.Unavailable), fails(codes.Unavailable), answers("c")
		ts := newTestRouter(t, Config{MaxAttempts: 2}, pool(a, b, c)...)
		if r := send(t, ts.URL, okBody, nil); r.code != 503 {
			t.Fatalf("status %d, want 503 after 2 attempts", r.code)
		}
		if c.calls() != 0 {
			t.Fatal("a third attempt was made with MaxAttempts=2")
		}
	})

	t.Run("a request that runs out of time returns 504", func(t *testing.T) {
		ts := newTestRouter(t, Config{AttemptTimeout: time.Second}, pool(hangs())...)
		if r := send(t, ts.URL, `{"model_id":"m","max_tokens":1,"timeout_ms":60}`, nil); r.code != 504 {
			t.Fatalf("status %d, want 504", r.code)
		}
	})
}

func TestBreakerAccounting(t *testing.T) {
	t.Run("client errors never open a worker's breaker", func(t *testing.T) {
		ws := pool(fails(codes.InvalidArgument))
		ts := newTestRouter(t, Config{}, ws...)
		send(t, ts.URL, okBody, nil)
		if got := ws[0].Status().Breaker; got != "closed" {
			t.Fatalf("breaker %s after InvalidArgument, want closed", got)
		}
	})

	t.Run("a worker that errors opens its breaker", func(t *testing.T) {
		ws := pool(fails(codes.Unavailable), answers("good"))
		ts := newTestRouter(t, Config{}, ws...)
		send(t, ts.URL, okBody, nil)
		if got := ws[0].Status().Breaker; got == "closed" {
			t.Fatal("breaker still closed after the worker failed (threshold 1)")
		}
	})

	t.Run("a request that runs out of its own deadline does not count against the worker", func(t *testing.T) {
		ws := pool(hangs())
		ts := newTestRouter(t, Config{AttemptTimeout: time.Minute, DefaultTimeout: time.Minute}, ws...)
		if r := send(t, ts.URL, `{"model_id":"m","max_tokens":1,"timeout_ms":50}`, nil); r.code != 504 {
			t.Fatalf("status %d, want 504", r.code)
		}
		if got := ws[0].Status().Breaker; got != "closed" {
			t.Fatalf("breaker %s after the caller's deadline expired, want closed", got)
		}
	})

	t.Run("a client that disconnects does not count against the worker", func(t *testing.T) {
		ws := pool(hangs())
		ts := newTestRouter(t, Config{AttemptTimeout: time.Minute, DefaultTimeout: time.Minute}, ws...)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/v1/infer", strings.NewReader(okBody))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Tenant-ID", "t")
		// The client gives up first, so this request is expected to fail.
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
			t.Fatalf("request finished with %d; it should have been cut by the client timeout", resp.StatusCode)
		}
		deadline := time.Now().Add(time.Second)
		for ws[0].inflight.Load() != 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if got := ws[0].Status().Breaker; got != "closed" {
			t.Fatalf("breaker %s after the client disconnected, want closed", got)
		}
	})
}

func TestRequestHandling(t *testing.T) {
	t.Run("a caller's X-Request-ID is echoed back and passed to the worker", func(t *testing.T) {
		w := answers("w")
		ts := newTestRouter(t, Config{}, pool(w)...)
		r := send(t, ts.URL, okBody, map[string]string{"X-Request-ID": "trace-123"})
		if r.header.Get("X-Request-ID") != "trace-123" || r.body.RequestID != "trace-123" || w.last().GetRequestId() != "trace-123" {
			t.Fatalf("header=%q body=%q worker=%q, want trace-123 everywhere", r.header.Get("X-Request-ID"), r.body.RequestID, w.last().GetRequestId())
		}
	})

	t.Run("a request without X-Request-ID gets a unique generated one", func(t *testing.T) {
		ts := newTestRouter(t, Config{}, pool(answers("w"))...)
		a, b := send(t, ts.URL, okBody, nil), send(t, ts.URL, okBody, nil)
		if a.body.RequestID == "" || a.body.RequestID == b.body.RequestID {
			t.Fatalf("ids %q and %q, want two different non-empty ids", a.body.RequestID, b.body.RequestID)
		}
	})

	t.Run("error responses carry the request id too", func(t *testing.T) {
		ts := newTestRouter(t, Config{})
		r := send(t, ts.URL, `{`, map[string]string{"X-Request-ID": "bad-1"})
		if r.code != 400 || r.errBody.RequestID != "bad-1" || r.errBody.Error == "" {
			t.Fatalf("got %d %+v", r.code, r.errBody)
		}
	})

	t.Run("required_vram_mb overrides the model catalog", func(t *testing.T) {
		w := answers("w")
		ts := newTestRouter(t, Config{}, pool(w)...)
		send(t, ts.URL, `{"model_id":"m","max_tokens":1,"required_vram_mb":7777}`, nil)
		if got := w.last().GetRequiredVramMb(); got != 7777 {
			t.Fatalf("worker asked for %d MB, want 7777", got)
		}
	})

	t.Run("a catalog model sends its catalog VRAM to the worker", func(t *testing.T) {
		w := answers("w")
		ts := newTestRouter(t, Config{}, pool(w)...)
		send(t, ts.URL, okBody, nil)
		if got := w.last().GetRequiredVramMb(); got != 1000 {
			t.Fatalf("worker asked for %d MB, want the catalog's 1000", got)
		}
	})

	t.Run("an unknown model is accepted when it states its VRAM", func(t *testing.T) {
		ts := newTestRouter(t, Config{}, pool(answers("w"))...)
		if r := send(t, ts.URL, `{"model_id":"custom","max_tokens":1,"required_vram_mb":500}`, nil); r.code != 200 {
			t.Fatalf("status %d, want 200", r.code)
		}
	})

	t.Run("the tenant id is passed to the worker", func(t *testing.T) {
		w := answers("w")
		ts := newTestRouter(t, Config{}, pool(w)...)
		send(t, ts.URL, okBody, map[string]string{"X-Tenant-ID": "acme"})
		if got := w.last().GetTenantId(); got != "acme" {
			t.Fatalf("tenant %q, want acme", got)
		}
	})

	t.Run("a body over 1 MiB is rejected without reaching a worker", func(t *testing.T) {
		w := answers("w")
		ts := newTestRouter(t, Config{}, pool(w)...)
		big := `{"model_id":"` + strings.Repeat("x", 1<<20) + `","max_tokens":1}`
		if r := send(t, ts.URL, big, nil); r.code != 400 {
			t.Fatalf("status %d, want 400", r.code)
		}
		if w.calls() != 0 {
			t.Fatal("oversized request reached a worker")
		}
	})

	t.Run("a negative prompt_tokens is rejected", func(t *testing.T) {
		ts := newTestRouter(t, Config{})
		if r := send(t, ts.URL, `{"model_id":"m","prompt_tokens":-1,"max_tokens":1}`, nil); r.code != 400 {
			t.Fatalf("status %d, want 400", r.code)
		}
	})

	t.Run("only POST is accepted on /v1/infer", func(t *testing.T) {
		ts := newTestRouter(t, Config{})
		if code := get(t, ts.URL+"/v1/infer", nil); code != http.StatusMethodNotAllowed {
			t.Fatalf("GET status %d, want 405", code)
		}
	})
}

func TestRequestTimeouts(t *testing.T) {
	rt := New(Config{DefaultTimeout: 2 * time.Second, MaxTimeout: 5 * time.Second}, NewRegistry(RegistryConfig{}, StaticDiscoverer(nil), nil), nil)
	t.Run("a request without timeout_ms gets the default timeout", func(t *testing.T) {
		if got := rt.timeoutFor(InferRequest{}); got != 2*time.Second {
			t.Fatalf("%v, want 2s", got)
		}
	})
	t.Run("timeout_ms is honoured below the maximum", func(t *testing.T) {
		if got := rt.timeoutFor(InferRequest{TimeoutMs: 300}); got != 300*time.Millisecond {
			t.Fatalf("%v, want 300ms", got)
		}
	})
	t.Run("timeout_ms is capped at the maximum timeout", func(t *testing.T) {
		if got := rt.timeoutFor(InferRequest{TimeoutMs: 3_600_000}); got != 5*time.Second {
			t.Fatalf("%v, want the 5s cap", got)
		}
	})
}

// tenantSeries counts distinct tenant label values on inferroute_requests_total.
func tenantSeries(t *testing.T, rt *Router) map[string]bool {
	t.Helper()
	mfs, err := rt.prom.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, mf := range mfs {
		if mf.GetName() != "inferroute_requests_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "tenant" {
					seen[l.GetValue()] = true
				}
			}
		}
	}
	return seen
}

func TestMetricCardinality(t *testing.T) {
	t.Run("caller-chosen tenant ids cannot create unbounded metric series", func(t *testing.T) {
		rt := New(Config{MaxTenantLabels: 3, Models: map[string]int64{"m": 1}}, NewRegistry(RegistryConfig{}, StaticDiscoverer(nil), nil), nil)
		for i := range 50 {
			rt.observe(time.Now(), 200, "m", "tenant-"+string(rune('A'+i)))
		}
		got := tenantSeries(t, rt)
		if len(got) != 4 || !got["other"] {
			t.Fatalf("tenant labels %v, want 3 tenants plus other", got)
		}
	})

	t.Run("tenants with configured limits always keep their own label", func(t *testing.T) {
		rt := New(Config{MaxTenantLabels: 1, TenantLimits: map[string]Limit{"premium": {1, 1}}}, NewRegistry(RegistryConfig{}, StaticDiscoverer(nil), nil), nil)
		rt.observe(time.Now(), 200, "m", "first")
		rt.observe(time.Now(), 200, "m", "second")
		rt.observe(time.Now(), 200, "m", "premium")
		got := tenantSeries(t, rt)
		if !got["premium"] || !got["first"] || !got["other"] || got["second"] {
			t.Fatalf("tenant labels %v, want first, other and premium", got)
		}
	})

	t.Run("a tenant keeps its label on later requests", func(t *testing.T) {
		rt := New(Config{MaxTenantLabels: 1}, NewRegistry(RegistryConfig{}, StaticDiscoverer(nil), nil), nil)
		rt.observe(time.Now(), 200, "m", "a")
		rt.observe(time.Now(), 200, "m", "b")
		rt.observe(time.Now(), 200, "m", "a")
		if got := tenantSeries(t, rt); !got["a"] || got["b"] {
			t.Fatalf("tenant labels %v", got)
		}
	})

	t.Run("unknown model ids are grouped as other", func(t *testing.T) {
		rt := New(Config{Models: map[string]int64{"m": 1}}, NewRegistry(RegistryConfig{}, StaticDiscoverer(nil), nil), nil)
		if rt.modelLabel("m") != "m" || rt.modelLabel("random-123") != "other" || rt.modelLabel("") != "unknown" {
			t.Fatal("model label not bounded to the catalog")
		}
	})
}
