package router

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestRouter(t *testing.T, cfg Config, ws ...*Worker) *httptest.Server {
	t.Helper()
	reg := NewRegistry(RegistryConfig{}, StaticDiscoverer(nil), nil)
	reg.sorted = ws
	if cfg.Models == nil {
		cfg.Models = map[string]int64{"m": 1000}
	}
	rt := New(cfg, reg, nil)
	ts := httptest.NewServer(rt.Handler())
	t.Cleanup(ts.Close)
	return ts
}

type posted struct {
	StatusCode int
	Header     http.Header
}

func post(t *testing.T, url, tenant, body string) posted {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url+"/v1/infer", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if tenant != "" {
		req.Header.Set("X-Tenant-ID", tenant)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	return posted{StatusCode: resp.StatusCode, Header: resp.Header}
}

// get fetches path and decodes a JSON body into v when v is not nil.
func get(t *testing.T, url string, v any) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if v != nil {
		if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
			t.Fatal(err)
		}
	}
	return resp.StatusCode
}

func TestHTTPValidation(t *testing.T) {
	ts := newTestRouter(t, Config{})
	cases := []struct {
		name, tenant, body string
		want               int
	}{
		{"missing tenant", "", `{"model_id":"m","max_tokens":1}`, 400},
		{"bad json", "t", `{`, 400},
		{"unknown field", "t", `{"model_id":"m","max_tokens":1,"x":1}`, 400},
		{"missing model", "t", `{"max_tokens":1}`, 400},
		{"unknown model no vram", "t", `{"model_id":"zzz","max_tokens":1}`, 400},
		{"zero max tokens", "t", `{"model_id":"m","max_tokens":0}`, 400},
	}
	for _, c := range cases {
		if got := post(t, ts.URL, c.tenant, c.body).StatusCode; got != c.want {
			t.Errorf("%s: status %d, want %d", c.name, got, c.want)
		}
	}
}

func TestHTTPRateLimit429WithRetryAfter(t *testing.T) {
	ts := newTestRouter(t, Config{RateLimit: Limit{Rate: 0.5, Burst: 2}, QueueTimeout: time.Millisecond})
	body := `{"model_id":"m","max_tokens":1}`
	for range 2 {
		if c := post(t, ts.URL, "t1", body).StatusCode; c == http.StatusTooManyRequests {
			t.Fatal("rate limited within burst")
		}
	}
	resp := post(t, ts.URL, "t1", body)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "2" {
		t.Fatalf("Retry-After = %q, want 2 (1 token at 0.5/s)", ra)
	}
	if c := post(t, ts.URL, "t2", body).StatusCode; c == http.StatusTooManyRequests {
		t.Fatal("other tenant limited")
	}
}

func TestHTTPNoWorkers503(t *testing.T) {
	ts := newTestRouter(t, Config{QueueTimeout: 10 * time.Millisecond})
	resp := post(t, ts.URL, "t", `{"model_id":"m","max_tokens":1}`)
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("status = %d retry-after=%q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
}

func TestHealthAndReady(t *testing.T) {
	w := newTestWorker(fakeWorker{addr: "a:1", vram: 1000})
	ts := newTestRouter(t, Config{}, w)
	var hb healthBody
	if code := get(t, ts.URL+"/healthz", &hb); code != http.StatusOK || hb.Status != "degraded" || len(hb.Workers) != 1 {
		t.Fatalf("healthz %d %+v", code, hb)
	}
	if code := get(t, ts.URL+"/readyz", nil); code != http.StatusServiceUnavailable {
		t.Fatalf("readyz with no healthy workers = %d", code)
	}
	w.healthy.Store(true)
	if code := get(t, ts.URL+"/readyz", nil); code != http.StatusOK {
		t.Fatalf("readyz = %d", code)
	}
}

func TestParsers(t *testing.T) {
	m, err := ParseModels("a=100, b=200")
	if err != nil || m["a"] != 100 || m["b"] != 200 {
		t.Fatalf("ParseModels = %v %v", m, err)
	}
	if _, err := ParseModels("a=x"); err == nil {
		t.Fatal("want error")
	}
	l, err := ParseTenantLimits("free=1:2,premium=100:200")
	if err != nil || l["premium"] != (Limit{100, 200}) {
		t.Fatalf("ParseTenantLimits = %v %v", l, err)
	}
	if _, err := ParseTenantLimits("x=1"); err == nil {
		t.Fatal("want error")
	}
	a, err := ParseAddrs("w1:50051, w2:50051")
	if err != nil || len(a) != 2 {
		t.Fatalf("ParseAddrs = %v %v", a, err)
	}
	if _, err := ParseAddrs("nope"); err == nil {
		t.Fatal("want error")
	}
}

func TestFailoverSeriesPreCreated(t *testing.T) {
	reg := NewRegistry(RegistryConfig{}, StaticDiscoverer(nil), nil)
	rt := New(Config{}, reg, nil)
	w := newTestWorker(fakeWorker{addr: "a:1"})
	if !reg.claimIdentity(w, "worker-9") {
		t.Fatal("claim failed")
	}
	mfs, err := rt.prom.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "inferroute_failovers_total" && len(mf.GetMetric()) == len(failoverReasons) {
			return
		}
	}
	t.Fatal("failover series not pre-created at zero for identified worker")
}
