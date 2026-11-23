// Package router implements the inferroute control plane: HTTP API, per-tenant
// rate limiting, resource-aware scheduling, circuit breaking and failover.
package router

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "inferroute/gen/inferencepb"
)

// Config holds the router's request-path settings. Zero values get defaults.
type Config struct {
	// DefaultTimeout applies when a request has no timeout_ms; MaxTimeout caps it.
	DefaultTimeout time.Duration
	MaxTimeout     time.Duration
	// AttemptTimeout bounds every attempt except the last, which gets
	// whatever is left of the request deadline. This leaves room to fail over
	// away from a worker that hangs instead of erroring.
	AttemptTimeout time.Duration
	// MaxAttempts is total tries per request; 2 means one failover retry.
	MaxAttempts  int
	QueueTimeout time.Duration
	// Models maps model_id to its VRAM requirement in MB. Requests may
	// override with required_vram_mb.
	Models       map[string]int64
	RateLimit    Limit
	TenantLimits map[string]Limit
	// MaxTenantLabels bounds the tenant label on request metrics. X-Tenant-ID
	// is caller-controlled, so without a bound any client could create
	// unlimited Prometheus series. Tenants with configured limits always keep
	// their own label; the first MaxTenantLabels others do too, the rest are
	// reported as "other".
	MaxTenantLabels int
}

func (c Config) withDefaults() Config {
	if c.DefaultTimeout <= 0 {
		c.DefaultTimeout = 10 * time.Second
	}
	if c.MaxTimeout < c.DefaultTimeout {
		c.MaxTimeout = max(60*time.Second, c.DefaultTimeout)
	}
	if c.AttemptTimeout <= 0 {
		c.AttemptTimeout = 3 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 2
	}
	if c.RateLimit.Rate <= 0 {
		c.RateLimit = Limit{Rate: 100, Burst: 200}
	}
	if c.MaxTenantLabels <= 0 {
		c.MaxTenantLabels = 100
	}
	return c
}

// Router is the HTTP front end: it admits, schedules and fails over requests.
type Router struct {
	cfg     Config
	log     *slog.Logger
	reg     *Registry
	sched   *Scheduler
	limiter *RateLimiter
	metrics *Metrics
	prom    prometheus.Gatherer

	labelMu sync.Mutex
	labels  map[string]struct{} // tenants that have their own metric label
}

// New builds a router over the workers in reg.
func New(cfg Config, reg *Registry, log *slog.Logger) *Router {
	cfg = cfg.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	promReg := prometheus.NewRegistry()
	rt := &Router{
		cfg:     cfg,
		log:     log,
		reg:     reg,
		sched:   NewScheduler(reg, cfg.QueueTimeout),
		limiter: NewRateLimiter(cfg.RateLimit, cfg.TenantLimits),
		metrics: NewMetrics(promReg, reg),
		prom:    promReg,
		labels:  map[string]struct{}{},
	}
	reg.OnIdentified = rt.metrics.initWorker
	return rt
}

// Limiter exposes the rate limiter so callers can run eviction.
func (rt *Router) Limiter() *RateLimiter { return rt.limiter }

// Handler serves /v1/infer, /healthz, /readyz and /metrics.
func (rt *Router) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/infer", rt.handleInfer)
	mux.HandleFunc("GET /healthz", rt.handleHealth)
	mux.HandleFunc("GET /readyz", rt.handleReady)
	mux.Handle("GET /metrics", promhttp.HandlerFor(rt.prom, promhttp.HandlerOpts{}))
	return mux
}

// InferRequest is the public JSON request body.
type InferRequest struct {
	ModelID        string `json:"model_id"`
	PromptTokens   int32  `json:"prompt_tokens"`
	MaxTokens      int32  `json:"max_tokens"`
	RequiredVRAMMB int64  `json:"required_vram_mb,omitempty"`
	TimeoutMs      int64  `json:"timeout_ms,omitempty"`
}

// InferResponse is the public JSON response body.
type InferResponse struct {
	RequestID        string  `json:"request_id"`
	WorkerID         string  `json:"worker_id"`
	Output           string  `json:"output"`
	CompletionTokens int32   `json:"completion_tokens"`
	ComputeMs        float64 `json:"compute_ms"`
	LatencyMs        float64 `json:"latency_ms"`
	Attempts         int     `json:"attempts"`
}

type errorBody struct {
	Error     string `json:"error"`
	RequestID string `json:"request_id,omitempty"`
}

// apiError is a client-facing failure. retryAfter > 0 sets Retry-After.
type apiError struct {
	code       int
	msg        string
	retryAfter time.Duration
}

// routed is the result of processing one request; model is the bounded
// metrics label and is set even when processing fails.
type routed struct {
	resp     *pb.InferResponse
	attempts int
	model    string
}

const modelUnknown = "unknown"

func (rt *Router) handleInfer(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqID := requestID(r)
	tenant := r.Header.Get("X-Tenant-ID")
	w.Header().Set("X-Request-ID", reqID)

	out, aerr := rt.process(w, r, reqID, tenant)
	code := http.StatusOK
	if aerr != nil {
		code = aerr.code
		writeError(w, reqID, aerr)
	} else {
		w.Header().Set("X-Worker-ID", out.resp.GetWorkerId())
		writeJSON(w, http.StatusOK, newInferResponse(reqID, out, start))
	}
	rt.observe(start, code, out.model, tenant)
}

// process admits, validates and routes one request.
func (rt *Router) process(w http.ResponseWriter, r *http.Request, reqID, tenant string) (routed, *apiError) {
	out := routed{model: modelUnknown}
	if aerr := rt.admit(tenant); aerr != nil {
		return out, aerr
	}
	in, aerr := decodeInferRequest(w, r)
	if aerr != nil {
		return out, aerr
	}
	out.model = rt.modelLabel(in.ModelID)
	vram, aerr := rt.validate(in)
	if aerr != nil {
		return out, aerr
	}

	ctx, cancel := context.WithTimeout(r.Context(), rt.timeoutFor(in))
	defer cancel()
	preq := &pb.InferRequest{
		RequestId: reqID, ModelId: in.ModelID, PromptTokens: in.PromptTokens,
		MaxTokens: in.MaxTokens, RequiredVramMb: vram, TenantId: tenant,
	}
	var err error
	out.resp, out.attempts, err = rt.Route(ctx, preq)
	if err != nil {
		aerr := toAPIError(err)
		rt.log.Warn("request failed", "request_id", reqID, "tenant", tenant, "model", in.ModelID,
			"attempts", out.attempts, "status", aerr.code, "err", err)
		return out, aerr
	}
	return out, nil
}

// admit enforces tenant identification and the tenant's rate limit.
func (rt *Router) admit(tenant string) *apiError {
	if tenant == "" {
		return &apiError{code: http.StatusBadRequest, msg: "missing X-Tenant-ID header"}
	}
	if ok, wait := rt.limiter.Allow(tenant); !ok {
		return &apiError{code: http.StatusTooManyRequests, msg: "rate limit exceeded for tenant", retryAfter: wait}
	}
	return nil
}

func decodeInferRequest(w http.ResponseWriter, r *http.Request) (InferRequest, *apiError) {
	var in InferRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		return in, &apiError{code: http.StatusBadRequest, msg: "invalid JSON body: " + err.Error()}
	}
	return in, nil
}

// validate checks the request and resolves its VRAM requirement: an explicit
// required_vram_mb wins over the model catalog.
func (rt *Router) validate(in InferRequest) (int64, *apiError) {
	vram := rt.cfg.Models[in.ModelID]
	if in.RequiredVRAMMB > 0 {
		vram = in.RequiredVRAMMB
	}
	bad := func(msg string) (int64, *apiError) {
		return 0, &apiError{code: http.StatusBadRequest, msg: msg}
	}
	switch {
	case in.ModelID == "":
		return bad("model_id is required")
	case vram <= 0:
		return bad("unknown model_id and no required_vram_mb given")
	case in.PromptTokens < 0 || in.MaxTokens <= 0:
		return bad("prompt_tokens must be >= 0 and max_tokens > 0")
	}
	return vram, nil
}

// modelLabel bounds metric cardinality to catalog models.
func (rt *Router) modelLabel(id string) string {
	if _, ok := rt.cfg.Models[id]; ok {
		return id
	}
	if id == "" {
		return modelUnknown
	}
	return "other"
}

func (rt *Router) timeoutFor(in InferRequest) time.Duration {
	if in.TimeoutMs > 0 {
		return min(time.Duration(in.TimeoutMs)*time.Millisecond, rt.cfg.MaxTimeout)
	}
	return rt.cfg.DefaultTimeout
}

func (rt *Router) observe(start time.Time, code int, model, tenant string) {
	st := strconv.Itoa(code)
	rt.metrics.requests.WithLabelValues(st, model, rt.tenantLabel(tenant)).Inc()
	rt.metrics.duration.WithLabelValues(model, st).Observe(time.Since(start).Seconds())
}

func newInferResponse(reqID string, out routed, start time.Time) InferResponse {
	return InferResponse{
		RequestID: reqID, WorkerID: out.resp.GetWorkerId(), Output: out.resp.GetOutput(),
		CompletionTokens: out.resp.GetCompletionTokens(), ComputeMs: out.resp.GetComputeMs(),
		LatencyMs: msSince(start), Attempts: out.attempts,
	}
}

// Route schedules req, failing over to a different worker on retryable
// errors, within ctx's deadline. It returns the number of attempts made.
func (rt *Router) Route(ctx context.Context, req *pb.InferRequest) (*pb.InferResponse, int, error) {
	exclude := map[*Worker]bool{}
	var lastErr error
	for attempt := 1; attempt <= rt.cfg.MaxAttempts; attempt++ {
		lease, err := rt.sched.Acquire(ctx, req.GetRequiredVramMb(), exclude)
		if err != nil {
			return nil, attempt - 1, preferWorkerError(err, lastErr)
		}
		resp, err := rt.attempt(ctx, lease, req, attempt == rt.cfg.MaxAttempts)
		switch {
		case err == nil:
			return resp, attempt, nil
		case ctx.Err() != nil:
			return nil, attempt, ctx.Err()
		case !retryable(status.Code(err)):
			return nil, attempt, err
		}
		lastErr = err
		exclude[lease.Worker] = true
		if attempt < rt.cfg.MaxAttempts {
			rt.recordFailover(req, lease.Worker, err)
		}
	}
	return nil, rt.cfg.MaxAttempts, lastErr
}

// attempt sends req to the leased worker and settles the lease. Every
// attempt but the last gets AttemptTimeout, leaving time to fail over away
// from a worker that hangs instead of erroring.
func (rt *Router) attempt(ctx context.Context, lease *Lease, req *pb.InferRequest, last bool) (*pb.InferResponse, error) {
	actx := ctx
	if !last {
		var cancel context.CancelFunc
		actx, cancel = context.WithTimeout(ctx, rt.cfg.AttemptTimeout)
		defer cancel()
	}
	sent := time.Now()
	resp, err := lease.Worker.client.Infer(actx, req)
	code := status.Code(err)
	rt.metrics.attempts.WithLabelValues(lease.Worker.ID(), code.String()).Inc()
	if err != nil {
		lease.Done(classify(code, ctx.Err() != nil))
		return nil, err
	}
	lease.Worker.ewma.Observe(msSince(sent))
	lease.Done(Success)
	return resp, nil
}

func (rt *Router) recordFailover(req *pb.InferRequest, from *Worker, err error) {
	code := status.Code(err)
	rt.metrics.failovers.WithLabelValues(from.ID(), reason(code)).Inc()
	rt.log.Info("failing over", "request_id", req.GetRequestId(), "from_worker", from.ID(),
		"code", code.String(), "err", status.Convert(err).Message())
}

// preferWorkerError reports the earlier worker error rather than a later
// scheduling failure, since it says more about why the request failed.
func preferWorkerError(schedErr, workerErr error) error {
	if workerErr != nil && !errors.Is(schedErr, context.DeadlineExceeded) {
		return workerErr
	}
	return schedErr
}

func msSince(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }

// classify maps an attempt's gRPC code to a breaker outcome.
func classify(code codes.Code, parentDone bool) Outcome {
	switch {
	case parentDone:
		return Neutral // the caller gave up; says nothing about the worker
	case code == codes.ResourceExhausted, code == codes.InvalidArgument, code == codes.Canceled:
		return Neutral
	}
	return Failure
}

func retryable(code codes.Code) bool {
	switch code {
	case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded, codes.Aborted, codes.Canceled:
		return true
	}
	return false
}

// failoverReasons lists every value reason can return.
var failoverReasons = []string{"unavailable", "resource_exhausted", "attempt_timeout", "other"}

func reason(code codes.Code) string {
	switch code {
	case codes.Unavailable:
		return "unavailable"
	case codes.ResourceExhausted:
		return "resource_exhausted"
	case codes.DeadlineExceeded:
		return "attempt_timeout"
	}
	return "other"
}

func toAPIError(err error) *apiError {
	unavailable := func(msg string) *apiError {
		return &apiError{code: http.StatusServiceUnavailable, msg: msg, retryAfter: time.Second}
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return &apiError{code: http.StatusGatewayTimeout, msg: "request deadline exceeded"}
	case errors.Is(err, context.Canceled):
		return &apiError{code: 499, msg: "client closed request"}
	case errors.Is(err, ErrNoCapacity), errors.Is(err, ErrNoWorkers), errors.Is(err, ErrQueueTimeout):
		return unavailable(err.Error())
	}
	msg := status.Convert(err).Message()
	switch status.Code(err) {
	case codes.InvalidArgument:
		return &apiError{code: http.StatusBadRequest, msg: msg}
	case codes.Unavailable, codes.ResourceExhausted:
		return unavailable("workers unavailable: " + msg)
	case codes.DeadlineExceeded:
		return &apiError{code: http.StatusGatewayTimeout, msg: "worker timed out"}
	}
	return &apiError{code: http.StatusBadGateway, msg: "worker error: " + msg}
}

func writeError(w http.ResponseWriter, reqID string, e *apiError) {
	if e.retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(max(e.retryAfter.Seconds(), 1)))))
	}
	writeJSON(w, e.code, errorBody{Error: e.msg, RequestID: reqID})
}

type healthBody struct {
	Status         string         `json:"status"`
	HealthyWorkers int            `json:"healthy_workers"`
	Workers        []WorkerStatus `json:"workers"`
}

func (rt *Router) health() healthBody {
	ws := rt.reg.Workers()
	b := healthBody{Status: "ok", Workers: make([]WorkerStatus, 0, len(ws))}
	for _, w := range ws {
		s := w.Status()
		if s.Healthy {
			b.HealthyWorkers++
		}
		b.Workers = append(b.Workers, s)
	}
	if b.HealthyWorkers == 0 {
		b.Status = "degraded"
	}
	return b
}

// handleHealth is liveness: the router process is up. The body reports the
// per-worker view for operators and the chaos benchmark.
func (rt *Router) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, rt.health())
}

// handleReady is readiness: at least one worker can take traffic.
func (rt *Router) handleReady(w http.ResponseWriter, _ *http.Request) {
	b := rt.health()
	code := http.StatusOK
	if b.HealthyWorkers == 0 {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, b)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// requestID honours a caller-supplied X-Request-ID, else generates one.
func requestID(r *http.Request) string {
	if id := r.Header.Get("X-Request-ID"); id != "" {
		return id
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// tenantLabel bounds the cardinality of the tenant metric label.
func (rt *Router) tenantLabel(t string) string {
	if t == "" {
		return "none"
	}
	if len(t) > 64 {
		t = t[:64]
	}
	if _, ok := rt.cfg.TenantLimits[t]; ok {
		return t
	}
	rt.labelMu.Lock()
	defer rt.labelMu.Unlock()
	if _, ok := rt.labels[t]; ok {
		return t
	}
	if len(rt.labels) >= rt.cfg.MaxTenantLabels {
		return "other"
	}
	rt.labels[t] = struct{}{}
	return t
}
