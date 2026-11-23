package router

import (
	"context"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	pb "inferroute/gen/inferencepb"
	"inferroute/internal/ewma"
)

// Worker is the router's view of one worker node. All fields are safe for
// concurrent use.
type Worker struct {
	Addr    string
	client  pb.WorkerServiceClient
	conn    *grpc.ClientConn
	breaker *Breaker
	ewma    *ewma.EWMA

	id           atomic.Pointer[string]
	healthy      atomic.Bool
	totalVRAM    atomic.Int64
	externalUsed atomic.Int64 // VRAM in use on the worker that this router did not reserve
	reserved     atomic.Int64 // VRAM reserved by this router's in-flight requests
	inflight     atomic.Int64 // this router's in-flight requests
	remoteActive atomic.Int64 // active requests reported by the worker
	hbFailures   atomic.Int32
	lastSeen     atomic.Int64 // unix nanos of last successful heartbeat
}

// ID is the worker-reported identity, falling back to the address until the
// first heartbeat succeeds.
func (w *Worker) ID() string {
	if p := w.id.Load(); p != nil {
		return *p
	}
	return w.Addr
}

// Healthy reports whether the worker's recent heartbeats succeeded.
func (w *Worker) Healthy() bool { return w.healthy.Load() }

// Free is the VRAM this router believes it can still place on the worker.
func (w *Worker) Free() int64 {
	return w.totalVRAM.Load() - w.externalUsed.Load() - w.reserved.Load()
}

// Capacity is the VRAM placeable on the worker if this router had nothing in flight.
func (w *Worker) Capacity() int64 {
	return w.totalVRAM.Load() - w.externalUsed.Load()
}

// UsedVRAM is the router's estimate of VRAM in use on the worker.
func (w *Worker) UsedVRAM() int64 { return w.externalUsed.Load() + w.reserved.Load() }

// TryReserve optimistically claims mb on the control plane. A CAS loop makes
// concurrent bursts unable to oversubscribe the router's view of the worker.
func (w *Worker) TryReserve(mb int64) bool {
	for {
		r := w.reserved.Load()
		if w.totalVRAM.Load()-w.externalUsed.Load()-(r+mb) < 0 {
			return false
		}
		if w.reserved.CompareAndSwap(r, r+mb) {
			return true
		}
	}
}

func (w *Worker) releaseVRAM(mb int64) { w.reserved.Add(-mb) }

// load is the queue depth estimate used for scoring.
func (w *Worker) load() int64 { return max(w.inflight.Load(), w.remoteActive.Load()) }

// WorkerStatus is the JSON view returned by /healthz.
type WorkerStatus struct {
	ID              string  `json:"id"`
	Addr            string  `json:"addr"`
	Healthy         bool    `json:"healthy"`
	Breaker         string  `json:"breaker"`
	TotalVRAMMB     int64   `json:"total_vram_mb"`
	UsedVRAMMB      int64   `json:"used_vram_mb"`
	Inflight        int64   `json:"inflight"`
	EWMALatencyMs   float64 `json:"ewma_latency_ms"`
	LastHeartbeatMs int64   `json:"last_heartbeat_ago_ms"`
}

// Status is a point-in-time snapshot of the worker for /healthz.
func (w *Worker) Status() WorkerStatus {
	ago := int64(-1)
	if ls := w.lastSeen.Load(); ls > 0 {
		ago = time.Since(time.Unix(0, ls)).Milliseconds()
	}
	return WorkerStatus{
		ID: w.ID(), Addr: w.Addr, Healthy: w.Healthy(), Breaker: w.breaker.State().String(),
		TotalVRAMMB: w.totalVRAM.Load(), UsedVRAMMB: w.UsedVRAM(), Inflight: w.inflight.Load(),
		EWMALatencyMs: w.ewma.Value(), LastHeartbeatMs: ago,
	}
}

// Discoverer returns the current set of worker addresses (host:port).
type Discoverer interface {
	Discover(ctx context.Context) ([]string, error)
}

// StaticDiscoverer returns a fixed address list.
type StaticDiscoverer []string

// Discover returns the fixed address list.
func (s StaticDiscoverer) Discover(context.Context) ([]string, error) { return s, nil }

// DNSDiscoverer resolves a hostname (e.g. a Kubernetes headless Service) to
// one address per A/AAAA record, so worker pods added by the HPA are picked up.
type DNSDiscoverer struct {
	Host     string
	Port     int
	Resolver *net.Resolver
}

// Discover resolves the DNS name to one address per IP.
func (d DNSDiscoverer) Discover(ctx context.Context) ([]string, error) {
	r := d.Resolver
	if r == nil {
		r = net.DefaultResolver
	}
	ips, err := r.LookupHost(ctx, d.Host)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ips))
	for _, ip := range ips {
		out = append(out, net.JoinHostPort(ip, strconv.Itoa(d.Port)))
	}
	return out, nil
}

// RegistryConfig sets heartbeat timing, discovery and the per-worker breaker.
type RegistryConfig struct {
	RouterID          string
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	// UnhealthyAfter consecutive heartbeat failures mark a worker unhealthy.
	UnhealthyAfter    int
	DiscoveryInterval time.Duration
	Breaker           BreakerConfig
}

// Registry tracks workers and keeps their state fresh via heartbeats.
type Registry struct {
	cfg  RegistryConfig
	log  *slog.Logger
	disc Discoverer
	cap  *capacitySignal

	mu      sync.RWMutex
	workers map[string]*Worker
	sorted  []*Worker

	idMu sync.Mutex // serialises identity claims across heartbeats

	// OnIdentified is called once per worker connection when its identity
	// is first confirmed. The router uses it to pre-create metric series.
	OnIdentified func(id string)
}

// NewRegistry tracks the workers that disc returns. Call Run to start heartbeats.
func NewRegistry(cfg RegistryConfig, disc Discoverer, log *slog.Logger) *Registry {
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 500 * time.Millisecond
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 400 * time.Millisecond
	}
	if cfg.UnhealthyAfter <= 0 {
		cfg.UnhealthyAfter = 2
	}
	if cfg.DiscoveryInterval <= 0 {
		cfg.DiscoveryInterval = 5 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	return &Registry{cfg: cfg, log: log, disc: disc, cap: newCapacitySignal(), workers: map[string]*Worker{}}
}

// Workers returns a stable-ordered snapshot of known workers.
func (r *Registry) Workers() []*Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.sorted
}

// Sync reconciles the registry with addrs: new addresses are dialed, missing
// ones are removed and their connections closed once idle.
func (r *Registry) Sync(addrs []string) {
	want := make(map[string]bool, len(addrs))
	for _, a := range addrs {
		want[a] = true
	}
	r.mu.Lock()
	changed := false
	for a := range want {
		if _, ok := r.workers[a]; ok {
			continue
		}
		w, err := r.newWorker(a)
		if err != nil {
			r.log.Error("dial worker", "addr", a, "err", err)
			continue
		}
		r.workers[a] = w
		changed = true
		r.log.Info("worker added", "addr", a)
	}
	for a, w := range r.workers {
		if want[a] {
			continue
		}
		delete(r.workers, a)
		changed = true
		w.healthy.Store(false)
		r.log.Info("worker removed", "addr", a, "id", w.ID())
		go closeWhenIdle(w)
	}
	if changed {
		r.resort()
	}
	r.mu.Unlock()
}

// resort rebuilds the ordered snapshot; callers hold r.mu.
func (r *Registry) resort() {
	sorted := make([]*Worker, 0, len(r.workers))
	for _, w := range r.workers {
		sorted = append(sorted, w)
	}
	slices.SortFunc(sorted, func(a, b *Worker) int {
		if a.Addr < b.Addr {
			return -1
		}
		return 1
	})
	r.sorted = sorted
}

// replace swaps w for a freshly dialed Worker on the same address. The new
// connection re-resolves the hostname and starts with no identity, breaker
// history or latency estimate.
func (r *Registry) replace(w *Worker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.workers[w.Addr] != w {
		return // already replaced or removed
	}
	nw, err := r.newWorker(w.Addr)
	if err != nil {
		r.log.Error("redial worker", "addr", w.Addr, "err", err)
		return
	}
	r.workers[w.Addr] = nw
	r.resort()
	go closeWhenIdle(w)
}

// claimIdentity pins id to w on first contact and reports whether id is
// consistent with w's pinned identity and unique across the registry.
func (r *Registry) claimIdentity(w *Worker, id string) bool {
	r.idMu.Lock()
	defer r.idMu.Unlock()
	if prev := w.id.Load(); prev != nil {
		return *prev == id
	}
	if r.idClaimedByOther(id, w) {
		return false
	}
	w.id.Store(&id)
	if r.OnIdentified != nil {
		r.OnIdentified(id)
	}
	return true
}

// idClaimedByOther reports whether a worker other than self already holds id.
func (r *Registry) idClaimedByOther(id string, self *Worker) bool {
	for _, o := range r.Workers() {
		if o != self {
			if p := o.id.Load(); p != nil && *p == id {
				return true
			}
		}
	}
	return false
}

func closeWhenIdle(w *Worker) {
	deadline := time.Now().Add(30 * time.Second)
	for w.inflight.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	_ = w.conn.Close()
}

func (r *Registry) newWorker(addr string) (*Worker, error) {
	// passthrough: resolve the hostname on every dial. gRPC's dns resolver
	// rate-limits re-resolution to once per 30s, which would delay recovery
	// when a restarted container comes back on a new IP.
	conn, err := grpc.NewClient("passthrough:///"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// Reconnect quickly after a worker restarts; the gRPC default backs
		// off to 120s, which would dominate recovery time.
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 2 * time.Second},
			MinConnectTimeout: 2 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 3 * time.Second, PermitWithoutStream: true}),
	)
	if err != nil {
		return nil, err
	}
	w := &Worker{
		Addr:    addr,
		conn:    conn,
		client:  pb.NewWorkerServiceClient(conn),
		breaker: NewBreaker(r.cfg.Breaker),
		ewma:    ewma.New(0.3),
	}
	w.breaker.notify = func(from, to BreakerState) {
		r.log.Warn("circuit breaker transition", "worker", w.ID(), "from", from.String(), "to", to.String())
		if to != StateOpen {
			r.cap.broadcast()
		}
	}
	return w, nil
}

// Run discovers workers and polls heartbeats until ctx is cancelled.
func (r *Registry) Run(ctx context.Context) {
	r.discover(ctx)
	r.heartbeatAll(ctx)
	hb := time.NewTicker(r.cfg.HeartbeatInterval)
	defer hb.Stop()
	disc := time.NewTicker(r.cfg.DiscoveryInterval)
	defer disc.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-disc.C:
			r.discover(ctx)
		case <-hb.C:
			r.heartbeatAll(ctx)
		}
	}
}

// Close closes all worker connections.
func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, w := range r.workers {
		_ = w.conn.Close()
	}
}

func (r *Registry) discover(ctx context.Context) {
	addrs, err := r.disc.Discover(ctx)
	if err != nil {
		r.log.Warn("worker discovery failed; keeping current set", "err", err)
		return
	}
	r.Sync(addrs)
}

func (r *Registry) heartbeatAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, w := range r.Workers() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.heartbeat(ctx, w)
		}()
	}
	wg.Wait()
}

func (r *Registry) heartbeat(ctx context.Context, w *Worker) {
	hctx, cancel := context.WithTimeout(ctx, r.cfg.HeartbeatTimeout)
	defer cancel()
	before := w.reserved.Load()
	resp, err := w.client.Heartbeat(hctx, &pb.HeartbeatRequest{RouterId: r.cfg.RouterID})
	after := w.reserved.Load()
	switch {
	case ctx.Err() != nil:
		return // router shutting down
	case err != nil:
		r.recordHeartbeatFailure(w, err)
	case !r.claimIdentity(w, resp.GetWorkerId()):
		// An address that suddenly answers as a different worker, or as one
		// another address already owns, is a stale connection to a reused IP
		// (container recreated, pod IP recycled). Trusting it would
		// double-count that worker's VRAM, so drop the connection and redial.
		w.healthy.Store(false)
		r.log.Warn("worker identity mismatch; reconnecting", "addr", w.Addr,
			"known_id", w.ID(), "reported_id", resp.GetWorkerId())
		r.replace(w)
	default:
		r.applyHeartbeat(w, resp, max(before, after))
	}
}

// recordHeartbeatFailure marks the worker unhealthy after UnhealthyAfter
// consecutive failures. The breaker trips once, on the healthy -> unhealthy
// edge; while the worker stays down it drifts to half-open and waits, so a
// restarted worker is probed as soon as heartbeats return.
func (r *Registry) recordHeartbeatFailure(w *Worker, err error) {
	n := w.hbFailures.Add(1)
	if int(n) >= r.cfg.UnhealthyAfter && w.healthy.Swap(false) {
		w.breaker.Trip()
		r.log.Warn("worker unhealthy: heartbeat failing", "worker", w.ID(), "addr", w.Addr, "err", err)
	}
}

// applyHeartbeat reconciles the router's view with the worker's report. The
// reported usage includes this router's own in-flight reservations;
// subtracting the larger local snapshot taken around the RPC keeps the
// estimate optimistic, with the worker's ResourceExhausted as the backstop.
func (r *Registry) applyHeartbeat(w *Worker, resp *pb.HeartbeatResponse, localReserved int64) {
	w.hbFailures.Store(0)
	w.lastSeen.Store(time.Now().UnixNano())
	used := resp.GetTotalVramMb() - resp.GetAvailableVramMb()
	w.totalVRAM.Store(resp.GetTotalVramMb())
	w.externalUsed.Store(max(0, used-localReserved))
	w.remoteActive.Store(int64(resp.GetActiveRequests()))
	if l := resp.GetEwmaLatencyMs(); l > 0 {
		w.ewma.Observe(l)
	}
	if was := w.healthy.Swap(resp.GetHealthy()); was != resp.GetHealthy() {
		r.log.Info("worker health changed", "worker", w.ID(), "addr", w.Addr, "healthy", resp.GetHealthy())
	}
	r.cap.broadcast()
}

// capacitySignal wakes requests waiting for VRAM. Waiters take the channel
// before checking capacity, then block on it; broadcast closes and replaces
// it, so no wakeup can be lost between the check and the wait.
type capacitySignal struct {
	mu sync.Mutex
	ch chan struct{}
}

func newCapacitySignal() *capacitySignal { return &capacitySignal{ch: make(chan struct{})} }

func (c *capacitySignal) wait() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ch
}

func (c *capacitySignal) broadcast() {
	c.mu.Lock()
	close(c.ch)
	c.ch = make(chan struct{})
	c.mu.Unlock()
}
