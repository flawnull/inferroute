package router

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"inferroute/internal/ewma"
)

type fakeWorker struct {
	addr     string
	vram     int64
	external int64
	latency  float64
	healthy  bool
}

func newTestWorker(f fakeWorker) *Worker {
	w := &Worker{Addr: f.addr, breaker: NewBreaker(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Hour}), ewma: ewma.New(0.5)}
	w.totalVRAM.Store(f.vram)
	w.externalUsed.Store(f.external)
	w.healthy.Store(f.healthy)
	if f.latency > 0 {
		w.ewma.Set(f.latency)
	}
	return w
}

func testScheduler(queue time.Duration, ws ...*Worker) *Scheduler {
	reg := NewRegistry(RegistryConfig{}, StaticDiscoverer(nil), nil)
	reg.sorted = ws
	return NewScheduler(reg, queue)
}

func acquireOne(t *testing.T, s *Scheduler, vram int64, exclude map[*Worker]bool) *Lease {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	l, err := s.Acquire(ctx, vram, exclude)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	return l
}

func TestSchedulerPrefersLowLatency(t *testing.T) {
	fast := newTestWorker(fakeWorker{addr: "fast", vram: 10000, latency: 10, healthy: true})
	slow := newTestWorker(fakeWorker{addr: "slow", vram: 10000, latency: 50, healthy: true})
	s := testScheduler(0, slow, fast)
	if l := acquireOne(t, s, 1000, nil); l.Worker != fast {
		t.Fatalf("picked %s, want fast", l.Worker.Addr)
	}
}

func TestSchedulerPrefersLeastLoaded(t *testing.T) {
	a := newTestWorker(fakeWorker{addr: "a", vram: 100000, latency: 20, healthy: true})
	b := newTestWorker(fakeWorker{addr: "b", vram: 100000, latency: 20, healthy: true})
	s := testScheduler(0, a, b)
	// Leases are held, so load accumulates and placement must alternate.
	counts := map[*Worker]int{}
	for range 10 {
		counts[acquireOne(t, s, 100, nil).Worker]++
	}
	if counts[a] != 5 || counts[b] != 5 {
		t.Fatalf("placement a=%d b=%d, want 5/5", counts[a], counts[b])
	}
}

func TestSchedulerLoadOutweighsSmallLatencyGap(t *testing.T) {
	fast := newTestWorker(fakeWorker{addr: "fast", vram: 100000, latency: 10, healthy: true})
	slower := newTestWorker(fakeWorker{addr: "slower", vram: 100000, latency: 15, healthy: true})
	fast.inflight.Store(3)
	s := testScheduler(0, fast, slower)
	if l := acquireOne(t, s, 100, nil); l.Worker != slower {
		t.Fatalf("picked %s; a busy fast worker should lose to an idle slightly slower one", l.Worker.Addr)
	}
}

func TestSchedulerFiltersIneligibleWorkers(t *testing.T) {
	unhealthy := newTestWorker(fakeWorker{addr: "unhealthy", vram: 50000, latency: 1})
	open := newTestWorker(fakeWorker{addr: "open", vram: 50000, latency: 1, healthy: true})
	open.breaker.Trip()
	small := newTestWorker(fakeWorker{addr: "small", vram: 500, latency: 1, healthy: true})
	full := newTestWorker(fakeWorker{addr: "full", vram: 50000, external: 49500, latency: 1, healthy: true})
	ok := newTestWorker(fakeWorker{addr: "ok", vram: 50000, latency: 100, healthy: true})
	s := testScheduler(0, unhealthy, open, small, full, ok)
	if l := acquireOne(t, s, 1000, nil); l.Worker != ok {
		t.Fatalf("picked %s, want ok", l.Worker.Addr)
	}
}

func TestSchedulerExclude(t *testing.T) {
	a := newTestWorker(fakeWorker{addr: "a", vram: 10000, latency: 1, healthy: true})
	b := newTestWorker(fakeWorker{addr: "b", vram: 10000, latency: 100, healthy: true})
	s := testScheduler(0, a, b)
	if l := acquireOne(t, s, 100, map[*Worker]bool{a: true}); l.Worker != b {
		t.Fatalf("picked excluded worker %s", l.Worker.Addr)
	}
}

func TestSchedulerNoCapacityFailsFast(t *testing.T) {
	a := newTestWorker(fakeWorker{addr: "a", vram: 4000, healthy: true})
	s := testScheduler(time.Hour, a)
	start := time.Now()
	_, err := s.Acquire(context.Background(), 8000, nil)
	if !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("err = %v, want ErrNoCapacity", err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("ErrNoCapacity should not wait")
	}
}

func TestSchedulerNoWorkersAfterQueueTimeout(t *testing.T) {
	a := newTestWorker(fakeWorker{addr: "a", vram: 4000})
	s := testScheduler(20*time.Millisecond, a)
	if _, err := s.Acquire(context.Background(), 100, nil); !errors.Is(err, ErrNoWorkers) {
		t.Fatalf("err = %v, want ErrNoWorkers", err)
	}
}

func TestSchedulerQueueTimeoutWhenFull(t *testing.T) {
	a := newTestWorker(fakeWorker{addr: "a", vram: 1000, healthy: true})
	s := testScheduler(20*time.Millisecond, a)
	acquireOne(t, s, 1000, nil)
	if _, err := s.Acquire(context.Background(), 1000, nil); !errors.Is(err, ErrQueueTimeout) {
		t.Fatalf("err = %v, want ErrQueueTimeout", err)
	}
}

func TestSchedulerWaitsForReleasedCapacity(t *testing.T) {
	a := newTestWorker(fakeWorker{addr: "a", vram: 1000, healthy: true})
	s := testScheduler(time.Second, a)
	held := acquireOne(t, s, 1000, nil)
	go func() {
		time.Sleep(20 * time.Millisecond)
		held.Done(Success)
	}()
	l := acquireOne(t, s, 1000, nil)
	if l.Worker != a || a.reserved.Load() != 1000 {
		t.Fatalf("reserved = %d", a.reserved.Load())
	}
}

func TestSchedulerConcurrentBurstNeverOversubscribes(t *testing.T) {
	a := newTestWorker(fakeWorker{addr: "a", vram: 10000, latency: 5, healthy: true})
	b := newTestWorker(fakeWorker{addr: "b", vram: 6000, latency: 5, healthy: true})
	s := testScheduler(10*time.Millisecond, a, b)

	var ok atomic.Int64
	var mu sync.Mutex
	var leases []*Lease
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := s.Acquire(context.Background(), 1000, nil)
			if err != nil {
				return
			}
			ok.Add(1)
			mu.Lock()
			leases = append(leases, l)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if ok.Load() != 16 {
		t.Fatalf("granted %d leases, want exactly 16 (10+6 GB / 1 GB)", ok.Load())
	}
	if a.reserved.Load() != 10000 || b.reserved.Load() != 6000 {
		t.Fatalf("reserved a=%d b=%d", a.reserved.Load(), b.reserved.Load())
	}
	for _, l := range leases {
		l.Done(Success)
	}
	if a.reserved.Load() != 0 || b.reserved.Load() != 0 || a.inflight.Load() != 0 {
		t.Fatal("reservations leaked")
	}
}

func TestSchedulerHalfOpenAdmitsSingleProbe(t *testing.T) {
	a := newTestWorker(fakeWorker{addr: "a", vram: 10000, latency: 1, healthy: true})
	b := newTestWorker(fakeWorker{addr: "b", vram: 10000, latency: 100, healthy: true})
	clk := newFakeClock()
	a.breaker = NewBreaker(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Second, HalfOpenMaxProbes: 1})
	a.breaker.now = clk.Now
	a.breaker.Trip()
	clk.Advance(2 * time.Second) // half-open

	s := testScheduler(0, a, b)
	first := acquireOne(t, s, 100, nil)
	second := acquireOne(t, s, 100, nil)
	if first.Worker != a || second.Worker != b {
		t.Fatalf("first=%s second=%s; half-open worker should take exactly one probe", first.Worker.Addr, second.Worker.Addr)
	}
}

func TestScoreMonotonic(t *testing.T) {
	w := newTestWorker(fakeWorker{vram: 10000, latency: 10, healthy: true})
	s0 := Score(w, 1000)
	w.inflight.Store(2)
	s1 := Score(w, 1000)
	w.externalUsed.Store(5000)
	s2 := Score(w, 1000)
	if !(s0 < s1 && s1 < s2) {
		t.Fatalf("score not monotonic in load and VRAM: %v %v %v", s0, s1, s2)
	}
}
