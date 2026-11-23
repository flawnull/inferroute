package router

import (
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func testBreaker(cfg BreakerConfig, clk *fakeClock, r float64) *Breaker {
	b := NewBreaker(cfg)
	b.now = clk.Now
	b.rand = func() float64 { return r }
	return b
}

func mustAcquire(t *testing.T, b *Breaker) Ticket {
	t.Helper()
	tk, ok := b.Acquire()
	if !ok {
		t.Fatalf("Acquire rejected in state %v", b.State())
	}
	return tk
}

func TestBreakerTripsAfterConsecutiveFailures(t *testing.T) {
	clk := newFakeClock()
	b := testBreaker(BreakerConfig{FailureThreshold: 3, OpenTimeout: time.Second}, clk, 0)

	b.Record(mustAcquire(t, b), Failure)
	b.Record(mustAcquire(t, b), Failure)
	b.Record(mustAcquire(t, b), Success) // resets the streak
	b.Record(mustAcquire(t, b), Failure)
	b.Record(mustAcquire(t, b), Failure)
	if b.State() != StateClosed {
		t.Fatalf("state = %v, want closed (streak was reset)", b.State())
	}
	b.Record(mustAcquire(t, b), Failure)
	if b.State() != StateOpen {
		t.Fatalf("state = %v, want open", b.State())
	}
	if _, ok := b.Acquire(); ok {
		t.Fatal("open breaker admitted a request")
	}
	if b.Ready() {
		t.Fatal("open breaker reports ready")
	}
}

func TestBreakerNeutralOutcomesDoNotTrip(t *testing.T) {
	clk := newFakeClock()
	b := testBreaker(BreakerConfig{FailureThreshold: 2}, clk, 0)
	for range 10 {
		b.Record(mustAcquire(t, b), Neutral)
	}
	if b.State() != StateClosed {
		t.Fatalf("state = %v", b.State())
	}
}

func TestBreakerHalfOpenProbeLimitAndRecovery(t *testing.T) {
	clk := newFakeClock()
	b := testBreaker(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Second, HalfOpenMaxProbes: 1, HalfOpenSuccesses: 2}, clk, 0)
	b.Record(mustAcquire(t, b), Failure)

	clk.Advance(999 * time.Millisecond)
	if b.State() != StateOpen {
		t.Fatalf("state = %v before timeout", b.State())
	}
	clk.Advance(time.Millisecond)
	if b.State() != StateHalfOpen {
		t.Fatalf("state = %v after timeout, want half-open", b.State())
	}

	p1 := mustAcquire(t, b)
	if _, ok := b.Acquire(); ok {
		t.Fatal("second concurrent probe admitted; want at most 1")
	}
	if b.Ready() {
		t.Fatal("Ready() true with probe slot taken")
	}
	b.Record(p1, Success)
	if b.State() != StateHalfOpen {
		t.Fatalf("closed after 1 success, want 2")
	}
	b.Record(mustAcquire(t, b), Success)
	if b.State() != StateClosed {
		t.Fatalf("state = %v, want closed", b.State())
	}
}

func TestBreakerHalfOpenFailureReopensWithBackoff(t *testing.T) {
	clk := newFakeClock()
	b := testBreaker(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Second, MaxOpenTimeout: 3 * time.Second}, clk, 0)

	b.Record(mustAcquire(t, b), Failure) // trip 1: 1s
	if got := b.OpenUntil().Sub(clk.Now()); got != time.Second {
		t.Fatalf("first open period = %v", got)
	}
	clk.Advance(time.Second)
	b.Record(mustAcquire(t, b), Failure) // probe fails, trip 2: 2s
	if got := b.OpenUntil().Sub(clk.Now()); got != 2*time.Second {
		t.Fatalf("second open period = %v, want 2s", got)
	}
	clk.Advance(2 * time.Second)
	b.Record(mustAcquire(t, b), Failure) // trip 3: capped at 3s
	if got := b.OpenUntil().Sub(clk.Now()); got != 3*time.Second {
		t.Fatalf("third open period = %v, want 3s cap", got)
	}
}

func TestBreakerJitterStretchesOpenPeriod(t *testing.T) {
	clk := newFakeClock()
	b := testBreaker(BreakerConfig{FailureThreshold: 1, OpenTimeout: 2 * time.Second, Jitter: 0.5}, clk, 0.5)
	b.Record(mustAcquire(t, b), Failure)
	// 2s + 2s*0.5*0.5
	if got := b.OpenUntil().Sub(clk.Now()); got != 2500*time.Millisecond {
		t.Fatalf("open period = %v, want 2.5s", got)
	}
}

func TestBreakerJitterSpreadsSimultaneousTrips(t *testing.T) {
	// Many breakers tripped at the same instant (e.g. a shared dependency
	// failed) must not all become half-open at the same instant.
	clk := newFakeClock()
	base := time.Second
	seen := map[time.Time]bool{}
	for range 50 {
		b := NewBreaker(BreakerConfig{FailureThreshold: 1, OpenTimeout: base, Jitter: 1})
		b.now = clk.Now
		b.Record(mustAcquire(t, b), Failure)
		u := b.OpenUntil()
		if d := u.Sub(clk.Now()); d < base || d >= 2*base {
			t.Fatalf("open period %v outside [1s, 2s)", d)
		}
		seen[u] = true
	}
	if len(seen) < 40 {
		t.Fatalf("only %d distinct probe times across 50 breakers", len(seen))
	}
}

func TestBreakerIgnoresStaleTickets(t *testing.T) {
	clk := newFakeClock()
	b := testBreaker(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Second, HalfOpenSuccesses: 1}, clk, 0)
	old := mustAcquire(t, b)             // admitted while closed
	b.Record(mustAcquire(t, b), Failure) // trips
	clk.Advance(time.Second)
	b.Record(old, Success) // stale: must not close the breaker
	if b.State() != StateHalfOpen {
		t.Fatalf("state = %v, stale success changed state", b.State())
	}
}

func TestBreakerTripForcesOpen(t *testing.T) {
	clk := newFakeClock()
	b := testBreaker(BreakerConfig{OpenTimeout: time.Second}, clk, 0)
	b.Trip()
	if b.State() != StateOpen {
		t.Fatal("Trip did not open breaker")
	}
	until := b.OpenUntil()
	b.Trip() // no-op while open: must not extend the period
	if !b.OpenUntil().Equal(until) {
		t.Fatal("Trip while open extended the open period")
	}
}

func TestBreakerConcurrentUse(t *testing.T) {
	b := NewBreaker(BreakerConfig{FailureThreshold: 3, OpenTimeout: time.Millisecond, HalfOpenMaxProbes: 2})
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 500 {
				if tk, ok := b.Acquire(); ok {
					o := Success
					if (i+j)%3 == 0 {
						o = Failure
					}
					b.Record(tk, o)
				}
				_ = b.State()
			}
		}()
	}
	wg.Wait()
}
