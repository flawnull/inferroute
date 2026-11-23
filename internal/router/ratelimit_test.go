package router

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testLimiter(def Limit, overrides map[string]Limit) (*RateLimiter, *fakeClock) {
	clk := newFakeClock()
	rl := NewRateLimiter(def, overrides)
	rl.now = clk.Now
	return rl, clk
}

func TestRateLimiterBurstThenDeny(t *testing.T) {
	rl, _ := testLimiter(Limit{Rate: 10, Burst: 5}, nil)
	for i := range 5 {
		if ok, _ := rl.Allow("a"); !ok {
			t.Fatalf("request %d denied within burst", i)
		}
	}
	ok, wait := rl.Allow("a")
	if ok {
		t.Fatal("request beyond burst allowed")
	}
	if wait != 100*time.Millisecond {
		t.Fatalf("retryAfter = %v, want 100ms (1 token at 10/s)", wait)
	}
}

func TestRateLimiterRefill(t *testing.T) {
	rl, clk := testLimiter(Limit{Rate: 10, Burst: 5}, nil)
	for range 5 {
		rl.Allow("a")
	}
	clk.Advance(250 * time.Millisecond) // 2.5 tokens
	allowed := 0
	for range 5 {
		if ok, _ := rl.Allow("a"); ok {
			allowed++
		}
	}
	if allowed != 2 {
		t.Fatalf("allowed after 250ms = %d, want 2", allowed)
	}
	clk.Advance(time.Hour) // refill is capped at burst
	allowed = 0
	for range 10 {
		if ok, _ := rl.Allow("a"); ok {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("allowed after long idle = %d, want burst 5", allowed)
	}
}

func TestRateLimiterPerTenantIsolationAndOverrides(t *testing.T) {
	rl, _ := testLimiter(Limit{Rate: 1, Burst: 1}, map[string]Limit{"premium": {Rate: 100, Burst: 3}})
	if ok, _ := rl.Allow("free"); !ok {
		t.Fatal("first free request denied")
	}
	if ok, _ := rl.Allow("free"); ok {
		t.Fatal("free tenant exceeded burst 1")
	}
	for i := range 3 {
		if ok, _ := rl.Allow("premium"); !ok {
			t.Fatalf("premium request %d denied; another tenant's usage leaked", i)
		}
	}
	if ok, _ := rl.Allow("other"); !ok {
		t.Fatal("fresh tenant denied")
	}
}

func TestRateLimiterConcurrentExactBurst(t *testing.T) {
	// With a frozen clock, exactly Burst requests may pass however many
	// goroutines race for the same bucket.
	rl, _ := testLimiter(Limit{Rate: 1, Burst: 100}, nil)
	var ok atomic.Int64
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				if allowed, _ := rl.Allow("t"); allowed {
					ok.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 100 {
		t.Fatalf("allowed = %d, want 100", ok.Load())
	}
}

func TestRateLimiterEvictsIdleBuckets(t *testing.T) {
	rl, clk := testLimiter(Limit{Rate: 10, Burst: 10}, nil)
	rl.Allow("a")
	for range 10 {
		rl.Allow("b")
	}
	clk.Advance(100 * time.Millisecond) // a refills to 10 (full); b only to 1
	if n := rl.Evict(); n != 1 {
		t.Fatalf("evicted %d, want 1", n)
	}
	if rl.Len() != 1 {
		t.Fatalf("len = %d", rl.Len())
	}
	clk.Advance(time.Second)
	rl.Evict()
	if rl.Len() != 0 {
		t.Fatalf("len after idle = %d", rl.Len())
	}
}
