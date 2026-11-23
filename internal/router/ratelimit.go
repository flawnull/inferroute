package router

import (
	"math"
	"sync"
	"time"
)

// Limit is a token-bucket configuration: Rate tokens per second refill, up to
// Burst tokens of capacity.
type Limit struct {
	Rate  float64
	Burst float64
}

type bucket struct {
	mu     sync.Mutex
	limit  Limit
	tokens float64
	last   time.Time
}

// take refills lazily from elapsed time and tries to consume one token.
// It returns how long until a token is available when it fails.
func (b *bucket) take(now time.Time) (bool, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens = math.Min(b.limit.Burst, b.tokens+elapsed.Seconds()*b.limit.Rate)
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	if b.limit.Rate <= 0 {
		return false, time.Hour
	}
	wait := time.Duration((1 - b.tokens) / b.limit.Rate * float64(time.Second))
	return false, wait
}

// RateLimiter is a per-tenant token bucket limiter. Buckets are created on
// first use and evicted once idle long enough to be full again, which bounds
// memory without changing behaviour.
type RateLimiter struct {
	mu        sync.RWMutex
	buckets   map[string]*bucket
	def       Limit
	overrides map[string]Limit
	now       func() time.Time
}

// NewRateLimiter gives every tenant a token bucket with limit def, or its entry in overrides.
func NewRateLimiter(def Limit, overrides map[string]Limit) *RateLimiter {
	if overrides == nil {
		overrides = map[string]Limit{}
	}
	return &RateLimiter{buckets: map[string]*bucket{}, def: def, overrides: overrides, now: time.Now}
}

func (rl *RateLimiter) limitFor(tenant string) Limit {
	if l, ok := rl.overrides[tenant]; ok {
		return l
	}
	return rl.def
}

// Allow consumes a token for tenant. When denied, retryAfter is the time
// until the next token refills.
func (rl *RateLimiter) Allow(tenant string) (ok bool, retryAfter time.Duration) {
	now := rl.now()
	rl.mu.RLock()
	b := rl.buckets[tenant]
	rl.mu.RUnlock()
	if b == nil {
		rl.mu.Lock()
		if b = rl.buckets[tenant]; b == nil {
			l := rl.limitFor(tenant)
			b = &bucket{limit: l, tokens: l.Burst, last: now}
			rl.buckets[tenant] = b
		}
		rl.mu.Unlock()
	}
	return b.take(now)
}

// Evict removes buckets that have been idle long enough to refill completely.
func (rl *RateLimiter) Evict() int {
	now := rl.now()
	rl.mu.Lock()
	defer rl.mu.Unlock()
	n := 0
	for k, b := range rl.buckets {
		b.mu.Lock()
		full := b.limit.Rate > 0 && now.Sub(b.last).Seconds()*b.limit.Rate+b.tokens >= b.limit.Burst
		b.mu.Unlock()
		if full {
			delete(rl.buckets, k)
			n++
		}
	}
	return n
}

// Len reports how many tenant buckets are held in memory.
func (rl *RateLimiter) Len() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return len(rl.buckets)
}
