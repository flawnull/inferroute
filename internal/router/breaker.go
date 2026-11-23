package router

import (
	"math/rand/v2"
	"sync"
	"time"
)

// BreakerState values double as the inferroute_circuit_breaker_state gauge.
type BreakerState int

// Breaker states: closed passes traffic, open rejects it, half-open admits probes.
const (
	StateClosed   BreakerState = 0
	StateHalfOpen BreakerState = 1
	StateOpen     BreakerState = 2
)

func (s BreakerState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half_open"
	case StateOpen:
		return "open"
	}
	return "unknown"
}

// BreakerConfig tunes when a worker's breaker opens and how it recovers.
type BreakerConfig struct {
	// FailureThreshold consecutive failures trip Closed -> Open.
	FailureThreshold int
	// OpenTimeout is the base wait before the first half-open probe. Each
	// consecutive trip without recovering doubles it, up to MaxOpenTimeout.
	OpenTimeout    time.Duration
	MaxOpenTimeout time.Duration
	// Jitter stretches each open period by a random factor in [0, Jitter),
	// so routers and workers that failed together do not probe together.
	Jitter float64
	// HalfOpenMaxProbes caps concurrent trial requests while half-open.
	HalfOpenMaxProbes int
	// HalfOpenSuccesses successful probes close the breaker.
	HalfOpenSuccesses int
}

func (c BreakerConfig) withDefaults() BreakerConfig {
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = 5
	}
	if c.OpenTimeout <= 0 {
		c.OpenTimeout = 2 * time.Second
	}
	if c.MaxOpenTimeout < c.OpenTimeout {
		c.MaxOpenTimeout = 30 * time.Second
	}
	if c.Jitter < 0 {
		c.Jitter = 0
	}
	if c.HalfOpenMaxProbes <= 0 {
		c.HalfOpenMaxProbes = 1
	}
	if c.HalfOpenSuccesses <= 0 {
		c.HalfOpenSuccesses = 2
	}
	return c
}

// Outcome is how a request attempt ended, from the breaker's point of view.
type Outcome int

const (
	// Success resets the failure count, and counts toward closing when half-open.
	Success Outcome = iota
	// Failure indicates worker health trouble (Unavailable, timeout, crash).
	Failure
	// Neutral says nothing about health (capacity rejection, client cancel).
	Neutral
)

// Ticket ties an outcome to the breaker generation it was admitted under, so
// results from requests that straddle a state change are ignored.
type Ticket struct {
	gen   uint64
	probe bool
}

// Breaker is a per-worker Closed -> Open -> Half-Open state machine.
type Breaker struct {
	mu        sync.Mutex
	cfg       BreakerConfig
	state     BreakerState
	gen       uint64
	failures  int
	trips     int
	openUntil time.Time
	probes    int
	successes int

	now    func() time.Time
	rand   func() float64
	notify func(from, to BreakerState)
}

// NewBreaker returns a closed breaker, filling unset config fields with defaults.
func NewBreaker(cfg BreakerConfig) *Breaker {
	return &Breaker{cfg: cfg.withDefaults(), now: time.Now, rand: rand.Float64}
}

// State reports the current state, promoting Open to Half-Open once the open
// period has elapsed.
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance()
	return b.state
}

// Ready reports whether Acquire would currently succeed, without consuming a
// probe slot. The scheduler uses it to filter candidates.
func (b *Breaker) Ready() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance()
	switch b.state {
	case StateClosed:
		return true
	case StateHalfOpen:
		return b.probes < b.cfg.HalfOpenMaxProbes
	}
	return false
}

// Acquire admits a request. In Half-Open it hands out at most
// HalfOpenMaxProbes concurrent probe tickets.
func (b *Breaker) Acquire() (Ticket, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance()
	switch b.state {
	case StateClosed:
		return Ticket{gen: b.gen}, true
	case StateHalfOpen:
		if b.probes < b.cfg.HalfOpenMaxProbes {
			b.probes++
			return Ticket{gen: b.gen, probe: true}, true
		}
	}
	return Ticket{}, false
}

// Record reports the outcome of an admitted request.
func (b *Breaker) Record(t Ticket, o Outcome) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if t.gen != b.gen {
		return // admitted under an earlier state; stale
	}
	switch b.state {
	case StateClosed:
		switch o {
		case Success:
			b.failures = 0
		case Failure:
			b.failures++
			if b.failures >= b.cfg.FailureThreshold {
				b.trip()
			}
		}
	case StateHalfOpen:
		if t.probe {
			b.probes--
		}
		switch o {
		case Success:
			b.successes++
			if b.successes >= b.cfg.HalfOpenSuccesses {
				b.transition(StateClosed)
				b.trips = 0
			}
		case Failure:
			b.trip()
		}
	}
}

// Trip forces the breaker open, e.g. when the worker becomes unreachable.
func (b *Breaker) Trip() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != StateOpen {
		b.trip()
	}
}

// OpenUntil returns when the current open period ends (zero if not open).
func (b *Breaker) OpenUntil() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != StateOpen {
		return time.Time{}
	}
	return b.openUntil
}

func (b *Breaker) trip() {
	b.trips++
	d := b.cfg.OpenTimeout
	for i := 1; i < b.trips && d < b.cfg.MaxOpenTimeout; i++ {
		d *= 2
	}
	d = min(d, b.cfg.MaxOpenTimeout)
	d += time.Duration(float64(d) * b.cfg.Jitter * b.rand())
	b.openUntil = b.now().Add(d)
	b.transition(StateOpen)
}

func (b *Breaker) advance() {
	if b.state == StateOpen && !b.now().Before(b.openUntil) {
		b.transition(StateHalfOpen)
	}
}

func (b *Breaker) transition(to BreakerState) {
	from := b.state
	b.state = to
	b.gen++
	b.failures, b.probes, b.successes = 0, 0, 0
	if b.notify != nil && from != to {
		b.notify(from, to)
	}
}
