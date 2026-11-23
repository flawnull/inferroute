// Package ewma provides a concurrency-safe exponentially weighted moving average.
package ewma

import "sync"

type EWMA struct {
	mu    sync.Mutex
	alpha float64
	value float64
	init  bool
}

// New returns an EWMA with smoothing factor alpha in (0,1]. Higher alpha
// reacts faster to change.
func New(alpha float64) *EWMA {
	if alpha <= 0 || alpha > 1 {
		alpha = 0.2
	}
	return &EWMA{alpha: alpha}
}

func (e *EWMA) Observe(v float64) {
	e.mu.Lock()
	if !e.init {
		e.value, e.init = v, true
	} else {
		e.value = e.alpha*v + (1-e.alpha)*e.value
	}
	e.mu.Unlock()
}

// Set overwrites the current value, e.g. from an authoritative remote report.
func (e *EWMA) Set(v float64) {
	e.mu.Lock()
	e.value, e.init = v, true
	e.mu.Unlock()
}

func (e *EWMA) Value() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.value
}
