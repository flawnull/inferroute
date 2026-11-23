package worker

import (
	"math"
	"sync/atomic"
)

// Faults holds fault-injection knobs. Every field is read on the hot path and
// written by the admin endpoint, so each one is an atomic.
type Faults struct {
	errorRate      atomic.Uint64 // float64 bits, probability in [0,1]
	extraLatencyMs atomic.Int64
	slowdown       atomic.Uint64 // float64 bits, latency multiplier >= 1
	unhealthy      atomic.Bool
}

// FaultConfig is the JSON shape accepted and returned by /admin/faults.
type FaultConfig struct {
	ErrorRate      float64 `json:"error_rate"`
	ExtraLatencyMs int64   `json:"extra_latency_ms"`
	Slowdown       float64 `json:"slowdown"`
	Unhealthy      bool    `json:"unhealthy"`
}

// NewFaults returns fault knobs set to cfg.
func NewFaults(cfg FaultConfig) *Faults {
	f := &Faults{}
	f.Set(cfg)
	return f
}

// Set replaces every knob, clamping values to their valid ranges.
func (f *Faults) Set(cfg FaultConfig) {
	rate := math.Max(0, math.Min(1, cfg.ErrorRate))
	slow := cfg.Slowdown
	if slow < 1 {
		slow = 1
	}
	if cfg.ExtraLatencyMs < 0 {
		cfg.ExtraLatencyMs = 0
	}
	f.errorRate.Store(math.Float64bits(rate))
	f.slowdown.Store(math.Float64bits(slow))
	f.extraLatencyMs.Store(cfg.ExtraLatencyMs)
	f.unhealthy.Store(cfg.Unhealthy)
}

// Get returns the current knobs.
func (f *Faults) Get() FaultConfig {
	return FaultConfig{
		ErrorRate:      math.Float64frombits(f.errorRate.Load()),
		ExtraLatencyMs: f.extraLatencyMs.Load(),
		Slowdown:       math.Float64frombits(f.slowdown.Load()),
		Unhealthy:      f.unhealthy.Load(),
	}
}
