package router

import (
	"context"
	"errors"
	"slices"
	"time"
)

var (
	// ErrNoWorkers means no healthy worker with a ready breaker exists.
	ErrNoWorkers = errors.New("no healthy workers")
	// ErrNoCapacity means no worker can ever fit the request's VRAM.
	ErrNoCapacity = errors.New("no worker has enough VRAM for this request")
	// ErrQueueTimeout means capacity exists but none freed up in time.
	ErrQueueTimeout = errors.New("timed out waiting for VRAM")
)

// Lease is a reservation of VRAM and a breaker slot on one worker. Exactly
// one call to Done must follow every successful Acquire.
type Lease struct {
	Worker *Worker
	vram   int64
	ticket Ticket
	sched  *Scheduler
}

// Done releases the reservation and reports the outcome to the breaker.
func (l *Lease) Done(o Outcome) {
	l.Worker.releaseVRAM(l.vram)
	l.Worker.inflight.Add(-1)
	l.Worker.breaker.Record(l.ticket, o)
	l.sched.reg.cap.broadcast()
}

// Scheduler places requests on workers.
type Scheduler struct {
	reg *Registry
	// QueueTimeout bounds how long a request waits for VRAM to free up when
	// every eligible worker is momentarily full.
	QueueTimeout time.Duration
}

func NewScheduler(reg *Registry, queueTimeout time.Duration) *Scheduler {
	return &Scheduler{reg: reg, QueueTimeout: queueTimeout}
}

// Score estimates relative completion latency on w if it takes a request
// needing vram MB. Lower is better.
//
//	ewma_latency * (1 + queue_depth) * (1 + vram_utilisation_after_placement)
//
// The first two terms approximate "time until this request finishes" for a
// worker that serves its queue in parallel with contention; the VRAM term
// spreads memory pressure so large requests keep finding room.
func Score(w *Worker, vram int64) float64 {
	lat := max(w.ewma.Value(), 1)
	total := w.totalVRAM.Load()
	util := 1.0
	if total > 0 {
		util = float64(w.UsedVRAM()+vram) / float64(total)
	}
	return lat * float64(1+w.load()) * (1 + util)
}

// Acquire picks the best eligible worker not in exclude and reserves vram on
// it. If every candidate is full it waits for capacity up to QueueTimeout or
// the ctx deadline, whichever is first.
func (s *Scheduler) Acquire(ctx context.Context, vram int64, exclude map[*Worker]bool) (*Lease, error) {
	var timeout <-chan time.Time
	if s.QueueTimeout > 0 {
		t := time.NewTimer(s.QueueTimeout)
		defer t.Stop()
		timeout = t.C
	}
	for {
		wake := s.reg.cap.wait()
		lease, err := s.tryAcquire(vram, exclude)
		if lease != nil || errors.Is(err, ErrNoCapacity) {
			return lease, err
		}
		// Full, or no worker eligible right now (e.g. mid-failover, or every
		// half-open probe slot taken): wait for heartbeats or releases.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			return nil, err
		case <-wake:
		}
	}
}

type candidate struct {
	w     *Worker
	score float64
}

// tryAcquire makes one non-blocking placement pass. ErrQueueTimeout here
// means "eligible workers exist but are full right now".
func (s *Scheduler) tryAcquire(vram int64, exclude map[*Worker]bool) (*Lease, error) {
	workers := s.reg.Workers()
	cands := make([]candidate, 0, len(workers))
	eligible, fits := false, false
	for _, w := range workers {
		if exclude[w] || !w.Healthy() || !w.breaker.Ready() {
			continue
		}
		eligible = true
		if w.Capacity() < vram {
			continue
		}
		fits = true
		if w.Free() >= vram {
			cands = append(cands, candidate{w, Score(w, vram)})
		}
	}
	slices.SortFunc(cands, func(a, b candidate) int {
		switch {
		case a.score < b.score:
			return -1
		case a.score > b.score:
			return 1
		}
		return 0
	})
	for _, c := range cands {
		if !c.w.TryReserve(vram) {
			continue // lost a race to a concurrent request; try the next one
		}
		t, ok := c.w.breaker.Acquire()
		if !ok {
			c.w.releaseVRAM(vram) // half-open probe slot taken concurrently
			continue
		}
		c.w.inflight.Add(1)
		return &Lease{Worker: c.w, vram: vram, ticket: t, sched: s}, nil
	}
	switch {
	case !eligible:
		return nil, ErrNoWorkers
	case !fits:
		return nil, ErrNoCapacity
	}
	return nil, ErrQueueTimeout
}
