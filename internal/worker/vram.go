package worker

import "sync/atomic"

// VRAMPool tracks in-flight VRAM reservations with a lock-free CAS loop.
// Reserve never lets used exceed total, even under concurrent callers.
type VRAMPool struct {
	total int64
	used  atomic.Int64
}

func NewVRAMPool(totalMB int64) *VRAMPool {
	return &VRAMPool{total: totalMB}
}

// Reserve atomically claims mb megabytes. It returns false and claims nothing
// if the pool cannot fit the request.
func (p *VRAMPool) Reserve(mb int64) bool {
	if mb < 0 {
		return false
	}
	for {
		cur := p.used.Load()
		next := cur + mb
		if next > p.total {
			return false
		}
		if p.used.CompareAndSwap(cur, next) {
			return true
		}
	}
}

// Release returns mb megabytes to the pool. Callers must only release what
// they reserved.
func (p *VRAMPool) Release(mb int64) {
	if p.used.Add(-mb) < 0 {
		panic("worker: VRAM released more than reserved")
	}
}

func (p *VRAMPool) Used() int64      { return p.used.Load() }
func (p *VRAMPool) Total() int64     { return p.total }
func (p *VRAMPool) Available() int64 { return p.total - p.used.Load() }
