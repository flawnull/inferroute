package ewma

import (
	"math"
	"sync"
	"testing"
)

func TestEWMA(t *testing.T) {
	e := New(0.5)
	e.Observe(10) // first sample seeds the average
	e.Observe(20)
	if got := e.Value(); got != 15 {
		t.Fatalf("value = %v, want 15", got)
	}
	e.Set(100)
	if e.Value() != 100 {
		t.Fatal("Set did not overwrite")
	}
}

func TestEWMAConcurrent(t *testing.T) {
	e := New(0.1)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				e.Observe(42)
			}
		}()
	}
	wg.Wait()
	if math.Abs(e.Value()-42) > 1e-9 {
		t.Fatalf("value = %v", e.Value())
	}
}
