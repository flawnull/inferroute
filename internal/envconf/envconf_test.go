package envconf

import (
	"testing"
	"time"
)

func TestDuration(t *testing.T) {
	t.Setenv("D_BARE", "250")
	t.Setenv("D_GO", "1.5s")
	if got := Duration("D_BARE", 0, time.Millisecond); got != 250*time.Millisecond {
		t.Fatalf("bare number = %v", got)
	}
	if got := Duration("D_GO", 0, time.Millisecond); got != 1500*time.Millisecond {
		t.Fatalf("go duration = %v", got)
	}
	if got := Duration("D_UNSET", time.Second, time.Millisecond); got != time.Second {
		t.Fatalf("default = %v", got)
	}
}

func TestTypedGetters(t *testing.T) {
	t.Setenv("I", "7")
	t.Setenv("F", "0.25")
	t.Setenv("B", "true")
	if Int("I", 0) != 7 || Float("F", 0) != 0.25 || !Bool("B", false) || String("S_UNSET", "d") != "d" {
		t.Fatal("typed getters returned wrong values")
	}
}
