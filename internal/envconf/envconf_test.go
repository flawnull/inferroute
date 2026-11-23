package envconf

import (
	"net/http"
	"net/http/httptest"
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

func TestHealthProbe(t *testing.T) {
	serve := func(code int) string {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }))
		t.Cleanup(ts.Close)
		return ts.URL
	}
	t.Run("passes when the endpoint answers 2xx", func(t *testing.T) {
		if err := probe(serve(http.StatusOK)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("fails when the endpoint answers 503", func(t *testing.T) {
		if err := probe(serve(http.StatusServiceUnavailable)); err == nil {
			t.Fatal("probe passed on 503")
		}
	})
	t.Run("fails when nothing is listening", func(t *testing.T) {
		if err := probe("http://127.0.0.1:1/healthz"); err == nil {
			t.Fatal("probe passed with no server")
		}
	})
	t.Run("fails on a URL that cannot be requested", func(t *testing.T) {
		if err := probe("://bad"); err == nil {
			t.Fatal("probe passed on a malformed URL")
		}
	})
}
