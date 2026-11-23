// Package envconf reads typed configuration from environment variables and
// provides the logger and container health-probe helpers shared by binaries.
package envconf

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func Int(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("env %s: %v", key, err))
	}
	return n
}

func Float(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		panic(fmt.Sprintf("env %s: %v", key, err))
	}
	return f
}

func Bool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		panic(fmt.Sprintf("env %s: %v", key, err))
	}
	return b
}

// Duration accepts Go duration strings ("250ms") or a bare number, which is
// interpreted in unit.
func Duration(key string, def, unit time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return time.Duration(f * float64(unit))
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		panic(fmt.Sprintf("env %s: %v", key, err))
	}
	return d
}

// Logger returns a JSON slog logger honouring LOG_LEVEL (debug|info|warn|error).
func Logger(component string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(strings.ToUpper(String("LOG_LEVEL", "info")))); err != nil {
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(h).With("component", component)
}

// Probe performs an HTTP GET and exits 0 on 2xx. Distroless images have no
// curl, so binaries use this for Docker HEALTHCHECK.
func Probe(url string) {
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		fmt.Fprintln(os.Stderr, "unhealthy:", resp.Status)
		os.Exit(1)
	}
	os.Exit(0)
}
