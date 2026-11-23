// Package envconf reads typed configuration from environment variables and
// provides the logger and container health-probe helpers shared by binaries.
package envconf

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// String returns the environment variable key, or def when it is unset or empty.
func String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// Int returns key parsed as an integer, or def when it is unset.
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

// Float returns key parsed as a float, or def when it is unset.
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

// Bool returns key parsed as a boolean, or def when it is unset.
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
	if err := probe(url); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func probe(url string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return errors.New("unhealthy: " + resp.Status)
	}
	return nil
}
