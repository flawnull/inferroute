package router

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// ParseModels parses "model=vram_mb,model2=vram_mb".
func ParseModels(s string) (map[string]int64, error) {
	out := map[string]int64{}
	for _, kv := range splitList(s) {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("model %q: want name=vram_mb", kv)
		}
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("model %q: invalid vram_mb", kv)
		}
		out[strings.TrimSpace(k)] = n
	}
	return out, nil
}

// ParseTenantLimits parses "tenant=rps:burst,tenant2=rps:burst".
func ParseTenantLimits(s string) (map[string]Limit, error) {
	out := map[string]Limit{}
	for _, kv := range splitList(s) {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("tenant limit %q: want tenant=rps:burst", kv)
		}
		rs, bs, ok := strings.Cut(v, ":")
		if !ok {
			return nil, fmt.Errorf("tenant limit %q: want tenant=rps:burst", kv)
		}
		rate, err1 := strconv.ParseFloat(strings.TrimSpace(rs), 64)
		burst, err2 := strconv.ParseFloat(strings.TrimSpace(bs), 64)
		if err1 != nil || err2 != nil || rate <= 0 || burst < 1 {
			return nil, fmt.Errorf("tenant limit %q: rps must be > 0 and burst >= 1", kv)
		}
		out[strings.TrimSpace(k)] = Limit{Rate: rate, Burst: burst}
	}
	return out, nil
}

// ParseAddrs parses a comma-separated host:port list.
func ParseAddrs(s string) ([]string, error) {
	addrs := splitList(s)
	for _, a := range addrs {
		if _, _, err := net.SplitHostPort(a); err != nil {
			return nil, fmt.Errorf("worker address %q: %w", a, err)
		}
	}
	return addrs, nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
