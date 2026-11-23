package worker

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type metrics struct {
	requests *prometheus.CounterVec
	compute  prometheus.Histogram
}

// newMetrics registers worker metrics on reg. Gauges are read from live state
// at scrape time, so there is no bookkeeping on the hot path.
func newMetrics(reg prometheus.Registerer, s *Server) *metrics {
	labels := prometheus.Labels{"worker_id": s.cfg.ID}
	m := &metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "inferworker_requests_total",
			Help:        "Infer RPCs handled by this worker, by gRPC code.",
			ConstLabels: labels,
		}, []string{"code"}),
		compute: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "inferworker_compute_seconds",
			Help:        "Simulated compute time per successful Infer.",
			ConstLabels: labels,
			Buckets:     prometheus.ExponentialBuckets(0.005, 2, 12),
		}),
	}
	reg.MustRegister(
		m.requests,
		m.compute,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "inferworker_vram_used_mb", Help: "VRAM reserved by in-flight requests.", ConstLabels: labels,
		}, func() float64 { return float64(s.vram.Used()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "inferworker_vram_total_mb", Help: "Total simulated VRAM.", ConstLabels: labels,
		}, func() float64 { return float64(s.vram.Total()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "inferworker_active_requests", Help: "In-flight Infer RPCs.", ConstLabels: labels,
		}, func() float64 { return float64(s.active.Load()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "inferworker_ewma_latency_ms", Help: "EWMA of simulated compute latency.", ConstLabels: labels,
		}, func() float64 { return s.ewma.Value() }),
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}
