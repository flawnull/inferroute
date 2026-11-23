package router

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics holds the router's Prometheus collectors.
type Metrics struct {
	requests  *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	failovers *prometheus.CounterVec
	attempts  *prometheus.CounterVec
}

// NewMetrics registers the router collectors on reg; r feeds the per-worker gauges.
func NewMetrics(reg prometheus.Registerer, r *Registry) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inferroute_requests_total",
			Help: "Inference requests by HTTP status, model and tenant.",
		}, []string{"status", "model", "tenant"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "inferroute_request_duration_seconds",
			Help:    "End-to-end request latency at the router, including retries.",
			Buckets: []float64{.005, .01, .025, .05, .075, .1, .15, .2, .3, .5, .75, 1, 1.5, 2.5, 5, 10},
		}, []string{"model", "status"}),
		failovers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inferroute_failovers_total",
			Help: "Requests retried on a different worker, by failed worker and reason.",
		}, []string{"from_worker", "reason"}),
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inferroute_worker_attempts_total",
			Help: "Infer attempts sent to each worker, by gRPC code.",
		}, []string{"worker", "code"}),
	}
	reg.MustRegister(m.requests, m.duration, m.failovers, m.attempts, &workerCollector{reg: r},
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// initWorker creates zero-valued failover series for a newly seen worker.
// Without them a counter series is born at 1 on the first failover, and
// Prometheus's increase()/rate() cannot see that first step.
func (m *Metrics) initWorker(id string) {
	for _, r := range failoverReasons {
		m.failovers.WithLabelValues(id, r)
	}
}

var (
	descVRAMUsed = prometheus.NewDesc("inferroute_worker_vram_used_mb",
		"Router's estimate of VRAM in use per worker (own reservations + other usage reported by heartbeat).", []string{"worker"}, nil)
	descVRAMTotal = prometheus.NewDesc("inferroute_worker_vram_total_mb",
		"Total VRAM per worker as reported by heartbeat.", []string{"worker"}, nil)
	descBreaker = prometheus.NewDesc("inferroute_circuit_breaker_state",
		"Circuit breaker state per worker: 0=closed, 1=half-open, 2=open.", []string{"worker"}, nil)
	descHealthy = prometheus.NewDesc("inferroute_worker_healthy",
		"1 if the worker's last heartbeat reported healthy.", []string{"worker"}, nil)
	descInflight = prometheus.NewDesc("inferroute_worker_inflight",
		"Requests this router has in flight on each worker.", []string{"worker"}, nil)
	descEWMA = prometheus.NewDesc("inferroute_worker_ewma_latency_ms",
		"EWMA latency per worker used for scheduling.", []string{"worker"}, nil)
)

// workerCollector reads per-worker gauges from the registry at scrape time.
type workerCollector struct{ reg *Registry }

func (c *workerCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descVRAMUsed, descVRAMTotal, descBreaker, descHealthy, descInflight, descEWMA} {
		ch <- d
	}
}

func (c *workerCollector) Collect(ch chan<- prometheus.Metric) {
	seen := map[string]bool{}
	for _, w := range c.reg.Workers() {
		id := w.ID()
		if seen[id] {
			id += "@" + w.Addr // never emit duplicate series, even transiently
		}
		seen[id] = true
		healthy := 0.0
		if w.Healthy() {
			healthy = 1
		}
		ch <- prometheus.MustNewConstMetric(descVRAMUsed, prometheus.GaugeValue, float64(w.UsedVRAM()), id)
		ch <- prometheus.MustNewConstMetric(descVRAMTotal, prometheus.GaugeValue, float64(w.totalVRAM.Load()), id)
		ch <- prometheus.MustNewConstMetric(descBreaker, prometheus.GaugeValue, float64(w.breaker.State()), id)
		ch <- prometheus.MustNewConstMetric(descHealthy, prometheus.GaugeValue, healthy, id)
		ch <- prometheus.MustNewConstMetric(descInflight, prometheus.GaugeValue, float64(w.inflight.Load()), id)
		ch <- prometheus.MustNewConstMetric(descEWMA, prometheus.GaugeValue, w.ewma.Value(), id)
	}
}
