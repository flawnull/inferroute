package worker

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// AdminMux exposes /healthz, /metrics and /admin/faults.
//
//	GET  /admin/faults            current fault config
//	POST /admin/faults {json}     replace fault config (omitted fields reset)
//	DELETE /admin/faults          clear all faults
func (s *Server) AdminMux(gatherer prometheus.Gatherer) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", s.HealthHandler())
	mux.Handle("GET /metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /admin/faults", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, s.faults.Get())
	})
	mux.HandleFunc("POST /admin/faults", func(w http.ResponseWriter, r *http.Request) {
		var cfg FaultConfig
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&cfg); err != nil {
			http.Error(w, "invalid fault config: "+err.Error(), http.StatusBadRequest)
			return
		}
		s.faults.Set(cfg)
		s.log.Warn("fault config updated", "faults", s.faults.Get())
		writeJSON(w, http.StatusOK, s.faults.Get())
	})
	mux.HandleFunc("DELETE /admin/faults", func(w http.ResponseWriter, _ *http.Request) {
		s.faults.Set(FaultConfig{})
		s.log.Info("fault config cleared")
		writeJSON(w, http.StatusOK, s.faults.Get())
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
