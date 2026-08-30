package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	EventsReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "events_received_total",
			Help: "Events polled from Kafka.",
		},
		[]string{"consumer"},
	)

	EventsDeduped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "events_deduped_total",
			Help: "Events suppressed by idempotency layer (would have caused duplicate downstream calls).",
		},
		[]string{"consumer"},
	)

	EventsDelivered = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "events_delivered_total",
			Help: "Successful downstream deliveries.",
		},
		[]string{"consumer"},
	)

	DownstreamErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "downstream_errors_total",
			Help: "Downstream 5xx / network errors.",
		},
		[]string{"consumer", "kind"},
	)

	BreakerOpenCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "breaker_open_total",
			Help: "Times the circuit breaker rejected a call (open state).",
		},
		[]string{"consumer"},
	)

	DeliveryLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "delivery_latency_seconds",
			Help:    "End-to-end latency: producer event timestamp -> successful downstream ack.",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.2, 0.4, 0.8, 1.2, 2.0, 3.0, 5.0, 10.0},
		},
		[]string{"consumer"},
	)

	DownstreamLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "downstream_latency_seconds",
			Help:    "Latency of the downstream HTTP call.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.4, 0.8, 1.5, 3.0},
		},
		[]string{"consumer"},
	)

	UptimeProbe = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "uptime_probe_total",
			Help: "Liveness/health-probe results, partitioned by outcome.",
		},
		[]string{"consumer", "outcome"},
	)
)

func MustRegister() {
	prometheus.MustRegister(
		EventsReceived,
		EventsDeduped,
		EventsDelivered,
		DownstreamErrors,
		BreakerOpenCount,
		DeliveryLatency,
		DownstreamLatency,
		UptimeProbe,
	)
}

// Serve starts a /metrics endpoint on addr. Blocks until error.
func Serve(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: addr, Handler: mux}
	return srv.ListenAndServe()
}
