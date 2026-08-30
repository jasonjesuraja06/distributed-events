package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jasonjesuraja06/distributed-events/internal/metrics"
	"github.com/jasonjesuraja06/distributed-events/internal/queue"
	"github.com/segmentio/kafka-go"
)

// Baseline consumer = intentionally naive:
//   - synchronous, one event at a time (no worker pool)
//   - no idempotency / dedup
//   - no rate limit, no circuit breaker
//   - fresh HTTP client per request (no keep-alive)
//
// Provides the comparison point against which the optimized consumer's
// duplicate-reduction, latency, and chaos-resilience metrics are measured.
const consumerLabel = "baseline"

func main() {
	brokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := envOr("KAFKA_TOPIC", "events-baseline")
	group := envOr("KAFKA_GROUP_ID", "baseline-consumers")
	downstreamURL := envOr("DOWNSTREAM_URL", "http://localhost:8081/process")
	metricsPort := envOr("METRICS_PORT", "9101")

	metrics.MustRegister()
	go func() { _ = metrics.Serve(":" + metricsPort) }()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        group,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10 * 1024 * 1024,
		CommitInterval: 100 * time.Millisecond,
		StartOffset:    kafka.FirstOffset,
		// Without this, a group that joins before its topic exists is assigned
		// zero partitions and stays Stable with zero partitions forever: the
		// group only re-evaluates the partition list on rebalance. Watching for
		// partition changes forces a rejoin when partitions appear.
		WatchPartitionChanges: true,
		ErrorLogger:           kafka.LoggerFunc(log.Printf),
	})
	defer reader.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Printf("BASELINE consumer brokers=%v topic=%s group=%s (no dedup, no breaker, no rate limit, no keepalive)", brokers, topic, group)

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("fetch error: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		ev, err := queue.Unmarshal(m.Value)
		if err != nil {
			log.Printf("unmarshal error: %v", err)
			_ = reader.CommitMessages(ctx, m)
			continue
		}
		metrics.EventsReceived.WithLabelValues(consumerLabel).Inc()
		processNaively(ctx, ev, downstreamURL)
		_ = reader.CommitMessages(ctx, m)
	}
}

func processNaively(ctx context.Context, ev *queue.Event, downstreamURL string) {
	// Fresh client per request: no connection reuse, DNS resolved every time.
	client := &http.Client{Timeout: 5 * time.Second}
	body, _ := json.Marshal(map[string]string{
		"event_id":        ev.EventID,
		"idempotency_key": ev.IdempotencyKey,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, downstreamURL, bytes.NewReader(body))
	if err != nil {
		metrics.DownstreamErrors.WithLabelValues(consumerLabel, "build").Inc()
		return
	}
	req.Header.Set("Content-Type", "application/json")
	dstart := time.Now()
	resp, err := client.Do(req)
	metrics.DownstreamLatency.WithLabelValues(consumerLabel).Observe(time.Since(dstart).Seconds())
	if err != nil {
		metrics.DownstreamErrors.WithLabelValues(consumerLabel, "network").Inc()
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		metrics.DownstreamErrors.WithLabelValues(consumerLabel, "5xx").Inc()
		return
	}
	if resp.StatusCode >= 400 {
		metrics.DownstreamErrors.WithLabelValues(consumerLabel, "4xx").Inc()
		return
	}
	metrics.EventsDelivered.WithLabelValues(consumerLabel).Inc()
	metrics.DeliveryLatency.WithLabelValues(consumerLabel).Observe(sinceEvent(ev.Timestamp))
}

// sinceEvent matches the optimized consumer's definition so the two lanes are
// comparable: end-to-end age of the event at delivery, clamped at zero.
func sinceEvent(stamped time.Time) float64 {
	d := time.Since(stamped).Seconds()
	if d < 0 {
		return 0
	}
	return d
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
