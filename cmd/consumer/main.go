package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jasonjesuraja06/distributed-events/internal/breaker"
	"github.com/jasonjesuraja06/distributed-events/internal/idempotency"
	"github.com/jasonjesuraja06/distributed-events/internal/metrics"
	"github.com/jasonjesuraja06/distributed-events/internal/queue"
	"github.com/jasonjesuraja06/distributed-events/internal/ratelimit"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

const consumerLabel = "optimized"

func main() {
	brokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := envOr("KAFKA_TOPIC", "events")
	group := envOr("KAFKA_GROUP_ID", "optimized-consumers")
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	downstreamURL := envOr("DOWNSTREAM_URL", "http://localhost:8081/process")
	rps := mustAtoi(envOr("RATE_LIMIT_RPS", "300"))
	brkThresh, _ := strconv.ParseFloat(envOr("BREAKER_THRESHOLD", "0.5"), 64)
	brkWindowSec := mustAtoi(envOr("BREAKER_WINDOW_SEC", "10"))
	brkOpenSec := mustAtoi(envOr("BREAKER_OPEN_SEC", "10"))
	metricsPort := envOr("METRICS_PORT", "9100")
	workerCount := mustAtoi(envOr("WORKER_COUNT", "16"))
	dedupTTLSec := mustAtoi(envOr("DEDUP_TTL_SEC", "3600"))

	metrics.MustRegister()
	go func() { _ = metrics.Serve(":" + metricsPort) }()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	dedup := idempotency.New(rdb, time.Duration(dedupTTLSec)*time.Second)
	lim := ratelimit.New(rps, rps*2)
	cb := breaker.New(breaker.Config{
		Name:            "downstream",
		FailureRateOpen: brkThresh,
		WindowRequests:  uint32(rps / 5),
		WindowInterval:  time.Duration(brkWindowSec) * time.Second,
		OpenDuration:    time.Duration(brkOpenSec) * time.Second,
	})

	// HTTP client with keep-alive + bounded connections (a key delta vs naive baseline).
	transport := &http.Transport{
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ExpectContinueTimeout: 1 * time.Second,
	}
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}

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

	// Worker pool. Fan-out for parallel downstream calls.
	jobs := make(chan *queue.Event, workerCount*4)
	for i := 0; i < workerCount; i++ {
		go worker(ctx, jobs, dedup, lim, cb, client, downstreamURL)
	}

	log.Printf("consumer brokers=%v topic=%s group=%s workers=%d rps=%d brkThresh=%.2f brkWindow=%ds brkOpen=%ds",
		brokers, topic, group, workerCount, rps, brkThresh, brkWindowSec, brkOpenSec)

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
		select {
		case jobs <- ev:
		case <-ctx.Done():
			return
		}
		_ = reader.CommitMessages(ctx, m)
	}
}

func worker(
	ctx context.Context,
	jobs <-chan *queue.Event,
	dedup *idempotency.Deduper,
	lim *ratelimit.Limiter,
	cb *breaker.Breaker,
	client *http.Client,
	downstreamURL string,
) {
	for ev := range jobs {
		processEvent(ctx, ev, dedup, lim, cb, client, downstreamURL)
	}
}

func processEvent(
	ctx context.Context,
	ev *queue.Event,
	dedup *idempotency.Deduper,
	lim *ratelimit.Limiter,
	cb *breaker.Breaker,
	client *http.Client,
	downstreamURL string,
) {
	claimed, err := dedup.TryClaim(ctx, ev.IdempotencyKey)
	if err != nil {
		log.Printf("dedup error: %v (event=%s)", err, ev.EventID)
		metrics.DownstreamErrors.WithLabelValues(consumerLabel, "dedup").Inc()
		return
	}
	if !claimed {
		metrics.EventsDeduped.WithLabelValues(consumerLabel).Inc()
		return
	}

	if err := lim.Wait(ctx); err != nil {
		metrics.DownstreamErrors.WithLabelValues(consumerLabel, "ratelimit").Inc()
		return
	}

	body, _ := json.Marshal(map[string]string{
		"event_id":        ev.EventID,
		"idempotency_key": ev.IdempotencyKey,
	})

	doCall := func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, downstreamURL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		dstart := time.Now()
		resp, err := client.Do(req)
		metrics.DownstreamLatency.WithLabelValues(consumerLabel).Observe(time.Since(dstart).Seconds())
		if err != nil {
			metrics.DownstreamErrors.WithLabelValues(consumerLabel, "network").Inc()
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 500 {
			metrics.DownstreamErrors.WithLabelValues(consumerLabel, "5xx").Inc()
			return errors.New("downstream 5xx")
		}
		if resp.StatusCode >= 400 {
			metrics.DownstreamErrors.WithLabelValues(consumerLabel, "4xx").Inc()
			return errors.New("downstream 4xx")
		}
		return nil
	}

	if err := cb.Do(doCall); err != nil {
		if errors.Is(err, breaker.ErrOpen) {
			metrics.BreakerOpenCount.WithLabelValues(consumerLabel).Inc()
		}
		// Release the dedup slot so a retry can re-attempt this key (negative caching).
		_ = dedup.Release(ctx, ev.IdempotencyKey)
		return
	}

	metrics.EventsDelivered.WithLabelValues(consumerLabel).Inc()
	metrics.DeliveryLatency.WithLabelValues(consumerLabel).Observe(sinceEvent(ev.Timestamp))
}

// sinceEvent is the end-to-end age of an event at delivery time: the producer
// stamps Timestamp before the Kafka write, so this covers queue wait plus all
// in-consumer work. Producer and consumer run on different clocks, so a
// negative reading (skew) is clamped to zero rather than recorded.
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
func mustAtoi(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("bad int %q: %v", s, err)
	}
	return i
}
