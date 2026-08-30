package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	mathrand "math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jasonjesuraja06/distributed-events/internal/queue"
	"github.com/segmentio/kafka-go"
)

// loadgen produces events at a configured RPS for a configured duration, with
// configurable duplicate-injection rate. Used for every benchmark in bench/.
//
// Designed for sustained soak tests:
//
//	--rate 12 --duration 24h   = 1,036,800 events (~1M events/day target)
//	--rate 100 --duration 1h   = 360K events (faster verification)
//	--rate 500 --burst-for 30s = chaos-spike test
func main() {
	brokers := flag.String("brokers", envOr("KAFKA_BROKERS", "localhost:9092"), "comma-separated Kafka brokers")
	topic := flag.String("topic", envOr("KAFKA_TOPIC", "events"), "topic")
	rate := flag.Int("rate", 12, "events per second sustained (12/s = ~1M/day)")
	duration := flag.Duration("duration", time.Minute, "total run duration")
	dupRate := flag.Float64("dup-rate", 0.10, "fraction of events that share idempotency_key with previous (duplicates)")
	burstRate := flag.Int("burst-rate", 0, "if >0, drive this RPS during --burst-for window")
	burstFor := flag.Duration("burst-for", 0, "duration of burst window mid-run")
	burstAfter := flag.Duration("burst-after", 0, "delay before starting burst window")
	report := flag.String("report", "", "if set, write JSON summary to this path")
	flag.Parse()

	w := &kafka.Writer{
		Addr:                   kafka.TCP(strings.Split(*brokers, ",")...),
		Topic:                  *topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
		BatchSize:              500,
		BatchTimeout:           10 * time.Millisecond,
		Async:                  true,
	}
	defer w.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	ctx, cancelTimeout := context.WithTimeout(ctx, *duration)
	defer cancelTimeout()

	var produced atomic.Uint64
	var duplicates atomic.Uint64

	currentRate := atomic.Int64{}
	currentRate.Store(int64(*rate))

	if *burstRate > 0 && *burstFor > 0 {
		go func() {
			if *burstAfter > 0 {
				select {
				case <-time.After(*burstAfter):
				case <-ctx.Done():
					return
				}
			}
			currentRate.Store(int64(*burstRate))
			log.Printf("BURST: rate -> %d/s for %s", *burstRate, *burstFor)
			select {
			case <-time.After(*burstFor):
			case <-ctx.Done():
				return
			}
			currentRate.Store(int64(*rate))
			log.Printf("BURST OVER: rate -> %d/s", *rate)
		}()
	}

	start := time.Now()
	var lastKey string
	tick := time.NewTicker(time.Second / time.Duration(currentRate.Load()))
	defer tick.Stop()

	resyncTick := time.NewTicker(250 * time.Millisecond)
	defer resyncTick.Stop()

	go func() {
		for range resyncTick.C {
			tick.Reset(time.Second / time.Duration(currentRate.Load()))
		}
	}()

	statTick := time.NewTicker(5 * time.Second)
	defer statTick.Stop()

	for {
		select {
		case <-ctx.Done():
			elapsed := time.Since(start).Seconds()
			p := produced.Load()
			d := duplicates.Load()
			log.Printf("DONE produced=%d duplicates=%d duration=%.1fs avg_rps=%.1f", p, d, elapsed, float64(p)/elapsed)
			if *report != "" {
				j, _ := json.MarshalIndent(map[string]any{
					"produced":             p,
					"injected_duplicates":  d,
					"duration_seconds":     elapsed,
					"avg_rps":              float64(p) / elapsed,
					"per_day_extrapolated": float64(p) / elapsed * 86400,
				}, "", "  ")
				_ = os.WriteFile(*report, j, 0o644)
			}
			return
		case <-statTick.C:
			log.Printf("loadgen produced=%d duplicates=%d rate=%d", produced.Load(), duplicates.Load(), currentRate.Load())
		case <-tick.C:
			key := randomHex(16)
			if lastKey != "" && mathrand.Float64() < *dupRate {
				key = lastKey
				duplicates.Add(1)
			}
			lastKey = key
			payload, _ := json.Marshal(map[string]any{"user": "u_" + randomHex(4), "amount": mathrand.IntN(10000)})
			e := queue.Event{
				EventID:        randomHex(16),
				IdempotencyKey: key,
				Type:           "notification.send",
				Payload:        payload,
				Timestamp:      time.Now().UTC(),
				AttemptNumber:  1,
			}
			b, _ := e.Marshal()
			if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: b}); err != nil {
				log.Printf("write: %v", err)
				continue
			}
			produced.Add(1)
		}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
