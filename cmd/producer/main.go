package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	mathrand "math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jasonjesuraja06/distributed-events/internal/queue"
	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := envOr("KAFKA_TOPIC", "events")
	ratePerSec := mustAtoi(envOr("PRODUCER_RATE_PER_SEC", "12"))
	dupRate, _ := strconv.ParseFloat(envOr("PRODUCER_DUPLICATE_RATE", "0.05"), 64)

	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
		BatchSize:              100,
		BatchTimeout:           20 * time.Millisecond,
	}
	defer w.Close()

	log.Printf("producer brokers=%v topic=%s rate=%d/s dupRate=%.2f", brokers, topic, ratePerSec, dupRate)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	tick := time.NewTicker(time.Second / time.Duration(ratePerSec))
	defer tick.Stop()
	var lastKey string
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			key := randomHex(16)
			// Inject duplicates by reusing the previous idempotency key.
			if lastKey != "" && mathrand.Float64() < dupRate {
				key = lastKey
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
				log.Printf("write error: %v", err)
			}
		}
	}
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
func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = cryptorand.Read(b)
	return hex.EncodeToString(b)
}
