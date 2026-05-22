package main

import (
	"encoding/json"
	"flag"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// Mock downstream service. Configurable artificial latency and failure rate so
// the benchmark suite can drive deterministic chaos scenarios.

var (
	port        = ":8081"
	failRate    = 0.02
	latencyMs   = 30
	chaosMode   atomic.Bool // when true, fail rate jumps to 0.6
	deliveries  atomic.Uint64
	idempotency atomic.Uint64 // count of unique IdempotencyKey seen
	seen        = map[string]struct{}{}
)

type req struct {
	EventID        string `json:"event_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

type resp struct {
	Status         string `json:"status"`
	EventID        string `json:"event_id"`
	Duplicate      bool   `json:"duplicate"`
	DeliveryNumber uint64 `json:"delivery_number"`
}

func main() {
	flag.Parse()
	if v := os.Getenv("DOWNSTREAM_PORT"); v != "" {
		port = ":" + v
	}
	if v := os.Getenv("DOWNSTREAM_FAIL_RATE"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			failRate = f
		}
	}
	if v := os.Getenv("DOWNSTREAM_LATENCY_MS"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			latencyMs = i
		}
	}

	http.HandleFunc("/process", processHandler)
	http.HandleFunc("/chaos/on", func(w http.ResponseWriter, r *http.Request) {
		chaosMode.Store(true)
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/chaos/off", func(w http.ResponseWriter, r *http.Request) {
		chaosMode.Store(false)
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]uint64{
			"deliveries":      deliveries.Load(),
			"unique_keys":     idempotency.Load(),
		})
	})
	http.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		deliveries.Store(0)
		idempotency.Store(0)
		seen = map[string]struct{}{}
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) })

	log.Printf("downstream listening on %s (failRate=%.3f latencyMs=%d)", port, failRate, latencyMs)
	log.Fatal(http.ListenAndServe(port, nil))
}

func processHandler(w http.ResponseWriter, r *http.Request) {
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad payload", http.StatusBadRequest)
		return
	}
	jitter := time.Duration(latencyMs+rand.IntN(latencyMs)) * time.Millisecond
	time.Sleep(jitter)

	fr := failRate
	if chaosMode.Load() {
		fr = 0.6
	}
	if rand.Float64() < fr {
		http.Error(w, "downstream 5xx", http.StatusInternalServerError)
		return
	}

	deliveries.Add(1)
	if _, ok := seen[body.IdempotencyKey]; !ok {
		seen[body.IdempotencyKey] = struct{}{}
		idempotency.Add(1)
	}
	_ = json.NewEncoder(w).Encode(resp{
		Status:         "ok",
		EventID:        body.EventID,
		Duplicate:      false,
		DeliveryNumber: deliveries.Load(),
	})
}
