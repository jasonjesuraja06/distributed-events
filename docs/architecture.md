# Architecture

```
                           +-----------+
            producer ----> |   Kafka   | ----> consumers (worker pool, 3 replicas)
            (loadgen)      |  (KRaft)  |        |   |   |
                           +-----------+        v   v   v
                                            +------------+
                                            | dedup      |  -- Redis SETNX
                                            +------------+
                                            | rate limit |  -- token bucket
                                            +------------+
                                            | breaker    |  -- gobreaker
                                            +------------+
                                                  |
                                                  v
                                            downstream HTTP

   Prometheus scrapes /metrics on each consumer (port 9100/9101).
   Grafana dashboards on :3000.
```

## Components

### Producer (`cmd/producer/main.go`)

Writes events to Kafka under `Hash` partitioning on the `IdempotencyKey`. This keeps duplicate-keyed events on the same partition so dedup works under at-least-once delivery. `Async` writes are batched at 100 messages or 20ms, whichever first.

### Kafka (KRaft single broker)

`apache/kafka:3.9.0` in KRaft mode (no Zookeeper). Topic `events` has 6 partitions; `events-baseline` is a parallel topic feeding the baseline consumer. Consumer groups commit offsets every 100ms.

### Optimized consumer (`cmd/consumer/main.go`)

Pulls from Kafka and fans messages out to a worker pool (16 goroutines per replica × 3 replicas = 48 concurrent workers). Each worker pipelines four steps:

1. **Dedup claim** — `Redis SETNX dedup:<xxhash(IdempotencyKey)> 1 EX <TTL>`. If the key already exists, the worker drops the message (it was already processed by another consumer). The dedup TTL defaults to 1h.
2. **Rate limit** — `golang.org/x/time/rate` token bucket caps outbound RPS to the configured budget.
3. **Circuit breaker** — `sony/gobreaker` wraps the downstream call. The breaker opens when the rolling failure rate crosses the configured threshold (default 50% over the last `BREAKER_WINDOW_SEC` seconds with at least `RATE_LIMIT_RPS / 5` requests), stays open for the configured duration, then half-opens on a single trial.
4. **Downstream call** — HTTP POST through a shared `http.Client` with keep-alive, 200-conn idle pool, 3s timeout. Failed calls release the dedup slot so a future retry can re-attempt.

### Baseline consumer (`cmd/baseline-consumer/main.go`)

Intentionally simple: synchronous processing, fresh `http.Client` per request (no keep-alive), no dedup, no rate limit, no breaker. Exists so benchmarks can measure deltas under identical workload rather than against guesses.

### Mock downstream (`cmd/downstream/main.go`)

Configurable artificial latency, configurable steady-state failure rate, runtime chaos toggle (`POST /chaos/on` jumps to 60% failure). Tracks unique-IdempotencyKey deliveries so dedup tests can verify suppression directly without relying on consumer metrics.

### Metrics (`internal/metrics/metrics.go`)

Prometheus instrumentation exposed on `:9100` (optimized) and `:9101` (baseline). Counters: events received, deduped, delivered, downstream errors (by kind), breaker open. Histograms: end-to-end delivery latency, downstream call latency. Uptime probe counter for healthcheck outcomes.

## Trade-offs

- **At-most-once within a 1h window.** Cross-window duplicates can slip through. Acceptable for notifications where retry windows are short; not appropriate for financial settlement, which would need a persistent dedup ledger.
- **Redis is a single point of failure.** A production deployment should use Redis Sentinel or Cluster. The current setup demonstrates the algorithm, not the HA topology.
- **Kafka single-broker.** KRaft single-node is sufficient for the benchmark; a production deployment would run at least three brokers with `replication.factor=3`.
- **Dedup-slot release on failure.** When the breaker opens or the downstream call fails, the worker `DEL`s the dedup key so a retry can re-attempt. This trades extra downstream traffic during outages for stronger eventual-delivery guarantees. An alternative would be to keep the slot and rely on a separate retry queue.
- **JSON serialization.** Schema evolution costs more than with Protobuf or Avro. Chosen for readability in this reference; production deployments should use a schema registry.
