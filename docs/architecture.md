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

   Prometheus discovers consumer replicas by DNS and scrapes /metrics
   on 9100 (optimized) and 9101 (baseline).
```

## Components

### Producer (`cmd/producer/main.go`)

Writes events to Kafka under `Hash` partitioning on the `IdempotencyKey`. This keeps duplicate-keyed events on the same partition. Writes are batched at 100 messages or 20ms, whichever comes first; the producer writes synchronously, while `cmd/loadgen` uses the same writer in `Async` mode with a 500-message batch.

Dedup does not depend on the partitioning: the Redis claim is global across replicas. Hash partitioning keeps a duplicate pair on one partition, which means one consumer usually resolves both and the Redis round trip stays local to that worker.

### Kafka (KRaft single broker)

`apache/kafka:3.9.0` in KRaft mode (no Zookeeper). Two listeners: `PLAINTEXT` on 19092 advertised as `kafka:19092` for in-network services, and `EXTERNAL` on 9092 advertised as `localhost:9092` so host-side tools reach the broker. Topics `events` and `events-baseline` are created with 6 partitions each by the `kafka-init` service before any consumer starts.

Topic creation has to precede group formation. A kafka-go consumer group that joins a topic which does not yet exist is assigned zero partitions and remains `Stable` with zero partitions, because the partition list is only re-evaluated on rebalance. Both readers set `WatchPartitionChanges: true` so a partition count that appears or changes later forces a rejoin.

### Optimized consumer (`cmd/consumer/main.go`)

Pulls from Kafka and fans messages out to a worker pool (16 goroutines per replica × 3 replicas = 48 concurrent workers). Each worker pipelines four steps:

1. **Dedup claim**: `SETNX dedup:<xxhash64(IdempotencyKey) as 16 hex digits> 1 EX <TTL>`. If the key already exists, the worker drops the message: another worker already claimed it. The dedup TTL defaults to 1h.
2. **Rate limit**: `golang.org/x/time/rate` token bucket caps outbound RPS to the configured budget.
3. **Circuit breaker**: `sony/gobreaker` wraps the downstream call. The breaker opens when the rolling failure rate crosses the configured threshold (default 50% over the last `BREAKER_WINDOW_SEC` seconds with at least `RATE_LIMIT_RPS / 5` requests), stays open for the configured duration, then half-opens on a single trial.
4. **Downstream call**: HTTP POST through a shared `http.Client` with keep-alive, a 200-connection idle pool, and a 3s timeout. Failed calls release the dedup slot so a later publish of the same key can re-attempt.

Offsets are committed when the event is handed to the worker pool, before the downstream call. A failed delivery is therefore dropped rather than retried, which is what makes the semantics at-most-once.

### Baseline consumer (`cmd/baseline-consumer/main.go`)

Intentionally simple: synchronous processing, fresh `http.Client` per request (no keep-alive), no dedup, no rate limit, no breaker. Exists so benchmarks can measure deltas under identical workload rather than against guesses.

### Mock downstream (`cmd/downstream/main.go`)

Configurable artificial latency, configurable steady-state failure rate, runtime chaos toggle (`POST /chaos/on` jumps to 60% failure). Tracks unique-IdempotencyKey deliveries so dedup tests can verify suppression directly without relying on consumer metrics.

### Metrics (`internal/metrics/metrics.go`)

Prometheus instrumentation exposed on `:9100` (optimized) and `:9101` (baseline), alongside `/healthz` on the same port. Counters: events received, deduped, delivered, downstream errors (by kind), breaker rejections. Histograms: `delivery_latency_seconds`, measured from the producer's timestamp to a successful downstream ack, and `downstream_latency_seconds`, measured around the HTTP call alone.

Both consumers compute delivery latency the same way, so the two lanes are comparable. The producer's clock is the host's and the consumers' is the container runtime's; a negative reading from skew is clamped to zero rather than recorded. The histogram's largest finite bucket is 10s, so a consumer far enough behind reports a P95 pinned at that boundary, which is a lower bound rather than a measurement.

## Trade-offs

- **At-most-once within a 1h window.** Cross-window duplicates can slip through. Acceptable for notifications where retry windows are short; not appropriate for financial settlement, which would need a persistent dedup ledger.
- **Redis is a single point of failure.** A production deployment should use Redis Sentinel or Cluster. The current setup demonstrates the algorithm, not the HA topology.
- **Kafka single-broker.** KRaft single-node is sufficient for the benchmark; a production deployment would run at least three brokers with `replication.factor=3`.
- **Dedup-slot release on failure.** When the breaker opens or the downstream call fails, the worker `DEL`s the dedup key so a retry can re-attempt. This trades extra downstream traffic during outages for stronger eventual-delivery guarantees. An alternative would be to keep the slot and rely on a separate retry queue.
- **JSON serialization.** Schema evolution costs more than with Protobuf or Avro. Chosen for readability in this reference; production deployments should use a schema registry.
- **Rate limit is per replica.** Each consumer holds its own token bucket, so the ceiling offered to the downstream is `RATE_LIMIT_RPS` times the replica count. A shared budget would need a distributed limiter.
- **Prometheus scrapes replicas by DNS.** The consumer service publishes no host port because it runs multiple replicas, so a static target would resolve to one replica and silently under-report the fleet. DNS service discovery on the compose service name returns one A record per replica.
