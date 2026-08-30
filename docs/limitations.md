# Scope and Limitations

## Current scope

The project is a reference implementation that prioritizes clarity of the core patterns (idempotency, rate limiting, circuit breaking, cost modeling) over production hardening. It runs on a single host via Docker Compose, against a single-broker Kafka and a single-node Redis.

All benchmarks measure the system on the host you run them on. The numbers in the
main README were measured on an Apple M4 Pro (14 cores, 48 GB RAM, macOS arm64,
single-host Docker Desktop) and the raw reports behind them are committed under
`bench/reports/`. Numbers from your own hardware will land in the same directory.

## Not yet in scope

### High availability
- **Kafka cluster.** Single-broker KRaft is suitable for the benchmark; a production deployment needs at least three brokers with `replication.factor=3` and proper ISR settings.
- **Redis high availability.** Redis Sentinel or Cluster is required for production; the current setup uses a single Redis node, which is a single point of failure.
- **Multi-region failover.** Out of scope.

### Schema and protocol
- **Schema registry.** Events are serialized as JSON. A production deployment should use Avro or Protobuf with a schema registry (Confluent or Karapace).
- **Headers and tracing.** OpenTelemetry traces are not yet propagated through the pipeline; only metrics are recorded.

### Security
- **TLS and SASL between services.** All in-cluster traffic is plaintext.
- **Downstream authentication.** The mock downstream accepts any caller; a production deployment would carry an auth token.
- **Multi-tenant isolation.** No tenant boundary; one consumer group sees all events.

### Reliability extras
- **Persistent dedup ledger.** The Redis dedup window is 1h; longer windows or persistent guarantees would need a backing store like DynamoDB or Postgres.
- **Quotas per upstream.** The rate limit is per consumer replica, not global and not per-tenant. Three replicas at `RATE_LIMIT_RPS=300` can offer 900 RPS to the downstream between them.
- **No dead-letter topic or replay.** Offsets are committed when an event enters the worker pool, so a failed downstream call drops the event. See `docs/runbook.md` section 2.

## Known sharp edges

- **Dedup-slot release on failure.** When the downstream call fails or the breaker opens, the worker deletes the dedup key so a retry can re-claim it. This is correct for at-most-once-within-TTL semantics, but it means a duplicate produced *after* the slot is released but *before* the retry succeeds can still slip through. For stronger guarantees, replace dedup-on-claim with a two-phase commit pattern against a persistent ledger.
- **Cost model assumes on-demand EC2 pricing.** Spot or reserved-instance economics would change the numbers significantly. The prices pinned in `scripts/cost_model.py` are list prices published in May 2026, used as inputs to arithmetic; nothing here has run on EC2 and nothing has been billed.
- **The cost model prices a container, not a deployment.** It converts measured container CPU and memory into an instance-hour equivalent. It does not model Kafka, Redis, network egress, or the operational overhead a real deployment would carry.
- **Delivery latency depends on two clocks.** `delivery_latency_seconds` is measured from the producer's timestamp to the consumer's successful downstream ack. The load generator runs on the host and the consumers run in containers, so the histogram carries whatever skew exists between them. Negative observations are clamped to zero.
- **Latency histogram tops out at 10s.** When a consumer falls far enough behind, its P95 lands in the `+Inf` bucket and `histogram_quantile` reports the top finite bucket boundary. Such a reading is a lower bound, not a measurement.

## Not covered by CI

CI (`.github/workflows/ci.yml`) runs formatting, vet, build, the unit tests
under the race detector, and a compose-config parse. It does not run the
benchmarks: those need Kafka, Redis, and Prometheus running, and they take
minutes to produce a single number. Benchmark results in the README were
produced by running the scripts locally on the hardware named there.
