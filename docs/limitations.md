# Scope and Roadmap

## Current scope

The project is a reference implementation that prioritizes clarity of the core patterns (idempotency, rate limiting, circuit breaking, cost modeling) over production hardening. It runs on a single host via Docker Compose, against a single-broker Kafka and a single-node Redis.

All benchmarks measure the system on the host you run them on. Numbers from the reference dry-run (Apple Silicon M3, single-host Docker) are listed in the main README under **Benchmarks**; numbers from your own hardware will land in `bench/reports/*.json` after `make bench-all`.

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
- **Quotas per upstream.** Rate limit is global, not per-tenant.

## Known sharp edges

- **Dedup-slot release on failure.** When the downstream call fails or the breaker opens, the worker deletes the dedup key so a retry can re-claim it. This is correct for at-most-once-within-TTL semantics, but it means a duplicate produced *after* the slot is released but *before* the retry succeeds can still slip through. For stronger guarantees, replace dedup-on-claim with a two-phase commit pattern against a persistent ledger.
- **Cost model assumes on-demand EC2 pricing.** Spot or reserved-instance economics would change the numbers significantly. The pinned date in `scripts/cost_model.py` is the source of truth for the prices used.
- **AVX-512 dependency.** ONNX INT8 quantization (separate project) is not relevant here; for *this* project, the cost model assumes amd64/arm64 EC2 instances and is hardware-agnostic.

## Contributing

Pull requests welcome. Open an issue for changes that would alter the benchmark numbers in the README, since those are reproducible across CI.
