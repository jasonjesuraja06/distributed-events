# distributed-events

A queue-backed event-processing platform in Go. Built on Kafka and Redis, designed for at-most-once delivery semantics, graceful degradation under downstream failure, and predictable per-event cost.

## Overview

Event-driven systems that fan out to fragile third-party APIs (notifications, webhooks, settlement services) routinely struggle with three problems:

1. **Duplicate delivery.** Producer retries, replay, and partition rebalancing all cause repeat messages. Without dedup, downstream sees the same event twice.
2. **Cascading failures.** When the downstream slows or fails, a naive consumer pool pounds it harder and the outage spreads.
3. **Cost creep.** Underutilized workers and uncompressed payload archives quietly burn money.

This project provides a small, deployable reference implementation that addresses all three: idempotent consumers, rate limiting + circuit breakers, and a measured cost model with S3 lifecycle policies for archived payloads.

## Features

- **Kafka producer + consumer** in Go, using `segmentio/kafka-go` (pure Go, no CGO).
- **Idempotent delivery** via Redis `SETNX` keyed on `IdempotencyKey`; dedup-window TTL is configurable.
- **Token-bucket rate limiter** (`golang.org/x/time/rate`) protects the downstream.
- **Circuit breaker** (`sony/gobreaker`) opens on failure-rate threshold and sheds load while downstream recovers.
- **Connection-pooled HTTP** to downstream with keep-alive and bounded idle connections.
- **Prometheus metrics** for received / deduped / delivered events, downstream errors, breaker open count, delivery latency, downstream latency, and health probes.
- **Naive baseline consumer** ships alongside the optimized one to provide a real "before" measurement under identical workload.
- **Load generator** with configurable sustained RPS, burst windows, and duplicate-injection rate.
- **Benchmark suite** covering throughput, P95 latency, duplicate reduction, chaos resilience, and uptime.
- **Cost model** that turns measured CPU/memory usage into monthly compute spend.
- **Terraform** S3 lifecycle policy for tiered archival of historical event payloads.

## Architecture

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
                                            (mock service with
                                             chaos toggle)

   Prometheus scrapes /metrics on each consumer (port 9100/9101).
   Grafana dashboards on :3000.
```

See [docs/architecture.md](docs/architecture.md) for the detailed component breakdown and design trade-offs.

## Getting started

Requirements: Docker (with Compose v2), Go 1.26+, GNU Make, Python 3.11+ (for cost-model script).

```bash
make tidy          # download Go module dependencies
make build         # compile binaries to bin/
cd deploy && docker compose --profile baseline up -d --build
cd ..
```

The stack brings up Kafka 3.9 (KRaft single-broker), Redis 7, Prometheus 3, Grafana 11, three replicas of the optimized consumer, one replica of the baseline consumer, and the mock downstream service.

Visit:
- Grafana: http://localhost:3000 (anonymous read access enabled)
- Prometheus: http://localhost:9090
- Downstream stats: http://localhost:8081/stats

## Usage

Send a steady 12 events/sec for 5 minutes:

```bash
KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=events \
  ./bin/loadgen --rate 12 --duration 5m --dup-rate 0.10
```

Drive a spike with 5x burst for 60 seconds in the middle of a run:

```bash
./bin/loadgen --rate 20 --duration 5m \
  --burst-rate 100 --burst-for 60s --burst-after 90s
```

Inspect Redis dedup state:

```bash
docker exec -it events-redis redis-cli --scan --pattern 'dedup:*' | head
```

## Benchmarks

Each benchmark is a script that drives a defined workload through both the optimized consumer and the naive baseline, then writes a JSON report under `bench/reports/`.

```bash
make bench-latency         # P95 delivery latency, before vs after
make bench-dedup           # duplicate-delivery reduction
make bench-chaos           # 5xx error rate during spike + chaos
DURATION=24h make bench-throughput   # sustained throughput soak
DURATION=24h make bench-uptime       # uptime probe (DURATION=168h for 7-day)
make cost-model            # CPU/mem -> monthly compute cost CSV
make bench-all             # everything except long soaks
```

Reference performance (from a 2026-05 dry-run on Apple Silicon M3, single-host Docker):

| Metric | Baseline | Optimized |
|---|---|---|
| P95 delivery latency | ~2.0s | ~0.4s |
| Duplicate downstream deliveries (10% dup rate input) | matches input ~10% | <0.5% |
| 5xx error count during 5x spike with 60% downstream fail rate | 100% reference | ~65% of reference |
| Steady-state replicas required | 6 | 3 |

Reproduce on your own hardware with `make bench-all`; reports land in `bench/reports/*.json`.

## Cost model

`scripts/cost_model.py` reads `docker stats` snapshots captured during a benchmark and projects monthly cost using pinned AWS on-demand pricing (`us-east-1`, see comment block in the script). The S3 lifecycle policy in `deploy/terraform/s3-lifecycle.tf` tiers archived payloads through STANDARD → STANDARD_IA → GLACIER_IR → DEEP_ARCHIVE; the pricing math in the trailing comment block estimates ~$80/mo savings on a 15 TB archive.

## Project layout

```
cmd/
  producer/             realistic producer (runs in stack)
  consumer/             optimized consumer: worker pool + dedup + ratelimit + breaker + keep-alive
  baseline-consumer/    naive consumer: sync, no dedup, no breaker, no keep-alive
  loadgen/              load generator: configurable RPS, duration, dup-rate, burst windows
  downstream/           mock downstream HTTP service with chaos toggle

internal/
  queue/                Event type and (de)serialization
  idempotency/          Redis SETNX dedup
  ratelimit/            token bucket
  breaker/              gobreaker wrapper
  metrics/              Prometheus counters + histograms + /metrics + /healthz

deploy/
  docker-compose.yml
  prometheus.yml
  Dockerfile.*          per-service distroless builds
  terraform/            S3 lifecycle policy

scripts/
  bench-*.sh            benchmark drivers
  cost_model.py         compute-cost projection
  capture-docker-stats.sh
  auto-replay.sh        DLQ replay automation
  auto-classify.py      classifies DLQ messages retriable vs permanent

docs/
  architecture.md       component breakdown and trade-offs
  runbook.md            operations procedures (manual and automated paths)
  limitations.md        current scope and roadmap
```

## Operations

See [docs/runbook.md](docs/runbook.md) for operational procedures (duplicate audit, DLQ replay, rate-limit tuning, cost-control audit, health monitoring), each documented in both manual and automated forms.

## Scope and roadmap

See [docs/limitations.md](docs/limitations.md) for current scope and known gaps.

## License

MIT (see LICENSE).
