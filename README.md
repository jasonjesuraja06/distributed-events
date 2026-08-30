# distributed-events

A Kafka and Redis event pipeline in Go that gives a fragile downstream service
at-most-once delivery, with a naive consumer alongside it as a measurable baseline.

## Why

A consumer that fans out to a third-party API has three problems that look
solved until traffic arrives. Producer retries and partition rebalances
republish the same logical event, so the downstream acts on it twice. A worker
pool that retries into a slowing downstream turns a slowdown into an outage.
And nobody knows what a replica costs until someone measures one.

This implements one answer to each and ships a deliberately naive consumer on a
parallel topic, so the difference is measured under identical load rather than
asserted.

## Architecture

```
  loadgen --> topic "events" ---------> consumer x3, 16 workers each --+
  (host)      (KRaft, 6 partitions)     dedup / rate limit / breaker   |
                                                                       v
  loadgen --> topic "events-baseline" -> baseline-consumer x1 -> mock downstream
  (host)                                 sync, none of the above  latency + chaos

  Prometheus discovers replicas by DNS, scrapes /metrics on 9100 and 9101.
```

Each event carries an `IdempotencyKey`. A worker claims it with
`SETNX dedup:<xxhash64 in hex> 1 EX 3600` before calling the downstream, and a
worker that loses the race skips the call. Offsets are committed when the event
enters the worker pool, before the downstream call, so a failed delivery is
dropped rather than retried: that is what makes the semantics at-most-once.
Component breakdown and trade-offs in
[docs/architecture.md](docs/architecture.md).

## Measured results

Hardware: Apple M4 Pro, 14 cores, 48 GB RAM, macOS arm64, Docker Desktop,
single host. Every number below came from the command in its last column, and
the raw JSON, logs, and CSVs are committed under `bench/reports/`. These are
short runs; each row states the scope it was measured at and is not
extrapolated beyond it except where the row says so.

| Measurement | Baseline | Optimized | Scope | Reproduce |
|---|---|---|---|---|
| Duplicate downstream deliveries | 268 of 2823 | 0 of 2525 | 2 min per lane, 25 events/s offered, 10% duplicate injection | `DURATION=2m RATE=25 DUP_RATE=0.10 make bench-dedup` |
| P95 / P99 delivery latency, below the baseline's capacity | 0.097 s / 0.099 s | 0.097 s / 0.099 s | 90 s per lane, 15 events/s offered | `DURATION=90s RATE=15 WINDOW=2m make bench-latency` |
| P50 / P95 delivery latency, above the baseline's capacity | 7.16 s / at least 10 s | 0.070 s / 0.097 s | same 25 events/s run as row 1, histograms read per lane | `bench/reports/latency-saturation-*.json` |
| Calls issued into a downstream forced to 60% failure | 2211 attempts, 534 returned 5xx (24.2%) | 780 attempts, 223 returned 5xx (28.6%), 1288 more shed at the breaker | 30 s steady, 45 s spike at 40 events/s with chaos on, 30 s recovery, per lane | `PRE_DURATION=30s SPIKE_DURATION=45s POST_DURATION=30s RATE=10 BURST_RATE=40 make bench-chaos` |
| Sustained ingest with consumers keeping up | not run | 24.0 events/s, 4320 events | 3 min, no backlog at the end | `DURATION=3m RATE=25 WINDOW=4m make bench-throughput` |
| Consumer fleet availability | not run | 87 of 87 samples with all 3 replicas up | 3 min, 2 s sampling interval | `DURATION=3m INTERVAL=2 make bench-uptime` |
| Median CPU / memory per replica | 4.25% / 21.8 MiB | 1.73% / 22.5 MiB | 95 s sample, 5 s interval, 25 events/s offered | `./scripts/capture-docker-stats.sh optimized 95 '^deploy-consumer-'`, then `make cost-model` |

Reading these honestly:

Below the baseline's capacity the two lanes have the same delivery latency,
because latency there is the downstream's own 30 to 60 ms. The worker pool buys
headroom, not per-event speed: rows 2 and 3 are the same code at 15 and at 25
events/s. The 10 s figure is the top finite histogram bucket, a lower bound.

The breaker works by removing attempts, not by making attempts succeed, which
is why the optimized lane's per-attempt error rate is slightly worse while it
puts less than half the failing traffic on the downstream. Those 1288 shed
calls are dropped events: with no replay path, protecting the downstream costs
delivery. The ingest figure is offered load the consumers kept up with, not a saturation
point. Three minutes of full availability says the fleet did not fall over in
three minutes; it is not a 99.9% claim. The cost model turns measured container
CPU and memory into EC2 list-price arithmetic, having never run on EC2.

## Quickstart

Requirements: Docker with Compose v2, Go 1.26+, GNU Make, Python 3.11+.

```bash
make test        # unit tests under the race detector, no Docker needed
make build
make stack-up    # Kafka, Redis, Prometheus, 3 consumers, baseline, downstream
./bin/loadgen --rate 25 --duration 2m --dup-rate 0.10
curl -s localhost:8081/stats   # deliveries minus unique_keys is the duplicate count
docker exec -it events-redis redis-cli --scan --pattern 'dedup:*' | head
make bench-dedup # or bench-latency, bench-chaos, bench-throughput, bench-uptime
make stack-down
```

Kafka advertises `kafka:19092` inside the compose network and `localhost:9092`
for host tools. A `kafka-init` service creates the topics before any consumer
starts: a group that joins a topic which does not exist yet is assigned zero
partitions and stays that way until the next rebalance.

## Layout

`cmd/consumer` worker pool, dedup, rate limit, breaker, keep-alive.
`cmd/baseline-consumer` synchronous, none of the above, on a parallel topic.
`cmd/downstream` mock downstream with artificial latency and a chaos toggle.
`cmd/loadgen` load generator with duplicate injection and burst windows.
`internal/` event type, Redis dedup, token bucket, breaker, metrics.
`deploy/` compose stack, Prometheus config, distroless Dockerfiles, S3 policy.
`scripts/` benchmark drivers, docker stats capture, cost model.
`bench/reports/` committed output behind every number above.

## Limitations

Single-broker Kafka, single-node Redis, one host. No dead-letter topic and no
replay: a failed downstream call drops the event. The rate limit is per
replica, so three replicas at `RATE_LIMIT_RPS=300` can offer 900 RPS between
them. Dedup holds within the 1 hour TTL only. The latency histogram tops out at
10 s, so a consumer far enough behind reports a P95 pinned at that boundary,
which is a lower bound rather than a measurement. The S3 lifecycle policy in
`deploy/terraform/` has never been applied and its cost comment is an
explicitly hypothetical sizing exercise. Full list in
[docs/limitations.md](docs/limitations.md). CI
(`.github/workflows/ci.yml`) runs gofmt, vet, build, `go test -race`, and a
compose config parse; it does not run the benchmarks, which need the stack up
and minutes of wall clock per number.

## License

MIT, see [LICENSE](LICENSE).
