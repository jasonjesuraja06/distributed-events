# Operations Runbook

Procedures for running the stack. This project has never been operated in
production, so nothing here carries incident frequencies, time-per-occurrence
figures, or savings claims. What follows is the procedure and the command, and
where the code implements the automated path, the file that implements it.

## 1. Duplicate-delivery audit

**What the system does.** `internal/idempotency/redis_dedup.go` claims a Redis
key per `IdempotencyKey` with `SETNX` before the downstream call. A consumer
that loses the race increments `events_deduped_total` and returns without
calling downstream, so the duplicate never reaches the downstream service.

**Manual audit.** Compare what the load generator injected against what the
downstream actually saw:

```bash
curl -s localhost:8081/stats        # {"deliveries":N,"unique_keys":M}
```

`deliveries - unique_keys` is the number of duplicate downstream calls over the
window since the last `POST /reset`. `scripts/bench-dedup.sh` automates exactly
this comparison across both consumer lanes.

**Inspect live dedup state.**

```bash
docker exec -it events-redis redis-cli --scan --pattern 'dedup:*' | head
docker exec -it events-redis redis-cli dbsize
```

## 2. Failed deliveries

There is no dead-letter topic and no replay path. The consumer commits the
Kafka offset when the event is handed to the worker pool, so an event whose
downstream call fails is dropped, counted in
`downstream_errors_total{kind="5xx"|"network"|"4xx"}`, and not retried. This is
the at-most-once semantics the project is built around; if you need
at-least-once, the offset commit has to move after the downstream call and a
retry topic has to exist.

The one thing the consumer does on failure is release the dedup slot
(`Deduper.Release`), so a later publish of the same `IdempotencyKey` is not
suppressed by the failed attempt.

**Check the failure counters.**

```bash
curl -s 'localhost:9090/api/v1/query' \
  --data-urlencode 'query=sum by (consumer, kind) (downstream_errors_total)'
```

## 3. Rate-limit tuning

`RATE_LIMIT_RPS` (default 300) sets the token-bucket rate per consumer replica;
burst is twice the rate. It also sets the circuit breaker's minimum request
count before the breaker will evaluate its failure rate (`RATE_LIMIT_RPS / 5`,
see `cmd/consumer/main.go`), so lowering the rate limit also lowers the number
of requests the breaker needs before it can trip.

To change it, edit the `consumer` service environment in
`deploy/docker-compose.yml` and recreate the service:

```bash
cd deploy && docker compose up -d --force-recreate consumer
```

## 4. Cost model

`scripts/cost_model.py` reads `docker stats` samples captured by
`scripts/capture-docker-stats.sh` and projects measured CPU and memory onto
published EC2 list prices. It exits non-zero when no samples exist rather than
printing a number that was not measured.

```bash
./scripts/capture-docker-stats.sh optimized 300 '^deploy-consumer-'
./scripts/capture-docker-stats.sh baseline 300 '^deploy-baseline-consumer-'
make cost-model
```

The S3 lifecycle policy in `deploy/terraform/s3-lifecycle.tf` has never been
applied; its cost comment is an explicitly hypothetical sizing exercise.

## 5. Health monitoring

Prometheus discovers every consumer replica by DNS (`deploy/prometheus.yml`)
and scrapes `/metrics` on 9100 (optimized) and 9101 (baseline). Each replica
also serves `/healthz` on the same port. No alerting rules are configured.

```bash
curl -s 'localhost:9090/api/v1/targets?state=active' | python3 -m json.tool | head -40
curl -s 'localhost:9090/api/v1/query' --data-urlencode 'query=up{job="consumer"}'
```

`scripts/bench-uptime.sh` samples `up{job="consumer"}` and the downstream
`/healthz` on an interval and reports the fraction of samples in which every
replica was up.
