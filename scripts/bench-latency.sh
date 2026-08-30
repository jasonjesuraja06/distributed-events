#!/usr/bin/env bash
# P95 delivery latency: baseline vs optimized consumer.
#
# Drives identical workload (15min default) through both consumers, then
# queries Prometheus for histogram_quantile P95 on each:
#   1. baseline-consumer (events-baseline topic) -- the reference
#   2. optimized consumer (events topic)         -- with worker pool, dedup,
#                                                   rate limit, breaker, keep-alive
set -euo pipefail
source "$(dirname "$0")/_common.sh"

DURATION="${DURATION:-15m}"
# PromQL range must cover the lane that just ran, plus the settle time.
WINDOW="${WINDOW:-${DURATION}}"
RATE="${RATE:-20}"
DUP_RATE="${DUP_RATE:-0.10}"

wait_for_prom
wait_for_downstream
ensure_built

ts=$(stamp)
report="${REPORTS_DIR}/latency-${ts}.json"
log="${REPORTS_DIR}/latency-${ts}.log"

echo "=== BASELINE: feeding events-baseline topic for ${DURATION} ===" | tee -a "${log}"
KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=events-baseline \
  "${REPO_ROOT}/bin/loadgen" --rate "${RATE}" --duration "${DURATION}" --dup-rate "${DUP_RATE}" \
  --report "${REPORTS_DIR}/latency-${ts}-baseline-load.json" 2>&1 | tee -a "${log}"

sleep 30

baseline_p95=$(prom_query "histogram_quantile(0.95, sum(rate(delivery_latency_seconds_bucket{consumer=\"baseline\"}[${WINDOW}])) by (le))")
baseline_p99=$(prom_query "histogram_quantile(0.99, sum(rate(delivery_latency_seconds_bucket{consumer=\"baseline\"}[${WINDOW}])) by (le))")

echo "=== OPTIMIZED: feeding events topic for ${DURATION} ===" | tee -a "${log}"
KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=events \
  "${REPO_ROOT}/bin/loadgen" --rate "${RATE}" --duration "${DURATION}" --dup-rate "${DUP_RATE}" \
  --report "${REPORTS_DIR}/latency-${ts}-optimized-load.json" 2>&1 | tee -a "${log}"

sleep 30

opt_p95=$(prom_query "histogram_quantile(0.95, sum(rate(delivery_latency_seconds_bucket{consumer=\"optimized\"}[${WINDOW}])) by (le))")
opt_p99=$(prom_query "histogram_quantile(0.99, sum(rate(delivery_latency_seconds_bucket{consumer=\"optimized\"}[${WINDOW}])) by (le))")

python3 - <<PY
import json
out = {
  "duration": "${DURATION}",
  "promql_range_window": "${WINDOW}",
  "rate_per_sec": ${RATE},
  "baseline_p95_seconds": float("${baseline_p95}" or 0),
  "baseline_p99_seconds": float("${baseline_p99}" or 0),
  "optimized_p95_seconds": float("${opt_p95}" or 0),
  "optimized_p99_seconds": float("${opt_p99}" or 0),
}
out["p95_reduction_pct"] = round((out["baseline_p95_seconds"] - out["optimized_p95_seconds"]) / out["baseline_p95_seconds"] * 100, 2) if out["baseline_p95_seconds"] else None
import pathlib
pathlib.Path("${report}").write_text(json.dumps(out, indent=2))
print(json.dumps(out, indent=2))
PY
echo "report -> ${report}"
