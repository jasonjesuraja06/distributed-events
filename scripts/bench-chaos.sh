#!/usr/bin/env bash
# Chaos / spike benchmark.
#
# Drives a traffic spike with the downstream's failure rate raised to 60% and
# measures what each consumer lane does with it.
#
# Raw 5xx counts are NOT comparable across the two lanes: the optimized lane
# runs 48 workers and the baseline runs one synchronous loop, so the baseline
# attempts far fewer calls in the same window and records fewer errors for that
# reason alone. The comparable figure is the 5xx rate per attempted delivery,
# which is what this script reports alongside the raw counts.
set -euo pipefail
source "$(dirname "$0")/_common.sh"

PRE_DURATION="${PRE_DURATION:-2m}"
SPIKE_DURATION="${SPIKE_DURATION:-1m}"
POST_DURATION="${POST_DURATION:-2m}"
RATE="${RATE:-20}"
BURST_RATE="${BURST_RATE:-100}"

wait_for_prom
wait_for_downstream
ensure_built

ts=$(stamp)
report="${REPORTS_DIR}/chaos-${ts}.json"

start=$(date +%s)

run_chaos_lane() {
  local label="$1" topic="$2"
  echo "[${label}] feeding ${topic} pre=${PRE_DURATION} spike=${SPIKE_DURATION} post=${POST_DURATION}"
  curl -fsS -X POST "${DOWNSTREAM_URL}/chaos/off" >/dev/null
  local total_duration
  total_duration=$(python3 -c "import re; m={'s':1,'m':60,'h':3600}; s=0
for tok in [r for r in '${PRE_DURATION} ${SPIKE_DURATION} ${POST_DURATION}'.split()]:
    n,u=tok[:-1],tok[-1]; s+=int(n)*m[u]
print(f'{s}s')")
  KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC="${topic}" \
    "${REPO_ROOT}/bin/loadgen" \
    --rate "${RATE}" --duration "${total_duration}" --dup-rate 0.05 \
    --burst-rate "${BURST_RATE}" --burst-for "${SPIKE_DURATION}" --burst-after "${PRE_DURATION}" \
    --report "${REPORTS_DIR}/chaos-${ts}-${label}-load.json" &
  local pid=$!
  # Enable chaos exactly during spike window
  pre_sec=$(python3 -c "u={'s':1,'m':60,'h':3600}; print(int('${PRE_DURATION}'[:-1])*u['${PRE_DURATION:(-1)}'])")
  spike_sec=$(python3 -c "u={'s':1,'m':60,'h':3600}; print(int('${SPIKE_DURATION}'[:-1])*u['${SPIKE_DURATION:(-1)}'])")
  sleep "${pre_sec}"
  curl -fsS -X POST "${DOWNSTREAM_URL}/chaos/on" >/dev/null
  sleep "${spike_sec}"
  curl -fsS -X POST "${DOWNSTREAM_URL}/chaos/off" >/dev/null
  wait "${pid}"
}

run_chaos_lane "baseline" "events-baseline"
sleep 30
run_chaos_lane "optimized" "events"
sleep 30

end=$(date +%s)
window="${SPIKE_DURATION}"

WINDOW="${WINDOW:-10m}"
baseline_5xx=$(prom_query "sum(increase(downstream_errors_total{consumer=\"baseline\",kind=\"5xx\"}[${WINDOW}]))")
opt_5xx=$(prom_query "sum(increase(downstream_errors_total{consumer=\"optimized\",kind=\"5xx\"}[${WINDOW}]))")
opt_breaker=$(prom_query "sum(increase(breaker_open_total{consumer=\"optimized\"}[${WINDOW}]))")
baseline_delivered=$(prom_query "sum(increase(events_delivered_total{consumer=\"baseline\"}[${WINDOW}]))")
opt_delivered=$(prom_query "sum(increase(events_delivered_total{consumer=\"optimized\"}[${WINDOW}]))")

python3 - <<PY
import json
b5 = float("${baseline_5xx}" or 0); o5 = float("${opt_5xx}" or 0)
bd = float("${baseline_delivered}" or 0); od = float("${opt_delivered}" or 0)
brk = float("${opt_breaker}" or 0)

def rate(errors, delivered):
    # An attempt either delivered or returned 5xx. Breaker rejections are not
    # attempts: the call never reached the downstream.
    attempts = errors + delivered
    return round(errors / attempts * 100, 3) if attempts else None

b_rate, o_rate = rate(b5, bd), rate(o5, od)
out = {
  "pre_duration": "${PRE_DURATION}",
  "spike_duration": "${SPIKE_DURATION}",
  "post_duration": "${POST_DURATION}",
  "promql_range_window": "${WINDOW}",
  "rate_steady": ${RATE},
  "rate_burst": ${BURST_RATE},
  "baseline_5xx": b5,
  "optimized_5xx": o5,
  "baseline_delivered": bd,
  "optimized_delivered": od,
  "baseline_attempts": b5 + bd,
  "optimized_attempts": o5 + od,
  "baseline_5xx_rate_pct": b_rate,
  "optimized_5xx_rate_pct": o_rate,
  "optimized_breaker_rejects": brk,
  "note": "raw 5xx counts are not comparable across lanes; the lanes attempt different numbers of calls in the same window",
}
import pathlib
pathlib.Path("${report}").write_text(json.dumps(out, indent=2))
print(json.dumps(out, indent=2))
PY
echo "report -> ${report}"
