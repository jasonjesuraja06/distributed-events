#!/usr/bin/env bash
# Sustained throughput soak: how many events/day can the system absorb at
# steady state without backpressure?
#
# Default config (12 events/sec for 24h) totals ~1,036,800 events. Override
# DURATION for shorter verification runs (e.g. DURATION=1h, DURATION=10m).
set -euo pipefail
source "$(dirname "$0")/_common.sh"

DURATION="${DURATION:-24h}"
RATE="${RATE:-12}"
DUP_RATE="${DUP_RATE:-0.05}"

wait_for_prom
wait_for_downstream
ensure_built
reset_downstream_stats

ts=$(stamp)
report="${REPORTS_DIR}/throughput-${ts}.json"
log="${REPORTS_DIR}/throughput-${ts}.log"

echo "soak duration=${DURATION} rate=${RATE}/s dup-rate=${DUP_RATE}"
KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=events \
  "${REPO_ROOT}/bin/loadgen" \
  --rate "${RATE}" --duration "${DURATION}" --dup-rate "${DUP_RATE}" \
  --report "${report}" 2>&1 | tee "${log}"

received=$(prom_query 'sum(increase(events_received_total{consumer="optimized"}[24h]))')
delivered=$(prom_query 'sum(increase(events_delivered_total{consumer="optimized"}[24h]))')
echo "kafka_received=${received} downstream_delivered=${delivered}"

python3 - <<PY
import json, pathlib
p = pathlib.Path("${report}")
d = json.loads(p.read_text())
d["kafka_events_received_24h"] = ${received}
d["downstream_delivered_24h"] = ${delivered}
d["sustained_per_day_extrapolated"] = round(d["per_day_extrapolated"])
p.write_text(json.dumps(d, indent=2))
print(json.dumps(d, indent=2))
PY
echo "report -> ${report}"
