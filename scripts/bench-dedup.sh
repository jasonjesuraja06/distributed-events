#!/usr/bin/env bash
# Duplicate-delivery benchmark.
#
# Sends N events with INJECTED_DUP_RATE fraction sharing IdempotencyKey with
# the previous event. Measures duplicate downstream deliveries for baseline vs
# optimized. A "duplicate" is a downstream delivery whose IdempotencyKey was
# already delivered once during the run.
set -euo pipefail
source "$(dirname "$0")/_common.sh"

DURATION="${DURATION:-5m}"
RATE="${RATE:-20}"
DUP_RATE="${DUP_RATE:-0.10}"

wait_for_prom
wait_for_downstream
ensure_built

ts=$(stamp)
report="${REPORTS_DIR}/dedup-${ts}.json"

# Run baseline
reset_downstream_stats
KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=events-baseline \
  "${REPO_ROOT}/bin/loadgen" --rate "${RATE}" --duration "${DURATION}" --dup-rate "${DUP_RATE}" \
  --report "${REPORTS_DIR}/dedup-${ts}-baseline-load.json" >/dev/null
sleep 20
baseline_stats=$(downstream_stats)

# Run optimized
reset_downstream_stats
KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=events \
  "${REPO_ROOT}/bin/loadgen" --rate "${RATE}" --duration "${DURATION}" --dup-rate "${DUP_RATE}" \
  --report "${REPORTS_DIR}/dedup-${ts}-optimized-load.json" >/dev/null
sleep 20
opt_stats=$(downstream_stats)

python3 - <<PY
import json
b = json.loads('''${baseline_stats}''')
o = json.loads('''${opt_stats}''')
# duplicates = (downstream deliveries) - (unique idempotency keys delivered)
b_dups = b["deliveries"] - b["unique_keys"]
o_dups = o["deliveries"] - o["unique_keys"]
reduction = round((b_dups - o_dups) / b_dups * 100, 2) if b_dups else None
out = {
  "baseline_deliveries": b["deliveries"],
  "baseline_unique_keys": b["unique_keys"],
  "baseline_duplicate_deliveries": b_dups,
  "optimized_deliveries": o["deliveries"],
  "optimized_unique_keys": o["unique_keys"],
  "optimized_duplicate_deliveries": o_dups,
  "duplicate_reduction_pct": reduction,
}
import pathlib
pathlib.Path("${report}").write_text(json.dumps(out, indent=2))
print(json.dumps(out, indent=2))
PY
echo "report -> ${report}"
