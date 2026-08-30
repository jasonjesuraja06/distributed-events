#!/usr/bin/env bash
# Availability probe.
#
# The consumer service runs with replicas > 1 and so publishes no host port;
# its /healthz is only reachable inside the compose network. Availability is
# therefore sampled two ways every INTERVAL seconds:
#
#   1. consumer fleet   -- Prometheus `up{job="consumer"}`, one series per
#                          replica, scraped from each replica's own metrics
#                          server. A sample counts as OK only when every
#                          replica reports up.
#   2. downstream       -- direct HTTP GET of the published /healthz port.
#
# A sample is not a second of wall clock: availability_pct is the fraction of
# samples that succeeded, over the DURATION window actually run.
set -euo pipefail
source "$(dirname "$0")/_common.sh"

DURATION="${DURATION:-1h}"
INTERVAL="${INTERVAL:-2}"
EXPECT_REPLICAS="${EXPECT_REPLICAS:-3}"

wait_for_prom
wait_for_downstream

ts=$(stamp)
report="${REPORTS_DIR}/uptime-${ts}.json"

end_epoch=$(python3 -c "import time; s='${DURATION}'; u={'s':1,'m':60,'h':3600,'d':86400}; print(int(time.time())+int(s[:-1])*u[s[-1]])")
echo "uptime probe duration=${DURATION} interval=${INTERVAL}s expect_replicas=${EXPECT_REPLICAS}"

consumer_ok=0; consumer_fail=0
downstream_ok=0; downstream_fail=0
start_epoch=$(date +%s)

while [[ $(date +%s) -lt ${end_epoch} ]]; do
  up=$(prom_query "sum(up{job=\"consumer\"})" 2>/dev/null || echo 0)
  if python3 -c "import sys; sys.exit(0 if float('${up}' or 0) >= ${EXPECT_REPLICAS} else 1)"; then
    consumer_ok=$((consumer_ok+1))
  else
    consumer_fail=$((consumer_fail+1))
  fi
  if curl -fsS --max-time 1 "${DOWNSTREAM_URL}/healthz" >/dev/null 2>&1; then
    downstream_ok=$((downstream_ok+1))
  else
    downstream_fail=$((downstream_fail+1))
  fi
  sleep "${INTERVAL}"
done

elapsed=$(( $(date +%s) - start_epoch ))
python3 - <<PY
import json, pathlib
c_ok, c_fail = ${consumer_ok}, ${consumer_fail}
d_ok, d_fail = ${downstream_ok}, ${downstream_fail}
c_total, d_total = c_ok + c_fail, d_ok + d_fail
out = {
  "duration_requested": "${DURATION}",
  "elapsed_seconds": ${elapsed},
  "interval_seconds": ${INTERVAL},
  "expected_consumer_replicas": ${EXPECT_REPLICAS},
  "consumer_samples_all_replicas_up": c_ok,
  "consumer_samples_degraded": c_fail,
  "consumer_samples_total": c_total,
  "consumer_availability_pct": round(c_ok / c_total * 100, 4) if c_total else None,
  "downstream_probes_ok": d_ok,
  "downstream_probes_fail": d_fail,
  "downstream_availability_pct": round(d_ok / d_total * 100, 4) if d_total else None,
}
pathlib.Path("${report}").write_text(json.dumps(out, indent=2))
print(json.dumps(out, indent=2))
PY
echo "report -> ${report}"
