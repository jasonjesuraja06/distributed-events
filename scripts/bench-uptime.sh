#!/usr/bin/env bash
# Uptime probe.
#
# Pings each consumer replica's /healthz every 2 seconds for DURATION and
# reports the success-rate-as-availability. The 99.9% target corresponds to
# at most 605 seconds of downtime per 7-day window.
#
# Run as DURATION=168h for the full 7-day soak; DURATION=24h is a useful
# verification window.
set -euo pipefail
source "$(dirname "$0")/_common.sh"

DURATION="${DURATION:-1h}"
INTERVAL="${INTERVAL:-2}"
TARGETS=(${TARGETS:-http://localhost:9100/healthz})

ts=$(stamp)
report="${REPORTS_DIR}/uptime-${ts}.json"
log="${REPORTS_DIR}/uptime-${ts}.log"

end_epoch=$(python3 -c "import time, re; s='${DURATION}'; u={'s':1,'m':60,'h':3600,'d':86400}; print(int(time.time())+int(s[:-1])*u[s[-1]])")
echo "uptime probe duration=${DURATION} interval=${INTERVAL}s targets=${TARGETS[*]}" | tee "${log}"

ok=0; fail=0
while [[ $(date +%s) -lt ${end_epoch} ]]; do
  for t in "${TARGETS[@]}"; do
    if curl -fsS --max-time 1 "${t}" >/dev/null 2>&1; then ok=$((ok+1)); else fail=$((fail+1)); fi
  done
  sleep "${INTERVAL}"
done

total=$((ok+fail))
python3 - <<PY
ok=${ok}; fail=${fail}; total=${total}
avail = round(ok / total * 100, 4) if total else None
allowed_downtime_per_99_9 = round(total * 0.001, 0)
import json, pathlib
out = {
  "duration": "${DURATION}",
  "interval_seconds": ${INTERVAL},
  "probes_ok": ok,
  "probes_fail": fail,
  "probes_total": total,
  "availability_pct": avail,
  "fails_allowed_for_99_9": int(allowed_downtime_per_99_9),
  "meets_99_9": (fail <= allowed_downtime_per_99_9) if total else False,
}
pathlib.Path("${report}").write_text(json.dumps(out, indent=2))
print(json.dumps(out, indent=2))
PY
echo "report -> ${report}"
