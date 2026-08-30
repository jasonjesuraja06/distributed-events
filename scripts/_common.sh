#!/usr/bin/env bash
# Shared helpers for benchmark scripts.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPORTS_DIR="${REPO_ROOT}/bench/reports"
mkdir -p "${REPORTS_DIR}"

PROM_URL="${PROM_URL:-http://localhost:9090}"
DOWNSTREAM_URL="${DOWNSTREAM_URL:-http://localhost:8081}"

# prom_query <promql> -> raw value of first result, or "0" when the query
# matched nothing. A rejected query (bad PromQL, Prometheus down) is fatal:
# silently substituting 0 would put an unmeasured number in a report.
prom_query() {
  local q="$1" body
  if ! body=$(curl -fsS --data-urlencode "query=${q}" "${PROM_URL}/api/v1/query"); then
    echo "prom_query failed: ${q}" >&2
    return 1
  fi
  printf '%s' "${body}" | python3 -c '
import sys, json
d = json.load(sys.stdin)
if d.get("status") != "success":
    sys.stderr.write("prometheus error: %s\n" % d.get("error", d))
    sys.exit(1)
r = d.get("data", {}).get("result", [])
print(r[0]["value"][1] if r else "0")
'
}

# prom_query_range <promql> <start_unix> <end_unix> <step> -> JSON array of [ts, value]
prom_query_range() {
  local q="$1" start="$2" end="$3" step="${4:-15}"
  curl -fsS \
    --data-urlencode "query=${q}" \
    --data-urlencode "start=${start}" \
    --data-urlencode "end=${end}" \
    --data-urlencode "step=${step}" \
    "${PROM_URL}/api/v1/query_range"
}

wait_for_prom() {
  for _ in {1..60}; do
    if curl -fsS "${PROM_URL}/-/ready" >/dev/null 2>&1; then return 0; fi
    sleep 2
  done
  echo "prometheus never came up" >&2
  return 1
}

wait_for_downstream() {
  for _ in {1..60}; do
    if curl -fsS "${DOWNSTREAM_URL}/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 2
  done
  echo "downstream never came up" >&2
  return 1
}

reset_downstream_stats() {
  curl -fsS -X POST "${DOWNSTREAM_URL}/reset" >/dev/null || true
}

downstream_stats() {
  curl -fsS "${DOWNSTREAM_URL}/stats"
}

ensure_built() {
  pushd "${REPO_ROOT}" >/dev/null
  go build -o bin/loadgen ./cmd/loadgen
  popd >/dev/null
}

stamp() { date -u +"%Y-%m-%dT%H-%M-%SZ"; }
