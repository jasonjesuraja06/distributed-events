#!/usr/bin/env bash
# Sample docker stats for a window and write CSV used by cost_model.py.
#
# Usage: capture-docker-stats.sh <lane> <duration_seconds> <container_name_regex>
#
# The regex is matched with `grep -E` against the container name. Note that a
# plain "consumer" filter also matches deploy-baseline-consumer-1, so the
# optimized lane needs an anchored pattern.
set -euo pipefail
source "$(dirname "$0")/_common.sh"

LANE="${1:-baseline}"
DUR="${2:-300}"
FILTER="${3:-^deploy-consumer-}"

ts=$(stamp)
out="${REPORTS_DIR}/docker-stats-${LANE}-${ts}.csv"
echo "ts,name,cpu_perc,mem_mib" > "${out}"

end=$(( $(date +%s) + DUR ))
while [[ $(date +%s) -lt ${end} ]]; do
  docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" \
    | grep -E "${FILTER}" \
    | while IFS=, read -r name cpu mem; do
        cpu_n=$(echo "${cpu}" | tr -d ' %')
        mem_n=$(echo "${mem}" | awk -F'/' '{print $1}' | tr -d ' ' \
                | python3 -c "import sys; s=sys.stdin.read().strip(); u={'MiB':1,'GiB':1024,'KiB':1/1024}; n=float(''.join(c for c in s if c.isdigit() or c=='.')); suf=''.join(c for c in s if c.isalpha()); print(n*u.get(suf,1))")
        echo "$(date -u +%s),${name},${cpu_n},${mem_n}" >> "${out}"
      done
  sleep 5
done
echo "wrote ${out}"
