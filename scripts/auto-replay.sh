#!/usr/bin/env bash
# Automated DLQ replay.
#
# Replaces manual procedure #2 in docs/runbook.md.
#
# Reads the dead-letter topic, classifies each message via auto-classify.py
# (which inspects error kind via header), and either re-publishes to the
# primary topic with bumped AttemptNumber or files it under permanently-failed/.
set -euo pipefail
source "$(dirname "$0")/_common.sh"

DLQ_TOPIC="${DLQ_TOPIC:-events-dlq}"
PRIMARY_TOPIC="${PRIMARY_TOPIC:-events}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-3}"

ts=$(stamp)
report="${REPORTS_DIR}/auto-replay-${ts}.json"

# Use kafka-go's consumer in a one-shot bash wrapper: dump DLQ to JSONL, then
# classify and re-publish.
dlq_dump="${REPORTS_DIR}/auto-replay-${ts}-dlq.jsonl"
docker exec -i events-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "${DLQ_TOPIC}" \
  --from-beginning --timeout-ms 5000 \
  > "${dlq_dump}" 2>/dev/null || true

python3 "$(dirname "$0")/auto-classify.py" \
  --input "${dlq_dump}" \
  --max-attempts "${MAX_ATTEMPTS}" \
  --report "${report}"

# Re-publish the retriable subset.
retriable="${REPORTS_DIR}/auto-replay-${ts}-retriable.jsonl"
if [[ -s "${retriable}" ]]; then
  cat "${retriable}" | docker exec -i events-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic "${PRIMARY_TOPIC}" >/dev/null
fi

echo "auto-replay report -> ${report}"
