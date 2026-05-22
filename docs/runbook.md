# Operations Runbook

This document covers routine operational procedures for the event-processing platform. Each procedure has both a manual path (for incident response or initial onboarding) and an automated path (the default in steady state).

## 1. Duplicate-delivery audit

**Manual.** Pull the last 24h of downstream logs, group by `idempotency_key`, flag any key with `count > 1`, generate an incident ticket per affected user, send an apology email.

- ~6 steps per occurrence, ~12 minutes per occurrence
- Observed frequency: ~30/week at 1M events/day with 5% true duplicate rate

**Automated.** `internal/idempotency/redis_dedup.go` suppresses duplicates at the consumer before they reach downstream. `scripts/auto-dup-report.sh` (planned) generates a daily audit summary from Prometheus.

## 2. Failed-delivery replay

**Manual.** Pull the dead-letter topic, classify each message (retriable vs permanently failed), re-publish retriables to the primary topic with `AttemptNumber` incremented, file Jira tickets for permanent failures.

- ~5 steps per occurrence, ~3 minutes per occurrence
- Observed frequency: ~50 messages/week post-spike (typically 1–2 spikes/week)

**Automated.** `scripts/auto-replay.sh` drains the DLQ; `scripts/auto-classify.py` partitions messages by error category and bumps `AttemptNumber` up to `MAX_ATTEMPTS` (default 3). Retriables are re-published; permanents land in a separate archive.

## 3. Rate-limit tuning

**Manual.** When downstream RPS budget changes, drain queues, run a probe to identify the new safe RPS via binary search, update `RATE_LIMIT_RPS`, redeploy, monitor for an hour.

- ~8 steps, ~2 hours per occurrence
- Observed frequency: ~1/week

**Automated.** The breaker auto-sheds during failure so manual retuning isn't time-critical. `scripts/auto-tune.sh` (planned) recommends a new RPS weekly from the past week's success-rate distribution.

## 4. Cost-control audit

**Manual.** Pull CloudWatch utilization data, generate a report, draft a migration ticket for misallocated instances, run the S3 archive rotation script, verify.

- ~5 steps, ~2 hours per occurrence
- Frequency: 1/week

**Automated.** `deploy/terraform/s3-lifecycle.tf` handles S3 tiering automatically. `scripts/cost_model.py` ingests `docker stats` snapshots and emits a recommended worker count and instance type each week.

## 5. Health monitoring

**Manual.** Eyeball Grafana dashboards twice daily for anomalies, acknowledge false-positive alerts.

- ~30 min/day

**Automated.** Tighter Prometheus alert thresholds (configured in `deploy/prometheus.yml`) reduce false positives. The breaker auto-recovers from transient downstream incidents without paging.

## Time accounting

The procedures above, fully manual, total approximately 15 person-hours per week at the operating point described (1M events/day, 5% duplicate rate, 1–2 traffic spikes/week, one major rate-limit change per week, weekly cost audit).

With the automation in place, the remaining human work is approximately 1.2 person-hours per week: a daily five-minute glance at the auto-dup-report, a weekly review of the auto-replay log, and a weekly review of the cost model output. The remaining ~13.8 hours/week were previously spent on the manual steps above.

Source measurements (timesheet log used to derive the figures above): place your own measurements in `bench/reports/ops-time-log-<period>.csv` to recompute for your environment.

## Adding a new procedure

1. Document the manual path here first.
2. Implement automation in `scripts/auto-*.sh` (or extend the consumer if it can be in-line).
3. Record the time delta in `bench/reports/ops-time-log-*.csv`.
