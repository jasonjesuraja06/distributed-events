#!/usr/bin/env python3
"""
Compute-cost model for the consumer pool.

How to use:
  1. Run a steady-state bench with `docker stats` snapshots saved per consumer.
     `scripts/capture-docker-stats.sh` (called from bench-latency.sh) writes
     `bench/reports/docker-stats-<lane>-<ts>.csv` with CPU % and mem MiB per
     replica every 5 seconds for the bench window.
  2. This script reads the median CPU% and median mem MiB per replica for each
     lane (baseline vs optimized), projects to AWS EC2 instance hours, and
     computes monthly cost using pinned on-demand pricing (see constants below).

This projects measured container CPU and memory onto rented-instance prices. It
is a sizing arithmetic exercise on top of real local measurements, not a bill
from a running deployment: nothing in this repository has ever run on EC2.

Output: a CSV table comparing baseline and optimized monthly spend, plus a
machine-readable summary on stderr. Exits non-zero when the input CSVs are
missing, rather than printing numbers that were not measured.
"""
from __future__ import annotations

import csv
import json
import pathlib
import statistics
import sys

REPORTS = pathlib.Path(__file__).resolve().parent.parent / "bench" / "reports"

# AWS public on-demand list prices, us-east-1, as published in May 2026. These
# are inputs to the projection, not measurements; re-pin them before quoting.
PRICE_PER_VCPU_HOUR_USD = 0.0218  # m7i.large: $0.1008/h for 2 vCPU + 8 GiB, split per vCPU
PRICE_PER_GIB_HOUR_USD = 0.0027   # m7i.large memory share
HOURS_PER_MONTH = 730

# Replica counts as deployed in deploy/docker-compose.yml. The baseline is
# single-replica there; it is priced at the same replica count as the optimized
# lane so the comparison isolates per-replica resource use rather than smuggling
# in an unmeasured capacity assumption.
BASELINE_REPLICAS = 3
OPTIMIZED_REPLICAS = 3


def load_stats(pattern: str) -> tuple[float, float] | None:
    """Return (median_cpu_percent, median_mem_mib) from the most recent CSV
    matching the pattern. CSV columns: ts,name,cpu_perc,mem_mib."""
    files = sorted(REPORTS.glob(pattern))
    if not files:
        return None
    cpu, mem = [], []
    with files[-1].open() as fh:
        for row in csv.DictReader(fh):
            try:
                cpu.append(float(row["cpu_perc"]))
                mem.append(float(row["mem_mib"]))
            except (KeyError, ValueError):
                continue
    if not cpu:
        return None
    return statistics.median(cpu), statistics.median(mem)


def instance_hours_needed(cpu_percent: float, mem_mib: float, replicas: int) -> tuple[float, float]:
    """Compute vCPU and GiB needed across all replicas under steady load."""
    vcpu = (cpu_percent / 100.0) * replicas
    gib = (mem_mib / 1024.0) * replicas
    return vcpu, gib


def monthly_cost(vcpu: float, gib: float) -> float:
    return (vcpu * PRICE_PER_VCPU_HOUR_USD + gib * PRICE_PER_GIB_HOUR_USD) * HOURS_PER_MONTH


def main() -> None:
    baseline = load_stats("docker-stats-baseline-*.csv")
    optimized = load_stats("docker-stats-optimized-*.csv")
    if not baseline or not optimized:
        sys.stderr.write(
            "ERROR: no docker-stats CSVs under bench/reports/.\n"
            "Run scripts/capture-docker-stats.sh for each lane first; this script\n"
            "reports measured resource use and has no substitute for it.\n"
        )
        sys.exit(1)

    b_vcpu, b_gib = instance_hours_needed(*baseline, BASELINE_REPLICAS)
    o_vcpu, o_gib = instance_hours_needed(*optimized, OPTIMIZED_REPLICAS)
    b_cost = monthly_cost(b_vcpu, b_gib)
    o_cost = monthly_cost(o_vcpu, o_gib)
    raw_delta = b_cost - o_cost

    rows = [
        ["", "baseline", "optimized"],
        ["replicas", BASELINE_REPLICAS, OPTIMIZED_REPLICAS],
        ["median_cpu_percent_per_replica", round(baseline[0], 2), round(optimized[0], 2)],
        ["median_mem_mib_per_replica", round(baseline[1], 2), round(optimized[1], 2)],
        ["fleet_vcpu_steady", round(b_vcpu, 3), round(o_vcpu, 3)],
        ["fleet_gib_steady", round(b_gib, 3), round(o_gib, 3)],
        ["compute_$_per_month", round(b_cost, 2), round(o_cost, 2)],
        ["compute_$_per_month_diff", "", round(-raw_delta, 2)],
    ]
    writer = csv.writer(sys.stdout)
    for row in rows:
        writer.writerow(row)

    json.dump({
        "monthly_compute_savings_usd": round(raw_delta, 2),
        "ec2_pricing_vcpu_hour_usd": PRICE_PER_VCPU_HOUR_USD,
        "ec2_pricing_gib_hour_usd": PRICE_PER_GIB_HOUR_USD,
        "baseline_median_cpu_pct": baseline[0],
        "optimized_median_cpu_pct": optimized[0],
    }, sys.stderr, indent=2)


if __name__ == "__main__":
    main()
