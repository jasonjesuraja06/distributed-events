# Benchmark reports

Raw output behind the results table in the top-level README. Every file here
was produced on an Apple M4 Pro (14 cores, 48 GB RAM, macOS arm64) running the
compose stack in Docker Desktop on a single host, on 2026-08-29/30 UTC.

| File | Table row | Command |
|---|---|---|
| `dedup-*.json` | duplicate downstream deliveries | `DURATION=2m RATE=25 DUP_RATE=0.10 make bench-dedup` |
| `latency-2026-08-30T00-17-26Z.json` | P95 / P99 below the baseline's capacity | `DURATION=90s RATE=15 WINDOW=2m make bench-latency` |
| `latency-saturation-*.json` | P50 / P95 above the baseline's capacity | derived from the `dedup-*` run's Prometheus histograms, read per lane at each lane's end |
| `chaos-*.json` | calls issued into a downstream forced to 60% failure | `PRE_DURATION=30s SPIKE_DURATION=45s POST_DURATION=30s RATE=10 BURST_RATE=40 make bench-chaos` |
| `throughput-*.json` | sustained ingest | `DURATION=3m RATE=25 WINDOW=4m make bench-throughput` |
| `uptime-*.json` | consumer fleet availability | `DURATION=3m INTERVAL=2 make bench-uptime` |
| `docker-stats-*.csv`, `../cost-model.csv` | median CPU / memory per replica | `./scripts/capture-docker-stats.sh <lane> 95 <regex>`, then `make cost-model` |

`*-load.json` files are the load generator's own summary for one lane of a run:
events produced, duplicates injected, and the achieved rate.

The scopes are short by design. They are what one sitting at a keyboard can
run and check, not soak tests, and the README states the scope beside every
number rather than extrapolating past it.
