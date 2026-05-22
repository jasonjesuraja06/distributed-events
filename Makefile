.PHONY: help tidy build stack-up stack-down stack-logs \
	bench-throughput bench-latency bench-dedup bench-chaos bench-uptime \
	bench-all clean cost-model

help:
	@echo "Targets:"
	@echo "  tidy             - go mod tidy"
	@echo "  build            - build all binaries"
	@echo "  stack-up         - bring up Kafka + Redis + Prometheus + workers"
	@echo "  stack-down       - tear down stack"
	@echo "  stack-logs       - tail stack logs"
	@echo "  bench-throughput - sustained throughput soak (24h target, configurable)"
	@echo "  bench-latency    - P95 latency before/after (naive baseline vs optimized)"
	@echo "  bench-dedup      - duplicate-reduction benchmark"
	@echo "  bench-chaos      - 5xx-during-spike benchmark (chaos)"
	@echo "  bench-uptime     - 99.9% availability uptime probe"
	@echo "  bench-all        - run every benchmark and write reports/"
	@echo "  cost-model       - generate AWS cost-model spreadsheet"

tidy:
	go mod tidy

build:
	go build -o bin/producer ./cmd/producer
	go build -o bin/consumer ./cmd/consumer
	go build -o bin/baseline-consumer ./cmd/baseline-consumer
	go build -o bin/loadgen ./cmd/loadgen

stack-up:
	cd deploy && docker compose up -d --build

stack-down:
	cd deploy && docker compose down -v

stack-logs:
	cd deploy && docker compose logs -f --tail=100

bench-throughput:
	./scripts/bench-throughput.sh

bench-latency:
	./scripts/bench-latency.sh

bench-dedup:
	./scripts/bench-dedup.sh

bench-chaos:
	./scripts/bench-chaos.sh

bench-uptime:
	./scripts/bench-uptime.sh

bench-all: bench-latency bench-dedup bench-chaos bench-uptime bench-throughput

cost-model:
	python3 scripts/cost_model.py > bench/cost-model.csv

clean:
	rm -rf bin/
	rm -rf bench/reports/
