.PHONY: help tidy build test stack-up stack-down stack-logs \
	bench-throughput bench-latency bench-dedup bench-chaos bench-uptime \
	bench-all clean cost-model

help:
	@echo "Targets:"
	@echo "  tidy             - go mod tidy"
	@echo "  build            - build all binaries to bin/"
	@echo "  test             - go test -race ./..."
	@echo "  stack-up         - bring up Kafka + Redis + Prometheus + workers"
	@echo "  stack-down       - tear down stack"
	@echo "  stack-logs       - tail stack logs"
	@echo "  bench-throughput - sustained throughput run (set DURATION, default 24h)"
	@echo "  bench-latency    - P95/P99 delivery latency, baseline vs optimized"
	@echo "  bench-dedup      - duplicate downstream deliveries, baseline vs optimized"
	@echo "  bench-chaos      - 5xx and breaker behaviour during a spike with chaos on"
	@echo "  bench-uptime     - availability sampling of the consumer fleet"
	@echo "  bench-all        - run every benchmark and write bench/reports/"
	@echo "  cost-model       - project captured docker stats onto EC2 list prices"

tidy:
	go mod tidy

build:
	go build -o bin/producer ./cmd/producer
	go build -o bin/consumer ./cmd/consumer
	go build -o bin/baseline-consumer ./cmd/baseline-consumer
	go build -o bin/downstream ./cmd/downstream
	go build -o bin/loadgen ./cmd/loadgen

test:
	go test -race ./...

stack-up:
	cd deploy && docker compose --profile baseline up -d --build

stack-down:
	cd deploy && docker compose --profile baseline --profile loadgen down -v

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
