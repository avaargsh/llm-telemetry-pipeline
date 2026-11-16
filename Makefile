.PHONY: test test-race sample infra-up infra-down kafka-topics publish consume

test:
	go test ./...

test-race:
	go test -race ./...

sample:
	go run ./cmd/event-generator --count 10 --seed sample --start 2026-11-16T11:00:00Z

infra-up:
	docker compose -f deployments/docker-compose.yml up -d

infra-down:
	docker compose -f deployments/docker-compose.yml down

kafka-topics:
	bash scripts/bootstrap-topics.sh

# M1 smoke path: run make infra-up && make kafka-topics first.
publish:
	go run ./cmd/event-generator --count 100000 --seed m1-smoke --start 2026-11-16T11:00:00Z | go run ./cmd/event-producer

consume:
	go run ./cmd/ingest-worker

.PHONY: ledger-ingest relay m2-smoke

ledger-ingest:
	go run ./cmd/event-generator --count 100 --seed local-m2 --start 2026-11-16T11:00:00Z | go run ./cmd/usage-ledger-ingest

relay:
	go run ./cmd/outbox-relay

m2-smoke:
	bash scripts/m2-smoke.sh

.PHONY: kafka-ha-smoke

# Dedicated three-broker fault lab; stops/removes its isolated volumes on exit.
kafka-ha-smoke:
	bash scripts/m3b-ha-smoke.sh

.PHONY: m3d-outage-smoke

# Stops/restarts ClickHouse in isolated disposable Docker volumes.
m3d-outage-smoke:
	bash scripts/m3d-ch-outage-smoke.sh

.PHONY: monitor m4a-smoke

# Read-only, single partition; requires newly bootstrapped LogAppendTime topic.
monitor:
	go run ./cmd/lag-monitor

# Disposable real Kafka / ClickHouse HTTP-throttle measurement.
m4a-smoke:
	bash scripts/m4a-monitor-smoke.sh
