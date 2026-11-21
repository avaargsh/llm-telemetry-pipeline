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

.PHONY: lag-exporter m3e-lag-smoke

# Read-only Kafka group observer; requires an established committed group
# and LogAppendTime topic (see docs/m3e-lag-monitor.md).
lag-exporter:
	go run ./cmd/lag-exporter

# Disposable Kafka/ClickHouse environment, built-in failure injection.
m3e-lag-smoke:
	bash scripts/m3e-lag-throttle-smoke.sh

.PHONY: m4a-alert-smoke

# Disposable Prometheus/Alertmanager webhook-delivery fault lab.
m4a-alert-smoke:
	bash scripts/m4a-alert-delivery-smoke.sh

.PHONY: m4-alert-e2e

# Live Kafka → exporter → Prometheus → Alertmanager → disposable webhook;
# separate from the synthetic metric M4a delivery-layer test above.
m4-alert-e2e:
	bash scripts/m4-alert-delivery-smoke.sh

.PHONY: m5a-http-recovery-smoke

# Disposable full-path test: HTTP-authenticated PG ingestion while Kafka is
# down, followed by Relay/ClickHouse recovery and strict reconciliation.
m5a-http-recovery-smoke:
	bash scripts/m5a-http-recovery-smoke.sh
