.PHONY: test test-race sample infra-up infra-down kafka-topics

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
	./scripts/bootstrap-topics.sh
