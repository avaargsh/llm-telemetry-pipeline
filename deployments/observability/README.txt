# M4a Kafka monitoring (read-only).
# After Kafka and the consumer group are running:
# go run ./cmd/lag-monitor --topic usage.critical.v1 --group telemetry-clickhouse-v1 --partition 0
# GET http://127.0.0.1:9194/metrics
# GET http://127.0.0.1:9194/status
# GET http://127.0.0.1:9194/healthz
