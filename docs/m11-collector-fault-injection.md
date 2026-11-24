# M11.5: real OpenTelemetry Collector and durable receiver fault injection

Refs #56. This is an isolated integration lab, not a production listener or delivery SLO.

## Topology

    OTLP/HTTP JSON test client
            |
            v
    OpenTelemetry Collector contrib 0.120.0
      - OTLP HTTP receiver :4318
      - deterministic sampled-out filter
      - batch processor (1 span)
      - OTLP/HTTP JSON exporter with bounded retry
            |
            v
    Fault-injection HTTP proxy :4319
            |
            v
    NewDurableOTLPHTTPHandler
            |
            v
    PostgreSQL genai_observations + 24h identity receipts

    usage_ledger + event_outbox are seeded and snapshotted independently.

The Collector runs in a disposable container with host networking in CI. The Go integration test owns the authenticated durable receiver and the fault-injection proxy. The proxy commits the first observation and deliberately returns HTTP 503 once, simulating a lost acknowledgement after commit.

## Required evidence

The exact-head CI job must prove all of the following against real PostgreSQL and a real Collector process:

1. A committed OTLP observation whose HTTP ACK is lost is retried by the Collector.
2. The durable receiver's (tenant_id, trace_id, span_id) receipt deduplicates the retry; only one queue row exists.
3. A replay after downstream ACK does not recreate the observation during the receipt retention window.
4. A full durable queue returns retryable 503; after one row is ACKed, the Collector retry is admitted and the queue drains.
5. A deterministic Collector filter drops a marked sampled-out span before export; no receiver request or durable row is created.
6. Full PostgreSQL usage_ledger and event_outbox row snapshots remain byte-for-byte identical after retry, saturation, replay, and sampling/drop paths.

The integration test writes evidence.json containing the exact tested commit and measured receiver attempt/503 counts. The workflow also archives Collector and PostgreSQL logs for 14 days.

## Run locally

Start PostgreSQL with the repository's normal migration mount, start the Collector with the same host-network configuration, then run:

    docker compose -f deployments/docker-compose.yml up -d postgres
    docker run --detach --name m11-otel-collector --network host \
      --volume "$PWD/deployments/otel-collector-m11.yaml:/etc/otelcol-contrib/config.yaml:ro" \
      otel/opentelemetry-collector-contrib:0.120.0 \
      --config=/etc/otelcol-contrib/config.yaml

    TEST_POSTGRES_DSN=postgres://telemetry:localdev-only@localhost:15432/telemetry?sslmode=disable \
    M11_COLLECTOR_ENDPOINT=http://127.0.0.1:4318 \
    go test -race -count=1 -v -run '^TestM11RealCollectorFaultInjectionAndLedgerOutboxInvariance$' ./internal/ledger

The test binds the fault-injection receiver to 127.0.0.1:4319; ensure that port is free. Stop the isolated lab with docker rm -f m11-otel-collector and docker compose -f deployments/docker-compose.yml down -v.

## Explicit limits

- The sampled-out path is a deterministic Collector filter used to exercise a sampling/drop decision; it is not a statistical sampler accuracy or tail-sampling benchmark.
- This verifies Collector-to-durable-ingress retries and the receiver's existing OTLP identity receipt. It does not claim exactly-once delivery, nor does it implement a production DurableExporter/receiver pair that propagates Delivery.IdempotencyKey.
- No raw prompts/messages/tool arguments are persisted by the receiver.
- No GenAI observation path writes, charges, or mutates the authoritative usage Ledger or Kafka Outbox.
- No production SLO, RPO/RTO, or external provider interoperability claim is made.
