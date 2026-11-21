# M5 ClickHouse asynchronous INSERT ACK safety boundary

## Why it matters

PostgreSQL usage_ledger remains the single **billing authority**. The Kafka consumer only commits offsets after confirmed ClickHouse (analytical projection) success or a confirmed DLQ write. Kafka replays may still produce physical duplicates.

ClickHouse's asynchronous INSERT settings do **not** all have the same acknowledgement semantics:

- `async_insert=0` — synchronous INSERT, used by the production HTTPWriter. Set `wait_end_of_query=1` and `wait_for_async_insert=1` explicitly; endpoint configuration cannot silently override them.
- `async_insert=1&wait_for_async_insert=1` — server buffers the INSERT but sends a successful ACK after the buffer flush completes. A successful response is followed by a query-visible row in this test.
- `async_insert=1&wait_for_async_insert=0` — response acknowledges buffer acceptance, not successful flush or persistence. **Never** use this response alone to advance a Kafka offset, mark billing complete, or infer durability.

Source: [ClickHouse asynchronous inserts documentation](https://clickhouse.com/docs/en/optimize/asynchronous-inserts).

## Real, reproducible fault-boundary experiment

```bash
bash scripts/m5-clickhouse-async-smoke.sh
```

The script starts ClickHouse 25.8 in a named, disposable Docker Compose project, creates a unique temporary MergeTree table, then:

1. Runs a Go unit regression proving `HTTPWriter.Insert` overrides a deliberately unsafe endpoint URL carrying `async_insert=1&wait_for_async_insert=0&wait_end_of_query=0`.
2. Sends one real `async_insert=1&wait_for_async_insert=1` INSERT and **requires one query-visible row immediately after the ACK**.
3. Sends one real `async_insert=1&wait_for_async_insert=0` INSERT. Records whether the row was query-visible immediately after ACK, but **does not assert that it must be invisible**. Later polls for eventual visibility within 25 seconds.
4. Archives a JSON evidence receipt with the Git commit SHA, HTTP status, ACK time, immediate visibility and eventual visibility. It contains no passwords or customer data. Deletes the test table, project containers and volumes.

CI: `.github/workflows/m5-clickhouse-async.yml`, Artifact `m5-clickhouse-async-ack-evidence`.

## Evidence limits

- The asynchronous wait=0 branch **does not prove that data will eventually flush under all failures**. The experiment only observes one successful flush in the isolated test.
- An immediate SELECT may or may not find buffered rows, depending on flush timing. The experiment never asserts an invisible row as a deterministic prerequisite.
- This test does **not** perform crash/restart during a buffered wait=0 INSERT, simulate server power failure, establish exactly-once, or justify switching the production consumer to wait=0.
- A `wait_for_async_insert=0` HTTP 200 **must not** be used as a substitute for a confirmed ClickHouse flush ACK.
- The current production consumer remains on forced synchronous `async_insert=0`. A future opt-in `wait_for_async_insert=1` writer requires separate timeout/unknown-ACK/replay and DLQ safety testing before production use.

Merge gate: current exact PR HEAD Go/race/vet tests and real ClickHouse integration must be green; machine-readable JSON evidence must exist; M1–M5 required regression checks remain green.

## HTTP status is part of the evidence contract

The production `HTTPWriter` now accepts only **HTTP 200 OK** as a successful SQL response, not arbitrary 2xx responses. In particular, `202 Accepted` from an upstream proxy is not a ClickHouse flush acknowledgement. Additional unit regression tests reject 201, 202 and 204 and preserve the existing error/unknown-ACK fail-closed behavior. This does not eliminate uncertainty after a timeout or connection loss; the Consumer must still replay uncommitted Kafka messages.
