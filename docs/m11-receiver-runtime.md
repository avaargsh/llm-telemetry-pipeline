# M11.7: independent durable observation receiver runtime

M11.7 extends the M11.6 durable sender/receiver contract without changing its
at-least-once delivery semantics. The receiver is a separately deployable Go
service; it does not own the usage Ledger, Outbox, Kafka or ClickHouse path.

## Runtime contract

- `cmd/genai-observation-receiver` serves authenticated OTLP/HTTP JSON at
  `POST /v1/traces`.
- A strict JSON credentials file maps bearer tokens to tenant IDs. Unknown
  fields, trailing JSON, duplicate tokens, invalid tenant IDs, weak tokens,
  oversized files and permissive file modes are rejected.
- Tenant scope comes only from the authenticated bearer credential. Payload
  tenant attributes are not trusted.
- Remote listeners require a certificate/key pair and TLS 1.2 or later.
  Plain HTTP is allowed only for loopback development and tests.
- The existing durable admission transaction persists the observation and
  idempotency receipts before returning 2xx. A lost ACK is retried with the
  same key. The guarantee is at-least-once plus bounded receiver-side
  deduplication, not end-to-end exactly-once.
- Request concurrency is bounded by the existing admission handler. HTTP
  read/write/idle/header limits are set by the runtime. The OTLP handler keeps
  its existing request-body bound.
- `/healthz`, `/readyz` and `/metrics` are exposed on the receiver listener.
  Metrics use fixed names and never label by tenant, trace, request, model or
  credential.

## Database roles and migration order

Apply migrations in lexical order, including
`003_genai_observation_delivery_receipts.sql` before deploying M11.6/M11.7
durable writers, receivers or workers. Then apply
`004_genai_observation_runtime_roles.sql` followed by
`005_genai_observation_receipt_expiry_atomic.sql` for M11.8.
Migration 005 makes receipt expiry and both persisted receipt counters atomic;
it does not reconcile pre-existing drift. See the [M11.8 migration contract](m11-durable-observation-queue.md).

Migration 004 creates two `NOLOGIN` capability roles:

- `llm_telemetry_observation_receiver`: SELECT/INSERT observations, receipt
  maintenance and counter updates. It cannot UPDATE/DELETE admitted
  observations or write Ledger/Outbox.
- `llm_telemetry_observation_worker`: SELECT/UPDATE/DELETE observations and
  queue-depth updates only. It cannot create or delete receiver receipts and
  cannot write Ledger/Outbox.

Both roles are denied direct UPDATE of `capacity_limit`. The receiver may call
the SECURITY DEFINER `public.ensure_genai_observation_capacity(bigint)`
initializer, which fixes capacity once and rejects mismatched replicas. The
function pins its search_path; durable store SQL is schema-qualified, and
runtime connections set `search_path TO pg_catalog, public`.

Create distinct LOGIN principals outside this migration and grant only the
matching capability role. Set each login credential through the database
secret-management process rather than in migration files:

```sql
CREATE ROLE receiver_login LOGIN;
GRANT llm_telemetry_observation_receiver TO receiver_login;

CREATE ROLE worker_login LOGIN;
GRANT llm_telemetry_observation_worker TO worker_login;
```

Use your secret manager for passwords and DSNs. The runtime performs
`SET ROLE` on every newly established PostgreSQL connection; it fails closed
if the login principal cannot assume the required role. Do not reuse one login
principal for both services in production.

## Configuration

Required receiver settings:

- `GENAI_OBSERVATION_DATABASE_URL`: DSN for a receiver-specific login
  principal that is a member of the receiver capability role.
- `GENAI_OBSERVATION_CREDENTIALS_FILE`: strict JSON credential mapping file.

Optional settings:

- `GENAI_OBSERVATION_LISTEN_ADDR`: defaults to `:4318`.
- `GENAI_OBSERVATION_TLS_CERT_FILE` and
  `GENAI_OBSERVATION_TLS_KEY_FILE`: required together for non-loopback binds.
- `GENAI_OBSERVATION_QUEUE_CAPACITY`: defaults to 10000; must match all
  durable writers and cannot change after initialization.
- `GENAI_OBSERVATION_MAX_CONCURRENT`: defaults to 8, range 1..64.
- `GENAI_OBSERVATION_HTTP_TIMEOUT`: defaults to 15s, range 1s..2m.

A credentials file contains a JSON array with only `token` and `tenant_id`
fields. Mount it read-only with permissions such as 0400 or 0440. Never commit
real credentials.

## Deployment

- `deployments/Dockerfile.genai-observation-receiver` builds a static binary
  and runs it as UID/GID 10001 in a minimal non-root image.
- `deployments/k8s/genai-observation-receiver.yaml` is an example Deployment,
  ClusterIP Service and NetworkPolicy. Replace the image placeholder with an
  immutable image tag or digest. Adjust namespace and PostgreSQL pod selectors
  to match the target cluster.
- The manifest disables service-account token mounting, uses read-only Secret
  mounts, a read-only root filesystem, dropped Linux capabilities,
  non-root execution, probes and restricted network policy.

The manifest is a deployment template, not evidence of a production rollout.
A successful local Docker build or manifest validation is not a production
Kubernetes verification.

## Queue and delivery telemetry

Receiver metrics:

- `llm_telemetry_genai_receiver_admission_requests_total`
- `llm_telemetry_genai_receiver_admission_accepted_total`
- `llm_telemetry_genai_receiver_admission_rejected_total`
- `llm_telemetry_genai_receiver_admission_failed_total`
- `llm_telemetry_genai_receiver_admission_duration_seconds_count` and
  `_sum`

Worker metrics include export latency count/sum, pending/terminal depth,
configured capacity, oldest pending age and last successful delivery timestamp.
All metric dimensions are bounded; no tenant or request identifiers are labels.

Initial operational guardrails are included in
`deployments/prometheus/alert-rules.yml`: queue occupancy above 50%/80%,
oldest pending age above 5/15 minutes, terminal rows, stalled successful
delivery, retry ratio above 10%/25%, and receiver 5xx ratio above 5%/20%.
These are starting Warning/Critical thresholds for validation, not production
SLOs; tune them only after collecting representative load and recovery data.
The local Prometheus profile scrapes the worker when its Compose overlay is
running. In Kubernetes, the receiver pod exposes standard Prometheus scrape
annotations; ensure the cluster Prometheus discovery policy honors them.

## Verification

```sh
go test -race ./...
go vet ./...
go build ./...
docker build -f deployments/Dockerfile.genai-observation-receiver .
docker build -f deployments/Dockerfile.genai-observation-worker .
```

PostgreSQL integration tests run with `TEST_POSTGRES_DSN` and verify database
role grants, queue statistics, idempotency, and recovery. The M11.7 fault
workflow also creates a second disposable PostgreSQL database, applies the
same migrations, assumes the receiver capability role, and proves sender
lease recovery and receiver deduplication across independent databases.
The migration is automatically applied by the repository's disposable Compose
PostgreSQL environment on a fresh volume. Existing databases must apply
all outstanding migrations through 005 explicitly through an approved migration
process before deploying the current binaries. An existing Compose volume is
not upgraded by the initialization mount.

No production availability, latency SLO, RPO or RTO is claimed by these tests.
