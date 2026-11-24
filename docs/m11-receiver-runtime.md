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
`004_genai_observation_runtime_roles.sql`.

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

Start with warning/critical thresholds derived from observed queue growth,
oldest pending age and retry/failure rates. Do not publish production SLOs
until sustained representative measurements exist.

## Verification

```sh
go test -race ./...
go vet ./...
go build ./...
docker build -f deployments/Dockerfile.genai-observation-receiver .
docker build -f deployments/Dockerfile.genai-observation-worker .
```

PostgreSQL integration tests run with `TEST_POSTGRES_DSN` and verify database
role grants, queue statistics, idempotency, and recovery. The migration is
automatically applied by the repository's disposable Compose PostgreSQL
environment on a fresh volume. Existing databases must apply migrations 003
and 004 explicitly before deploying new binaries.

No production availability, latency SLO, RPO or RTO is claimed by these tests.
