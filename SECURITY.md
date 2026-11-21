# Security policy and responsible reporting

## Supported versions

This repository currently provides **experimental, disposable infrastructure
and fault-lab examples**, not a maintained production release. There is no
published production support or security-patch SLA.

## Reporting

For non-sensitive bugs, open a GitHub issue with a minimal sanitized
reproduction. **Do not put tokens, real tenant data, internal hostnames,
customer assets, exploit payloads or private credentials in public issues.**

If GitHub private vulnerability reporting is enabled for this repository,
use the repository's private *Report a vulnerability* workflow. If it is not
enabled, avoid posting sensitive details publicly; contact the maintainer
through a private channel you have independently confirmed. No security
contact address or response-time guarantee is invented here.

## Deployment scope

- `localdev-only` passwords and plaintext local listeners in Docker Compose
  are restricted to disposable loopback experiments, not production.
- Production use would require independently reviewed TLS, IAM/secret
  lifecycle, network isolation, retention/backup, recovery, monitoring,
  on-call procedures and deployment threat modeling.
- Kafka/ClickHouse analytical writes may be replayed; only the PostgreSQL
  Ledger/Outbox transaction is authoritative for usage.
- No production exactly-once, all-fault RPO=0, automatic HA or production
  performance SLO is claimed.

See [OSS readiness](docs/oss-readiness.md) for outstanding ownership,
license and publication requirements.
