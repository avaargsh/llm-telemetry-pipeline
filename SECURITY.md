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


## Go vulnerability scan

The proposed `go-vulnerability-scan` workflow scans the exact checked-out
commit using the Go version declared in `go.mod` and a separately installed,
version-pinned `govulncheck` v1.10.0. It runs on pull requests, pushes to
`master`, manual dispatch, and weekly after the workflow is merged.
It uses the official Go vulnerability database; only module queries are
sent to that database, not repository source code. See
[Go vulnerability management](https://go.dev/doc/security/vuln/) and
[scanner documentation](https://pkg.go.dev/golang.org/x/vuln@v1.10.0/cmd/govulncheck).

To reproduce with the configured Go toolchain:

```sh
GOTOOLCHAIN=local go install golang.org/x/vuln/cmd/govulncheck@v1.10.0
GOTOOLCHAIN=local "$(go env GOPATH)/bin/govulncheck" -show verbose ./...
```

The workflow preserves the exact commit, run identity, scan timestamp,
compiler/scanner build information and text output. A nonzero scanner or
log-writer exit is a failed job; findings must be triaged against the
affected version and reachable call paths, then fixed and retested.
Do not use a JSON-only scan's zero exit code as a clean security verdict.
A source scan is not comprehensive image, OS-package, secret, license,
runtime-configuration or production-security verification. A green result
reflects the database and build configuration at scan time only.
