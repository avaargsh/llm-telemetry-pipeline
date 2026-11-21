# OSS release readiness — P0 rights and distribution gate

**Current verdict: BLOCKED / OWNER_REVIEW_REQUIRED** as of 2026-11-21.
This repository is public source, but **no top-level LICENSE is present**.
No open-source distribution permission, dependency audit, or employer/IP
provenance assessment is certified by the existing CI. Do not market this
repository as a licensed OSS release until the owner explicitly approves it.

Tracked as [Issue #26](https://github.com/avaargsh/llm-telemetry-pipeline/issues/26).

## Required owner approvals (not satisfied by code tests)

- [ ] Confirm ownership and right to license each source file, generated file,
  manifest, script, diagram, template and documentation asset. Specifically
  exclude any employer/customer proprietary source, vendor NDAs, or private
  internal materials. Do not infer ownership from a public commit history.
- [ ] Review third-party dependencies in `go.mod`/`go.sum`, imported snippets
  and Docker base images; record licenses, attribution, notice obligations and
  any incompatible terms. Version pins are not license approvals.
- [ ] Independently select an appropriate OSI-approved license after the rights
  review. The maintainer must make this choice; **no default license is
  assumed or silently generated**.
- [ ] Add the exact `LICENSE` text chosen by the owner, any required `NOTICE`
  or bundled attributions, and update the GitHub repository license metadata.
- [ ] Review distributable samples for passwords, tokens, company/customer
  names, internal addresses and false claims of production safety.
  The intentionally weak `localdev-only` Docker values are local-only lab
  examples; they must not become deployment defaults.
- [ ] Decide contribution policy, support contact, vulnerability reporting
  channel, release versioning and minimal compatibility matrix before accepting
  external code.
- [ ] Confirm clean release artifacts and an explicit exact-head verification
  matrix from a tagged commit; release bundles must exclude test secrets,
  temporary CSVs and fault-lab state.

## Engineering evidence (not a license or an SLA)

- PostgreSQL `usage_ledger` remains the **only authoritative usage/billing**
  record. Outbox and trusted ClickHouse projections are not independent bills.
- M9 #36: controlled PG17 `remote_apply` synchronous standby, manual
  promotion during 25-RPS/180s load, same Event ID retry, and exact-head
  fail-closed 4,500/4,500 reconciliation.
- M3b reconnect #37: three independent disposable combined-role Kafka KRaft
  fault trials passed after refusing a stopped old-leader connection and
  ACKing the unchanged message on the new live ISR. RF3/ISR3 post-quorum
  recovery assertion was retained.
- The CI evidence does **not** establish Patroni/DCS automatic fencing,
  arbitrary network partitions, backup/restore, production RPO/RTO,
  production percentile SLOs or Kafka exactly-once.
- [Issue #28](https://github.com/avaargsh/llm-telemetry-pipeline/issues/28)
  remains open for the distinct intermittent KRaft voter-key/ISR recovery
  mechanism despite passing recent fault trials.

## Suggested audit record for each asset

| Field | Evidence the owner should enter |
| --- | --- |
| Path or dependency | Exact relative path or module/image name |
| Origin/author | Human-confirmed authorship; do not guess |
| Ownership/license | Confirmed rights with source URL or written evidence |
| Notice obligations | Required attribution or LICENSE/NOTICE inclusion |
| Secrets/provenance decision | PASS, BLOCKED, or NEEDS_REVIEW with rationale |
| Reviewed by/date | Reviewer and date |

Any unresolved ownership or license field keeps the **release status BLOCKED**.
A passing fault test does not override this gate.
