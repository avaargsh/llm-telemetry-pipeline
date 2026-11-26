# M13.9a: canonical tariff payload preparation

This first catalog slice is a pure Go preparation function. It does not
write PostgreSQL or change existing estimates.

CanonicalTariffPayload validates a complete PriceTable with the existing
M13.6 fingerprint algorithm, rejects null rate arrays and invalid UTF-8
or NUL identities, preserves exact version/model strings, sorts models
by byte order and normalizes decimal rates to six fractional digits.
The result is a detached copy with the same canonical content digest.
An explicit empty rates array is distinct from missing/null input.

## Proposed storage boundaries (not implemented)

- The existing scoped registry remains authoritative for version/content
  binding. Persist one complete validated payload per tenant/project/version;
  verify its canonical digest against the registry on write and read.
  Do not use a mutable JSON field or permit ordinary UPDATE/DELETE.
- Publish effective half-open UTC intervals separately from content.
  Use explicit finite microsecond-aligned bounds and a database-enforced
  non-overlap invariant for concurrent registrations. A gap in a requested
  reporting interval must fail closed. Registration time is not effective time.
- Preserve the existing dedicated project login and scope checks.
  Read tariff payloads, effective periods and usage in one consistent
  scoped transaction. Keep legacy offline/unverified modes explicit.
- Administrator-provided tariffs are not provider-signed evidence.
  Corrections require new immutable version/publication identities and
  an auditable process; no silent historical rewrite.
- Do not migrate existing fingerprint-only rows by inventing missing rates
  or effective periods. Treat missing evidence as unavailable.

M13.9b below establishes immutable payload storage and scoped PostgreSQL
integration coverage. Effective periods, publication approvals and provenance
remain separate work before the reporting API can use the complete catalog.
No production billing, invoice, or price provenance claim is made.

## Verification

Run go test ./internal/finops -run CanonicalTariffPayload -v and the
repository's full CI suite. Tests cover deterministic normalization,
content fingerprint compatibility, exact version identity, detached data,
invalid Unicode/NUL, invalid rates, duplicates and empty versus null rates.

## M13.9b: immutable PostgreSQL payload storage

Migration `009_price_tariff_payloads.sql` adds one immutable row per
`(tenant_id, project_id, price_version)`. It stores schema version 1,
currency, the canonical sorted rates array as JSONB, and the matching
M13.8 registry fingerprint. An insert trigger requires the registry binding
and rechecks the fingerprint and validates exact rate-object
keys, model ordering/uniqueness, and six-place canonical decimal strings.
UPDATE, DELETE and TRUNCATE are rejected by an append-only trigger.

The base table has no PUBLIC or runtime-reader grants. A
`security_barrier` view joins the existing database-authenticated scope
binding and is readable only by the existing non-login query-reader role.
The Go reader rechecks direct base-table privileges and recomputes the
canonical fingerprint after loading the JSONB payload.

The trusted provisioning function is:

```go
payload, err := finops.RegisterTariffPayload(ctx, ownerDB, tenant, project, table)
```

It requires a pre-existing immutable registry binding, checks its exact
fingerprint, inserts once, and permits idempotent retries only when the
stored currency, fingerprint and complete rates array are identical.
A different payload for the same version is rejected. The owner connection
is a provisioning boundary; never expose it to the runtime query API.

Read a payload through the existing project-scoped reader:

```go
payload, err := finops.LoadRegisteredTariff(ctx, scopedReader, tenant, project, version)
```

This returns only after the reader's database scope and base-table privilege
checks pass and the stored rates recompute to the registered fingerprint.
No row, invalid schema or digest mismatch fails closed.

### Verification and limits

The PostgreSQL integration test provisions a unique disposable login and
scope, registers and reads a payload, checks idempotence and version mismatch,
asserts database-level fingerprint and canonical-shape rejection, verifies
UPDATE/DELETE/TRUNCATE refusal, confirms direct base-table denial and
cross-project view isolation, and tests revoked scope.

This catalog stores price content, not its source, effective time, approval,
signature, or provider authenticity. The effective-period schedule and
time-overlap constraints are still separate work. It is not an invoice or
automatic billing authority.

## M13.9c: immutable effective-time publications

Migration `010_price_tariff_effective_periods.sql` adds immutable publication
rows that bind a tenant/project/version to a finite UTC half-open interval
`[effective_from, effective_to)`, the exact payload fingerprint, a required
change reason, optional source reference, database publication timestamp,
and `session_user` publisher identity.

A private per-tenant/project lock row and a BEFORE INSERT trigger
serialize overlap checks. The trigger locks the scope row until transaction
end, then checks for an intersecting interval under READ COMMITTED; concurrent
writers cannot both publish overlapping periods. Adjacent periods are valid;
gaps are deliberately representable and must be rejected by a report that
requires complete coverage. This uses core PostgreSQL features and has no
extension prerequisite.

The insert trigger verifies that the referenced immutable payload exists
and that its fingerprint matches. No FK is used, preserving the payload
table's existing append-only/TRUNCATE guard contract. UPDATE, DELETE and
TRUNCATE on publications are rejected. Runtime readers receive only the
security-barrier scope view and are checked for direct base-table privileges.

Trusted provisioning:

```go
published, err := finops.PublishTariffPeriod(ctx, ownerDB, finops.PricePeriodPublication{
    TenantID: tenant,
    ProjectID: project,
    PriceVersion: version,
    EffectiveFrom: from,
    EffectiveTo: to,
    Reason: "approved rate change",
    SourceReference: "change-record-123",
})
```

Scoped reads use `ledger.ScopedUsageReader.PriceEffectivePeriods` and return
only periods intersecting a bounded query interval. The per-scope row lock
and READ COMMITTED overlap trigger are the concurrency authority; an overlap
is returned as `finops.ErrPricePeriodOverlap`. The database rejects writes
at other isolation levels. The publication API rejects zero, non-finite,
sub-microsecond, reversed bounds and blank reasons before writing.

The PostgreSQL integration test covers adjacent intervals, overlap rejection,
separate-project isolation, concurrent overlapping publications, missing
payloads, invalid bounds, immutable writes, scope-view isolation, direct
base-table denial and scope revocation.

The publisher identity is the database `session_user`; if a shared owner
credential is used, it identifies that credential rather than a human
approver. This slice does not implement approval workflows, source signatures,
provider authenticity, schedule-gap completeness, invoices or billing.
M13.9d must load periods and payloads with usage in one consistent scoped
snapshot before the verified historical CLI can rely on the catalog.
