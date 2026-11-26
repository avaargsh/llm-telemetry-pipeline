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

The next slice must establish database schema and integration tests for
identity, immutability, concurrency, cross-scope isolation and rollback
before the reporting API can use this catalog. No production billing,
invoice, or price provenance claim is made.

## Verification

Run go test ./internal/finops -run CanonicalTariffPayload -v and the
repository's full CI suite. Tests cover deterministic normalization,
content fingerprint compatibility, exact version identity, detached data,
invalid Unicode/NUL, invalid rates, duplicates and empty versus null rates.

## M13.9b: immutable PostgreSQL payload storage

Migration `009_price_tariff_payloads.sql` adds one immutable row per
`(tenant_id, project_id, price_version)`. It stores schema version 1,
currency, the canonical sorted rates array as JSONB, and the matching
M13.8 registry fingerprint. A foreign key requires the registry binding;
an insert trigger rechecks the fingerprint and validates exact rate-object
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
