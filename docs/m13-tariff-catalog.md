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
