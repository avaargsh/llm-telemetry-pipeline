# M13.8a: persistent price-version binding registry

The administrator-owned PostgreSQL registry persists the M13.6 canonical
price content digest against the exact (tenant, project, version) identity.
It is the first storage slice, not a complete price catalog. It does not
store rates or their effective intervals, automatically authenticate a
tariff, or modify usage ledger/outbox data.

## Install

A fresh disposable PostgreSQL volume loads migration 008 after 007.
For existing installations, review the existing migration state and
privileges, take an approved backup, then apply 008 once using the trusted
migration owner with psql ON_ERROR_STOP. Do not reset or delete existing
volumes to upgrade.

## Register and read

Validate the original price table using the M13.6 calculation before
registration. For example, an administrator can register the fictional
example in m13-cost-estimate.md:

```sql
INSERT INTO public.price_version_registry
    (tenant_id, project_id, price_version, price_fingerprint)
VALUES
    ('example-tenant', 'example-project', 'example-v1',
     'sha256:433d2e0b10d8481699f2a2353f26bef71f057dd2cb08dc7069c026c0d7cc3cd5');
```

No runtime writer grant is introduced. Duplicate registrations (including
identical retries) fail with unique violation; clients must not treat an
arbitrary conflict as successful registration. A corrected price must
receive a new version. Ordinary UPDATE, DELETE and TRUNCATE fail, even
when executed by the owner. The owner/superuser can still alter/drop
objects and must be trusted. Database constraints check shape and exact
bytewise identity; they do not independently recompute digests or replace
the application's Unicode/decimal validation.

A dedicated existing usage-query reader login with a matching administrator
scope binding can read its own rows from:

```sql
SELECT price_version, price_fingerprint, registered_at
FROM public.usage_query_price_versions
ORDER BY price_version;
```

The base table and mutation function are not granted to query readers.
Unbound and revoked logins see no rows. Do not grant broader inherited
roles to these logins. The registered_at timestamp is registration time,
not an effective price date.

## Verification

The existing M2 PostgreSQL integration job runs the new
TestPriceVersionRegistryConstraintsAndScope test against migrations
loaded in disposable PostgreSQL. It checks exact version identity,
cross-scope isolation, duplicates, malformed digest and version length,
mutation rejection, read-only scope, and revocation. All test fixtures
are rolled back. Session authorization exercises the SQL identity
boundary, not a new network-authentication test; the separate existing
M12 test exercises real scoped logins.

## Remaining work

The existing CLI still uses an optional caller-supplied binding file.
No runtime registry lookup, complete immutable rate payload, effective
interval schedule, trusted tariff ingestion, or authoritative billing
report is implemented by this slice. Do not enable a FinOps dashboard
based solely on the binding table.
