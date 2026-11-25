-- M13.8a: persistent, administrator-provisioned version/content bindings.
-- This is a binding registry, not a rate payload or effective-period catalog.
-- Migration owner and database administrators remain trusted.
BEGIN;

CREATE TABLE public.price_version_registry (
    tenant_id text COLLATE "C" NOT NULL CHECK (btrim(tenant_id) <> ''),
    project_id text COLLATE "C" NOT NULL CHECK (btrim(project_id) <> ''),
    price_version text COLLATE "C" NOT NULL
        CHECK (octet_length(price_version) BETWEEN 1 AND 128
               AND price_version ~ '[^[:space:]]'),
    price_fingerprint text COLLATE "C" NOT NULL
        CHECK (octet_length(price_fingerprint) = 71
               AND price_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
    registered_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (tenant_id, project_id, price_version)
);

-- Even privileged ordinary DML cannot rewrite registered history.
-- Trusted owners can still alter/drop objects; this is not tamper-proof
-- storage against an administrator or an out-of-band backup restore.
CREATE FUNCTION public.reject_price_version_registry_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION 'price version registry is append-only'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER price_version_registry_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON public.price_version_registry
FOR EACH STATEMENT
EXECUTE FUNCTION public.reject_price_version_registry_mutation();

-- Reuse the existing database-authenticated usage scope. The base table
-- has no runtime grants; an unbound/revoked login sees no bindings.
CREATE VIEW public.usage_query_price_versions WITH (security_barrier = true) AS
SELECT r.tenant_id, r.project_id, r.price_version, r.price_fingerprint,
       r.registered_at
FROM public.price_version_registry r
JOIN public.usage_query_scope_bindings b
  ON b.tenant_id = r.tenant_id AND b.project_id = r.project_id
WHERE b.login_role = session_user;

REVOKE ALL ON public.price_version_registry FROM PUBLIC, llm_telemetry_usage_query_reader;
REVOKE ALL ON FUNCTION public.reject_price_version_registry_mutation() FROM PUBLIC, llm_telemetry_usage_query_reader;
REVOKE ALL ON public.usage_query_price_versions FROM PUBLIC, llm_telemetry_usage_query_reader;
GRANT SELECT ON public.usage_query_price_versions TO llm_telemetry_usage_query_reader;

COMMIT;
