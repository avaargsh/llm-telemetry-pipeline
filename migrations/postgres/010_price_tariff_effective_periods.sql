-- M13.9c: immutable effective-time publication of registered tariff payloads.
-- btree_gist is required for a database-enforced per-project range exclusion.
CREATE EXTENSION IF NOT EXISTS btree_gist;

BEGIN;

CREATE TABLE public.price_tariff_effective_periods (
    publication_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id text COLLATE "C" NOT NULL,
    project_id text COLLATE "C" NOT NULL,
    price_version text COLLATE "C" NOT NULL,
    price_fingerprint text COLLATE "C" NOT NULL
        CHECK (octet_length(price_fingerprint) = 71
               AND price_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
    effective_from timestamptz NOT NULL,
    effective_to timestamptz NOT NULL,
    effective_period tstzrange GENERATED ALWAYS AS
        (tstzrange(effective_from, effective_to, '[)')) STORED,
    publication_reason text NOT NULL
        CHECK (length(btrim(publication_reason)) BETWEEN 1 AND 1024),
    source_reference text NOT NULL DEFAULT ''
        CHECK (length(source_reference) <= 2048),
    published_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    published_by text NOT NULL DEFAULT session_user,
    CHECK (isfinite(effective_from) AND isfinite(effective_to)),
    CHECK (effective_from < effective_to),
    CONSTRAINT price_tariff_effective_period_no_overlap
        EXCLUDE USING gist (
            tenant_id WITH =,
            project_id WITH =,
            effective_period WITH &&
        )
);

-- No FK is used so the existing append-only payload table retains its
-- statement-level TRUNCATE guard. This trigger enforces the same binding.
CREATE FUNCTION public.validate_price_tariff_effective_period()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    registered_fingerprint text;
BEGIN
    SELECT price_fingerprint
      INTO registered_fingerprint
      FROM public.price_tariff_payloads
     WHERE tenant_id = NEW.tenant_id
       AND project_id = NEW.project_id
       AND price_version = NEW.price_version
     FOR KEY SHARE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'registered price tariff payload missing'
            USING ERRCODE = '23503';
    END IF;
    IF registered_fingerprint IS DISTINCT FROM NEW.price_fingerprint THEN
        RAISE EXCEPTION 'effective period price fingerprint mismatch'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER price_tariff_effective_period_validate
BEFORE INSERT ON public.price_tariff_effective_periods
FOR EACH ROW
EXECUTE FUNCTION public.validate_price_tariff_effective_period();

CREATE FUNCTION public.reject_price_tariff_effective_period_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION 'price tariff effective periods are append-only'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER price_tariff_effective_period_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON public.price_tariff_effective_periods
FOR EACH STATEMENT
EXECUTE FUNCTION public.reject_price_tariff_effective_period_mutation();

CREATE VIEW public.usage_query_price_periods WITH (security_barrier = true) AS
SELECT p.publication_id, p.tenant_id, p.project_id, p.price_version,
       p.price_fingerprint, p.effective_from, p.effective_to,
       p.publication_reason, p.source_reference, p.published_at, p.published_by
FROM public.price_tariff_effective_periods p
JOIN public.usage_query_scope_bindings b
  ON b.tenant_id = p.tenant_id AND b.project_id = p.project_id
WHERE b.login_role = session_user;

REVOKE ALL ON public.price_tariff_effective_periods FROM PUBLIC, llm_telemetry_usage_query_reader;
REVOKE ALL ON FUNCTION public.validate_price_tariff_effective_period() FROM PUBLIC, llm_telemetry_usage_query_reader;
REVOKE ALL ON FUNCTION public.reject_price_tariff_effective_period_mutation() FROM PUBLIC, llm_telemetry_usage_query_reader;
REVOKE ALL ON public.usage_query_price_periods FROM PUBLIC, llm_telemetry_usage_query_reader;
GRANT SELECT ON public.usage_query_price_periods TO llm_telemetry_usage_query_reader;

COMMENT ON TABLE public.price_tariff_effective_periods IS
    'M13.9c immutable half-open tariff effective periods; no provider authenticity or invoice claim';

COMMIT;
