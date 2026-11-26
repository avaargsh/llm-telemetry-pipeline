-- M13.9b: immutable canonical tariff payloads bound to M13.8 registry rows.
-- The migration owner/trusted provisioning service is the only writer.
BEGIN;

CREATE TABLE public.price_tariff_payloads (
    tenant_id text COLLATE "C" NOT NULL,
    project_id text COLLATE "C" NOT NULL,
    price_version text COLLATE "C" NOT NULL,
    price_fingerprint text COLLATE "C" NOT NULL
        CHECK (octet_length(price_fingerprint) = 71
               AND price_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
    payload_schema_version smallint NOT NULL DEFAULT 1
        CHECK (payload_schema_version = 1),
    currency text COLLATE "C" NOT NULL CHECK (currency = 'USD'),
    rates jsonb NOT NULL
        CHECK (CASE WHEN jsonb_typeof(rates) = 'array'
                    THEN jsonb_array_length(rates) <= 1000
                    ELSE false END),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (tenant_id, project_id, price_version)
);

-- The Go writer recomputes SHA-256. PostgreSQL enforces that the payload
-- references the same immutable registry binding and canonical JSON shape.
CREATE FUNCTION public.validate_price_tariff_payload()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    registered_fingerprint text;
    item jsonb;
    model_name text;
    previous_model text;
BEGIN
    SELECT price_fingerprint
      INTO registered_fingerprint
      FROM public.price_version_registry
     WHERE tenant_id = NEW.tenant_id
       AND project_id = NEW.project_id
       AND price_version = NEW.price_version
     FOR KEY SHARE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'price version registry binding missing'
            USING ERRCODE = '23503';
    END IF;
    IF registered_fingerprint IS DISTINCT FROM NEW.price_fingerprint THEN
        RAISE EXCEPTION 'price tariff payload fingerprint mismatch'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.payload_schema_version <> 1 OR NEW.currency <> 'USD' THEN
        RAISE EXCEPTION 'unsupported price tariff payload schema or currency'
            USING ERRCODE = '23514';
    END IF;
    IF jsonb_typeof(NEW.rates) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'invalid price tariff rates array'
            USING ERRCODE = '23514';
    END IF;
    IF jsonb_array_length(NEW.rates) > 1000 THEN
        RAISE EXCEPTION 'too many price tariff rates'
            USING ERRCODE = '23514';
    END IF;

    FOR item IN SELECT value FROM jsonb_array_elements(NEW.rates) LOOP
        IF jsonb_typeof(item) IS DISTINCT FROM 'object' THEN
            RAISE EXCEPTION 'invalid price tariff rate object'
                USING ERRCODE = '23514';
        END IF;
        IF NOT (item ?& ARRAY['model', 'input_per_million', 'output_per_million'])
           OR item - ARRAY['model', 'input_per_million', 'output_per_million'] <> '{}'::jsonb
           OR jsonb_typeof(item->'model') IS DISTINCT FROM 'string'
           OR jsonb_typeof(item->'input_per_million') IS DISTINCT FROM 'string'
           OR jsonb_typeof(item->'output_per_million') IS DISTINCT FROM 'string' THEN
            RAISE EXCEPTION 'invalid price tariff rate object'
                USING ERRCODE = '23514';
        END IF;

        model_name := item->>'model';
        IF octet_length(model_name) NOT BETWEEN 1 AND 512
           OR model_name <> btrim(model_name)
           OR model_name ~ '^[[:space:]]|[[:space:]]$'
           OR (previous_model IS NOT NULL AND model_name COLLATE "C" <= previous_model COLLATE "C") THEN
            RAISE EXCEPTION 'price tariff models must be unique and byte-sorted'
                USING ERRCODE = '23514';
        END IF;
        IF (item->>'input_per_million') !~ '^[0-9]+[.][0-9][0-9][0-9][0-9][0-9][0-9]$'
           OR (item->>'output_per_million') !~ '^[0-9]+[.][0-9][0-9][0-9][0-9][0-9][0-9]$'
           OR length(split_part(item->>'input_per_million', '.', 1)) > 18
           OR length(split_part(item->>'output_per_million', '.', 1)) > 18
           OR (length(split_part(item->>'input_per_million', '.', 1)) > 1 AND left(item->>'input_per_million', 1) = '0')
           OR (length(split_part(item->>'output_per_million', '.', 1)) > 1 AND left(item->>'output_per_million', 1) = '0')
           OR (item->>'input_per_million') ~ '[[:space:]]'
           OR (item->>'output_per_million') ~ '[[:space:]]' THEN
            RAISE EXCEPTION 'price tariff rates must be canonical nonnegative USD decimals'
                USING ERRCODE = '23514';
        END IF;
        previous_model := model_name;
    END LOOP;

    RETURN NEW;
END;
$$;

CREATE TRIGGER price_tariff_payload_validate
BEFORE INSERT ON public.price_tariff_payloads
FOR EACH ROW
EXECUTE FUNCTION public.validate_price_tariff_payload();

CREATE FUNCTION public.reject_price_tariff_payload_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION 'price tariff payloads are append-only'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER price_tariff_payload_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON public.price_tariff_payloads
FOR EACH STATEMENT
EXECUTE FUNCTION public.reject_price_tariff_payload_mutation();

CREATE VIEW public.usage_query_price_tariffs WITH (security_barrier = true) AS
SELECT p.tenant_id, p.project_id, p.price_version, p.price_fingerprint,
       p.payload_schema_version, p.currency, p.rates, p.created_at
FROM public.price_tariff_payloads p
JOIN public.usage_query_scope_bindings b
  ON b.tenant_id = p.tenant_id AND b.project_id = p.project_id
WHERE b.login_role = session_user;

REVOKE ALL ON public.price_tariff_payloads FROM PUBLIC, llm_telemetry_usage_query_reader;
REVOKE ALL ON FUNCTION public.validate_price_tariff_payload() FROM PUBLIC, llm_telemetry_usage_query_reader;
REVOKE ALL ON FUNCTION public.reject_price_tariff_payload_mutation() FROM PUBLIC, llm_telemetry_usage_query_reader;
REVOKE ALL ON public.usage_query_price_tariffs FROM PUBLIC, llm_telemetry_usage_query_reader;
GRANT SELECT ON public.usage_query_price_tariffs TO llm_telemetry_usage_query_reader;

COMMENT ON TABLE public.price_tariff_payloads IS
    'M13.9b immutable canonical rate payloads; only trusted provisioning writes; no provider authenticity claim';

COMMIT;
