-- M11.7 least-privilege capabilities for the independently deployed receiver and worker.
-- Apply only after 003_genai_observation_delivery_receipts.sql.
DO $roles$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'llm_telemetry_observation_receiver') THEN
		CREATE ROLE llm_telemetry_observation_receiver NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
	ELSE
		ALTER ROLE llm_telemetry_observation_receiver NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
	END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'llm_telemetry_observation_worker') THEN
		CREATE ROLE llm_telemetry_observation_worker NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
	ELSE
		ALTER ROLE llm_telemetry_observation_worker NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
	END IF;
END
$roles$;

CREATE OR REPLACE FUNCTION public.ensure_genai_observation_capacity(requested_capacity BIGINT)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $function$
DECLARE
	configured_capacity BIGINT;
BEGIN
	IF requested_capacity < 1 OR requested_capacity > 1000000 THEN
		RAISE EXCEPTION 'invalid durable observation capacity' USING ERRCODE = '22023';
	END IF;
	SELECT capacity_limit INTO configured_capacity
	FROM public.genai_observation_queue_state
	WHERE singleton = TRUE
	FOR UPDATE;
	IF NOT FOUND THEN
		RAISE EXCEPTION 'durable observation queue state is missing' USING ERRCODE = '55000';
	END IF;
	IF configured_capacity = 0 THEN
		UPDATE public.genai_observation_queue_state
		SET capacity_limit = requested_capacity
		WHERE singleton = TRUE AND capacity_limit = 0;
		SELECT capacity_limit INTO configured_capacity
		FROM public.genai_observation_queue_state WHERE singleton = TRUE;
	END IF;
	IF configured_capacity <> requested_capacity THEN
		RAISE EXCEPTION 'durable observation capacity does not match initialized value' USING ERRCODE = '22023';
	END IF;
	RETURN configured_capacity;
END
$function$;

REVOKE ALL ON FUNCTION public.ensure_genai_observation_capacity(BIGINT) FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO llm_telemetry_observation_receiver, llm_telemetry_observation_worker;

REVOKE ALL ON TABLE public.genai_observations,
	public.genai_observation_receipts,
	public.genai_observation_delivery_receipts,
	public.genai_observation_queue_state,
	public.usage_ledger,
	public.event_outbox
FROM PUBLIC, llm_telemetry_observation_receiver, llm_telemetry_observation_worker;

GRANT SELECT, INSERT ON TABLE public.genai_observations TO llm_telemetry_observation_receiver;
GRANT SELECT, INSERT, DELETE ON TABLE public.genai_observation_receipts TO llm_telemetry_observation_receiver;
GRANT SELECT, INSERT, DELETE ON TABLE public.genai_observation_delivery_receipts TO llm_telemetry_observation_receiver;
GRANT SELECT ON TABLE public.genai_observation_queue_state TO llm_telemetry_observation_receiver;
GRANT UPDATE (queue_items, receipt_items, delivery_receipt_items)
	ON TABLE public.genai_observation_queue_state TO llm_telemetry_observation_receiver;
GRANT EXECUTE ON FUNCTION public.ensure_genai_observation_capacity(BIGINT)
	TO llm_telemetry_observation_receiver;

GRANT SELECT, DELETE ON TABLE public.genai_observations TO llm_telemetry_observation_worker;
GRANT UPDATE (attempts, available_at, lease_owner, lease_until, last_error, terminal_at, terminal_reason)
    ON TABLE public.genai_observations TO llm_telemetry_observation_worker;
GRANT SELECT ON TABLE public.genai_observation_queue_state TO llm_telemetry_observation_worker;
GRANT UPDATE (queue_items) ON TABLE public.genai_observation_queue_state TO llm_telemetry_observation_worker;

REVOKE ALL ON TABLE public.usage_ledger, public.event_outbox
	FROM llm_telemetry_observation_receiver, llm_telemetry_observation_worker;

-- Supports oldest-pending-age sampling without scanning the entire queue.
CREATE INDEX IF NOT EXISTS idx_genai_observations_oldest_pending
    ON public.genai_observations (created_at, observation_id)
    WHERE terminal_at IS NULL;
