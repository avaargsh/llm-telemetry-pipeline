-- M11.8: make receipt expiry and durable capacity counters one atomic DB operation.
-- Keep the function signature/return counts stable for older enqueue writers.
CREATE OR REPLACE FUNCTION public.expire_genai_observation_receipts()
RETURNS TABLE(expired_receipts BIGINT, expired_delivery_receipts BIGINT)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $function$
DECLARE
	current_receipt_items BIGINT;
	current_delivery_receipt_items BIGINT;
BEGIN
	-- All receipt mutations serialize on the singleton capacity row. Enqueue
	-- already holds this lock before calling us; direct receiver calls now do too.
	SELECT receipt_items, delivery_receipt_items
	INTO current_receipt_items, current_delivery_receipt_items
	FROM public.genai_observation_queue_state
	WHERE singleton = TRUE
	FOR UPDATE;

	IF NOT FOUND THEN
		RAISE EXCEPTION 'durable observation queue state is missing'
			USING ERRCODE = '55000';
	END IF;

	DELETE FROM public.genai_observation_receipts WHERE expires_at <= now();
	GET DIAGNOSTICS expired_receipts = ROW_COUNT;

	DELETE FROM public.genai_observation_delivery_receipts WHERE expires_at <= now();
	GET DIAGNOSTICS expired_delivery_receipts = ROW_COUNT;

	-- Never turn pre-existing counter drift into a negative or silently reset it.
	-- Raising aborts the statement/transaction, rolling back both DELETEs.
	IF expired_receipts > current_receipt_items
		OR expired_delivery_receipts > current_delivery_receipt_items THEN
		RAISE EXCEPTION 'durable observation receipt counter is inconsistent'
			USING ERRCODE = '23514';
	END IF;

	UPDATE public.genai_observation_queue_state
	SET receipt_items = current_receipt_items - expired_receipts,
		delivery_receipt_items = current_delivery_receipt_items - expired_delivery_receipts
	WHERE singleton = TRUE;

	IF NOT FOUND THEN
		RAISE EXCEPTION 'durable observation queue state disappeared during receipt expiry'
			USING ERRCODE = '55000';
	END IF;

	RETURN NEXT;
END
$function$;

REVOKE ALL ON FUNCTION public.expire_genai_observation_receipts() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.expire_genai_observation_receipts()
	TO llm_telemetry_observation_receiver;
