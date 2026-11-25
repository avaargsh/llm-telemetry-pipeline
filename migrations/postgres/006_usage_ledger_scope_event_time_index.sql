-- M12.4: shared access path for authoritative project usage queries.
-- Non-unique: preserves ledger identity and existing ingestion semantics.
-- Standard index build takes a write-conflicting lock. Existing production
-- databases require an approved maintenance window or a separately reviewed
-- concurrent build procedure; do not apply blindly to a live large ledger.
CREATE INDEX idx_usage_ledger_scope_event_time
    ON public.usage_ledger (tenant_id, project_id, event_time);
