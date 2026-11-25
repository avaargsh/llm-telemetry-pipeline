-- M12.5a: database-enforced read scope, independent of HTTP parameters.
-- The migration owner must remain trusted. Scope bindings are provisioned
-- by an administrator; each project service uses a dedicated login.
DO $roles$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'llm_telemetry_usage_query_reader') THEN
        CREATE ROLE llm_telemetry_usage_query_reader NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles
        WHERE rolname = 'llm_telemetry_usage_query_reader'
          AND (rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)
    ) THEN
        RAISE EXCEPTION 'unsafe existing usage query reader role';
    END IF;
END
$roles$;

CREATE TABLE public.usage_query_scope_bindings (
    login_role name PRIMARY KEY,
    tenant_id text NOT NULL CHECK (btrim(tenant_id) <> ''),
    project_id text NOT NULL CHECK (btrim(project_id) <> '')
);

-- session_user identifies the authenticated database login, not a caller-
-- supplied GUC or SET ROLE target. Unbound logins see no rows.
CREATE VIEW public.usage_query_scope WITH (security_barrier = true) AS
SELECT tenant_id, project_id
FROM public.usage_query_scope_bindings
WHERE login_role = session_user;

CREATE VIEW public.usage_query_ledger WITH (security_barrier = true) AS
SELECT l.tenant_id, l.project_id, l.event_time, l.model,
       l.prompt_tokens, l.completion_tokens
FROM public.usage_ledger l
JOIN public.usage_query_scope_bindings b
  ON b.tenant_id = l.tenant_id AND b.project_id = l.project_id
WHERE b.login_role = session_user;

REVOKE ALL ON public.usage_query_scope_bindings FROM PUBLIC, llm_telemetry_usage_query_reader;
REVOKE ALL ON public.usage_query_scope, public.usage_query_ledger FROM PUBLIC, llm_telemetry_usage_query_reader;
GRANT USAGE ON SCHEMA public TO llm_telemetry_usage_query_reader;
GRANT SELECT ON public.usage_query_scope, public.usage_query_ledger TO llm_telemetry_usage_query_reader;

-- No grant on usage_ledger, event_outbox, or the binding table.
-- This is an additive view boundary, not an RLS change on the write path.
-- A dedicated login must not inherit other roles with broader privileges.
