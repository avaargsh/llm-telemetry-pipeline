package ledger

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

type usageQueryDB interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// Only package-owned constant relations are used in query construction.
type usageRelation string
const (
	baseUsageRelation usageRelation = "public.usage_ledger"
	scopedUsageRelation usageRelation = "public.usage_query_ledger"
)

var ErrUsageQueryScope = errors.New("database query scope is unavailable or mismatched")

// ScopedUsageReader is a read-only query boundary for a dedicated project
// login. Do not use the ledger writer connection pool here.
type ScopedUsageReader struct {
	db *sql.DB
	tenant, project string
}

func NewScopedUsageReader(ctx context.Context, db *sql.DB, tenant, project string) (*ScopedUsageReader, error) {
	if db == nil || strings.TrimSpace(tenant) == "" || strings.TrimSpace(project) == "" {
		return nil, ErrUsageQueryScope
	}
	s := &ScopedUsageReader{db:db, tenant:tenant, project:project}
	if _, err := scopedRead(ctx,s,tenant,project,func(*sql.Tx)(bool,error){ return true,nil }); err != nil {
		return nil,err
	}
	return s,nil
}

// Verify the authenticated session, membership, effective direct table
// privileges and exact scope. This is not a complete database privilege
// audit; deployment must also control ownership, grants and role changes.
func verifyQueryScope(ctx context.Context, q usageQueryDB, tenant, project string) error {
	var allowed bool
	err := q.QueryRowContext(ctx, `
SELECT session_user = current_user
 AND r.rolcanlogin AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls)
 AND pg_has_role(session_user, 'llm_telemetry_usage_query_reader', 'USAGE')
 AND NOT EXISTS (
     SELECT 1 FROM pg_catalog.pg_roles other
     WHERE other.rolname NOT IN (session_user, 'llm_telemetry_usage_query_reader')
       AND pg_has_role(session_user, other.oid, 'MEMBER')
 )
 AND NOT has_schema_privilege(session_user, 'public', 'CREATE')
 AND NOT EXISTS (
     SELECT 1 FROM (VALUES ('public.usage_ledger'), ('public.event_outbox'),
                           ('public.usage_query_scope_bindings')) AS x(relation)
     WHERE has_table_privilege(session_user, x.relation, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
        OR has_any_column_privilege(session_user, x.relation, 'SELECT,INSERT,UPDATE,REFERENCES')
 )
 AND EXISTS (SELECT 1 FROM public.usage_query_scope WHERE tenant_id=$1 AND project_id=$2)
FROM pg_catalog.pg_roles r WHERE r.rolname = session_user
`,tenant,project).Scan(&allowed)
	if err != nil || !allowed { return ErrUsageQueryScope }
	return nil
}

// Revalidate every transaction; a revoked or reassigned binding must not
// be reported as an empty successful usage query. Both checks and read
// operate on one repeatable-read snapshot of the same connection.
func scopedRead[T any](ctx context.Context, s *ScopedUsageReader, tenant, project string, read func(*sql.Tx)(T,error)) (T,error) {
	var zero T
	if s == nil || s.db == nil || tenant != s.tenant || project != s.project { return zero,ErrUsageQueryScope }
	tx,err := s.db.BeginTx(ctx,&sql.TxOptions{ReadOnly:true,Isolation:sql.LevelRepeatableRead})
	if err != nil { return zero,err }
	defer tx.Rollback()
	if err := verifyQueryScope(ctx,tx,tenant,project); err != nil { return zero,err }
	result,err := read(tx)
	if err != nil { return zero,err }
	if err := tx.Commit(); err != nil { return zero,err }
	return result,nil
}

func (s *ScopedUsageReader) UsageSummary(ctx context.Context, tenant, project string, from,to time.Time)(UsageSummary,error) {
	return scopedRead(ctx,s,tenant,project,func(tx *sql.Tx)(UsageSummary,error){
		return queryUsageSummary(ctx,tx,scopedUsageRelation,tenant,project,from,to)
	})
}

func (s *ScopedUsageReader) UsageByModel(ctx context.Context, tenant, project string, from,to time.Time)([]ModelUsage,error) {
	return scopedRead(ctx,s,tenant,project,func(tx *sql.Tx)([]ModelUsage,error){
		return queryUsageByModel(ctx,tx,scopedUsageRelation,tenant,project,from,to)
	})
}

func (s *ScopedUsageReader) UsageSeries(ctx context.Context, tenant, project string, from,to time.Time, granularity string)([]UsageBucket,error) {
	return scopedRead(ctx,s,tenant,project,func(tx *sql.Tx)([]UsageBucket,error){
		return queryUsageSeries(ctx,tx,scopedUsageRelation,tenant,project,from,to,granularity)
	})
}
