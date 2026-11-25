package ledger

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"
)

// Disposable PostgreSQL superuser fixture only. Roll back all roles, scope
// bindings and test rows together; no login password is created.
func TestUsageQueryDatabaseScopeMigration(t *testing.T) {
	db := testDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx,nil)
	if err != nil { t.Fatal(err) }
	defer tx.Rollback()
	suffix := strings.ToLower(rand.Text())
	bound := "m12_bound_" + suffix
	unbound := "m12_unbound_" + suffix
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := tx.ExecContext(ctx,q,args...); err != nil { t.Fatal(err) }
	}
	for _, role := range []string{bound,unbound} {
		exec("CREATE ROLE "+role+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS")
		exec("GRANT llm_telemetry_usage_query_reader TO "+role)
	}
	tenant := "m12-scope-"+suffix
	exec("INSERT INTO public.usage_query_scope_bindings(login_role,tenant_id,project_id) VALUES ($1,$2,'p')",bound,tenant)
	for i, scope := range []struct{tenant,project string}{
		{tenant,"p"},{tenant,"other"},{tenant+"-other","p"},
	} {
		exec(`INSERT INTO public.usage_ledger
(event_id,tenant_id,project_id,request_id,event_type,schema_version,model,prompt_tokens,completion_tokens,payload_hash,event_time)
VALUES (gen_random_uuid(),$1,$2,$3,'usage.finalized',1,'model',10,3,repeat('a',64),now())`,
			scope.tenant,scope.project,suffix+string(rune('a'+i)))
	}
	if err := verifyQueryScope(ctx, tx, tenant, "p"); err == nil { t.Fatal("privileged login accepted") }
	for _, tc := range []struct{role string; count int}{{bound,1},{unbound,0}} {
		exec("SET LOCAL SESSION AUTHORIZATION "+tc.role)
		var count int
		if err := tx.QueryRowContext(ctx,"SELECT count(*) FROM public.usage_query_ledger").Scan(&count); err != nil || count != tc.count {
			t.Fatalf("scope count %s: %d %v",tc.role,count,err)
		}
		if err := tx.QueryRowContext(ctx,"SELECT count(*) FROM public.usage_query_scope").Scan(&count); err != nil || count != tc.count {
			t.Fatalf("scope binding %s: %d %v",tc.role,count,err)
		}
		for _, check := range []struct{table,privilege string}{
			{"public.usage_ledger","SELECT"},{"public.usage_ledger","INSERT"},
			{"public.event_outbox","SELECT"},{"public.usage_query_scope_bindings","SELECT"},
			{"public.usage_query_scope_bindings","INSERT"},{"public.usage_query_ledger","INSERT"},
		} {
			var allowed bool
			if err := tx.QueryRowContext(ctx,"SELECT has_table_privilege(current_user,$1,$2)",check.table,check.privilege).Scan(&allowed); err != nil || allowed {
				t.Fatalf("unexpected privilege %s %s: %t %v",check.table,check.privilege,allowed,err)
			}
		}
		if tc.role == bound {
			if err := verifyQueryScope(ctx, tx, tenant, "p"); err != nil { t.Fatal(err) }
			if err := verifyQueryScope(ctx, tx, tenant, "other"); err == nil { t.Fatal("mismatched project accepted") }
			if err := verifyQueryScope(ctx, tx, tenant+"-other", "p"); err == nil { t.Fatal("mismatched tenant accepted") }
		} else if err := verifyQueryScope(ctx, tx, tenant, "p"); err == nil {
			t.Fatal("unbound login accepted")
		}
		exec("RESET SESSION AUTHORIZATION")
	}
}
