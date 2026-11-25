package ledger

import (
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The disposable M2 PostgreSQL integration job loads all migrations.
// Roll back every fixture, including the role and immutable rows.
func TestPriceVersionRegistryConstraintsAndScope(t *testing.T) {
	db := testDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil { t.Fatal(err) }
	defer tx.Rollback()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := tx.ExecContext(ctx,q,args...); err != nil { t.Fatal(err) }
	}
	expect := func(code,q string,args ...any) {
		t.Helper()
		exec("SAVEPOINT registry_check")
		_, err := tx.ExecContext(ctx,q,args...)
		var pgErr *pgconn.PgError
		if !errors.As(err,&pgErr) || pgErr.Code != code {
			t.Fatalf("expected SQLSTATE %s, got %v",code,err)
		}
		exec("ROLLBACK TO SAVEPOINT registry_check")
		exec("RELEASE SAVEPOINT registry_check")
	}
	count := func(q string,args ...any) int {
		t.Helper()
		var n int
		if err := tx.QueryRowContext(ctx,q,args...).Scan(&n);err!=nil{t.Fatal(err)}
		return n
	}
	suffix := strings.ToLower(rand.Text())
	tenant, project := "m13-"+suffix, "p"
	role := "m13_reader_"+suffix
	ident := pgx.Identifier{role}.Sanitize()
	const insert = "INSERT INTO public.price_version_registry(tenant_id,project_id,price_version,price_fingerprint) VALUES($1,$2,$3,$4)"
	fp := "sha256:"+strings.Repeat("a",64)
	other := "sha256:"+strings.Repeat("b",64)
	exec(insert,tenant,project,"v1",fp)
	exec(insert,tenant,project,"V1",other)
	exec(insert,tenant,"other","v1",other)
	exec(insert,tenant+"-other",project,"v1",other)
	expect("23505",insert,tenant,project,"v1",fp)
	expect("23505",insert,tenant,project,"v1",other)
	expect("23514",insert,tenant,project,"bad","sha256:"+strings.Repeat("A",64))
	expect("23514",insert,tenant,project,"bad","sha256:"+strings.Repeat("a",63))
	expect("23514",insert,tenant,project,"",fp)
	expect("23514",insert,tenant,project,strings.Repeat("x",129),fp)
	expect("55000","UPDATE public.price_version_registry SET price_fingerprint=$1 WHERE tenant_id=$2",other,tenant)
	expect("55000","DELETE FROM public.price_version_registry WHERE tenant_id=$1",tenant)
	expect("55000","TRUNCATE public.price_version_registry")

	exec("CREATE ROLE "+ident+" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS")
	exec("GRANT llm_telemetry_usage_query_reader TO "+ident)
	// SET SESSION AUTHORIZATION changes session_user (unlike SET ROLE).
	// This exercises the view and privileges, not authentication transport.
	exec("SET LOCAL SESSION AUTHORIZATION "+ident)
	if got:=count("SELECT count(*) FROM public.usage_query_price_versions");got!=0{t.Fatalf("unbound rows=%d",got)}
	exec("RESET SESSION AUTHORIZATION")
	exec("INSERT INTO public.usage_query_scope_bindings(login_role,tenant_id,project_id) VALUES($1,$2,$3)",role,tenant,project)
	exec("SET LOCAL SESSION AUTHORIZATION "+ident)
	if got:=count("SELECT count(*) FROM public.usage_query_price_versions");got!=2{t.Fatalf("scoped rows=%d",got)}
	var version,digest string
	err=tx.QueryRowContext(ctx,"SELECT price_version,price_fingerprint FROM public.usage_query_price_versions WHERE price_version='v1'").Scan(&version,&digest)
	if err!=nil||version!="v1"||digest!=fp{t.Fatalf("binding=%q %q err=%v",version,digest,err)}
	expect("42501","SELECT * FROM public.price_version_registry")
	expect("42501",insert,tenant,project,"v2",fp)
	// PostgreSQL rejects DML on a non-updatable join view before checking
	// write privileges (SQLSTATE 55000). Assert the absence of privileges
	// separately so structural rejection cannot mask an accidental grant.
	for _, privilege := range []string{"INSERT","UPDATE","DELETE","TRUNCATE"} {
		var granted bool
		if err := tx.QueryRowContext(ctx,"SELECT has_table_privilege(current_user, 'public.usage_query_price_versions', $1)",privilege).Scan(&granted);err!=nil||granted {
			t.Fatalf("unexpected view privilege %s: granted=%v err=%v",privilege,granted,err)
		}
	}
	expect("55000","UPDATE public.usage_query_price_versions SET price_fingerprint=$1",other)
	expect("55000","DELETE FROM public.usage_query_price_versions")
	exec("RESET SESSION AUTHORIZATION")
	exec("DELETE FROM public.usage_query_scope_bindings WHERE login_role=$1",role)
	exec("SET LOCAL SESSION AUTHORIZATION "+ident)
	if got:=count("SELECT count(*) FROM public.usage_query_price_versions");got!=0{t.Fatalf("revoked rows=%d",got)}
}
