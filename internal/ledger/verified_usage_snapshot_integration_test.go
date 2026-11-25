package ledger

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/event"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// Only run against the disposable migration-loaded TEST_POSTGRES_DSN.
// Immutable registry fixtures use unique scopes and live until the test
// database is destroyed; no mutation trigger is disabled for cleanup.
func TestVerifiedUsageSnapshotRealLogin(t *testing.T) {
	admin := testDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	suffix := strings.ToLower(rand.Text())
	role := "m13_snap_" + suffix
	tenant, project := "m13-snap-"+suffix, "p"
	password := rand.Text()
	ident := pgx.Identifier{role}.Sanitize()
	if _, err := admin.ExecContext(ctx, "CREATE ROLE "+ident+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+password+"'"); err != nil {
		t.Fatal("create disposable login failed")
	}
	t.Cleanup(func() {
		c, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if _, err := admin.ExecContext(c, "REVOKE ALL ON public.price_version_registry FROM "+ident); err != nil {
			t.Errorf("grant cleanup: %v", err)
		}
		if _, err := admin.ExecContext(c, "DELETE FROM public.usage_query_scope_bindings WHERE login_role=$1", role); err != nil {
			t.Errorf("binding cleanup: %v", err)
		}
		if _, err := admin.ExecContext(c, "DROP ROLE "+ident); err != nil {
			t.Errorf("role cleanup: %v", err)
		}
	})
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := admin.ExecContext(ctx, q, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec("GRANT llm_telemetry_usage_query_reader TO " + ident)
	exec("INSERT INTO public.usage_query_scope_bindings(login_role,tenant_id,project_id) VALUES ($1,$2,$3)", role, tenant, project)
	const insert = "INSERT INTO public.price_version_registry(tenant_id,project_id,price_version,price_fingerprint) VALUES ($1,$2,$3,$4)"
	fp := "sha256:" + strings.Repeat("a", 64)
	other := "sha256:" + strings.Repeat("b", 64)
	exec(insert, tenant, project, "v1", fp)
	exec(insert, tenant, project, "V1", other)
	exec(insert, tenant, "other", "hidden", other)
	exec(insert, tenant+"-other", project, "hidden", other)

	cfg, err := pgx.ParseConfig(os.Getenv("TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatal("parse fixture DSN failed")
	}
	cfg.User, cfg.Password = role, password
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	reader, err := NewScopedUsageReader(ctx, db, tenant, project)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	e, err := event.SyntheticUsageEvent(tenant, 0, start)
	if err != nil { t.Fatal(err) }
	e.TenantID, e.ProjectID, e.Timestamp = tenant, project, start
	e.Model, e.PromptTokens, e.CompletionTokens = "model", 10, 3
	if err := e.Seal(); err != nil { t.Fatal(err) }
	if result, err := (&Store{DB: admin}).Record(ctx,e); err != nil || result != Created {
		t.Fatalf("record: %v %v", result, err)
	}
	windows := []ModelQueryWindow{{From:start, To:end}, {From:end, To:end.Add(time.Hour)}}
	valid := []PriceVersionExpectation{{"v1",fp},{"V1",other}}
	got, err := reader.VerifiedUsageByModelWindows(ctx,tenant,project,windows,valid)
	if err != nil || len(got)!=2 || len(got[0].Models)!=1 ||
		got[0].Models[0].PromptTokens!="10" || got[1].Models==nil || len(got[1].Models)!=0 {
		t.Fatalf("verified capture: %+v %v",got,err)
	}
	for _, tc := range []struct{
		name string
		bindings []PriceVersionExpectation
		want error
	}{
		{"missing",[]PriceVersionExpectation{{"v1",fp},{"hidden",other}},ErrPriceVersionNotFound},
		{"mismatch",[]PriceVersionExpectation{{"v1",other}},ErrPriceVersionMismatch},
		{"empty",nil,ErrInvalidPriceExpectation},
	} {
		t.Run(tc.name,func(t *testing.T){
			got,err:=reader.VerifiedUsageByModelWindows(ctx,tenant,project,windows,tc.bindings)
			if got!=nil || !errors.Is(err,tc.want){t.Fatalf("got %+v %v want %v",got,err,tc.want)}
		})
	}
	if got,err:=reader.VerifiedUsageByModelWindows(ctx,tenant,"other",windows,valid);got!=nil || !errors.Is(err,ErrUsageQueryScope){t.Fatalf("wrong scope: %+v %v",got,err)}
	exec("DELETE FROM public.usage_query_scope_bindings WHERE login_role=$1",role)
	if got,err:=reader.VerifiedUsageByModelWindows(ctx,tenant,project,windows,valid);got!=nil || !errors.Is(err,ErrUsageQueryScope){t.Fatalf("revoked scope: %+v %v",got,err)}
}
