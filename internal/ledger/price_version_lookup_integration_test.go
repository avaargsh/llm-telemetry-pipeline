package ledger

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// Only run against the disposable migration-loaded TEST_POSTGRES_DSN.
// Immutable registry fixtures use unique scopes and live until the test
// database is destroyed; no mutation trigger is disabled for cleanup.
func TestPriceVersionLookupRealLogin(t *testing.T) {
	admin := testDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	suffix := strings.ToLower(rand.Text())
	role := "m13_lookup_" + suffix
	tenant, project := "m13-lookup-"+suffix, "p"
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
	for _, tc := range []struct {
		version, want string
	}{
		{"v1", fp}, {"V1", other},
	} {
		if got, err := reader.PriceVersionFingerprint(ctx, tenant, project, tc.version); err != nil || got != tc.want {
			t.Fatalf("lookup %q: got %q, err %v", tc.version, got, err)
		}
	}
	for _, version := range []string{"missing", "hidden", "v1 "} {
		if got, err := reader.PriceVersionFingerprint(ctx, tenant, project, version); got != "" || !errors.Is(err, ErrPriceVersionNotFound) {
			t.Fatalf("missing %q: got %q, err %v", version, got, err)
		}
	}
	if got, err := reader.PriceVersionFingerprint(ctx, tenant, "other", "hidden"); got != "" || !errors.Is(err, ErrUsageQueryScope) {
		t.Fatalf("wrong project: got %q, err %v", got, err)
	}
	if got, err := reader.PriceVersionFingerprint(ctx, tenant, project, ""); got != "" || !errors.Is(err, ErrInvalidPriceVersion) {
		t.Fatalf("invalid version: got %q, err %v", got, err)
	}
	exec("GRANT SELECT ON public.price_version_registry TO " + ident)
	if got, err := reader.PriceVersionFingerprint(ctx, tenant, project, "v1"); got != "" || !errors.Is(err, ErrUsageQueryScope) {
		t.Fatalf("overprivileged reader: got %q, err %v", got, err)
	}
	exec("REVOKE SELECT ON public.price_version_registry FROM " + ident)
	if got, err := reader.PriceVersionFingerprint(ctx, tenant, project, "v1"); got != fp || err != nil {
		t.Fatalf("restored privileges: got %q, err %v", got, err)
	}
	exec("DELETE FROM public.usage_query_scope_bindings WHERE login_role=$1", role)
	if got, err := reader.PriceVersionFingerprint(ctx, tenant, project, "v1"); got != "" || !errors.Is(err, ErrUsageQueryScope) {
		t.Fatalf("revoked scope: got %q, err %v", got, err)
	}
}
