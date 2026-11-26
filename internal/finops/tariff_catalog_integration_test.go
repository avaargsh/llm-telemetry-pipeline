package finops

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func tariffCatalogAdminDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is required for PostgreSQL catalog integration")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal("open PostgreSQL test database")
	}
	if err := db.Ping(); err != nil {
		db.Close()
		t.Fatal("ping PostgreSQL test database")
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestTariffCatalogRealLogin(t *testing.T) {
	admin := tariffCatalogAdminDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	suffix := strings.ToLower(rand.Text())
	role := "m139_reader_" + suffix
	tenant, project := "m139-"+suffix, "project-a"
	password := rand.Text()
	ident := pgx.Identifier{role}.Sanitize()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := admin.ExecContext(ctx, q, args...); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := admin.ExecContext(ctx, "CREATE ROLE "+ident+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+password+"'"); err != nil {
		t.Fatal("create disposable login")
	}
	t.Cleanup(func() {
		c, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if _, err := admin.ExecContext(c, "DELETE FROM public.usage_query_scope_bindings WHERE login_role=$1", role); err != nil {
			t.Errorf("remove test scope: %v", err)
		}
		if _, err := admin.ExecContext(c, "REVOKE llm_telemetry_usage_query_reader FROM "+ident); err != nil {
			t.Errorf("revoke test role: %v", err)
		}
		if _, err := admin.ExecContext(c, "DROP ROLE "+ident); err != nil {
			t.Errorf("drop test role: %v", err)
		}
	})
	exec("GRANT llm_telemetry_usage_query_reader TO " + ident)
	exec("INSERT INTO public.usage_query_scope_bindings(login_role,tenant_id,project_id) VALUES ($1,$2,$3)", role, tenant, project)

	table := PriceTable{
		Version:  "catalog-v1",
		Currency: "USD",
		Rates: []ModelRate{
			{Model: "模型-a", InputPerMillion: "1.25", OutputPerMillion: "2"},
			{Model: "model-b", InputPerMillion: "0.5", OutputPerMillion: "0"},
		},
	}
	expected, err := CanonicalTariffPayload(table)
	if err != nil {
		t.Fatal(err)
	}
	exec("INSERT INTO public.price_version_registry(tenant_id,project_id,price_version,price_fingerprint) VALUES ($1,$2,$3,$4)",
		tenant, project, table.Version, expected.Fingerprint)

	otherFingerprint := "sha256:" + strings.Repeat("b", 64)
	if _, err := admin.ExecContext(ctx, `
INSERT INTO public.price_tariff_payloads
    (tenant_id,project_id,price_version,price_fingerprint,payload_schema_version,currency,rates)
VALUES ($1,$2,'missing-binding',$3,1,'USD',$4::jsonb)`,
		tenant, project, expected.Fingerprint,
		`[{"model":"model-b","input_per_million":"0.500000","output_per_million":"0.000000"}]`); err == nil ||
		!strings.Contains(err.Error(), "price version registry binding missing") {
		t.Fatalf("database did not require registry binding: %v", err)
	}
	exec("INSERT INTO public.price_version_registry(tenant_id,project_id,price_version,price_fingerprint) VALUES ($1,$2,$3,$4)",
		tenant, project, "bad-binding", expected.Fingerprint)
	if _, err := admin.ExecContext(ctx, `
INSERT INTO public.price_tariff_payloads
    (tenant_id,project_id,price_version,price_fingerprint,payload_schema_version,currency,rates)
VALUES ($1,$2,'bad-binding',$3,1,'USD',$4::jsonb)`,
		tenant, project, otherFingerprint,
		`[{"model":"model-b","input_per_million":"0.500000","output_per_million":"0.000000"}]`); err == nil ||
		!strings.Contains(err.Error(), "price tariff payload fingerprint mismatch") {
		t.Fatalf("database did not enforce registry fingerprint binding: %v", err)
	}

	exec("INSERT INTO public.price_version_registry(tenant_id,project_id,price_version,price_fingerprint) VALUES ($1,$2,$3,$4)",
		tenant, project, "bad-order", expected.Fingerprint)
	if _, err := admin.ExecContext(ctx, `
INSERT INTO public.price_tariff_payloads
    (tenant_id,project_id,price_version,price_fingerprint,payload_schema_version,currency,rates)
VALUES ($1,$2,'bad-order',$3,1,'USD',$4::jsonb)`,
		tenant, project, expected.Fingerprint,
		`[{"model":"模型-a","input_per_million":"1.250000","output_per_million":"2.000000"},{"model":"model-b","input_per_million":"0.500000","output_per_million":"0.000000"}]`); err == nil ||
		!strings.Contains(err.Error(), "price tariff models must be unique and byte-sorted") {
		t.Fatalf("database did not enforce canonical model ordering: %v", err)
	}
	exec("INSERT INTO public.price_version_registry(tenant_id,project_id,price_version,price_fingerprint) VALUES ($1,$2,$3,$4)",
		tenant, project, "bad-shape", expected.Fingerprint)
	if _, err := admin.ExecContext(ctx, `
INSERT INTO public.price_tariff_payloads
    (tenant_id,project_id,price_version,price_fingerprint,payload_schema_version,currency,rates)
VALUES ($1,$2,'bad-shape',$3,1,'USD',$4::jsonb)`,
		tenant, project, expected.Fingerprint, `[1]`); err == nil ||
		!strings.Contains(err.Error(), "invalid price tariff rate object") {
		t.Fatalf("database did not reject non-object rate entries: %v", err)
	}
	exec("INSERT INTO public.price_version_registry(tenant_id,project_id,price_version,price_fingerprint) VALUES ($1,$2,$3,$4)",
		tenant, project, "bad-rate", expected.Fingerprint)
	if _, err := admin.ExecContext(ctx, `
INSERT INTO public.price_tariff_payloads
    (tenant_id,project_id,price_version,price_fingerprint,payload_schema_version,currency,rates)
VALUES ($1,$2,'bad-rate',$3,1,'USD',$4::jsonb)`,
		tenant, project, expected.Fingerprint,
		`[{"model":"model-b","input_per_million":"01.000000","output_per_million":"0.000000"},{"model":"模型-a","input_per_million":"1.250000","output_per_million":"2.000000"}]`); err == nil ||
		!strings.Contains(err.Error(), "price tariff rates must be canonical nonnegative USD decimals") {
		t.Fatalf("database did not enforce canonical rate encoding: %v", err)
	}

	stored, err := RegisterTariffPayload(ctx, admin, tenant, project, table)
	if err != nil || stored.Fingerprint != expected.Fingerprint {
		t.Fatalf("register: %+v %v", stored, err)
	}
	again, err := RegisterTariffPayload(ctx, admin, tenant, project, table)
	if err != nil || again.Fingerprint != expected.Fingerprint {
		t.Fatalf("idempotent register: %+v %v", again, err)
	}
	changed := table
	changed.Rates = append([]ModelRate(nil), table.Rates...)
	changed.Rates[0].InputPerMillion = "1.5"
	if _, err := RegisterTariffPayload(ctx, admin, tenant, project, changed); !errors.Is(err, ledger.ErrPriceVersionMismatch) {
		t.Fatalf("changed version binding: %v", err)
	}
	missing := table
	missing.Version = "not-registered"
	if _, err := RegisterTariffPayload(ctx, admin, tenant, project, missing); !errors.Is(err, ledger.ErrPriceVersionNotFound) {
		t.Fatalf("missing registry binding: %v", err)
	}

	mutations := []struct {
		name  string
		query string
		args  []any
	}{
		{"update", "UPDATE public.price_tariff_payloads SET currency='EUR' WHERE tenant_id=$1 AND project_id=$2 AND price_version=$3", []any{tenant, project, table.Version}},
		{"delete", "DELETE FROM public.price_tariff_payloads WHERE tenant_id=$1 AND project_id=$2 AND price_version=$3", []any{tenant, project, table.Version}},
		{"truncate", "TRUNCATE public.price_tariff_payloads", nil},
	}
	for _, mutation := range mutations {
		_, err := admin.ExecContext(ctx, mutation.query, mutation.args...)
		if err == nil || !strings.Contains(err.Error(), "price tariff payloads are append-only") {
			t.Fatalf("%s did not hit append-only trigger: %v", mutation.name, err)
		}
	}
	var count int
	if err := admin.QueryRowContext(ctx, "SELECT count(*) FROM public.price_tariff_payloads WHERE tenant_id=$1 AND project_id=$2 AND price_version=$3",
		tenant, project, table.Version).Scan(&count); err != nil || count != 1 {
		t.Fatalf("immutable row count=%d err=%v", count, err)
	}

	cfg, err := pgx.ParseConfig(os.Getenv("TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatal("parse fixture DSN")
	}
	cfg.User, cfg.Password = role, password
	readerDB := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = readerDB.Close() })
	reader, err := ledger.NewScopedUsageReader(ctx, readerDB, tenant, project)
	if err != nil {
		t.Fatal(err)
	}
	got, err := LoadRegisteredTariff(ctx, reader, tenant, project, table.Version)
	if err != nil || got.Fingerprint != expected.Fingerprint || len(got.Table.Rates) != 2 {
		t.Fatalf("read registered tariff: %+v %v", got, err)
	}
	var directCount int
	if err := readerDB.QueryRowContext(ctx, "SELECT count(*) FROM public.price_tariff_payloads").Scan(&directCount); err == nil {
		t.Fatal("runtime login unexpectedly read base tariff table")
	}
	var crossScopeRows int
	if err := readerDB.QueryRowContext(ctx,
		"SELECT count(*) FROM public.usage_query_price_tariffs WHERE tenant_id=$1 AND project_id=$2",
		tenant, "other-project").Scan(&crossScopeRows); err != nil || crossScopeRows != 0 {
		t.Fatalf("scoped view leaked another project: rows=%d err=%v", crossScopeRows, err)
	}
	if _, err := reader.PriceTariffPayload(ctx, tenant, "other-project", table.Version); !errors.Is(err, ledger.ErrUsageQueryScope) {
		t.Fatalf("cross-project read: %v", err)
	}
	if _, err := admin.ExecContext(ctx, "DELETE FROM public.usage_query_scope_bindings WHERE login_role=$1", role); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.PriceTariffPayload(ctx, tenant, project, table.Version); !errors.Is(err, ledger.ErrUsageQueryScope) {
		t.Fatalf("revoked scope read: %v", err)
	}
}
