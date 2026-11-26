package finops

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestPriceEffectivePeriodsRealLogin(t *testing.T) {
	admin := tariffCatalogAdminDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	suffix := strings.ToLower(rand.Text())
	role := "m139c_reader_" + suffix
	tenant, project := "m139c-"+suffix, "project-a"
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

	register := func(projectID, version string) {
		t.Helper()
		table := PriceTable{
			Version: version, Currency: "USD",
			Rates: []ModelRate{{Model: "model-a", InputPerMillion: "1", OutputPerMillion: "2"}},
		}
		payload, err := CanonicalTariffPayload(table)
		if err != nil {
			t.Fatal(err)
		}
		exec("INSERT INTO public.price_version_registry(tenant_id,project_id,price_version,price_fingerprint) VALUES ($1,$2,$3,$4)",
			tenant, projectID, version, payload.Fingerprint)
		if _, err := RegisterTariffPayload(ctx, admin, tenant, projectID, table); err != nil {
			t.Fatalf("register payload %s/%s: %v", projectID, version, err)
		}
	}
	register(project, "v1")
	register(project, "v2")
	register(project, "v3")
	register("project-b", "other-v1")

	start := time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	mid := start.Add(time.Hour)
	end := mid.Add(time.Hour)
	first, err := PublishTariffPeriod(ctx, admin, PricePeriodPublication{
		TenantID: tenant, ProjectID: project, PriceVersion: "v1",
		EffectiveFrom: start, EffectiveTo: mid,
		Reason: "initial tariff", SourceReference: "change-001",
	})
	if err != nil || first.PriceFingerprint == "" || first.PublishedAt.IsZero() || first.PublishedBy == "" {
		t.Fatalf("publish first period: %+v %v", first, err)
	}
	second, err := PublishTariffPeriod(ctx, admin, PricePeriodPublication{
		TenantID: tenant, ProjectID: project, PriceVersion: "v2",
		EffectiveFrom: mid, EffectiveTo: end, Reason: "scheduled price change",
	})
	if err != nil || second.PriceVersion != "v2" {
		t.Fatalf("publish adjacent period: %+v %v", second, err)
	}
	if _, err := PublishTariffPeriod(ctx, admin, PricePeriodPublication{
		TenantID: tenant, ProjectID: project, PriceVersion: "v3",
		EffectiveFrom: mid.Add(-time.Minute), EffectiveTo: end.Add(time.Minute), Reason: "overlap",
	}); !errors.Is(err, ErrPricePeriodOverlap) {
		t.Fatalf("overlapping period: %v", err)
	}
	if _, err := PublishTariffPeriod(ctx, admin, PricePeriodPublication{
		TenantID: tenant, ProjectID: "project-b", PriceVersion: "other-v1",
		EffectiveFrom: start, EffectiveTo: mid, Reason: "separate project",
	}); err != nil {
		t.Fatalf("different project overlap should be allowed: %v", err)
	}
	if _, err := PublishTariffPeriod(ctx, admin, PricePeriodPublication{
		TenantID: tenant, ProjectID: project, PriceVersion: "missing",
		EffectiveFrom: end, EffectiveTo: end.Add(time.Hour), Reason: "missing payload",
	}); !errors.Is(err, ledger.ErrPriceTariffPayloadNotFound) {
		t.Fatalf("missing payload: %v", err)
	}
	if _, err := PublishTariffPeriod(ctx, admin, PricePeriodPublication{
		TenantID: tenant, ProjectID: project, PriceVersion: "v3",
		EffectiveFrom: end.Add(time.Nanosecond), EffectiveTo: end.Add(time.Hour), Reason: "unaligned",
	}); !errors.Is(err, ErrInvalidPricePeriod) {
		t.Fatalf("submicrosecond boundary: %v", err)
	}
	if _, err := PublishTariffPeriod(ctx, admin, PricePeriodPublication{
		TenantID: tenant, ProjectID: project, PriceVersion: "v3",
		EffectiveFrom: end, EffectiveTo: end.Add(time.Hour), Reason: "  ",
	}); !errors.Is(err, ErrInvalidPricePeriod) {
		t.Fatalf("empty reason: %v", err)
	}

	// The exclusion constraint must arbitrate overlapping concurrent writers.
	raceStart, raceEnd := end.Add(2*time.Hour), end.Add(3*time.Hour)
	startGate := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, version := range []string{"v1", "v2"} {
		wg.Add(1)
		go func(v string) {
			defer wg.Done()
			<-startGate
			_, err := PublishTariffPeriod(ctx, admin, PricePeriodPublication{
				TenantID: tenant, ProjectID: project, PriceVersion: v,
				EffectiveFrom: raceStart, EffectiveTo: raceEnd, Reason: "concurrent publication",
			})
			results <- err
		}(version)
	}
	close(startGate)
	wg.Wait()
	close(results)
	successes, overlaps := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrPricePeriodOverlap):
			overlaps++
		default:
			t.Fatalf("unexpected concurrent publication error: %v", err)
		}
	}
	if successes != 1 || overlaps != 1 {
		t.Fatalf("concurrent exclusion results: success=%d overlap=%d", successes, overlaps)
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
	periods, err := reader.PriceEffectivePeriods(ctx, tenant, project, start, end.Add(time.Hour))
	if err != nil || len(periods) != 2 || periods[0].PriceVersion != "v1" || periods[1].PriceVersion != "v2" {
		t.Fatalf("read scoped periods: %+v %v", periods, err)
	}
	if periods[0].PublishedBy == "" || periods[0].Reason != "initial tariff" || periods[0].SourceReference != "change-001" {
		t.Fatalf("publication evidence missing: %+v", periods[0])
	}
	var directCount int
	if err := readerDB.QueryRowContext(ctx, "SELECT count(*) FROM public.price_tariff_effective_periods").Scan(&directCount); err == nil {
		t.Fatal("runtime login unexpectedly read base period table")
	}
	var crossScopeRows int
	if err := readerDB.QueryRowContext(ctx,
		"SELECT count(*) FROM public.usage_query_price_periods WHERE tenant_id=$1 AND project_id=$2",
		tenant, "project-b").Scan(&crossScopeRows); err != nil || crossScopeRows != 0 {
		t.Fatalf("scoped view leaked another project: rows=%d err=%v", crossScopeRows, err)
	}
	for name, q := range map[string]string{
		"update": "UPDATE public.price_tariff_effective_periods SET publication_reason='rewritten'",
		"delete": "DELETE FROM public.price_tariff_effective_periods",
		"truncate": "TRUNCATE public.price_tariff_effective_periods",
	} {
		if _, err := admin.ExecContext(ctx, q); err == nil || !strings.Contains(err.Error(), "price tariff effective periods are append-only") {
			t.Fatalf("%s did not hit append-only trigger: %v", name, err)
		}
	}
	if _, err := admin.ExecContext(ctx, "DELETE FROM public.usage_query_scope_bindings WHERE login_role=$1", role); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.PriceEffectivePeriods(ctx, tenant, project, start, end); !errors.Is(err, ledger.ErrUsageQueryScope) {
		t.Fatalf("revoked scope read: %v", err)
	}
}
