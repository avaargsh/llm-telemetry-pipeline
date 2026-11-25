package ledger

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/event"
)

func TestUsageSummaryValidatesScopeAndWindow(t *testing.T) {
	start := time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	s := &Store{}
	for _, tc := range []struct {
		tenant, project string
		from, to time.Time
	}{
		{"", "p", start, start.Add(time.Hour)},
		{"t", "", start, start.Add(time.Hour)},
		{"t", "p", start, start},
		{"t", "p", start, start.Add(32*24*time.Hour)},
		{"t", "p", time.Time{}, start},
	} {
		if _, err := s.UsageSummary(context.Background(), tc.tenant, tc.project, tc.from, tc.to); err == nil {
			t.Fatal("invalid query accepted")
		}
	}
}

func TestUsageSummaryAuthoritativeScopeAndHalfOpenWindow(t *testing.T) {
	db := testDB(t)
	s := &Store{DB: db}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	tenant := "m12-" + rand.Text()
	project := "project-a"
	t.Cleanup(func() {
		c, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		for _, query := range []string{
			"DELETE FROM event_outbox WHERE event_id IN (SELECT event_id FROM usage_ledger WHERE tenant_id IN ($1,$2))",
			"DELETE FROM usage_ledger WHERE tenant_id IN ($1,$2)",
		} {
			if _, err := db.ExecContext(c, query, tenant, tenant+"-other"); err != nil {
				t.Errorf("fixture cleanup: %v", err)
			}
		}
	})
	for i, row := range []struct {
		tenant, project string
		at time.Time
	}{
		{tenant, project, start},
		{tenant, project, start.Add(time.Minute)},
		{tenant, project, end},
		{tenant, project, start.Add(-time.Second)},
		{tenant, "other-project", start},
		{tenant+"-other", project, start},
	} {
		e, err := event.SyntheticUsageEvent(tenant, i, start)
		if err != nil { t.Fatal(err) }
		e.TenantID, e.ProjectID, e.Timestamp = row.tenant, row.project, row.at
		e.PromptTokens, e.CompletionTokens = 10, 3
		if i == 0 { e.Model = "model-a" } else { e.Model = "model-b" }
		if err := e.Seal(); err != nil { t.Fatal(err) }
		if out, err := s.Record(ctx, e); err != nil || out != Created {
			t.Fatalf("record: %s %v", out, err)
		}
		if out, err := s.Record(ctx, e); err != nil || out != Duplicate {
			t.Fatalf("retry: %s %v", out, err)
		}
	}
	got, err := s.UsageSummary(ctx, tenant, project, start, end)
	if err != nil { t.Fatal(err) }
	if got.RequestCount != 2 || got.PromptTokens != "20" || got.CompletionTokens != "6" || got.TotalTokens != "26" {
		t.Fatalf("wrong aggregate: %+v", got)
	}
	empty, err := s.UsageSummary(ctx, tenant, "missing", start, end)
	if err != nil { t.Fatal(err) }
	if empty.RequestCount != 0 || empty.PromptTokens != "0" || empty.CompletionTokens != "0" || empty.TotalTokens != "0" {
		t.Fatalf("wrong empty aggregate: %+v", empty)
	}
	models, err := s.UsageByModel(ctx, tenant, project, start, end)
	if err != nil { t.Fatal(err) }
	if len(models) != 2 || models[0].Model != "model-a" || models[1].Model != "model-b" {
		t.Fatalf("wrong model ordering or scope: %+v", models)
	}
	for _, row := range models {
		if row.RequestCount != 1 || row.PromptTokens != "10" || row.CompletionTokens != "3" || row.TotalTokens != "13" {
			t.Fatalf("wrong model aggregate: %+v", row)
		}
	}
	noModels, err := s.UsageByModel(ctx, tenant, "missing", start, end)
	if err != nil || noModels == nil || len(noModels) != 0 { t.Fatalf("empty models: %+v %v", noModels, err) }

}

func TestUsageByModelRejectsOverflowWithoutPartialTotals(t *testing.T) {
	db := testDB(t)
	s := &Store{DB: db}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	tenant := "m12-model-limit-" + rand.Text()
	project := "project-a"
	t.Cleanup(func() {
		c, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		for _, query := range []string{
			"DELETE FROM event_outbox WHERE event_id IN (SELECT event_id FROM usage_ledger WHERE tenant_id=$1)",
			"DELETE FROM usage_ledger WHERE tenant_id=$1",
		} {
			if _, err := db.ExecContext(c, query, tenant); err != nil {
				t.Errorf("fixture cleanup: %v", err)
			}
		}
	})
	for i := 0; i <= MaxUsageModels; i++ {
		e, err := event.SyntheticUsageEvent(tenant, i, start)
		if err != nil { t.Fatal(err) }
		e.TenantID, e.ProjectID, e.Model = tenant, project, fmt.Sprintf("model-%03d", i)
		e.Timestamp = start
		if err := e.Seal(); err != nil { t.Fatal(err) }
		if out, err := s.Record(ctx, e); err != nil || out != Created {
			t.Fatalf("record: %s %v", out, err)
		}
		if i == MaxUsageModels-1 {
			rows, err := s.UsageByModel(ctx, tenant, project, start, start.Add(time.Hour))
			if err != nil || len(rows) != MaxUsageModels {
				t.Fatalf("at limit: %d rows, %v", len(rows), err)
			}
		}
	}
	rows, err := s.UsageByModel(ctx, tenant, project, start, start.Add(time.Hour))
	if !errors.Is(err, ErrUsageModelLimit) || rows != nil {
		t.Fatalf("overflow must not expose partial totals: %d rows, %v", len(rows), err)
	}
	summary, err := s.UsageSummary(ctx, tenant, project, start, start.Add(time.Hour))
	if err != nil || summary.RequestCount != int64(MaxUsageModels+1) {
		t.Fatalf("overall summary must remain complete: %+v %v", summary, err)
	}
}
