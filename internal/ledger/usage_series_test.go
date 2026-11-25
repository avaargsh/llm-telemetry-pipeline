package ledger

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/event"
)

func TestUsageSeriesUTCWindowsAndEmptyBuckets(t *testing.T) {
	db := testDB(t)
	// Keep this disposable test pool on a single session, with a non-UTC
	// database timezone. UTC aggregation must not depend on session defaults.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.Exec("SET TIME ZONE 'America/Los_Angeles'"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.Exec("RESET TIME ZONE"); err != nil { t.Errorf("reset timezone: %v", err) }
	})
	s := &Store{DB: db}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Date(2026, 11, 21, 23, 0, 0, 0, time.UTC)
	end := start.Add(4*time.Hour)
	tenant := "m12-series-" + rand.Text()
	project := "p"
	t.Cleanup(func() {
		c, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		for _, query := range []string{
			"DELETE FROM event_outbox WHERE event_id IN (SELECT event_id FROM usage_ledger WHERE tenant_id IN ($1,$2))",
			"DELETE FROM usage_ledger WHERE tenant_id IN ($1,$2)",
		} {
			if _, err := db.ExecContext(c, query, tenant, tenant+"-other"); err != nil {
				t.Errorf("cleanup: %v", err)
			}
		}
	})
	for i, row := range []struct {
		tenant, project string
		at time.Time
	}{
		{tenant, project, start},
		{tenant, project, start.Add(time.Hour)},
		{tenant, project, start.Add(3*time.Hour)},
		{tenant, project, end},
		{tenant, project, start.Add(-time.Second)},
		{tenant, "other", start},
		{tenant+"-other", project, start},
	} {
		e, err := event.SyntheticUsageEvent(tenant, i, start)
		if err != nil { t.Fatal(err) }
		e.TenantID, e.ProjectID, e.Timestamp = row.tenant, row.project, row.at
		e.PromptTokens, e.CompletionTokens = 10, 3
		if err := e.Seal(); err != nil { t.Fatal(err) }
		if out, err := s.Record(ctx,e); err != nil || out != Created { t.Fatalf("record: %s %v",out,err) }
		if out, err := s.Record(ctx,e); err != nil || out != Duplicate { t.Fatalf("retry: %s %v",out,err) }
	}
	got, err := s.UsageSeries(ctx, tenant, project, start, end, "hour")
	if err != nil { t.Fatal(err) }
	if len(got) != 4 { t.Fatalf("buckets: %+v",got) }
	for i, row := range got {
		if !row.BucketStart.Equal(start.Add(time.Duration(i)*time.Hour)) { t.Fatalf("bucket %d: %+v", i,row) }
		want := UsageSummary{RequestCount:1, PromptTokens:"10", CompletionTokens:"3", TotalTokens:"13"}
		if i == 2 { want = UsageSummary{PromptTokens:"0", CompletionTokens:"0", TotalTokens:"0"} }
		if row.UsageSummary != want { t.Fatalf("bucket %d: %+v, want %+v",i,row,want) }
	}
	dayStart := start.Truncate(24*time.Hour)
	days, err := s.UsageSeries(ctx,tenant,project,dayStart,dayStart.Add(48*time.Hour),"day")
	if err != nil || len(days) != 2 { t.Fatalf("days: %+v %v",days,err) }
	// The wider window includes the two fixtures outside the hourly window.
	if days[0].RequestCount != 2 || days[1].RequestCount != 3 { t.Fatalf("days: %+v",days) }
}

func TestUsageSeriesRejectsInvalidWindows(t *testing.T) {
	start := time.Date(2026,11,21,0,0,0,0,time.UTC)
	for _, tc := range []struct{from,to time.Time; granularity string}{
		{start,start.Add(time.Hour),"minute"},
		{start.Add(time.Minute),start.Add(time.Hour),"hour"},
		{start,start.Add(time.Hour),"day"},
		{start,start.Add(32*24*time.Hour),"day"},
	} {
		if _, err := usageSeriesStep(tc.from,tc.to,tc.granularity); err == nil { t.Fatalf("accepted %+v",tc) }
	}
}

func TestUsageSeriesFullEmptyWindowAndOffsetEquivalence(t *testing.T) {
	db := testDB(t)
	s := &Store{DB: db}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	from := time.Date(2026, 11, 21, 8, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))
	to := from.Add(MaxUsageWindow)
	tenant := "m12-empty-" + rand.Text()
	for _, tc := range []struct{
		granularity string
		step time.Duration
		count int
	}{
		{"hour", time.Hour, 744},
		{"day", 24*time.Hour, 31},
	} {
		rows, err := s.UsageSeries(ctx, tenant, "p", from, to, tc.granularity)
		if err != nil || len(rows) != tc.count {
			t.Fatalf("%s: count=%d err=%v",tc.granularity,len(rows),err)
		}
		for i, row := range rows {
			want := from.UTC().Add(time.Duration(i)*tc.step)
			if row.BucketStart.Location() != time.UTC || !row.BucketStart.Equal(want) ||
				row.UsageSummary != (UsageSummary{PromptTokens:"0", CompletionTokens:"0", TotalTokens:"0"}) {
				t.Fatalf("%s bucket %d: %+v",tc.granularity,i,row)
			}
		}
	}
}
