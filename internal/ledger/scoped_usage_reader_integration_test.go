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

// This integration test uses an actual separately authenticated database
// connection, not SET ROLE or a simulated session identity. Only run with
// TEST_POSTGRES_DSN against the disposable migration-loaded database.
func TestScopedUsageReaderRealLoginAndRevocation(t *testing.T) {
	admin := testDB(t)
	ctx,cancel := context.WithTimeout(context.Background(),30*time.Second)
	defer cancel()
	suffix := strings.ToLower(rand.Text())
	role := "m12_login_"+suffix
	tenant := "m12-real-"+suffix
	project := "p"
	password := rand.Text()
	roleIdent := pgx.Identifier{role}.Sanitize()
	// rand.Text emits only base32 alphabet characters, suitable for this
	// generated test-only SQL literal. Never use caller input as DDL.
	if _,err := admin.ExecContext(ctx,"CREATE ROLE "+roleIdent+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+password+"'"); err != nil {
		t.Fatal("create disposable login failed")
	}
	t.Cleanup(func(){
		c,stop := context.WithTimeout(context.Background(),15*time.Second)
		defer stop()
		for _,q := range []string{
			"DELETE FROM public.event_outbox WHERE event_id IN (SELECT event_id FROM public.usage_ledger WHERE tenant_id IN ($1,$2))",
			"DELETE FROM public.usage_ledger WHERE tenant_id IN ($1,$2)",
		} {
			if _,err := admin.ExecContext(c,q,tenant,tenant+"-other"); err != nil { t.Errorf("row cleanup: %v",err) }
		}
		if _,err := admin.ExecContext(c,"DELETE FROM public.usage_query_scope_bindings WHERE login_role=$1",role);err != nil { t.Errorf("binding cleanup: %v",err) }
		if _,err := admin.ExecContext(c,"DROP ROLE "+roleIdent);err != nil { t.Errorf("role cleanup: %v",err) }
	})
	if _,err := admin.ExecContext(ctx,"GRANT llm_telemetry_usage_query_reader TO "+roleIdent);err != nil { t.Fatal(err) }
	if _,err := admin.ExecContext(ctx,"INSERT INTO public.usage_query_scope_bindings(login_role,tenant_id,project_id) VALUES ($1,$2,$3)",role,tenant,project);err != nil { t.Fatal(err) }
	start := time.Date(2026,11,21,0,0,0,0,time.UTC)
	end := start.Add(time.Hour)
	writer := &Store{DB:admin}
	for i,scope := range []struct{tenant,project string}{
		{tenant,project},{tenant,"other"},{tenant+"-other",project},
	} {
		e,err := event.SyntheticUsageEvent(tenant,i,start)
		if err != nil { t.Fatal(err) }
		e.TenantID,e.ProjectID,e.Timestamp = scope.tenant,scope.project,start
		e.Model,e.PromptTokens,e.CompletionTokens = "model",10,3
		if err := e.Seal();err != nil { t.Fatal(err) }
		if result,err := writer.Record(ctx,e);err != nil || result != Created { t.Fatalf("record: %v %v",result,err) }
	}
	config,err := pgx.ParseConfig(os.Getenv("TEST_POSTGRES_DSN"))
	if err != nil { t.Fatal("parse fixture DSN failed") }
	config.User,config.Password = role,password
	readerDB := stdlib.OpenDB(*config)
	defer readerDB.Close()
	readerDB.SetMaxOpenConns(2)
	if _,err := NewScopedUsageReader(ctx,admin,tenant,project);!errors.Is(err,ErrUsageQueryScope) { t.Fatalf("admin readiness: %v",err) }
	if _,err := NewScopedUsageReader(ctx,readerDB,tenant,"other");!errors.Is(err,ErrUsageQueryScope) { t.Fatalf("wrong binding readiness: %v",err) }
	reader,err := NewScopedUsageReader(ctx,readerDB,tenant,project)
	if err != nil { t.Fatal(err) }
	want := UsageSummary{RequestCount:1,PromptTokens:"10",CompletionTokens:"3",TotalTokens:"13"}
	summary,err := reader.UsageSummary(ctx,tenant,project,start,end)
	if err != nil || summary != want { t.Fatalf("summary: %+v %v",summary,err) }
	models,err := reader.UsageByModel(ctx,tenant,project,start,end)
	if err != nil || len(models) != 1 || models[0].UsageSummary != want || models[0].Model != "model" { t.Fatalf("models: %+v %v",models,err) }
	series,err := reader.UsageSeries(ctx,tenant,project,start,end,"hour")
	if err != nil || len(series) != 1 || series[0].UsageSummary != want { t.Fatalf("series: %+v %v",series,err) }
	// The event at the rate boundary belongs only to the second window.
	e,err := event.SyntheticUsageEvent(tenant,3,start)
	if err != nil { t.Fatal(err) }
	e.TenantID,e.ProjectID,e.Timestamp = tenant,project,end
	e.Model,e.PromptTokens,e.CompletionTokens = "model",20,5
	if err := e.Seal(); err != nil { t.Fatal(err) }
	if result,err := writer.Record(ctx,e);err != nil || result != Created { t.Fatalf("boundary record: %v %v",result,err) }
	windows := []ModelQueryWindow{{From:start,To:end},{From:end,To:end.Add(time.Hour)},{From:end.Add(time.Hour),To:end.Add(2*time.Hour)}}
	captures,err := reader.UsageByModelWindows(ctx,tenant,project,windows)
	if err != nil || len(captures)!=3 { t.Fatalf("captures: %+v %v",captures,err) }
	if len(captures[0].Models)!=1 || captures[0].Models[0].UsageSummary!=want ||
		len(captures[1].Models)!=1 || captures[1].Models[0].PromptTokens!="20" ||
		captures[2].Models==nil || len(captures[2].Models)!=0 {
		t.Fatalf("wrong window usage: %+v",captures)
	}
	for i,w := range windows {
		if !captures[i].From.Equal(w.From) || !captures[i].To.Equal(w.To) { t.Fatalf("wrong capture bounds: %+v",captures) }
	}
	if got,err := reader.UsageByModelWindows(ctx,tenant,"other",windows);!errors.Is(err,ErrUsageQueryScope)||got!=nil { t.Fatalf("wrong scope: %+v %v",got,err) }
	if got,err := reader.UsageByModelWindows(ctx,tenant,project,[]ModelQueryWindow{{From:start,To:end},{From:end.Add(time.Second),To:end.Add(time.Hour)}});!errors.Is(err,ErrUsageQueryWindows)||got!=nil { t.Fatalf("gap: %+v %v",got,err) }
	if _,err := reader.UsageSummary(ctx,tenant,"other",start,end);!errors.Is(err,ErrUsageQueryScope) { t.Fatalf("wrong requested scope: %v",err) }
	if _,err := admin.ExecContext(ctx,"DELETE FROM public.usage_query_scope_bindings WHERE login_role=$1",role);err != nil { t.Fatal(err) }
	// Reuse the same reader and its pool: revocation is enforced on each
	// new transaction, not only at initialization.
	if _,err := reader.UsageSummary(ctx,tenant,project,start,end);!errors.Is(err,ErrUsageQueryScope) { t.Fatalf("revoked summary: %v",err) }
	if _,err := reader.UsageByModel(ctx,tenant,project,start,end);!errors.Is(err,ErrUsageQueryScope) { t.Fatalf("revoked models: %v",err) }
	if got,err := reader.UsageByModelWindows(ctx,tenant,project,windows);!errors.Is(err,ErrUsageQueryScope)||got!=nil { t.Fatalf("revoked windows: %+v %v",got,err) }
	if _,err := reader.UsageSeries(ctx,tenant,project,start,end,"hour");!errors.Is(err,ErrUsageQueryScope) { t.Fatalf("revoked series: %v",err) }
	if _,err := admin.ExecContext(ctx,"INSERT INTO public.usage_query_scope_bindings(login_role,tenant_id,project_id) VALUES ($1,$2,'other')",role,tenant);err != nil { t.Fatal(err) }
	if _,err := reader.UsageSummary(ctx,tenant,project,start,end);!errors.Is(err,ErrUsageQueryScope) { t.Fatalf("reassigned scope: %v",err) }
}
