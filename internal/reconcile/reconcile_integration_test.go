package reconcile

import (
    "context"
    "database/sql"
    "fmt"
    "io"
    "net/http"
    "os"
    "strings"
    "testing"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/clickhouse"
    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
    "github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
    _ "github.com/jackc/pgx/v5/stdlib"
)

// TestRealDatabasesReconcileAllStatuses is deliberately backed by both
// databases. It leaves synthetic fixtures in short-lived CI Docker volumes
// so the CLI can independently assert the machine-readable report.
func TestRealDatabasesReconcileAllStatuses(t *testing.T){
    dsn,endpoint:=os.Getenv("TEST_POSTGRES_DSN"),os.Getenv("TEST_CLICKHOUSE_HTTP")
    if dsn==""||endpoint==""{t.Skip("requires isolated PostgreSQL and ClickHouse")}
    db,err:=sql.Open("pgx",dsn);if err!=nil{t.Fatal(err)}
    defer db.Close()
    ctx,cancel:=context.WithTimeout(context.Background(),60*time.Second);defer cancel()
    if err:=db.PingContext(ctx);err!=nil{t.Fatal(err)}
    start:=time.Date(2026,11,16,11,0,0,0,time.UTC)
    seed:=fmt.Sprintf("m3c-ci-%d",time.Now().UnixNano())
    var events []event.UsageEvent
    for i:=0;i<8;i++{
        e,err:=event.SyntheticUsageEvent(seed,i,start)
        if err!=nil{t.Fatal(err)}
        events=append(events,e)
    }
    led:=&ledger.Store{DB:db}
    for i:=0;i<7;i++{
        if _,err:=led.Record(ctx,events[i]);err!=nil{t.Fatal(err)}
    }
    now:=time.Now().UTC()
    for _,i:=range []int{0,1,2,3,6}{
        if _,err:=db.ExecContext(ctx,`UPDATE event_outbox SET published_at=$2 WHERE event_id=$1::uuid`,
            events[i].EventID,now.Add(-10*time.Minute));err!=nil{t.Fatal(err)}
    }
    if _,err:=db.ExecContext(ctx,`UPDATE event_outbox SET published_at=$2 WHERE event_id=$1::uuid`,
        events[5].EventID,now.Add(-time.Minute));err!=nil{t.Fatal(err)}

    writer:=&clickhouse.HTTPWriter{Endpoint:endpoint,Database:"telemetry",User:"telemetry",Password:"localdev-only"}
    for _,i:=range []int{0,1,1,2,7}{
        if err:=writer.Insert(ctx,[]event.UsageEvent{events[i]});err!=nil{t.Fatal(err)}
    }
    // Inject one forged raw row with the same ID but a different hash.
    unsafeInsert:=func(e event.UsageEvent,hash string,tokenIncrement int64){
        t.Helper()
        stmt:=fmt.Sprintf(`INSERT INTO inference_events_raw
(event_id,event_time,tenant_id,project_id,request_id,model,
prompt_tokens,completion_tokens,latency_ms,status,payload_hash)
VALUES (toUUID('%s'),fromUnixTimestamp64Milli(%d),
'%s','%s','%s','%s',%d,%d,%d,'%s',toFixedString('%s',64))`,
            e.EventID,e.Timestamp.UnixMilli(),e.TenantID,e.ProjectID,e.RequestID,e.Model,
            e.PromptTokens+tokenIncrement,e.CompletionTokens,e.LatencyMS,e.Status,hash)
        req,err:=http.NewRequestWithContext(ctx,http.MethodPost,endpoint+"?database=telemetry",strings.NewReader(stmt))
        if err!=nil{t.Fatal(err)}
        req.SetBasicAuth("telemetry","localdev-only")
        resp,err:=http.DefaultClient.Do(req)
        if err!=nil{t.Fatal(err)}
        defer resp.Body.Close()
        b,err:=io.ReadAll(io.LimitReader(resp.Body,4096))
        if err!=nil||resp.StatusCode!=200{t.Fatalf("ClickHouse test injection status=%d body=%s err=%v",resp.StatusCode,b,err)}
    }
    unsafeInsert(events[2],strings.Repeat("f",64),1)
    // Same stored hash but wrong token count: this must still be detected.
    unsafeInsert(events[6],events[6].PayloadHash,1)
    o:=Options{Start:start,End:start.Add(time.Minute),Now:now,Grace:3*time.Minute,MaxRecords:100}
    r,err:=Run(ctx,o,PostgreSQL{DB:db},ClickHouseHTTP{
        Endpoint:endpoint,Database:"telemetry",User:"telemetry",Password:"localdev-only",
    })
    if err!=nil{t.Fatal(err)}
    if r.Result!="FAIL"||r.LedgerEvents!=7||r.AnalyticsEvents!=5{
        t.Fatalf("unexpected reconciled population %+v",r)
    }
    want:=map[Status]int{
        StatusMatched:1,StatusReplayed:1,StatusConflict:1,StatusMissing:1,
        StatusPending:1,StatusInFlight:1,StatusMismatch:1,StatusOrphan:1,
    }
    for status,count:=range want{
        if r.Counts[status]!=count{t.Errorf("status %s got %d want %d",status,r.Counts[status],count)}
    }
    if r.RawDuplicateCopies!=2{t.Fatalf("physical duplicate count=%d expected 2",r.RawDuplicateCopies)}
    t.Logf("PASS real PostgreSQL/ClickHouse comparison: result=%s ledger=%d analytics=%d counts=%v",
        r.Result,r.LedgerEvents,r.AnalyticsEvents,r.Counts)
}
