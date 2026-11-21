package ingestapi

import (
    "bytes"
    "context"
    "database/sql"
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "os"
    "testing"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
    "github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
    _ "github.com/jackc/pgx/v5/stdlib"
)

// This test uses the real transactional Ledger + Outbox schema; it never
// contacts Kafka. Running without TEST_POSTGRES_DSN only skips this test.
func TestHTTPIngestionUsesOneAtomicLedgerOutboxTransaction(t *testing.T){
    dsn:=os.Getenv("TEST_POSTGRES_DSN")
    if dsn==""{t.Skip("TEST_POSTGRES_DSN not configured")}
    db,err:=sql.Open("pgx",dsn);if err!=nil{t.Fatal(err)};defer db.Close()
    ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel()
    if err:=db.PingContext(ctx);err!=nil{t.Fatal(err)}
    e,err:=event.SyntheticUsageEvent("m5a-integration",0,time.Date(2026,11,21,10,0,0,0,time.UTC));if err!=nil{t.Fatal(err)}
    cleanup:=func(){_,_=db.Exec("DELETE FROM event_outbox WHERE event_id=$1::uuid",e.EventID);_,_=db.Exec("DELETE FROM usage_ledger WHERE event_id=$1::uuid",e.EventID)}
    cleanup();t.Cleanup(cleanup)
    h,err:=NewHandler(&ledger.Store{DB:db},[]Credential{{Token:testToken,TenantID:e.TenantID,ProjectID:e.ProjectID}})
    if err!=nil{t.Fatal(err)}
    sendReal:=func(e event.UsageEvent)int{
        raw,err:=json.Marshal(e);if err!=nil{t.Fatal(err)}
        req:=httptest.NewRequest(http.MethodPost,"/v1/usage-events",bytes.NewReader(raw))
        req.Header.Set("Authorization","Bearer "+testToken);req.Header.Set("Content-Type","application/json")
        rr:=httptest.NewRecorder();h.ServeHTTP(rr,req);return rr.Code
    }
    if status:=sendReal(e);status!=201{t.Fatalf("first record status %d",status)}
    if status:=sendReal(e);status!=200{t.Fatalf("retry status %d",status)}
    changed:=e;changed.Model="different-model";if err:=changed.Seal();err!=nil{t.Fatal(err)}
    if status:=sendReal(changed);status!=409{t.Fatalf("conflict status %d",status)}
    var ledgerRows,outboxRows int
    if err:=db.QueryRow("SELECT COUNT(*) FROM usage_ledger WHERE event_id=$1::uuid",e.EventID).Scan(&ledgerRows);err!=nil{t.Fatal(err)}
    if err:=db.QueryRow("SELECT COUNT(*) FROM event_outbox WHERE event_id=$1::uuid",e.EventID).Scan(&outboxRows);err!=nil{t.Fatal(err)}
    if ledgerRows!=1||outboxRows!=1{t.Fatalf("expected one ledger + outbox, got ledger=%d outbox=%d",ledgerRows,outboxRows)}
}
