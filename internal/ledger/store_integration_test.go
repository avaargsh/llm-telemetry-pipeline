package ledger

import (
    "context"
    "database/sql"
    "errors"
    "os"
    "sync"
    "testing"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
    _ "github.com/jackc/pgx/v5/stdlib"
)

func testDB(t *testing.T)*sql.DB{
    t.Helper()
    dsn:=os.Getenv("TEST_POSTGRES_DSN")
    if dsn=="" {t.Skip("TEST_POSTGRES_DSN not set; PostgreSQL integration test")}
    db,err:=sql.Open("pgx",dsn);if err!=nil{t.Fatal(err)}
    ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel()
    if err:=db.PingContext(ctx);err!=nil{t.Fatal(err)}
    cleanup := func() {
        // Isolate repeated test executions without touching non-test usage.
        db.Exec("DELETE FROM event_outbox WHERE event_id IN (SELECT event_id FROM usage_ledger WHERE request_id LIKE 'ledger-%')")
        db.Exec("DELETE FROM usage_ledger WHERE request_id LIKE 'ledger-%'")
    }
    cleanup()
    t.Cleanup(func(){cleanup();db.Close()})
    return db
}
func freshEvent(t *testing.T,seed string)event.UsageEvent{
    t.Helper()
    e,err:=event.SyntheticUsageEvent(seed,1,time.Date(2026,11,16,11,0,0,0,time.UTC))
    if err!=nil{t.Fatal(err)}
    return e
}
func counts(t *testing.T, db *sql.DB, id string)(int,int){
    t.Helper()
    var ledgerRows,outboxRows int
    if err:=db.QueryRow("SELECT count(*) FROM usage_ledger WHERE event_id=$1::uuid",id).Scan(&ledgerRows);err!=nil{t.Fatal(err)}
    if err:=db.QueryRow("SELECT count(*) FROM event_outbox WHERE event_id=$1::uuid",id).Scan(&outboxRows);err!=nil{t.Fatal(err)}
    return ledgerRows,outboxRows
}

func TestRecordAtomicIdempotence(t *testing.T){
    db:=testDB(t);s:=Store{DB:db};ctx:=context.Background()
    e:=freshEvent(t,"ledger-idempotence")
    result,err:=s.Record(ctx,e);if err!=nil||result!=Created{t.Fatalf("create=%v err=%v",result,err)}
    result,err=s.Record(ctx,e);if err!=nil||result!=Duplicate{t.Fatalf("retry=%v err=%v",result,err)}
    l,o:=counts(t,db,e.EventID);if l!=1||o!=1{t.Fatalf("atomic count ledger=%d outbox=%d",l,o)}
}

func TestRecordRejectsEventIDConflict(t *testing.T){
    db:=testDB(t);s:=Store{DB:db};ctx:=context.Background()
    e:=freshEvent(t,"ledger-conflict-event")
    if _,err:=s.Record(ctx,e);err!=nil{t.Fatal(err)}
    changed:=e;changed.Model="other-model";if err:=changed.Seal();err!=nil{t.Fatal(err)}
    if _,err:=s.Record(ctx,changed);!errors.Is(err,ErrConflict){t.Fatalf("wanted ErrConflict, got %v",err)}
    l,o:=counts(t,db,e.EventID);if l!=1||o!=1{t.Fatalf("conflict changed storage: %d,%d",l,o)}
}

func TestRecordRejectsDifferentEventIDForSameRequest(t *testing.T){
    db:=testDB(t);s:=Store{DB:db};ctx:=context.Background()
    e:=freshEvent(t,"ledger-conflict-request")
    if _,err:=s.Record(ctx,e);err!=nil{t.Fatal(err)}
    changed:=e;changed.EventID="123e4567-e89b-42d3-a456-426614174888"
    if err:=changed.Seal();err!=nil{t.Fatal(err)}
    if _,err:=s.Record(ctx,changed);!errors.Is(err,ErrConflict){t.Fatalf("wanted ErrConflict, got %v",err)}
    l,o:=counts(t,db,changed.EventID);if l!=0||o!=0{t.Fatalf("conflicting ID inserted: %d,%d",l,o)}
}

func TestRecordConcurrentDuplicates(t *testing.T){
    db:=testDB(t);s:=Store{DB:db};e:=freshEvent(t,"ledger-concurrent")
    var wg sync.WaitGroup
    results:=make(chan Outcome,12);errs:=make(chan error,12)
    for i:=0;i<12;i++{wg.Add(1);go func(){defer wg.Done();r,err:=s.Record(context.Background(),e);results<-r;errs<-err}()}
    wg.Wait();close(results);close(errs)
    creates,duplicates:=0,0
    for err:=range errs {if err!=nil{t.Fatal(err)}}
    for r:=range results {if r==Created{creates++} else if r==Duplicate{duplicates++}}
    if creates!=1||duplicates!=11{t.Fatalf("creates=%d duplicates=%d",creates,duplicates)}
    l,o:=counts(t,db,e.EventID);if l!=1||o!=1{t.Fatalf("concurrent counts=%d,%d",l,o)}
}

func TestRecordRollsBackWhenOutboxInsertFails(t *testing.T){
    db:=testDB(t);ctx:=context.Background()
    _,err:=db.ExecContext(ctx,`
CREATE OR REPLACE FUNCTION m2_test_reject_ledger_outbox() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF (NEW.event_payload->>'request_id') LIKE 'ledger-rollback%' THEN
     RAISE EXCEPTION 'forced outbox failure for transactional rollback test';
  END IF;
  RETURN NEW;
END;$$`)
    if err!=nil{t.Fatal(err)}
    if _,err:=db.ExecContext(ctx,`CREATE TRIGGER m2_test_reject_outbox_insert
BEFORE INSERT ON event_outbox FOR EACH ROW
EXECUTE FUNCTION m2_test_reject_ledger_outbox()`);err!=nil{t.Fatal(err)}
    t.Cleanup(func(){
        db.Exec("DROP TRIGGER IF EXISTS m2_test_reject_outbox_insert ON event_outbox")
        db.Exec("DROP FUNCTION IF EXISTS m2_test_reject_ledger_outbox()")
    })
    e:=freshEvent(t,"ledger-rollback")
    if _,err:=(&Store{DB:db}).Record(ctx,e);err==nil{t.Fatal("expected forced Outbox insert failure")}
    a,b:=counts(t,db,e.EventID)
    if a!=0||b!=0{t.Fatalf("transaction was partially committed: ledger=%d outbox=%d",a,b)}
}
