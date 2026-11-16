package outbox

import (
    "context"
    "database/sql"
    "errors"
    "os"
    "testing"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
    "github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
    _ "github.com/jackc/pgx/v5/stdlib"
)

func testStore(t *testing.T)(*Store,*sql.DB){
    t.Helper()
    dsn:=os.Getenv("TEST_POSTGRES_DSN");if dsn==""{t.Skip("TEST_POSTGRES_DSN not set")}
    db,err:=sql.Open("pgx",dsn);if err!=nil{t.Fatal(err)}
    if err:=db.Ping();err!=nil{t.Fatal(err)}
    t.Cleanup(func(){db.Close()})
    return &Store{DB:db},db
}
func seed(t *testing.T,db *sql.DB,logical string)event.UsageEvent{
    t.Helper()
    e,err:=event.SyntheticUsageEvent(logical,1,time.Date(2026,11,16,11,0,0,0,time.UTC));if err!=nil{t.Fatal(err)}
    if _,err:=(&ledger.Store{DB:db}).Record(context.Background(),e);err!=nil{t.Fatal(err)}
    return e
}
func TestClaimAndPublishWithLeaseFencing(t *testing.T){
    repo,db:=testStore(t);ctx:=context.Background()
    e:=seed(t,db,"outbox-fence")
    rows,err:=repo.Claim(ctx,"worker-a",30*time.Second,100);if err!=nil{t.Fatal(err)}
    var item *Row
    for i:=range rows {if rows[i].EventID==e.EventID{item=&rows[i];break}}
    if item==nil{t.Fatalf("missing event %s in claim",e.EventID)}
    if err:=repo.MarkPublished(ctx,*item,"worker-b");!errors.Is(err,ErrLeaseLost){t.Fatalf("wrong worker marked published: %v",err)}
    _,err=db.ExecContext(ctx,"UPDATE event_outbox SET lease_until=now()-interval '1 second' WHERE outbox_id=$1",item.ID)
    if err!=nil{t.Fatal(err)}
    others,err:=repo.Claim(ctx,"worker-b",30*time.Second,100);if err!=nil{t.Fatal(err)}
    var reclaimed *Row
    for i:=range others {if others[i].ID==item.ID{reclaimed=&others[i];break}}
    if reclaimed==nil{t.Fatal("expired lease was not reclaimed")}
    if err:=repo.MarkPublished(ctx,*item,"worker-a");!errors.Is(err,ErrLeaseLost){t.Fatalf("stale owner marked published: %v",err)}
    if err:=repo.MarkPublished(ctx,*reclaimed,"worker-b");err!=nil{t.Fatal(err)}
    var published bool
    if err:=db.QueryRowContext(ctx,"SELECT published_at IS NOT NULL FROM event_outbox WHERE event_id=$1::uuid",e.EventID).Scan(&published);err!=nil||!published{t.Fatalf("not marked published: %v",err)}
}
func TestRetryReturnsToPendingAfterDue(t *testing.T){
    repo,db:=testStore(t);ctx:=context.Background()
    e:=seed(t,db,"outbox-retry")
    rows,err:=repo.Claim(ctx,"worker-retry",30*time.Second,100);if err!=nil{t.Fatal(err)}
    var row *Row
    for i:=range rows{if rows[i].EventID==e.EventID{row=&rows[i];break}}
    if row==nil{t.Fatal("not claimed")}
    if err:=repo.ScheduleRetry(ctx,*row,"worker-retry",20*time.Second,"broker unavailable");err!=nil{t.Fatal(err)}
    var eligible bool
    if err:=db.QueryRowContext(ctx,"SELECT next_attempt_at > now() AND lease_until IS NULL AND published_at IS NULL FROM event_outbox WHERE outbox_id=$1",row.ID).Scan(&eligible);err!=nil||!eligible{t.Fatalf("retry not scheduled: %v",err)}
    if _,err:=db.ExecContext(ctx,"UPDATE event_outbox SET next_attempt_at=now()-interval '1 second' WHERE outbox_id=$1",row.ID);err!=nil{t.Fatal(err)}
    after,err:=repo.Claim(ctx,"worker-next",30*time.Second,100);if err!=nil{t.Fatal(err)}
    found:=false;for _,candidate:=range after{if candidate.ID==row.ID {found=true;break}}
    if !found{t.Fatal("retry was not reclaimed after due")}
}
