package genaiobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func durableTestObservation(tenant, trace string, tokens int64) Observation {
	return Observation{
		SchemaVersion:Version,TenantID:tenant,TraceID:trace,SpanID:"0123456789abcdef",
		Provider:"openai",Operation:"chat",Status:"ok",InputTokens:&tokens,
	}
}

func openDurableTestStore(t *testing.T, capacity int) (*DurableStore,*sql.DB) {
	t.Helper()
	dsn:=os.Getenv("TEST_POSTGRES_DSN")
	if dsn=="" { t.Skip("TEST_POSTGRES_DSN not set") }
	db,err:=sql.Open("pgx",dsn);if err!=nil { t.Fatal(err) }
	if err=db.Ping();err!=nil { db.Close();t.Fatal(err) }
	store,err:=NewDurableStore(db,capacity);if err!=nil { db.Close();t.Fatal(err) }
	t.Cleanup(func(){
		_,_=db.Exec("DELETE FROM genai_observations WHERE tenant_id LIKE 'durable-test-%'")
		_,_=db.Exec("DELETE FROM genai_observation_receipts WHERE tenant_id LIKE 'durable-test-%'")
		_,_=db.Exec(`UPDATE genai_observation_queue_state
SET queue_items=(SELECT count(*) FROM genai_observations),
    receipt_items=(SELECT count(*) FROM genai_observation_receipts),
    capacity_limit=0
WHERE singleton=true`)
		_ = db.Close()
	})
	return store,db
}

func TestDurableStoreAdmissionIsBoundedAtomicAndIdempotent(t *testing.T) {
	store,_:=openDurableTestStore(t,2);ctx:=context.Background()
	tenant:=fmt.Sprintf("durable-test-%d",time.Now().UnixNano())
	a:=durableTestObservation(tenant,"0123456789abcdef0123456789abcdef",11)
	b:=durableTestObservation(tenant,"1123456789abcdef0123456789abcdef",12)
	c:=durableTestObservation(tenant,"2123456789abcdef0123456789abcdef",13)
	if err:=store.EnqueueBatch(ctx,[]Observation{a,b});err!=nil { t.Fatal(err) }
	if err:=store.EnqueueBatch(ctx,[]Observation{a,b});err!=nil { t.Fatalf("idempotent replay: %v",err) }
	if n,err:=store.Len(ctx);err!=nil||n!=2 { t.Fatalf("len=%d err=%v",n,err) }
	var queueItems,receiptItems,capacity int64
	if err:=store.DB.QueryRowContext(ctx,"SELECT queue_items,receipt_items,capacity_limit FROM genai_observation_queue_state WHERE singleton=true").Scan(&queueItems,&receiptItems,&capacity);err!=nil || queueItems!=2 || receiptItems!=2 || capacity!=2 {
		t.Fatalf("counter state queue=%d receipts=%d capacity=%d err=%v",queueItems,receiptItems,capacity,err)
	}
	conflict:=a;changed:=int64(999);conflict.InputTokens=&changed
	if err:=store.Enqueue(ctx,conflict);!errors.Is(err,ErrObservationConflict) { t.Fatalf("conflicting identity: %v",err) }
	if err:=store.EnqueueBatch(ctx,[]Observation{c});!errors.Is(err,ErrDurableQueueFull) { t.Fatalf("expected full queue: %v",err) }
	if n,err:=store.Len(ctx);err!=nil||n!=2 { t.Fatalf("failed batch partially admitted: len=%d err=%v",n,err) }
}

func TestDurableStoreLeaseFencingRestartRecoveryAndAckReceipt(t *testing.T) {
	store,db:=openDurableTestStore(t,8);ctx:=context.Background()
	tenant:=fmt.Sprintf("durable-test-%d",time.Now().UnixNano())
	o:=durableTestObservation(tenant,"3123456789abcdef0123456789abcdef",21)
	if err:=store.Enqueue(ctx,o);err!=nil { t.Fatal(err) }
	first,err:=store.Claim(ctx,"worker-a",time.Minute,1);if err!=nil||len(first)!=1 { t.Fatalf("first claim=%d err=%v",len(first),err) }
	if first[0].Attempts!=1 { t.Fatalf("attempts=%d",first[0].Attempts) }
	if next,err:=store.Claim(ctx,"worker-b",time.Minute,1);err!=nil||len(next)!=0 { t.Fatalf("live lease stolen: %d %v",len(next),err) }
	// Simulate process death; a replacement process may claim after lease expiry.
	if _,err:=db.ExecContext(ctx,"UPDATE genai_observations SET lease_until=now()-interval '1 second' WHERE observation_id=$1",first[0].ID);err!=nil { t.Fatal(err) }
	second,err:=store.Claim(ctx,"worker-b",time.Minute,1);if err!=nil||len(second)!=1 { t.Fatalf("recovery claim=%d err=%v",len(second),err) }
	if second[0].ID!=first[0].ID||second[0].Attempts!=2 { t.Fatalf("recovery did not advance fencing generation: first=%+v second=%+v",first[0],second[0]) }
	if err:=store.Ack(ctx,first[0],"worker-a");!errors.Is(err,ErrDurableLeaseLost) { t.Fatalf("stale ACK not fenced: %v",err) }
	if err:=store.Ack(ctx,second[0],"worker-b");err!=nil { t.Fatal(err) }
	if n,err:=store.Len(ctx);err!=nil||n!=0 { t.Fatalf("ACK did not remove row: %d %v",n,err) }
	var queueItems,receiptItems int64
	if err:=store.DB.QueryRowContext(ctx,"SELECT queue_items,receipt_items FROM genai_observation_queue_state WHERE singleton=true").Scan(&queueItems,&receiptItems);err!=nil || queueItems!=0 || receiptItems!=1 {
		t.Fatalf("ACK counter state queue=%d receipts=%d err=%v",queueItems,receiptItems,err)
	}
	// The 24-hour receipt prevents an acknowledged OTLP replay from being re-enqueued.
	if err:=store.Enqueue(ctx,o);err!=nil { t.Fatal(err) }
	if n,err:=store.Len(ctx);err!=nil||n!=0 { t.Fatalf("ACK receipt did not deduplicate replay: %d %v",n,err) }
}

func TestDurableStoreRetryTerminalAndExplicitOperatorActions(t *testing.T) {
	store,db:=openDurableTestStore(t,8);ctx:=context.Background()
	tenant:=fmt.Sprintf("durable-test-%d",time.Now().UnixNano())
	o:=durableTestObservation(tenant,"4123456789abcdef0123456789abcdef",31)
	if err:=store.Enqueue(ctx,o);err!=nil { t.Fatal(err) }
	first,err:=store.Claim(ctx,"worker-a",time.Minute,1);if err!=nil||len(first)!=1 { t.Fatalf("claim=%d %v",len(first),err) }
	if err:=store.Retry(ctx,first[0],"worker-a",time.Minute,"export_failed");err!=nil { t.Fatal(err) }
	if next,err:=store.Claim(ctx,"worker-b",time.Minute,1);err!=nil||len(next)!=0 { t.Fatalf("retry ignored persisted backoff: %d %v",len(next),err) }
	if _,err:=db.ExecContext(ctx,"UPDATE genai_observations SET available_at=now()-interval '1 second' WHERE observation_id=$1",first[0].ID);err!=nil { t.Fatal(err) }
	second,err:=store.Claim(ctx,"worker-b",time.Minute,1);if err!=nil||len(second)!=1||second[0].Attempts!=2 { t.Fatalf("retry claim=%+v err=%v",second,err) }
	if err:=store.MarkTerminal(ctx,second[0],"worker-b","retry_exhausted");err!=nil { t.Fatal(err) }
	terminal,err:=store.ListTerminal(ctx,tenant,10)
	if err!=nil||len(terminal)!=1||terminal[0].ID!=second[0].ID||terminal[0].Reason!="retry_exhausted" {
		t.Fatalf("terminal operator listing: %+v %v",terminal,err)
	}
	if next,err:=store.Claim(ctx,"worker-c",time.Minute,1);err!=nil||len(next)!=0 { t.Fatalf("terminal row auto-claimed: %d %v",len(next),err) }
	if err:=store.RetryFailed(ctx,second[0].ID);err!=nil { t.Fatal(err) }
	third,err:=store.Claim(ctx,"worker-c",time.Minute,1);if err!=nil||len(third)!=1||third[0].Attempts!=1 { t.Fatalf("manual retry did not reset: %+v %v",third,err) }
	if err:=store.MarkTerminal(ctx,third[0],"worker-c","permanent_export_failure");err!=nil { t.Fatal(err) }
	dropped,err:=store.DropFailed(ctx,third[0].ID);if err!=nil||dropped.TenantID!=tenant { t.Fatalf("explicit drop: %+v %v",dropped,err) }
	if _,err:=store.DropFailed(ctx,third[0].ID);!errors.Is(err,ErrNoFailedObservation) { t.Fatalf("second drop: %v",err) }
	if n,err:=store.Len(ctx);err!=nil||n!=0 { t.Fatalf("terminal drop len=%d err=%v",n,err) }
	if err:=store.Enqueue(ctx,o);err!=nil { t.Fatal(err) }
	if n,err:=store.Len(ctx);err!=nil||n!=0 { t.Fatalf("dropped receipt did not deduplicate replay: %d %v",n,err) }
}

type durableWorkerRepoFake struct {
	delivery Delivery
	claimed bool
	acked bool
	retried bool
	terminal string
	released bool
}
func (f *durableWorkerRepoFake) Claim(_ context.Context,_ string,_ time.Duration,_ int)([]Delivery,error) {
	if f.claimed { return nil,nil };f.claimed=true;return []Delivery{f.delivery},nil
}
func (f *durableWorkerRepoFake) Ack(_ context.Context,_ Delivery,_ string)error { f.acked=true;return nil }
func (f *durableWorkerRepoFake) Retry(_ context.Context,_ Delivery,_ string,_ time.Duration,_ string)error { f.retried=true;return nil }
func (f *durableWorkerRepoFake) MarkTerminal(_ context.Context,_ Delivery,_ string,reason string)error { f.terminal=reason;return nil }
func (f *durableWorkerRepoFake) Release(_ context.Context,_ Delivery,_ string)error { f.released=true;return nil }

type durableWorkerExporterFake struct { err error; seen []Delivery }
func (f *durableWorkerExporterFake) Export(_ context.Context,d Delivery)error { f.seen=append(f.seen,d);return f.err }

func TestDurableWorkerUsesPersistentAttemptCountAndStableDeliveryID(t *testing.T) {
	o:=durableTestObservation("durable-test-worker","5123456789abcdef0123456789abcdef",41)
	repo:=&durableWorkerRepoFake{delivery:Delivery{ID:77,Observation:o,Attempts:2}}
	exp:=&durableWorkerExporterFake{err:errors.New("temporary")}
	policy:=RetryPolicy{MaxAttempts:2,InitialBackoff:time.Second,MaxBackoff:time.Minute,Jitter:0}
	w,err:=NewDurableWorker(repo,exp,"worker-a",time.Minute,policy);if err!=nil { t.Fatal(err) }
	delivered,err:=w.RunOnce(context.Background())
	if delivered||!errors.Is(err,ErrRetryExhausted)||repo.terminal!="retry_exhausted" { t.Fatalf("delivered=%v err=%v terminal=%q",delivered,err,repo.terminal) }
	if len(exp.seen)!=1||exp.seen[0].ID!=77||exp.seen[0].IdempotencyKey()!="genaiobs/77" { t.Fatalf("unstable delivery identity: %+v",exp.seen) }
}

func TestDurableOTLPHTTPAdmissionSurvivesWorkerRestart(t *testing.T) {
	store,db:=openDurableTestStore(t,8)
	tenant:=fmt.Sprintf("durable-test-%d",time.Now().UnixNano())
	const token="durable-test-http-token-0123456789abcdef"
	handler,err:=NewDurableOTLPHTTPHandler(store,[]OTLPCredential{{Token:token,TenantID:tenant}},2)
	if err!=nil { t.Fatal(err) }
	server:=httptest.NewTLSServer(handler)
	defer server.Close()
	client:=server.Client()
	const wire=`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"6123456789abcdef0123456789abcdef","spanId":"0123456789abcdef","status":{"code":1},"attributes":[{"key":"gen_ai.provider.name","value":{"stringValue":"openai"}},{"key":"gen_ai.operation.name","value":{"stringValue":"chat"}},{"key":"gen_ai.usage.input_tokens","value":{"intValue":"7"}}]}]}]}]}`
	post:=func() int {
		t.Helper()
		req,err:=http.NewRequest(http.MethodPost,server.URL+"/v1/traces",strings.NewReader(wire))
		if err!=nil { t.Fatal(err) }
		req.Header.Set("Content-Type","application/json")
		req.Header.Set("Authorization","Bearer "+token)
		resp,err:=client.Do(req);if err!=nil { t.Fatal(err) }
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if status:=post();status!=http.StatusOK { t.Fatalf("durable admission status=%d",status) }
	// A fresh store object models a replacement process using the same database.
	restarted,err:=NewDurableStore(db,8);if err!=nil { t.Fatal(err) }
	rows,err:=restarted.Claim(context.Background(),"replacement-worker",time.Minute,1)
	if err!=nil||len(rows)!=1 { t.Fatalf("recovered rows=%d err=%v",len(rows),err) }
	if rows[0].Observation.TenantID!=tenant||rows[0].Observation.InputTokens==nil||*rows[0].Observation.InputTokens!=7 {
		t.Fatalf("recovered wrong trusted projection: %+v",rows[0])
	}
	if err:=restarted.Ack(context.Background(),rows[0],"replacement-worker");err!=nil { t.Fatal(err) }
	// Replaying the same authenticated OTLP span is accepted idempotently but
	// does not recreate the already-ACKed queue row within the receipt window.
	if status:=post();status!=http.StatusOK { t.Fatalf("replay admission status=%d",status) }
	if n,err:=restarted.Len(context.Background());err!=nil||n!=0 { t.Fatalf("replay recreated queue row: %d %v",n,err) }
}

func TestDurableStoreCapacityIsGlobalAcrossConcurrentWriters(t *testing.T) {
	store,db:=openDurableTestStore(t,2)
	other,err:=NewDurableStore(db,2);if err!=nil { t.Fatal(err) }
	tenant:=fmt.Sprintf("durable-test-%d",time.Now().UnixNano())
	const writers=12
	var wg sync.WaitGroup
	var mu sync.Mutex
	admitted,full,unexpected:=0,0,[]error{}
	for i:=0;i<writers;i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			trace:=fmt.Sprintf("%032x",i+1)
			err:=[]*DurableStore{store,other}[i%2].Enqueue(context.Background(),durableTestObservation(tenant,trace,int64(i)))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err==nil: admitted++
			case errors.Is(err,ErrDurableQueueFull): full++
			default: unexpected=append(unexpected,err)
			}
		}(i)
	}
	wg.Wait()
	if len(unexpected)>0 { t.Fatalf("unexpected concurrent admission errors: %v",unexpected) }
	if admitted!=2||full!=writers-2 { t.Fatalf("admitted=%d full=%d; capacity was not globally fenced",admitted,full) }
	if n,err:=store.Len(context.Background());err!=nil||n!=2 { t.Fatalf("durable count=%d err=%v",n,err) }
}

type failingObservationAdmitter struct { err error }
func (f failingObservationAdmitter) MaxBatchSize() int { return MaxBatchSize }
func (f failingObservationAdmitter) AdmitBatch(context.Context,Scope,string,[]Span) error { return f.err }

func TestOTLPHTTPDurableBackendFailureReturnsRetryable503(t *testing.T) {
	const token="durable-test-http-token-0123456789abcdef"
	handler,err:=NewOTLPHTTPHandlerWithAdmitter(failingObservationAdmitter{err:errors.New("database unavailable")},[]OTLPCredential{{Token:token,TenantID:"durable-test-http"}},1)
	if err!=nil { t.Fatal(err) }
	server:=httptest.NewTLSServer(handler)
	defer server.Close()
	req,err:=http.NewRequest(http.MethodPost,server.URL+"/v1/traces",strings.NewReader(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"7123456789abcdef0123456789abcdef","spanId":"1123456789abcdef","status":{"code":1},"attributes":[{"key":"gen_ai.provider.name","value":{"stringValue":"openai"}},{"key":"gen_ai.operation.name","value":{"stringValue":"chat"}}]}]}]}]}`))
	if err!=nil { t.Fatal(err) }
	req.Header.Set("Content-Type","application/json")
	req.Header.Set("Authorization","Bearer "+token)
	resp,err:=server.Client().Do(req);if err!=nil { t.Fatal(err) }
	defer resp.Body.Close()
	if resp.StatusCode!=http.StatusServiceUnavailable||resp.Header.Get("Retry-After")!="1" {
		t.Fatalf("backend failure must be retryable 503, got %d retry-after=%q",resp.StatusCode,resp.Header.Get("Retry-After"))
	}
}

func TestDurableWorkerAcknowledgesOnlyAfterExporterSuccess(t *testing.T) {
	o:=durableTestObservation("durable-test-worker","8123456789abcdef0123456789abcdef",51)
	repo:=&durableWorkerRepoFake{delivery:Delivery{ID:88,Observation:o,Attempts:1}}
	exp:=&durableWorkerExporterFake{}
	w,err:=NewDurableWorker(repo,exp,"worker-ack",time.Minute,workerTestPolicy())
	if err!=nil { t.Fatal(err) }
	delivered,err:=w.RunOnce(context.Background())
	if !delivered||err!=nil||!repo.acked||repo.retried||repo.terminal!="" {
		t.Fatalf("delivered=%v err=%v repo=%+v",delivered,err,repo)
	}
}

func TestDurableWorkerPermanentFailureRequiresTerminalAction(t *testing.T) {
	o:=durableTestObservation("durable-test-worker","9123456789abcdef0123456789abcdef",61)
	repo:=&durableWorkerRepoFake{delivery:Delivery{ID:99,Observation:o,Attempts:1}}
	exp:=&durableWorkerExporterFake{err:Permanent(errors.New("secret endpoint detail"))}
	w,err:=NewDurableWorker(repo,exp,"worker-terminal",time.Minute,workerTestPolicy())
	if err!=nil { t.Fatal(err) }
	if delivered,err:=w.RunOnce(context.Background());delivered||!errors.Is(err,ErrPermanentFailure) {
		t.Fatalf("delivered=%v err=%v",delivered,err)
	}
	if repo.terminal!="permanent_export_failure"||repo.acked||repo.retried {
		t.Fatalf("permanent failure did not become terminal: %+v",repo)
	}
}

type cancelingDurableExporter struct { cancel context.CancelFunc }
func (e cancelingDurableExporter) Export(ctx context.Context,_ Delivery) error {
	e.cancel()
	return ctx.Err()
}

func TestDurableWorkerReleasesLeaseWhenExportContextIsCanceled(t *testing.T) {
	o:=durableTestObservation("durable-test-worker","a123456789abcdef0123456789abcdef",71)
	repo:=&durableWorkerRepoFake{delivery:Delivery{ID:101,Observation:o,Attempts:1}}
	ctx,cancel:=context.WithCancel(context.Background())
	w,err:=NewDurableWorker(repo,cancelingDurableExporter{cancel:cancel},"worker-cancel",time.Minute,workerTestPolicy())
	if err!=nil { t.Fatal(err) }
	if delivered,err:=w.RunOnce(ctx);delivered||!errors.Is(err,context.Canceled) {
		t.Fatalf("delivered=%v err=%v",delivered,err)
	}
	if !repo.released||repo.acked||repo.retried { t.Fatalf("canceled export lease not released: %+v",repo) }
}
