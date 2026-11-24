package genaiobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type workerScriptExporter struct {
	mu sync.Mutex
	results []error
	seen []Observation
}
func (f *workerScriptExporter) Export(_ context.Context, o Observation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = append(f.seen, o)
	if len(f.results)==0 { return nil }
	err:=f.results[0]
	f.results=f.results[1:]
	return err
}
func (f *workerScriptExporter) calls() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.seen) }
func workerTestObservation() Observation {
	n:=int64(17)
	return Observation{SchemaVersion:Version,TenantID:"tenant-a",TraceID:"0123456789abcdef0123456789abcdef",SpanID:"0123456789abcdef",Provider:"openai",Operation:"chat",Status:"ok",InputTokens:&n}
}
func workerTestPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts:3,InitialBackoff:time.Second,MaxBackoff:4*time.Second,Jitter:0}
}
func workerTestQueue(t *testing.T) *Queue {
	t.Helper()
	q,err:=NewQueue(4);if err!=nil{t.Fatal(err)}
	if err=q.Submit(Scope{TenantID:"tenant-a"},Version,Span{TraceID:"0123456789abcdef0123456789abcdef",SpanID:"0123456789abcdef",Status:"ok",Attributes:[]Attribute{{Key:"gen_ai.provider.name",Value:"openai"},{Key:"gen_ai.operation.name",Value:"chat"},{Key:"gen_ai.usage.input_tokens",Value:int64(17)}}});err!=nil{t.Fatal(err)}
	return q
}
func TestWorkerAcknowledgesOnlyExporterSuccess(t *testing.T) {
	q:=workerTestQueue(t);now:=time.Date(2026,11,24,0,0,0,0,time.UTC)
	exp:=&workerScriptExporter{results:[]error{errors.New("temporary"),nil}}
	w,err:=NewWorker(q,exp,workerTestPolicy());if err!=nil{t.Fatal(err)}
	w.now=func()time.Time{return now}
	if delivered,err:=w.RunOnce(context.Background());delivered||!errors.Is(err,ErrRetryScheduled){t.Fatalf("delivered=%v err=%v",delivered,err)}
	if q.Len()!=1 {t.Fatalf("transient error removed item: %d",q.Len())}
	if _,ok:=q.Pop();ok {t.Fatal("Pop bypassed an active delivery lease")}
	if delivered,err:=w.RunOnce(context.Background());delivered||!errors.Is(err,ErrBackoffActive){t.Fatalf("backoff delivered=%v err=%v",delivered,err)}
	if exp.calls()!=1 {t.Fatalf("export called during backoff: %d",exp.calls())}
	now=now.Add(time.Second)
	if delivered,err:=w.RunOnce(context.Background());!delivered||err!=nil{t.Fatalf("delivered=%v err=%v",delivered,err)}
	if q.Len()!=0||exp.calls()!=2 {t.Fatalf("queue=%d calls=%d",q.Len(),exp.calls())}
}
func TestWorkerExhaustionRequiresExplicitRetryOrDrop(t *testing.T) {
	q:=workerTestQueue(t);now:=time.Now()
	exp:=&workerScriptExporter{results:[]error{errors.New("temporary"),errors.New("temporary"),nil}}
	p:=workerTestPolicy();p.MaxAttempts=2
	w,err:=NewWorker(q,exp,p);if err!=nil{t.Fatal(err)}
	w.now=func()time.Time{return now}
	if _,err=w.RunOnce(context.Background());!errors.Is(err,ErrRetryScheduled){t.Fatal(err)}
	now=now.Add(time.Second)
	if _,err=w.RunOnce(context.Background());!errors.Is(err,ErrRetryExhausted){t.Fatalf("want exhausted, got %v",err)}
	if q.Len()!=1 {t.Fatal("exhausted item was silently dropped")}
	if err=w.RetryFailed();err!=nil{t.Fatal(err)}
	if delivered,err:=w.RunOnce(context.Background());!delivered||err!=nil{t.Fatalf("manual retry: %v %v",delivered,err)}
	if q.Len()!=0 {t.Fatal("manual retry did not ACK")}
}
func TestWorkerPermanentFailureRequiresExplicitDrop(t *testing.T) {
	q:=workerTestQueue(t);exp:=&workerScriptExporter{results:[]error{Permanent(errors.New("invalid destination"))}}
	w,err:=NewWorker(q,exp,workerTestPolicy());if err!=nil{t.Fatal(err)}
	if _,err=w.RunOnce(context.Background());!errors.Is(err,ErrPermanentFailure){t.Fatalf("want permanent failure, got %v",err)}
	if q.Len()!=1 {t.Fatal("permanent failure was silently dropped")}
	o,err:=w.DropFailed();if err!=nil{t.Fatal(err)}
	if o.TenantID!="tenant-a"||q.Len()!=0 {t.Fatalf("dropped observation=%+v queue=%d",o,q.Len())}
	if _,err=w.DropFailed();!errors.Is(err,ErrNoFailedObservation){t.Fatal(err)}
}
func TestWorkerCloseReleasesLeaseWithoutDropping(t *testing.T) {
	q:=workerTestQueue(t);exp:=&workerScriptExporter{results:[]error{errors.New("temporary")}}
	w,err:=NewWorker(q,exp,workerTestPolicy());if err!=nil{t.Fatal(err)}
	if _,err=w.RunOnce(context.Background());!errors.Is(err,ErrRetryScheduled){t.Fatal(err)}
	if _,ok:=q.Pop();ok {t.Fatal("leased item popped")}
	if err=w.Close();err!=nil{t.Fatal(err)}
	o,ok:=q.Pop();if !ok||o.TenantID!="tenant-a"||q.Len()!=0 {t.Fatal("close did not release retained item")}
}
func TestWorkerPermanentAndRetryPolicyValidation(t *testing.T) {
	q:=workerTestQueue(t);exp:=&workerScriptExporter{}
	for _,p:=range []RetryPolicy{
		{MaxAttempts:0,InitialBackoff:time.Second,MaxBackoff:time.Second},
		{MaxAttempts:21,InitialBackoff:time.Second,MaxBackoff:time.Second},
		{MaxAttempts:1,InitialBackoff:0,MaxBackoff:time.Second},
		{MaxAttempts:1,InitialBackoff:2*time.Second,MaxBackoff:time.Second},
		{MaxAttempts:1,InitialBackoff:time.Second,MaxBackoff:time.Hour+time.Second},
		{MaxAttempts:1,InitialBackoff:time.Second,MaxBackoff:time.Second,Jitter:0.51},
	} {
		if w,err:=NewWorker(q,exp,p);err==nil||w!=nil {t.Fatalf("invalid policy accepted: %+v",p)}
	}
	if w,err:=NewWorker(nil,exp,workerTestPolicy());err==nil||w!=nil {t.Fatal("nil queue accepted")}
	if w,err:=NewWorker(q,nil,workerTestPolicy());err==nil||w!=nil {t.Fatal("nil exporter accepted")}
}
func TestWorkerJitterIsStableAndBounded(t *testing.T) {
	o:=workerTestObservation();p:=workerTestPolicy();p.Jitter=0.25
	a:=retryDelay(o,2,p);b:=retryDelay(o,2,p)
	if a!=b||a<1500*time.Millisecond||a>2500*time.Millisecond {t.Fatalf("unstable/out-of-range jitter: %s %s",a,b)}
}
func TestWorkerCancellationDoesNotClaim(t *testing.T) {
	q:=workerTestQueue(t);exp:=&workerScriptExporter{};w,err:=NewWorker(q,exp,workerTestPolicy());if err!=nil{t.Fatal(err)}
	ctx,cancel:=context.WithCancel(context.Background());cancel()
	if _,err=w.RunOnce(ctx);!errors.Is(err,context.Canceled){t.Fatalf("got %v",err)}
	if exp.calls()!=0 {t.Fatal("export invoked after cancellation")}
	if _,ok:=q.Pop();!ok {t.Fatal("canceled call retained a lease")}
}
func TestWorkerSerializesQueueLeaseAcrossWorkers(t *testing.T) {
	q:=workerTestQueue(t);exp:=&workerScriptExporter{results:[]error{errors.New("temporary"),nil}}
	w1,err:=NewWorker(q,exp,workerTestPolicy());if err!=nil{t.Fatal(err)}
	w2,err:=NewWorker(q,exp,workerTestPolicy());if err!=nil{t.Fatal(err)}
	if _,err=w1.RunOnce(context.Background());!errors.Is(err,ErrRetryScheduled){t.Fatal(err)}
	if delivered,err:=w2.RunOnce(context.Background());delivered||err!=nil{t.Fatalf("second worker: %v %v",delivered,err)}
	if exp.calls()!=1 {t.Fatal("second worker bypassed queue lease")}
	if err=w1.Close();err!=nil{t.Fatal(err)}
	if delivered,err:=w2.RunOnce(context.Background());!delivered||err!=nil{t.Fatalf("released lease: %v %v",delivered,err)}
}
