package outbox

import (
    "context"
    "errors"
    "testing"
    "time"
)

type fakeRepo struct{ rows []Row; marked int; retried int }
func(f *fakeRepo)Claim(_ context.Context,_ string,_ time.Duration,_ int)([]Row,error){return f.rows,nil}
func(f *fakeRepo)MarkPublished(_ context.Context,_ Row,_ string)error{f.marked++;return nil}
func(f *fakeRepo)ScheduleRetry(_ context.Context,_ Row,_ string,_ time.Duration,_ string)error{f.retried++;return nil}
type fakePublish struct{ err error; calls int }
func(f *fakePublish)Publish(_ context.Context,_ Row)error{f.calls++;return f.err}
func TestRelayMarksOnlyConfirmedMessages(t *testing.T){
    r:=&fakeRepo{rows:[]Row{{ID:1,EventID:"id",Topic:"usage.critical.v1",Attempts:1}}}
    p:=&fakePublish{}
    relay:=Relay{Repo:r,Publisher:p,WorkerID:"a",Lease:30*time.Second,Limit:1}
    if n,err:=relay.RunOnce(context.Background());n!=1||err!=nil{t.Fatalf("n=%d err=%v",n,err)}
    if r.marked!=1||r.retried!=0||p.calls!=1{t.Fatalf("marked=%d retried=%d publishes=%d",r.marked,r.retried,p.calls)}
}
func TestRelayDoesNotMarkOnUnknownKafkaACK(t *testing.T){
    r:=&fakeRepo{rows:[]Row{{ID:1,EventID:"id",Topic:"usage.critical.v1",Attempts:1}}}
    p:=&fakePublish{err:errors.New("timeout outcome unknown")}
    relay:=Relay{Repo:r,Publisher:p,WorkerID:"a",Lease:30*time.Second,Limit:1}
    if _,err:=relay.RunOnce(context.Background());err==nil{t.Fatal("expected Kafka timeout error")}
    if r.marked!=0||r.retried!=1{t.Fatalf("marked=%d retried=%d",r.marked,r.retried)}
}
