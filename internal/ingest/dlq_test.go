package ingest

import (
    "context"
    "errors"
    "reflect"
    "testing"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
)

type orderedSink struct {calls *[]string}
func(s orderedSink) Insert(_ context.Context,records []event.UsageEvent)error{
    *s.calls=append(*s.calls,"clickhouse")
    return nil
}
type orderedCommitter struct{calls *[]string;committed []Message}
func(c *orderedCommitter)Commit(_ context.Context, messages []Message)error{
    *c.calls=append(*c.calls,"offset")
    c.committed=append([]Message(nil),messages...)
    return nil
}
type orderedDLQ struct{calls *[]string;records []DeadLetter;err error}
func(d *orderedDLQ)Publish(_ context.Context,m DeadLetter)error{
    *d.calls=append(*d.calls,"dlq")
    if d.err!=nil{return d.err}
    d.records=append(d.records,m)
    return nil
}

func TestDLQMustBeACKedBeforeSourceOffsetCommit(t *testing.T){
    steps:=[]string{}
    sink:=orderedSink{&steps}
    offsets:=&orderedCommitter{calls:&steps}
    dlq:=&orderedDLQ{calls:&steps}
    good:=sample(t,0,3)
    poison:=Message{Topic:good.Topic,Partition:0,Offset:4,Value:[]byte("{not-json")}
    if err:=ProcessBatchWithDLQ(context.Background(),[]Message{good,poison},sink,offsets,dlq);err!=nil{t.Fatal(err)}
    if !reflect.DeepEqual(steps,[]string{"dlq","clickhouse","offset"}){t.Fatalf("unexpected side-effect ordering %v",steps)}
    if len(dlq.records)!=1||dlq.records[0].StableKey()!="usage.critical.v1/0/4"{t.Fatalf("wrong DLQ record: %+v",dlq.records)}
    if string(dlq.records[0].RawPayload)!=string(poison.Value){t.Fatal("DLQ did not preserve raw message for replay")}
    if len(offsets.committed)!=1||offsets.committed[0].Offset!=4{t.Fatal("unexpected offset commit")}
}
func TestDLQFailureLeavesSourceOffsetUncommitted(t *testing.T){
    steps:=[]string{}
    sink:=orderedSink{&steps}
    offset:=&orderedCommitter{calls:&steps}
    dlq:=&orderedDLQ{calls:&steps,err:errors.New("broker timeout: outcome unknown")}
    poison:=Message{Topic:"usage.critical.v1",Partition:2,Offset:9,Value:[]byte("garbled")}
    if err:=ProcessBatchWithDLQ(context.Background(),[]Message{poison},sink,offset,dlq);err==nil{t.Fatal("DLQ timeout must return error")}
    if !reflect.DeepEqual(steps,[]string{"dlq"}){t.Fatalf("advanced or inserted after DLQ failure: %v",steps)}
}
func TestPoisonOnlyBatchCommitsAfterDurableDLQ(t *testing.T){
    steps:=[]string{}
    sink:=orderedSink{&steps}
    offset:=&orderedCommitter{calls:&steps}
    dlq:=&orderedDLQ{calls:&steps}
    m:=Message{Topic:"usage.critical.v1",Partition:1,Offset:12,Value:[]byte("bad")}
    if err:=ProcessBatchWithDLQ(context.Background(),[]Message{m},sink,offset,dlq);err!=nil{t.Fatal(err)}
    if !reflect.DeepEqual(steps,[]string{"dlq","offset"}){t.Fatalf("wrong ordering %v",steps)}
}
func TestDLQStableIdentityOnReplay(t *testing.T){
    a:=DeadLetter{SourceTopic:"usage.critical.v1",SourcePartition:3,SourceOffset:99}
    b:=a
    if a.StableKey()!=b.StableKey(){t.Fatal("redelivery generated a different DLQ identity")}
}
