package ingest

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
)

// Message is Kafka metadata plus payload; no offset is committed implicitly.
type Message struct {
    Topic string
    Partition int32
    Offset int64
    Value []byte
}

type Inserter interface { Insert(context.Context, []event.UsageEvent) error }
type Committer interface { Commit(context.Context, []Message) error }

// DeadLetter carries original topic, partition and offset. A replay uses
// the same StableKey, but Kafka can physically contain duplicate DLQ records.
type DeadLetter struct {
    SourceTopic string `json:"source_topic"`
    SourcePartition int32 `json:"source_partition"`
    SourceOffset int64 `json:"source_offset"`
    Reason string `json:"reason"`
    RawPayload []byte `json:"raw_payload"`
}
func(d DeadLetter)StableKey()string{return fmt.Sprintf("%s/%d/%d",d.SourceTopic,d.SourcePartition,d.SourceOffset)}
type DeadLetterPublisher interface{Publish(context.Context,DeadLetter)error}

// ProcessBatch without a DLQ remains strictly fail-closed.
func ProcessBatch(ctx context.Context,msgs []Message,sink Inserter,offsets Committer)error{
    return ProcessBatchWithDLQ(ctx,msgs,sink,offsets,nil)
}

// ProcessBatchWithDLQ acknowledges poison messages to Kafka's DLQ before
// committing source offsets. All valid events require a confirmed CH insert.
func ProcessBatchWithDLQ(ctx context.Context,msgs []Message,sink Inserter,offsets Committer,dlq DeadLetterPublisher)error{
    if len(msgs)==0{return nil}
    valid:=make([]event.UsageEvent,0,len(msgs))
    for _,m:=range msgs{
        var e event.UsageEvent
        var problem string
        if err:=json.Unmarshal(m.Value,&e);err!=nil{problem="invalid_json: "+err.Error()
        }else if err:=e.Validate();err!=nil{problem="invalid_usage_event: "+err.Error()}
        if problem!=""{
            if dlq==nil{return fmt.Errorf("poison event %s/%d/%d: %s (DLQ unavailable)",m.Topic,m.Partition,m.Offset,problem)}
            raw:=append([]byte(nil),m.Value...)
            d:=DeadLetter{SourceTopic:m.Topic,SourcePartition:m.Partition,SourceOffset:m.Offset,Reason:problem,RawPayload:raw}
            if err:=dlq.Publish(ctx,d);err!=nil{return fmt.Errorf("DLQ ACK unknown for %s; original offset uncommitted: %w",d.StableKey(),err)}
            continue
        }
        valid=append(valid,e)
    }
    if len(valid)>0{
        if err:=sink.Insert(ctx,valid);err!=nil{return fmt.Errorf("ClickHouse insert not confirmed; offset uncommitted: %w",err)}
    }
    // All observed records are now durably handled. Commit only observed
    // partition maxima; Kafka offsets themselves need not be consecutive.
    last:=map[string]int{}
    for i,m:=range msgs{
        k:=fmt.Sprintf("%s/%d",m.Topic,m.Partition)
        if j,ok:=last[k];!ok||msgs[j].Offset<m.Offset{last[k]=i}
    }
    commit:=make([]Message,0,len(last))
    for _,i:=range last{commit=append(commit,msgs[i])}
    if err:=offsets.Commit(ctx,commit);err!=nil{return fmt.Errorf("downstream ACKed but offset commit failed; replay possible: %w",err)}
    return nil
}
