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

// ProcessBatch is deliberately fail-closed: invalid messages and unknown insert
// outcomes prevent any offset advance. DLQ routing is a separate M2 feature.
func ProcessBatch(ctx context.Context, messages []Message, sink Inserter, offsets Committer) error {
    if len(messages)==0{return nil}
    events:=make([]event.UsageEvent,0,len(messages))
    for _,m:=range messages {
        var e event.UsageEvent
        if err:=json.Unmarshal(m.Value,&e);err!=nil{return fmt.Errorf("decode %s/%d/%d: %w",m.Topic,m.Partition,m.Offset,err)}
        if err:=e.Validate();err!=nil{return fmt.Errorf("validate %s/%d/%d: %w",m.Topic,m.Partition,m.Offset,err)}
        events=append(events,e)
    }
    if err:=sink.Insert(ctx,events);err!=nil{return fmt.Errorf("insert not confirmed; offsets uncommitted: %w",err)}
    // All records in the batch were confirmed. Commit only the last offset
    // observed per topic/partition; Kafka commits its successor position.
    last:=make(map[string]int)
    for i,m:=range messages {
        k:=fmt.Sprintf("%s/%d",m.Topic,m.Partition)
        if prev,ok:=last[k];!ok || messages[prev].Offset<m.Offset {last[k]=i}
    }
    commit:=make([]Message,0,len(last))
    for _,i:=range last{commit=append(commit,messages[i])}
    if err:=offsets.Commit(ctx,commit);err!=nil{return fmt.Errorf("insert confirmed but offset commit failed; replay is possible: %w",err)}
    return nil
}
