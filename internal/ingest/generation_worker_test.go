package ingest

import (
    "context"
    "encoding/json"
    "errors"
    "strings"
    "testing"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
)

type commitProbe struct {
    calls int
    offsets map[string]map[int]int64
    err error
}
func (p *commitProbe) CommitOffsets(offsets map[string]map[int]int64) error {
    p.calls++
    p.offsets=offsets
    return p.err
}
func testSourceMessage(t *testing.T, partition int32, offset int64) Message {
    t.Helper()
    e,err:=event.SyntheticUsageEvent("rebalance-fence",int(offset),time.Date(2026,11,16,11,0,0,0,time.UTC))
    if err!=nil{t.Fatal(err)}
    raw,err:=json.Marshal(e);if err!=nil{t.Fatal(err)}
    return Message{Topic:"usage.critical.v1",Partition:partition,Offset:offset,Value:raw}
}

func TestGenerationCommitterUsesNextObservedOffset(t *testing.T) {
    genCtx:=context.Background()
    p:=&commitProbe{}
    c:=GenerationCommitter{Generation:genCtx,Topic:"usage.critical.v1",Partition:1,CommitOffsets:p.CommitOffsets}
    // Offsets are not required to be numerically contiguous.
    messages:=[]Message{testSourceMessage(t,1,100),testSourceMessage(t,1,105)}
    if err:=c.Commit(context.Background(),messages);err!=nil{t.Fatal(err)}
    if p.calls!=1||p.offsets["usage.critical.v1"][1]!=106 {
        t.Fatalf("wrong generation-tagged commit: calls=%d offsets=%v",p.calls,p.offsets)
    }
}
func TestGenerationCommitterBlocksRevokedAssignmentEvenWhenSinkReturnsSuccess(t *testing.T) {
    genCtx,revoke:=context.WithCancel(context.Background())
    p:=&commitProbe{}
    c:=GenerationCommitter{Generation:genCtx,Topic:"usage.critical.v1",Partition:1,CommitOffsets:p.CommitOffsets}
    s:=sinkFunc(func(context.Context,[]event.UsageEvent)error{
        // Mimics ClickHouse persisting an INSERT just as Kafka revokes the
        // generation. A stale worker MUST NOT call the coordinator commit.
        revoke()
        return nil
    })
    err:=ProcessBatchWithDLQ(context.Background(),[]Message{testSourceMessage(t,1,42)},s,c,nil)
    if err==nil||!strings.Contains(err.Error(),"generation"){
        t.Fatalf("stale generation was accepted: %v",err)
    }
    if p.calls!=0{t.Fatalf("broker commit attempted after generation revoked: %d",p.calls)}
}
func TestGenerationCommitterRejectsUnassignedPartition(t *testing.T) {
    p:=&commitProbe{}
    c:=GenerationCommitter{Generation:context.Background(),Topic:"usage.critical.v1",Partition:1,CommitOffsets:p.CommitOffsets}
    if err:=c.Commit(context.Background(),[]Message{testSourceMessage(t,0,7)});err==nil {
        t.Fatal("unassigned partition was committed")
    }
    if p.calls!=0{t.Fatal("called coordinator with unassigned partition")}
}
func TestGenerationCommitterDoesNotAcknowledgeBrokerFailure(t *testing.T) {
    p:=&commitProbe{err:errors.New("ILLEGAL_GENERATION")}
    c:=GenerationCommitter{Generation:context.Background(),Topic:"usage.critical.v1",Partition:1,CommitOffsets:p.CommitOffsets}
    if err:=c.Commit(context.Background(),[]Message{testSourceMessage(t,1,17)});err==nil{
        t.Fatal("broker generation error was silently accepted")
    }
    if p.calls!=1{t.Fatal("expected a real coordinator commit attempt")}
}
type sinkFunc func(context.Context,[]event.UsageEvent)error
func(f sinkFunc)Insert(ctx context.Context,es []event.UsageEvent)error{return f(ctx,es)}
