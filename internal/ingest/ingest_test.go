package ingest

import (
    "context"
    "encoding/json"
    "errors"
    "testing"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
)

type fakeSink struct{ err error; calls int }
func (s *fakeSink) Insert(_ context.Context,ev []event.UsageEvent)error{s.calls++;return s.err}
type fakeCommit struct{err error; calls int; committed []Message}
func (c *fakeCommit) Commit(_ context.Context,m []Message)error{c.calls++;c.committed=m;return c.err}
func sample(t *testing.T,p int32,o int64) Message{
    t.Helper()
    e,err:=event.SyntheticUsageEvent("test",int(o),time.Date(2026,11,16,11,0,0,0,time.UTC));if err!=nil{t.Fatal(err)}
    b,err:=json.Marshal(e);if err!=nil{t.Fatal(err)}
    return Message{Topic:"usage.critical.v1",Partition:p,Offset:o,Value:b}
}
func TestCommitOnlyAfterConfirmedInsert(t *testing.T){
    s:=&fakeSink{};c:=&fakeCommit{}
    if err:=ProcessBatch(context.Background(),[]Message{sample(t,0,2),sample(t,0,4),sample(t,1,10)},s,c);err!=nil{t.Fatal(err)}
    if s.calls!=1||c.calls!=1||len(c.committed)!=2 {t.Fatalf("unexpected calls: sink=%d commit=%d records=%v",s.calls,c.calls,c.committed)}
    offsets:=map[int32]int64{}
    for _,m:=range c.committed{offsets[m.Partition]=m.Offset}
    if offsets[0]!=4 || offsets[1]!=10{t.Fatalf("wrong partition commit offsets: %+v",offsets)}
}
func TestFailedInsertNeverCommits(t *testing.T){
    s:=&fakeSink{err:errors.New("timeout")};c:=&fakeCommit{}
    if err:=ProcessBatch(context.Background(),[]Message{sample(t,0,2)},s,c);err==nil{t.Fatal("expected insert error")}
    if c.calls!=0{t.Fatal("advanced offset after unknown insert result")}
}
func TestBadMessageStopsWithoutWriting(t *testing.T){
    s:=&fakeSink{};c:=&fakeCommit{}
    good:=sample(t,0,1)
    bad:=Message{Topic:good.Topic,Partition:0,Offset:2,Value:[]byte("bad")}
    if err:=ProcessBatch(context.Background(),[]Message{good,bad},s,c);err==nil {t.Fatal("expected poison message failure")}
    if s.calls!=0||c.calls!=0{t.Fatal("did not fail closed on poison message")}
}
func TestOffsetCommitFailureIsExplicit(t *testing.T){
    s:=&fakeSink{};c:=&fakeCommit{err:errors.New("rebalance")}
    if err:=ProcessBatch(context.Background(),[]Message{sample(t,0,1)},s,c);err==nil{t.Fatal("expected error")}
    if s.calls!=1||c.calls!=1{t.Fatal("commit path not exercised")}
}
