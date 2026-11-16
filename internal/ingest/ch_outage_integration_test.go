package ingest

import (
 "context"
 "encoding/json"
 "fmt"
 "os"
 "os/exec"
 "sync"
 "testing"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/clickhouse"
 "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
 "github.com/segmentio/kafka-go"
)

// This integration test uses a real Kafka coordinator and real ClickHouse
// Docker service. CH is stopped while batch 2 is in the worker's Insert call.
type outageAfterFirstBatch struct{
 real Inserter
 mu sync.Mutex
 calls int
 triggered chan struct{}
}
func(s *outageAfterFirstBatch)Insert(ctx context.Context,events []event.UsageEvent)error{
 s.mu.Lock()
 s.calls++
 n:=s.calls
 s.mu.Unlock()
 if n==2{
  cmd:=exec.CommandContext(ctx,"docker","compose","-f","deployments/docker-compose.yml","stop","clickhouse")
  cmd.Dir="../.."
  output,err:=cmd.CombinedOutput()
  if err!=nil{return fmt.Errorf("inject CH outage: %w: %s",err,output)}
  close(s.triggered)
 }
 return s.real.Insert(ctx,events)
}
type captureInsert struct{
 real Inserter
 mu sync.Mutex
 first string
}
func(s *captureInsert)Insert(ctx context.Context,events []event.UsageEvent)error{
 s.mu.Lock()
 if s.first==""&&len(events)>0{s.first=events[0].EventID}
 s.mu.Unlock()
 return s.real.Insert(ctx,events)
}
func TestClickHouseOutageMidStreamKeepsCommittedPrefixAndReplays(t *testing.T){
 if os.Getenv("TEST_M3D_OUTAGE")!="1"{t.Skip("requires isolated Kafka/ClickHouse fault lab")}
 broker:=os.Getenv("TEST_KAFKA_BROKER")
 endpoint:=os.Getenv("TEST_CLICKHOUSE_HTTP")
 if broker==""||endpoint==""{t.Fatal("Kafka and ClickHouse endpoints required")}
 const topic="m3d.ch.outage.v1"
 group:=fmt.Sprintf("m3d-outage-%d",time.Now().UnixNano())
 ctx,cancel:=context.WithTimeout(context.Background(),150*time.Second)
 defer cancel()
 ack:=make([]event.UsageEvent,24)
 writer:=&kafka.Writer{Addr:kafka.TCP(broker),Topic:topic,RequiredAcks:kafka.RequireAll,
  Balancer:&kafka.Hash{},Async:false,AllowAutoTopicCreation:false}
 for i:=range ack{
  e,err:=event.SyntheticUsageEvent(group,i,time.Date(2026,11,16,11,0,0,0,time.UTC))
  if err!=nil{t.Fatal(err)}
  raw,err:=json.Marshal(e);if err!=nil{t.Fatal(err)}
  if err:=writer.WriteMessages(ctx,kafka.Message{Key:[]byte(e.EventID),Value:raw});err!=nil{t.Fatal(err)}
  ack[i]=e
 }
 if err:=writer.Close();err!=nil{t.Fatal(err)}
 sink:=&clickhouse.HTTPWriter{Endpoint:endpoint,Database:"telemetry",User:"telemetry",Password:"localdev-only"}
 failSink:=&outageAfterFirstBatch{real:sink,triggered:make(chan struct{})}
 var commitsMu sync.Mutex
 committed:=[]int64{}
 worker:=GroupWorker{
  Brokers:[]string{broker},Topic:topic,GroupID:group,BatchSize:8,
  FetchWait:250*time.Millisecond,Sink:failSink,
  OnCommitted:func(m Message){commitsMu.Lock();committed=append(committed,m.Offset);commitsMu.Unlock()},
 }
 done:=make(chan error,1)
 go func(){done<-worker.Run(ctx)}()
 select{
 case <-failSink.triggered:
 case err:=<-done:t.Fatalf("consumer exited before induced ClickHouse outage: %v",err)
 case <-ctx.Done():t.Fatalf("worker never reached second batch: %v",ctx.Err())
 }
 select{
 case err:=<-done:
  if err==nil{t.Fatal("consumer accepted ClickHouse outage with a successful offset commit")}
 case <-ctx.Done():t.Fatalf("consumer did not exit on failed ClickHouse write: %v",ctx.Err())
 }
 commitsMu.Lock()
 prefix:=append([]int64(nil),committed...)
 commitsMu.Unlock()
 if len(prefix)!=1||prefix[0]!=7{t.Fatalf("source advanced past CH failure; committed offsets=%v",prefix)}
 cmd:=exec.CommandContext(ctx,"docker","compose","-f","deployments/docker-compose.yml","start","clickhouse")
 cmd.Dir="../.."
 if out,err:=cmd.CombinedOutput();err!=nil{t.Fatalf("cannot restart ClickHouse: %v: %s",err,out)}
 var up bool
 for i:=0;i<80;i++{
  attemptCtx,stop:=context.WithTimeout(ctx,2*time.Second)
  err:=sink.Insert(attemptCtx,[]event.UsageEvent{}) // deliberately no-op: not health proof
  stop()
  _=err
  // Real SELECT proof will be checked with m3Query below after recovery;
  // a timed SELECT can return transient failures while server starts.
  if pingClickHouseM3D(endpoint){up=true;break}
  select{case <-ctx.Done():t.Fatalf("ClickHouse did not recover: %v",ctx.Err());case <-time.After(time.Second):}
 }
 if !up{t.Fatal("ClickHouse did not become queryable")}
 recoverySink:=&captureInsert{real:sink}
 replayed:=[]int64{}
 recoveredCtx,stopWorker:=context.WithCancel(ctx)
 recovered:=make(chan error,1)
 restart:=GroupWorker{
  Brokers:[]string{broker},Topic:topic,GroupID:group,BatchSize:8,
  FetchWait:250*time.Millisecond,Sink:recoverySink,
  OnCommitted:func(m Message){
   commitsMu.Lock()
   replayed=append(replayed,m.Offset)
   if m.Offset==23{stopWorker()}
   commitsMu.Unlock()
  },
 }
 go func(){recovered<-restart.Run(recoveredCtx)}()
 select{
 case err:=<-recovered:
  if err!=nil&&err!=context.Canceled{t.Fatalf("recovery worker error: %v",err)}
 case <-ctx.Done():t.Fatalf("recovery timed out: %v",ctx.Err())
 }
 commitsMu.Lock()
 replay:=append([]int64(nil),replayed...)
 commitsMu.Unlock()
 if len(replay)!=2||replay[0]!=15||replay[1]!=23{
  t.Fatalf("wrong recovered group offsets: %v",replay)
 }
 if recoverySink.first!=ack[8].EventID{
  t.Fatalf("consumer did not restart at first uncommitted event: got %s want %s",recoverySink.first,ack[8].EventID)
 }
 raw:=m3Query(t,endpoint,"SELECT count(), uniqExact(event_id) FROM inference_events_raw FORMAT TabSeparatedRaw")
 trusted:=m3Query(t,endpoint,"SELECT count() FROM inference_events_dedup_v1 FORMAT TabSeparatedRaw")
 conflicts:=m3Query(t,endpoint,"SELECT count() FROM inference_events_integrity_v1 FORMAT TabSeparatedRaw")
 if raw!="24\t24"||trusted!="24"||conflicts!="0"{
  t.Fatalf("recovery mismatch: raw=%s trusted=%s conflicts=%s",raw,trusted,conflicts)
 }
 report:=map[string]any{"scenario":"ClickHouse stopped during second batch Insert",
  "topic":topic,"group":group,"producer_acks":24,
  "initial_committed_offsets":prefix,"recovery_committed_offsets":replay,
  "first_recovered_event_id":recoverySink.first,
  "raw_rows":24,"trusted_event_ids":24,"integrity_conflicts":0,
  "fail_closed":true,"recovered":true,
 }
 if path:=os.Getenv("M3D_REPORT_PATH");path!=""{
  raw,err:=json.MarshalIndent(report,"","  ");if err!=nil{t.Fatal(err)}
  if err:=os.WriteFile(path,raw,0600);err!=nil{t.Fatal(err)}
 }
 t.Log("PASS CH outage: commit stops at offset 7; after CH restart recovered offset 8..23, 24/24 trusted events")
}
