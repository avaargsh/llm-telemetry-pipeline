package ingest

import (
 "context"
 "encoding/json"
 "fmt"
 "io"
 "net/http"
 "net/http/httptest"
 "os"
 "strings"
 "sync"
 "testing"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/clickhouse"
 "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
 "github.com/avaargsh/llm-telemetry-pipeline/internal/lagmonitor"
 "github.com/avaargsh/llm-telemetry-pipeline/internal/lagrisk"
 "github.com/segmentio/kafka-go"
)

// This test-only HTTP gate throttles the second batch BEFORE it reaches
// the real ClickHouse server. It does not inject any code into the worker.
type gatedClickHouse struct{
 upstream string
 mu sync.Mutex
 requests int
 blocked chan struct{}
 release chan struct{}
}
func(g *gatedClickHouse)ServeHTTP(w http.ResponseWriter,r *http.Request){
 g.mu.Lock()
 g.requests++
 n:=g.requests
 g.mu.Unlock()
 if n==2{
  close(g.blocked)
  select{
  case <-g.release:
  case <-r.Context().Done():return
  }
 }
 upstreamRequest,err:=http.NewRequestWithContext(r.Context(),r.Method,g.upstream+r.URL.RequestURI(),r.Body)
 if err!=nil{http.Error(w,err.Error(),http.StatusBadGateway);return}
 upstreamRequest.Header=r.Header.Clone()
 upstreamResponse,err:=http.DefaultClient.Do(upstreamRequest)
 if err!=nil{http.Error(w,err.Error(),http.StatusBadGateway);return}
 defer upstreamResponse.Body.Close()
 for key,vals:=range upstreamResponse.Header{
  for _,v:=range vals{w.Header().Add(key,v)}
 }
 w.WriteHeader(upstreamResponse.StatusCode)
 _,_=io.Copy(w,io.LimitReader(upstreamResponse.Body,2<<20))
}
func TestRealClickHouseThrottleAndCatchUp(t *testing.T){
 if os.Getenv("TEST_M3E_THROTTLE")!="1"{t.Skip("requires isolated real Kafka/ClickHouse Docker CI")}
 broker,endpoint:=os.Getenv("TEST_KAFKA_BROKER"),os.Getenv("TEST_CLICKHOUSE_HTTP")
 if broker==""||endpoint==""{t.Fatal("broker and ClickHouse endpoints required")}
 const topic="m3e.throttle.v1"
 group:=fmt.Sprintf("m3e-throttle-%d",time.Now().UnixNano())
 ctx,cancel:=context.WithTimeout(context.Background(),120*time.Second)
 defer cancel()
 producer:=&kafka.Writer{Addr:kafka.TCP(broker),Topic:topic,RequiredAcks:kafka.RequireAll,
  Async:false,AllowAutoTopicCreation:false}
 for i:=0;i<32;i++{
  e,err:=event.SyntheticUsageEvent(group,i,time.Date(2026,11,16,11,0,0,0,time.UTC))
  if err!=nil{t.Fatal(err)}
  payload,err:=json.Marshal(e);if err!=nil{t.Fatal(err)}
  if err:=producer.WriteMessages(ctx,kafka.Message{Key:[]byte(e.EventID),Value:payload});err!=nil{t.Fatal(err)}
 }
 if err:=producer.Close();err!=nil{t.Fatal(err)}
 gate:=&gatedClickHouse{upstream:endpoint,blocked:make(chan struct{}),release:make(chan struct{})}
 server:=httptest.NewServer(gate)
 defer server.Close()
 defer func(){select{case <-gate.release:default:close(gate.release)}}()
 sink:=&clickhouse.HTTPWriter{Endpoint:server.URL,Database:"telemetry",
  User:"telemetry",Password:"localdev-only"}
 workerCtx,stopWorker:=context.WithCancel(ctx)
 defer stopWorker()
 commits:=make(chan int64,8)
 worker:=GroupWorker{
  Brokers:[]string{broker},Topic:topic,GroupID:group,BatchSize:8,
  FetchWait:250*time.Millisecond,Sink:sink,
  OnCommitted:func(m Message){
   commits<-m.Offset
   if m.Offset==31{stopWorker()}
  },
 }
 result:=make(chan error,1)
 go func(){result<-worker.Run(workerCtx)}()
 select{
 case <-gate.blocked:
 case err:=<-result:t.Fatalf("consumer terminated before slow second insert: %v",err)
 case <-ctx.Done():t.Fatalf("second batch was never blocked: %v",ctx.Err())
 }
 select{
 case off:=<-commits:
  if off!=7{t.Fatalf("incorrect first committed offset %d",off)}
 default:t.Fatal("first ClickHouse batch had not been committed")
 }
 // The second (eight-event) batch is blocked before ClickHouse sees it.
 // Kafka's actual consumer group offset MUST remain 8.
 source:=lagmonitor.KafkaSource{
  Brokers:[]string{broker},Topic:topic,GroupID:group,
  SafetyMargin:35*time.Second,MaxPartitions:2,
 }
 mon:=&lagmonitor.Monitor{GroupID:group,Source:source}
 if err:=mon.Poll(ctx,time.Now().UTC());err!=nil{t.Fatal(err)}
 state:=mon.State()
 if !state.Up||len(state.Partitions)!=1{t.Fatalf("failed to sample live broker lag: %+v",state)}
 risk:=state.Partitions[0]
 if risk.CommittedOffset!=8||risk.LogEndOffset!=32||risk.LagOffsetDistance!=24{
  t.Fatalf("slow ClickHouse advanced broker offset: %+v",risk)
 }
 if risk.Status!=lagrisk.StatusWarning{
  t.Fatalf("expected early retention warning while slow insert blocked; got %s",risk.Status)
 }
 // No source commit callback may occur while downstream is blocked.
 select{
 case off:=<-commits:t.Fatalf("offset %d committed without ClickHouse ACK",off)
 case <-time.After(500*time.Millisecond):
 }
 start:=time.Now()
 close(gate.release)
 select{
 case err:=<-result:
  if err!=nil&&err!=context.Canceled{t.Fatalf("post-release worker failed: %v",err)}
 case <-ctx.Done():t.Fatalf("catch-up timed out: %v",ctx.Err())
 }
 duration:=time.Since(start)
 offsets:=make([]int64,0,3)
 for{
  select{case off:=<-commits:offsets=append(offsets,off)
  default:goto drained}
 }
drained:
 if len(offsets)!=3||offsets[0]!=15||offsets[1]!=23||offsets[2]!=31{
  t.Fatalf("post-release commits not bounded by CH ACK: %+v",offsets)
 }
 if err:=mon.Poll(ctx,time.Now().UTC());err!=nil{t.Fatal(err)}
 recovered:=mon.State().Partitions[0]
 if recovered.CommittedOffset!=32||recovered.Status!=lagrisk.StatusNoBacklog{
  t.Fatalf("consumer failed to catch up or alert remained active: %+v",recovered)
 }
 raw:=m3Query(t,endpoint,"SELECT count(),uniqExact(event_id) FROM inference_events_raw FORMAT TabSeparatedRaw")
 trusted:=m3Query(t,endpoint,"SELECT count() FROM inference_events_dedup_v1 FORMAT TabSeparatedRaw")
 integrity:=m3Query(t,endpoint,"SELECT count() FROM inference_events_integrity_v1 FORMAT TabSeparatedRaw")
 if raw!="32\t32"||trusted!="32"||integrity!="0"{
  t.Fatalf("ClickHouse recovery mismatch: raw=%q trusted=%s integrity=%s",raw,trusted,integrity)
 }
 rate:=float64(24)/duration.Seconds()
 report:=map[string]any{
  "scenario":"second ClickHouse batch gated before durable INSERT ACK",
  "producer_acks":32,"blocked_second_batch_offset":8,
  "observed_lag_offsets":24,"warning_during_block":true,
  "recovered_committed_offset":32,"alert_resolved_after_catchup":true,
  "catchup_records":24,"catchup_seconds":duration.Seconds(),
  "catchup_events_per_second":rate,
  "raw_events":32,"trusted_events":32,"integrity_conflicts":0,
  "topic_retention_seconds":60,"source":"real Kafka coordinator + ClickHouse, test-only HTTP gate",
 }
 if p:=os.Getenv("M3E_THROTTLE_REPORT_PATH");p!=""{
  b,err:=json.MarshalIndent(report,"","  ");if err!=nil{t.Fatal(err)}
  if err:=os.WriteFile(p,b,0600);err!=nil{t.Fatal(err)}
 }
 t.Logf("PASS throttling: Kafka committed=8 with 24 pending, after ACK resumed to 32, caught up 24 in %.3fs (%.1f/s); risk resolved",duration.Seconds(),rate)
 _=strings.TrimSpace
}
