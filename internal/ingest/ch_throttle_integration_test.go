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
 "sync/atomic"
 "testing"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/clickhouse"
 "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
 "github.com/avaargsh/llm-telemetry-pipeline/internal/lagmonitor"
 "github.com/avaargsh/llm-telemetry-pipeline/internal/lagrisk"
 "github.com/segmentio/kafka-go"
)

// TestClickHouseBackpressure measures recovery from a real withheld
// ClickHouse HTTP ACK. The database is not stopped or mocked, and Kafka
// group commits still use the real generation-scoped coordinator path.
func TestClickHouseBackpressureCatchUp(t *testing.T){
 if os.Getenv("TEST_M3E_THROTTLE")!="1"{t.Skip("requires isolated Kafka and ClickHouse")}
 broker,clickhouseURL:=os.Getenv("TEST_KAFKA_BROKER"),os.Getenv("TEST_CLICKHOUSE_HTTP")
 if broker==""||clickhouseURL==""{t.Fatal("missing integration endpoints")}
 const topic="m3e.throttle.v1"
 group:=fmt.Sprintf("m3e-slow-%d",time.Now().UnixNano())
 ctx,cancel:=context.WithTimeout(context.Background(),100*time.Second)
 defer cancel()

 producer:=&kafka.Writer{Addr:kafka.TCP(broker),Topic:topic,
  RequiredAcks:kafka.RequireAll,Async:false,AllowAutoTopicCreation:false}
 for i:=0;i<24;i++{
  e,err:=event.SyntheticUsageEvent(group,i,time.Date(2026,11,16,11,0,0,0,time.UTC))
  if err!=nil{t.Fatal(err)}
  payload,err:=json.Marshal(e);if err!=nil{t.Fatal(err)}
  if err:=producer.WriteMessages(ctx,kafka.Message{Key:[]byte(e.EventID),Value:payload});err!=nil{t.Fatal(err)}
 }
 if err:=producer.Close();err!=nil{t.Fatal(err)}

 var requests atomic.Int32
 blocked:=make(chan struct{})
 release:=make(chan struct{})
 proxy:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
  n:=requests.Add(1)
  if n==2{
   close(blocked)
   select{case <-release:case <-r.Context().Done():return}
  }
  target,err:=http.NewRequestWithContext(r.Context(),r.Method,clickhouseURL+r.URL.RequestURI(),r.Body)
  if err!=nil{http.Error(w,err.Error(),500);return}
  target.Header=r.Header.Clone()
  resp,err:=http.DefaultClient.Do(target)
  if err!=nil{http.Error(w,err.Error(),http.StatusBadGateway);return}
  defer resp.Body.Close()
  for k,values:=range resp.Header{for _,v:=range values{w.Header().Add(k,v)}}
  w.WriteHeader(resp.StatusCode)
  _,_=io.Copy(w,resp.Body)
 }))
 defer proxy.Close()

 sink:=&clickhouse.HTTPWriter{Endpoint:proxy.URL,Database:"telemetry",
  User:"telemetry",Password:"localdev-only"}
 commits:=make(chan int64,8)
 workerCtx,stopWorker:=context.WithCancel(ctx)
 defer stopWorker()
 worker:=GroupWorker{
  Brokers:[]string{broker},Topic:topic,GroupID:group,BatchSize:8,
  FetchWait:250*time.Millisecond,Sink:sink,
  OnCommitted:func(m Message){
   commits<-m.Offset
   if m.Offset==23{stopWorker()}
  },
 }
 done:=make(chan error,1)
 go func(){done<-worker.Run(workerCtx)}()
 select{
 case offset:=<-commits:
  if offset!=7{t.Fatalf("first commit was %d not 7",offset)}
 case err:=<-done:t.Fatalf("worker exited before first commit: %v",err)
 case <-ctx.Done():t.Fatal(ctx.Err())
 }
 select{
 case <-blocked:
 case err:=<-done:t.Fatalf("worker exited before throttled batch: %v",err)
 case <-ctx.Done():t.Fatal(ctx.Err())
 }
 source:=lagmonitor.KafkaSource{
  Brokers:[]string{broker},Topic:topic,Group:group,Partition:0,
  Margin:2*time.Minute,Timeout:7*time.Second,
 }
 monitor:=lagmonitor.New(source,group,time.Second)
 if err:=monitor.Poll(ctx);err!=nil{t.Fatalf("cannot read committed group offset while slowed: %v",err)}
 before:=monitor.Report().Assessment
 if before.CommittedOffset!=8||before.LagOffsetDistance!=16||
    before.Status!=lagrisk.StatusHealthy{
  t.Fatalf("throttled worker advanced offsets unexpectedly: %+v",before)
 }
 // Ensure lag remains even after the handler has been blocked over 1 second.
 select{case <-time.After(1300*time.Millisecond):case <-ctx.Done():t.Fatal(ctx.Err())}
 if err:=monitor.Poll(ctx);err!=nil{t.Fatal(err)}
 during:=monitor.Report().Assessment
 if during.CommittedOffset!=8||during.LagOffsetDistance!=16{
  t.Fatalf("source committed unconfirmed ClickHouse data: %+v",during)
 }
 exposed:=httptest.NewRecorder()
 monitor.ServeHTTP(exposed,httptest.NewRequest(http.MethodGet,"/metrics",nil))
 if !strings.Contains(exposed.Body.String(),"telemetry_kafka_monitor_up 1")||
    !strings.Contains(exposed.Body.String(),"telemetry_kafka_lag_offsets") {
  t.Fatalf("lag exporter not usable during backpressure: %s",exposed.Body.String())
 }
 releasedAt:=time.Now()
 close(release)
 var observed []int64
 for len(observed)<2{
  select{
  case offset:=<-commits:
   observed=append(observed,offset)
  case err:=<-done:
   t.Fatalf("worker exited before all post-throttle commits: %v",err)
  case <-ctx.Done():t.Fatal(ctx.Err())
  }
 }
 if observed[0]!=15||observed[1]!=23{
  t.Fatalf("unexpected recovered commit boundaries: %v",observed)
 }
 catchUp:=time.Since(releasedAt)
 if catchUp<=0||catchUp>30*time.Second{t.Fatalf("invalid catch-up duration %v",catchUp)}
 select{
 case err:=<-done:if err!=nil&&err!=context.Canceled{t.Fatalf("worker exit: %v",err)}
 case <-time.After(10*time.Second):t.Fatal("worker failed to exit after final commit")
 }
 if err:=monitor.Poll(ctx);err!=nil{t.Fatal(err)}
 after:=monitor.Report().Assessment
 if after.Status!=lagrisk.StatusNoBacklog||after.CommittedOffset!=24||after.LagOffsetDistance!=0{
  t.Fatalf("lag did not clear: %+v",after)
 }
 count:=m3Query(t,clickhouseURL,"SELECT count(),uniqExact(event_id) FROM inference_events_raw FORMAT TabSeparatedRaw")
 trusted:=m3Query(t,clickhouseURL,"SELECT count() FROM inference_events_dedup_v1 FORMAT TabSeparatedRaw")
 if count!="24\t24"||trusted!="24"{t.Fatalf("ClickHouse backpressure recovery corrupted data: raw=%s trusted=%s",count,trusted)}
 rate:=16/catchUp.Seconds()
 report:=map[string]any{
  "scenario":"real_clickhouse_insert_http_ack_delayed",
  "producer_acks":24,"first_committed_next_offset":8,
  "held_committed_next_offset":during.CommittedOffset,
  "held_lag_offset_distance":during.LagOffsetDistance,
  "held_duration_seconds":1.3,
  "recovery_committed_offsets":observed,
  "catchup_events":16,"catchup_seconds":catchUp.Seconds(),
  "catchup_events_per_second":rate,
  "final_committed_next_offset":after.CommittedOffset,
  "final_lag_offset_distance":after.LagOffsetDistance,
  "raw_rows":24,"trusted_events":24,
 }
 if path:=os.Getenv("M3E_THROTTLE_REPORT");path!=""{
  bytes,err:=json.MarshalIndent(report,"","  ");if err!=nil{t.Fatal(err)}
  if err:=os.WriteFile(path,bytes,0600);err!=nil{t.Fatal(err)}
 }
 t.Logf("PASS M3e throttle: committed=8 while CH ACK held, lag=16, recovered 16 events in %s (%.1f events/s), final 24/24",catchUp,rate)
}
