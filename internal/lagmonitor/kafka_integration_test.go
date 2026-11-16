package lagmonitor

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
 "github.com/avaargsh/llm-telemetry-pipeline/internal/ingest"
 "github.com/segmentio/kafka-go"
)

func chQueryM4A(t *testing.T,endpoint,sql string)string{
 t.Helper()
 ctx,cancel:=context.WithTimeout(context.Background(),12*time.Second)
 defer cancel()
 req,err:=http.NewRequestWithContext(ctx,"POST",endpoint+"?database=telemetry",strings.NewReader(sql))
 if err!=nil{t.Fatal(err)}
 req.SetBasicAuth("telemetry","localdev-only")
 resp,err:=http.DefaultClient.Do(req)
 if err!=nil{t.Fatal(err)}
 defer resp.Body.Close()
 b,err:=io.ReadAll(io.LimitReader(resp.Body,4096))
 if err!=nil||resp.StatusCode!=200{t.Fatalf("ClickHouse query status=%d err=%v body=%s",resp.StatusCode,err,b)}
 return strings.TrimSpace(string(b))
}
func m4aWait(t *testing.T,ctx context.Context,signal <-chan struct{},done <-chan error,what string){
 t.Helper()
 select{
 case <-signal:return
 case err:=<-done:t.Fatalf("worker exited before %s: %v",what,err)
 case <-ctx.Done():t.Fatalf("timeout waiting for %s: %v",what,ctx.Err())
 }
}

// Real Kafka + real ClickHouse with a test-only HTTP delay proxy. No fault
// injection code is added to the production worker.
func TestRealKafkaLagGrowsUnderClickHouseHTTPThrottleAndRecovers(t *testing.T){
 if os.Getenv("TEST_M4A_MONITOR")!="1"{t.Skip("requires isolated Kafka and ClickHouse")}
 broker,chURL:=os.Getenv("TEST_KAFKA_BROKER"),os.Getenv("TEST_CLICKHOUSE_HTTP")
 if broker==""||chURL==""{t.Fatal("missing Kafka/ClickHouse endpoints")}
 const topic="m4a.throttle.v1"
 ctx,cancel:=context.WithTimeout(context.Background(),150*time.Second)
 defer cancel()
 seed:=fmt.Sprintf("m4a-%d",time.Now().UnixNano())
 group:=seed+"-group"

 writer:=&kafka.Writer{Addr:kafka.TCP(broker),Topic:topic,
  Balancer:&kafka.Hash{},RequiredAcks:kafka.RequireAll,
  Async:false,AllowAutoTopicCreation:false}
 events:=make([]event.UsageEvent,24)
 for i:=range events{
  e,err:=event.SyntheticUsageEvent(seed,i,time.Date(2026,11,16,11,0,0,0,time.UTC))
  if err!=nil{t.Fatal(err)}
  bytes,err:=json.Marshal(e);if err!=nil{t.Fatal(err)}
  if err:=writer.WriteMessages(ctx,kafka.Message{Key:[]byte(e.EventID),Value:bytes});err!=nil{t.Fatal(err)}
  events[i]=e
 }
 if err:=writer.Close();err!=nil{t.Fatal(err)}

 blocked:=make(chan struct{})
 release:=make(chan struct{})
 var once sync.Once
 var reqMu sync.Mutex
 calls:=0
 proxy:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
  reqMu.Lock()
  calls++
  n:=calls
  reqMu.Unlock()
  if n==2{
   once.Do(func(){close(blocked)})
   select{
   case <-release:
   case <-r.Context().Done():return
   }
  }
  upstream,err:=http.NewRequestWithContext(r.Context(),r.Method,chURL+r.URL.RequestURI(),r.Body)
  if err!=nil{http.Error(w,err.Error(),500);return}
  upstream.Header=r.Header.Clone()
  response,err:=http.DefaultClient.Do(upstream)
  if err!=nil{http.Error(w,err.Error(),502);return}
  defer response.Body.Close()
  w.WriteHeader(response.StatusCode)
  _,_=io.Copy(w,response.Body)
 }))
 defer proxy.Close()

 sink:=&clickhouse.HTTPWriter{Endpoint:proxy.URL,Database:"telemetry",User:"telemetry",Password:"localdev-only"}
 firstCommitted:=make(chan struct{})
 finished:=make(chan struct{})
 var firstOnce,finishOnce sync.Once
 workerCtx,stop:=context.WithCancel(ctx)
 defer stop()
 commits:=[]int64{}
 var commitMu sync.Mutex
 worker:=ingest.GroupWorker{
  Brokers:[]string{broker},Topic:topic,GroupID:group,
  BatchSize:8,FetchWait:250*time.Millisecond,Sink:sink,
  OnCommitted:func(m ingest.Message){
   commitMu.Lock()
   commits=append(commits,m.Offset)
   commitMu.Unlock()
   if m.Offset==7{firstOnce.Do(func(){close(firstCommitted)})}
   if m.Offset==23{finishOnce.Do(func(){close(finished)});stop()}
  },
 }
 done:=make(chan error,1)
 go func(){done<-worker.Run(workerCtx)}()
 m4aWait(t,ctx,firstCommitted,done,"initial commit")
 m4aWait(t,ctx,blocked,done,"HTTP backpressure on second batch")

 source:=KafkaSource{
  Brokers:[]string{broker},Topic:topic,Group:group,Partition:0,
  SafetyMargin:2*time.Minute,Timeout:8*time.Second,
 }
 monitor:=New(source,group,30*time.Second)
 if err:=monitor.Sample(ctx);err!=nil{t.Fatalf("live Kafka broker lag query: %v",err)}
 state:=httptest.NewRecorder()
 monitor.ServeHTTP(state,httptest.NewRequest("GET","/status",nil))
 var during struct{Up bool `json:"up"`; Assessment struct{
  LagOffsetDistance int64 `json:"lag_offset_distance"`
  CommittedOffset int64 `json:"committed_offset"`
  LogEndOffset int64 `json:"log_end_offset"`
  Status string `json:"status"`
  TimeToExpirySeconds int64 `json:"time_to_expiry_seconds"`
 } `json:"assessment"`}
 if err:=json.Unmarshal(state.Body.Bytes(),&during);err!=nil{t.Fatal(err)}
 if !during.Up||during.Assessment.CommittedOffset!=8||
    during.Assessment.LogEndOffset!=24||during.Assessment.LagOffsetDistance!=16{
  t.Fatalf("incorrect live Kafka lag during ClickHouse throttle: %+v",during)
 }
 // During a controlled second-batch delay, a new Kafka commit is unsafe.
 select{
 case <-time.After(1200*time.Millisecond):
 case <-ctx.Done():t.Fatal(ctx.Err())
 }
 if err:=monitor.Sample(ctx);err!=nil{t.Fatal(err)}
 state=httptest.NewRecorder()
 monitor.ServeHTTP(state,httptest.NewRequest("GET","/status",nil))
 if err:=json.Unmarshal(state.Body.Bytes(),&during);err!=nil{t.Fatal(err)}
 if during.Assessment.CommittedOffset!=8{
  t.Fatalf("Kafka consumer advanced source position before CH ACK: %+v",during)
 }
 proxyReleased:=time.Now()
 close(release)
 m4aWait(t,ctx,finished,done,"catch-up Kafka offset 24")
 select{
 case err:=<-done:if err!=nil&&err!=context.Canceled{t.Fatalf("worker recovery: %v",err)}
 case <-time.After(15*time.Second):t.Fatal("worker did not exit after recovery")
 }
 elapsed:=time.Since(proxyReleased)
 if elapsed<=0{t.Fatal("invalid measured recovery duration")}
 // Query the actual group after its last commit; no guessed offsets.
 if err:=monitor.Sample(ctx);err!=nil{t.Fatalf("Kafka group post-recovery query: %v",err)}
 final:=httptest.NewRecorder()
 monitor.ServeHTTP(final,httptest.NewRequest("GET","/status",nil))
 var after struct{Up bool `json:"up"`;Assessment struct{
  Status string `json:"status"`
  LagOffsetDistance int64 `json:"lag_offset_distance"`
  CommittedOffset int64 `json:"committed_offset"`
 } `json:"assessment"`}
 if err:=json.Unmarshal(final.Body.Bytes(),&after);err!=nil{t.Fatal(err)}
 if !after.Up||after.Assessment.Status!="no_backlog"||
    after.Assessment.CommittedOffset!=24||after.Assessment.LagOffsetDistance!=0{
  t.Fatalf("lag alert did not resolve after catch-up: %+v",after)
 }
 raw:=chQueryM4A(t,chURL,"SELECT count(),uniqExact(event_id) FROM inference_events_raw FORMAT TabSeparatedRaw")
 trusted:=chQueryM4A(t,chURL,"SELECT count() FROM inference_events_dedup_v1 FORMAT TabSeparatedRaw")
 bad:=chQueryM4A(t,chURL,"SELECT count() FROM inference_events_integrity_v1 FORMAT TabSeparatedRaw")
 if raw!="24\t24"||trusted!="24"||bad!="0"{
  t.Fatalf("ClickHouse after throttle: raw=%q trusted=%q integrity=%q",raw,trusted,bad)
 }
 commitMu.Lock()
 history:=append([]int64(nil),commits...)
 commitMu.Unlock()
 if len(history)!=3||history[0]!=7||history[1]!=15||history[2]!=23{
  t.Fatalf("unexpected broker-confirmed commit history: %v",history)
 }
 metrics:=httptest.NewRecorder()
 monitor.ServeHTTP(metrics,httptest.NewRequest("GET","/metrics",nil))
 if !strings.Contains(metrics.Body.String(),"telemetry_kafka_lag_monitor_up 1") ||
    !strings.Contains(metrics.Body.String(),"telemetry_kafka_lag_offset_distance"){
  t.Fatalf("Prometheus exporter missing recovered metrics: %s",metrics.Body.String())
 }
 report:=map[string]any{
  "schema_version":1,
  "scenario":"test-only CH HTTP delay during second Kafka batch",
  "producer_acks":len(events),
  "throttle_delay_ms":1200,
  "committed_before_release":during.Assessment.CommittedOffset,
  "lag_offset_distance_during_throttle":during.Assessment.LagOffsetDistance,
  "retention_time_to_expiry_seconds_during_throttle":during.Assessment.TimeToExpirySeconds,
  "oldest_status_during_throttle":during.Assessment.Status,
  "committed_after_recovery":after.Assessment.CommittedOffset,
  "lag_offset_distance_after_recovery":after.Assessment.LagOffsetDistance,
  "recovery_duration_ms":elapsed.Milliseconds(),
  "recovery_throughput_records_per_second":16/elapsed.Seconds(),
  "last_commits":history,
  "clickhouse_raw_events":24,
  "clickhouse_trusted_events":24,
  "clickhouse_hash_conflicts":0,
  "metrics_up":true,
  "scope":"single Kafka partition, local development containers, no general SLO claim",
 }
 if path:=os.Getenv("M4A_REPORT_PATH");path!=""{
  out,err:=json.MarshalIndent(report,"","  ");if err!=nil{t.Fatal(err)}
  if err:=os.WriteFile(path,out,0600);err!=nil{t.Fatal(err)}
 }
 t.Logf("PASS M4a: HTTP throttle blocked group offset=8 with lag=16; recovered offset=24, trusted=24, measured catch-up %.2f events/s",16/elapsed.Seconds())
}
