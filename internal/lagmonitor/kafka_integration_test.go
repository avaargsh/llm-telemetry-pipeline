package lagmonitor

import (
 "context"
 "encoding/json"
 "fmt"
 "net/http"
 "net/http/httptest"
 "os"
 "strings"
 "testing"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
 "github.com/avaargsh/llm-telemetry-pipeline/internal/lagrisk"
 "github.com/segmentio/kafka-go"
)

func TestRealKafkaBrokerLagAndRetentionMonitoring(t *testing.T){
 if os.Getenv("TEST_M3E_KAFKA")!="1"{t.Skip("requires isolated Kafka topic and broker")}
 broker:=os.Getenv("TEST_KAFKA_BROKER")
 if broker==""{t.Fatal("TEST_KAFKA_BROKER required")}
 topic:="m3e.monitor.v1"
 group:=fmt.Sprintf("m3e-monitor-%d",time.Now().UnixNano())
 ctx,cancel:=context.WithTimeout(context.Background(),90*time.Second)
 defer cancel()
 producer:=&kafka.Writer{Addr:kafka.TCP(broker),Topic:topic,
   RequiredAcks:kafka.RequireAll,Balancer:&kafka.Hash{},Async:false,AllowAutoTopicCreation:false}
 for i:=0;i<5;i++{
  e,err:=event.SyntheticUsageEvent(group,i,time.Date(2026,11,16,11,0,0,0,time.UTC))
  if err!=nil{t.Fatal(err)}
  raw,err:=json.Marshal(e);if err!=nil{t.Fatal(err)}
  if err:=producer.WriteMessages(ctx,kafka.Message{Key:[]byte(e.EventID),Value:raw});err!=nil{t.Fatal(err)}
 }
 if err:=producer.Close();err!=nil{t.Fatal(err)}
 r:=kafka.NewReader(kafka.ReaderConfig{Brokers:[]string{broker},Topic:topic,
   GroupID:group,StartOffset:kafka.FirstOffset,CommitInterval:0,MinBytes:1,MaxBytes:10e6})
 for i:=0;i<2;i++{
  msg,err:=r.FetchMessage(ctx)
  if err!=nil{r.Close();t.Fatal(err)}
  if err:=r.CommitMessages(ctx,msg);err!=nil{r.Close();t.Fatal(err)}
 }
 if err:=r.Close();err!=nil{t.Fatal(err)}
 source:=KafkaSource{Brokers:[]string{broker},Topic:topic,Group:group,
    Partition:0,Margin:2*time.Minute,Timeout:10*time.Second}
 monitor:=New(source,group,time.Second)
 var report Report
 for i:=0;i<15;i++{
  if err:=monitor.Poll(ctx);err==nil{
   report=monitor.Report()
   if report.Healthy&&report.Assessment.LagOffsetDistance==3{break}
  }
  select{case <-time.After(300*time.Millisecond):case <-ctx.Done():t.Fatal(ctx.Err())}
 }
 if !report.Healthy{t.Fatalf("monitor not healthy: %+v",monitor.Report())}
 a:=report.Assessment
 if a.LagOffsetDistance!=3||a.CommittedOffset!=2||a.LogEndOffset!=5||
   a.RetentionSeconds!=600||a.OldestUnconsumedTimestamp==nil||
   a.Status!=lagrisk.StatusHealthy{
  t.Fatalf("broker-derived lag and retention invalid: %+v",a)
 }
 // The source is authoritative Kafka group commits, not the monitor's own
 // cursor. The monitor must not affect group offsets when it polls.
 recorder:=httptest.NewRecorder()
 monitor.ServeHTTP(recorder,httptest.NewRequest(http.MethodGet,"/metrics",nil))
 if recorder.Code!=http.StatusOK||!strings.Contains(recorder.Body.String(),"telemetry_kafka_lag_offsets")||
  !strings.Contains(recorder.Body.String()," 3\n"){
  t.Fatalf("Prometheus live metrics absent: %s",recorder.Body.String())
 }
 r=kafka.NewReader(kafka.ReaderConfig{Brokers:[]string{broker},Topic:topic,
    GroupID:group,StartOffset:kafka.FirstOffset,CommitInterval:0,MinBytes:1,MaxBytes:10e6})
 for i:=0;i<3;i++{
  msg,err:=r.FetchMessage(ctx)
  if err!=nil{r.Close();t.Fatal(err)}
  if err:=r.CommitMessages(ctx,msg);err!=nil{r.Close();t.Fatal(err)}
 }
 if err:=r.Close();err!=nil{t.Fatal(err)}
 if err:=monitor.Poll(ctx);err!=nil{t.Fatal(err)}
 after:=monitor.Report().Assessment
 if after.Status!=lagrisk.StatusNoBacklog||after.LagOffsetDistance!=0||
   after.CommittedOffset!=5{
  t.Fatalf("monitor did not report recovery from backlog: %+v",after)
 }
 t.Logf("PASS M3e live Kafka: committed 2/5 lag 3, retention 600s, then committed 5/5 lag 0")
}
