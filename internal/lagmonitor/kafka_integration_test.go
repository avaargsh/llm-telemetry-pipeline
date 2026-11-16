package lagmonitor

import (
 "context"
 "fmt"
 "net/http"
 "net/http/httptest"
 "os"
 "strings"
 "testing"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/lagrisk"
 "github.com/segmentio/kafka-go"
)

func TestRealKafkaGroupOffsetsAndEffectiveRetentionMetrics(t *testing.T){
 if os.Getenv("TEST_KAFKA_LAG")!="1"{t.Skip("requires isolated Kafka Docker environment")}
 broker:=os.Getenv("TEST_KAFKA_BROKER")
 if broker==""{t.Fatal("TEST_KAFKA_BROKER required")}
 const topic="m3e.monitor.v1"
 group:=fmt.Sprintf("m3e-lag-%d",time.Now().UnixNano())
 ctx,cancel:=context.WithTimeout(context.Background(),90*time.Second)
 defer cancel()
 w:=&kafka.Writer{Addr:kafka.TCP(broker),Topic:topic,RequiredAcks:kafka.RequireAll,
  Async:false,AllowAutoTopicCreation:false,BatchSize:1}
 for i:=0;i<20;i++{
  if err:=w.WriteMessages(ctx,kafka.Message{Key:[]byte(fmt.Sprint(i)),Value:[]byte(fmt.Sprintf("record-%02d",i))});err!=nil{
   _=w.Close();t.Fatal(err)
  }
 }
 if err:=w.Close();err!=nil{t.Fatal(err)}
 reader:=kafka.NewReader(kafka.ReaderConfig{Brokers:[]string{broker},Topic:topic,GroupID:group,
  StartOffset:kafka.FirstOffset,CommitInterval:0,MinBytes:1,MaxBytes:10e6,MaxWait:250*time.Millisecond})
 for i:=0;i<8;i++{
  m,err:=reader.FetchMessage(ctx)
  if err!=nil{_ = reader.Close();t.Fatal(err)}
  if m.Offset!=int64(i){_ = reader.Close();t.Fatalf("wrong source offset got=%d want=%d",m.Offset,i)}
  if i==7{
   if err:=reader.CommitMessages(ctx,m);err!=nil{_ = reader.Close();t.Fatal(err)}
  }
 }
 if err:=reader.Close();err!=nil{t.Fatal(err)}
 source:=KafkaSource{Brokers:[]string{broker},GroupID:group,Topic:topic,
  SafetyMargin:35*time.Second,MaxPartitions:2}
 monitor:=&Monitor{GroupID:group,Source:source}
 if err:=monitor.Poll(ctx,time.Now().UTC());err!=nil{t.Fatal(err)}
 state:=monitor.State()
 if !state.Up||len(state.Partitions)!=1{t.Fatalf("collector failed: %+v",state)}
 a:=state.Partitions[0]
 if a.CommittedOffset!=8||a.LogEndOffset!=20||a.LagOffsetDistance!=12{
  t.Fatalf("Kafka group offset/lag not actually observed: %+v",a)
 }
 if a.RetentionSeconds!=60||a.OldestUnconsumedTimestamp==nil{
  t.Fatalf("effective Kafka retention or broker timestamp missing: %+v",a)
 }
 if a.Status!=lagrisk.StatusWarning{
  t.Fatalf("expected active warning from short retention and 35s margin; got %s",a.Status)
 }
 resp:=httptest.NewRecorder()
 monitor.Handler().ServeHTTP(resp,httptest.NewRequest(http.MethodGet,"/metrics",nil))
 if !strings.Contains(resp.Body.String(),`status="warning"} 1`){
  t.Fatalf("live Kafka warning not exposed on /metrics: %s",resp.Body.String())
 }
 // Alter the risk *policy*, not Kafka time, to explicitly exercise a
 // critical alert on the same real observations; unit tests cover age
 // progression and severity boundaries independently.
 source.SafetyMargin=59*time.Second
 monitor.Source=source
 if err:=monitor.Poll(ctx,time.Now().UTC());err!=nil{t.Fatal(err)}
 if got:=monitor.State().Partitions[0].Status;got!=lagrisk.StatusCritical{
  t.Fatalf("expected critical risk under 59s margin, got %s",got)
 }

 // Resume consumer group from its confirmed broker offset 8, then ensure
 // real committed offset 20 removes the retention alarm.
 rest:=kafka.NewReader(kafka.ReaderConfig{Brokers:[]string{broker},Topic:topic,GroupID:group,
  StartOffset:kafka.FirstOffset,CommitInterval:0,MinBytes:1,MaxBytes:10e6,MaxWait:250*time.Millisecond})
 start:=time.Now()
 for i:=8;i<20;i++{
  msg,err:=rest.FetchMessage(ctx)
  if err!=nil{_ = rest.Close();t.Fatal(err)}
  if msg.Offset!=int64(i){_ = rest.Close();t.Fatalf("replay jumped/skipped: got=%d want=%d",msg.Offset,i)}
  if i==19{if err:=rest.CommitMessages(ctx,msg);err!=nil{_ = rest.Close();t.Fatal(err)}}
 }
 recovery:=time.Since(start)
 if err:=rest.Close();err!=nil{t.Fatal(err)}
 if err:=monitor.Poll(ctx,time.Now().UTC());err!=nil{t.Fatal(err)}
 if got:=monitor.State().Partitions[0].Status;got!=lagrisk.StatusNoBacklog{
  t.Fatalf("consumer caught up but alert still active: %s",got)
 }
 if path:=os.Getenv("M3E_LAG_REPORT_PATH");path!=""{
  record:=fmt.Sprintf("{\"broker_observed_lag_offsets\":12,\"effective_retention_seconds\":60,\"initial_committed_offset\":8,\"final_committed_offset\":20,\"warning_fired\":true,\"critical_fired_under_59s_margin\":true,\"alert_resolved_after_catchup\":true,\"recovery_records\":12,\"recovery_seconds\":%.4f}\n",recovery.Seconds())
  if err:=os.WriteFile(path,[]byte(record),0600);err!=nil{t.Fatal(err)}
 }
 t.Logf("PASS Kafka source: real group committed 8→20, lag 12→0, warning→critical(policy)→resolved, recovery=%.3fs",recovery.Seconds())
}
