package lagmonitor

import (
 "context"
 "errors"
 "net/http"
 "net/http/httptest"
 "strings"
 "strconv"
 "testing"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/lagrisk"
)

type fakeSource struct {
 snap lagrisk.Snapshot
 err error
}
func(f *fakeSource)Collect(context.Context)(lagrisk.Snapshot,error){return f.snap,f.err}

func TestMonitorExportsActualLagAndAlertState(t *testing.T){
 now:=time.Date(2026,11,16,12,0,0,0,time.UTC)
 source:=&fakeSource{snap:lagrisk.Snapshot{
  Topic:"usage.critical.v1",Partition:0,
  CommittedOffset:100,LogStartOffset:0,LogEndOffset:120,
  OldestUnconsumedTimestamp:now.Add(-7*time.Minute),
  Now:now,Retention:10*time.Minute,SafetyMargin:2*time.Minute,
 }}
 m:=New(source,"clickhouse-usage",3*time.Minute)
 if err:=m.Sample(context.Background());err!=nil{t.Fatal(err)}
 response:=httptest.NewRecorder()
 m.ServeHTTP(response,httptest.NewRequest(http.MethodGet,"/metrics",nil))
 body:=response.Body.String()
 required:=[]string{
  "telemetry_kafka_lag_monitor_up 1",
  `telemetry_kafka_lag_offset_distance{group="clickhouse-usage",topic="usage.critical.v1",partition="0"} 20`,
  `telemetry_kafka_retention_warning{group="clickhouse-usage",topic="usage.critical.v1",partition="0"} 1`,
  `telemetry_kafka_retention_critical{group="clickhouse-usage",topic="usage.critical.v1",partition="0"} 0`,
  `telemetry_kafka_retention_time_to_expiry_seconds{group="clickhouse-usage",topic="usage.critical.v1",partition="0"} 180`,
 }
 for _,want:=range required{if !strings.Contains(body,want){t.Errorf("missing metric %q in:\n%s",want,body)}}
}
func TestCollectorErrorsAreNotReportedHealthy(t *testing.T){
 now:=time.Now().UTC()
 source:=&fakeSource{snap:lagrisk.Snapshot{
  Topic:"events",Partition:0,CommittedOffset:7,LogStartOffset:0,LogEndOffset:7,
  Now:now,Retention:10*time.Minute,SafetyMargin:2*time.Minute,
 }}
 m:=New(source,"group",time.Minute)
 if err:=m.Sample(context.Background());err!=nil{t.Fatal(err)}
 source.err=errors.New("group authorization failed")
 if err:=m.Sample(context.Background());err==nil{t.Fatal("expected broker error")}
 rec:=httptest.NewRecorder()
 m.ServeHTTP(rec,httptest.NewRequest("GET","/metrics",nil))
 if !strings.Contains(rec.Body.String(),"telemetry_kafka_lag_monitor_up 0"){
  t.Fatalf("error masked by old healthy reading: %s",rec.Body.String())
 }
 if !strings.Contains(rec.Body.String(),"telemetry_kafka_lag_monitor_failures_total 1"){
  t.Fatalf("missing error counter: %s",rec.Body.String())
 }
 if m.Healthy(){t.Fatal("healthy on failed broker collection")}
}
func TestMetricsEscapesUserControlledLabels(t *testing.T){
 now:=time.Now().UTC()
 source:=&fakeSource{snap:lagrisk.Snapshot{
  Topic:"x\"\\\n y",Partition:0,LogStartOffset:0,CommittedOffset:2,LogEndOffset:2,
  Now:now,Retention:10*time.Minute,SafetyMargin:time.Minute,
 }}
 m:=New(source,"grp",time.Minute)
 if err:=m.Sample(context.Background());err!=nil{t.Fatal(err)}
 rec:=httptest.NewRecorder()
 m.ServeHTTP(rec,httptest.NewRequest("GET","/metrics",nil))
 if strings.Contains(rec.Body.String(),"topic=\"x\"\\\n y\""){t.Fatal("unescaped label injection")}
 if !strings.Contains(rec.Body.String(),"topic="+strconv.Quote(source.snap.Topic)){
  t.Fatalf("escaped topic not found: %s",rec.Body.String())
 }
}
func TestConfigValidationFailsClosed(t *testing.T){
 for _,entries:=range [][]ConfigEntry{
  {{Name:"retention.ms",Value:"-1"},{Name:"message.timestamp.type",Value:"LogAppendTime"}},
  {{Name:"retention.ms",Value:"600000"},{Name:"message.timestamp.type",Value:"CreateTime"}},
  {{Name:"retention.ms",Value:"not-a-duration"},{Name:"message.timestamp.type",Value:"LogAppendTime"}},
  {{Name:"retention.ms",Value:"600000"}},
  {},
 }{
  if _,err:=ValidatedRetention(entries);err==nil{t.Errorf("invalid effective Kafka configs accepted: %+v",entries)}
 }
 got,err:=ValidatedRetention([]ConfigEntry{
  {Name:"retention.ms",Value:"600000"},
  {Name:"message.timestamp.type",Value:"LogAppendTime"},
 })
 if err!=nil||got!=10*time.Minute{t.Fatalf("valid inherited topic retention rejected: %v %s",err,got)}
}
