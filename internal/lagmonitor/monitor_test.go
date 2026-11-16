package lagmonitor

import (
 "context"
 "errors"
 "net/http"
 "net/http/httptest"
 "strings"
 "testing"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/lagrisk"
)

type fixture struct {
 snapshot lagrisk.Snapshot
 err error
}
func(f *fixture) Read(context.Context)(lagrisk.Snapshot,error){return f.snapshot,f.err}

func baseline()lagrisk.Snapshot{
 now:=time.Date(2026,11,17,8,0,0,0,time.UTC)
 return lagrisk.Snapshot{
  Topic:"m3e.monitor.v1",Partition:0,LogStartOffset:0,LogEndOffset:10,
  CommittedOffset:5,OldestUnconsumedTimestamp:now.Add(-time.Minute),
  Now:now,Retention:10*time.Minute,SafetyMargin:2*time.Minute,
 }
}
func TestMonitorPublishesRealStatusAndCannotKeepStaleHealthyOnFailure(t *testing.T){
 f:=&fixture{snapshot:baseline()}
 m:=New(f,"monitor-group",time.Minute)
 if err:=m.Poll(context.Background());err!=nil{t.Fatal(err)}
 rec:=httptest.NewRecorder()
 m.ServeHTTP(rec,httptest.NewRequest(http.MethodGet,"/metrics",nil))
 if rec.Code!=200{t.Fatal(rec.Code)}
 if !strings.Contains(rec.Body.String(),"telemetry_kafka_monitor_up 1")||
   !strings.Contains(rec.Body.String(),"telemetry_kafka_lag_offsets")||
   !strings.Contains(rec.Body.String(),`state="healthy"} 1`){
  t.Fatalf("metrics did not reflect broker evidence: %s",rec.Body.String())
 }
 f.err=errors.New("coordinator unavailable")
 if err:=m.Poll(context.Background());err==nil{t.Fatal("failed poll was accepted")}
 rec=httptest.NewRecorder()
 m.ServeHTTP(rec,httptest.NewRequest(http.MethodGet,"/metrics",nil))
 text:=rec.Body.String()
 if !strings.Contains(text,"telemetry_kafka_monitor_up 0")||
   !strings.Contains(text,"telemetry_kafka_monitor_error 1")||
   strings.Contains(text,`state="healthy"} 1`){
  t.Fatalf("stale healthy leaked after collector failed: %s",text)
 }
}

func TestAlertSeverityTransitionsAndRecovery(t *testing.T){
 s:=baseline()
 f:=&fixture{snapshot:s}
 m:=New(f,"monitor-group",time.Minute)
 steps:=[]struct{age time.Duration;status lagrisk.Status}{
  {time.Minute,lagrisk.StatusHealthy},
  {7*time.Minute,lagrisk.StatusWarning},
  {9*time.Minute,lagrisk.StatusCritical},
  {11*time.Minute,lagrisk.StatusExpired},
 }
 for _,step:=range steps{
  f.snapshot.OldestUnconsumedTimestamp=s.Now.Add(-step.age)
  if err:=m.Poll(context.Background());err!=nil{t.Fatal(err)}
  report:=m.Report()
  if report.Assessment.Status!=step.status{t.Fatalf("age=%s: got %s expected %s",step.age,report.Assessment.Status,step.status)}
 }
 f.snapshot.LogStartOffset=7
 f.snapshot.OldestUnconsumedTimestamp=s.Now.Add(-time.Minute)
 if err:=m.Poll(context.Background());err!=nil{t.Fatal(err)}
 if m.Report().Assessment.Status!=lagrisk.StatusDataGap{t.Fatal("missing gap alert")}
 f.snapshot.LogStartOffset=0
 f.snapshot.CommittedOffset=f.snapshot.LogEndOffset
 f.snapshot.OldestUnconsumedTimestamp=time.Time{}
 if err:=m.Poll(context.Background());err!=nil{t.Fatal(err)}
 if m.Report().Assessment.Status!=lagrisk.StatusNoBacklog{t.Fatal("alert did not resolve after catch-up")}
}

func TestRetentionConfigValidation(t *testing.T){
 for _,tc:=range []struct{raw string;ok bool}{
  {"600000",true},{"86400000",true},{"-1",false},{"0",false},{"",false},{"not-a-number",false},{"999999999999999999999999",false},
 }{
  _,err:=ParseRetentionMillis(tc.raw)
  if (err==nil)!=tc.ok{t.Errorf("retention %q expected ok=%t err=%v",tc.raw,tc.ok,err)}
 }
}

func TestMonitorRejectsUnsupportedPaths(t *testing.T){
 m:=New(&fixture{snapshot:baseline()},"monitor-group",time.Minute)
 r:=httptest.NewRecorder()
 m.ServeHTTP(r,httptest.NewRequest(http.MethodGet,"/admin",nil))
 if r.Code!=404{t.Fatalf("got %d",r.Code)}
}
