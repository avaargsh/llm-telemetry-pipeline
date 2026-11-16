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
type scriptedSource struct{
 readings [][]lagrisk.Snapshot
 errors []error
 calls int
}
func (s *scriptedSource)Read(ctx context.Context,now time.Time)([]lagrisk.Snapshot,error){
 i:=s.calls;s.calls++
 if i<len(s.errors)&&s.errors[i]!=nil{return nil,s.errors[i]}
 return s.readings[i],nil
}
func monitorFixture()lagrisk.Snapshot{
 now:=time.Date(2026,11,17,0,0,0,0,time.UTC)
 return lagrisk.Snapshot{
  Topic:"m3e.monitor.v1",Partition:0,LogStartOffset:0,LogEndOffset:20,
  CommittedOffset:8,Now:now,
  OldestUnconsumedTimestamp:now.Add(-11*time.Second),
  Retention:time.Minute,SafetyMargin:25*time.Second,
 }
}
func TestMonitorExposesRetentionWarningAndRecovers(t *testing.T){
 p:=monitorFixture()
 resolved:=p
 resolved.CommittedOffset=20
 resolved.OldestUnconsumedTimestamp=time.Time{}
 src:=&scriptedSource{readings:[][]lagrisk.Snapshot{{p},{resolved}}}
 m:=&Monitor{GroupID:"m3e-consumer",Source:src}
 if err:=m.Poll(context.Background(),p.Now);err!=nil{t.Fatal(err)}
 state:=m.State()
 if !state.Up||len(state.Partitions)!=1||state.Partitions[0].Status!=lagrisk.StatusWarning {
  t.Fatalf("warning not surfaced: %+v",state)
 }
 mux:=m.Handler()
 rr:=httptest.NewRecorder()
 mux.ServeHTTP(rr,httptest.NewRequest(http.MethodGet,"/metrics",nil))
 if rr.Code!=200{t.Fatalf("metrics HTTP %d",rr.Code)}
 for _,want:=range []string{
  "llm_telemetry_lag_scrape_success 1",
  "llm_telemetry_kafka_lag_offsets{group=\"m3e-consumer\",topic=\"m3e.monitor.v1\",partition=\"0\"} 12",
  "llm_telemetry_kafka_retention_risk{group=\"m3e-consumer\",topic=\"m3e.monitor.v1\",partition=\"0\",status=\"warning\"} 1",
 }{
  if !strings.Contains(rr.Body.String(),want){t.Fatalf("missing metric %q in %s",want,rr.Body.String())}
 }
 if err:=m.Poll(context.Background(),p.Now.Add(time.Second));err!=nil{t.Fatal(err)}
 if got:=m.State().Partitions[0].Status;got!=lagrisk.StatusNoBacklog{t.Fatalf("alert not resolved after commit: %s",got)}
 rr=httptest.NewRecorder()
 mux.ServeHTTP(rr,httptest.NewRequest(http.MethodGet,"/metrics",nil))
 if strings.Contains(rr.Body.String(),`status="warning"} 1`){t.Fatalf("stale warning still firing: %s",rr.Body.String())}
}
func TestMonitorCollectorFailureDropsStaleHealthyGauges(t *testing.T){
 p:=monitorFixture();p.CommittedOffset=20;p.OldestUnconsumedTimestamp=time.Time{}
 src:=&scriptedSource{readings:[][]lagrisk.Snapshot{{p}},errors:[]error{nil,errors.New("broker unavailable")}}
 m:=&Monitor{GroupID:"test-group",Source:src}
 if err:=m.Poll(context.Background(),p.Now);err!=nil{t.Fatal(err)}
 if err:=m.Poll(context.Background(),p.Now.Add(time.Second));err==nil{t.Fatal("fetch failure hidden")}
 if m.State().Up||len(m.State().Partitions)!=0{t.Fatal("last good metrics retained after failed scrape")}
 r:=httptest.NewRecorder();m.Handler().ServeHTTP(r,httptest.NewRequest(http.MethodGet,"/metrics",nil))
 if !strings.Contains(r.Body.String(),"llm_telemetry_lag_scrape_success 0"){t.Fatalf("missing failure gauge: %s",r.Body.String())}
 if strings.Contains(r.Body.String(),"llm_telemetry_kafka_lag_offsets"){t.Fatalf("stale previous metrics emitted: %s",r.Body.String())}
}
func TestMonitorFailsClosedOnIncompleteAndDuplicateReadings(t *testing.T){
 p:=monitorFixture()
 src:=&scriptedSource{readings:[][]lagrisk.Snapshot{{},{p,p}}}
 m:=&Monitor{GroupID:"group",Source:src}
 for i:=0;i<2;i++{
  if err:=m.Poll(context.Background(),p.Now);err==nil{t.Fatalf("invalid reading %d accepted",i)}
  if m.State().Up{t.Fatalf("failed reading %d was healthy",i)}
 }
}
func TestMonitorSerializesStatusWithoutSensitiveRawEvents(t *testing.T){
 p:=monitorFixture()
 m:=&Monitor{GroupID:"group",Source:&scriptedSource{readings:[][]lagrisk.Snapshot{{p}}}}
 if err:=m.Poll(context.Background(),p.Now);err!=nil{t.Fatal(err)}
 rr:=httptest.NewRecorder()
 m.Handler().ServeHTTP(rr,httptest.NewRequest(http.MethodGet,"/status",nil))
 if rr.Code!=200||!strings.Contains(rr.Body.String(),`"up":true`){t.Fatalf("bad JSON status %s",rr.Body.String())}
 if strings.Contains(rr.Body.String(),"password")||strings.Contains(rr.Body.String(),"payload"){t.Fatal("status leaked event content")}
}
