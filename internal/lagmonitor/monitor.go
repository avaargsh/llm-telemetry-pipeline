// Package lagmonitor publishes live Kafka consumer-group lag and retention
// risk as Prometheus exposition and structured JSON. No cached healthy state
// survives a failed scrape, and this package does not commit Kafka offsets.
package lagmonitor

import (
 "context"
 "encoding/json"
 "fmt"
 "net/http"
 "strconv"
 "strings"
 "sync"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/lagrisk"
)

type Source interface{Read(context.Context)(lagrisk.Snapshot,error)}
type Report struct{
 ObservedAt time.Time `json:"observed_at"`
 Healthy bool `json:"collector_healthy"`
 Error string `json:"error,omitempty"`
 Assessment lagrisk.Assessment `json:"assessment"`
}
type Monitor struct{
 source Source
 group string
 interval time.Duration
 mu sync.RWMutex
 report Report
}
func New(source Source,group string,interval time.Duration)*Monitor{
 if interval<=0{interval=time.Second}
 return &Monitor{source:source,group:group,interval:interval,report:Report{Error:"no poll performed"}}
}
func(m *Monitor)Report()Report{m.mu.RLock();defer m.mu.RUnlock();return m.report}
func(m *Monitor)Poll(ctx context.Context)error{
 var assessment lagrisk.Assessment
 var err error
 if m.source==nil{err=fmt.Errorf("Kafka source missing")}else{
  var sample lagrisk.Snapshot
  sample,err=m.source.Read(ctx)
  if err==nil{assessment,err=lagrisk.Assess(sample)}
 }
 m.mu.Lock()
 defer m.mu.Unlock()
 if err!=nil{
  m.report=Report{ObservedAt:time.Now().UTC(),Healthy:false,Error:err.Error()}
  return err
 }
 m.report=Report{ObservedAt:time.Now().UTC(),Healthy:true,Assessment:assessment}
 return nil
}
func(m *Monitor)Run(ctx context.Context){
 _=m.Poll(ctx)
 ticker:=time.NewTicker(m.interval);defer ticker.Stop()
 for{
  select{
  case <-ctx.Done():return
  case <-ticker.C:_=m.Poll(ctx)
  }
 }
}
func metricLabels(group string,a lagrisk.Assessment)string{
 return "{group="+strconv.Quote(group)+",topic="+strconv.Quote(a.Topic)+",partition="+strconv.Quote(strconv.Itoa(a.Partition))+"}"
}
func(m *Monitor)ServeHTTP(w http.ResponseWriter,r *http.Request){
 switch r.URL.Path{
 case "/metrics":
  w.Header().Set("Content-Type","text/plain; version=0.0.4; charset=utf-8")
  report:=m.Report()
  up,errorMetric:=0,1
  if report.Healthy{up,errorMetric=1,0}
  _,_=fmt.Fprintf(w,"# HELP telemetry_kafka_monitor_up Whether the last Kafka observation completed.\n# TYPE telemetry_kafka_monitor_up gauge\ntelemetry_kafka_monitor_up %d\n",up)
  _,_=fmt.Fprintf(w,"# HELP telemetry_kafka_monitor_error Whether the last observation failed or has never run.\n# TYPE telemetry_kafka_monitor_error gauge\ntelemetry_kafka_monitor_error %d\n",errorMetric)
  if !report.Healthy{return}
  a:=report.Assessment
  labels:=metricLabels(m.group,a)
  for _,x:=range []struct{name string;value any}{
   {"telemetry_kafka_lag_offsets",a.LagOffsetDistance},
   {"telemetry_kafka_log_start_offset",a.LogStartOffset},
   {"telemetry_kafka_log_end_offset",a.LogEndOffset},
   {"telemetry_kafka_committed_offset",a.CommittedOffset},
   {"telemetry_kafka_missing_offset_distance",a.MissingOffsetDistance},
   {"telemetry_kafka_retention_remaining_seconds",a.TimeToExpirySeconds},
   {"telemetry_kafka_retention_config_seconds",a.RetentionSeconds},
  }{
   _,_=fmt.Fprintf(w,"# TYPE %s gauge\n%s%s %v\n",x.name,x.name,labels,x.value)
  }
  // State-set metrics permit direct alerting without an ad-hoc threshold
  // computed from unchecked timestamps in a PromQL expression.
  for _,status:=range []lagrisk.Status{
   lagrisk.StatusNoBacklog,lagrisk.StatusHealthy,lagrisk.StatusWarning,
   lagrisk.StatusCritical,lagrisk.StatusExpired,lagrisk.StatusDataGap,
  }{
   v:=0;if a.Status==status{v=1}
   _,_=fmt.Fprintf(w,"telemetry_kafka_retention_state%s %d\n",
     strings.TrimSuffix(labels,"}")+",state="+strconv.Quote(string(status))+"}",v)
  }
 case "/status":
  report:=m.Report()
  w.Header().Set("Content-Type","application/json")
  if !report.Healthy{w.WriteHeader(http.StatusServiceUnavailable)}
  _=json.NewEncoder(w).Encode(report)
 default:
  http.NotFound(w,r)
 }
}
