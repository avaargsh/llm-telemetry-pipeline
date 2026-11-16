// Package lagmonitor exposes verified Kafka retention/lag risk snapshots as
// metrics and JSON. No Kafka offsets are ever committed by this monitor.
package lagmonitor

import (
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "net/http"
 "strconv"
 "strings"
 "sync"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/lagrisk"
)

type ConfigEntry struct{ Name string; Value string }
func ValidatedRetention(entries []ConfigEntry)(time.Duration,error){
 configs:=map[string]string{}
 for _,e:=range entries{configs[e.Name]=e.Value}
 raw,ok:=configs["retention.ms"]
 if !ok{return 0,errors.New("Kafka effective retention.ms not available")}
 ms,err:=strconv.ParseInt(raw,10,64)
 if err!=nil||ms<=0||ms>int64((1<<63-1)/int64(time.Millisecond)){
  return 0,fmt.Errorf("Kafka effective retention.ms is not a finite positive duration: %q",raw)
 }
 // Payload-supplied CreateTime timestamps cannot be treated as reliable
 // broker oldest-message age. Explicitly require broker LogAppendTime.
 if configs["message.timestamp.type"]!="LogAppendTime"{
  return 0,fmt.Errorf("Kafka message.timestamp.type must be LogAppendTime, got %q",configs["message.timestamp.type"])
 }
 return time.Duration(ms)*time.Millisecond,nil
}

type Source interface{ Collect(context.Context)(lagrisk.Snapshot,error) }

type Monitor struct {
 mu sync.RWMutex
 source Source
 group string
 staleAfter time.Duration
 latest lagrisk.Assessment
 lastSuccess time.Time
 hasSample bool
 lastError string
 failures uint64
}

func New(source Source,group string,staleAfter time.Duration)*Monitor{
 return &Monitor{source:source,group:group,staleAfter:staleAfter}
}
func(m *Monitor)Sample(ctx context.Context)error{
 if m.source==nil{return m.fail(errors.New("Kafka source not configured"))}
 s,err:=m.source.Collect(ctx)
 if err!=nil{return m.fail(err)}
 a,err:=lagrisk.Assess(s)
 if err!=nil{return m.fail(err)}
 m.mu.Lock()
 m.latest=a
 m.hasSample=true
 m.lastSuccess=time.Now().UTC()
 m.lastError=""
 m.mu.Unlock()
 return nil
}
func(m *Monitor)fail(err error)error{
 m.mu.Lock()
 m.lastError=err.Error()
 m.failures++
 m.mu.Unlock()
 return err
}
func(m *Monitor)Healthy()bool{
 m.mu.RLock()
 defer m.mu.RUnlock()
 return m.hasSample&&m.lastError==""&&m.staleAfter>0&&time.Since(m.lastSuccess)<=m.staleAfter
}
func statusCode(b bool) int{
 if b{return 1}
 return 0
}
func promLabels(group string,a lagrisk.Assessment)string{
 return "{group="+strconv.Quote(group)+",topic="+strconv.Quote(a.Topic)+",partition="+strconv.Quote(strconv.Itoa(a.Partition))+"}"
}
func promGauge(b *strings.Builder,name,labels string,v any){
 fmt.Fprintf(b,"%s%s %v\n",name,labels,v)
}
func(m *Monitor)ServeHTTP(w http.ResponseWriter,r *http.Request){
 if r.Method!=http.MethodGet{http.Error(w,"GET required",http.StatusMethodNotAllowed);return}
 m.mu.RLock()
 a,ok,errText,last,failures:=m.latest,m.hasSample,m.lastError,m.lastSuccess,m.failures
 group,staleAfter:=m.group,m.staleAfter
 m.mu.RUnlock()
 healthy:=ok&&errText==""&&staleAfter>0&&time.Since(last)<=staleAfter
 switch r.URL.Path{
 case "/healthz":
  if !healthy{http.Error(w,"Kafka lag observation missing, failed or stale",http.StatusServiceUnavailable);return}
  w.WriteHeader(http.StatusOK)
  fmt.Fprintln(w,"ok")
 case "/status":
  w.Header().Set("Content-Type","application/json")
  status:=struct {
   Up bool `json:"up"`
   Error string `json:"last_error,omitempty"`
   LastSuccess time.Time `json:"last_success,omitempty"`
   Assessment *lagrisk.Assessment `json:"assessment,omitempty"`
  }{Up:healthy,Error:errText,LastSuccess:last}
  if ok{status.Assessment=&a}
  _=json.NewEncoder(w).Encode(status)
 case "/metrics":
  w.Header().Set("Content-Type","text/plain; version=0.0.4; charset=utf-8")
  var b strings.Builder
  promGauge(&b,"telemetry_kafka_lag_monitor_up","",statusCode(healthy))
  promGauge(&b,"telemetry_kafka_lag_monitor_failures_total","",failures)
  promGauge(&b,"telemetry_kafka_lag_monitor_stale","",statusCode(!healthy))
  if ok{
   labels:=promLabels(group,a)
   promGauge(&b,"telemetry_kafka_lag_monitor_last_success_timestamp_seconds","",last.Unix())
   promGauge(&b,"telemetry_kafka_lag_offset_distance",labels,a.LagOffsetDistance)
   promGauge(&b,"telemetry_kafka_log_start_offset",labels,a.LogStartOffset)
   promGauge(&b,"telemetry_kafka_log_end_offset",labels,a.LogEndOffset)
   promGauge(&b,"telemetry_kafka_group_committed_offset",labels,a.CommittedOffset)
   promGauge(&b,"telemetry_kafka_retention_time_to_expiry_seconds",labels,a.TimeToExpirySeconds)
   promGauge(&b,"telemetry_kafka_retention_configured_seconds",labels,a.RetentionSeconds)
   promGauge(&b,"telemetry_kafka_retention_missing_offset_distance",labels,a.MissingOffsetDistance)
   promGauge(&b,"telemetry_kafka_retention_warning",labels,statusCode(a.Status==lagrisk.StatusWarning))
   promGauge(&b,"telemetry_kafka_retention_critical",labels,statusCode(a.Status==lagrisk.StatusCritical||a.Status==lagrisk.StatusExpired))
   promGauge(&b,"telemetry_kafka_retention_data_gap",labels,statusCode(a.Status==lagrisk.StatusDataGap))
   promGauge(&b,"telemetry_kafka_retention_no_backlog",labels,statusCode(a.Status==lagrisk.StatusNoBacklog))
   if a.OldestUnconsumedTimestamp!=nil{
    promGauge(&b,"telemetry_kafka_oldest_unconsumed_timestamp_seconds",labels,a.OldestUnconsumedTimestamp.Unix())
    age:=a.ObservedAt.Sub(*a.OldestUnconsumedTimestamp).Seconds()
    promGauge(&b,"telemetry_kafka_oldest_unconsumed_age_seconds",labels,fmt.Sprintf("%.3f",age))
   }
  }
  _,_ = w.Write([]byte(b.String()))
 default:http.NotFound(w,r)
 }
}
