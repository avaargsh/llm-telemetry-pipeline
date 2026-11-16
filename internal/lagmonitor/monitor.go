// Package lagmonitor publishes read-only, fail-closed Kafka group lag and
// estimated retention headroom. It never joins the consumer group.
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

type Source interface {
 Read(context.Context,time.Time)([]lagrisk.Snapshot,error)
}
type State struct {
 Up bool `json:"up"`
 CollectedAt time.Time `json:"collected_at"`
 Error string `json:"error,omitempty"`
 Partitions []lagrisk.Assessment `json:"partitions"`
}
type Monitor struct {
 GroupID string
 Source Source
 mu sync.RWMutex
 state State
}

func(m *Monitor)State()State{
 m.mu.RLock();defer m.mu.RUnlock()
 s:=m.state
 s.Partitions=append([]lagrisk.Assessment(nil),m.state.Partitions...)
 return s
}
func(m *Monitor)storeFailure(now time.Time,err error)error{
 m.mu.Lock()
 // Remove prior successful partition gauges on *any* failed scrape, otherwise
 // a stale healthy result can hide a real broker/source outage.
 m.state=State{Up:false,CollectedAt:now.UTC(),Error:err.Error(),Partitions:[]lagrisk.Assessment{}}
 m.mu.Unlock()
 return err
}
func(m *Monitor)Poll(ctx context.Context,now time.Time)error{
 if m.GroupID==""||m.Source==nil||now.IsZero(){
  return m.storeFailure(now,errors.New("monitor group/source/observation time required"))
 }
 snaps,err:=m.Source.Read(ctx,now)
 if err!=nil{return m.storeFailure(now,fmt.Errorf("read Kafka lag: %w",err))}
 if len(snaps)==0||len(snaps)>32{
  return m.storeFailure(now,fmt.Errorf("invalid partition reading count %d (expected 1..32)",len(snaps)))
 }
 seen:=make(map[string]bool,len(snaps))
 values:=make([]lagrisk.Assessment,0,len(snaps))
 for _,snapshot:=range snaps{
  key:=fmt.Sprintf("%s/%d",snapshot.Topic,snapshot.Partition)
  if seen[key]{return m.storeFailure(now,fmt.Errorf("duplicate partition reading %s",key))}
  seen[key]=true
  assessed,err:=lagrisk.Assess(snapshot)
  if err!=nil{return m.storeFailure(now,fmt.Errorf("partition %s: %w",key,err))}
  values=append(values,assessed)
 }
 m.mu.Lock()
 m.state=State{Up:true,CollectedAt:now.UTC(),Partitions:values}
 m.mu.Unlock()
 return nil
}

func label(s string)string{
 s=strings.ReplaceAll(s,"\\","\\\\")
 s=strings.ReplaceAll(s,"\n","\\n")
 return strings.ReplaceAll(s,"\"","\\\"")
}
func (m *Monitor)Handler()http.Handler {
 mux:=http.NewServeMux()
 mux.HandleFunc("/status",func(w http.ResponseWriter,r *http.Request){
  if r.Method!=http.MethodGet{http.Error(w,"method not allowed",http.StatusMethodNotAllowed);return}
  w.Header().Set("Content-Type","application/json")
  state:=m.State()
  if !state.Up{w.WriteHeader(http.StatusServiceUnavailable)}
  _=json.NewEncoder(w).Encode(state)
 })
 mux.HandleFunc("/metrics",func(w http.ResponseWriter,r *http.Request){
  if r.Method!=http.MethodGet{http.Error(w,"method not allowed",http.StatusMethodNotAllowed);return}
  w.Header().Set("Content-Type","text/plain; version=0.0.4; charset=utf-8")
  state:=m.State()
  fmt.Fprintln(w,"# HELP llm_telemetry_lag_scrape_success Latest broker lag sample succeeded (0 means no trusted partition metrics).")
  fmt.Fprintln(w,"# TYPE llm_telemetry_lag_scrape_success gauge")
  if !state.Up{fmt.Fprintln(w,"llm_telemetry_lag_scrape_success 0");return}
  fmt.Fprintln(w,"llm_telemetry_lag_scrape_success 1")
  specs:=[]struct{name,help string}{
   {"llm_telemetry_kafka_lag_offsets","Observed broker log-end minus group committed offset, not necessarily event count."},
   {"llm_telemetry_kafka_log_start_offset","Kafka log start offset."},
   {"llm_telemetry_kafka_committed_offset","Broker-confirmed consumer group next offset."},
   {"llm_telemetry_kafka_retention_headroom_seconds","Estimated time until configured retention is reached, not a deletion guarantee."},
   {"llm_telemetry_kafka_oldest_unconsumed_age_seconds","Age of oldest available unconsumed broker-append timestamp."},
   {"llm_telemetry_kafka_retention_risk","Partition risk state currently active (value 1)."},
   {"llm_telemetry_kafka_data_gap_offsets","Offset distance already below broker log start."},
  }
  for _,spec:=range specs{
   fmt.Fprintf(w,"# HELP %s %s\n# TYPE %s gauge\n",spec.name,spec.help,spec.name)
  }
  for _,p:=range state.Partitions{
   ls:=fmt.Sprintf("group=\"%s\",topic=\"%s\",partition=\"%s\"",label(m.GroupID),label(p.Topic),strconv.Itoa(p.Partition))
   fmt.Fprintf(w,"llm_telemetry_kafka_lag_offsets{%s} %d\n",ls,p.LagOffsetDistance)
   fmt.Fprintf(w,"llm_telemetry_kafka_log_start_offset{%s} %d\n",ls,p.LogStartOffset)
   fmt.Fprintf(w,"llm_telemetry_kafka_committed_offset{%s} %d\n",ls,p.CommittedOffset)
   fmt.Fprintf(w,"llm_telemetry_kafka_data_gap_offsets{%s} %d\n",ls,p.MissingOffsetDistance)
   fmt.Fprintf(w,"llm_telemetry_kafka_retention_risk{%s,status=\"%s\"} 1\n",ls,p.Status)
   if p.OldestUnconsumedTimestamp!=nil{
    age:=p.ObservedAt.Sub(*p.OldestUnconsumedTimestamp).Seconds()
    fmt.Fprintf(w,"llm_telemetry_kafka_oldest_unconsumed_age_seconds{%s} %.3f\n",ls,age)
    fmt.Fprintf(w,"llm_telemetry_kafka_retention_headroom_seconds{%s} %d\n",ls,p.TimeToExpirySeconds)
   }
  }
 })
 return mux
}
