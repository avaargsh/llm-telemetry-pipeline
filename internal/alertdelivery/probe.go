// Package alertdelivery is an isolated CI-only Prometheus sample source
// and Alertmanager webhook evidence collector. It never contacts on-call
// endpoints or modifies Kafka or ClickHouse production state.
package alertdelivery

import (
 "encoding/json"
 "fmt"
 "io"
 "net/http"
 "sync"
 "time"
)

type Notification struct {
 ReceivedAt time.Time `json:"received_at"`
 Status string `json:"status"`
 AlertName string `json:"alertname"`
 Severity string `json:"severity"`
}
type webhook struct {
 Status string `json:"status"`
 Alerts []struct{
  Status string `json:"status"`
  Labels map[string]string `json:"labels"`
 } `json:"alerts"`
}
type Probe struct {
 mu sync.RWMutex
 healthy bool
 critical bool
 gap float64
 notifications []Notification
}
func NewProbe()*Probe{return &Probe{}}
func(p *Probe)SetMetrics(healthy,critical bool,gap float64){
 p.mu.Lock();p.healthy=healthy;p.critical=critical;p.gap=gap;p.mu.Unlock()
}
func(p *Probe)Notifications()[]Notification{
 p.mu.RLock();defer p.mu.RUnlock()
 return append([]Notification(nil),p.notifications...)
}
func(p *Probe)Handler()http.Handler{
 mux:=http.NewServeMux()
 mux.HandleFunc("/metrics",func(w http.ResponseWriter,r *http.Request){
  if r.Method!=http.MethodGet{http.Error(w,"method not allowed",http.StatusMethodNotAllowed);return}
  p.mu.RLock()
  healthy,critical,gap:=p.healthy,p.critical,p.gap
  p.mu.RUnlock()
  w.Header().Set("Content-Type","text/plain; version=0.0.4")
  success:=0
  if healthy{success=1}
  _,_=fmt.Fprintf(w,"# TYPE llm_telemetry_lag_scrape_success gauge\nllm_telemetry_lag_scrape_success %d\n",success)
  if !healthy{return} // never export stale partition data
  active:=0;if critical{active=1}
  _,_=fmt.Fprintf(w,"# TYPE llm_telemetry_kafka_retention_risk gauge\nllm_telemetry_kafka_retention_risk{group=\"m4a\",topic=\"alert-lab\",partition=\"0\",status=\"critical\"} %d\n",active)
  _,_=fmt.Fprintf(w,"# TYPE llm_telemetry_kafka_data_gap_offsets gauge\nllm_telemetry_kafka_data_gap_offsets{group=\"m4a\",topic=\"alert-lab\",partition=\"0\"} %.0f\n",gap)
 })
 mux.HandleFunc("/alerts",func(w http.ResponseWriter,r *http.Request){
  if r.Method!=http.MethodPost{http.Error(w,"method not allowed",http.StatusMethodNotAllowed);return}
  defer r.Body.Close()
  decoder:=json.NewDecoder(io.LimitReader(r.Body,1<<20))
  var payload webhook
  if err:=decoder.Decode(&payload);err!=nil{http.Error(w,"invalid webhook JSON",http.StatusBadRequest);return}
  if (payload.Status!="firing"&&payload.Status!="resolved")||len(payload.Alerts)==0{
   http.Error(w,"invalid webhook status/alerts",http.StatusBadRequest);return
  }
  events:=make([]Notification,0,len(payload.Alerts))
  now:=time.Now().UTC()
  for _,a:=range payload.Alerts{
   if (a.Status!="firing"&&a.Status!="resolved")||a.Labels["alertname"]==""{
    http.Error(w,"invalid webhook alert",http.StatusBadRequest);return
   }
   events=append(events,Notification{
    ReceivedAt:now,Status:a.Status,
    AlertName:a.Labels["alertname"],Severity:a.Labels["severity"],
   })
  }
  p.mu.Lock();p.notifications=append(p.notifications,events...);p.mu.Unlock()
  w.WriteHeader(http.StatusOK)
 })
 return mux
}
