package alertdelivery

import (
 "encoding/json"
 "net/http"
 "net/http/httptest"
 "strings"
 "testing"
)

func TestProbeExportsOnlyCurrentMetricsAndCapturesWebhook(t *testing.T){
 p:=NewProbe()
 p.SetMetrics(true,false,0)
 res:=httptest.NewRecorder()
 p.Handler().ServeHTTP(res,httptest.NewRequest(http.MethodGet,"/metrics",nil))
 if res.Code!=200||!strings.Contains(res.Body.String(),"llm_telemetry_lag_scrape_success 1"){
  t.Fatalf("healthy scrape not published: %s",res.Body.String())
 }
 p.SetMetrics(false,true,5)
 res=httptest.NewRecorder()
 p.Handler().ServeHTTP(res,httptest.NewRequest(http.MethodGet,"/metrics",nil))
 if !strings.Contains(res.Body.String(),"llm_telemetry_lag_scrape_success 0")||
    !strings.Contains(res.Body.String(),`status="critical"} 1`)||
    !strings.Contains(res.Body.String(),"llm_telemetry_kafka_data_gap_offsets"){
  t.Fatalf("current severity and gap not exported: %s",res.Body.String())
 }
 payload:=`{"status":"firing","alerts":[{"status":"firing","labels":{"alertname":"KafkaRetentionCritical","severity":"critical"},"annotations":{"summary":"test"}}]}`
 rec:=httptest.NewRecorder()
 req:=httptest.NewRequest(http.MethodPost,"/alerts",strings.NewReader(payload))
 req.Header.Set("Content-Type","application/json")
 p.Handler().ServeHTTP(rec,req)
 if rec.Code!=200{t.Fatalf("webhook POST status %d",rec.Code)}
 events:=p.Notifications()
 if len(events)!=1||events[0].Status!="firing"||
    events[0].AlertName!="KafkaRetentionCritical"||events[0].Severity!="critical"{
  b,_:=json.Marshal(events);t.Fatalf("failed to capture notification: %s",b)
 }
}
func TestProbeRejectsInvalidWebhookAndNeverRecordsIt(t *testing.T){
 p:=NewProbe()
 for _,bad:=range []string{
  "{",`{"status":"firing","alerts":[]}`,
  `{"status":"invented","alerts":[{"labels":{"alertname":"a"}}]}`,
 }{
  rec:=httptest.NewRecorder()
  p.Handler().ServeHTTP(rec,httptest.NewRequest(http.MethodPost,"/alerts",strings.NewReader(bad)))
  if rec.Code<400{t.Fatalf("accepted invalid payload %q: code=%d",bad,rec.Code)}
 }
 if len(p.Notifications())!=0{t.Fatal("invalid notifications changed evidence")}
}
