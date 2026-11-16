package alertdelivery

import (
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "net"
 "net/http"
 "os"
 "os/exec"
 "testing"
 "time"
)

func runComposeM4A(t *testing.T,ctx context.Context,args ...string){
 t.Helper()
 a:=append([]string{"compose","-f","deployments/alerting/docker-compose.yml"},args...)
 cmd:=exec.CommandContext(ctx,"docker",a...)
 cmd.Dir="../.." // go test executes in internal/alertdelivery
 output,err:=cmd.CombinedOutput()
 if err!=nil{t.Fatalf("docker %v failed: %v\n%s",a,err,output)}
}
func awaitHTTPM4A(ctx context.Context,url string)error{
 client:=&http.Client{Timeout:3*time.Second}
 ticker:=time.NewTicker(time.Second)
 defer ticker.Stop()
 for{
  req,err:=http.NewRequestWithContext(ctx,http.MethodGet,url,nil)
  if err!=nil{return err}
  resp,err:=client.Do(req)
  if err==nil{
   _=resp.Body.Close()
   if resp.StatusCode==http.StatusOK{return nil}
  }
  select{case <-ctx.Done():return ctx.Err();case <-ticker.C:}
 }
}
func awaitNotificationM4A(ctx context.Context,p *Probe,name,status string)(time.Duration,error){
 start:=time.Now()
 ticker:=time.NewTicker(350*time.Millisecond)
 defer ticker.Stop()
 for{
  for _,event:=range p.Notifications(){
   if event.AlertName==name&&event.Status==status{
    return time.Since(start),nil
   }
  }
  select{case <-ctx.Done():return 0,fmt.Errorf("waiting for %s %s: %w (events=%v)",name,status,ctx.Err(),p.Notifications())
  case <-ticker.C:}
 }
}

func TestRealPrometheusAlertmanagerWebhookFiringAndResolved(t *testing.T){
 if os.Getenv("TEST_M4A_ALERT_DELIVERY")!="1"{
  t.Skip("requires Docker Prometheus and Alertmanager; isolated CI only")
 }
 ctx,cancel:=context.WithTimeout(context.Background(),7*time.Minute)
 defer cancel()
 p:=NewProbe()
 p.SetMetrics(true,false,0)
 listener,err:=net.Listen("tcp","0.0.0.0:19080")
 if err!=nil{t.Fatal(err)}
 srv:=&http.Server{Handler:p.Handler(),ReadHeaderTimeout:5*time.Second}
 served:=make(chan error,1)
 go func(){served<-srv.Serve(listener)}()
 t.Cleanup(func(){
  shutdownCtx,stop:=context.WithTimeout(context.Background(),3*time.Second)
  defer stop()
  _=srv.Shutdown(shutdownCtx)
  select{case err:=<-served:if err!=nil&&!errors.Is(err,http.ErrServerClosed){t.Log(err)}
  case <-time.After(time.Second):}
 })
 runComposeM4A(t,ctx,"up","-d")
 t.Cleanup(func(){
  cleanupCtx,stop:=context.WithTimeout(context.Background(),30*time.Second)
  defer stop()
  cmd:=exec.CommandContext(cleanupCtx,"docker","compose","-f","deployments/alerting/docker-compose.yml","down","-v","--remove-orphans")
  cmd.Dir="../.."
  if out,err:=cmd.CombinedOutput();err!=nil{t.Logf("compose cleanup error: %v %s",err,out)}
 })
 if err:=awaitHTTPM4A(ctx,"http://localhost:19090/-/ready");err!=nil{t.Fatal(err)}
 if err:=awaitHTTPM4A(ctx,"http://localhost:19093/-/ready");err!=nil{t.Fatal(err)}
 // Baseline needs a real successful scrape; avoid attributing bootstrap
 // target discovery to the alert scenario.
 select{case <-time.After(5*time.Second):case <-ctx.Done():t.Fatal(ctx.Err())}
 t.Log("Prometheus and Alertmanager ready; initial telemetry sample healthy")
 
 criticalAndGapStart:=time.Now()
 p.SetMetrics(true,true,5)
 names:=[]string{"KafkaRetentionCritical","KafkaCommittedBeforeLogStart"}
 latencies:=map[string]float64{}
 for _,name:=range names{
  elapsed,err:=awaitNotificationM4A(ctx,p,name,"firing")
  if err!=nil{t.Fatal(err)}
  latencies[name+"_firing_seconds"]=elapsed.Seconds()
 }
 // Both alerts must have originated from Prometheus and been routed through
 // Alertmanager, not from direct test-side POSTs to our receiver.
 p.SetMetrics(true,false,0)
 for _,name:=range names{
  elapsed,err:=awaitNotificationM4A(ctx,p,name,"resolved")
  if err!=nil{t.Fatal(err)}
  latencies[name+"_resolved_seconds"]=elapsed.Seconds()
 }
 t.Logf("received firing+resolved notifications for %v after %v",names,time.Since(criticalAndGapStart))
 
 p.SetMetrics(false,false,0)
 unavailStart:=time.Now()
 elapsed,err:=awaitNotificationM4A(ctx,p,"KafkaLagCollectorUnavailable","firing")
 if err!=nil{t.Fatal(err)}
 latencies["KafkaLagCollectorUnavailable_firing_seconds"]=elapsed.Seconds()
 // This alert has a deliberate production for:30s rule; it must not arrive
 // until the collector has remained unavailable for approximately that long.
 if time.Since(unavailStart)<25*time.Second{
  t.Fatalf("collector outage notification fired too early (for:30s): %s",time.Since(unavailStart))
 }
 p.SetMetrics(true,false,0)
 elapsed,err=awaitNotificationM4A(ctx,p,"KafkaLagCollectorUnavailable","resolved")
 if err!=nil{t.Fatal(err)}
 latencies["KafkaLagCollectorUnavailable_resolved_seconds"]=elapsed.Seconds()

 events:=p.Notifications()
 for _,name:=range []string{"KafkaRetentionCritical","KafkaCommittedBeforeLogStart","KafkaLagCollectorUnavailable"}{
  for _,status:=range []string{"firing","resolved"}{
   found:=false
   for _,e:=range events{
    if e.AlertName==name&&e.Status==status&&e.Severity=="critical"{found=true;break}
   }
   if !found{t.Fatalf("missing real Alertmanager notification %s %s; got %v",name,status,events)}
  }
 }
 report:=map[string]any{
  "scenario":"prometheus_rules_to_alertmanager_webhook_firing_and_resolved",
  "prometheus_image":"prom/prometheus:v3.5.0",
  "alertmanager_image":"prom/alertmanager:v0.28.1",
  "event_count":len(events),
  "required_alerts":[]string{"KafkaRetentionCritical","KafkaCommittedBeforeLogStart","KafkaLagCollectorUnavailable"},
  "firing_and_resolved_verified":true,
  "latency_seconds":latencies,
  "notifications":events,
  "evidence":"real alert-engine evaluation and delivery, synthetic input metrics; not an on-call receiver",
 }
 if path:=os.Getenv("M4A_REPORT_PATH");path!=""{
  b,err:=json.MarshalIndent(report,"","  ");if err!=nil{t.Fatal(err)}
  if err:=os.WriteFile(path,b,0600);err!=nil{t.Fatal(err)}
 }
 t.Logf("PASS real Prometheus→Alertmanager→webhook: %d messages across 3 firing/resolved alerts",len(events))
}
