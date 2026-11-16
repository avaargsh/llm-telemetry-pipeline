// lag-exporter reads Kafka consumer group offsets without joining the group.
// It publishes observed lag and estimated retention risk, never billing data.
package main

import (
 "context"
 "errors"
 "flag"
 "log"
 "net/http"
 "os"
 "os/signal"
 "strings"
 "syscall"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/lagmonitor"
)

func main(){
 brokers:=flag.String("brokers","localhost:29092","comma-separated Kafka bootstrap addresses")
 group:=flag.String("group","telemetry-clickhouse-v1","Kafka consumer group to observe (read only)")
 topic:=flag.String("topic","usage.critical.v1","Kafka topic to observe (read only)")
 listen:=flag.String("listen","127.0.0.1:9108","local Prometheus/JSON HTTP listen address")
 interval:=flag.Duration("interval",15*time.Second,"sampling interval")
 timeout:=flag.Duration("timeout",10*time.Second,"per-sample timeout")
 margin:=flag.Duration("safety-margin",2*time.Minute,"critical retention headroom threshold")
 maxPartitions:=flag.Int("max-partitions",16,"maximum sampled topic partitions (1..32)")
 flag.Parse()
 if *interval<=0||*timeout<=0||*margin<=0{
  log.Fatal("interval, timeout and safety-margin must be positive")
 }
 ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGINT,syscall.SIGTERM)
 defer stop()
 source:=lagmonitor.KafkaSource{
  Brokers:strings.Split(*brokers,","),GroupID:*group,Topic:*topic,
  SafetyMargin:*margin,MaxPartitions:*maxPartitions,
 }
 monitor:=&lagmonitor.Monitor{GroupID:*group,Source:source}
 poll:=func(){
  pctx,cancel:=context.WithTimeout(ctx,*timeout)
  defer cancel()
  if err:=monitor.Poll(pctx,time.Now().UTC());err!=nil{
   log.Printf("lag/retention sample FAIL (metric success=0): %v",err)
  }
 }
 poll()
 srv:=&http.Server{Addr:*listen,Handler:monitor.Handler(),ReadHeaderTimeout:5*time.Second}
 errCh:=make(chan error,1)
 go func(){errCh<-srv.ListenAndServe()}()
 ticker:=time.NewTicker(*interval)
 defer ticker.Stop()
 for {
  select{
  case <-ctx.Done():
   closeCtx,cancel:=context.WithTimeout(context.Background(),5*time.Second)
   _=srv.Shutdown(closeCtx)
   cancel()
   return
  case err:=<-errCh:
   if err!=nil&&!errors.Is(err,http.ErrServerClosed){log.Fatalf("lag exporter HTTP server failed: %v",err)}
   return
  case <-ticker.C:
   poll()
  }
 }
}
