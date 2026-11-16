// lag-monitor exposes read-only Kafka group lag and retention risk as
// Prometheus metrics/JSON. It does not modify Kafka offsets.
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
 brokers:=flag.String("brokers","localhost:29092","Kafka bootstrap addresses, comma-separated")
 topic:=flag.String("topic","usage.critical.v1","Kafka topic")
 group:=flag.String("group","telemetry-clickhouse-v1","Kafka consumer group ID")
 partition:=flag.Int("partition",0,"Kafka partition to observe")
 listen:=flag.String("listen","127.0.0.1:9194","HTTP listen address (default loopback)")
 interval:=flag.Duration("interval",5*time.Second,"Kafka scrape interval")
 timeout:=flag.Duration("timeout",4*time.Second,"Kafka scrape timeout")
 margin:=flag.Duration("safety-margin",2*time.Hour,"retention critical safety margin")
 flag.Parse()
 if *interval<=0||*timeout<=0||*timeout>=*interval||
    *margin<=0||*group==""||*topic==""||*brokers==""||*partition<0{
  log.Fatal("invalid monitor options: need interval > timeout > 0, topic, group, partition, safety margin")
 }
 ctx,stop:=signal.NotifyContext(context.Background(),os.Interrupt,syscall.SIGTERM)
 defer stop()
 src:=lagmonitor.KafkaSource{
  Brokers:strings.Split(*brokers,","),Topic:*topic,Group:*group,
  Partition:*partition,SafetyMargin:*margin,Timeout:*timeout,
 }
 monitor:=lagmonitor.New(src,*group,3*(*interval))
 mux:=http.NewServeMux()
 mux.Handle("/metrics",monitor)
 mux.Handle("/status",monitor)
 mux.Handle("/healthz",monitor)
 server:=&http.Server{
  Addr:*listen,Handler:mux,ReadHeaderTimeout:5*time.Second,
 }
 go func(){
  <-ctx.Done()
  shutCtx,cancel:=context.WithTimeout(context.Background(),5*time.Second)
  defer cancel()
  _=server.Shutdown(shutCtx)
 }()
 go func(){
  takeSample:=func(){
   if err:=monitor.Sample(ctx);err!=nil{log.Printf("Kafka monitoring sample failed (not healthy): %v",err)}
  }
  takeSample()
  ticker:=time.NewTicker(*interval)
  defer ticker.Stop()
  for{
   select{
   case <-ctx.Done():return
   case <-ticker.C:takeSample()
   }
  }
 }()
 log.Printf("lag-monitor listen=%s topic=%s group=%s partition=%d",*listen,*topic,*group,*partition)
 if err:=server.ListenAndServe();err!=nil&&!errors.Is(err,http.ErrServerClosed){log.Fatal(err)}
}
