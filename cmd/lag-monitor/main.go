// lag-monitor observes one Kafka consumer-group topic partition without
// joining that group or modifying any offsets. Metrics are informational,
// not authorization for billing or automatic deletion.
package main

import (
 "context"
 "flag"
 "fmt"
 "log"
 "net/http"
 "os"
 "os/signal"
 "strings"
 "syscall"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/lagmonitor"
)

func run()error{
 brokers:=flag.String("brokers","localhost:29092","comma separated Kafka brokers")
 topic:=flag.String("topic","usage.critical.v1","monitored topic")
 group:=flag.String("group","telemetry-clickhouse-v1","observed consumer group")
 partition:=flag.Int("partition",0,"observed partition number")
 listen:=flag.String("listen","127.0.0.1:9098","HTTP listen address; use an auth proxy for shared environments")
 interval:=flag.Duration("interval",5*time.Second,"poll interval")
 margin:=flag.Duration("safety-margin",2*time.Minute,"retention critical threshold")
 timeout:=flag.Duration("timeout",6*time.Second,"Kafka poll timeout")
 flag.Parse()
 if *interval<=0||*margin<=0||*timeout<=0||*topic==""||*group==""||*partition<0{
  return fmt.Errorf("invalid monitor parameters")
 }
 source:=lagmonitor.KafkaSource{
  Brokers:strings.Split(*brokers,","),Topic:*topic,Group:*group,
  Partition:*partition,Margin:*margin,Timeout:*timeout,
 }
 monitor:=lagmonitor.New(source,*group,*interval)
 ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGINT,syscall.SIGTERM)
 defer stop()
 server:=&http.Server{Addr:*listen,Handler:monitor,ReadHeaderTimeout:5*time.Second}
 go monitor.Run(ctx)
 errCh:=make(chan error,1)
 go func(){errCh<-server.ListenAndServe()}()
 select{
 case <-ctx.Done():
  closeCtx,cancel:=context.WithTimeout(context.Background(),5*time.Second)
  defer cancel()
  return server.Shutdown(closeCtx)
 case err:=<-errCh:
  if err==http.ErrServerClosed{return nil}
  return err
 }
}
func main(){
 if err:=run();err!=nil{log.Printf("lag monitor exited: %v",err);os.Exit(1)}
}
