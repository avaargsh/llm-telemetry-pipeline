// ingest-worker uses Kafka's actual consumer group generation IDs for
// ownership and offset commits. It fails closed on unknown downstream ACKs.
package main

import (
    "context"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "log"
    "os/signal"
    "strings"
    "syscall"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/clickhouse"
    "github.com/avaargsh/llm-telemetry-pipeline/internal/ingest"
    "github.com/segmentio/kafka-go"
)

// KafkaDLQ synchronously acknowledges poison events before a source offset
// can be committed. A timeout or ambiguous ACK leaves the source uncommitted.
type KafkaDLQ struct{writer *kafka.Writer}
func(p KafkaDLQ)Publish(ctx context.Context,d ingest.DeadLetter)error{
    encoded,err:=json.Marshal(d);if err!=nil{return err}
    publishCtx,cancel:=context.WithTimeout(ctx,15*time.Second)
    defer cancel()
    return p.writer.WriteMessages(publishCtx,kafka.Message{
        Key:[]byte(d.StableKey()),Value:encoded,
    })
}

func run()error{
    brokers:=flag.String("brokers","localhost:29092","comma-separated bootstrap brokers")
    topic:=flag.String("topic","usage.critical.v1","Kafka source topic")
    group:=flag.String("group","telemetry-clickhouse-v1","consumer group ID")
    dlqTopic:=flag.String("dlq-topic","events.dlq.v1","durable dead-letter topic")
    batchSize:=flag.Int("batch-size",250,"maximum confirmed events per partition batch")
    fetchWait:=flag.Duration("fetch-wait",time.Second,"flush partial batch if idle")
    endpoint:=flag.String("clickhouse","http://localhost:18123","ClickHouse HTTP endpoint")
    db:=flag.String("database","telemetry","ClickHouse database")
    user:=flag.String("user","telemetry","ClickHouse user")
    password:=flag.String("password","localdev-only","local development password")
    flag.Parse()
    if *batchSize<1||*batchSize>10000||*fetchWait<=0||*group==""||
       *dlqTopic==""||*topic==""||*brokers==""{
        return errors.New("invalid batch/idle/group/topic/DLQ/broker configuration")
    }
    ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGINT,syscall.SIGTERM)
    defer stop()
    hosts:=strings.Split(*brokers,",")
    writer:=&kafka.Writer{
        Addr:kafka.TCP(hosts...),Topic:*dlqTopic,Balancer:&kafka.Hash{},
        RequiredAcks:kafka.RequireAll,Async:false,AllowAutoTopicCreation:false,
        BatchTimeout:100*time.Millisecond,
    }
    defer func(){if err:=writer.Close();err!=nil{log.Printf("DLQ close: %v",err)}}()
    sink:=&clickhouse.HTTPWriter{Endpoint:*endpoint,Database:*db,User:*user,Password:*password}
    worker:=ingest.GroupWorker{
        Brokers:hosts,Topic:*topic,GroupID:*group,BatchSize:*batchSize,
        FetchWait:*fetchWait,Sink:sink,DLQ:KafkaDLQ{writer},
    }
    if err:=worker.Run(ctx);err!=nil&&!errors.Is(err,context.Canceled){
        return fmt.Errorf("generation-fenced ingestion stopped with uncommitted records: %w",err)
    }
    return nil
}

func main(){
    if err:=run();err!=nil{log.Fatal(err)}
}
