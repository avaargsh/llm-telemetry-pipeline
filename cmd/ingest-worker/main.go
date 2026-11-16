// ingest-worker implements a single, synchronous, fail-closed consumer-group
// worker. On any uncertain ClickHouse outcome, it exits WITHOUT committing.
// Kafka replays from the last committed position when restarted.
package main

import (
    "context"
    "encoding/json"
    "errors"
    "flag"
    "log"
    "os"
    "os/signal"
    "strings"
    "syscall"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/clickhouse"
    "github.com/avaargsh/llm-telemetry-pipeline/internal/ingest"
    "github.com/segmentio/kafka-go"
)

type groupCommitter struct { reader *kafka.Reader }
func (c groupCommitter) Commit(ctx context.Context,messages []ingest.Message)error{
    msgs:=make([]kafka.Message,0,len(messages))
    for _,m:=range messages{msgs=append(msgs,kafka.Message{Topic:m.Topic,Partition:int(m.Partition),Offset:m.Offset})}
    return c.reader.CommitMessages(ctx,msgs...)
}

// KafkaDLQ sends a stable identity and raw message to a dedicated topic.
// A failed or ambiguous ACK is NOT permission to skip the source record.
type KafkaDLQ struct{writer *kafka.Writer}
func(p KafkaDLQ)Publish(ctx context.Context,d ingest.DeadLetter)error{
    encoded,err:=json.Marshal(d);if err!=nil{return err}
    publishCtx,cancel:=context.WithTimeout(ctx,15*time.Second)
    defer cancel()
    return p.writer.WriteMessages(publishCtx,kafka.Message{Key:[]byte(d.StableKey()),Value:encoded})
}
func run(ctx context.Context,reader *kafka.Reader,sink *clickhouse.HTTPWriter,dlq ingest.DeadLetterPublisher,batchSize int,fetchWait time.Duration)error{
    batch:=make([]ingest.Message,0,batchSize)
    flush:=func()error{
        if len(batch)==0{return nil}
        // An insert may have succeeded even if this call returns an error.
        // Do not commit and do not try to skip a poison message in M1.
        if err:=ingest.ProcessBatchWithDLQ(ctx,batch,sink,groupCommitter{reader},dlq);err!=nil{return err}
        log.Printf("confirmed and committed batch=%d",len(batch))
        batch=batch[:0]
        return nil
    }
    for {
        fetchCtx,cancel:=context.WithTimeout(ctx,fetchWait)
        msg,err:=reader.FetchMessage(fetchCtx)
        cancel()
        if err!=nil {
            if ctx.Err()!=nil {return ctx.Err()}
            if errors.Is(err,context.DeadlineExceeded){if err:=flush();err!=nil{return err};continue}
            return err
        }
        batch=append(batch,ingest.Message{Topic:msg.Topic,Partition:int32(msg.Partition),Offset:msg.Offset,Value:msg.Value})
        if len(batch)>=batchSize{if err:=flush();err!=nil{return err}}
    }
}

func main(){
    brokers:=flag.String("brokers","localhost:29092","comma-separated bootstrap brokers")
    topic:=flag.String("topic","usage.critical.v1","Kafka topic")
    group:=flag.String("group","telemetry-clickhouse-v1","consumer group ID")
    dlqTopic:=flag.String("dlq-topic","events.dlq.v1","durable poison-message topic")
    batchSize:=flag.Int("batch-size",250,"maximum events per ClickHouse insert")
    fetchWait:=flag.Duration("fetch-wait",1*time.Second,"flush partial batch if no message arrives")
    endpoint:=flag.String("clickhouse","http://localhost:18123","ClickHouse HTTP endpoint")
    db:=flag.String("database","telemetry","ClickHouse database")
    user:=flag.String("user","telemetry","ClickHouse username")
    password:=flag.String("password","localdev-only","ClickHouse password (local dev only)")
    flag.Parse()
    if *batchSize<1 || *batchSize>10000 || *fetchWait<=0 {log.Fatal("invalid batch-size or fetch-wait")}
    ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGINT,syscall.SIGTERM);defer stop()
    reader:=kafka.NewReader(kafka.ReaderConfig{
        Brokers:strings.Split(*brokers,","),Topic:*topic,GroupID:*group,
        StartOffset:kafka.FirstOffset,CommitInterval:0,MinBytes:1,MaxBytes:10e6,MaxWait:250*time.Millisecond,
    })
    defer func(){if err:=reader.Close();err!=nil{log.Printf("reader close: %v",err)}}()
    if *dlqTopic=="" {log.Fatal("dlq-topic is required")}
    dlqWriter:=&kafka.Writer{Addr:kafka.TCP(strings.Split(*brokers,",")...),Topic:*dlqTopic,Balancer:&kafka.Hash{},
        RequiredAcks:kafka.RequireAll,Async:false,AllowAutoTopicCreation:false,BatchTimeout:100*time.Millisecond}
    defer dlqWriter.Close()
    sink:=&clickhouse.HTTPWriter{Endpoint:*endpoint,Database:*db,User:*user,Password:*password}
    if err:=run(ctx,reader,sink,KafkaDLQ{dlqWriter},*batchSize,*fetchWait);err!=nil && !errors.Is(err,context.Canceled){log.Printf("stopped without committing incomplete batch: %v",err);os.Exit(1)}
}
