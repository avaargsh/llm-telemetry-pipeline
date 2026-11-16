// outbox-relay publishes persisted PostgreSQL Outbox rows to Kafka.
// A crash after broker ACK but before MarkPublished may publish twice.
package main

import (
    "context"
    "database/sql"
    "errors"
    "flag"
    "log"
    "os"
    "os/signal"
    "strings"
    "syscall"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/outbox"
    "github.com/segmentio/kafka-go"
    _ "github.com/jackc/pgx/v5/stdlib"
)
type kafkaPublisher struct{writer *kafka.Writer}
func(p kafkaPublisher)Publish(ctx context.Context,r outbox.Row)error{
    pubCtx,cancel:=context.WithTimeout(ctx,15*time.Second);defer cancel()
    return p.writer.WriteMessages(pubCtx,kafka.Message{Topic:r.Topic,Key:[]byte(r.EventID),Value:r.Payload})
}
func main(){
    dsn:=flag.String("dsn","postgres://telemetry:localdev-only@localhost:15432/telemetry?sslmode=disable","PostgreSQL DSN (local dev)")
    brokers:=flag.String("brokers","localhost:29092","comma-separated Kafka brokers")
    worker:=flag.String("worker","relay-local-1","stable lease owner ID; must be unique among active relay instances")
    poll:=flag.Duration("poll",300*time.Millisecond,"idle/retry polling interval")
    flag.Parse()
    if *worker==""||*poll<=0{log.Fatal("invalid worker or poll interval")}
    ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGINT,syscall.SIGTERM);defer stop()
    db,err:=sql.Open("pgx",*dsn);if err!=nil{log.Fatal(err)};defer db.Close()
    if err:=db.PingContext(ctx);err!=nil{log.Fatal(err)}
    writer:=&kafka.Writer{
        Addr:kafka.TCP(strings.Split(*brokers,",")...),Balancer:&kafka.Hash{},
        RequiredAcks:kafka.RequireAll,Async:false,AllowAutoTopicCreation:false,
        BatchTimeout:100*time.Millisecond,
    }
    defer writer.Close()
    relay:=outbox.Relay{
        Repo:&outbox.Store{DB:db},Publisher:kafkaPublisher{writer},
        WorkerID:*worker,Lease:60*time.Second,Limit:100,
    }
    for ctx.Err()==nil{
        count,err:=relay.RunOnce(ctx)
        if err!=nil&&!errors.Is(err,context.Canceled){log.Printf("outbox retryable error: %v",err)}
        if count==0 || err!=nil{
            select{case <-ctx.Done():return;case <-time.After(*poll):}
        } else {
            log.Printf("outbox published=%d",count)
        }
    }
}
