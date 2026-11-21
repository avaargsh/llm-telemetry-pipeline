// outbox-relay publishes persisted PostgreSQL Outbox rows to Kafka.
// A crash after broker ACK but before MarkPublished may publish twice.
package main

import (
    "context"
    "database/sql"
    "errors"
    "fmt"
    "flag"
    "log"
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
// validateRelayTuning constrains the experiment knobs independently of Kafka
// ACK semantics. The defaults preserve the previously tested behavior.
func validateRelayTuning(limit int, batchTimeout time.Duration) error {
    if limit < 1 || limit > 500 {
        return fmt.Errorf("relay claim limit must be 1..500, got %d", limit)
    }
    if batchTimeout < time.Millisecond || batchTimeout > time.Second {
        return fmt.Errorf("Kafka batch timeout must be between 1ms and 1s, got %s", batchTimeout)
    }
    return nil
}
func main(){
    dsn:=flag.String("dsn","postgres://telemetry:localdev-only@localhost:15432/telemetry?sslmode=disable","PostgreSQL DSN (local dev)")
    brokers:=flag.String("brokers","localhost:29092","comma-separated Kafka brokers")
    worker:=flag.String("worker","relay-local-1","stable lease owner ID; must be unique among active relay instances")
    poll:=flag.Duration("poll",300*time.Millisecond,"idle/retry polling interval")
    claimLimit:=flag.Int("claim-limit",5,"maximum outbox rows leased per run, 1..500")
    batchTimeout:=flag.Duration("kafka-batch-timeout",100*time.Millisecond,"Kafka synchronous publisher batch timeout; 1ms..1s")
    flag.Parse()
    if *worker==""||*poll<=0{log.Fatal("invalid worker or poll interval")}
    if err:=validateRelayTuning(*claimLimit,*batchTimeout);err!=nil{log.Fatal(err)}
    ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGINT,syscall.SIGTERM);defer stop()
    db,err:=sql.Open("pgx",*dsn);if err!=nil{log.Fatal(err)};defer db.Close()
    if err:=db.PingContext(ctx);err!=nil{log.Fatal(err)}
    writer:=&kafka.Writer{
        Addr:kafka.TCP(strings.Split(*brokers,",")...),Balancer:&kafka.Hash{},
        RequiredAcks:kafka.RequireAll,Async:false,AllowAutoTopicCreation:false,
        BatchTimeout:*batchTimeout,
    }
    defer writer.Close()
    relay:=outbox.Relay{
        Repo:&outbox.Store{DB:db},Publisher:kafkaPublisher{writer},
        WorkerID:*worker,Lease:120*time.Second,Limit:*claimLimit,
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
