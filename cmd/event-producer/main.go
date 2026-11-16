// event-producer reads sealed usage events from stdin and sends them to Kafka.
// A successful run confirms Kafka acks, NOT ClickHouse persistence.
package main

import (
    "bufio"
    "context"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "os"
    "os/signal"
    "strings"
    "syscall"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
    "github.com/segmentio/kafka-go"
)

func main() {
    brokers:=flag.String("brokers","localhost:29092","comma-separated bootstrap brokers")
    topic:=flag.String("topic","usage.critical.v1","pre-created Kafka topic")
    batchSize:=flag.Int("batch-size",250,"max messages per synchronous Kafka write")
    flag.Parse()
    if *batchSize<1 || *batchSize>10000 {log.Fatal("batch-size must be 1..10000")}
    ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGINT,syscall.SIGTERM);defer stop()
    writer:=&kafka.Writer{
       Addr:kafka.TCP(strings.Split(*brokers,",")...),Topic:*topic,
       Balancer:&kafka.Hash{},RequiredAcks:kafka.RequireAll,
       Async:false,AllowAutoTopicCreation:false,
       BatchTimeout:100*time.Millisecond,
    }
    defer func(){if err:=writer.Close();err!=nil{log.Printf("writer close: %v",err)}}()
    scanner:=bufio.NewScanner(os.Stdin)
    scanner.Buffer(make([]byte,64*1024),8<<20)
    batch:=make([]kafka.Message,0,*batchSize)
    sent:=0
    flush:=func()error{
        if len(batch)==0{return nil}
        if err:=writer.WriteMessages(ctx,batch...);err!=nil{return fmt.Errorf("Kafka publish outcome unknown for batch of %d: %w",len(batch),err)}
        sent+=len(batch)
        batch=batch[:0]
        return nil
    }
    for scanner.Scan(){
        var e event.UsageEvent
        if err:=json.Unmarshal(scanner.Bytes(),&e);err!=nil{log.Fatalf("invalid NDJSON: %v",err)}
        if err:=e.Validate();err!=nil{log.Fatalf("invalid usage event: %v",err)}
        b:=append([]byte(nil),scanner.Bytes()...)
        batch=append(batch,kafka.Message{Key:[]byte(e.TenantID+"|"+e.RequestID),Value:b})
        if len(batch)>=*batchSize {if err:=flush();err!=nil{log.Fatal(err)}}
    }
    if err:=scanner.Err();err!=nil{log.Fatal(err)}
    if err:=flush();err!=nil{log.Fatal(err)}
    log.Printf("Kafka acknowledged %d usage events; duplicates are possible on ambiguous network failures",sent)
}
