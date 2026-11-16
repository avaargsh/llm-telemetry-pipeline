// A disposable test fixture, never an inference event or billing authority.
// It establishes a real Kafka committed group position for M4 alert E2E.
package main

import (
 "context"
 "errors"
 "flag"
 "fmt"
 "log"
 "time"

 "github.com/segmentio/kafka-go"
)

func run()error{
 mode:=flag.String("mode","","seed or catchup")
 broker:=flag.String("broker","localhost:29092","Kafka bootstrap address")
 topic:=flag.String("topic","m4.alert.e2e.v1","disposable topic")
 group:=flag.String("group","m4-live-alert-ci","disposable consumer group")
 flag.Parse()
 ctx,cancel:=context.WithTimeout(context.Background(),50*time.Second)
 defer cancel()
 switch *mode {
 case "seed":
  writer:=&kafka.Writer{
   Addr:kafka.TCP(*broker),Topic:*topic,
   RequiredAcks:kafka.RequireAll,Async:false,AllowAutoTopicCreation:false,
  }
  for i:=0;i<20;i++{
   if err:=writer.WriteMessages(ctx,kafka.Message{
    Key:[]byte(fmt.Sprintf("m4-%02d",i)),
    Value:[]byte(fmt.Sprintf("m4-alert-test-message-%02d",i)),
   });err!=nil{_ = writer.Close();return fmt.Errorf("seed offset %d: %w",i,err)}
  }
  if err:=writer.Close();err!=nil{return err}
  return consume(ctx,*broker,*topic,*group,0,8)
 case "catchup":
  return consume(ctx,*broker,*topic,*group,8,20)
 default:
  return errors.New("--mode must be seed or catchup")
 }
}
func consume(ctx context.Context,broker,topic,group string,start,end int)error{
 reader:=kafka.NewReader(kafka.ReaderConfig{
  Brokers:[]string{broker},Topic:topic,GroupID:group,
  StartOffset:kafka.FirstOffset,CommitInterval:0,MinBytes:1,MaxBytes:10e6,
  MaxWait:250*time.Millisecond,
 })
 defer reader.Close()
 for offset:=start;offset<end;offset++{
  m,err:=reader.FetchMessage(ctx)
  if err!=nil{return fmt.Errorf("fetch offset %d: %w",offset,err)}
  if m.Offset!=int64(offset){
   return fmt.Errorf("expected Kafka source offset %d, got %d",offset,m.Offset)
  }
  if offset==end-1{
   if err:=reader.CommitMessages(ctx,m);err!=nil{
    return fmt.Errorf("commit next Kafka offset %d: %w",end,err)
   }
  }
 }
 log.Printf("group=%s committed next offset=%d topic=%s",group,end,topic)
 return nil
}
func main(){if err:=run();err!=nil{log.Fatal(err)}}
