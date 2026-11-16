package lagmonitor

import (
 "context"
 "errors"
 "fmt"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/lagrisk"
 "github.com/segmentio/kafka-go"
)

type KafkaSource struct{
 Brokers []string
 Topic string
 Group string
 Partition int
 SafetyMargin time.Duration
 Timeout time.Duration
}

// Collect reads Kafka's authoritative consumer-group committed offset,
// partition log boundaries, effective topic retention and oldest broker append
// timestamp. It does not commit, change topic configs or alter consumer groups.
func(s KafkaSource)Collect(ctx context.Context)(lagrisk.Snapshot,error){
 if len(s.Brokers)==0||s.Topic==""||s.Group==""||s.Partition<0||
    s.SafetyMargin<=0{return lagrisk.Snapshot{},errors.New("Kafka source requires brokers, topic, group, partition and safety margin")}
 timeout:=s.Timeout
 if timeout<=0{timeout=10*time.Second}
 requestCtx,cancel:=context.WithTimeout(ctx,timeout)
 defer cancel()
 client:=&kafka.Client{Addr:kafka.TCP(s.Brokers...),Timeout:timeout}
 offsets,err:=client.OffsetFetch(requestCtx,&kafka.OffsetFetchRequest{
  GroupID:s.Group,Topics:map[string][]int{s.Topic:{s.Partition}},
 })
 if err!=nil{return lagrisk.Snapshot{},fmt.Errorf("fetch Kafka committed group offset: %w",err)}
 if offsets.Error!=nil{return lagrisk.Snapshot{},fmt.Errorf("Kafka offset-fetch group response: %w",offsets.Error)}
 entries:=offsets.Topics[s.Topic]
 if len(entries)!=1||entries[0].Partition!=s.Partition{
  return lagrisk.Snapshot{},errors.New("Kafka group committed offset missing for requested partition")
 }
 if entries[0].Error!=nil{return lagrisk.Snapshot{},fmt.Errorf("Kafka partition offset error: %w",entries[0].Error)}
 committed:=entries[0].CommittedOffset
 if committed<0{return lagrisk.Snapshot{},errors.New("no committed Kafka group offset (unknown bootstrap position)")}

 conf,err:=client.DescribeConfigs(requestCtx,&kafka.DescribeConfigsRequest{
  Resources:[]kafka.DescribeConfigRequestResource{{
   ResourceType:kafka.ResourceTypeTopic,ResourceName:s.Topic,
   ConfigNames:[]string{"retention.ms","message.timestamp.type"},
  }},
 })
 if err!=nil{return lagrisk.Snapshot{},fmt.Errorf("Kafka DescribeConfigs: %w",err)}
 if len(conf.Resources)!=1||conf.Resources[0].ResourceName!=s.Topic{
  return lagrisk.Snapshot{},errors.New("Kafka topic effective config absent")
 }
 if conf.Resources[0].Error!=nil{return lagrisk.Snapshot{},fmt.Errorf("Kafka topic config: %w",conf.Resources[0].Error)}
 actual:=make([]ConfigEntry,0,len(conf.Resources[0].ConfigEntries))
 for _,entry:=range conf.Resources[0].ConfigEntries{
  actual=append(actual,ConfigEntry{Name:entry.ConfigName,Value:entry.ConfigValue})
 }
 retention,err:=ValidatedRetention(actual)
 if err!=nil{return lagrisk.Snapshot{},err}

 var conn *kafka.Conn
 var lastErr error
 for _,broker:=range s.Brokers{
  conn,lastErr=kafka.DialLeader(requestCtx,"tcp",broker,s.Topic,s.Partition)
  if lastErr==nil{break}
 }
 if lastErr!=nil{return lagrisk.Snapshot{},fmt.Errorf("Kafka partition leader unavailable: %w",lastErr)}
 first,err:=conn.ReadFirstOffset()
 if err!=nil{conn.Close();return lagrisk.Snapshot{},fmt.Errorf("Kafka first offset: %w",err)}
 end,err:=conn.ReadLastOffset()
 _=conn.Close()
 if err!=nil{return lagrisk.Snapshot{},fmt.Errorf("Kafka log end: %w",err)}
 snap:=lagrisk.Snapshot{
  Topic:s.Topic,Partition:s.Partition,
  LogStartOffset:first,LogEndOffset:end,CommittedOffset:committed,
  Now:time.Now().UTC(),Retention:retention,SafetyMargin:s.SafetyMargin,
 }
 if committed>=first&&committed<end{
  reader:=kafka.NewReader(kafka.ReaderConfig{
   Brokers:s.Brokers,Topic:s.Topic,Partition:s.Partition,
   MinBytes:1,MaxBytes:1e6,MaxWait:250*time.Millisecond,
  })
  if err:=reader.SetOffset(committed);err!=nil{reader.Close();return lagrisk.Snapshot{},err}
  msg,err:=reader.FetchMessage(requestCtx)
  _=reader.Close()
  if err!=nil{return lagrisk.Snapshot{},fmt.Errorf("oldest unconsumed broker record: %w",err)}
  if msg.Offset<committed||msg.Offset>=end{
   return lagrisk.Snapshot{},fmt.Errorf("unexpected oldest pending offset: %d not in [%d,%d)",msg.Offset,committed,end)
  }
  snap.OldestUnconsumedTimestamp=msg.Time.UTC()
 }
 return snap,nil
}
