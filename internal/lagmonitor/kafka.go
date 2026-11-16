package lagmonitor

import (
 "context"
 "fmt"
 "strconv"
 "strings"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/lagrisk"
 "github.com/segmentio/kafka-go"
)

// KafkaSource reads the broker's committed offset (never the worker's
// in-memory cursor), log boundaries, first pending record and effective
// topic retention configuration. A missing commit or CreateTime policy is
// an UNKNOWN condition, not a healthy empty backlog.
type KafkaSource struct{
 Brokers []string
 Topic string
 Group string
 Partition int
 Margin time.Duration
 Timeout time.Duration
}

func ParseRetentionMillis(raw string)(time.Duration,error){
 n,err:=strconv.ParseInt(strings.TrimSpace(raw),10,64)
 if err!=nil||n<=0||n>int64((1<<63-1)/int64(time.Millisecond)){
  return 0,fmt.Errorf("Kafka retention.ms %q must be finite, positive and representable",raw)
 }
 return time.Duration(n)*time.Millisecond,nil
}

func(s KafkaSource)Read(ctx context.Context)(lagrisk.Snapshot,error){
 if len(s.Brokers)==0||s.Topic==""||s.Group==""||s.Partition<0||
    s.Margin<=0{return lagrisk.Snapshot{},fmt.Errorf("invalid Kafka monitor configuration")}
 timeout:=s.Timeout
 if timeout<=0{timeout=10*time.Second}
 subctx,cancel:=context.WithTimeout(ctx,timeout)
 defer cancel()
 client:=&kafka.Client{Addr:kafka.TCP(s.Brokers...),Timeout:timeout}
 response,err:=client.OffsetFetch(subctx,&kafka.OffsetFetchRequest{
  GroupID:s.Group,Topics:map[string][]int{s.Topic:{s.Partition}},
 })
 if err!=nil{return lagrisk.Snapshot{},fmt.Errorf("fetch coordinator committed offset: %w",err)}
 if response.Error!=nil{return lagrisk.Snapshot{},fmt.Errorf("offset fetch broker error: %w",response.Error)}
 offsets:=response.Topics[s.Topic]
 var committed int64=-1
 found:=false
 for _,part:=range offsets{
  if part.Partition!=s.Partition{continue}
  if part.Error!=nil{return lagrisk.Snapshot{},fmt.Errorf("partition offset fetch: %w",part.Error)}
  committed=part.CommittedOffset
  found=true
  break
 }
 if !found||committed<0{return lagrisk.Snapshot{},fmt.Errorf("no confirmed Kafka group commit for %s/%d group %s",s.Topic,s.Partition,s.Group)}
 config,err:=client.DescribeConfigs(subctx,&kafka.DescribeConfigsRequest{
  Resources:[]kafka.DescribeConfigRequestResource{{
   ResourceType:kafka.ResourceType(2), // Kafka protocol resource type TOPIC
   ResourceName:s.Topic,ConfigNames:[]string{"retention.ms","message.timestamp.type"},
  }},
 })
 if err!=nil{return lagrisk.Snapshot{},fmt.Errorf("describe effective topic retention: %w",err)}
 if len(config.Resources)!=1||config.Resources[0].Error!=nil{
  return lagrisk.Snapshot{},fmt.Errorf("Kafka returned invalid topic configuration or resource error")
 }
 settings:=make(map[string]string)
 for _,entry:=range config.Resources[0].ConfigEntries{
  settings[entry.ConfigName]=entry.ConfigValue
 }
 retention,err:=ParseRetentionMillis(settings["retention.ms"])
 if err!=nil{return lagrisk.Snapshot{},err}
 if settings["message.timestamp.type"]!="LogAppendTime" {
  return lagrisk.Snapshot{},fmt.Errorf("Kafka message.timestamp.type=%q: broker append-time required for retention-age estimate",settings["message.timestamp.type"])
 }
 conn,err:=kafka.DialLeader(subctx,"tcp",s.Brokers[0],s.Topic,s.Partition)
 if err!=nil{return lagrisk.Snapshot{},fmt.Errorf("dial partition leader: %w",err)}
 first,err:=conn.ReadFirstOffset()
 if err!=nil{_ =conn.Close();return lagrisk.Snapshot{},fmt.Errorf("read log start: %w",err)}
 end,err:=conn.ReadLastOffset()
 _=conn.Close()
 if err!=nil{return lagrisk.Snapshot{},fmt.Errorf("read log end: %w",err)}
 observed:=time.Now().UTC()
 snap:=lagrisk.Snapshot{
  Topic:s.Topic,Partition:s.Partition,LogStartOffset:first,
  LogEndOffset:end,CommittedOffset:committed,
  Retention:retention,SafetyMargin:s.Margin,Now:observed,
 }
 if committed>=first&&committed<end{
  reader:=kafka.NewReader(kafka.ReaderConfig{
   Brokers:s.Brokers,Topic:s.Topic,Partition:s.Partition,
   MinBytes:1,MaxBytes:10e6,MaxWait:250*time.Millisecond,
  })
  defer reader.Close()
  if err:=reader.SetOffset(committed);err!=nil{return lagrisk.Snapshot{},err}
  record,err:=reader.FetchMessage(subctx)
  if err!=nil{return lagrisk.Snapshot{},fmt.Errorf("first pending Kafka record: %w",err)}
  if record.Offset<committed||record.Offset>=end||record.Time.IsZero() {
   return lagrisk.Snapshot{},fmt.Errorf("first pending record offset/timestamp invalid")
  }
  snap.OldestUnconsumedTimestamp=record.Time.UTC()
 }
 return snap,nil
}
