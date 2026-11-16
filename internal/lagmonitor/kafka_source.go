package lagmonitor

import (
 "context"
 "fmt"
 "net"
 "strconv"
 "strings"
 "sort"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/lagrisk"
 "github.com/segmentio/kafka-go"
)

// EffectiveRetention reads the broker-returned *effective* topic setting,
// not an operator-supplied estimate. Infinite retention and CreateTime
// policies are refused: the latter cannot prove the broker append time.
func EffectiveRetention(entries []kafka.DescribeConfigResponseConfigEntry)(time.Duration,error){
 found:=map[string]string{}
 for _,e:=range entries{found[e.ConfigName]=e.ConfigValue}
 if found["message.timestamp.type"]!="LogAppendTime"{
  return 0,fmt.Errorf("topic must use LogAppendTime for broker-derived oldest message age (found %q)",found["message.timestamp.type"])
 }
 ms,err:=strconv.ParseInt(found["retention.ms"],10,64)
 if err!=nil||ms<=0||ms>int64((time.Duration(1<<63-1))/time.Millisecond){
  return 0,fmt.Errorf("topic retention.ms must be finite positive milliseconds (found %q)",found["retention.ms"])
 }
 return time.Duration(ms)*time.Millisecond,nil
}
func CommittedOffsets(res *kafka.OffsetFetchResponse,topic string,partitions []int)(map[int]int64,error){
 if res==nil{return nil,fmt.Errorf("missing group offset response")}
 if res.Error!=nil{return nil,fmt.Errorf("Kafka group OffsetFetch: %w",res.Error)}
 offsets:=make(map[int]int64,len(partitions))
 for _,p:=range res.Topics[topic]{
  if p.Error!=nil{return nil,fmt.Errorf("Kafka offset topic=%s partition=%d: %w",topic,p.Partition,p.Error)}
  if _,duplicate:=offsets[p.Partition];duplicate{return nil,fmt.Errorf("duplicate Kafka offset partition %d",p.Partition)}
  if p.CommittedOffset<0{return nil,fmt.Errorf("no confirmed group commit for partition %d",p.Partition)}
  offsets[p.Partition]=p.CommittedOffset
 }
 for _,partition:=range partitions{
  if _,ok:=offsets[partition];!ok{return nil,fmt.Errorf("missing group commit for %s/%d",topic,partition)}
 }
 if len(offsets)!=len(partitions){return nil,fmt.Errorf("unexpected partition offset response")}
 return offsets,nil
}

type KafkaSource struct{
 Brokers []string
 GroupID string
 Topic string
 SafetyMargin time.Duration
 MaxPartitions int
}

// Read never joins the consumer group, commits offsets or modifies Topic
// configs. This remains a diagnostic sampling snapshot rather than a
// distributed transaction across broker data and Kafka coordinator state.
func(s KafkaSource)Read(ctx context.Context,now time.Time)([]lagrisk.Snapshot,error){
 if len(s.Brokers)==0||s.GroupID==""||s.Topic==""||s.SafetyMargin<=0||
  s.MaxPartitions<1||s.MaxPartitions>32{return nil,fmt.Errorf("invalid Kafka monitoring config")}
 client:=&kafka.Client{Addr:kafka.TCP(s.Brokers...),Timeout:8*time.Second}
 meta,err:=client.Metadata(ctx,&kafka.MetadataRequest{Topics:[]string{s.Topic}})
 if err!=nil{return nil,fmt.Errorf("fetch topic metadata: %w",err)}
 if len(meta.Topics)!=1||meta.Topics[0].Name!=s.Topic||meta.Topics[0].Error!=nil{
  return nil,fmt.Errorf("topic %s metadata unavailable or inconsistent",s.Topic)
 }
 parts:=make([]int,0,len(meta.Topics[0].Partitions))
 for _,p:=range meta.Topics[0].Partitions{
  if p.Error!=nil{return nil,fmt.Errorf("topic %s partition %d error: %w",s.Topic,p.ID,p.Error)}
  parts=append(parts,p.ID)
 }
 sort.Ints(parts)
 if len(parts)==0||len(parts)>s.MaxPartitions{return nil,fmt.Errorf("topic partition count %d exceeds configured bound",len(parts))}
 cfg,err:=client.DescribeConfigs(ctx,&kafka.DescribeConfigsRequest{
  Resources:[]kafka.DescribeConfigRequestResource{{
   ResourceType:kafka.ResourceTypeTopic,ResourceName:s.Topic,
   ConfigNames:[]string{"retention.ms","message.timestamp.type"},
  }},
 })
 if err!=nil{return nil,fmt.Errorf("effective topic config query: %w",err)}
 if len(cfg.Resources)!=1||cfg.Resources[0].Error!=nil{
  return nil,fmt.Errorf("effective topic config unavailable for %s",s.Topic)
 }
 retention,err:=EffectiveRetention(cfg.Resources[0].ConfigEntries)
 if err!=nil{return nil,err}
 if s.SafetyMargin>=retention{return nil,fmt.Errorf("safety margin must be smaller than effective Kafka retention")}
 coord,err:=client.FindCoordinator(ctx,&kafka.FindCoordinatorRequest{Key:s.GroupID,KeyType:kafka.CoordinatorKeyTypeConsumer})
 if err!=nil{return nil,fmt.Errorf("find group coordinator: %w",err)}
 if coord.Error!=nil||coord.Coordinator==nil{return nil,fmt.Errorf("Kafka group coordinator unavailable: %v",coord.Error)}
 addr:=kafka.TCP(net.JoinHostPort(coord.Coordinator.Host,strconv.Itoa(coord.Coordinator.Port)))
 fetched,err:=client.OffsetFetch(ctx,&kafka.OffsetFetchRequest{
  Addr:addr,GroupID:s.GroupID,Topics:map[string][]int{s.Topic:parts},
 })
 if err!=nil{return nil,fmt.Errorf("read Kafka group committed positions: %w",err)}
 committed,err:=CommittedOffsets(fetched,s.Topic,parts)
 if err!=nil{return nil,err}
 out:=make([]lagrisk.Snapshot,0,len(parts))
 for _,part:=range parts{
  dialAddr:=strings.TrimSpace(s.Brokers[0])
  conn,err:=kafka.DialLeader(ctx,"tcp",dialAddr,s.Topic,part)
  if err!=nil{return nil,fmt.Errorf("Kafka log position for partition %d: %w",part,err)}
  first,err:=conn.ReadFirstOffset()
  if err!=nil{_ = conn.Close();return nil,fmt.Errorf("log start %d: %w",part,err)}
  end,err:=conn.ReadLastOffset()
  _=conn.Close()
  if err!=nil{return nil,fmt.Errorf("log end %d: %w",part,err)}
  snap:=lagrisk.Snapshot{
   Topic:s.Topic,Partition:part,LogStartOffset:first,LogEndOffset:end,
   CommittedOffset:committed[part],Retention:retention,
   SafetyMargin:s.SafetyMargin,Now:now.UTC(),
  }
  // Once log start overtakes committed group position, data_gap is already
  // observable. Do not silently read at log start and mark the gap healthy.
  if committed[part]>=first&&committed[part]<end{
   reader:=kafka.NewReader(kafka.ReaderConfig{Brokers:s.Brokers,Topic:s.Topic,
    Partition:part,MinBytes:1,MaxBytes:10e6,MaxWait:250*time.Millisecond})
   if err:=reader.SetOffset(committed[part]);err!=nil{_ = reader.Close();return nil,err}
   fetchCtx,cancel:=context.WithTimeout(ctx,5*time.Second)
   msg,fetchErr:=reader.FetchMessage(fetchCtx)
   cancel()
   _=reader.Close()
   if fetchErr!=nil{return nil,fmt.Errorf("fetch oldest pending broker timestamp on %d: %w",part,fetchErr)}
   if msg.Offset<committed[part]||msg.Time.IsZero(){
    return nil,fmt.Errorf("invalid Kafka oldest pending offset/timestamp on %d",part)
   }
   snap.OldestUnconsumedTimestamp=msg.Time.UTC()
  }
  out=append(out,snap)
 }
 return out,nil
}
