package lagmonitor

import (
 "testing"
 "time"

 "github.com/segmentio/kafka-go"
)

func TestTopicEffectiveConfigOnlyAllowsBrokerAppendTime(t *testing.T){
 valid:=[]kafka.DescribeConfigResponseConfigEntry{
  {ConfigName:"retention.ms",ConfigValue:"60000"},
  {ConfigName:"message.timestamp.type",ConfigValue:"LogAppendTime"},
 }
 d,err:=EffectiveRetention(valid)
 if err!=nil||d!=time.Minute{t.Fatalf("expected valid broker config, duration=%v err=%v",d,err)}
 tests:=[][]kafka.DescribeConfigResponseConfigEntry{
  {{ConfigName:"retention.ms",ConfigValue:"-1"},{ConfigName:"message.timestamp.type",ConfigValue:"LogAppendTime"}},
  {{ConfigName:"retention.ms",ConfigValue:"60000"},{ConfigName:"message.timestamp.type",ConfigValue:"CreateTime"}},
  {{ConfigName:"retention.ms",ConfigValue:"bad"},{ConfigName:"message.timestamp.type",ConfigValue:"LogAppendTime"}},
  {{ConfigName:"message.timestamp.type",ConfigValue:"LogAppendTime"}},
 }
 for i,entries:=range tests{
  if _,err:=EffectiveRetention(entries);err==nil{t.Errorf("bad effective config %d accepted",i)}
 }
}
func TestCommittedOffsetRequiresExactlyOneValidPartition(t *testing.T){
 resp:=&kafka.OffsetFetchResponse{
  Topics:map[string][]kafka.OffsetFetchPartition{
   "m3e.monitor.v1":{{Partition:0,CommittedOffset:8}},
  },
 }
 got,err:=CommittedOffsets(resp,"m3e.monitor.v1",[]int{0})
 if err!=nil||got[0]!=8{t.Fatalf("offset parsing failed: got=%v err=%v",got,err)}
 resp.Topics["m3e.monitor.v1"][0].CommittedOffset=-1
 if _,err:=CommittedOffsets(resp,"m3e.monitor.v1",[]int{0});err==nil{t.Fatal("no-group-offset incorrectly marked healthy")}
}
