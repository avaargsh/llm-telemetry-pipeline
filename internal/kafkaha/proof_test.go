package kafkaha

import (
 "testing"

 "github.com/segmentio/kafka-go"
)

func TestRequireReplicaAndISRRejectsWeakOrWrongMetadata(t *testing.T) {
 p:=kafka.Partition{Topic:"ha.critical.v1",ID:0,Leader:kafka.Broker{ID:2},
    Replicas:[]kafka.Broker{{ID:1},{ID:2},{ID:3}},
    Isr:[]kafka.Broker{{ID:1},{ID:2},{ID:3}}}
 if err:=RequireReplicaAndISR(p,3,3);err!=nil{t.Fatal(err)}
 p.Isr=p.Isr[:2]
 if err:=RequireReplicaAndISR(p,3,3);err==nil{t.Fatal("accepted 2 ISR when 3 required")}
 if err:=RequireReplicaAndISR(p,3,2);err!=nil{t.Fatal(err)}
 p.Replicas=p.Replicas[:2]
 if err:=RequireReplicaAndISR(p,3,2);err==nil{t.Fatal("accepted RF=2 topic as RF=3")}
 p.Replicas=[]kafka.Broker{{ID:1},{ID:2},{ID:3}}
 p.Leader.ID=5
 if err:=RequireReplicaAndISR(p,3,2);err==nil{t.Fatal("accepted leader outside replica set")}
}

func TestVerifyAcknowledgedRecords(t *testing.T){
 acknowledged:=map[string][]byte{"event-01":[]byte("signed-one"),"event-02":[]byte("signed-two")}
 received:=[]Record{{ID:"event-01",Value:[]byte("signed-one")},{ID:"event-02",Value:[]byte("signed-two")}}
 if err:=VerifyAcknowledged(acknowledged,received);err!=nil{t.Fatal(err)}
 if err:=VerifyAcknowledged(acknowledged,received[:1]);err==nil{t.Fatal("silent data loss")}
 corrupted:=[]Record{{ID:"event-01",Value:[]byte("different")},received[1]}
 if err:=VerifyAcknowledged(acknowledged,corrupted);err==nil{t.Fatal("corrupted ACKed record")}
 duplicated:=append(append([]Record{},received...),received[0])
 if err:=VerifyAcknowledged(acknowledged,duplicated);err==nil{t.Fatal("physical duplicate hidden")}
 // A failed/unknown-ACK write can still appear; do not promote it
 // into the acknowledged set or call it confirmed data loss.
 extraneous:=append(append([]Record{},received...),Record{ID:"unknown-ack",Value:[]byte("payload")})
 if err:=VerifyAcknowledged(acknowledged,extraneous);err!=nil{t.Fatalf("extra unconfirmed event wrongly considered lost: %v",err)}
}


func TestLiveISREndpointsExcludesStoppedLeader(t *testing.T) {
  brokers:=[]string{"127.0.0.1:39092","127.0.0.1:39093","127.0.0.1:39094"}
  p:=kafka.Partition{Topic:"m3b.ha.critical.v1",ID:0,
     Leader:kafka.Broker{ID:2},
     Replicas:[]kafka.Broker{{ID:1},{ID:2},{ID:3}},
     Isr:[]kafka.Broker{{ID:1},{ID:2}}}
  selected,err:=LiveISREndpoints(p,brokers,2)
  if err!=nil{t.Fatal(err)}
  if len(selected)!=2||selected[0]!=brokers[1]||selected[1]!=brokers[0]{
     t.Fatalf("not ordered by current leader and live ISR: %v",selected)
  }
  for _,s:=range selected{if s==brokers[2]{t.Fatal("stopped former leader included in bootstrap seeds")}}
}

func TestLiveISREndpointsFailClosedOnInvalidMetadata(t *testing.T) {
  brokers:=[]string{"127.0.0.1:39092","127.0.0.1:39093","127.0.0.1:39094"}
  valid:=func() kafka.Partition {
    return kafka.Partition{Topic:"m3b.ha.critical.v1",ID:0,
      Leader:kafka.Broker{ID:2},
      Replicas:[]kafka.Broker{{ID:1},{ID:2},{ID:3}},
      Isr:[]kafka.Broker{{ID:1},{ID:2}}}
  }
  cases:=map[string]func(*kafka.Partition){
    "leader not in ISR":func(p *kafka.Partition){p.Isr=[]kafka.Broker{{ID:1},{ID:3}}},
    "duplicate ISR":func(p *kafka.Partition){p.Isr=[]kafka.Broker{{ID:2},{ID:2}}},
    "invalid broker id":func(p *kafka.Partition){p.Isr=[]kafka.Broker{{ID:2},{ID:9}}},
    "too few live ISR":func(p *kafka.Partition){p.Isr=[]kafka.Broker{{ID:2}}},
    "invalid leader":func(p *kafka.Partition){p.Leader.ID=0},
    "replica membership mismatch":func(p *kafka.Partition){p.Replicas=[]kafka.Broker{{ID:1},{ID:2},{ID:4}}},
  }
  for name,modify:=range cases{
     t.Run(name,func(t *testing.T){
       p:=valid();modify(&p)
       if endpoints,err:=LiveISREndpoints(p,brokers,2);err==nil{
         t.Fatalf("accepted invalid partition state: %v",endpoints)
       }
     })
  }
  if _,err:=LiveISREndpoints(valid(),[]string{"","127.0.0.1:39093","127.0.0.1:39094"},2);err==nil{
     t.Fatal("empty live broker endpoint accepted")
  }
}
