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
