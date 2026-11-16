// Package kafkaha contains bounded evidence helpers for Kafka broker
// failover tests. It does not modify broker state or authorize billing.
package kafkaha

import (
 "bytes"
 "fmt"

 "github.com/segmentio/kafka-go"
)

// RequireReplicaAndISR verifies actual Kafka partition metadata. A broker
// being reachable is not evidence that its partition replica is in-sync.
func RequireReplicaAndISR(p kafka.Partition, replication, minISR int) error {
 if replication<1||minISR<1||minISR>replication {
  return fmt.Errorf("invalid desired RF/ISR: %d/%d",replication,minISR)
 }
 if len(p.Replicas)!=replication {
  return fmt.Errorf("partition %s/%d RF=%d want %d",p.Topic,p.ID,len(p.Replicas),replication)
 }
 if len(p.Isr)<minISR {
  return fmt.Errorf("partition %s/%d ISR=%d want at least %d",p.Topic,p.ID,len(p.Isr),minISR)
 }
 isReplica:=false
 for _,b:=range p.Replicas {
  if b.ID==p.Leader.ID {isReplica=true;break}
 }
 if !isReplica {
  return fmt.Errorf("partition %s/%d leader=%d is not a replica",p.Topic,p.ID,p.Leader.ID)
 }
 return nil
}

type Record struct {
 ID string
 Value []byte
}

// VerifyAcknowledged checks every broker-ACKed event is present exactly once
// with identical bytes. A rejected/ambiguous write MAY appear in the raw log
// and must never be reported as a broker-ACKed event.
func VerifyAcknowledged(acknowledged map[string][]byte,received []Record)error{
 seen:=make(map[string]int,len(acknowledged))
 for _,r:=range received {
  expected,ok:=acknowledged[r.ID]
  if !ok{continue}
  seen[r.ID]++
  if !bytes.Equal(expected,r.Value){
   return fmt.Errorf("payload changed for acknowledged ID %s",r.ID)
  }
  if seen[r.ID]>1{return fmt.Errorf("duplicate acknowledged ID %s in Kafka log",r.ID)}
 }
 for id:=range acknowledged {
  if seen[id]!=1{return fmt.Errorf("acknowledged ID %s not visible (count=%d)",id,seen[id])}
 }
 return nil
}
