package kafkaha

import (
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "os"
 "os/exec"
 "strings"
 "testing"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
 "github.com/segmentio/kafka-go"
)

const haTopic="m3b.ha.critical.v1"
var haBrokers=[]string{"localhost:39092","localhost:39093","localhost:39094"}

func haCompose(t *testing.T,ctx context.Context,args ...string)string{
 t.Helper()
 a:=append([]string{"compose","-f","deployments/docker-compose-ha.yml"},args...)
 cmd:=exec.CommandContext(ctx,"docker",a...)
 // go test runs with the package directory as its working directory.
 cmd.Dir="../.."
 result,err:=cmd.CombinedOutput()
 if err!=nil{t.Fatalf("docker %v: %v\n%s",a,err,result)}
 return string(result)
}
func haTopicConfig(t *testing.T,ctx context.Context,brokerID,value int){
 t.Helper()
 b:=fmt.Sprintf("broker%d",brokerID)
 v:=fmt.Sprintf("min.insync.replicas=%d",value)
 haCompose(t,ctx,"exec","-T",b,"/opt/kafka/bin/kafka-configs.sh",
    "--bootstrap-server",b+":19092","--entity-type","topics",
    "--entity-name",haTopic,"--alter","--add-config",v)
 out:=haCompose(t,ctx,"exec","-T",b,"/opt/kafka/bin/kafka-configs.sh",
    "--bootstrap-server",b+":19092","--entity-type","topics",
    "--entity-name",haTopic,"--describe")
 if !strings.Contains(out,v){t.Fatalf("topic config did not confirm %s: %s",v,out)}
 // Allow the leader to receive the newly committed metadata.
 select{
 case <-time.After(1500*time.Millisecond):
 case <-ctx.Done():t.Fatal(ctx.Err())
 }
}
func haMetadata(ctx context.Context)(kafka.Partition,error){
 var last error
 for _,addr:=range haBrokers{
    dialCtx,cancel:=context.WithTimeout(ctx,3*time.Second)
    c,err:=kafka.DialContext(dialCtx,"tcp",addr)
    if err!=nil{cancel();last=err;continue}
    parts,err:=c.ReadPartitions(haTopic)
    _=c.Close()
    cancel()
    if err!=nil{last=err;continue}
    for _,p:=range parts{
        if p.Topic==haTopic&&p.ID==0{return p,nil}
    }
    last=fmt.Errorf("topic partition metadata not found")
 }
 return kafka.Partition{},last
}
func haWaitPartition(t *testing.T,ctx context.Context,expectedISR int,oldLeader int)(kafka.Partition){
 t.Helper()
 var last string
 for {
    p,err:=haMetadata(ctx)
    if err==nil{
      last=fmt.Sprintf("leader=%d ISR=%d RF=%d",p.Leader.ID,len(p.Isr),len(p.Replicas))
      if len(p.Isr)==expectedISR && len(p.Replicas)==3 &&
        (oldLeader<0||p.Leader.ID!=oldLeader) {
        if err:=RequireReplicaAndISR(p,3,expectedISR);err!=nil{t.Fatal(err)}
        return p
      }
    }else{last=err.Error()}
    select{
    case <-ctx.Done():t.Fatalf("waiting RF3/ISR%d and leader !=%d: %v (last %s)",expectedISR,oldLeader,ctx.Err(),last)
    case <-time.After(time.Second):
    }
 }
}
func haNewWriter(attempts int)*kafka.Writer{
 return &kafka.Writer{
    Addr:kafka.TCP(haBrokers...),Topic:haTopic,
    Balancer:&kafka.Hash{},RequiredAcks:kafka.RequireAll,
    Async:false,AllowAutoTopicCreation:false,
    BatchSize:1,BatchTimeout:10*time.Millisecond,
    MaxAttempts:attempts,WriteTimeout:5*time.Second,ReadTimeout:5*time.Second,
 }
}
func haPublish(t *testing.T,ctx context.Context,seed string,start,count int,ack map[string][]byte){
 t.Helper()
 writer:=haNewWriter(10)
 defer writer.Close()
 for n:=start;n<start+count;n++{
    e,err:=event.SyntheticUsageEvent(seed,n,
      time.Date(2026,11,16,11,0,0,0,time.UTC))
    if err!=nil{t.Fatal(err)}
    raw,err:=json.Marshal(e);if err!=nil{t.Fatal(err)}
    writeCtx,cancel:=context.WithTimeout(ctx,45*time.Second)
    err=writer.WriteMessages(writeCtx,kafka.Message{Key:[]byte(e.EventID),Value:raw})
    cancel()
    if err!=nil{t.Fatalf("producer failed acks=all at record %d: %v",n,err)}
    ack[e.EventID]=raw
 }
}
func haReadAcknowledged(t *testing.T,ctx context.Context,ack map[string][]byte)int{
 t.Helper()
 reader:=kafka.NewReader(kafka.ReaderConfig{
    Brokers:haBrokers,Topic:haTopic,Partition:0,MinBytes:1,
    MaxBytes:10e6,MaxWait:250*time.Millisecond,
 })
 defer reader.Close()
 if err:=reader.SetOffset(0);err!=nil{t.Fatal(err)}
 received:=make([]Record,0,len(ack)+10)
 seen:=map[string]bool{}
 for len(seen)<len(ack){
    readCtx,cancel:=context.WithTimeout(ctx,25*time.Second)
    m,err:=reader.FetchMessage(readCtx)
    cancel()
    if err!=nil{t.Fatalf("Kafka replay failed after %d/%d ACKed IDs: %v",len(seen),len(ack),err)}
    var e event.UsageEvent
    if err:=json.Unmarshal(m.Value,&e);err!=nil{t.Fatalf("malformed Kafka record: %v",err)}
    if err:=e.Validate();err!=nil{t.Fatalf("unsealed Kafka record: %v",err)}
    received=append(received,Record{ID:e.EventID,Value:m.Value})
    if _,ok:=ack[e.EventID];ok{seen[e.EventID]=true}
    if len(received)>len(ack)+20{t.Fatalf("excess unconfirmed records before completing replay")}
 }
 if err:=VerifyAcknowledged(ack,received);err!=nil{t.Fatal(err)}
 return len(received)
}
func haWriteFailure(t *testing.T,ctx context.Context,seed string)string{
 t.Helper()
 e,err:=event.SyntheticUsageEvent(seed,999,time.Date(2026,11,16,11,0,0,0,time.UTC))
 if err!=nil{t.Fatal(err)}
 b,err:=json.Marshal(e);if err!=nil{t.Fatal(err)}
 writer:=haNewWriter(1)
 defer writer.Close()
 writeCtx,cancel:=context.WithTimeout(ctx,12*time.Second)
 err=writer.WriteMessages(writeCtx,kafka.Message{Key:[]byte(e.EventID),Value:b})
 cancel()
 if err==nil{t.Fatalf("Kafka ACKed a write while ISR below min.insync.replicas")}
 // A generic deadline/network error alone is not proof of the ISR safety
 // mechanism. Kafka must explicitly identify an insufficient replica count.
 if !errors.Is(err,kafka.NotEnoughReplicas) &&
    !errors.Is(err,kafka.NotEnoughReplicasAfterAppend) &&
    !strings.Contains(strings.ToLower(err.Error()),"not enough replicas") {
    t.Fatalf("expected Kafka NotEnoughReplicas response, not merely a transport failure: %v",err)
 }
 return err.Error()
}

func TestThreeBrokerLeaderFailoverAndISRFailClosed(t *testing.T){
 if os.Getenv("TEST_KAFKA_HA")!="1"{t.Skip("requires isolated three-broker Docker profile")}
 ctx,cancel:=context.WithTimeout(context.Background(),7*time.Minute)
 defer cancel()
 seed:=fmt.Sprintf("m3b-ha-%d",time.Now().UnixNano())
 ack:=make(map[string][]byte)
 initial:=haWaitPartition(t,ctx,3,-1)
 t.Logf("initial leader=%d ISR=%d",initial.Leader.ID,len(initial.Isr))
 haPublish(t,ctx,seed,0,15,ack)

 stopped:=initial.Leader.ID
 if stopped<1||stopped>3{t.Fatalf("unexpected broker leader ID=%d",stopped)}
 haCompose(t,ctx,"stop",fmt.Sprintf("broker%d",stopped))
 degraded:=haWaitPartition(t,ctx,2,stopped)
 t.Logf("stopped leader %d, new leader=%d, remaining ISR=%d",stopped,degraded.Leader.ID,len(degraded.Isr))
 haPublish(t,ctx,seed,100,12,ack)
 if err:=RequireReplicaAndISR(degraded,3,2);err!=nil{t.Fatal(err)}

 // Reconfigure only this TEST TOPIC with two live brokers to make ISR=2
 // smaller than minISR=3 while preserving the controller majority.
 haTopicConfig(t,ctx,degraded.Leader.ID,3)
 rejected:=haWriteFailure(t,ctx,seed)
 t.Logf("correctly rejected insufficient ISR: %s",rejected)
 haTopicConfig(t,ctx,degraded.Leader.ID,2)
 haPublish(t,ctx,seed,200,4,ack)

 haCompose(t,ctx,"start",fmt.Sprintf("broker%d",stopped))
 restored:=haWaitPartition(t,ctx,3,-1)
 t.Logf("replica recovery ISR=%d leader=%d",len(restored.Isr),restored.Leader.ID)
 haPublish(t,ctx,seed,300,4,ack)
 observed:=haReadAcknowledged(t,ctx,ack)
 if observed<len(ack){t.Fatalf("observed %d records for %d acknowledged",observed,len(ack))}

 report:=map[string]any{
    "schema_version":1,
    "scenario":"three_broker_combined_kraft_leader_failover_minISR",
    "topic":haTopic,"replication_factor":3,"min_insync_replicas":2,
    "initial_leader":initial.Leader.ID,"failover_leader":degraded.Leader.ID,
    "stopped_broker":stopped,"isr_after_failure":len(degraded.Isr),
    "isr_after_recovery":len(restored.Isr),
    "insufficient_isr_probe_min_isr":3,
    "insufficient_isr_probe_isr":len(degraded.Isr),
    "rejected_error":rejected,
    "broker_acknowledged_ids":len(ack),
    "readback_records":observed,
    "all_acknowledged_verified":true,
    "notes":"Confirmed ACKed records only; failed/unknown ACK may still create a physical record. No cross-AZ, disk-loss, controller quorum-loss, or exactly-once claim.",
 }
 if path:=os.Getenv("M3B_HA_REPORT_PATH");path!=""{
    b,err:=json.MarshalIndent(report,"","  ");if err!=nil{t.Fatal(err)}
    if err:=os.WriteFile(path,b,0600);err!=nil{t.Fatal(err)}
 }
 t.Logf("PASS HA: leader %d→%d, ISR 3→2→3, insufficient ISR refused, %d ACKed IDs replayed",initial.Leader.ID,degraded.Leader.ID,len(ack))
}
