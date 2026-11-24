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
var haBrokers=[]string{"127.0.0.1:39092","127.0.0.1:39093","127.0.0.1:39094"}

// The test-only dynamic-IP control runs identical fault logic against a
// distinct Docker Compose project. Production Kafka code never reads this.
func haComposeFile() string {
 if path:=os.Getenv("M3B_HA_COMPOSE_FILE");path!=""{return path}
 return "deployments/docker-compose-ha.yml"
}

func haCompose(t *testing.T,ctx context.Context,args ...string)string{
 t.Helper()
 a:=append([]string{"compose","-f",haComposeFile()},args...)
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
 return haWaitPartitionWithTimeoutObservation(t,ctx,expectedISR,oldLeader,nil)
}

// The optional callback records actual last-observed metadata only on a deadline.
// A generic setup, quorum, readback, or metadata-only failure is not an ISR proof.
func haWaitPartitionWithTimeoutObservation(t *testing.T,ctx context.Context,expectedISR int,oldLeader int,onTimeout func(kafka.Partition))(kafka.Partition){
 t.Helper()
 var last string
 var lastObserved kafka.Partition
 var observed bool
 for {
    p,err:=haMetadata(ctx)
    if err==nil{
      lastObserved=p
      observed=true
      last=fmt.Sprintf("leader=%d ISR=%d RF=%d",p.Leader.ID,len(p.Isr),len(p.Replicas))
      if len(p.Isr)==expectedISR && len(p.Replicas)==3 &&
        (oldLeader<0||p.Leader.ID!=oldLeader) {
        if err:=RequireReplicaAndISR(p,3,expectedISR);err!=nil{t.Fatal(err)}
        return p
      }
    }else{last=err.Error()}
    select{
    case <-ctx.Done():
      if onTimeout!=nil && observed && ctx.Err()==context.DeadlineExceeded {
        onTimeout(lastObserved)
      }
      t.Fatalf("waiting RF3/ISR%d and leader !=%d: %v (last %s)",expectedISR,oldLeader,ctx.Err(),last)
    case <-time.After(time.Second):
    }
 }
}
func haNewWriter(attempts int,bootstrap ...string)*kafka.Writer{
 if len(bootstrap)==0{bootstrap=haBrokers}
 return &kafka.Writer{
    Addr:kafka.TCP(bootstrap...),Topic:haTopic,
    Balancer:&kafka.Hash{},RequiredAcks:kafka.RequireAll,
    Async:false,AllowAutoTopicCreation:false,
    BatchSize:1,BatchTimeout:10*time.Millisecond,
    MaxAttempts:attempts,WriteTimeout:5*time.Second,ReadTimeout:5*time.Second,
 }
}
func haPublish(t *testing.T,ctx context.Context,seed string,start,count int,ack map[string][]byte,bootstrap ...string){
 t.Helper()
 writer:=haNewWriter(10,bootstrap...)
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
func haWriteFailure(t *testing.T,ctx context.Context,seed string,bootstrap ...string)string{
 t.Helper()
 e,err:=event.SyntheticUsageEvent(seed,999,time.Date(2026,11,16,11,0,0,0,time.UTC))
 if err!=nil{t.Fatal(err)}
 b,err:=json.Marshal(e);if err!=nil{t.Fatal(err)}
 writer:=haNewWriter(1,bootstrap...)
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


// haProbeStaleBootstrapReconnect is a real client-side failover experiment:
// old leader's TCP bootstrap is physically unavailable after docker stop.
// The same signed event is retried against freshly verified live ISR seeds.
// A failed/unknown old attempt is never elevated into the acknowledged map.
func haProbeStaleBootstrapReconnect(t *testing.T,ctx context.Context,seed string,
    stopped int,liveSeeds []string,ack map[string][]byte)string{
 t.Helper()
 if stopped<1||stopped>len(haBrokers)||len(liveSeeds)<2{t.Fatal("invalid failover bootstrap proof")}
 for _,endpoint:=range liveSeeds{
    if endpoint==haBrokers[stopped-1]{t.Fatalf("stopped broker %d still in live seeds",stopped)}
 }
 e,err:=event.SyntheticUsageEvent(seed,100,
     time.Date(2026,11,16,11,0,0,0,time.UTC))
 if err!=nil{t.Fatal(err)}
 raw,err:=json.Marshal(e);if err!=nil{t.Fatal(err)}
 msg:=kafka.Message{Key:[]byte(e.EventID),Value:raw}
 stale:=haNewWriter(1,haBrokers[stopped-1])
 oldCtx,oldCancel:=context.WithTimeout(ctx,4*time.Second)
 oldErr:=stale.WriteMessages(oldCtx,msg)
 oldCancel()
 _=stale.Close()
 if oldErr==nil{
    t.Fatal("stopped former leader unexpectedly ACKed the stale-bootstrap probe")
 }
 t.Logf("stopped leader bootstrap was unavailable (UNKNOWN append, no billing ACK): %v",oldErr)
 recovered:=haNewWriter(10,liveSeeds...)
 defer recovered.Close()
 retryCtx,retryCancel:=context.WithTimeout(ctx,45*time.Second)
 // Reuse the exact original Kafka message key, payload and sealed Event ID.
 err=recovered.WriteMessages(retryCtx,msg)
 retryCancel()
 if err!=nil{t.Fatalf("live ISR rebootstrap failed to ACK unchanged Event ID: %v",err)}
 ack[e.EventID]=raw
 t.Logf("rebootstrap ACKed original Event ID on verified live ISR seeds (%d)",len(liveSeeds))
 return oldErr.Error()
}

// haQuorumStatusObserved performs a genuine Kafka metadata-quorum status
// operation against the surviving host-side Docker broker endpoint. This
// observation is NOT the instant quorum became healthy: CLI scheduling,
 // retries and metadata response add measurement delay.
func haQuorumStatusObserved(t *testing.T,ctx context.Context,keeper int) QuorumStatus{
 t.Helper()
 if keeper<1||keeper>3{t.Fatal("invalid live KRaft controller ID")}
 probeStart:=time.Now()
 var lastClass string
 for {
    probeCtx,cancel:=context.WithTimeout(ctx,8*time.Second)
    service:=fmt.Sprintf("broker%d",keeper)
    args:=[]string{"compose","-f",haComposeFile(),
      "exec","-T",service,"/opt/kafka/bin/kafka-metadata-quorum.sh",
      "--bootstrap-server",service+":19092","describe","--status"}
    cmd:=exec.CommandContext(probeCtx,"docker",args...)
    cmd.Dir="../.."
    output,err:=cmd.CombinedOutput()
    cancel()
    if err==nil{
      status,parseErr:=ParseQuorumStatus(string(output))
      if parseErr==nil{return status}
      lastClass="invalid_quorum_status"
    }else{lastClass="quorum_cli_unavailable"}
    if time.Since(probeStart)>45*time.Second{
       t.Fatalf("quorum status cannot be independently verified after first voter restart: %s",lastClass)
    }
    select{
    case <-ctx.Done():t.Fatalf("quorum status observation interrupted: %v",ctx.Err())
    case <-time.After(time.Second):
    }
 }
}

// haProbeQuorumLossAndRestore deliberately removes two of three combined
// controller/broker processes while retaining the current data leader. The
// producer must NOT receive an ACK. A timeout or transport error is treated
// as an unknown append outcome, not as proof the event never reached storage.
func haProbeQuorumLossAndRestore(t *testing.T,ctx context.Context,seed string,
    leader int,previousACKs map[string][]byte,trace *RecoveryTimeline)string{
 t.Helper()
 if leader<1||leader>3{t.Fatalf("invalid keeper broker %d",leader)}
 var stopped []string
 for id:=1;id<=3;id++{
    if id!=leader {stopped=append(stopped,fmt.Sprintf("broker%d",id))}
 }
 for _,service:=range stopped{
    haCompose(t,ctx,"stop",service)
 }
 trace.Mark("two_voters_stopped",leader,0,0,0)
 // Leave enough time for the surviving broker to recognize that a quorum
 // and all follower replicas are absent.
 select{
 case <-time.After(8*time.Second):
 case <-ctx.Done():t.Fatal(ctx.Err())
 }
 e,err:=event.SyntheticUsageEvent(seed,1000,time.Date(2026,11,16,11,0,0,0,time.UTC))
 if err!=nil{t.Fatal(err)}
 raw,err:=json.Marshal(e);if err!=nil{t.Fatal(err)}
 // Bootstrap from the lone surviving leader, not from the two known-stopped
 // controllers. A lack of ACK is still only an UNKNOWN append outcome.
 writer:=haNewWriter(1,haBrokers[leader-1])
 writeCtx,cancel:=context.WithTimeout(ctx,12*time.Second)
 err=writer.WriteMessages(writeCtx,kafka.Message{Key:[]byte(e.EventID),Value:raw})
 cancel()
 _=writer.Close()
 if err==nil{
    t.Fatalf("producer ACKed while only one of three combined Kafka nodes was available")
 }
 failure:=err.Error()
 trace.Mark("quorum_loss_no_ack",leader,0,0,0)
 t.Logf("no ACK during loss of 2/3 controller+broker nodes; unconfirmed append possible: %s",failure)
 // Restore one voter first. Once the original leader plus one restarted
 // voter re-establish the controller majority and ISR=2, start the final
 // voter. Starting both stopped KRaft controllers simultaneously caused
 // cross-target VOTE requests (INVALID_VOTER_KEY) in the isolated CI cluster;
 // staged recovery keeps the same 2/3 quorum-loss assertion while making
 // controller membership convergence observable at each step.
 haCompose(t,ctx,"start",stopped[0])
 trace.Mark("first_voter_restarted",leader,0,0,0)
 quorum:=haQuorumStatusObserved(t,ctx,leader)
 trace.Mark("quorum_status_observed",quorum.LeaderID,0,quorum.HighWatermark,0)
 intermediate:=haWaitPartition(t,ctx,2,-1)
 trace.Mark("intermediate_isr2_observed",intermediate.Leader.ID,len(intermediate.Isr),0,0)
 if err:=RequireReplicaAndISR(intermediate,3,2);err!=nil{t.Fatal(err)}
 haCompose(t,ctx,"start",stopped[1])
 trace.Mark("second_voter_restarted",intermediate.Leader.ID,0,0,0)
 recovered:=haWaitPartition(t,ctx,3,-1)
 trace.Mark("final_isr3_observed",recovered.Leader.ID,len(recovered.Isr),0,0)
 if err:=RequireReplicaAndISR(recovered,3,3);err!=nil{t.Fatal(err)}
 got:=haReadAcknowledged(t,ctx,previousACKs)
 if got<len(previousACKs){
    t.Fatalf("after quorum recovery visible=%d ACKed=%d",got,len(previousACKs))
 }
 trace.Mark("all_acked_replayed",recovered.Leader.ID,len(recovered.Isr),0,len(previousACKs))
 if _,err:=ValidateRecoveryMilestones(trace.steps);err!=nil{t.Fatalf("independent quorum/ISR timeline failed closed: %v",err)}
 return failure
}

func TestThreeBrokerLeaderFailoverAndISRFailClosed(t *testing.T){
 if os.Getenv("TEST_KAFKA_HA")!="1"{t.Skip("requires isolated three-broker Docker profile")}
 ctx,cancel:=context.WithTimeout(context.Background(),7*time.Minute)
 defer cancel()
 seed:=fmt.Sprintf("m3b-ha-%d",time.Now().UnixNano())
 trace:=NewRecoveryTimeline()
 t.Cleanup(func(){
   if err:=trace.WriteReceiptAlways(os.Getenv("M3B_RECOVERY_TIMELINE_PATH"),os.Getenv("M3B_EXACT_HEAD"));err!=nil{
     t.Errorf("cannot preserve fail-closed KRaft recovery timeline: %v",err)
   }
 })
 ack:=make(map[string][]byte)
 initial:=haWaitPartition(t,ctx,3,-1)
 t.Logf("initial leader=%d ISR=%d",initial.Leader.ID,len(initial.Isr))
 haPublish(t,ctx,seed,0,15,ack)

 stopped:=initial.Leader.ID
 if stopped<1||stopped>3{t.Fatalf("unexpected broker leader ID=%d",stopped)}
 haCompose(t,ctx,"stop",fmt.Sprintf("broker%d",stopped))
 degraded:=haWaitPartition(t,ctx,2,stopped)
 t.Logf("stopped leader %d, new leader=%d, remaining ISR=%d",stopped,degraded.Leader.ID,len(degraded.Isr))
 degradedSeeds,err:=LiveISREndpoints(degraded,haBrokers,2)
 if err!=nil{t.Fatalf("bad live ISR metadata after leader loss: %v",err)}
 staleBootstrapError:=haProbeStaleBootstrapReconnect(t,ctx,seed,stopped,degradedSeeds,ack)
 haPublish(t,ctx,seed,101,11,ack,degradedSeeds...)
 if err:=RequireReplicaAndISR(degraded,3,2);err!=nil{t.Fatal(err)}

 // Reconfigure only this TEST TOPIC with two live brokers to make ISR=2
 // smaller than minISR=3 while preserving the controller majority.
 haTopicConfig(t,ctx,degraded.Leader.ID,3)
 rejected:=haWriteFailure(t,ctx,seed,degradedSeeds[0])
 t.Logf("correctly rejected insufficient ISR: %s",rejected)
 haTopicConfig(t,ctx,degraded.Leader.ID,2)
 haPublish(t,ctx,seed,200,4,ack,degradedSeeds...)

 haCompose(t,ctx,"start",fmt.Sprintf("broker%d",stopped))
 restored:=haWaitPartition(t,ctx,3,-1)
 t.Logf("replica recovery ISR=%d leader=%d",len(restored.Isr),restored.Leader.ID)
 restoredSeeds,err:=LiveISREndpoints(restored,haBrokers,3)
 if err!=nil{t.Fatalf("incomplete ISR3 recovery: %v",err)}
 haPublish(t,ctx,seed,300,4,ack,restoredSeeds...)
 observed:=haReadAcknowledged(t,ctx,ack)
 if observed<len(ack){t.Fatalf("observed %d records for %d acknowledged",observed,len(ack))}
 quorumFailure:=haProbeQuorumLossAndRestore(t,ctx,seed,restored.Leader.ID,ack,trace)
 t.Logf("quorum loss produced no new Kafka ACK: %s",quorumFailure)

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
    "stale_bootstrap_probe_error":staleBootstrapError,
    "rebootstrap_same_event_id_ack":true,
    "rebootstrap_live_isr_size":len(degradedSeeds),
    "quorum_loss_tested":true,
    "quorum_isr_timeline_verified":true,
    "quorum_status_observed_not_actual_restoration_instant":true,
    "quorum_loss_producer_error":quorumFailure,
    "acknowledged_ids_verified_after_quorum_restore":true,
    "notes":"Confirmed ACKed records only; failed/unknown ACK may still create a physical record. No cross-AZ, disk-loss, controller quorum-loss, or exactly-once claim.",
 }
 if path:=os.Getenv("M3B_HA_REPORT_PATH");path!=""{
    b,err:=json.MarshalIndent(report,"","  ");if err!=nil{t.Fatal(err)}
    if err:=os.WriteFile(path,b,0600);err!=nil{t.Fatal(err)}
 }
 t.Logf("PASS HA: leader %d→%d, ISR 3→2→3, insufficient ISR refused, %d ACKed IDs replayed",initial.Leader.ID,degraded.Leader.ID,len(ack))
}
