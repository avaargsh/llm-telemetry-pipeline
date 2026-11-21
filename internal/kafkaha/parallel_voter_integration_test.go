package kafkaha

import (
 "context"
 "encoding/json"
 "fmt"
 "os"
 "path/filepath"
 "testing"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
 "github.com/segmentio/kafka-go"
)

// This is an isolated CI-only reenactment of the historical M3b failure:
// the two stopped combined controller/brokers are started with a SINGLE Docker
// Compose invocation (no ISR2 gate between starts). Existing staged recovery
// remains separate and unchanged.
func TestThreeBrokerParallelVoterRestart(t *testing.T) {
 if os.Getenv("TEST_KAFKA_HA_CONCURRENT")!="1" {
    t.Skip("requires isolated Kafka KRaft 3-broker Docker fault profile")
 }
 ctx,cancel:=context.WithTimeout(context.Background(),7*time.Minute)
 defer cancel()
 report:=map[string]any{
    "schema_version":1,
    "scenario":"two_voter_parallel_restart",
    "result":"INCOMPLETE",
    "exact_head":os.Getenv("M3B_EXACT_HEAD"),
    "scope":"disposable_kraft_combined_roles_parallel_start_not_production_RTO",
 }
 // Persist partial evidence if the real Docker/Kafka operation fails.
 // Never turn an incomplete trace into a successful receipt.
 t.Cleanup(func(){
    path:=os.Getenv("M3B_PARALLEL_REPORT_PATH")
    if path==""{t.Error("M3B_PARALLEL_REPORT_PATH required");return}
    if err:=os.MkdirAll(filepath.Dir(path),0700);err!=nil{t.Error(err);return}
    data,err:=json.MarshalIndent(report,"","  ")
    if err!=nil{t.Error(err);return}
    tmp,err:=os.CreateTemp(filepath.Dir(path),".m3b-parallel-")
    if err!=nil{t.Error(err);return}
    defer os.Remove(tmp.Name())
    if err=tmp.Chmod(0600);err!=nil{_ = tmp.Close();t.Error(err);return}
    _,err=tmp.Write(append(data,'\n'))
    if err!=nil{_ = tmp.Close();t.Error(err);return}
    if err=tmp.Close();err!=nil{t.Error(err);return}
    if err=os.Rename(tmp.Name(),path);err!=nil{t.Error(err)}
 })
 seed:=fmt.Sprintf("m3b-parallel-%d",time.Now().UnixNano())
 ack:=map[string][]byte{}
 initial:=haWaitPartition(t,ctx,3,-1)
 report["initial_leader"]=initial.Leader.ID
 report["initial_replication_factor"]=len(initial.Replicas)
 report["initial_isr"]=len(initial.Isr)
 t.Logf("before simultaneous controller restart: leader=%d RF=%d ISR=%d",
     initial.Leader.ID,len(initial.Replicas),len(initial.Isr))
 haPublish(t,ctx,seed,0,35,ack)
 report["acknowledged_ids"]=len(ack)
 keeper:=initial.Leader.ID
 if keeper<1||keeper>3{t.Fatalf("invalid data leader %d",keeper)}
 var stopped []string
 var stoppedIDs []int
 for i:=1;i<=3;i++{
    if i!=keeper{stopped=append(stopped,fmt.Sprintf("broker%d",i));stoppedIDs=append(stoppedIDs,i)}
 }
 report["stopped_voter_ids"]=stoppedIDs
 // Stop both voters, preserving every Kafka Docker named volume.
 haCompose(t,ctx,append([]string{"stop"},stopped...)...)
 t.Logf("two non-leader voters stopped; survivor broker%d; ACK must fail",keeper)
 select {case <-time.After(8*time.Second):case <-ctx.Done():t.Fatal(ctx.Err())}
 e,err:=event.SyntheticUsageEvent(seed,1000,time.Date(2026,11,16,11,0,0,0,time.UTC))
 if err!=nil{t.Fatal(err)}
 raw,err:=json.Marshal(e)
 if err!=nil{t.Fatal(err)}
 writer:=haNewWriter(1,haBrokers[keeper-1])
 writeCtx,stopWrite:=context.WithTimeout(ctx,12*time.Second)
 writeErr:=writer.WriteMessages(writeCtx,kafka.Message{Key:[]byte(e.EventID),Value:raw})
 stopWrite()
 _=writer.Close()
 if writeErr==nil{t.Fatal("unsafe Kafka ACK under two-controller quorum loss")}
 // The unacknowledged record may still exist; never call it absent/rolled back.
 report["no_ack_during_quorum_loss"]=true
 report["quorum_loss_probe_outcome"]="UNKNOWN_NO_ACK"
 t.Logf("during loss of quorum no ACK (outcome UNKNOWN): %T",writeErr)

 // Critically: ONE Docker compose start request with BOTH voters. No pause,
 // sequential ISR2 observation, delete, format or volume reset between them.
 restartStart:=time.Now()
 haCompose(t,ctx,append([]string{"start"},stopped...)...)
 report["single_compose_restart_request"]=true
 t.Logf("start of both stopped voter services requested in one docker compose command")
 q:=haQuorumStatusObserved(t,ctx,keeper)
 report["quorum_status_observed"]=true
 report["quorum_leader_id"]=q.LeaderID
 report["quorum_high_watermark"]=q.HighWatermark
 quorumMS:=time.Since(restartStart).Milliseconds()
 t.Logf("quorum status observed after requested restart at %d ms",quorumMS)
 recovered:=haWaitPartition(t,ctx,3,-1)
 if err:=RequireReplicaAndISR(recovered,3,3);err!=nil{t.Fatal(err)}
 isr3MS:=time.Since(restartStart).Milliseconds()
 report["recovered_replication_factor"]=len(recovered.Replicas)
 report["recovered_isr"]=len(recovered.Isr)
 t.Logf("real RF3/ISR3 observed at %d ms leader=%d",isr3MS,recovered.Leader.ID)
 got:=haReadAcknowledged(t,ctx,ack)
 if got<len(ack){t.Fatalf("only %d of %d previous ACKs visible",got,len(ack))}
 readbackMS:=time.Since(restartStart).Milliseconds()
 report["readback_records"]=got
 report["all_acknowledged_verified"]=true
 report["timings_ms"]=map[string]int64{
    "restart_to_quorum_status_observed":quorumMS,
    "restart_to_isr3_observed":isr3MS,
    "restart_to_ack_readback":readbackMS,
 }
 report["result"]="PASS_EXPERIMENT_ONLY"
 t.Logf("PASS concurrent voters: RF3/ISR3, no unsafe ACK, %d/%d ACKs replayed",got,len(ack))
}
