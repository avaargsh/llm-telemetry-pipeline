package kafkaha

// Recovery evidence for an isolated three-node combined-role KRaft fault lab.
// Controller-status observations and topic ISR catch-up are separate milestones;
// neither a TCP probe nor a successful write can stand in for the other.
import (
 "encoding/json"
 "errors"
 "fmt"
 "os"
 "path/filepath"
 "strconv"
 "strings"
 "time"
)

type QuorumStatus struct {
 LeaderID int `json:"leader_id"`
 HighWatermark int64 `json:"high_watermark"`
 Summary string `json:"summary"`
}

// ParseQuorumStatus retains numeric status only; raw quorum output, credentials,
// endpoints and internal broker metadata never enter the machine-readable proof.
func ParseQuorumStatus(raw string)(QuorumStatus,error){
 values:=map[string]string{}
 for _,line:=range strings.Split(raw,"\n"){
   key,value,ok:=strings.Cut(line,":")
   if !ok{continue}
   key=strings.TrimSpace(key)
   if key=="LeaderId"||key=="HighWatermark"||key=="CurrentVoters" {
      if _,seen:=values[key];seen{return QuorumStatus{},errors.New("duplicate quorum status field")}
      values[key]=strings.TrimSpace(value)
   }
 }
 id,err:=strconv.Atoi(values["LeaderId"])
 if err!=nil||id<1||id>3{return QuorumStatus{},errors.New("invalid Kafka quorum LeaderId")}
 high,err:=strconv.ParseInt(values["HighWatermark"],10,64)
 if err!=nil||high<0{return QuorumStatus{},errors.New("invalid Kafka quorum HighWatermark")}
 voters:=values["CurrentVoters"]
 if voters==""||voters=="[]"||!strings.HasPrefix(voters,"[")||!strings.HasSuffix(voters,"]"){
   return QuorumStatus{},errors.New("missing Kafka CurrentVoters")
 }
 return QuorumStatus{LeaderID:id,HighWatermark:high,
   Summary:fmt.Sprintf("leader=%d;high_watermark=%d",id,high)},nil
}

type RecoveryMilestone struct {
 Name string `json:"name"`
 ElapsedMS int64 `json:"elapsed_ms"`
 LeaderID int `json:"leader_id,omitempty"`
 ISR int `json:"isr,omitempty"`
 HighWatermark int64 `json:"high_watermark,omitempty"`
 AckedCount int `json:"acked_count,omitempty"`
}

type RecoveryDurations struct {
 QuorumStatusObservedMS int64 `json:"first_restart_to_quorum_status_observed_ms"`
 FirstVoterToISR2MS int64 `json:"first_restart_to_isr2_observed_ms"`
 SecondVoterToISR3MS int64 `json:"second_restart_to_isr3_observed_ms"`
 EndToEndObservationMS int64 `json:"two_voter_loss_to_ack_readback_ms"`
}

func ValidateRecoveryMilestones(steps []RecoveryMilestone)(RecoveryDurations,error){
 expected:=[]string{
  "two_voters_stopped","quorum_loss_no_ack","first_voter_restarted",
  "quorum_status_observed","intermediate_isr2_observed",
  "second_voter_restarted","final_isr3_observed","all_acked_replayed",
 }
 if len(steps)!=len(expected){return RecoveryDurations{},fmt.Errorf("missing/extra KRaft recovery milestones: got %d want %d",len(steps),len(expected))}
 for i,s:=range steps{
   if s.Name!=expected[i]{return RecoveryDurations{},fmt.Errorf("unexpected recovery milestone %q at index %d, want %q",s.Name,i,expected[i])}
   if s.ElapsedMS<0||(i>0&&s.ElapsedMS<steps[i-1].ElapsedMS){
      return RecoveryDurations{},errors.New("monotonic recovery milestones out of order")
   }
 }
 // Stopped containers and an unacknowledged write do not establish a
 // broker ISR size. ISR fields at these stages must remain NOT_OBSERVED.
 if steps[0].ISR!=0||steps[1].ISR!=0||
    steps[3].LeaderID<1||steps[3].LeaderID>3||steps[3].HighWatermark<0||
    steps[4].ISR!=2||steps[4].LeaderID<1||
    steps[6].ISR!=3||steps[6].LeaderID<1||steps[7].AckedCount!=35{
    return RecoveryDurations{},errors.New("unproven quorum/ISR safety or lost acknowledged messages")
 }
 return RecoveryDurations{
  QuorumStatusObservedMS:steps[3].ElapsedMS-steps[2].ElapsedMS,
  FirstVoterToISR2MS:steps[4].ElapsedMS-steps[2].ElapsedMS,
  SecondVoterToISR3MS:steps[6].ElapsedMS-steps[5].ElapsedMS,
  EndToEndObservationMS:steps[7].ElapsedMS-steps[0].ElapsedMS,
 },nil
}

type RecoveryTimeline struct {
 started time.Time
 steps []RecoveryMilestone
}

// A Go monotonic clock captures all durations in the same test process.
func NewRecoveryTimeline() *RecoveryTimeline{return &RecoveryTimeline{started:time.Now()}}
func (r *RecoveryTimeline) Mark(name string,leader,isr int,high int64,acked int){
 if r==nil{return}
 r.steps=append(r.steps,RecoveryMilestone{
  Name:name,ElapsedMS:time.Since(r.started).Milliseconds(),
  LeaderID:leader,ISR:isr,HighWatermark:high,AckedCount:acked,
 })
}

// WriteReceiptAlways persists a redacted partial timeline even when the real
// experiment called t.Fatal. An INCOMPLETE receipt never means the test passed.
func (r *RecoveryTimeline) WriteReceiptAlways(path,head string) error {
 if path==""||r==nil{return nil}
 proof,validationErr:=ValidateRecoveryMilestones(r.steps)
 status:="INCOMPLETE"
 if validationErr==nil{status="PASS_EXPERIMENT_ONLY"}
 envelope:=map[string]any{
   "schema_version":1,
   "head_sha":head,
   "result":status,
   "recovery_milestones":r.steps,
   "scope":"three_combined_kraft_brokers_disposable_lab_no_production_rto",
   "quorum_status_observed_not_actual_quorum_establishment_instant":true,
 }
 if validationErr==nil{envelope["observed_durations_ms"]=proof}
 if err:=os.MkdirAll(filepath.Dir(path),0700);err!=nil{return err}
 data,err:=json.MarshalIndent(envelope,"","  ")
 if err!=nil{return err}
 tmp,err:=os.CreateTemp(filepath.Dir(path),".recovery-evidence-")
 if err!=nil{return err}
 defer os.Remove(tmp.Name())
 if err=tmp.Chmod(0600);err!=nil{tmp.Close();return err}
 if _,err=tmp.Write(append(data,'\n'));err!=nil{tmp.Close();return err}
 if err=tmp.Close();err!=nil{return err}
 return os.Rename(tmp.Name(),path)
}
