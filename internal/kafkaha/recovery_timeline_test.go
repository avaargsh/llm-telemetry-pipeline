package kafkaha

import (
 "strings"
 "testing"
)

func sampleRecoveryMilestones() []RecoveryMilestone {
 return []RecoveryMilestone{
  {Name:"two_voters_stopped",ElapsedMS:100,ISR:1},
  {Name:"quorum_loss_no_ack",ElapsedMS:250,ISR:1},
  {Name:"first_voter_restarted",ElapsedMS:400},
  {Name:"quorum_status_observed",ElapsedMS:700,LeaderID:1,HighWatermark:51},
  {Name:"intermediate_isr2_observed",ElapsedMS:1100,ISR:2,LeaderID:1},
  {Name:"second_voter_restarted",ElapsedMS:1500},
  {Name:"final_isr3_observed",ElapsedMS:2200,ISR:3,LeaderID:1},
  {Name:"all_acked_replayed",ElapsedMS:2600,AckedCount:35},
 }
}

func TestRecoveryMilestonesProofHasSeparateQuorumAndISRBoundaries(t *testing.T){
 steps:=sampleRecoveryMilestones()
 result,err:=ValidateRecoveryMilestones(steps)
 if err!=nil{t.Fatal(err)}
 if result.QuorumStatusObservedMS!=300{
  t.Fatalf("first voter restart -> quorum observation want 300ms, got %d",result.QuorumStatusObservedMS)
 }
 if result.FirstVoterToISR2MS!=700{t.Fatal("missing quorum-to-ISR2 distinction")}
 if result.SecondVoterToISR3MS!=700{t.Fatal("second voter to ISR3 timing incorrect")}
 if result.EndToEndObservationMS!=2500{t.Fatal("unexpected end-to-end elapsed time")}
}
func TestRecoveryMilestonesRejectMissingOrOutOfOrderQuorumEvidence(t *testing.T){
 base:=sampleRecoveryMilestones()
 copyWithout:=func(drop string) []RecoveryMilestone {
  var out []RecoveryMilestone
  for _,v:=range base{if v.Name!=drop{out=append(out,v)}}
  return out
 }
 for _,s:=range []string{"quorum_status_observed","final_isr3_observed","quorum_loss_no_ack","all_acked_replayed"}{
  if _,err:=ValidateRecoveryMilestones(copyWithout(s));err==nil{t.Fatalf("accepted missing %s",s)}
 }
 swapped:=append([]RecoveryMilestone(nil),base...)
 swapped[3],swapped[4]=swapped[4],swapped[3]
 if _,err:=ValidateRecoveryMilestones(swapped);err==nil{t.Fatal("accepted out-of-order recovery milestones")}
 duplicate:=append(append([]RecoveryMilestone(nil),base...),base[7])
 if _,err:=ValidateRecoveryMilestones(duplicate);err==nil{t.Fatal("accepted duplicate final recovery milestone")}
 regressed:=append([]RecoveryMilestone(nil),base...)
 regressed[5].ElapsedMS=1000
 if _,err:=ValidateRecoveryMilestones(regressed);err==nil{t.Fatal("accepted regressing event time")}
}
func TestRecoveryMilestonesEnforceISR3AndAckedCount(t *testing.T){
 for _,mutate:=range []func([]RecoveryMilestone){
  func(a []RecoveryMilestone){a[6].ISR=2},
  func(a []RecoveryMilestone){a[4].ISR=1},
  func(a []RecoveryMilestone){a[3].LeaderID=0},
  func(a []RecoveryMilestone){a[3].HighWatermark=-1},
  func(a []RecoveryMilestone){a[7].AckedCount=34},
 }{
  v:=append([]RecoveryMilestone(nil),sampleRecoveryMilestones()...)
  mutate(v)
  if _,err:=ValidateRecoveryMilestones(v);err==nil{t.Fatalf("incomplete HA proof accepted: %+v",v)}
 }
}
func TestParseQuorumStatusIsStrictAndSanitized(t *testing.T){
 status,err:=ParseQuorumStatus("ClusterId: test\nLeaderId: 2\nHighWatermark: 314\nCurrentVoters: [{id=1}, {id=2}, {id=3}]\nsecret: never-store\n")
 if err!=nil{t.Fatal(err)}
 if status.LeaderID!=2||status.HighWatermark!=314{t.Fatalf("bad parsing: %+v",status)}
 if strings.Contains(status.Summary,"secret") {t.Fatal("raw metadata leaked to summary")}
 for _,raw:=range []string{
  "LeaderId: 2\nHighWatermark: 314\n",
  "LeaderId: -1\nHighWatermark: 314\nCurrentVoters: [1,2,3]\n",
  "LeaderId: 2\nHighWatermark: -1\nCurrentVoters: [1,2,3]\n",
  "LeaderId: invalid\nHighWatermark: 1\nCurrentVoters: [1,2,3]\n",
  "LeaderId: 2\nHighWatermark: 1\nCurrentVoters: []\n",
 }{
  if _,err:=ParseQuorumStatus(raw);err==nil{t.Fatalf("accepted invalid quorum status: %q",raw)}
 }
}
