package main

import (
 "testing"
)

func TestRequirePostgreSQLRole(t *testing.T) {
 cases:=[]struct{phase string; recovery bool; wantErr bool}{
  {"primary",false,false},{"standby",true,false},{"promoted",false,false},
  {"primary",true,true},{"standby",false,true},{"promoted",true,true},
  {"unrecognized",false,true},
 }
 for _,tc:=range cases {
  err:=requireRole(tc.phase,tc.recovery)
  if (err!=nil)!=tc.wantErr {t.Errorf("phase=%s recovery=%v err=%v wantErr=%v",tc.phase,tc.recovery,err,tc.wantErr)}
 }
}

func TestRequireAtomicLedgerOutboxCounts(t *testing.T) {
 cases:=[]struct{ledger,outbox,expected int;wantErr bool}{
  {12,12,12,false},{13,13,13,false},{12,11,12,true},{13,14,13,true},{0,0,12,true},
  {-1,12,12,true},{12,12,0,true},
 }
 for _,tc:=range cases {
  err:=requireAtomicCounts(tc.ledger,tc.outbox,tc.expected)
  if (err!=nil)!=tc.wantErr {t.Errorf("%+v err=%v",tc,err)}
 }
}

func TestValidateFailoverEvidenceRejectsUnprovenClaims(t *testing.T) {
 e:=failoverReceipt{
  SchemaVersion:1,Mode:"manual_synchronous_pg17",CommittedBefore:12,
  ReplayedOnStandby:12,PromotedLedger:13,PromotedOutbox:13,
  UncommittedOldPrimaryRolledBack:true,StableIDRetryCreated:true,
  ConfirmedBeforeRetriedAsDuplicate:12,StaleLeaseFenced:true,
  OldPrimaryRemainedStopped:true,PublishedWithoutKafkaACK:0,
 }
 if err:=validateReceipt(e);err!=nil{t.Fatal(err)}
 changed:=e;changed.StaleLeaseFenced=false
 if err:=validateReceipt(changed);err==nil{t.Fatal("stale lease not fenced but PASS possible")}
 changed=e;changed.PublishedWithoutKafkaACK=1
 if err:=validateReceipt(changed);err==nil{t.Fatal("unconfirmed Kafka publishing must never count PASS")}
 changed=e;changed.ReplayedOnStandby=11
 if err:=validateReceipt(changed);err==nil{t.Fatal("ACKed ledger not present on standby")}
 changed=e;changed.OldPrimaryRemainedStopped=false
 if err:=validateReceipt(changed);err==nil{t.Fatal("old primary risk must fail closed")}
}
