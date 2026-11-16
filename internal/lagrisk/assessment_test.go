package lagrisk

import (
  "strings"
  "testing"
  "time"
)
func snap() Snapshot {
  now:=time.Date(2026,11,16,12,0,0,0,time.UTC)
  return Snapshot{
    Topic:"m3d.outage.v1",Partition:0,LogStartOffset:0,
    LogEndOffset:120,CommittedOffset:100,
    OldestUnconsumedTimestamp:now.Add(-30*time.Second),
    Now:now,Retention:10*time.Minute,SafetyMargin:2*time.Minute,
  }
}
func TestRetentionRiskBoundaries(t *testing.T){
 tests:=[]struct{name string; age time.Duration;want Status;toExpiry time.Duration}{
  {"healthy",time.Minute,StatusHealthy,9*time.Minute},
  {"warning",8*time.Minute,StatusWarning,2*time.Minute},
  {"critical",9*time.Minute,StatusCritical,time.Minute},
  {"expired",11*time.Minute,StatusExpired,-time.Minute},
 }
 for _,tc:=range tests{
  t.Run(tc.name,func(t *testing.T){
   s:=snap();s.OldestUnconsumedTimestamp=s.Now.Add(-tc.age)
   r,err:=Assess(s);if err!=nil{t.Fatal(err)}
   if r.Status!=tc.want||r.TimeToExpirySeconds!=int64(tc.toExpiry/time.Second){
    t.Fatalf("status=%s expiry=%d expected=%s/%d",r.Status,r.TimeToExpirySeconds,tc.want,int64(tc.toExpiry/time.Second))
   }
   if r.LagOffsetDistance!=20{t.Fatalf("wrong lag=%d",r.LagOffsetDistance)}
  })
 }
}
func TestAlreadyTrimmedCommittedOffsetIsDataGap(t *testing.T){
 s:=snap();s.LogStartOffset=105
 r,err:=Assess(s);if err!=nil{t.Fatal(err)}
 if r.Status!=StatusDataGap||r.MissingOffsetDistance!=5{t.Fatalf("not flagged as data gap: %+v",r)}
}
func TestEmptyLagCannotRaiseFalseRetentionAlarm(t *testing.T){
 s:=snap();s.CommittedOffset=s.LogEndOffset
 s.OldestUnconsumedTimestamp=time.Time{}
 r,err:=Assess(s);if err!=nil{t.Fatal(err)}
 if r.Status!=StatusNoBacklog||r.LagOffsetDistance!=0{t.Fatalf("unexpected result %+v",r)}
}
func TestUnknownTimestampOrOffsetFailsClosed(t *testing.T){
 for _,mutate:=range []func(*Snapshot){
  func(s *Snapshot){s.OldestUnconsumedTimestamp=time.Time{}},
  func(s *Snapshot){s.CommittedOffset=-1},
  func(s *Snapshot){s.LogStartOffset=130},
  func(s *Snapshot){s.Retention=0},
  func(s *Snapshot){s.SafetyMargin=s.Retention+time.Second},
  func(s *Snapshot){s.OldestUnconsumedTimestamp=s.Now.Add(time.Minute)},
 }{
  s:=snap();mutate(&s)
  if _,err:=Assess(s);err==nil{t.Fatalf("invalid lag input accepted: %+v",s)}
 }
}
func TestRetentionRiskCarriesEvidenceAndWarnThreshold(t *testing.T){
 s:=snap()
 r,err:=Assess(s);if err!=nil{t.Fatal(err)}
 if r.Topic!=s.Topic||r.Partition!=s.Partition||r.LogStartOffset!=0||r.CommittedOffset!=100{
  t.Fatalf("missing source offsets: %+v",r)
 }
 if r.TimeToExpirySeconds<=0 || !strings.Contains(r.Reason,"retention"){
  t.Fatalf("missing retention reason: %+v",r)
 }
}
