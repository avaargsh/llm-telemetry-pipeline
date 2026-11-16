// Package lagrisk assesses Kafka offset-distance and estimated retention
// expiry risks from externally observed broker data. It is read-only.
package lagrisk

import (
 "errors"
 "fmt"
 "time"
)

type Status string
const (
 StatusNoBacklog Status = "no_backlog"
 StatusHealthy Status = "healthy"
 StatusWarning Status = "warning"
 StatusCritical Status = "critical"
 StatusExpired Status = "expiry_risk"
 StatusDataGap Status = "data_gap"
)

type Snapshot struct {
 Topic string `json:"topic"`
 Partition int `json:"partition"`
 LogStartOffset int64 `json:"log_start_offset"`
 LogEndOffset int64 `json:"log_end_offset"`
 CommittedOffset int64 `json:"committed_offset"`
 OldestUnconsumedTimestamp time.Time `json:"oldest_unconsumed_timestamp"`
 Now time.Time `json:"observed_at"`
 Retention time.Duration `json:"-"`
 SafetyMargin time.Duration `json:"-"`
}
type Assessment struct {
 Topic string `json:"topic"`
 Partition int `json:"partition"`
 Status Status `json:"status"`
 Reason string `json:"reason"`
 LogStartOffset int64 `json:"log_start_offset"`
 LogEndOffset int64 `json:"log_end_offset"`
 CommittedOffset int64 `json:"committed_offset"`
 LagOffsetDistance int64 `json:"lag_offset_distance"`
 MissingOffsetDistance int64 `json:"missing_offset_distance"`
 OldestUnconsumedTimestamp *time.Time `json:"oldest_unconsumed_timestamp,omitempty"`
 ObservedAt time.Time `json:"observed_at"`
 RetentionSeconds int64 `json:"retention_seconds"`
 SafetyMarginSeconds int64 `json:"safety_margin_seconds"`
 TimeToExpirySeconds int64 `json:"time_to_expiry_seconds"`
}

// Assess refuses unknown offsets or timestamps while a partition has lag.
// Offset distance is not the precise number of available Kafka records when
// compaction, transactions or offset gaps exist. The oldest record timestamp
// is an estimate of time-to-retention-expiry, NOT an authoritative deadline:
// segment-level deletion and Kafka retention implementation may differ.
func Assess(s Snapshot)(Assessment,error){
 if s.Topic==""||s.Partition<0||s.Now.IsZero(){
  return Assessment{},errors.New("topic/partition/observation time required")
 }
 if s.Retention<=0||s.SafetyMargin<=0||s.SafetyMargin>=s.Retention{
  return Assessment{},errors.New("finite retention and positive smaller safety margin required")
 }
 if s.LogStartOffset<0||s.LogEndOffset<s.LogStartOffset||
    s.CommittedOffset<0||s.CommittedOffset>s.LogEndOffset{
  return Assessment{},errors.New("unknown or inconsistent Kafka offsets")
 }
 r:=Assessment{
  Topic:s.Topic,Partition:s.Partition,
  LogStartOffset:s.LogStartOffset,LogEndOffset:s.LogEndOffset,
  CommittedOffset:s.CommittedOffset,ObservedAt:s.Now.UTC(),
  RetentionSeconds:int64(s.Retention/time.Second),
  SafetyMarginSeconds:int64(s.SafetyMargin/time.Second),
  LagOffsetDistance:s.LogEndOffset-s.CommittedOffset,
 }
 if s.CommittedOffset<s.LogStartOffset{
  r.MissingOffsetDistance=s.LogStartOffset-s.CommittedOffset
  r.Status=StatusDataGap
  r.Reason=fmt.Sprintf("committed offset %d precedes Kafka log start %d: retention/data gap; do not assume recovery",s.CommittedOffset,s.LogStartOffset)
  return r,nil
 }
 if s.CommittedOffset==s.LogEndOffset{
  r.Status=StatusNoBacklog
  r.Reason="no unconsumed Kafka offset distance in this partition"
  return r,nil
 }
 if s.OldestUnconsumedTimestamp.IsZero()||s.OldestUnconsumedTimestamp.After(s.Now){
  return Assessment{},errors.New("real oldest unconsumed Kafka timestamp required when lag is positive")
 }
 timestamp:=s.OldestUnconsumedTimestamp.UTC()
 r.OldestUnconsumedTimestamp=&timestamp
 remaining:=timestamp.Add(s.Retention).Sub(s.Now)
 r.TimeToExpirySeconds=int64(remaining/time.Second)
 // Leave an actionable warning interval before the critical window.
 // Saturate at retention to avoid overflowing time.Duration for large margins.
 warningThreshold:=s.Retention
 if s.SafetyMargin<=s.Retention/2{warningThreshold=2*s.SafetyMargin}
 switch {
 case remaining<=0:
  r.Status=StatusExpired
  r.Reason="oldest pending record has reached or exceeded estimated retention age; data may already be at risk"
 case remaining<=s.SafetyMargin:
  r.Status=StatusCritical
  r.Reason="estimated Kafka retention expiry is inside configured critical safety margin"
 case remaining<=warningThreshold:
  r.Status=StatusWarning
  r.Reason="estimated Kafka retention expiry has entered the early-warning window"
 default:
  r.Status=StatusHealthy
  r.Reason="estimated Kafka retention expiry is beyond early-warning window"
 }
 return r,nil
}
