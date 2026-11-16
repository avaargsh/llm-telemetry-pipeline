// Package outbox manages retryable, lease-fenced delivery to Kafka.
// Leasing does not make Kafka publication exactly-once.
package outbox

import (
    "context"
    "database/sql"
    "errors"
    "fmt"
    "time"
)

var ErrLeaseLost=errors.New("outbox lease was lost or superseded")

type Row struct {
    ID int64
    EventID string
    Topic string
    Payload []byte
    Attempts int
}
type Store struct {DB *sql.DB}

func requireLease(worker string, ttl time.Duration)error{
    if worker==""{return errors.New("outbox worker ID is required")}
    if ttl<time.Second{return errors.New("lease must be at least one second")}
    return nil
}

// Claim reserves pending rows with row locks and increments attempts as the
// fencing generation. It never holds a DB transaction during Kafka I/O.
func(s *Store)Claim(ctx context.Context,worker string,lease time.Duration,limit int)([]Row,error){
    if s==nil||s.DB==nil{return nil,errors.New("outbox database required")}
    if err:=requireLease(worker,lease);err!=nil{return nil,err}
    if limit<1||limit>500{return nil,errors.New("claim limit must be 1..500")}
    leaseSeconds:=int64((lease+time.Second-1)/time.Second)
    rows,err:=s.DB.QueryContext(ctx,`
WITH picked AS (
    SELECT outbox_id FROM event_outbox
    WHERE published_at IS NULL AND next_attempt_at<=now()
      AND (lease_until IS NULL OR lease_until<=now())
    ORDER BY next_attempt_at,outbox_id
    FOR UPDATE SKIP LOCKED LIMIT $1
)
UPDATE event_outbox e
SET lease_owner=$2, lease_until=now()+($3::bigint * interval '1 second'),
    attempts=e.attempts+1, last_error=NULL
FROM picked WHERE e.outbox_id=picked.outbox_id
RETURNING e.outbox_id,e.event_id::text,e.topic,e.event_payload::text,e.attempts
`,limit,worker,leaseSeconds)
    if err!=nil{return nil,fmt.Errorf("claim outbox: %w",err)}
    defer rows.Close()
    result:=make([]Row,0,limit)
    for rows.Next(){
        var r Row
        var payload string
        if err:=rows.Scan(&r.ID,&r.EventID,&r.Topic,&payload,&r.Attempts);err!=nil{return nil,err}
        r.Payload=[]byte(payload)
        result=append(result,r)
    }
    if err:=rows.Err();err!=nil{return nil,err}
    return result,nil
}

func(s *Store)MarkPublished(ctx context.Context,r Row,worker string)error{
    if s==nil||s.DB==nil{return errors.New("outbox database required")}
    result,err:=s.DB.ExecContext(ctx,`
UPDATE event_outbox SET published_at=now(), lease_owner=NULL,lease_until=NULL,last_error=NULL
WHERE outbox_id=$1 AND lease_owner=$2 AND attempts=$3 AND lease_until>now()
  AND published_at IS NULL`,r.ID,worker,r.Attempts)
    if err!=nil{return err}
    n,err:=result.RowsAffected();if err!=nil{return err}
    if n!=1{return ErrLeaseLost}
    return nil
}

// ScheduleRetry may be called after an ambiguous Kafka write result. It
// releases only the current lease generation, with persisted backoff.
func(s *Store)ScheduleRetry(ctx context.Context,r Row,worker string,backoff time.Duration,reason string)error{
    if s==nil||s.DB==nil{return errors.New("outbox database required")}
    if backoff<0{return errors.New("negative retry delay")}
    if len(reason)>512 {reason=reason[:512]}
    seconds:=int64((backoff+time.Second-1)/time.Second)
    result,err:=s.DB.ExecContext(ctx,`
UPDATE event_outbox SET next_attempt_at=now()+($4::bigint * interval '1 second'),
 lease_owner=NULL,lease_until=NULL,last_error=$5
WHERE outbox_id=$1 AND lease_owner=$2 AND attempts=$3
 AND lease_until>now() AND published_at IS NULL
`,r.ID,worker,r.Attempts,seconds,reason)
    if err!=nil{return err}
    n,err:=result.RowsAffected();if err!=nil{return err}
    if n!=1{return ErrLeaseLost}
    return nil
}
