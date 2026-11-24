package genaiobs

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	MaxDurableQueueCapacity = 1000000
	MaxDurableReceiptCapacity = 2000000
	durableCapacityAdvisoryLock int64 = 0x47454e41494f4253
	durableReceiptRetention = 24 * time.Hour
)

var (
	ErrDurableQueueFull = errors.New("durable observation queue full")
	ErrObservationConflict = errors.New("observation identity conflicts with a different projection")
	ErrDurableLeaseLost = errors.New("durable observation lease lost or superseded")
	ErrInvalidDurableConfig = errors.New("invalid durable observation store configuration")
)

type Delivery struct {
	ID int64
	Observation Observation
	Attempts int
	LeaseUntil time.Time
}

// IdempotencyKey is stable across retries and process restarts for this queue row.
func (d Delivery) IdempotencyKey() string { return fmt.Sprintf("genaiobs/%d", d.ID) }

type DurableRepository interface {
	Claim(context.Context,string,time.Duration,int)([]Delivery,error)
	Ack(context.Context,Delivery,string)error
	Retry(context.Context,Delivery,string,time.Duration,string)error
	MarkTerminal(context.Context,Delivery,string,string)error
	Release(context.Context,Delivery,string)error
}

type DurableStore struct {
	DB *sql.DB
	Capacity int
}

func NewDurableStore(db *sql.DB, capacity int) (*DurableStore,error) {
	if db==nil || capacity<1 || capacity>MaxDurableQueueCapacity {
		return nil,ErrInvalidDurableConfig
	}
	return &DurableStore{DB:db,Capacity:capacity},nil
}

func validateObservation(o Observation) error {
	if o.SchemaVersion!=Version || !identifier(o.TenantID,128) ||
		!hexID(o.TraceID,32) || !hexID(o.SpanID,16) ||
		!optionalID(o.RequestID,128) || !optionalID(o.AttemptID,128) ||
		!identifier(o.Provider,256) || !optionalID(o.RequestModel,256) ||
		!optionalID(o.ResponseModel,256) {
		return ErrInvalid
	}
	switch o.Operation { case "chat","generate_content","realtime_inference","text_completion","embeddings": default: return ErrInvalid }
	switch o.Status { case "unset","ok","error": default: return ErrInvalid }
	if (o.InputTokens!=nil && *o.InputTokens<0) || (o.OutputTokens!=nil && *o.OutputTokens<0) {
		return ErrInvalid
	}
	return nil
}

func observationHash(o Observation) (string,error) {
	data,err:=json.Marshal(o)
	if err!=nil { return "",err }
	sum:=sha256.Sum256(data)
	return hex.EncodeToString(sum[:]),nil
}

func (s *DurableStore) requireDB() error {
	if s==nil || s.DB==nil || s.Capacity<1 || s.Capacity>MaxDurableQueueCapacity {
		return ErrInvalidDurableConfig
	}
	return nil
}

// Enqueue admits one normalized observation. Its identity receipt suppresses
// identical replays for 24 hours, including replays after downstream ACK.
func (s *DurableStore) Enqueue(ctx context.Context,o Observation) error {
	return s.EnqueueBatch(ctx,[]Observation{o})
}

// EnqueueBatch is all-or-nothing. A transaction-scoped advisory lock serializes
// capacity checks across replicas. Duplicate identities with equal projections
// are idempotent; the same identity with different data is an integrity error.
func (s *DurableStore) EnqueueBatch(ctx context.Context,observations []Observation) error {
	if err:=s.requireDB();err!=nil { return err }
	if ctx==nil { return errors.New("context required") }
	if len(observations)==0 { return nil }
	if len(observations)>s.Capacity { return ErrDurableQueueFull }
	type prepared struct { o Observation; hash string }
	items:=make([]prepared,len(observations))
	for i,o:=range observations {
		if err:=validateObservation(o);err!=nil { return err }
		hash,err:=observationHash(o);if err!=nil { return err }
		items[i]=prepared{o:o,hash:hash}
	}
	tx,err:=s.DB.BeginTx(ctx,nil);if err!=nil { return fmt.Errorf("begin durable observation admission: %w",err) }
	defer tx.Rollback()
	if _,err=tx.ExecContext(ctx,"SELECT pg_advisory_xact_lock($1)",durableCapacityAdvisoryLock);err!=nil {
		return fmt.Errorf("lock durable observation capacity: %w",err)
	}
	var queueCount,receiptCount,configuredCapacity int64
	if err=tx.QueryRowContext(ctx,`SELECT queue_items,receipt_items,capacity_limit
FROM genai_observation_queue_state WHERE singleton=true FOR UPDATE`).Scan(&queueCount,&receiptCount,&configuredCapacity);err!=nil {
		return fmt.Errorf("lock durable observation counters: %w",err)
	}
	if configuredCapacity==0 {
		configuredCapacity=int64(s.Capacity)
		if _,err=tx.ExecContext(ctx,"UPDATE genai_observation_queue_state SET capacity_limit=$1 WHERE singleton=true",configuredCapacity);err!=nil {
			return fmt.Errorf("configure durable observation capacity: %w",err)
		}
	} else if configuredCapacity!=int64(s.Capacity) {
		return ErrInvalidDurableConfig
	}
	expiredResult,expireErr:=tx.ExecContext(ctx,"DELETE FROM genai_observation_receipts WHERE expires_at<=now()")
	if expireErr!=nil { return fmt.Errorf("expire observation dedupe receipts: %w",expireErr) }
	expired,expireErr:=expiredResult.RowsAffected()
	if expireErr!=nil { return expireErr }
	receiptCount-=expired
	if receiptCount<0 { return errors.New("durable observation receipt counter is inconsistent") }
	var queueAdded,receiptAdded int64
	for _,item:=range items {
		o:=item.o
		var receiptHash string
		err=tx.QueryRowContext(ctx,`INSERT INTO genai_observation_receipts(tenant_id,trace_id,span_id,payload_hash,expires_at)
VALUES($1,$2,$3,$4,now()+($5::bigint*interval '1 millisecond'))
ON CONFLICT(tenant_id,trace_id,span_id) DO NOTHING
RETURNING payload_hash`,o.TenantID,o.TraceID,o.SpanID,item.hash,durableReceiptRetention.Milliseconds()).Scan(&receiptHash)
		if errors.Is(err,sql.ErrNoRows) {
			if err=tx.QueryRowContext(ctx,`SELECT payload_hash FROM genai_observation_receipts
WHERE tenant_id=$1 AND trace_id=$2 AND span_id=$3 FOR SHARE`,o.TenantID,o.TraceID,o.SpanID).Scan(&receiptHash);err!=nil {
				return fmt.Errorf("read observation dedupe receipt: %w",err)
			}
			if strings.TrimSpace(receiptHash)!=item.hash { return ErrObservationConflict }
			continue
		}
		if err!=nil { return fmt.Errorf("write observation dedupe receipt: %w",err) }
		receiptAdded++
		var input,output any
		if o.InputTokens!=nil { input=*o.InputTokens }
		if o.OutputTokens!=nil { output=*o.OutputTokens }
		result,insertErr:=tx.ExecContext(ctx,`INSERT INTO genai_observations
(schema_version,tenant_id,trace_id,span_id,request_id,attempt_id,provider,operation,
 request_model,response_model,status,input_tokens,output_tokens,payload_hash)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
ON CONFLICT(tenant_id,trace_id,span_id) DO NOTHING`,
			o.SchemaVersion,o.TenantID,o.TraceID,o.SpanID,o.RequestID,o.AttemptID,o.Provider,o.Operation,
			o.RequestModel,o.ResponseModel,o.Status,input,output,item.hash)
		if insertErr!=nil { return fmt.Errorf("insert durable observation: %w",insertErr) }
		affected,insertErr:=result.RowsAffected();if insertErr!=nil { return insertErr }
		if affected==1 {
			queueAdded++
		} else {
			var existing string
			if err=tx.QueryRowContext(ctx,`SELECT payload_hash FROM genai_observations
WHERE tenant_id=$1 AND trace_id=$2 AND span_id=$3 FOR SHARE`,o.TenantID,o.TraceID,o.SpanID).Scan(&existing);err!=nil {
				return fmt.Errorf("verify existing durable observation: %w",err)
			}
			if strings.TrimSpace(existing)!=item.hash { return ErrObservationConflict }
		}
	}
	if queueCount+queueAdded>configuredCapacity || receiptCount+receiptAdded>MaxDurableReceiptCapacity {
		return ErrDurableQueueFull
	}
	if _,err=tx.ExecContext(ctx,`UPDATE genai_observation_queue_state
SET queue_items=$1,receipt_items=$2 WHERE singleton=true`,queueCount+queueAdded,receiptCount+receiptAdded);err!=nil {
		return fmt.Errorf("update durable observation counters: %w",err)
	}
	if err=tx.Commit();err!=nil { return fmt.Errorf("commit durable observation admission; retry same identities: %w",err) }
	return nil
}

func validWorkerLease(worker string,lease time.Duration) error {
	if !identifier(worker,128) || lease<time.Second || lease>24*time.Hour {
		return errors.New("worker ID and lease (1s..24h) are required")
	}
	return nil
}

func (s *DurableStore) Claim(ctx context.Context,worker string,lease time.Duration,limit int)([]Delivery,error) {
	if err:=s.requireDB();err!=nil { return nil,err }
	if ctx==nil { return nil,errors.New("context required") }
	if err:=validWorkerLease(worker,lease);err!=nil { return nil,err }
	if limit<1 || limit>100 { return nil,errors.New("durable claim limit must be 1..100") }
	ms:=lease.Milliseconds();if lease%time.Millisecond!=0 { ms++ }
	rows,err:=s.DB.QueryContext(ctx,`WITH picked AS (
 SELECT observation_id FROM genai_observations
 WHERE terminal_at IS NULL AND available_at<=now()
   AND (lease_until IS NULL OR lease_until<=now())
 ORDER BY available_at,observation_id
 FOR UPDATE SKIP LOCKED LIMIT $1
)
UPDATE genai_observations AS o
SET lease_owner=$2,lease_until=now()+($3::bigint*interval '1 millisecond'),
    attempts=o.attempts+1,last_error=NULL
FROM picked WHERE o.observation_id=picked.observation_id
RETURNING o.observation_id,o.schema_version,o.tenant_id,o.trace_id,o.span_id,
 o.request_id,o.attempt_id,o.provider,o.operation,o.request_model,o.response_model,
 o.status,o.input_tokens,o.output_tokens,o.attempts,o.lease_until`,
		limit,worker,ms)
	if err!=nil { return nil,fmt.Errorf("claim durable observations: %w",err) }
	defer rows.Close()
	out:=make([]Delivery,0,limit)
	for rows.Next() {
		d,scanErr:=scanDelivery(rows);if scanErr!=nil { return nil,scanErr }
		out=append(out,d)
	}
	if err=rows.Err();err!=nil { return nil,err }
	return out,nil
}

type rowScanner interface { Scan(...any) error }

func scanDelivery(row rowScanner)(Delivery,error) {
	var d Delivery
	var input,output sql.NullInt64
	err:=row.Scan(&d.ID,&d.Observation.SchemaVersion,&d.Observation.TenantID,&d.Observation.TraceID,&d.Observation.SpanID,
		&d.Observation.RequestID,&d.Observation.AttemptID,&d.Observation.Provider,&d.Observation.Operation,
		&d.Observation.RequestModel,&d.Observation.ResponseModel,&d.Observation.Status,&input,&output,&d.Attempts,&d.LeaseUntil)
	if err!=nil { return Delivery{},err }
	if input.Valid { v:=input.Int64;d.Observation.InputTokens=&v }
	if output.Valid { v:=output.Int64;d.Observation.OutputTokens=&v }
	return d,nil
}

func (s *DurableStore) Ack(ctx context.Context,d Delivery,worker string)error {
	if err:=s.requireDB();err!=nil { return err }
	if ctx==nil { return errors.New("context required") }
	if d.ID<1 || d.Attempts<1 || !identifier(worker,128) { return ErrDurableLeaseLost }
	tx,err:=s.DB.BeginTx(ctx,nil);if err!=nil { return err }
	defer tx.Rollback()
	// Keep the same state-row -> observation-row lock order as EnqueueBatch.
	var queueItems int64
	if err=tx.QueryRowContext(ctx,"SELECT queue_items FROM genai_observation_queue_state WHERE singleton=true FOR UPDATE").Scan(&queueItems);err!=nil {
		return fmt.Errorf("lock durable queue counter for ACK: %w",err)
	}
	result,err:=tx.ExecContext(ctx,`DELETE FROM genai_observations
WHERE observation_id=$1 AND lease_owner=$2 AND attempts=$3 AND lease_until>now() AND terminal_at IS NULL`,d.ID,worker,d.Attempts)
	if err!=nil { return fmt.Errorf("ack durable observation: %w",err) }
	n,err:=result.RowsAffected();if err!=nil { return err };if n!=1 { return ErrDurableLeaseLost }
	result,err=tx.ExecContext(ctx,"UPDATE genai_observation_queue_state SET queue_items=queue_items-1 WHERE singleton=true AND queue_items>0")
	if err!=nil { return fmt.Errorf("decrement durable queue counter: %w",err) }
	n,err=result.RowsAffected();if err!=nil { return err };if n!=1 { return errors.New("durable observation queue counter is inconsistent") }
	if err=tx.Commit();err!=nil { return fmt.Errorf("commit durable observation ACK: %w",err) }
	return nil
}

func safeFailureCode(reason string) string {
	if len(reason)==0 || len(reason)>128 || !identifier(reason,128) { return "unspecified" }
	return reason
}

func (s *DurableStore) Retry(ctx context.Context,d Delivery,worker string,delay time.Duration,reason string)error {
	if err:=s.requireDB();err!=nil { return err }
	if ctx==nil { return errors.New("context required") }
	if d.ID<1 || d.Attempts<1 || !identifier(worker,128) || delay<0 || delay>24*time.Hour {
		return errors.New("invalid durable observation retry")
	}
	ms:=delay.Milliseconds();if delay%time.Millisecond!=0 { ms++ }
	result,err:=s.DB.ExecContext(ctx,`UPDATE genai_observations
SET available_at=now()+($4::bigint*interval '1 millisecond'),lease_owner=NULL,lease_until=NULL,last_error=$5
WHERE observation_id=$1 AND lease_owner=$2 AND attempts=$3 AND lease_until>now() AND terminal_at IS NULL`,
		d.ID,worker,d.Attempts,ms,safeFailureCode(reason))
	if err!=nil { return fmt.Errorf("schedule durable observation retry: %w",err) }
	n,err:=result.RowsAffected();if err!=nil { return err };if n!=1 { return ErrDurableLeaseLost }
	return nil
}

func (s *DurableStore) MarkTerminal(ctx context.Context,d Delivery,worker,reason string)error {
	if err:=s.requireDB();err!=nil { return err }
	if ctx==nil { return errors.New("context required") }
	if d.ID<1 || d.Attempts<1 || !identifier(worker,128) { return ErrDurableLeaseLost }
	result,err:=s.DB.ExecContext(ctx,`UPDATE genai_observations
SET terminal_at=now(),terminal_reason=$4,lease_owner=NULL,lease_until=NULL,last_error=NULL
WHERE observation_id=$1 AND lease_owner=$2 AND attempts=$3 AND lease_until>now() AND terminal_at IS NULL`,
		d.ID,worker,d.Attempts,safeFailureCode(reason))
	if err!=nil { return fmt.Errorf("mark durable observation terminal: %w",err) }
	n,err:=result.RowsAffected();if err!=nil { return err };if n!=1 { return ErrDurableLeaseLost }
	return nil
}

func (s *DurableStore) Release(ctx context.Context,d Delivery,worker string)error {
	if err:=s.requireDB();err!=nil { return err }
	if ctx==nil { return errors.New("context required") }
	if d.ID<1 || d.Attempts<1 || !identifier(worker,128) { return ErrDurableLeaseLost }
	result,err:=s.DB.ExecContext(ctx,`UPDATE genai_observations
SET lease_owner=NULL,lease_until=NULL,available_at=now()
WHERE observation_id=$1 AND lease_owner=$2 AND attempts=$3 AND lease_until>now() AND terminal_at IS NULL`,
		d.ID,worker,d.Attempts)
	if err!=nil { return fmt.Errorf("release durable observation lease: %w",err) }
	n,err:=result.RowsAffected();if err!=nil { return err };if n!=1 { return ErrDurableLeaseLost }
	return nil
}

func (s *DurableStore) RetryFailed(ctx context.Context,id int64)error {
	if err:=s.requireDB();err!=nil { return err }
	if ctx==nil || id<1 { return ErrNoFailedObservation }
	result,err:=s.DB.ExecContext(ctx,`UPDATE genai_observations
SET terminal_at=NULL,terminal_reason=NULL,attempts=0,available_at=now(),last_error=NULL,lease_owner=NULL,lease_until=NULL
WHERE observation_id=$1 AND terminal_at IS NOT NULL`,id)
	if err!=nil { return fmt.Errorf("re-arm terminal observation: %w",err) }
	n,err:=result.RowsAffected();if err!=nil { return err };if n!=1 { return ErrNoFailedObservation }
	return nil
}

func (s *DurableStore) DropFailed(ctx context.Context,id int64)(Observation,error) {
	if err:=s.requireDB();err!=nil { return Observation{},err }
	if ctx==nil || id<1 { return Observation{},ErrNoFailedObservation }
	tx,err:=s.DB.BeginTx(ctx,nil);if err!=nil { return Observation{},err }
	defer tx.Rollback()
	// Keep the same state-row -> observation-row lock order as EnqueueBatch.
	var queueItems int64
	if err=tx.QueryRowContext(ctx,"SELECT queue_items FROM genai_observation_queue_state WHERE singleton=true FOR UPDATE").Scan(&queueItems);err!=nil {
		return Observation{},fmt.Errorf("lock durable queue counter for terminal drop: %w",err)
	}
	d,err:=scanDelivery(tx.QueryRowContext(ctx,`DELETE FROM genai_observations
WHERE observation_id=$1 AND terminal_at IS NOT NULL
RETURNING observation_id,schema_version,tenant_id,trace_id,span_id,request_id,attempt_id,
 provider,operation,request_model,response_model,status,input_tokens,output_tokens,attempts,now()`,id))
	if errors.Is(err,sql.ErrNoRows) { return Observation{},ErrNoFailedObservation }
	if err!=nil { return Observation{},fmt.Errorf("drop terminal observation: %w",err) }
	result,err:=tx.ExecContext(ctx,"UPDATE genai_observation_queue_state SET queue_items=queue_items-1 WHERE singleton=true AND queue_items>0")
	if err!=nil { return Observation{},fmt.Errorf("decrement durable queue counter: %w",err) }
	n,err:=result.RowsAffected();if err!=nil { return Observation{},err };if n!=1 { return Observation{},errors.New("durable observation queue counter is inconsistent") }
	if err=tx.Commit();err!=nil { return Observation{},fmt.Errorf("commit terminal observation drop: %w",err) }
	return d.Observation,nil
}

func (s *DurableStore) Len(ctx context.Context)(int,error) {
	if err:=s.requireDB();err!=nil { return 0,err }
	if ctx==nil { return 0,errors.New("context required") }
	var n int
	if err:=s.DB.QueryRowContext(ctx,"SELECT count(*) FROM genai_observations").Scan(&n);err!=nil {
		return 0,fmt.Errorf("count durable observations: %w",err)
	}
	return n,nil
}

type TerminalDelivery struct {
	ID int64
	Observation Observation
	Attempts int
	Reason string
	FailedAt time.Time
}

// ListTerminal returns operator-actionable failures for one tenant only.
func (s *DurableStore) ListTerminal(ctx context.Context,tenant string,limit int)([]TerminalDelivery,error) {
	if err:=s.requireDB();err!=nil { return nil,err }
	if ctx==nil || !identifier(tenant,128) { return nil,errors.New("valid tenant and context required") }
	if limit<1 || limit>100 { return nil,errors.New("terminal list limit must be 1..100") }
	rows,err:=s.DB.QueryContext(ctx,`SELECT observation_id,schema_version,tenant_id,trace_id,span_id,
request_id,attempt_id,provider,operation,request_model,response_model,status,input_tokens,output_tokens,
attempts,terminal_reason,terminal_at
FROM genai_observations WHERE tenant_id=$1 AND terminal_at IS NOT NULL
ORDER BY terminal_at,observation_id LIMIT $2`,tenant,limit)
	if err!=nil { return nil,fmt.Errorf("list terminal observations: %w",err) }
	defer rows.Close()
	out:=make([]TerminalDelivery,0,limit)
	for rows.Next() {
		var d TerminalDelivery
		var input,output sql.NullInt64
		if err:=rows.Scan(&d.ID,&d.Observation.SchemaVersion,&d.Observation.TenantID,&d.Observation.TraceID,&d.Observation.SpanID,
			&d.Observation.RequestID,&d.Observation.AttemptID,&d.Observation.Provider,&d.Observation.Operation,
			&d.Observation.RequestModel,&d.Observation.ResponseModel,&d.Observation.Status,&input,&output,
			&d.Attempts,&d.Reason,&d.FailedAt);err!=nil { return nil,err }
		if input.Valid { v:=input.Int64;d.Observation.InputTokens=&v }
		if output.Valid { v:=output.Int64;d.Observation.OutputTokens=&v }
		out=append(out,d)
	}
	if err:=rows.Err();err!=nil { return nil,err }
	return out,nil
}
