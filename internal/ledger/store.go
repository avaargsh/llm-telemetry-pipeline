// Package ledger owns authoritative finalized usage accounting.
// ClickHouse raw event rows are not a billing source.
package ledger

import (
    "context"
    "database/sql"
    "encoding/json"
    "errors"
    "fmt"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
)

var ErrConflict = errors.New("usage identity conflicts with an existing event")
type Outcome string
const (
    Created Outcome = "created"
    Duplicate Outcome = "duplicate"
)
type Store struct{ DB *sql.DB }

// Record atomically writes a finalized usage record and its durable Kafka
// outbox message. Retrying the same event_id and payload_hash is a no-op.
// An ambiguous database Commit response must be retried with the SAME event.
func(s *Store)Record(ctx context.Context,e event.UsageEvent)(Outcome,error){
    if s==nil||s.DB==nil{return "",errors.New("ledger database is required")}
    if err:=e.Validate();err!=nil{return "",fmt.Errorf("invalid usage event: %w",err)}
    payload,err:=json.Marshal(e);if err!=nil{return "",err}
    tx,err:=s.DB.BeginTx(ctx,nil);if err!=nil{return "",err}
    defer tx.Rollback()
    result,err:=tx.ExecContext(ctx,`
INSERT INTO usage_ledger
(event_id,tenant_id,project_id,request_id,event_type,schema_version,model,prompt_tokens,completion_tokens,payload_hash,event_time)
VALUES ($1::uuid,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT DO NOTHING
`,e.EventID,e.TenantID,e.ProjectID,e.RequestID,e.EventType,e.SchemaVersion,e.Model,e.PromptTokens,e.CompletionTokens,e.PayloadHash,e.Timestamp)
    if err!=nil{return "",fmt.Errorf("insert ledger: %w",err)}
    affected,err:=result.RowsAffected();if err!=nil{return "",err}
    if affected==1{
        if _,err:=tx.ExecContext(ctx,`INSERT INTO event_outbox(event_id,event_payload,topic)
VALUES ($1::uuid,$2::jsonb,'usage.critical.v1')`,e.EventID,string(payload));err!=nil{
            return "",fmt.Errorf("insert transactional outbox: %w",err)
        }
        if err:=tx.Commit();err!=nil{return "",fmt.Errorf("commit uncertain; retry same event_id: %w",err)}
        return Created,nil
    }
    if affected!=0{return "",fmt.Errorf("unexpected insert count %d",affected)}
    rows,err:=tx.QueryContext(ctx,`SELECT event_id::text, payload_hash FROM usage_ledger
WHERE event_id=$1::uuid OR (tenant_id=$2 AND request_id=$3 AND event_type=$4)
FOR SHARE`,e.EventID,e.TenantID,e.RequestID,e.EventType)
    if err!=nil{return "",err}
    count:=0
    same:=true
    for rows.Next(){
        var eventID,hash string
        if err:=rows.Scan(&eventID,&hash);err!=nil{rows.Close();return "",err}
        count++
        if eventID!=e.EventID||hash!=e.PayloadHash {same=false}
    }
    rowErr:=rows.Err()
    rows.Close()
    if rowErr!=nil{return "",rowErr}
    if count!=1||!same{return "",ErrConflict}
    if err:=tx.Commit();err!=nil{return "",fmt.Errorf("commit duplicate check: %w",err)}
    return Duplicate,nil
}
