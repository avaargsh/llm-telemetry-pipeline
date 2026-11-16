package reconcile

import (
    "context"
    "database/sql"
    "fmt"
    "time"
)

// PostgreSQL queries the authoritative ledger without mutating business state.
type PostgreSQL struct{DB *sql.DB}

func(p PostgreSQL) ListLedger(ctx context.Context,start,end time.Time,max int)([]LedgerRecord,error){
    if p.DB==nil{return nil,fmt.Errorf("nil PostgreSQL connection")}
    rows,err:=p.DB.QueryContext(ctx,`
SELECT l.event_id::text,l.tenant_id,l.project_id,l.request_id,l.model,
       l.payload_hash::text,l.prompt_tokens,l.completion_tokens,
       (o.outbox_id IS NOT NULL) AS has_outbox,o.published_at
FROM usage_ledger l
LEFT JOIN event_outbox o ON o.event_id=l.event_id
WHERE l.event_time >= $1 AND l.event_time < $2
ORDER BY l.event_id
LIMIT $3
`,start.UTC(),end.UTC(),max+1)
    if err!=nil{return nil,err}
    defer rows.Close()
    result:=make([]LedgerRecord,0)
    for rows.Next(){
        var l LedgerRecord
        var published sql.NullTime
        if err:=rows.Scan(&l.EventID,&l.TenantID,&l.ProjectID,&l.RequestID,
            &l.Model,&l.PayloadHash,&l.PromptTokens,&l.CompletionTokens,
            &l.HasOutbox,&published);err!=nil{return nil,err}
        if published.Valid {at:=published.Time.UTC();l.PublishedAt=&at}
        result=append(result,l)
        if len(result)>max{return nil,fmt.Errorf("PostgreSQL ledger exceeds max-records limit %d",max)}
    }
    if err:=rows.Err();err!=nil{return nil,err}
    return result,nil
}
