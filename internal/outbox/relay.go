package outbox

import (
    "context"
    "errors"
    "fmt"
    "time"
)
type Repository interface{
    Claim(context.Context,string,time.Duration,int)([]Row,error)
    MarkPublished(context.Context,Row,string)error
    ScheduleRetry(context.Context,Row,string,time.Duration,string)error
}
type Publisher interface{Publish(context.Context,Row)error}
type Relay struct{
    Repo Repository
    Publisher Publisher
    WorkerID string
    Lease time.Duration
    Limit int
}

// RunOnce sends each leased record and marks it delivered only after its
// Kafka ACK. Unknown ACKs and a crash between ACK/mark can yield duplicates.
func(r *Relay)RunOnce(ctx context.Context)(int,error){
    if r==nil||r.Repo==nil||r.Publisher==nil{return 0,errors.New("relay dependencies required")}
    if err:=requireLease(r.WorkerID,r.Lease);err!=nil{return 0,err}
    if r.Limit<1||r.Limit>500{return 0,errors.New("invalid relay claim limit")}
    rows,err:=r.Repo.Claim(ctx,r.WorkerID,r.Lease,r.Limit)
    if err!=nil{return 0,err}
    published:=0
    for _,row:=range rows{
        if ctx.Err()!=nil{return published,ctx.Err()}
        if err:=r.Publisher.Publish(ctx,row);err!=nil{
            retryErr:=r.Repo.ScheduleRetry(ctx,row,r.WorkerID,Backoff(row.Attempts),err.Error())
            if retryErr!=nil{return published,errors.Join(fmt.Errorf("Kafka ACK unknown for outbox %d: %w",row.ID,err),retryErr)}
            return published,fmt.Errorf("Kafka ACK unknown for outbox %d; retry scheduled: %w",row.ID,err)
        }
        if err:=r.Repo.MarkPublished(ctx,row,r.WorkerID);err!=nil{
            return published,fmt.Errorf("Kafka ACKed but outbox %d not marked (may replay): %w",row.ID,err)
        }
        published++
    }
    return published,nil
}
func Backoff(attempt int)time.Duration{
    if attempt<1 {attempt=1}
    if attempt>6 {attempt=6}
    return time.Duration(1<<uint(attempt-1))*time.Second
}
