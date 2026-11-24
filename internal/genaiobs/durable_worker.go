package genaiobs

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DurableExporter receives a stable delivery ID so receivers can deduplicate
// at-least-once retries. The receiver must actually persist that key for the
// deduplication guarantee to hold.
type DurableExporter interface {
	Export(context.Context,Delivery) error
}

// DurableWorker has no process-local retry state. Every attempt and lease is
// claimed from PostgreSQL, so another process can recover after lease expiry.
type DurableWorker struct {
	repo DurableRepository
	exporter DurableExporter
	workerID string
	lease time.Duration
	policy RetryPolicy
	now func() time.Time
}

func NewDurableWorker(repo DurableRepository,exporter DurableExporter,workerID string,lease time.Duration,policy RetryPolicy)(*DurableWorker,error) {
	if repo==nil || exporter==nil { return nil,errors.New("durable repository and exporter required") }
	if err:=validWorkerLease(workerID,lease);err!=nil { return nil,err }
	if err:=policy.validate();err!=nil { return nil,err }
	return &DurableWorker{repo:repo,exporter:exporter,workerID:workerID,lease:lease,policy:policy,now:time.Now},nil
}

// RunOnce claims at most one row. A nil exporter error is the receiver ACK;
// a failed SQL ACK leaves the row recoverable after lease expiry and may replay.
func (w *DurableWorker) RunOnce(ctx context.Context)(bool,error) {
	if ctx==nil { return false,errors.New("context required") }
	if err:=ctx.Err();err!=nil { return false,err }
	rows,err:=w.repo.Claim(ctx,w.workerID,w.lease,1)
	if err!=nil { return false,err }
	if len(rows)==0 { return false,nil }
	d:=rows[0]
	if err:=ctx.Err();err!=nil {
		releaseCtx,cancel:=context.WithTimeout(context.Background(),2*time.Second)
		defer cancel()
		_ = w.repo.Release(releaseCtx,d,w.workerID)
		return false,err
	}
	err=w.exporter.Export(ctx,Delivery{ID:d.ID,Observation:cloneObservation(d.Observation),Attempts:d.Attempts,LeaseUntil:d.LeaseUntil})
	if err==nil {
		if ackErr:=w.repo.Ack(ctx,d,w.workerID);ackErr!=nil {
			return false,fmt.Errorf("receiver ACKed but durable queue ACK failed; delivery may replay: %w",ackErr)
		}
		return true,nil
	}
	if ctx.Err()!=nil {
		releaseCtx,cancel:=context.WithTimeout(context.Background(),2*time.Second)
		defer cancel()
		_ = w.repo.Release(releaseCtx,d,w.workerID)
		return false,ctx.Err()
	}
	var permanent permanentExporterError
	if errors.As(err,&permanent) {
		if markErr:=w.repo.MarkTerminal(ctx,d,w.workerID,"permanent_export_failure");markErr!=nil {
			return false,errors.Join(ErrPermanentFailure,markErr)
		}
		return false,ErrPermanentFailure
	}
	if d.Attempts>=w.policy.MaxAttempts {
		if markErr:=w.repo.MarkTerminal(ctx,d,w.workerID,"retry_exhausted");markErr!=nil {
			return false,errors.Join(ErrRetryExhausted,markErr)
		}
		return false,ErrRetryExhausted
	}
	delay:=retryDelay(d.Observation,d.Attempts,w.policy)
	if retryErr:=w.repo.Retry(ctx,d,w.workerID,delay,"export_failed");retryErr!=nil {
		return false,fmt.Errorf("schedule durable observation retry: %w",retryErr)
	}
	return false,&RetryError{kind:ErrRetryScheduled,attempt:d.Attempts,next:w.now().Add(delay)}
}
