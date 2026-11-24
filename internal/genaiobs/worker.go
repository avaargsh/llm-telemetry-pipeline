package genaiobs

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"
)

var (
	ErrInvalidRetryPolicy = errors.New("invalid observation retry policy")
	ErrRetryScheduled = errors.New("observation retry scheduled")
	ErrBackoffActive = errors.New("observation retry backoff active")
	ErrRetryExhausted = errors.New("observation retry attempts exhausted")
	ErrPermanentFailure = errors.New("permanent observation export failure")
	ErrNoFailedObservation = errors.New("no terminal observation to drop or retry")
)

type Exporter interface {
	// Export returns nil only after the downstream receiver acknowledges the
	// complete observation. An error may have an ambiguous remote outcome.
	Export(context.Context, Observation) error
}

type RetryPolicy struct {
	// MaxAttempts includes the initial export attempt.
	MaxAttempts int
	InitialBackoff time.Duration
	MaxBackoff time.Duration
	// Jitter is a deterministic per-observation fraction in [0, 0.5].
	Jitter float64
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts:5,InitialBackoff:250*time.Millisecond,MaxBackoff:10*time.Second,Jitter:0.2}
}

func (p RetryPolicy) validate() error {
	if p.MaxAttempts<1 || p.MaxAttempts>20 || p.InitialBackoff<time.Millisecond ||
		p.MaxBackoff<p.InitialBackoff || p.MaxBackoff>time.Hour || p.Jitter<0 || p.Jitter>0.5 || math.IsNaN(p.Jitter) || math.IsInf(p.Jitter,0) {
		return ErrInvalidRetryPolicy
	}
	return nil
}

type permanentExporterError struct{ cause error }
func (e permanentExporterError) Error() string { return "permanent exporter failure" }
func (e permanentExporterError) Unwrap() error { return e.cause }

// Permanent marks an exporter error as non-retryable. The wrapped error is
// intentionally not included in Worker errors to avoid leaking remote details.
func Permanent(err error) error {
	if err==nil { return nil }
	return permanentExporterError{cause:err}
}

type RetryError struct {
	kind error
	attempt int
	next time.Time
}
func (e *RetryError) Error() string {
	if e.next.IsZero() { return fmt.Sprintf("%s after attempt %d",e.kind,e.attempt) }
	return fmt.Sprintf("%s after attempt %d; next attempt at %s",e.kind,e.attempt,e.next.UTC().Format(time.RFC3339Nano))
}
func (e *RetryError) Unwrap() error { return e.kind }
func (e *RetryError) Attempt() int { return e.attempt }
func (e *RetryError) NextAttempt() time.Time { return e.next }

// Worker exports one queue head at a time. The head remains counted against
// queue capacity until Export returns an ACK and RunOnce removes it. Retry
// schedule and lease are process-local; process restart loses the entire queue.
type Worker struct {
	mu sync.Mutex
	queue *Queue
	exporter Exporter
	policy RetryPolicy
	now func() time.Time
	lease uint64
	pending Observation
	attempts int
	nextAttempt time.Time
	terminal error
}

func NewWorker(q *Queue, exporter Exporter, policy RetryPolicy) (*Worker,error) {
	if q==nil || len(q.slots)==0 || exporter==nil { return nil,errors.New("observation queue and exporter required") }
	if err:=policy.validate();err!=nil { return nil,err }
	return &Worker{queue:q,exporter:exporter,policy:policy,now:time.Now},nil
}

// RunOnce performs at most one export attempt. The caller owns polling and
// should wait until RetryError.NextAttempt when ErrRetryScheduled is returned.
func (w *Worker) RunOnce(ctx context.Context) (bool,error) {
	if ctx==nil { return false,errors.New("context required") }
	w.mu.Lock()
	defer w.mu.Unlock()
	if err:=ctx.Err();err!=nil { return false,err }
	if w.lease==0 {
		o,token,ok:=w.queue.leaseHead()
		if !ok { return false,nil }
		w.pending=o
		w.lease=token
		w.attempts=0
		w.nextAttempt=time.Time{}
		w.terminal=nil
	}
	if w.terminal!=nil { return false,w.terminal }
	if err:=ctx.Err();err!=nil { return false,err }
	now:=w.now()
	if now.Before(w.nextAttempt) {
		return false,&RetryError{kind:ErrBackoffActive,attempt:w.attempts,next:w.nextAttempt}
	}
	w.attempts++
	err:=w.exporter.Export(ctx,cloneObservation(w.pending))
	if err==nil {
		if err:=w.queue.ackLease(w.lease);err!=nil { return false,err }
		w.resetLocked()
		return true,nil
	}
	var permanent permanentExporterError
	if errors.As(err,&permanent) {
		w.terminal=ErrPermanentFailure
		return false,w.terminal
	}
	if w.attempts>=w.policy.MaxAttempts {
		w.terminal=ErrRetryExhausted
		return false,w.terminal
	}
	w.nextAttempt=now.Add(retryDelay(w.pending,w.attempts,w.policy))
	return false,&RetryError{kind:ErrRetryScheduled,attempt:w.attempts,next:w.nextAttempt}
}

// RetryFailed explicitly re-arms a terminal observation. It never drops it.
func (w *Worker) RetryFailed() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.lease==0 || w.terminal==nil { return ErrNoFailedObservation }
	w.attempts=0
	w.nextAttempt=time.Time{}
	w.terminal=nil
	return nil
}

// DropFailed explicitly removes a terminal observation. The caller receives
// the normalized record and is responsible for any external audit/dead-letter.
func (w *Worker) DropFailed() (Observation,error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.lease==0 || w.terminal==nil { return Observation{},ErrNoFailedObservation }
	o:=cloneObservation(w.pending)
	if err:=w.queue.ackLease(w.lease);err!=nil { return Observation{},err }
	w.resetLocked()
	return o,nil
}

// Close releases an in-flight lease without removing its observation. A new
// Worker may resume it; retry counters are process-local and will reset.
func (w *Worker) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.lease==0 { return nil }
	if err:=w.queue.releaseLease(w.lease);err!=nil { return err }
	w.resetLocked()
	return nil
}

func (w *Worker) resetLocked() {
	w.lease=0
	w.pending=Observation{}
	w.attempts=0
	w.nextAttempt=time.Time{}
	w.terminal=nil
}

func cloneObservation(o Observation) Observation {
	if o.InputTokens!=nil { v:=*o.InputTokens; o.InputTokens=&v }
	if o.OutputTokens!=nil { v:=*o.OutputTokens; o.OutputTokens=&v }
	return o
}

func retryDelay(o Observation, attempt int, p RetryPolicy) time.Duration {
	if attempt<1 { attempt=1 }
	delay:=p.InitialBackoff
	for i:=1;i<attempt && delay<p.MaxBackoff;i++ {
		if delay>p.MaxBackoff/2 { delay=p.MaxBackoff; break }
		delay*=2
	}
	if delay>p.MaxBackoff { delay=p.MaxBackoff }
	if p.Jitter==0 { return delay }
	sum:=sha256.Sum256([]byte(o.TraceID+"\x00"+o.SpanID+"\x00"+strconv.Itoa(attempt)))
	unit:=float64(binary.BigEndian.Uint64(sum[:8]))/float64(^uint64(0))
	factor:=1-p.Jitter+(2*p.Jitter*unit)
	jittered:=time.Duration(float64(delay)*factor)
	if jittered<time.Millisecond { jittered=time.Millisecond }
	if jittered>p.MaxBackoff { jittered=p.MaxBackoff }
	return jittered
}
