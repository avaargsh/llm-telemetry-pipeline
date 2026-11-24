package genaiobs

import (
	"context"
	"errors"
)

// ObservationAdmitter accepts a fully decoded batch under one trusted scope.
// Implementations must admit all observations or none.
type ObservationAdmitter interface {
	AdmitBatch(context.Context,Scope,string,[]Span) error
	MaxBatchSize() int
}

// IdempotentObservationAdmitter persists a receiver-side delivery key in the
// same transaction as admission. A key identifies exactly one observation.
type IdempotentObservationAdmitter interface {
	AdmitBatchWithIdempotencyKey(context.Context,Scope,string,[]Span,string) error
}

func (q *Queue) MaxBatchSize() int {
	if q==nil { return 0 }
	n:=len(q.slots)
	if n>MaxBatchSize { n=MaxBatchSize }
	return n
}

func (q *Queue) AdmitBatch(ctx context.Context,scope Scope,version string,spans []Span) error {
	if q==nil { return ErrInvalidCapacity }
	if ctx==nil { return errors.New("context required") }
	if err:=ctx.Err();err!=nil { return err }
	return q.SubmitBatch(scope,version,spans)
}

func (s *DurableStore) MaxBatchSize() int {
	if s==nil || s.Capacity<1 { return 0 }
	n:=s.Capacity
	if n>MaxBatchSize { n=MaxBatchSize }
	return n
}

// AdmitBatch projects the complete OTLP batch before opening the durable
// transaction, then commits it atomically with the PostgreSQL capacity check.
func (s *DurableStore) AdmitBatch(ctx context.Context,scope Scope,version string,spans []Span) error {
	if len(spans)==0 || len(spans)>s.MaxBatchSize() { return ErrInvalidBatchSize }
	if ctx==nil { return errors.New("context required") }
	if err:=ctx.Err();err!=nil { return err }
	observations:=make([]Observation,len(spans))
	for i,span:=range spans {
		o,err:=NormalizeInference(scope,version,span)
		if err!=nil { return err }
		observations[i]=o
	}
	return s.EnqueueBatch(ctx,observations)
}

 
// AdmitBatchWithIdempotencyKey projects one observation under the authenticated
// scope and persists the delivery key with the durable admission transaction.
func (s *DurableStore) AdmitBatchWithIdempotencyKey(ctx context.Context,scope Scope,version string,spans []Span,key string) error {
	if s==nil || len(spans)!=1 || len(spans)>s.MaxBatchSize() { return ErrInvalidBatchSize }
	if !validIdempotencyKey(key) { return ErrInvalidIdempotencyKey }
	if ctx==nil { return errors.New("context required") }
	if err:=ctx.Err();err!=nil { return err }
	o,err:=NormalizeInference(scope,version,spans[0])
	if err!=nil { return err }
	return s.EnqueueWithIdempotencyKey(ctx,o,key)
}
