package genaiobs

import (
	"errors"
	"strings"
	"sync"
)

const MaxQueueCapacity = 4096

var (
	ErrInvalidCapacity = errors.New("invalid observation queue capacity")
	ErrQueueFull       = errors.New("observation queue full")
	ErrQueueLeaseLost  = errors.New("observation queue lease lost")
)

// Queue is a bounded, volatile FIFO of normalized observations. Construct it
// with NewQueue; do not copy after use. It owns no storage or billing authority.
type Queue struct {
	mu sync.Mutex
	slots []Observation
	head, size int
	leaseGeneration uint64
	leaseToken uint64
	leased bool
}

func NewQueue(capacity int) (*Queue, error) {
	if capacity <= 0 || capacity > MaxQueueCapacity {
		return nil, ErrInvalidCapacity
	}
	return &Queue{slots: make([]Observation, capacity)}, nil
}

// Submit validates before admission. A full queue rejects the new observation
// without evicting existing ones. The caller owns its input until return.
func (q *Queue) Submit(scope Scope, version string, span Span) error {
	o, err := NormalizeInference(scope, version, span)
	if err != nil { return err }
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size == len(q.slots) { return ErrQueueFull }
	q.appendLocked(o)
	return nil
}

// appendLocked requires q.mu and available capacity.
func (q *Queue) appendLocked(o Observation) {
	// A short string can alias a large input buffer. Copy retained fields so
	// the bounded projection cannot pin the caller's raw payload in memory.
	for _, s := range []*string{&o.TenantID,&o.TraceID,&o.SpanID,&o.RequestID,&o.AttemptID,&o.Provider,&o.Operation,&o.RequestModel,&o.ResponseModel,&o.Status} {
		*s = strings.Clone(*s)
	}
	q.slots[(q.head+q.size)%len(q.slots)] = o
	q.size++
}

// leaseHead reserves the FIFO head without removing it. Only one lease may be
// active per queue; capacity continues to include the leased observation.
func (q *Queue) leaseHead() (Observation,uint64,bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size==0 || q.leased { return Observation{},0,false }
	q.leaseGeneration++
	if q.leaseGeneration==0 { q.leaseGeneration++ }
	q.leaseToken=q.leaseGeneration
	q.leased=true
	return cloneObservation(q.slots[q.head]),q.leaseToken,true
}

// ackLease removes the head only after the exporter has confirmed delivery.
func (q *Queue) ackLease(token uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.leased || token==0 || token!=q.leaseToken { return ErrQueueLeaseLost }
	q.slots[q.head]=Observation{}
	q.head=(q.head+1)%len(q.slots)
	q.size--
	q.leased=false
	q.leaseToken=0
	return nil
}

// releaseLease relinquishes a reservation without dropping the observation.
func (q *Queue) releaseLease(token uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.leased || token==0 || token!=q.leaseToken { return ErrQueueLeaseLost }
	q.leased=false
	q.leaseToken=0
	return nil
}

// Pop transfers the FIFO head to the caller. It returns false when empty or
// while an exporter owns the head lease. Pop is destructive and is not an ACK.
func (q *Queue) Pop() (Observation,bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size==0 || q.leased { return Observation{},false }
	o:=q.slots[q.head]
	q.slots[q.head]=Observation{}
	q.head=(q.head+1)%len(q.slots)
	q.size--
	return o,true
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.size
}
