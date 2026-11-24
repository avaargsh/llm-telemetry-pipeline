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
)

// Queue is a bounded, volatile FIFO of normalized observations. Construct it
// with NewQueue; do not copy after use. It does not acknowledge durable delivery.
// It owns no workers, transport, storage, or billing authority.
type Queue struct {
	mu         sync.Mutex
	slots      []Observation
	head, size int
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
	if err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size == len(q.slots) {
		return ErrQueueFull
	}
	q.appendLocked(o)
	return nil
}

// appendLocked requires q.mu and available capacity.
func (q *Queue) appendLocked(o Observation) {
	// A short string can alias a large input buffer. Copy the retained fields
	// so the bounded projection cannot pin the caller's raw payload in memory.
	for _, s := range []*string{&o.TenantID, &o.TraceID, &o.SpanID, &o.RequestID, &o.AttemptID, &o.Provider, &o.Operation, &o.RequestModel, &o.ResponseModel, &o.Status} {
		*s = strings.Clone(*s)
	}
	q.slots[(q.head+q.size)%len(q.slots)] = o
	q.size++
}

// Pop transfers ownership to the caller. Empty returns false. It is not a
// delivery acknowledgment: the caller may lose the item after popping it.
func (q *Queue) Pop() (Observation, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size == 0 {
		return Observation{}, false
	}
	o := q.slots[q.head]
	q.slots[q.head] = Observation{}
	q.head = (q.head + 1) % len(q.slots)
	q.size--
	return o, true
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.size
}
