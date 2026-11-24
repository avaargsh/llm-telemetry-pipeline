package genaiobs

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func batchSpan(id string) Span {
	return Span{TraceID: "0123456789abcdef0123456789abcdef", SpanID: "0123456789abcdef", RequestID: id, Attributes: []Attribute{{Key: "gen_ai.provider.name", Value: "openai"}, {Key: "gen_ai.operation.name", Value: "chat"}}}
}

func TestBatchAtomicAdmission(t *testing.T) {
	q, err := NewQueue(3)
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{TenantID: "trusted"}
	if err := q.Submit(scope, Version, batchSpan("seed")); err != nil {
		t.Fatal(err)
	}
	bad := batchSpan("bad")
	bad.Attributes = nil
	if err := q.SubmitBatch(scope, Version, []Span{batchSpan("valid"), bad}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid batch: %v", err)
	}
	if q.Len() != 1 {
		t.Fatal("partial invalid batch admitted")
	}
	if err := q.SubmitBatch(scope, Version, []Span{batchSpan("a"), batchSpan("b"), batchSpan("c")}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("overload: %v", err)
	}
	if q.Len() != 1 {
		t.Fatal("partial overloaded batch admitted")
	}
	if err := q.SubmitBatch(scope, Version, []Span{batchSpan("a"), batchSpan("b")}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"seed", "a", "b"} {
		o, ok := q.Pop()
		if !ok || o.RequestID != want || o.TenantID != "trusted" {
			t.Fatalf("want %s: %+v", want, o)
		}
	}
	if q.Len() != 0 {
		t.Fatal("not drained")
	}
}

func TestBatchBounds(t *testing.T) {
	q, err := NewQueue(MaxQueueCapacity)
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{TenantID: "trusted"}
	for _, spans := range [][]Span{nil, make([]Span, MaxBatchSize+1)} {
		if err := q.SubmitBatch(scope, Version, spans); !errors.Is(err, ErrInvalidBatchSize) {
			t.Fatalf("size: %v", err)
		}
	}
	if err := q.SubmitBatch(scope, "unsupported", []Span{batchSpan("a")}); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("version: %v", err)
	}
	if q.Len() != 0 {
		t.Fatal("invalid retained")
	}
}

func TestConcurrentBatchesRemainWhole(t *testing.T) {
	q, err := NewQueue(31)
	if err != nil {
		t.Fatal(err)
	}
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprint(i)
			err := q.SubmitBatch(Scope{TenantID: "trusted"}, Version, []Span{batchSpan(id + "a"), batchSpan(id + "b")})
			if err == nil {
				admitted.Add(1)
			} else if !errors.Is(err, ErrQueueFull) {
				t.Errorf("unexpected: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if admitted.Load() != 15 || q.Len() != 30 {
		t.Fatalf("admitted %d len %d", admitted.Load(), q.Len())
	}
	for i := 0; i < 15; i++ {
		a, okA := q.Pop()
		b, okB := q.Pop()
		if !okA || !okB || len(a.RequestID) < 2 || a.RequestID[len(a.RequestID)-1:] != "a" || b.RequestID != a.RequestID[:len(a.RequestID)-1]+"b" {
			t.Fatalf("split batch: %s %s", a.RequestID, b.RequestID)
		}
	}
	if q.Len() != 0 {
		t.Fatal("not drained")
	}
}
