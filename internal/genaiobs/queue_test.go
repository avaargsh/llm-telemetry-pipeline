package genaiobs

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func queueSpan(id string) Span {
	return Span{TraceID: "0123456789abcdef0123456789abcdef", SpanID: "0123456789abcdef", RequestID: id, Attributes: []Attribute{{Key: "gen_ai.provider.name", Value: "openai"}, {Key: "gen_ai.operation.name", Value: "chat"}, {Key: "gen_ai.usage.input_tokens", Value: int64(0)}, {Key: "tenant_id", Value: "spoofed"}}}
}

func TestQueueCapacityAndFIFO(t *testing.T) {
	for _, n := range []int{-1, 0, MaxQueueCapacity + 1} {
		if _, err := NewQueue(n); !errors.Is(err, ErrInvalidCapacity) {
			t.Fatalf("capacity %d: %v", n, err)
		}
	}
	q, err := NewQueue(2)
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{TenantID: "trusted"}
	if err = q.Submit(scope, Version, queueSpan("first")); err != nil {
		t.Fatal(err)
	}
	if err = q.Submit(scope, Version, queueSpan("second")); err != nil {
		t.Fatal(err)
	}
	if err = q.Submit(scope, Version, queueSpan("third")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full: %v", err)
	}
	if q.Len() != 2 {
		t.Fatal("rejection changed queue")
	}
	first, ok := q.Pop()
	if !ok || first.RequestID != "first" || first.TenantID != "trusted" || first.InputTokens == nil || *first.InputTokens != 0 {
		t.Fatalf("first: %+v, %v", first, ok)
	}
	if err = q.Submit(scope, Version, queueSpan("third")); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"second", "third"} {
		o, ok := q.Pop()
		if !ok || o.RequestID != want {
			t.Fatalf("want %s: %+v, %v", want, o, ok)
		}
	}
	if _, ok := q.Pop(); ok || q.Len() != 0 {
		t.Fatal("queue not empty")
	}
}

func TestQueueRejectsBeforeRetentionAndPreservesReplay(t *testing.T) {
	q, err := NewQueue(3)
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{TenantID: "tenant-a"}
	span := queueSpan("request")
	if err = q.Submit(scope, "unsupported", span); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}
	bad := queueSpan("bad")
	bad.Attributes[2].Value = int64(-1)
	if err = q.Submit(scope, Version, bad); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if q.Len() != 0 {
		t.Fatal("invalid retained")
	}
	if err = q.Submit(scope, Version, span); err != nil {
		t.Fatal(err)
	}
	span.Attributes[2].Value = int64(99)
	if err = q.Submit(scope, Version, span); err != nil {
		t.Fatal(err)
	}
	if err = q.Submit(Scope{TenantID: "tenant-b"}, Version, span); err != nil {
		t.Fatal(err)
	}
	first, ok1 := q.Pop()
	second, ok2 := q.Pop()
	third, ok3 := q.Pop()
	if !ok1 || !ok2 || !ok3 || first.InputTokens == nil || second.InputTokens == nil || third.InputTokens == nil || first.TenantID != "tenant-a" || second.TenantID != "tenant-a" || third.TenantID != "tenant-b" || *first.InputTokens != 0 || *second.InputTokens != 99 || *third.InputTokens != 99 || first.RequestID != second.RequestID || second.RequestID != third.RequestID {
		t.Fatalf("unexpected projections: %+v %+v %+v", first, second, third)
	}
}

func TestQueueConcurrentAdmissionBound(t *testing.T) {
	const capacity = 32
	q, err := NewQueue(capacity)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var admitted, full atomic.Int64
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := q.Submit(Scope{TenantID: "tenant"}, Version, queueSpan(fmt.Sprint(i)))
			switch {
			case err == nil:
				admitted.Add(1)
			case errors.Is(err, ErrQueueFull):
				full.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if admitted.Load() != capacity || full.Load() != 128-capacity || q.Len() != capacity {
		t.Fatalf("admitted %d full %d len %d", admitted.Load(), full.Load(), q.Len())
	}
	seen := map[string]bool{}
	var mu sync.Mutex
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				o, ok := q.Pop()
				if !ok {
					return
				}
				mu.Lock()
				if seen[o.RequestID] {
					t.Errorf("duplicate pop %s", o.RequestID)
				}
				seen[o.RequestID] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != capacity || q.Len() != 0 {
		t.Fatalf("drained %d len %d", len(seen), q.Len())
	}
}
