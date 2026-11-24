package genaiobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDurableStoreIdempotencyKeyReceiptSemantics(t *testing.T) {
	store, _ := openDurableTestStore(t, 8)
	ctx := context.Background()
	tenant := fmt.Sprintf("durable-test-key-%d", time.Now().UnixNano())
	observation := durableTestObservation(tenant, "6123456789abcdef0123456789abcdef", 61)
	key := "genaiobs/9001"

	if err := store.EnqueueWithIdempotencyKey(ctx, observation, key); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueWithIdempotencyKey(ctx, observation, key); err != nil {
		t.Fatalf("same key and payload must be idempotent: %v", err)
	}

	conflict := observation
	changed := int64(999)
	conflict.InputTokens = &changed
	if err := store.EnqueueWithIdempotencyKey(ctx, conflict, key); !errors.Is(err, ErrObservationConflict) {
		t.Fatalf("same key with changed payload must conflict: %v", err)
	}

	// A second key for the same natural observation is recorded but must not
	// create another queue row.
	if err := store.EnqueueWithIdempotencyKey(ctx, observation, "genaiobs/9002"); err != nil {
		t.Fatal(err)
	}
	var queued, receipts int
	if err := store.DB.QueryRowContext(ctx, "SELECT count(*) FROM genai_observations WHERE tenant_id=$1", tenant).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if err := store.DB.QueryRowContext(ctx, "SELECT count(*) FROM genai_observation_delivery_receipts WHERE tenant_id=$1", tenant).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if queued != 1 || receipts != 2 {
		t.Fatalf("queued=%d delivery receipts=%d; want 1 and 2", queued, receipts)
	}

	claimed, err := store.Claim(ctx, "worker-key-receipt", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%d err=%v", len(claimed), err)
	}
	if claimed[0].IdempotencyKey() != key {
		t.Fatalf("unexpected stable key: %q", claimed[0].IdempotencyKey())
	}
	if err := store.Ack(ctx, claimed[0], "worker-key-receipt"); err != nil {
		t.Fatal(err)
	}
	// A duplicate after the source row has been ACKed is still suppressed by
	// the persisted delivery receipt, not merely by the live queue row.
	if err := store.EnqueueWithIdempotencyKey(ctx, observation, key); err != nil {
		t.Fatal(err)
	}
	if err := store.DB.QueryRowContext(ctx, "SELECT count(*) FROM genai_observations WHERE tenant_id=$1", tenant).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 0 {
		t.Fatalf("post-ACK replay recreated %d queue rows", queued)
	}
	var counter int64
	if err := store.DB.QueryRowContext(ctx, "SELECT delivery_receipt_items FROM genai_observation_queue_state WHERE singleton=true").Scan(&counter); err != nil {
		t.Fatal(err)
	}
	if counter < 2 {
		t.Fatalf("delivery receipt counter=%d; want at least 2", counter)
	}
}

func TestDurableStoreRejectsInvalidIdempotencyKeys(t *testing.T) {
	store, _ := openDurableTestStore(t, 8)
	ctx := context.Background()
	observation := durableTestObservation("durable-test-invalid-key", "7123456789abcdef0123456789abcdef", 71)
	for _, key := range []string{"", "bad key", "bad\nkey", strings.Repeat("x", 257)} {
		if err := store.EnqueueWithIdempotencyKey(ctx, observation, key); !errors.Is(err, ErrInvalidIdempotencyKey) {
			t.Fatalf("key %q: got %v, want ErrInvalidIdempotencyKey", key, err)
		}
	}
}
