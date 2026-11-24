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
	if err := store.Enqueue(ctx, observation); err != nil {
		t.Fatal(err)
	}
	var observationID int64
	if err := store.DB.QueryRowContext(ctx, "SELECT observation_id FROM genai_observations WHERE tenant_id=$1", tenant).Scan(&observationID); err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("genaiobs/%d", observationID)

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
	if err := store.EnqueueWithIdempotencyKey(ctx, observation, key+"/alternate"); err != nil {
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
	// The receiver's queue row gets its own downstream delivery identity;
	// the inbound source key is retained separately in the delivery receipt.
	wantDownstreamKey := fmt.Sprintf("genaiobs/%d", claimed[0].ID)
	if claimed[0].IdempotencyKey() != wantDownstreamKey {
		t.Fatalf("unexpected downstream key: got %q want %q", claimed[0].IdempotencyKey(), wantDownstreamKey)
	}
	var storedKey string
	if err := store.DB.QueryRowContext(ctx, "SELECT idempotency_key FROM genai_observation_delivery_receipts WHERE tenant_id=$1 AND idempotency_key=$2", tenant, key).Scan(&storedKey); err != nil {
		t.Fatal(err)
	}
	if storedKey != key {
		t.Fatalf("receiver did not preserve inbound key: got %q want %q", storedKey, key)
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

func TestDurableStoreDeliveryReceiptTenantIsolationExpiryAndCapacity(t *testing.T) {
	store, db := openDurableTestStore(t, 8)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenantA := "durable-test-receipt-a-" + suffix
	tenantB := "durable-test-receipt-b-" + suffix
	tenantC := "durable-test-receipt-c-" + suffix
	key := "genaiobs/shared-tenant-key"
	observationA := durableTestObservation(tenantA, "8123456789abcdef0123456789abcdef", 81)
	observationB := observationA
	observationB.TenantID = tenantB

	if err := store.EnqueueWithIdempotencyKey(ctx, observationA, key); err != nil {
		t.Fatal(err)
	}
	var expiresAt time.Time
	if err := db.QueryRowContext(ctx,
		"SELECT expires_at FROM public.genai_observation_delivery_receipts WHERE tenant_id=$1 AND idempotency_key=$2",
		tenantA, key).Scan(&expiresAt); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if !expiresAt.After(now.Add(29*24*time.Hour)) || expiresAt.After(now.Add(31*24*time.Hour)) {
		t.Fatalf("delivery receipt retention is not approximately 30 days: expires_at=%s now=%s", expiresAt, now)
	}

	claimed, err := store.Claim(ctx, "worker-receipt-expiry", time.Minute, 1)
	if err != nil || len(claimed) != 1 || claimed[0].Observation.TenantID != tenantA {
		t.Fatalf("claim tenant A receipt row: claimed=%+v err=%v", claimed, err)
	}
	if err := store.Ack(ctx, claimed[0], "worker-receipt-expiry"); err != nil {
		t.Fatal(err)
	}

	// The same key and same trace/span identity are independent under another
	// authenticated tenant because both receipt uniqueness keys include tenant_id.
	if err := store.EnqueueWithIdempotencyKey(ctx, observationB, key); err != nil {
		t.Fatalf("same idempotency key must be isolated by tenant: %v", err)
	}
	for _, tenant := range []string{tenantA, tenantB} {
		var receipts int
		if err := db.QueryRowContext(ctx,
			"SELECT count(*) FROM public.genai_observation_delivery_receipts WHERE tenant_id=$1 AND idempotency_key=$2",
			tenant, key).Scan(&receipts); err != nil {
			t.Fatal(err)
		}
		if receipts != 1 {
			t.Fatalf("tenant %s has %d receipts for shared key; want 1", tenant, receipts)
		}
	}

	// Expire both natural and delivery receipts to model a replay after the
	// configured retention window. The next admission may create a new row.
	if _, err := db.ExecContext(ctx, `UPDATE public.genai_observation_delivery_receipts
SET accepted_at=now()-interval '31 days', expires_at=now()-interval '1 second' WHERE tenant_id=$1 AND idempotency_key=$2`, tenantA, key); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE public.genai_observation_receipts
SET accepted_at=now()-interval '2 days', expires_at=now()-interval '1 second'
WHERE tenant_id=$1 AND trace_id=$2 AND span_id=$3`,
		tenantA, observationA.TraceID, observationA.SpanID); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueWithIdempotencyKey(ctx, observationA, key); err != nil {
		t.Fatalf("re-admission after receipt expiry: %v", err)
	}
	var requeued int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM public.genai_observations WHERE tenant_id=$1", tenantA).Scan(&requeued); err != nil {
		t.Fatal(err)
	}
	if requeued != 1 {
		t.Fatalf("expired receipt did not permit replay admission: rows=%d", requeued)
	}

	// Capacity is enforced against the durable receipt counter in the same
	// transaction; an over-capacity admission must roll back both receipts and row.
	if _, err := db.ExecContext(ctx,
		"UPDATE public.genai_observation_queue_state SET delivery_receipt_items=$1 WHERE singleton=true",
		int64(MaxDurableDeliveryReceiptCapacity)); err != nil {
		t.Fatal(err)
	}
	observationC := durableTestObservation(tenantC, "9123456789abcdef0123456789abcdef", 91)
	if err := store.EnqueueWithIdempotencyKey(ctx, observationC, "genaiobs/capacity-boundary"); !errors.Is(err, ErrDurableQueueFull) {
		t.Fatalf("delivery receipt capacity must fail closed, got %v", err)
	}
	var rows, receipts int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM public.genai_observations WHERE tenant_id=$1", tenantC).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM public.genai_observation_delivery_receipts WHERE tenant_id=$1", tenantC).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if rows != 0 || receipts != 0 {
		t.Fatalf("over-capacity admission partially committed: rows=%d receipts=%d", rows, receipts)
	}
}
