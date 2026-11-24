package genaiobs

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// Exercise the granted SQL capability independently of EnqueueBatch. Calling
// the expiry function directly must not leave the persisted capacity stale.
func TestDurableStoreDirectReceiptExpiryUpdatesCounters(t *testing.T) {
	store, db := openDurableTestStore(t, 8)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tenant := fmt.Sprintf("durable-test-direct-expiry-%d", time.Now().UnixNano())
	o := durableTestObservation(tenant, "a123456789abcdef0123456789abcdef", 12)
	key := "direct-expiry-key"
	if err := store.EnqueueWithIdempotencyKey(ctx, o, key); err != nil {
		t.Fatal(err)
	}
	deliveries, err := store.Claim(ctx, "direct-expiry-worker", time.Minute, 1)
	if err != nil || len(deliveries) != 1 || deliveries[0].Observation.TenantID != tenant {
		t.Fatalf("claim fixture: deliveries=%d err=%v", len(deliveries), err)
	}
	if err := store.Ack(ctx, deliveries[0], "direct-expiry-worker"); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"genai_observation_receipts", "genai_observation_delivery_receipts"} {
		// Only fixed fixture table names are used; timestamps respect expiry > acceptance.
		if _, err := db.ExecContext(ctx, "UPDATE public."+table+" SET accepted_at=now()-interval '31 days', expires_at=now()-interval '1 second' WHERE tenant_id=$1", tenant); err != nil {
			t.Fatal(err)
		}
	}

	type counters struct{ natural, delivery int64 }
	readCounters := func(tx *sql.Tx) counters {
		t.Helper()
		var c counters
		if err := tx.QueryRowContext(ctx, "SELECT receipt_items,delivery_receipt_items FROM public.genai_observation_queue_state WHERE singleton=true FOR UPDATE").Scan(&c.natural, &c.delivery); err != nil {
			t.Fatal(err)
		}
		return c
	}
	expire := func(tx *sql.Tx) counters {
		t.Helper()
		var c counters
		if err := tx.QueryRowContext(ctx, "SELECT expired_receipts,expired_delivery_receipts FROM public.expire_genai_observation_receipts()").Scan(&c.natural, &c.delivery); err != nil {
			t.Fatal(err)
		}
		return c
	}
	assertRows := func(tx *sql.Tx, want int64) {
		t.Helper()
		for _, table := range []string{"genai_observation_receipts", "genai_observation_delivery_receipts"} {
			var count int64
			if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM public."+table+" WHERE tenant_id=$1", tenant).Scan(&count); err != nil || count != want {
				t.Fatalf("%s rows=%d want=%d err=%v", table, count, want, err)
			}
		}
	}
	begin := func() *sql.Tx {
		t.Helper()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tx.Rollback() })
		return tx
	}
	for _, commit := range []bool{false, true} {
		tx := begin()
		before := readCounters(tx)
		if _, err := tx.ExecContext(ctx, "SET LOCAL ROLE llm_telemetry_observation_receiver"); err != nil {
			t.Fatal(err)
		}
		assertRows(tx, 1)
		removed := expire(tx)
		if removed.natural < 1 || removed.delivery < 1 {
			t.Fatalf("expiry did not delete fixture receipts: %+v", removed)
		}
		assertRows(tx, 0)
		after := readCounters(tx)
		want := counters{before.natural - removed.natural, before.delivery - removed.delivery}
		if after != want {
			t.Fatalf("receipt expiry counter drift: before=%+v removed=%+v after=%+v want=%+v", before, removed, after, want)
		}
		if again := expire(tx); again != (counters{}) {
			t.Fatalf("repeat expiry changed state: %+v", again)
		}
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			check := begin()
			assertRows(check, 1)
			if err := check.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := store.EnqueueWithIdempotencyKey(ctx, o, key); err != nil {
		t.Fatalf("admission after committed expiry: %v", err)
	}
	var queued int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM public.genai_observations WHERE tenant_id=$1", tenant).Scan(&queued); err != nil || queued != 1 {
		t.Fatalf("readmission queued=%d err=%v", queued, err)
	}
}
