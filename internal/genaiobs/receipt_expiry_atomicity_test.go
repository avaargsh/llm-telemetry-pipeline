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
	expireFixture := func() {
		for _, table := range []string{"genai_observation_receipts", "genai_observation_delivery_receipts"} {
			// Table names are fixed; preserve expires_at > accepted_at.
			if _, err := db.ExecContext(ctx, "UPDATE public."+table+" SET accepted_at=now()-interval '31 days', expires_at=now()-interval '1 second' WHERE tenant_id=$1", tenant); err != nil {
				t.Fatal(err)
			}
		}
	}
	expireFixture()

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
			if got := readCounters(check); got != before {
				t.Fatalf("rollback changed counters: got=%+v want=%+v", got, before)
			}
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
	assertGlobalReceiptCountersMatchRows(t, db)

	// Backward compatibility: an older enqueue writer reads counters before
	// expiry, consumes the returned delete counts, then writes absolute totals.
	// With migration 005 the SQL function also updates the row; the writer\x27s
	// final absolute update must converge to the same exact count, not subtract twice.
	deliveries, err = store.Claim(ctx, "direct-expiry-compat-worker", time.Minute, 1)
	if err != nil || len(deliveries) != 1 || deliveries[0].Observation.TenantID != tenant {
		t.Fatalf("claim compatibility fixture: deliveries=%d err=%v", len(deliveries), err)
	}
	if err := store.Ack(ctx, deliveries[0], "direct-expiry-compat-worker"); err != nil {
		t.Fatal(err)
	}
	expireFixture()
	if err := store.EnqueueWithIdempotencyKey(ctx, o, key); err != nil {
		t.Fatalf("enqueue with same-transaction expiry: %v", err)
	}
	assertGlobalReceiptCountersMatchRows(t, db)
}

func assertGlobalReceiptCountersMatchRows(t *testing.T, db *sql.DB) {
	t.Helper()
	var naturalRows, deliveryRows, naturalCount, deliveryCount int64
	if err := db.QueryRow("SELECT count(*) FROM public.genai_observation_receipts").Scan(&naturalRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT count(*) FROM public.genai_observation_delivery_receipts").Scan(&deliveryRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT receipt_items, delivery_receipt_items FROM public.genai_observation_queue_state WHERE singleton=true").Scan(&naturalCount, &deliveryCount); err != nil {
		t.Fatal(err)
	}
	if naturalCount != naturalRows || deliveryCount != deliveryRows {
		t.Fatalf("receipt counters disagree with rows: natural=%d/%d delivery=%d/%d", naturalCount, naturalRows, deliveryCount, deliveryRows)
	}
}

func seedExpiredReceiptFixture(t *testing.T, store *DurableStore, db *sql.DB, tenant, trace, key string) Observation {
	t.Helper()
	ctx := context.Background()
	o := durableTestObservation(tenant, trace, 17)
	if err := store.EnqueueWithIdempotencyKey(ctx, o, key); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(ctx, "expiry-fixture-worker", time.Minute, 1)
	if err != nil || len(claimed) != 1 || claimed[0].Observation.TenantID != tenant {
		t.Fatalf("claim expiry fixture: deliveries=%d err=%v", len(claimed), err)
	}
	if err := store.Ack(ctx, claimed[0], "expiry-fixture-worker"); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"genai_observation_receipts", "genai_observation_delivery_receipts"} {
		if _, err := db.Exec("UPDATE public."+table+" SET accepted_at=now()-interval '31 days', expires_at=now()-interval '1 second' WHERE tenant_id=$1", tenant); err != nil {
			t.Fatal(err)
		}
	}
	return o
}

func TestDurableStoreReceiptExpiryFailsClosedOnCounterUnderflow(t *testing.T) {
	store, db := openDurableTestStore(t, 8)
	tenant := fmt.Sprintf("durable-test-expiry-underflow-%d", time.Now().UnixNano())
	seedExpiredReceiptFixture(t, store, db, tenant, "b123456789abcdef0123456789abcdef", "underflow-key")
	if _, err := db.Exec("UPDATE public.genai_observation_queue_state SET receipt_items=0, delivery_receipt_items=0 WHERE singleton=true"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil { t.Fatal(err) }
	if _, err := tx.Exec("SET LOCAL ROLE llm_telemetry_observation_receiver"); err != nil { t.Fatal(err) }
	var natural, delivery int64
	err = tx.QueryRow("SELECT expired_receipts,expired_delivery_receipts FROM public.expire_genai_observation_receipts()").Scan(&natural, &delivery)
	if err == nil {
		t.Fatalf("counter underflow must fail closed, function returned %d/%d", natural, delivery)
	}
	_ = tx.Rollback()
	var naturalRows, deliveryRows int64
	if err := db.QueryRow("SELECT count(*) FROM public.genai_observation_receipts WHERE tenant_id=$1", tenant).Scan(&naturalRows); err != nil { t.Fatal(err) }
	if err := db.QueryRow("SELECT count(*) FROM public.genai_observation_delivery_receipts WHERE tenant_id=$1", tenant).Scan(&deliveryRows); err != nil { t.Fatal(err) }
	if naturalRows != 1 || deliveryRows != 1 {
		t.Fatalf("underflow failure deleted receipts: natural=%d delivery=%d", naturalRows, deliveryRows)
	}
}

func TestDurableStoreReceiptExpirySerializesWithAdmission(t *testing.T) {
	store, db := openDurableTestStore(t, 8)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	tenant := fmt.Sprintf("durable-test-expiry-race-%d", time.Now().UnixNano())
	o := seedExpiredReceiptFixture(t, store, db, tenant, "c123456789abcdef0123456789abcdef", "expiry-race-key")
	blocker, err := db.BeginTx(ctx, nil)
	if err != nil { t.Fatal(err) }
	if _, err := blocker.ExecContext(ctx, "SELECT receipt_items FROM public.genai_observation_queue_state WHERE singleton=true FOR UPDATE"); err != nil { t.Fatal(err) }
	results := make(chan error, 2)
	go func() {
		tx, err := db.BeginTx(ctx, nil)
		if err == nil {
			_, err = tx.ExecContext(ctx, "SET LOCAL ROLE llm_telemetry_observation_receiver")
		}
		if err == nil {
			var natural, delivery int64
			err = tx.QueryRowContext(ctx, "SELECT expired_receipts,expired_delivery_receipts FROM public.expire_genai_observation_receipts()").Scan(&natural, &delivery)
		}
		if err == nil { err = tx.Commit() } else if tx != nil { _ = tx.Rollback() }
		results <- err
	}()
	go func() { results <- store.EnqueueWithIdempotencyKey(ctx, o, "expiry-race-key") }()
	time.Sleep(75 * time.Millisecond)
	if err := blocker.Commit(); err != nil { t.Fatal(err) }
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			if err != nil { t.Fatalf("concurrent expiry/admission failed: %v", err) }
		case <-ctx.Done():
			t.Fatal("concurrent expiry/admission deadlocked or exceeded timeout")
		}
	}
	assertGlobalReceiptCountersMatchRows(t, db)
}
