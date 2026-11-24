package ledger

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/genaiobs"
)

const m11DurableExporterTestToken = "m11-6-receiver-test-token-012345678901234567890123456789"

func TestM11DurableHTTPExporterReceiverPersistsKeyAcrossAckLoss(t *testing.T) {
	db := testDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenantA := "ledger-m11-6-a-" + suffix
	tenantB := "ledger-m11-6-b-" + suffix

	authoritative := Store{DB: db}
	for i, tenant := range []string{tenantA, tenantB} {
		seed := freshEvent(t, fmt.Sprintf("ledger-m11-6-seed-%d-%s", i, suffix))
		seed.TenantID = tenant
		seed.RequestID = fmt.Sprintf("ledger-m11-6-seed-%d-%s", i, suffix)
		if err := seed.Seal(); err != nil {
			t.Fatal(err)
		}
		if outcome, err := authoritative.Record(ctx, seed); err != nil || outcome != Created {
			t.Fatalf("seed authoritative ledger event: outcome=%s err=%v", outcome, err)
		}
	}
	before := genAIUsageSnapshot(t, ctx, db, tenantA, tenantB)
	durable, err := genaiobs.NewDurableStore(db, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec("DELETE FROM genai_observations WHERE tenant_id IN ($1, $2)", tenantA, tenantB)
		_, _ = db.Exec("DELETE FROM genai_observation_receipts WHERE tenant_id IN ($1, $2)", tenantA, tenantB)
		_, _ = db.Exec("DELETE FROM genai_observation_delivery_receipts WHERE tenant_id IN ($1, $2)", tenantA, tenantB)
		_, _ = db.Exec(`UPDATE genai_observation_queue_state
SET queue_items=(SELECT count(*) FROM genai_observations),
    receipt_items=(SELECT count(*) FROM genai_observation_receipts),
    delivery_receipt_items=(SELECT count(*) FROM genai_observation_delivery_receipts),
    capacity_limit=0
WHERE singleton=true`)
	})

	observation := genaiobs.Observation{
		SchemaVersion: genaiobs.Version,
		TenantID: tenantA,
		TraceID: "8123456789abcdef0123456789abcdef",
		SpanID: "8123456789abcdef",
		RequestID: "ledger-m11-6-request-" + suffix,
		AttemptID: "attempt-1",
		Provider: "openai",
		Operation: "chat",
		RequestModel: "gpt-4.1",
		ResponseModel: "gpt-4.1",
		Status: "ok",
	}
	inputTokens := int64(0)
	outputTokens := int64(9007199254740993)
	observation.InputTokens = &inputTokens
	observation.OutputTokens = &outputTokens
	if err := durable.Enqueue(ctx, observation); err != nil {
		t.Fatal(err)
	}

	handler, err := genaiobs.NewDurableOTLPHTTPHandler(
		durable,
		[]genaiobs.OTLPCredential{{Token: m11DurableExporterTestToken, TenantID: tenantA}},
		4,
	)
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int64
	var mu sync.Mutex
	var keys []string
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		mu.Lock()
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		mu.Unlock()

		recorded := httptest.NewRecorder()
		handler.ServeHTTP(recorded, r)
		if attempt == 1 && recorded.Code == http.StatusOK {
			// Receiver committed the delivery receipt but its ACK is lost.
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		for name, values := range recorded.Header() {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}
		w.WriteHeader(recorded.Code)
		_, _ = io.Copy(w, recorded.Body)
	}))
	defer proxy.Close()

	exporter, err := genaiobs.NewHTTPDurableExporter(proxy.URL+"/v1/traces", m11DurableExporterTestToken, proxy.Client())
	if err != nil {
		t.Fatal(err)
	}
	policy := genaiobs.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Second, Jitter: 0}
	claimed, err := durable.Claim(ctx, "worker-before-crash", time.Second, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("pre-crash claim=%d err=%v", len(claimed), err)
	}
	// The receiver commits the key, but the proxy drops the ACK. Abandon the
	// lease without Retry/Ack/Release to model the sender process disappearing.
	if err := exporter.Export(ctx, claimed[0]); err == nil {
		t.Fatal("lost receiver ACK should be reported as a retryable transport error")
	}
	observationID := claimed[0].ID
	expectedKey := fmt.Sprintf("genaiobs/%d", observationID)
	var receipts int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM genai_observation_delivery_receipts WHERE tenant_id=$1 AND idempotency_key=$2", tenantA, expectedKey).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 {
		t.Fatalf("receiver did not persist the delivery key: %d", receipts)
	}
	var leaseOwner string
	if err := db.QueryRowContext(ctx, "SELECT lease_owner FROM genai_observations WHERE observation_id=$1", observationID).Scan(&leaseOwner); err != nil {
		t.Fatal(err)
	}
	if leaseOwner != "worker-before-crash" {
		t.Fatalf("unacknowledged lease owner=%q", leaseOwner)
	}
	// Expire the persisted lease deterministically instead of sleeping. The
	// replacement worker must reclaim the row after the first process is gone.
	if _, err := db.ExecContext(ctx, "UPDATE genai_observations SET lease_until=now()-interval '1 second' WHERE observation_id=$1", observationID); err != nil {
		t.Fatal(err)
	}

	replacementWorker, err := genaiobs.NewDurableWorker(durable, exporter, "worker-after-crash", time.Minute, policy)
	if err != nil {
		t.Fatal(err)
	}
	if delivered, err := replacementWorker.RunOnce(ctx); !delivered || err != nil {
		t.Fatalf("replacement delivery=%v err=%v", delivered, err)
	}
	if err := exporter.Export(ctx, genaiobs.Delivery{ID: observationID, Observation: observation}); err != nil {
		t.Fatalf("post-ACK duplicate should be acknowledged by persisted key: %v", err)
	}

	mu.Lock()
	gotKeys := append([]string(nil), keys...)
	mu.Unlock()
	if len(gotKeys) != 3 || gotKeys[0] != expectedKey || gotKeys[1] != expectedKey || gotKeys[2] != expectedKey {
		t.Fatalf("idempotency key changed across retry/restart/replay: %#v want %q", gotKeys, expectedKey)
	}
	var queued int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM genai_observations WHERE tenant_id=$1", tenantA).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 0 {
		t.Fatalf("ACKed queue still contains %d rows", queued)
	}
	if after := genAIUsageSnapshot(t, ctx, db, tenantA, tenantB); after != before {
		t.Fatal("durable HTTP exporter/receiver changed authoritative Ledger or Outbox rows")
	}
}
