package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/genaiobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const m11SeparateDatabaseToken = "m11-separate-database-test-token-01234567890123456789"

func openM11ReceiverRoleDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid receiver database configuration")
	}
	db := stdlib.OpenDB(*config, stdlib.OptionAfterConnect(func(ctx context.Context, conn *pgx.Conn) error {
		if _, err := conn.Exec(ctx, "SET ROLE llm_telemetry_observation_receiver"); err != nil {
			return err
		}
		_, err := conn.Exec(ctx, "SET search_path TO pg_catalog, public")
		return err
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Fatalf("connect with receiver capability role: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// This is a true two-database delivery test: the sender queue and the
// receiver's observation/receipt tables are hosted in different databases.
// The receiver connection assumes its NOLOGIN capability role on every
// physical connection, rather than using the database owner for admission.
func TestM11DurableHTTPExporterReceiverUsesIndependentDatabases(t *testing.T) {
	senderDB := testDB(t)
	receiverDSN := os.Getenv("TEST_RECEIVER_POSTGRES_DSN")
	if receiverDSN == "" {
		t.Skip("TEST_RECEIVER_POSTGRES_DSN not set; independent receiver database integration test")
	}
	receiverDB := openM11ReceiverRoleDB(t, receiverDSN)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenant := "ledger-m11-7-separated-" + suffix
	senderStore, err := genaiobs.NewDurableStore(senderDB, 8)
	if err != nil {
		t.Fatal(err)
	}
	receiverStore, err := genaiobs.NewDurableStore(receiverDB, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = senderDB.Exec("DELETE FROM public.genai_observations WHERE tenant_id=$1", tenant)
		_, _ = senderDB.Exec("DELETE FROM public.genai_observation_receipts WHERE tenant_id=$1", tenant)
		_, _ = senderDB.Exec("DELETE FROM public.genai_observation_delivery_receipts WHERE tenant_id=$1", tenant)
		_, _ = senderDB.Exec(`UPDATE public.genai_observation_queue_state
SET queue_items=(SELECT count(*) FROM public.genai_observations),
    receipt_items=(SELECT count(*) FROM public.genai_observation_receipts),
    delivery_receipt_items=(SELECT count(*) FROM public.genai_observation_delivery_receipts),
    capacity_limit=0 WHERE singleton=true`)
	})

	observation := genaiobs.Observation{
		SchemaVersion: genaiobs.Version,
		TenantID: tenant,
		TraceID: "a123456789abcdef0123456789abcdef",
		SpanID: "a123456789abcdef",
		RequestID: "m11-7-independent-db-" + suffix,
		AttemptID: "attempt-1",
		Provider: "openai",
		Operation: "chat",
		RequestModel: "gpt-4.1",
		ResponseModel: "gpt-4.1",
		Status: "ok",
	}
	inputTokens := int64(0)
	outputTokens := int64(42)
	observation.InputTokens = &inputTokens
	observation.OutputTokens = &outputTokens
	if err := senderStore.Enqueue(ctx, observation); err != nil {
		t.Fatal(err)
	}

	handler, err := genaiobs.NewDurableOTLPHTTPHandler(
		receiverStore,
		[]genaiobs.OTLPCredential{{Token: m11SeparateDatabaseToken, TenantID: tenant}},
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
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("test server does not support connection hijacking")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("hijack response connection: %v", err)
				return
			}
			_ = conn.Close()
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

	exporter, err := genaiobs.NewHTTPDurableExporter(
		proxy.URL+"/v1/traces", m11SeparateDatabaseToken, proxy.Client(),
	)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := senderStore.Claim(ctx, "sender-before-restart", time.Second, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("sender claim=%d err=%v", len(claimed), err)
	}
	if err := exporter.Export(ctx, claimed[0]); err == nil {
		t.Fatal("lost receiver ACK should be reported as a retryable transport error")
	}

	observationID := claimed[0].ID
	expectedKey := fmt.Sprintf("genaiobs/%d", observationID)
	var receiverReceipts, receiverRows int
	if err := receiverDB.QueryRowContext(ctx,
		"SELECT count(*) FROM public.genai_observation_delivery_receipts WHERE tenant_id=$1 AND idempotency_key=$2",
		tenant, expectedKey).Scan(&receiverReceipts); err != nil {
		t.Fatal(err)
	}
	if err := receiverDB.QueryRowContext(ctx,
		"SELECT count(*) FROM public.genai_observations WHERE tenant_id=$1",
		tenant).Scan(&receiverRows); err != nil {
		t.Fatal(err)
	}
	if receiverReceipts != 1 || receiverRows != 1 {
		t.Fatalf("receiver commit was not durable: receipts=%d observations=%d", receiverReceipts, receiverRows)
	}
	var senderReceipts, senderRows int
	if err := senderDB.QueryRowContext(ctx,
		"SELECT count(*) FROM public.genai_observation_delivery_receipts WHERE tenant_id=$1 AND idempotency_key=$2",
		tenant, expectedKey).Scan(&senderReceipts); err != nil {
		t.Fatal(err)
	}
	if err := senderDB.QueryRowContext(ctx,
		"SELECT count(*) FROM public.genai_observations WHERE tenant_id=$1",
		tenant).Scan(&senderRows); err != nil {
		t.Fatal(err)
	}
	if senderReceipts != 0 || senderRows != 1 {
		t.Fatalf("sender/receiver persistence crossed database boundary: sender receipts=%d rows=%d", senderReceipts, senderRows)
	}

	result, err := senderDB.ExecContext(ctx,
		"UPDATE public.genai_observations SET lease_until=now()-interval '1 second' WHERE observation_id=$1 AND lease_owner=$2",
		observationID, "sender-before-restart")
	if err != nil {
		t.Fatal(err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("expire sender lease: affected=%d err=%v", affected, err)
	}
	replacement, err := genaiobs.NewDurableWorker(
		senderStore, exporter, "sender-after-restart", time.Minute,
		genaiobs.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Second, Jitter: 0},
	)
	if err != nil {
		t.Fatal(err)
	}
	if delivered, err := replacement.RunOnce(ctx); !delivered || err != nil {
		t.Fatalf("replacement delivery=%v err=%v", delivered, err)
	}
	if err := exporter.Export(ctx, genaiobs.Delivery{ID: observationID, Observation: observation}); err != nil {
		t.Fatalf("post-ACK duplicate should be acknowledged by receiver database: %v", err)
	}

	mu.Lock()
	gotKeys := append([]string(nil), keys...)
	mu.Unlock()
	if len(gotKeys) != 3 || gotKeys[0] != expectedKey || gotKeys[1] != expectedKey || gotKeys[2] != expectedKey {
		t.Fatalf("idempotency key changed across restart/replay: %#v want %q", gotKeys, expectedKey)
	}
	if err := senderDB.QueryRowContext(ctx,
		"SELECT count(*) FROM public.genai_observations WHERE tenant_id=$1",
		tenant).Scan(&senderRows); err != nil {
		t.Fatal(err)
	}
	if err := receiverDB.QueryRowContext(ctx,
		"SELECT count(*) FROM public.genai_observations WHERE tenant_id=$1",
		tenant).Scan(&receiverRows); err != nil {
		t.Fatal(err)
	}
	if senderRows != 0 || receiverRows != 1 {
		t.Fatalf("unexpected final database ownership: sender rows=%d receiver rows=%d", senderRows, receiverRows)
	}
}
