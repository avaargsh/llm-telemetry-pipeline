package ingestapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/event"
	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// This test uses the real transactional Ledger + Outbox schema; it never
// contacts Kafka. Running without TEST_POSTGRES_DSN only skips this test.
func TestHTTPIngestionUsesOneAtomicLedgerOutboxTransaction(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not configured")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close integration connection: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	e, err := event.SyntheticUsageEvent("m5a-integration-"+rand.Text(), 0, time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, query := range []string{
			"DELETE FROM event_outbox WHERE event_id=$1::uuid",
			"DELETE FROM usage_ledger WHERE event_id=$1::uuid",
		} {
			if _, err := db.ExecContext(ctx, query, e.EventID); err != nil {
				t.Errorf("clean up integration event: %v", err)
			}
		}
	})

	h, err := NewHandler(&ledger.Store{DB: db}, []Credential{{Token: testToken, TenantID: e.TenantID, ProjectID: e.ProjectID}})
	if err != nil {
		t.Fatal(err)
	}
	sendReal := func(raw []byte) int {
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/usage-events", bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+testToken)
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr.Code
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	const clientCount = 24
	ready := make(chan struct{})
	statuses := make(chan int, clientCount)
	for range clientCount {
		go func() {
			<-ready
			statuses <- sendReal(raw)
		}()
	}
	close(ready)
	counts := make(map[int]int)
	for range clientCount {
		counts[<-statuses]++
	}
	if counts[http.StatusCreated] != 1 || counts[http.StatusOK] != clientCount-1 {
		t.Fatalf("concurrent submissions: want one create and %d duplicates, got %v", clientCount-1, counts)
	}
	changed := e
	changed.Model = "different-model"
	if err := changed.Seal(); err != nil {
		t.Fatal(err)
	}
	conflicting, err := json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	if status := sendReal(conflicting); status != http.StatusConflict {
		t.Fatalf("conflict status %d", status)
	}
	var ledgerRows, outboxRows int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM usage_ledger WHERE event_id=$1::uuid", e.EventID).Scan(&ledgerRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM event_outbox WHERE event_id=$1::uuid AND published_at IS NULL", e.EventID).Scan(&outboxRows); err != nil {
		t.Fatal(err)
	}
	if ledgerRows != 1 || outboxRows != 1 {
		t.Fatalf("expected one ledger row and one unpublished outbox row, got ledger=%d outbox=%d", ledgerRows, outboxRows)
	}
}
