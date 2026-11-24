package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/genaiobs"
)

const m11CollectorTestToken = "m11-collector-test-token-0123456789abcdef"

type m11CollectorEvidence struct {
	TestedHead                    string
	CollectorEndpoint             string
	ReceiverAttempts              int64
	ReceiverAcceptedResponses    int64
	Retryable503Responses         int64
	PostCommitAckLossInjected     bool
	PostCommitReplayDeduplicated  bool
	QueueSaturationRecovered      bool
	SampledOutSpanDropped         bool
	LedgerOutboxSnapshotUnchanged bool
	VerifiedAt                    time.Time
}

func TestM11RealCollectorFaultInjectionAndLedgerOutboxInvariance(t *testing.T) {
	collectorEndpoint := strings.TrimRight(os.Getenv("M11_COLLECTOR_ENDPOINT"), "/")
	if collectorEndpoint == "" {
		t.Skip("M11_COLLECTOR_ENDPOINT not set; real OpenTelemetry Collector integration test")
	}

	db := testDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenantA := "ledger-m11-a-" + suffix
	tenantB := "ledger-m11-b-" + suffix

	authoritative := Store{DB: db}
	for i, tenant := range []string{tenantA, tenantB} {
		seed := freshEvent(t, fmt.Sprintf("ledger-m11-collector-seed-%d-%s", i, suffix))
		seed.TenantID = tenant
		seed.RequestID = fmt.Sprintf("ledger-m11-collector-seed-%d-%s", i, suffix)
		if err := seed.Seal(); err != nil {
			t.Fatal(err)
		}
		if outcome, err := authoritative.Record(ctx, seed); err != nil || outcome != Created {
			t.Fatalf("seed authoritative ledger event: outcome=%s err=%v", outcome, err)
		}
	}
	before := genAIUsageSnapshot(t, ctx, db, tenantA, tenantB)
	assertLedgerInvariant := func(stage string) {
		t.Helper()
		if got := genAIUsageSnapshot(t, ctx, db, tenantA, tenantB); got != before {
			t.Fatalf("GenAI collector stage %q changed authoritative Ledger/Outbox rows", stage)
		}
	}

	durable, err := genaiobs.NewDurableStore(db, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec("DELETE FROM genai_observations WHERE tenant_id IN ($1, $2)", tenantA, tenantB)
		_, _ = db.Exec("DELETE FROM genai_observation_receipts WHERE tenant_id IN ($1, $2)", tenantA, tenantB)
		_, _ = db.Exec("UPDATE genai_observation_queue_state\nSET queue_items=(SELECT count(*) FROM genai_observations),\n    receipt_items=(SELECT count(*) FROM genai_observation_receipts),\n    capacity_limit=0\nWHERE singleton=true")
	})

	durableHandler, err := genaiobs.NewDurableOTLPHTTPHandler(
		durable,
		[]genaiobs.OTLPCredential{{Token: m11CollectorTestToken, TenantID: tenantA}},
		4,
	)
	if err != nil {
		t.Fatal(err)
	}

	var receiverAttempts atomic.Int64
	var receiverAccepted atomic.Int64
	var receiver503 atomic.Int64
	var ackLossInjected atomic.Bool
	proxy := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receiverAttempts.Add(1)
		recorded := httptest.NewRecorder()
		durableHandler.ServeHTTP(recorded, r)
		result := recorded.Result()
		defer result.Body.Close()
		if result.StatusCode == http.StatusOK {
			receiverAccepted.Add(1)
		}

		// Commit the observation first, then lose the HTTP ACK exactly once.
		// The real Collector must retry and the durable receipt must deduplicate it.
		if result.StatusCode == http.StatusOK && ackLossInjected.CompareAndSwap(false, true) {
			receiver503.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "{\"message\":\"injected post-commit acknowledgement loss\"}\n")
			return
		}
		if result.StatusCode == http.StatusServiceUnavailable {
			receiver503.Add(1)
		}
		for key, values := range result.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(result.StatusCode)
		_, _ = io.Copy(w, result.Body)
	})

	// The collector configuration targets this fixed loopback port. The CI job
	// runs the Collector with host networking and starts it before this test.
	listener, err := net.Listen("tcp", "127.0.0.1:4319")
	if err != nil {
		t.Fatalf("listen for real Collector receiver: %v", err)
	}
	receiverServer := &http.Server{Handler: proxy, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		_ = receiverServer.Serve(listener)
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = receiverServer.Shutdown(shutdownCtx)
	})

	client := &http.Client{Timeout: 5 * time.Second}
	postCollector := func(body string) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, collectorEndpoint+"/v1/traces", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("send OTLP JSON to real Collector: %v", err)
		}
		defer resp.Body.Close()
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("Collector OTLP receiver status=%d body=%q readErr=%v", resp.StatusCode, responseBody, readErr)
		}
	}

	// First path: the receiver commits, the injected 503 loses the ACK, and the
	// Collector retries the exact span. Only one durable row may exist.
	firstTrace := "0123456789abcdef0123456789abcdef"
	firstSpan := "0123456789abcdef"
	firstBody := m11CollectorWire(firstTrace, firstSpan, false)
	postCollector(firstBody)
	m11WaitFor(t, ctx, 15*time.Second, func() bool {
		n, err := durable.Len(ctx)
		return err == nil && receiverAttempts.Load() >= 2 && receiverAccepted.Load() >= 2 && n == 1
	}, "Collector retry after post-commit ACK loss")
	if !ackLossInjected.Load() || receiver503.Load() < 1 {
		t.Fatal("post-commit ACK loss was not injected")
	}

	first, err := durable.Claim(ctx, "m11-collector-worker", time.Minute, 1)
	if err != nil || len(first) != 1 {
		t.Fatalf("claim retried Collector observation: rows=%d err=%v", len(first), err)
	}
	got := first[0].Observation
	if got.TenantID != tenantA || got.TraceID != firstTrace || got.SpanID != firstSpan ||
		got.InputTokens == nil || *got.InputTokens != 123 || got.OutputTokens == nil || *got.OutputTokens != 45 {
		t.Fatalf("Collector wire projection or trusted tenant changed: %+v", got)
	}
	if err := durable.Ack(ctx, first[0], "m11-collector-worker"); err != nil {
		t.Fatal(err)
	}
	assertLedgerInvariant("post-commit ACK loss and retry")

	// A replay after downstream ACK must be suppressed by the retained receipt.
	beforeReplay := receiverAccepted.Load()
	postCollector(firstBody)
	m11WaitFor(t, ctx, 10*time.Second, func() bool {
		return receiverAccepted.Load() > beforeReplay
	}, "post-ACK replay to reach the receiver")
	if n, err := durable.Len(ctx); err != nil || n != 0 {
		t.Fatalf("post-ACK Collector replay recreated a row: len=%d err=%v", n, err)
	}
	assertLedgerInvariant("post-ACK replay deduplication")

	// Second path: fill the durable queue, observe retryable 503 from the real
	// receiver, drain one row, then require the Collector retry to be admitted.
	fillerOne := m11CollectorObservation(tenantA, "a123456789abcdef0123456789abcdef", "a123456789abcdef", 1)
	fillerTwo := m11CollectorObservation(tenantA, "b123456789abcdef0123456789abcdef", "b123456789abcdef", 2)
	if err := durable.Enqueue(ctx, fillerOne); err != nil {
		t.Fatal(err)
	}
	if err := durable.Enqueue(ctx, fillerTwo); err != nil {
		t.Fatal(err)
	}
	attemptsBeforeSaturation := receiverAttempts.Load()
	acceptedBeforeSaturation := receiverAccepted.Load()
	overloadsBeforeSaturation := receiver503.Load()
	secondTrace := "2123456789abcdef0123456789abcdef"
	secondSpan := "2123456789abcdef"
	secondBody := m11CollectorWire(secondTrace, secondSpan, false)
	postCollector(secondBody)
	m11WaitFor(t, ctx, 10*time.Second, func() bool {
		return receiver503.Load() > overloadsBeforeSaturation
	}, "durable receiver saturation response")

	drained, err := durable.Claim(ctx, "m11-saturation-drain", time.Minute, 1)
	if err != nil || len(drained) != 1 {
		t.Fatalf("claim one queue row to release capacity: rows=%d err=%v", len(drained), err)
	}
	if drained[0].Observation.TraceID != fillerOne.TraceID {
		t.Fatalf("unexpected FIFO row drained under saturation: %+v", drained[0].Observation)
	}
	if err := durable.Ack(ctx, drained[0], "m11-saturation-drain"); err != nil {
		t.Fatal(err)
	}
	m11WaitFor(t, ctx, 12*time.Second, func() bool {
		n, err := durable.Len(ctx)
		return err == nil && receiverAttempts.Load() >= attemptsBeforeSaturation+2 && receiverAccepted.Load() > acceptedBeforeSaturation && n == 2
	}, "Collector retry after queue capacity is released")

	recovered, err := durable.Claim(ctx, "m11-recovered-worker", time.Minute, 2)
	if err != nil || len(recovered) != 2 {
		t.Fatalf("claim recovered queue: rows=%d err=%v", len(recovered), err)
	}
	foundSecond := false
	for _, delivery := range recovered {
		if delivery.Observation.TraceID == secondTrace && delivery.Observation.SpanID == secondSpan {
			foundSecond = true
		}
		if err := durable.Ack(ctx, delivery, "m11-recovered-worker"); err != nil {
			t.Fatal(err)
		}
	}
	if !foundSecond {
		t.Fatal("Collector retry was not admitted after queue capacity was released")
	}
	if n, err := durable.Len(ctx); err != nil || n != 0 {
		t.Fatalf("recovered queue did not drain: len=%d err=%v", n, err)
	}
	assertLedgerInvariant("queue saturation and Collector retry recovery")

	// A deterministic Collector filter represents a sampled-out span. It must
	// never reach the durable receiver or the authoritative Ledger/Outbox.
	beforeDrop := receiverAttempts.Load()
	postCollector(m11CollectorWire("3123456789abcdef0123456789abcdef", "3123456789abcdef", true))
	time.Sleep(750 * time.Millisecond) // greater than the configured batch timeout
	if receiverAttempts.Load() != beforeDrop {
		t.Fatalf("sampled-out span reached receiver: attempts before=%d after=%d", beforeDrop, receiverAttempts.Load())
	}
	if n, err := durable.Len(ctx); err != nil || n != 0 {
		t.Fatalf("sampled-out span entered durable queue: len=%d err=%v", n, err)
	}
	assertLedgerInvariant("sampled-out drop")

	evidence := m11CollectorEvidence{
		TestedHead:                    os.Getenv("EXPECTED_HEAD"),
		CollectorEndpoint:             collectorEndpoint,
		ReceiverAttempts:              receiverAttempts.Load(),
		ReceiverAcceptedResponses:    receiverAccepted.Load(),
		Retryable503Responses:         receiver503.Load(),
		PostCommitAckLossInjected:     ackLossInjected.Load(),
		PostCommitReplayDeduplicated:  true,
		QueueSaturationRecovered:      true,
		SampledOutSpanDropped:         true,
		LedgerOutboxSnapshotUnchanged: true,
		VerifiedAt:                    time.Now().UTC(),
	}
	if path := os.Getenv("M11_EVIDENCE_PATH"); path != "" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		data, err := json.MarshalIndent(evidence, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("M11.5 exact-head Collector evidence: %+v", evidence)
}

func m11CollectorWire(traceID, spanID string, sampledOut bool) string {
	attributes := []string{
		"{\"key\":\"gen_ai.provider.name\",\"value\":{\"stringValue\":\"openai\"}}",
		"{\"key\":\"gen_ai.operation.name\",\"value\":{\"stringValue\":\"chat\"}}",
		"{\"key\":\"gen_ai.usage.input_tokens\",\"value\":{\"intValue\":\"123\"}}",
		"{\"key\":\"gen_ai.usage.output_tokens\",\"value\":{\"intValue\":\"45\"}}",
		"{\"key\":\"tenant.id\",\"value\":{\"stringValue\":\"untrusted-wire-tenant\"}}",
	}
	if sampledOut {
		attributes = append(attributes, "{\"key\":\"m11.sampled_out\",\"value\":{\"stringValue\":\"true\"}}")
	}
	return fmt.Sprintf(
		"{\"resourceSpans\":[{\"scopeSpans\":[{\"spans\":[{\"traceId\":%q,\"spanId\":%q,\"status\":{\"code\":1},\"attributes\":[%s]}]}]}]}",
		traceID, spanID, strings.Join(attributes, ","),
	)
}

func m11CollectorObservation(tenant, traceID, spanID string, tokens int64) genaiobs.Observation {
	return genaiobs.Observation{
		SchemaVersion: genaiobs.Version,
		TenantID:      tenant,
		TraceID:       traceID,
		SpanID:        spanID,
		Provider:      "openai",
		Operation:     "chat",
		Status:        "ok",
		InputTokens:   &tokens,
	}
}

func m11WaitFor(t *testing.T, ctx context.Context, timeout time.Duration, predicate func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended waiting for %s: %v", description, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for %s", description)
}
