package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/genaiobs"
)

// Compare full authoritative rows, not just counts: a changed token total,
// digest, event identity or outbox payload must also fail this assertion.
// Restrict the snapshot to the two isolated test tenants and request prefix
// so other packages' integration fixtures cannot make this test flaky.
func genAIUsageSnapshot(t *testing.T, ctx context.Context, db *sql.DB, tenantA, tenantB string) string {
	t.Helper()
	const q = `
SELECT jsonb_build_object(
  'ledger', COALESCE((
    SELECT jsonb_agg(to_jsonb(l) ORDER BY l.event_id)
    FROM usage_ledger l
    WHERE l.tenant_id IN ($1, $2) OR l.request_id LIKE 'ledger-m10-%'
  ), '[]'::jsonb),
  'outbox', COALESCE((
    SELECT jsonb_agg(to_jsonb(o) ORDER BY o.event_id)
    FROM event_outbox o
    WHERE o.event_payload->>'tenant_id' IN ($1, $2)
       OR o.event_payload->>'request_id' LIKE 'ledger-m10-%'
       OR o.event_id IN (
         SELECT event_id FROM usage_ledger
         WHERE tenant_id IN ($1, $2) OR request_id LIKE 'ledger-m10-%'
       )
  ), '[]'::jsonb)
)::text`
	var snapshot string
	if err := db.QueryRowContext(ctx, q, tenantA, tenantB).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestGenAIObservationsDoNotChangeLedger(t *testing.T) {
	db := testDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const tenantA = "ledger-m10-tenant-a"
	const tenantB = "ledger-m10-tenant-b"
	store := Store{DB: db}
	first := freshEvent(t, "ledger-m10-a")
	second := freshEvent(t, "ledger-m10-b")
	first.TenantID, second.TenantID = tenantA, tenantB
	if err := first.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := second.Seal(); err != nil {
		t.Fatal(err)
	}
	if result, err := store.Record(ctx, first); err != nil || result != Created {
		t.Fatalf("seed first ledger event: %v %v", result, err)
	}
	if result, err := store.Record(ctx, second); err != nil || result != Created {
		t.Fatalf("seed second ledger event: %v %v", result, err)
	}
	for _, id := range []string{first.EventID, second.EventID} {
		if l, o := counts(t, db, id); l != 1 || o != 1 {
			t.Fatalf("seed missing: ledger=%d outbox=%d", l, o)
		}
	}
	before := genAIUsageSnapshot(t, ctx, db, tenantA, tenantB)
	unchanged := func(t *testing.T) {
		t.Helper()
		if got := genAIUsageSnapshot(t, ctx, db, tenantA, tenantB); got != before {
			t.Fatal("GenAI observation changed authoritative ledger/outbox rows")
		}
	}
	base := genaiobs.Span{
		TraceID: "0123456789abcdef0123456789abcdef",
		SpanID: "0123456789abcdef",
		RequestID: first.RequestID,
		AttemptID: "attempt-1",
		Status: "ok",
		Attributes: []genaiobs.Attribute{
			{Key: "gen_ai.provider.name", Value: "openai"},
			{Key: "gen_ai.operation.name", Value: "chat"},
		},
	}
	observe := func(t *testing.T, scope string, span genaiobs.Span) genaiobs.Observation {
		t.Helper()
		o, err := genaiobs.NormalizeInference(genaiobs.Scope{TenantID: scope}, genaiobs.Version, span)
		if err != nil || o.TenantID != scope {
			t.Fatalf("observation rejected or scope changed: %v", err)
		}
		if _, err := json.Marshal(o); err != nil {
			t.Fatal(err)
		}
		unchanged(t)
		return o
	}
	t.Run("sampled out no projection", func(t *testing.T) {
		// Deliberately do not call the adapter: no span was sampled.
		unchanged(t)
	})
	t.Run("missing usage", func(t *testing.T) {
		o := observe(t, tenantA, base)
		if o.InputTokens != nil || o.OutputTokens != nil {
			t.Fatal("missing usage was invented")
		}
	})
	t.Run("replay and changed token observation", func(t *testing.T) {
		s := base
		s.Attributes = append(append([]genaiobs.Attribute(nil), base.Attributes...),
			genaiobs.Attribute{Key: "gen_ai.usage.input_tokens", Value: int64(999999)},
			genaiobs.Attribute{Key: "gen_ai.usage.output_tokens", Value: int64(0)})
		for i := 0; i < 3; i++ {
			o := observe(t, tenantA, s)
			if o.InputTokens == nil || *o.InputTokens != 999999 || o.OutputTokens == nil || *o.OutputTokens != 0 {
				t.Fatal("observation token projection differs")
			}
		}
		s.AttemptID = "attempt-2"
		observe(t, tenantA, s)
	})
	t.Run("unfinalized request and spoofed tenant", func(t *testing.T) {
		s := base
		s.RequestID = "ledger-m10-never-finalized"
		s.Attributes = append(append([]genaiobs.Attribute(nil), base.Attributes...),
			genaiobs.Attribute{Key: "tenant.id", Value: tenantA})
		observe(t, tenantB, s)
	})
	t.Run("rejected observation", func(t *testing.T) {
		s := base
		s.Attributes = append(append([]genaiobs.Attribute(nil), base.Attributes...),
			genaiobs.Attribute{Key: "gen_ai.usage.input_tokens", Value: int64(-1)})
		if _, err := genaiobs.NormalizeInference(genaiobs.Scope{TenantID: tenantA}, genaiobs.Version, s); err != genaiobs.ErrInvalid {
			t.Fatalf("expected invalid observation: %v", err)
		}
		unchanged(t)
	})
	t.Run("bounded queue saturation replay and drain", func(t *testing.T) {
		q, err := genaiobs.NewQueue(2)
		if err != nil {
			t.Fatal(err)
		}
		scopeA := genaiobs.Scope{TenantID: tenantA}
		scopeB := genaiobs.Scope{TenantID: tenantB}
		replay := base
		replay.Attributes = append(append([]genaiobs.Attribute(nil), base.Attributes...),
			genaiobs.Attribute{Key: "gen_ai.usage.input_tokens", Value: int64(999999)})
		if err := q.Submit(scopeA, genaiobs.Version, base); err != nil {
			t.Fatal(err)
		}
		unchanged(t)
		if err := q.Submit(scopeA, genaiobs.Version, replay); err != nil {
			t.Fatal(err)
		}
		unchanged(t)
		pending := base
		pending.RequestID = "ledger-m10-queue-unfinalized"
		pending.Attributes = append(append([]genaiobs.Attribute(nil), base.Attributes...),
			genaiobs.Attribute{Key: "tenant.id", Value: tenantA})
		if err := q.Submit(scopeB, genaiobs.Version, pending); err != genaiobs.ErrQueueFull {
			t.Fatalf("expected explicit overload: %v", err)
		}
		if q.Len() != 2 {
			t.Fatal("overload changed queue length")
		}
		unchanged(t)
		o, ok := q.Pop()
		if !ok || o.TenantID != tenantA || o.RequestID != base.RequestID || o.InputTokens != nil {
			t.Fatal("oldest queued observation changed")
		}
		unchanged(t)
		if err := q.Submit(scopeB, genaiobs.Version, pending); err != nil {
			t.Fatal(err)
		}
		unchanged(t)
		o, ok = q.Pop()
		if !ok || o.TenantID != tenantA || o.RequestID != base.RequestID || o.InputTokens == nil || *o.InputTokens != 999999 {
			t.Fatal("replayed observation changed")
		}
		unchanged(t)
		o, ok = q.Pop()
		if !ok || o.TenantID != tenantB || o.RequestID != pending.RequestID {
			t.Fatal("trusted tenant or queued request changed")
		}
		unchanged(t)
		if _, ok := q.Pop(); ok || q.Len() != 0 {
			t.Fatal("queue not drained")
		}
		invalid := base
		invalid.Attributes = append(append([]genaiobs.Attribute(nil), base.Attributes...),
			genaiobs.Attribute{Key: "gen_ai.usage.input_tokens", Value: int64(-1)})
		if err := q.Submit(scopeA, genaiobs.Version, invalid); err != genaiobs.ErrInvalid || q.Len() != 0 {
			t.Fatalf("invalid observation retained: %v", err)
		}
		unchanged(t)
	})
	t.Run("atomic batch rejection replay and drain", func(t *testing.T) {
		q, err := genaiobs.NewQueue(3)
		if err != nil {
			t.Fatal(err)
		}
		scopeA := genaiobs.Scope{TenantID: tenantA}
		scopeB := genaiobs.Scope{TenantID: tenantB}
		if err := q.Submit(scopeA, genaiobs.Version, base); err != nil {
			t.Fatal(err)
		}
		unchanged(t)
		replay := base
		replay.Attributes = append(append([]genaiobs.Attribute(nil), base.Attributes...),
			genaiobs.Attribute{Key: "gen_ai.usage.input_tokens", Value: int64(999999)})
		pending := base
		pending.RequestID = "ledger-m10-batch-unfinalized"
		pending.Attributes = append(append([]genaiobs.Attribute(nil), base.Attributes...),
			genaiobs.Attribute{Key: "tenant.id", Value: tenantA})
		invalid := base
		invalid.Attributes = append(append([]genaiobs.Attribute(nil), base.Attributes...),
			genaiobs.Attribute{Key: "gen_ai.usage.input_tokens", Value: int64(-1)})
		if err := q.SubmitBatch(scopeB, genaiobs.Version, []genaiobs.Span{pending, invalid}); err != genaiobs.ErrInvalid || q.Len() != 1 {
			t.Fatalf("invalid batch must not admit its valid prefix: %v", err)
		}
		unchanged(t)
		if err := q.SubmitBatch(scopeB, genaiobs.Version, []genaiobs.Span{pending, replay, pending}); err != genaiobs.ErrQueueFull || q.Len() != 1 {
			t.Fatalf("overloaded batch must not admit its prefix: %v", err)
		}
		unchanged(t)
		if err := q.SubmitBatch(scopeB, genaiobs.Version, []genaiobs.Span{pending, replay}); err != nil || q.Len() != 3 {
			t.Fatalf("valid batch admission: %v", err)
		}
		unchanged(t)
		o, ok := q.Pop()
		if !ok || o.TenantID != tenantA || o.RequestID != base.RequestID || o.InputTokens != nil {
			t.Fatal("original observation changed")
		}
		unchanged(t)
		o, ok = q.Pop()
		if !ok || o.TenantID != tenantB || o.RequestID != pending.RequestID || o.InputTokens != nil {
			t.Fatal("pending observation scope or FIFO changed")
		}
		unchanged(t)
		o, ok = q.Pop()
		if !ok || o.TenantID != tenantB || o.RequestID != replay.RequestID || o.InputTokens == nil || *o.InputTokens != 999999 {
			t.Fatal("replayed observation scope or tokens changed")
		}
		unchanged(t)
		if _, ok := q.Pop(); ok || q.Len() != 0 {
			t.Fatal("batch queue not drained")
		}
	})
	t.Run("OTLP JSON projection rejection replay and drain", func(t *testing.T) {
		q, err := genaiobs.NewQueue(2)
		if err != nil {
			t.Fatal(err)
		}
		scope := genaiobs.Scope{TenantID: tenantB}
		const wire = `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"0123456789ABCDEF0123456789ABCDEF","spanId":"0123456789ABCDEF","status":{"code":1},"attributes":[{"key":"gen_ai.provider.name","value":{"stringValue":"openai"}},{"key":"gen_ai.operation.name","value":{"stringValue":"chat"}},{"key":"gen_ai.usage.input_tokens","value":{"intValue":"9007199254740993"}},{"key":"gen_ai.usage.output_tokens","value":{"intValue":0}},{"key":"tenant.id","value":{"stringValue":"ledger-m10-tenant-a"}},{"key":"gen_ai.input.messages","value":{"stringValue":"discard-me"}}]}]}]}]}`
		for _, body := range [][]byte{
			[]byte(strings.Replace(wire, `"code":1`, `"code":1,"code":2`, 1)),
			[]byte(strings.Replace(wire, `"intValue":"9007199254740993"`, `"intValue":"-1"`, 1)),
			make([]byte, genaiobs.MaxOTLPJSONBytes+1),
		} {
			if spans, err := genaiobs.DecodeOTLPJSON(body); err == nil || spans != nil || q.Len() != 0 {
				t.Fatalf("invalid wire batch accepted: %v", err)
			}
			unchanged(t)
		}
		spans, err := genaiobs.DecodeOTLPJSON([]byte(wire))
		if err != nil || len(spans) != 1 || len(spans[0].Attributes) != 4 {
			t.Fatalf("valid wire projection: %v", err)
		}
		unchanged(t)
		for i := 0; i < 2; i++ {
			if err := q.SubmitBatch(scope, genaiobs.Version, spans); err != nil {
				t.Fatal(err)
			}
			unchanged(t)
		}
		if err := q.SubmitBatch(scope, genaiobs.Version, spans); err != genaiobs.ErrQueueFull || q.Len() != 2 {
			t.Fatalf("expected overload: %v", err)
		}
		unchanged(t)
		for i := 0; i < 2; i++ {
			o, ok := q.Pop()
			if !ok || o.TenantID != tenantB || o.TraceID != "0123456789abcdef0123456789abcdef" || o.SpanID != "0123456789abcdef" || o.RequestID != "" || o.AttemptID != "" || o.Status != "ok" || o.InputTokens == nil || *o.InputTokens != 9007199254740993 || o.OutputTokens == nil || *o.OutputTokens != 0 {
				t.Fatal("wire projection changed scope, identity or integer precision")
			}
			unchanged(t)
		}
		if _, ok := q.Pop(); ok || q.Len() != 0 {
			t.Fatal("queue not drained")
		}
	})
	// The authoritative retry remains a duplicate with exactly the same rows.
	if result, err := store.Record(ctx, first); err != nil || result != Duplicate {
		t.Fatalf("authoritative retry changed semantics: %v %v", result, err)
	}
	unchanged(t)
}
