package event

import (
	"testing"
	"time"
)

func validEvent() UsageEvent {
	return UsageEvent{
		EventID:   "123e4567-e89b-42d3-a456-426614174000",
		EventType: "usage.finalized", SchemaVersion: 1,
		TenantID: "tenant_1", ProjectID: "project_1", RequestID: "req_1",
		Model: "Qwen3-32B", PromptTokens: 1024, CompletionTokens: 512,
		LatencyMS: 2350, Status: "success",
		Timestamp: time.Date(2026, 11, 16, 11, 0, 0, 0, time.UTC),
	}
}

func TestUsageEventDigestIsDeterministic(t *testing.T) {
	first := validEvent()
	if err := first.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := first.Validate(); err != nil {
		t.Fatal(err)
	}
	second := validEvent()
	if err := second.Seal(); err != nil {
		t.Fatal(err)
	}
	if first.PayloadHash != second.PayloadHash {
		t.Fatalf("same immutable event produced distinct hashes")
	}
}

func TestPayloadTamperingIsRejected(t *testing.T) {
	e := validEvent()
	if err := e.Seal(); err != nil {
		t.Fatal(err)
	}
	e.CompletionTokens++
	if err := e.Validate(); err == nil {
		t.Fatal("mutated event must fail hash validation")
	}
}

func TestUnknownSchemaAndBadIDsAreRejected(t *testing.T) {
	for _, mutate := range []func(*UsageEvent){
		func(e *UsageEvent) { e.SchemaVersion = 2 },
		func(e *UsageEvent) { e.EventID = "not-a-uuid" },
		func(e *UsageEvent) { e.TenantID = "" },
		func(e *UsageEvent) { e.PromptTokens = -1 },
		func(e *UsageEvent) { e.Timestamp = time.Time{} },
	} {
		e := validEvent()
		mutate(&e)
		if err := e.Seal(); err == nil {
			t.Fatal("invalid event unexpectedly accepted")
		}
	}
}

func TestStableSyntheticEventOnRetry(t *testing.T) {
	start := time.Date(2026, 11, 16, 11, 0, 0, 0, time.UTC)
	a, err := SyntheticUsageEvent("run-1", 4, start)
	if err != nil {
		t.Fatal(err)
	}
	b, err := SyntheticUsageEvent("run-1", 4, start)
	if err != nil {
		t.Fatal(err)
	}
	if a.EventID != b.EventID || a.PayloadHash != b.PayloadHash {
		t.Fatal("retry must be bitwise deterministic at the event layer")
	}
	c, err := SyntheticUsageEvent("run-1", 5, start)
	if err != nil {
		t.Fatal(err)
	}
	if a.EventID == c.EventID {
		t.Fatal("different logical requests must have different IDs")
	}
}
