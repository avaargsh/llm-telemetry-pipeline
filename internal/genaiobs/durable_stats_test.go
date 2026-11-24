package genaiobs

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestDurableStoreStatsSeparatePendingTerminalAndCapacity(t *testing.T) {
	store, _ := openDurableTestStore(t, 8)
	ctx := context.Background()
	tenant := fmt.Sprintf("durable-test-%d", time.Now().UnixNano())
	first := durableTestObservation(tenant, "0123456789abcdef0123456789abcdef", 11)
	second := durableTestObservation(tenant, "1123456789abcdef0123456789abcdef", 12)
	if err := store.EnqueueBatch(ctx, []Observation{first, second}); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(ctx, "stats-worker", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%d err=%v", len(claimed), err)
	}
	if err := store.MarkTerminal(ctx, claimed[0], "stats-worker", "permanent_export_failure"); err != nil {
		t.Fatal(err)
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 2 || stats.Pending != 1 || stats.Terminal != 1 || stats.Capacity != 8 {
		t.Fatalf("unexpected durable queue stats: %+v", stats)
	}
	if stats.OldestPendingAgeSeconds < 0 {
		t.Fatalf("oldest pending age must be non-negative: %+v", stats)
	}
}
