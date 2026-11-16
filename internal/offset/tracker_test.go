package offset

import "testing"

var partition = Partition{Topic: "usage.critical.v1", ID: 0}

func TestCommitOnlyAcrossObservedAcknowledgedPrefix(t *testing.T) {
	tr := NewTracker()
	tr.Assign(partition, 1)
	for _, offset := range []int64{100, 101, 102} {
		if err := tr.Observe(partition, 1, offset); err != nil {
			t.Fatal(err)
		}
	}
	if err := tr.Ack(partition, 1, 102); err != nil {
		t.Fatal(err)
	}
	if _, ok := tr.Committable(partition, 1); ok {
		t.Fatal("cannot skip an unacknowledged earlier record")
	}
	if err := tr.Ack(partition, 1, 100); err != nil {
		t.Fatal(err)
	}
	if next, ok := tr.Committable(partition, 1); !ok || next != 101 {
		t.Fatalf("expected next=101, got %d %t", next, ok)
	}
	if err := tr.Ack(partition, 1, 101); err != nil {
		t.Fatal(err)
	}
	if next, ok := tr.Committable(partition, 1); !ok || next != 103 {
		t.Fatalf("expected next=103, got %d %t", next, ok)
	}
	if err := tr.ConfirmCommit(partition, 1, 103); err != nil {
		t.Fatal(err)
	}
	if _, ok := tr.Committable(partition, 1); ok {
		t.Fatal("already committed prefix must not be recommitted")
	}
}

func TestKafkaOffsetHolesAreAllowed(t *testing.T) {
	tr := NewTracker()
	tr.Assign(partition, 1)
	for _, value := range []int64{100, 105} {
		if err := tr.Observe(partition, 1, value); err != nil {
			t.Fatal(err)
		}
		if err := tr.Ack(partition, 1, value); err != nil {
			t.Fatal(err)
		}
	}
	if next, ok := tr.Committable(partition, 1); !ok || next != 106 {
		t.Fatalf("expected 106 despite offset gap; got %d %t", next, ok)
	}
}

func TestOldGenerationIsFencedAfterRebalance(t *testing.T) {
	tr := NewTracker()
	tr.Assign(partition, 10)
	if err := tr.Observe(partition, 10, 42); err != nil {
		t.Fatal(err)
	}
	tr.Assign(partition, 11)
	if err := tr.Ack(partition, 10, 42); err == nil {
		t.Fatal("stale worker acknowledged record after partition revocation")
	}
	if _, ok := tr.Committable(partition, 10); ok {
		t.Fatal("old generation must never commit")
	}
	if err := tr.Observe(partition, 11, 42); err != nil {
		t.Fatal(err)
	}
	if err := tr.Ack(partition, 11, 42); err != nil {
		t.Fatal(err)
	}
	if next, ok := tr.Committable(partition, 11); !ok || next != 43 {
		t.Fatalf("new generation should own offset 42")
	}
}

func TestUnknownOffsetAndOutOfOrderObservationAreRejected(t *testing.T) {
	tr := NewTracker()
	tr.Assign(partition, 1)
	if err := tr.Ack(partition, 1, 3); err == nil {
		t.Fatal("cannot ack unknown record")
	}
	if err := tr.Observe(partition, 1, 5); err != nil {
		t.Fatal(err)
	}
	if err := tr.Observe(partition, 1, 3); err == nil {
		t.Fatal("consumer records must be observed in partition order")
	}
	if err := tr.Ack(partition, 1, 5); err != nil {
		t.Fatal(err)
	}
	if err := tr.ConfirmCommit(partition, 1, 7); err == nil {
		t.Fatal("cannot confirm never committed offset")
	}
}
