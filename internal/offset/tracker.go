package offset

import (
	"errors"
	"fmt"
	"sync"
)

// Partition identifies a Kafka topic partition.
type Partition struct {
	Topic string
	ID    int32
}

type observed struct {
	offset int64
	acked  bool
}
type progress struct {
	generation    uint64
	records       []observed
	lastCommitted int64
	hasCommit     bool
}

// Tracker tracks the acknowledged prefix of records observed in fetch order.
// Kafka offsets may have holes: no arithmetic-contiguous offset requirement is imposed.
// Generation is a LOCAL fence. Broker-side commit must also use the current group ownership.
type Tracker struct {
	mu    sync.Mutex
	parts map[Partition]*progress
}

func NewTracker() *Tracker { return &Tracker{parts: make(map[Partition]*progress)} }

// Assign replaces the partition's in-flight state on a new generation.
func (t *Tracker) Assign(p Partition, generation uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if old := t.parts[p]; old != nil && old.generation > generation {
		return
	}
	t.parts[p] = &progress{generation: generation}
}

// Revoke invalidates in-flight acknowledgments on partition loss.
func (t *Tracker) Revoke(p Partition, generation uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if old := t.parts[p]; old != nil && old.generation == generation {
		delete(t.parts, p)
	}
}

func (t *Tracker) get(p Partition, generation uint64) (*progress, error) {
	part, ok := t.parts[p]
	if !ok || part.generation != generation {
		return nil, fmt.Errorf("stale or unassigned partition %s/%d generation %d", p.Topic, p.ID, generation)
	}
	return part, nil
}

// Observe must be called in Kafka poll order for each partition.
func (t *Tracker) Observe(p Partition, generation uint64, offset int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	pr, err := t.get(p, generation)
	if err != nil {
		return err
	}
	if offset < 0 {
		return errors.New("invalid negative offset")
	}
	if len(pr.records) > 0 && offset <= pr.records[len(pr.records)-1].offset {
		return errors.New("non-increasing observed offset")
	}
	if pr.hasCommit && offset < pr.lastCommitted {
		return errors.New("already committed offset observed without reassignment")
	}
	pr.records = append(pr.records, observed{offset: offset})
	return nil
}

func (t *Tracker) Ack(p Partition, generation uint64, offset int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	pr, err := t.get(p, generation)
	if err != nil {
		return err
	}
	for i := range pr.records {
		if pr.records[i].offset == offset {
			pr.records[i].acked = true
			return nil
		}
	}
	return fmt.Errorf("cannot acknowledge unobserved offset %d", offset)
}

func committable(pr *progress) (int64, bool) {
	var next int64
	ok := false
	for _, record := range pr.records {
		if !record.acked {
			break
		}
		next = record.offset + 1 // Kafka commit API accepts the NEXT read position.
		ok = true
	}
	if !ok || pr.hasCommit && next <= pr.lastCommitted {
		return 0, false
	}
	return next, true
}

func (t *Tracker) Committable(p Partition, generation uint64) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	pr, err := t.get(p, generation)
	if err != nil {
		return 0, false
	}
	return committable(pr)
}

// ConfirmCommit is called ONLY after Kafka confirms a commit at nextOffset.
func (t *Tracker) ConfirmCommit(p Partition, generation uint64, nextOffset int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	pr, err := t.get(p, generation)
	if err != nil {
		return err
	}
	if nextOffset < 0 || pr.hasCommit && nextOffset <= pr.lastCommitted {
		return errors.New("invalid or non-increasing committed offset")
	}
	prefix := 0
	found := false
	for _, record := range pr.records {
		if !record.acked {
			break
		}
		prefix++
		if record.offset+1 == nextOffset {
			found = true
			break
		}
	}
	if !found {
		return errors.New("cannot confirm unacknowledged offset range")
	}
	pr.records = append([]observed(nil), pr.records[prefix:]...)
	pr.hasCommit = true
	pr.lastCommitted = nextOffset
	return nil
}
