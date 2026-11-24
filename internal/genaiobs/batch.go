package genaiobs

import "errors"

const MaxBatchSize = 256

var ErrInvalidBatchSize = errors.New("invalid observation batch size")

// SubmitBatch validates every span before admission, then admits the entire
// batch in order or rejects it without changing the queue. It uses one trusted
// scope per batch. Success is volatile admission, not delivery or deduplication.
// The caller must not mutate inputs concurrently with this call.
func (q *Queue) SubmitBatch(scope Scope, version string, spans []Span) error {
	if len(spans) == 0 || len(spans) > MaxBatchSize {
		return ErrInvalidBatchSize
	}
	observations := make([]Observation, len(spans))
	for i, span := range spans {
		o, err := NormalizeInference(scope, version, span)
		if err != nil {
			return err
		}
		observations[i] = o
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(observations) > len(q.slots)-q.size {
		return ErrQueueFull
	}
	for _, o := range observations {
		q.appendLocked(o)
	}
	return nil
}
