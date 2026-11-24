# M11 slice 1: bounded observation admission

Refs #56. This is an internal volatile FIFO, **not an OTLP receiver**, collector exporter, durable queue, or acknowledgment protocol.

Construct with NewQueue(capacity), between 1 and 4096 observations inclusive. Submit accepts a trusted M10 Scope, version and typed Span. It validates through NormalizeInference, then admits the normalized observation under a mutex. Saturation returns ErrQueueFull without eviction; invalid data retains nothing. Callers distinguish saturation from validation errors. Pop transfers one observation in FIFO admission order and clears the slot; Len is a momentary count, not a reservation. Do not copy the queue after use. Input must not be mutated concurrently with Submit.

Only normalized bounded fields are retained. Retained strings are cloned to avoid pinning a larger raw input backing buffer. Raw attributes are not stored. The capacity is a count limit per queue, not a total process memory or request-concurrency limit. Admission may briefly contend for the mutex. Replays are independent observations; no deduplication or billing semantics. A shared queue offers no per-tenant fairness guarantee.

Submit success means in-memory admission only. Pop is destructive and does not mean downstream delivery. Process exit, restart or caller failure after Pop can lose observations. No retry, persistence, network listener, authentication, metrics, sampling, or ledger write path is introduced here. External ingress must authenticate scope and enforce wire size, concurrency and attribute content policy. Future slices must define exporter acknowledgment/retry and restart behavior before exposing any delivery guarantee.

Tests exercise invalid capacity, FIFO wraparound, full rejection, trusted scope, changed replay observations, rejection before admission and concurrent admission/drain. The existing M10 real ledger test remains a regression gate; this slice does not itself prove actual OTLP or collector-to-ledger isolation.
