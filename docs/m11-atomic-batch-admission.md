# M11 slice 2a: atomic batch admission

Refs #56. Prerequisite for a bounded OTLP receiver; this is not an OTLP wire implementation.

SubmitBatch takes one trusted scope and 1–256 typed spans, validates every span, then holds the existing queue mutex for the capacity check and append. Invalid size/version/span or insufficient capacity leaves the queue unchanged. A valid batch enters FIFO in input order without interleaving other admissions. Single Submit and batch admission share the same normalized-field cloning path. No deduplication: a retry after an ambiguous successful admission can still duplicate observations.

Batch length is bounded independently of queue capacity. A batch larger than available space returns ErrQueueFull (including when larger than total capacity). The future transport must enforce body size, total concurrency, authentication, semantic policy and a compatible batch limit. It must distinguish permanently oversized requests from transient queue saturation. Empty batch handling at the wire layer is not defined here. No network endpoint, durable acknowledgement, retry, persistence, or ledger writer is added.

Tests cover invalid batch rollback, saturation rollback, existing FIFO preservation, unsupported version, invalid size and 32 concurrent two-span batches against 31 slots. Local isolated Go 1.23.2 RED (missing API) then GREEN for the three new tests with race, vet and build; full repo CI on the pinned compiler remains required. The prior real PostgreSQL regression covers single admission; batch-specific DB and actual collector transport evidence remain separate acceptance work.
