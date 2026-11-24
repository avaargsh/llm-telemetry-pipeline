# M10 first slice: GenAI observation contract

Refs #52. This is a local typed projection, **not an OTLP receiver or billing API**.

Pin [GenAI semantic conventions v0.1.0](https://github.com/open-telemetry/semantic-conventions-genai/releases/tag/v0.1.0) and its [client inference span specification](https://github.com/open-telemetry/semantic-conventions-genai/blob/v0.1.0/docs/gen-ai/client-inference.md). General semantic conventions dependency: v1.45.0. The fields below are Development status in the pinned GenAI version; local schema identifier is `genai-semconv/v0.1.0`. Do not silently map legacy names or mutable main.

| Input | Projection |
| --- | --- |
| gen_ai.provider.name | provider (required) |
| gen_ai.operation.name | operation (required; local inference subset only) |
| gen_ai.request.model | request_model (optional) |
| gen_ai.response.model | response_model (optional) |
| gen_ai.usage.input_tokens | input_tokens (optional nonnegative int64) |
| gen_ai.usage.output_tokens | output_tokens (optional nonnegative int64) |

Trace/span IDs must be canonical lowercase nonzero hex. Request/attempt IDs are optional local caller-provided correlation fields, not standard GenAI attributes and not billing identities. Status is a local normalized enum. Tenant scope must be established by authenticated ingress; this package does not authenticate. Bounds: 64 attributes, 128-byte keys, 256-byte allowlisted identifiers, 128-byte scope and local correlation identifiers. Unknown attribute values are discarded. Duplicate mapped attributes, unsupported versions, malformed IDs and invalid token types fail closed without echoing raw data. Identifier validation is a bounded local subset, not comprehensive secret detection or full upstream conformance. Sensitive contents must also be filtered at ingress.

Missing tokens remain absent, explicit zero remains zero. No summing, inference, deduplication, charge creation, or finalization. The adapter imports only the Go standard library, has no sink/writer, and does not alter the existing authenticated Usage API, PostgreSQL Ledger/Outbox, event digest or idempotency rules. Sampling, drops and replays are observations only. Do not emit tenant/run/trace IDs as metric labels.

Fixtures are synthetic, derived from the tagged attribute mapping, not captured customer data. Local tests cover valid correlation, tenant-attribute spoofing, missing versus zero, replay isolation, invalid IDs, duplicates, bounds and token types. Full repository CI and a real Ledger invariance integration proof are required before closing #52. OTLP transport/backpressure, persistence, runtime correlation, and production SLOs are out of scope.
