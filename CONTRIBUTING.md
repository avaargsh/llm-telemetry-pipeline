# Contributing

Thank you for your interest in the telemetry pipeline.

**Current contribution status: not open for external code contributions.**
The public repository does not yet include an explicit open-source license.
See [OSS readiness](docs/oss-readiness.md) and [Issue #26](https://github.com/avaargsh/llm-telemetry-pipeline/issues/26).
Please do not assume rights to reuse/redistribute the source or submit
third-party code while the maintainer is completing ownership and license
review. Non-sensitive bug reports and reproducible issue descriptions
are welcome.

For maintainers working on the repository itself:

1. Preserve PostgreSQL Ledger/Outbox as the authoritative accounting path.
2. Start with failing focused tests before changing failure, replay, ACK,
   authorization, offset or leasing behavior.
3. Use the existing `make test`, `make test-race`, and the relevant
   disposable Docker smoke command. A unit test alone never proves a real
   Kafka/ClickHouse fault envelope.
4. Archive redacted evidence with the exact tested Git HEAD. Keep fault
   tests, scenario SLOs and production reliability claims separate.
5. Never commit credentials, real usage payloads, company/customer source,
   or CI environment exports. Local example credentials are **not**
   deployment-safe defaults.

A complete external contribution and licensing policy will be published
only after Issue #26 is resolved.
