# M13.2: historical price-window estimation core

This additive internal Go package extension partitions an authoritative
event-time query into explicit historical price intervals and estimates
each captured interval independently. It does not change M12, the
ledger, or the existing offline M13.1 CLI.

## Contract

- A requested window is positive, at most 31 days, half-open [from,to),
  with microsecond-aligned endpoints. Times are normalized to UTC.
- A schedule contains at most 128 explicitly bounded, chronological,
  non-overlapping price periods, each with a unique version and valid
  M13.1 USD rate table. Gaps outside the requested window are allowed;
  a gap inside the window rejects the whole estimate.
- PlanPriceWindows returns the clipped, contiguous windows and the
  applicable price version for each. The earlier rate excludes its end;
  the next rate includes its start.
- EstimateHistorical requires exactly one model breakdown for every
  planned window, in order, with exactly matching boundaries.
  An explicit empty model array is valid; a missing/null capture is not.
- Every window uses M13.1 exact integer arithmetic and validation.
  The overall total is summed in integer trillionths of a dollar.
  Missing model rates or invalid input reject the whole estimate.

## Reproducible synthetic case

For one model, a fictional USD input rate of 1 per million tokens applies
before 01:00 UTC, and 2 per million from 01:00 UTC. Output rates are
explicitly zero. Query [00:00,02:00) on 2026-11-21.

| Captured UTC interval | Input tokens | Version | Estimated USD |
| --- | ---: | --- | ---: |
| [00:00,01:00) | 100 | v1 | 0.000100000000 |
| [01:00,02:00) | 200 | v2 | 0.000400000000 |
| Total | 300 | both | 0.000500000000 |

A single aggregate of 300 tokens for the entire two-hour period is
insufficient: the distribution across the rate boundary is unknown.
The implementation rejects that capture instead of using the latest
rate or prorating usage by elapsed time.

## Verification

```sh
go test ./internal/finops -run Historical -v
go test -race ./...
```

The tests cover clipped intervals, exact boundary ownership, rate
changes, explicit zero usage, gaps, overlaps, duplicate versions,
submicrosecond endpoints and missing/mismatched captures.

## Remaining integration

This slice is a pure calculation and planning core, not a historical
pricing service. A future query integration must capture every planned
window from the same scoped ledger snapshot, preserve tenant/project
identity and evidence, validate the schedule's provenance, and expose
a documented user-facing command or API. Separately captured HTTP
responses can observe late arrivals at different times; this core
cannot certify capture completeness, snapshot consistency or genuine
historical rates. It does not persist schedules or enforce immutable
versions across deployments. Do not use it as an invoice or automatic
charge mechanism.
