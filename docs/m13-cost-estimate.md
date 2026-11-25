# M13.1: offline model cost estimation

This first FinOps slice prices a previously captured, authoritative M12
model usage breakdown with an explicitly selected price snapshot. It is
an estimate, not an invoice, provider price feed, historical rate table,
or automatic charge. It does not alter the ledger or call an external API.

## Reproduce the example

Save this synthetic model breakdown as usage.json:

```json
[{"model":"Qwen3-32B","request_count":2,"prompt_tokens":"408","completion_tokens":"58","total_tokens":"466"}]
```

Save the following **fictional example rates**, not actual provider prices,
as prices.json:

```json
{"version":"example-v1","currency":"USD","rates":[{"model":"Qwen3-32B","input_per_million":"2.5","output_per_million":"10"}]}
```

```sh
go run ./cmd/usage-cost-estimate --usage usage.json --prices prices.json
```

Expected output:

```json
{
  "currency": "USD",
  "price_version": "example-v1",
  "price_fingerprint": "sha256:433d2e0b10d8481699f2a2353f26bef71f057dd2cb08dc7069c026c0d7cc3cd5",
  "models": [
    {"model":"Qwen3-32B","input_cost":"0.001020000000","output_cost":"0.000580000000","total_cost":"0.001600000000"}
  ],
  "total_cost": "0.001600000000"
}
```

For real estimates, save a successful response from the authenticated
M12 /v1/usage-summary/models endpoint and supply the applicable rates.
Do not store bearer tokens or private usage snapshots in Git. Retain the
usage scope, window, source response and exact price file separately for
reproducibility; the estimator does not certify their provenance.

## Contract and limitations

- One explicit USD price snapshot, a nonblank version (max 128 bytes),
  up to 1,000 distinct model rates and 100 usage rows.
- Input and output rates are independent nonnegative decimal strings
  per million tokens, at most 18 integer digits and six fractional
  digits. Zero must be explicit. No implicit rate, exponent or rounding.
- Token totals are unsigned canonical decimal strings, at most 40 digits;
  input plus output must equal total. Each model row has a positive
  request count and a unique exact model name (max 512 bytes).
- Cost is integer token count multiplied by integer microdollars per
  million, represented as an exact decimal string with twelve fractional
  digits. No floating point or integer overflow at the supported bounds.
- Missing rates, duplicate models or rates, inconsistent token totals,
  invalid amounts and unsupported currencies fail the entire calculation.
  A genuinely empty usage array returns zero; missing data is not zero.
- CLI accepts two regular JSON files, at most 1 MiB each, nesting at most
  16, no duplicate object keys (case-insensitive), unknown fields or
  trailing JSON. Validation errors emit no estimate and exit nonzero.
- Rates are applied uniformly to the entire supplied window. Do not use
  a single snapshot across a historical price change unless intentionally
  doing a scenario estimate. No taxes, discounts, cache-specific tokens,
  FX conversion, provider identity, invoice rounding or budget enforcement
  are supported in this slice.

## Verification

```sh
go test ./internal/finops ./cmd/usage-cost-estimate
go test -race ./...
```

Tests include exact sub-cent costs, token totals beyond uint64,
explicit zero rates, missing prices, duplicate and invalid inputs,
and the file-based command entry point. This is not production billing.

## Price content identity (M13.6)

Every successful estimate includes an additive `price_fingerprint` field.
It binds the complete validated rate table, including unused models, rather
than just the caller-provided price version. Equivalent decimal spellings,
rate order, version labels and usage inputs do not change the fingerprint.
Model names are exact (not case-folded or Unicode-normalized).

Canonical v1 encoding: concatenate each field as its decimal **byte** length,
a colon, then the exact field bytes. Fields, in order, are the domain tag
`llm-telemetry/price-table/v1`, currency, decimal rate count, then each exact
model name, input rate and output rate. Sort model names by Go string byte
order; encode rates as canonical nonnegative integer microdollars per million
tokens. Hash the resulting bytes using SHA-256 and render lowercase hex with
a `sha256:` prefix. A fixed vector appears in the example above.

This identifies effective price content; it is not a digest of the original
JSON bytes, an immutable version registry, a signature, a proof of provider
price authenticity, or an invoice. Retain the original source and version
label; a future catalog must enforce version-to-content binding separately.

## Optional version-content verification (M13.7)

An independently maintained JSON manifest can pin each price version to
its expected canonical content fingerprint. For the fictional example above,
save the following as bindings.json:

```json
[{"version":"example-v1","price_fingerprint":"sha256:433d2e0b10d8481699f2a2353f26bef71f057dd2cb08dc7069c026c0d7cc3cd5"}]
```

```sh
go run ./cmd/usage-cost-estimate --usage usage.json --prices prices.json \\
  --price-bindings bindings.json
```

When the option is supplied, the entire manifest must be a valid non-null
array with at most 1,000 distinct exact version labels, each paired with a
canonical lowercase `sha256:` digest. Missing versions, digest mismatches,
duplicate labels (even identical bindings) and malformed entries fail closed
with no estimate. Unused valid entries are allowed. The existing strict JSON
file limits apply. The option is optional for backwards compatibility; its
absence does not verify version ownership. Keep the trusted manifest separate
from the price file under independent change control. This is verification
against caller-supplied expectations, not a persisted registry, source
signature, access-control boundary or proof of tariff authenticity.
