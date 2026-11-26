package finops

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

var (
	ErrTariffPayloadConflict = errors.New("immutable tariff payload already exists with different content")
	ErrTariffCatalogScope = errors.New("invalid tariff catalog scope")
)

// RegisterTariffPayload provisions a complete canonical payload using a
// trusted database owner connection. Runtime query logins must never receive
// write access to either the registry or payload table.
func RegisterTariffPayload(ctx context.Context, db *sql.DB, tenant, project string, table PriceTable) (TariffPayload, error) {
	if db == nil || !validStorageIdentity(tenant) || !validStorageIdentity(project) ||
		strings.TrimSpace(tenant) == "" || strings.TrimSpace(project) == "" ||
		len(tenant) > 1024 || len(project) > 1024 {
		return TariffPayload{}, ErrTariffCatalogScope
	}

	payload, err := CanonicalTariffPayload(table)
	if err != nil {
		return TariffPayload{}, err
	}
	ratesJSON, err := json.Marshal(payload.Table.Rates)
	if err != nil {
		return TariffPayload{}, err
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return TariffPayload{}, err
	}
	defer tx.Rollback()

	var registeredFingerprint string
	err = tx.QueryRowContext(ctx, `
SELECT price_fingerprint
FROM public.price_version_registry
WHERE tenant_id=$1 AND project_id=$2 AND price_version=$3
FOR KEY SHARE`, tenant, project, payload.Table.Version).Scan(&registeredFingerprint)
	if errors.Is(err, sql.ErrNoRows) {
		return TariffPayload{}, ledger.ErrPriceVersionNotFound
	}
	if err != nil {
		return TariffPayload{}, err
	}
	if registeredFingerprint != payload.Fingerprint {
		return TariffPayload{}, ledger.ErrPriceVersionMismatch
	}

	_, err = tx.ExecContext(ctx, `
INSERT INTO public.price_tariff_payloads
    (tenant_id, project_id, price_version, price_fingerprint,
     payload_schema_version, currency, rates)
VALUES ($1, $2, $3, $4, 1, $5, $6::jsonb)
ON CONFLICT (tenant_id, project_id, price_version) DO NOTHING`,
		tenant, project, payload.Table.Version, payload.Fingerprint,
		payload.Table.Currency, string(ratesJSON))
	if err != nil {
		return TariffPayload{}, err
	}

	var identical bool
	err = tx.QueryRowContext(ctx, `
SELECT price_fingerprint=$4
   AND payload_schema_version=1
   AND currency=$5
   AND rates=$6::jsonb
FROM public.price_tariff_payloads
WHERE tenant_id=$1 AND project_id=$2 AND price_version=$3`,
		tenant, project, payload.Table.Version, payload.Fingerprint,
		payload.Table.Currency, string(ratesJSON)).Scan(&identical)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !identical) {
		return TariffPayload{}, ErrTariffPayloadConflict
	}
	if err != nil {
		return TariffPayload{}, err
	}
	if err := tx.Commit(); err != nil {
		return TariffPayload{}, err
	}
	return payload, nil
}

// TariffPayloadReader is implemented by the database-scoped ledger reader.
type TariffPayloadReader interface {
	PriceTariffPayload(context.Context, string, string, string) (ledger.PriceTariffPayloadRecord, error)
}

// LoadRegisteredTariff reads a project-scoped immutable payload and
// independently recomputes its canonical fingerprint before returning it.
func LoadRegisteredTariff(ctx context.Context, reader TariffPayloadReader, tenant, project, version string) (TariffPayload, error) {
	if reader == nil || !validStorageIdentity(version) || strings.TrimSpace(version) == "" {
		return TariffPayload{}, ledger.ErrInvalidPriceVersion
	}
	record, err := reader.PriceTariffPayload(ctx, tenant, project, version)
	if err != nil {
		return TariffPayload{}, err
	}
	var rates []ModelRate
	if err := json.Unmarshal(record.RatesJSON, &rates); err != nil || rates == nil {
		return TariffPayload{}, ErrInvalidEstimate
	}
	payload, err := CanonicalTariffPayload(PriceTable{
		Version: record.Version,
		Currency: record.Currency,
		Rates: rates,
	})
	if err != nil {
		return TariffPayload{}, err
	}
	if record.Version != version || payload.Fingerprint != record.Fingerprint {
		return TariffPayload{}, ledger.ErrPriceVersionMismatch
	}
	return payload, nil
}
