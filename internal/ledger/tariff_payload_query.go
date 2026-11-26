package ledger

import (
	"context"
	"database/sql"
	"errors"
)

var ErrPriceTariffPayloadNotFound = errors.New("registered price tariff payload not found")

type PriceTariffPayloadRecord struct {
	Version       string
	Fingerprint   string
	SchemaVersion int16
	Currency      string
	RatesJSON     []byte
}

// PriceTariffPayload reads only the payload visible through the authenticated
// project view. The login receives no direct grant on the base catalog table.
func (s *ScopedUsageReader) PriceTariffPayload(ctx context.Context, tenant, project, version string) (PriceTariffPayloadRecord, error) {
	if !validPriceVersionLookup(version) {
		return PriceTariffPayloadRecord{}, ErrInvalidPriceVersion
	}
	return scopedRead(ctx, s, tenant, project, func(tx *sql.Tx) (PriceTariffPayloadRecord, error) {
		if err := verifyPriceRegistryPrivileges(ctx, tx); err != nil {
			return PriceTariffPayloadRecord{}, err
		}
		if err := verifyPriceTariffPayloadPrivileges(ctx, tx); err != nil {
			return PriceTariffPayloadRecord{}, err
		}

		var record PriceTariffPayloadRecord
		err := tx.QueryRowContext(ctx, `
SELECT price_version, price_fingerprint, payload_schema_version, currency, rates
FROM public.usage_query_price_tariffs
WHERE tenant_id=$1 AND project_id=$2 AND price_version=$3`,
			tenant, project, version).Scan(
			&record.Version, &record.Fingerprint, &record.SchemaVersion,
			&record.Currency, &record.RatesJSON)
		if errors.Is(err, sql.ErrNoRows) {
			return PriceTariffPayloadRecord{}, ErrPriceTariffPayloadNotFound
		}
		if err != nil {
			return PriceTariffPayloadRecord{}, err
		}
		if record.SchemaVersion != 1 {
			return PriceTariffPayloadRecord{}, ErrPriceTariffPayloadNotFound
		}
		return record, nil
	})
}

func verifyPriceTariffPayloadPrivileges(ctx context.Context, tx *sql.Tx) error {
	var restricted bool
	err := tx.QueryRowContext(ctx, `
SELECT NOT (
    has_table_privilege(session_user, 'public.price_tariff_payloads',
        'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
    OR has_any_column_privilege(session_user, 'public.price_tariff_payloads',
        'SELECT,INSERT,UPDATE,REFERENCES')
)`).Scan(&restricted)
	if err != nil || !restricted {
		return ErrUsageQueryScope
	}
	return nil
}
