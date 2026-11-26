package ledger

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var ErrInvalidPricePeriodWindow = errors.New("invalid tariff effective-period query window")

type PriceEffectivePeriodRecord struct {
	PublicationID   int64
	PriceVersion    string
	PriceFingerprint string
	EffectiveFrom   time.Time
	EffectiveTo     time.Time
	Reason          string
	SourceReference string
	PublishedAt     time.Time
	PublishedBy     string
}

// PriceEffectivePeriods returns only publications intersecting the requested
// bounded interval and visible to the authenticated tenant/project login.
func (s *ScopedUsageReader) PriceEffectivePeriods(ctx context.Context, tenant, project string, from, to time.Time) ([]PriceEffectivePeriodRecord, error) {
	if !validLedgerPriceInstant(from) || !validLedgerPriceInstant(to) ||
		!from.Before(to) || to.Sub(from) > MaxUsageWindow {
		return nil, ErrInvalidPricePeriodWindow
	}
	return scopedRead(ctx, s, tenant, project, func(tx *sql.Tx) ([]PriceEffectivePeriodRecord, error) {
		if err := verifyPriceRegistryPrivileges(ctx, tx); err != nil {
			return nil, err
		}
		if err := verifyPriceTariffPayloadPrivileges(ctx, tx); err != nil {
			return nil, err
		}
		if err := verifyPriceEffectivePeriodPrivileges(ctx, tx); err != nil {
			return nil, err
		}

		rows, err := tx.QueryContext(ctx, `
SELECT publication_id, price_version, price_fingerprint,
       effective_from, effective_to, publication_reason, source_reference,
       published_at, published_by
FROM public.usage_query_price_periods
WHERE tenant_id=$1 AND project_id=$2
  AND effective_from < $4 AND effective_to > $3
ORDER BY effective_from, publication_id`,
			tenant, project, from.UTC(), to.UTC())
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		result := make([]PriceEffectivePeriodRecord, 0)
		for rows.Next() {
			var record PriceEffectivePeriodRecord
			if err := rows.Scan(
				&record.PublicationID, &record.PriceVersion, &record.PriceFingerprint,
				&record.EffectiveFrom, &record.EffectiveTo, &record.Reason,
				&record.SourceReference, &record.PublishedAt, &record.PublishedBy); err != nil {
				return nil, err
			}
			result = append(result, record)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return result, nil
	})
}

func validLedgerPriceInstant(t time.Time) bool {
	return !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999 && t.Nanosecond()%1000 == 0
}

func verifyPriceEffectivePeriodPrivileges(ctx context.Context, tx *sql.Tx) error {
	var restricted bool
	err := tx.QueryRowContext(ctx, `
SELECT NOT (
    has_table_privilege(session_user, 'public.price_tariff_effective_periods',
        'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
    OR has_any_column_privilege(session_user, 'public.price_tariff_effective_periods',
        'SELECT,INSERT,UPDATE,REFERENCES')
)`).Scan(&restricted)
	if err != nil || !restricted {
		return ErrUsageQueryScope
	}
	return nil
}
