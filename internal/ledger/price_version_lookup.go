package ledger

import (
	"context"
	"database/sql"
	"errors"
)

var (
	ErrInvalidPriceVersion = errors.New("invalid price version")
	ErrPriceVersionNotFound = errors.New("registered price version not found")
)

// PriceVersionFingerprint returns the exact registered binding, not a tariff.
func (s *ScopedUsageReader) PriceVersionFingerprint(ctx context.Context, tenant, project, version string) (string, error) {
	if !validPriceVersionLookup(version) {
		return "", ErrInvalidPriceVersion
	}
	return scopedRead(ctx, s, tenant, project, func(tx *sql.Tx) (string, error) {
		var restricted bool
		err := tx.QueryRowContext(ctx, "SELECT NOT (has_table_privilege(session_user, 'public.price_version_registry', 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') OR has_any_column_privilege(session_user, 'public.price_version_registry', 'SELECT,INSERT,UPDATE,REFERENCES'))").Scan(&restricted)
		if err != nil || !restricted {
			return "", ErrUsageQueryScope
		}
		var fingerprint string
		err = tx.QueryRowContext(ctx, "SELECT price_fingerprint FROM public.usage_query_price_versions WHERE tenant_id=$1 AND project_id=$2 AND price_version=$3", tenant, project, version).Scan(&fingerprint)
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrPriceVersionNotFound
		}
		if err != nil {
			return "", err
		}
		return fingerprint, nil
	})
}
