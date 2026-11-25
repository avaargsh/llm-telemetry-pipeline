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
		if err := verifyPriceRegistryPrivileges(ctx, tx); err != nil {
			return "", err
		}
		return queryPriceVersionFingerprint(ctx, tx, tenant, project, version)
	})
}
