package ledger

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

type PriceVersionExpectation struct {
	Version     string
	Fingerprint string
}

var (
	ErrInvalidPriceExpectation = errors.New("invalid price version expectations")
	ErrPriceVersionMismatch = errors.New("registered price version fingerprint mismatch")
)

func validatePriceExpectations(expectations []PriceVersionExpectation) error {
	if len(expectations) == 0 || len(expectations) > MaxModelQueryWindows {
		return ErrInvalidPriceExpectation
	}
	seen := make(map[string]bool, len(expectations))
	for _, item := range expectations {
		if !validPriceVersionLookup(item.Version) || seen[item.Version] ||
			len(item.Fingerprint) != 71 || !strings.HasPrefix(item.Fingerprint, "sha256:") {
			return ErrInvalidPriceExpectation
		}
		for _, c := range item.Fingerprint[7:] {
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return ErrInvalidPriceExpectation
			}
		}
		seen[item.Version] = true
	}
	return nil
}

// VerifiedUsageByModelWindows checks every supplied expected price binding
// and captures usage in one scoped read-only repeatable-read transaction.
// Expectations are explicit caller claims, not a persisted rate schedule.
func (s *ScopedUsageReader) VerifiedUsageByModelWindows(ctx context.Context, tenant, project string, windows []ModelQueryWindow, expectations []PriceVersionExpectation) ([]ModelWindowCapture, error) {
	if err := validateModelQueryWindows(windows); err != nil {
		return nil, err
	}
	if err := validatePriceExpectations(expectations); err != nil {
		return nil, err
	}
	return scopedRead(ctx, s, tenant, project, func(tx *sql.Tx) ([]ModelWindowCapture, error) {
		if err := verifyPriceRegistryPrivileges(ctx, tx); err != nil {
			return nil, err
		}
		for _, item := range expectations {
			fp, err := queryPriceVersionFingerprint(ctx, tx, tenant, project, item.Version)
			if err != nil {
				return nil, err
			}
			if fp != item.Fingerprint {
				return nil, ErrPriceVersionMismatch
			}
		}
		return captureModelWindows(ctx, tx, tenant, project, windows)
	})
}
