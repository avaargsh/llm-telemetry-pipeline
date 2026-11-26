package finops

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrInvalidPricePeriod = errors.New("invalid tariff effective period")
	ErrPricePeriodOverlap = errors.New("tariff effective period overlaps an existing publication")
)

type PricePeriodPublication struct {
	TenantID      string
	ProjectID     string
	PriceVersion  string
	EffectiveFrom time.Time
	EffectiveTo   time.Time
	Reason        string
	SourceReference string
}

type PublishedPricePeriod struct {
	PublicationID int64
	TenantID      string
	ProjectID     string
	PriceVersion  string
	PriceFingerprint string
	EffectiveFrom time.Time
	EffectiveTo   time.Time
	Reason        string
	SourceReference string
	PublishedAt   time.Time
	PublishedBy   string
}

func validTariffInstant(t time.Time) bool {
	return !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999 && t.Nanosecond()%1000 == 0
}

// PublishTariffPeriod appends an immutable half-open [from,to) effective
// interval for an already registered payload. The database exclusion
// constraint serializes concurrent overlapping publications per project.
func PublishTariffPeriod(ctx context.Context, db *sql.DB, publication PricePeriodPublication) (PublishedPricePeriod, error) {
	if db == nil ||
		!validStorageIdentity(publication.TenantID) ||
		!validStorageIdentity(publication.ProjectID) ||
		!validStorageIdentity(publication.PriceVersion) ||
		strings.TrimSpace(publication.TenantID) == "" ||
		strings.TrimSpace(publication.ProjectID) == "" ||
		strings.TrimSpace(publication.PriceVersion) == "" ||
		len(publication.TenantID) > 1024 || len(publication.ProjectID) > 1024 ||
		len(publication.PriceVersion) > 128 ||
		!validTariffInstant(publication.EffectiveFrom) ||
		!validTariffInstant(publication.EffectiveTo) ||
		!publication.EffectiveFrom.Before(publication.EffectiveTo) ||
		!validStorageIdentity(publication.Reason) ||
		strings.TrimSpace(publication.Reason) == "" || len(publication.Reason) > 1024 ||
		!validStorageIdentity(publication.SourceReference) || len(publication.SourceReference) > 2048 {
		return PublishedPricePeriod{}, ErrInvalidPricePeriod
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return PublishedPricePeriod{}, err
	}
	defer tx.Rollback()

	var fingerprint string
	err = tx.QueryRowContext(ctx, `
SELECT price_fingerprint
FROM public.price_tariff_payloads
WHERE tenant_id=$1 AND project_id=$2 AND price_version=$3
FOR KEY SHARE`, publication.TenantID, publication.ProjectID, publication.PriceVersion).Scan(&fingerprint)
	if errors.Is(err, sql.ErrNoRows) {
		return PublishedPricePeriod{}, ledger.ErrPriceTariffPayloadNotFound
	}
	if err != nil {
		return PublishedPricePeriod{}, err
	}

	result := PublishedPricePeriod{
		TenantID: publication.TenantID,
		ProjectID: publication.ProjectID,
		PriceVersion: publication.PriceVersion,
		PriceFingerprint: fingerprint,
		EffectiveFrom: publication.EffectiveFrom.UTC(),
		EffectiveTo: publication.EffectiveTo.UTC(),
		Reason: publication.Reason,
		SourceReference: publication.SourceReference,
	}
	err = tx.QueryRowContext(ctx, `
INSERT INTO public.price_tariff_effective_periods
    (tenant_id, project_id, price_version, price_fingerprint,
     effective_from, effective_to, publication_reason, source_reference)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
RETURNING publication_id, published_at, published_by`,
		result.TenantID, result.ProjectID, result.PriceVersion, result.PriceFingerprint,
		result.EffectiveFrom, result.EffectiveTo, result.Reason, result.SourceReference).Scan(
		&result.PublicationID, &result.PublishedAt, &result.PublishedBy)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23P01" {
			return PublishedPricePeriod{}, ErrPricePeriodOverlap
		}
		return PublishedPricePeriod{}, err
	}
	if err := tx.Commit(); err != nil {
		return PublishedPricePeriod{}, err
	}
	return result, nil
}
