package ledger

import (
	"context"
	"database/sql"
	"errors"
)

func verifyPriceRegistryPrivileges(ctx context.Context, tx *sql.Tx) error {
	var restricted bool
	err := tx.QueryRowContext(ctx, "SELECT NOT (has_table_privilege(session_user, 'public.price_version_registry', 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') OR has_any_column_privilege(session_user, 'public.price_version_registry', 'SELECT,INSERT,UPDATE,REFERENCES'))").Scan(&restricted)
	if err != nil || !restricted {
		return ErrUsageQueryScope
	}
	return nil
}

func queryPriceVersionFingerprint(ctx context.Context, tx *sql.Tx, tenant, project, version string) (string, error) {
	var fingerprint string
	err := tx.QueryRowContext(ctx, "SELECT price_fingerprint FROM public.usage_query_price_versions WHERE tenant_id=$1 AND project_id=$2 AND price_version=$3", tenant, project, version).Scan(&fingerprint)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrPriceVersionNotFound
	}
	if err != nil {
		return "", err
	}
	return fingerprint, nil
}

func captureModelWindows(ctx context.Context, tx *sql.Tx, tenant, project string, windows []ModelQueryWindow) ([]ModelWindowCapture, error) {
	result := make([]ModelWindowCapture, 0, len(windows))
	for _, w := range windows {
		models, err := queryUsageByModel(ctx, tx, scopedUsageRelation, tenant, project, w.From, w.To)
		if err != nil {
			return nil, err
		}
		result = append(result, ModelWindowCapture{ModelQueryWindow: ModelQueryWindow{From: w.From.UTC(), To: w.To.UTC()}, Models: models})
	}
	return result, nil
}
