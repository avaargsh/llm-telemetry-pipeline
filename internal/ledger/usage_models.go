package ledger

import (
	"context"
	"errors"
	"strings"
	"time"
)

// MaxUsageModels bounds the number of distinct models returned by a single
// query. A window that exceeds the limit fails explicitly; it is never
// silently truncated and presented as a complete breakdown.
const MaxUsageModels = 100

var ErrUsageModelLimit = errors.New("usage model breakdown exceeds limit")

type ModelUsage struct {
	Model string `json:"model"`
	UsageSummary
}

// UsageByModel reads finalized authoritative ledger rows, using the same
// scope and half-open event-time semantics as UsageSummary.
func (s *Store) UsageByModel(ctx context.Context, tenant, project string, from, to time.Time) ([]ModelUsage, error) {
	if strings.TrimSpace(tenant) == "" || strings.TrimSpace(project) == "" ||
		from.IsZero() || to.IsZero() || !to.After(from) || to.Sub(from) > MaxUsageWindow {
		return nil, errors.New("tenant, project and a positive time window of at most 31 days are required")
	}
	if s == nil || s.DB == nil {
		return nil, errors.New("ledger database is required")
	}
	rows, err := s.DB.QueryContext(ctx, `
SELECT model, count(*),
       coalesce(sum(prompt_tokens), 0)::text,
       coalesce(sum(completion_tokens), 0)::text,
       (coalesce(sum(prompt_tokens), 0) + coalesce(sum(completion_tokens), 0))::text
FROM public.usage_ledger
WHERE tenant_id = $1 AND project_id = $2
  AND event_time >= $3 AND event_time < $4
GROUP BY model
ORDER BY model COLLATE "C"
LIMIT $5
`, tenant, project, from, to, MaxUsageModels+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ModelUsage, 0)
	for rows.Next() {
		var m ModelUsage
		if err := rows.Scan(&m.Model, &m.RequestCount, &m.PromptTokens, &m.CompletionTokens, &m.TotalTokens); err != nil {
			return nil, err
		}
		result = append(result, m)
		if len(result) > MaxUsageModels {
			return nil, ErrUsageModelLimit
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
