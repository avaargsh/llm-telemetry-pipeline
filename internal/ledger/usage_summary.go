package ledger

import (
	"context"
	"errors"
	"strings"
	"time"
)

// UsageSummary represents finalized ledger events in one project. Token
// totals are decimal strings: PostgreSQL SUM(bigint) returns numeric and a
// JSON number can lose precision in clients such as JavaScript.
type UsageSummary struct {
	RequestCount     int64  `json:"request_count"`
	PromptTokens     string `json:"prompt_tokens"`
	CompletionTokens string `json:"completion_tokens"`
	TotalTokens      string `json:"total_tokens"`
}

const MaxUsageWindow = 31 * 24 * time.Hour

// UsageSummary reads a half-open event-time window from the authoritative
// ledger only. Outbox, Kafka and ClickHouse replay cannot inflate the count.
func (s *Store) UsageSummary(ctx context.Context, tenant, project string, from, to time.Time) (UsageSummary, error) {
	var result UsageSummary
	if strings.TrimSpace(tenant) == "" || strings.TrimSpace(project) == "" ||
		from.IsZero() || to.IsZero() || !to.After(from) || to.Sub(from) > MaxUsageWindow {
		return result, errors.New("tenant, project and a positive time window of at most 31 days are required")
	}
	if s == nil || s.DB == nil {
		return result, errors.New("ledger database is required")
	}
	err := s.DB.QueryRowContext(ctx, `
SELECT count(*),
       coalesce(sum(prompt_tokens), 0)::text,
       coalesce(sum(completion_tokens), 0)::text,
       (coalesce(sum(prompt_tokens), 0) + coalesce(sum(completion_tokens), 0))::text
FROM public.usage_ledger
WHERE tenant_id = $1 AND project_id = $2
  AND event_time >= $3 AND event_time < $4
`, tenant, project, from, to).Scan(&result.RequestCount, &result.PromptTokens, &result.CompletionTokens, &result.TotalTokens)
	if err != nil {
		return UsageSummary{}, err
	}
	return result, nil
}
