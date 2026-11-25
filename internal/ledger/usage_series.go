package ledger

import (
	"context"
	"errors"
	"strings"
	"time"
)

type UsageBucket struct {
	BucketStart time.Time `json:"bucket_start"`
	UsageSummary
}

// usageSeriesStep requires whole UTC buckets, so callers never mistake
// partially covered buckets for full hours or days.
func usageSeriesStep(from, to time.Time, granularity string) (time.Duration, error) {
	var step time.Duration
	switch granularity {
	case "hour":
		step = time.Hour
	case "day":
		step = 24 * time.Hour
	default:
		return 0, errors.New("granularity must be hour or day")
	}
	if from.IsZero() || to.IsZero() || !to.After(from) || to.Sub(from) > MaxUsageWindow ||
		!from.Equal(from.Truncate(step)) || !to.Equal(to.Truncate(step)) {
		return 0, errors.New("usage series requires aligned UTC buckets within 31 days")
	}
	return step, nil
}

// UsageSeries returns one row per UTC bucket in a half-open event-time
// window, including empty buckets. A single SQL statement supplies the
// populated buckets, ensuring one consistent PostgreSQL read snapshot.
func (s *Store) UsageSeries(ctx context.Context, tenant, project string, from, to time.Time, granularity string) ([]UsageBucket, error) {
	step, err := usageSeriesStep(from, to, granularity)
	if err != nil { return nil, err }
	if strings.TrimSpace(tenant) == "" || strings.TrimSpace(project) == "" {
		return nil, errors.New("tenant and project are required")
	}
	if s == nil || s.DB == nil { return nil, errors.New("ledger database is required") }
	from, to = from.UTC(), to.UTC()
	result := make([]UsageBucket, int(to.Sub(from)/step))
	for i := range result {
		result[i] = UsageBucket{
			BucketStart: from.Add(time.Duration(i)*step),
			UsageSummary: UsageSummary{PromptTokens:"0", CompletionTokens:"0", TotalTokens:"0"},
		}
	}
	rows, err := s.DB.QueryContext(ctx, `
SELECT date_trunc($5, event_time, 'UTC') AS bucket, count(*),
       coalesce(sum(prompt_tokens), 0)::text,
       coalesce(sum(completion_tokens), 0)::text,
       (coalesce(sum(prompt_tokens), 0) + coalesce(sum(completion_tokens), 0))::text
FROM public.usage_ledger
WHERE tenant_id = $1 AND project_id = $2
  AND event_time >= $3 AND event_time < $4
GROUP BY 1
ORDER BY 1
`, tenant, project, from, to, granularity)
	if err != nil { return nil, err }
	defer rows.Close()
	for rows.Next() {
		var b UsageBucket
		if err := rows.Scan(&b.BucketStart,&b.RequestCount,&b.PromptTokens,&b.CompletionTokens,&b.TotalTokens); err != nil { return nil, err }
		delta := b.BucketStart.Sub(from)
		if delta < 0 || delta >= to.Sub(from) || delta%step != 0 {
			return nil, errors.New("unexpected ledger bucket")
		}
		b.BucketStart = b.BucketStart.UTC()
		result[int(delta/step)] = b
	}
	if err := rows.Err(); err != nil { return nil, err }
	return result, nil
}
