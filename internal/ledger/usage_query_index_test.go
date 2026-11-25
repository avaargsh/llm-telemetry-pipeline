package ledger

import (
	"context"
	"testing"
	"time"
)

// This test runs in the existing disposable PostgreSQL integration job.
// Verify the real migration rather than creating an index inside the test.
func TestUsageQueryIndexMigration(t *testing.T) {
	db := testDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var valid, ready, btree, nonunique, full bool
	var columns string
	err := db.QueryRowContext(ctx, `
SELECT i.indisvalid, i.indisready, am.amname = 'btree',
       NOT i.indisunique, i.indpred IS NULL AND i.indexprs IS NULL,
       (SELECT string_agg(a.attname, ',' ORDER BY k.ordinality)
          FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ordinality)
          JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
          WHERE k.ordinality <= i.indnkeyatts)
FROM pg_index i
JOIN pg_class idx ON idx.oid = i.indexrelid
JOIN pg_am am ON am.oid = idx.relam
WHERE i.indrelid = 'public.usage_ledger'::regclass
  AND idx.relname = 'idx_usage_ledger_scope_event_time'
`).Scan(&valid, &ready, &btree, &nonunique, &full, &columns)
	if err != nil { t.Fatal(err) }
	if !valid || !ready || !btree || !nonunique || !full || columns != "tenant_id,project_id,event_time" {
		t.Fatalf("unexpected usage index: valid=%t ready=%t btree=%t nonunique=%t full=%t keys=%s",
			valid, ready, btree, nonunique, full, columns)
	}
}
