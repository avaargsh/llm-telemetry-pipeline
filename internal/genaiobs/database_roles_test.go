package genaiobs

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func roleHasTablePrivilege(t *testing.T, tx *sql.Tx, table, privilege string) bool {
	t.Helper()
	var allowed bool
	if err := tx.QueryRowContext(context.Background(),
		"SELECT has_table_privilege(current_user, $1, $2)", table, privilege).Scan(&allowed); err != nil {
		t.Fatal(err)
	}
	return allowed
}

func TestObservationDatabaseRolesAreIsolatedFromBillingAuthority(t *testing.T) {
	_, db := openDurableTestStore(t, 8)
	ctx := context.Background()
	for _, role := range []string{"llm_telemetry_observation_receiver", "llm_telemetry_observation_worker"} {
		t.Run(role, func(t *testing.T) {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.ExecContext(ctx, "SET LOCAL ROLE "+role); err != nil {
				t.Fatalf("role migration missing or role cannot be assumed: %v", err)
			}
			var canUpdateCapacity bool
			if err := tx.QueryRowContext(ctx,
				"SELECT has_column_privilege(current_user, 'public.genai_observation_queue_state', 'capacity_limit', 'UPDATE')").Scan(&canUpdateCapacity); err != nil {
				t.Fatal(err)
			}
			if canUpdateCapacity {
				t.Fatalf("%s must not be able to change immutable queue capacity", role)
			}
			if _, err := tx.ExecContext(ctx, "SAVEPOINT capacity_denial"); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, "UPDATE public.genai_observation_queue_state SET capacity_limit = capacity_limit + 1 WHERE singleton = true"); err == nil || !strings.Contains(strings.ToLower(err.Error()), "permission denied") {
				t.Fatalf("%s must be denied direct capacity mutation, got %v", role, err)
			}
			if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT capacity_denial"); err != nil {
				t.Fatalf("rollback denied-capacity probe: %v", err)
			}
			for _, table := range []string{"public.usage_ledger", "public.event_outbox"} {
				for _, privilege := range []string{"SELECT", "INSERT", "UPDATE", "DELETE"} {
					if roleHasTablePrivilege(t, tx, table, privilege) {
						t.Fatalf("%s unexpectedly has %s on %s", role, privilege, table)
					}
				}
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO public.usage_ledger DEFAULT VALUES"); err == nil || !strings.Contains(strings.ToLower(err.Error()), "permission denied") {
				t.Fatalf("%s must be denied from writing usage_ledger, got %v", role, err)
			}
		})
	}
}

func TestObservationReceiverAndWorkerHaveDisjointDatabaseCapabilities(t *testing.T) {
	_, db := openDurableTestStore(t, 8)
	ctx := context.Background()
	tests := []struct {
		role  string
		allow map[string][]string
		deny  map[string][]string
	}{
		{
			role: "llm_telemetry_observation_receiver",
			allow: map[string][]string{
				"public.genai_observations": {"SELECT", "INSERT"},
				"public.genai_observation_receipts": {"SELECT", "INSERT", "DELETE"},
				"public.genai_observation_delivery_receipts": {"SELECT", "INSERT", "DELETE"},
				"public.genai_observation_queue_state": {"SELECT"},
			},
			deny: map[string][]string{
				"public.genai_observations": {"UPDATE", "DELETE"},
			},
		},
		{
			role: "llm_telemetry_observation_worker",
			allow: map[string][]string{
				"public.genai_observations": {"SELECT", "UPDATE", "DELETE"},
				"public.genai_observation_queue_state": {"SELECT"},
			},
			deny: map[string][]string{
				"public.genai_observations": {"INSERT"},
				"public.genai_observation_receipts": {"INSERT", "DELETE"},
				"public.genai_observation_delivery_receipts": {"INSERT", "DELETE"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.role, func(t *testing.T) {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.ExecContext(ctx, "SET LOCAL ROLE "+tc.role); err != nil {
				t.Fatal(err)
			}
			var queueItemsUpdate, receiptItemsUpdate, deliveryItemsUpdate, capacityUpdate bool
			if err := tx.QueryRowContext(ctx, `SELECT
has_column_privilege(current_user, 'public.genai_observation_queue_state', 'queue_items', 'UPDATE'),
has_column_privilege(current_user, 'public.genai_observation_queue_state', 'receipt_items', 'UPDATE'),
has_column_privilege(current_user, 'public.genai_observation_queue_state', 'delivery_receipt_items', 'UPDATE'),
has_column_privilege(current_user, 'public.genai_observation_queue_state', 'capacity_limit', 'UPDATE')`).Scan(
				&queueItemsUpdate, &receiptItemsUpdate, &deliveryItemsUpdate, &capacityUpdate); err != nil {
				t.Fatal(err)
			}
			if capacityUpdate || !queueItemsUpdate {
				t.Fatalf("%s has unexpected queue-state column privileges: queue=%v receipts=%v delivery=%v capacity=%v",
					tc.role, queueItemsUpdate, receiptItemsUpdate, deliveryItemsUpdate, capacityUpdate)
			}
			if tc.role == "llm_telemetry_observation_receiver" && (!receiptItemsUpdate || !deliveryItemsUpdate) {
				t.Fatalf("receiver lacks receipt counter update privileges: receipts=%v delivery=%v", receiptItemsUpdate, deliveryItemsUpdate)
			}
			if tc.role == "llm_telemetry_observation_worker" && (receiptItemsUpdate || deliveryItemsUpdate) {
				t.Fatalf("worker has receipt counter update privileges: receipts=%v delivery=%v", receiptItemsUpdate, deliveryItemsUpdate)
			}
			for table, privileges := range tc.allow {
				for _, privilege := range privileges {
					if !roleHasTablePrivilege(t, tx, table, privilege) {
						t.Fatalf("%s lacks expected %s on %s", tc.role, privilege, table)
					}
				}
			}
			for table, privileges := range tc.deny {
				for _, privilege := range privileges {
					if roleHasTablePrivilege(t, tx, table, privilege) {
						t.Fatalf("%s unexpectedly has %s on %s", tc.role, privilege, table)
					}
				}
			}
			if tc.role == "llm_telemetry_observation_receiver" {
				var canExecute bool
				if err := tx.QueryRowContext(ctx,
					"SELECT has_function_privilege(current_user, 'public.ensure_genai_observation_capacity(bigint)', 'EXECUTE')").Scan(&canExecute); err != nil {
					t.Fatal(err)
				}
				if !canExecute {
					t.Fatal("receiver cannot execute the immutable capacity initializer")
				}
			}
		})
	}
}
