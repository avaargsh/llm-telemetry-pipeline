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

func roleHasColumnPrivilege(t *testing.T, tx *sql.Tx, table, column, privilege string) bool {
	t.Helper()
	var allowed bool
	if err := tx.QueryRowContext(context.Background(),
		"SELECT has_column_privilege(current_user, $1, $2, $3)", table, column, privilege).Scan(&allowed); err != nil {
		t.Fatal(err)
	}
	return allowed
}

func assertPermissionDenied(t *testing.T, tx *sql.Tx, statement string) {
	t.Helper()
	if _, err := tx.ExecContext(context.Background(), "SAVEPOINT expected_permission_denial"); err != nil {
		t.Fatal(err)
	}
	_, execErr := tx.ExecContext(context.Background(), statement)
	if execErr == nil || !strings.Contains(strings.ToLower(execErr.Error()), "permission denied") {
		_, _ = tx.ExecContext(context.Background(), "ROLLBACK TO SAVEPOINT expected_permission_denial")
		t.Fatalf("expected permission denied, got %v", execErr)
	}
	if _, err := tx.ExecContext(context.Background(), "ROLLBACK TO SAVEPOINT expected_permission_denial"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(context.Background(), "RELEASE SAVEPOINT expected_permission_denial"); err != nil {
		t.Fatal(err)
	}
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
			if roleHasColumnPrivilege(t, tx, "public.genai_observation_queue_state", "capacity_limit", "UPDATE") {
				t.Fatalf("%s must not be able to change immutable queue capacity", role)
			}
			assertPermissionDenied(t, tx,
				"UPDATE public.genai_observation_queue_state SET capacity_limit = capacity_limit + 1 WHERE singleton = true")
			for _, table := range []string{"public.usage_ledger", "public.event_outbox"} {
				for _, privilege := range []string{"SELECT", "INSERT", "UPDATE", "DELETE"} {
					if roleHasTablePrivilege(t, tx, table, privilege) {
						t.Fatalf("%s unexpectedly has %s on %s", role, privilege, table)
					}
				}
			}
			assertPermissionDenied(t, tx, "INSERT INTO public.usage_ledger DEFAULT VALUES")
		})
	}
}

func TestObservationReceiverAndWorkerHaveDisjointDatabaseCapabilities(t *testing.T) {
	_, db := openDurableTestStore(t, 8)
	ctx := context.Background()
	tests := []struct {
		role        string
		allow       map[string][]string
		deny        map[string][]string
		allowColumn map[string][]string
		denyColumn  map[string][]string
	}{
		{
			role: "llm_telemetry_observation_receiver",
			allow: map[string][]string{
				"public.genai_observations":                  {"SELECT", "INSERT"},
				"public.genai_observation_receipts":          {"SELECT", "INSERT", "DELETE"},
				"public.genai_observation_delivery_receipts": {"SELECT", "INSERT", "DELETE"},
				"public.genai_observation_queue_state":       {"SELECT"},
			},
			deny: map[string][]string{
				"public.genai_observations": {"UPDATE", "DELETE"},
			},
			allowColumn: map[string][]string{
				"public.genai_observation_queue_state": {"queue_items", "receipt_items", "delivery_receipt_items"},
			},
			denyColumn: map[string][]string{
				"public.genai_observation_queue_state": {"capacity_limit"},
			},
		},
		{
			role: "llm_telemetry_observation_worker",
			allow: map[string][]string{
				"public.genai_observations":            {"SELECT", "DELETE"},
				"public.genai_observation_queue_state": {"SELECT"},
			},
			deny: map[string][]string{
				"public.genai_observations":                  {"INSERT", "UPDATE"},
				"public.genai_observation_receipts":          {"SELECT", "INSERT", "UPDATE", "DELETE"},
				"public.genai_observation_delivery_receipts": {"SELECT", "INSERT", "UPDATE", "DELETE"},
			},
			allowColumn: map[string][]string{
				"public.genai_observations": {
					"attempts", "available_at", "lease_owner", "lease_until",
					"last_error", "terminal_at", "terminal_reason",
				},
				"public.genai_observation_queue_state": {"queue_items"},
			},
			denyColumn: map[string][]string{
				"public.genai_observations": {
					"schema_version", "tenant_id", "trace_id", "span_id", "request_id",
					"attempt_id", "provider", "operation", "request_model", "response_model",
					"status", "input_tokens", "output_tokens", "payload_hash", "created_at",
				},
				"public.genai_observation_queue_state": {"receipt_items", "delivery_receipt_items", "capacity_limit"},
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
			for table, columns := range tc.allowColumn {
				for _, column := range columns {
					if !roleHasColumnPrivilege(t, tx, table, column, "UPDATE") {
						t.Fatalf("%s lacks UPDATE on %s.%s", tc.role, table, column)
					}
				}
			}
			for table, columns := range tc.denyColumn {
				for _, column := range columns {
					if roleHasColumnPrivilege(t, tx, table, column, "UPDATE") {
						t.Fatalf("%s unexpectedly has UPDATE on %s.%s", tc.role, table, column)
					}
				}
			}
			if tc.role == "llm_telemetry_observation_worker" {
				assertPermissionDenied(t, tx,
					"UPDATE public.genai_observations SET tenant_id = tenant_id WHERE observation_id = 1")
				assertPermissionDenied(t, tx,
					"UPDATE public.genai_observations SET payload_hash = payload_hash WHERE observation_id = 1")
			}
			var canExecute bool
			if err := tx.QueryRowContext(ctx,
				"SELECT has_function_privilege(current_user, 'public.ensure_genai_observation_capacity(bigint)', 'EXECUTE')").Scan(&canExecute); err != nil {
				t.Fatal(err)
			}
			if tc.role == "llm_telemetry_observation_receiver" && !canExecute {
				t.Fatal("receiver cannot execute the immutable capacity initializer")
			}
			if tc.role == "llm_telemetry_observation_worker" && canExecute {
				t.Fatal("worker unexpectedly has capacity initializer authority")
			}
		})
	}
}
