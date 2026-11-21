#!/usr/bin/env bash
# Disposable PostgreSQL 17 synchronous streaming standby + manual failover.
# Requires no production cluster, no Patroni and never reformats an old primary.
set -Eeuo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME="llmtelemetry-pgha-${GITHUB_RUN_ID:-local}-$$"
compose=(docker compose -f deployments/docker-compose-pg-ha.yml)
tmp="$(mktemp -d)"
cleanup() {
  local rc=$?
  trap - EXIT
  if (( rc != 0 )); then
    echo "M7 PGHA fault lab FAILED exit=$rc" >&2
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --tail 75 pg-primary pg-standby >&2 || true
    [[ ! -f "$tmp/held.log" ]] || tail -20 "$tmp/held.log" >&2
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$tmp"
  exit "$rc"
}
trap cleanup EXIT

primary_dsn='postgres://telemetry:localdev-only@127.0.0.1:25432/telemetry?sslmode=disable'
standby_dsn='postgres://telemetry:localdev-only@127.0.0.1:25433/telemetry?sslmode=disable'
pgsql() {
  local server="$1" statement="$2"
  "${compose[@]}" exec -T "$server" psql -U telemetry -d telemetry -v ON_ERROR_STOP=1 -tAc "$statement"
}
ready=0
"${compose[@]}" up -d pg-primary
for i in $(seq 1 100);do
  if [[ "$(pgsql pg-primary 'SELECT count(*) FROM usage_ledger' 2>/dev/null)" == 0 ]];then
    ready=1;break
  fi
  sleep 2
done
if (( ready == 0 ));then echo 'primary schema not ready' >&2;exit 1;fi
"${compose[@]}" up -d pg-standby
ready=0
for i in $(seq 1 120);do
  if [[ "$(pgsql pg-standby 'SELECT pg_is_in_recovery()' 2>/dev/null)" == "t" ]] &&
     [[ "$(pgsql pg-primary "SELECT count(*) FROM pg_stat_replication WHERE application_name='standby1' AND state='streaming'" 2>/dev/null)" == 1 ]];then
    ready=1;break
  fi
  sleep 2
done
if (( ready == 0 ));then echo 'physical standby is not streaming' >&2;exit 1;fi

# AFTER the standby connects, opt the lab into synchronous replication and
# remote_apply, so successful Ledger.Commit was applied on the hot standby.
pgsql pg-primary "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (standby1)'" >/dev/null
pgsql pg-primary "ALTER SYSTEM SET synchronous_commit = 'remote_apply'" >/dev/null
pgsql pg-primary 'SELECT pg_reload_conf()' >/dev/null
ready=0
for i in $(seq 1 40);do
  if [[ "$(pgsql pg-primary "SELECT count(*) FROM pg_stat_replication WHERE application_name='standby1' AND sync_state='sync'" 2>/dev/null)" == 1 ]] &&
     [[ "$(pgsql pg-primary 'SHOW synchronous_commit' 2>/dev/null)" == "remote_apply" ]];then
    ready=1;break
  fi
  sleep 1
done
if (( ready == 0 ));then echo 'synchronous remote_apply protection not active' >&2;exit 1;fi

# Prove hot standby is not writable even though its TCP connection is healthy.
if "${compose[@]}" exec -T pg-standby \
  psql -U telemetry -d telemetry -v ON_ERROR_STOP=1 -c 'CREATE TABLE pgha_write_must_fail (id integer)' >/dev/null 2>&1;then
  echo 'standby unexpectedly accepted a write: HALT' >&2;exit 1
fi

go test -count=1 ./cmd/pgha-probe
go build -o "$tmp/pgha-probe" ./cmd/pgha-probe
seed="pgha-${GITHUB_RUN_ID:-local}-$$"
"$tmp/pgha-probe" --phase seed --seed "$seed" --dsn "$primary_dsn"
"$tmp/pgha-probe" --phase standby --seed "$seed" --dsn "$standby_dsn"
# Open a real transaction with BOTH rows inserted, hold before COMMIT, and
# kill the primary process. PostgreSQL must not replay a partial transaction.
"$tmp/pgha-probe" --phase hold --seed "$seed" --dsn "$primary_dsn" \
    --hold-ready "$tmp/hold.ready" >"$tmp/held.log" 2>&1 &
hold_pid=$!
ready=0
for i in $(seq 1 30);do
  if [[ -f "$tmp/hold.ready" ]] && kill -0 "$hold_pid" 2>/dev/null;then ready=1;break;fi
  if ! kill -0 "$hold_pid" 2>/dev/null;then echo 'transaction holder exited before fault' >&2;exit 1;fi
  sleep 1
done
if (( ready == 0 ));then echo 'transaction holder not ready' >&2;exit 1;fi

"${compose[@]}" kill -s SIGKILL pg-primary
wait "$hold_pid" 2>/dev/null || true
# Old primary may NEVER rejoin as a writable node during this experiment.
if [[ -n "$("${compose[@]}" ps --status running -q pg-primary)" ]];then
  echo 'old primary is still running; refusing promotion' >&2;exit 1
fi
# Probe the published primary port without needing host-side psql.
if timeout 3 bash -c '</dev/tcp/127.0.0.1/25432' >/dev/null 2>&1;then
  echo 'old primary still accepts connections; refusing promotion' >&2;exit 1
fi
"$tmp/pgha-probe" --phase standby --seed "$seed" --dsn "$standby_dsn"

"${compose[@]}" exec -T -u postgres pg-standby \
  pg_ctl -D /var/lib/postgresql/data promote
ready=0
for i in $(seq 1 60);do
  if [[ "$(pgsql pg-standby 'SELECT pg_is_in_recovery()' 2>/dev/null)" == f ]];then
    ready=1;break
  fi
  sleep 1
done
if (( ready == 0 ));then echo 'standby promotion timeout' >&2;exit 1;fi

out="${M7_PGHA_EVIDENCE_PATH:-artifacts/m7-pgha-failover.json}"
if [[ "$out" != /* ]];then out="$PWD/$out";fi
"$tmp/pgha-probe" --phase promoted --seed "$seed" --dsn "$standby_dsn" \
  --old-primary-stopped --evidence "$out"
python3 - "$out" <<'PY'
import json,sys
receipt=json.load(open(sys.argv[1],encoding="utf-8"))
assert receipt["result"]=="PASS" and receipt["mode"]=="manual_synchronous_pg17",receipt
assert receipt["committed_before"]==receipt["replayed_on_standby"]==12,receipt
assert receipt["promoted_ledger"]==receipt["promoted_outbox"]==13,receipt
assert receipt["stale_lease_fenced"] and receipt["uncommitted_old_primary_rolled_back"],receipt
assert receipt["published_without_kafka_ack"]==0,receipt
assert "password" not in json.dumps(receipt),receipt
print("PASS M7: synchronous remote_apply replay, primary SIGKILL, manual promotion, rollback, stable-id retry, Outbox lease fence")
PY
