#!/usr/bin/env bash
set -Eeuo pipefail
# Lab only: initialize a persistent standby via physical pg_basebackup -R.
# No rewriting WAL, no deleting volumes on broker restart.
mkdir -p "$PGDATA"
chown -R postgres:postgres "$PGDATA"
chmod 0700 "$PGDATA"
if [[ ! -s "$PGDATA/PG_VERSION" ]]; then
  for i in $(seq 1 60); do
    if gosu postgres pg_basebackup \
      --dbname='host=pg-primary port=5432 user=replicator password=lab-replication-only application_name=standby1' \
      --pgdata="$PGDATA" --format=plain --wal-method=stream --write-recovery-conf; then
      break
    fi
    if (( i == 60 )); then echo 'basebackup failed after 60 attempts' >&2; exit 1; fi
    sleep 2
  done
fi
if [[ ! -s "$PGDATA/standby.signal" ]]; then
  echo 'standby bootstrap missing standby.signal; fail closed' >&2
  exit 1
fi
exec docker-entrypoint.sh postgres -c hot_standby=on
