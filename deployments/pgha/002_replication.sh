#!/usr/bin/env bash
set -Eeuo pipefail
# Lab-only replication role and permissive lab network pg_hba. NEVER deploy.
psql --set=ON_ERROR_STOP=1 --username telemetry --dbname telemetry \
  -c "CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'lab-replication-only'"
printf '\nhost replication replicator 0.0.0.0/0 scram-sha-256\n' >> "$PGDATA/pg_hba.conf"
