# M14.4 Local Grafana provisioning

This profile provisions the M14.1 Kafka operator and M14.2 GenAI runtime
dashboards and a stable Prometheus datasource UID. It is a local developer
acceptance profile, not a production HA/SLO deployment.

## Start

Set a unique admin credential. Do not commit it or put it in a Compose file:

```sh
export GRAFANA_ADMIN_USER=telemetry-admin
export GRAFANA_ADMIN_PASSWORD="$(openssl rand -hex 32)"
make grafana-up
```

Open `http://127.0.0.1:13000` and sign in with the exported credentials.
The profile combines the base developer stack, the M4 Prometheus/Alertmanager
profile, and the Grafana overlay. Prometheus is reachable inside the Compose
network at `http://prometheus:9090`; the Grafana datasource has immutable
UID `prometheus`. Both dashboards are file-provisioned into the
`LLM Telemetry` folder and are read-only in the UI. The dashboard JSON
references the stable datasource UID directly, so provisioning and API import
do not depend on Grafana's interactive `__inputs` substitution.

The Grafana HTTP port is bound to host loopback only. Anonymous access and
user self-signup are disabled. The container has a read-only root filesystem,
a writable named data volume, a temporary filesystem for `/tmp`, all
Linux capabilities dropped, and `no-new-privileges`. The admin password is
required at Compose interpolation time.

The local Prometheus profile scrapes the Kafka lag exporter. It also has the
GenAI worker scrape job, which is populated when the optional worker overlay
is running. The receiver dashboard expects a Kubernetes-discovered HTTPS
receiver target on port 4318; this local profile does not deploy that receiver.
A dashboard with no current samples must be treated as unknown, not as zero.

## Verify and stop

```sh
make grafana-smoke
make grafana-down
```

The smoke test creates an isolated Compose project, verifies Compose
configuration, starts Grafana without requiring the upstream Prometheus
services, checks that unauthenticated datasource API access is rejected,
then verifies the provisioned Prometheus datasource and both dashboards
through the authenticated Grafana HTTP API. It removes only its own
project and volumes on exit.

A persistent local Grafana volume retains its initial admin credentials.
Changing `GRAFANA_ADMIN_PASSWORD` after first initialization does not
automatically rotate that existing account password. Use Grafana's supported
password-rotation flow; do not delete a volume that contains data you need.

## Live dashboard acceptance

With Docker Compose and Go available, run:

```sh
export GRAFANA_ADMIN_USER=telemetry-ci
export GRAFANA_ADMIN_PASSWORD="$(openssl rand -hex 32)"
make grafana-live-smoke
```

This starts a fresh isolated single-broker Kafka lab, creates the disposable
`m4.alert.e2e.v1` topic with broker append timestamps, writes 20 synthetic
messages and commits the consumer group at offset 8. The real lag exporter
must report scrape success and offset distance 12. Prometheus must scrape
that result, and the exact PromQL from the provisioned Kafka dashboard panel
must return the same value through Prometheus' query API. Grafana's
authenticated API must also return the provisioned dashboard. The test tears
down only its own Compose project and volumes.

This proves one real developer-stack path from Kafka offsets to a provisioned
Grafana dashboard. It is synthetic operational telemetry, not inference
usage, tenant billing, a multi-broker production topology, or an availability
SLO. The GenAI receiver remains a Kubernetes-discovered target and the worker
metrics require the optional worker overlay.

## Production boundary

Before production use, provide TLS termination, managed credentials or
OIDC, a supported persistent Grafana database, Prometheus authentication and
HA/retention, restricted network policy, backup/restore, and operational
ownership. The local profile intentionally does not publish Grafana on a
public interface or configure anonymous access.
