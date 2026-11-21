# M3b-P1 · Three-broker KRaft RF=3/minISR=2 fault experiments

**Status: real Docker fault tests PASS; PR #12 merged as `8651745`.** This is a disposable Docker fault lab, not a production HA deployment recipe.

## Topology

- Three `apache/kafka:4.1.0` *combined* controller+broker KRaft nodes (IDs 1, 2, 3).
- A static three-controller voter quorum, node-specific durable Docker volumes, separate host ports 39092–39094.
- Topic `m3b.ha.critical.v1`: one partition, **RF=3**, normal `min.insync.replicas=2`, producer `acks=all`.
- Internal `__consumer_offsets` RF=3, transaction-state RF=3/minISR=2.
- The existing developer `deployments/docker-compose.yml` remains **single-broker, RF=1** and is unchanged.

## Reproducible experiment

```bash
bash scripts/m3b-ha-smoke.sh
# Machine-readable evidence: /tmp/m3b-kafka-ha-report.json
```

The integration test uses actual partition metadata to verify replica count, ISR membership and leader IDs, not merely a successful TCP connection.

1. With ISR=3, produce 15 sealed usage events and record only Kafka-ACKed Event IDs.
2. Stop the **actual elected data partition leader**; wait for a new leader and ISR=2 while the other two controllers retain quorum.
3. With `acks=all` and `minISR=2`, produce 12 additional events; all must receive broker ACK.
4. With both surviving brokers still online, temporarily change the **test Topic only** to `minISR=3`; a write must fail explicitly with Kafka's **NotEnoughReplicas** protocol error. This isolates ISR safety from controller quorum failure.
5. Restore `minISR=2`; verify new ACKs. Restart the stopped broker; wait for ISR=3 and verify further ACKs.
6. Replay the topic from offset zero. Verify **all 35 broker-ACKed Event IDs** exist exactly once with byte-identical sealed payloads. Rejected/unknown-ACK messages are not elevated to the acknowledged set.
7. Keeping the current data partition leader online, stop the other **two combined Broker/Controller nodes**. After the remaining broker detects insufficient ISR / loss of controller quorum, the probe must receive **no Kafka ACK**. Its error can reflect insufficient replicas or transport uncertainty; do not claim that the unacknowledged record was definitely absent from the log.
8. Restart both nodes, wait for ISR=3, and **recheck all 35 previously ACKed events**. Export JSON with initial/failover leaders, ISR transitions, Kafka rejection, quorum-loss probe and read-back proof.

## Important boundaries

- Three *combined* controller+broker processes share failure domains in this lab; Kafka recommends dedicated controllers in critical production environments.
- A single-partition, single-leader experiment does not prove all partitions, rack-awareness, zone failure, disk corruption or cross-region survivability.
- `minISR=3` during step 4 is **test-only** and should not be mistaken for the permanent target. Stopping two controllers at once loses controller quorum and therefore is tested **separately** in step 7; its error cannot be used as evidence of ISR-only rejection.
- Acknowledged records can still be lost under disasters outside tested persistence boundaries; no absolute zero-loss claim.
- Kafka `acks=all` does not imply ClickHouse ingestion or PostgreSQL accounting has completed.
- Replaying an unknown-ACK write may expose extra physical messages; treat these as unconfirmed until reconciled, never invent an acknowledgment.

## Verified CI runs

- [Single leader loss + minISR3 negative test](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43891556301): PASS; RF3, ISR 3→2→3, leader 2→3; 35/35 ACKed Event IDs reread.
- [Separate two-controller loss + recovery](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43892476284): PASS; leader 3→1; an unacknowledged `acks=all` request failed with NotEnoughReplicas; recovery verified 35/35 previously acknowledged events. The `m3b-kafka-ha-report` Actions Artifact contains machine-readable results.
- The test Topic is one partition; these experiments are **not** representative throughput benchmarks or HA availability SLO measurements.

## Merge gate

All **nine** required GitHub Actions workflows passed on PR head `b6ed95d`, including the 100,000-event Kafka→ClickHouse regression. The [final three-broker HA run](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43893131251) uploaded `m3b-kafka-ha-report`; squash-merged as `8651745`.


## Investigation: intermittent post-quorum-loss voter mismatch (Issue #28)

The historic successes above are real but do **not** imply every subsequent
quorum-loss recovery succeeded. During PR #27 run
[44861653992](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/44861653992)
and PR #29 run
[44862289014](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/44862289014),
the final recovery check **failed at ISR=1/3** after the controller quorum
and two combined broker/controller nodes had been stopped and restarted.
Logs from all three brokers included repeated
`Voter key for VOTE or BEGIN_QUORUM_EPOCH request didn't match the receiver's
replica key` / VOTE requests addressed to a different replica ID.

This is evidence of a KRaft voter/endpoint identity inconsistency during
recovery, **not** proof yet of the underlying cause. Possible causes include
container DNS/IP changes with JVM DNS caching, persisted KRaft directory-ID
and voter records, cluster-format version, or a Kafka defect. Do not claim a
specific one without independent observations. Kafka 4.1's static-voter quorum
configuration and metadata-quorum CLI semantics are documented in
[Apache Kafka KRaft operations](https://kafka.apache.org/41/operations/kraft/).

The M3b experiment now collects read-only KRaft diagnostics:

```bash
python3 -m unittest discover -s scripts -p 'test_m3b_kraft_diagnostics.py' -v
python3 scripts/m3b_kraft_diagnostics.py --phase manual \
  --out-dir /tmp/m3b-kraft-diagnostics
make kafka-ha-smoke
```

The complete CI job writes separate timestamped receipts:

- `before_fault.json` — Kafka nodes are healthy, before leader/controller faults
- `after_failure.json` — snapshot **before** destroying disposable volumes if the actual HA gate fails
- `after_success.json` — when the unchanged RF3/ISR3 recovery and ACKed-record checks succeed

Artifact: `m3b-kraft-quorum-diagnostics`, present independently of the
main `m3b-kafka-ha-report`. It includes per-node persisted `node.id`,
`cluster.id`, `directory.id`, the Docker-assigned IPv4 address, what
each running broker resolves for every other broker, the KRaft quorum
`LeaderId` and high watermark, sanitized diagnostic error classes, and
a mismatch list. It does **not** export environment variables, raw Kafka
events, message payloads or credentials. It makes no writes to Kafka,
does not format or reset the storage directory, and does not fix the quorum.

**Review gate:** compare healthy and failed metadata, determine whether
the persisted voter identities and Docker DNS endpoints remained stable
after stop/start, and inspect quorum LeaderId/high-watermark progress.
Fix the diagnosed cause in a *separate* patch and repeat the actual
full fault matrix. Never waive `ISR=3`, replace the broker ACK check with
a TCP readiness probe, or treat one flaky rerun as a successful fix.

## Follow-up: stopped-leader producer rebootstrap (Issue #28, isolated fix)

A separate CI failure ([run 44867739156, original attempt](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/44867739156))
occurred **before** the quorum-loss/recovery phase. After stopping the data
leader, partition metadata had a new leader and ISR=2, but the producer
tried `localhost:39094` for a stopped former leader and received a
connection-refused error. That event did not receive a Kafka ACK. This is
different from the earlier failure to restore ISR=3 after quorum restart;
the latter remains unresolved as a root-cause investigation.

This lab now makes the client recovery action explicit:

1. Verify actual RF=3, ISR=2, and new leader from Kafka partition metadata.
2. Attempt the sealed record through the **stopped former leader only**;
   require failure (UNKNOWN outcome, not an ACK).
3. Validate that the new bootstrap list contains only *current ISR members*
   and that the live leader is first. Reject invalid/duplicate broker IDs,
   an absent leader, or insufficient ISR. Never silently fall back to the
   stopped broker.
4. Retry **the exact same signed Event ID, key and payload** using the
   live ISR seed list and require a real Kafka `acks=all` confirmation.
   Continue sending the remaining records on live brokers.
5. During the separate `minISR=3` rejection probe, contact a live broker
   and require the actual Kafka NotEnoughReplicas response. A generic
   connection refusal **cannot** pass that safety gate.
6. During the 2/3 controller+broker loss probe, contact the lone surviving
   broker and reject any Kafka ACK; a transport failure is treated as
   UNKNOWN, not proof of rollback or non-append.

Only the *disposable Docker host-facing* advertised listeners and Go bootstrap
addresses are changed from `localhost` to `127.0.0.1`. There is no
production network/client code change. Existing voter quorum settings,
RF3/minISR2 baseline, ISR3 final recovery requirement and 35 ACKed record
verification remain unchanged.

A separate exact-HEAD CI workflow `m3b-kraft-recovery-repeated-evidence`
runs **three independent, fully isolated real fault trials**, each with
its own successful HA JSON and sanitized before/after identity/quorum artifacts.
All three must pass. Repeating successful runs improves confidence but
**does not prove the underlying intermittent voter-key recovery problem
is permanently solved**, guarantee a bounded RTO, or establish production
Kafka HA. Retain Issue #28 until its quorum-loss failure mechanism is
reproduced and distinguished from the client reconnect issue.

## M3b-P2: Fail-closed quorum status versus partition ISR recovery evidence

Historical Issue #28 includes genuine failures after controller restart in
which KRaft VOTE requests carried target voter IDs different from the
receiving broker's persisted node identity, while the partition remained
stuck below ISR=3. This is **not** explained by the separately fixed stale
producer bootstrap path (#37). Until there is a controlled reproduction
comparing before/after identity and quorum state, do not assert the Kafka
root cause.

The lab now records an independent, exact-test-code-HEAD timeline to
`/tmp/m3b-kraft-recovery-timeline.json` for every real fault experiment.
The test saves this receipt even on a `t.Fatal`/partial recovery. Incomplete
evidence has `result=INCOMPLETE`, never `PASS`. The successful receipt
requires *all eight* ordered observations:

1. Two combined broker/controller nodes stopped (no inferred ISR).
2. Broker write has **no ACK** during quorum loss (append outcome UNKNOWN).
3. First voter process restarted.
4. Actual `kafka-metadata-quorum.sh describe --status` observation has a
   parseable controller LeaderId, HighWatermark and CurrentVoters. This is the
   **time the status was observed**, not the exact instant quorum formed.
5. Fresh Kafka partition metadata verifies the *intermediate ISR=2*.
6. Second voter process restarted.
7. Fresh Kafka partition metadata verifies *final ISR=3*.
8. All 35 previously broker-ACKed Event IDs are reread and validated.

The receipt reports distinct sampled durations from first restart to first
valid KRaft quorum status, first restart to ISR2, second restart to ISR3,
and two-voter-loss to completed acknowledged readback. All measurements come
from the same Go process's monotonic clock. This does **not** estimate a
production RTO, prove voter-key mismatch is resolved, or turn CLI
availability into Kafka client availability.

A missing, duplicated, out-of-order, non-monotonic or unverifiable milestone
must fail the independent timeline gate. The existing Topic minISR
NotEnoughReplicas assertion, final RF3/ISR3 assertion and broker ACK
replay checks are unchanged. The real CI workflows attach both the timeline
and separate sanitized voter identity/DNS/quorum snapshots for each run,
including failures where available.

Release and incident review must **not** close Issue #28 merely because
the lab eventually reports ISR3 in a few trials. Comparing the KRaft voter
IDs in logged requests with broker persisted metadata, controller quorum
HighWatermark, DNS and elapsed ISR catch-up across multiple fixed-image runs
remains the outstanding investigation.

## P0 identity and cross-target VOTE evidence audit (Issue #28)

Historical failed CI logs reported VOTE requests from `RaftManager id=1`
carrying a target `ReplicaKey(id=3)` to the receiving broker with
`RaftManager id=2`, and the symmetric wrong target from the same sender to
broker 3. The recipients rejected the voter keys. This is **observed wrong
receiver/target matching**, not proof of Docker DNS, Java address caching,
persistent storage corruption, or a Kafka bug.

This experiment now produces an additional redacted artifact:
`m3b-voter-identity-audit` (or `m3b-voter-audit-trial-*` for independent
trials). The audit runs **after** the real fault test, or during failure
cleanup before disposable volumes are removed:

- Compares `before_fault.json` with `after_success.json` or
  `after_failure.json`: persisted per-broker `node.id`, `cluster.id`,
  `directory.id`, Docker IPv4 changes and peers' actual Docker DNS mapping.
- Samples a bounded tail of real KRaft container logs, extracting only
  `sender_id`, `receiver_id`, `requested_voter_id` and counts for
  *cross-target* VOTE recipient failures; original log lines, opaque directory
  values, event payloads and secrets are **not** persisted in the audit JSON.
- Uses clear outcomes: `NO_MISMATCH_OBSERVED`,
  `VOTER_TARGET_ID_MISMATCH_OBSERVED`,
  `IDENTITY_OR_DNS_DRIFT_OBSERVED` or `INCOMPLETE`.
- Requires a complete baseline, complete post-recovery snapshot, a successful
  log collection, exact tested Git HEAD and zero observed mismatches to pass
  the normal real-HA CI gate. Missing observations do **not** mean healthy.
- If the HA experiment itself fails, the cleanup phase still records
  incomplete/abnormal evidence whenever possible. The audit does not restart
  brokers, reset metadata, change quorum membership, or waive ISR3.

Local reproducibility and offline analysis:

```bash
python3 -m unittest discover -s scripts -p 'test_m3b_kraft_audit.py' -v
make kafka-ha-smoke
# The smoke script invokes the audit automatically before Docker cleanup.
cat /tmp/m3b-kraft-diagnostics/voter-identity-audit.json
```

The snapshot uses `getent hosts` *inside* the Kafka containers, which
does **not** reveal the Kafka JVM's potentially cached resolved addresses.
A bounded log tail can also miss older events. Therefore
`NO_MISMATCH_OBSERVED` means only that **this measured window** showed no
cross-target mismatch; it is not a root-cause exclusion or production HA
claim. Compare anomalous logs and snapshots before changing any cluster
configuration.

## Controlled Docker endpoint-stability experiment (Issue #28)

PR #40's exact-HEAD fault trials exposed an actual test-harness instability:
during otherwise successful staged quorum recovery, Docker reassigned container
IPv4 addresses. In trial 1, broker2/broker3 exchanged their .3/.2 addresses;
in trial 2, they exchanged .4/.2; a third independent trial changed broker1
and broker3. Post-restart `getent` DNS and persisted `node.id` /
`cluster.id` / `directory.id` were consistent, and no cross-target VOTE
mismatch was observed in those successful recovery log samples.

The **experiment-only** Compose file now attaches the three
`apache/kafka:4.1.0` combined-role voters to an explicitly configured
private Docker network with stable addresses:

| Kafka voter | Fixed Docker IPv4 | Controller listener |
|---|---|---|
| broker1 / node.id=1 | 172.29.241.11 | broker1:9093 |
| broker2 / node.id=2 | 172.29.241.12 | broker2:9093 |
| broker3 / node.id=3 | 172.29.241.13 | broker3:9093 |

This network is restricted to a disposable local/CI lab, is not production
IPAM advice, and must not replace real network/security configuration. The
KRaft static quorum IDs and all existing ACK, RF3/ISR3, minISR, quorum-loss,
readback and exact-head evidence checks remain unchanged. The audit now
**fails closed** on any container IPv4 change even when the post-restart DNS
eventually resolves consistently; a regression unit test first reproduced
the earlier false `NO_MISMATCH_OBSERVED` classification on swapped IPs.

**What this experiment can establish:** Docker-assigned voter addresses stayed
stable across the tested stop/start fault windows, and all existing real
Kafka recovery assertions passed.

**What it cannot establish:** The Kafka JVM previously cached a wrong voter
address, or all historical cross-target VOTE errors originated from Docker IP
reuse. That would require evidence of the JVM's effective resolved peer
endpoint at the time it sent the wrong VOTE request, or an independently
reproduced old failure rescued by controlled endpoint stabilization.
Keep [Issue #28](https://github.com/avaargsh/llm-telemetry-pipeline/issues/28)
open for that independent root-cause verification; do not claim a production
Kafka HA or recovery-time guarantee.
