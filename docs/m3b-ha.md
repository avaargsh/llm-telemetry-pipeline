# M3b-P1 · Three-broker KRaft RF=3/minISR=2 fault experiments

**Status: in review.** This is a disposable Docker fault lab, not a production HA deployment recipe.

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
7. Export JSON with initial/failover leaders, ISR transitions, Kafka rejection, ACK counts and read-back proof.

## Important boundaries

- Three *combined* controller+broker processes share failure domains in this lab; Kafka recommends dedicated controllers in critical production environments.
- A single-partition, single-leader experiment does not prove all partitions, rack-awareness, zone failure, disk corruption or cross-region survivability.
- `minISR=3` during step 4 is **test-only** and should not be mistaken for the permanent target. Stopping two controllers at once would lose controller quorum and confound the ISR-only probe.
- Acknowledged records can still be lost under disasters outside tested persistence boundaries; no absolute zero-loss claim.
- Kafka `acks=all` does not imply ClickHouse ingestion or PostgreSQL accounting has completed.
- Replaying an unknown-ACK write may expose extra physical messages; treat these as unconfirmed until reconciled, never invent an acknowledgment.
