#!/usr/bin/env python3
"""Independent read-only pinned-vs-dynamic Kafka controller endpoint comparator.

Any Kafka no-ACK, RF/ISR, 35 ACKed replay, persisted identity or DNS failure
remains a FAIL in both modes. Only explicitly dynamic Docker IP reallocation
and sampled wrong-target VOTE events are observational, never a production PASS
or causal root-cause proof.
"""
import argparse
import ipaddress
import json
import os
import pathlib
import tempfile

NODES = ("broker1","broker2","broker3")
MODES = {"pinned","dynamic","dynamic-ttl-zero"}


def ensure(ok, reason):
    if not ok:
        raise ValueError(reason)


def _ip_map(snapshot):
    ensure(isinstance(snapshot,dict) and isinstance(snapshot.get("nodes"),dict),
           "missing persisted before/after network identity")
    nodes = snapshot["nodes"]
    ensure(set(nodes) == set(NODES), "incomplete controller membership snapshot")
    values = {}
    for node in NODES:
        row = nodes[node]
        ensure(isinstance(row,dict),"invalid node row")
        ip = row.get("container_ipv4")
        try:
            ensure(ipaddress.ip_address(ip).version==4,"missing Docker IPv4")
        except (ValueError,TypeError):
            raise ValueError("invalid Docker IPv4 in snapshot") from None
        values[node] = ip
    ensure(len(set(values.values()))==3,"duplicate Docker container addresses")
    return values


def observe(profile, head, audit, sockets, before, after):
    ensure(profile in MODES,"unsupported network profile")
    ensure(isinstance(head,str) and head and head!="UNBOUND","missing exact HEAD")
    for name,doc in (("audit",audit),("sockets",sockets)):
        ensure(isinstance(doc,dict) and doc.get("exact_head")==head,
               name+" proof bound to wrong head")
    ensure(audit.get("complete_observations") is True,
           "missing persisted voter/Docker DNS evidence")
    ensure(audit.get("identity_changes")==[], "persisted voter ID changed")
    ensure(audit.get("dns_misroutes")==[], "current Docker DNS does not match container")
    ensure(sockets.get("result")=="EVIDENCE_CAPTURED_NOT_CAUSAL_PROOF",
           "missing real controller TCP connection evidence")
    ensure(type(sockets.get("sample_count")) is int and sockets["sample_count"]>=3,
           "too few socket samples")
    before_ips,after_ips=_ip_map(before),_ip_map(after)
    changed=sorted(n for n in NODES if before_ips[n]!=after_ips[n])
    audited_changes=audit.get("ip_changed_nodes")
    ensure(isinstance(audited_changes,list) and
           all(isinstance(n,str) and n in NODES for n in audited_changes) and
           len(set(audited_changes))==len(audited_changes),
           "missing or invalid audited endpoint changes")
    ensure(sorted(audited_changes)==changed,
           "audit and snapshots disagree on endpoint changes")
    mismatches=audit.get("observed_voter_target_mismatches")
    ensure(isinstance(mismatches,list),"missing cross-target VOTE counts")
    count=0
    for row in mismatches:
        ensure(isinstance(row,dict) and type(row.get("count")) is int
               and row["count"]>0,"invalid cross-target VOTE count")
        count+=row["count"]
    if profile=="pinned":
        ensure(audit.get("verdict")=="NO_MISMATCH_OBSERVED"
               and not changed and not mismatches,
               "pinned voter endpoint / VOTE identity unstable")
    else:
        ensure(audit.get("verdict") in {
            "NO_MISMATCH_OBSERVED",
            "CONTAINER_ENDPOINT_DRIFT_OBSERVED",
            "VOTER_TARGET_ID_MISMATCH_OBSERVED",
        }, "dynamic experiment has unclassified unsafe evidence")
    reassigned=[
        {"ipv4": ip, "before_owner": prior, "after_owner": current}
        for prior,ip in before_ips.items()
        for current,now in after_ips.items()
        if prior!=current and ip==now
    ]
    if profile=="pinned":
        exposure="PINNED_CONTROL"
    elif reassigned:
        exposure="ADDRESS_REASSIGNMENT_OBSERVED"
    elif changed:
        exposure="ADDRESS_CHANGE_WITHOUT_REASSIGNMENT"
    else:
        exposure="NO_ADDRESS_CHANGE_OBSERVED"
    return {
        "schema_version":1, "exact_head":head, "profile":profile,
        "endpoint_exposure":exposure,
        "ip_changed_nodes":changed, "reassigned_addresses":reassigned,
        "cross_target_voter_request_count":count,
        "controller_socket_samples":sockets["sample_count"],
        "causal_root_cause_proven":False,
        "scope":"isolated_Docker_A_B_not_java_DNS_cache_not_production_HA",
    }


def assess(profile, head, report, audit, sockets, before, after):
    ensure(isinstance(report,dict) and report.get("exact_head")==head,
           "Kafka proof bound to wrong head")
    ensure(report.get("scenario")=="two_voter_parallel_restart"
           and report.get("result")=="PASS_EXPERIMENT_ONLY",
           "real parallel KRaft fault did not recover")
    ensure(report.get("initial_isr")==3 and report.get("recovered_isr")==3,
           "RF3/ISR3 recovery not independently verified")
    ensure(report.get("acknowledged_ids")==35 and
           report.get("all_acknowledged_verified") is True,
           "35/35 ACKed records not independently recovered")
    ensure(report.get("no_ack_during_quorum_loss") is True,
           "unsafe Kafka broker ACK during controller majority loss")
    result=observe(profile,head,audit,sockets,before,after)
    result.update(
        result=("RECOVERY_VERIFIED_ENDPOINTS_FIXED" if profile=="pinned"
                else "RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED"),
        initial_ack_count=35, acknowledged_readback_verified=True)
    return result


def assess_failed_run(profile, head, report, audit, sockets, before, after):
    """Preserve observed exposure without accepting an unsuccessful run."""
    ensure(isinstance(report,dict) and report.get("exact_head")==head,
           "Kafka proof bound to wrong head")
    ensure(report.get("scenario")=="two_voter_parallel_restart"
           and report.get("result") in {"INCOMPLETE","PASS_EXPERIMENT_ONLY"},
           "unknown Kafka receipt state")
    result=observe(profile,head,audit,sockets,before,after)
    result.update(result="FAILED_RUN_RECOVERY_UNVERIFIED",
                  kafka_report_result=report["result"],
                  acknowledged_readback_verified=False)
    return result


def load(path):
    return json.loads(pathlib.Path(path).read_text(encoding="utf-8"))


def write_atomic(path,data):
    out=pathlib.Path(path)
    out.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    candidate=None
    try:
        with tempfile.NamedTemporaryFile("w",encoding="utf-8",dir=out.parent,
                                         prefix=".m3b-network-ab-",delete=False) as f:
            candidate=f.name
            os.chmod(candidate,0o600)
            json.dump(data,f,indent=2,sort_keys=True)
            f.write("\n")
        os.replace(candidate,out)
    finally:
        if candidate and os.path.exists(candidate):
            os.unlink(candidate)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ("profile","head","report","audit","sockets","before","after","out"):
        p.add_argument("--"+name,required=True)
    p.add_argument("--failed-run",action="store_true")
    x=p.parse_args()
    try:
        assess_run=assess_failed_run if x.failed_run else assess
        result=assess_run(x.profile,x.head,load(x.report),load(x.audit),load(x.sockets),
                      load(x.before),load(x.after))
    except (ValueError,OSError,UnicodeError) as exc:
        write_atomic(x.out,{"schema_version":1,"exact_head":x.head,"profile":x.profile,
                            "result":"INCOMPLETE_OR_FAILED","error_class":"evidence_validation",
                            "causal_root_cause_proven":False})
        p.exit(2, f"FAIL CLOSED KRaft network comparator: {exc}\n")
    write_atomic(x.out,result)
    print(json.dumps(result,sort_keys=True))
    if x.failed_run:
        p.exit(2, "FAIL CLOSED: recovery run unsuccessful; exposure recorded\n")


if __name__=="__main__":
    main()
