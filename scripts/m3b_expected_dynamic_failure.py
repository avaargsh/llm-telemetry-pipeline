"""Classify only the known untreated dynamic-IP KRaft failure signature."""
import argparse, ipaddress, json
from pathlib import Path
NODES={"broker1","broker2","broker3"}
def verify(head, report, comparison, audit, sockets, before, after):
    docs=(report,comparison,audit,sockets,before,after)
    if not isinstance(head,str) or len(head)!=40 or any(c not in "0123456789abcdef" for c in head): raise ValueError("invalid exact HEAD")
    if any(not isinstance(d,dict) or d.get("exact_head")!=head for d in docs): raise ValueError("evidence is not bound to exact HEAD")
    if report.get("scenario")!="two_voter_parallel_restart" or report.get("result")!="INCOMPLETE": raise ValueError("not an incomplete recovery receipt")
    if report.get("initial_isr")!=3 or report.get("acknowledged_ids")!=35 or report.get("no_ack_during_quorum_loss") is not True: raise ValueError("initial ISR/ACK safety evidence incomplete")
    if comparison.get("profile")!="dynamic" or comparison.get("result")!="FAILED_RUN_RECOVERY_UNVERIFIED": raise ValueError("not the expected untreated dynamic failure")
    if comparison.get("endpoint_exposure")!="ADDRESS_REASSIGNMENT_OBSERVED" or comparison.get("acknowledged_readback_verified") is not False or comparison.get("causal_root_cause_proven") is not False: raise ValueError("invalid failure/exposure classification")
    changed=comparison.get("ip_changed_nodes"); rows=comparison.get("reassigned_addresses")
    if not isinstance(changed,list) or len(changed)!=2 or len(set(changed))!=2 or not set(changed)<=NODES or not isinstance(rows,list) or len(rows)!=2: raise ValueError("expected two changed voters and two reassigned addresses")
    ips,olds,news=set(),set(),set()
    for row in rows:
        if not isinstance(row,dict): raise ValueError("invalid reassignment row")
        ip=ipaddress.IPv4Address(row.get("ipv4","")); old=row.get("before_owner"); new=row.get("after_owner")
        if old not in changed or new not in changed or old==new or ip in ips or old in olds or new in news: raise ValueError("reassignment owners or addresses inconsistent")
        ips.add(ip); olds.add(old); news.add(new)
    count=comparison.get("cross_target_voter_request_count")
    if type(count) is not int or count<1: raise ValueError("no measured cross-target VOTE mismatch")
    if audit.get("complete_observations") is not True or audit.get("verdict")!="VOTER_TARGET_ID_MISMATCH_OBSERVED" or audit.get("identity_changes")!=[] or audit.get("dns_misroutes")!=[] or audit.get("ip_changed_nodes")!=changed: raise ValueError("independent voter audit did not confirm exact signature")
    mismatches=audit.get("observed_voter_target_mismatches")
    if not isinstance(mismatches,list) or sum(x.get("count",0) for x in mismatches if isinstance(x,dict))!=count: raise ValueError("independent VOTE counts disagree")
    if sockets.get("result")!="EVIDENCE_CAPTURED_NOT_CAUSAL_PROOF" or type(sockets.get("sample_count")) is not int or sockets["sample_count"]<3: raise ValueError("insufficient kernel socket evidence")
    if before.get("phase")!="before_fault" or after.get("phase")!="after_failure": raise ValueError("incorrect diagnostic phases")
    for snap in (before,after):
        if snap.get("identity_verdict")!="PASS_IDENTITY" or not isinstance(snap.get("nodes"),dict) or set(snap["nodes"])!=NODES: raise ValueError("incomplete persisted identity snapshot")
    return {"schema_version":1,"exact_head":head,"classification":"EXPECTED_UNTREATED_FAILURE_REPRODUCED","kafka_recovery":"FAILED_ISR3_NOT_RESTORED","endpoint_exposure":"ADDRESS_REASSIGNMENT_OBSERVED","cross_target_voter_request_count":count,"acknowledged_readback_verified":False,"causal_root_cause_proven":False,"scope":"expected_failure_reproduction_only_not_a_Kafka_recovery_pass"}
def main():
    p=argparse.ArgumentParser(description=__doc__)
    for n in ("head","report","comparison","audit","sockets","before","after","out"): p.add_argument("--"+n,required=True)
    a=p.parse_args()
    try:
        docs=[json.loads(Path(x).read_text(encoding="utf-8")) for x in (a.report,a.comparison,a.audit,a.sockets,a.before,a.after)]
        result=verify(a.head,*docs); Path(a.out).write_text(json.dumps(result,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    except (OSError,UnicodeError,ValueError,TypeError,AttributeError,KeyError) as exc: p.exit(2,f"expected dynamic failure not proven: {type(exc).__name__}\n")
    print(json.dumps(result,sort_keys=True))
if __name__=="__main__": main()
