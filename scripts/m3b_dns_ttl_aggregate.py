"""Aggregate three independent TTL-treated trials without counting unexposed runs."""
import argparse, json
from pathlib import Path

def verify(head, reports, expected_trials=3, minimum_exposed=2):
    if len(reports)!=expected_trials: raise ValueError("wrong number of TTL trial receipts")
    exposed=inconclusive=0
    for doc in reports:
        if not isinstance(doc,dict) or doc.get("exact_head")!=head: raise ValueError("TTL receipt bound to wrong HEAD")
        if doc.get("profile")!="dynamic-ttl-zero" or doc.get("result")!="RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED": raise ValueError("TTL trial did not verify Kafka recovery")
        if doc.get("acknowledged_readback_verified") is not True or doc.get("initial_ack_count")!=35: raise ValueError("TTL trial did not verify all 35 ACKed records")
        if doc.get("cross_target_voter_request_count")!=0 or doc.get("causal_root_cause_proven") is not False: raise ValueError("TTL trial contains voter mismatch or unsupported causality claim")
        exposure=doc.get("endpoint_exposure"); changed=doc.get("ip_changed_nodes"); reassigned=doc.get("reassigned_addresses")
        if exposure=="ADDRESS_REASSIGNMENT_OBSERVED":
            if not isinstance(changed,list) or len(changed)!=2 or len(set(changed))!=2: raise ValueError("invalid changed-node list")
            if not isinstance(reassigned,list) or len(reassigned)!=2: raise ValueError("invalid reassignment list")
            exposed+=1
        elif exposure in {"NO_ADDRESS_CHANGE_OBSERVED","ADDRESS_CHANGE_WITHOUT_REASSIGNMENT"}:
            if reassigned!=[]: raise ValueError("unexposed trial carries reassignment rows")
            inconclusive+=1
        else: raise ValueError("unknown endpoint exposure classification")
    if exposed<minimum_exposed: raise ValueError("insufficient independently exposed TTL trials")
    if exposed+inconclusive!=expected_trials: raise ValueError("TTL trial accounting mismatch")
    return {"exact_head":head,"result":"TTL_TREATMENT_EXPOSURE_GATE_PASS","exposed_recovery_trials":exposed,
            "inconclusive_unexposed_trials":inconclusive,"required_exposed_trials":minimum_exposed,
            "total_trials":expected_trials,"causal_root_cause_proven":False,
            "scope":"CI_Docker_TTL_treatment_evidence_not_production_HA_or_root_cause_proof"}

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--head",required=True);p.add_argument("--directory",required=True)
    p.add_argument("--expected-trials",type=int,default=3);p.add_argument("--minimum-exposed",type=int,default=2);p.add_argument("--out",required=True);a=p.parse_args()
    try:
        files=sorted(Path(a.directory).rglob("network-ab-comparison.json"))
        docs=[json.loads(x.read_text(encoding="utf-8")) for x in files]
        result=verify(a.head,docs,a.expected_trials,a.minimum_exposed)
        Path(a.out).write_text(json.dumps(result,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    except (OSError,UnicodeError,ValueError,TypeError,AttributeError) as exc: p.exit(2,f"TTL exposure aggregate failed closed: {type(exc).__name__}\n")
    print(json.dumps(result,sort_keys=True))
if __name__=="__main__": main()
