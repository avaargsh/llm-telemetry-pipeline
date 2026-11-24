"""Require actual endpoint reallocation, not merely a successful restart."""
import argparse
import json
from pathlib import Path

def verify(head, report):
    if not head or head=="UNBOUND" or report.get("exact_head")!=head:
        raise ValueError("wrong exact head")
    if (report.get("profile")!="dynamic" or
        report.get("result")!="RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED" or
        report.get("acknowledged_readback_verified") is not True or
        report.get("initial_ack_count")!=35 or
        report.get("causal_root_cause_proven") is not False):
        raise ValueError("recovery receipt not verified")
    count=report.get("cross_target_voter_request_count")
    if type(count) is not int or count!=0:
        raise ValueError("cross-target vote observed or unknown")
    exposure=report.get("endpoint_exposure")
    reassigned=report.get("reassigned_addresses")
    if exposure in {"NO_ADDRESS_CHANGE_OBSERVED","ADDRESS_CHANGE_WITHOUT_REASSIGNMENT"} and reassigned==[]:
        return "UNEXPOSED_INCONCLUSIVE"
    if exposure!="ADDRESS_REASSIGNMENT_OBSERVED" or not isinstance(reassigned,list) or not reassigned:
        raise ValueError("missing reassignment evidence")
    return "EXPOSED_RECOVERY_VERIFIED"

if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--head",required=True)
    p.add_argument("--report",required=True)
    a=p.parse_args()
    try:
        result=verify(a.head,json.loads(Path(a.report).read_text()))
    except (ValueError,OSError,TypeError,AttributeError) as exc:
        p.exit(2,f"TTL experiment rejected: {exc}\n")
    print(result)
    if result!="EXPOSED_RECOVERY_VERIFIED":
        p.exit(2,"No qualifying address reassignment; no mitigation proof\n")
