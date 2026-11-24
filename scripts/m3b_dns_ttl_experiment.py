"""Require actual endpoint reallocation, not merely a successful restart."""
import argparse
import json
import ipaddress
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
    nodes={"broker1","broker2","broker3"}
    changed=report.get("ip_changed_nodes")
    if not isinstance(changed,list) or any(not isinstance(n,str) or n not in nodes for n in changed) or len(set(changed))!=len(changed):
        raise ValueError("invalid changed node list")
    if exposure=="NO_ADDRESS_CHANGE_OBSERVED" and reassigned==[] and not changed:
        return "UNEXPOSED_INCONCLUSIVE"
    if exposure=="ADDRESS_CHANGE_WITHOUT_REASSIGNMENT" and reassigned==[] and changed:
        return "UNEXPOSED_INCONCLUSIVE"
    if exposure!="ADDRESS_REASSIGNMENT_OBSERVED" or not isinstance(reassigned,list) or not reassigned:
        raise ValueError("missing reassignment evidence")
    addresses, prior, current=set(),set(),set()
    for row in reassigned:
        if not isinstance(row,dict):
            raise ValueError("invalid reassignment row")
        ip,before,after=row.get("ipv4"),row.get("before_owner"),row.get("after_owner")
        if not isinstance(ip,str) or not isinstance(before,str) or not isinstance(after,str):
            raise ValueError("invalid reassignment fields")
        addr=ipaddress.IPv4Address(ip)
        if before==after or before not in changed or after not in changed:
            raise ValueError("reassignment owners inconsistent with changed nodes")
        if addr in addresses or before in prior or after in current:
            raise ValueError("duplicate reassignment address or owner")
        addresses.add(addr)
        prior.add(before)
        current.add(after)
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
