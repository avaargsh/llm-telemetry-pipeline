"""A/B profile evidence guard: recovery safety remains mandatory in both modes."""
import importlib.util
import pathlib
import json
import subprocess
import sys
import tempfile
import unittest

spec=importlib.util.spec_from_file_location(
    "m3b_network_ab_evidence", pathlib.Path(__file__).with_name("m3b_network_ab_evidence.py"))
evidence=importlib.util.module_from_spec(spec)
spec.loader.exec_module(evidence)

HEAD="deadbeef"
def receipt():
    return {"exact_head":HEAD,"result":"PASS_EXPERIMENT_ONLY",
            "scenario":"two_voter_parallel_restart", "initial_isr":3,
            "recovered_isr":3, "acknowledged_ids":35,
            "all_acknowledged_verified":True,"no_ack_during_quorum_loss":True}

def audit():
    return {"exact_head":HEAD,"complete_observations":True,
            "verdict":"NO_MISMATCH_OBSERVED",
            "identity_changes":[],"dns_misroutes":[],
            "ip_changed_nodes":[],"observed_voter_target_mismatches":[]}

def sockets():
    return {"exact_head":HEAD,"result":"EVIDENCE_CAPTURED_NOT_CAUSAL_PROOF",
            "sample_count":8,"captured_nodes":["broker1","broker2","broker3"],
            "unique_observed_controller_peers":["172.29.241.11","172.29.241.12"]}

def nodes(ips):
    return {"nodes": {f"broker{i}":{"container_ipv4":ip} for i,ip in enumerate(ips,1)}}

class NetworkABContract(unittest.TestCase):
    def test_dynamic_verdict_must_match_details(self):
        before=nodes(['172.22.0.11','172.22.0.12','172.22.0.13'])
        after=nodes(['172.22.0.11','172.22.0.13','172.22.0.12'])
        mismatch={'sender_id':1,'receiver_id':2,'target_id':3,'count':7}
        for verdict, rows, changed, snapshot in (
            ('VOTER_TARGET_ID_MISMATCH_OBSERVED', [], ['broker2','broker3'], after),
            ('NO_MISMATCH_OBSERVED', [mismatch], [], before),
            ('NO_MISMATCH_OBSERVED', [], ['broker2','broker3'], after),
            ('CONTAINER_ENDPOINT_DRIFT_OBSERVED', [], [], before),
            ('CONTAINER_ENDPOINT_DRIFT_OBSERVED', [mismatch], ['broker2','broker3'], after),
        ):
            a=audit(); a.update(verdict=verdict, observed_voter_target_mismatches=rows, ip_changed_nodes=changed)
            with self.subTest(verdict=verdict,rows=rows,changed=changed), self.assertRaises(ValueError):
                evidence.observe('dynamic',HEAD,a,sockets(),before,snapshot)

    def test_dynamic_valid_verdicts(self):
        before=nodes(['172.22.0.11','172.22.0.12','172.22.0.13'])
        after=nodes(['172.22.0.11','172.22.0.13','172.22.0.12'])
        mismatch={'sender_id':1,'receiver_id':2,'target_id':3,'count':7}
        for verdict,rows,changed,snapshot,count in (
            ('NO_MISMATCH_OBSERVED',[],[],before,0),
            ('CONTAINER_ENDPOINT_DRIFT_OBSERVED',[],['broker2','broker3'],after,0),
            ('VOTER_TARGET_ID_MISMATCH_OBSERVED',[mismatch],['broker2','broker3'],after,7),
        ):
            a=audit();a.update(verdict=verdict,observed_voter_target_mismatches=rows,ip_changed_nodes=changed)
            with self.subTest(verdict=verdict):
                self.assertEqual(evidence.observe('dynamic',HEAD,a,sockets(),before,snapshot)['cross_target_voter_request_count'],count)

    def test_fixed_profile_requires_zero_drift(self):
        result=evidence.assess("pinned",HEAD,receipt(),audit(),sockets(),nodes(["1.1.1.1","1.1.1.2","1.1.1.3"]),nodes(["1.1.1.1","1.1.1.2","1.1.1.3"]))
        self.assertEqual(result["result"],"RECOVERY_VERIFIED_ENDPOINTS_FIXED")
        self.assertEqual(result["reassigned_addresses"],[])
        self.assertEqual(result["endpoint_exposure"],"PINNED_CONTROL")

    def test_dynamic_swapped_ipv4_is_observed_not_suppressed(self):
        a=audit()
        a.update(ip_changed_nodes=["broker2","broker3"],verdict="CONTAINER_ENDPOINT_DRIFT_OBSERVED")
        before=nodes(["172.22.0.11","172.22.0.12","172.22.0.13"])
        after=nodes(["172.22.0.11","172.22.0.13","172.22.0.12"])
        result=evidence.assess("dynamic",HEAD,receipt(),a,sockets(),before,after)
        self.assertEqual(result["result"],"RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED")
        self.assertEqual(len(result["reassigned_addresses"]),2)
        self.assertEqual(result["endpoint_exposure"],"ADDRESS_REASSIGNMENT_OBSERVED")
        self.assertIn({"ipv4":"172.22.0.12","before_owner":"broker2","after_owner":"broker3"},result["reassigned_addresses"])

    def test_audited_cross_target_voter_requests_are_not_hidden(self):
        a=audit()
        a["observed_voter_target_mismatches"]=[{"sender_id":1,"receiver_id":2,"target_id":3,"count":7}]
        a["verdict"]="VOTER_TARGET_ID_MISMATCH_OBSERVED"
        result=evidence.assess("dynamic",HEAD,receipt(),a,sockets(),nodes(["1.1.1.1","1.1.1.2","1.1.1.3"]),nodes(["1.1.1.1","1.1.1.2","1.1.1.3"]))
        self.assertEqual(result["cross_target_voter_request_count"],7)
        self.assertFalse(result["causal_root_cause_proven"])

    def test_fixed_drift_and_cross_target_both_fail_closed(self):
        for key,value in (("ip_changed_nodes",["broker2"]),("observed_voter_target_mismatches",[{"count":4}])):
            a=audit();a[key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):
                evidence.assess("pinned",HEAD,receipt(),a,sockets(),nodes(["1.1.1.1","1.1.1.2","1.1.1.3"]),nodes(["1.1.1.1","1.1.1.2","1.1.1.3"]))

    def test_dynamic_cannot_waive_identity_or_dns_misroutes(self):
        for key,val in (("identity_changes",[{"node":"broker2","field":"node.id"}]),("dns_misroutes",[{"observer":"broker1","target":"broker2"}])):
            a=audit();a[key]=val
            with self.subTest(key=key),self.assertRaises(ValueError):
                evidence.assess("dynamic",HEAD,receipt(),a,sockets(),nodes(["1.1.1.1","1.1.1.2","1.1.1.3"]),nodes(["1.1.1.1","1.1.1.2","1.1.1.3"]))

    def test_missing_quorum_ack_or_socket_proof_never_pass(self):
        for key,val in (("acknowledged_ids",34),("no_ack_during_quorum_loss",False),("all_acknowledged_verified",False)):
            r=receipt();r[key]=val
            with self.subTest(key=key),self.assertRaises(ValueError):
                evidence.assess("dynamic",HEAD,r,audit(),sockets(),nodes(["1.1.1.1","1.1.1.2","1.1.1.3"]),nodes(["1.1.1.1","1.1.1.2","1.1.1.3"]))
        bad=sockets();bad["result"]="INCOMPLETE"
        with self.assertRaises(ValueError):
            evidence.assess("dynamic",HEAD,receipt(),audit(),bad,nodes(["1.1.1.1","1.1.1.2","1.1.1.3"]),nodes(["1.1.1.1","1.1.1.2","1.1.1.3"]))


    def test_dynamic_unchanged_addresses_are_not_causal_exposure(self):
        same=nodes(["172.22.0.11","172.22.0.12","172.22.0.13"])
        result=evidence.assess("dynamic",HEAD,receipt(),audit(),sockets(),same,same)
        self.assertEqual(result["endpoint_exposure"],"NO_ADDRESS_CHANGE_OBSERVED")
        self.assertFalse(result["causal_root_cause_proven"])

    def test_dynamic_new_address_without_reassignment_is_distinct(self):
        a=audit()
        a.update(ip_changed_nodes=["broker2"],verdict="CONTAINER_ENDPOINT_DRIFT_OBSERVED")
        result=evidence.assess("dynamic",HEAD,receipt(),a,sockets(),
                               nodes(["172.22.0.11","172.22.0.12","172.22.0.13"]),
                               nodes(["172.22.0.11","172.22.0.14","172.22.0.13"]))
        self.assertEqual(result["endpoint_exposure"],"ADDRESS_CHANGE_WITHOUT_REASSIGNMENT")
        self.assertEqual(result["reassigned_addresses"],[])
        self.assertFalse(result["causal_root_cause_proven"])

    def test_missing_or_invalid_ip_change_evidence_rejected(self):
        for value in (None, "", {}, ["broker2","broker2"]):
            a=audit()
            a["ip_changed_nodes"]=value
            same=nodes(["172.22.0.11","172.22.0.12","172.22.0.13"])
            with self.subTest(value=value),self.assertRaises(ValueError):
                evidence.assess("dynamic",HEAD,receipt(),a,sockets(),same,same)


    def test_failed_recovery_preserves_reassignment_without_pass(self):
        r=receipt()
        r.update(result="INCOMPLETE", all_acknowledged_verified=False)
        a=audit()
        a.update(ip_changed_nodes=["broker1","broker2"],
                 verdict="VOTER_TARGET_ID_MISMATCH_OBSERVED",
                 observed_voter_target_mismatches=[
                     {"sender_id":3,"receiver_id":1,"target_id":2,"count":500}])
        before=nodes(["172.18.0.4","172.18.0.2","172.18.0.3"])
        after=nodes(["172.18.0.2","172.18.0.4","172.18.0.3"])
        result=evidence.assess_failed_run("dynamic",HEAD,r,a,sockets(),before,after)
        self.assertEqual(result["result"],"FAILED_RUN_RECOVERY_UNVERIFIED")
        self.assertEqual(result["endpoint_exposure"],"ADDRESS_REASSIGNMENT_OBSERVED")
        self.assertEqual(len(result["reassigned_addresses"]),2)
        self.assertEqual(result["cross_target_voter_request_count"],500)
        self.assertFalse(result["acknowledged_readback_verified"])
        self.assertFalse(result["causal_root_cause_proven"])
        with self.assertRaises(ValueError):
            evidence.assess("dynamic",HEAD,r,a,sockets(),before,after)

    def test_failed_run_cannot_reuse_another_head(self):
        r=receipt()
        r["exact_head"]="wrong-head"
        same=nodes(["172.18.0.2","172.18.0.3","172.18.0.4"])
        with self.assertRaises(ValueError):
            evidence.assess_failed_run("dynamic",HEAD,r,audit(),sockets(),same,same)

    def test_failed_run_with_success_receipt_stays_failed(self):
        same=nodes(["172.18.0.2","172.18.0.3","172.18.0.4"])
        result=evidence.assess_failed_run("pinned",HEAD,receipt(),audit(),sockets(),same,same)
        self.assertEqual(result["result"],"FAILED_RUN_RECOVERY_UNVERIFIED")
        self.assertFalse(result["acknowledged_readback_verified"])
        self.assertEqual(result["kafka_report_result"],"PASS_EXPERIMENT_ONLY")


    def test_failed_run_cli_writes_receipt_and_exits_nonzero(self):
        same=nodes(["172.18.0.2","172.18.0.3","172.18.0.4"])
        r=receipt()
        r["result"]="INCOMPLETE"
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp)
            args=[sys.executable,str(pathlib.Path(evidence.__file__).resolve()),
                  "--failed-run","--profile","dynamic","--head",HEAD]
            for name,data in (("report",r),("audit",audit()),("sockets",sockets()),
                              ("before",same),("after",same)):
                p=root/(name+".json")
                p.write_text(json.dumps(data),encoding="utf-8")
                args.extend(["--"+name,str(p)])
            out=root/"comparison.json"
            completed=subprocess.run(args+["--out",str(out)],
                                     capture_output=True,text=True,check=False)
            self.assertEqual(completed.returncode,2,completed.stderr)
            result=json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(result["result"],"FAILED_RUN_RECOVERY_UNVERIFIED")
            self.assertFalse(result["acknowledged_readback_verified"])
            self.assertFalse(result["causal_root_cause_proven"])

    def test_unbound_head_and_malformed_mode_fail(self):
        for profile,head in (("dynamic","UNBOUND"),("fixed",HEAD),("random",HEAD)):
            with self.subTest(profile=profile,head=head),self.assertRaises(ValueError):
                evidence.assess(profile,head,receipt(),audit(),sockets(),nodes(["1.1.1.1","1.1.1.2","1.1.1.3"]),nodes(["1.1.1.1","1.1.1.2","1.1.1.3"]))

if __name__=="__main__":
    unittest.main()
