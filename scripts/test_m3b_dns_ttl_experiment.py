import unittest
from m3b_dns_ttl_experiment import verify

HEAD="test-head"
def observed():
    return {"exact_head":HEAD,"profile":"dynamic-ttl-zero",
            "result":"RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED",
            "endpoint_exposure":"ADDRESS_REASSIGNMENT_OBSERVED",
            "ip_changed_nodes":["broker1","broker2"],
            "reassigned_addresses":[{"ipv4":"172.18.0.2","before_owner":"broker1","after_owner":"broker2"},
                                    {"ipv4":"172.18.0.3","before_owner":"broker2","after_owner":"broker1"}],
            "cross_target_voter_request_count":0,
            "initial_ack_count":35,"acknowledged_readback_verified":True,
            "causal_root_cause_proven":False}
class MitigationEvidence(unittest.TestCase):
    def test_untreated_control_cannot_pass_ttl_gate(self):
        r=observed(); r["profile"]="dynamic"
        with self.assertRaises(ValueError): verify(HEAD,r)
    def test_exposed_recovery(self):
        self.assertEqual(verify(HEAD,observed()),"EXPOSED_RECOVERY_VERIFIED")
    def test_unexposed_not_success(self):
        r=observed()
        r.update(endpoint_exposure="NO_ADDRESS_CHANGE_OBSERVED",reassigned_addresses=[],ip_changed_nodes=[])
        self.assertEqual(verify(HEAD,r),"UNEXPOSED_INCONCLUSIVE")
    def test_wrong_target_or_failed_recovery_rejected(self):
        for k,v in (("cross_target_voter_request_count",1),("acknowledged_readback_verified",False),
                    ("result","FAILED_RUN_RECOVERY_UNVERIFIED"),("exact_head","wrong"),
                    ("initial_ack_count",34),("causal_root_cause_proven",True),
                    ("reassigned_addresses",[]),("cross_target_voter_request_count",False)):
            with self.subTest(k=k):
                r=observed();r[k]=v
                with self.assertRaises(ValueError): verify(HEAD,r)
    def test_reassignment_structure_must_be_consistent(self):
        for rows in ([{}], ["invalid"], [{"ipv4":"bad","before_owner":"broker1","after_owner":"broker2"}],
                     [{"ipv4":"172.18.0.2","before_owner":"broker1","after_owner":"broker1"}],
                     [{"ipv4":"172.18.0.2","before_owner":"broker1","after_owner":"broker4"}],
                     [{"ipv4":"172.18.0.2","before_owner":"broker1","after_owner":"broker3"}],
                     [observed()["reassigned_addresses"][0]]*2):
            with self.subTest(rows=rows):
                r=observed();r["reassigned_addresses"]=rows
                with self.assertRaises(ValueError): verify(HEAD,r)
    def test_endpoint_classification_agrees_with_changed_nodes(self):
        for mode, changed in (("NO_ADDRESS_CHANGE_OBSERVED",["broker1"]),
                              ("ADDRESS_CHANGE_WITHOUT_REASSIGNMENT",[]),
                              ("ADDRESS_REASSIGNMENT_OBSERVED",["broker1"]),
                              ("ADDRESS_REASSIGNMENT_OBSERVED",["broker1","broker2","broker2"])):
            with self.subTest(mode=mode,changed=changed):
                r=observed();r.update(endpoint_exposure=mode,ip_changed_nodes=changed)
                if mode!="ADDRESS_REASSIGNMENT_OBSERVED": r["reassigned_addresses"]=[]
                with self.assertRaises(ValueError): verify(HEAD,r)
if __name__=="__main__":
    unittest.main()
