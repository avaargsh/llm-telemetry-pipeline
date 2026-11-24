import unittest
from m3b_dns_ttl_experiment import verify

HEAD="test-head"
def observed():
    return {"exact_head":HEAD,"profile":"dynamic",
            "result":"RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED",
            "endpoint_exposure":"ADDRESS_REASSIGNMENT_OBSERVED",
            "reassigned_addresses":[{"ipv4":"172.18.0.2","before_owner":"broker1","after_owner":"broker2"}],
            "cross_target_voter_request_count":0,
            "initial_ack_count":35,"acknowledged_readback_verified":True,
            "causal_root_cause_proven":False}
class MitigationEvidence(unittest.TestCase):
    def test_exposed_recovery(self):
        self.assertEqual(verify(HEAD,observed()),"EXPOSED_RECOVERY_VERIFIED")
    def test_unexposed_not_success(self):
        r=observed()
        r.update(endpoint_exposure="NO_ADDRESS_CHANGE_OBSERVED",reassigned_addresses=[])
        self.assertEqual(verify(HEAD,r),"UNEXPOSED_INCONCLUSIVE")
    def test_wrong_target_or_failed_recovery_rejected(self):
        for k,v in (("cross_target_voter_request_count",1),("acknowledged_readback_verified",False),
                    ("result","FAILED_RUN_RECOVERY_UNVERIFIED"),("exact_head","wrong"),
                    ("initial_ack_count",34),("causal_root_cause_proven",True),
                    ("reassigned_addresses",[]),("cross_target_voter_request_count",False)):
            with self.subTest(k=k):
                r=observed();r[k]=v
                with self.assertRaises(ValueError): verify(HEAD,r)
if __name__=="__main__":
    unittest.main()
