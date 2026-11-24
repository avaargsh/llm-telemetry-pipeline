import unittest
from m3b_dns_ttl_aggregate import verify
HEAD="a"*40
def receipt(exposed=True):
    return {"exact_head":HEAD,"profile":"dynamic-ttl-zero","result":"RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED",
            "endpoint_exposure":"ADDRESS_REASSIGNMENT_OBSERVED" if exposed else "NO_ADDRESS_CHANGE_OBSERVED",
            "ip_changed_nodes":["broker1","broker2"] if exposed else [],
            "reassigned_addresses":[{"ipv4":"172.18.0.2","before_owner":"broker1","after_owner":"broker2"},{"ipv4":"172.18.0.3","before_owner":"broker2","after_owner":"broker1"}] if exposed else [],
            "acknowledged_readback_verified":True,"initial_ack_count":35,"cross_target_voter_request_count":0,"causal_root_cause_proven":False}
class TTLExposureAggregateTests(unittest.TestCase):
    def test_two_exposed_and_one_unexposed(self):
        r=verify(HEAD,[receipt(),receipt(),receipt(False)])
        self.assertEqual(r["exposed_recovery_trials"],2);self.assertEqual(r["inconclusive_unexposed_trials"],1)
    def test_one_exposed_is_insufficient(self):
        with self.assertRaises(ValueError):verify(HEAD,[receipt(),receipt(False),receipt(False)])
    def test_failed_recovery_wrong_target_or_causal_claim_rejected(self):
        for k,v in (("acknowledged_readback_verified",False),("cross_target_voter_request_count",1),("result","FAILED_RUN_RECOVERY_UNVERIFIED"),("causal_root_cause_proven",True)):
            rows=[receipt(),receipt(),receipt()];rows[1][k]=v
            with self.subTest(field=k),self.assertRaises(ValueError):verify(HEAD,rows)
    def test_wrong_head_profile_and_trial_count_rejected(self):
        rows=[receipt(),receipt(),receipt()];rows[1]["exact_head"]="b"*40
        with self.assertRaises(ValueError):verify(HEAD,rows)
        rows=[receipt(),receipt(),receipt()];rows[1]["profile"]="dynamic"
        with self.assertRaises(ValueError):verify(HEAD,rows)
        with self.assertRaises(ValueError):verify(HEAD,[receipt(),receipt()])
    def test_malformed_reassignment_rejected(self):
        rows=[receipt(),receipt(),receipt(False)];rows[0]["reassigned_addresses"]=[]
        with self.assertRaises(ValueError):verify(HEAD,rows)
if __name__=="__main__":unittest.main()
