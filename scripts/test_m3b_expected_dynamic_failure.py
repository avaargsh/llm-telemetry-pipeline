import copy, unittest
from m3b_expected_dynamic_failure import verify
HEAD="a"*40
def evidence():
    report={"exact_head":HEAD,"scenario":"two_voter_parallel_restart","result":"INCOMPLETE","initial_isr":3,"acknowledged_ids":35,"no_ack_during_quorum_loss":True,"single_compose_restart_request":True,"quorum_status_observed":True,"failure_reason":"ISR3_RECOVERY_TIMEOUT","last_observed_replication_factor":3,"last_observed_isr":1}
    comparison={"exact_head":HEAD,"profile":"dynamic","result":"FAILED_RUN_RECOVERY_UNVERIFIED","endpoint_exposure":"ADDRESS_REASSIGNMENT_OBSERVED","ip_changed_nodes":["broker2","broker3"],"reassigned_addresses":[{"ipv4":"172.18.0.2","before_owner":"broker2","after_owner":"broker3"},{"ipv4":"172.18.0.3","before_owner":"broker3","after_owner":"broker2"}],"cross_target_voter_request_count":1000,"acknowledged_readback_verified":False,"causal_root_cause_proven":False}
    audit={"exact_head":HEAD,"complete_observations":True,"verdict":"VOTER_TARGET_ID_MISMATCH_OBSERVED","identity_changes":[],"dns_misroutes":[],"ip_changed_nodes":["broker2","broker3"],"observed_voter_target_mismatches":[{"count":500},{"count":500}]}
    sockets={"exact_head":HEAD,"result":"EVIDENCE_CAPTURED_NOT_CAUSAL_PROOF","sample_count":184}
    before={"exact_head":HEAD,"phase":"before_fault","identity_verdict":"PASS_IDENTITY","nodes":{n:{} for n in ("broker1","broker2","broker3")}}
    after={"exact_head":HEAD,"phase":"after_failure","identity_verdict":"PASS_IDENTITY","nodes":{n:{} for n in ("broker1","broker2","broker3")}}
    return report,comparison,audit,sockets,before,after
class ExpectedFailureEvidenceTests(unittest.TestCase):
    def test_known_failure_is_classified_not_relabelled_recovery(self):
        result=verify(HEAD,*evidence()); self.assertEqual(result["classification"],"EXPECTED_UNTREATED_FAILURE_REPRODUCED"); self.assertEqual(result["kafka_recovery"],"FAILED_ISR3_NOT_RESTORED"); self.assertFalse(result["acknowledged_readback_verified"]); self.assertFalse(result["causal_root_cause_proven"])
    def test_unexpected_failure_signatures_never_become_green(self):
        mutations=[(0,"no_ack_during_quorum_loss",False),(1,"endpoint_exposure","NO_ADDRESS_CHANGE_OBSERVED"),(1,"cross_target_voter_request_count",0),(1,"acknowledged_readback_verified",True),(2,"identity_changes",[{"node":"broker2"}]),(2,"dns_misroutes",[{"observer":"broker1"}]),(2,"verdict","NO_MISMATCH_OBSERVED"),(3,"sample_count",0),(4,"phase","after_success"),(5,"identity_verdict","INCOMPLETE")]
        for i,k,v in mutations:
            with self.subTest(index=i,key=k):
                docs=copy.deepcopy(evidence()); docs[i][k]=v
                with self.assertRaises(ValueError): verify(HEAD,*docs)
    def test_wrong_head_and_non_dynamic_profile_rejected(self):
        docs=evidence(); docs[1]["exact_head"]="b"*40
        with self.assertRaises(ValueError): verify(HEAD,*docs)
        docs=evidence(); docs[1]["profile"]="dynamic-ttl-zero"
        with self.assertRaises(ValueError): verify(HEAD,*docs)
    def test_reassignment_owners_and_counts_must_be_exact(self):
        for k,v in (("reassigned_addresses",[]),("ip_changed_nodes",["broker2"]),("cross_target_voter_request_count",999)):
            docs=evidence(); docs[1][k]=v
            with self.subTest(field=k),self.assertRaises(ValueError): verify(HEAD,*docs)
    def test_only_observed_isr3_timeout_can_be_expected_failure(self):
        for k,v in (("failure_reason",None),("failure_reason","READBACK_FAILED"),("single_compose_restart_request",False),("quorum_status_observed",False),("last_observed_replication_factor",2),("last_observed_isr",3),("last_observed_isr",True),("recovered_isr",3),("all_acknowledged_verified",True)):
            docs=evidence(); docs[0][k]=v
            with self.subTest(field=k,value=v),self.assertRaises(ValueError): verify(HEAD,*docs)
if __name__=="__main__": unittest.main()
