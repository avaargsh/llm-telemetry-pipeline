"""Fail-closed contract tests for KRaft voter identity evidence."""
import importlib.util
import pathlib
import unittest

spec=importlib.util.spec_from_file_location(
    "m3b_kraft_audit", pathlib.Path(__file__).with_name("m3b_kraft_audit.py"))
audit=importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def snap(phase):
    nodes={}
    for i in (1,2,3):
        nodes[f"broker{i}"]={
            "expected_id":i,
            "container_ipv4":f"172.20.0.{i+11}",
            "meta":{"node.id":str(i), "cluster.id":"synthetic-lab",
                    "directory.id":f"unique-dir-{i}"},
            "quorum":{"LeaderId":"1", "HighWatermark":"42"},
            "dns_from_node":{f"broker{j}":f"172.20.0.{j+11}" for j in (1,2,3)},
            "errors":[],
        }
    return {"schema_version":1,"phase":phase,
            "identity_verdict":"PASS_IDENTITY","nodes":nodes}


VOTER_MISMATCH=(
    'broker2-1 | [2026-11-21 03:44:27,601] INFO [RaftManager id=2] '
    'A replica 1 sent a voter key (Optional[ReplicaKey(id=3, directoryId=<undefined>)]) '
    "in the VOTE request that doesn't match the local key (OptionalInt[2], secret-dir); rejecting"
)


class KafkaAuditTests(unittest.TestCase):
    def test_clean_snapshots_and_logs_have_no_mismatch(self):
        result=audit.analyze(snap("before_fault"),snap("after_success"),
                             "broker2-1 | harmless log line",True,"abc123")
        self.assertEqual(result["verdict"],"NO_MISMATCH_OBSERVED")
        self.assertEqual(result["exact_head"],"abc123")
        self.assertEqual(result["observed_voter_target_mismatches"],[])
        self.assertEqual(result["identity_changes"],[])

    def test_real_form_voter_request_target_differs_from_receiver(self):
        result=audit.analyze(snap("before_fault"),snap("after_success"),
                             (VOTER_MISMATCH+"\n")*2,True,"abc123")
        self.assertEqual(result["verdict"],"VOTER_TARGET_ID_MISMATCH_OBSERVED")
        self.assertEqual(result["observed_voter_target_mismatches"],[
            {"sender_id":1,"receiver_id":2,"target_id":3,"count":2}])
        self.assertNotIn("secret-dir",str(result))
        self.assertNotIn("synthetic-lab",str(result))

    def test_correct_receiver_and_generic_voter_errors_not_cross_target(self):
        same=VOTER_MISMATCH.replace("ReplicaKey(id=3","ReplicaKey(id=2")
        result=audit.analyze(snap("before_fault"),snap("after_success"),
                             same+"\n"+"generic voter-key mismatch",True,"head")
        self.assertEqual(result["observed_voter_target_mismatches"],[])

    def test_persisted_directory_id_mutation_not_disclosed(self):
        before,after=snap("before_fault"),snap("after_success")
        after["nodes"]["broker3"]["meta"]["directory.id"]="private-other-id"
        result=audit.analyze(before,after,"",True,"head")
        self.assertEqual(result["verdict"],"IDENTITY_OR_DNS_DRIFT_OBSERVED")
        self.assertIn({"node":"broker3","field":"directory.id"},result["identity_changes"])
        self.assertNotIn("private-other-id",str(result))

    def test_dns_misroute_and_ipv4_replacement_separately_reported(self):
        before,after=snap("before_fault"),snap("after_success")
        after["nodes"]["broker2"]["container_ipv4"]="172.20.0.99"
        after["nodes"]["broker1"]["dns_from_node"]["broker2"]="172.20.0.13"
        result=audit.analyze(before,after,"",True,"head")
        self.assertIn("broker2",result["ip_changed_nodes"])
        self.assertIn({"observer":"broker1","target":"broker2"},result["dns_misroutes"])
        self.assertEqual(result["verdict"],"IDENTITY_OR_DNS_DRIFT_OBSERVED")

    def test_stopped_node_incomplete_snapshot_is_not_normal(self):
        after=snap("after_failure")
        after["nodes"]["broker3"]["meta"]={}
        after["nodes"]["broker3"]["errors"]=["meta_properties:exit_nonzero"]
        after["identity_verdict"]="FAIL_IDENTITY"
        result=audit.analyze(snap("before_fault"),after,"",True,"head")
        self.assertEqual(result["verdict"],"INCOMPLETE")
        self.assertFalse(result["complete_observations"])

    def test_missing_logs_or_snapshot_cannot_pass(self):
        for before,after,log_ok in (
            (snap("before_fault"),snap("after_success"),False),
            (None,snap("after_failure"),True),
            (snap("before_fault"),None,True),
        ):
            result=audit.analyze(before,after,"",log_ok,"head")
            self.assertEqual(result["verdict"],"INCOMPLETE")
            self.assertFalse(result["complete_observations"])

    def test_wrong_phase_and_malformed_schema_cannot_pass(self):
        bad=snap("after_success")
        bad["schema_version"]=17
        self.assertEqual(audit.analyze(snap("before_fault"),bad,"",True,"head")["verdict"],"INCOMPLETE")
        invalid=snap("before_fault")
        self.assertEqual(audit.analyze(snap("before_fault"),invalid,"",True,"head")["verdict"],"INCOMPLETE")


if __name__=="__main__":
    unittest.main()
