import unittest
from m3b_trial_summary import summarize

HEAD = 'a' * 40
PROFILES = ('pinned', 'dynamic', 'dynamic-ttl-zero')


def matrix():
    result = {}
    for profile in PROFILES:
        for trial in (1, 2, 3):
            pinned = profile == 'pinned'
            failed = profile == 'dynamic'
            unexposed = profile == 'dynamic-ttl-zero' and trial == 2
            changed = not pinned and not unexposed
            result[profile, trial] = dict(
                schema_version=1, exact_head=HEAD, profile=profile,
                causal_root_cause_proven=False, controller_socket_samples=3,
                endpoint_exposure=('PINNED_CONTROL' if pinned else
                    'ADDRESS_REASSIGNMENT_OBSERVED' if changed else 'NO_ADDRESS_CHANGE_OBSERVED'),
                ip_changed_nodes=['broker1', 'broker3'] if changed else [],
                reassigned_addresses=[dict(ipv4='172.18.0.2', before_owner='broker1',
                    after_owner='broker3')] if changed else [],
                cross_target_voter_request_count=1000 if failed else 0,
                result=('FAILED_RUN_RECOVERY_UNVERIFIED' if failed else
                    'RECOVERY_VERIFIED_ENDPOINTS_FIXED' if pinned else
                    'RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED'),
                acknowledged_readback_verified=not failed,
                **({'kafka_report_result': 'INCOMPLETE'} if failed else {'initial_ack_count': 35}))
    return result


class TrialSummary(unittest.TestCase):
    def test_observed_matrix_preserves_failure_and_unexposed(self):
        report = summarize(HEAD, matrix())
        self.assertEqual(report['profiles']['dynamic']['exposed_failed'], 3)
        self.assertEqual(report['profiles']['dynamic-ttl-zero']['exposed_recovered'], 2)
        self.assertEqual(report['profiles']['dynamic-ttl-zero']['unexposed_recovered'], 1)
        self.assertFalse(report['merge_ready'])
        self.assertFalse(report['causal_root_cause_proven'])

    def test_missing_trial(self):
        rows = matrix()
        del rows['dynamic', 2]
        with self.assertRaises(ValueError):
            summarize(HEAD, rows)

    def test_wrong_head_profile_and_contradictory_receipt(self):
        for key, value in [('exact_head', 'b'*40), ('profile', 'pinned'),
                           ('acknowledged_readback_verified', True),
                           ('reassigned_addresses', []), ('cross_target_voter_request_count', False),
                           ('causal_root_cause_proven', True)]:
            with self.subTest(key=key):
                rows = matrix()
                rows['dynamic', 1][key] = value
                with self.assertRaises(ValueError):
                    summarize(HEAD, rows)

    def test_failed_unexposed_is_not_recovery(self):
        rows = matrix()
        rows['dynamic-ttl-zero', 2].update(result='FAILED_RUN_RECOVERY_UNVERIFIED',
            acknowledged_readback_verified=False, kafka_report_result='INCOMPLETE')
        report = summarize(HEAD, rows)
        self.assertEqual(report['profiles']['dynamic-ttl-zero']['unexposed_failed'], 1)
        self.assertEqual(report['profiles']['dynamic-ttl-zero']['unexposed_recovered'], 0)


if __name__ == '__main__':
    unittest.main()
