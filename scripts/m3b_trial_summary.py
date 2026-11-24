"""Offline summary of validated M3b comparison receipts; never a merge gate."""
import argparse
import ipaddress
import json
import re
from pathlib import Path

PROFILES = ('pinned', 'dynamic', 'dynamic-ttl-zero')
NODES = {'broker1', 'broker2', 'broker3'}
SLOTS = {(profile, trial) for profile in PROFILES for trial in (1, 2, 3)}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def classify(head, profile, row):
    require(isinstance(row, dict) and row.get('exact_head') == head and
            row.get('profile') == profile and type(row.get('schema_version')) is int and
            row['schema_version'] == 1 and row.get('causal_root_cause_proven') is False,
            'wrong receipt identity or schema')
    count, samples = row.get('cross_target_voter_request_count'), row.get('controller_socket_samples')
    require(type(count) is int and count >= 0 and type(samples) is int and samples >= 3,
            'invalid observation counts')
    changed, reassigned = row.get('ip_changed_nodes'), row.get('reassigned_addresses')
    require(isinstance(changed, list) and all(isinstance(n, str) and n in NODES for n in changed)
            and len(set(changed)) == len(changed) and isinstance(reassigned, list), 'invalid endpoint lists')
    ips, before, after = set(), set(), set()
    for item in reassigned:
        require(isinstance(item, dict), 'invalid reassignment row')
        ip, old, new = item.get('ipv4'), item.get('before_owner'), item.get('after_owner')
        require(isinstance(ip, str) and isinstance(old, str) and isinstance(new, str)
                and old != new and old in changed and new in changed, 'invalid reassignment owners')
        addr = ipaddress.IPv4Address(ip)
        require(addr not in ips and old not in before and new not in after, 'duplicate reassignment')
        ips.add(addr); before.add(old); after.add(new)
    exposure = ('PINNED_CONTROL' if profile == 'pinned' else 'ADDRESS_REASSIGNMENT_OBSERVED'
                if reassigned else 'ADDRESS_CHANGE_WITHOUT_REASSIGNMENT' if changed
                else 'NO_ADDRESS_CHANGE_OBSERVED')
    require(row.get('endpoint_exposure') == exposure, 'contradictory exposure')
    if profile == 'pinned':
        require(not changed and not reassigned and count == 0, 'invalid pinned control')
    good = 'RECOVERY_VERIFIED_ENDPOINTS_FIXED' if profile == 'pinned' else 'RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED'
    if row.get('result') == good:
        require(row.get('acknowledged_readback_verified') is True and
                type(row.get('initial_ack_count')) is int and row['initial_ack_count'] == 35,
                'missing ACK readback')
        # A recovered treatment with wrong-target votes is not qualifying mitigation evidence.
        if profile == 'dynamic-ttl-zero' and count != 0:
            return 'exposed_nonqualifying' if reassigned else 'unexposed_nonqualifying'
        outcome = 'recovered'
    else:
        require(row.get('result') == 'FAILED_RUN_RECOVERY_UNVERIFIED' and
                row.get('acknowledged_readback_verified') is False and
                row.get('kafka_report_result') in ('INCOMPLETE', 'PASS_EXPERIMENT_ONLY'),
                'missing or contradictory failure receipt')
        outcome = 'failed'
    return ('pinned' if profile == 'pinned' else 'exposed' if reassigned else 'unexposed') + '_' + outcome


def summarize(head, rows):
    require(isinstance(head, str) and re.fullmatch(r'[0-9a-f]{40}', head) is not None, 'invalid exact HEAD')
    require(set(rows) == SLOTS, 'missing or unexpected trial slots')
    counts = {profile: dict.fromkeys(('pinned_recovered', 'pinned_failed', 'exposed_recovered',
        'exposed_failed', 'unexposed_recovered', 'unexposed_failed',
        'exposed_nonqualifying', 'unexposed_nonqualifying'), 0) for profile in PROFILES}
    for (profile, trial), row in sorted(rows.items()):
        counts[profile][classify(head, profile, row)] += 1
    return {'exact_head': head, 'profiles': counts, 'scope': 'observational_summary_only',
            'causal_root_cause_proven': False, 'merge_ready': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--head', required=True)
    parser.add_argument('--directory', required=True, type=Path,
                        help='directory containing PROFILE-TRIAL.json for all nine slots')
    args = parser.parse_args()
    try:
        rows = {(p, t): json.loads((args.directory / f'{p}-{t}.json').read_text()) for p, t in SLOTS}
        print(json.dumps(summarize(args.head, rows), sort_keys=True, indent=2))
    except (ValueError, OSError) as exc:
        parser.exit(2, f'incomplete trial summary: {exc}\n')


if __name__ == '__main__':
    main()
