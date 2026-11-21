"""Kernel TCP evidence contract for the isolated KRaft HA lab.

Pure fixture tests. These do not assert Kafka JVM DNS-cache contents.
"""
import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location(
    "m3b_controller_sockets", pathlib.Path(__file__).with_name("m3b_controller_sockets.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

PROC_HEADER = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
# 172.29.241.12:9093 = 0CF11DAC:2385 in Linux IPv4 little-endian form.
ESTABLISHED4 = "  0: 0B1DF1AC:E398 0CF11DAC:2385 01 00000000:00000000 00:00000000 00000000  1000 0 88789 1 000000\n"
SYN_SENT4 = "  1: 0B1DF1AC:E398 0DF11DAC:2385 02 00000000:00000000 00:00000000 00000000  1000 0 99221\n"
LISTEN4 = "  2: 00000000:2385 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000 0 11831\n"
CLIENT4 = "  3: 0B1DF1AC:E398 0CF11DAC:2386 01 00000000:00000000 00:00000000 00000000  1000 0 33444\n"
MAPPED6 = "  0: 00000000000000000000FFFF0B1DF1AC:E398 0000000000000000FFFF00000CF11DAC:2385 01 00000000:00000000 00:00000000 00000000 1000 0 9911\n"


def observation(t, status="observed", peers=None):
    return {"elapsed_ms": t, "exact_head": "head123", "schema_version": 1,
            "scope": "container_netns_kernel_tcp_controller_port_not_jvm_dns_cache",
            "nodes": {
                "broker1": {"status": status, "controller_peers": peers if peers is not None else []},
                "broker2": {"status": "stopped", "controller_peers": []},
                "broker3": {"status": "stopped", "controller_peers": []},
            }}


class ControllerSocketTests(unittest.TestCase):
    def test_ipv4_controller_tcp_established_and_syn_sent_only(self):
        rows = mod.parse_proc_tcp(PROC_HEADER + ESTABLISHED4 + SYN_SENT4 + LISTEN4 + CLIENT4,
                                  family="tcp")
        self.assertEqual(rows, [
            {"peer_ip": "172.29.241.12", "tcp_state": "ESTABLISHED"},
            {"peer_ip": "172.29.241.13", "tcp_state": "SYN_SENT"},
        ])
        self.assertNotIn("88789", str(rows))  # kernel inode never exported
        self.assertNotIn("E398", str(rows))   # client ephemeral port never exported

    def test_ipv4_mapped_ipv6_socket_decodes_to_same_peer_ip(self):
        rows = mod.parse_proc_tcp(PROC_HEADER + MAPPED6, family="tcp6")
        self.assertEqual(rows, [{"peer_ip": "172.29.241.12", "tcp_state": "ESTABLISHED"}])

    def test_ignore_garbage_noncontroller_ports_and_listeners(self):
        rows = mod.parse_proc_tcp(PROC_HEADER + "garbage\n" + LISTEN4 + CLIENT4, family="tcp")
        self.assertEqual(rows, [])
        with self.assertRaises(ValueError):
            mod.parse_proc_tcp(PROC_HEADER + ESTABLISHED4, family="wrong")

    def test_summary_requires_exact_head_and_real_peer_samples(self):
        values = [
            observation(0, peers=[{"peer_ip": "172.29.241.12", "tcp_state": "ESTABLISHED"}]),
            observation(2001, peers=[{"peer_ip": "172.29.241.13", "tcp_state": "SYN_SENT"}]),
            observation(4002, status="stopped"),
        ]
        summary = mod.summarize(values, "head123")
        self.assertEqual(summary["result"], "EVIDENCE_CAPTURED_NOT_CAUSAL_PROOF")
        self.assertEqual(summary["sample_count"], 3)
        self.assertEqual(summary["unique_observed_controller_peers"], ["172.29.241.12", "172.29.241.13"])
        self.assertEqual(summary["captured_nodes"], ["broker1"])

    def test_missing_socket_observation_must_be_incomplete(self):
        values = [observation(0), observation(2000), observation(4000)]
        self.assertEqual(mod.summarize(values, "head123")["result"], "INCOMPLETE")
        self.assertEqual(mod.summarize([], "head123")["result"], "INCOMPLETE")

    def test_wrong_head_nonmonotonic_and_malformed_receipts_do_not_pass(self):
        values = [
            observation(0, peers=[{"peer_ip": "172.29.241.12", "tcp_state": "ESTABLISHED"}]),
            observation(2000),
            observation(3000),
        ]
        for mutation in (
            lambda a: a[0].update(exact_head="wrong"),
            lambda a: a[1].update(elapsed_ms=-1),
            lambda a: a[2].update(elapsed_ms=0),
            lambda a: a[1].update(schema_version=9),
            lambda a: a[0]["nodes"].update(broker1={"status": "observed",
                                                       "controller_peers": [{"peer_ip": "x", "tcp_state": "FAKE"}]}),
        ):
            items = [observation(v["elapsed_ms"],
                                 peers=v["nodes"]["broker1"]["controller_peers"]) for v in values]
            mutation(items)
            self.assertEqual(mod.summarize(items, "head123")["result"], "INCOMPLETE")

    def test_no_claim_that_namespace_peer_is_a_jvm_owned_socket(self):
        summary = mod.summarize([observation(0), observation(2000)], "head123")
        self.assertIn("not_jvm_dns_cache", summary["scope"])
        self.assertEqual(summary["result"], "INCOMPLETE")


if __name__ == "__main__":
    unittest.main()
