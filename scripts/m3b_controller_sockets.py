#!/usr/bin/env python3
"""Bounded read-only Kafka KRaft controller socket evidence sampler.

Captures Linux TCP peer IPs for port 9093 in each *container network namespace*.
It does NOT prove which Kafka JVM thread owns a socket, the target voter ID
the JVM intended, the JVM DNS cache, or production fault tolerance.
No raw /proc tables, host PIDs, socket inodes, ports of client connections,
environment variables or secrets enter the published JSONL/summary.
"""
import argparse
import ipaddress
import json
import os
import pathlib
import signal
import subprocess
import tempfile
import time

NODES = ("broker1", "broker2", "broker3")
SCOPE = "container_netns_kernel_tcp_controller_port_not_jvm_dns_cache"
STATES = {"01": "ESTABLISHED", "02": "SYN_SENT", "03": "SYN_RECV"}
STOP = False


def _ipv4(raw):
    if len(raw) != 8:
        raise ValueError("invalid /proc/net/tcp IPv4 address")
    return str(ipaddress.IPv4Address(bytes.fromhex(raw)[::-1]))


def _ipv6(raw):
    if len(raw) != 32:
        raise ValueError("invalid /proc/net/tcp6 IPv6 address")
    data = bytes.fromhex(raw)
    data = b"".join(data[i:i+4][::-1] for i in range(0, 16, 4))
    ip = ipaddress.IPv6Address(data)
    return str(ip.ipv4_mapped or ip)


def parse_proc_tcp(raw, family):
    """Never retain /proc PID, UID, source-port, socket inode or raw lines."""
    if family not in ("tcp", "tcp6"):
        raise ValueError("unsupported proc TCP family")
    peers = set()
    for line in raw.splitlines()[1:]:
        cols = line.split()
        if len(cols) < 4:
            continue
        try:
            ip_hex, port_hex = cols[2].split(":", 1)
            state = STATES.get(cols[3].upper())
            if state is None or int(port_hex, 16) != 9093:
                continue
            peer = _ipv4(ip_hex) if family == "tcp" else _ipv6(ip_hex)
            ipaddress.ip_address(peer)
            if peer not in ("0.0.0.0", "::"):
                peers.add((peer, state))
        except (ValueError, IndexError):
            continue
    return [{"peer_ip": ip, "tcp_state": state} for ip, state in sorted(peers)]


def _cmd(args, timeout=5):
    try:
        proc = subprocess.run(args, capture_output=True, text=True,
                              timeout=timeout, check=False)
        return proc.stdout.strip() if proc.returncode == 0 else ""
    except (OSError, subprocess.TimeoutExpired, UnicodeError):
        return ""


def _node_peers(node, compose):
    # Docker inspect resolves the live container network-namespace PID on the
    # host. Reading /proc/PID/net/tcp* is observational; no namespace join,
    # process pause, privileged tcpdump, tracer, or Kafka mutation.
    cid = _cmd(("docker", "compose", "-f", compose, "ps", "-q", node))
    if not cid or "\n" in cid:
        return {"status": "stopped", "controller_peers": []}
    pid = _cmd(("docker", "inspect", "--format", "{{.State.Pid}}", cid))
    if not pid.isdecimal() or int(pid) < 1:
        return {"status": "unavailable", "controller_peers": []}
    peers = set()
    observed = 0
    for family in ("tcp", "tcp6"):
        try:
            path = pathlib.Path("/proc") / pid / "net" / family
            raw = path.read_text(encoding="ascii")
            observed += 1
            for peer in parse_proc_tcp(raw, family):
                peers.add((peer["peer_ip"], peer["tcp_state"]))
        except (OSError, UnicodeError, ValueError):
            continue
    if not observed:
        return {"status": "unavailable", "controller_peers": []}
    return {"status": "observed", "controller_peers": [
        {"peer_ip": ip, "tcp_state": state} for ip, state in sorted(peers)
    ]}


def capture(compose, head, elapsed_ms):
    return {
        "schema_version": 1, "exact_head": head, "elapsed_ms": elapsed_ms,
        "scope": SCOPE,
        "nodes": {node: _node_peers(node, compose) for node in NODES},
    }


def summarize(samples, expected_head):
    summary = {
        "schema_version": 1, "exact_head": expected_head,
        "result": "INCOMPLETE", "scope": SCOPE,
        "sample_count": len(samples),
        "captured_nodes": [], "unique_observed_controller_peers": [],
        "cannot_prove_jvm_dns_cache_causality": True,
    }
    if not expected_head or expected_head == "UNBOUND" or len(samples) < 3:
        return summary
    peers = set()
    nodes = set()
    previous = -1
    for sample in samples:
        if not isinstance(sample, dict):
            return summary
        elapsed = sample.get("elapsed_ms")
        if (sample.get("schema_version") != 1
                or sample.get("exact_head") != expected_head
                or sample.get("scope") != SCOPE
                or type(elapsed) is not int or elapsed <= previous):
            return summary
        previous = elapsed
        state = sample.get("nodes")
        if not isinstance(state, dict) or set(state) != set(NODES):
            return summary
        for name, row in state.items():
            if not isinstance(row, dict) or row.get("status") not in (
                    "observed", "stopped", "unavailable"):
                return summary
            entries = row.get("controller_peers")
            if not isinstance(entries, list):
                return summary
            if row["status"] != "observed" and entries:
                return summary
            if entries:
                nodes.add(name)
            for entry in entries:
                if not isinstance(entry, dict) or set(entry) != {"peer_ip", "tcp_state"}:
                    return summary
                try:
                    ipaddress.ip_address(entry["peer_ip"])
                except (ValueError, TypeError, KeyError):
                    return summary
                if entry["tcp_state"] not in STATES.values():
                    return summary
                peers.add(entry["peer_ip"])
    summary["captured_nodes"] = sorted(nodes)
    summary["unique_observed_controller_peers"] = sorted(
        peers, key=lambda x: (ipaddress.ip_address(x).version, int(ipaddress.ip_address(x))))
    if peers:
        summary["result"] = "EVIDENCE_CAPTURED_NOT_CAUSAL_PROOF"
    return summary


def _stopped(_signum, _frame):
    global STOP
    STOP = True


def sample(args):
    signal.signal(signal.SIGTERM, _stopped)
    signal.signal(signal.SIGINT, _stopped)
    pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    start = time.monotonic()
    with open(args.out, "w", encoding="utf-8") as out:
        os.chmod(args.out, 0o600)
        while not STOP and time.monotonic() - start < args.duration:
            delta = int((time.monotonic() - start) * 1000)
            out.write(json.dumps(capture(args.compose, args.head, delta),
                                 sort_keys=True) + "\n")
            out.flush()  # Keep every completed observation across t.Fatal.
            until = time.monotonic() + args.interval
            while not STOP and time.monotonic() < until:
                time.sleep(min(0.1, max(0, until - time.monotonic())))


def verify(args):
    try:
        with open(args.input, encoding="utf-8") as fh:
            samples = [json.loads(line) for line in fh if line.strip()]
    except (OSError, ValueError, UnicodeError):
        samples = []
    summary = summarize(samples, args.head)
    path = pathlib.Path(args.out)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent,
                                         prefix=".m3b-sockets-", delete=False) as stream:
            tmp = stream.name
            os.chmod(tmp, 0o600)
            json.dump(summary, stream, indent=2, sort_keys=True)
            stream.write("\n")
        os.replace(tmp, path)
    finally:
        if tmp and os.path.exists(tmp):
            os.unlink(tmp)
    print(json.dumps(summary, sort_keys=True))
    if summary["result"] != "EVIDENCE_CAPTURED_NOT_CAUSAL_PROOF":
        raise SystemExit(2)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="mode", required=True)
    live = sub.add_parser("sample")
    live.add_argument("--compose", default="deployments/docker-compose-ha.yml")
    live.add_argument("--head", required=True)
    live.add_argument("--out", required=True)
    live.add_argument("--interval", type=float, default=2.0)
    live.add_argument("--duration", type=int, default=540)
    val = sub.add_parser("verify")
    val.add_argument("--input", required=True)
    val.add_argument("--head", required=True)
    val.add_argument("--out", required=True)
    opts = ap.parse_args()
    if opts.mode == "sample":
        if not (0.25 <= opts.interval <= 30 and 1 <= opts.duration <= 900):
            ap.error("unbounded sampling interval or duration")
        sample(opts)
    else:
        verify(opts)


if __name__ == "__main__":
    main()
