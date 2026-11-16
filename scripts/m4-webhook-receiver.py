#!/usr/bin/env python3
"""Ephemeral, test-only Alertmanager receiver that records alert state changes.

No auth, no persistence. Never expose beyond the isolated Docker network.
"""
import datetime
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_LOCK = threading.Lock()
_DELIVERIES = []

class Receiver(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        if self.path != "/events":
            self.send_error(404)
            return
        with _LOCK:
            payload = {"schema_version": 1, "deliveries": list(_DELIVERIES)}
        data = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        if self.path != "/hook":
            self.send_error(404)
            return
        try:
            n = int(self.headers.get("Content-Length", "0"))
            if not (0 < n <= 1_000_000):
                raise ValueError("body size not allowed")
            message = json.loads(self.rfile.read(n))
            states = []
            for alert in message.get("alerts", []):
                name = alert.get("labels", {}).get("alertname")
                status = alert.get("status")
                if not isinstance(name, str) or status not in ("firing", "resolved"):
                    raise ValueError("unknown Alertmanager payload")
                states.append({"alertname": name, "status": status})
            if not states:
                raise ValueError("empty Alertmanager alert list")
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            with _LOCK:
                for state in states:
                    _DELIVERIES.append({"received_at": now, **state})
        except (ValueError, TypeError, json.JSONDecodeError):
            self.send_error(400, "invalid alert notification")
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"accepted")

if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 19099), Receiver).serve_forever()
