"""Tests against a local mock of the management API (contract: api/openapi.json)."""

import json
import os
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db2graph_sync_client import Client, D2GError  # noqa: E402

SEEN = []


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _send(self, status, obj, ctype="application/json"):
        cid = self.headers.get("x-correlation-id", "gen")
        body = obj if isinstance(obj, str) else json.dumps(obj)
        self.send_response(status)
        self.send_header("content-type", ctype)
        self.send_header("x-correlation-id", cid)
        self.end_headers()
        self.wfile.write(body.encode())

    def _handle(self):
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length).decode() if length else ""
        SEEN.append({"method": self.command, "path": self.path, "headers": dict(self.headers), "body": body})
        cid = self.headers.get("x-correlation-id", "gen")
        if self.path == "/v1/about":
            return self._send(200, {"name": "db2graph-sync", "project": "db2graph-sync — an open-source project by Adventa."})
        if self.path == "/v1/health/ready":
            return self._send(503, "snapshotting", "text/plain")
        if self.headers.get("authorization") != "Bearer good":
            return self._send(401, {"code": "unauthorized", "message": "invalid token", "correlation_id": cid})
        if self.path == "/v1/status":
            return self._send(200, {"pipeline": "p", "phase": "streaming", "ready": True})
        if self.path == "/v1/pause":
            return self._send(200, {"ok": True, "phase": "paused"})
        if self.path == "/v1/mapping/approve":
            d = json.loads(body)["digest"]
            if len(d) == 32:
                return self._send(200, {"ok": True, "phase": "streaming"})
            return self._send(400, {"code": "invalid_request", "message": "digest", "correlation_id": cid})
        if self.path == "/v1/retrieval/validate":
            hits = json.loads(body)["hits"]
            return self._send(200, {"verdicts": [{"verdict": "eligible", "hit": h} for h in hits]})
        return self._send(404, {"code": "invalid_request", "message": "no route", "correlation_id": cid})

    do_GET = _handle
    do_POST = _handle


class ClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.url = f"http://127.0.0.1:{cls.server.server_address[1]}"
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def test_refuses_plaintext_unless_allowed(self):
        with self.assertRaises(D2GError) as e:
            Client("http://example.test")
        self.assertEqual(e.exception.code, "security")

    def test_routes_headers_and_results(self):
        c = Client(self.url, "good", allow_insecure_http=True)
        self.assertIn("Adventa", c.about()["project"])
        self.assertFalse(c.ready())
        self.assertEqual(c.status(correlation_id="abc")["phase"], "streaming")
        self.assertEqual(SEEN[-1]["headers"].get("x-correlation-id"), "abc")
        self.assertEqual(c.pause(idempotency_key="k1")["phase"], "paused")
        self.assertEqual(SEEN[-1]["headers"].get("idempotency-key"), "k1")
        v = c.validate_retrieval({"tenant": "t"}, [{"entity_id": "e", "version": "v"}])
        self.assertEqual(v["verdicts"][0]["verdict"], "eligible")
        self.assertEqual(json.loads(SEEN[-1]["body"])["version_rule"], "exact_version")

    def test_typed_errors(self):
        with self.assertRaises(D2GError) as e:
            Client(self.url, "bad", allow_insecure_http=True).status()
        self.assertEqual((e.exception.code, e.exception.http_status), ("unauthorized", 401))
        with self.assertRaises(D2GError) as e:
            Client(self.url, "good", allow_insecure_http=True).approve_mapping("zz")
        self.assertEqual((e.exception.code, e.exception.http_status), ("invalid_request", 400))
        with self.assertRaises(D2GError) as e:
            Client(self.url, allow_insecure_http=True).status()
        self.assertEqual(e.exception.code, "unauthorized")


if __name__ == "__main__":
    unittest.main()
