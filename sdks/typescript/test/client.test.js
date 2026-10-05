// Tests against a local mock of the management API (contract from api/openapi.json).
import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { Client, D2GError } from "../src/index.js";

function mock() {
  const seen = [];
  const server = http.createServer((req, res) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      seen.push({ method: req.method, url: req.url, headers: req.headers, body });
      const cid = req.headers["x-correlation-id"] ?? "gen";
      const send = (status, obj, type = "application/json") => {
        res.writeHead(status, { "content-type": type, "x-correlation-id": cid });
        res.end(typeof obj === "string" ? obj : JSON.stringify(obj));
      };
      if (req.url === "/v1/about") return send(200, { name: "db2graph-sync", project: "db2graph-sync — an open-source project by Adventa." });
      if (req.url === "/v1/health/ready") return send(503, "snapshotting", "text/plain");
      if (req.headers.authorization !== "Bearer good") return send(401, { code: "unauthorized", message: "invalid token", correlation_id: cid });
      if (req.url === "/v1/status") return send(200, { pipeline: "p", phase: "streaming", ready: true });
      if (req.url === "/v1/pause") return send(200, { ok: true, phase: "paused" });
      if (req.url === "/v1/mapping/approve") {
        const d = JSON.parse(body).digest;
        return /^[0-9a-f]{32}$/.test(d) ? send(200, { ok: true, phase: "streaming" }) : send(400, { code: "invalid_request", message: "digest", correlation_id: cid });
      }
      if (req.url === "/v1/retrieval/validate") return send(200, { verdicts: JSON.parse(body).hits.map((h) => ({ verdict: "eligible", hit: h })) });
      send(404, { code: "invalid_request", message: "no route", correlation_id: cid });
    });
  });
  return new Promise((resolve) => server.listen(0, "127.0.0.1", () => resolve({ server, seen, url: `http://127.0.0.1:${server.address().port}` })));
}

test("refuses plaintext unless explicitly allowed", () => {
  assert.throws(() => new Client("http://example.test"), (e) => e instanceof D2GError && e.code === "security");
});

test("calls the documented routes with auth, correlation and idempotency headers", async () => {
  const { server, seen, url } = await mock();
  try {
    const c = new Client(url, { token: "good", allowInsecureHttp: true });
    assert.match((await c.about()).project, /Adventa/);
    assert.equal(await c.ready(), false);
    assert.equal((await c.status({ correlationId: "abc" })).phase, "streaming");
    assert.equal(seen.at(-1).headers["x-correlation-id"], "abc");
    assert.equal((await c.pause({ idempotencyKey: "k1" })).phase, "paused");
    assert.equal(seen.at(-1).headers["idempotency-key"], "k1");
    const v = await c.validateRetrieval({ tenant: "t" }, [{ entity_id: "e", version: "v" }]);
    assert.equal(v.verdicts[0].verdict, "eligible");
    assert.equal(JSON.parse(seen.at(-1).body).version_rule, "exact_version");
  } finally {
    server.close();
  }
});

test("surfaces typed errors", async () => {
  const { server, url } = await mock();
  try {
    const bad = new Client(url, { token: "bad", allowInsecureHttp: true });
    await assert.rejects(bad.status(), (e) => e instanceof D2GError && e.code === "unauthorized" && e.httpStatus === 401);
    const good = new Client(url, { token: "good", allowInsecureHttp: true });
    await assert.rejects(good.approveMapping("zz"), (e) => e.code === "invalid_request" && e.httpStatus === 400);
    const none = new Client(url, { allowInsecureHttp: true });
    await assert.rejects(none.status(), (e) => e.code === "unauthorized");
  } finally {
    server.close();
  }
});
