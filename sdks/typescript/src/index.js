// db2graph-sync service SDK — HTTP client for the management API (v1).
// db2graph-sync — an open-source project by Adventa. https://www.adventa.com.au
//
// This client controls or observes a *running* db2graph-sync service. It never
// embeds or starts the synchronization engine.

/** Typed error carrying the server's error code and correlation id. */
export class D2GError extends Error {
  constructor(code, message, httpStatus, correlationId) {
    super(`${code}: ${message}`);
    this.name = "D2GError";
    this.code = code;
    this.httpStatus = httpStatus;
    this.correlationId = correlationId;
  }
}

function randomId() {
  const b = new Uint8Array(12);
  globalThis.crypto.getRandomValues(b);
  return Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
}

export class Client {
  /**
   * @param {string} baseUrl e.g. "https://sync.internal:8443"
   * @param {{ token?: string, fetch?: typeof fetch, timeoutMs?: number, allowInsecureHttp?: boolean }} [opts]
   */
  constructor(baseUrl, opts = {}) {
    const url = new URL(baseUrl);
    if (url.protocol !== "https:" && !opts.allowInsecureHttp) {
      throw new D2GError("security", "the management API must be reached over https (set allowInsecureHttp only for local development)", 0, "");
    }
    this.base = url.toString().replace(/\/$/, "");
    this.token = opts.token;
    this.fetch = opts.fetch ?? globalThis.fetch;
    this.timeoutMs = opts.timeoutMs ?? 30000;
  }

  async #request(method, path, { body, auth = true, idempotencyKey, correlationId, raw = false } = {}) {
    const headers = { "x-correlation-id": correlationId ?? randomId() };
    if (auth) {
      if (!this.token) throw new D2GError("unauthorized", "a bearer token is required for this call", 0, headers["x-correlation-id"]);
      headers.authorization = `Bearer ${this.token}`;
    }
    if (idempotencyKey) headers["idempotency-key"] = idempotencyKey;
    if (body !== undefined) headers["content-type"] = "application/json";
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), this.timeoutMs);
    let resp;
    try {
      resp = await this.fetch(this.base + path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal: ctrl.signal });
    } catch (e) {
      throw new D2GError("timeout", `request failed: ${e?.message ?? e}`, 0, headers["x-correlation-id"]);
    } finally {
      clearTimeout(timer);
    }
    const cid = resp.headers.get("x-correlation-id") ?? headers["x-correlation-id"];
    const text = await resp.text();
    if (!resp.ok) {
      let code = "internal";
      let message = text;
      try {
        const j = JSON.parse(text);
        code = j.code ?? code;
        message = j.message ?? message;
      } catch { /* non-JSON body (e.g. readiness) */ }
      throw new D2GError(code, message, resp.status, cid);
    }
    if (raw) return text;
    return text ? JSON.parse(text) : {};
  }

  /** Attribution, version and compiled features (no auth). */
  about() { return this.#request("GET", "/v1/about", { auth: false }); }
  /** true when live replication is established (no auth). */
  async ready() {
    try { await this.#request("GET", "/v1/health/ready", { auth: false, raw: true }); return true; }
    catch (e) { if (e instanceof D2GError && e.httpStatus === 503) return false; throw e; }
  }
  /** Full engine status (scope: read). */
  status(opts) { return this.#request("GET", "/v1/status", opts); }
  /** Prometheus text exposition (scope: read). */
  metrics(opts) { return this.#request("GET", "/v1/metrics", { ...opts, raw: true }); }
  /** Pause capture and application; idempotent (scope: operate). */
  pause(opts) { return this.#request("POST", "/v1/pause", opts); }
  /** Resume; idempotent (scope: operate). */
  resume(opts) { return this.#request("POST", "/v1/resume", opts); }
  /** Approve a mapping plan digest (32 hex) and resume (scope: operate). */
  approveMapping(digest, opts) { return this.#request("POST", "/v1/mapping/approve", { ...opts, body: { digest } }); }
  /**
   * Validate vector hits at the authorized retrieval boundary (scope: read).
   * @param {{tenant: string, classifications?: string[], cui_categories?: string[], export_authorizations?: string[], releasability_code?: string|null}} principal
   * @param {{entity_id: string, version: string}[]} hits
   * @param {"exact_version"|"any_version"} [versionRule]
   */
  validateRetrieval(principal, hits, versionRule = "exact_version", opts) {
    return this.#request("POST", "/v1/retrieval/validate", { ...opts, body: { principal, hits, version_rule: versionRule } });
  }
}
