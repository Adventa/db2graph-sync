"""db2graph-sync service SDK: HTTP client for the management API (v1).

db2graph-sync — an open-source project by Adventa. https://www.adventa.com.au

This client controls or observes a *running* db2graph-sync service. It never
embeds or starts the synchronization engine. Standard library only.
"""

from __future__ import annotations

import json
import secrets
import ssl
import urllib.error
import urllib.request
from typing import Any, Optional

__all__ = ["Client", "D2GError"]
__version__ = "0.1.0"


class D2GError(Exception):
    """Typed error carrying the server's error code and correlation id."""

    def __init__(self, code: str, message: str, http_status: int = 0, correlation_id: str = "") -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.http_status = http_status
        self.correlation_id = correlation_id


class Client:
    """Client for one db2graph-sync management endpoint.

    :param base_url: e.g. ``https://sync.internal:8443``
    :param token: bearer token (scopes are enforced by the server)
    :param ssl_context: optional :class:`ssl.SSLContext` (e.g. with a private CA).
        Certificate verification is always on; there is no option to disable it.
    :param allow_insecure_http: permit ``http://`` (local development only)
    """

    def __init__(
        self,
        base_url: str,
        token: Optional[str] = None,
        *,
        timeout: float = 30.0,
        ssl_context: Optional[ssl.SSLContext] = None,
        allow_insecure_http: bool = False,
    ) -> None:
        if not base_url.startswith("https://") and not allow_insecure_http:
            raise D2GError("security", "the management API must be reached over https (allow_insecure_http is for local development only)")
        self._base = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout
        self._ssl = ssl_context or ssl.create_default_context()

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        auth: bool = True,
        idempotency_key: Optional[str] = None,
        correlation_id: Optional[str] = None,
        raw: bool = False,
    ) -> Any:
        cid = correlation_id or secrets.token_hex(12)
        headers = {"x-correlation-id": cid}
        if auth:
            if not self._token:
                raise D2GError("unauthorized", "a bearer token is required for this call", 0, cid)
            headers["authorization"] = f"Bearer {self._token}"
        if idempotency_key:
            headers["idempotency-key"] = idempotency_key
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["content-type"] = "application/json"
        req = urllib.request.Request(self._base + path, data=data, method=method, headers=headers)
        context = self._ssl if self._base.startswith("https://") else None
        try:
            with urllib.request.urlopen(req, timeout=self._timeout, context=context) as resp:
                text = resp.read().decode()
        except urllib.error.HTTPError as e:
            text = e.read().decode()
            code, message = "internal", text
            try:
                j = json.loads(text)
                code, message = j.get("code", code), j.get("message", message)
            except ValueError:
                pass
            raise D2GError(code, message, e.code, e.headers.get("x-correlation-id", cid)) from None
        except (urllib.error.URLError, TimeoutError) as e:
            raise D2GError("timeout", f"request failed: {e}", 0, cid) from None
        if raw:
            return text
        return json.loads(text) if text else {}

    def about(self) -> dict:
        """Attribution, version and compiled features (no auth)."""
        return self._request("GET", "/v1/about", auth=False)

    def ready(self) -> bool:
        """True when live replication is established (no auth)."""
        try:
            self._request("GET", "/v1/health/ready", auth=False, raw=True)
            return True
        except D2GError as e:
            if e.http_status == 503:
                return False
            raise

    def status(self, **kw: Any) -> dict:
        """Full engine status (scope: read)."""
        return self._request("GET", "/v1/status", **kw)

    def metrics(self, **kw: Any) -> str:
        """Prometheus text exposition (scope: read)."""
        return self._request("GET", "/v1/metrics", raw=True, **kw)

    def pause(self, **kw: Any) -> dict:
        """Pause capture and application; idempotent (scope: operate)."""
        return self._request("POST", "/v1/pause", **kw)

    def resume(self, **kw: Any) -> dict:
        """Resume; idempotent (scope: operate)."""
        return self._request("POST", "/v1/resume", **kw)

    def approve_mapping(self, digest: str, **kw: Any) -> dict:
        """Approve a mapping plan digest (32 hex) and resume (scope: operate)."""
        return self._request("POST", "/v1/mapping/approve", body={"digest": digest}, **kw)

    def validate_retrieval(self, principal: dict, hits: list, version_rule: str = "exact_version", **kw: Any) -> dict:
        """Validate vector hits at the authorized retrieval boundary (scope: read)."""
        return self._request(
            "POST", "/v1/retrieval/validate", body={"principal": principal, "hits": hits, "version_rule": version_rule}, **kw
        )
