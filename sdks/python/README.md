# db2graph-sync-client (Python service SDK)

**db2graph-sync — an open-source project by Adventa.** <https://www.adventa.com.au>

An HTTP client for the db2graph-sync management API (v1). It controls or
observes a running db2graph-sync service; it does **not** embed or start the
synchronization engine. Standard library only (Python ≥ 3.9).

Status: unpublished (not on PyPI; install from a checkout with
`pip install ./sdks/python`). Its unit tests run against a local mock of the
API in CI; they have not been executed on the development machine used for
this release (no Python interpreter was available).

```python
from db2graph_sync_client import Client

c = Client("https://sync.internal:8443", token=open("/run/secrets/d2g_token").read().strip())
print(c.status()["phase"])
verdicts = c.validate_retrieval({"tenant": "acme"}, [{"entity_id": "...", "version": "..."}])
```

HTTPS with certificate verification is required (`allow_insecure_http=True`
only for local development). Pass `ssl_context=` for a private CA. Errors are
raised as `D2GError(code, message, http_status, correlation_id)`.

Licensed under Apache-2.0.
