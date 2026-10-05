# Service SDKs

The repository has two service SDKs:

| SDK | Location | Status in this release |
|---|---|---|
| TypeScript | `sdks/typescript` | Unpublished. Tested with Node.js 21 where it was available; see the SDK's README and IMPLEMENTATION_STATUS.md for the exact result. |
| Python | `sdks/python` | Unpublished. Not executed in this release (no Python interpreter was available). |

## What they are

- **HTTP clients** for the [management API](../reference/management-api.md).
  They call a running engine over HTTPS with a bearer token.
- They return **remote** status: the state of an engine running elsewhere,
  embedded in a Rust application or as the standalone binary.
- They expose the routes the API offers: about, health, status, metrics,
  pause, resume, mapping approval and retrieval validation.

## What they are not

- They don't embed, start or stop the engine. There are no native Node.js or
  Python bindings.
- They don't capture changes, write to the graph, or connect to the source
  database.
- They can't do anything the management API doesn't offer. Policy updates,
  recovery and key rotation are operator operations (Rust API or CLI).

## Installation

No package has been published to npm or PyPI, and no package names have
been reserved. Install from a checkout, as described in each SDK's
directory. Registry installation is pending publication.

## Security notes

- Use HTTPS. Outside `development`, the standalone listener requires TLS.
- Keep bearer tokens in a secret store; the engine only stores their
  SHA-256.
- Give each client the narrowest scope it needs: `read` for status, metrics
  and retrieval validation; `operate` for pause, resume and approval.
- Send `x-correlation-id` to link SDK calls to engine logs.
