# db2graph-sync-client (TypeScript/JavaScript service SDK)

**db2graph-sync — an open-source project by Adventa.** <https://www.adventa.com.au>

An HTTP client for the db2graph-sync management API (v1). It controls or
observes a running db2graph-sync service; it does **not** embed or start the
synchronization engine. No runtime dependencies (Node ≥ 18 `fetch`, or any
`fetch` you pass in). ESM with type declarations.

Status: unpublished (`"private": true`; not on npm). Tests (`npm test`) run
against a local mock of the API and passed on Node 21.7.1.

```js
import { Client } from "db2graph-sync-client";

const c = new Client("https://sync.internal:8443", { token: process.env.D2G_TOKEN });
console.log((await c.status()).phase);
const { verdicts } = await c.validateRetrieval({ tenant: "acme" }, [{ entity_id: "...", version: "..." }]);
```

HTTPS is required (`allowInsecureHttp: true` only for local development).
Certificate verification follows the runtime's `fetch`. Browsers must never
hold source-database credentials; this SDK only uses management tokens, which
should be scoped (`read`) and served from your backend.

Licensed under Apache-2.0.
