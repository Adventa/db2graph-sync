# Quickstart: PostgreSQL to Neo4j

> **Status:** This CLI walkthrough has **not been executed in this release**.
> The PostgreSQL connector and Neo4j sink are implemented; integration-tested
> against live servers (local Docker, Windows; see the
> [status page](../project/status.md)); not yet CI-qualified on all
> platforms. Those integration tests exercise the library against the same
> `deploy/test/docker-compose.yml` services, not these CLI steps. If you
> want something that runs without any services, use the examples instead
> (`cargo run --example
> host_lifecycle --features service`, `restart_replay`, `policy_filtering`,
> `composite_keys`, `custom_graph_sink`, `custom_state_backend`). Those were
> executed.

This walkthrough uses three repository files:

- `deploy/test/docker-compose.yml`: PostgreSQL 16 and Neo4j 5, with
  **test-only** credentials.
- `examples/config/postgres-neo4j.dev.toml`: a `development` profile config
  with explicit plaintext enabled (`engine.allow_plaintext = true`).
- the `db2graph-sync` CLI (see [Installation](installation.md)).

All data is synthetic.

## 1. Start the services

```sh
docker compose -f deploy/test/docker-compose.yml up -d --wait
```

| Service | Address | Credentials (test only) |
|---|---|---|
| PostgreSQL 16 (`wal_level=logical`) | `127.0.0.1:55432`, database `d2g` | `d2g` / `d2g-test-only` |
| Neo4j 5 community (Bolt) | `127.0.0.1:57687` | `neo4j` / `d2g-test-only` |
| Qdrant (not used here) | `127.0.0.1:56333` | none |

## 2. Create synthetic tables

The dev config captures `public.customers` and `public.orders`, and refers to
the foreign key `orders_customer_id_fkey`. This illustrative schema matches
it (not executed in this release):

```sh
docker compose -f deploy/test/docker-compose.yml exec -T postgres psql -U d2g -d d2g <<'SQL'
CREATE TABLE public.customers (id bigint PRIMARY KEY, name text, notes text, ssn text);
CREATE TABLE public.orders (
  id bigint PRIMARY KEY,
  customer_id bigint REFERENCES public.customers(id),  -- constraint name: orders_customer_id_fkey
  total numeric(12,2)
);
INSERT INTO public.customers VALUES (1, 'Ada Example', 'synthetic', '000-00-0000');
INSERT INTO public.orders VALUES (10, 1, 42.50);
SQL
```

The policy denies `ssn` (`deny_fields = ["ssn"]`), so it is never journaled
or projected.

## 3. Set the secrets the config references

The config holds only references. These three must resolve:

| Reference | Value |
|---|---|
| `env:D2G_PG_PASSWORD` | `d2g-test-only` |
| `env:D2G_NEO4J_PASSWORD` | `d2g-test-only` |
| `env:D2G_STATE_KEY` | a base64 32-byte key from `db2graph-sync keygen` |

bash:

```sh
export D2G_PG_PASSWORD=d2g-test-only
export D2G_NEO4J_PASSWORD=d2g-test-only
export D2G_STATE_KEY="$(db2graph-sync keygen)"
```

PowerShell:

```powershell
$env:D2G_PG_PASSWORD = 'd2g-test-only'
$env:D2G_NEO4J_PASSWORD = 'd2g-test-only'
$env:D2G_STATE_KEY = db2graph-sync keygen
```

Every later run against the same state directory (`./.d2g-state`) must use
the same `D2G_STATE_KEY`. With a different key the state fails
authentication (`state_tampered`).

## 4. Validate the configuration (offline)

```sh
db2graph-sync validate -c examples/config/postgres-neo4j.dev.toml
```

## 5. Print and apply the source setup

```sh
db2graph-sync source-setup -c examples/config/postgres-neo4j.dev.toml
```

This prints SQL for an administrator to review: `wal_level`, the
`REPLICATION` role attribute, `CREATE PUBLICATION d2g_demo FOR TABLE …`, and
`GRANT SELECT`. db2graph-sync never runs these statements. The compose file
already starts PostgreSQL with `wal_level=logical`; run the publication and
grant statements with `psql`. The replication slot `d2g_demo` is created by
the engine on first start, and the engine never drops it.

## 6. Bootstrap the graph

```sh
db2graph-sync bootstrap -c examples/config/postgres-neo4j.dev.toml
```

This creates the uniqueness constraints and lookup indexes the engine needs
in Neo4j. It is a separate operator step: `run` never creates them, and the
graph preflight fails at startup if they are missing.

## 7. Inspect the mapping plan

```sh
db2graph-sync plan -c examples/config/postgres-neo4j.dev.toml
```

The output shows the plan digest, the node label of each relation
(`public.orders` → `Order`, from the override), its key, and its
relationships (`PLACED_BY`, via `orders_customer_id_fkey`). The `development`
profile does not require approval. `enterprise` and `restricted` require
`mapping.approved_digest` to equal this digest.

Optionally, check prerequisites:

```sh
db2graph-sync preflight -c examples/config/postgres-neo4j.dev.toml
```

## 8. Run

```sh
db2graph-sync run -c examples/config/postgres-neo4j.dev.toml
```

The engine runs preflight, acquires the fence, snapshots the captured tables,
validates the snapshot and then streams changes. Logs go to stderr; set
`D2G_LOG=debug` for more detail. The dev config has no `[management]`
listener, so the remote `status` command is not available here. Press Ctrl-C
for an orderly stop (it drains for 30 s by default).

## 9. Look at the graph (illustrative, not executed)

```sh
docker compose -f deploy/test/docker-compose.yml exec neo4j \
  cypher-shell -u neo4j -p d2g-test-only \
  "MATCH (n:_D2G_Entity) RETURN labels(n), count(*); MATCH ()-[r:PLACED_BY]->() RETURN count(r);"
```

## Cleaning up

Reset the graph and the state directory **together**. If the graph pipeline
marker is ahead of a deleted or restored state directory, the next start
fails with `state_rollback`
(see [Recovery](../operations/recovery.md)).

```sh
docker compose -f deploy/test/docker-compose.yml down -v
rm -rf ./.d2g-state
```

If you keep the database, drop the slot yourself:
`SELECT pg_drop_replication_slot('d2g_demo');`.
