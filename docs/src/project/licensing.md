# Licensing

db2graph-sync — an open-source project by Adventa. (<https://www.adventa.com.au>)

## Project license

db2graph-sync is licensed under the **Apache License, Version 2.0**
(`LICENSE` at the repository root). Both crates, `db2graph-sync` and
`db2graph-sync-cli`, declare `license = "Apache-2.0"`.

- `NOTICE` contains the attribution notices that must be retained when
  redistributing the software, as required by section 4(d) of the license.
- `THIRD_PARTY_NOTICES.md` lists the licenses of third-party dependencies
  distributed with the project.
- Contributions are accepted under Apache-2.0 (inbound = outbound, section 5
  of the license). No CLA or DCO is required. See `CONTRIBUTING.md`.

## Dependency license policy

`deny.toml` at the repository root defines the dependency policy enforced by
`cargo deny check`: the set of allowed licenses, security advisories,
banned crates and permitted sources. Every pull request that changes
dependencies must pass it, and releases are checked against it (see
`RELEASING.md`).

## No lock-in

Using db2graph-sync does **not** require:

- an account with Adventa or any other party;
- a license key or license server;
- telemetry — the software sends no usage data anywhere;
- any network call to Adventa;
- Adventa branding in your product.

The software connects only to the systems you configure (source database,
graph database, vector store, embedding endpoint, audit destinations) and,
in standalone mode, serves the management API only on the address you
configure. The library never binds a port.

The Adventa name identifies the project's origin. The Apache-2.0 license does
not grant permission to use Adventa's trade names or trademarks except as
required for describing the origin of the work (section 6 of the license).

## Third-party terms are separate

The license of db2graph-sync does not cover the systems it connects to.
Your use of PostgreSQL (and managed PostgreSQL services), Neo4j, Qdrant,
Pinecone, embedding providers, cloud services, and any cryptographic module
you supply (for example a validated module for the restricted profile) is
governed by those products' own licenses and terms of service. Check them
yourself, including any terms on sending data to hosted embedding services.

## Reuse decision: Supabase ETL

Reusing the Supabase ETL project for PostgreSQL change capture was evaluated
and **not adopted**; db2graph-sync uses its own replication client over the
pgoutput protocol. The context, alternatives and reasoning are recorded in the
architecture decision record `docs/src/adr/0003-postgres-capture-supabase-etl.md`.
