#!/usr/bin/env bash
# Runs the real-service integration tests (PostgreSQL, Neo4j, Qdrant).
# Fails if any test reports UNEXECUTED, so missing services can never be
# mistaken for passing coverage.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "${D2G_IT_SKIP_COMPOSE:-0}" != "1" ]; then
  docker compose -f deploy/test/docker-compose.yml up -d --wait
fi

export D2G_IT_PG_HOST="${D2G_IT_PG_HOST:-127.0.0.1}"
export D2G_IT_PG_PORT="${D2G_IT_PG_PORT:-55432}"
export D2G_IT_PG_USER="${D2G_IT_PG_USER:-d2g}"
export D2G_IT_PG_PASSWORD="${D2G_IT_PG_PASSWORD:-d2g-test-only}"
export D2G_IT_PG_DB="${D2G_IT_PG_DB:-d2g}"
export D2G_IT_NEO4J_URI="${D2G_IT_NEO4J_URI:-bolt://127.0.0.1:57687}"
export D2G_IT_NEO4J_USER="${D2G_IT_NEO4J_USER:-neo4j}"
export D2G_IT_NEO4J_PASSWORD="${D2G_IT_NEO4J_PASSWORD:-d2g-test-only}"
export D2G_IT_QDRANT_URL="${D2G_IT_QDRANT_URL:-http://127.0.0.1:56333}"

mkdir -p target
set +e
cargo test -p db2graph-sync --features "postgres neo4j vector-qdrant" --test pg_neo4j --test qdrant -- --nocapture --test-threads 1 2>&1 | tee target/integration.log
code=${PIPESTATUS[0]}
set -e
if grep -q '^UNEXECUTED' target/integration.log; then
  echo "Some integration tests did not execute (see target/integration.log)" >&2
  exit 2
fi
exit "$code"
