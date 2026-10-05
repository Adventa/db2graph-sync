# Runs the real-service integration tests (PostgreSQL, Neo4j, Qdrant).
# Fails if any test reports UNEXECUTED, so missing services can never be
# mistaken for passing coverage.
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

docker compose -f deploy/test/docker-compose.yml up -d --wait
if ($LASTEXITCODE -ne 0) { throw "docker compose failed" }

$env:D2G_IT_PG_HOST = '127.0.0.1'
$env:D2G_IT_PG_PORT = '55432'
$env:D2G_IT_PG_USER = 'd2g'
$env:D2G_IT_PG_PASSWORD = 'd2g-test-only'
$env:D2G_IT_PG_DB = 'd2g'
$env:D2G_IT_NEO4J_URI = 'bolt://127.0.0.1:57687'
$env:D2G_IT_NEO4J_USER = 'neo4j'
$env:D2G_IT_NEO4J_PASSWORD = 'd2g-test-only'
$env:D2G_IT_QDRANT_URL = 'http://127.0.0.1:56333'

$log = Join-Path $root 'target/integration.log'
New-Item -ItemType Directory -Force (Split-Path $log) | Out-Null
cargo test -p db2graph-sync --features "postgres neo4j vector-qdrant" --test pg_neo4j --test qdrant -- --nocapture --test-threads 1 2>&1 | Tee-Object -FilePath $log
$code = $LASTEXITCODE
if (Select-String -Path $log -Pattern '^UNEXECUTED' -Quiet) { Write-Error "Some integration tests did not execute (see $log)"; exit 2 }
exit $code
