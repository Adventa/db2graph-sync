# Windows equivalent of clean-consumer.sh: package, unpack, build and run an
# independent consumer against the packaged crate.
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

foreach ($f in 'LICENSE', 'NOTICE') {
  if ((Get-FileHash $f).Hash -ne (Get-FileHash "crates/db2graph-sync/$f").Hash) { throw "crates/db2graph-sync/$f differs from root $f" }
}

cargo package -p db2graph-sync --locked --allow-dirty
if ($LASTEXITCODE -ne 0) { throw "cargo package failed" }
$meta = cargo metadata --format-version 1 --no-deps | ConvertFrom-Json
$version = ($meta.packages | Where-Object { $_.name -eq 'db2graph-sync' }).version
$work = Join-Path ([System.IO.Path]::GetTempPath()) ("d2g-consumer-" + [guid]::NewGuid())
New-Item -ItemType Directory -Force $work | Out-Null
tar xzf "target/package/db2graph-sync-$version.crate" -C $work
foreach ($f in 'LICENSE', 'NOTICE', 'api/openapi.json') {
  if (-not (Test-Path (Join-Path $work "db2graph-sync-$version/$f"))) { throw "packaged crate is missing $f" }
}
New-Item -ItemType Directory -Force (Join-Path $work 'consumer/src') | Out-Null
Copy-Item scripts/clean-consumer/main.rs (Join-Path $work 'consumer/src/main.rs')
@"
[package]
name = "d2g-clean-consumer"
version = "0.0.0"
edition = "2024"
publish = false

[workspace]

[dependencies]
db2graph-sync = { path = "../db2graph-sync-$version", default-features = false }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
"@ | Set-Content (Join-Path $work 'consumer/Cargo.toml') -Encoding utf8NoBOM
Push-Location (Join-Path $work 'consumer')
$env:CARGO_TARGET_DIR = Join-Path $root 'target/clean-consumer'
cargo run --quiet
$code = $LASTEXITCODE
Pop-Location
Remove-Item Env:CARGO_TARGET_DIR
if ($code -ne 0) { throw "clean consumer failed" }
Write-Output "clean consumer OK (db2graph-sync $version)"
