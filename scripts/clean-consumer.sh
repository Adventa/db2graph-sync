#!/usr/bin/env bash
# Packages db2graph-sync exactly as it would be published, unpacks the .crate
# into a temporary directory, and builds + runs an independent consumer
# project against it (no workspace, no path back into this repository).
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(pwd)

cmp LICENSE crates/db2graph-sync/LICENSE
cmp NOTICE crates/db2graph-sync/NOTICE

cargo package -p db2graph-sync --locked --allow-dirty
version=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print([p["version"] for p in json.load(sys.stdin)["packages"] if p["name"]=="db2graph-sync"][0])')
work=$(mktemp -d)
tar xzf "target/package/db2graph-sync-${version}.crate" -C "$work"
test -f "$work/db2graph-sync-${version}/LICENSE"
test -f "$work/db2graph-sync-${version}/NOTICE"
test -f "$work/db2graph-sync-${version}/api/openapi.json"

mkdir -p "$work/consumer/src"
cp scripts/clean-consumer/main.rs "$work/consumer/src/main.rs"
cat > "$work/consumer/Cargo.toml" <<EOF
[package]
name = "d2g-clean-consumer"
version = "0.0.0"
edition = "2024"
publish = false

[workspace]

[dependencies]
db2graph-sync = { path = "../db2graph-sync-${version}", default-features = false }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
EOF
cd "$work/consumer"
CARGO_TARGET_DIR="$root/target/clean-consumer" cargo run --quiet
echo "clean consumer OK (db2graph-sync ${version})"
