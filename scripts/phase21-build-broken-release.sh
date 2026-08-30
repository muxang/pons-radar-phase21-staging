#!/usr/bin/env bash
set -euo pipefail

source_tree="${PHASE21_SOURCE_TREE:-/tmp/pons-phase21-src-v3}"
broken_tree="${PHASE21_BROKEN_TREE:-/tmp/pons-phase21-src-c}"
source_version="${PHASE21_SOURCE_VERSION:-0.1.5}"
target_version="${PHASE21_BROKEN_VERSION:-0.1.6}"
build_id="${PHASE21_BROKEN_BUILD_ID:-stage-c-broken-final}"
rm -rf "$broken_tree"
cp -a "$source_tree" "$broken_tree"
sed -i "s/version = \"${source_version}\"/version = \"${target_version}\"/" "$broken_tree/Cargo.toml"
perl -0pi -e 's/let ready = match &state\.readiness \{.*?\n    \};/let ready = false;/s' "$broken_tree/crates/app/src/server.rs"
grep -A2 -n 'async fn readyz' "$broken_tree/crates/app/src/server.rs"
(
  cd "$broken_tree/frontend"
  VITE_APP_VERSION="$target_version" VITE_FRONTEND_BUILD_ID="$build_id" VITE_API_SCHEMA_VERSION=1 npm run build >/dev/null
)
(
  cd "$broken_tree"
  FRONTEND_BUILD_ID="$build_id" /home/ubuntu/.cargo/bin/cargo build --workspace --release
  ./target/release/pons-radar --version-json
)
