#!/usr/bin/env bash
set -euo pipefail

release_dir="${PHASE21_RELEASE_DIR:?PHASE21_RELEASE_DIR is required}"
version="${PHASE21_RELEASE_VERSION:?PHASE21_RELEASE_VERSION is required}"
build_id="${PHASE21_FRONTEND_BUILD_ID:?PHASE21_FRONTEND_BUILD_ID is required}"
repository="${PHASE21_GITHUB_REPOSITORY:?PHASE21_GITHUB_REPOSITORY is required}"
signing_key="${PHASE21_SIGNING_KEY_FILE:?PHASE21_SIGNING_KEY_FILE is required}"
key_id="${PHASE21_SIGNING_KEY_ID:?PHASE21_SIGNING_KEY_ID is required}"
asset="$release_dir/pons-radar-linux-x86_64.tar.gz"
manifest="$release_dir/release-manifest.json"

size="$(stat -c '%s' "$asset")"
hash="$(sha256sum "$asset" | cut -d' ' -f1)"
now="$(date -u +%FT%TZ)"
jq -n --arg version "$version" --arg now "$now" --arg build "$build_id" \
  --arg key "$key_id" --arg hash "$hash" --arg repository "$repository" \
  --argjson size "$size" \
  '{manifest_version:1,app_version:$version,channel:"stable",published_at:$now,git_commit:"phase21-staging",build_timestamp:$now,frontend_build_id:$build,api_schema_version:1,min_db_schema:27,max_db_schema:27,target_db_schema:27,rollback_safe:true,old_binary_compatible_with_target_schema:true,assets:[{platform:"linux",architecture:"x86_64",filename:"pons-radar-linux-x86_64.tar.gz",size:$size,sha256:$hash}],release_notes:("https://github.com/"+$repository+"/releases/tag/v"+$version),signing_key_id:$key}' > "$manifest"
openssl pkeyutl -sign -rawin -inkey "$signing_key" -in "$manifest" | base64 -w0 > "$release_dir/release-manifest.sig"
sha256sum "$asset" "$manifest" "$release_dir/release-manifest.sig" > "$release_dir/SHA256SUMS"
echo "PASS: signed release $version packaged"
