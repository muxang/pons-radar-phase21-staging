#!/usr/bin/env bash
set -euo pipefail

config=/etc/pons-radar/config.toml
public_hex_file=/tmp/pons-ui-release-public.hex
public_hex="$(cat "$public_hex_file")"
[[ "$public_hex" =~ ^[0-9a-f]{64}$ ]]
sed -i \
  -e 's/key_id = "phase21-staging-2026"/key_id = "phase21-ui-2026"/' \
  -e "s/public_key_hex = \"[0-9a-f]*\"/public_key_hex = \"$public_hex\"/" \
  "$config"
systemctl restart pons-radar
for _ in $(seq 1 60); do
  if curl --fail --silent --output /dev/null http://127.0.0.1:3000/readyz; then
    echo 'PASS: deployment-pinned release key rotated and service is ready'
    exit 0
  fi
  sleep 1
done
echo 'service did not recover after trust pin rotation' >&2
exit 1
