#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo 'run as root on the isolated staging host' >&2
  exit 1
fi

cleanup() {
  runuser -u postgres -- psql --set ON_ERROR_STOP=1 --command 'ALTER ROLE pons_radar NOCREATEDB' >/dev/null
}
trap cleanup EXIT

runuser -u postgres -- psql --set ON_ERROR_STOP=1 --command 'ALTER ROLE pons_radar CREATEDB' >/dev/null
set -a
# shellcheck disable=SC1091
source /etc/pons-radar/environment
set +a
cd /tmp/pons-phase21-src-v3
sudo -u ubuntu -E env HOME=/home/ubuntu RUSTUP_HOME=/home/ubuntu/.rustup CARGO_HOME=/home/ubuntu/.cargo \
  /home/ubuntu/.cargo/bin/cargo test --workspace
