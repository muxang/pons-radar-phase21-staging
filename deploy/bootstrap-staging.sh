#!/usr/bin/env bash
set -euo pipefail

id pons-radar >/dev/null 2>&1 || useradd --system --home /var/lib/pons-radar --shell /usr/sbin/nologin pons-radar
install -d -o pons-radar -g pons-radar -m 0750 /var/lib/pons-radar /var/lib/pons-radar/update /var/lib/pons-radar/update/staging /var/lib/pons-radar/update/backups
install -d -o root -g pons-radar -m 0750 /etc/pons-radar
install -d -o pons-radar -g pons-radar -m 0770 /opt/pons-radar
install -o pons-radar -g pons-radar -m 0750 /tmp/pons-phase21-src-v2/target/release/pons-radar /opt/pons-radar/pons-radar
install -o pons-radar -g pons-radar -m 0750 /tmp/pons-phase21-src-v2/target/release/pons-radar-updater /opt/pons-radar/pons-radar-updater

if ! sudo -u postgres psql -Atqc "SELECT 1 FROM pg_roles WHERE rolname='pons_radar'" | grep -q 1; then
  sudo -u postgres psql -v ON_ERROR_STOP=1 -c "CREATE ROLE pons_radar LOGIN" >/dev/null
fi
if ! sudo -u postgres psql -Atqc "SELECT 1 FROM pg_database WHERE datname='pons_radar'" | grep -q 1; then
  sudo -u postgres createdb -O pons_radar pons_radar
fi

if [[ ! -f /etc/pons-radar/environment ]]; then
  db_password="$(openssl rand -hex 32)"
  sudo -u postgres psql -v ON_ERROR_STOP=1 -c "ALTER ROLE pons_radar PASSWORD '${db_password}'" >/dev/null
  setup_token="$(openssl rand -hex 32)"
  cat > /etc/pons-radar/environment <<EOF
DATABASE_URL=postgresql://pons_radar:${db_password}@127.0.0.1:5432/pons_radar
ADMIN_SETUP_TOKEN=${setup_token}
RUST_LOG=info
EOF
fi
if [[ ! -f /root/pons-radar-admin-initial ]]; then
  admin_password="$(openssl rand -hex 32)"
  cat > /root/pons-radar-admin-initial <<EOF
username=stageadmin
password=${admin_password}
EOF
fi
chown root:pons-radar /etc/pons-radar/environment
chmod 0640 /etc/pons-radar/environment
chmod 0600 /root/pons-radar-admin-initial

sudo -u postgres psql -d pons_radar -Atqc 'SELECT current_database(), current_user'
stat -c '%U:%G %a %n' /opt/pons-radar/pons-radar /etc/pons-radar/environment /var/lib/pons-radar
