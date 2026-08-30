# Ubuntu production/staging deployment

Supported Phase 21 target: Ubuntu Linux x86_64, PostgreSQL, systemd, and a
same-host updater helper. Run installation commands as root; run validation as
the unprivileged `pons-radar` operator unless a command explicitly says root.

## Filesystem and account

```sh
useradd --system --home /var/lib/pons-radar --shell /usr/sbin/nologin pons-radar
install -d -o root -g pons-radar -m 0770 /opt/pons-radar
install -d -o root -g pons-radar -m 0750 /etc/pons-radar
install -o pons-radar -g pons-radar -m 0750 pons-radar /opt/pons-radar/pons-radar
install -o pons-radar -g pons-radar -m 0750 pons-radar-updater /opt/pons-radar/pons-radar-updater
install -o root -g root -m 0644 deploy/pons-radar.tmpfiles.conf /usr/lib/tmpfiles.d/pons-radar.conf
systemd-tmpfiles --create /usr/lib/tmpfiles.d/pons-radar.conf
```

The updater performs same-filesystem atomic rename, so `/opt/pons-radar` must be
writable by its service group. This is a deliberate deployment trust boundary:
the service account can replace only files inside that directory. Configuration,
environment secrets, and PostgreSQL remain outside it and root-controlled.

## Configuration and secrets

```sh
install -o root -g pons-radar -m 0640 config.example.toml /etc/pons-radar/config.toml
install -o root -g pons-radar -m 0640 deploy/environment.example /etc/pons-radar/environment
```

Edit the installed files, never the repository examples. Set production values:

- `[app].environment = "production"`
- `[app].data_dir = "/var/lib/pons-radar"`
- `security.allowed_origin` to the exact HTTPS origin
- `security.session_cookie_secure = true`
- authorized Robinhood HTTP/WSS endpoints in the EnvironmentFile
- PostgreSQL URL only in the EnvironmentFile
- updater repository/channel and deployment-pinned public keys, if used
- keep `updater.auto_install = false`
- keep `ai.use_ai_research_in_signal = false`

The environment file must remain `root:pons-radar 0640`. Never pass secrets on a
command line, store them in TOML, return them to the browser, or include them in
validation evidence.

## PostgreSQL

Create a dedicated database/role with a long random password and restrict it to
the application database. PostgreSQL backups and restore drills are operator
responsibilities. Before a migration-bearing release, take a verified backup and
confirm the signed manifest rollback compatibility policy.

## systemd and updater authorization

```sh
install -o root -g root -m 0644 deploy/pons-radar.service /etc/systemd/system/pons-radar.service
install -o root -g root -m 0644 deploy/90-pons-radar-updater.rules /etc/polkit-1/rules.d/90-pons-radar-updater.rules
systemctl daemon-reload
systemctl enable --now pons-radar.service
```

The polkit rule grants the service user only `restart` on
`pons-radar.service`. Verify it on the exact Ubuntu/polkit version before enabling
the updater. Do not replace it with unrestricted passwordless sudo.

## Reverse proxy

Terminate TLS at the reverse proxy, preserve WebSocket Upgrade headers for `/ws`,
and forward the original HTTPS Origin. Do not expose the application port or
PostgreSQL publicly. `/healthz` is liveness; `/readyz` includes PostgreSQL
readiness. The application shell must retain `Cache-Control: no-store`, while
hashed assets remain immutable.

## Initial verification

```sh
systemd-analyze verify /etc/systemd/system/pons-radar.service
systemctl status pons-radar.service
curl --fail http://127.0.0.1:3000/healthz
curl --fail http://127.0.0.1:3000/readyz
journalctl -u pons-radar.service --since -10min --no-pager
```

Continue with `docs/phase-21-real-world-validation.md`. A successful install is
not sufficient to claim production readiness; live-chain, restart, updater,
browser, and soak evidence are separate gates.
