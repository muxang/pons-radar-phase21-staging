# Phase 17 secure GitHub release updater

The updater is optional infrastructure and never participates in application readiness. Stable discovery uses the official GitHub Releases API, ignores drafts/prereleases, and compares parsed SemVer values. Public repositories need no credential; private repositories may use only the `GITHUB_TOKEN` environment variable. Tokens are never persisted, returned by an API, or logged without redaction.

## Trust and compatibility

`release-manifest.json` is verified byte-for-byte with an Ed25519 detached signature before any manifest field is trusted. The locally configured `key_id -> Ed25519 public key` set is the trust root and permits an overlap window for key rotation. A remote manifest cannot add a trusted key. Missing/unknown/invalid signatures and unsupported manifest versions fail closed.

The signed manifest declares app/build/API identity, platform assets and SHA256, database schema bounds/target, and both `rollback_safe` and `old_binary_compatible_with_target_schema`. Installation is blocked unless the current schema is supported and the previous binary remains compatible with the target schema. Binary rollback is not database rollback; forward-only migration releases must use an expand/contract-compatible schema or remain manual/out of band. PostgreSQL backup is strongly recommended before every migration-bearing release.

Assets are streamed with timeout and signed size bounds from the selected trusted GitHub release asset endpoint. SHA256 must match the signed manifest. Extraction accepts exactly `pons-radar`, `pons-radar-updater`, and `VERSION`, rejecting absolute/traversal paths, nested entries, links and unexpected files. The staged binary's `--version-json` output must match the signed app version, frontend build ID and API schema version.

## Handoff and rollback

The main process commits `system.update_installing`, starts the independent helper, then cancels the normal application cancellation token. Axum and durable workers shut down gracefully. The helper waits for the parent PID to exit, backs up the old binary, copies the candidate to a same-directory temporary file, preserves executable permissions, fsyncs it, and atomically renames it over the executable. It restarts the single configured systemd unit and requires `/healthz`, `/readyz`, and exact version/build/schema identity within the timeout.

Failure restores only the directly previous binary, restarts systemd, and verifies old liveness/readiness. Results are durable `SUCCEEDED`, `ROLLED_BACK`, or `ROLLBACK_FAILED` history plus outbox events. A new binary can finish a pending handoff after helper interruption once its own health/readiness is available. A rollback failure produces a CRITICAL system warning.

The service account needs a narrowly scoped operating-system authorization for its helper to replace the configured executable and restart only `pons-radar.service`; do not grant general passwordless shell access. Config files and PostgreSQL are never modified by the helper except normal application migrations and updater state rows.

## Operations

Defaults are `auto_check=true`, `auto_install=false`. Installation always requires an authenticated admin mutation with same-origin/CSRF validation and explicit confirmation. A PostgreSQL partial unique index permits only one active installation. Network/rate-limit failures mark only updater health `DEGRADED`; chain indexing and `/readyz` remain independent.

Phase 17 emits durable update alerts but intentionally does not implement the Phase 18 frontend build-mismatch refresh modal or automatic refresh.

GitHub Artifact Attestations/Sigstore provenance remain optional defense-in-depth hardening. Runtime verification intentionally depends on the local Ed25519 trust root and signed asset hash, not `gh` CLI or GitHub attestation availability.
