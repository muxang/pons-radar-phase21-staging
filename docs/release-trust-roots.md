# Release trust roots

The updater accepts a release manifest only when its `signing_key_id` resolves in
the local trusted set and the detached Ed25519 signature verifies. The set is the
union of roots compiled into `pons-radar`/`pons-radar-updater` and optional
deployment-pinned roots in `config.toml`. Release JSON, GitHub API responses and
downloaded assets cannot add or replace keys; unknown manifest fields are rejected.
Release builds compile the active public root from the non-secret GitHub repository
variables `RELEASE_TRUST_ROOT_ID` and `RELEASE_TRUST_ROOT_HEX`; these are build
inputs, never values obtained from the release being verified.

Deployment pins are a host-level trust decision, not application administration.
There is no Web Admin endpoint for them and the updater never writes configuration.
On Unix production deployments, a config containing pins must be owned by root and
must not be group/world writable. Recommended mode is `root:<service-group> 0640`
or stricter, with the service account read-only.

Rotate keys with overlapping binary generations. Ship generation N with A+B, sign
the next trusted release with B, then ship generation N+1 with B+C. Only after the
fleet trusts C may a later binary remove B. Once a key is removed, releases signed
only by that key fail closed. Signing private keys remain solely in the protected
release environment and are never packaged or configured at runtime.
