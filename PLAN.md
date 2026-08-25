# PLAN

## Task

- Objective: make the documented Compose deployment consume an explicit
  immutable Hummingbot image identity instead of `hummingbot/hummingbot:latest`.
- Owner repo: `hummingbot`.
- Capital impact: live-capital-adjacent deployment plumbing; no service start,
  credentials, orders, publication, merge, or trading action is authorized.

## Context and assumptions

- PR #27 publishes digest-only images and intentionally emits no `latest` tag.
- `docker-compose.yml`, `Makefile`, and `README.md` still route operators through
  a mutable tag and a stale build comment that omits required source identity.
- The release lane's exact
  `hummingbot/hummingbot@sha256:<64 lowercase hex>` reference or a local
  `sha256:<64 lowercase hex>` image ID is an acceptable immutable runtime input.

## Constraints

- Preserve the current Compose volume, network, Gateway-profile, and shutdown
  behavior.
- `make deploy` must fail before invoking Docker on missing, mutable, malformed,
  multiline, or shell-shaped image input.
- `.compose.env` is declarative input owned by `make setup`: exactly one
  `COMPOSE_PROFILES=` or `COMPOSE_PROFILES=gateway` line. It must never be
  sourced as shell code or control the image identity.
- Automatic `.env` loading and ambient Compose file/profile/env-file routing
  must not alter the controlled deployment path.
- Deploy and shutdown must share a fixed Compose project identity so ambient
  project routing cannot redirect the kill path.
- Local builds must continue through the clean-tree, exact-SHA `make build`
  path; Compose must not regain an unbound inline build path.
- Gateway remains a separately disclosed mutable-image risk.

## Options considered

1. Keep `latest` and document the digest separately.
   - Rejected: the runtime would remain mutable and disconnected from the
     release output.
2. Require an immutable image through Compose and validate it in `make deploy`.
   - Chosen: smallest fail-closed contract compatible with registry digests and
     exact local image IDs.
3. Add automatic registry resolution or local build-and-deploy orchestration.
   - Deferred: adds network, credential, tag-race, and lifecycle behavior not
     required to close this defect.

## Execution and tests

1. Add RED tests for missing/mutable/malformed image rejection and exact digest/
   local-ID acceptance before any Compose invocation.
2. Require `HUMMINGBOT_IMAGE` in Compose, validate its process-environment value
   in Make, and parse the allowlisted optional Compose environment as a literal
   Gateway toggle.
3. Update README deployment examples and explicitly disclose the Gateway tag.
4. Run focused tests, Compose config checks, pre-commit, actionlint, and diff
   hygiene; obtain fresh independent review before pushing.

## Risks and rollback

- Risk: existing `make deploy` callers must now supply an immutable identity.
  This intentional fail-closed break is required because the new release lane
  does not publish `latest`.
- Rollback: revert the bounded Compose/Makefile/README/test commit. No external
  runtime state is created by this work.

## Memory impact

- Record the immutable `HUMMINGBOT_IMAGE` deployment contract and validation
  commands if confirmed.

## Final notes

- Compose now requires an explicit immutable Hummingbot registry digest or
  exact local image ID through the documented `make deploy` boundary; mutable,
  malformed, multiline, or shell-shaped values fail before Docker is invoked.
- `.compose.env` is never sourced. Its complete contents are matched against
  the two allowlisted Gateway-profile byte sequences with `cmp` and translated
  into an explicit `--profile` argument. The Compose file is selected
  explicitly; ambient file/profile/env-file routing is cleared and automatic
  `.env` loading is disabled before Docker. Both lifecycle commands use the
  fixed Compose project name `hummingbot`.
- `make down` supplies an exact parse-only local image sentinel so shutdown
  remains available without a deployment image identity; it never pulls or
  starts that sentinel.
- The stale inline Compose build path was removed. README documents published
  digest and clean local-build flows and discloses Gateway's separate mutable
  `latest` risk.
- Two independent reviews correctly rejected shell-sourcing, multiline,
  malformed-reference, ambient Compose routing, unterminated-record, and
  shutdown regressions. A third review found default `.env` and NUL-byte
  bypasses. All reproductions are now regression tests.
- Final validation: 70/70 image-identity tests passed, including real local
  BuildKit cases; changed-file pre-commit, actionlint, and `git diff --check`
  passed. The fresh push-gate review returned `READY_WITH_RISK` at 0.97
  confidence with no blocking correctness or security finding.
- Residual risk is explicit: direct Compose remains outside Make's validation
  boundary and optional Gateway remains mutable at `latest`. This is
  source/test proof, not runtime/deployment proof. No service was started,
  image published, or trade sent.
- Remaining before push: commit the reviewed slice and non-force push only if
  the remote branch head is unchanged.
