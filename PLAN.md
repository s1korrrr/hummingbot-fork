# PLAN

## Task

- Objective: make H28 release-image dependency installation deterministic for
  both `linux/amd64` and `linux/arm64`, without weakening the existing
  `HOLD_FOR_EVIDENCE` publication gate.
- Owner repo: `hummingbot`.
- Branch: `feat/andrzej_hummingbot_miniforge_release` in the existing isolated
  `qte_miniforge_release` worktree.
- Capital impact: release/runtime-adjacent; no image publication, service
  deployment, credentials, orders, merge, or trading action is authorized.
- Spec authority: this plan records the dependency-mutation blocker found by
  the H28 PR audit. There is no separate dependency-lock specification.

## Evidence and assumptions

- H28 currently solves broad `setup/environment.yml` ranges during every image
  build and separately installs unpinned `eip712-structs`.
- Read-only dry runs succeeded for both `linux-64` and `linux-aarch64` when
  `CONDA_OVERRIDE_GLIBC=2.17` declared the Linux solver virtual package.
- Both current solves select Python `3.13.15`; the installed development
  environment also uses Python 3.13.
- Installed pip-only inputs are `injective-py==1.13.0` and
  `eip712-structs==1.1.0`.
- Conda 26.5 documentation supports target-specific `@EXPLICIT` URL locks with
  SHA-256 fragments and direct `conda create --file` consumption.

## Global constraints

- Release Docker stages must never solve `environment.yml` or a broad package
  range. They must consume a committed lock selected from `TARGETPLATFORM`.
- Commit one explicit Conda lock for `linux-64` and one for
  `linux-aarch64`. Every package URL must be HTTPS, from `conda-forge`, belong
  to the target subdir or `noarch`, and end in an exact 64-hex SHA-256 fragment.
- Lock Python to the same selected release version on both targets. Any target,
  source-fingerprint, package-origin, platform, or hash mismatch fails before
  source compilation.
- Commit one pip lock containing only exact versions and SHA-256 hashes for the
  pip-only packages. Release installation must use `--require-hashes --no-deps`.
- Keep broad environment recipes available for developer workflows, but pin
  their pip-only direct requirements to the exact release versions so intent
  does not diverge silently.
- A checked-in validator must bind lock artifacts to the canonical source
  recipes and validate them without network access. A regeneration command and
  provenance notes must be documented.
- Preserve the Miniforge digest, image-identity gates, license inventory,
  notices, tini/runtime contract, immutable deploy contract, and current
  `HOLD_FOR_EVIDENCE` policy.
- No placeholder hashes, partial target locks, defaults-channel artifacts,
  hidden fallback solve, automatic lock refresh, or hosted-CI claim.

## Options considered

1. Keep broad solves and rely only on the generated license inventory.
   - Rejected: evidence detects drift after the solve but does not make a later
     build reproducible.
2. Add target-specific explicit Conda locks plus a hash-checked pip lock.
   - Chosen: uses Conda's native no-solve format, keeps platform differences
     explicit, and requires no permanent lock-tool dependency.
3. Introduce `conda-lock` or Pixi as a new repository toolchain.
   - Deferred: unnecessary until the native explicit-lock route is proven;
     adding a permanent dependency would expand maintenance and bootstrap scope.

## Task 1: Generate and validate complete target locks

- Add a deterministic lock generator and offline validator under `scripts/`.
- Add both target-specific explicit Conda locks, the exact pip hash lock, and a
  small provenance/fingerprint record under `setup/locks/`.
- Pin direct pip-only source requirements to the exact locked versions.
- Add focused tests for exact target coverage, URL/channel/subdir/hash grammar,
  source fingerprint binding, pip hashes, unsupported targets, and tampering.
- DoD: validator passes offline for both targets; every adversarial fixture
  fails at its intended rule; no release Dockerfile change is included yet.

## Task 2: Consume locks in the release image

- Update `Dockerfile` so `builder-environment` selects the exact Conda lock from
  `TARGETPLATFORM`, validates all lock inputs, creates the environment without a
  solve, and installs the pip lock with `--require-hashes --no-deps`.
- Preserve package URL, selected-Python, pip-report, and license-inventory
  evidence paths expected downstream.
- Extend image-identity tests so missing/unsupported/mismatched locks and any
  fallback to broad recipes fail closed.
- Update the release gate runbook with the exact regeneration and verification
  commands and the distinction between reproducible source inputs and actual
  per-platform runtime evidence.
- DoD: static/fake tests pass and local `builder-environment` builds succeed for
  both `linux/amd64` and `linux/arm64`, or the exact external/runtime blocker is
  recorded without weakening the lock contract.

## Task 3: Audit, close out, and push the reviewed slice

- Run license tests, the complete relevant image-identity suite, changed-file
  hooks, workflow syntax, lock verification, diff hygiene, and exact-tag cleanup.
- Obtain independent spec/quality review and a broad final branch review.
- Update `PLAN.md`, `TODO.md`, and `MEMORY.md` with exact evidence, rollback,
  residual risks, and any rulings.
- Commit coherent reviewed slices and non-force push only after checking the
  remote H28 head has not moved.
- DoD: pushed source is at least `repo-ready`; image/runtime/legal/publication
  ceilings remain stated truthfully.

## Test plan

- Unit/static: lock parser/generator/validator and Docker selection contract.
- Integration: `conda create --dry-run --file <explicit-lock>` where supported,
  pip `--require-hashes` resolution/download checks, and license inventory tests.
- Image: both target `builder-environment` builds, environment provenance probe,
  selected Python equality, and exact cleanup of generated test tags.
- Regression: existing identity, deploy/down, Miniforge, license-policy, and
  generated-inventory tests.

## Risks and rollback

- Lock generation may expose package availability or Python compatibility
  divergence between architectures. Invalidate the slice rather than omitting a
  target or substituting a solve.
- Explicit locks reproduce packages; they do not prove compiled Hummingbot or
  runtime behavior. That proof remains a separate gate.
- Package URLs can disappear upstream. Regeneration is an explicit reviewed
  operation, never an automatic fallback.
- Rollback: revert the lock-consumption commits to the current H28 source state.
  Publication remains held throughout, so rollback requires no registry or
  runtime cleanup.

## Memory impact

- Record confirmed generation/verification commands, target naming, lock/source
  binding, Python selection, Docker consumption boundary, and remaining runtime
  or legal gates.

## Final notes

- Pending implementation.
