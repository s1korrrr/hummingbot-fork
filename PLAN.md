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
- Commit one complete, target-specific hashed pip closure for
  `injective-py==1.13.0` per release architecture, plus the separate exact
  `eip712-structs==1.1.0` direct-artifact lock. Release installation must use
  `--require-hashes --no-deps`, so neither pip install performs a solve.
- Treat `safe-pysha3==1.0.4` as the single explicit runtime substitute for
  `eip712-structs==1.1.0`'s stale `pysha3>=1.0.2` metadata edge. The validator
  must keep this exception named and bounded in pip-lock provenance and never
  claim that generic `pip check` is clean. Both target images must prove the `sha3` and
  `eip712_structs` imports plus a representative EIP-712 operation before the
  exception can advance beyond source readiness.
- Keep broad environment recipes available for developer workflows, but pin
  their pip-only direct requirements to the exact release versions so intent
  does not diverge silently.
- A checked-in validator must bind lock artifacts to the canonical source
  recipes and validate them without network access. A regeneration command and
  provenance notes must be documented.
- Resolve and validate the combined Conda-plus-pip environment. Every package
  present in both layers must have the same normalized version in both locks;
  source constraints such as `urllib3<2` remain binding. Missing, extra, or
  version-drifted pip transitive entries fail against an exact closure manifest.
- Python artifact generation must ignore ambient pip/uv configuration, use the
  explicit `https://pypi.org/simple` origin policy, and bind that policy into
  provenance. Host indexes, extra indexes, find-links, and local config may not
  influence committed locks.
- Preserve the Miniforge digest, image-identity gates, license inventory,
  notices, tini/runtime contract, immutable deploy contract, and current
  `HOLD_FOR_EVIDENCE` policy.
- No placeholder hashes, partial target locks, defaults-channel artifacts,
  hidden fallback solve, automatic lock refresh, or hosted-CI claim.

## Options considered

1. Keep broad solves and rely only on the generated license inventory.
   - Rejected: evidence detects drift after the solve but does not make a later
     build reproducible.
2. Add target-specific explicit Conda locks and promote pip dependencies into
   Conda.
   - Rejected after proof: `linux-aarch64` cannot satisfy the
     `eip712 -> eth-pydantic-types -> cchecksum` edge on Conda Forge.
3. Use native Conda explicit locks plus target-specific full pip closures.
   - Chosen: Conda remains no-solve; exact `uv 0.11.29` generation resolves a
     58-package Python 3.13 closure on both manylinux targets; install remains
     resolver-free with exact hashes. `uv` is a pinned regeneration tool, not
     a runtime or global repository dependency.

## Task 1: Generate and validate complete target locks

- Add a deterministic lock generator and offline validator under `scripts/`.
- Add both target-specific explicit Conda locks, both complete target-specific
  Injective pip closures, the separate exact EIP-712 direct lock, and a small
  provenance/fingerprint record under `setup/locks/`.
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

- Review correction in progress on 2026-08-25. The original generator used
  `actions.FETCH`, which can omit already cached packages; a new lock must be
  built from complete `actions.LINK` records with `CONDA_PKGS_DIRS` confined to
  the task temporary directory. No lock regeneration will proceed until the
  pip dependency-closure feasibility check is recorded.
- Final correction ruling: preserve complete cache-isolated native Conda locks,
  but move the Injective dependency closure to target-specific hashed pip
  locks generated by exact `uv 0.11.29` for Python 3.13 manylinux x86_64 and
  aarch64. Keep `eip712-structs==1.1.0` as the separate hashed no-deps install
  so the existing two pip evidence reports retain their meaning. Bind the
  platform, generator version, direct inputs, full lock hashes, and the single
  `pysha3 -> safe-pysha3==1.0.4` exception into offline provenance. Reversal
  condition: either target closure fails to regenerate, hashed artifacts fail
  to download, or the later target-image EIP-712 smoke fails.
- Correction outcome: the one authorized cache-isolated solve stopped at
  `linux-aarch64`. Conda reported `eip712>=0.3.0,<0.4.0 ->
  eth-pydantic-types>=0.2.4,<0.3 -> cchecksum>=0.0.3,<1`, but `cchecksum` is
  unavailable from Conda Forge. The promoted source specs, generator/validator
  correction, and test changes were reverted rather than leaving an invalid
  partial slice. Read-only feasibility probes then resolved the chosen
  58-package hashed Injective closure for both manylinux targets with exact
  `uv 0.11.29`; Task 1 must now regenerate under that ruling.
- Re-review correction: standalone closure feasibility is insufficient. The
  first closures conflict with ten Conda versions, including violating the
  source `urllib3<2` constraint with `urllib3==2.7.0`. Regeneration must
  constrain pip resolution against each exact Conda result and prove zero
  incompatible overlaps. Resolver configuration and PyPI origin must be
  isolated/bound, and provenance must carry exact normalized closure maps so
  missing, extra, or drifted transitive entries fail offline. If no compatible
  combined solution exists, stop and record the exact dependency conflict.
- Task 1 completed on 2026-08-25. Added native explicit Conda lock generation
  and offline validation under `scripts/`, with committed `linux-64` and
  `linux-aarch64` locks. Both select Python `3.13.15`; each contains 273
  package URLs pinned to HTTPS `conda-forge` artifacts and SHA-256 fragments.
- `setup/locks/provenance.json` binds each lock artifact and the canonical
  `setup/environment.yml` and `setup/pip_packages.txt` inputs. The pip lock
  contains only `injective-py==1.13.0` and `eip712-structs==1.1.0`, each with
  an exact SHA-256 hash. Source recipes now use those exact direct pins.
- Validation run: focused lock tests (15 passed), offline lock validator,
  `pip download --require-hashes --no-deps`, changed-file pre-commit hooks,
  Python compilation, diff hygiene, and the existing image-identity test run
  (no final failures recorded). Dockerfile/image behavior remains untouched.
- Rollback: revert the Task 1 commit to remove the locks and helpers. No
  registry, runtime, deployment, credential, or trading state was changed.
- Follow-up: Task 2 must consume only these validated locks in Docker and run
  per-platform builder evidence; source reproducibility is not runtime proof.
- Correction completed on 2026-08-25: regenerated both cache-isolated Conda
  locks and replaced the direct pip pair with two exact target-specific
  Injective closures plus the separate EIP-712 direct lock. The validator now
  checks actual YAML pip nesting, canonical source coverage, exact artifact
  inventory, UV 0.11.29/provenance bindings, and the bounded pysha3 exception.
  Source validation and adversarial tests are green; host pip cannot complete
  its non-installing closure download because `coincurve` fails its macOS sdist
  metadata hook, so exact UV target regeneration byte comparison is the
  recorded equivalent. Task 2 still requires target-image runtime evidence.
- Final combined resolution uses `eth-utils>=5,<6`, `eth-typing>=5,<6`, and
  `hexbytes>=1,<2`; both targets resolve 59 pip packages with 49 equal
  Conda/pip overlaps. Source locks remain distinct from target-image runtime
  evidence.
