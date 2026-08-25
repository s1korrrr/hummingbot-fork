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

## Task 2 review correction: optimized runtime gate

- Finding: the initial smoke used Python `assert`, so `python -O` removed every
  semantic check; it also checked only signable length/prefix rather than the
  deterministic EIP-712 vector.
- Finding: `builder-bootstrap` installed apt packages before its descendant
  validated dependency locks, contradicting the documented fail-before-install
  boundary and escaping tests scoped only to `builder-environment` text.
- Options considered:
  1. Keep assertions and rely on the Docker invocation omitting `-O`.
     Rejected because ambient optimization must not silently disable a release
     reversal gate.
  2. Use explicit comparisons that raise `RuntimeError` and bind the complete
     signable vector plus its Keccak digest. Chosen because failure behavior is
     optimization-independent and the vector detects semantic encoding drift.
  3. Add a `lock-validation` stage directly from `identity-check`, then make
     `builder-bootstrap` inherit it. Chosen so unsupported targets and invalid
     locks stop before apt or environment installation while validated
     artifacts remain available to descendants.
- TDD boundary: execute a temporary helper copy under `python -O` after
  replacing the accepted empty-input digest with a wrong value. The old helper
  must incorrectly exit zero (RED); the corrected helper must exit nonzero.
- Cross-stage TDD boundary: assert the exact ancestry
  `identity-check -> lock-validation -> builder-bootstrap -> builder-environment`
  and prove the validator is in the ancestor while apt is absent from it. The
  current Dockerfile must fail before stage refactoring.
- Scope: `Dockerfile`, `scripts/verify_release_eip712_runtime.py`, behavioral
  and cross-stage regressions in `test/test_docker_image_identity.py`, and Task
  2 truth artifacts only.
- Validation: behavioral regression, helper under normal and optimized Python,
  focused Docker contract, lock/license suites as relevant, Python compilation,
  changed-file pre-commit, and diff hygiene.
- Image rebuild decision: the Docker stage graph changes, so both target
  dependency images must be rebuilt, inspected, and cleaned even though the
  dependency sets and selected locks remain unchanged.

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
     59-package Python 3.13 closure on both manylinux targets; install remains
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

### Task 2 execution contract

- Owner paths: `Dockerfile`, `test/test_docker_image_identity.py`,
  `scripts/verify_release_eip712_runtime.py`,
  `docs/release/image-license-gate.md`, `PLAN.md`, `TODO.md`, and `MEMORY.md`.
- Capital and authority boundary: local source, tests, and disposable image
  builds only. No push, merge, registry publication, service mutation,
  credentials, orders, or trading action.
- Source inputs: canonical `setup/environment.yml` and
  `setup/pip_packages.txt`, the complete six-artifact `setup/locks/` inventory,
  and `scripts/validate_release_dependency_locks.py` must enter the environment
  stage together. The pinned Miniforge base Python must import `yaml` and
  `conda.models.version` before the offline validator runs; validator bootstrap
  may not install packages.
- Target mapping: `linux/amd64` consumes `linux-64.explicit.txt` and
  `pip-injective-linux-64.lock.txt`; `linux/arm64` consumes
  `linux-aarch64.explicit.txt` and
  `pip-injective-linux-aarch64.lock.txt`. Empty or unsupported values fail
  closed with no default or broad-recipe fallback.
- Installation boundary: create `/opt/conda/envs/hummingbot` only through the
  selected `@EXPLICIT` file. Install the selected complete Injective closure
  and then `pip-eip712-direct.lock.txt` with isolated, hash-required, no-deps,
  PyPI-only pip commands and distinct install reports.
- Runtime reversal gate: after EIP-712 installation, the target image must run
  a deterministic signable-bytes operation and the canonical empty-input
  Keccak-256 check. Generic `pip check` remains intentionally non-green and is
  neither run nor claimed because upstream EIP-712 metadata still requires
  `pysha3` rather than the reviewed `safe-pysha3` substitute.
- Audit evidence: retain the selected Conda and pip locks, source recipes,
  provenance, target selection, final Conda lists, explicit package URLs,
  selected Python, and the two semantically distinct pip reports under
  `/opt/conda/image-evidence` or the established `/opt/conda/conda-meta` paths.
- Missing optional context: this nested repository has no task-local daily
  memory files, lesson stores, or `TOOLS.md`; writer ownership does not include
  creating them. The existing `MEMORY.md`, Task 1 report, and workspace
  operating files are the available context for this slice.

### Task 2 options considered

1. Keep EIP-712 installation in the later source-builder stage.
   - Rejected: a `builder-environment` target build would not exercise the
     named substitution or produce both dependency reports, so it could not be
     the required dependency-runtime proof surface.
2. Install both locked pip sets and run the deterministic smoke in
   `builder-environment`.
   - Chosen: dependency creation, evidence, and the substitution reversal gate
     stay in one architecture-specific, source-compilation-independent stage.

### Task 2 test and rollback plan

- TDD red gate: add focused static contract tests before changing the
  Dockerfile or adding the smoke helper, then record their expected failures.
- Focused validation: image-identity static tests, dependency-lock tests and
  offline validator, license inventory tests, Python compilation, changed-file
  pre-commit, `git diff --check`, and a shadow-diff/status review.
- Runtime validation: build disposable exact tags for `builder-environment` on
  `linux/arm64` and `linux/amd64`, inspect selected-lock/evidence paths, run the
  smoke inside each image, and remove the exact tags. If platform emulation or
  upstream artifact access blocks a target, record the exact failure without a
  fallback.
- Rollback: revert only the Task 2 commits. The prior broad-solve Dockerfile is
  not publication-ready, and publication remains `HOLD_FOR_EVIDENCE` before,
  during, and after rollback.

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

### Authoritative current result

- Task 1 was independently accepted `READY_WITH_RISK` on 2026-08-25 as
  `repo-ready` source-lock evidence. It does not establish target-image runtime
  evidence.
- The six-artifact inventory contains two 367-record explicit Conda locks, two
  target-specific 59-package Injective pip closures, one direct EIP-712 lock,
  and provenance. Each target has 49 equal normalized Conda/pip overlaps and
  zero version conflicts.
- Both target resolutions select Python `3.13.15`. Exact `uv 0.11.29`, direct
  inputs, source hashes, artifact hashes, closure maps, overlap maps, PyPI
  origin, and the named `pysha3 -> safe-pysha3==1.0.4` runtime substitution are
  bound in provenance and enforced offline.
- Resolver subprocesses strip ambient pip/UV configuration, use isolated
  caches and configuration, and explicitly select `https://pypi.org/simple`.
  Source version constraints are checked against each resolved Conda lock.
- Validation includes the focused adversarial suite, offline validation for
  both targets, Python compilation, exact Python 3.13.15 regeneration with a
  byte-for-byte artifact comparison, changed-file pre-commit hooks, and diff
  hygiene. Exact results are recorded in the Task 1 report.
- Host pip cannot complete a non-installing closure download because
  `coincurve` fails its macOS sdist metadata hook. Target-aware UV regeneration
  is the recorded source-lock equivalent. Task 2 subsequently proved the
  selected closures and representative EIP-712 behavior in both target
  dependency images.
- Task 2 maps each supported target to exactly one Conda and Injective lock,
  rejects other/empty targets, validates all six artifacts before installation,
  creates the environment from `@EXPLICIT`, and disables both dependency and
  PEP 517 build isolation for the two hash-required pip installs.
- The first local target build exposed that pinned Miniforge provides
  `ruamel.yaml` and Conda `VersionSpec` but not PyYAML. A narrow build-only
  compatibility module now supplies the validator's `safe_load`/`YAMLError`
  interface without installing or solving a bootstrap dependency.
- A provisional pair of green builds was rejected after its logs showed pip
  installing EIP build dependencies. A new RED assertion required
  `--no-build-isolation`; both corrected builds then omitted that network solve.
- Corrected local `builder-environment` images built and were inspected for
  `linux/arm64` (`sha256:637416ca42487401729eaf9420d7badd024e675ea696bad77b7b3a9b1cbd89da`)
  and `linux/amd64`
  (`sha256:e73de950235a283b15463c954711f93b06c99be16565cab724367cef97852b34`).
  Each selected Python 3.13.15, preserved byte-identical selected locks and
  nonempty installed-set evidence, kept the two pip reports semantically
  distinct, and passed the EIP-712/Keccak runtime smoke. Exact temporary tags
  were removed and confirmed absent.
- Diagnostic `pip check` remained non-green on both targets, including
  `eip712-structs 1.1.0 requires pysha3, which is not installed.` This result is
  recorded but is not represented as a green generic environment gate; the
  scoped runtime smoke is the named-substitution reversal gate.
- Final local validation: focused Docker contract `6 passed, 84 deselected`;
  complete lock suite `37 passed`; offline validator confirmed both targets and
  Python 3.13.15; license policy/generated inventory/third-party inventory
  suite `17 passed`; Python compilation, all changed-file pre-commit hooks, and
  `git diff --check` passed.
- Independent Task 2 audit correction moved complete offline validation into a
  `lock-validation` stage directly descended from `identity-check`; both fresh
  build logs show validation completing before the first apt step. The runtime
  helper now uses explicit `RuntimeError` comparisons that remain active under
  `python -O` and binds the independently recomputed signable vector
  `1901de57171073f97873bc93388ae5e87afd47d803e4b4a59a7a402a0eb71b7aa2e664e15bdc268f69b1f35c21c9559e329a2f3587fe22a9800badf545aec280d842`
  and Keccak
  `358eb3432f7a6fdfb6bbec6fcc85bf845d3feea114fb95722bbbf854a934453e`.
- Corrected implementation commit `5553e63b6ebd8bcfcdd4e7c54459a32e52f9858c`
  built and inspected on `linux/arm64`
  (`sha256:6994c7f29e0a8f4795ebedf4787f0e65b6bb0e62f19cc35d3f3a1f362be48978`)
  and `linux/amd64`
  (`sha256:db5faec279e86983c0618b294d6fcb19fec3d1b2b250dc1957070cd3393da75e`).
  Both passed the helper under normal and optimized Python, exact lock/evidence
  inspection, and the expected non-green generic pip diagnostic; exact tags
  were deleted and confirmed absent.
- Audit-correction behavioral tests cover wrong signable body, prefix, length,
  expected signable Keccak, and an optimized subprocess with a deliberately
  wrong expected empty-input digest. The broad image-identity suite passed
  `94 passed` in 188.05 seconds.
- Fresh independent correction re-review accepted the complete Task 2 range
  `068d0f1fa8727e8c9b351cee92f6686b9f1ee611..558b8810df7b12a68320153d77c41629313fa5e5`
  as `READY_WITH_RISK` with high confidence and no remaining blocker finding.
- Task 3 reran the exact final implementation tree locally: image-identity
  `94 passed` in 57.12 seconds, lock tests `37 passed`, license tests `17
  passed`, both-target offline validation, helper execution under normal and
  optimized Python, actionlint, all changed-file hooks, and diff hygiene.
- The non-force remote guard found H28 still at expected prior head
  `8f507cef03e1dba21316cfc765f6c67f5d763938`; the reviewed implementation was
  pushed and read back at
  `558b8810df7b12a68320153d77c41629313fa5e5` on draft PR #28. The PR remains
  draft; no merge, ready-state change, image publication, or deployment was
  performed.
- No registry, final application image, deployment, service, credential,
  publication, legal, or trading state was changed. Publication remains
  `HOLD_FOR_EVIDENCE` and final-image evidence remains a later gate.
- Rollback: revert the Task 2 commits. The prior broad-solve Dockerfile is not
  an acceptable release path, so rollback restores source state but does not
  create release readiness.
