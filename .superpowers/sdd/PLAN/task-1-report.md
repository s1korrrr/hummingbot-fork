# H28 Task 1 Report: Complete Target Dependency Locks

## Status

Complete and committed locally. This slice establishes reproducible source
inputs for the two release targets. It does not alter `Dockerfile`, build an
image, publish an artifact, start a service, or authorize trading.

## Implementation

- Added `scripts/generate_release_dependency_locks.py`. It solves each allowed
  target with the existing Conda executable in `TemporaryDirectory` storage,
  emits native `@EXPLICIT` locks, downloads only the two direct pip artifacts,
  writes source/artifact SHA-256 provenance, validates the staged result, and
  refuses to overwrite an existing lock directory.
- Added `scripts/validate_release_dependency_locks.py`. It performs no network
  access and rejects unsupported target coverage, missing locks, non-HTTPS or
  non-`conda-forge` URLs, wrong subdirs, malformed/absent hashes, a Python
  selection other than `3.13.15`, source drift, pip version/hash drift, and
  valid-looking artifact tampering.
- Added `setup/locks/linux-64.explicit.txt` and
  `setup/locks/linux-aarch64.explicit.txt`: 273 Conda package entries each,
  plus headers. Both select Python `3.13.15` and contain only target/noarch
  Conda Forge HTTPS URLs with exact lowercase SHA-256 fragments.
- Added `setup/locks/pip-direct.lock.txt` with only
  `injective-py==1.13.0` and `eip712-structs==1.1.0`, each hash pinned, and
  `setup/locks/provenance.json` binding the source recipes and all three locks.
- Pinned the matching direct developer-recipe requirements in
  `setup/environment.yml` and `setup/pip_packages.txt`.

## TDD Evidence

1. RED: before implementation,
   `python -m pytest -q test/test_release_dependency_locks.py` failed during
   collection because `scripts.validate_release_dependency_locks` did not
   exist.
2. RED: `test_conda_specs_from_environment_excludes_channels_before_dependencies`
   failed with the previous result `['conda-forge', 'nodefaults',
   'python==3.13.15']`, proving channels leaked into solve specs. The parser
   now enters only the `dependencies:` section.
3. RED: `test_source_recipe_direct_pip_requirements_are_exactly_pinned` failed
   because the validator compared an indented YAML item without normalizing
   whitespace. It now normalizes source lines before its exact pin check.
4. GREEN: `python -m pytest -q test/test_release_dependency_locks.py` reported
   `15 passed in 0.14s`.

## Validation

- `python scripts/validate_release_dependency_locks.py`
  - passed: `validated targets: linux-64, linux-aarch64; Python 3.13.15`.
- `python -m pip download --require-hashes --no-deps --dest <temporary-dir> -r setup/locks/pip-direct.lock.txt`
  - passed: downloaded only `injective-py-1.13.0-py3-none-any.whl` and
    `eip712-structs-1.1.0.tar.gz`.
- `python -m pytest -q test/test_release_dependency_locks.py`
  - passed: 15 focused validator/generator tests.
- Post-commit repeat of the offline validator and focused test suite:
  - passed: `validated targets: linux-64, linux-aarch64; Python 3.13.15` and
    `15 passed in 0.20s`.
- `python -m pre_commit run --files <all Task 1 files>`
  - passed after one formatting correction; trailing whitespace, EOF, YAML,
    large-file, Flake8, private-key, autopep8, and isort checks were green.
- `python -m py_compile scripts/validate_release_dependency_locks.py scripts/generate_release_dependency_locks.py`
  - passed.
- `git diff --check`
  - passed.
- `python -m pytest -q test/test_docker_image_identity.py`
  - executed once as an existing focused regression. Its direct terminal
    session detached after 31 progress dots; afterward `.pytest_cache` had an
    empty `lastfailed` object. Treat this as supporting local evidence, not a
    replacement for Task 2 image proof.

## Changed Files

- `scripts/generate_release_dependency_locks.py`
- `scripts/validate_release_dependency_locks.py`
- `setup/locks/linux-64.explicit.txt`
- `setup/locks/linux-aarch64.explicit.txt`
- `setup/locks/pip-direct.lock.txt`
- `setup/locks/provenance.json`
- `setup/environment.yml`
- `setup/pip_packages.txt`
- `test/test_release_dependency_locks.py`
- `PLAN.md`, `TODO.md`, and `MEMORY.md`

## Self-Review

- No Dockerfile, Miniforge digest, deployment, license, or workflow file was
  modified.
- The validator checks semantics before artifact fingerprints so adversarial
  fixtures fail at the intended rule; content tampering that remains syntactic
  fails its artifact fingerprint rule.
- The generator uses existing Conda/pip executables only and creates no global
  Conda environment. It has no fallback solve or target fallback.
- The committed pip download smoke confirms that the exact hashes are usable
  with `--require-hashes --no-deps`.

## Concerns and Handoff

- These are reproducible dependency source inputs, not per-platform Docker,
  compiled-extension, runtime, legal, publication, or deployment evidence.
- The explicit Conda locks reference upstream artifacts; Task 2 must consume
  them without a solve and demonstrate the target-specific builder behavior.
- Publication remains `HOLD_FOR_EVIDENCE`. Roll back by reverting this Task 1
  commit; no external cleanup is required.

## Review Correction: NOT_READY

The review found that the original slice cannot be promoted to Task 2. The
committed locks must be treated as invalid, not as release inputs.

### Cache and source-validation findings

- The generator reads Conda `actions.FETCH`; that list can omit package records
  satisfied by a pre-existing package cache. The review correctly observed
  omitted canonical packages including `pytest-asyncio`, `setuptools`, and
  `requests`. A focused RED test reproduced the defect with `FETCH` containing
  only Python while `LINK` also contained Requests.
- The original source-pin check scanned text rather than the YAML
  `dependencies -> pip` list. RED tests proved it accepted both a misplaced
  top-level `injective-py` pin and conflicting nested pins. Another RED test
  proved an extra `osx-arm64.explicit.txt` escaped inventory validation.
- The corrective implementation was deliberately not retained or committed:
  it would make the existing partial locks fail closed but could not produce a
  valid replacement while the pip-closure gate below remains unresolved.

### Bounded pip-closure feasibility evidence

- Method: in temporary storage, downloaded the two direct artifacts without
  installing them, inspected `injective_py-1.13.0` wheel `METADATA`, and
  inspected `eip712-structs-1.1.0/setup.py`. A separate pip dry-run reached
  the same direct edges but could not finish because the `coincurve` sdist
  metadata path errors under its current scikit-build-core configuration.
- `injective-py==1.13.0` requires: `aiohttp`, `bech32`, `bip32`, `ecdsa`,
  `eip712`, `eth-pydantic-types`, `grpcio`, `grpcio-tools`, `hdwallets`,
  `mnemonic`, `protobuf`, `requests`, `safe-pysha3`, `web3`, and `websockets`.
- Crucially, `eip712-structs==1.1.0` has an unconditional runtime dependency
  on `pysha3>=1.0.2` (alongside `eth-utils>=1.4.0`). Conda Forge's exact
  package API returned HTTP 404 for `pysha3`; `safe-pysha3` is a different
  normalized distribution and cannot meet the `pysha3` requirement. The gate
  is therefore infeasible for both `linux-64` and `linux-aarch64` before a
  complete recursive resolution or a new target solve is attempted.

### Required decision before resuming

Do not regenerate locks or widen the pip lock silently. The smallest
evidence-backed policy revision is one of:

1. authorize a complete, hash-pinned pip closure that includes `pysha3` and
   separately prove that its source build supports Python 3.13 on both targets;
   or
2. replace `eip712-structs==1.1.0` with an explicitly approved compatible
   dependency that has a fully Conda-satisfiable closure.

Until one is selected and validated, Task 1 is `NOT_READY`; publication stays
`HOLD_FOR_EVIDENCE`, and no Dockerfile or deployment work may consume the
current locks.

### Revised safe-pysha3 ruling result: still blocked

The revised ruling authorized exactly one cache-isolated two-target solve after
promoting every direct edge except `pysha3`. The generator used a temporary
`CONDA_PKGS_DIRS` and staged output directory; no current lock was replaced.
`linux-64` completed, but `linux-aarch64` failed closed with:

```text
eip712 >=0.3.0,<0.4.0
  -> eth-pydantic-types >=0.2.4,<0.3
  -> cchecksum >=0.0.3,<1, which does not exist (perhaps a missing channel)
```

This is a distinct target-specific closure blocker beyond the approved
`pysha3 -> safe-pysha3==1.0.4` runtime substitution. The promoted source
specifications and the uncommitted cache/YAML/inventory/manifest implementation
were reverted rather than committing a partial or falsely green lock set.
No further solve is authorized until an explicit ruling resolves the missing
`cchecksum` edge for `linux-aarch64` (for example, a reviewed replacement
dependency or separately authorized runtime substitution with target evidence).

### Superseding target-specific Injective closure ruling

Read-only feasibility is positive for the superseding model. The installed
`uv 0.11.29` compiled `injective-py==1.13.0` plus
`safe-pysha3==1.0.4` with `--python-version 3.13 --generate-hashes` for both
`x86_64-manylinux_2_17` and `aarch64-manylinux_2_17`. Each result contained
2,012 lock lines; the aarch64 result included both direct inputs. This proves
the chosen pip-closure model can resolve without promoting the incompatible
`eip712` Conda edge. It is feasibility evidence only: Task 1 remains pending
the required cache-isolated Conda lock correction, artifact replacement,
offline provenance validation, adversarial tests, and hash-enforced download
evidence.

### Correction completion: target closures and fail-closed provenance

- Replaced the obsolete combined `pip-direct.lock.txt` with
  `pip-injective-linux-64.lock.txt`, `pip-injective-linux-aarch64.lock.txt`,
  and `pip-eip712-direct.lock.txt`. The two Injective locks each contain the
  complete 58-package, 2,012-line Python 3.13 manylinux closure generated by
  `uv 0.11.29`, with every requirement version-pinned and every artifact
  SHA-256-pinned. The EIP-712 lock contains only
  `eip712-structs==1.1.0` and its exact hash.
- Regenerated cache-isolated Conda artifacts for both targets. Each explicit
  lock now has 367 package URLs selected from the complete `LINK` plan and
  every source Conda requirement is represented after canonical name
  normalization. `CONDA_PKGS_DIRS` is confined below the generation temporary
  directory, so a host cache cannot omit a linked package from the lock.
- The offline validator now parses the actual YAML `dependencies -> pip`
  section, requires exactly `injective-py==1.13.0` there, enforces the exact
  six-file lock-directory inventory, and binds UV version, platform mapping,
  direct Injective inputs, all artifact fingerprints, and the only approved
  substitution: `pysha3>=1.0.2 -> safe-pysha3==1.0.4`.
- RED/GREEN evidence for the final grammar defect: with the non-final UV hash
  continuation rejected, `test_pip_lock_accepts_uv_multiline_hash_continuations`
  raised `LockValidationError: pip hash must be an exact lowercase SHA-256`.
  After allowing a trailing continuation backslash while retaining the exact
  lowercase 64-hex grammar, that test passed. The final focused suite reported
  `26 passed in 0.32s`.

#### Final validation

- `python scripts/validate_release_dependency_locks.py`
  - passed: `validated targets: linux-64, linux-aarch64; Python 3.13.15`.
- `python -m pytest -q test/test_release_dependency_locks.py`
  - passed: `26 passed in 0.32s`; includes cache/FETCH omission, LINK-plan,
    canonical Conda coverage, YAML placement/conflict, inventory, generator,
    target, and named-exception adversaries.
- `python -m pre_commit run --files <Task 1 files>`
  - passed: whitespace, EOF, size, Flake8, private-key/wallet-key, autopep8,
    and isort hooks; YAML/ESLint were not applicable.
- `python -m py_compile scripts/validate_release_dependency_locks.py
  scripts/generate_release_dependency_locks.py` and `git diff --check`
  - passed.
- Hash-enforced host-pip attempt: `pip download --require-hashes --no-deps`
  began both Injective lock downloads but stopped at `coincurve==20.0.0` while
  evaluating its sdist metadata: `ERROR: Use build.verbose instead of
  cmake.verbose for scikit-build-core >= 0.10`. This is the pre-existing host
  macOS metadata-build failure, not a missing or mismatched lock hash. The
  EIP-712 direct lock download passed. As the approved host-pip fallback,
  exact `uv 0.11.29` regenerated both target closures in temporary storage and
  byte-compared them successfully with both committed locks. Generic `pip
  check` remains known non-clean because the approved pysha3 metadata edge is
  intentionally represented by `safe-pysha3`; Task 2 owns per-target runtime
  EIP-712 smoke.

#### Self-review and residual risk

- No Dockerfile or global environment/tool was modified; no image was built,
  published, deployed, or pushed. The old combined pip lock was retained in a
  recoverable task-temporary backup during replacement.
- The source lock validator is offline and fail-closed; the generator itself
  performs networked resolution only in temporary storage. Upstream artifact
  availability and per-target native build/runtime behavior remain outside
  this Task 1 source-proof slice. Publication remains `HOLD_FOR_EVIDENCE`.
