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
