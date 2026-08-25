# MEMORY

## Repo overview

- `hummingbot` owns engine/runtime, connectors, executors, and direct strategy
  execution behavior.

## Commands

- Focused image-identity tests: `conda run -n hummingbot python -m pytest -q test/test_docker_image_identity.py`.
- Enforced Compose deployments run through `make deploy` and require
  `HUMMINGBOT_IMAGE`; use
  `hummingbot/hummingbot@sha256:<64-lowercase-hex>` for a published digest or
  Docker's exact `sha256:<64-lowercase-hex>` local image ID.
- Workflow syntax: `actionlint .github/workflows/docker_buildx_workflow.yml`.
- Changed-file hooks: `conda run -n hummingbot pre-commit run --files <paths>`.
- Offline dependency lock verification: `python scripts/validate_release_dependency_locks.py`.
- Build a disposable locked dependency stage with
  `docker buildx build --platform <linux/amd64|linux/arm64> --target builder-environment --build-arg SOURCE_COMMIT=<sha> --build-arg COMMIT=<sha> --load --tag <exact-test-tag> .`.
- Regenerate release locks only as an explicit reviewed operation in temporary
  storage: `python scripts/generate_release_dependency_locks.py --python "$(which python)"`.
  The generator refuses to overwrite `setup/locks/`; remove or replace prior
  locks only through a deliberate reviewed change.

## Architecture and safety

- Image source/revision identity is distinct from registry publication,
  runtime deployment, and trading authority.
- No local test, PR push, or source review authorizes service startup,
  credentials, orders, paper, canary, or live trading.
- Release lock inputs are `setup/environment.yml` and `setup/pip_packages.txt`.
  `setup/locks/provenance.json` fingerprints those sources and all committed
  lock artifacts; validation must pass before Docker consumes either target.
- `setup/locks/linux-64.explicit.txt` and
  `setup/locks/linux-aarch64.explicit.txt` are native Conda `@EXPLICIT` locks
  for Python `3.13.15`. They use only `https://conda.anaconda.org/conda-forge/`
  URLs in the target subdir or `noarch` with lowercase SHA-256 fragments.
- The six current artifacts are two explicit Conda locks, two 59-package
  target Injective closures, `pip-eip712-direct.lock.txt`, and provenance.
  Both targets have 367 Conda entries and 49 version-equal Conda/pip overlaps.
  Generation strips ambient `UV_*`/`PIP_*`, uses UV `--no-config` and pip
  `--isolated`, and pins `https://pypi.org/simple`.
- `lock-validation` inherits directly from `identity-check`, copies and checks
  every canonical source/lock/provenance artifact, and rejects unsupported or
  empty targets before its `builder-bootstrap` descendant runs apt.
  `builder-environment` then maps `linux/amd64` to the `linux-64`
  Conda/Injective locks and `linux/arm64` to the `linux-aarch64` locks before
  exact `conda create --prefix ... --file ... --yes` and hash-required,
  no-deps, no-build-isolation pip installs for the two distinct reports.
- The builder keeps selection and installed-set evidence in
  `/opt/conda/image-evidence`, explicit package URLs and Python version in
  `/opt/conda/conda-meta`, and runs
  `scripts/verify_release_eip712_runtime.py` after the direct EIP install.
  Generic `pip check` is intentionally non-green because upstream metadata
  still names `pysha3`; the deterministic EIP-712/Keccak smoke is the named
  `safe-pysha3` substitution gate.
- Release smoke checks must use explicit failures rather than Python `assert`,
  which disappears under `python -O`. The EIP-712 gate binds the complete
  signable vector, its Keccak digest, and empty-input Keccak; validate the helper
  in both normal and optimized modes.

## Known pitfalls

- The release workflow publishes digest-only output. `make deploy` validates
  the process-environment image before Docker. Direct `docker compose` only
  checks that the value is non-empty and is not the enforced boundary.
- `.compose.env` must contain exactly one allowlisted `COMPOSE_PROFILES=` or
  `COMPOSE_PROFILES=gateway` byte sequence, including the final newline. `cmp`
  checks its exact bytes before selecting an explicit `--profile`; it is never
  sourced or passed through as general Compose input.
- `make deploy` selects `docker-compose.yml` explicitly and clears ambient
  `COMPOSE_FILE`/`COMPOSE_PROFILES`/`COMPOSE_ENV_FILES`, then disables default
  `.env` loading. Deploy and shutdown pin the Compose project to `hummingbot`.
  `make down` uses the same routing isolation plus a parse-only all-zero local
  image ID so shutdown does not depend on a deployment identity.
- Gateway remains a separate `hummingbot/gateway:latest` service; enabling that
  profile does not provide immutable Gateway runtime identity.
- Pinned Miniforge base `26.5.3-0` has `ruamel.yaml` and Conda's
  `VersionSpec`, but not the `yaml` import used by the offline validator. The
  Docker build supplies a narrow build-only `yaml.safe_load`/`YAMLError` shim
  from `ruamel.yaml`; do not solve or pip-install validator dependencies.
- `--no-deps` does not disable PEP 517 build isolation. Keep
  `--no-build-isolation` on locked pip installs so the EIP-712 sdist cannot
  fetch unhashed build dependencies outside the accepted lock surface.
