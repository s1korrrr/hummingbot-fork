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
