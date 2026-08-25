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

## Architecture and safety

- Image source/revision identity is distinct from registry publication,
  runtime deployment, and trading authority.
- No local test, PR push, or source review authorizes service startup,
  credentials, orders, paper, canary, or live trading.

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
