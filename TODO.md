# TODO

- [x] Add RED immutable-deployment contract tests.
  DoD: missing/mutable/malformed values fail for the intended reason; exact
  registry digests and local image IDs describe the accepted behavior.
- [x] Implement the minimum Compose and Make validation contract.
  DoD: `make deploy` cannot invoke Docker without a validated immutable
  identity, an explicit Compose file, and an allowlisted declarative
  `.compose.env`; `make down` remains image-independent.
- [x] Update operator documentation.
  DoD: published-digest and exact-local-ID flows are clear; Gateway's separate
  mutable-image risk is explicit.
- [x] Run repository validation and fresh review.
  DoD: focused tests, Compose config, hooks, actionlint, diff checks, and an
  independent verdict cover the exact committed range.
- [x] Update `MEMORY.md` and close out.
  DoD: durable command/boundary notes and exact evidence are recorded.
