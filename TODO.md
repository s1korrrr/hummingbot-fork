# TODO

## OK RSI positional runtime-constructor regression

- [x] Reproduce the exact `StrategyV2Base` positional constructor failure.
  DoD: a focused regression fails with the observed duplicate `config` error before production code changes.
- [x] Repair positional forwarding without changing controller semantics.
  DoD: positional and existing keyword construction both initialize successfully.
- [x] Run focused runtime-path validation and close out evidence.
  DoD: OK RSI tests, compile/lint, diff checks, and plan/memory notes are current; no service or order action occurs.

## OK RSI end-to-end controller audit and merge readiness

- [x] Establish exact ownership, branch/base, documentation, and authority boundaries.
  DoD: intentional files, ignored runtime state, source-of-truth classes, official docs, forbidden live surfaces, and merge target are explicit in `PLAN.md`.
- [x] Trace controller behavior end to end against local runtime contracts.
  DoD: config through market data, signals, actions, executors, inventory/recovery, status/logging, loader, and shutdown paths are mapped with concrete invariants.
- [x] Reproduce suspected defects and close test gaps.
  DoD: every repair has a failing behavior test or direct failure signal; clean-checkout portability and warnings are tested beyond the original suite.
- [x] Repair confirmed issues in the smallest safe slice.
  DoD: no silent executor fallback, hidden config dependency, unsafe balance/exposure behavior, known contract violation, or per-feed rate-limit burst remains.
- [x] Run safe integration and startup/log validation.
  DoD: real loader/config/controller lifecycle and a public-data-only 15-feed process complete without credentials or orders; capital-capable restarts are excluded.
- [x] Run final correctness, risk, execution, maintainability, security, and performance review.
  DoD: official-doc/local-source checklist, risk rails, execution assumptions, tests, compilation, hooks, warning gate, diff/status audit, and findings are resolved or block shipping.
- [x] Repair and verify the owner-repository full-suite entry point.
  DoD: `make test` uses the active Hummingbot interpreter, isolates duplicate test module names, and its pre-existing Vertex/shutdown blockers are reproduced and recorded separately from controller results.
- [x] Update owner-repo plans and workspace learning stores.
  DoD: verified facts, commands, blockers, rollback, and final gate are recorded without secrets.
- [x] Publish the focused Hummingbot PR and obtain explicit merge authorization.
  DoD: intentional commits only; PR #29 targets `master`, its description matches the diff, the operator explicitly accepted the documented CI/full-suite risks on 2026-08-28, and the final scoped warning-clean gate passed 325 tests plus 22 subtests immediately before merge. The resulting merge SHA is verified during root gitlink integration.
