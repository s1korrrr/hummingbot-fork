# TODO

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
- [x] Update owner-repo plans and workspace learning stores.
  DoD: verified facts, commands, blockers, rollback, and final gate are recorded without secrets.
- [-] Publish and merge the focused Hummingbot PR only if ready.
  DoD: intentional commits only; PR targets `master`, description matches the diff, CI/review are green, and the merge is verified on `origin/master`.
