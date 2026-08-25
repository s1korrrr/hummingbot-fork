# TODO

- [x] Confirm the parity root cause and exact file/module scope
  DoD: `PLAN.md` records the provenance, economics, terminal-outcome, and authority defects.

- [x] Add RED regression tests
  DoD: focused tests fail for artifact-byte substitution, attribution-only drift, identity drift, and zero/negative NAV evidence before production changes.

- [x] Implement the smallest strict v2 parity boundary
  DoD: actual artifact bytes and complete episode economics determine `MATCH`/`MISMATCH`; terminal failure is explicit.

- [x] Preserve the authority ceiling
  DoD: unit and CLI evidence show every authority field and execution authority remain false.

- [x] Run focused and realistic validation
  DoD: focused pytest, compileall, pre-commit, exact tamper probes, `git diff --check`, and clean intended status pass.

- [x] Update documentation and durable memory if warranted
  DoD: PR-local report reflects v2 behavior; `MEMORY.md` changes only for an evergreen fact.

  Result: PR-local report updated. No repo `MEMORY.md` change: the new wire contract is branch-specific and recorded in the report rather than promoted as durable workspace fact before review.

- [x] Final close-out
  DoD: `PLAN.md` final notes and this checklist reflect exact results; local Conventional Commit(s) exist; nothing is pushed.
