# TODO

- [x] Task 1: generate and validate complete target locks.
  DoD: both explicit Conda locks, exact pip hash lock, source binding, validator,
  generator, and adversarial tests are independently accepted `READY_WITH_RISK`
  as source-lock evidence. Docker consumption is deferred to Task 2.
- [x] Task 1 review correction: complete lock coverage and pip closure.
  DoD: cache-isolated solves lock all LINK records; source pip YAML and exact
  lock inventory fail closed; exact `uv 0.11.29` generates complete hashed
  Injective closures for both Python 3.13 manylinux targets; provenance binds
  the single named pysha3 substitution; the final independent review found no
  blocker findings in the exact Task 1 range.
- [x] Task 2.1: add focused static lock-consumption tests and run them red.
  DoD: tests fail for the missing target mapping, resolver-free install,
  evidence, and deterministic EIP-712 runtime contract before implementation.
- [x] Task 2.2: consume validated target locks in `builder-environment`.
  DoD: empty/unsupported targets fail closed; supported targets use only their
  exact Conda and Injective locks, plus the direct EIP-712 lock, without a
  dependency solve or fallback.
- [x] Task 2.3: preserve distinct dependency evidence and runtime proof.
  DoD: selected sources/locks/provenance, explicit URLs, Python version, Conda
  lists, and two pip reports remain auditable; deterministic EIP-712 and Keccak
  behavior passes after installation while generic `pip check` stays unclaimed.
- [x] Task 2.4: update the release evidence runbook and durable memory.
  DoD: docs distinguish source locks from target-image proof and retain
  `HOLD_FOR_EVIDENCE`; confirmed commands and boundaries are recorded.
- [x] Task 2.5: run focused, lock, license, compile, hook, and diff validation.
  DoD: exact commands and outputs are recorded; any failure is diagnosed rather
  than bypassed.
- [x] Task 2.6: build and inspect both disposable target environment images.
  DoD: `linux/arm64` and `linux/amd64` succeed with exact inspected evidence, or
  the precise platform/runtime blocker is recorded; exact temporary tags are
  removed.
- [x] Task 2.7: commit only owned intentional changes and hand off for review.
  DoD: coherent Conventional Commits leave a clean tracked worktree and the
  final report names commits, red evidence, validations, residual risks, and
  review readiness without pushing.
- [ ] Task 3: audit, close out, and push.
  DoD: all relevant local checks and reviews are recorded, durable memory is
  updated, and only reviewed commits are non-force pushed.
