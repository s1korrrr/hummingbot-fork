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
- [-] Task 2: consume locks in the release image.
  DoD: Docker uses only the selected validated locks; both target environment
  builds or exact blockers are recorded; docs and tests match runtime flow.
- [ ] Task 3: audit, close out, and push.
  DoD: all relevant local checks and reviews are recorded, durable memory is
  updated, and only reviewed commits are non-force pushed.
