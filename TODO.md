# TODO

- [-] Task 1: generate and validate complete target locks.
  DoD: both explicit Conda locks, exact pip hash lock, source binding, validator,
  generator, and adversarial tests are complete and independently reviewed.
- [ ] Task 2: consume locks in the release image.
  DoD: Docker uses only the selected validated locks; both target environment
  builds or exact blockers are recorded; docs and tests match runtime flow.
- [ ] Task 3: audit, close out, and push.
  DoD: all relevant local checks and reviews are recorded, durable memory is
  updated, and only reviewed commits are non-force pushed.
