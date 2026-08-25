# H28 Task 1 Report: Dependency Locks

## Status

Independently accepted `READY_WITH_RISK` as corrected `repo-ready` source-lock
evidence at `7fe315f6d8cdf32ef634eb58ab9f4d5814dc4ad9`.
Publication remains `HOLD_FOR_EVIDENCE`; no Dockerfile, image, deployment, or push changed.

## Final implementation

- Cache-isolated complete LINK plans project to matching FETCH URL/SHA records.
- `eth-utils>=5,<6`, `eth-typing>=5,<6`, and `hexbytes>=1,<2` yield 367
  Conda records, 59 pip packages, and 49 equal normalized overlaps per target.
- Provenance binds exact closure/overlap maps, UV 0.11.29, artifact hashes,
  source hashes, direct inputs, pysha3 substitution, and isolated PyPI origin.
- Generation strips ambient `UV_*`/`PIP_*`, uses pip `--isolated`, UV
  `--no-config`, and `https://pypi.org/simple`.

## Final validation

- Focused suite: `37 passed`.
- Offline validator passed for both targets and Python 3.13.15.
- Adversaries cover LINK/FETCH identity, hostile origin, exact closure maps,
  missing/extra/version drift, and zero-overlap conflicts.

## Changed files and commits

Commits: `7aef9e139`, `743841513`, `815d86f6f`, `43da388cb`,
`e2889d299`, `b2d4a60b7`, `4c4dca783`, `2e555bd61`, `f0a847c60`, and
`ca031427b`, `69e5dc3da`, `12f020ade`, `ef15e4819`, and `ae3f06bd2`; this
closeout docs/format commit follows them.
Artifacts are two explicit locks, two Injective closures,
`pip-eip712-direct.lock.txt`, and provenance.

## Residual risk and Task 2 handoff

Task 2 may proceed. It must consume locks without solves and prove target
runtime imports/EIP-712 behavior on both architectures.
Generic `pip check` remains intentionally non-clean for the approved pysha3
metadata substitution.

Exact Python 3.13.15 regeneration byte-matched all six artifacts. Origin and
PEP 503 alias adversaries are covered.

## Rollback

Revert the Task 1 commits; no external cleanup is required.
