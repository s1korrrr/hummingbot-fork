# H28 Task 1 Report: Dependency Locks

## Status

**Complete as source-lock evidence; publication remains `HOLD_FOR_EVIDENCE`.**
The final combined resolution is compatible for both targets; Task 2 must still
prove target-image runtime behavior.
No Dockerfile, image, service, deployment, publication, or push was changed.

## Implemented source-lock baseline

- `scripts/generate_release_dependency_locks.py` generates cache-isolated
  target Conda locks from complete LINK plans and target-specific, hash-pinned
  Injective closures with `uv 0.11.29`.
- `scripts/validate_release_dependency_locks.py` validates both Python
  `3.13.15` Conda locks, source fingerprints/YAML pip nesting, canonical Conda
  coverage, exact six-file inventory, artifact hashes, direct inputs, target
  mappings, and the approved `pysha3>=1.0.2 -> safe-pysha3==1.0.4` exception.
- The committed pip artifacts are
  `pip-injective-linux-64.lock.txt`,
  `pip-injective-linux-aarch64.lock.txt`, and
  `pip-eip712-direct.lock.txt`. They are standalone closures only, not proof
  that pip and Conda can coexist in one environment.

## Combined-environment feasibility evidence

The required bounded check built an exact constraint file from every normalized
package name shared by the `linux-64` Conda explicit lock and the current
Injective closure (48 overlaps), then ran exactly:

```text
uv 0.11.29 pip compile injective-py==1.13.0 safe-pysha3==1.0.4
  --constraint <exact-linux-64-overlap-map>
  --python-version 3.13 --python-platform x86_64-manylinux_2_17
  --generate-hashes --no-annotate --no-header --no-config
  --default-index https://pypi.org/simple
```

The command failed before any lock mutation:

```text
Because eip712>=0.3.0 depends on eth-utils>=2.3.1,<6 and web3==7.16.0
depends on eth-utils==6.0.0, we can conclude that eip712>=0.3.0 and
web3==7.16.0 are incompatible.
And because injective-py==1.13.0 depends on eip712>=0.3.0 ... and
web3==7.16.0, we can conclude that your requirements are unsatisfiable.
```

The exact Conda `eth-utils==6.0.0` overlap is incompatible with Injective's
required `eip712` range. Python 3.13 is not causal. This is sufficient to fail
the all-target gate; the aarch64 constrained resolve was deliberately not
started after the linux-64 fail-fast result.

The smallest evidence-backed revision candidate is to constrain the Conda
environment to `eth-utils<6` and perform a new cache-isolated two-target Conda
solve followed by both constrained UV resolutions. It is a candidate, not an
approved or proven solution: other exact overlaps (including `coincurve`,
`urllib3`, and `cffi`) still require the resulting combined solves to pass.
No source constraint was changed here.

## Test and validation evidence

- Previous source-lock baseline: focused validator/generator suite passed
  `26 passed in 0.26s`; offline validator passed for both targets; changed-file
  hooks, compilation, and diff checks passed.
- Current combined-environment gate: RED feasibility result above; no GREEN is
  possible without an explicit source-recipe revision and fresh lock solves.
- Resolver origin isolation, exact closure-map provenance, hostile ambient
  configuration tests, and zero-overlap validation were **not implemented**:
  the binding feasibility gate failed first, and the ruling requires stopping
  rather than strengthening artifacts that are known not to compose.

## Changed-files state

- Baseline commit: `743841513eea705e66de51a8860540c256dce091`
  (`build(release): complete target dependency locks`) contains the current
  standalone lock artifacts and validator/generator baseline.
- This re-review created only planning/report/memory status updates. It made no
  lock, generator, validator, test, Dockerfile, or source-recipe mutation and
  deliberately creates no follow-up implementation commit while the gate is
  red.

## Rollback and handoff

No external rollback is needed. Do not advance to Docker consumption. To
resume, approve a source-recipe revision addressing the exact `eth-utils<6`
conflict, regenerate both Conda targets in isolated temporary storage, run the
two constrained UV resolutions, and only then add the required origin,
closure-map, and overlap provenance/validator controls.
