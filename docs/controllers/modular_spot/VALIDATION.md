# Validation receipt — isolated modular spot candidate

Date: 2026-09-18. Author validation, not an independent audit.
Native source baseline inspected through the authenticated GitHub connector:
`s1korrrr/hummingbot@4df9ff3158a405b71d28b1c07446cfc7864a19fa`.
The local workspace contains only the additive candidate files, not a full compiled
Hummingbot checkout. Tests used Python 3.13, Pydantic 2.13.4, pandas, numpy,
SQLAlchemy and pytest. No exchange/account credentials or running state were used.

## Executed

`PYTHONPATH=. python -m pytest -q test/modular_spot`

Result: **61 passed, 1 native test module skipped**. The skip reports that the
native Hummingbot package/compiled environment is unavailable. It is not native
executor validation. New tests include real temporary SQLite transactions, not
an in-memory imitation of compare-and-swap.

`python -m compileall -q controllers/generic/modular_spot.py controllers/generic/modular_spot_core test/modular_spot`

Result: passed. This proves Python compilation, not native import/runtime success.

`git diff --cached --check`

Result: passed for the additive candidate. Only the new entry point, its private
core, scoped tests and scoped documentation are staged.

Observed red-to-green cycles covered initially missing pure modules, followed by
regressions for late inventory after campaign close, an unfilled DCA stage being
used as a fill reference, and premature claim release. Structural checks prohibit
legacy-controller imports, global executor-map access and direct buy/sell/order
submission in the new controller code; native order event callbacks remain inherited.

The author performed a second source review of dispatch/claim identity, risk latches,
partial-fill references, native factory selection, fees/floors, unknown state and
restart behavior. This is explicitly not a separate reviewer or independent audit.

## Not executed / required before merge or promotion

- Native Hummingbot imports and the native integration test module.
- Existing native PositionExecutor and ExecutorOrchestrator regression suites.
- Repository flake8/isort/pre-commit hooks (not installed in this sandbox).
- Full native partial-fill, fee-correction, cancel/late-fill, factory admission and
  restart/held-position scenarios with real native classes and recorder.
- Independent review, CI completion, native image build, Condor/API schema discovery,
  deployed profile correctness, account/market/report freshness and alert delivery.
- Research OS intake, authorized backtests, statistical/economic result audits,
  paper/canary/live operation or any capital authorization.

The current native planned-position-resume path bypasses the controller-local
executor selector. This controller rejects a pending resume manifest for its ID;
no shared native implementation was altered to make it appear supported.

## Publication contract

Publish a new draft PR against `master`, based on the exact inspected commit.
Verify the remote diff contains additions only under these paths:

- `controllers/generic/modular_spot.py`
- `controllers/generic/modular_spot_core/`
- `test/modular_spot/`
- `docs/controllers/modular_spot/`

Compare the remote blob identities with the locally tested UTF-8 file bytes.
The final PR head is the publication receipt; this document does not manufacture
a future commit SHA. No existing-controller source, shared-engine source, running
configuration, native database, root gitlink, deployment or other PR is modified.

Merge verdict: **HOLD for native validation and independent review**.
Activation verdict: **not authorized**. Keep `execution_enabled=false`.
