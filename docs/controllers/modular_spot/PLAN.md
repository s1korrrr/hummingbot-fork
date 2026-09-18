# Isolated modular spot controller

## Scope and authority

Implement the user's revised 2026-09-18 scope in s1korrrr/hummingbot, based on
4df9ff3158a405b71d28b1c07446cfc7864a19fa. Repository implementation and a draft PR
are authorized. Deployment, activation, account access, economic backtests,
existing-controller edits, shared-engine edits and rsibot gitlink changes are not.

This supersedes the earlier proposal to refactor OK RSI. The new entry point is
`controllers/generic/modular_spot.py`, directly extending native ControllerBase.
Its components live exclusively in `controllers/generic/modular_spot_core/`.
No existing controller imports them. No compatibility re-exports or global
executor mappings are changed. Native reflection must discover the config and
controller from the entry-point module.

## Design

Use native candles/market-data, market-analysis context, ExecutorActions,
PositionExecutor, per-controller executor-class hook, recorder and Metadata.
Native orders, fills, held positions and performance remain authoritative.
Only decision/campaign/risk state is stored in a controller-namespaced Metadata
record with compare-and-swap. Persist admission before emission; missing receipts
remain unresolved, never implicitly retried. No new table, service or ledger.

Compose explicit signal, acquisition, portfolio-budget, risk, inventory and
trailing/profit-floor policies. DCA tranches are separate native positions with
per-tranche profit protection, not a synthetic basket-average position. No new
acquisition after distribution begins. Retained inventory blocks new campaigns;
there is no automatic adoption, basis reset or forced loss sale.

Observation is the default. The optional dispatch path requires a native recorder,
valid native accounting, current inputs and bounded preallocated capital. This is
not an account-wide allocator for existing bots. No cross-process account ownership
or deployment admission is established by this PR.

## Work and evidence

1. Write failing pure-policy and boundary tests, then implement the components.
2. Add native ControllerBase integration and a local PositionExecutor specialization
   through the existing hook; keep shared order lifecycle methods inherited.
3. Test real SQLite CAS, state corruption, replay, risk latches, DCA limits, missing
   execution evidence, candle causality and source isolation.
4. Add native integration tests and disabled example configuration. Report native
   dependencies unavailable in this sandbox rather than substituting test doubles.
5. Verify exact remote changed-file scope and Git blob identities; create draft PR.

Run: `python -m pytest -q test/modular_spot` and
`python -m compileall -q controllers/generic/modular_spot.py controllers/generic/modular_spot_core test/modular_spot`.
Native owner tests, independent review, image validation and any activation remain
separate gates. Completion evidence is recorded in VALIDATION.md.

## Rollback

Before activation, close/revert this additive PR; existing controllers and state
are unchanged. After any separately authorized use, do not delete decision records
or replace an execution owner. Stop/reconcile using native procedures, preserve
native fills and held inventory, then remove only the new controller's deployment.
