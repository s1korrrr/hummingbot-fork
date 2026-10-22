# Momentum v2 B0/C1 — decision-only implementation

This implements the first bounded policy/feature slice of rsibot PR #199 P2.
It is **not a native ControllerBase adapter, admitted economic strategy or trading service**.
No existing controller or canonical component is modified. No entrypoint is registered.

## Files and contract

`controllers/generic/momentum_fusion_v2_core/recipe.py` defines strict B0/C1 recipes,
explicit feature/shelf parameters, three dataset bindings, initialization and external
loss/exit/sizing/capital policy hashes. It reuses `RecipeBinding` and checks all four
identities, including recipe ID/hash (beyond the canonical helper's source/config checks).
The caller must establish the full source closure; a supplied hash is not an attestation.
`assert_single_treatment` rejects a comparison with any difference except B0 versus C1.
Unknown variants, legacy profiles, execution enablement and insufficient history reject.

`features.py` registers three actual `FeatureSpec` functions in the existing
`FeatureRegistry`. It computes prior-channel high, prior simple ATR, prior median-relative
volume, one-bar return and closed 5m/15m moving-average trend. References exclude the
candidate bar. Zero/unavailable reference ATR or volume rejects rather than becoming a
fabricated value. This is a versioned minimal feature set, not all v1 features or a claim
that its SMA/ATR conventions equal `component_breakout` or an optimized strategy.

`setups.py` is a deterministic reducer:

```python
state, decision = step(recipe, previous_state, observed_features,
                       scope=scope, now_ms=now_ms, prerequisites=checks)
```

The caller supplies all clocks, immutable evidence and current prerequisite observations.
State is explicit and serializable; no balances, fills, orders or private database are owned
here. `PolicyDecision.trace` is the canonical `DecisionTrace` and always has
`execution_authorized=false`. Even `ELIGIBLE_FOR_NATIVE_ADMISSION` creates no order and
reserves no capital. Missing owner/risk/capital prerequisites are UNAVAILABLE; optimistic
alpha cannot override them. A prerequisite named `setup` cannot be injected by a caller.

## Exact setup behavior

B0 requires a completed close above the prior high plus its explicit ATR buffer,
positive return over the configured threshold, sufficient relative volume, and positive
closed higher-timeframe trends. First observation is always a seed HOLD.

C1 starts from the same qualifying event but arms an immutable impulse range and expiry.
It requires the declared number of later compressed shelf bars, a shallow retracement and
renewed breakout over the frozen impulse/shelf reference. A premature continuation, failed
structure, excessive extension from candle-HLC3 anchored VWAP, excessive holding bars or
expiry invalidates it. The anchored VWAP is a **bar approximation**, not trade-level VWAP.
Failed/expired candidates need a new observed reset below the channel before rearming.

Each scope emits at most one eligible opportunity. `issued=true` then remains latched:
there is deliberately **no native settlement/re-entry implementation** in this slice.
A future admitted owner must define reconciled episode turnover; deleting state to bypass
the latch is forbidden. Blocked/not-qualified bars still advance the observation cursor,
so position/entry gating does not itself manufacture a data gap. Genuine stream gaps,
epoch changes and conflicting revisions hold and clear the candidate. Heartbeats cannot
extend its expiry or discard it merely by updating publication time. Shadow and baseline
use different scopes, and cross-scope state reuse is rejected.

JSON restoration validates recipe, feature contract, cursor, bar count and fixed shelf
clocks. It is a **pure state round-trip**, not crash-safe persistence, authenticated state
or proof of native executor restoration. No native inventory is adopted.

## Dataset support and remaining boundaries

The adapter consumes **three already-confirmed snapshots** on the exact recipe's native
OKX 1m/5m/15m datasets. It rejects gaps, malformed/unconfirmed copies, wrong scope/history,
future/old observations, wrong latest higher-timeframe bucket and lossy millisecond
conversion. Feature callables are stateless; setup state is outside the registry.

It does not implement candle collection or derive higher-timeframe candles. Production
must use the existing native aggregation/provider contract and prove minute-constituent
lineage. Test-only aggregate fixtures are not a qualified native aggregation adapter.
Cross-snapshot correction detection and source transport/reconciliation need the actual
native reader integration. Missing/proxy historical data is not relabeled as OKX evidence.

`component_breakout.py` is an inspected existing paper-only, exact-configuration reference,
not a generic recipe host. It is unchanged. This package does not inherit it or RSI profiles.
No files are added under `hummingbot.strategy_v2.components`, preserving its source tree.
Actual installed legacy suites/source seals still require full-owner qualification.

Native admission, capital locks, executor mapping/fees/TTL, exits, inventory/recovery,
Research OS program/result admission, independent audits and economics remain outstanding.
Canonical recorder reservations are not account-wide locks. External loss-policy hashes
are required provenance references, not proof that loss-taking is admitted. Existing
profit-only RSI/SUI policies remain unchanged.

## Verification and use

From a complete owning checkout with its installed dependencies:

```bash
PYTHONPATH=. python -m pytest -q -ra test/momentum_fusion_v2
python -m compileall -q controllers/generic/momentum_fusion_v2_core test/momentum_fusion_v2
```

The implementation was tested in a Linux partial source reconstruction using **unmodified,
Git-blob-verified canonical modules and their real package parents**, not fabricated native
classes. Python 3.13.5, pandas 2.2.3, NumPy 2.3.5, Pydantic 2.13.4, pytest 9.0.2.
71 component tests passed. The compiled owner runtime, full repository tests/lint,
operator state/cache, independent audit and financial backtest were unavailable/not run.
The consistent synthetic tape test demonstrates B0 at the initial break versus C1 after
two shelf bars, one opportunity per scope; it has no fills or economic return.

All numerical defaults are unoptimized engineering choices. They must be frozen in an
independently audited preregistration before any economic comparison on the required cache.
No operating configuration or source pin is changed. Rollback removes only these new policy,
test and documentation paths; no runtime restart or database manipulation is needed.
