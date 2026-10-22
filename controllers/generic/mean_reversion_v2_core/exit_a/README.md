# EXIT-A v2 pure policy foundation

Implements the isolated v2 continuation overlay for rsibot PR #181. This is not
a live controller, compatibility profile, or economic result. No existing
configuration selects this package. Importing it starts no services.
`ExitAParametersV2.enabled` defaults to false.

## Interfaces

`load_model(raw: bytes, expected_sha256=...)` validates externally pinned, bounded
JSON. `predict_values(model, feature_names, values)` returns continuation value in
basis points plus upper/lower/no-touch cumulative incidence. `estimate(context,
model)` adds exact context/feature/model/time binding. Invalid arithmetic or
unavailable model timing produces an unavailable estimate.

`evaluate_exit_a(context, estimate, parameters, previous=..., prerequisites=...)`
returns a canonical cursor/trace and optionally a canonical `ExitProposal`.
Mandatory prerequisite names are `v2_recipe`, `native_inventory`, and
`native_protection`; PASS needs source evidence. These are supplied owner evidence,
not self-issued trading authority. Caller must separately bind the full actual
configuration; a recipe's behavioral digest excludes some controls.

The first valid observation seeds a cursor. Same-event revisions and replays do
not rearm it. Setup keys partition owner/executor/inventory revision/recipe. The
reduction-only decision scope has acquisition_allowed=false regardless of the
entry-pause flag, so pausing entries does not invalidate a reduction cursor.
Results always have `execution_authorized=false`.

`bind_exit_a_v2` seals `program_id`, `owner_id` and implementation files under
`mean_reversion_v2_core` plus canonical `@strategy_components/` closure. It
rejects `ok_rsi`, `rsi_v5`, `rsi_modular`, `okx_spot_dca` and `r11`. Recipe
identity is not a Research OS registration or a live image pin.

`admit` runs canonical `arbitrate_exit` and persists an `ExitAClaimV2` on native
Metadata without an order callback. A proposal `minimum_price=0` is not an order
price. The assessment limit comes from the native floor. `request_cancel` and
`recover_late_fill` keep `inventory_released=false`.

`dispatch` rechecks the admitted claim against the current `NativeExitView`
and atomically moves it to `DISPATCH_PENDING` before invoking a caller `place`
callback. A repeated call, including through a reloaded store, cannot place
the same claim again. A changed receipt or
`native_close_pending` cancels the claim. Dust, incomplete fees and floor
quantity mismatch hold. Uncertain `place` failures retain the pending claim
for native order/fill reconciliation; cancel and late-fill transitions preserve
inventory ownership. Older source cannot parse a pending claim, so rollback
after use requires state-compatible recovery rather than a blind code revert.
Peak price and cost basis are returned unchanged. Entry pause does not block
a reduction. This still does not import `native_composition` or start a
compiled `network_iterator` controller.

## Model artifact

Schema `exit_a_linear_v2` has exact keys: training_manifest_sha256, feature_names,
means, scales, horizon_bins, hazard_weights, payoff_weights, payoff_intercept,
label_cutoff_ms, trained_at_ms, valid_until_ms, and schema. The training manifest
must bind data, target basis/cost semantics, bin duration, features, code, folds,
parameters and numerical environment. It is independent of the future recipe
that pins these model bytes, avoiding a circular model/recipe digest.

At most 1 MiB, 16 nesting levels, 32 features and 64 bins are accepted. Duplicate
keys, nonfinite coefficients, wrong dimensions/order and zero scales reject.
Hazard rows use coefficients on `[1, standardized features..., (bin+1)/bins]` in
UPPER/LOWER/SURVIVE order. Payoff weights use standardized features followed by
`F_UPPER,F_LOWER,SURVIVE`; direct controls set hazard_weights=null and omit those
three weights. All timing values are UTC epoch milliseconds. Model availability
is a declared replay/owner contract, not proof of a historical training event.

## Verification

From an actual compatible native checkout, run:

```bash
python -m pytest -q test/mean_reversion_v2
```

2026-09-23 Hummingbot conda env (`Python 3.13.13`) with temporary compiled
native module links: the focused `test_exit_a_*.py` suites observed **74 passed**
after the dispatch-claim correction. The historical 71-test receipt predates it.
The compiled-loader smoke constructed the existing `rsi_modular` `ok_rsi`
profile; EXIT-A has no controller entrypoint and no owner startup was run.
Profile source seals are AST closures over `ok_rsi` /
`rsi_v5` / `rsi_modular` and exclude `mean_reversion_v2_core`. That is not a
live recorder recovery.

The new namespace is outside the two currently recursive shared source scans.
A static `modular_spot_core` text scan found no `mean_reversion_v2` imports.
That is not a substitute for full old/new effective-seal tests. Rollback removes
only these unused additive files. Never reset inventory, trailing state or a
native recorder.
