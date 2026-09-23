# Isolated NRE v2 engineering components

**Source-only research increment. Not a registered recipe/controller, fitted model,
execution adapter or economic result.** This package implements parts of P2/P3 in
rsibot PR #183. It is not imported by an existing controller or activation path.

## Ownership and isolation

Existing native `RecipeBinding`, `DecisionScope`, `ObservationRef`, `Opportunity`,
`Prerequisite`, `CandleSnapshot` and their validation are reused. This package does
not start feeds, services, executors, orders, recorder writes or model training.

The proposed `strategy_v2/components/recovery_path.py` placement was rejected:
that directory is recursively included in existing source seals. This opt-in
`controllers/generic/nre_v2_core` package sits outside it and `modular_spot_core`.
No shared component, legacy controller, compatibility profile, source scanner,
config/default, state key, dependency lock or deployment is edited. A future
neutral-library promotion requires a separately reviewed source-seal revision.

The isolated consumer is `controllers/generic/nre_v2.py`. It wraps `ModularSpotController`
and seals that file plus this package's `MODULES` set plus `modular_spot.py` and
`modular_spot_core`. `MODULES` is unchanged, so `ok_rsi`, `rsi_v5`, and `modular_spot`
source hashes still exclude this package.

## Implemented interfaces

- `recovery_path.start/step/view`: immutable descriptive reducer, version 2 uses
  exact integer milliseconds. Same supported arithmetic as the retained Quants
  prototype: frozen scale/trough, close-based peak, signed retention, bounded
  history, terminal failure/expiry, no rearm. Invalid inputs raise without mutation.
- `binding.PathContract` and `observe/evidence`: consume actual native confirmed
  OKX spot 1m snapshots, bind recipe/setup/source/initialization/ATR provenance,
  preserve nulls and missingness, and hash the immutable state chain. The first
  delivery requires the ATR parent to equal the exact seed snapshot.
  `scale_from_snapshot` computes Wilder ATR on confirmed completed rows and
  returns that parent plus implementation/initialization hashes.
- `admission.classify_role` / `acquisition_dispatch_allowed`: map acquire/reduce
  stages onto INITIAL_ENTRY, SCALE_IN, REDUCE or UNKNOWN. Only a PASS
  prerequisite may dispatch an applicable initial-entry gate.
- `admission.evaluate`: returns only a native prerequisite for an explicitly
  sourced INITIAL_ENTRY role. Native reductions and identified SCALE_IN/PYRAMID
  acquisition bypass this optional gate. Unknown acquisition roles hold. Native
  risk, event seeding, consumption, reservations and execution remain downstream.
- `source_manifest.verify_implementation`: explicit research preflight/completion
  read of the exact NRE Python module set, parent package initializers and existing
  canonical component closure. Missing, extra, nested or symlink source rejects.

All thresholds, horizons, model/scaler hashes and time/resource limits are explicit.
Tests use synthetic values, not optimized or recommended trading parameters.
`Prediction` is a validated input record, not a forecast producer or proof that a
model artifact was fitted. Its three bps terms are comparable units; uncertainty
of the mean is not tail protection. Gate PASS never authorizes an order.

## Clock and trust boundaries

A native snapshot's serialized decimal-second publication time must convert to
milliseconds exactly; submillisecond evidence is rejected, never truncated.
Observation availability includes later setup/scale knowledge. Heartbeats do not
refresh original feature expiry or confirmation. Revisions, reconnect epochs,
skipped observations and changed overlapping candle history require explicit
reconstruction, not new setup creation or replayed entry permission.

State objects are trusted in-memory products of the reducer. There is no untrusted
checkpoint deserializer, persistence adapter or exactly-once execution protocol.
Quants reconstruction must replay original source-bound observations and compare
with an independently retained final-state hash. Hashes bind data, not its truth:
upstream ownership, ATR computation, role attribution and model production remain
unimplemented integration requirements. Neither a caller-supplied hash nor an
in-memory fixture is an authenticated native receipt.

The optional gate does not touch other opportunity dependencies; each must retain
its own native readiness/admission checks. Entry pause, unavailable NRE and removal
from an entry screener must not suppress native inventory management/exits. Full
runtime routing of those behaviors is NOT TESTED by these pure contracts.

## Checks and remaining work

From the native checkout with its qualified environment:

```bash
python -m unittest discover -s test/controllers/generic -p 'test_nre_*.py' -v
```

The development source assembly passed 34 targeted tests on Python 3.13.5/Linux,
using seven byte-verified canonical dependency files, pydantic 2.13.4 and pandas
2.2.3. This is not a full installed native-engine/loader or all-legacy suite.
Quants adds independent prototype arithmetic comparison, source-pinned replay,
reconstruction and causal target-selection tests. Exact commands and receipts
belong in the root implementation report; later source changes need fresh checks.

A full installed source manifest differs from a partial assembly. Obtain its
external expected digest from the admitted source package; never use an assembly
hash as a production lock. Unverified model/state/owner inputs remain unavailable.

Pending: full installed-loader and all-legacy suites, venue-fitted model on the
cached May-August corpus, registered v2 program, independent audits, and
explicit execution authority. No root gitlinks are changed. Rollback removes
this unused package, `controllers/generic/nre_v2.py`, and their tests; no service
restart or database reset is required.
