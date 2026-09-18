# Modular desk protocol — implemented first slice

Date: 2026-09-18. Dependency: PR #55 at `808919077af8b0fa88736a0690a725def31fff44`.
Status: **partial implementation of the approved 22-block program; draft, not production-ready**.

The full cross-owner specification and living ExecPlan are in the companion rsibot branch
`feat/andrzej_modular_desk_protocol_20260918`:

- `docs/specs/modular-trading-desk-v1.md`
- `docs/plans/active/2026-09-18-modular-trading-desk.md`
- `docs/specs/modular-trading-desk-coverage.json`

## What is implemented

| Source under `controllers/generic/modular_spot_core/` | Actual behaviour |
|---|---|
| `recipe_contract.py` | Immutable reference recipes, compatibility checks, canonical identities and normalized configuration/source seals. |
| `recipe_compiler.py` | Reuses #55 `ProgramProfile`/`build_profile`; read-only inspection and exclusive disabled exports that require genuine native configuration validation. |
| `feature_registry.py` | Bounded dependency DAG over #55 `CandleSnapshot` and `FeatureCache`; explicit input/revision/code/initialization provenance, freshness on every hit, immutable results and learned-artifact timing checks. |
| `decision_protocol.py` | Distinct source-bound opportunities, prerequisites, cursor and traces; deterministic eligibility replay with explicit millisecond clocks, seed/duplicate/correction/expiry/rearm holds. |
| `capital_policy.py` | Read-only account/program/base/order assessments, fixed allocation targets and sizing caps that call the existing fee-reserved `portfolio.allocate`. |
| `exit_policy.py` | One quantity-bounded reduction proposal over the same native lot; complete native receipts and an exact-quantity native profit-floor observation required. No market fallback. |

`modular_spot.py` now accepts an optional `RecipeBinding`, verifies source/configuration identity in its native configuration validator and projects that identity through existing `processed_data`. Existing native admission, actions, persistence, executor factory, trailing and recovery refusals remain unchanged.

**Integration boundary:** the feature registry, eligibility replay, capital assessment and exit-arbitration libraries are not wired into the live action path or selectable as additional native recipe flags in this slice. They do not replace #55's planner, reservations, existing feature computation or native exits. Their native adoption and validation remain explicit ExecPlan work. Do not treat library tests as end-to-end program support.

## Three reference recipes

The files under `recipe-examples/` illustrate different compositions; instruments and amounts are not trading recommendations or evidence of account eligibility. The impulse example intentionally uses a tiny catalog, not a claim of all-symbol production capacity.

- `rebound.json`: selected pairs, RSI rebound and bounded per-tranche dip stages.
- `impulse.json`: shared observations, bounded breakout shortlist and one-shot acquisition.
- `sell-only.json`: no entry signal/acquisition; **native external inventory handoff remains unsupported**. This does not adopt or replace the existing SUI owner.

Inspect without importing the native execution package:

```bash
python -m controllers.generic.modular_spot_core.recipe_compiler inspect \
  docs/controllers/modular_spot/recipe-examples/rebound.json
```

Export requires the genuine owning Hummingbot environment, not substitutes for its native classes:

```bash
python -m controllers.generic.modular_spot_core.recipe_compiler export \
  docs/controllers/modular_spot/recipe-examples/rebound.json \
  --output /absolute/new/non-running/recipe-export
```

The output parent must exist; the output directory must not exist or use symlinks. Writes are exclusive, file-synced and marked `INCOMPLETE` until finished. Every controller is disabled, inventory adoption is empty/unsupported and the receipt includes payload hashes. `CONFIGURATION_PASSED` means configuration parsing only, not native executor validation. No CLI command starts, deploys or trades.

The source seal covers this controller entry point and all private Python source files, not the full native package/image or arbitrary transitive dependencies. Exact native image and full execution-input closure remain separate gates. Source additions invalidate existing seals; do not hot-reload open positions, discard checkpoints or edit a running configuration to evade this.

## Contracts and limitations

**Feature computation:** register inspectable, stateless Python functions taking a copied frame, immutable dependency results and explicit parameters. Closures/bound callables/default state are refused. This is provenance and isolation of supplied data, **not a Python sandbox or proof of purity**. Review global/transitive dependencies and bind full runtime/model inputs before native or economic use. Model/scaler hashes are declared provenance; the registry does not load, train or attest those artifacts. Full-window computation is supported; persisted incremental feature-state restoration is not added here.

**Decision clocks:** `*_ms` fields are explicit UTC epoch milliseconds supplied by the caller. No wall clock is read by pure policies. Opportunity lifetime is anchored to input availability, not repeated delivery. Corrections change evidence identity but cannot rearm the same event. The first observation seeds a cursor without eligibility. A replay report is an eligibility trace, not a fill simulator or economic backtest. This library cursor is not installed as a second native state owner.

**Capital:** snapshot marks include retained inventory. Reserved acquisition values contain remaining unfilled commitments and must not double-count filled inventory. All amounts are already in the declared native valuation currency, including cross-quote pairs; no FX or cost basis is synthesized. Same-base exposures aggregate across pairs. A caller's `complete` flag is not independent proof of actual account coverage. The native adapter must supply reconciled authoritative observations and atomically revalidate grants before execution. These functions reserve nothing and cannot solve concurrency across accounts/processes.

**Exits:** competing requests on one attributed native executor overlap and are combined using their maximum, not their sum. Existing sell commitments (including their fee obligations) are subtracted; new base-fee consumption is bounded. The native profit floor must be for exactly the resulting quantized quantity and matching native receipt. A different quantity requires native floor recalculation. Policies may tighten but never lower that floor. A gap, unknown fee/cost, dust, stale report or ownership conflict produces a hold. A valid limit proposal still does not guarantee a fill or bounded mark-to-market loss.

**Authority:** a seal, a policy pass, a healthy source or a successful test cannot authorize capital, inventory handoff, recovery, promotion or engine restart. This delivery changes no existing controllers, shared engine, deployment, account state, database, root component pin or live owner.

## Research handoff

The companion root `scripts/modular_recipe_handoff.py` reads a completed export and the exact source directory. It verifies export/config/source hashes without importing execution code, then emits an unregistered, disabled design intake with Research OS-compatible source locators and observed input hashes. It deliberately leaves the market program, native closure, owner adapter, cache, baseline and audit unbound. Existing `okx_spot_dca` references `ok_rsi`; do not relabel that frozen baseline as this new controller.

Use the existing Research OS supervisor/knowledge process to review that packet. This is not a new research store, supervisor or execution wrapper.

## Validation and next gates

See `DESK_VALIDATION.md` for actually executed commands. Native configuration/roundtrip tests are present in `test_recipe_native_integration.py`; the entire module skips only when the native package is absent. In a full native checkout, missing transitive native dependencies must fail rather than silently count as a pass.

Before merge: close the relevant #55 native milestones, run the complete existing plus new suite and actual lint/hooks, test full lifecycle/recovery, and obtain genuinely independent review. Before any separately authorized activation: prove account authority/topology, reconciliation, freshness, image/source/state binding, alert delivery and rollback. Native sell-only handoff, Quants economics and Condor integration are **not implemented by this slice**.
