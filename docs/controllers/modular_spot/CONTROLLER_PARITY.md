# Native controller composition and recovery evidence

Date: 2026-09-19. Scope: isolated candidate implementation. This document does not authorize activation, deployment, inventory transfer or trading.

## Implementation decision

The experimental `modular_spot` RSI-cross/breakout implementation is retained unchanged as a separate baseline. It is not relabeled as OK RSI or RSI V5.

`generic.modular_ok_rsi.ModularOKRSIController` inherits the complete existing `OKRSIController`; `generic.modular_rsi_v5.ModularRSIV5Controller` inherits the complete existing `RSIv5Controller`. Their configuration classes inherit the actual native schemas, including native validators. The composition seam changes only optional candle sourcing, a default-disabled new-risk gate, source/configuration binding, and persistence of existing native held-recovery trail state. It does not implement another signal engine, order manager, fill ledger or execution mapping.

## Capability and configuration map

| Capability | Native owner reused | Composition behavior and evidence |
|---|---|---|
| RSI, Bollinger, MACD, score, trend/regime, confirmation | OK RSI → `hl_rsi` and `rsi_signals`; RSI V5 → `rsi_v5` and `rsi_signals` | Full native `update_processed_data` executes. Differential tests compare actual indicator frames, health, signal and confirmation state on identical candle history. |
| Native configuration | `OKRSIControllerConfig` / `RSIv5ControllerConfig` | All native fields pass through their real schemas. Unknown recipe fields and generic screening options that these families do not consume are rejected. |
| Entry sizing, maker placement, DCA/cooldown, exposure limits | Inherited native methods | Real `determine_executor_actions` and `get_executor_config` outputs match the original controllers after excluding generated executor IDs. Native retry/single-attempt differences are preserved. |
| OK RSI durable admission and down-only acquisition | Existing native gate/store and executor factory | No bypass or alternate authorization. Episode BUY remains held where the native owner already requires additional single-attempt support. |
| Position/order execution, fees and fills | Existing native PositionExecutor/OrderExecutor and orchestrator | Differential tests inject actual partial fill, cancellation and late-fill events into the genuine native executor. Filled exposure and trailing state remain equal. |
| Trailing parameters and exits | Native OK RSI adaptive trailing; native RSI V5 trailing/recovery | Native executor configuration matches exactly. Disabling new risk does not stop inherited protective exits or native held-inventory reductions. |
| Flash-crash protection, bag freeze, loss accounting | Existing native controller methods | Delegated, not rewritten. This does not establish economic efficacy or independent correctness of every legacy policy. |
| Held-recovery peak across restart | Exact native `RecoveryTrailState`, persisted by the wrapper in recorder Metadata | Real SQLite restart tests restore the prior peak before a reduction decision. Missing/corrupt/inconsistent state holds new actions. |
| Native planned-position resume | Existing engine receipt and orchestrator path | Engine-owned PositionExecutor trailing receipts remain authoritative. No new permission to resume or transfer a running owner is introduced. |
| #57 feature/decision/capital foundations | Existing separate pure libraries | Native compositions consume their native equivalents rather than overriding them. There are no accepted native recipe flags pretending these separate libraries participate in the action path. |

## Candle contract

Native transport remains the default. The current owner OKXSpotCandles implementation explicitly admits confirmed rows only in both REST and WebSocket parsing. This fact is engine-version-specific; the candidate does not synthesize forming rows or assert parity to a different running image without matching that image.

Shared transport is an explicit opt-in `DataPolicy(mode="shared", transport="stream", ...)`. The candle-only provider delegates prices, balances, rate sources and connector readiness to the original native provider. It never creates an exchange connector. One confirmed 1m dataset supplies the native primary window; the existing OK RSI aggregation implementation derives completed 5m/15m windows. Differential tests compare these windows with an independent pandas resample, including quote volume. Source readiness and candle age are checked independently; an initial/recovery snapshot never permits a historical entry.

Native warm-up requirements are calculated from the native configuration. Defaults require 1,500 1m rows for OK RSI and 7,514 rows for RSI V5's 500-record 15m window, including worst-case alignment padding. OK RSI episode context requires at least 3,660 rows. Custom indicator/timeframe settings may require more; configurations above the admitted 10,000-row bound are rejected. HTTP snapshot admission remains limited to 5,000 rows. No history truncation is hidden from the native policy.

The native wrappers currently own one DataPool and one WebSocket connection per wrapper. The Rust service admits at most 32 clients. This is a selected-pair composition boundary: streaming the full 294-symbol universe through multiplexed readers does not establish or admit 294 concurrent native controllers. Shared-pool reuse and that larger controller-capacity test remain separate work. The scoped public native probe uses four wrappers across two symbols, within the current connection limit.

## Native-family recipe and source seal

Use the separate strict `NativePolicyRecipe` contract instead of generic signal/acquisition aliases:

```json
{
  "schema_version": 1,
  "name": "fixed_btc_ok_rsi",
  "family": "ok_rsi",
  "controller_id": "modular_spot_fixed_btc_ok_rsi",
  "parameters": {
    "trading_pair": "BTC-USDC",
    "usd_per_entry": "30",
    "max_total_position_usd": "100"
  },
  "data": {"mode": "native"}
}
```

The other admitted family is `rsi_v5`. The shared recipe compiler dispatches through `parse_recipe`, `render_recipe`, `bind_native_bundle` and `write_bundle`. Exports always set `execution_enabled=false`, carry no initial positions, and refuse legacy-owner inventory adoption. Each controller file is `<controller_id>.yml` and retains the common `RecipeBinding`.

Native-family receipts declare `receipt_kind="native_policy_composition_v1"`, `source_scope="controllers"` and `source_files` as a relative-path/SHA-256 map. The bounded dependency closure includes the selected wrapper, modular support code, imported native controller modules, relative imports such as `rsi_signals`/`pandas_compat`, and package initializers. It is a controller-source seal. Native Hummingbot engine dependencies outside `controllers` must additionally be bound by the verified package/image identity. Existing generic baseline receipts retain their existing source scope.

## Native runner identity boundary

The native `v2_with_controllers` runner and executor orchestrator share `is_ok_rsi_policy`. It preserves the existing `ok_rsi` behavior and recognizes `modular_ok_rsi` only through its exact native configuration class; a name or family string alone cannot claim the policy. This restores the existing durable strategy risk-state path, including drawdown peaks and operator pauses, and preserves strict unknown-cost wallet-sale classification and PnL availability. Every existing sell-only, connector, side, position action, level, price-floor and leverage condition remains required. RSI V5 does not gain an OK RSI exception. These engine/helper/script bytes are outside the controller-source seal and require the final engine image binding.

The focused new runner tests plus existing restart, reporting and wallet-sale suites passed 78 tests. New regressions exercised real runner construction against disposable SQLite for valid, missing, malformed, non-finite and wrong-owner risk state, plus actual orchestrator classification and negative policy/executor cases.

## Recovery persistence boundary

The wrapper uses the existing recorder's Metadata table under a separate key scoped by recorder configuration, controller ID, connector and pair. It stores exact decimal strings for native held-trail fields, source/configuration identity, a native position-key/amount/cost snapshot, and a bounded pending native-reduction receipt. It does not create position or fill truth.

Before decision dispatch, runtime held positions must agree with the recorder's owner-scoped positions. Normal report/persistence lag is a transient hold that clears after reconciliation. A first checkpoint is permitted only while that owner is flat. After restart, missing state with inventory, corrupt state, changed source/configuration or unexplained inventory changes require explicit reconciliation; the wrapper does not infer a new peak.

Native RSI V5 and HL RSI intentionally reset the recovery peak when they propose a recovery SELL. The adapter preserves this exact policy transition while retaining the prior peak and emitted executor/configuration identity in the checkpoint. The next cycle/restart must see the matching native recorder executor receipt before the reset is reusable. A dropped/unpersisted request remains held; it is not replayed. Other peak regression and stale concurrent writers fail closed.

Checkpoint CAS completes before returning any new action list. A persistence failure suppresses dispatch and restores the exact native held-trail state from before the decision, including the prior peak. Transient native inventory lag holds until reconciliation; corruption, CAS conflicts and other persistence failures latch the wrapper's action hold. Already running native executor protection remains independent. The wrapper reports `modular_composition.recovery_hold`; it does not clear local executor state or modify native accounting tables.

## Validation and remaining boundary

Focused tests live in `test_native_composition.py` and `test_native_composition_recovery.py`. They cover full native signal/configuration parity, actual BUY action generation, native partial/cancel/late-fill ownership, running held recovery, a real SQLite restart, missing/corrupt checkpoints, transient native report lag, CAS conflicts, unsupported peak regression and persistence failure before action dispatch. A regression test injects inventory lag after a native recovery SELL proposal resets its peak; no action escapes, the previous peak is restored, and the reconciled next cycle emits one native SELL. The focused composition and recovery suite passed 23 tests after this fix.

These are local inert-transport tests. They establish the tested native behavior and checkpoint boundaries, not exchange execution, economic performance, production image deployment, live-owner migration or all possible legacy parameter combinations. The inherited native episode BUY hold remains an explicit unsupported capability; neither an export nor passing tests removes it. Live-stack integration still requires the source/image/immutable-bundle evidence owned by the parent integration task.
