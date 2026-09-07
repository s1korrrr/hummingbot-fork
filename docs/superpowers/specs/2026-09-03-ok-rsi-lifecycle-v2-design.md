# OK RSI lifecycle v2 design

## Decision

Implement a research-only, opt-in lifecycle-v2 candidate in the native
Hummingbot OK RSI controller. Keep the shared reducer under `ok_rsi_core` so
`buy_sell` and assigned-inventory `sell_only` use the same accounting and exit
semantics. Keep `ok_rsi.py` as the runtime adapter.

The May-August evidence is inspected development evidence. It may diagnose and
shape this candidate, but it is not an out-of-sample or promotion result.

## Objective and acceptance criteria

The candidate must reduce accumulation risk without hiding losses in ending
inventory or weakening fill-driven accounting. The development target is:

- at least 50 percent reduction in worst candidate drawdown;
- at least 90 percent retention of current candidate aggregate PnL;
- positive PnL for BTC-USDC, ETH-USDC, and BNB-USDC;
- zero underwater candidate sales at 15 and 20 bps per side;
- meaningful activity rather than a near-no-trade result.

These are falsifiable research gates, not promised outcomes. Failure produces a
reject or hold verdict rather than further rescue tuning.

## Authority and boundaries

- Authority ceiling: local research only.
- Allowed: local code, focused tests, cached public historical data,
  deterministic replays, report generation, and read-only browser smoke.
- Forbidden: paper or live activation, exchange orders, deployments, messages,
  authenticated exchange calls, pushes, pull requests, and capital actions.
- Spot accounting remains independent 1x cash and inventory per asset.
- The legacy controller remains the baseline and is not changed by
  lifecycle-v2 configuration defaults.

## Controller behavior

### C1: causal sequential scale-ins

When enabled, every non-scout buy requires a fresh owner buy signal and 15
consecutive completed one-minute observations without a new low. The reducer
selects the lowest eligible open level. It never jumps to a deeper level and
never marks intermediate levels skipped merely because price crossed them.

An expired setup is re-armed only by a later qualifying observation. Expiry
does not reopen spent budget or create a second simultaneous authorization.

### C2: adverse-regime latch

An episode started while the higher-timeframe context is hostile or reset is
limited to the hostile scout fraction. Scale-ins remain blocked until the last
two completed hourly bars satisfy the causal hourly recovery rule. This status
is derived from completed candles and does not depend on mutable wall-clock
state.

### C3: economic episode budget

The episode budget is a dedicated optional configuration value and is always
bounded by the existing hard position cap. When unset, legacy episode behavior
uses the hard cap for compatibility. The preregistered research candidate uses
500 USDC while retaining each asset's existing hard cap.

### C4: profitable recovery exit

When enabled, completed 15-minute recovery and an executable bid above the
fee-adjusted aggregate minimum-profit floor arm an aggregate recovery trail.
Hourly markup remains the condition for wider distribution. Recovery never
bypasses the profit floor.

`sell_only` can use this same recovery exit for explicitly assigned inventory.
It never creates a buy. Underwater risk exits remain disabled in this candidate.

### Execution intent

Every durable request carries an explicit intent:

- `SCOUT_BUY`;
- `SCALE_IN_BUY`;
- `RECOVERY_TAKE_PROFIT`;
- `DISTRIBUTION_SELL`;
- `TRAILING_PROFIT_EXIT`;
- `TERMINAL_PROFIT_EXIT`.

The intent is copied into native executor configuration and receipt metadata.
Unknown intent is rejected at the native factory boundary. The research ledger
uses this field directly and fails closed on an unknown sell category.

## Configuration

Add opt-in fields to `OKRSIControllerConfig` with compatibility-preserving
defaults:

- `episode_budget_quote: Decimal | None = None`;
- `episode_sequential_scale_ins: bool = False`;
- `episode_require_scale_in_signal: bool = False`;
- `episode_scale_in_stabilization_bars: int = 2`;
- `episode_hostile_risk_latch: bool = False`;
- `episode_recovery_exit_enabled: bool = False`.

The lifecycle-v2 research binding explicitly enables C1-C4 and sets the 500
USDC budget. Existing episode configurations preserve v1 behavior unless these
fields are changed.

## State, restart, and accounting

No market decision may dispatch before its checkpoint is durable. Requests
remain fill-driven, one at a time, and budget reservations survive restart.
Configuration identity includes every new policy field, so an unresolved
episode cannot silently hot-reload between variants. Existing v1 checkpoints
remain readable, but a policy identity change requires a reconciled restart.

## Research evaluation

Reuse the exact cached May-August signal and context sources. Freeze baseline
and current-v1 receipts. Execute isolated C1, C2, and C3 variants at 15 bps for
all three assets, then the combined C1-C3 variant and combined C1-C4 variant.
Only mechanism-valid variants proceed to 20 bps. Only the single frozen
survivor receives strict, no-fill, latency, and partial-fill controls.

The final verdict reports per-asset PnL, drawdown, capital-days, exposure
occupancy, maximum adverse excursion, episode duration, time to recovery,
turnover, realized and unrealized PnL, and all failure cases.

## Rollback

Disable the new opt-in configuration fields or revert the lifecycle-v2 branch.
The legacy baseline and v1 defaults remain unchanged. Never discard unrelated
root or nested repository work.
