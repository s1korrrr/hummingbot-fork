# OK RSI runtime contract

`ok_rsi` is an OKX spot specialization of the shared `hl_rsi`/RSI V5
controller. It keeps signal, regime, recovery, and inventory behavior in the
shared controller and owns only the OKX boundary rules, sell-only sizing, and
continuous trailing selection.

This document describes repository behavior. It is not permission or evidence
to run the strategy with capital.

## Supported launch paths

The normal buy/sell pack uses Hummingbot's native
`scripts/v2_with_controllers.py` loader. Put one explicit controller YAML per
pair in `conf/controllers/`, reference those filenames from a script YAML in
`conf/scripts/`, and start it with:

```text
start --v2 ok_rsi.yml
```

The grouped inventory-only launcher uses `scripts/ok_rsi_sell_only.py`. Its
script YAML declares the list of pairs and is started independently:

```text
start --v2 ok_rsi_sell_only.yml
```

Do not start both groups simultaneously. Candle feeds within one Hummingbot
process share a connector-level rate limiter and reserve half of OKX's
documented public-IP allowance for connector or other-process traffic. Separate
Hummingbot processes still cannot coordinate one exchange/IP rate budget.
Stagger process startup and inspect the OKX logs before adding another group.

## Configuration boundary

The controller accepts only `okx` and `okx_paper_trade`, `BASE-QUOTE` pairs,
leverage `1`, and `ONEWAY` position mode. Paper execution still uses public
`okx` candles because there is no paper-trade candle endpoint. Execution and
candle pairs must match, and all configured candle intervals must be supported
by the checked-out OKX candle implementation.

The current EEA operator profiles use the authenticated account's live USDC
instrument IDs: BTC/ETH/SOL/XRP/BNB-USDC for buy/sell and SUI/XLM/ENA-USDC for
sell-only. Regional availability must be rechecked through OKX's authenticated
account-instruments endpoint before copying these profiles to another account.

Each controller creates only its configured base candle feed. Medium and long
trend intervals remain editable, but their candles are derived locally from
complete UTC-aligned base bars:

```yaml
interval: 1m
trend_confirmation_enabled: true
trend_confirmation_medium_interval: 5m
trend_confirmation_long_interval: 15m
derived_higher_timeframe_max_records: 100
```

The base feed retains enough rows for the configured largest interval and
derived record count. A derived bucket is omitted unless it contains exactly
the expected consecutive base timestamps; the active incomplete bucket is
also omitted. Higher intervals must be larger whole multiples of the base
interval. This removes independent higher-timeframe REST/WebSocket feeds while
keeping timeframe selection explicit in configuration.

The ordinary buy/sell path is trailing-only. Configuring a fixed stop loss,
take profit, time limit, positive `take_profit_pct`, or positive
`max_hold_time_seconds` is rejected because those inherited fields are not
wired into this controller's executor contract. Disabling trailing in
`buy_sell` mode is also rejected. This prevents a syntactically valid config
from silently creating an executor without an ordinary exit.

The following trailing values are explicit strategy inputs and should remain
visible in each controller YAML:

```yaml
use_trailing_exit: true
trailing_activation_pct: 0.015
trailing_delta_pct: 0.0035
trailing_stop_order_type: LIMIT
entry_limit_max_age_seconds: 30
trailing_stop_limit_max_age_seconds: 3

use_adaptive_trailing: true
trailing_atr_baseline: 0.002
trailing_activation_min_pct: 0.003
trailing_activation_max_pct: 0.05
trailing_delta_min_pct: 0.001
trailing_delta_max_pct: 0.02
trailing_min_locked_profit_pct: 0.004
trailing_activation_volatility_exponent: 0.70
trailing_delta_volatility_exponent: 0.35
trailing_min_multiplier: 0.5
trailing_max_multiplier: 2.5
```

All numeric inputs must be finite. The controller rejects inverted bounds,
delta greater than or equal to activation, and any permitted bound combination
that would violate the locked-profit floor. A market trailing order must use a
zero limit-fallback timeout because no limit order exists to expire.

## Continuous trailing behavior

The YAML activation and delta are neutral anchors, not low/normal/high named
profiles. For every new BUY executor, the controller computes the causal
volatility ratio `r = ATR% / trailing_atr_baseline`, clamps it to the configured
multiplier range, and selects:

```text
activation = clamp(anchor_activation * r^activation_exponent)
delta      = clamp(anchor_delta      * r^delta_exponent)
```

Activation and delta have independent exponents and independent hard bounds.
The final delta is reduced when necessary so `activation - delta` remains at
least `trailing_min_locked_profit_pct`. The selected pair is frozen in the
created `PositionExecutorConfig`; it does not move during that executor's
lifetime.

The current selection is exposed in controller `custom_info` and status-summary
telemetry under `trailing_policy`, including the anchor values, selected values,
volatility ratio, ATR, and initial locked profit. Before the first BUY executor,
selected values are `null` and the policy reports whether fixed or continuous
adaptive behavior is configured.

Recovery trailing is intentionally separate. It manages inventory held after
an early stop/bag freeze, using recovery target and pullback fields. It does not
reuse or dynamically mutate the ordinary PositionExecutor trailing barrier.

## Execution and inventory safeguards

- LIMIT_MAKER entry orders expire after `entry_limit_max_age_seconds` instead
  of remaining open indefinitely.
- A trailing LIMIT exit falls back according to
  `trailing_stop_limit_max_age_seconds` and the checked-out PositionExecutor.
- Missing ATR, non-finite/non-positive prices, missing trading rules, or failed
  quantization block executor creation. There is no unquantized fallback.
- Sell sizing uses exchange-reported available base balance only. Total balance
  is not a fallback because it may include inventory reserved by open orders.
- In `sell_only`, inventory worth less than `sell_all_below_quote` is sold in
  full; otherwise `sell_only_inventory_fraction` is targeted. Trading-rule and
  available-balance bounds remain authoritative.

## Legacy history and inventory migration

The canonical buy/sell runtime identity is `ok_rsi.yml`, which creates
`data/ok_rsi.sqlite`. Its controller IDs are `ok-rsi-btc`, `ok-rsi-eth`,
`ok-rsi-sol`, `ok-rsi-xrp`, and `ok-rsi-bnb`. Each controller YAML records its
corresponding `rsi-v5-*-usdc-1m` ID in `legacy_controller_ids`; this is a
reporting/provenance mapping, not permission to mix legacy executors into
current cooldown, position, or order decisions.

Use `scripts/ok_rsi_wallet_snapshot.py` to capture authenticated balances
without producing controller, executor, order, or cancel actions. The one-shot
migration CLI is dry-run by default:

```text
conda run -n hummingbot python bin/ok_rsi_history_migration.py \
  --source data/rsi_v5.sqlite \
  --target data/ok_rsi.sqlite \
  --wallet-snapshot data/ok_rsi_wallet_snapshot.json \
  --receipt data/ok_rsi_migration_receipt.json \
  --dry-run
```

The source database is immutable. Legacy rows retain their original Binance
connector, pair, controller, and config provenance. Current OKX inventory is
represented by separate deterministic `Position` and `PositionHoldLedger`
seeds whose payload explicitly says that the seed is not an exchange fill.
The planner rejects a wallet balance below the source tracked amount; it never
creates a position for inventory that the authenticated wallet cannot prove.
Excess wallet inventory is reported as untracked and is not assigned a
fabricated average price.

Reported minimum-signal and recovery-arm prices include migrated cumulative
fees per tracked base unit because the controller gates both paths on net PnL,
not gross price appreciation. They remain conditional eligibility levels: RSI,
reversal/trailing state, exchange quantization, spread, and available balance
still determine whether and where an order is eventually submitted or filled.

Before `--apply`, make SQLite online backups, verify integrity, stop every
writer, and review all five reconciliations. Apply requires the exact dry-run
receipt and rechecks source, target, and wallet-snapshot checksums. Verification
then requires the applied target checksum, integrity result, and deterministic
seed counts. The tool never uses `INSERT OR REPLACE` and never relabels a
legacy fill as OKX.

## Operational limits

The current runtime has per-controller position, executor-count, drawdown,
manual-kill, consecutive-loss, and cooldown controls inherited from the shared
RSI controller. It does not provide a single portfolio-wide exposure allocator
across independent scripts, nor a calendar-day loss reset. Those gaps block a
paper/canary/live promotion decision until they are explicitly designed and
tested.

Repository tests and a public-candle startup smoke cannot validate authenticated
balances, regional instrument access, fills, partial fills, spread, slippage,
latency, exchange-side cancellations, or market impact. Those require a
separate bounded paper/canary gate with monitoring, rollback, and explicit
operator authority.

## Validation

Run the focused controller and shared market-data regressions from the
`hummingbot/` owner repository:

```text
conda run -n hummingbot python -m unittest test.test_ok_rsi_controller -v
conda run -n hummingbot python -m unittest test.hummingbot.data_feed.test_market_data_provider -v
```

The committed YAMLs under `test/fixtures/ok_rsi/` are loader fixtures, not
recommended capital allocations. Runtime YAMLs under `conf/` are operator-owned
and intentionally ignored by Git.
