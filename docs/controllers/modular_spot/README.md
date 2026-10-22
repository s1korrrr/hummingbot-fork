> Current native composition: [CONTROLLER_PARITY.md](CONTROLLER_PARITY.md). Rust transport history: [RUST_CANDLE_VALIDATION.md](RUST_CANDLE_VALIDATION.md). Final cross-repository evidence belongs to `rsibot/docs/reports/2026-09-19-modular-native-completion.md`. Earlier counts below retain their original source scope.

# Modular spot controller — isolated candidate

Status: locally validated integration candidate, execution disabled by default.
The `modular_ok_rsi` and `modular_rsi_v5` families reuse the complete existing native
controllers through thin composition wrappers. The separate `modular_spot` RSI-cross
and breakout policies remain experimental baselines. They are not relabeled as
either native strategy. See CONTROLLER_PARITY.md for their distinct contracts.
Native contract baseline: Hummingbot commit
`4df9ff3158a405b71d28b1c07446cfc7864a19fa`.

## Isolation and ownership

`controllers/generic/modular_spot.py` declares `ModularSpotController(ControllerBase)`
and its native-discoverable configuration in the same module. All policies and
adapters are private to `controllers/generic/modular_spot_core/`. The new native
wrappers live beside that controller and inherit the original implementations.
Three runner/accounting gates recognize genuine modular OK RSI through a narrow
native configuration-class predicate. The API companion binds native configuration
and immutable image identity. Existing controller algorithms, connector and executor
implementations, live deployment configuration and root component pins are unchanged.
No running owner is migrated or automatically enrolled.

Native-family recipes use `NativePolicyRecipe`, the actual native parameter schema,
and an explicit separate owner ID. Exports are disabled and contain no initial
inventory adoption. Native candle transport remains available; shared confirmed
WebSocket transport is opt-in. The Rust service admits 2–10,000 rows per pair within
a three-million-row service bound, with validated persistent cache recovery. Native
warm-up defaults are 1,500 rows for OK RSI, 3,660 for episode context, and 7,514 for
RSI V5. These bounds do not constitute an all-universe long-history benchmark.

Each native wrapper currently uses one stream connection; the service admits 32
clients. The 294-pair multiplexed feed test is distinct from running 294 native
controllers. Source seals cover controller dependencies; the native engine and
runner must additionally be bound by the tested image identity.

The remaining component and policy sections describe the original `modular_spot`
baseline. Native families retain their own signal, sizing, execution and exit rules.

Native Hummingbot owns candles/market-data access, trend-context calculation,
ExecutorActions, executor construction, orders, fills, cancellation, terminal-fill
reconciliation, position holding, persistence and performance reports. The existing
controller-local `get_executor_class_for_config` hook selects a PositionExecutor
subclass only for this controller. Order event callbacks and the single-attempt
entry lifecycle remain inherited. No global executor mapping is changed.

## Components

| File | Responsibility |
|---|---|
| `config.py` | Strict, versioned composition inputs; unknown options rejected |
| `market.py` | Closed, ordered, contiguous and finite candle views; Wilder RSI and native trend context |
| `signals.py` | RSI rebound or breakout entry policy; no order side effects |
| `acquisition.py` | Fresh-signal, bounded dip-stage eligibility |
| `portfolio.py` | Quantized use of an explicitly preallocated quote budget |
| `inventory.py` | Read-only projection of native receipts and retained inventory |
| `risk.py` | Marked-PnL drawdown/daily entry restrictions with a persistent latch |
| `trailing.py` | Pure price-floor calculation using a native after-cost floor |
| `planner.py` | Deterministic composition, episode transitions and entry proposals |
| `state.py` | Atomic decision/claim state in the native recorder's Metadata table |
| `execution.py` | Native PositionExecutor configuration and controller-local restrictions |
| `runtime.py` | Native snapshots, persistence-before-dispatch and revalidation |

The separate modules are composition boundaries, not network services. There is
no second order book, accounting ledger, portfolio service or executor engine.

## Implemented semantics

The first valid observation seeds the decision watermark without placing an order.
RSI rebound requires a closed-bar crossing back above the configured oversold
threshold. Breakout compares the latest closed price with earlier highs and can
require the existing native ADX/DI trend context. These are explicit baselines, not
copies or claims of equivalence to OK RSI. HMM fitting and economic optimization
are not performed by this controller.

Each acquisition tranche is a separate native long spot PositionExecutor, with
`leverage=1`, limit entry, `entry_order_policy=single_attempt`, and a bounded entry
window. Subsequent dip stages require a fresh signal, terminal entry evidence and
a lower price relative to the last actual acquisition, never an unfilled order.
The cumulative admitted quote commitment cannot exceed `total_amount_quote`.
Failed/unfilled stages do not automatically replenish that commitment while a
campaign remains open. No further acquisition occurs after any tranche starts
selling. A campaign resets only when every claimed native executor is terminal
and flat, and the reset is persisted before a new campaign can be admitted.

This is per-tranche management, **not basket-average DCA**. The initial breakout
profile rejects multi-stage dip averaging. New strategy families can be implemented
inside this private composition without changing any existing controller.

Native trailing owns activation, high-water marks, ratcheting and order lifecycle.
The local policy adds an after-cost positive floor:

`ceil_to_tick(native_no_loss_price * (1 + activation - delta))`.

Therefore this initial profile's minimum profit margin is explicitly
`activation - delta`; it is not an independent optimizer parameter. Its full
policy survives serialization in an unmodified native PositionExecutorConfig.
Exits require complete native fill/fee observations; unsupported fee currencies
and missing observations block a new exit. Trailing closes are limits with no
market fallback. Stop-loss, time-limit and competing fixed take-profit barriers
are disabled in this profile. Stop requests retain native inventory rather than
forcing loss realization. A price gap can leave inventory unsold or a limit order
unfilled: **profit-only selling does not guarantee bounded mark-to-market loss**.
Late fee corrections still require native accounting reconciliation.

Held/dust inventory is not erased or renamed into free capital. The buy/sell
profile does not adopt another owner's lot. The separate sell-only profile requires
an explicit, persisted handoff bound to complete native receipts; a configured
wallet balance or initial-position assignment does not establish ownership.

The current sell-only policy liquidates the admitted remaining lot when its
after-fee profit floor and fresh-data checks allow a native LIMIT SELL CLOSE. It
is not the legacy RSI rollover or adaptive trailing policy. Unsupported fee
forms, missing/corrected receipts, an unresolved sale or a below-floor bid hold
new sales. Native orders, fills and cancellations remain owned by OrderExecutor.

## Capital, risk and operational boundaries

The quote budget is an operator-preallocated cap for this controller and pair. It
is **not** an account-wide portfolio optimizer or a coordination mechanism for
other bot processes. Use only a separately admitted, uniquely owned account/budget
scope. Existing native OKX wallet reservations remain in force. Cross-process
ownership and aggregate allocations must be proved by the stack before use; this
PR grants neither. No controller-local signal can authorize additional capital.

Risk calculations consume native marked global PnL, including unrealized inventory,
and stop new acquisition at configured drawdown/daily-loss thresholds. Thresholds
are fractions of assigned quote capital, not guaranteed exit-loss bounds. The UTC
day baseline is the first valid observation in that day, not a reconstructed
midnight account statement. Pauses do not automatically clear at midnight or after
recovery. Transient missing native reports hold new risk; corrupt state, config
mismatch, contradictory receipts and CAS conflicts require diagnosis. Fresh
candles and `ready` flags alone do not prove account/book/report freshness; native
runtime heartbeat, reconciliation and alert behavior remain admission checks.

Decision state contains configuration/source identity, candle watermark, campaign
number, native executor IDs, admitted commitment and risk state. It contains no
independently maintained fills or PnL ledger. A real SQL compare-and-swap precedes
CreateExecutorAction emission. A missing acknowledgement leaves the claim unresolved.
Neither expiry nor process absence releases ownership or authorizes a replacement.
Store errors never dispatch. The current configuration and inputs are checked again
at factory admission and at native entry acquisition; filled inventory continues
under its native exit owner.

Source/configuration changes require restart and reconciliation. The fingerprint
covers this controller's source files and behavioral settings; the native image
must additionally be pinned by the deployment. The current native planned-resume
implementation hardcodes PositionExecutor rather than honoring the local selector;
a pending manifest for this controller is rejected. Do not work around that gate,
create a second owner, delete checkpoints or overwrite the running database.

## Disabled example (not installed into any running configuration)

```yaml
id: modular_spot_research
controller_name: modular_spot
controller_type: generic
connector_name: okx
trading_pair: BTC-USDC
execution_enabled: false
account: isolated_research_account
program_revision: '1'
policy_revision: '1'
budget:
  group: modular_research
  max_quote: '100'
  max_assets: 1
total_amount_quote: '100'
interval: 1m
max_records: 300
max_candle_age_seconds: 120
signal:
  kind: rsi_rebound
  rsi_length: 14
  oversold: '30'
acquisition:
  fractions: ['0.3', '0.4', '0.3']
  minimum_drop: '0.02'
  cooldown_seconds: 300
  entry_timeout_seconds: 60
trailing:
  activation: '0.015'
  delta: '0.005'
risk:
  max_drawdown_fraction: '0.10'
  max_daily_loss_fraction: '0.05'
  max_spread_fraction: '0.003'
  fee_reserve_fraction: '0.005'
  cash_reserve_quote: '0'
```

These numbers are illustrative and have no profitability evidence. Changing the
flag is not approval to run paper/canary/live. No deployed config or API profile
is changed by this PR.

## Review and validation gates

Run `python -m pytest -q test/modular_spot` with the complete native Hummingbot
runtime. Missing native imports must fail validation; a skipped native module is
not an accepted result. Explicitly bind PYTHONPATH to the owner checkout:

```bash
python -c 'from controllers.generic.modular_spot import ModularSpotControllerConfig; from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor'
python -m pytest -q test/modular_spot
```

Before merge, run native controller discovery/config round-trip and factory tests,
existing PositionExecutor/orchestrator regression tests, and the repository's lint
hooks in its pinned environment. Require a genuinely independent reviewer.
Before activation, additionally verify restart/partial-fill/cancel races with the
real native executor, fees and quantity accounting, report freshness, post-only or
limit venue semantics, ownership locks, recovery refusal, alert delivery, backups,
image/source binding, and rollback. Research OS/quants-lab intake and authorized
native-semantic economic evaluation are separate work; none was executed here.
See `VALIDATION.md` for the evidence actually produced.

## Public candle path and availability

`OKX public WebSocket -> native OKXSpotCandles -> SnapshotHub -> authenticated
loopback HTTP snapshots -> DataPool -> modular policy`. Native REST history fills
the initial/recovery window. It does not replace live WebSocket updates.
No new exchange socket implementation or message broker is needed.

Run the bounded public-data smoke from this checkout with the native interpreter:

```bash
PYTHONPATH="$PWD" /Users/s1kor/anaconda3/envs/hummingbot/bin/python \
  docs/controllers/modular_spot/public_candle_smoke.py \
  --seconds 240 --output /tmp/pr55-public-candle-smoke.json
```

It starts two native public 1m feeds, an ephemeral loopback publisher and two
readers, verifies confirmed bars and new-bar entry readiness, reconnects one
reader while the other stays available, then shuts its own processes down. It
uses no exchange credentials, account connector, owner DB or orders. This is a
bounded data-path smoke, not a persistent deployment or a live trading soak.

`profiles.py` exports disabled controller configurations and their matching
manifest. The standalone publisher is available as:

```bash
PYTHONPATH="$PWD" python -m controllers.generic.modular_spot_core.candle_transport \
  --manifest /absolute/path/candle-manifest.json \
  --token-file /absolute/private/path/reader-token --port 8100
```

The token must be an absolute, non-symlink, private regular file containing at
least 32 non-whitespace ASCII characters. Do not place its content in YAML/Git.
The sample account name does not prove segregation or shared-wallet authority.
Native mode remains available without deploying a shared publisher.
