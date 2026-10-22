# Opt-in OK RSI inventory episode runtime

This implementation is for local research and testing. Existing configurations
default to `episode_enabled: false`. No operational configuration, connector,
database, process, or trading authority is changed by installing this source.

`OKRSIController` remains the public loader entry point. Legacy configuration,
completed UTC candle aggregation, and trailing helpers are compatibility exports
from `ok_rsi_core/config.py`, `candles.py`, and `trailing.py`. The episode path
is `OKRSIController.determine_executor_actions` -> `EpisodeRuntime` -> pure
`episode.transition` -> durable checkpoint -> native `CreateExecutorAction` or
`StopExecutorAction`. It bypasses the legacy independent trailing/recovery exit
owners for new episodes.

The runtime consumes at least 3,060 consecutive closed 1m bars, requesting 3,660
records. It derives complete 15m and 1h bars, ATR14, EMA20/50, and the previous
20-hour high without the current hour. Runtime context and signal computation
use a controller-local completed-candle provider. The original shared provider
and other controllers are unchanged. Current BBO, available wallet balances,
exchange quantity/price increments, and minimum notionals remain separate live
inputs to the reducer. Historical BBO proxies must be labelled by the research
runner, never treated as observed executable quotes.

The all-in budget and mark cap use `max_total_position_usd`. `episode_fee_rate`
is an explicit conservative fee reserve, default 0.001. Actual native receipt
fees are reconciled in their real base/quote currencies. Unknown third-asset
valuation or incomplete native fill detail puts the controller on hold.
Restored accounting is checked against cumulative episode receipts before a
decision. Late acquisition fees restate the average cost of remaining inventory
and the episode equity trail. When pricing is unavailable, base-fee restatement
uses the explicitly retained last valid bid, so missing prices do not manufacture
a reversal. Actual inventory and cash still reflect the full reported fee.

Sell-only requires immutable `episode_assignment_id` and positive
`episode_initial_base`. `episode_initial_cost` is the total acquisition cost in
quote, required when the profit guard is enabled. Available wallet surplus is
never assigned automatically. Buy/sell starts fresh: existing untagged active
executors or held inventory block activation. Automatic legacy-executor handoff
is deliberately not implemented.

`EpisodeOrderExecutor` uses exactly one bounded maker-limit order attempt per
durable request. It preserves fixed maximum buy/minimum sell price, never
falls back to market, and stops rather than retrying the original full amount
after cancellation/failure. The controller may separately authorize a remaining
sell target after terminal reconciliation. Native expiry remains enforced even
when market context is unavailable. Partially filled cancellation emits native
held-position records containing gross fills and explicit `actual_base_fee`;
the native `PositionHold` opt-in projection deducts base fees from buys/adds them
to sell consumption, while quote fees exclude base conversion. Legacy records
without this field retain existing accounting.

Checkpoints reuse the existing `RSIV5RuntimeEvent` table through
`RSIV5RuntimeStore.append_many/load`; no schema is added. Each checkpoint
contains the entire policy state, command reservations and last valid market
snapshot. The append happens before any action is returned. Persisted commands
missing from native reports remain reserved; restart never blindly resubmits
them. Explicitly injected in-memory stores are for tests/research only. A native
runtime requires an initialized `MarketsRecorder`.

Config changes, stale reload checkpoints, and disabling while inventory or
orders remain unresolved fail closed. Hot reload uses a small optional generic
transfer guard before replacing the old controller. Process restart also checks
for unresolved durable episode state before allowing the disabled path. To
rollback an in-flight episode, reconcile the native requests and complete an
explicit exit-owner transfer first; do not erase checkpoints or flip the flag.

Local validation covers loader compatibility, unchanged legacy action tests,
closed-candle causality/gaps, durable reserve-before-dispatch, unknown pending
commands across restart, partial native fills, fee denomination, zero oversell,
manual kill/operator pause, explicit sell-only assignment, changed factory
parameters, and native hold import/deduplication. No exchange lifecycle, paper,
canary, live fills, authenticated fee schedule, or deployment was exercised.
Exchange cancellation acknowledgement/fill-delivery ordering still needs native
receipts before promotion; deterministic local tests do not prove that boundary.

Owner verification command:

```bash
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -W error -m pytest -q \
  test/test_ok_rsi_episode.py \
  test/test_ok_rsi_episode_runtime.py test/test_ok_rsi_episode_execution.py \
  test/test_ok_rsi_episode_factory.py \
  test/test_ok_rsi_episode_context.py test/test_ok_rsi_controller.py \
  test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py
```

The above focused integration command passed 161 tests and 19 subtests in
6.07 seconds at source commit `102649fde`. An expanded command also including
`test/test_rsi_v5_runtime_store.py` passed its assertions but failed pytest
teardown under warnings-as-errors because existing SQLite connections were not
closed. That expanded invocation is not a clean validation result.
