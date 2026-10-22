# OK RSI accumulation, recovery, and distribution — design for review

Date: 2026-09-03. Revision: 2. Status: proposed architecture; not implemented or enabled.
Owner: `hummingbot/`. Authority: research-only local design and validation.

## Amendment record

On 2026-09-03 the user requested inclusion of the second, accumulation-focused
proposal. This revision explicitly extends the shared episode design with gross
spending limits, persistent scale-in levels, stabilization, defensive/exhausted
states, and bounded sizing. It supersedes revision 1's proposal to retain the
existing accumulation policy in the final candidate and its exit-first research
sequence. Revision 1 is preserved in `ok-rsi-episode-design-v1.md` alongside this
file. Inventory attribution, no-force-sell semantics, exit ownership, and local
authority remain unchanged.

The request authorizes incorporating the material into this design. The pasted
document's embedded approval request, fixed YAML values, portfolio allocator,
and baseline-changing instructions are not independent user instructions.

## Objective and completion bar

Improve the way OK RSI admits buys, manages accumulated spot inventory, and
distributes it during recovery and strength. Both `buy_sell` and `sell_only`
must use one deterministic episode policy through the existing
`OKRSIController`. Keep `ok_rsi.py` understandable by moving cohesive logic
into supporting files. Sharing code does not mean sharing inventory between
controller instances, modes, accounts, or assets.

The supplied Binance chart is a behavioral example. The attached proposal is
reference material, not authority to enable its YAML, force liquidation, change
loss policy, tune against the visible peak, or modify unrelated controllers.

Engineering completion requires causal inputs, explicit inventory ownership,
exclusive exit ownership, restart-safe decisions and execution reconciliation,
loader compatibility, and regression tests for both modes. Better trading
outcomes require separate evidence after engineering correctness is established.

## Verified current state

Inspected Hummingbot HEAD:
`91372522a8e4bb178a959f1b0c42ec7276cf89ba`, branch
`feat/andrzej_ok_rsi_history_migration`. This differs from the attachment's
`354c92a1260dfdc6882ea1c3623a3e4afad9fd73` reference. This identifies inspected
source, not an authenticated running deployment.

- `controllers/directional_trading/ok_rsi.py` is 702 lines, inheriting from
  `hl_rsi.py` (4,888 lines). It already contains candle aggregation, adaptive
  trailing calculations, config validation, and exchange/executor adaptation.
- Both modes already use `OKRSIController`; the sell-only launcher builds the
  same config with `trade_mode="sell_only"` and
  `sell_only_if_profitable=False` in `scripts/ok_rsi_script_common.py`.
- Current sell-only sizing uses available wallet balance: 10% at or above
  20 quote units, full amount below 20, subject to exchange sizing rules.
- Ordinary buys create independent `PositionExecutorConfig` instances with
  a trailing selection frozen at creation. A config change cannot be assumed
  to disable an already-running executor's trailing exit.
- Held inventory is aggregated by `_aggregate_held_inventory`, but active buy
  executor inventory is handled separately. This is not yet a complete episode
  book containing all fills, reservations, and realized cash flows.
- Recovery trails are keyed by position. Signal and recovery sells have
  separate gates and executor paths.
- Higher-timeframe derivation checks complete, consecutive UTC buckets. The
  base-interval getter returns the feed copy directly; that helper alone does
  not establish that every one-minute decision is on a completed candle.
- A generic `rsi_v5_core` reducer exists but is not called by OK RSI. Its
  lifecycle and hard-stop semantics are different; replacing OK RSI with it
  would silently change behavior. Reuse compatible utilities only after tests.
- The current migration contract explicitly preserves profit-only/no-force-sell
  buy/sell behavior and prevents later wallet deposits from adopting old basis.
- Independent review reproduced a gap in the exact position-executor shutdown
  method: a cancelled entry with 0.4 filled base produced zero held records and
  `EARLY_STOP`, because that method selects `is_filled` orders only. This isolated
  result does not prove permanent inventory loss across all reconciliation paths;
  it proves the proposed handoff cannot yet rely on that method alone.
- Existing SELL config construction recalculates the quantity from inventory
  and legacy slicing, ignoring its `amount` argument. Episode target amounts
  therefore require an explicit sizing boundary rather than that call unchanged.
- The second attachment cites submodule commit `d174e5e81543757e58122b5bc9ed30f79a6b5229`.
  The inspected owner HEAD remains `91372522a8e4bb178a959f1b0c42ec7276cf89ba`.
  Its code claims were checked against the latter, not assumed current from the
  attachment. `_get_total_position_value_usd()` in `hl_rsi.py:1423` marks held
  and filled active inventory to current prices. This is a market-exposure
  measure; the inspected buy admission path lacks a separate cumulative episode
  spend limit including outstanding buy reservations.
- `rsi_signals.py:131` computes the purported mean-reversion score solely from
  RSI distance from 50. `hl_rsi.py:2584` can count it in addition to oversold RSI.
  It is an extremeness transform, not a calibrated recovery probability.
- `rsi_signals.py:145` adds regime confidence positively to signal strength
  without taking regime direction. `hl_rsi.py:3340` uses that strength for the
  size multiplier; the existing optional scout path separately attenuates
  hostile context. Directionless confidence must not enlarge episode tranches.
- `_resolve_buy_entry_plan()` in `hl_rsi.py:847` can uplift an order to exchange
  minimum size/notional. The new all-in level cap must be checked after final
  price, quantization, and fee reserve; attenuation cannot be undone by uplift.
- Independent review executed the exact legacy sizing method with a zero size
  multiplier and a 30-quote configured entry: it returned 30 quote, because
  `hl_rsi.py:723` replaces zero with `1.0`. A zero factor must be an explicit
  no-request/no-executor decision, not a value routed through that fallback.

## Architecture decision

**ACCEPT WITH CHANGES.** Use an OK RSI-local episode policy, shared by both
modes, composed by the existing controller. Preserve the existing inheritance
initially and extract only responsibilities needed for this change. Do not
copy all of `hl_rsi.py` or turn it into the new distribution owner.

Alternatives considered:

1. Put the whole engine into `hl_rsi.py`: fewer initial adapter calls, but it
   expands an already large shared module and affects controllers outside scope.
2. Replace OK RSI with the existing generic RSI reducer: appealing reuse, but
   its lifecycle, exposure types, and unconditional hard-stop path do not match
   the required mode and profit contracts.
3. Compose a focused OK RSI policy with existing Hummingbot execution contracts:
   recommended; it keeps strategy changes local while retaining one execution
   authority and explicit integration tests.

Confidence is high in this boundary and provisional in any economic benefit.
Reverse the integration choice if tests prove the existing executor contracts
cannot perform a safe ownership handoff: document the smallest necessary
Hummingbot extension before proceeding. Do not create a competing order engine.

## File responsibilities

```text
controllers/directional_trading/
  ok_rsi.py                  existing public controller/config entry point
  ok_rsi_core/
    __init__.py              narrow public exports
    config.py                OK RSI config and validation
    market_context.py        closed-bar aggregation and causal context
    episode.py               state, inputs, outputs, pure transition function
    accumulation.py          dip setups, level admission, bounded capital requests
    inventory.py             attributed inventory projection and economics
    execution.py             reconcile targets; adapt to existing executors
    state_store.py           versioned episode/checkpoint persistence adapter
    trailing.py              extracted existing continuous trailing selector
```

These are responsibilities, not a requirement to create empty modules upfront.
Create each file when it contains implemented behavior. Keep legacy import
paths for `OKRSIControllerConfig`, the candle helper, and trailing helper valid.
Use module-qualified config imports in script modules so the real Hummingbot
loader does not discover the wrong config class.

`hl_rsi.py` supplies unchanged behavior for the legacy baseline. The candidate's
new accumulation admission and sizing are owned by `ok_rsi_core`. Narrow
hooks may be necessary to route OK RSI actions; any such hook must preserve
legacy behavior for other subclasses and have a regression test. New episode
policy and tuning remain in `ok_rsi_core`.

## One policy, separate instances and mode permissions

An episode is scoped to account/runtime identity, connector, pair,
controller ID, and episode ID. Attributed fills from the latest managed-flat
boundary define a buy/sell episode's actual inventory. Allocate its immutable
ID and reserve its first request before submission; an unfilled request does
not create fictitious inventory or spending. Sell-only begins with an explicit,
persisted inventory assignment; deposits do not enlarge it automatically.

| Phase | buy_sell permission | sell_only permission | Exit behavior |
| --- | --- | --- | --- |
| WATCH | Arm a qualifying dip setup under current risk gates | Wait for assigned inventory | None |
| SCOUT | Submit/reconcile the bounded first level | Not entered for order creation | Existing owner until safe handoff |
| ACCUMULATE | Admit only a fresh eligible level within all budgets | No new buy levels | Existing owner until safe handoff |
| DEFENSIVE | Hold inventory; block new buy requests | May expose the same defensive context; never buy | Preserve existing protection and ownership |
| EXHAUSTED | No level or usable budget remains; no new cycle | Not required for assigned inventory | Await recovery or explicit permitted risk handling |
| RECOVER | Block additional DCA while recovery is accepted | Observe assigned inventory for recovery/exit conditions | No mandatory break-even liquidation |
| MARKUP | Block buys | Never buy | Arm aggregate ratchet after exit ownership is acquired |
| DISTRIBUTE | Block buys | Never buy | Reduce target inventory and retain a runner |
| EXIT | Block buys | Never buy | Keep zero target; reconcile executable remaining quantity |
| COOLDOWN | Wait for a completed higher-timeframe reset | Episode finished; no automatic adoption | No immediate reentry |

Phase is a strategy lifecycle, not a claim that the true market regime is
known. Separate observable status records data health, exit owner, outstanding
handoff, profitability block, unavailable inventory, or exchange dust.
Such status must not erase episode state, reset its target, or release reservations.

```text
WATCH -> SCOUT -> ACCUMULATE <-> DEFENSIVE
                       |           |
                       +-> EXHAUSTED
                       |           |
                       +-----------+-> RECOVER -> MARKUP -> DISTRIBUTE -> EXIT
                                                                            |
WATCH <- completed higher-timeframe reset <- COOLDOWN <----------------------+
```

The arrows show typical paths, not a mandatory order. Any filled pre-markup
phase can enter RECOVER on the causal recovery predicate. Before markup,
recovery failure routes to ACCUMULATE only if a fresh eligible level and budget
remain; otherwise it routes to DEFENSIVE or EXHAUSTED. A defensive release
requires fresh valid stabilization and acceptable context, not elapsed time
alone. Exhaustion is retained across temporary defensive/data-health states.
Once distribution
ownership is acquired, the episode cannot return to accumulation. Failed markup
exits through EXIT/COOLDOWN. Existing protective actions retain their precedence.

Sell-only skips scout and accumulation orders: a reconciled assignment enters
RECOVER as an observation phase, or DEFENSIVE if required inputs are invalid.
RECOVER does not assert that the position is already profitable. Its buy budget
is zero in every phase. Flat completion of any filled episode enters COOLDOWN,
including one closed by legacy exits before markup; a freeze is never flat
completion. An entirely unfilled, cancelled scout may return to WATCH only after
its orders and reservations are reconciled, retaining the setup replay identity.

Sell-only shares state and exit rules but never gains buy permission, including
at direct executor construction. If cost basis is unavailable and profitability
is required, report `basis_unavailable`. With the existing sell-only setting
that does not require profit, use an explicit activation price as the extension
reference and report basis/PnL as unavailable. Do not call it break-even or profit.

## Market inputs and buying behavior

The exact baseline preserves its RSI confirmation, score, sizing, and exits.
The new accumulation candidate retains risk/cash/exposure vetoes and uses
separate depth, stabilization, regime, budget, and execution gates. RSI remains
an observable extremeness/reversal input. Neither low RSI nor a combined score
may override a veto. Removing the duplicate RSI score and introducing a scout
without rebound are separate candidate changes, not edits to the exact baseline.
Use `rsi_extremeness` in new candidate diagnostics. Preserve legacy serialized
field/import compatibility explicitly rather than renaming the shared helper
across unrelated controllers as part of this change.

The candidate observes completed 15-minute and one-hour context with one-minute
execution opportunities. Every context observation must include source interval,
close time, available-at time, warm-up coverage, and validity. Rolling breakout
highs exclude the current candle. Repeated one-minute ticks cannot count as
additional higher-timeframe confirmations. Missing, gapped, stale, future, or
unfinished data cannot advance a phase or loosen a ratchet.

The concrete transition predicates and their numerical thresholds must be frozen
in the implementation specification before changing behavior. The attachment's
20-hour breakout, drawdown quantiles, and ATR ladder are candidate inputs, not
adopted defaults.
Use synthetic fixtures to make the predicates observable; do not choose them
from the marked chart peak. Separate accumulation experiments from exit-policy
experiments as specified below; they cannot share a claim of unchanged fills.

## Accumulation and capital contract

### Persistent episode and level identity

Each episode persists the authorized all-in quote budget, filled quote spending,
buy fees, outstanding buy reservations, anchor and its source timestamp, level
definitions, level request/attempt IDs, actual fills, last fill price/time,
remaining inventory basis, adverse/favorable excursion, current eligibility,
and invalidation reason. Freeze, executor replacement, restart, and hot reload
preserve those values. They never reset the ladder or authorize fresh capital.

Each level has one authorized opportunity and an all-in cap. Partial fills and
bounded cancel/replace attempts belong to that opportunity; literal one-fill
per level would be incompatible with normal exchange partial fills. Retry only
the unfilled remainder of the same approved target before expiry and after
uncertain order status is reconciled. A lower price does not replenish consumed
level capacity. No-fill expiry, partial-fill finalization, skipped levels, and
unused attenuated allocation are explicit states, not permission to start over.
Persist `(episode_id, level_id, authorization_revision)` independently of
`executor_id`; existing scout/runner role labels identify roles, not economic
levels. Each order attempt and fill links back to the durable authorization.

### Two independent capital limits

Keep these separate: gross episode buy spending, net acquisition cash outlay
after sells, current marked exposure, remaining cash, and realized/unrealized
PnL. All budget values use the actual quote currency; do not silently treat
USDC amounts as interchangeable with USD cash.

```text
spent_all_in = cumulative_actual_buy_quote + cumulative_buy_fees_quote_equivalent
gross_committed = spent_all_in + pending_buy_all_in_reservations
remaining_episode_budget = authorized_quote_budget - gross_committed

new_request_all_in <= remaining_episode_budget
new_request_all_in <= remaining_authorized_level_cap
current_mark_exposure + pending_buy_exposure + proposed_buy_exposure <= mark_cap
```

Falling prices cannot reduce `spent_all_in`; partial sells cannot reopen this
episode's budget. Fees are counted once: do not include them in both buy quote
and the fee term. A signed, attributable accounting correction is distinct from
an ordinary price change. Missing historical spend/fees on restoration blocks
new additions rather than inferring a fresh budget from wallet cash or basis.

The available-wallet test is additional. Exchange-reserved quote is normally
already excluded from available balance; subtract only locally outstanding
reservations not represented in that snapshot, with explicit snapshot alignment.
Unknown dispatch/order state retains its reservation. Partial fills move their
actual cost from reserved to spent atomically, retaining a conservative reserve
for the rest. Cancellation releases only the confirmed unfilled remainder.

For example, with a 500-quote budget and 100 quote already spent, a price drop
that reduces inventory value to 75 leaves 400 quote before fees/reservations,
not 425. This is an accounting invariant, not a claim of better returns.

### Severity, path, and stabilization

Freeze a causal pre-dip anchor and a versioned ladder before the first request.
The first diagnostic uses cumulative levels in fixed volatility units; a
point-in-time historical drawdown-quantile ladder is a separately registered
challenger. Calibration data must end before use, record asset/venue, horizon,
sample count and estimator, and be frozen for the episode. Missing or inadequate
calibration produces unavailable input, never fitted-on-test quantiles.

Log drawdown divided by volatility requires dimensionless return volatility
over the same declared horizon. ATR is a price quantity and must be normalized
to price if substituted. Record denominator floors and units. Freeze the ladder's
volatility scale so shrinking volatility alone cannot unlock another level;
current volatility may attenuate size or veto buying.

Buy admission uses current executable ask including a conservative fill-cost
bound, never a historical candle low or stale maker quote as a guaranteed fill.
Later levels must be materially deeper than previous purchases, pass the
last-fill spacing test, and pass a fee-aware projected basis-improvement gate.
Arming may track completed-candle lows; execution requires the current price to
remain inside the authorized depth/price band after rebound. Recheck before
each order attempt. If price rebounds out of that band, expire/skip the request.

Candidate stabilization: arm the reached level; track its low; require two
subsequent completed one-minute bars without a new low and a rebound of at least
`max(0.25 * ATR_1m, 2 * ask_bid_spread)` before ten completed bars elapse. These
numbers are retained as test hypotheses, not enabled defaults. Require valid
contemporaneous quotes; resetting the low restarts confirmation, not budget.
The no-rebound scout is a separate ablation and never bypasses data, liquidity,
hostile-regime, or risk vetoes. A gap through several levels cannot submit a
stack of purchases: select at most one deepest eligible level, mark bypassed
levels skipped, and do not add their allocations to it.

DEFENSIVE blocks new buy requests during a cascade, sufficiently hostile trend,
abnormal spread/impact, or invalid required data. Request cancellation of pending
buy remainders when permitted, retain reservations until acknowledged, and
accept/reconcile late fills. A manual kill switch retains its existing semantics;
this state cannot bypass it. EXHAUSTED blocks additions when every opportunity
has finished/skipped or no admissible all-in allocation remains, even if a mark
cap later has room. Invalidation never implicitly liquidates existing inventory.

### Bounded sizing and basis improvement

Use configured episode budget, not the attachment's universal 500 value. A
candidate allocation is `[10%, 15%, 20%, 25%, 30%]`; equal weights are its simple
comparator. Allocations are all-in caps including fees, not notionals with fees
added outside a 100% budget. Within each cap:

```text
request_all_in = level_all_in_cap * regime_factor * volatility_factor * quality_factor
0 <= each_factor <= 1
```

Hostile direction cannot make the factor larger as confidence increases.
Cascade/invalid data implies no new request. Unused attenuated allocation stays
unused for the episode; no later redistribution is implicit. Current dynamic
signal upsizing is bypassed only in the candidate, with its reason visible.
Any zero factor rejects before the legacy entry planner or executor factory;
direct construction must enforce the same veto. Legacy `value or default`
fallbacks and minimum-order uplift cannot turn zero into a positive purchase.

Convert the all-in request into a price-bounded, quantized order and conservative
fee reserve. Recheck all caps on the final order. If exchange minimums would
exceed the attenuated target or any cap, skip the order with a reason; do not
silently uplift it. Unexpected fee/fill overruns are recorded as a breach and
block further additions; never clamp actual spending to make the cap appear met.

Compute projected remaining basis from existing net base and its fee-inclusive
remaining acquisition cost, plus proposed all-in acquisition cost and proposed
net base after base-asset fees. Do not double-add old fees or ignore base fees.
Admission requires both a meaningful last-fill gap and configured minimum basis
improvement after expected friction. Missing basis blocks a scale-in. A lower
basis does not prove improved expected PnL: additional quantity increases downside
exposure, which is assessed separately under adverse price and cost scenarios.

### Request boundary and scope

`accumulation.py` returns an immutable capital request, not an order: episode,
level and attempt identity; final quote/base/fee limits; current price ceiling;
anchor/calibration/config/snapshot hashes; individual gate reasons; expiry;
basis before/after; and budget before/after. `execution.py` validates, durably
reserves, and dispatches through Hummingbot only when all existing risk gates
permit it. Inspectable request records are not new trading authority.

No new portfolio optimizer, cross-asset allocator, RL service, independent risk
service, or research framework is included. Preserve any existing upstream risk
vetoes. Controller-local approval cannot claim portfolio concentration control;
cross-controller shared cash reservations require an existing runtime authority
or a separately scoped owner change, never independent claims on the same cash.
External studies cited by the attachment were not reviewed for this amendment
and are not being used as evidence of crypto performance.

## Inventory, target, and accounting contract

Hummingbot order/fill records remain truth. `inventory.py` builds a projection
with exact source IDs; it does not invent fills or become a second wallet ledger.
Required values include remaining attributed base, outstanding buy quantity,
remaining close reservations, acquisition cost, net sell proceeds, fees in their
actual currencies, and any explicit initial inventory assignment.

Base-asset fees reduce actual base. Quote/third-asset fee accounting needs an
explicit value/conversion source. Unknown fees cannot be silently treated as zero.
Avoid double-counting the same fill through active executors and held positions.

At distribution handoff, freeze `distribution_start_quantity`. Each ladder target
is an absolute quantity based on that snapshot. Under late fills, retain the
target and reconcile the extra quantity; do not silently enlarge the runner.
For an ATR ladder, also freeze the extension reference and ATR denominator at
activation so falling volatility alone cannot trigger another stage. Current
completed-bar ATR may influence the runner subject to the monotonic floor.
Crossing multiple stages in one event selects one final target, not overlapping
orders for every crossed stage.

```text
target[t] <= target[t-1]                 during MARKUP/DISTRIBUTE/EXIT
needed_close = max(0, actual_attributed - target - pending_close)
new_close <= min(needed_close, available_attributed_unreserved)
```

`actual_attributed` already includes executed sells; `pending_close` includes
only the unfilled reserved remainder. Available wallet balance already excludes
exchange locks, so reservations cannot be subtracted twice. Unknown pending
orders retain reservations until reconciled. Exchange quantization and minimum
notional may produce an explicit dust residual; dust is not zero inventory.

Keep remaining-inventory cost/break-even distinct from whole-episode equity:

```text
episode_net_value = net_realized_sell_proceeds + remaining_base * executable_bid
                    - total_acquisition_cost - estimated_remaining_close_cost
```

Fees must occur once in these terms. Initial attributed inventory cost is
included in acquisition cost when known. The equity peak/floor only ratchets
after marking data is valid. Late fee corrections are explicit accounting
events, not evidence of a price reversal or an excuse to silently lower a floor.

## Execution ownership, persistence, and terminal behavior

Exactly one discretionary exit owner may reserve a quantity: legacy executor,
held-bag recovery, or episode policy. The episode layer cannot simply suppress
new SELL signals while existing position executors continue trailing.

The handoff is explicit: block new entries; request existing owners to stop
with position preservation where supported; wait for final order/cancellation
status, late fills, released reservations, and held-position materialization;
reconcile; then grant the episode policy exit ownership. Existing pending closes
must settle before new episode closes can overlap their inventory. Handoff
failure blocks episode activation and is visible; it cannot cause force-selling.

The partial-fill shutdown observation is a required implementation prerequisite.
Add an executor/orchestrator integration regression for cancelled partially
filled entries and already-partially-closed positions. If the gap is confirmed
there, make the smallest Hummingbot position-hold repair with complete actual-fill
records and duplicate protection, in a separate reviewable slice. This is a
necessary supporting-file change, not permission for a broad engine refactor.
Also adapt OK RSI's validation that currently mandates per-entry trailing so
only a fully integrated episode mode may select another exit owner; legacy
configurations retain that requirement. Do not bypass it during a handoff.

Persist episode ID, phase, config/schema hash, processed event identity,
distribution start quantity, target revision, high-water/floor values, cooldown
marker, inventory assignment, ownership handoff and outstanding command identities.
Persist the entire accumulation authorization, spending/reservation ledger,
level states, armed-low/confirmation/expiry state, and calibration identity too.
Persist a command reservation before dispatch. On restart reconcile uncertain
commands against actual executors/orders before retrying. A repeated target
revision may need another order after confirmed partial fill/cancellation, so
revision identity alone is insufficient: persist each execution attempt and
its remaining reservation. Never claim exactly-once network submission.
Hot reload must follow the same restore/reconcile gate as process restart:
copying reports and `processed_data` does not restore private policy state.
An incompatible config/schema change requires an explicit state migration and
cannot reset an open episode to WATCH.

Preserve the current buy/sell profit-only rule. A terminal target of zero does
not override it. If the executable exit would violate the configured aggregate
profit guard, remain in EXIT with `profit_guard_blocked`. Do not reactivate buys
to repair the basis. This deliberately cannot guarantee eventual flattening.
Gap risk also prevents any trail from guaranteeing a realized profit floor.

Patient tranche execution and faster terminal execution may use supported
Hummingbot order strategies. An unconditional timed MARKET fallback is not part
of the proposed default: it could defeat the price/profit floor. If a market
order is allowed by an explicitly different policy, its fill cannot be guaranteed
above that floor. Preserve manual kill-switch and risk semantics.

Enter COOLDOWN only after no owned actionable inventory or pending orders remain.
An explicit dust state remains distinguishable from flat. A later sell-only
assignment creates a new episode; a later deposit by itself does not.

## Delivery and validation sequence

1. Extract current pure helpers/config with unchanged defaults and compatibility
   exports. Run existing controller and actual loader regressions. This proves
   refactoring separately from strategy changes.
2. Implement attributed accounting, independent spending/exposure rails,
   durable level authorizations, reservations, and restart reconciliation using
   native Hummingbot records. Prove that price declines and freeze cannot
   recreate budget, and that zero attenuation cannot construct an executor.
3. Implement the pure episode policy, mode permissions, causal accumulation
   predicates, and exit ownership handoff with deterministic fixtures. Include
   the existing partial-fill handoff prerequisite. No trading enablement before
   these gates are complete.
4. Connect both modes in an isolated source checkout with the candidate disabled
   in existing operator configs. Exercise real controller action generation
   against local fake connectors and deterministic executor/fill events.
5. Run the sequential experiment contract below before claiming improvement.

### Sequential experiment contract

First reproduce the exact version-bound baseline, including current dynamic
sizing, order lifecycle, fees, inventory and exits. Disabling dynamic sizing is
a candidate, not baseline cleanup. If reproduction violates required execution
or accounting invariants, retain its diagnostic trace and hold economic
selection until a separately identified corrected reference is admitted.

Then change one factor per comparison:

1. Add gross spending/fee/pending reservations alongside the existing mark cap,
   leaving signals, sizing requests, and exits unchanged except for new vetoes.
2. Add persistent level identities; prove no reuse after freeze/restart. Keep
   the same signal and exit rules. New entry vetoes may change realized fills.
3. Remove the duplicate RSI extremeness score contribution in a separately
   named ablation; freeze gate thresholds before evaluation.
4. Compare current spacing with a simple cumulative frozen-ATR ladder using
   equal all-in level allocations. A causal quantile ladder is an optional
   preregistered challenger only when its calibration evidence is sufficient.
5. Compare equal allocation with the proposed 10/15/20/25/30 schedule. A
   front-loaded schedule is a separate predeclared question, not an automatic
   search obligation or a named operator profile.
6. Isolate stabilization, including whether the first scout needs a rebound.
7. Isolate regime/volatility attenuation and removal of dynamic size expansion.
   Separate these effects further if a combined change prevents attribution.
8. Freeze the accumulation candidate under the declared selection rule. Replay
   its identical entry fills for exit-policy attribution: existing exits,
   simple aggregate trailing, and one staged distribution plus runner.
9. Run the complete interacting controller with real entry/exit permissions and
   cash availability. A fixed-entry replay can conceal changed reinvestment,
   cooldown and feasibility; it cannot establish full-controller economics.

Conservative execution costs, spread, fee reserves, latency, partial/no fills,
and order acknowledgement are requirements from the first economic comparison,
not an optional realism step added after choosing a winner. Idealized fills are
diagnostic upper bounds only. Run declared cost stress on candidates, including
1.0/1.5/2.0 times modeled friction where justified. Compare fee/slippage stress
without changing the data or favorable-fill assumptions.

The reference set includes current OK RSI, no-DCA RSI, calendar DCA, a simple
equal-price ladder, and buy-and-hold with the same initial capital and horizon.
Use these as interpretable controls; do not cross-product all ladder/size/gate
choices. Freeze the trial budget and selection/invalidation rules, account for
multiple comparisons and overlapping episodes, and reserve a later untouched
holdout. The known ETH chart is a development example. BTC/ETH/BNB results use
separate 1x cash books and must report failed/nonrecovering episodes too.

Required regressions: duplicate/out-of-order events, repeated same bar, missing
HTF data, false breakout/recovery failure, monotonic targets/floors, late buy
fills, base fees, partial sells, cancel/fill races, rejected/unacknowledged orders,
restart at each handoff/dispatch boundary, two controllers with one asset,
unrelated wallet deposits, absent basis, exact legacy 20-quote sizing boundary,
dust, profit-guard breach, cooldown/reentry, and kill-switch behavior.
Accumulation regressions also cover: mark declines without budget reset,
unfilled buy reservations, reservation-to-spend transfer on partial fill,
unknown order status, zero factors, negative/non-finite factors, minimum-notional
uplift rejection, fee-inclusive all-in caps, base/third-asset fees, spent-level
reuse after freeze, gaps crossing multiple levels, expiry with late fills,
out-of-band rebound prices, basis gate failure, hostile confidence increases,
exhaustion surviving recovery failure, calibration lookahead/units, and explicit
per-mode exclusion of BUY actions in every state.

Existing owner checks to retain (run from `hummingbot/` in its environment):

```bash
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -W error -m pytest -q test/test_ok_rsi_controller.py test/test_hl_rsi_controller.py
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -W error -m pytest -q test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py
git diff --check
```

These commands were identified, not run as part of this design-only review.
Economic comparisons must bind controller/config/data/runner versions, preserve
each asset's 1x cash accounting, include fees/spread/slippage/latency/no-fill and
partial-fill assumptions, value all residual inventory at the same horizon,
and keep development episodes separate from later holdout evidence. An exit
VWAP over only the sold fraction cannot hide an unsold remainder. Do not repeat
earlier rejected parameter searches without a new admitted hypothesis.

### Lifecycle metrics

Report net NAV change and return on the entire authorized cash allocation,
including idle cash and marked residual inventory at a common terminal time.
Also report acquisition basis, peak gross commitments, cash remaining at maximum
adverse excursion, drawdown, recovery duration, unresolved inventory/bag age,
per-level cost and basis improvement, quantity and friction, exit VWAP, fill
rates, and capital utilization. Unknown sell-only acquisition basis remains
unavailable, not zero.

Distinguish `gross_spend_days` (integral of cumulative historical all-in buys
while an episode remains open) from `capital_at_risk_days` (integral of positive
net acquisition cash outlay after realized net sales) and marked-exposure-days.
Define `underwater_area` as the time integral of negative net episode PnL,
including remaining inventory at executable valuation and close costs. State
units and denominator. Never call reduced basis an incremental profit estimate.
Per-level marginal PnL requires a controlled counterfactual with the same other
policies; simple allocation of realized PnL to fills is attribution only.
Evaluate additional decline scenarios such as 10/20/40% as stress, not forecast.

## Authority, review, and rollback

Independent review used source tracing plus isolated executions of the exact
shutdown and sizing methods. It did not run a full controller/connector integration suite.
The five reviewed hazards (partial-fill handoff, competing exit owners,
sell-only admission, restart/hot reload state, and ignored target sizing) are
included above as required implementation checks.
The second review also confirmed marked-exposure versus spend semantics, omitted
pending exposure in that measure, zero/default and minimum-order sizing hazards,
nonpersistent role labels, directionless confidence sizing, and the need to
preserve an unchanged baseline. These are included in the accumulation contract.

This artifact changes no controller, config, database, service, or order. The
source checkout contains unrelated work that must be preserved. No economic
improvement, deployment readiness, or running-version parity has been verified.

Review this design before implementing the new architecture, as required by
the brainstorming skill. A subsequent self-contained implementation plan must
freeze the exact predicates and acceptance fixtures; the attachment is not that
approval. Keep all live/paper/canary starts, migrations, external writes, and
orders outside local implementation authority.

Rollback of the design is simply to discard this standalone review artifact.
For implementation, an episode may not switch back to legacy exits while any
episode order or reservation is outstanding. Reconcile and complete an explicit
owner transfer before restoring the previous policy; disabling a flag alone
is not a safe in-flight rollback.
