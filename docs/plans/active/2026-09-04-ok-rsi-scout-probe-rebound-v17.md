# OK RSI scout probe and rebound completion v17

## Objective and completion bar

Test one causal entry-placement change on the current V15 lifecycle: preserve
every qualifying scout opportunity with an immediate bounded probe, then
commit the rest of the already-authorized scout quote only after the existing
local stabilization and rebound evidence. Complete the iteration when native
Hummingbot behavior, durable receipts, bounded BNB replay, exact May-August
BTC/ETH/BNB owner replay, location and participation audits, and a verified
detailed Plotly report produce an explicit accept, reject, or hold verdict.

Success must come from better placed inventory and better lifecycle exits. A
lower buy count, materially lower filled quote, or smaller exposure alone
cannot establish improvement.

## Ownership, scope, and authority

- Hummingbot owns the probe/remainder state, quote cap, setup clock, order
  authorization, checkpoint, restart behavior, and shared `buy_sell` and
  `sell_only` semantics.
- Quants Lab owns exact owner replay, observed-trade execution, costs,
  continuous spot accounting, causal audits, gates, and report generation.
- Research OS may consume sealed structured evidence. It does not copy the
  controller or execution state machine.

Authority is local research only. Paper, canary, live, exchange, wallet,
deployment, push, pull request, order, and capital actions are forbidden.

## Evidence and frozen hypothesis

- C0 produced 224.321726 USDC with 33 buys and -9.231492 percent worst
  drawdown. V15 produced 235.534740 USDC with 36 buys and the same broad
  drawdown class, and is the current lifecycle development leader.
- V4 delayed the whole scout until a rebound and produced 269.748841 USDC with
  -1.833007 percent drawdown, but it retained only 75.76 percent of entries,
  18.18 percent on BTC, and 26.67 percent of matched C0 scout opportunities.
- Recovery-only confirmation and reference fallback also lost useful
  opportunities. The evidence does not support another all-or-nothing entry
  gate.
- Two late-August BNB V15 scale-ins deployed 375-500 USDC before later, lower
  C0 scouts. More generally, the state machine needs a way to retain exposure
  to a signal without committing the full scout before the path stabilizes.
- The 50 percent probe fraction is frozen before replay. It matches the
  controller's existing hostile-scout attenuation and is a single symmetric
  diagnostic split, not a fitted pair or price threshold.

Add opt-in `SCOUT_PROBE_REBOUND_V1` on top of the exact V15 stack:

1. At every otherwise valid initial scout authorization, compute the exact
   preexisting full quote cap before quantization. Authorize 50 percent as an
   immediate probe at the exact V15 reference and adaptive-maker price.
2. Persist the original full cap, probe request identity, setup timestamp, and
   causal rolling low. Probe plus remainder spend and reservations may never
   exceed the original cap or the episode budget.
3. If the probe obtains attributed inventory, use the existing two no-new-low
   bars, 0.25 one-minute ATR rebound floor, two-spread floor, and ten-bar setup
   lifetime. Authorize the unspent remainder only when the completed-candle
   reference price confirms that rebound and the resulting aggregate basis
   passes the existing basis-improvement rule.
4. Invalid or gapped data, cascade, hostile, reset, recovery, buy veto, setup
   expiry, or zero probe fill abandons only the remainder. The funded probe and
   ordinary lifecycle remain explicit and unchanged.
5. Preserve the V15 adaptive reservation and one-child fallback rules for each
   authorized request. A remainder or fallback expiry cannot reopen a level
   that already has an attributed probe fill.
6. Keep one unresolved request maximum, actual-fill accounting, quote and mark
   caps, exchange minima, no same-decision double authorization, and immutable
   completed-order tombstones.
7. Leave scale-in levels, distribution targets, deep-recovery rotation,
   cooldown, and sell execution unchanged. The option remains disabled by
   default and creates no buy in `sell_only`.

## Frozen development gates

- exact current-source V15 behavior with the option disabled and exact V15
  decisions through every first changed scout authorization;
- one probe maximum per opportunity, exact V15 timestamp and selected price,
  50 percent pre-quantization quote cap, and complete setup provenance;
- one confirmed remainder maximum, only after the causal rebound and basis
  gates, with total level-0 spend plus reservation no greater than the original
  full scout cap;
- zero remainder after unsafe context, timeout, zero-fill probe, or an
  unresolved request; zero duplicate continuation after restart or late
  receipts;
- retain at least 100 percent of V15 filled scout opportunity roots and filled
  buy count in aggregate, and at least 90 percent of V15 filled buy notional in
  aggregate and 80 percent on every asset;
- retain at least 90 percent of C0 filled opportunity identities in aggregate
  and 80 percent on every asset;
- improve quote-weighted matched scout basis versus V15, reduce
  quote-weighted 24-hour scout adverse excursion by at least 10 percent, and
  leave no asset more than 10 percent worse;
- keep summed average deployment and maximum single-asset deployment at or
  below V15 and at or below 1.05 times C0;
- leave zero unresolved requests, native hold rows, forbidden underwater
  sells, overlapping sell reserves, or continuous-account errors;
- exceed V15 aggregate net PnL and achieve at least 10 percent more aggregate
  net PnL than C0 with worst drawdown no more than 0.25 percentage points worse
  than C0, or reduce worst drawdown by at least 1 percentage point while
  retaining at least 95 percent of V15 PnL.

If every gate passes, run 20-bps cost, strict-touch/no-fill, plausible venue
minimum, and sealed untouched-transfer controls. If any gate fails, do not tune
the fraction, rebound, timeout, RSI, ATR, pair, size, or gate on May-August.

## Execution plan

- [x] Add RED native tests for exact probe cap and price, rebound remainder,
  unsafe abandonment, timeout, basis guard, zero/partial fill, fallback expiry,
  restart, default-off identity, and inert `sell_only` behavior.
- [x] Implement the smallest config, reducer, runtime status, and executor
  receipt change while keeping the public `ok_rsi.py` wrapper thin.
- [x] Run focused and broad OK RSI native tests, Ruff, and shadow-diff checks.
- [ ] Bind the option and receipts into the Quants Lab owner runner and add
  frozen participation, quote-cap, path-quality, and gate tests.
- [ ] Run a bounded BNB late-August replay and inspect every changed action and
  fill before the full window.
- [ ] Replay exact May-August BTC/ETH/BNB baseline, C0, V15, and V17 at 15 bps
  from one final native and research source identity.
- [ ] Apply the frozen gate, run controls only if it passes, and render a
  self-contained report with exact probe, remainder, scale-in, and distinct
  sell markers plus state, basis, targets, NAV, drawdown, exposure, and cash.
- [ ] Verify all ledgers, marker identities, hashes, and report artifacts;
  record the verdict, limitations, rollback, and next causal question.

## Validation and rollback

Use the Hummingbot environment for reducer/runtime behavior and native replay,
and the Quants Lab Plotly environment for analysis and report verification.
Bind controller, config, data, context, execution, parent, input-spec, and
report hashes. Reconcile every order outcome and plotted marker.

Rollback by reverting only the V17 Hummingbot and Quants Lab commits. V15
remains disabled and is the current research comparator. Existing reports and
all unrelated workspace changes remain untouched.
