"""Tidewater protected runner: native lifecycle state (opt-in `exit_runner`).

Policy. The pure functions of `runner.py` (a42150506's research policy, unchanged). When a tranche's
native trailing exit fires, `runner_split` keeps `runner_fraction` of its owned base as the runner;
the rest is the harvest sale. The runner's floor is `runner_floor`; each closed one-minute bar after the
harvest takes `runner_bar` (floor first, the exit check on the close, then the high water). An exit is
placed at `runner_exit_fill(bid, cost_floor)`: at the bid when the bid is at or above the cost floor,
else as a resting limit at the cost floor. No stop-loss; never a sale below the cost floor.

Ownership. The runner stays inside its tranche's own native executor (`runner_execution.py`): the
executor sells only the harvest part, returns to RUNNING owning the runner base, and later closes it
with a native limit order. There is no second inventory: the executor's own receipts carry the runner.

Cost floor. `runner_cost_floor(cost basis, fee fraction)` as in the research owner: the cost basis is
the tranche's gross entry price (executed quote / executed base) and the fee fraction its native
round-trip fee rates (entry fees paid + the expected exit fee). It is never below the native after-fee
no-loss price (relevant only at a zero margin). The native trailing exit's protected floor (after-fee
breakeven raised by activation - delta) applies to the harvest part, not to the runner, whose floor is
the research policy's.

Daily ATR. `runner_atr_pct` over whole UTC daily bars, built by `runner_daily_bars` from the native
one-minute frame the runtime observes. The frame holds 1,500 minutes, so a day is captured shortly
after it completes and stored (RUNNER_DAY); the trail applies once `atr_days` days are stored. A fresh
database therefore has no ATR trail for its first `atr_days` days (the floor is the cost floor).

Durability. The split, every floor/high-water/phase change and every daily bar are stored under
explicit keys before they take effect (receipts.py, RUNNER and RUNNER_DAY), and a restart rebuilds
from them. A bar that changes nothing is not stored; after a restart the minutes since the last stored
state are replayed from the frame and give the same result. A failed write keeps the previous state
in force; the next tick re-sends exactly the same record (same key, same payload) before anything new,
so a write that committed but lost its acknowledgement is confirmed instead of being refused by the
event store as a different fact. Until the durable state is read no runner decision is taken: a
harvest waits (its position is kept) and no runner exit is placed.

Native boundaries. A runner is not demoted to the held bag by the bag line: Tidewater drops the bag
logic's keep-position stop for a tranche that carries a runner (an operator or shutdown stop still
demotes it). Tidewater's own pacing reads (V4.1 spacing zones, the tide-gate reference) leave runners
out, as the candle research owner does. Meridian's sealed planner still counts a runner's executor as
an active tranche (buy-gap reference, tranche slots); that difference is reported, not hidden.
"""
from decimal import Decimal

import numpy as np

from controllers.generic.modular_spot_core.domain import DataUnavailable, PolicyError
from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction

from .receipts import RUNNER, RUNNER_DAY, read_receipts
from .runner import (
    DAY_SECONDS,
    DailyBar,
    RunnerLot,
    runner_atr_pct,
    runner_bar,
    runner_cost_floor,
    runner_daily_bars,
    runner_exit_fill,
    runner_split,
)

OPEN, EXIT = 'open', 'exit'
MINUTE = 60
# Frame and recorder failures hold the runner step of one pair; they never fault a decision.
TICK_ERRORS = (DataUnavailable, PolicyError, ArithmeticError, KeyError, TypeError, ValueError, RuntimeError)
DECIMALS = ('runner_base', 'sold_base', 'tranche_base', 'cost_basis', 'cost_floor', 'harvest_price', 'high_water',
            'floor', 'fee_fraction')


class RunnerHold:
    """A runner decision cannot be taken yet (durable state unread or unwritable); retried next tick."""

    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return f'RunnerHold({self.reason})'


def _encode(state):
    return {k: str(v) if isinstance(v, Decimal) else v for k, v in state.items()}


def _decode(payload):
    state = {k: payload[k] for k in ('executor_id', 'pair', 'phase', 'last_minute', 'seq', 'gaps', 'harvested_at')}
    state.update({k: Decimal(payload[k]) for k in DECIMALS})
    return state


def _lot(state):
    return RunnerLot(state['runner_base'], state['cost_basis'], state['cost_floor'], state['high_water'],
                     state['floor'])


class RunnerBook:
    def __init__(self, owner, cfg, receipts):
        self.owner, self.cfg, self.receipts = owner, cfg, receipts
        self.lots = {}  # executor id -> runner state (open or exiting)
        self.skipped = {}  # executor id -> reason no runner was kept
        self.closed = {}  # executor id -> how its executor ended
        self.days = {}  # pair -> {UTC day start: DailyBar}
        self._captured = {}  # pair -> latest completed day already looked for in a frame
        self._atr = {}
        self._frames = None
        self.loaded = False
        self.fault = None
        self.status = {}  # pair -> latest runner step reason, for reporting
        self.kept_stops = 0  # bag-line demotions of runner tranches withheld
        self._pending = {}  # executor id -> (code, event id, state) whose write is unconfirmed; re-sent unchanged

    # ------------------------------------------------------------------ durable state
    def load(self):
        """Rebuild once per process from the stored records. False (retried next tick) when unreadable."""
        if self.loaded:
            return True
        try:
            records = read_receipts(self.receipts.recorder(), self.owner.config.id)
        except Exception as exc:  # report only; every runner decision holds until the read succeeds
            self.fault = f'runner_load:{type(exc).__name__}:{exc}'[:200]
            return False
        lots, skipped, closed, days = {}, {}, {}, {}
        for r in records:
            if r['kind'] == RUNNER_DAY:
                days.setdefault(r['pair'], {})[int(r['start'])] = DailyBar(
                    int(r['start']), Decimal(r['high']), Decimal(r['low']), Decimal(r['close']),
                    None if r['previous_close'] is None else Decimal(r['previous_close']))
            elif r['kind'] == RUNNER:
                eid, code = r['cut'], r['code']
                if code in ('created', 'state'):
                    lots[eid] = _decode(r)
                elif code == 'skipped':
                    skipped[eid] = r['reason']
                elif code == 'closed':
                    closed[eid] = r['close_type']
        for eid in closed:
            lots.pop(eid, None)
        self.lots, self.skipped, self.closed, self.days = lots, skipped, closed, days
        self._atr = {}
        self.loaded, self.fault = True, None
        return True

    def _persist(self, code, eid, pair, payload, event_id, now):
        return self.receipts.persist(RUNNER, pair=pair, cut=eid, code=code, payload=payload, now=now,
                                     event_id=event_id)

    def _commit(self, code, eid, state, now):
        """Store `state` under its explicit key; True once stored. An unconfirmed write is kept and re-sent
        unchanged by `_confirm` (it may have committed without its acknowledgement)."""
        event_id = f'tidewater:runner:{eid}:created' if code == 'created' else (
            f'tidewater:runner:{eid}:state:{state["seq"]}')
        self._pending[eid] = (code, event_id, state)
        return self._confirm(eid, now)

    def _confirm(self, eid, now):
        """Re-send the unconfirmed write of `eid` (if any); on success its state takes effect. True when
        nothing is unconfirmed."""
        if eid not in self._pending:
            return True
        code, event_id, state = self._pending[eid]
        if not self._persist(code, eid, state['pair'], _encode(state), event_id, now):
            return False
        del self._pending[eid]
        self.lots[eid] = state
        return True

    def _now(self):
        return float(self.owner.market_data_provider.time())

    # ------------------------------------------------------------------ executor surface
    def runner(self, eid):
        """The runner state of a tranche, None when it carries none, or a RunnerHold."""
        if not self.load():
            return RunnerHold('runner_state_unread')
        return self.lots.get(eid)

    def harvest(self, facts):
        """The tranche's trailing exit fired: keep a runner (stored before it takes effect) or record why not.

        `facts` are the executor's native values at the trigger (runner_execution.harvest_facts). Returns
        the runner state, None (no runner: the native full exit proceeds) or a RunnerHold.
        """
        if not self.load():
            return RunnerHold('runner_state_unread')
        eid, pair = facts['executor_id'], facts['pair']
        if eid in self.lots:
            return self.lots[eid]
        if eid in self.skipped:
            return None
        now = self._now()
        if eid in self._pending:  # the split was decided and sent; only its confirmation is outstanding
            return self.lots[eid] if self._confirm(eid, now) else RunnerHold('runner_write_failed')
        split = runner_split(self.cfg, amount=facts['amount'], price=facts['price'], amount_step=facts['amount_step'],
                             minimum_notional=facts['minimum_notional'])
        reason = split.skipped
        if reason is None and split.runner_base < facts['minimum_order']:
            reason = 'runner_below_min_order'
        elif reason is None and split.sold_base < facts['minimum_order']:
            reason = 'sold_below_min_order'
        if reason is not None:
            # Report only: a lost skip record changes nothing (the split is recomputed the same way).
            self._persist('skipped', eid, pair, {'reason': reason, 'tranche_base': str(facts['amount']),
                                                 'harvest_price': str(facts['price'])},
                          f'tidewater:runner:{eid}:skipped', now)
            self.skipped[eid] = reason
            return None
        if facts['no_loss_floor'] is None or facts['fee_fraction'] is None or facts['cost_basis'] is None:
            return RunnerHold('runner_cost_floor_unavailable')
        cost_floor = max(runner_cost_floor(self.cfg, cost_basis=facts['cost_basis'],
                                           fee_fraction=facts['fee_fraction']), facts['no_loss_floor'])
        state = {'executor_id': eid, 'pair': pair, 'phase': OPEN, 'last_minute': self._last_closed_minute(pair, now),
                 'seq': 0, 'gaps': 0, 'harvested_at': now, 'runner_base': split.runner_base,
                 'sold_base': split.sold_base, 'tranche_base': facts['amount'], 'cost_basis': facts['cost_basis'],
                 'cost_floor': cost_floor, 'harvest_price': facts['price'], 'high_water': facts['price'],
                 'floor': cost_floor, 'fee_fraction': facts['fee_fraction']}
        if not self._commit('created', eid, state, now):
            return RunnerHold('runner_write_failed')
        self.owner.logger().info(f'Tidewater {pair} runner kept executor={eid} runner_base={split.runner_base} '
                                 f'sold_base={split.sold_base} cost_floor={cost_floor} high_water={facts["price"]}')
        return state

    def exit_order(self, eid, bid, floor):
        """(kind, price) of the runner's exit at this bid: MARKET at the bid, or LIMIT at `floor` (the cost
        floor, or the harvest part's protected floor when that part is still unsold and higher)."""
        state = self.lots[eid]
        if floor < state['cost_floor']:
            raise PolicyError('runner exit floor below the cost floor')
        kind, price = runner_exit_fill(next_close=bid, cost_floor=floor)
        # Report only (the order itself is the native record); one receipt per distinct exit order.
        self.receipts.record(RUNNER, pair=state['pair'], cut=eid, code='exit_placed', dedupe=(kind, str(price)),
                             payload={'fill_kind': kind, 'price': str(price), 'bid': str(bid), 'order_floor': str(floor),
                                      'cost_floor': str(state['cost_floor']), 'floor': str(state['floor'])},
                             now=self._now())
        return kind, price

    # ------------------------------------------------------------------ controller tick
    def tick(self):
        """Settle ended runners, store newly completed daily bars, then step each open runner bar by bar."""
        if not self.load():
            return
        self._frames = {}
        try:
            now = self._now()
            self._settle(now)
            for pair in self.owner.contexts:
                self._capture_days(pair, now)
            for eid, state in list(self.lots.items()):
                if state['phase'] == OPEN:
                    self._advance(eid, state, now)
        finally:
            self._frames = None

    def filter(self, actions):
        """Drop the bag line's keep-position stop of a tranche carrying a runner; everything else unchanged."""
        kept = []
        for action in actions:
            if (isinstance(action, StopExecutorAction) and action.keep_position
                    and action.executor_id in self.lots):
                self.kept_stops += 1
                continue
            kept.append(action)
        return kept

    def excluded(self):
        """Executor ids whose tranche carries a runner (left out of Tidewater's pacing reads)."""
        return frozenset(self.lots)

    def _market(self, pair):
        return self.owner.contexts[pair].runtime.market

    def _last_closed_minute(self, pair, now):
        market = self._market(pair)
        if market is not None:
            return int(market.candle_open)
        return int(now // MINUTE) * MINUTE - MINUTE

    def _frame(self, pair):
        if pair not in self._frames:
            frame = self.owner.portfolio._candles(pair)
            if frame is None or not len(frame):
                raise DataUnavailable('runner candle frame is empty')
            self._frames[pair] = (frame.timestamp.to_numpy().astype('int64'), frame.high.to_numpy(),
                                  frame.low.to_numpy(), frame.close.to_numpy())
        return self._frames[pair]

    def _settle(self, now):
        infos = {info.id: info for info in self.owner.executors_info}
        if any(eid not in infos for eid in self.lots):
            # Archived or restored executors: the recorder holds their final rows.
            try:
                rows = self.receipts.recorder().get_executors_by_controller(self.owner.config.id)
            except Exception as exc:  # report only; settled on a later tick
                self.status['settle'] = f'runner_settle_unavailable:{type(exc).__name__}:{exc}'[:200]
                rows = ()
            infos.update({info.id: info for info in rows if info.id in self.lots and info.id not in infos})
        for eid, state in list(self.lots.items()):
            info = infos.get(eid)
            if info is None or not info.is_done:
                continue
            close_type = getattr(info.close_type, 'name', info.close_type)
            payload = {'close_type': close_type, 'phase': state['phase'], 'floor': str(state['floor']),
                       'high_water': str(state['high_water']),
                       'net_pnl_quote': str(getattr(info, 'net_pnl_quote', None))}
            if self._persist('closed', eid, state['pair'], payload, f'tidewater:runner:{eid}:closed', now):
                del self.lots[eid]
                self.closed[eid] = close_type

    def _capture_days(self, pair, now):
        market = self._market(pair)
        if market is None:
            return
        cutoff = int(market.candle_open)
        latest = (cutoff + MINUTE) // DAY_SECONDS * DAY_SECONDS - DAY_SECONDS  # the latest completed UTC day
        stored = self.days.setdefault(pair, {})
        if latest < 0 or latest in stored or self._captured.get(pair) == latest:
            return
        try:
            t, h, lo, c = self._frame(pair)
            kept = t <= cutoff
            bars = runner_daily_bars(t[kept], h[kept], lo[kept], c[kept])
        except TICK_ERRORS as exc:
            self.status[pair] = f'runner_days_unavailable:{type(exc).__name__}:{exc}'[:200]
            return
        for bar in bars:
            if bar.start in stored:
                continue
            payload = {'start': bar.start, 'high': str(bar.high), 'low': str(bar.low), 'close': str(bar.close),
                       'previous_close': None if bar.previous_close is None else str(bar.previous_close)}
            if not self.receipts.persist(RUNNER_DAY, pair=pair, cut=bar.start, code='day', payload=payload, now=now,
                                         event_id=f'tidewater:runner_day:{self.owner.config.id}:{pair}:{bar.start}'):
                return  # retried next tick
            stored[bar.start] = bar
        self._captured[pair] = latest  # a frame read once per completed day; later frames cannot add it

    def atr_pct(self, pair, minute):
        """Daily ATR% for a bar opening at `minute`: days completed before its UTC day (runner_atr_pct)."""
        days = self.days.get(pair, {})
        day = int(minute) // DAY_SECONDS
        key = (pair, day, len(days))
        if key not in self._atr:
            self._atr[key] = runner_atr_pct(tuple(days[k] for k in sorted(days)), now=day * DAY_SECONDS,
                                            atr_days=self.cfg.atr_days)
        return self._atr[key]

    def _advance(self, eid, state, now):
        pair = state['pair']
        if eid in self._pending:
            if not self._confirm(eid, now):
                self.status[pair] = 'runner_write_failed'
                return  # the stored state stays in force until the unconfirmed one is confirmed
            state = self.lots[eid]
            if state['phase'] != OPEN:
                self._exit_signal(eid, state)
                return
        market = self._market(pair)
        if market is None:
            self.status[pair] = 'runner_market_unavailable'
            return
        cutoff = int(market.candle_open)
        last = int(state['last_minute'])
        if cutoff <= last:
            return
        try:
            t, h, _, c = self._frame(pair)
        except TICK_ERRORS as exc:
            self.status[pair] = f'runner_frame_unavailable:{type(exc).__name__}:{exc}'[:200]
            return
        rows = np.flatnonzero((t > last) & (t <= cutoff))
        if not len(rows):
            return
        lot, phase, gaps, previous = _lot(state), OPEN, int(state['gaps']), last
        for i in rows:
            minute = int(t[i])
            if minute != previous + MINUTE:
                gaps += 1  # minutes missing from the frame (an outage longer than it holds) are not replayed
            previous = minute
            step = runner_bar(self.cfg, lot, high=Decimal(str(float(h[i]))), close=Decimal(str(float(c[i]))),
                              atr_pct=self.atr_pct(pair, minute))
            lot = step.lot
            if step.exit:
                phase = EXIT
                break
        before = (state['floor'], state['high_water'], state['phase'], state['gaps'])
        changed = (lot.floor, lot.high_water, phase, gaps) != before
        updated = {**state, 'floor': lot.floor, 'high_water': lot.high_water, 'phase': phase, 'gaps': gaps,
                   'last_minute': previous}
        if not changed:
            self.lots[eid] = updated  # nothing new to store; a restart replays these minutes to the same state
            return
        updated['seq'] = int(state['seq']) + 1
        if not self._commit('state', eid, updated, now):
            self.status[pair] = 'runner_write_failed'
            return  # the previous state stays in force; this exact record is re-sent next tick
        if phase == EXIT:
            self._exit_signal(eid, updated)

    def _exit_signal(self, eid, state):
        self.owner.logger().info(f'Tidewater {state["pair"]} runner exit signal executor={eid} '
                                 f'minute={state["last_minute"]} floor={state["floor"]} '
                                 f'cost_floor={state["cost_floor"]}')

    # ------------------------------------------------------------------ reporting
    def report(self):
        open_base = sum((s['runner_base'] for s in self.lots.values()), Decimal('0'))
        return {'config': self.cfg.model_dump(mode='json'), 'loaded': self.loaded, 'fault': self.fault,
                'open': len(self.lots), 'exiting': sum(s['phase'] == EXIT for s in self.lots.values()),
                'open_base': str(open_base), 'skipped': len(self.skipped), 'closed': len(self.closed),
                'kept_stops': self.kept_stops,
                'days': {pair: len(days) for pair, days in self.days.items()}, 'status': dict(self.status)}
