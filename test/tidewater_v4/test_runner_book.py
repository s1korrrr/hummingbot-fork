"""Tidewater protected runner book: bar-by-bar state against the research loop, durability and failures.

The book is driven with a scripted owner and an in-memory recorder that enforces the native event-store
contract (an event id records one fact). The expected values are computed independently with the pure
policy exactly as the candle research owner's `_runner_bar` loop does (quants-lab
`core/backtesting/spot_modular_minute.py`): each closed bar takes `runner_bar` with the daily ATR of the
UTC days completed before the bar's day.
"""
import json
import logging
from decimal import Decimal as D
from types import SimpleNamespace as NS

import numpy as np
import pandas as pd
import pytest

from controllers.generic.tidewater_v4_core.config import RUNNER_ITER5_B
from controllers.generic.tidewater_v4_core.receipts import RUNNER, RUNNER_DAY, ReceiptWriter, read_receipts
from controllers.generic.tidewater_v4_core.runner import (
    DAY_SECONDS,
    RunnerLot,
    runner_atr_pct,
    runner_bar,
    runner_cost_floor,
    runner_daily_bars,
)
from controllers.generic.tidewater_v4_core.runner_book import EXIT, OPEN, RunnerBook, RunnerHold
from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction

PAIR = 'BNB-USDC'
OWNER = 'modular_spot_tidewater_v4_paper'
CFG = RUNNER_ITER5_B.model_copy(update={'atr_days': 2})
DAY0 = 20_000 * DAY_SECONDS


class Recorder:
    """The native controller event store contract: append-only, one fact per event id."""

    def __init__(self):
        self.rows, self.fail, self.fail_reads = [], False, False
        self.lost_acks = 0  # the next N appends commit, then the acknowledgement is lost (the call raises)

    def append_rsi_v5_runtime_events(self, events):
        if self.fail:
            raise RuntimeError('store unavailable')
        for event in events:
            known = next((r for r in self.rows if r.event_id == event['event_id']), None)
            if known is not None:
                if known.payload_json != event['payload_json']:
                    raise RuntimeError('event id already records a different fact')
                continue
            self.rows.append(NS(sequence=len(self.rows) + 1, **event))
        if self.lost_acks:
            self.lost_acks -= 1
            raise RuntimeError('acknowledgement lost after commit')

    def get_rsi_v5_runtime_events(self, controller_id):
        if self.fail_reads:
            raise RuntimeError('store unavailable')
        return [r for r in self.rows if r.controller_id == controller_id]

    def get_executors_by_controller(self, controller_id):
        return []


def tape(days=4, seed=7):
    """Whole UTC days of one-minute bars from DAY0: a rally with noise, then a decline."""
    n = days * 1440
    rng = np.random.default_rng(seed)
    close = 100 + np.cumsum(rng.normal(0.004, 0.06, n))
    close[int(n * .75):] -= np.linspace(0, 20, n - int(n * .75))
    return pd.DataFrame(dict(timestamp=DAY0 + 60 * np.arange(n), open=close, high=close + rng.uniform(0, .2, n),
                             low=close - rng.uniform(0, .2, n), close=close, volume=np.ones(n)))


class Owner:
    """The controller surface the book reads; the frame is cut at the scripted clock (1,500 minutes)."""

    def __init__(self, frame, recorder, *, window=1500):
        self.frame, self.window, self.clock = frame, window, None
        self.config = NS(id=OWNER)
        self.executors_info = []
        self.reads = 0
        self.market_data_provider = NS(time=lambda: self.clock)
        self.contexts = {PAIR: NS(runtime=NS(market=None))}
        self.portfolio = NS(_candles=self._candles)
        self.receipts = ReceiptWriter(OWNER, recorder=recorder)

    def at(self, minute):
        """The owner clock one minute after the bar opening at `minute` closed."""
        self.clock = minute + 60
        self.contexts[PAIR].runtime.market = NS(candle_open=minute)

    def _candles(self, pair):
        self.reads += 1
        cut = self.frame[self.frame.timestamp <= self.contexts[pair].runtime.market.candle_open]
        return cut.tail(self.window)

    def logger(self):
        return logging.getLogger(__name__)

    def book(self):
        return RunnerBook(self, CFG, self.receipts)


FACTS = dict(executor_id='e1', pair=PAIR, amount=D('1.393'), amount_step=D('.001'), minimum_notional=D('1'),
             minimum_order=D('.001'), cost_basis=D('95'), fee_fraction=D('0.003'), no_loss_floor=D('95.4'))


def research_reference(frame, start_minute, end_minute, high_water, cost_floor):
    """The candle research owner's loop: all days of the feed, one `runner_bar` per closed bar."""
    days = runner_daily_bars(frame.timestamp.to_numpy(), frame.high.to_numpy(), frame.low.to_numpy(),
                             frame.close.to_numpy())
    lot, exited = RunnerLot(D('0.696'), D('95'), cost_floor, high_water, cost_floor), None
    for row in frame[(frame.timestamp > start_minute) & (frame.timestamp <= end_minute)].itertuples():
        atr = runner_atr_pct(days, now=int(row.timestamp) // DAY_SECONDS * DAY_SECONDS, atr_days=CFG.atr_days)
        step = runner_bar(CFG, lot, high=D(str(float(row.high))), close=D(str(float(row.close))), atr_pct=atr)
        lot = step.lot
        if step.exit:
            exited = int(row.timestamp)
            break
    return lot, exited


def run(owner, book, minutes):
    for minute in minutes:
        owner.at(int(minute))
        book.tick()


def harvest_at(owner, book, minute, price):
    owner.at(minute)
    book.tick()
    return book.harvest({**FACTS, 'price': price})


def test_the_book_steps_each_runner_exactly_as_the_research_loop():
    frame = tape()
    owner = Owner(frame, Recorder())
    book = owner.book()
    minutes = frame.timestamp.to_numpy()
    start = int(minutes[2 * 1440 + 30])  # day 3, after two whole days are stored
    run(owner, book, minutes[:2 * 1440 + 30])
    price = D(str(float(frame.close[frame.timestamp == start].iloc[0])))
    state = harvest_at(owner, book, start, price)
    assert (state['runner_base'], state['sold_base'], state['phase']) == (D('0.696'), D('0.697'), OPEN)
    cost_floor = runner_cost_floor(CFG, cost_basis=D('95'), fee_fraction=D('0.003'))
    assert state['cost_floor'] == cost_floor == D('96.235')
    run(owner, book, minutes[2 * 1440 + 31:])
    expected, exited = research_reference(frame, start, int(minutes[-1]), price, cost_floor)
    assert exited is not None  # the decline ends the runner inside the tape
    stored = book.lots['e1']
    assert (stored['floor'], stored['high_water'], stored['phase'], stored['last_minute']) == (
        expected.floor, expected.high_water, EXIT, exited)
    assert sorted(book.days[PAIR]) == [DAY0 + k * DAY_SECONDS for k in range(4)]
    assert book.atr_pct(PAIR, exited) is not None


def test_daily_bars_are_captured_once_per_day_and_equal_the_whole_frame_bars():
    frame = tape(days=3)
    owner = Owner(frame, Recorder())
    book = owner.book()
    run(owner, book, frame.timestamp.to_numpy())
    reference = {bar.start: bar for bar in runner_daily_bars(frame.timestamp.to_numpy(), frame.high.to_numpy(),
                                                             frame.low.to_numpy(), frame.close.to_numpy())}
    assert book.days[PAIR] == reference and len(reference) == 3
    assert reference[DAY0].previous_close is None  # the first day of the feed, as in the research owner
    # one frame read per completed-day candidate (the day before the tape, then each of its days),
    # not one per minute
    assert owner.reads == 4
    records = [r for r in read_receipts(owner.receipts.recorder(), OWNER) if r['kind'] == RUNNER_DAY]
    assert [r['start'] for r in records] == [DAY0, DAY0 + DAY_SECONDS, DAY0 + 2 * DAY_SECONDS]


def test_a_restart_on_the_same_store_resumes_to_the_uninterrupted_state():
    frame = tape()
    minutes = frame.timestamp.to_numpy()
    start = int(minutes[2 * 1440 + 30])
    price = D(str(float(frame.close[frame.timestamp == start].iloc[0])))

    def prepared():
        owner = Owner(frame, Recorder())
        book = owner.book()
        run(owner, book, minutes[:2 * 1440 + 30])
        harvest_at(owner, book, start, price)
        return owner, book

    owner, book = prepared()
    run(owner, book, minutes[2 * 1440 + 31:])
    uninterrupted = book.lots['e1']

    owner, book = prepared()
    middle = 2 * 1440 + 400
    run(owner, book, minutes[2 * 1440 + 31:middle])
    before = book.lots['e1']
    restarted = owner.book()  # a new process: nothing but the store
    owner.at(int(minutes[middle - 1]))
    assert restarted.load()
    reloaded = restarted.lots['e1']
    assert {k: reloaded[k] for k in ('floor', 'high_water', 'phase', 'seq', 'cost_floor')} == {
        k: before[k] for k in ('floor', 'high_water', 'phase', 'seq', 'cost_floor')}
    assert restarted.days == book.days
    run(owner, restarted, minutes[middle:])
    resumed = restarted.lots['e1']
    for key in ('floor', 'high_water', 'phase', 'last_minute', 'seq'):
        assert resumed[key] == uninterrupted[key], key


def test_a_failed_write_keeps_the_stored_state_and_the_retry_gives_the_same_result():
    frame = tape()
    minutes = frame.timestamp.to_numpy()
    start = int(minutes[2 * 1440 + 30])
    price = D(str(float(frame.close[frame.timestamp == start].iloc[0])))
    recorder = Recorder()
    owner = Owner(frame, recorder)
    book = owner.book()
    run(owner, book, minutes[:2 * 1440 + 30])
    harvest_at(owner, book, start, price)
    run(owner, book, minutes[2 * 1440 + 31:2 * 1440 + 200])
    before = dict(book.lots['e1'])
    recorder.fail = True
    run(owner, book, minutes[2 * 1440 + 200:2 * 1440 + 900])
    assert {k: book.lots['e1'][k] for k in ('floor', 'high_water', 'seq')} == {
        k: before[k] for k in ('floor', 'high_water', 'seq')}  # nothing unstored takes effect
    assert owner.receipts.fault is not None
    recorder.fail = False
    run(owner, book, minutes[2 * 1440 + 900:])
    expected, exited = research_reference(frame, start, int(minutes[-1]), price, book.lots['e1']['cost_floor'])
    assert (book.lots['e1']['floor'], book.lots['e1']['high_water'], book.lots['e1']['last_minute']) == (
        expected.floor, expected.high_water, exited)


def test_a_state_write_committed_without_its_acknowledgement_is_confirmed_by_the_identical_retry():
    frame = tape()
    minutes = frame.timestamp.to_numpy()
    start = int(minutes[2 * 1440 + 30])
    price = D(str(float(frame.close[frame.timestamp == start].iloc[0])))
    recorder = Recorder()
    owner = Owner(frame, recorder)
    book = owner.book()
    run(owner, book, minutes[:2 * 1440 + 30])
    harvest_at(owner, book, start, price)
    run(owner, book, minutes[2 * 1440 + 31:2 * 1440 + 200])
    seq = book.lots['e1']['seq']
    before = len(recorder.rows)
    k = 2 * 1440 + 200
    while len(recorder.rows) == before:  # the next state change commits; its acknowledgement is lost
        recorder.lost_acks = 1
        run(owner, book, minutes[k:k + 1])
        k += 1
    recorder.lost_acks = 0
    assert book.lots['e1']['seq'] == seq  # unconfirmed: the stored state is not yet in force
    committed = json.loads(recorder.rows[-1].payload_json)
    assert committed['seq'] == seq + 1
    run(owner, book, minutes[k:k + 120])  # later minutes change nothing about the stored fact
    assert owner.receipts.fault['count'] == 1  # the retry re-sends the same fact: never refused
    assert book.lots['e1']['seq'] > seq + 1
    run(owner, book, minutes[k + 120:])
    states = [json.loads(r.payload_json) for r in recorder.rows if r.event_id.startswith('tidewater:runner:e1:state:')]
    assert [st['seq'] for st in states] == list(range(1, len(states) + 1))
    expected, exited = research_reference(frame, start, int(minutes[-1]), price, book.lots['e1']['cost_floor'])
    assert (book.lots['e1']['floor'], book.lots['e1']['high_water'], book.lots['e1']['last_minute']) == (
        expected.floor, expected.high_water, exited)


def test_a_split_committed_without_its_acknowledgement_is_confirmed_unchanged():
    recorder = Recorder()
    owner = Owner(tape(days=1), recorder)
    book = owner.book()
    owner.at(DAY0 + 600)
    recorder.lost_acks = 1
    assert isinstance(book.harvest({**FACTS, 'price': D('98')}), RunnerHold)
    assert book.lots == {}
    owner.at(DAY0 + 660)  # a later trigger price and clock: the stored split is the fact, not a new one
    state = book.harvest({**FACTS, 'price': D('98.5')})
    assert (state['harvest_price'], state['harvested_at']) == (D('98'), DAY0 + 660)
    assert owner.receipts.fault['count'] == 1
    [row] = recorder.rows
    assert book.lots['e1'] == state and json.loads(row.payload_json)['harvest_price'] == '98'


def test_no_runner_decision_before_the_store_is_read_or_when_the_split_cannot_be_stored():
    recorder = Recorder()
    owner = Owner(tape(days=1), recorder)
    book = owner.book()
    owner.at(DAY0 + 600)
    recorder.fail_reads = True
    assert isinstance(book.harvest({**FACTS, 'price': D('98')}), RunnerHold)
    assert isinstance(book.runner('e1'), RunnerHold)
    assert book.fault.startswith('runner_load:')
    recorder.fail_reads, recorder.fail = False, True
    assert isinstance(book.harvest({**FACTS, 'price': D('98')}), RunnerHold)
    assert book.lots == {}
    recorder.fail = False
    assert book.harvest({**FACTS, 'price': D('98')})['runner_base'] == D('0.696')
    assert [r['code'] for r in read_receipts(recorder, OWNER) if r['kind'] == RUNNER] == ['created']


@pytest.mark.parametrize('facts, reason', [
    ({'amount': D('0.5'), 'price': D('98')}, None),
    ({'amount': D('0.02'), 'price': D('100'), 'minimum_order': D('0.011')}, 'runner_below_min_order'),
    ({'amount': D('0.015'), 'price': D('100')}, 'runner_below_min_notional'),
])
def test_split_skips_are_recorded_and_leave_the_native_exit(facts, reason):
    owner = Owner(tape(days=1), Recorder())
    book = owner.book()
    owner.at(DAY0 + 600)
    result = book.harvest({**FACTS, **facts})
    if reason is None:
        assert result['runner_base'] == D('0.250')
    else:
        assert result is None and book.skipped == {'e1': reason}
        assert book.harvest({**FACTS, **facts}) is None  # decided once


def test_the_cost_floor_is_never_below_the_native_no_loss_price():
    owner = Owner(tape(days=1), Recorder())
    book = owner.book()
    owner.at(DAY0 + 600)
    book.cfg = CFG.model_copy(update={'floor_margin': D('0')})
    state = book.harvest({**FACTS, 'price': D('98'), 'no_loss_floor': D('95.4')})
    assert state['cost_floor'] == D('95.4') > D('95') * D('1.003')


def test_only_the_bag_lines_keep_position_stop_of_a_runner_tranche_is_dropped():
    owner = Owner(tape(days=1), Recorder())
    book = owner.book()
    owner.at(DAY0 + 600)
    book.harvest({**FACTS, 'price': D('98')})
    actions = [StopExecutorAction(controller_id=OWNER, executor_id='e1', keep_position=True),
               StopExecutorAction(controller_id=OWNER, executor_id='e2', keep_position=True),
               StopExecutorAction(controller_id=OWNER, executor_id='e1', keep_position=False)]
    assert book.filter(actions) == actions[1:]
    assert book.kept_stops == 1


def test_stored_records_are_plain_json_facts():
    recorder = Recorder()
    owner = Owner(tape(days=1), recorder)
    owner.at(DAY0 + 600)
    owner.book().harvest({**FACTS, 'price': D('98')})
    [row] = recorder.rows
    assert row.event_id == 'tidewater:runner:e1:created' and row.event_type == RUNNER
    payload = json.loads(row.payload_json)
    assert (payload['cost_floor'], payload['floor'], payload['high_water']) == ('96.235', '96.235', '98')
