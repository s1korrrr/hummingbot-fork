"""V4 decisions through the real modular runtime, decision store and program budget.

Only the orchestrator and the venue are emulated (the bag-lifecycle harness). The native paper
exchange is replaced by a stub, so the exact-connector check is patched here and asserted on its
own: the controller refuses any other connector without the patch.
"""
import asyncio
from decimal import Decimal as D
from test.meridian_v4.test_meridian_v4_contract import config
from test.modular_spot.test_bag_lifecycle import Venue
from types import SimpleNamespace as NS

import pytest

import controllers.generic.meridian_v4 as meridian_v4
from controllers.generic.modular_spot_core.bag import BagFeatures
from controllers.generic.modular_spot_core.market import MarketFeatures
from controllers.generic.modular_spot_core.program_budget import BudgetPolicy, BudgetStore
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction

PAIR, START = 'BNB-USDC', 1_800_000_000


def _provider(venue):
    rules = NS(min_price_increment=D('0.01'))
    return NS(time=lambda: venue.now, ready=True, connectors={'okx_paper_trade': NS(trading_rules={PAIR: rules})})


def _controller(venue, monkeypatch, **overrides):
    monkeypatch.setattr(meridian_v4, 'is_native_paper_connector', lambda connector: True)
    store = BudgetStore(venue.recorder, BudgetPolicy(group='meridian_v4', max_quote=D('5000'), max_assets=5),
                        connector='okx_paper_trade', quote='USDC', account='meridian_v4_paper',
                        metadata_model=venue.Metadata)
    controller = meridian_v4.MeridianV4Controller(config(execution_enabled=True, **overrides), _provider(venue),
                                                  asyncio.Queue())
    controller._status = RunnableStatus.RUNNING
    rt = controller.runtime
    rt.bindings.recorder = lambda: venue.recorder
    rt.bindings.metadata_model = venue.Metadata
    rt.bindings.fees_permit = lambda controller, request: True
    rt.store, rt.budget = None, store
    controller.executors_info, controller.positions_held = venue.executors, venue.positions
    rt.decision_inputs = lambda candle: {}
    rt.observe = lambda: rt.market
    return controller


def _tick(controller, venue, candle, *, atr, prev='28', rsi='30', mid=D('600')):
    rt = controller.runtime
    venue.now = START + 60 * candle + 30
    rt.market = MarketFeatures(START + 60 * candle, mid, D(prev), D(rsi), False, False, atr_fraction=D(atr),
                               bag=BagFeatures(D(prev), mid, D('0.1'), D('0'), D('0.002'), 'range', D('0')))
    rt.snapshot = lambda: dict(bid=mid - D('0.05'), ask=mid + D('0.05'), available_quote=D('100000'), pnl=D('0'),
                               amount_step=D('0.001'), minimum_amount=D('0.001'), minimum_notional=D('1'))
    actions = controller.determine_executor_actions()
    assert rt.fault is None, rt.fault
    return actions


def test_rebound_enters_only_inside_the_v4_atr_ceiling(tmp_path, monkeypatch):
    venue = Venue(tmp_path)
    controller = _controller(venue, monkeypatch)
    assert _tick(controller, venue, 0, atr='0.0011') == []  # the first cut only seeds the checkpoint
    assert _tick(controller, venue, 1, atr='0.0013') == []  # a rebound above the 0.0012 ceiling
    assert controller.runtime.reason == 'acquisition_not_eligible'
    [entry] = _tick(controller, venue, 2, atr='0.0011')
    assert isinstance(entry, CreateExecutorAction)
    order = entry.executor_config
    assert order.trading_pair == PAIR and order.connector_name == 'okx_paper_trade'
    assert order.amount * order.entry_price <= 100  # one 100-quote buy, fee reserve included


def test_entry_carries_the_v4_trailing_exit_and_never_a_stop_loss(tmp_path, monkeypatch):
    venue = Venue(tmp_path)
    controller = _controller(venue, monkeypatch)
    _tick(controller, venue, 0, atr='0.0011')
    [entry] = _tick(controller, venue, 1, atr='0.0011')
    barrier = entry.executor_config.triple_barrier_config
    # At the adaptive baseline ATR (0.002) the multiplier is 1: the recipe's 2.5% / 0.5% exactly.
    assert (barrier.trailing_stop.activation_price, barrier.trailing_stop.trailing_delta) == (D('0.025'), D('0.005'))
    assert barrier.stop_loss is None and barrier.take_profit is None and barrier.time_limit is None


def test_unsealed_paper_and_other_connectors_never_dispatch(tmp_path, monkeypatch):
    venue = Venue(tmp_path)
    with pytest.raises(ValueError, match='native paper connector'):
        meridian_v4.MeridianV4Controller(config(execution_enabled=True), _provider(venue), asyncio.Queue())
    controller = _controller(venue, monkeypatch)
    # An operator edit that unseals the config is refused, but pydantic has already assigned the value
    # when the model validator raises; the dispatch gates must still refuse every order in that state.
    with pytest.raises(ValueError, match='sealed recipe binding'):
        controller.config.recipe_binding = None
    assert controller.config.recipe_binding is None
    assert _tick(controller, venue, 0, atr='0.0011') == []
    assert _tick(controller, venue, 1, atr='0.0011') == []
    assert controller.runtime.reason == 'meridian_v4_recipe_binding_required'
