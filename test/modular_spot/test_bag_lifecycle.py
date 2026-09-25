"""Two complete bag cycles through the real modular runtime, decision store and program budget.

Only the orchestrator is emulated: after each dispatched action the test records the native
executor/position state the orchestrator would report. Everything the controller decides runs
through ``ModularRuntime.actions`` and its permission gates.
"""
import asyncio
from decimal import Decimal as D
from test.modular_spot.test_capital_admission import metadata_engine
from types import SimpleNamespace as NS

import pytest

from controllers.generic.modular_spot import ModularSpotController, ModularSpotControllerConfig
from controllers.generic.modular_spot_core.bag import BagConfig, BagFeatures, LiveCapitalConfig
from controllers.generic.modular_spot_core.market import MarketFeatures
from controllers.generic.modular_spot_core.program_budget import BudgetPolicy, BudgetStore
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction

PAIR, START = 'BNB-USDC', 1_800_000_000


class Venue:
    """Orchestrator/recorder emulation: executor receipts and the blended held position."""

    def __init__(self, tmp_path):
        self.engine, recorder, self.Metadata = metadata_engine(tmp_path, 'bag-lifecycle.db')
        self.executors, self.positions = [], []
        recorder.get_all_executors = lambda: list(self.executors)
        recorder.get_executors_by_controller = lambda cid: [e for e in self.executors if e.controller_id == cid]
        recorder.get_positions_by_controller = lambda cid: []
        self.recorder = recorder
        self.now = START + 30

    def tranche(self, config, *, filled_base, price):
        info = NS(id=config.id, controller_id=config.controller_id, config=config, is_active=True, is_done=False,
                  timestamp=config.timestamp, close_timestamp=None, filled_amount_quote=filled_base * price,
                  custom_info={'entry_price': price, 'entry_lifecycle': {'state': 'managing_fill'},
                               'execution_receipt': {'remaining_owned_base': filled_base,
                                                     'entry_gross_quote': filled_base * price,
                                                     'exit_gross_base': D('0'), 'accounting_owner': 'executor'}})
        self.executors.append(info)
        return info

    def hold(self, info, *, breakeven, fees):
        """StopExecutorAction(keep_position=True): the tranche's base moves into PositionHold."""
        info.is_active, info.is_done, info.close_timestamp = False, True, self.now
        info.custom_info['execution_receipt']['accounting_owner'] = 'held_position'
        amount = info.custom_info['execution_receipt']['remaining_owned_base']
        self.positions[:] = [NS(connector_name='okx', trading_pair=PAIR, side=TradeType.BUY, amount=amount,
                                breakeven_price=breakeven, remaining_fees_quote=fees, cum_fees_quote=fees)]

    def sell_filled(self, config, *, residual):
        self.executors.append(NS(id=config.id, controller_id=config.controller_id, config=config, is_active=False,
                                 is_done=True, timestamp=config.timestamp, close_timestamp=self.now,
                                 filled_amount_quote=config.amount * config.price,
                                 custom_info={'executed_amount_base': config.amount}))
        self.positions[:] = [] if residual == 0 else [
            NS(connector_name='okx', trading_pair=PAIR, side=TradeType.BUY, amount=residual,
               breakeven_price=self.positions[0].breakeven_price, remaining_fees_quote=D('0'), cum_fees_quote=D('0'))]


def _controller(venue, store, pair=PAIR, capital=None):
    cfg = ModularSpotControllerConfig(
        id=f'modular_spot_{pair.replace("-", "_").lower()}', connector_name='okx', trading_pair=pair,
        execution_enabled=True, account='paper-okx', max_records=1276, bag=BagConfig(), capital=capital,
        budget={'group': 'desk', 'max_quote': '1000', 'max_assets': 3}, total_amount_quote=D('500'),
        universe={'mode': 'fixed', 'symbols': [pair], 'max_candidates': 1},
        acquisition={'fractions': ['1'], 'minimum_drop': '.008', 'cooldown_seconds': 1, 'entry_timeout_seconds': 60})
    rules = NS(min_price_increment=D('0.01'))
    provider = NS(time=lambda: venue.now, ready=True, connectors={'okx': NS(trading_rules={pair: rules})})
    controller = ModularSpotController(cfg, provider, asyncio.Queue())
    controller._status = RunnableStatus.RUNNING
    rt = controller.runtime
    rt.bindings.recorder = lambda: venue.recorder
    rt.bindings.metadata_model = venue.Metadata
    rt.bindings.fees_permit = lambda controller, request: True
    rt.store, rt.budget = None, store
    # The orchestrator's view of this controller shares the recorder rows here.
    controller.executors_info = venue.executors if pair == PAIR else []
    controller.positions_held = venue.positions if pair == PAIR else []
    rt.decision_inputs = lambda candle: {}
    rt.observe = lambda: rt.market
    return controller


def _tick(controller, venue, *, mid, rsi, prev, candle):
    rt = controller.runtime
    venue.now = START + 60 * candle + 30
    rt.market = MarketFeatures(START + 60 * candle, mid, prev, rsi, False, False,
                               bag=BagFeatures(prev, D('100'), D('0.1'), D('0'), D('0.002'), 'range', D('0')))
    rt.snapshot = lambda: dict(bid=mid - D('0.005'), ask=mid + D('0.005'), available_quote=D('1000'), pnl=D('0'),
                               amount_step=D('0.001'), minimum_amount=D('0.001'), minimum_notional=D('1'))
    actions = controller.determine_executor_actions()
    assert rt.fault is None, rt.fault
    return actions


def _dispatch(controller, action):
    """The orchestrator admits a create only through the controller's own gates."""
    config = action.executor_config
    assert controller.validate_executor_config(config)
    return controller.get_executor_class_for_config(config), config


def _bag_cycle(controller, venue, store, first_candle, *, gapped=False):
    """Buy on a rebound, hold it at -3%, recover it through the live trail, release and reset."""
    candle = first_candle
    [entry] = _tick(controller, venue, mid=D('100'), rsi=D('31'), prev=D('29'), candle=candle)
    assert isinstance(entry, CreateExecutorAction)
    _, config = _dispatch(controller, entry)
    tranche = venue.tranche(config, filled_base=D('0.9992'), price=config.entry_price)  # net of the base fee
    candle += 1
    stops = _tick(controller, venue, mid=D('96.9'), rsi=D('24'), prev=D('25'), candle=candle)
    assert [(a.executor_id, a.keep_position) for a in stops if isinstance(a, StopExecutorAction)] == [(config.id, True)]
    venue.hold(tranche, breakeven=config.entry_price, fees=D('0.3'))
    for mid in (D('105'), D('108')):  # arm (RSI 80, net >= 4%), then a new peak
        candle += 1
        assert _tick(controller, venue, mid=mid, rsi=D('80'), prev=D('80.2'), candle=candle) == []
    pullback = D('107.1')
    if gapped:
        # Within 1% of the previous cycle's 107.11 sell: live's sell-gap rule blocks it.
        candle += 1
        assert _tick(controller, venue, mid=pullback, rsi=D('80'), prev=D('80.2'), candle=candle) == []
        assert controller.runtime.bag_status['reason'] == 'price-gap'
        candle += 1
        assert _tick(controller, venue, mid=D('113'), rsi=D('80'), prev=D('80.2'), candle=candle) == []
        pullback = D('112')
    candle += 1
    [sell] = _tick(controller, venue, mid=pullback, rsi=D('80'), prev=D('80.2'), candle=candle)  # 0.8% pullback
    executor_class, sell_config = _dispatch(controller, sell)
    assert executor_class is OrderExecutor
    assert sell_config.min_sell_price == config.entry_price * D('1.02') + D('0.3') / D('0.9992')  # live floor
    assert sell_config.amount == D('0.999')  # the whole bag, floored to the lot: 0.0002 is left over
    venue.sell_filled(sell_config, residual=D('0.0002'))
    candle += 1
    assert _tick(controller, venue, mid=D('104'), rsi=D('60'), prev=D('61'), candle=candle) == []
    return candle, config, sell_config


@pytest.mark.parametrize('capital', [None, LiveCapitalConfig()])
def test_two_complete_bag_cycles_release_capital_and_keep_the_controller_running(tmp_path, capital):
    venue = Venue(tmp_path)
    store = BudgetStore(venue.recorder, BudgetPolicy(group='desk', max_quote=D('1000'), max_assets=3),
                        connector='okx', quote='USDC', account='paper-okx', metadata_model=venue.Metadata)
    controller = _controller(venue, store, capital=capital)
    rt = controller.runtime
    assert _tick(controller, venue, mid=D('100'), rsi=D('31'), prev=D('29'), candle=0) == []  # seed checkpoint

    candle, first, first_sell = _bag_cycle(controller, venue, store, 1)
    # The dust residual reconciles with the receipts: capital is released durably and the campaign resets.
    assert store.released() == {first.id, first_sell.id}
    assert rt.bag_status['release'].startswith('released:1_held')
    state, _ = rt.store.load()
    assert state.claims == () and state.cycle == 1

    # Cycle two starts from the retired held receipt without faulting, then bags and recovers again.
    candle, second, second_sell = _bag_cycle(controller, venue, store, candle + 1, gapped=True)
    assert second.id != first.id and store.released() == {first.id, first_sell.id, second.id, second_sell.id}
    assert rt.store.load()[0].cycle == 2

    # Every other program member reads the same durable release with no per-call proof.
    other = _controller(venue, store, 'ETH-USDC')
    assert other.runtime.acquisition_permitted() is True
    assert store.occupied_pairs(native_executors=venue.executors) == ()
    venue.engine.dispose()


def test_receipts_that_disagree_with_the_held_position_keep_capital_reserved(tmp_path):
    venue = Venue(tmp_path)
    store = BudgetStore(venue.recorder, BudgetPolicy(group='desk', max_quote=D('1000'), max_assets=3),
                        connector='okx', quote='USDC', account='paper-okx', metadata_model=venue.Metadata)
    controller = _controller(venue, store)
    rt = controller.runtime
    _tick(controller, venue, mid=D('100'), rsi=D('31'), prev=D('29'), candle=0)
    [entry] = _tick(controller, venue, mid=D('100'), rsi=D('31'), prev=D('29'), candle=1)
    _, config = _dispatch(controller, entry)
    tranche = venue.tranche(config, filled_base=D('0.9992'), price=config.entry_price)
    _tick(controller, venue, mid=D('96.9'), rsi=D('24'), prev=D('25'), candle=2)
    venue.hold(tranche, breakeven=config.entry_price, fees=D('0.3'))
    venue.positions[:] = []  # an incomplete ledger restore reports no held position
    _tick(controller, venue, mid=D('97'), rsi=D('30'), prev=D('31'), candle=3)
    assert rt.bag_status['release'] == 'unproven:receipts_disagree'
    assert store.released() == frozenset() and store.occupied_pairs(native_executors=venue.executors) == (PAIR,)
    venue.engine.dispose()


def test_bag_stops_run_from_mid_when_candle_features_are_unavailable(tmp_path):
    venue = Venue(tmp_path)
    store = BudgetStore(venue.recorder, BudgetPolicy(group='desk', max_quote=D('1000'), max_assets=3),
                        connector='okx', quote='USDC', account='paper-okx', metadata_model=venue.Metadata)
    controller = _controller(venue, store)
    _tick(controller, venue, mid=D('100'), rsi=D('31'), prev=D('29'), candle=0)
    [entry] = _tick(controller, venue, mid=D('100'), rsi=D('31'), prev=D('29'), candle=1)
    _, config = _dispatch(controller, entry)
    venue.tranche(config, filled_base=D('0.9992'), price=config.entry_price)
    controller.runtime.observe = lambda: None  # stale feed / warmup / bag feature failure

    def stale_report():
        raise AssertionError('the mid-only stop path must not need the native performance snapshot')

    controller.runtime.snapshot = stale_report
    book = {'BestBid': D('96.895'), 'BestAsk': D('96.905')}
    controller.market_data_provider.get_price_by_type = lambda connector, pair, kind: book[kind.name]
    actions = controller.determine_executor_actions()
    assert [(a.executor_id, a.keep_position) for a in actions] == [(config.id, True)]
    controller.config.manual_kill_switch = True
    assert controller.determine_executor_actions() == []
    venue.engine.dispose()


def test_recovery_permission_refuses_unsafe_or_foreign_sells(tmp_path):
    venue = Venue(tmp_path)
    store = BudgetStore(venue.recorder, BudgetPolicy(group='desk', max_quote=D('1000'), max_assets=3),
                        connector='okx', quote='USDC', account='paper-okx', metadata_model=venue.Metadata)
    controller = _controller(venue, store)
    _tick(controller, venue, mid=D('100'), rsi=D('31'), prev=D('29'), candle=0)
    [entry] = _tick(controller, venue, mid=D('100'), rsi=D('31'), prev=D('29'), candle=1)
    _, config = _dispatch(controller, entry)
    tranche = venue.tranche(config, filled_base=D('0.9992'), price=config.entry_price)
    _tick(controller, venue, mid=D('96.9'), rsi=D('24'), prev=D('25'), candle=2)
    venue.hold(tranche, breakeven=config.entry_price, fees=D('0.3'))
    for candle, mid in ((3, D('105')), (4, D('108'))):
        _tick(controller, venue, mid=mid, rsi=D('80'), prev=D('80.2'), candle=candle)
    [sell] = _tick(controller, venue, mid=D('107.1'), rsi=D('80'), prev=D('80.2'), candle=5)
    request, rt = sell.executor_config, controller.runtime
    too_big = request.model_copy(update={'amount': D('2')})
    rt.pending[too_big.id] = too_big
    assert not controller.validate_executor_config(too_big)  # more than the bag holds
    rt.pending[request.id] = request
    other = request.model_copy(update={'id': 'another-live-sell'})
    venue.executors.append(NS(id=other.id, controller_id=controller.config.id, config=other, is_active=True,
                              is_done=False, timestamp=1.0, close_timestamp=None, filled_amount_quote=D('0'),
                              custom_info={}))
    assert not controller.validate_executor_config(request)  # one live recovery sell per bag
    venue.executors.pop()
    controller.config.execution_enabled = False
    assert not controller.validate_executor_config(request)
    controller.config.execution_enabled = True
    controller.config.manual_kill_switch = True
    assert not controller.validate_executor_config(request)
    controller.config.manual_kill_switch = False
    rt.fault = 'earlier fault'
    assert not controller.validate_executor_config(request)
    rt.fault = None
    assert controller.validate_executor_config(request)
    venue.engine.dispose()
