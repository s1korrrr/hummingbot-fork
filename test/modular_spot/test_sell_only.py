"""Sell-only admission and native reduction. Native skips fail M6."""
import asyncio
import importlib.util
from contextlib import contextmanager
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import Column, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

if importlib.util.find_spec('hummingbot') is None:
    pytest.fail('native Hummingbot package/compiled runtime is unavailable')

from test.modular_spot.test_native_lifecycle import disposable_recorder, plan_inputs, release_recorder  # noqa: E402

from controllers.generic.modular_spot import ModularSpotController, ModularSpotControllerConfig  # noqa: E402
from controllers.generic.modular_spot_core.acquisition import next_stage  # noqa: E402
from controllers.generic.modular_spot_core.config import AcquisitionPolicy, TrailingPolicy  # noqa: E402
from controllers.generic.modular_spot_core.domain import PolicyError  # noqa: E402
from controllers.generic.modular_spot_core.execution import ModularPositionExecutor, native_config  # noqa: E402
from controllers.generic.modular_spot_core.inventory import inventory_view  # noqa: E402
from controllers.generic.modular_spot_core.market import MarketFeatures  # noqa: E402
from controllers.generic.modular_spot_core.native_bindings import NativeBindings  # noqa: E402
from controllers.generic.modular_spot_core.profiles import ProgramProfile, build_profile  # noqa: E402
from controllers.generic.modular_spot_core.program_budget import BudgetPolicy, BudgetStore  # noqa: E402
from controllers.generic.modular_spot_core.sell_only import (  # noqa: E402
    ExitObservation,
    InventoryHandoff,
    ModularReduceExecutor,
    SellOnlyStore,
    admit_inventory,
    propose_reduction,
)
from controllers.generic.modular_spot_core.state import DecisionState  # noqa: E402
from hummingbot.connector.markets_recorder import MarketsRecorder  # noqa: E402
from hummingbot.connector.trading_rule import TradingRule  # noqa: E402
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType  # noqa: E402
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate  # noqa: E402
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee  # noqa: E402
from hummingbot.strategy.strategy_v2_base import StrategyV2Base  # noqa: E402
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator  # noqa: E402
from hummingbot.strategy_v2.executors.order_executor.data_types import (  # noqa: E402
    ExecutionStrategy,
    OrderExecutorConfig,
)
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor  # noqa: E402
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor  # noqa: E402
from hummingbot.strategy_v2.models.base import RunnableStatus  # noqa: E402
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction  # noqa: E402
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder  # noqa: E402

PAIRS = ('A-USDC', 'B-USDC', 'C-USDC')


def owner(prefix, pair):
    return f'modular_spot_{prefix}_{pair.replace("-", "_")}'


def sell_cfg(pair='A-USDC', **overrides):
    values = dict(
        id=owner('inventory', pair), connector_name='okx', trading_pair=pair,
        acquisition_enabled=False,
        universe={'mode': 'sell_only', 'symbols': list(PAIRS), 'max_candidates': 3},
        account='paper-okx')
    values.update(overrides)
    return ModularSpotControllerConfig(**values)


def tracked(pair, oid, side, fill, state, *, price=D('100'), amount=D('1'), fee=None, trade_id=None):
    order = InFlightOrder(client_order_id=oid, exchange_order_id=oid, trading_pair=pair,
                          order_type=OrderType.LIMIT, trade_type=side, amount=amount, price=price,
                          creation_timestamp=1, initial_state=state)
    if D(fill):
        order.update_with_trade_update(TradeUpdate(
            trade_id=trade_id or oid, client_order_id=oid, exchange_order_id=oid, trading_pair=pair,
            fill_price=price, fill_base_amount=D(fill), fill_quote_amount=D(fill) * price,
            fee=fee or AddedToCostTradeFee(), fill_timestamp=2))
    result = TrackedOrder(order_id=oid)
    result.order = order
    return result


def prior_filled(pair, prior, *, amount='1', price='100', identifier=None, fee=None, terminal=True):
    identifier = identifier or f'{prior}-lot'
    cfg = ModularSpotControllerConfig(id=prior, connector_name='okx', trading_pair=pair)
    strategy = MagicMock(spec=StrategyV2Base)
    strategy.current_timestamp = 1000
    strategy.controllers = {}
    connector = MagicMock()
    connector.quantize_order_amount.side_effect = lambda trading_pair, quantity: quantity
    connector.get_available_balance.return_value = D('10')
    connector.get_fee.return_value = AddedToCostTradeFee(percent=D('0.001'))
    connector.in_flight_orders = {}
    strategy.connectors = {'okx': connector}
    native = native_config(cfg, identifier=identifier, timestamp=120, price=D(price), amount=D(amount),
                           cycle=0, stage=0)
    rule = TradingRule(pair, min_order_size=D('.01'), min_price_increment=D('.01'),
                       min_base_amount_increment=D('.001'), min_notional_size=D('1'))
    with patch.object(PositionExecutor, 'get_trading_rules', return_value=rule):
        executor = ModularPositionExecutor(strategy, native)
    executor._open_order = tracked(pair, f'{identifier}-entry', TradeType.BUY, amount, OrderState.FILLED,
                                   price=D(price), amount=D(amount), fee=fee)
    if terminal:
        executor.close_type = CloseType.POSITION_HOLD
        executor._status = RunnableStatus.TERMINATED
    return executor


def assigned(executor, new_owner, **extra):
    info = executor.executor_info
    receipt = info.custom_info['execution_receipt']
    view = inventory_view([info], [], controller_id=info.controller_id,
                          connector=info.config.connector_name, pair=info.config.trading_pair)[info.id]
    values = dict(
        account='paper-okx', connector='okx', trading_pair=info.config.trading_pair,
        prior_owner=info.controller_id, new_owner=new_owner,
        available_base=view.remaining_base, reserved_base=D('0'),
        entry_gross_base=D(str(receipt['entry_gross_base'])), entry_gross_quote=view.entry_quote,
        fees_quote=D(str(receipt.get('fees_quote') or 0)), fees_base=D('0'),
        reconciliation_id=info.id, policy_revision='v1', authority='operator-handoff-sui-2026')
    values.update(extra)
    return InventoryHandoff(**values)


def observation(pair='A-USDC', bid='110', **extra):
    values = dict(
        trading_pair=pair, bid=D(bid), ask=D(bid) + D('0.1'), sell_signal=True,
        amount_step=D('.001'), minimum_amount=D('.01'), minimum_notional=D('1'),
        price_step=D('.01'), sell_fee_rate=D('0.001'), sell_fee_base=D('0'),
        trailing=TrailingPolicy(), timestamp=1200.0)
    values.update(extra)
    return ExitObservation(**values)


def reduce_executor(config):
    strategy = MagicMock(spec=StrategyV2Base)
    strategy.current_timestamp = 1000
    strategy.controllers = {}
    connector = MagicMock()
    connector.quantize_order_amount.side_effect = lambda trading_pair, quantity: quantity
    connector.get_available_balance.return_value = D('10')
    connector.get_fee.return_value = AddedToCostTradeFee(percent=D('0.001'))
    connector.in_flight_orders = {}
    strategy.connectors = {'okx': connector}
    rule = TradingRule(config.trading_pair, min_order_size=D('.01'), min_price_increment=D('.01'),
                       min_base_amount_increment=D('.001'), min_notional_size=D('1'))
    with patch.object(OrderExecutor, 'get_trading_rules', return_value=rule):
        executor = ModularReduceExecutor(strategy, config)
    executor._status = RunnableStatus.RUNNING
    executor.get_price = MagicMock(return_value=config.price)
    executor.place_order = MagicMock(return_value=f'{config.id}-sell')
    return executor


def metadata_engine(tmp_path, name='sell-only.db'):
    base = declarative_base()

    class Metadata(base):
        __tablename__ = 'Metadata'
        key = Column(Text, primary_key=True)
        value = Column(Text, nullable=False)

    engine = create_engine('sqlite:///' + str(tmp_path / name))
    base.metadata.create_all(engine)
    recorder = NS(config_file_path='native-strategy.yml',
                  sql_manager=NS(get_new_session=sessionmaker(bind=engine)))
    return engine, recorder, Metadata


def bind_sell_runtime(rt, *, scope, native_rows):
    rt.ensure_store = lambda: NS(recorder=NS(get_all_executors=lambda: list(native_rows)))
    rt._sell_only_store = lambda: NS(load=lambda: (scope, 'raw'))
    rt.observe = lambda: MarketFeatures(1140, D('100'), D('29'), D('31'), False, False)
    rt.snapshot = lambda: dict(bid=D('110'), ask=D('110.1'), available_quote=D('100'), pnl=D('0'),
                               amount_step=D('.001'), minimum_amount=D('.01'), minimum_notional=D('1'))
    rt.controller.market_data_provider = NS(
        time=lambda: 1200,
        connectors={'okx': NS(
            trading_rules={'A-USDC': NS(min_price_increment=D('.01'))},
            get_fee=lambda **k: AddedToCostTradeFee(percent=D('0.001')))})
    rt.controller._status = RunnableStatus.RUNNING
    return rt


def okx_strategy(controller):
    connector = MagicMock()
    connector.in_flight_orders = {}
    connector.get_available_balance.return_value = D('10')
    strategy = MagicMock(spec=StrategyV2Base)
    strategy.controllers = {controller.config.id: controller}
    strategy.connectors = {'okx': connector}
    strategy.current_timestamp = 1000
    strategy._controller_restart_required = {}
    strategy._is_stop_triggered = False
    strategy._controller_reports_valid = True
    return strategy


@contextmanager
def orchestrator_for(controller):
    strategy = okx_strategy(controller)
    stub = NS(
        sql_manager=NS(db_path=None), get_all_executors=lambda: [], get_all_positions=lambda: [],
        get_all_position_hold_ledger_entries=lambda: [], get_trades_for_config=lambda *a, **k: [],
        config_file_path='modular.yml', store_or_update_executor=lambda executor: None)
    previous = MarketsRecorder._shared_instance
    try:
        with patch('hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder') as markets:
            markets.get_instance.return_value = stub
            yield ExecutorOrchestrator(strategy=strategy), strategy
    finally:
        current = MarketsRecorder._shared_instance
        try:
            if current is not None and current is not previous:
                current.stop()
        finally:
            MarketsRecorder._shared_instance = previous


def test_native_order_executor_is_limit_reduce_not_short_position():
    with pytest.raises(ValueError, match='minimum sell price requires a limit sell'):
        OrderExecutorConfig(
            id='market', timestamp=1, connector_name='okx', trading_pair='A-USDC',
            side=TradeType.SELL, amount=D('1'), price=D('110'),
            execution_strategy=ExecutionStrategy.MARKET, min_sell_price=D('101'))
    cfg = OrderExecutorConfig(
        id='limit', timestamp=1, controller_id='modular_spot_inventory_A_USDC',
        connector_name='okx', trading_pair='A-USDC', side=TradeType.SELL, amount=D('1'),
        price=D('110'), position_action=PositionAction.CLOSE, execution_strategy=ExecutionStrategy.LIMIT,
        leverage=1, min_sell_price=D('101.11'))
    executor = reduce_executor(cfg)
    assert isinstance(executor, OrderExecutor)
    assert not isinstance(executor, PositionExecutor)
    assert executor._max_retries == 0
    assert 'execution_receipt' not in executor.get_custom_info()
    assert executor.get_cum_fees_quote() == 0
    buy = native_config(sell_cfg(), identifier='buy', timestamp=120, price=D('100'), amount=D('1'),
                        cycle=0, stage=0)
    assert buy.side == TradeType.BUY
    with pytest.raises(PolicyError, match='refuses buy'):
        ModularReduceExecutor(executor._strategy, buy)


def test_three_assigned_symbols_admitted_with_native_reconciliation():
    executors = [prior_filled(pair, owner('sui', pair)) for pair in PAIRS]
    handoffs = [assigned(executor, owner('inventory', executor.config.trading_pair)) for executor in executors]
    scope = admit_inventory(handoffs, [executor.executor_info for executor in executors])
    assert tuple(lot.trading_pair for lot in scope.lots) == PAIRS
    assert all(lot.entry_gross_quote == D('100') and lot.available_base == D('1') for lot in scope.lots)
    assert {lot.new_owner for lot in scope.lots} == {owner('inventory', pair) for pair in PAIRS}


def test_missing_or_unknown_basis_is_rejected_without_price_fabrication():
    executor = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    info = executor.executor_info
    with pytest.raises(PolicyError, match='unknown acquisition cost|missing native'):
        admit_inventory(assigned(executor, owner('inventory', 'A-USDC'), entry_gross_quote=D('0')), [info])
    missing = info.model_copy(deep=True)
    missing.custom_info.pop('execution_receipt')
    with pytest.raises(PolicyError, match='unavailable|missing native'):
        admit_inventory(assigned(executor, owner('inventory', 'A-USDC')), [missing])
    with pytest.raises(PolicyError, match='wallet|native reconciliation'):
        admit_inventory(assigned(executor, owner('inventory', 'A-USDC')), [NS(amount=D('4'))])
    with pytest.raises(PolicyError, match='authority|mark price|configuration'):
        admit_inventory(assigned(executor, owner('inventory', 'A-USDC'), authority='mark_price'), [info])


def test_another_owners_lot_is_rejected():
    executor = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    other = NS(
        id='other-lot', controller_id=owner('other', 'A-USDC'), is_done=False, is_active=True,
        config=NS(controller_id=owner('other', 'A-USDC'), connector_name='okx', trading_pair='A-USDC',
                  side=TradeType.BUY),
        custom_info={})
    with pytest.raises(PolicyError, match='another owner'):
        admit_inventory(assigned(executor, owner('inventory', 'A-USDC')),
                        [executor.executor_info, other])


def test_duplicated_or_conflicting_handoff_is_rejected():
    first = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    second = prior_filled('A-USDC', owner('sui2', 'A-USDC'), identifier='second-lot')
    left = assigned(first, owner('inventory', 'A-USDC'))
    with pytest.raises(PolicyError, match='duplicated or conflicting'):
        admit_inventory((left, left), [first.executor_info])
    right = assigned(second, owner('inventory', 'A-USDC'))
    with pytest.raises(PolicyError, match='duplicated or conflicting'):
        admit_inventory((left, right), [first.executor_info, second.executor_info])


def test_repeated_sale_signal_does_not_double_sell():
    executor = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    lot = assigned(executor, owner('inventory', 'A-USDC'))
    scope = admit_inventory(lot, [executor.executor_info])
    first = propose_reduction(scope, observation(), [executor.executor_info])
    assert first.action == 'sell' and first.config.side == TradeType.SELL
    live = reduce_executor(first.config)
    second = propose_reduction(scope, observation(), [executor.executor_info, live.executor_info])
    assert second.action == 'hold' and second.reason == 'outstanding_sell_commitment'
    assert second.entry_quote == D('100')


def test_existing_reservation_occupies_the_committed_base():
    executor = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    lot = assigned(executor, owner('inventory', 'A-USDC'), reserved_base=D('1'))
    scope = admit_inventory(lot, [executor.executor_info])
    reduction = propose_reduction(scope, observation(), [executor.executor_info])
    assert reduction.action == 'hold'
    assert reduction.reason == 'existing_reservation_occupies_base'


def test_net_base_fees_are_included_in_the_sale_cap():
    executor = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    lot = assigned(executor, owner('inventory', 'A-USDC'))
    scope = admit_inventory(lot, [executor.executor_info])
    reduction = propose_reduction(scope, observation(sell_fee_base=D('0.01')), [executor.executor_info])
    assert reduction.action == 'sell'
    assert reduction.amount == D('0.990')
    assert reduction.amount * D('1.01') <= D('1')


def test_dust_is_an_explicit_hold():
    executor = prior_filled('A-USDC', owner('sui', 'A-USDC'), amount='.005')
    lot = assigned(executor, owner('inventory', 'A-USDC'))
    scope = admit_inventory(lot, [executor.executor_info])
    reduction = propose_reduction(scope, observation(bid='110'), [executor.executor_info])
    assert reduction.action == 'hold' and reduction.reason == 'dust_below_exchange_minimum'


def test_partial_sell_cancel_and_late_fill_conserve_quantity_and_cost():
    executor = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    lot = assigned(executor, owner('inventory', 'A-USDC'))
    scope = admit_inventory(lot, [executor.executor_info])
    proposal = propose_reduction(scope, observation(), [executor.executor_info])
    seller = reduce_executor(proposal.config)
    partial = tracked('A-USDC', 'sell-1', TradeType.SELL, '.4', OrderState.PARTIALLY_FILLED,
                      price=D('110'), amount=proposal.amount)
    seller._order = partial
    after_partial = propose_reduction(scope, observation(), [executor.executor_info, seller.executor_info])
    assert after_partial.action == 'hold'
    assert after_partial.sold_base == D('.4')
    assert after_partial.remaining_base == D('.6')
    assert after_partial.entry_quote == D('100')
    partial.order.current_state = OrderState.CANCELED
    seller._partial_filled_orders.append(seller._order)
    seller._order = None
    seller.close_type = CloseType.POSITION_HOLD
    seller._status = RunnableStatus.TERMINATED
    seller._held_position_orders = [partial.order.to_json()]
    leftover = propose_reduction(scope, observation(), [executor.executor_info, seller.executor_info])
    assert leftover.action == 'sell'
    assert leftover.amount == D('.6')
    assert leftover.entry_quote == D('100')
    apply = TradeUpdate(trade_id='late', client_order_id='sell-1', exchange_order_id='sell-1',
                        trading_pair='A-USDC', fill_price=D('110'), fill_base_amount=D('.6'),
                        fill_quote_amount=D('66'), fee=AddedToCostTradeFee(), fill_timestamp=4)
    partial.order.update_with_trade_update(apply)
    seller._status = RunnableStatus.TERMINATED
    late = propose_reduction(scope, observation(), [executor.executor_info, seller.executor_info])
    assert late.sold_base == D('1')
    assert late.remaining_base == D('0')
    assert late.entry_quote == D('100')
    assert late.action == 'hold'


def test_gap_below_profit_floor_holds_without_market_fallback():
    executor = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    lot = assigned(executor, owner('inventory', 'A-USDC'))
    scope = admit_inventory(lot, [executor.executor_info])
    reduction = propose_reduction(scope, observation(bid='90'), [executor.executor_info])
    assert reduction.action == 'hold' and reduction.reason == 'gap_below_profit_floor'
    with pytest.raises(ValueError):
        OrderExecutorConfig(
            id='dump', timestamp=1, connector_name='okx', trading_pair='A-USDC',
            side=TradeType.SELL, amount=D('1'), execution_strategy=ExecutionStrategy.MARKET,
            min_sell_price=D('101'))


def test_fee_revision_does_not_invent_a_cheaper_basis():
    executor = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    lot = assigned(executor, owner('inventory', 'A-USDC'))
    scope = admit_inventory(lot, [executor.executor_info])
    cheap = executor.executor_info.model_copy(deep=True)
    cheap.custom_info['execution_receipt']['entry_gross_quote'] = '10'
    cheap.custom_info['entry_price'] = D('10')
    with pytest.raises(PolicyError, match='reconciliation changed'):
        propose_reduction(scope, observation(bid='90'), [cheap])


def test_restart_retains_admitted_scope_and_planned_resume_still_holds(tmp_path):
    executor = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    lot = assigned(executor, owner('inventory', 'A-USDC'))
    engine, recorder, Metadata = metadata_engine(tmp_path)
    recorder.get_all_executors = lambda: [executor.executor_info]
    controller = ModularSpotController(sell_cfg(), NS(ready=False), asyncio.Queue())
    controller.runtime.bindings.metadata_model = Metadata
    controller.runtime.ensure_store = lambda: NS(recorder=recorder)
    scope = controller.runtime.admit_inventory(lot, [executor.executor_info])
    assert tuple(item.trading_pair for item in scope.lots) == ('A-USDC',)
    loaded, _ = SellOnlyStore(recorder, lot.new_owner, 'okx', 'A-USDC', metadata_model=Metadata).load()
    assert loaded.lots == scope.lots
    reduction = propose_reduction(loaded, observation(), [executor.executor_info])
    assert reduction.action == 'sell'
    other = prior_filled('B-USDC', owner('sui', 'B-USDC'))
    empty = controller.runtime.admit_inventory(
        assigned(other, owner('inventory', 'B-USDC')), [other.executor_info])
    assert empty.lots == ()
    retained, _ = SellOnlyStore(recorder, lot.new_owner, 'okx', 'A-USDC', metadata_model=Metadata).load()
    assert retained.lots == scope.lots
    with patch.object(SellOnlyStore, 'replace', return_value=False):
        with pytest.raises(PolicyError, match='persistence failed'):
            controller.runtime.admit_inventory(lot, [executor.executor_info])
    failed, _ = SellOnlyStore(recorder, lot.new_owner, 'okx', 'A-USDC', metadata_model=Metadata).load()
    assert failed.lots == scope.lots
    native = disposable_recorder(tmp_path)
    try:
        with patch('hummingbot.strategy_v2.executors.planned_position_resume.pending_manifest',
                   return_value={'positions': [{'controller_id': controller.config.id}]}):
            with pytest.raises(PolicyError, match='bypasses local executor selection'):
                NativeBindings.guard_resume(controller, native)
    finally:
        release_recorder(native)
        engine.dispose()


def test_zero_buy_dca_repurchase_and_rearm_at_every_boundary(tmp_path):
    bundle = build_profile(ProgramProfile(
        name='inventory', connector='okx', symbols=PAIRS, mode='sell_only',
        quote_budget='300', max_positions=3, account='paper-okx'))
    payloads = [ModularSpotControllerConfig.model_validate(item) for item in bundle['controllers']]
    assert all(c.execution_enabled is False and c.acquisition_enabled is False for c in payloads)
    assert all(c.acquisition.fractions == (D('1'),) and c.initial_positions == [] for c in payloads)
    assert bundle['inventory_adoption_supported'] is False
    executor = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    lot = assigned(executor, owner('inventory', 'A-USDC'))
    scope = admit_inventory(lot, [executor.executor_info])
    reduction = propose_reduction(scope, observation(), [executor.executor_info])
    assert reduction.config.side == TradeType.SELL
    assert reduction.config.position_action == PositionAction.CLOSE
    assert reduction.config.execution_strategy == ExecutionStrategy.LIMIT
    state = DecisionState(fingerprint='a' * 64, candle=1080)
    _, proposal, reason = plan_inputs(state, {}, allow_acquisition=False)
    assert proposal is None and reason == 'acquisition_disabled'
    assert next_stage(AcquisitionPolicy(), stage=0, bid=D('100'), last_fill_price=None,
                      fresh_signal=True, entry_terminal=True, distribution_started=False) == D('1')
    engine, recorder, Metadata = metadata_engine(tmp_path, 'budget.db')
    budget = BudgetStore(recorder, BudgetPolicy(group='inventory', max_quote=D('300'), max_assets=3),
                         connector='okx', quote='USDC', account='paper-okx', metadata_model=Metadata)
    controller = ModularSpotController(sell_cfg(execution_enabled=True), NS(ready=True, time=lambda: 1200),
                                       asyncio.Queue())
    rt = controller.runtime
    assert rt.acquisition_permitted() is False
    assert rt.reason == 'sell_only_no_acquisition'
    rt.budget = budget
    rt.bindings.action = lambda cid, config: config
    rt.ensure_store = lambda: NS(recorder=NS(get_executors_by_controller=lambda cid: [executor.executor_info],
                                             get_all_executors=lambda: [executor.executor_info]))
    rt._sell_only_store = lambda: NS(load=lambda: (scope, 'raw'))
    rt.observe = lambda: MarketFeatures(1140, D('100'), D('29'), D('31'), False, False)
    rt.snapshot = lambda: dict(bid=D('110'), ask=D('110.1'), available_quote=D('100'), pnl=D('0'),
                               amount_step=D('.001'), minimum_amount=D('.01'), minimum_notional=D('1'))
    rt.controller.market_data_provider = NS(
        time=lambda: 1200,
        connectors={'okx': NS(
            trading_rules={'A-USDC': NS(min_price_increment=D('.01'))},
            get_fee=lambda **k: AddedToCostTradeFee(percent=D('0.001')))})
    with patch('controllers.generic.modular_spot_core.runtime.plan') as planned:
        actions = rt.actions()
    planned.assert_not_called()
    assert len(actions) == 1
    assert actions[0].side == TradeType.SELL
    assert budget._load() is None
    buy = native_config(sell_cfg(execution_enabled=True), identifier='rearm', timestamp=120,
                        price=D('100'), amount=D('1'), cycle=0, stage=0)
    with pytest.raises(PolicyError, match='refuses buy'):
        rt.executor_class(buy)
    rt.pending[actions[0].id] = actions[0]
    assert rt.executor_class(actions[0]) is ModularReduceExecutor
    with pytest.raises(PolicyError, match='ungranted|consumed'):
        rt.executor_class(actions[0])
    created = reduce_executor(actions[0])
    assert created._max_retries == 0
    engine.dispose()


def test_granted_reduce_passes_validate_and_create_executor():
    executor = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    lot = assigned(executor, owner('inventory', 'A-USDC'))
    scope = admit_inventory(lot, [executor.executor_info])
    granted = propose_reduction(scope, observation(), [executor.executor_info]).config
    controller = ModularSpotController(sell_cfg(execution_enabled=True), NS(ready=True), asyncio.Queue())
    controller._status = RunnableStatus.RUNNING
    rt = controller.runtime
    disabled = ModularSpotController(sell_cfg(), NS(ready=True), asyncio.Queue())
    disabled._status = RunnableStatus.RUNNING
    disabled.runtime.pending[granted.id] = granted.model_copy(deep=True)
    assert not disabled.validate_executor_config(granted)
    buy = native_config(sell_cfg(execution_enabled=True), identifier='buy-gate', timestamp=120,
                        price=D('100'), amount=D('1'), cycle=0, stage=0)
    short = OrderExecutorConfig(
        id='short-gate', timestamp=1200, controller_id=controller.config.id, connector_name='okx',
        trading_pair='A-USDC', side=TradeType.SELL, amount=D('1'), price=D('110'),
        position_action=PositionAction.OPEN, execution_strategy=ExecutionStrategy.LIMIT, leverage=1,
        min_sell_price=D('101.11'))
    unfloored = OrderExecutorConfig(
        id='unfloored-gate', timestamp=1200, controller_id=controller.config.id, connector_name='okx',
        trading_pair='A-USDC', side=TradeType.SELL, amount=D('1'), price=D('110'),
        position_action=PositionAction.CLOSE, execution_strategy=ExecutionStrategy.LIMIT, leverage=1)
    with orchestrator_for(controller) as (orchestrator, strategy):
        # This tests the controller intent gate after wallet admission; provide
        # enough quote for the deliberately invalid 100-USDC BUY intent.
        strategy.connectors['okx'].get_available_balance.side_effect = lambda asset: D('200') if asset == 'USDC' else D('10')

        def rejected(config, *, pending=True):
            if pending:
                rt.pending[config.id] = config.model_copy(deep=True)
            assert not controller.validate_executor_config(config)
            with pytest.raises(PolicyError, match='refuses buy, short, or ungranted'):
                controller.get_executor_class_for_config(config)
            before = list(orchestrator.active_executors[controller.config.id])
            orchestrator.create_executor(CreateExecutorAction(
                controller_id=controller.config.id, executor_config=config))
            assert orchestrator.active_executors[controller.config.id] == before
            rt.pending.pop(config.id, None)

        rejected(buy)
        rejected(short)
        rejected(unfloored)
        rejected(granted.model_copy(update={'id': 'ungranted-gate'}), pending=False)
        rt.pending[granted.id] = granted.model_copy(deep=True)
        assert controller.validate_executor_config(granted)
        with patch.object(ModularReduceExecutor, 'start',
                          lambda self: setattr(self, '_status', RunnableStatus.RUNNING)):
            orchestrator.create_executor(CreateExecutorAction(
                controller_id=controller.config.id, executor_config=granted))
        created = orchestrator.active_executors[controller.config.id][0]
        assert type(created) is ModularReduceExecutor
        assert isinstance(created, OrderExecutor)
        assert not isinstance(created, PositionExecutor)
        assert created._max_retries == 0
        assert created.config.id == granted.id
        assert not controller.validate_executor_config(granted)


def test_actions_emits_leftover_sale_after_partial_cancel():
    executor = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    lot = assigned(executor, owner('inventory', 'A-USDC'))
    scope = admit_inventory(lot, [executor.executor_info])
    native_rows = [executor.executor_info]
    controller = ModularSpotController(sell_cfg(execution_enabled=True), NS(ready=True), asyncio.Queue())
    rt = bind_sell_runtime(controller.runtime, scope=scope, native_rows=native_rows)
    first = rt.actions()
    assert len(first) == 1
    granted = first[0].executor_config
    assert granted.amount == D('1')
    second_while_pending = rt.actions()
    assert second_while_pending == []
    seller = reduce_executor(granted)
    partial = tracked('A-USDC', 'sell-1', TradeType.SELL, '.4', OrderState.PARTIALLY_FILLED,
                      price=D('110'), amount=granted.amount)
    seller._order = partial
    partial.order.current_state = OrderState.CANCELED
    seller._partial_filled_orders.append(seller._order)
    seller._order = None
    seller.close_type = CloseType.POSITION_HOLD
    seller._status = RunnableStatus.TERMINATED
    seller._held_position_orders = [partial.order.to_json()]
    native_rows.append(seller.executor_info)
    leftover = rt.actions()
    assert len(leftover) == 1
    assert leftover[0].executor_config.amount == D('.6')
    assert leftover[0].executor_config.id != granted.id
    assert granted.id not in rt.pending


def test_all_executors_visible_to_reduction_and_admit():
    executor = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    lot = assigned(executor, owner('inventory', 'A-USDC'))
    scope = admit_inventory(lot, [executor.executor_info])
    foreign = NS(
        id='foreign-sell', controller_id=owner('other', 'A-USDC'), is_done=False, is_active=True,
        config=NS(controller_id=owner('other', 'A-USDC'), connector_name='okx', trading_pair='A-USDC',
                  side=TradeType.SELL, amount=D('1')),
        custom_info={'executed_amount_base': 0})
    native_rows = [executor.executor_info, foreign]
    controller = ModularSpotController(sell_cfg(execution_enabled=True), NS(ready=True), asyncio.Queue())
    rt = bind_sell_runtime(controller.runtime, scope=scope, native_rows=native_rows)
    assert rt.actions() == []
    assert rt.reason == 'another owner already manages this lot'
    admit_rt = ModularSpotController(sell_cfg(execution_enabled=True), NS(ready=True), asyncio.Queue()).runtime
    admit_rt.ensure_store = lambda: NS(recorder=NS(get_all_executors=lambda: [foreign]))
    admit_rt._sell_only_store = lambda: NS(replace=lambda lots: True)
    with pytest.raises(PolicyError, match='another owner'):
        admit_rt.admit_inventory(lot, [executor.executor_info])


def test_native_entry_fee_prevents_loss_and_fee_handoff_mutation():
    from hummingbot.core.data_type.trade_fee import TokenAmount
    prior = prior_filled('A-USDC', owner('sui', 'A-USDC'),
                         fee=AddedToCostTradeFee(flat_fees=[TokenAmount('USDC', D('5'))]))
    lot = assigned(prior, owner('inventory', 'A-USDC'))
    scope = admit_inventory(lot, [prior.executor_info])
    assert propose_reduction(scope, observation(bid='102'), [prior.executor_info]).action == 'hold'
    with pytest.raises(PolicyError, match='fee'):
        admit_inventory(assigned(prior, lot.new_owner, fees_quote=D('0')), [prior.executor_info])


def test_missing_current_native_receipt_does_not_authorize_sale():
    prior = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    scope = admit_inventory(assigned(prior, owner('inventory', 'A-USDC')), [prior.executor_info])
    with pytest.raises(PolicyError, match='reconciliation'):
        propose_reduction(scope, observation(), [])


def test_sell_runtime_requires_observation_and_bound_account():
    from dataclasses import replace
    prior = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    lot = assigned(prior, owner('inventory', 'A-USDC'))
    scope = admit_inventory(lot, [prior.executor_info])
    controller = ModularSpotController(sell_cfg(execution_enabled=True), NS(ready=True), asyncio.Queue())
    rt = bind_sell_runtime(controller.runtime, scope=scope, native_rows=[prior.executor_info])
    rt.observe = lambda: None
    assert rt.actions() == []
    rt._sell_only_store = lambda: NS(replace=lambda lots: True)
    with pytest.raises(PolicyError, match='account'):
        rt.admit_inventory(replace(lot, account='other-account'), [prior.executor_info])


def test_sell_runtime_uses_fresher_native_fill_report():
    prior = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    scope = admit_inventory(assigned(prior, owner('inventory', 'A-USDC')), [prior.executor_info])
    config = propose_reduction(scope, observation(), [prior.executor_info]).config
    seller = reduce_executor(config)
    partial = tracked('A-USDC', 'sell1', TradeType.SELL, '.4', OrderState.CANCELED,
                      price=D('110'), amount=D('1'))
    seller._partial_filled_orders = [partial]
    seller._held_position_orders = [partial.order.to_json()]
    seller.close_type = CloseType.POSITION_HOLD
    seller._status = RunnableStatus.TERMINATED
    stale = seller.executor_info
    partial.order.update_with_trade_update(TradeUpdate(
        trade_id='late', client_order_id='sell1', exchange_order_id='sell1', trading_pair='A-USDC',
        fill_price=D('110'), fill_base_amount=D('.6'), fill_quote_amount=D('66'),
        fee=AddedToCostTradeFee(), fill_timestamp=4))
    seller._held_position_orders = [partial.order.to_json()]
    controller = ModularSpotController(sell_cfg(execution_enabled=True), NS(ready=True), asyncio.Queue())
    rt = bind_sell_runtime(controller.runtime, scope=scope, native_rows=[prior.executor_info, stale])
    controller.executors_info = [seller.executor_info]
    assert rt.actions() == []


def test_reduce_executor_checks_retry_limit_before_order_submission():
    prior = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    scope = admit_inventory(assigned(prior, owner('inventory', 'A-USDC')), [prior.executor_info])
    seller = reduce_executor(propose_reduction(scope, observation(), [prior.executor_info]).config)
    seller._current_retries = 1
    seller.control_order()
    seller.place_order.assert_not_called()


def test_runtime_holds_unsupported_base_and_flat_sale_fees():
    from hummingbot.core.data_type.trade_fee import TokenAmount
    prior = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    scope = admit_inventory(assigned(prior, owner('inventory', 'A-USDC')), [prior.executor_info])
    for fee in (AddedToCostTradeFee(percent=D('.01'), percent_token='A'),
                AddedToCostTradeFee(flat_fees=[TokenAmount('A', D('.01'))]),
                AddedToCostTradeFee(flat_fees=[TokenAmount('USDC', D('1'))])):
        controller = ModularSpotController(sell_cfg(execution_enabled=True), NS(ready=True), asyncio.Queue())
        rt = bind_sell_runtime(controller.runtime, scope=scope, native_rows=[prior.executor_info])
        controller.market_data_provider.connectors['okx'].get_fee = lambda **kwargs: fee
        assert rt.actions() == []
        assert rt.reason == 'unsupported_fees_or_book'


def test_sell_handoff_rejects_incomplete_and_nonquote_native_fees():
    from hummingbot.core.data_type.trade_fee import TokenAmount
    prior = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    lot = assigned(prior, owner('inventory', 'A-USDC'))
    incomplete = prior.executor_info.model_copy(deep=True)
    incomplete.custom_info['execution_order_snapshots'][0]['order_fills'] = {}
    with pytest.raises(PolicyError, match='incomplete'):
        admit_inventory(lot, [incomplete])
    charged = prior_filled('A-USDC', owner('sui', 'A-USDC'),
                           fee=AddedToCostTradeFee(flat_fees=[TokenAmount('A', D('.01'))]))
    with pytest.raises(PolicyError, match='non-quote fee'):
        admit_inventory(assigned(charged, lot.new_owner), [charged.executor_info])


def test_native_reduce_rechecks_fee_asset_before_submission():
    from hummingbot.core.data_type.trade_fee import TokenAmount
    prior = prior_filled('A-USDC', owner('sui', 'A-USDC'))
    scope = admit_inventory(assigned(prior, owner('inventory', 'A-USDC')), [prior.executor_info])
    seller = reduce_executor(propose_reduction(scope, observation(), [prior.executor_info]).config)
    seller.connectors['okx'].get_fee.return_value = AddedToCostTradeFee(
        flat_fees=[TokenAmount('A', D('.1'))])
    seller.control_order()
    seller.place_order.assert_not_called()
