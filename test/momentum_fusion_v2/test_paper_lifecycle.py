"""Real synthetic offline lifecycle, never exchange activation or economic replay."""
import hashlib
import json
from decimal import Decimal as D

import pandas as pd
import pytest

from controllers.generic.momentum_fusion_v2 import MomentumFusionV2Config
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import NativePaperTradeExchange
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.trade_fee import TradeFeeSchema
from hummingbot.strategy_v2.paper_session import NativePaperSession

from .fixtures import T, frames, recipe


def make_market():
    market = NativePaperTradeExchange({'BNB-USDC': TradingRule('BNB-USDC', min_order_size=D('.01'),
                                                               min_base_amount_increment=D('.01'), min_price_increment=D('.01'), min_notional_size=D('1'))},
                                      fee_schema=TradeFeeSchema(maker_percent_fee_decimal=D('.001'), taker_percent_fee_decimal=D('.002')))
    market.set_balance('USDC', D('1000'))
    return market


def settings(market):
    from controllers.generic.momentum_fusion_v2_runtime.paper_lifecycle import MomentumPaperPolicy
    policy = MomentumPaperPolicy(trade_quote='100', account_quote='200', max_loss_quote='20',
                                 stop_loss='.05', trailing_activation='.02', trailing_delta='.01', entry_ttl_seconds=2)
    r = recipe(**policy.recipe_bindings(hashlib.sha256(json.dumps(
        market.tracking_states['balances'], sort_keys=True).encode()).hexdigest()))
    config = MomentumFusionV2Config(id='momentum_v2_fixture', trading_pair='BNB-USDC', recipe=r,
                                    execution_mode='paper', connector_name=market.name)
    return config, policy


def inputs(config, i=0, **last):
    return [dict(connector_name='okx', trading_pair='BNB-USDC', interval=s.dataset.interval,
                 available_at=T + i * 60 + 1, frame=pd.DataFrame([dict(timestamp=c.open_time, open=c.open, high=c.high,
                                                                       low=c.low, close=c.close, volume=c.volume) for c in s.rows]))
            for s in frames(config.recipe, i, **last).values()]


def book(timestamp, sequence, bid=100, ask=101):
    return dict(trading_pair='BNB-USDC', timestamp=timestamp, sequence=sequence, bids=[(bid, 10)], asks=[(ask, 10)])


@pytest.mark.asyncio
async def test_native_opportunity_is_durable_and_consumed_once(tmp_path):
    market = make_market(); config, policy = settings(market)
    session = NativePaperSession(market, [config], tmp_path / 'native.sqlite', momentum_fixture_policy=policy)
    try:
        seeded = await session.step(T + 1, books=[book(T + 1, 1)], candles=inputs(config))
        assert seeded['actions'] == []
        entered = await session.step(T + 61, books=[book(T + 61, 2)], candles=inputs(config, 1, close=D('102'), high=D('103'), low=D('100'), volume=D('1000')))
        assert len(entered['actions']) == 1, session.controllers[config.id].processed_data
        oid = list(market.in_flight_orders)
        assert len(oid) == 1
        again = await session.step(T + 61, books=[book(T + 61, 3)])
        assert again['actions'] == [] and list(market.in_flight_orders) == oid
        assert len(session.recorder.get_all_executors()) == 1
        assert config.execution_enabled is False
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_partial_entry_fee_and_trailing_survive_restart_then_native_exit(tmp_path):
    from hummingbot.core.data_type.common import TradeType
    market = make_market(); config, policy = settings(market); path = tmp_path / 'native.sqlite'
    session = NativePaperSession(market, [config], path, momentum_fixture_policy=policy)
    restored = None
    try:
        await session.step(T + 1, books=[book(T + 1, 1)], candles=inputs(config))
        await session.step(T + 61, books=[book(T + 61, 2)], candles=inputs(config, 1, close=D('102'), high=D('103'), low=D('100'), volume=D('1000')))
        await session.step(T + 62, books=[book(T + 62, 3)], trades=[dict(trading_pair='BNB-USDC', sequence=1, timestamp=T + 62,
                                                                         side=TradeType.SELL, price=D('100'), amount=D('4'))])
        await session.step(T + 64, books=[book(T + 64, 4)])
        executor = session.executor_orchestrator.active_executors[config.id][0]
        assert executor.open_filled_amount == D('.4')
        assert executor._open_order.order.is_cancelled
        assert executor.cum_fees_quote == D('.04')
        await session.step(T + 65, books=[book(T + 65, 5, bid=105, ask=106)])
        trigger = executor._trailing_stop_trigger_pct
        assert trigger is not None
        cash = market.get_balance('USDC')
        await session.suspend()
        restored_market = make_market()
        restored = NativePaperSession(restored_market, [config], path, restore=True, momentum_fixture_policy=policy)
        assert restored_market.get_balance('USDC') == cash
        native = restored.executor_orchestrator.active_executors[config.id][0]
        assert native.config.id == executor.config.id
        assert native._trailing_stop_trigger_pct == trigger
        assert native.open_filled_amount == D('.4') and native.cum_fees_quote == D('.04')
        assert not restored.executor_orchestrator.recovery_required.get(config.id)
        await restored.step(T + 66, books=[book(T + 66, 6, bid=102, ask=103)])
        await restored.step(T + 67, books=[book(T + 67, 7, bid=102, ask=103)])
        assert native.close_filled_amount == D('.4')
        assert restored_market.get_balance('BNB') == 0
        assert native.cum_fees_quote == D('.1216')
        assert len(restored.recorder.get_trades_for_config('native-paper-session')) == 2
        assert len(restored.recorder.get_all_executors()) == 1
    finally:
        await (restored or session).close()


@pytest.mark.asyncio
async def test_capital_prerequisite_cannot_be_fabricated_from_signal(tmp_path):
    market = make_market(); market.set_balance('USDC', D('50')); config, policy = settings(market)
    session = NativePaperSession(market, [config], tmp_path / 'native.sqlite', momentum_fixture_policy=policy)
    try:
        await session.step(T + 1, books=[book(T + 1, 1)], candles=inputs(config))
        result = await session.step(T + 61, books=[book(T + 61, 2)], candles=inputs(config, 1, close=D('102'), high=D('103'), low=D('100'), volume=D('1000')))
        assert result['actions'] == [] and not market.in_flight_orders
        decision = session.controllers[config.id].processed_data['decision']
        assert 'capital:HOLD' in decision['trace']['blockers']
        assert session.recorder.get_all_executors() == []
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_pending_entry_restart_holds_without_replaying_buy(tmp_path):
    market = make_market(); config, policy = settings(market); path = tmp_path / 'native.sqlite'
    session = NativePaperSession(market, [config], path, momentum_fixture_policy=policy)
    restored = None
    try:
        await session.step(T + 1, books=[book(T + 1, 1)], candles=inputs(config))
        await session.step(T + 61, books=[book(T + 61, 2)], candles=inputs(config, 1, close=D('102'), high=D('103'), low=D('100'), volume=D('1000')))
        ids = set(market.in_flight_orders)
        await session.suspend()
        restored = NativePaperSession(make_market(), [config], path, restore=True, momentum_fixture_policy=policy)
        assert restored.executor_orchestrator.recovery_required[config.id]
        assert restored.executor_orchestrator.active_executors[config.id] == []
        result = await restored.step(T + 62, books=[book(T + 62, 3)], candles=inputs(config, 1, close=D('102'), high=D('103'), low=D('100'), volume=D('1000')))
        assert result['actions'] == [] and set(restored._connector.in_flight_orders) == ids
        assert len(restored.recorder.get_all_executors()) == 1
    finally:
        await (restored or session).close()


@pytest.mark.asyncio
async def test_recipe_mapping_cannot_change_under_same_binding(tmp_path):
    market = make_market(); config, policy = settings(market)
    with pytest.raises(ValueError, match='differs from the recipe'):
        NativePaperSession(market, [config], tmp_path / 'native.sqlite', momentum_fixture_policy=policy.model_copy(update={'trade_quote': D('200')}))


@pytest.mark.asyncio
async def test_unknown_account_asset_blocks_complete_evidence(tmp_path):
    market = make_market(); config, policy = settings(market)
    session = NativePaperSession(market, [config], tmp_path / 'native.sqlite', momentum_fixture_policy=policy)
    try:
        await session.step(T + 1, books=[book(T + 1, 1)], candles=inputs(config))
        market.set_balance('UNKNOWN', D('1'))
        result = await session.step(T + 61, books=[book(T + 61, 2)], candles=inputs(config, 1, close=D('102'), high=D('103'), low=D('100'), volume=D('1000')))
        assert result['actions'] == [] and not market.in_flight_orders
        assert not session.controllers[config.id].processed_data['data_health']['ready']
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_unstarted_checkpoint_with_prior_canceled_order_cannot_replay(tmp_path):
    from hummingbot.strategy_v2.models.base import RunnableStatus
    market = make_market()
    config, policy = settings(market)
    session = NativePaperSession(market, [config], tmp_path / 'native.sqlite', momentum_fixture_policy=policy)
    try:
        await session.step(T + 1, books=[book(T + 1, 1)], candles=inputs(config))
        await session.step(T + 61, books=[book(T + 61, 2)], candles=inputs(
            config, 1, close=D('102'), high=D('103'), low=D('100'), volume=D('1000')))
        await market.cancel_all(5)
        info = session.recorder.get_all_executors()[0].model_copy(update={
            'status': RunnableStatus.NOT_STARTED, 'custom_info': {'execution_order_snapshots': []}})
        assert session.momentum_fixture.restore_executor(info) is None
        assert 'native order history' in session.controllers[config.id]._recovery_hold
    finally:
        await session.close()


async def enter_and_close(session, config):
    from hummingbot.core.data_type.common import TradeType
    await session.step(T + 1, books=[book(T + 1, 1)], candles=inputs(config))
    await session.step(T + 61, books=[book(T + 61, 2)], candles=inputs(
        config, 1, close=D('102'), high=D('103'), low=D('100'), volume=D('1000')))
    await session.step(T + 62, books=[book(T + 62, 3)], trades=[dict(
        trading_pair='BNB-USDC', sequence=1, timestamp=T + 62, side=TradeType.SELL, price=D('100'), amount=D('9.9'))])
    session.momentum_fixture.pause_acquisition(True)
    await session.step(T + 63, books=[book(T + 63, 4, bid=105, ask=106)])
    await session.step(T + 64, books=[book(T + 64, 5, bid=102, ask=103)])
    await session.step(T + 65, books=[book(T + 65, 6, bid=102, ask=103)])
    assert session._connector.get_balance('BNB') == 0
    assert session.recorder.get_all_executors()[0].is_done


@pytest.mark.asyncio
async def test_new_episode_requires_flat_terminal_proof_and_never_reuses_consumption(tmp_path):
    from hummingbot.model.metadata import Metadata
    market = make_market()
    config, policy = settings(market)
    session = NativePaperSession(market, [config], tmp_path / 'native.sqlite', momentum_fixture_policy=policy)
    try:
        await enter_and_close(session, config)
        assert session.momentum_fixture.acquisition_paused()
        assert not market.in_flight_orders
        session.momentum_fixture.pause_acquisition(False)
        seeded = await session.step(T + 121, books=[book(T + 121, 7)], candles=inputs(config, 2))
        assert seeded['actions'] == []
        entered = await session.step(T + 181, books=[book(T + 181, 8)], candles=inputs(
            config, 3, close=D('102'), high=D('103'), low=D('100'), volume=D('1000')))
        assert len(entered['actions']) == 1
        again = await session.step(T + 181, books=[book(T + 181, 9)])
        assert again['actions'] == []
        rows = session.recorder.get_all_executors()
        assert len(rows) == 2 and len({row.id for row in rows}) == 2
        with session._sql.get_new_session() as db:
            consumed = db.query(Metadata).filter(Metadata.key.like('momentum-paper-v1:%:consumed:%')).all()
        assert len(consumed) == 2
        assert session.momentum_fixture._checkpoint['episode'] == 1
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_missing_terminal_receipt_prevents_new_episode(tmp_path):
    from hummingbot.model.order_status import OrderStatus
    market = make_market()
    config, policy = settings(market)
    session = NativePaperSession(market, [config], tmp_path / 'native.sqlite', momentum_fixture_policy=policy)
    try:
        await enter_and_close(session, config)
        session.momentum_fixture.pause_acquisition(False)
        with session._sql.get_new_session() as db, db.begin():
            db.query(OrderStatus).delete()
        result = await session.step(T + 121, books=[book(T + 121, 7)], candles=inputs(config, 2))
        assert result['actions'] == []
        assert len(session.recorder.get_all_executors()) == 1
        assert session.momentum_fixture._checkpoint['episode'] == 0
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_native_fixture_holds_process_lease_until_closed(tmp_path):
    import subprocess
    import sys
    market = make_market()
    config, policy = settings(market)
    path = tmp_path / 'native.sqlite'
    session = NativePaperSession(market, [config], path, momentum_fixture_policy=policy)
    code = '''import fcntl,sys
with open(sys.argv[1], 'a') as lease:
    try: fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError: sys.exit(23)
'''
    try:
        result = subprocess.run([sys.executable, '-c', code, str(path.with_suffix('.momentum-owner.lock'))], timeout=10)
        assert result.returncode == 23
    finally:
        await session.close()
    assert subprocess.run([sys.executable, '-c', code, str(path.with_suffix('.momentum-owner.lock'))], timeout=10).returncode == 0


async def two_episode_outcome(path, suspend_second):
    from hummingbot.core.data_type.common import TradeType
    market = make_market()
    config, policy = settings(market)
    session = NativePaperSession(market, [config], path, momentum_fixture_policy=policy)
    try:
        await enter_and_close(session, config)
        session.momentum_fixture.pause_acquisition(False)
        await session.step(T + 121, books=[book(T + 121, 7)], candles=inputs(config, 2))
        await session.step(T + 181, books=[book(T + 181, 8)], candles=inputs(
            config, 3, close=D('102'), high=D('103'), low=D('100'), volume=D('1000')))
        await session.step(T + 182, books=[book(T + 182, 9)], trades=[dict(
            trading_pair='BNB-USDC', sequence=2, timestamp=T + 182, side=TradeType.SELL, price=D('100'), amount=D('4'))])
        await session.step(T + 184, books=[book(T + 184, 10)])
        session.momentum_fixture.pause_acquisition(True)
        await session.step(T + 185, books=[book(T + 185, 11, bid=105, ask=106)])
        active = [e for e in session.executor_orchestrator.active_executors[config.id] if not e.is_closed][0]
        trigger = active._trailing_stop_trigger_pct
        if suspend_second:
            await session.suspend()
            session = NativePaperSession(make_market(), [config], path, restore=True, momentum_fixture_policy=policy)
            assert session.momentum_fixture.acquisition_paused()
            active = session.executor_orchestrator.active_executors[config.id][0]
            assert active._trailing_stop_trigger_pct == trigger
            assert not session.executor_orchestrator.recovery_required.get(config.id)
        await session.step(T + 186, books=[book(T + 186, 12, bid=102, ask=103)])
        await session.step(T + 187, books=[book(T + 187, 13, bid=102, ask=103)])
        assert session._connector.get_balance('BNB') == 0
        infos = session.recorder.get_all_executors()
        assert len(infos) == 2 and all(info.is_done for info in infos)
        fills = session.recorder.get_trades_for_config('native-paper-session')
        return dict(balances=session._connector.get_all_balances(), trigger=trigger,
                    executors=sorted(info.id for info in infos),
                    fills=sorted((fill.trade_type, fill.amount_decimal, fill.price_decimal,
                                  fill.trade_fee_in_quote_decimal) for fill in fills),
                    episode=session.momentum_fixture._checkpoint['episode'])
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_resumed_second_episode_matches_uninterrupted_native_outcome(tmp_path):
    uninterrupted = await two_episode_outcome(tmp_path / 'continuous.sqlite', False)
    resumed = await two_episode_outcome(tmp_path / 'resumed.sqlite', True)
    assert resumed == uninterrupted
    assert len(resumed['fills']) == 4 and resumed['episode'] == 1


@pytest.mark.asyncio
async def test_corrupt_initial_cash_checkpoint_cannot_change_loss_allowance(tmp_path):
    from hummingbot.model.metadata import Metadata
    market = make_market()
    config, policy = settings(market)
    session = NativePaperSession(market, [config], tmp_path / 'native.sqlite', momentum_fixture_policy=policy)
    try:
        await session.step(T + 1, books=[book(T + 1, 1)], candles=inputs(config))
        with session._sql.get_new_session() as db, db.begin():
            row = db.get(Metadata, session.momentum_fixture.key)
            payload = json.loads(row.value)
            payload['initial_quote'] = '1'
            row.value = json.dumps(payload)
        result = await session.step(T + 61, books=[book(T + 61, 2)], candles=inputs(
            config, 1, close=D('102'), high=D('103'), low=D('100'), volume=D('1000')))
        assert result['actions'] == [] and not market.in_flight_orders
        assert 'initial cash changed' in session.controllers[config.id].processed_data['data_health']['reason']
    finally:
        await session.close()
