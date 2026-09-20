"""One recorder, multiple genuine native pair owners, one bounded intent envelope."""
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from controllers.generic.modular_spot_core.native_funding import CandidateFundingPolicy
from controllers.generic.rsi_modular import get_config_class


def test_strategy_scope_requires_shared_explicit_limits_and_exact_native_owners():
    from controllers.generic.modular_spot_core.native_funding import bind_funding_scope
    policy = dict(allocation_quote='50', fee_reserve_fraction='.003', scope='strategy', max_position_quote='25')
    owners = {}
    for pair in ('BTC-USDC', 'ETH-USDC'):
        cfg = get_config_class('ok_rsi')(profile='ok_rsi', id='modular_spot_v2_' + pair[:3].lower(), trading_pair=pair, funding=policy)
        owners[cfg.id] = SimpleNamespace(config=cfg)
    bind_funding_scope(owners)
    assert all(owner._funding_peers == tuple(owners.values()) for owner in owners.values())
    with pytest.raises(ValueError):
        CandidateFundingPolicy(**(policy | {'max_position_quote': '51'}))
    owners[next(iter(owners))].config.funding = CandidateFundingPolicy(**(policy | {'allocation_quote': '49'}))
    with pytest.raises(ValueError, match='identical'):
        bind_funding_scope(owners)


@pytest.mark.asyncio
async def test_actual_native_pairs_share_budget_and_retain_partial_fill_across_restart(tmp_path):
    from test.hummingbot.strategy_v2.paper.test_paper_session import causal_reversal_frames

    from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import NativePaperTradeExchange
    from hummingbot.connector.markets_recorder import MarketsRecorder
    from hummingbot.connector.trading_rule import TradingRule
    from hummingbot.core.data_type.common import TradeType
    from hummingbot.core.data_type.trade_fee import TradeFeeSchema
    from hummingbot.strategy_v2.paper_session import NativePaperSession
    pairs = ('BTC-USDC', 'ETH-USDC', 'SOL-USDC')
    rules = {p: TradingRule(p, min_order_size=D('.01'), min_price_increment=D('.01'),
                            min_base_amount_increment=D('.01'), min_notional_size=D('1')) for p in pairs}

    def market():
        result = NativePaperTradeExchange(rules, fee_schema=TradeFeeSchema(
            maker_percent_fee_decimal=D('.001'), taker_percent_fee_decimal=D('.002')), trade_participation=D('1'))
        result.set_balance('USDC', D('1000'))
        return result
    configs = [get_config_class('ok_rsi')(profile='ok_rsi', id='modular_spot_' + p[:3].lower(),
                                          trading_pair=p, connector_name='okx_paper_trade', execution_mode='paper', candles_connector='okx',
                                          execution_enabled=True, min_signal_score=1, strict_hmm_mode=False, trend_confirmation_enabled=False,
                                          dynamic_position_sizing=False, usd_per_entry=D('24'), funding=dict(allocation_quote='50',
                                                                                                             max_position_quote='25', fee_reserve_fraction='.003', scope='strategy')) for p in pairs]
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    with patch('socket.socket.connect', side_effect=AssertionError('network forbidden')):
        session = NativePaperSession(market(), configs, tmp_path / 'shared.sqlite')
        try:
            for i, frame in enumerate(causal_reversal_frames()):
                now = 600000 + i * 60
                await session.step(now, books=[dict(trading_pair=p, sequence=i + 1, timestamp=now,
                                                    bids=[(90, 10)], asks=[(90.1, 10)]) for p in pairs], candles=[dict(connector_name='okx',
                                                                                                                       trading_pair=p, interval='1m', frame=frame, available_at=now) for p in pairs])
            active = [e for entries in session.executor_orchestrator.active_executors.values() for e in entries]
            assert len(active) == 2, [c._funding_hold for c in session.controllers.values()]
            assert sum(e.config.amount * e.config.entry_price * D('1.003') for e in active) <= 50
            assert all(e.config.amount * e.config.entry_price * D('1.003') <= 25 for e in active)
            assert 'exhausted' in session.controllers['modular_spot_sol']._funding_hold
            from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
            owner = session.controllers['modular_spot_btc']
            extra = PositionExecutorConfig(timestamp=600060, connector_name='okx_paper_trade', trading_pair='BTC-USDC',
                                           side=TradeType.BUY, amount=D('.02'), entry_price=D('90'))
            with pytest.raises(ValueError, match='per-position'):
                owner._funding_store.admit(extra)
            await session.step(600061, trades=[dict(trading_pair='BTC-USDC', sequence=1, timestamp=600061,
                                                    side=TradeType.SELL, price=active[0].config.entry_price - D('.01'), amount=D('.1'))])
            assert session._connector.get_balance('BTC') == D('.1')
        finally:
            await session.close()
        restored = NativePaperSession(market(), configs, tmp_path / 'shared.sqlite', restore=True)
        try:
            from controllers.generic.modular_spot_core.native_funding import NativeFundingStore
            assert restored._connector.get_balance('BTC') == D('.1')
            stores = [NativeFundingStore(c, restored.recorder) for c in restored.controllers.values()]
            statuses = [store.reconcile() for store in stores]
            assert len({store.binding for store in stores}) == 1
            assert len({s['available_quote'] for s in statuses}) == 1
            assert D('40') < D(statuses[0]['available_quote']) < D('42')
            assert all(s['released_intents'] == 2 for s in statuses)
            assert not restored._connector.in_flight_orders
        finally:
            await restored.close()
            MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_native_cash_shortage_precedes_durable_funding_admission():
    from unittest.mock import Mock

    from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
    from hummingbot.core.data_type.common import TradeType
    from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
    from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
    from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
    connector = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['BTC-USDC'])
    owner = SimpleNamespace(config=SimpleNamespace(id='owned'), validate_executor_config=Mock(return_value=True))
    orchestrator = object.__new__(ExecutorOrchestrator)
    orchestrator._planned_resume_pending = False
    orchestrator.recovery_required = {}
    orchestrator.active_executors = {'owned': []}
    orchestrator.stored_executors = {}
    orchestrator.strategy = SimpleNamespace(controllers={'owned': owner}, connectors={'okx': connector})
    config = PositionExecutorConfig(timestamp=1, connector_name='okx', trading_pair='BTC-USDC',
                                    side=TradeType.BUY, amount=D('.1'), entry_price=D('90'))
    with pytest.raises(RuntimeError, match='Insufficient unreserved'):
        orchestrator.create_executor(CreateExecutorAction(controller_id='owned', executor_config=config))
    owner.validate_executor_config.assert_not_called()
