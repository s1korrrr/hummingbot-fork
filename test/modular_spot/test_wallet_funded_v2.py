"""Wallet-funded V2 preserves native admission while removing accumulated caps."""
from decimal import Decimal as D

import pytest

from controllers.generic.modular_ok_rsi import ModularOKRSIControllerConfig
from controllers.generic.modular_spot_core.bag import LiveCapitalConfig, live_capital_decision
from controllers.generic.modular_spot_core.native_funding import NativeFundingStore, funding_projection


def test_wallet_funded_composition_requires_explicit_unbounded_funding_policy():
    common = dict(id='modular_spot_wallet', trading_pair='BTC-USDC', usd_per_entry=D('100'),
                  allocation_mode='wallet_funded', execution_enabled=True)
    funding = dict(allocation_quote=None, max_position_quote=None,
                   fee_reserve_fraction='.003', resize_to_available_quote=True,
                   scope='strategy')
    config = ModularOKRSIControllerConfig(**common, funding=funding)
    assert config.funding.allocation_quote is None
    assert config.funding.max_position_quote is None
    assert config.usd_per_entry == D('100')
    assert config.model_dump(mode='json')['allocation_mode'] == 'wallet_funded'
    for invalid in (dict(funding, allocation_quote='2500'),
                    dict(funding, max_position_quote='505')):
        with pytest.raises(ValueError, match='wallet_funded'):
            ModularOKRSIControllerConfig(**common, funding=invalid)
    with pytest.raises(ValueError, match='bounded'):
        ModularOKRSIControllerConfig(**(common | {'allocation_mode': 'bounded'}), funding=funding)


def test_bounded_and_wallet_reload_compare_validated_fields_without_changing_identity():
    import asyncio

    from test_native_composition import provider

    from controllers.generic.modular_ok_rsi import ModularOKRSIController

    bounded = ModularOKRSIControllerConfig(id='modular_spot_reload', trading_pair='BTC-USDC')
    assert 'allocation_mode' not in bounded.model_dump(mode='json')
    bot = ModularOKRSIController(bounded, provider(), asyncio.Queue())
    bot.update_config(type(bounded).model_validate(bounded.model_dump(mode='json')))
    assert bot.config.allocation_mode == 'bounded'
    payload = bounded.model_dump(mode='json')
    payload['allocation_mode'] = 'wallet_funded'
    payload['funding'] = {'allocation_quote': None, 'max_position_quote': None,
                          'fee_reserve_fraction': '.003', 'scope': 'controller'}
    wallet = type(bounded).model_validate(payload)
    with pytest.raises(ValueError, match='restart required.*allocation_mode'):
        bot.update_config(wallet)
    assert bot.config.allocation_mode == 'bounded'
    wallet_bot = ModularOKRSIController(wallet, provider(), asyncio.Queue())
    wallet_bot.update_config(type(wallet).model_validate(wallet.model_dump(mode='json')))
    assert wallet_bot.config.allocation_mode == 'wallet_funded'


@pytest.mark.parametrize('mode', ['bounded', 'wallet_funded'])
def test_legacy_hl_rsi_reload_accepts_unchanged_allocation_mode(mode):
    import asyncio

    from test_native_composition import provider

    from controllers.directional_trading.hl_rsi import HLRSIController, HLRSIControllerConfig

    config = HLRSIControllerConfig(id='hl_rsi_reload', allocation_mode=mode,
                                   candles_connector='hyperliquid', candles_trading_pair='HYPE-USDC')
    if mode == 'bounded':
        assert 'allocation_mode' not in config.model_dump(mode='json')
    bot = HLRSIController(config, provider(), asyncio.Queue())
    bot.update_config(type(config).model_validate(config.model_dump(mode='json')))
    assert bot.config.allocation_mode == mode


@pytest.mark.parametrize(('before_mode', 'after_mode'), [
    ('bounded', 'wallet_funded'), ('wallet_funded', 'bounded'),
])
def test_legacy_allocation_mode_change_requires_guarded_restart(before_mode, after_mode):
    import asyncio

    from test_native_composition import provider

    from controllers.directional_trading.hl_rsi import HLRSIController, HLRSIControllerConfig

    config = HLRSIControllerConfig(id='hl_rsi_restart_mode', allocation_mode=before_mode,
                                   candles_connector='hyperliquid', candles_trading_pair='HYPE-USDC')
    bot = HLRSIController(config, provider(), asyncio.Queue())
    before = bot.config.model_dump(mode='json')
    target = type(config).model_validate({**before, 'allocation_mode': after_mode})
    with pytest.raises(ValueError, match='restart required.*allocation_mode'):
        bot.update_config(target)
    assert bot.config is config
    assert bot.config.model_dump(mode='json') == before


def test_wallet_funded_capital_allows_existing_exposure_and_tranches_but_requires_cash():
    cap = LiveCapitalConfig(slice_quote=D('100'), allocation_mode='wallet_funded', position_cap_quote=None,
                            max_active_tranches=None, funded_cap_quote=None)
    assert live_capital_decision(cap, active_count=80, exposure=D('100000'),
                                 available_quote=D('101')) == (D('100'), 'admitted')
    assert live_capital_decision(cap, active_count=80, exposure=D('100000'),
                                 available_quote=D('99')) == (None, 'cash_exhausted')
    assert 'allocation_mode' not in LiveCapitalConfig(position_cap_quote=D('500')).model_dump(mode='json')


@pytest.mark.asyncio
async def test_wallet_funding_admits_above_old_pair_and_shared_caps_with_real_cash(tmp_path):
    from test.hummingbot.strategy_v2.paper.test_paper_funding import funded_config, paper
    from test.modular_spot.test_native_funding import fill
    from unittest.mock import patch

    from hummingbot.connector.markets_recorder import MarketsRecorder
    from hummingbot.core.data_type.common import TradeType
    from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
    from hummingbot.strategy_v2.paper_session import NativePaperSession

    previous_recorder = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    bounded = funded_config('2500')
    payload = bounded.model_dump(mode='json')
    payload['allocation_mode'] = 'wallet_funded'
    payload['funding']['allocation_quote'] = None
    payload['funding']['max_position_quote'] = None
    session = NativePaperSession(paper(), [type(bounded).model_validate(payload)], tmp_path / 'wallet-cap.sqlite')
    try:
        owner = next(iter(session.controllers.values()))
        store = NativeFundingStore(owner, session.recorder)
        old_fill = fill('old-buy', '1', 'BUY', '30', '100', '3')
        old_fill.market = 'okx_paper_trade'
        projection = funding_projection([old_fill],
                                        'okx_paper_trade', 'BTC-USDC')
        assert projection['used'] > D('2500')
        projection.update(used_by_pair={'BTC-USDC': projection['used']},
                          pending_by_pair={'BTC-USDC': D('0')}, legacy_uncertain_quote=D('0'))
        state = {'claims': {}, 'releases': {}, 'fills': {}}
        proposal = PositionExecutorConfig(timestamp=1, connector_name='okx_paper_trade',
                                          trading_pair='BTC-USDC', side=TradeType.BUY,
                                          amount=D('1'), entry_price=D('90'))
        with (patch.object(store, 'reconcile', return_value={}),
              patch.object(store, '_load', return_value=('raw', state)),
              patch.object(store, '_evidence', return_value=(projection, D('0'), {})),
              patch.object(store, '_write', return_value=True)):
            assert store.admit(proposal)
            assert D(store.status['available_quote']) >= D('90')
            with patch.object(session._connector, 'get_available_balance', return_value=D('90')):
                with pytest.raises(ValueError, match='allocation exhausted'):
                    store.admit(proposal.model_copy(update={'id': 'next-wallet-entry'}))
    finally:
        await session.close()
        MarketsRecorder._shared_instance = previous_recorder


@pytest.mark.asyncio
async def test_wallet_transition_keeps_filled_active_buy_and_refuses_partial_or_unfunded_entry(tmp_path):
    from test.hummingbot.strategy_v2.paper import test_paper_funding as fixtures
    from test.hummingbot.strategy_v2.paper.test_paper_funding import funded_config, paper
    from unittest.mock import patch

    from hummingbot.connector.markets_recorder import MarketsRecorder
    from hummingbot.core.data_type.common import TradeType
    from hummingbot.model.metadata import Metadata
    from hummingbot.strategy_v2.paper_session import NativePaperSession

    previous_recorder = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    with patch('socket.socket.connect', side_effect=AssertionError('network forbidden')):
        session = NativePaperSession(paper(), [funded_config()], tmp_path / 'wallet-transition.sqlite')
        try:
            await fixtures.NativePaperFundingTests.seed(None, session)
            owner = next(iter(session.controllers.values()))
            active = session.executor_orchestrator.active_executors[owner.config.id][0]
            prior_config = owner.config.model_dump(mode='json')
            old_store = owner._funding_store
            old_binding = old_store.binding
            target = owner.config.model_dump(mode='json')
            target['allocation_mode'] = 'wallet_funded'
            target['funding']['allocation_quote'] = None
            target['funding']['max_position_quote'] = None
            owner.config = type(owner.config).model_validate(target)
            from controllers.generic.modular_spot_core.recipe_contract import configuration_digest
            owner._composition_digest = configuration_digest(owner.config.model_dump(mode='json'))
            wallet = NativeFundingStore(owner, session.recorder)
            prior = {owner.config.id: prior_config}
            with pytest.raises(ValueError, match='active native orders'):
                wallet.plan_binding_transition(old_binding, allocation_only_previous_configs=prior)
            # A stopped mode transition cannot run controller ticks under the
            # target configuration before native fills finish under the old one.
            owner.config = type(owner.config).model_validate(prior_config)
            owner._composition_digest = configuration_digest(owner.config.model_dump(mode='json'))
            amount = active.config.amount
            await session.step(600061, trades=[dict(trading_pair='BTC-USDC', sequence=1, timestamp=600061,
                                                    side=TradeType.SELL, price=active.config.entry_price - D('.01'),
                                                    amount=amount)])
            await session.step(600062, books=[dict(trading_pair='BTC-USDC', sequence=4, timestamp=600062,
                                                   bids=[(90, 10)], asks=[(90.1, 10)])])
            owner.config = type(owner.config).model_validate(target)
            owner._composition_digest = configuration_digest(owner.config.model_dump(mode='json'))
            wallet = NativeFundingStore(owner, session.recorder)
            plan = wallet.plan_binding_transition(old_binding, allocation_only_previous_configs=prior)
            assert plan['allocation_only']['previous_config_sha256'][owner.config.id]
            held = plan['allocation_only']['held_recovery'][owner.config.id]
            assert held['previous_configuration_sha256'] == plan['allocation_only']['previous_config_sha256'][owner.config.id]
            assert held['target_configuration_sha256'] == owner._composition_digest
            if held['before_raw'] is not None:
                from controllers.generic.modular_spot_core.native_recovery import decode, encode
                original = held['before_raw']
                with session.recorder.sql_manager.get_new_session() as sql, sql.begin():
                    sql.get(Metadata, held['key']).value = encode(decode(original).model_copy(
                        update={'source_sha256': 'd' * 64}))
                with pytest.raises(ValueError, match='held recovery prior'):
                    wallet.apply_binding_transition(plan['transition_id'])
                with session.recorder.sql_manager.get_new_session() as sql, sql.begin():
                    assert old_binding in sql.get(Metadata, wallet.key).value
                    sql.get(Metadata, held['key']).value = original
            assert D(plan['pending_quote']) == 0
            before = session.recorder.get_trades_for_config(session.recorder.config_file_path)
            wallet.apply_binding_transition(plan['transition_id'])
            with session.recorder.sql_manager.get_new_session() as sql:
                assert wallet.binding in sql.get(Metadata, wallet._source_key).value
            assert len(session.recorder.get_trades_for_config(session.recorder.config_file_path)) == len(before)
            assert wallet.assess()['inventory_cost'] != '0'
            assert wallet.entry_notional_ceiling() > D('100')
            # One native order must still fit free wallet cash plus the explicit fee bound.
            next_config = owner.get_executor_config(TradeType.BUY, D('90'), D('.3'))
            with patch.object(session._connector, 'get_available_balance', return_value=D('0')):
                assert wallet.entry_notional_ceiling() == 0
                with pytest.raises(ValueError, match='allocation exhausted'):
                    wallet.admit(next_config)
            wallet_config = owner.config.model_dump(mode='json')
            owner.config = type(owner.config).model_validate(prior_config)
            owner._composition_digest = configuration_digest(owner.config.model_dump(mode='json'))
            reverse = NativeFundingStore(owner, session.recorder)
            reverse_plan = reverse.plan_binding_transition(
                wallet.binding, allocation_only_previous_configs={owner.config.id: wallet_config})
            assert reverse_plan['to_binding'] == old_binding
            reverse.apply_binding_transition(reverse_plan['transition_id'])
            assert D(reverse.assess()['available_quote']) < D('50')
            assert len(session.recorder.get_trades_for_config(session.recorder.config_file_path)) == len(before)
        finally:
            await session.close()
            MarketsRecorder._shared_instance = previous_recorder


@pytest.mark.asyncio
async def test_wallet_transition_refuses_foreign_held_ledger_scope(tmp_path):
    from test.hummingbot.strategy_v2.paper.test_paper_funding import funded_config, paper

    from controllers.generic.modular_spot_core.recipe_contract import configuration_digest
    from hummingbot.connector.markets_recorder import MarketsRecorder
    from hummingbot.model.position_hold_ledger import PositionHoldLedger
    from hummingbot.strategy_v2.paper_session import NativePaperSession

    previous_recorder = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'foreign-held.sqlite')
    try:
        owner = next(iter(session.controllers.values()))
        old = NativeFundingStore(owner, session.recorder)
        raw, state = old._load()
        assert old._write(raw, state)
        old_binding = old.binding
        previous_config = owner.config.model_dump(mode='json')
        target = owner.config.model_dump(mode='json')
        target['allocation_mode'] = 'wallet_funded'
        target['funding']['allocation_quote'] = None
        target['funding']['max_position_quote'] = None
        owner.config = type(owner.config).model_validate(target)
        owner._composition_digest = configuration_digest(owner.config.model_dump(mode='json'))
        wallet = NativeFundingStore(owner, session.recorder)
        with session.recorder.sql_manager.get_new_session() as sql, sql.begin():
            sql.add(PositionHoldLedger(id='foreign-held-ledger', controller_id='unbound-controller',
                                       executor_id='unbound-executor', connector_name='okx_paper_trade',
                                       trading_pair='BTC-USDC', position_side=None, order_side='BUY',
                                       timestamp=1, client_order_id='foreign-order', exchange_order_id=None,
                                       price=D('1'), amount=D('1'), executed_amount_base=D('1'),
                                       executed_amount_quote=D('1'), cumulative_fee_paid_quote=D('0'),
                                       order_payload={}))
        with pytest.raises(ValueError, match='foreign native owner or scope'):
            wallet.plan_binding_transition(old_binding,
                                           allocation_only_previous_configs={owner.config.id: previous_config})
    finally:
        with session.recorder.sql_manager.get_new_session() as sql, sql.begin():
            foreign = sql.get(PositionHoldLedger, 'foreign-held-ledger')
            if foreign is not None:
                sql.delete(foreign)
        await session.close()
        MarketsRecorder._shared_instance = previous_recorder


@pytest.mark.asyncio
async def test_wallet_transition_retains_unreleased_and_legacy_reservations(tmp_path):
    import json
    from test.hummingbot.strategy_v2.paper.test_paper_funding import funded_config, paper
    from unittest.mock import patch

    from controllers.generic.modular_spot_core.domain import identity
    from controllers.generic.modular_spot_core.native_funding import LEGACY_NEVER_STARTED, _config_identity
    from controllers.generic.modular_spot_core.recipe_contract import configuration_digest
    from hummingbot.connector.markets_recorder import MarketsRecorder
    from hummingbot.core.data_type.common import TradeType
    from hummingbot.model.executors import Executors
    from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
    from hummingbot.strategy_v2.models.base import RunnableStatus
    from hummingbot.strategy_v2.models.executors import CloseType
    from hummingbot.strategy_v2.models.executors_info import ExecutorInfo
    from hummingbot.strategy_v2.paper_session import NativePaperSession

    previous_recorder = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    with patch('socket.socket.connect', side_effect=AssertionError('network forbidden')):
        session = NativePaperSession(paper(), [funded_config('2500')], tmp_path / 'wallet-pending.sqlite')
        try:
            owner = next(iter(session.controllers.values()))
            old = NativeFundingStore(owner, session.recorder)
            previous_config = owner.config.model_dump(mode='json')
            # A stopped no-fill acquisition with no order or terminal receipt must
            # retain its entire funding reservation, as in the copied V2 recorder.
            config = PositionExecutorConfig(id='unreleased-no-order', timestamp=1,
                                            connector_name='okx_paper_trade', trading_pair='BTC-USDC',
                                            side=TradeType.BUY, amount=D('.4'), entry_price=D('90'))
            info = ExecutorInfo(id=config.id, timestamp=1, type='position_executor',
                                status=RunnableStatus.TERMINATED, config=config, close_type=CloseType.EXPIRED,
                                net_pnl_pct=D('0'), net_pnl_quote=D('0'), cum_fees_quote=D('0'),
                                filled_amount_quote=D('0'), is_active=False, is_trading=False,
                                controller_id=owner.config.id, custom_info={
                                    'order_ids': [], 'execution_order_snapshots': [],
                                    'entry_reconciliation': None,
                                    'execution_receipt': {'entry_gross_base': '0'},
                                    'entry_lifecycle': {'policy': 'single_attempt', 'state': 'ended_no_fill',
                                                        'stop_reason': 'upward_bound_breached'}})
            with session.recorder.sql_manager.get_new_session() as sql, sql.begin():
                sql.add(Executors(**json.loads(info.model_dump_json())))
            raw, state = old._load()
            no_order_claim = {'config': _config_identity(config, owner.config.id), 'amount': '.4',
                              'price': '90', 'fee_fraction': '.003'}
            legacy_claim = {'config': identity(['historical', 'absent']), 'amount': '1',
                            'price': '3', 'fee_fraction': '.003'}
            state['claims'][config.id] = no_order_claim
            state['claims']['historical-absent'] = legacy_claim
            state['releases']['historical-absent'] = dict(LEGACY_NEVER_STARTED)
            assert old._write(raw, state)
            expected_pending = D('.4') * D('90') * D('1.003') + D('3') * D('1.003')
            assert D(old.assess()['pending_quote']) == expected_pending

            target = owner.config.model_dump(mode='json')
            target['allocation_mode'] = 'wallet_funded'
            target['funding']['allocation_quote'] = None
            target['funding']['max_position_quote'] = None
            owner.config = type(owner.config).model_validate(target)
            owner._composition_digest = configuration_digest(owner.config.model_dump(mode='json'))
            wallet = NativeFundingStore(owner, session.recorder)
            plan = wallet.plan_binding_transition(old.binding,
                                                  allocation_only_previous_configs={owner.config.id: previous_config})
            assert D(plan['pending_quote']) == expected_pending
            assert D(plan['legacy_uncertain_quote']) == D('3.009')
            wallet.apply_binding_transition(plan['transition_id'])
            _, reloaded = wallet._load()
            assert reloaded['claims'][config.id] == no_order_claim
            assert reloaded['claims']['historical-absent'] == legacy_claim
            assert config.id not in reloaded['releases']
            assert reloaded['releases']['historical-absent'] == LEGACY_NEVER_STARTED
            assert D(wallet.reconcile()['pending_quote']) == expected_pending
            with patch.object(session._connector, 'get_available_balance', return_value=D('50')):
                assert D(wallet.assess()['available_quote']) == D('50') - expected_pending
                assert wallet.entry_notional_ceiling() == (D('50') - expected_pending) / D('1.003')
                owner._latest_atr_pct = .01
                candidate = owner.get_executor_config(TradeType.BUY, D('90'), D('.2'))
                with pytest.raises(ValueError, match='allocation exhausted'):
                    wallet.admit(candidate)

            bounded = owner.config.model_dump(mode='json')
            bounded['allocation_mode'] = 'bounded'
            bounded['funding']['allocation_quote'] = '30'
            bounded['funding']['max_position_quote'] = '30'
            owner.config = type(owner.config).model_validate(bounded)
            owner._composition_digest = configuration_digest(owner.config.model_dump(mode='json'))
            reverse = NativeFundingStore(owner, session.recorder)
            with pytest.raises(ValueError, match='target cannot contain existing native exposure'):
                reverse.plan_binding_transition(wallet.binding,
                                                allocation_only_previous_configs={owner.config.id: target})
        finally:
            await session.close()
            MarketsRecorder._shared_instance = previous_recorder
