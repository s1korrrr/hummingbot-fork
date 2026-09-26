"""Native modular_spot bag lifecycle: per-tick decisions, persistence and configuration."""
from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from controllers.generic.modular_spot_core.bag import (
    ActiveTranche,
    AdaptiveTrailingConfig,
    BagConfig,
    BagFeatures,
    BagTrailStore,
    HeldBag,
    LiveSell,
    decide,
    is_recovery_sell,
    recovery_sell_config,
)
from controllers.generic.modular_spot_core.domain import PolicyError
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.components.held_inventory import RecoveryTrail

CFG = BagConfig()
HELD = HeldBag(D('1'), D('100'), D('0.3'))


def feats(rsi=80.0, rsi_prev=80.2, ema=100.0, macd=0.1, macd_prev=0.0, trend='range', conf='0'):
    return SimpleNamespace(rsi=D(str(rsi))), BagFeatures(D(str(rsi_prev)), D(str(ema)), D(str(macd)),
                                                         D(str(macd_prev)), D('0.002'), trend, D(conf))


def tick(**changes):
    market, bag = feats(**changes.pop('features', {}))
    values = dict(trail=None, held=HELD, tranches=(), live_sells=(), last_sell_price=None, mid=D('106'),
                  features=market, bag=bag, first_tick_after_close=False, tick=D('0.01'), amount_step=D('0.001'),
                  minimum_amount=D('0.001'), minimum_notional=D('1'))
    values['best_ask'] = changes.pop('best_ask', changes.get('mid', values['mid']))
    return decide(changes.pop('cfg', CFG), **{**values, **changes})


def test_recovery_arms_peaks_and_sells_a_live_style_slice_with_floor():
    first = tick()
    assert first.reason == 'armed' and first.sell is None
    peak = tick(trail=first.trail, mid=D('108'))
    assert peak.reason == 'new_peak'
    sold = tick(trail=peak.trail, mid=D('107.1'))
    assert sold.sell is not None and sold.sell.reason == 'pullback-trail'
    assert sold.sell.proposal_price == D('107.11')  # one tick above the touch (maker)
    assert sold.sell.amount == D('1.000')  # $200 slice exceeds the bag: whole bag
    assert sold.sell.min_sell_price == D('100') * D('1.02') + D('0.3')
    assert sold.trail.peak_price == D('107.1')  # re-anchored after the proposal


def test_trail_is_frozen_while_a_recovery_sell_is_live_and_dropped_on_breakeven_change():
    armed = tick().trail
    frozen = tick(trail=armed, mid=D('115'), live_sells=(LiveSell('s', D('200')),))
    assert frozen.trail == armed and frozen.sell is None and frozen.reason == 'recovery_sell_live'
    merged = tick(trail=armed, held=HeldBag(D('2'), D('95'), D('0.5')), features={'rsi': 70.0})
    assert merged.trail is None  # a new bag re-averaged the breakeven: trail restarts


def test_sell_signal_only_on_first_tick_after_close():
    armed = tick().trail
    reversal = {'rsi': 80.0, 'rsi_prev': 80.2, 'ema': 106.5, 'macd': -0.1, 'macd_prev': 0.2}
    signal = dict(trail=armed, mid=D('105.9'), features=reversal)
    assert tick(**signal).sell is None
    assert tick(first_tick_after_close=True, **signal).sell.reason == 'sell-signal'
    held_up = {**signal, 'features': {**signal['features'], 'rsi_prev': 80.1, 'ema': 100.0, 'trend': 'up',
                                      'conf': '0.5'}}
    assert tick(first_tick_after_close=True, **held_up).sell is None  # MACD-only reversal is trend-held


def test_stale_cancel_early_stop_gap_and_dust():
    hold_up = {'trend': 'up', 'conf': '0.9', 'rsi_prev': 80.0, 'ema': 90.0}
    stale = tick(live_sells=(LiveSell('s', D('105')),), mid=D('105.6'), features=hold_up)
    assert stale.cancels == ('s',)
    tranches = (ActiveTranche('down', D('110')), ActiveTranche('flat', D('106')))
    assert tick(tranches=tranches, mid=D('106.7')).stops == ('down',)  # 106.7/110 - 1 = -3.0%
    armed = tick().trail
    peak = tick(trail=armed, mid=D('108')).trail
    gapped = tick(trail=peak, mid=D('107.1'), last_sell_price=D('107.5'))
    assert gapped.sell is None and gapped.reason == 'price-gap'
    dust = tick(held=HeldBag(D('0.005'), D('100'), D('0')), mid=D('106'))
    assert dust.reason == 'no_bag' and dust.trail is None
    # A whole-bag sweep leaves an off-grid residual below one lot: that residual is dust, not a bag.
    residual = tick(held=HeldBag(D('0.0004'), D('100'), D('0')), mid=D('106'), amount_step=D('0.001'))
    assert residual.reason == 'no_bag' and residual.sell is None
    # The trail of a sold-out bag does not survive on its dust, even at the same breakeven.
    armed = tick().trail
    assert tick(trail=armed, held=HeldBag(D('0.0004'), D('100'), D('0.3'))).trail is None


def test_recovery_proposal_is_one_tick_above_the_best_ask_like_live():
    armed = tick().trail
    peak = tick(trail=armed, mid=D('108')).trail
    sold = tick(trail=peak, mid=D('107.095'), best_ask=D('107.105'))
    assert sold.sell.proposal_price == D('107.11')  # live _shade_limit_maker_price: ceil(max(mid, ask))
    assert tick(trail=peak, mid=D('107.095')).sell.proposal_price == D('107.10')  # mid as ask is half a spread low


def test_lazy_trend_precheck_keeps_signals_for_a_low_arm_threshold():
    cfg = BagConfig(recovery_arm_rsi=55)
    armed = tick(cfg=cfg, features={'rsi': 58.0}).trail
    reversal = {'rsi': 58.0, 'rsi_prev': 58.2, 'ema': 106.5, 'macd': -0.1, 'macd_prev': 0.2}
    fired = tick(cfg=cfg, trail=armed, mid=D('105.9'), first_tick_after_close=True, features=reversal)
    assert fired.sell is not None and fired.sell.reason == 'sell-signal'


def test_trail_store_roundtrip_and_rejects_corruption():
    trail = RecoveryTrail(D('100'), D('106'), D('108'), 81.5, D('1'))
    raw = BagTrailStore.encode(trail, 1_800_000_000)
    assert BagTrailStore.decode(raw) == (trail, 1_800_000_000)
    assert BagTrailStore.decode(BagTrailStore.encode(None, None)) == (None, None)
    with pytest.raises(PolicyError):
        BagTrailStore.decode('{"trail": null}')


def test_held_bag_from_positions_is_long_blended_and_owned():
    position = SimpleNamespace(connector_name='okx', trading_pair='BNB-USDC', side=TradeType.BUY, amount=D('2'),
                               breakeven_price=D('95'), remaining_fees_quote=D('0.4'), cum_fees_quote=D('0.6'))
    assert HeldBag.from_positions([position], connector='okx', pair='BNB-USDC') == HeldBag(D('2'), D('95'), D('0.4'))
    assert HeldBag.from_positions([], connector='okx', pair='BNB-USDC').amount == 0
    with pytest.raises(PolicyError):
        HeldBag.from_positions([position, position], connector='okx', pair='BNB-USDC')
    with pytest.raises(PolicyError):
        HeldBag.from_positions([SimpleNamespace(**{**position.__dict__, 'side': TradeType.SELL})],
                               connector='okx', pair='BNB-USDC')


def test_recovery_sell_config_is_a_live_chaser_close():
    from controllers.generic.modular_spot_core.bag import SellRequest

    controller = SimpleNamespace(id='modular_spot_bnb', connector_name='okx', trading_pair='BNB-USDC', bag=CFG)
    config = recovery_sell_config(controller, SellRequest(D('1'), D('107.11'), D('102.3'), 'pullback-trail'),
                                  identifier='sell1', timestamp=1.0)
    assert is_recovery_sell(config) and config.side == TradeType.SELL
    assert config.execution_strategy.name == 'LIMIT_CHASER' and config.min_sell_price == D('102.3')
    assert config.chaser_config.distance == D('0.002') and config.chaser_config.refresh_threshold == D('0.003')


def test_adaptive_trailing_config_matches_the_shared_live_formula():
    from controllers.shared.trailing import calculate_continuous_adaptive_trailing

    cfg = AdaptiveTrailingConfig()
    expected = calculate_continuous_adaptive_trailing(
        atr_pct=0.004, baseline_atr_pct=0.002, base_activation=D('0.015'), base_delta=D('0.004'),
        activation_min=D('0.003'), activation_max=D('0.05'), delta_min=D('0.001'), delta_max=D('0.02'),
        min_locked_profit=D('0.004'), activation_exponent=0.70, delta_exponent=0.35, min_multiplier=0.5,
        max_multiplier=2.5)
    assert cfg.select(D('0.004'), D('0.015'), D('0.004')) == (expected.activation, expected.delta)
    with pytest.raises(ValueError):
        AdaptiveTrailingConfig(activation_min=D('0.06'))


# ---------------------------------------------------------------- runtime integration
def _runtime(recorder, *, positions, executors=(), market_close='106'):
    from controllers.generic.modular_spot import ModularSpotControllerConfig
    from controllers.generic.modular_spot_core.market import MarketFeatures
    from controllers.generic.modular_spot_core.runtime import ModularRuntime
    from controllers.generic.modular_spot_core.state import DecisionStore
    from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction

    cfg = ModularSpotControllerConfig(id='modular_spot_bag', connector_name='okx', trading_pair='BNB-USDC',
                                      execution_enabled=True, max_records=1276, bag=BagConfig())
    rules = SimpleNamespace(min_price_increment=D('0.01'))
    provider = SimpleNamespace(time=lambda: 1_800_000_030.0,
                               connectors={'okx': SimpleNamespace(trading_rules={'BNB-USDC': rules})})
    controller = SimpleNamespace(config=cfg, market_data_provider=provider, positions_held=list(positions),
                                 executors_info=list(executors), logger=lambda: SimpleNamespace(error=print))
    bindings = SimpleNamespace(metadata_model=None, action=lambda cid, config: CreateExecutorAction(
        controller_id=cid, executor_config=config))
    runtime = object.__new__(ModularRuntime)
    runtime.controller, runtime.bindings, runtime.fingerprint = controller, bindings, cfg.fingerprint()
    runtime.store = DecisionStore(recorder, cfg.id, 'okx', 'BNB-USDC')
    runtime.pending, runtime.dispatched, runtime.fault = {}, set(), None
    runtime.budget, runtime.bag_status = None, None
    _, bag = feats()
    runtime.market = MarketFeatures(1_800_000_000, D(market_close), D('80.2'), D('80'), False, False, bag=bag)
    return runtime


def _position(amount='1', breakeven='100', fees='0.3'):
    return SimpleNamespace(connector_name='okx', trading_pair='BNB-USDC', side=TradeType.BUY, amount=D(amount),
                           breakeven_price=D(breakeven), remaining_fees_quote=D(fees), cum_fees_quote=D(fees))


def _snapshot(mid):
    half = D('0.005')
    return dict(bid=mid - half, ask=mid + half, amount_step=D('0.001'), minimum_amount=D('0.001'),
                minimum_notional=D('1'))


@pytest.fixture
def bag_recorder(tmp_path):
    from test.modular_spot.test_native_lifecycle import disposable_recorder, release_recorder

    value = disposable_recorder(tmp_path)
    try:
        yield value
    finally:
        release_recorder(value)


def test_runtime_bag_tick_holds_losers_places_one_recovery_sell_and_persists_the_trail(bag_recorder):
    from controllers.generic.modular_spot_core.inventory import InventoryView
    from controllers.generic.modular_spot_core.state import DecisionState
    from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
    from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction

    held = HeldBag(D('1'), D('100'), D('0.3'))
    state = DecisionState(fingerprint='f' * 64, candle=1_799_999_940, claims=('loser',), committed_quote=D('110'))
    inventory = {'loser': InventoryView(D('1'), D('110'), D('110'), D('0'), False, True, False, D('110'))}
    runtime = _runtime(bag_recorder, positions=[_position()])
    armed = runtime._bag_actions(inventory, state, held, _snapshot(D('106')))
    # 106 vs the 110 fill is -3.6%: the live tranche is held (keep_position), and the bag arms.
    assert [type(a) for a in armed] == [StopExecutorAction] and armed[0].keep_position is True
    assert runtime.bag_status['reason'] == 'armed'
    runtime.market = runtime.market.__class__(1_800_000_060, D('108'), D('80.2'), D('80'), False, False,
                                              bag=runtime.market.bag)
    runtime._bag_actions({}, state.model_copy(update={'claims': ()}), held, _snapshot(D('108')))
    sell = runtime._bag_actions({}, state.model_copy(update={'claims': ()}), held, _snapshot(D('107.1')))
    create = [a for a in sell if isinstance(a, CreateExecutorAction)]
    assert len(create) == 1 and runtime.bag_status['reason'] == 'sell:pullback-trail'
    config = create[0].executor_config
    assert runtime.recovery_permission(config) and runtime.executor_class(config) is OrderExecutor
    frozen = runtime._bag_actions({}, state.model_copy(update={'claims': ()}), held, _snapshot(D('115')))
    assert frozen == [] and runtime.bag_status['reason'] == 'recovery_sell_live'
    # A fresh runtime (restart) restores the persisted trail and evaluated candle.
    restarted = _runtime(bag_recorder, positions=[_position()])
    trail, candle, _ = restarted._bag_store().load()
    assert candle == 1_800_000_060 and trail is not None and trail.peak_price == D('107.1')


def test_runtime_stale_cancel_and_single_signal_per_candle(bag_recorder):
    from controllers.generic.modular_spot_core.state import DecisionState
    from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction

    held = HeldBag(D('1'), D('100'), D('0.3'))
    state = DecisionState(fingerprint='f' * 64, candle=1_799_999_940)
    live = SimpleNamespace(id='sell-1', is_active=True, filled_amount_quote=D('0'), timestamp=1.0,
                           close_timestamp=None, controller_id='modular_spot_bag',
                           config=recovery_sell_config(SimpleNamespace(
                               id='modular_spot_bag', connector_name='okx', trading_pair='BNB-USDC', bag=CFG),
                               __import__('controllers.generic.modular_spot_core.bag', fromlist=['SellRequest'])
                               .SellRequest(D('1'), D('105'), D('102.3'), 'pullback-trail'),
                               identifier='sell-1', timestamp=1.0))
    runtime = _runtime(bag_recorder, positions=[_position()], executors=[live])
    runtime.market = runtime.market.__class__(1_800_000_000, D('105.6'), D('80'), D('80'), False, False,
                                              bag=BagFeatures(D('80'), D('90'), D('0.1'), D('0'), D('0.002'), 'up',
                                                              D('0.9')))
    runtime._recovery_executors = lambda: {'sell-1': live}
    actions = runtime._bag_actions({}, state, held, _snapshot(D('105.6')))
    assert [a.executor_id for a in actions if isinstance(a, StopExecutorAction)] == ['sell-1']
    _, candle, _ = runtime._bag_store().load()
    assert candle == 1_800_000_000  # the signal candle is consumed once


def _frame(rows=1400):
    import numpy as np
    import pandas as pd

    start = 1_800_000_000 - 1_800_000_000 % 900
    close = 100 + np.sin(np.arange(rows) / 25) * 3 + np.arange(rows) * 0.001
    return pd.DataFrame({'timestamp': np.arange(start, start + rows * 60, 60), 'open': close, 'high': close + .2,
                         'low': close - .2, 'close': close, 'volume': np.ones(rows)})


def test_market_features_carry_live_bag_inputs_only_when_enabled():
    from controllers.generic.modular_spot_core.config import SignalPolicy
    from controllers.generic.modular_spot_core.market import features
    from hummingbot.strategy_v2.utils.market_analysis import calculate_trend_state

    frame = _frame()
    now = float(frame.timestamp.iloc[-1] + 60)
    kwargs = dict(now=now, interval=60, max_age=120, policy=SignalPolicy(rsi_length=17, oversold=D('32')),
                  trend_analyzer=calculate_trend_state)
    plain = features(frame, **kwargs)
    bagged = features(frame, bag_config=CFG, **kwargs)
    assert plain.bag is None and bagged.bag is not None
    assert (plain.candle_open, plain.rsi, plain.previous_rsi) == (bagged.candle_open, bagged.rsi, bagged.previous_rsi)
    assert bagged.bag.rsi_prev == bagged.previous_rsi and bagged.bag.atr_pct > 0
    assert bagged.bag.trend_direction in {'up', 'down', 'range'}
    with pytest.raises(PolicyError):
        features(frame.iloc[-600:], bag_config=CFG, **kwargs)  # live trend windows need 1274 closed rows


def test_controller_config_requires_live_history_and_the_fixed_rsi_profile():
    from controllers.generic.modular_spot import ModularSpotControllerConfig

    base = dict(id='modular_spot_bag', connector_name='okx', trading_pair='BNB-USDC')
    with pytest.raises(ValueError, match='max_records'):
        ModularSpotControllerConfig(**base, bag=BagConfig())
    assert ModularSpotControllerConfig(**base, max_records=1276, bag=BagConfig()).bag == BagConfig()
    assert ModularSpotControllerConfig(**base, max_records=1276, adaptive_trailing=AdaptiveTrailingConfig(),
                                       exit_floor='live_no_loss').exit_floor == 'live_no_loss'
    with pytest.raises(ValueError):
        ModularSpotControllerConfig(**base, exit_floor='anything')
    turn = ModularSpotControllerConfig(**base, max_records=1276, bag=BagConfig(), signal={'kind': 'rsi_turn'})
    assert turn.signal.kind == 'rsi_turn'  # the OK RSI reversal entry composes with live bags
    with pytest.raises(ValueError, match='RSI entry profile'):
        ModularSpotControllerConfig(**base, max_records=1276, bag=BagConfig(), signal={'kind': 'breakout'},
                                    acquisition={'fractions': ['1']})


def test_native_entry_uses_live_adaptive_geometry_from_the_decision_candle_atr():
    from controllers.generic.modular_spot import ModularSpotControllerConfig
    from controllers.generic.modular_spot_core.execution import native_config

    cfg = ModularSpotControllerConfig(id='modular_spot_bag', connector_name='okx', trading_pair='BNB-USDC',
                                      max_records=1276, adaptive_trailing=AdaptiveTrailingConfig())
    _, bag = feats()
    market = SimpleNamespace(bag=bag)
    executor = native_config(cfg, identifier='x', timestamp=1, price=D('100'), amount=D('1'), cycle=0, stage=0,
                             market=market)
    expected = cfg.adaptive_trailing.select(bag.atr_pct, cfg.trailing.activation, cfg.trailing.delta)
    trailing = executor.triple_barrier_config.trailing_stop
    assert (trailing.activation_price, trailing.trailing_delta) == expected
    with pytest.raises(PolicyError):
        native_config(cfg, identifier='x', timestamp=1, price=D('100'), amount=D('1'), cycle=0, stage=0)


def test_trend_is_only_evaluated_when_a_decision_depends_on_it():
    calls = []

    def trend():
        calls.append(1)
        return 'range', D('0')

    tick(trend=trend)  # plain recovery arming: no signal tick, no live sell
    tick(trend=trend, live_sells=(LiveSell('s', D('200')),))  # live sell far above: no stale check needed
    assert calls == []
    reversal = {'rsi': 80.0, 'rsi_prev': 80.2, 'ema': 106.5, 'macd': -0.1, 'macd_prev': 0.2}
    tick(trend=trend, first_tick_after_close=True, trail=tick().trail, mid=D('105.9'), features=reversal)
    assert calls == [1]  # evaluated once per tick, and only for the sell signal


def test_live_no_loss_exit_floor_is_the_plain_after_fee_floor(monkeypatch):
    from controllers.generic.modular_spot_core.execution import ModularPositionExecutor
    from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor

    monkeypatch.setattr(PositionExecutor, '_min_no_loss_sell_price', lambda self: D('100.5'))
    trailing = SimpleNamespace(activation_price=D('0.015'), trailing_delta=D('0.004'))
    executor = object.__new__(ModularPositionExecutor)
    executor.config = SimpleNamespace(controller_id='modular_spot_bag',
                                      triple_barrier_config=SimpleNamespace(trailing_stop=trailing))
    monkeypatch.setattr(ModularPositionExecutor, 'trading_rules', SimpleNamespace(min_price_increment=D('0.01')),
                        raising=False)

    def floor(mode):
        controllers = {} if mode is None else {'modular_spot_bag': SimpleNamespace(
            config=SimpleNamespace(exit_floor=mode))}
        executor._strategy = SimpleNamespace(controllers=controllers)
        return executor._min_no_loss_sell_price()

    assert floor('live_no_loss') == D('100.5')
    protected = floor('protected')
    assert protected is not None and protected > D('100.5')  # locks the trailing minimum profit
    assert floor(None) == protected  # a missing controller falls back to the stricter floor


# ---------------------------------------------------------------- live capital model
def test_live_capital_marks_the_held_bag_and_active_buys_at_mid_against_the_position_cap():
    from controllers.generic.modular_spot_core.bag import LiveCapitalConfig, live_capital_quote

    cap = LiveCapitalConfig(slice_quote=D('100'), position_cap_quote=D('500'), max_active_tranches=8)
    # The bag cost ~498 but is worth 0.696 * 560 = 389.76 at mid: 389.76 + 100 <= 500 -> a buy is admitted.
    assert live_capital_quote(cap, held_amount=D('0.696'), held_dust=False, mid=D('560'), active=(),
                              available_quote=D('1000')) == (D('100'), 'admitted')
    # At 650 the bag is worth 452.40: the next $100 would breach the cap (live gate 'max_position').
    assert live_capital_quote(cap, held_amount=D('0.696'), held_dust=False, mid=D('650'), active=(),
                              available_quote=D('1000')) == (None, 'max_position')
    # Active buys count owned base at mid plus the unfilled remainder at its commitment price.
    active = ((D('0.15'), D('0'), D('0')), (D('0'), D('0.15'), D('600')))
    assert live_capital_quote(cap, held_amount=D('0'), held_dust=True, mid=D('600'), active=active,
                              available_quote=D('1000')) == (D('100'), 'admitted')  # 90 + 90 + 100 <= 500
    # A dust bag is not exposure; the active-executor limit binds before the cap.
    eight = ((D('0'), D('0'), D('0')),) * 8
    assert live_capital_quote(cap, held_amount=D('0.0004'), held_dust=True, mid=D('600'), active=eight,
                              available_quote=D('1000')) == (None, 'max_executors')
    # Unfunded (V1): a buy the wallet cannot pay for is refused (live balance_low), never shrunk.
    assert live_capital_quote(cap, held_amount=D('0'), held_dust=True, mid=D('600'), active=(),
                              available_quote=D('37.5')) == (None, 'cash_exhausted')
    assert live_capital_quote(cap, held_amount=D('0'), held_dust=True, mid=D('600'), active=(),
                              available_quote=D('0')) == (None, 'cash_exhausted')
    with pytest.raises(ValueError):
        LiveCapitalConfig(slice_quote=D('600'), position_cap_quote=D('500'))


def test_bag_mode_planner_uses_live_capital_instead_of_the_fixed_allocation():
    from controllers.generic.modular_spot_core.bag import LiveCapitalConfig
    from controllers.generic.modular_spot_core.config import AcquisitionPolicy, RiskPolicy, SignalPolicy
    from controllers.generic.modular_spot_core.market import MarketFeatures
    from controllers.generic.modular_spot_core.planner import plan
    from controllers.generic.modular_spot_core.state import DecisionState

    cap = LiveCapitalConfig(slice_quote=D('100'), position_cap_quote=D('500'), max_active_tranches=8)
    state = DecisionState(fingerprint='a' * 64, candle=1080)
    base = dict(fingerprint='a' * 64, budget=D('500'), now=1200,
                market=MarketFeatures(1140, D('560'), D('29'), D('31'), False, False),
                bid=D('560'), ask=D('560.1'), available_quote=D('1000'), amount_step=D('.001'),
                minimum_amount=D('.001'), minimum_notional=D('1'), pnl=D('0'), signal=SignalPolicy(),
                acquisition=AcquisitionPolicy(fractions=(D('.2'),) * 5, minimum_drop=D('.008')), risk=RiskPolicy())
    bag = HeldBag(D('0.696'), D('715.5'), D('1.5'))  # ~498 at cost, 389.8 at mid
    fixed = plan(state, {}, **base, bag=bag)
    assert fixed[1] is None and fixed[2] == 'allocation_below_minimum'  # the bag at cost leaves 0.5 of 500
    live = plan(state, {}, **base, bag=bag, capital=cap)
    assert live[2] == 'entry_proposed' and live[1].amount == D('0.177')  # floor(100/560) after the native fee reserve
    capped = plan(state, {}, **{**base, 'bid': D('650'), 'ask': D('650.1')}, bag=bag, capital=cap)
    assert capped[2] == 'max_position'


def test_controller_config_admits_live_capital_only_with_bags():
    from controllers.generic.modular_spot import ModularSpotControllerConfig
    from controllers.generic.modular_spot_core.bag import LiveCapitalConfig

    base = dict(id='modular_spot_bag', connector_name='okx', trading_pair='BNB-USDC', max_records=1276)
    live = ModularSpotControllerConfig(**base, bag=BagConfig(), capital=LiveCapitalConfig(position_cap_quote=None))
    assert live.capital == LiveCapitalConfig(slice_quote=D('100'), position_cap_quote=None, max_active_tranches=8)
    with pytest.raises(ValueError, match='bag lifecycle'):
        ModularSpotControllerConfig(**base, capital=LiveCapitalConfig(position_cap_quote=None))


def test_live_sell_reference_expires_after_twenty_newer_stored_executors():
    from controllers.generic.modular_spot_core.bag import live_sell_reference

    sell = ((10, 0), D('110'))
    assert live_sell_reference([sell]) == D('110')
    newer = [((11 + i, 0), None) for i in range(19)]
    assert live_sell_reference([sell, *newer]) == D('110')  # 20 visible: still the reference
    assert live_sell_reference([sell, *newer, ((40, 0), None)]) is None  # 21st newer executor pushes it out
    assert live_sell_reference([((5, 0), D('100')), sell, ((12, 1), D('120'))]) == D('120')  # newest filled sell


def test_live_v2_funding_resizes_the_buy_before_the_mid_cap_check():
    from controllers.generic.modular_spot_core.bag import LiveCapitalConfig, live_capital_decision, live_capital_quote

    v1 = LiveCapitalConfig(slice_quote=D('100'), position_cap_quote=D('500'), max_active_tranches=8)
    v2 = LiveCapitalConfig(slice_quote=D('100'), position_cap_quote=D('500'), max_active_tranches=8,
                           funded_cap_quote=D('505'))
    held = dict(held_amount=D('0.696'), held_dust=False, mid=D('604'), active=(), available_quote=D('1000'))
    # Bag worth 420.38 at mid: the full 100 slice breaches the cap, so V1-style refuses.
    assert live_capital_quote(v1, **held) == (None, 'max_position')
    # V2 first shrinks the buy to the funding ceiling (505 - 480) / 1.003, which then fits under the cap.
    quote, reason = live_capital_quote(v2, **held, funded_used_quote=D('480'))
    assert reason == 'admitted' and quote == (D('25') / D('1.003'))
    assert live_capital_quote(v2, **held, funded_used_quote=D('505')) == (None, 'funding_exhausted')
    with pytest.raises(PolicyError):
        live_capital_quote(v2, **held)
    # The pure decision recomputes from recorded facts (verifier path).
    assert live_capital_decision(v1, active_count=8, exposure=D('0'), available_quote=D('1000')) == (None, 'max_executors')


def test_bag_mode_planner_funded_v2_resizes_to_the_funding_ceiling():
    from controllers.generic.modular_spot_core.bag import LiveCapitalConfig
    from controllers.generic.modular_spot_core.config import AcquisitionPolicy, RiskPolicy, SignalPolicy
    from controllers.generic.modular_spot_core.market import MarketFeatures
    from controllers.generic.modular_spot_core.planner import plan
    from controllers.generic.modular_spot_core.state import DecisionState

    cap = LiveCapitalConfig(slice_quote=D('100'), position_cap_quote=D('500'), max_active_tranches=8,
                            funded_cap_quote=D('505'), funding_fee_reserve=D('0.003'))
    base = dict(fingerprint='a' * 64, budget=D('500'), now=1200,
                market=MarketFeatures(1140, D('560'), D('29'), D('31'), False, False),
                bid=D('560'), ask=D('560.1'), available_quote=D('1000'), amount_step=D('.001'),
                minimum_amount=D('.001'), minimum_notional=D('1'), pnl=D('0'), signal=SignalPolicy(),
                acquisition=AcquisitionPolicy(fractions=(D('.2'),) * 5, minimum_drop=D('.008')), risk=RiskPolicy())
    state = DecisionState(fingerprint='a' * 64, candle=1080)
    bag = HeldBag(D('0.6'), D('700'), D('1'))  # 421 at cost: ceiling (505 - 421) / 1.003 = 83.75
    revised, proposal, reason = plan(state, {}, **base, bag=bag, capital=cap)
    assert reason == 'entry_proposed' and proposal.amount * D('560') <= D('84')
    spent = HeldBag(D('0.72'), D('700'), D('1'))  # 505 at cost: the funding ceiling is exhausted
    assert plan(state, {}, **base, bag=spent, capital=cap)[2] == 'funding_exhausted'


def test_uncapped_capital_and_the_live_atr_buy_gap():
    from controllers.generic.modular_spot_core.bag import (
        BuyGapConfig,
        LiveCapitalConfig,
        live_buy_gap,
        live_capital_decision,
    )

    uncapped = LiveCapitalConfig(slice_quote=D('100'), position_cap_quote=None, max_active_tranches=8)
    assert live_capital_decision(uncapped, active_count=0, exposure=D('100000'), available_quote=D('5000')) == (
        D('100'), 'admitted')
    gap = BuyGapConfig()  # live BNB: 0.8% base, x3 ATR, 0.5-1.8%
    assert live_buy_gap(gap, D('0.002')) == D('0.006')
    assert live_buy_gap(gap, D('0.0001')) == D('0.005') and live_buy_gap(gap, D('0.01')) == D('0.018')
    assert live_buy_gap(gap, None) == D('0.008')
    with pytest.raises(ValueError):
        BuyGapConfig(min_gap=D('0.02'), max_gap=D('0.01'))


def test_live_buy_gap_is_absolute_from_the_last_active_buy_and_the_bag():
    from controllers.generic.modular_spot_core.bag import BuyGapConfig, LiveCapitalConfig
    from controllers.generic.modular_spot_core.config import AcquisitionPolicy, RiskPolicy, SignalPolicy
    from controllers.generic.modular_spot_core.inventory import InventoryView
    from controllers.generic.modular_spot_core.market import MarketFeatures
    from controllers.generic.modular_spot_core.planner import plan
    from controllers.generic.modular_spot_core.state import DecisionState

    cap = LiveCapitalConfig(slice_quote=D('100'), position_cap_quote=None, max_active_tranches=8)
    _, feats_ = feats()  # atr_pct 0.002 -> gap 0.6%
    market = MarketFeatures(1140, D('100'), D('29'), D('31'), False, False, bag=feats_)
    state = DecisionState(fingerprint='a' * 64, candle=1080, claims=('a1',), committed_quote=D('100'))
    view = {'a1': InventoryView(D('1'), D('100'), D('100'), D('0'), False, True, False, D('100'))}
    base = dict(fingerprint='a' * 64, budget=D('5000'), now=1200, market=market, available_quote=D('5000'),
                amount_step=D('.001'), minimum_amount=D('.001'), minimum_notional=D('1'), pnl=D('0'),
                signal=SignalPolicy(), acquisition=AcquisitionPolicy(), risk=RiskPolicy())
    empty = HeldBag(D('0'), D('0'), D('0'))
    near = plan(state, view, **base, bid=D('100.4'), ask=D('100.5'), bag=empty, capital=cap, buy_gap=BuyGapConfig())
    assert near[2] == 'acquisition_not_eligible'  # 0.4% from the last buy < 0.6%
    above = plan(state, view, **base, bid=D('100.7'), ask=D('100.8'), bag=empty, capital=cap, buy_gap=BuyGapConfig())
    assert above[2] == 'entry_proposed'  # live gap is absolute: 0.7% above the last buy qualifies
    with pytest.raises(PolicyError):
        plan(state, view, **base, bid=D('100.7'), ask=D('100.8'), bag=empty, buy_gap=BuyGapConfig())


def test_live_capital_cap_must_be_explicit_and_the_buy_gap_also_spaces_from_the_bag():
    from controllers.generic.modular_spot_core.bag import BuyGapConfig, LiveCapitalConfig
    from controllers.generic.modular_spot_core.config import AcquisitionPolicy, RiskPolicy, SignalPolicy
    from controllers.generic.modular_spot_core.market import MarketFeatures
    from controllers.generic.modular_spot_core.planner import plan
    from controllers.generic.modular_spot_core.state import DecisionState

    with pytest.raises(ValueError):
        LiveCapitalConfig()  # omitting the cap is not 'no cap'
    cap = LiveCapitalConfig(position_cap_quote=None)
    _, feats_ = feats()  # atr 0.002 -> gap 0.6%
    market = MarketFeatures(1140, D('100'), D('29'), D('31'), False, False, bag=feats_)
    base = dict(fingerprint='a' * 64, budget=D('5000'), now=1200, market=market, available_quote=D('5000'),
                amount_step=D('.001'), minimum_amount=D('.001'), minimum_notional=D('1'), pnl=D('0'),
                signal=SignalPolicy(), acquisition=AcquisitionPolicy(), risk=RiskPolicy())
    state = DecisionState(fingerprint='a' * 64, candle=1080)
    bag = HeldBag(D('1'), D('100'), D('0.3'))
    assert plan(state, {}, **base, bid=D('100.5'), ask=D('100.6'), bag=bag, capital=cap,
                buy_gap=BuyGapConfig())[2] == 'bag_gap_not_met'  # 0.5% from the bag cost basis < 0.6%
    assert plan(state, {}, **base, bid=D('99.3'), ask=D('99.4'), bag=bag, capital=cap,
                buy_gap=BuyGapConfig())[2] == 'entry_proposed'  # 0.7% below the cost basis


def test_controller_config_admits_the_buy_gap_only_with_live_capital():
    from controllers.generic.modular_spot import ModularSpotControllerConfig
    from controllers.generic.modular_spot_core.bag import BuyGapConfig, LiveCapitalConfig

    base = dict(id='modular_spot_bag', connector_name='okx', trading_pair='BNB-USDC', max_records=1276, bag=BagConfig())
    with pytest.raises(ValueError, match='requires live capital'):
        ModularSpotControllerConfig(**base, buy_gap=BuyGapConfig())
    ok = ModularSpotControllerConfig(**base, capital=LiveCapitalConfig(position_cap_quote=None), buy_gap=BuyGapConfig())
    assert ok.buy_gap == BuyGapConfig()


def test_depth_budget_unlocks_capital_with_depth_below_the_recent_high():
    from controllers.generic.modular_spot_core.bag import (
        DepthBudgetConfig,
        depth_budget_allowed,
        depth_budget_decision,
        depth_reference,
    )

    cfg = DepthBudgetConfig(budget_quote=D('5000'), lookback_minutes=1440, full_depth=D('0.3'), base_fraction=D('0.2'))
    assert depth_budget_allowed(cfg, reference_high=D('700'), price=D('700')) == (D('0'), D('1000'))
    depth, allowed = depth_budget_allowed(cfg, reference_high=D('700'), price=D('595'))  # 15% down
    assert depth == D('0.15') and allowed == D('3000')
    assert depth_budget_allowed(cfg, reference_high=D('700'), price=D('400'))[1] == D('5000')  # past full depth
    assert depth_budget_allowed(cfg, reference_high=D('700'), price=D('720'))[0] == 0  # above the high
    assert depth_reference(D('600'), D('650')) == D('650') and depth_reference(D('600'), None) == D('600')
    assert depth_reference(D('600'), D('550')) == D('600')
    assert depth_budget_decision(cfg, D('100'), deployed_quote=D('2900'), reference_high=D('700'),
                                 price=D('595')) == (D('100'), 'admitted')
    assert depth_budget_decision(cfg, D('100'), deployed_quote=D('2901'), reference_high=D('700'),
                                 price=D('595')) == (None, 'depth_budget')
    with pytest.raises(ValueError):
        DepthBudgetConfig(budget_quote=D('5000'), lookback_minutes=30, full_depth=D('0.3'), base_fraction=D('0.2'))


def test_planner_refuses_a_buy_past_the_depth_budget_and_counts_the_bag_at_cost():
    from controllers.generic.modular_spot_core.bag import DepthBudgetConfig, LiveCapitalConfig
    from controllers.generic.modular_spot_core.config import AcquisitionPolicy, RiskPolicy, SignalPolicy
    from controllers.generic.modular_spot_core.market import MarketFeatures
    from controllers.generic.modular_spot_core.planner import plan
    from controllers.generic.modular_spot_core.state import DecisionState

    cap = LiveCapitalConfig(slice_quote=D('100'), position_cap_quote=None, max_active_tranches=8)
    depth = DepthBudgetConfig(budget_quote=D('5000'), lookback_minutes=1440, full_depth=D('0.3'), base_fraction=D('0.2'))
    _, bag_features_ = feats()
    base = dict(fingerprint='a' * 64, budget=D('5000'), now=1200, available_quote=D('5000'), amount_step=D('.001'),
                minimum_amount=D('.001'), minimum_notional=D('1'), pnl=D('0'), signal=SignalPolicy(),
                acquisition=AcquisitionPolicy(), risk=RiskPolicy())
    state = DecisionState(fingerprint='a' * 64, candle=1080)
    bag = HeldBag(D('10'), D('100'), D('0'))  # 1000 deployed at cost
    shallow_bag = HeldBag(D('17'), D('95'), D('0'))  # 1615 at cost, breakeven 95 above the 90.5 high
    at_high = MarketFeatures(1140, D('90'), D('29'), D('31'), False, False,
                             bag=replace(bag_features_, recent_high=D('90.5')))
    assert plan(state, {}, **base, market=at_high, bid=D('90'), ask=D('90.1'), bag=shallow_bag, capital=cap,
                depth_budget=depth)[2] == 'depth_budget'  # 5.3% below 95: 1615 + 100 > 1702
    deep = MarketFeatures(1140, D('90'), D('29'), D('31'), False, False,
                          bag=replace(bag_features_, recent_high=D('100')))
    assert plan(state, {}, **base, market=deep, bid=D('90'), ask=D('90.1'), bag=bag, capital=cap,
                depth_budget=depth)[2] == 'entry_proposed'  # 10% down: 1000 + 100 <= 2333
    # The held bag's breakeven is the reference when it is above the recent high: 10% below the
    # 100 high allows 2333, 18.2% below a 110 breakeven allows 3424.
    high_cost = HeldBag(D('40'), D('110'), D('0'))  # 4400 at cost, above 3424: refused
    assert plan(state, {}, **base, market=deep, bid=D('90'), ask=D('90.1'), bag=high_cost, capital=cap,
                depth_budget=depth)[2] == 'depth_budget'
    below = HeldBag(D('24'), D('110'), D('0'))  # 2640 at cost + 100 fits under 3424, not under 2333
    assert plan(state, {}, **base, market=deep, bid=D('90'), ask=D('90.1'), bag=below, capital=cap,
                depth_budget=depth)[2] == 'entry_proposed'
    assert plan(state, {}, **base, market=MarketFeatures(1140, D('90'), D('29'), D('31'), False, False, bag=bag_features_),
                bid=D('90'), ask=D('90.1'), bag=bag, capital=cap, depth_budget=depth)[2] == 'depth_history_unavailable'
    with pytest.raises(PolicyError, match='requires live capital'):
        plan(state, {}, **base, market=deep, bid=D('90'), ask=D('90.1'), bag=bag, depth_budget=depth)


def test_depth_feature_is_the_highest_closed_high_over_the_lookback():
    import numpy as np
    import pandas as pd

    from controllers.generic.modular_spot_core.bag import bag_features

    n = 1400
    stamps = np.arange(n) * 60
    close = np.linspace(100, 80, n)
    frame = pd.DataFrame({'timestamp': stamps, 'open': close, 'high': close + 1, 'low': close - 1, 'close': close,
                          'volume': np.ones(n)})
    kwargs = dict(rsi_prev=30, now=float(n * 60), short_records=500, higher_records=85, adx_threshold=18)
    assert bag_features(frame, **kwargs).recent_high is None
    assert bag_features(frame, **kwargs, depth_lookback=60).recent_high == D(str(float(close[-60] + 1)))
    assert bag_features(frame, **kwargs, depth_lookback=n + 1).recent_high is None  # short history: entries only refuse


def test_controller_config_depth_budget_needs_capital_and_history():
    from controllers.generic.modular_spot import ModularSpotControllerConfig
    from controllers.generic.modular_spot_core.bag import DepthBudgetConfig, LiveCapitalConfig

    depth = DepthBudgetConfig(budget_quote=D('5000'), lookback_minutes=1440, full_depth=D('0.3'), base_fraction=D('0.2'))
    base = dict(id='modular_spot_bag', connector_name='okx', trading_pair='BNB-USDC', bag=BagConfig())
    with pytest.raises(ValueError, match='requires live capital'):
        ModularSpotControllerConfig(**base, max_records=1500, depth_budget=depth)
    with pytest.raises(ValueError, match='max_records >= 1441'):
        ModularSpotControllerConfig(**base, max_records=1300, capital=LiveCapitalConfig(position_cap_quote=None),
                                    depth_budget=depth)
    ok = ModularSpotControllerConfig(**base, max_records=1500, capital=LiveCapitalConfig(position_cap_quote=None),
                                     depth_budget=depth)
    assert ok.depth_budget == depth


def test_depth_scaled_buy_size_grows_with_depth_and_stays_within_the_budget():
    from controllers.generic.modular_spot_core.bag import DepthBudgetConfig, LiveCapitalConfig, depth_slice_quote
    from controllers.generic.modular_spot_core.config import AcquisitionPolicy, RiskPolicy, SignalPolicy
    from controllers.generic.modular_spot_core.market import MarketFeatures
    from controllers.generic.modular_spot_core.planner import plan
    from controllers.generic.modular_spot_core.state import DecisionState

    cfg = DepthBudgetConfig(budget_quote=D('5000'), lookback_minutes=1440, full_depth=D('0.2'), base_fraction=D('1'),
                            max_slice_multiplier=D('3'))
    assert depth_slice_quote(cfg, D('100'), D('0')) == D('100')
    assert depth_slice_quote(cfg, D('100'), D('0.1')) == D('200')
    assert depth_slice_quote(cfg, D('100'), D('0.5')) == D('300')  # capped at full depth
    assert DepthBudgetConfig(budget_quote=D('1'), lookback_minutes=60, full_depth=D('0.2'), base_fraction=D('1')).max_slice_multiplier == 1
    cap = LiveCapitalConfig(slice_quote=D('100'), position_cap_quote=None, max_active_tranches=8)
    _, bag_features_ = feats()
    market = MarketFeatures(1140, D('90'), D('29'), D('31'), False, False, bag=replace(bag_features_, recent_high=D('100')))
    base = dict(fingerprint='a' * 64, budget=D('5000'), now=1200, available_quote=D('5000'), amount_step=D('.001'),
                minimum_amount=D('.001'), minimum_notional=D('1'), pnl=D('0'), signal=SignalPolicy(),
                acquisition=AcquisitionPolicy(), risk=RiskPolicy(), market=market, bid=D('90'), ask=D('90.1'))
    state = DecisionState(fingerprint='a' * 64, candle=1080)
    empty = HeldBag(D('0'), D('0'), D('0'))
    _, proposal, reason = plan(state, {}, **base, bag=empty, capital=cap, depth_budget=cfg)
    assert reason == 'entry_proposed' and proposal.amount * D('90') > D('190')  # 10% deep: a 2x (200) buy
    poor = dict(base, available_quote=D('150'))
    assert plan(state, {}, **poor, bag=empty, capital=cap, depth_budget=cfg)[2] == 'cash_exhausted'  # the sized buy is refused, not shrunk


def test_live_capital_pending_buy_is_reserved_at_at_least_current_mid():
    from controllers.generic.modular_spot_core.bag import LiveCapitalConfig, live_capital_exposure, live_capital_quote

    cap = LiveCapitalConfig(slice_quote=D('100'), position_cap_quote=D('250'))
    inputs = dict(held_amount=D('0'), held_dust=True, mid=D('200'),
                  active=((D('0'), D('1'), D('100')),))
    assert live_capital_exposure(**inputs) == D('200')
    assert live_capital_quote(cap, **inputs, available_quote=D('1000')) == (None, 'max_position')
    # A limit above mid still binds; owned base and pending base are separate.
    assert live_capital_exposure(held_amount=D('1'), held_dust=False, mid=D('90'),
                                 active=((D('0.5'), D('0.5'), D('100')),)) == D('185')
