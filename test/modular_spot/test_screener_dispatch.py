"""Screener dispatch. Native skips fail M7. Observation is not capital permission."""
import asyncio
import importlib.util
from decimal import Decimal as D
from types import SimpleNamespace as NS

import pytest

if importlib.util.find_spec('hummingbot') is None:
    pytest.fail('native Hummingbot package/compiled runtime is unavailable')

from test.modular_spot.test_native_lifecycle import disposable_recorder, release_recorder  # noqa: E402

from controllers.generic.modular_spot import ModularSpotController, ModularSpotControllerConfig  # noqa: E402
from controllers.generic.modular_spot_core.candle_data import Candle, CandleSnapshot, Replica  # noqa: E402
from controllers.generic.modular_spot_core.data_reader import DataPolicy  # noqa: E402
from controllers.generic.modular_spot_core.domain import PolicyError, identity  # noqa: E402
from controllers.generic.modular_spot_core.execution import native_config  # noqa: E402
from controllers.generic.modular_spot_core.profiles import ProgramProfile, build_profile  # noqa: E402
from controllers.generic.modular_spot_core.program_budget import Reservation  # noqa: E402
from controllers.generic.modular_spot_core.screener import (  # noqa: E402
    CapitalAuthority,
    NativeMarketReadiness,
    admit_candidates,
    screen,
)
from controllers.generic.modular_spot_core.selection_state import SelectionStore  # noqa: E402
from hummingbot.core.data_type.common import PriceType  # noqa: E402
from hummingbot.model.metadata import Metadata  # noqa: E402

EXEC = ('A-USDC', 'B-USDC', 'C-USDC', 'D-USDC', 'E-USDC')
NEW = 'Z-USDC'
BENCH = ('BTC-USDC', 'ETH-USDC')
OBSERVE = EXEC + (NEW,) + BENCH
NOW = 6000.0
CLOSES = {'A-USDC': '110', 'B-USDC': '108', 'C-USDC': '106', 'D-USDC': '104',
          'E-USDC': '102', NEW: '120'}


def impulse_rows(end, count, last_close='100', last_volume='5', base='100'):
    start = end - (count - 1) * 60
    rows = [Candle(open_time=t, open=D(base), high=D(base) + 1, low=D(base) - 1,
                   close=D(base), volume=D('5')) for t in range(start, end, 60)]
    close = D(last_close)
    rows.append(Candle(open_time=end, open=D(base), high=max(close, D(base) + 1),
                       low=D(base) - 1, close=close, volume=D(last_volume)))
    return tuple(rows)


def snap(spec, *, last_open, seq, published_at, last_close='100', last_volume='5'):
    impulse = last_close != '100'
    return CandleSnapshot(
        dataset=spec, epoch='boot-one', sequence=seq, published_at=published_at,
        rows=impulse_rows(last_open, spec.history, last_close=last_close,
                          last_volume='20' if impulse else last_volume))


def seed_catalog(pool, *, closes=None, unfinished=(), skip=(), now=NOW):
    closes = closes or {}
    replica = Replica(tuple(pool.datasets.values()), max_transport_age=15)
    replica.begin_session()
    for spec in pool.datasets.values():
        pair = spec.trading_pair
        if pair in skip:
            continue
        close = closes.get(pair, '100')
        if pair in unfinished:
            replica.accept(snap(spec, last_open=5820, seq=1, published_at=now, last_close=close), now=now)
            replica.accept(snap(spec, last_open=5880, seq=2, published_at=now, last_close=close), now=now)
            continue
        replica.accept(snap(spec, last_open=5880, seq=1, published_at=now - 60, last_close=close), now=now - 60)
        replica.accept(snap(spec, last_open=5940, seq=2, published_at=now, last_close=close), now=now)
    pool.client = NS(replica=replica)
    pool.screen_cache = None
    pool.ranking_cache = {}
    return replica


def bundle_payloads(**overrides):
    values = dict(name='impulse', connector='okx', symbols=EXEC, observation_symbols=EXEC + (NEW,),
                  benchmarks=BENCH, mode='impulse', quote_budget='100', max_positions=2,
                  account='paper-okx', history=61)
    values.update(overrides)
    return [ModularSpotControllerConfig.model_validate(item)
            for item in build_profile(ProgramProfile(**values))['controllers']]


def enabled(cfg, **updates):
    payload = {'execution_enabled': True, **updates}
    return cfg.model_copy(update=payload)


def rule():
    return NS(min_price_increment=D('0.01'), min_base_amount_increment=D('0.001'),
              min_order_size=D('0.001'), min_notional_size=D('1'))


class Provider:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def provider(*, listed=EXEC, ready=True, broken=(), now=NOW):
    def prices(connector, pair, ptype):
        if pair in broken:
            raise RuntimeError('book missing')
        return D('100') if ptype is PriceType.BestBid else D('100.1')

    market = NS(ready=ready, supports_terminal_fill_reconciliation=True,
                trading_rules={pair: rule() for pair in listed},
                get_available_balance=lambda quote: D('100'))
    return Provider(ready=True, time=lambda: now, connectors={'okx': market}, get_price_by_type=prices)


def bind(controller, recorder):
    rt = controller.runtime
    rt.bindings.recorder = lambda: recorder
    rt.bindings.guard_resume = lambda *args, **kwargs: None
    rt.store = None
    rt.budget = None
    rt.selection_store = None
    controller.executors_info = []
    controller.positions_held = []
    rt.inventory = lambda: {}
    rt.ensure_store()
    return rt


def claim(cfg, pair, quote='50', ident=None):
    return Reservation(
        executor_id=ident or f'acq-{pair}', controller_id=cfg.id, trading_pair=pair,
        reserved_quote=D(quote), reserved_base=D('1'), account='paper-okx', connector='okx',
        program=cfg.budget.group, slot=pair.split('-')[0], expires_at=D('0'))


def shortlist_hash(cfg):
    return identity([cfg.universe.model_dump(mode='json'), cfg.data.model_dump(mode='json'),
                     cfg.connector_name, cfg.interval])


def authority(cfg, budget, recorder, **extra):
    values = dict(
        universe=cfg.universe,
        selection_store=SelectionStore(
            recorder, group=cfg.budget.group, policy_hash=shortlist_hash(cfg), metadata_model=Metadata),
        budget=budget, native_executors=(), fingerprint=cfg.fingerprint(), account=cfg.account)
    values.update(extra)
    return CapitalAuthority(**values)


def readiness(*, tradable=EXEC, available=OBSERVE, **extra):
    return NativeMarketReadiness(available=available, tradable=tradable, **extra)


@pytest.fixture
def desk(tmp_path):
    recorder = disposable_recorder(tmp_path)
    configs = [enabled(cfg) for cfg in bundle_payloads()]
    market = provider()
    controllers = [ModularSpotController(cfg, market, asyncio.Queue()) for cfg in configs]
    pool = controllers[0].runtime.pool
    seed_catalog(pool, closes=CLOSES)
    for controller in controllers:
        bind(controller, recorder)
    try:
        yield recorder, controllers, pool, configs
    finally:
        release_recorder(recorder)


def test_many_candidates_fewer_slots_without_cash_or_slot_oversubscription(desk):
    recorder, controllers, pool, configs = desk
    assert all(cfg.execution_enabled is True for cfg in configs)
    assert all(item.execution_enabled is False for item in bundle_payloads())
    ranked = screen(pool, configs[0], NS(now=NOW))
    assert ranked.coverage == D('1')
    assert set(ranked.observed) == set(OBSERVE)
    assert {item.trading_pair for item in ranked.opportunities} == set(EXEC + (NEW,))
    budget = controllers[0].runtime.budget
    assert budget.occupied_pairs(native_executors=[]) == ()
    admitted = admit_candidates(ranked, readiness(), authority(configs[0], budget, recorder))
    acquired = {item.trading_pair for item in admitted.proposals}
    assert set(OBSERVE) <= set(admitted.observation)
    assert len(acquired) == 2
    assert acquired <= set(EXEC)
    assert NEW not in acquired
    assert acquired == {'A-USDC', 'B-USDC'}
    by_pair = {cfg.trading_pair: controller for cfg, controller in zip(configs, controllers)}
    permitted = {pair for pair, controller in by_pair.items() if controller.runtime.acquisition_permitted()}
    assert permitted == acquired
    granted = 0
    for pair in acquired:
        granted += bool(budget.reserve(claim(by_pair[pair].config, pair), native_executors=[]))
    assert granted == 2
    leftover = claim(configs[2], 'C-USDC', '1')
    assert not budget.reserve(leftover, native_executors=[])
    total = D('0')
    bases = set()
    for pair in EXEC:
        row = claim(by_pair[pair].config, pair)
        if budget.contains(row, native_executors=[]):
            total += row.reserved_quote
            bases.add(row.slot)
    assert total <= D('100')
    assert len(bases) <= 2
    assert budget.occupied_pairs(native_executors=[]) == ('A-USDC', 'B-USDC')


def test_stale_eligibility_or_missing_context_blocks_admission(desk):
    recorder, controllers, pool, configs = desk
    ranked = screen(pool, configs[0], NS(now=NOW))
    budget = controllers[0].runtime.budget
    stale = admit_candidates(ranked, readiness(stale=('A-USDC',), context_valid=False),
                             authority(configs[0], budget, recorder))
    assert stale.proposals == ()
    assert stale.reason == 'stale_eligibility'
    missing = admit_candidates(
        ranked, readiness(available=EXEC + (NEW,)), authority(configs[0], budget, recorder))
    assert missing.proposals == ()
    assert missing.reason == 'missing_context'
    dropped = admit_candidates(ranked, readiness(stale=('A-USDC',)),
                               authority(configs[0], budget, recorder))
    assert 'A-USDC' not in dropped.entries
    assert dropped.entries == ('B-USDC', 'C-USDC')


def test_unready_native_book_or_rules_block_admission(desk):
    recorder, controllers, pool, configs = desk
    ranked = screen(pool, configs[0], NS(now=NOW))
    budget = controllers[0].runtime.budget
    blocked = admit_candidates(ranked, readiness(unready=('A-USDC',)),
                               authority(configs[0], budget, recorder))
    assert 'A-USDC' not in blocked.entries
    assert 'B-USDC' in blocked.entries
    market = provider(broken=('A-USDC',))
    for controller in controllers:
        controller.market_data_provider = market
        bind(controller, recorder)
        controller.runtime.pool = pool
    assert controllers[0].config.trading_pair == 'A-USDC'
    assert controllers[0].runtime.acquisition_permitted() is False
    assert controllers[1].runtime.acquisition_permitted() is True


def test_delisting_between_selection_and_dispatch_keeps_management(desk):
    recorder, controllers, pool, configs = desk
    ranked = screen(pool, configs[0], NS(now=NOW))
    budget = controllers[0].runtime.budget
    first = admit_candidates(ranked, readiness(), authority(configs[0], budget, recorder))
    assert 'A-USDC' in first.entries
    later = admit_candidates(
        ranked, readiness(tradable=('B-USDC', 'C-USDC', 'D-USDC', 'E-USDC')),
        authority(configs[0], budget, recorder, managed=('A-USDC',),
                  selection_store=None, previous=first.entries))
    assert 'A-USDC' not in later.entries
    assert 'A-USDC' in later.management
    assert later.entries == ('B-USDC',)
    market = provider(listed=('B-USDC', 'C-USDC', 'D-USDC', 'E-USDC'))
    owner = controllers[0]
    owner.market_data_provider = market
    bind(owner, recorder)
    owner.runtime.pool = pool
    owner.runtime.inventory = lambda: {
        'held-a': NS(remaining_base=D('1'), terminal=False)}
    owner.executors_info = [NS(config=NS(trading_pair='A-USDC'))]
    owner.runtime._managed_pairs = lambda: ('A-USDC',)
    assert owner.runtime.acquisition_permitted() is False
    assert 'A-USDC' in owner.runtime._managed_pairs()


def test_rank_churn_hysteresis_cannot_defer_hard_exclusion(desk):
    recorder, controllers, pool, configs = desk
    cfg = enabled(configs[0], universe=configs[0].universe.model_copy(
        update={'replacement_margin': D('0.5'), 'max_candidates': 1}))
    ranked = screen(pool, cfg, NS(now=NOW))
    budget = controllers[0].runtime.budget
    store = SelectionStore(recorder, group='hysteresis', policy_hash=shortlist_hash(cfg), metadata_model=Metadata)
    held = admit_candidates(ranked, readiness(),
                            authority(cfg, budget, recorder, selection_store=store))
    assert held.entries == ('A-USDC',)
    seed_catalog(pool, closes={**CLOSES, 'B-USDC': '112'})
    churned = screen(pool, cfg, NS(now=NOW))
    assert churned.scores['B-USDC'] > ranked.scores['A-USDC']
    still = admit_candidates(churned, readiness(),
                             authority(cfg, budget, recorder, selection_store=store))
    assert still.entries == ('A-USDC',)
    excluded = admit_candidates(churned, readiness(blocked=('A-USDC',)),
                                authority(cfg, budget, recorder, selection_store=store))
    assert excluded.entries == ('B-USDC',)


def test_one_unavailable_instrument_does_not_corrupt_valid_peers(desk):
    recorder, controllers, pool, configs = desk
    cfg = configs[0]
    assert DataPolicy().minimum_coverage == D('1')
    assert cfg.data.minimum_coverage == D('1')
    seed_catalog(pool, closes=CLOSES, skip=('D-USDC',))
    ranked = screen(pool, cfg, NS(now=NOW))
    assert ranked.coverage == D('0.875')
    assert 'D-USDC' not in ranked.scores
    assert ranked.scores['A-USDC'] > 0 and ranked.scores['B-USDC'] > 0
    budget = controllers[0].runtime.budget
    admitted = admit_candidates(ranked, readiness(unready=('C-USDC',)),
                                authority(cfg, budget, recorder, selection_store=None))
    assert admitted.entries == ('A-USDC', 'B-USDC')
    by_pair = {item.trading_pair: controller for item, controller in zip(configs, controllers)}
    assert by_pair['A-USDC'].runtime.acquisition_permitted() is True
    assert by_pair['B-USDC'].runtime.acquisition_permitted() is True
    assert by_pair['D-USDC'].runtime.acquisition_permitted() is False
    seed_catalog(pool, closes=CLOSES)
    market = provider(broken=('C-USDC',))
    for controller in controllers:
        controller.market_data_provider = market
        bind(controller, recorder)
        controller.runtime.pool = pool
    by_pair = {item.trading_pair: controller for item, controller in zip(configs, controllers)}
    assert by_pair['A-USDC'].runtime.acquisition_permitted() is True
    assert by_pair['C-USDC'].runtime.acquisition_permitted() is False


def test_newly_observed_listing_is_not_auto_enrolled(desk):
    recorder, controllers, pool, configs = desk
    pairs = {cfg.trading_pair for cfg in configs}
    assert NEW not in pairs
    ranked = screen(pool, configs[0], NS(now=NOW))
    assert NEW in ranked.scores
    admitted = admit_candidates(ranked, readiness(),
                                authority(configs[0], controllers[0].runtime.budget, recorder))
    assert NEW not in admitted.entries
    assert NEW in admitted.observation
    expanded = admit_candidates(
        ranked, readiness(tradable=EXEC + (NEW,)),
        authority(configs[0], controllers[0].runtime.budget, recorder))
    assert NEW not in expanded.entries


def test_selection_change_does_not_rewrite_frozen_open_position(desk):
    recorder, controllers, pool, configs = desk
    cfg = configs[0]
    store = SelectionStore(recorder, group=cfg.budget.group, policy_hash='shortlist', metadata_model=Metadata)
    admitted = native_config(cfg, identifier='claim-1', timestamp=120, price=D('100'), amount=D('1'),
                             cycle=0, stage=0)
    store.freeze(executor_id='claim-1', trading_pair='A-USDC', policy_hash=cfg.fingerprint())
    ranked = screen(pool, cfg, NS(now=NOW))
    later = admit_candidates(ranked, readiness(blocked=('A-USDC',)),
                             authority(cfg, controllers[0].runtime.budget, recorder, selection_store=store))
    assert 'A-USDC' not in later.entries
    store.freeze(executor_id='claim-1', trading_pair='A-USDC', policy_hash=cfg.fingerprint())
    mutated = cfg.model_copy(update={'trailing': cfg.trailing.model_copy(update={'activation': D('.02')})})
    assert mutated.fingerprint() != cfg.fingerprint()
    with pytest.raises(PolicyError):
        store.freeze(executor_id='claim-1', trading_pair='A-USDC', policy_hash=mutated.fingerprint())
    later_native = native_config(cfg, identifier='claim-1', timestamp=120, price=D('100'), amount=D('1'),
                                 cycle=0, stage=0)
    assert later_native.triple_barrier_config.trailing_stop == admitted.triple_barrier_config.trailing_stop
    assert later_native.controller_id == cfg.id


def test_unfinished_bar_is_not_a_closed_bar_impulse(desk):
    recorder, controllers, pool, configs = desk
    cfg = configs[0]
    assert cfg.data.minimum_coverage == D('1')
    seed_catalog(pool, closes=CLOSES, unfinished=('A-USDC',))
    ranked = screen(pool, cfg, NS(now=NOW))
    assert ranked.coverage == D('0.875')
    assert 'A-USDC' not in ranked.scores
    assert ranked.scores['B-USDC'] > 0
    admitted = admit_candidates(ranked, readiness(),
                                authority(cfg, controllers[0].runtime.budget, recorder))
    assert 'A-USDC' not in admitted.entries
    seed_catalog(pool, closes=CLOSES)
    closed = screen(pool, cfg, NS(now=NOW))
    assert closed.scores['A-USDC'] > 0
    assert closed.candle == 5940
