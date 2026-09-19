"""Fixed-symbol programs through native controller discovery. Native skips fail M4."""
import asyncio
import importlib.util
from decimal import Decimal as D
from types import SimpleNamespace as NS

import pytest
from sqlalchemy import Column, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

if importlib.util.find_spec('hummingbot') is None:
    pytest.fail('native Hummingbot package/compiled runtime is unavailable')

from test_shared_data import rows, snapshot  # noqa: E402

from controllers.generic.modular_spot import ModularSpotController, ModularSpotControllerConfig  # noqa: E402
from controllers.generic.modular_spot_core.candle_data import Replica  # noqa: E402
from controllers.generic.modular_spot_core.data_reader import attach_pool, dataset_for, release_pool  # noqa: E402
from controllers.generic.modular_spot_core.domain import PolicyError  # noqa: E402
from controllers.generic.modular_spot_core.execution import native_config  # noqa: E402
from controllers.generic.modular_spot_core.native_bindings import NativeBindings  # noqa: E402
from controllers.generic.modular_spot_core.profiles import ProgramProfile, build_profile  # noqa: E402
from controllers.generic.modular_spot_core.selection_state import SelectionStore  # noqa: E402
from controllers.generic.modular_spot_core.universe import UniversePolicy, select_universe  # noqa: E402
from hummingbot.client.settings import AllConnectorSettings, ConnectorType  # noqa: E402
from hummingbot.core.data_type.common import PriceType  # noqa: E402

EXEC = ('A-USDC', 'B-USDC', 'C-USDC', 'D-USDC', 'E-USDC')
BENCH = ('BTC-USDC', 'ETH-USDC')


def profile(**overrides):
    values = dict(name='selected', connector='okx', symbols=EXEC, benchmarks=BENCH, mode='fixed',
                  quote_budget='500', max_positions=3, account='paper-okx',
                  program_revision='p1', policy_revision='v1')
    values.update(overrides)
    return ProgramProfile(**values)


def compiled_universe(bundle):
    return UniversePolicy.model_validate(bundle['controllers'][0]['universe'])


class Provider:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def test_five_execution_symbols_plus_two_benchmarks_do_not_grant_capital():
    bundle = build_profile(profile())
    payloads = [ModularSpotControllerConfig.model_validate(item) for item in bundle['controllers']]
    assert len(payloads) == 5
    assert {c.trading_pair for c in payloads} == set(EXEC)
    assert {row['trading_pair'] for row in bundle['candle_manifest']['datasets']} == set(EXEC + BENCH)
    assert all(c.execution_enabled is False and c.account == 'paper-okx' for c in payloads)
    assert all(c.program_revision == 'p1' and c.policy_revision == 'v1' for c in payloads)
    assert all(c.universe.benchmarks == BENCH and c.universe.max_candidates == 3 for c in payloads)
    assert all(c.budget.max_quote == D('500') and c.total_amount_quote == D('500') / 3 for c in payloads)
    assert bundle['execution_authorized'] is False
    selection = select_universe(
        compiled_universe(bundle), available=EXEC + BENCH, tradable=EXEC, scores={},
        managed=(), pending=(), blocked=(), previous=())
    assert len(selection.entries) == 3
    assert set(selection.entries) <= set(EXEC)
    assert set(BENCH) <= set(selection.observation)
    assert set(BENCH).isdisjoint(selection.entries)


def test_duplicate_quote_aliases_cannot_evade_base_concentration():
    with pytest.raises(ValueError):
        profile(symbols=('BTC-USDC', 'BTC-USDT'), benchmarks=(), max_positions=1, quote_budget='100')
    bundle = build_profile(profile(symbols=('BTC-USDC', 'ETH-USDC'), benchmarks=(), max_positions=2,
                                   quote_budget='200'))
    selection = select_universe(
        compiled_universe(bundle), available=('BTC-USDC', 'ETH-USDC', 'BTC-USDT'),
        tradable=('BTC-USDC', 'ETH-USDC'), scores={}, managed=('BTC-USDT',), pending=(),
        blocked=(), previous=())
    assert 'BTC-USDT' in selection.management
    assert 'BTC-USDC' not in selection.entries
    assert selection.entries == ('ETH-USDC',)


def test_conflicting_owner_ids_are_rejected():
    with pytest.raises(ValueError):
        profile(symbols=('A-USDC', 'B-USDC'), benchmarks=(), max_positions=2, quote_budget='200',
                native_owners=('modular_spot_desk_a', 'modular_spot_desk_a'))
    bundle = build_profile(profile(
        symbols=('A-USDC', 'B-USDC'), benchmarks=(), max_positions=2, quote_budget='200',
        native_owners=('modular_spot_desk_a', 'modular_spot_desk_b')))
    ids = [ModularSpotControllerConfig.model_validate(item).id for item in bundle['controllers']]
    assert ids == ['modular_spot_desk_a', 'modular_spot_desk_b']


@pytest.mark.parametrize('instrument', ['bad', 'btc-usdc', 'USDC-USDC', 'BTC-USDC-SWAP'])
def test_invalid_spot_instruments_rejected(instrument):
    with pytest.raises(ValueError):
        profile(symbols=(instrument,), benchmarks=(), max_positions=1, quote_budget='100')


def test_missing_context_holds_entries_and_keeps_subscriptions():
    bundle = build_profile(profile())
    policy = compiled_universe(bundle)
    selection = select_universe(
        policy, available=EXEC, tradable=EXEC, scores={}, managed=(), pending=(), blocked=(), previous=())
    assert selection.entries == ()
    assert set(BENCH) <= set(selection.observation)
    assert set(EXEC) <= set(selection.observation)
    assert {row['trading_pair'] for row in bundle['candle_manifest']['datasets']} == set(EXEC + BENCH)


def test_removed_symbol_keeps_management_subscriptions_and_obligations():
    bundle = build_profile(profile(symbols=('A-USDC', 'B-USDC'), max_positions=2, quote_budget='200'),
                           managed=('C-USDC',), pending=('D-USDC',))
    configs = [ModularSpotControllerConfig.model_validate(item) for item in bundle['controllers']]
    by_pair = {c.trading_pair: c for c in configs}
    assert set(by_pair) == {'A-USDC', 'B-USDC', 'C-USDC', 'D-USDC'}
    assert by_pair['A-USDC'].acquisition_enabled is True
    assert by_pair['C-USDC'].acquisition_enabled is False
    assert by_pair['D-USDC'].acquisition_enabled is False
    assert by_pair['C-USDC'].universe.symbols == ('A-USDC', 'B-USDC')
    datasets = {row['trading_pair'] for row in bundle['candle_manifest']['datasets']}
    assert {'A-USDC', 'B-USDC', 'C-USDC', 'D-USDC', *BENCH} <= datasets
    selection = select_universe(
        compiled_universe(bundle), available=('A-USDC', 'B-USDC', 'C-USDC', 'D-USDC', *BENCH),
        tradable=('A-USDC', 'B-USDC'), scores={}, managed=('C-USDC',), pending=('D-USDC',),
        blocked=(), previous=())
    assert set(selection.management) == {'C-USDC', 'D-USDC'}
    assert {'C-USDC', 'D-USDC'} <= set(selection.observation)
    provider = Provider()
    pool = attach_pool(provider, by_pair['C-USDC'])
    try:
        assert any(spec.trading_pair == 'C-USDC' for spec in pool.datasets.values())
        assert any(spec.trading_pair == 'BTC-USDC' for spec in pool.datasets.values())
    finally:
        release_pool(provider, pool)


def test_admitted_behavioral_policy_is_frozen_while_selection_changes(tmp_path):
    base = declarative_base()

    class Metadata(base):
        __tablename__ = 'Metadata'
        key = Column(Text, primary_key=True)
        value = Column(Text, nullable=False)

    engine = create_engine('sqlite:///' + str(tmp_path / 'freeze.db'), connect_args={'timeout': 5})
    base.metadata.create_all(engine)
    recorder = NS(config_file_path='native-strategy.yml', sql_manager=NS(get_new_session=sessionmaker(bind=engine)))
    store = SelectionStore(recorder, group='selected', policy_hash='shortlist', metadata_model=Metadata)
    five = ModularSpotControllerConfig.model_validate(build_profile(profile())['controllers'][0])
    four = ModularSpotControllerConfig.model_validate(
        build_profile(profile(symbols=EXEC[:-1], max_positions=3))['controllers'][0])
    assert five.fingerprint() == four.fingerprint()
    admitted = native_config(five, identifier='claim-1', timestamp=120, price=D('100'), amount=D('1'),
                             cycle=0, stage=0)
    later = native_config(four, identifier='claim-1', timestamp=120, price=D('100'), amount=D('1'),
                          cycle=0, stage=0)
    assert admitted.triple_barrier_config.trailing_stop == later.triple_barrier_config.trailing_stop
    store.freeze(executor_id='claim-1', trading_pair='A-USDC', policy_hash=five.fingerprint())
    store.freeze(executor_id='claim-1', trading_pair='A-USDC', policy_hash=five.fingerprint())
    mutated = five.model_copy(update={'trailing': five.trailing.model_copy(update={'activation': D('.02')})})
    assert mutated.fingerprint() != five.fingerprint()
    with pytest.raises(PolicyError):
        store.freeze(executor_id='claim-1', trading_pair='A-USDC', policy_hash=mutated.fingerprint())
    engine.dispose()


def test_discovery_path_feeds_deterministic_snapshots_and_native_receipts(monkeypatch):
    bundle = build_profile(profile(symbols=('BTC-USDC',), benchmarks=('ETH-USDC',), max_positions=1,
                                   quote_budget='100', history=61))
    payload = bundle['controllers'][0]
    cfg = ModularSpotControllerConfig.model_validate(payload)
    assert cfg.get_controller_class() is ModularSpotController
    assert cfg.execution_enabled is False
    spec = dataset_for(cfg)
    snap = snapshot(stream=spec, candles=rows(count=61))
    replica = Replica((spec,))
    replica.begin_session()
    replica.accept(snap, now=6000)
    setting = NS(type=ConnectorType.Exchange)
    monkeypatch.setattr(AllConnectorSettings, 'get_connector_settings', classmethod(lambda cls: {'okx': setting}))
    rules = NS(min_price_increment=D('0.01'), min_base_amount_increment=D('0.001'),
               min_order_size=D('0.001'), min_notional_size=D('1'))
    connector = NS(ready=True, supports_terminal_fill_reconciliation=True, trading_rules={'BTC-USDC': rules},
                   get_available_balance=lambda quote: D('100'))
    provider = Provider(ready=True, time=lambda: 6000, connectors={'okx': connector},
                        get_price_by_type=lambda *args, **kwargs: D('100') if args[2] is PriceType.BestBid else D('100.1'))
    controller = cfg.get_controller_class()(cfg, provider, asyncio.Queue())
    controller.runtime.pool.client = NS(replica=replica)
    controller.runtime.bindings.trend = lambda frame, **kwargs: NS(is_valid=True, direction=NS(value='up'))
    market = controller.runtime.observe()
    assert market is not None
    assert controller.runtime.input_identity == snap.digest
    controller.performance_report = NS(pnl_available=True, global_pnl_quote=D('1.25'))
    receipt = NativeBindings.snapshot(controller)
    assert receipt['bid'] == D('100') and receipt['ask'] == D('100.1') and receipt['pnl'] == D('1.25')
    assert controller.runtime.reason == 'execution_disabled'
