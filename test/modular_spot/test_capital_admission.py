"""Capital and slot admission. Native skips fail M5. BudgetStore is one recorder/strategy."""
import asyncio
import importlib.util
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal as D
from types import SimpleNamespace as NS

import pytest
from sqlalchemy import Column, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

if importlib.util.find_spec('hummingbot') is None:
    pytest.fail('native Hummingbot package/compiled runtime is unavailable')

from controllers.generic.modular_spot import ModularSpotController, ModularSpotControllerConfig  # noqa: E402
from controllers.generic.modular_spot_core.config import AcquisitionPolicy, RiskPolicy, SignalPolicy  # noqa: E402
from controllers.generic.modular_spot_core.domain import PolicyError  # noqa: E402
from controllers.generic.modular_spot_core.execution import native_config  # noqa: E402
from controllers.generic.modular_spot_core.inventory import InventoryView  # noqa: E402
from controllers.generic.modular_spot_core.market import MarketFeatures  # noqa: E402
from controllers.generic.modular_spot_core.planner import plan  # noqa: E402
from controllers.generic.modular_spot_core.portfolio import allocate  # noqa: E402
from controllers.generic.modular_spot_core.program_budget import BudgetPolicy, BudgetStore, Reservation  # noqa: E402
from controllers.generic.modular_spot_core.state import DecisionState, reconcile_claims  # noqa: E402
from controllers.generic.modular_spot_core.universe import UniversePolicy, select_universe  # noqa: E402
from hummingbot.strategy_v2.models.base import RunnableStatus  # noqa: E402

EXEC = ('A-USDC', 'B-USDC', 'C-USDC', 'D-USDC', 'E-USDC')


def metadata_engine(tmp_path, name='budget.db'):
    base = declarative_base()

    class Metadata(base):
        __tablename__ = 'Metadata'
        key = Column(Text, primary_key=True)
        value = Column(Text, nullable=False)

    engine = create_engine('sqlite:///' + str(tmp_path / name), connect_args={'timeout': 5})
    base.metadata.create_all(engine)
    recorder = NS(config_file_path='native-strategy.yml',
                  sql_manager=NS(get_new_session=sessionmaker(bind=engine)),
                  get_all_executors=lambda: [],
                  get_executors_by_controller=lambda controller_id: [],
                  get_positions_by_controller=lambda controller_id: [])
    return engine, recorder, Metadata


@pytest.fixture
def budget(tmp_path):
    engine, recorder, Metadata = metadata_engine(tmp_path)
    policy = BudgetPolicy(group='desk', max_quote=D('100'), max_assets=2)
    store = BudgetStore(recorder, policy, connector='okx', quote='USDC', account='paper-okx',
                        metadata_model=Metadata)
    yield store, recorder, Metadata
    engine.dispose()


def claim(i=1, quote='50', pair='BTC-USDC', **extra):
    values = dict(
        executor_id=f'claim-{i}', controller_id=f'modular_spot_{i}', trading_pair=pair,
        reserved_quote=D(quote), reserved_base=D('1'), account='paper-okx', connector='okx',
        program='desk', slot=pair.split('-')[0], expires_at=D('0'))
    values.update(extra)
    return Reservation(**values)


def receipt(*, identifier='claim-1', remaining='0', done=True, owner='executor',
            controller='modular_spot_1', pair='BTC-USDC', connector='okx'):
    return NS(id=identifier, controller_id=controller, is_done=done, is_active=not done,
              config=NS(controller_id=controller, connector_name=connector, trading_pair=pair),
              custom_info={'execution_receipt': {'remaining_owned_base': remaining, 'accounting_owner': owner}})


def bound_fields(row: Reservation):
    return (row.account, row.connector, row.program, row.executor_id, row.trading_pair,
            row.reserved_quote, row.reserved_base, row.slot, row.expires_at)


def test_requests_bind_account_program_intent_instrument_caps_and_slot():
    row = claim(expires_at=D('1700000060'))
    assert bound_fields(row) == (
        'paper-okx', 'okx', 'desk', 'claim-1', 'BTC-USDC', D('50'), D('1'), 'BTC', D('1700000060'))
    allocation = allocate(stage_quote=D('50'), remaining_quote=D('70'), available_quote=D('100'),
                          price=D('30'), amount_step=D('.1'), minimum_amount=D('.1'),
                          minimum_notional=D('1'), fee_buffer=D('.01'), cash_reserve=D('0'))
    assert allocation.reserved_quote == D('48.480')
    assert allocation.reserved_base == D('1.616')
    assert allocation.reserved_quote == allocation.amount * D('30') * D('1.01')


def test_two_intents_racing_for_the_same_remaining_cash_and_slot(budget):
    store, _, _ = budget
    with ThreadPoolExecutor(max_workers=4) as pool:
        cash = list(pool.map(lambda i: store.reserve(claim(i, '60', 'ETH-USDC'), native_executors=[]), range(1, 5)))
    assert sum(cash) == 1
    with ThreadPoolExecutor(max_workers=4) as pool:
        slots = list(pool.map(
            lambda item: store.reserve(claim(item[0], '10', item[1]), native_executors=[]),
            ((10, 'SOL-USDC'), (11, 'XRP-USDC'), (12, 'DOGE-USDC'), (13, 'ADA-USDC'))))
    assert sum(slots) == 1
    total = D('0')
    bases = set()
    for i in range(1, 14):
        for pair, quote in (('ETH-USDC', '60'), ('SOL-USDC', '10'), ('XRP-USDC', '10'),
                            ('DOGE-USDC', '10'), ('ADA-USDC', '10')):
            row = claim(i, quote, pair)
            if store.contains(row, native_executors=[]):
                total += row.reserved_quote
                bases.add(row.slot)
    assert total <= D('100')
    assert len(bases) <= 2


def test_lost_submission_acknowledgement_keeps_the_commitment(budget):
    store, _, _ = budget
    assert store.reserve(claim(quote='100'), native_executors=[])
    assert store.contains(claim(quote='100'), native_executors=[])
    assert not store.reserve(claim(2, '1', 'ETH-USDC'), native_executors=[])


def test_cancellation_plus_late_fill_does_not_release_then_orphan(budget):
    store, _, _ = budget
    assert store.reserve(claim(quote='100'), native_executors=[])
    cancelled = receipt(remaining='0', done=False, owner='executor')
    assert not store.reserve(claim(2, '1', 'ETH-USDC'), native_executors=[cancelled])
    late = receipt(remaining='.4', done=False)
    assert not store.reserve(claim(2, '1', 'ETH-USDC'), native_executors=[late])
    assert store.contains(claim(quote='100'), native_executors=[late])


def test_expired_signal_with_uncertain_execution_does_not_free_capital(budget):
    store, _, _ = budget
    expired = claim(quote='100', expires_at=D('1'))
    assert store.reserve(expired, native_executors=[])
    assert store.contains(expired, native_executors=[])
    assert not store.reserve(claim(2, '1', 'ETH-USDC', expires_at=D('999')), native_executors=[])


def test_restarted_owner_reloads_the_same_commitments(tmp_path):
    engine, recorder, Metadata = metadata_engine(tmp_path, 'restart.db')
    policy = BudgetPolicy(group='desk', max_quote=D('100'), max_assets=2)
    first = BudgetStore(recorder, policy, connector='okx', quote='USDC', account='paper-okx',
                        metadata_model=Metadata)
    assert first.reserve(claim(quote='80'), native_executors=[])
    second = BudgetStore(recorder, policy, connector='okx', quote='USDC', account='paper-okx',
                         metadata_model=Metadata)
    assert second.contains(claim(quote='80'), native_executors=[])
    assert not second.reserve(claim(2, '30', 'ETH-USDC'), native_executors=[])
    engine.dispose()


def test_retained_inventory_holds_cash_and_slot(budget):
    store, _, _ = budget
    assert store.reserve(claim(quote='100'), native_executors=[])
    held = receipt(remaining='.2', done=True, owner='held_position')
    assert not store.reserve(claim(2, '1', 'ETH-USDC'), native_executors=[held])
    assert store.contains(claim(quote='100'), native_executors=[held])


def test_reappearing_historical_inventory_blocks_new_admission(budget):
    store, _, _ = budget
    assert store.reserve(claim(quote='100'), native_executors=[])
    assert store.reserve(claim(2, '50', 'ETH-USDC'), native_executors=[receipt()])
    ghost = receipt(remaining='.1', done=True, owner='executor')
    with pytest.raises(PolicyError, match='unreserved or reappearing'):
        store.reserve(claim(3, '10', 'SOL-USDC'), native_executors=[ghost])


def test_wrong_program_or_member_is_rejected(budget):
    store, _, _ = budget
    assert store.reserve(claim(quote='40'), native_executors=[])
    with pytest.raises(PolicyError, match='program|member|account|connector'):
        store.reserve(claim(2, '10', 'ETH-USDC', program='other'), native_executors=[])
    with pytest.raises(PolicyError, match='program|member|account|connector'):
        store.reserve(claim(2, '10', 'ETH-USDC', account='other-desk'), native_executors=[])
    with pytest.raises(PolicyError, match='program|member|account|connector'):
        store.reserve(claim(2, '10', 'ETH-USDC', connector='binance'), native_executors=[])
    switched = claim(2, '10', 'ETH-USDC', controller_id='modular_spot_1')
    with pytest.raises(PolicyError, match='instrument ownership|member'):
        store.reserve(switched, native_executors=[])


def test_base_asset_reached_through_multiple_quote_pairs(budget):
    store, _, _ = budget
    usdt = receipt(identifier='claim-9', controller='modular_spot_9', remaining='1', done=False,
                   pair='BTC-USDT')
    assert not store.reserve(claim(1, '10', 'BTC-USDC'), native_executors=[usdt])
    assert store.reserve(claim(1, '10', 'BTC-USDC'), native_executors=[])
    with pytest.raises(PolicyError, match='quote differs|wrong program|connector'):
        store.reserve(claim(2, '10', 'BTC-USDT'), native_executors=[])
    assert store.reserve(claim(3, '10', 'ETH-USDC'), native_executors=[])
    assert not store.reserve(claim(4, '10', 'SOL-USDC'), native_executors=[])


def test_expiry_absence_recovery_label_and_unconfirmed_cancel_cannot_release(budget):
    store, _, _ = budget
    assert store.reserve(claim(quote='100', expires_at=D('1')), native_executors=[])
    assert store.contains(claim(quote='100', expires_at=D('1')), native_executors=[])
    restarted = BudgetStore(store.recorder, store.policy, connector='okx', quote='USDC',
                            account='paper-okx', metadata_model=store.model)
    assert restarted.contains(claim(quote='100', expires_at=D('1')), native_executors=[])
    recovery = receipt(remaining='0', done=True, owner='recovery')
    assert not store.reserve(claim(2, '1', 'ETH-USDC'), native_executors=[recovery])
    unconfirmed = receipt(remaining='0', done=False)
    assert not store.reserve(claim(2, '1', 'ETH-USDC'), native_executors=[unconfirmed])
    assert store.contains(claim(quote='100', expires_at=D('1')), native_executors=[unconfirmed])


def test_foreign_spender_is_not_enrolled_and_blocks_topology(budget):
    store, _, _ = budget
    foreign = receipt(identifier='ok-rsi-1', controller='ok_rsi', remaining='1', done=False)
    with pytest.raises(PolicyError, match='foreign|topology'):
        store.reserve(claim(quote='10'), native_executors=[foreign])
    with pytest.raises(PolicyError, match='foreign|topology'):
        store.occupied_pairs(native_executors=[foreign])


def test_observation_is_not_a_free_slot():
    policy = UniversePolicy(symbols=EXEC, max_candidates=3)
    empty = select_universe(policy, available=EXEC, tradable=EXEC, scores={},
                            managed=(), pending=(), blocked=(), previous=())
    assert len(empty.entries) == 3
    assert 'D-USDC' not in empty.entries
    held = select_universe(policy, available=EXEC, tradable=EXEC, scores={},
                           managed=('A-USDC', 'B-USDC', 'C-USDC'), pending=(), blocked=(), previous=())
    assert held.entries == ()
    rotated = UniversePolicy(symbols=('D-USDC', 'E-USDC', 'A-USDC'), max_candidates=3)
    still_full = select_universe(rotated, available=EXEC, tradable=rotated.symbols, scores={},
                                 managed=('A-USDC', 'B-USDC', 'C-USDC'), pending=(), blocked=(), previous=())
    assert still_full.entries == ()


def config(pair='A-USDC', **overrides):
    values = dict(
        id=f'modular_spot_{pair.replace("-", "_")}', connector_name='okx', trading_pair=pair,
        execution_enabled=True, account='paper-okx',
        budget={'group': 'desk', 'max_quote': '500', 'max_assets': 3},
        universe={'mode': 'fixed', 'symbols': list(EXEC), 'max_candidates': 3},
        total_amount_quote=D('500') / 3)
    values.update(overrides)
    return ModularSpotControllerConfig(**values)


def attach_store(controller, recorder, Metadata, store):
    controller.runtime.bindings.recorder = lambda: recorder
    controller.runtime.bindings.metadata_model = Metadata
    controller.runtime.bindings.guard_resume = lambda *args, **kwargs: None
    controller.runtime.store = None
    controller.runtime.budget = store
    controller.executors_info = []
    controller.positions_held = []
    return controller.runtime


def proposed_native(rt, cfg, *, now=1200):
    state = DecisionState(fingerprint=rt.fingerprint, candle=1080)
    revised, proposal, reason = plan(
        state, {}, fingerprint=rt.fingerprint, budget=cfg.total_amount_quote, now=now,
        market=MarketFeatures(1140, D('100'), D('29'), D('31'), False, False),
        bid=D('100'), ask=D('100.1'), available_quote=D('200'), amount_step=D('0.001'),
        minimum_amount=D('0.001'), minimum_notional=D('1'), pnl=D('0'),
        signal=SignalPolicy(), acquisition=AcquisitionPolicy(), risk=RiskPolicy(),
        allow_acquisition=True)
    assert reason == 'entry_proposed' and proposal is not None
    native = native_config(cfg, identifier=proposal.identifier, timestamp=proposal.timestamp,
                           price=proposal.price, amount=proposal.amount, cycle=proposal.cycle,
                           stage=proposal.stage)
    return revised, proposal, native


def admit_executor(rt, native, revised):
    rt.pending[native.id] = native.model_copy(deep=True)
    rt.pending_inputs[native.id] = dict(candle=0, rows={})
    rt.input_identity = 'inputs'
    store_decision = NS(load=lambda: (revised, 'raw'), compare_and_swap=lambda *a, **k: True,
                        recorder=NS(get_all_executors=lambda: []))
    rt.ensure_store = lambda: store_decision
    rt.observe = lambda: True
    rt.snapshot = lambda: dict(bid=D('100'), ask=D('100.1'), available_quote=D('200'), pnl=D('0'))
    rt.bindings.fees_permit = lambda controller, request: True
    return store_decision


def test_missing_topology_keeps_acquisitions_disabled():
    controller = ModularSpotController(
        ModularSpotControllerConfig(id='modular_spot_test', connector_name='okx', trading_pair='BTC-USDC',
                                    execution_enabled=True),
        NS(ready=True, time=lambda: 1200), asyncio.Queue())
    assert controller.config.execution_enabled is True
    assert controller.runtime.acquisition_permitted() is False
    assert controller.runtime.reason == 'topology_unproven'
    controller._status = RunnableStatus.RUNNING
    assert controller.determine_executor_actions() == []


def test_fixed_mode_live_slots_ignore_watchlist_membership(tmp_path):
    engine, recorder, Metadata = metadata_engine(tmp_path, 'slots.db')
    policy = BudgetPolicy(group='desk', max_quote=D('500'), max_assets=3)
    store = BudgetStore(recorder, policy, connector='okx', quote='USDC', account='paper-okx',
                        metadata_model=Metadata)
    assert store.reserve(claim(1, '10', 'A-USDC', program='desk'), native_executors=[])
    assert store.reserve(claim(2, '10', 'B-USDC', program='desk'), native_executors=[])
    assert store.reserve(claim(3, '10', 'C-USDC', program='desk'), native_executors=[])
    controller = ModularSpotController(config('D-USDC'), NS(ready=True, time=lambda: 1200), asyncio.Queue())
    rt = attach_store(controller, recorder, Metadata, store)
    rt.inventory = lambda: {}
    assert 'D-USDC' in controller.config.universe.symbols
    assert rt.acquisition_permitted() is False
    assert rt.reason == 'no_budget_slot'
    engine.dispose()


def test_proposal_dispatch_and_executor_admission_all_require_the_reservation(tmp_path):
    engine, recorder, Metadata = metadata_engine(tmp_path, 'wire.db')
    policy = BudgetPolicy(group='desk', max_quote=D('500'), max_assets=3)
    store = BudgetStore(recorder, policy, connector='okx', quote='USDC', account='paper-okx',
                        metadata_model=Metadata)
    cfg = config('A-USDC')
    controller = ModularSpotController(cfg, NS(ready=True, time=lambda: 1200), asyncio.Queue())
    rt = attach_store(controller, recorder, Metadata, store)
    rt.inventory = lambda: {}
    assert rt.acquisition_permitted() is True
    revised, proposal, native = proposed_native(rt, cfg)
    reserved = rt.reservation(native)
    assert bound_fields(reserved)[:5] == ('paper-okx', 'okx', 'desk', native.id, 'A-USDC')
    assert reserved.reserved_quote == native.amount * native.entry_price * (1 + cfg.risk.fee_reserve_fraction)
    assert reserved.reserved_base == native.amount * (1 + cfg.risk.fee_reserve_fraction)
    assert reserved.slot == 'A'
    assert store.reserve(reserved, native_executors=[])
    assert store.contains(reserved, native_executors=[])
    assert rt.acquisition_permitted() is True
    rt.pending[native.id] = native.model_copy(deep=True)
    rt.pending_inputs[native.id] = dict(candle=0, rows={})
    rt.input_identity = 'inputs'
    store_decision = NS(load=lambda: (revised, 'raw'), compare_and_swap=lambda *a, **k: True,
                        recorder=NS(get_all_executors=lambda: []))
    rt.ensure_store = lambda: store_decision
    rt.observe = lambda: True
    rt.snapshot = lambda: dict(bid=D('100'), ask=D('100.1'), available_quote=D('200'), pnl=D('0'))
    rt.bindings.fees_permit = lambda controller, request: True
    controller.market_data_provider = NS(time=lambda: proposal.timestamp)
    assert rt.entry_permission(native, check_funds=False) is True
    other = native_config(cfg, identifier='unreserved', timestamp=proposal.timestamp,
                          price=proposal.price, amount=proposal.amount, cycle=0, stage=0)
    rt.pending[other.id] = other.model_copy(deep=True)
    rt.pending_inputs[other.id] = dict(candle=0, rows={})
    store_decision.load = lambda: (revised.model_copy(update={'claims': (*revised.claims, other.id)}), 'raw')
    assert rt.entry_permission(other, check_funds=False) is False
    assert 'reservation' in rt.reason
    with pytest.raises(PolicyError, match='ungranted'):
        rt.executor_class(other)
    engine.dispose()


def test_reserved_pair_is_admitted_by_unstubbed_acquisition_and_entry_permission(tmp_path):
    engine, recorder, Metadata = metadata_engine(tmp_path, 'self-slot.db')
    policy = BudgetPolicy(group='desk', max_quote=D('500'), max_assets=3)
    store = BudgetStore(recorder, policy, connector='okx', quote='USDC', account='paper-okx',
                        metadata_model=Metadata)
    cfg = config('A-USDC')
    controller = ModularSpotController(cfg, NS(ready=True, time=lambda: 1200), asyncio.Queue())
    rt = attach_store(controller, recorder, Metadata, store)
    rt.inventory = lambda: {}
    assert rt.acquisition_permitted() is True
    revised, proposal, native = proposed_native(rt, cfg)
    reserved = rt.reservation(native)
    assert store.reserve(reserved, native_executors=[])
    assert store.occupied_pairs(native_executors=[]) == ('A-USDC',)
    assert rt.acquisition_permitted() is True
    rt.pending[native.id] = native.model_copy(deep=True)
    assert rt.acquisition_permitted() is True
    assert store.reserve(claim(2, '10', 'B-USDC', program='desk'), native_executors=[])
    assert store.reserve(claim(3, '10', 'C-USDC', program='desk'), native_executors=[])
    assert rt.acquisition_permitted() is True
    watcher = ModularSpotController(config('D-USDC'), NS(ready=True, time=lambda: 1200), asyncio.Queue())
    watch = attach_store(watcher, recorder, Metadata, store)
    watch.inventory = lambda: {}
    assert 'D-USDC' in watcher.config.universe.symbols
    assert watch.acquisition_permitted() is False
    assert watch.reason == 'no_budget_slot'
    admit_executor(rt, native, revised)
    controller.market_data_provider = NS(time=lambda: proposal.timestamp)
    assert rt.entry_permission(native, check_funds=False) is True
    assert rt.executor_class(native) is not None
    engine.dispose()


def test_decision_state_does_not_release_commitments_on_held_inventory():
    state = DecisionState(fingerprint='a' * 64, candle=60, claims=('claim-1',), committed_quote=D('50'))
    held = {'claim-1': InventoryView(D('.2'), D('50'), D('100'), D('0'), True, True, True)}
    revised, reason = reconcile_claims(state, held)
    assert revised.committed_quote == D('50')
    assert revised.claims == ('claim-1',)
    assert reason == 'held_inventory_requires_reconciliation'
