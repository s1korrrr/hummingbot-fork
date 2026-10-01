from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal as D
from types import SimpleNamespace as NS

import pytest
from sqlalchemy import Column, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.program_budget import BudgetPolicy, BudgetStore, Reservation


@pytest.fixture
def budget(tmp_path):
    base = declarative_base()

    class Metadata(base):
        __tablename__ = 'Metadata'
        key = Column(Text, primary_key=True)
        value = Column(Text, nullable=False)
    engine = create_engine('sqlite:///' + str(tmp_path / 'budget.db'), connect_args={'timeout': 5})
    base.metadata.create_all(engine)
    recorder = NS(config_file_path='native-strategy.yml', sql_manager=NS(get_new_session=sessionmaker(bind=engine)))
    policy = BudgetPolicy(group='five-assets', max_quote=D('100'), max_assets=2)
    store = BudgetStore(recorder, policy, connector='okx', quote='USDC', account='paper-okx',
                        metadata_model=Metadata)
    yield store
    engine.dispose()


def reservation(i=1, amount='50', pair='BTC-USDC'):
    return Reservation(executor_id=f'claim-{i}', controller_id=f'modular_spot_{i}', trading_pair=pair,
                       reserved_quote=D(amount), reserved_base=D('1'), account='paper-okx',
                       connector='okx', program='five-assets', slot=pair.split('-')[0], expires_at=D('0'))


def test_queued_unacknowledged_claims_count_against_capital(budget):
    assert budget.reserve(reservation(), native_executors=[])
    assert budget.reserve(reservation(2), native_executors=[])
    assert not budget.reserve(reservation(3, '1'), native_executors=[])
    assert budget.contains(reservation())


def test_same_claim_is_idempotent_but_mutation_is_rejected(budget):
    assert budget.reserve(reservation(), native_executors=[])
    assert budget.reserve(reservation(), native_executors=[])
    with pytest.raises(PolicyError):
        budget.reserve(reservation(amount='49'), native_executors=[])


def test_distinct_assets_cap_includes_pending_inventory(budget):
    assert budget.reserve(reservation(1, '10', 'BTC-USDC'), native_executors=[])
    assert budget.reserve(reservation(2, '10', 'ETH-USDC'), native_executors=[])
    assert not budget.reserve(reservation(3, '10', 'SOL-USDC'), native_executors=[])


def receipt(*, remaining='0', done=True, owner='executor', controller='modular_spot_1'):
    return NS(id='claim-1', controller_id=controller, is_done=done, is_active=not done,
              config=NS(controller_id=controller, connector_name='okx', trading_pair='BTC-USDC'),
              custom_info={'execution_receipt': {'remaining_owned_base': remaining, 'accounting_owner': owner}})


def test_only_authoritative_flat_terminal_receipt_releases_capital(budget):
    assert budget.reserve(reservation(amount='100'), native_executors=[])
    for result in (receipt(remaining='.1'), receipt(done=False), receipt(owner='held_position')):
        assert not budget.reserve(reservation(2, '1'), native_executors=[result])
    assert budget.reserve(reservation(2, '100'), native_executors=[receipt()])
    assert not budget.contains(reservation(amount='100'))


def test_receipt_owner_mismatch_is_not_proof_of_flatness(budget):
    assert budget.reserve(reservation(amount='100'), native_executors=[])
    with pytest.raises(PolicyError):
        budget.reserve(reservation(2, '1'), native_executors=[receipt(controller='other')])


def test_real_sqlite_concurrent_claims_cannot_overcommit(budget):
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda i: budget.reserve(reservation(i, '25'), native_executors=[]), range(1, 11)))
    assert sum(results) == 4


def test_an_already_terminal_executor_cannot_be_readmitted(budget):
    assert not budget.reserve(reservation(), native_executors=[receipt()])


def test_missing_group_checkpoint_with_native_history_is_not_new_capital(budget):
    with pytest.raises(PolicyError):
        budget.reserve(reservation(2, '100'), native_executors=[receipt(remaining='.3', done=False)])


def test_late_inventory_after_released_claim_blocks_other_group_members(budget):
    assert budget.reserve(reservation(amount='100'), native_executors=[])
    assert budget.reserve(reservation(2, '50'), native_executors=[receipt()])
    with pytest.raises(PolicyError):
        budget.reserve(reservation(3, '50'), native_executors=[receipt(remaining='.1')])


def test_existing_group_member_cannot_switch_instrument_ownership(budget):
    assert budget.reserve(reservation(amount='50'), native_executors=[])
    changed = Reservation(executor_id='new-claim', controller_id='modular_spot_1',
                          trading_pair='ETH-USDC', reserved_quote=D('50'), reserved_base=D('1'),
                          account='paper-okx', connector='okx', program='five-assets', slot='ETH',
                          expires_at=D('0'))
    with pytest.raises(PolicyError):
        budget.reserve(changed, native_executors=[receipt()])


def _held(i=1, remaining='.5', done=True, pair='BTC-USDC'):
    return NS(id=f'claim-{i}', controller_id=f'modular_spot_{i}', is_done=done, is_active=not done,
              config=NS(controller_id=f'modular_spot_{i}', connector_name='okx', trading_pair=pair),
              custom_info={'execution_receipt': {'remaining_owned_base': remaining, 'accounting_owner': 'held_position'}})


def _sell(ident, *, controller='modular_spot_1', active=True, bag=True, pair='BTC-USDC'):
    level = 'modular_spot:v1:bag:recovery:pullback' if bag else 'modular_spot:v1:reduce:1'
    return NS(id=ident, controller_id=controller, is_done=not active, is_active=active, custom_info={},
              config=NS(type='order_executor', side=NS(name='SELL'), position_action=NS(name='CLOSE'),
                        level_id=level, min_sell_price=D('101') if bag else None,
                        controller_id=controller, connector_name='okx', trading_pair=pair))


def test_held_bag_keeps_its_reservation_until_a_durable_release(budget):
    assert budget.reserve(reservation(amount='100'), native_executors=[])
    raw_v1 = budget._load()
    assert '"schema_version":1' in raw_v1 and 'released' not in raw_v1  # legacy bytes unchanged
    held = _held()
    assert not budget.reserve(reservation(2, '1', 'ETH-USDC'), native_executors=[held])  # capital stays reserved
    sold = _sell('bag-sell-1', active=False)
    assert budget.release_held('modular_spot_1', ['claim-1', 'bag-sell-1'], native_executors=[held, sold]) == {
        'claim-1', 'bag-sell-1'}
    assert '"schema_version":2' in budget._load() and budget.released() == {'claim-1', 'bag-sell-1'}
    # Durable for this member and every other member, with no per-call proof.
    assert budget.occupied_pairs(native_executors=[held, sold]) == ()
    assert budget.reserve(reservation(2, '100', 'ETH-USDC'), native_executors=[held, sold])
    assert budget.contains(reservation(2, '100', 'ETH-USDC'), native_executors=[held, sold])
    assert budget.occupied_pairs(native_executors=[held, sold]) == ('ETH-USDC',)
    # Idempotent; a released executor can never be reserved again.
    assert budget.release_held('modular_spot_1', ['claim-1'], native_executors=[held]) == {'claim-1', 'bag-sell-1'}
    assert not budget.reserve(reservation(amount='1'), native_executors=[held])


def test_active_receipt_reappearing_after_held_release_holds_program_budget(budget):
    assert budget.reserve(reservation(amount='100'), native_executors=[])
    held = _held()
    sold = _sell('bag-sell-1', active=False)
    budget.release_held('modular_spot_1', ['claim-1', 'bag-sell-1'], native_executors=[held, sold])
    assert budget.occupied_pairs(native_executors=[held, sold]) == ()

    for reappeared in (_held(done=False), _sell('bag-sell-1', active=True)):
        with pytest.raises(PolicyError, match='unreserved or reappearing'):
            budget.occupied_pairs(native_executors=[reappeared])
        with pytest.raises(PolicyError, match='unreserved or reappearing'):
            budget.reserve(reservation(2, '100', 'ETH-USDC'), native_executors=[reappeared])


@pytest.mark.parametrize('target', [
    _held(done=False),  # still active
    _held(2),  # another member's executor
    NS(id='claim-1', controller_id='modular_spot_1', is_done=True, is_active=False,
       config=NS(controller_id='modular_spot_1', connector_name='okx', trading_pair='BTC-USDC'),
       custom_info={'execution_receipt': {'remaining_owned_base': '.5', 'accounting_owner': 'executor'}}),
    _sell('claim-1', active=False, bag=False),  # a reduce sell is not a bag sell
])
def test_release_rejects_anything_but_this_members_terminal_held_or_bag_sell(budget, target):
    assert budget.reserve(reservation(amount='50'), native_executors=[])
    assert budget.reserve(reservation(2, '50', 'ETH-USDC'), native_executors=[])
    with pytest.raises(PolicyError):
        budget.release_held('modular_spot_1', [target.id], native_executors=[target])
    with pytest.raises(PolicyError):
        budget.release_held('modular_spot_1', ['unknown'], native_executors=[target])
    with pytest.raises(PolicyError):
        budget.release_held('modular_spot_9', ['claim-1'], native_executors=[_held()])
    assert budget.released() == frozenset()


def test_tampered_release_checkpoint_fails_closed(budget):
    import json

    assert budget.reserve(reservation(amount='50'), native_executors=[])
    good = json.loads(budget._load())
    for bad in ({**good, 'schema_version': 2, 'released': []}, {**good, 'schema_version': 2, 'released': ['b', 'a']},
                {**good, 'schema_version': 2, 'released': ['claim-1']}, {**good, 'released': ['x']}):
        with pytest.raises(PolicyError):
            budget._parse(json.dumps(bad))


def test_member_bag_sell_is_capital_neutral_but_occupies_its_pair_while_live(budget):
    assert budget.reserve(reservation(amount='50'), native_executors=[])
    live = _sell('bag-sell-1')
    assert budget.reserve(reservation(2, '50', 'BTC-USDC'), native_executors=[live])
    assert budget.occupied_pairs(native_executors=[live]) == ('BTC-USDC',)
    buy = NS(id='bare-buy', controller_id='modular_spot_1', is_done=False, is_active=True,
             config=NS(type='order_executor', side=NS(name='BUY'), position_action=NS(name='OPEN'),
                       controller_id='modular_spot_1', connector_name='okx', trading_pair='BTC-USDC'),
             custom_info={})
    with pytest.raises(PolicyError):
        budget.contains(reservation(), native_executors=[buy])


def test_reduce_only_exemption_does_not_touch_legacy_sellers(budget):
    assert budget.reserve(reservation(amount='50'), native_executors=[])
    # A sell_only controller's live reduce sell keeps occupying its pair (impulse concentration).
    reduce = _sell('reduce-1', controller='modular_spot_seller', bag=False, pair='ETH-USDC')
    assert 'ETH-USDC' in budget.occupied_pairs(native_executors=[reduce])
    # A foreign live SELL on the same connector quote is still an unproven topology.
    foreign = _sell('ok-rsi-exit', controller='ok-rsi-bnb', bag=True)
    with pytest.raises(PolicyError, match='foreign'):
        budget.contains(reservation(amount='50'), native_executors=[foreign])
    # An unreserved member SELL that is not a bag recovery sell is not capital-neutral.
    with pytest.raises(PolicyError, match='unreserved'):
        budget.contains(reservation(amount='50'), native_executors=[_sell('claim-x', bag=False)])
