from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal as D
from types import SimpleNamespace as NS

import pytest
from sqlalchemy import Column, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from controllers.generic.modular_spot_core.program_budget import BudgetPolicy, BudgetStore, Reservation
from controllers.generic.modular_spot_core.domain import PolicyError


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
    store = BudgetStore(recorder, policy, connector='okx', quote='USDC', metadata_model=Metadata)
    yield store
    engine.dispose()


def reservation(i=1, amount='50', pair='BTC-USDC'):
    return Reservation(executor_id=f'claim-{i}', controller_id=f'modular_spot_{i}', trading_pair=pair,
                       reserved_quote=D(amount))


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
                          trading_pair='ETH-USDC', reserved_quote=D('50'))
    with pytest.raises(PolicyError):
        budget.reserve(changed, native_executors=[receipt()])
