from types import SimpleNamespace

import pytest

from controllers.generic.momentum_fusion_v2_runtime import runtime


def test_disappearing_executor_keeps_claim_reserved(tmp_path):
    store = runtime.ClaimStore(tmp_path / 'claims.json')
    claim = runtime.Claim(claim_id='c' * 64, opportunity_id='o' * 64, controller_id='momentum_v2_test',
                          trading_pair='BNB-USDC', status='PARTIAL', reserved_quote=10,
                          reserved_base=1, executor_identity='executor', seen_executor_id='executor')
    store.put(claim)
    runtime.reconcile_claims(store, [])
    assert store.get(claim.claim_id).status == 'PARTIAL'


def test_done_flag_without_terminal_receipts_keeps_capacity(tmp_path):
    store = runtime.ClaimStore(tmp_path / 'claims.json')
    claim = runtime.Claim(claim_id='c' * 64, opportunity_id='o' * 64, controller_id='momentum_v2_test',
                          trading_pair='BNB-USDC', status='PARTIAL', reserved_quote=10,
                          reserved_base=1, executor_identity='executor', seen_executor_id='executor')
    store.put(claim)
    runtime.reconcile_claims(store, [SimpleNamespace(id='executor', is_done=True, filled_amount_quote=10)])
    assert store.get(claim.claim_id).status == 'PARTIAL'


def test_claim_updates_are_not_lost_between_store_instances(tmp_path):
    a = runtime.ClaimStore(tmp_path / 'claims.json')
    b = runtime.ClaimStore(tmp_path / 'claims.json')
    for index, store in enumerate((a, b)):
        store.put(runtime.Claim(claim_id=str(index) * 64, opportunity_id=str(index) * 64,
                                controller_id='momentum_v2_test', trading_pair='BNB-USDC',
                                status='RESERVED', reserved_quote=10, reserved_base=1))
    assert len(runtime.ClaimStore(a.path).open_claims()) == 2


@pytest.mark.asyncio
async def test_claim_release_requires_durable_native_executor_and_order_proof(tmp_path, native_evidence):
    from decimal import Decimal as D

    from hummingbot.core.data_type.common import OrderType, TradeType
    from hummingbot.strategy_v2.executors.position_executor.data_types import (
        PositionExecutorConfig,
        TripleBarrierConfig,
    )
    from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
    from hummingbot.strategy_v2.models.base import RunnableStatus
    s, market = native_evidence.session, native_evidence.session._connector
    cfg = PositionExecutorConfig(
        id='native-proof', timestamp=s.current_timestamp,
        controller_id='momentum_v2_fixture', connector_name=market.name,
        trading_pair='BNB-USDC', side=TradeType.BUY, amount=D('.1'), entry_price=D('99'),
        entry_order_policy='single_attempt', triple_barrier_config=TripleBarrierConfig(
            open_order_type=OrderType.LIMIT, open_order_limit_max_age_seconds=1))
    executor = PositionExecutor(s, cfg)
    executor.register_events()
    try:
        executor._status = RunnableStatus.RUNNING
        await executor.control_task()
        await market.drain()
        market.tick(s.current_timestamp + 2)
        await executor.control_task()
        await market.drain()
        await executor.control_task()
        task = executor._entry_expiry_reconciliation_task
        if task is not None:
            await task
        await executor.control_task()
        assert executor.is_closed
        store = runtime.ClaimStore(tmp_path / 'release-claims.json')
        claim = runtime.Claim(claim_id='c' * 64, opportunity_id='o' * 64, controller_id=cfg.controller_id,
                              trading_pair=cfg.trading_pair, status='SUBMITTED_UNACKNOWLEDGED', reserved_quote=10,
                              reserved_base=D('.1'), executor_identity=cfg.id, seen_executor_id=cfg.id)
        store.put(claim)
        with pytest.raises(ValueError):
            store.release(claim.claim_id, native_evidence)
        s.recorder.store_or_update_executor(executor)
        store.release(claim.claim_id, native_evidence)
        restored = runtime.ClaimStore(store.path)
        assert restored.get(claim.claim_id).status == 'RECONCILED_TERMINAL'
        assert restored.get(claim.claim_id).terminal_receipt_sha256
        assert restored.open_claims() == ()
    finally:
        executor.unregister_events()


def test_terminal_flag_cannot_release_without_native_proof(tmp_path):
    store = runtime.ClaimStore(tmp_path / 'claims.json')
    claim = runtime.Claim(claim_id='c' * 64, opportunity_id='o' * 64, controller_id='momentum_v2_test',
                          trading_pair='BNB-USDC', status='RESERVED', reserved_quote=10, reserved_base=1)
    store.put(claim)
    with pytest.raises(ValueError, match='positive native terminal proof'):
        store.put(claim.model_copy(update={'status': 'RECONCILED_TERMINAL'}))


def test_independent_writers_preserve_all_claims(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    path = tmp_path / 'claims.json'

    def write(index):
        runtime.ClaimStore(path).put(runtime.Claim(claim_id=str(index).zfill(8), opportunity_id=str(index).zfill(8),
                                                   controller_id='momentum_v2_test', trading_pair='BNB-USDC',
                                                   status='RESERVED', reserved_quote=1, reserved_base=1))
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(write, range(20)))
    assert len(runtime.ClaimStore(path).open_claims()) == 20
