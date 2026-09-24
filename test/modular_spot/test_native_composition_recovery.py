"""Real SQLite restart/CAS evidence for native held-trail metadata, with inert order transport."""
from copy import deepcopy
from decimal import Decimal as D
from types import SimpleNamespace

import pytest
from test_native_composition import FAMILIES, controllers, persist_position
from test_native_lifecycle import recorder as _recorder_fixture

from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.native_recovery import NativeRecoveryStore
from hummingbot.core.data_type.common import TradeType
from hummingbot.model.metadata import Metadata

recorder = _recorder_fixture


def owner():
    return controllers(*FAMILIES[1], use_bag_freeze=False, early_stop_drawdown_pct=D('0'))[0]


def observe(controller, price='102', *, reversal=False):
    controller.market_data_provider.get_price_by_type.return_value = D(price)
    controller.positions_held = [SimpleNamespace(connector_name='okx', trading_pair='BTC-USDC', side=TradeType.BUY,
                                                 amount=D('1'), breakeven_price=D('100'))]
    controller.processed_data = {
        'signal': 0, 'trend_confirmation': {'direction': 'up', 'confidence': .74, 'alignment_score': .74, 'states': {}},
        'signal_state': {'rsi': 81.5 if reversal else 84., 'rsi_prev': 83. if reversal else 82.4,
                         'ema_fast': 101.9 if reversal else 100.6},
    }


def armed_owner(recorder):
    result = owner()
    assert result.determine_executor_actions() == []
    persist_position(recorder, result.config.id)
    observe(result, '101')
    assert result.determine_executor_actions() == []
    observe(result, '102')
    assert result.determine_executor_actions() == []
    assert result._recovery_fault is None
    assert next(iter(result._recovery_trails.values())).peak_price == D('102')
    return result


def test_real_sqlite_restart_restores_native_peak_before_reduction(recorder):
    first = armed_owner(recorder)
    restored = owner()
    observe(restored, '101.6', reversal=True)
    actions = restored.determine_executor_actions()
    assert len(actions) == 1 and actions[0].executor_config.side == TradeType.SELL
    assert restored._recovery_store.previous.pending_reductions[0].prior_peak == D('102')
    assert restored._recovery_store.previous.sequence == first._recovery_store.previous.sequence + 1
    # The native policy deliberately resets its peak on a proposed reduction; the
    # durable pending receipt preserves the prior peak and cannot be silently reused.
    again = owner()
    observe(again, '101.6', reversal=True)
    assert again.determine_executor_actions() == []
    assert 'reduction receipt pending' in again._recovery_hold
    assert again._recovery_trails == {}


def test_missing_checkpoint_with_native_held_inventory_is_hard_hold(recorder, caplog):
    caplog.set_level('WARNING')
    result = owner()
    persist_position(recorder, result.config.id)
    observe(result)
    assert result.determine_executor_actions() == []
    assert 'no authoritative recovery checkpoint' in result._recovery_fault
    assert result._recovery_trails == {}
    assert 'native recovery fault holds actions' in ' '.join(caplog.messages)


def test_corrupt_checkpoint_cannot_reset_native_trailing_peak(recorder):
    first = armed_owner(recorder)
    with recorder.sql_manager.get_new_session() as session:
        with session.begin():
            session.get(Metadata, first._recovery_store.key).value = '{"version":1}'
    result = owner()
    observe(result)
    assert result.determine_executor_actions() == []
    assert 'corrupt' in result._recovery_fault
    assert result._recovery_trails == {}


def test_native_report_database_lag_is_transient_without_rearming(recorder):
    result = owner()
    assert result.determine_executor_actions() == []
    observe(result, '101')
    assert result.determine_executor_actions() == []
    assert 'not reconciled' in result._recovery_hold and result._recovery_fault is None
    assert result._recovery_trails == {}
    persist_position(recorder, result.config.id)
    assert result.determine_executor_actions() == []
    assert result._recovery_hold is None
    assert next(iter(result._recovery_trails.values())).peak_price == D('101')


def test_stale_writer_cas_and_unsanctioned_peak_regression_are_rejected(recorder):
    first = armed_owner(recorder)
    second = owner()
    observe(second)
    stale = NativeRecoveryStore(second, recorder, second._native_source_hash)
    positions = stale.prepare()
    first._recovery_store.persist(first._recovery_store.inventory())
    with pytest.raises(PolicyError, match='concurrent writer'):
        stale.persist(positions)
    native_before = deepcopy(first._recovery_store.previous)
    trail = next(iter(first._recovery_trails.values()))
    trail.peak_price = D('101.5')
    with pytest.raises(PolicyError, match='regressed'):
        first._recovery_store.persist(first._recovery_store.inventory())
    assert first._recovery_store.previous == native_before


def test_persistence_failure_suppresses_action_and_restart_retains_prior_peak(recorder, monkeypatch):
    result = armed_owner(recorder)
    observe(result, '101.6', reversal=True)

    def fail(*args):
        raise OSError('disposable disk failure')

    monkeypatch.setattr(result._recovery_store, 'persist', fail)
    assert result.determine_executor_actions() == []
    assert 'disposable disk failure' in result._recovery_fault
    restored = owner()
    observe(restored, '102')
    assert restored.determine_executor_actions() == []
    assert restored._recovery_fault is None
    assert next(iter(restored._recovery_trails.values())).peak_price == D('102')


def test_restart_inventory_or_configuration_change_requires_explicit_reconciliation(recorder):
    first = armed_owner(recorder)
    persist_position(recorder, first.config.id, amount=D('.5'))
    result = owner()
    observe(result)
    result.positions_held[0].amount = D('.5')
    assert result.determine_executor_actions() == []
    assert 'inventory changed across restart' in result._recovery_fault


def test_transient_inventory_change_rolls_back_uncommitted_native_peak_reset(recorder, monkeypatch):
    from controllers.generic.modular_spot_core.domain import DataUnavailable

    result = armed_owner(recorder)
    observe(result, '101.6', reversal=True)
    persist = result._recovery_store.persist

    def changed_inventory(*args):
        # The native decision already proposed its reduction and reset this peak.
        assert next(iter(result._recovery_trails.values())).peak_price == D('101.6')
        raise DataUnavailable('native held inventory changed during the decision')

    monkeypatch.setattr(result._recovery_store, 'persist', changed_inventory)
    assert result.determine_executor_actions() == []
    assert result._recovery_fault is None
    assert next(iter(result._recovery_trails.values())).peak_price == D('102')
    assert not result._recovery_store.previous.pending_reductions
    monkeypatch.setattr(result._recovery_store, 'persist', persist)
    actions = result.determine_executor_actions()
    assert len(actions) == 1 and actions[0].executor_config.side == TradeType.SELL
    assert result._recovery_hold is None
    assert result._recovery_store.previous.pending_reductions[0].prior_peak == D('102')


def test_empty_book_source_hash_change_reseals_instead_of_faulting(recorder):
    first = owner()
    assert first.determine_executor_actions() == []
    assert first._recovery_fault is None
    previous = first._recovery_store.previous
    assert previous is not None and previous.positions == ()
    second = owner()
    second._native_source_hash = 'b' * 64
    assert second.determine_executor_actions() == []
    assert second._recovery_fault is None
    sealed = second._recovery_store.previous
    assert sealed.source_sha256 == 'b' * 64
    assert sealed.positions == ()
    assert sealed.sequence == previous.sequence + 1


def test_source_hash_change_with_held_inventory_still_requires_reconciliation(recorder):
    armed_owner(recorder)
    result = owner()
    result._native_source_hash = 'b' * 64
    observe(result)
    assert result.determine_executor_actions() == []
    assert 'source/configuration changed' in result._recovery_fault
    assert result._recovery_trails == {}


@pytest.mark.parametrize('observer_mode', ('source_outage', 'paused', 'removed_asset'))
def test_event_observer_hold_does_not_suppress_native_inventory_reduction(recorder, tmp_path, observer_mode):
    """An independent no-order observer cannot replace the native owner's exit path."""
    import asyncio
    import json
    from test.cross_venue_events_v2.test_contracts import evidence, snapshot

    from controllers.generic.cross_venue_event_v2 import CrossVenueEventV2Config, CrossVenueEventV2Controller

    class Clock:
        def time(self):
            return 1780000000

    snapshot_path = tmp_path / 'missing-events-v2.json'
    if observer_mode == 'removed_asset':
        snapshot_path.write_text(json.dumps(snapshot((evidence(kind='PAIR_REMOVAL',
                                                               instrument_pair='BTC-USDC'),)).model_dump(mode='json')))
    observer = CrossVenueEventV2Controller(
        CrossVenueEventV2Config(id='events-v2', snapshot_path=str(snapshot_path)), Clock(), asyncio.Queue())
    if observer_mode == 'paused':
        observer.config.manual_kill_switch = True
    native_owner = armed_owner(recorder)
    observe(native_owner, '101.6', reversal=True)

    asyncio.run(observer.update_processed_data())
    assert observer.processed_data['status'] == (
        'OBSERVER_PAUSED' if observer_mode == 'paused' else
        'NO_ORDER_OBSERVER_V2' if observer_mode == 'removed_asset' else 'UNAVAILABLE')
    if observer_mode == 'removed_asset':
        assert 'negative_event_veto' in observer.processed_data['reasons']
    assert observer.determine_executor_actions() == []
    actions = native_owner.determine_executor_actions()
    assert len(actions) == 1 and actions[0].executor_config.side == TradeType.SELL
    assert native_owner._recovery_store.previous.pending_reductions[0].prior_peak == D('102')

    # Restart holds an unconfirmed reduction independently of the observer.
    restarted_owner = owner()
    observe(restarted_owner, '101.6', reversal=True)
    assert restarted_owner.determine_executor_actions() == []
    assert 'reduction receipt pending' in restarted_owner._recovery_hold
    assert observer.determine_executor_actions() == []
