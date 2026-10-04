"""Read-only V2 shared-candle entry-readiness projection (rsi_modular custom info and status file)."""
import asyncio
from types import SimpleNamespace as Obj

import pytest
from test_candle_stream import row
from test_native_composition import provider

from controllers.generic import rsi_modular
from controllers.generic.modular_spot_core.candle_stream import CandleStreamClient
from controllers.generic.modular_spot_core.data_reader import DataPolicy, sanitize_stream_error
from hummingbot.strategy.native_observability import controller_observation
from hummingbot.strategy_v2.models.base import RunnableStatus

TOKEN = 'test-only-candle-stream-token-1234567890'
SHARED = DataPolicy(mode='shared', transport='stream', origin='http://127.0.0.1:8093', token_file='/unused')
T0 = 600000


def live_v2(data=SHARED):
    config_class = rsi_modular.get_config_class('ok_rsi')
    config = config_class(id='modular_spot_v2_btc', profile='ok_rsi', trading_pair='BTC-USDC',
                          strict_hmm_mode=False, data=data)
    controller = config.get_controller_class()(config, provider(), asyncio.Queue())
    controller._status = RunnableStatus.RUNNING
    return controller


def at(controller, now):
    controller.shared_candles.delegate.time.return_value = now


def attach_stream(controller):
    reader = controller.shared_candles
    stream = CandleStreamClient(SHARED.origin, SHARED.token_file, (reader.spec,), clock=reader.delegate.time)
    reader.pool.client = stream
    stream.begin_session()
    return stream


def snapshot_message(now, history):
    return dict(v=1, type='snapshot', epoch='live-epoch', published_at=now, pair='BTC-USDC', sequence=1,
                received_at=now, rows=[row(t) for t in range(now - history * 60, now, 60)])


def candle_message(now, sequence):
    return dict(v=1, type='candle', epoch='live-epoch', published_at=now, pair='BTC-USDC', sequence=sequence,
                received_at=now, row=row(now - 60))


def heartbeat_message(now, sequence):
    return dict(v=1, type='heartbeat', epoch='live-epoch', published_at=now,
                states={'BTC-USDC': dict(ready=True, sequence=sequence)})


def readiness(controller):
    projection = controller.get_custom_info()['candle_readiness']
    assert projection['schema'] == 'shared_candle_readiness.v1'
    assert projection['clock'] == 'controller_market_data_provider_time'
    assert projection['data_mode'] == 'shared' and projection['transport'] == 'stream'
    [dataset] = projection['datasets']
    # The projection must report exactly the BUY gate's own result, never a recomputation.
    assert dataset['entry_ready'] is controller.shared_candles.entry_ready()
    return projection, dataset


def assert_null_reasons(dataset):
    for name, value in dataset.items():
        if name in {'entry_ready_reason', 'entry_ready_detail', 'stream_error', 'dataset_error', 'null_reasons'}:
            continue
        if value is None:
            assert dataset['null_reasons'].get(name), f'{name} is null without an explicit reason'


def test_reader_not_started_is_explicit_and_fail_visible():
    controller = live_v2()
    projection, dataset = readiness(controller)
    assert projection['status'] == 'available'
    assert projection['observed_at'] == T0
    assert dataset['trading_pair'] == 'BTC-USDC' and dataset['interval'] == '1m'
    assert dataset['entry_ready'] is False
    assert dataset['entry_ready_reason'] == 'reader_not_started'
    assert dataset['client_state'] == 'not_started'
    assert dataset['replica_sequence'] is None and dataset['last_closed_candle_open_time'] is None
    assert dataset['heartbeat_age_seconds'] is None
    assert dataset['null_reasons']['replica_sequence'] == 'reader_not_started'
    assert_null_reasons(dataset)


def test_ready_replica_exports_sequence_candle_and_clock_ages():
    controller = live_v2()
    history = controller.shared_candles.spec.history
    stream = attach_stream(controller)
    at(controller, T0)
    stream.apply(snapshot_message(T0, history))
    at(controller, T0 + 60)
    stream.apply(candle_message(T0 + 60, 2))
    at(controller, T0 + 63)
    stream.apply(heartbeat_message(T0 + 63, 2))
    at(controller, T0 + 66)
    revision = stream.replica.revision
    projection, dataset = readiness(controller)
    assert stream.replica.revision == revision  # Pure projection: no replica mutation.
    assert dataset['entry_ready'] is True
    assert dataset['entry_ready_reason'] is None and dataset['entry_ready_detail'] is None
    assert dataset['client_state'] == 'connected'
    assert dataset['last_closed_candle_open_time'] == T0
    assert dataset['last_closed_candle_close_time'] == T0 + 60
    assert dataset['seed_candle_open_time'] == T0 - 60
    assert dataset['replica_sequence'] == 2
    assert dataset['replica_revision'] == revision
    assert dataset['replica_epoch'] == 'live-epoch'
    assert dataset['source_published_at'] == T0 + 63
    assert dataset['source_publication_age_seconds'] == 3
    assert dataset['receive_age_seconds'] == 6
    assert dataset['heartbeat_age_seconds'] == 3
    assert dataset['stream_error'] is None and dataset['dataset_error'] is None
    assert dataset['stream_sessions'] == 1 and dataset['stream_failures'] == 0
    assert_null_reasons(dataset)


def test_seed_only_replica_is_not_entry_ready_with_reason():
    controller = live_v2()
    stream = attach_stream(controller)
    at(controller, T0)
    stream.apply(snapshot_message(T0, controller.shared_candles.spec.history))
    at(controller, T0 + 2)
    _, dataset = readiness(controller)
    assert dataset['entry_ready'] is False
    assert dataset['entry_ready_reason'] == 'awaiting_post_seed_candle'
    assert dataset['heartbeat_age_seconds'] is None
    assert dataset['null_reasons']['heartbeat_age_seconds'] == 'no_heartbeat_received'
    assert_null_reasons(dataset)


def test_stale_transport_reports_ages_and_gate_reason():
    controller = live_v2()
    stream = attach_stream(controller)
    at(controller, T0)
    stream.apply(snapshot_message(T0, controller.shared_candles.spec.history))
    at(controller, T0 + 60)
    stream.apply(candle_message(T0 + 60, 2))
    at(controller, T0 + 90)
    _, dataset = readiness(controller)
    assert dataset['entry_ready'] is False
    assert dataset['entry_ready_reason'] == 'candles_unavailable'
    assert dataset['entry_ready_detail'] == 'candle transport is stale'
    assert dataset['source_publication_age_seconds'] == 30
    assert dataset['receive_age_seconds'] == 30
    assert dataset['replica_sequence'] == 2


def test_upstream_dataset_error_and_disconnect_are_sanitized_and_explicit():
    controller = live_v2()
    stream = attach_stream(controller)
    at(controller, T0)
    stream.apply(snapshot_message(T0, controller.shared_candles.spec.history))
    stream.apply(dict(v=1, type='status', epoch='live-epoch', published_at=T0, pair='BTC-USDC', sequence=2,
                      ready=False, reason='upstream disconnected'))
    _, dataset = readiness(controller)
    assert dataset['entry_ready'] is False
    assert dataset['entry_ready_reason'] == 'dataset_unavailable'
    assert dataset['dataset_error'] == 'upstream disconnected'
    assert dataset['replica_sequence'] is None
    assert dataset['null_reasons']['replica_sequence'] == 'dataset_not_synchronized'

    stream.error = f'ClientResponseError: 401 Authorization: Bearer {TOKEN}'
    stream.failures = 1
    stream.replica.disconnect()
    _, dataset = readiness(controller)
    assert dataset['entry_ready'] is False
    assert dataset['entry_ready_reason'] == 'replica_disconnected'
    assert dataset['client_state'] == 'disconnected'
    assert dataset['stream_failures'] == 1
    assert TOKEN not in str(dataset)
    assert dataset['stream_error'] == 'ClientResponseError: 401 Authorization: Bearer <redacted>'


def test_error_sanitizer_is_bounded_and_redacts_token_shapes():
    assert sanitize_stream_error(None) is None
    assert TOKEN not in sanitize_stream_error(f'failed {TOKEN} again')
    assert len(sanitize_stream_error('x ' * 1000)) <= 256
    assert sanitize_stream_error(RuntimeError('secret')) == 'RuntimeError'


def test_native_candles_are_reported_not_applicable():
    projection = live_v2(DataPolicy()).get_custom_info()['candle_readiness']
    assert projection['status'] == 'not_applicable'
    assert projection['reason'] == 'native_candles_not_replica_gated'
    assert projection['datasets'] == []


def test_projection_failure_is_visible_and_never_raises():
    controller = live_v2()
    controller.shared_candles.pool.readiness = lambda *args, **kwargs: (_ for _ in ()).throw(KeyError('boom'))
    projection = controller.get_custom_info()['candle_readiness']
    assert projection['status'] == 'unavailable'
    assert projection['reason'] == 'projection_error: KeyError'
    assert projection['datasets'] == []


def test_status_file_observation_allowlists_candle_readiness():
    controller = live_v2()
    stream = attach_stream(controller)
    at(controller, T0)
    stream.apply(snapshot_message(T0, controller.shared_candles.spec.history))
    at(controller, T0 + 60)
    stream.apply(candle_message(T0 + 60, 2))
    result = controller_observation({'controller_id': 'modular_spot_v2_btc', 'controller': controller})
    assert result['observation_status'] == 'available'
    observed = result['custom_info']['candle_readiness']
    expected = controller.get_custom_info()['candle_readiness']
    assert observed == expected
    assert observed['datasets'][0]['entry_ready'] is True
    assert 'trailing_policy' in result['custom_info']


@pytest.mark.parametrize('change', [
    {'datasets': [{'entry_ready': 'yes'}]},
    {'status': 'ready'},
    {'datasets': [{'stream_error': 'x' * 300}]},
    {'datasets': [{'replica_sequence': -1}]},
])
def test_malformed_candle_readiness_is_contained_to_its_section(change):
    base = {'schema': 'shared_candle_readiness.v1', 'status': 'available', 'reason': None, 'data_mode': 'shared',
            'transport': 'stream', 'observed_at': T0, 'clock': 'controller_market_data_provider_time',
            'datasets': [{'trading_pair': 'BTC-USDC', 'interval': '1m', 'entry_ready': False}]}
    payload = {**base, **{k: v for k, v in change.items() if k != 'datasets'}}
    if 'datasets' in change:
        payload['datasets'] = [{**base['datasets'][0], **change['datasets'][0]}]
    custom = {'candle_readiness': payload, 'trailing_policy': {'policy': 'adaptive_continuous'}}
    controller = Obj(get_custom_info=lambda: custom, config=Obj(connector_name='okx'))
    result = controller_observation({'controller_id': 'c1', 'controller': controller})
    assert result['observation_status'] == 'available'
    assert result['custom_info']['trailing_policy'] == {'policy': 'adaptive_continuous'}
    assert result['custom_info']['candle_readiness'] == {
        'schema': 'shared_candle_readiness.v1', 'status': 'unavailable',
        'reason': 'invalid_candle_readiness_projection', 'datasets': []}
    assert TOKEN not in str(result)


def test_unknown_candle_readiness_fields_are_dropped_by_the_allowlist():
    payload = {'schema': 'shared_candle_readiness.v1', 'status': 'available', 'reason': None,
               'data_mode': 'shared', 'transport': 'stream', 'observed_at': T0,
               'clock': 'controller_market_data_provider_time', 'token_file': '/secret/path',
               'datasets': [{'trading_pair': 'BTC-USDC', 'interval': '1m', 'entry_ready': True,
                             'injected_secret': TOKEN, 'null_reasons': {'heartbeat_age_seconds': 'no_heartbeat_received'}}]}
    controller = Obj(get_custom_info=lambda: {'candle_readiness': payload}, config=Obj(connector_name='okx'))
    observed = controller_observation({'controller_id': 'c1', 'controller': controller})['custom_info']['candle_readiness']
    assert observed['status'] == 'available'
    assert observed['datasets'] == [{'trading_pair': 'BTC-USDC', 'interval': '1m', 'entry_ready': True,
                                     'null_reasons': {'heartbeat_age_seconds': 'no_heartbeat_received'}}]
    assert TOKEN not in str(observed) and 'token_file' not in observed


def _replica_client(controller, *, rows, published, now, sequence=1):
    from controllers.generic.modular_spot_core.candle_data import Candle, CandleSnapshot, Replica
    spec = controller.shared_candles.spec
    replica = Replica((spec,))
    replica.begin_session()
    candles = tuple(Candle(open_time=t, open='100', high='101', low='99', close='100', volume='5')
                    for t in rows)
    replica.accept(CandleSnapshot(dataset=spec, epoch='parity', sequence=sequence, published_at=published,
                                  rows=candles), now=now)
    controller.shared_candles.pool.client = Obj(replica=replica, error=None, dataset_errors={})
    return replica


def _actual_gate(controller):
    from controllers.generic.modular_spot_core.domain import DataUnavailable, PolicyError
    reader = controller.shared_candles
    try:
        reader.get_candles_df('okx', reader.spec.trading_pair, '1m', reader.spec.history)
        return None
    except (DataUnavailable, PolicyError) as error:
        return str(error)


@pytest.mark.parametrize('scenario', ['not_started', 'ready_seed', 'ready_post_seed', 'transport_stale',
                                      'candle_stale', 'future', 'warming', 'disconnected'])
def test_cheap_frame_gate_mirror_matches_actual_buy_gate(scenario):
    controller = live_v2()
    history = controller.shared_candles.spec.history
    full = list(range(T0 - history * 60, T0, 60))
    if scenario != 'not_started':
        rows = full[history // 2:] if scenario == 'warming' else full
        replica = _replica_client(controller, rows=rows, published=T0, now=T0)
        if scenario == 'ready_post_seed':
            replica.seed_bars[controller.shared_candles.spec.key] = T0 - 120
        if scenario == 'disconnected':
            replica.disconnect()
    now = {'transport_stale': T0 + 11, 'future': T0 - 1}.get(scenario, T0 + 1)
    if scenario == 'candle_stale':
        replica.max_transport_age = 10_000
        now = T0 + controller.shared_candles._max_age() + 1
    at(controller, now)
    _, dataset = readiness(controller)
    actual = _actual_gate(controller)
    expected_detail = None if dataset['entry_ready'] else actual
    assert dataset['entry_ready_detail'] == expected_detail
    assert controller.shared_candles._frame_gate_detail(now) == actual
    assert dataset['entry_ready'] is (scenario == 'ready_post_seed')
