"""T8: Shared Market Context bound to Meridian v3 in observe-only mode.

Unit checks cover the policy and each observation state. The paired lifecycle runs
prove that observe mode never changes a decision, grant, order or exit: the same
native paper script with context off and observe (available, warming, missing,
stale, erroring, timing out) yields identical actions, grants and balances.
"""
import asyncio
import logging
import tempfile
import warnings
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v3.lifecycle_harness import START, Scenario, creates, rebound
from test.meridian_v3.test_unified_runtime import unified
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

import pytest

from controllers.generic.meridian_v3_unified_core import market_context as mc
from controllers.generic.meridian_v3_unified_core.config import MeridianUnifiedConfig
from hummingbot.core.data_type.common import TradeType
from hummingbot.data_feed import market_context_client as mcc
from hummingbot.strategy_v2.market_context import contracts as c

M = 60_000
T = START * 1000 - START * 1000 % M
SHA = '9' * 64
STREAM = 'okx-usdc-basket5'
BTC, ETH = 'BTC-USDC', 'ETH-USDC'
REQUIREMENT = dict(consumer='execution', schema_major=1, stream_id=STREAM, venue='okx', numeraire='USDC',
                   config_digest=SHA, universe_hash=SHA, max_age_ms=3_600_000, min_count_coverage='0.95',
                   min_weight_coverage='0.95', mandatory_features=('market_return_eq_h1',),
                   allowed_source_kinds=('observed',))
OBSERVE = {'mode': 'observe', 'origin': 'http://127.0.0.1:8000', 'credentials_file': '/run/context/credentials',
           'requirement': REQUIREMENT}


def snapshot(*, cutoff=T, status='READY', available=3_000, expires=2 * 3_600_000):
    valid = status != 'WARMING'
    feature = c.NamedFeature(name='market_return_eq_h1', value=0.001 if valid else None, unit='return_fraction',
                             horizon_minutes=1, status='VALID' if valid else 'WARMUP_INCOMPLETE',
                             reasons=() if valid else ('WARMUP_INCOMPLETE',), valid_count=5, expected_count=5,
                             valid_weight_fraction=1.0, model_id=None, input_available_at_ms=cutoff + 500)
    content = c.SnapshotContent(
        venue='okx', numeraire='USDC', cutoff_ms=cutoff, max_input_available_at_ms=cutoff + 500,
        provenance=c.Provenance(code_digest='1' * 64, config_digest=SHA, universe_hash=SHA,
                                input_manifest_digest='2' * 64, model_digest='3' * 64,
                                serializer_version=c.SERIALIZER_VERSION, artifact_refs=()),
        coverage=c.Coverage(expected_count=5, valid_count=5, valid_weight_fraction=1.0, missing=()),
        status=status, reasons=() if status == 'READY' else ('WARMUP_INCOMPLETE',), market=(feature,), assets=())
    return c.seal_snapshot(content, stream_id=STREAM, epoch='e1', sequence=1, source_kind='observed',
                           available_at_ms=cutoff + available, expires_at_ms=cutoff + expires, supersedes=None)


def stream_status(value):
    return c.ContextStreamStatus(stream_id=STREAM, epoch=value.epoch, lease_generation=1, sequence=value.sequence,
                                 snapshot_id=value.snapshot_id, status=value.status,
                                 available_at_ms=value.available_at_ms, expires_at_ms=value.expires_at_ms,
                                 authority='observation_only').model_dump_json().encode()


class NoTransport:
    """Any network read fails the test; installed snapshots come from recorded bytes."""

    async def status(self):
        raise AssertionError('unexpected context read')

    latest = snapshot = status


class HangingTransport:
    async def _hang(self, *args):
        await asyncio.sleep(3600)

    status = latest = snapshot = _hang


def real_client(value=None, *, transport=None, receipt=None):
    client = mcc.MarketContextClient(transport or NoTransport(), stream_id=STREAM, clock_ms=lambda: T,
                                     poll_seconds=0.01, max_backoff_seconds=0.02)
    if value is not None:
        client.accept_recorded(c.serialize_snapshot(value), received_at_ms=receipt or value.available_at_ms,
                               status=stream_status(value))
    return client


class ExplodingClient:
    task = None
    counters = {}

    def start(self):
        pass

    def select(self, requirement, clock):
        raise RuntimeError('boom')

    async def close(self):
        pass


class StartedTask:
    def done(self):
        return False

    def cancel(self):
        pass


def started(client):
    """Installed snapshot only; the refresh task is a pending future that never reads the network."""
    try:
        client.task = asyncio.get_running_loop().create_future()
    except RuntimeError:
        client.task = StartedTask()
    return client


def observer(client=None, *, factory=None, monotonic=None):
    policy = mc.MarketContextPolicy(**OBSERVE)
    logger = logging.getLogger('test.meridian.market_context')
    return mc.MarketContextObserver(policy, clock_ms=lambda: T, logger=logger,
                                    client_factory=factory or (lambda p, clock: client),
                                    monotonic=monotonic or (lambda: 0.0))


# ---------------------------------------------------------------------- policy
def test_default_is_off_and_constructs_nothing():
    config = unified()
    assert config.market_context.mode == 'off'
    calls = []
    watcher = mc.MarketContextObserver(config.market_context, clock_ms=lambda: T, logger=logging.getLogger('x'),
                                       client_factory=lambda *a: calls.append(a))
    assert watcher.after_cycle(object()) is None and calls == [] and watcher.client is None
    assert watcher.custom_info() == {'mode': 'off', 'authority': 'observation_only'}


@pytest.mark.parametrize('mode', ['gate', 'required', 'optional', 'enforce'])
def test_no_gating_mode_is_accepted(mode):
    with pytest.raises(ValueError, match='not authorized; only off or observe'):
        mc.MarketContextPolicy(**{**OBSERVE, 'mode': mode})
    with pytest.raises(ValueError, match='not authorized'):
        unified(market_context={**OBSERVE, 'mode': mode})


@pytest.mark.parametrize('change', [
    {'mode': 'off'},  # off with a binding is ambiguous
    {'origin': None}, {'credentials_file': None}, {'requirement': None},
    {'origin': 'http://10.0.0.5:8000'},  # remote plain HTTP
    {'origin': 'http://127.0.0.1:8000/path'},
    {'credentials_file': 'relative/credentials'},
    {'requirement': {**REQUIREMENT, 'consumer': 'research'}},
    {'requirement': {**REQUIREMENT, 'allowed_source_kinds': ('observed', 'synthetic')}},
    {'unknown': 1},
])
def test_incomplete_or_unsafe_observe_binding_is_refused(change):
    with pytest.raises(ValueError):
        mc.MarketContextPolicy(**{**OBSERVE, **change})


def test_observe_mode_is_not_part_of_the_behavioural_seal():
    sealed = unified(execution_enabled=True)
    observed = MeridianUnifiedConfig(**{**sealed.model_dump(mode='json'), 'market_context': OBSERVE})
    assert observed.recipe_binding == sealed.recipe_binding
    assert observed.seal() == sealed.seal()


# ---------------------------------------------------------------------- observation states
def test_available_context_records_its_causal_identity():
    value = snapshot()
    watcher = observer(started(real_client(value, receipt=T + 3_200)))
    record = watcher.observe(T + 60_000)
    assert (record['state'], record['accepted'], record['reasons']) == ('AVAILABLE', True, [])
    identity = record['snapshot']
    assert identity['snapshot_id'] == value.snapshot_id and identity['epoch'] == 'e1' and identity['sequence'] == 1
    assert identity['feature_version']['code_digest'] == '1' * 64
    assert identity['feature_version']['config_digest'] == SHA
    assert (identity['cutoff_ms'], identity['available_at_ms'], identity['receipt_ms']) == (T, T + 3_000, T + 3_200)
    assert record['effective_available_at_ms'] == T + 3_200 <= record['decision_ms']
    assert record['age_ms'] == 60_000
    assert record['market'] == [{'name': 'market_return_eq_h1', 'value': 0.001, 'unit': 'return_fraction',
                                 'horizon_minutes': 1, 'status': 'VALID'}]


def test_context_not_yet_received_at_decision_time_is_not_used():
    value = snapshot()
    record = observer(started(real_client(value, receipt=T + 3_200))).observe(T + 3_100)
    assert (record['state'], record['reasons'], record['market']) == ('UNAVAILABLE', ['CONTEXT_FUTURE'], None)


def test_missing_context_is_unavailable_not_calm():
    record = observer(started(real_client())).observe(T)
    assert (record['state'], record['reasons'], record['snapshot'], record['market']) == (
        'UNAVAILABLE', ['CONTEXT_UNAVAILABLE'], None, None)


def test_expired_context_is_stale_with_identity_but_no_values():
    value = snapshot(expires=65_000)
    record = observer(started(real_client(value))).observe(T + 70_000)
    assert (record['state'], record['reasons'], record['market']) == ('STALE', ['CONTEXT_EXPIRED'], None)
    assert record['snapshot']['snapshot_id'] == value.snapshot_id


def test_warming_context_is_warming_with_no_values():
    record = observer(started(real_client(snapshot(status='WARMING')))).observe(T + 60_000)
    assert (record['state'], record['reasons'], record['market']) == ('WARMING', ['FEATURE_UNAVAILABLE'], None)
    assert record['snapshot']['envelope_status'] == 'WARMING'


def test_client_errors_become_unavailable_and_never_raise():
    assert observer(ExplodingClient()).observe(T)['detail'] == 'observer_error:RuntimeError'

    def factory(policy, clock):
        raise OSError('no route')
    assert observer(factory=factory).observe(T)['detail'] == 'observer_error:OSError'

    class Done:
        def done(self):
            return True
    stopped = real_client()
    stopped.task = Done()
    assert observer(stopped).observe(T)['detail'] == 'client_task_stopped'

    class Portfolio:
        last_target = None

        def now_ms(self):
            raise ValueError('clock')
    record = observer(real_client()).after_cycle(Portfolio())
    assert (record['state'], record['detail']) == ('UNAVAILABLE', 'decision_clock_error:ValueError')


def test_start_without_a_loop_is_unavailable():
    # A controller tick always runs on the loop; outside one, start fails and is reported, not raised.
    client = real_client()
    with warnings.catch_warnings(record=True) as emitted:
        warnings.simplefilter('always')
        record = observer(client).observe(T)
    assert (record['state'], record['detail']) == ('UNAVAILABLE', 'observer_error:RuntimeError')
    assert client.task is None and client._wake is None
    assert not [w for w in emitted if issubclass(w.category, RuntimeWarning)]


def test_unavailable_logging_is_rate_limited(caplog):
    clock = [0.0]
    watcher = observer(ExplodingClient(), monotonic=lambda: clock[0])

    class Portfolio:
        last_target = None

        def now_ms(self):
            return T
    with caplog.at_level(logging.DEBUG, logger='test.meridian.market_context'):
        for _ in range(5):
            watcher.after_cycle(Portfolio())
        clock[0] = mc.LOG_INTERVAL_SECONDS
        watcher.after_cycle(Portfolio())
    levels = [r.levelno for r in caplog.records]
    assert levels == [logging.WARNING, *[logging.DEBUG] * 4, logging.WARNING]


def test_decision_cycle_binds_the_new_target_identity():
    watcher = observer(started(real_client(snapshot())))

    class Target:
        identity = 'f' * 64
        decision_time_ms = T + 61_000

    class Portfolio:
        last_target = Target()

        def now_ms(self):
            return T + 62_000
    first = watcher.after_cycle(Portfolio())
    second = watcher.after_cycle(Portfolio())
    assert (first['target_sha256'], first['decision_ms']) == ('f' * 64, T + 61_000)
    assert (second['target_sha256'], second['decision_ms']) == (None, T + 62_000)
    [recent] = watcher.custom_info()['recent_decisions']
    assert recent['target_sha256'] == 'f' * 64 and recent['state'] == 'AVAILABLE'


# ---------------------------------------------------------------------- paired non-interference
def _action(action):
    config = getattr(action, 'executor_config', None)
    if config is None:
        return (type(action).__name__, action.model_dump(mode='json'))
    return (type(action).__name__, config.model_dump(mode='json'))


class ObserveNeverInterferes(IsolatedAsyncioTestCase):
    setUp = session_tests.NativePaperSessionTests.setUp  # denies all network connections

    async def asyncSetUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.scenarios = []

    async def asyncTearDown(self):
        for scenario in self.scenarios:
            if not scenario.session._closed:
                await self.finish(scenario)
        self._directory.cleanup()

    async def run_script(self, name, client_factory=None, **config):
        directory = Path(self._directory.name) / name
        directory.mkdir()
        s = Scenario(directory, **config)
        self.scenarios.append(s)
        if client_factory is not None:
            s.controller.market_context.client_factory = client_factory
        trace = []

        async def step(**kwargs):
            receipt = await s.step(**kwargs)
            trace.append(self.capture(s, receipt))
            return receipt

        async def fill(pair, amount='1'):
            trace.append(self.capture(s, await s.fill(pair, amount)))

        async def lift(pair, **kwargs):
            trace.append(self.capture(s, await s.lift(pair, **kwargs)))

        try:
            await self.script(s, step, fill, lift)
        except BaseException:
            await self.finish(s)
            raise
        info = s.controller.get_custom_info()['market_context']
        status = s.controller.to_format_status()
        await self.finish(s)
        return (info, status), trace

    async def script(self, s, step, fill, lift):
        await step()  # first cut seeds
        receipt = await step(rsi={BTC: rebound(), ETH: rebound()})
        self.assertEqual({a.executor_config.trading_pair for a in creates(receipt)}, {BTC, ETH})
        await fill(BTC)
        await fill(ETH)
        await step(mids={BTC: '96.9'})  # BTC falls into a held bag
        await step(mids={BTC: '96.8'})
        for mid in ('104.5', '105.5', '104.9'):  # ETH trailing exit
            await step(mids={ETH: mid})
        await lift(ETH)
        for mid in ('101', '103', '104'):  # BTC bag recovery
            await step(mids={BTC: mid}, rsi={BTC: (80, 80)})
        await step(mids={BTC: '103.05'}, rsi={BTC: (80, 80)})
        await lift(BTC, amount='5', above='0.5')
        await step()
        await step(rsi={BTC: rebound()})  # capital came back: re-entry

    async def finish(self, s):
        try:
            task = s.controller.market_context.close()
            if task is not None:
                await task
        finally:
            await s.close()  # one native paper recorder per process at a time

    def capture(self, s, receipt):
        portfolio = s.controller.portfolio
        target = portfolio.last_target
        return {
            'actions': [_action(a) for a in receipt['actions']],
            'grants': [g.model_dump(mode='json') if hasattr(g, 'model_dump') else repr(g) for g in s.grants()],
            'portfolio': repr(portfolio.status),
            'target': None if target is None else target.identity,
            'balances': {k: str(v) for k, v in sorted(s.session._connector.get_all_balances().items())},
            'executors': sorted((e.config.id, e.config.trading_pair, str(e.close_type), e.is_active)
                                for e in s.executors()),
            'held': [str(s.held(p)) for p in (BTC, ETH)],
            'symbols': {p: (ctx.runtime.reason, repr(ctx.runtime.bag_status))
                        for p, ctx in sorted(s.controller.contexts.items())},
        }

    async def test_observe_never_changes_decisions_grants_orders_or_exits(self):
        (off, _), baseline = await self.run_script('off')
        _, repeat = await self.run_script('off_repeat')
        self.assertEqual(off, {'mode': 'off', 'authority': 'observation_only'})
        self.assertEqual(repeat, baseline)  # the harness itself is deterministic
        kinds = [(kind, config.get('side'), config.get('type')) for step in baseline for kind, config in step['actions']]
        self.assertIn(('CreateExecutorAction', TradeType.SELL.value, 'order_executor'), kinds)  # bag recovery sell
        self.assertIn(('CreateExecutorAction', TradeType.BUY.value, 'position_executor'), kinds)
        self.assertIn('CloseType.TRAILING_STOP', {e[2] for step in baseline for e in step['executors']})
        self.assertIn('StopExecutorAction', [k for k, _, _ in kinds])
        cases = {
            'available': (lambda p, clock: started(real_client(snapshot())), 'AVAILABLE'),
            'warming': (lambda p, clock: started(real_client(snapshot(status='WARMING'))), 'WARMING'),
            'missing': (lambda p, clock: started(real_client()), 'UNAVAILABLE'),
            'stale': (lambda p, clock: started(real_client(snapshot(cutoff=T - 3 * 3_600_000))), 'STALE'),
            'erroring': (lambda p, clock: ExplodingClient(), 'UNAVAILABLE'),
            'factory_error': (lambda p, clock: (_ for _ in ()).throw(OSError('down')), 'UNAVAILABLE'),
            'timeout': (lambda p, clock: real_client(transport=HangingTransport()), 'UNAVAILABLE'),
        }
        for name, (factory, state) in cases.items():
            with self.subTest(name), patch.object(mcc, 'REFRESH_TIMEOUT_SECONDS', 0.001):
                (info, status), trace = await self.run_script(name, factory, market_context=OBSERVE)
                self.assertEqual(trace, baseline, name)
                self.assertEqual(info['latest']['state'], state, info['latest'])
                self.assertEqual(info['counters']['observations'], len(trace))
                self.assertTrue(info['recent_decisions'])  # observations bound to targets
                self.assertTrue(all(r['target_sha256'] for r in info['recent_decisions']))
                self.assertIn('market_context=observe', status[1])
                if name == 'timeout':
                    self.assertGreater(info['client_counters']['refresh_failures'], 0)

    async def test_the_paired_comparison_detects_a_feedback(self):
        """Sensitivity control: an observer that did feed back (pausing entries) changes the trace."""
        (_, _), baseline = await self.run_script('off')
        original = mc.MarketContextObserver.after_cycle

        def feedback(observer, portfolio):
            portfolio.owner.operator_entry_paused = True
            return original(observer, portfolio)
        with patch.object(mc.MarketContextObserver, 'after_cycle', feedback):
            with self.assertRaises(AssertionError):  # the script's own entry assertion fails first
                await self.run_script('feedback', lambda p, clock: started(real_client(snapshot())),
                                      market_context=OBSERVE)
