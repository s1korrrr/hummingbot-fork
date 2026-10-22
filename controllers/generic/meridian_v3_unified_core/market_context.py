"""Observe-only Shared Market Context binding for Meridian v3 (operator decision 2026-09-29).

Alongside each portfolio decision cycle the controller records the context snapshot it
would have seen through the cached read-only client, with its causal identity, or an
explicit UNAVAILABLE / WARMING / STALE state with reasons. Nothing here is read by the
portfolio, the symbol runtimes, grants, orders or exits: an observation is reporting
only. There is deliberately no gating mode; a request for one is refused at config
validation so a context-dependent decision needs a new, separately reviewed recipe.

A client or selection failure never raises into the controller tick. It becomes an
UNAVAILABLE observation and is logged at WARNING at most once per state/reason key per
``LOG_INTERVAL_SECONDS``; repeats are DEBUG.
"""
import asyncio
import inspect
import logging
import time
from collections import deque
from pathlib import Path
from typing import Callable, Literal

from pydantic import Field, model_validator

from hummingbot.strategy_v2.components.candle_transport import validate_origin
from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.market_context.contracts import ClockView, ContextRequirement

MODES = ('off', 'observe')
LOG_INTERVAL_SECONDS = 300.0
RECENT_DECISIONS = 64
AUTHORITY = 'observation_only'
SUMMARY_FIELDS = ('target_sha256', 'decision_ms', 'state', 'reasons', 'detail', 'effective_available_at_ms')


class MarketContextPolicy(PolicyModel):
    """`off` constructs nothing. `observe` records context and never changes a decision."""
    mode: Literal['off', 'observe'] = 'off'
    origin: str | None = None
    credentials_file: str | None = None
    requirement: ContextRequirement | None = None
    poll_seconds: float = Field(default=5.0, gt=0, le=60)
    timeout_seconds: float = Field(default=5.0, gt=0, le=15)

    @model_validator(mode='before')
    @classmethod
    def refuse_gate(cls, values):
        mode = values.get('mode') if isinstance(values, dict) else getattr(values, 'mode', None)
        if mode is not None and mode not in MODES:
            raise ValueError(f'market context mode {mode!r} is not authorized; only off or observe '
                             '(observe-only for the first live launch, operator decision 2026-09-29)')
        return values

    @model_validator(mode='after')
    def complete_binding(self):
        fields = (self.origin, self.credentials_file, self.requirement)
        if self.mode == 'off':
            if any(value is not None for value in fields):
                raise ValueError('market context off takes no origin, credentials or requirement')
            return self
        if any(value is None for value in fields):
            raise ValueError('market context observe requires origin, credentials_file and requirement')
        validate_origin(self.origin)
        if not Path(self.credentials_file).is_absolute():
            raise ValueError('market context credentials_file must be an absolute path')
        if self.requirement.consumer != 'execution':
            raise ValueError('an execution runtime observes with the execution consumer requirement')
        return self


def default_client_factory(policy: MarketContextPolicy, clock_ms: Callable[[], int]):
    from hummingbot.data_feed.market_context_client import HttpMarketContextTransport, MarketContextClient
    transport = HttpMarketContextTransport(policy.origin, policy.credentials_file,
                                           timeout_seconds=policy.timeout_seconds)
    return MarketContextClient(transport, stream_id=policy.requirement.stream_id, clock_ms=clock_ms,
                               poll_seconds=policy.poll_seconds, max_backoff_seconds=max(30.0, policy.poll_seconds))


def _snapshot_identity(snapshot, receipt_ms):
    provenance = snapshot.provenance
    return {
        'snapshot_id': snapshot.snapshot_id, 'stream_id': snapshot.stream_id, 'epoch': snapshot.epoch,
        'sequence': snapshot.sequence, 'schema_version': snapshot.schema_version,
        'source_kind': snapshot.source_kind, 'envelope_status': snapshot.status,
        'envelope_reasons': list(snapshot.reasons),
        'feature_version': {'code_digest': provenance.code_digest, 'config_digest': provenance.config_digest,
                            'universe_hash': provenance.universe_hash, 'model_digest': provenance.model_digest,
                            'input_manifest_digest': provenance.input_manifest_digest,
                            'serializer_version': provenance.serializer_version},
        'coverage': {'expected': snapshot.coverage.expected_count, 'valid': snapshot.coverage.valid_count,
                     'valid_weight_fraction': snapshot.coverage.valid_weight_fraction},
        # cutoff = as-of close boundary; availability is publication, receipt is this consumer's fetch.
        'cutoff_ms': snapshot.cutoff_ms, 'max_input_available_at_ms': snapshot.max_input_available_at_ms,
        'available_at_ms': snapshot.available_at_ms, 'expires_at_ms': snapshot.expires_at_ms,
        'receipt_ms': receipt_ms,
    }


def _market(snapshot):
    """Every market feature with its own status; a non-VALID feature is null, never a neutral zero."""
    return [{'name': f.name, 'value': f.value, 'unit': f.unit, 'horizon_minutes': f.horizon_minutes,
             'status': f.status} for f in snapshot.market]


class MarketContextObserver:
    def __init__(self, policy: MarketContextPolicy, *, clock_ms: Callable[[], int], logger: logging.Logger,
                 client_factory=default_client_factory, monotonic: Callable[[], float] = time.monotonic):
        self.policy = policy
        self.clock_ms = clock_ms
        self.logger = logger
        self.client_factory = client_factory
        self.monotonic = monotonic
        self.client = None
        self.latest = None
        self.recent = deque(maxlen=RECENT_DECISIONS)
        self.counters = {'observations': 0, 'unavailable': 0, 'errors': 0}
        self._target_sha = None
        self._logged = {}

    @property
    def enabled(self) -> bool:
        return self.policy.mode == 'observe'

    # ------------------------------------------------------------------ recording
    def after_cycle(self, portfolio) -> dict | None:
        """Record one observation for this decision cycle. Never raises; never feeds back."""
        if not self.enabled:
            return None
        target_sha = None
        try:
            target = portfolio.last_target
            if target is not None and target.identity != self._target_sha:
                target_sha, decision_ms = target.identity, int(target.decision_time_ms)
            else:
                decision_ms = int(portfolio.now_ms())
        except Exception as exc:  # clock/target read failed: still an explicit observation
            return self._record(self._unavailable(None, f'decision_clock_error:{type(exc).__name__}'), None)
        return self._record(self.observe(decision_ms), target_sha)

    def observe(self, decision_ms: int) -> dict:
        try:
            client = self._client()
            task = getattr(client, 'task', None)
            if task is None:
                client.start()  # requires the controller's running event loop
            elif task.done():
                return self._unavailable(decision_ms, 'client_task_stopped')
            selection = client.select(self.policy.requirement, ClockView(decision_ms=decision_ms, receipt_ms=None))
            return self._classify(client, selection, decision_ms)
        except Exception as exc:
            self.counters['errors'] += 1
            return self._unavailable(decision_ms, f'observer_error:{type(exc).__name__}')

    def _client(self):
        if self.client is None:
            self.client = self.client_factory(self.policy, self.clock_ms)
        return self.client

    def _classify(self, client, selection, decision_ms):
        current = getattr(client, 'current', None)
        snapshot = None if current is None else current.snapshot
        refresh = list(getattr(client, 'last_reasons', ()) or ())
        if selection.accepted:
            status = selection.snapshot.status
            state = {'READY': 'AVAILABLE', 'DEGRADED': 'DEGRADED', 'WARMING': 'WARMING'}.get(status, 'UNAVAILABLE')
            reasons = list(selection.snapshot.reasons)
        else:
            reasons = list(selection.reasons)
            if 'CONTEXT_EXPIRED' in reasons:
                state = 'STALE'
            elif snapshot is not None and snapshot.status == 'WARMING':
                state = 'WARMING'
            else:
                state = 'UNAVAILABLE'
        record = {
            'mode': 'observe', 'authority': AUTHORITY, 'state': state, 'accepted': selection.accepted,
            'reasons': reasons, 'detail': None, 'refresh_reasons': refresh, 'decision_ms': decision_ms,
            'effective_available_at_ms': selection.effective_available_at_ms,
            'age_ms': None if snapshot is None else decision_ms - snapshot.cutoff_ms,
            'snapshot': None if snapshot is None else _snapshot_identity(snapshot, current.receipt_ms),
            'market': _market(selection.snapshot) if selection.accepted else None,
        }
        return record

    def _unavailable(self, decision_ms, detail):
        return {'mode': 'observe', 'authority': AUTHORITY, 'state': 'UNAVAILABLE', 'accepted': False,
                'reasons': ['CONTEXT_UNAVAILABLE'], 'detail': detail, 'refresh_reasons': [],
                'decision_ms': decision_ms, 'effective_available_at_ms': None, 'age_ms': None,
                'snapshot': None, 'market': None}

    def _record(self, record, target_sha):
        record['target_sha256'] = target_sha
        self.counters['observations'] += 1
        if record['state'] != 'AVAILABLE':
            self.counters['unavailable'] += 1
        self.latest = record
        if target_sha is not None:
            self._target_sha = target_sha
            summary = {key: record[key] for key in SUMMARY_FIELDS}
            summary['snapshot_id'] = None if record['snapshot'] is None else record['snapshot']['snapshot_id']
            self.recent.append(summary)
        self._log(record)
        return record

    def _log(self, record):
        if record['state'] in ('AVAILABLE', 'DEGRADED'):
            return
        key = (record['state'], tuple(record['reasons']), record['detail'])
        now = self.monotonic()
        last = self._logged.get(key)
        level = logging.WARNING if last is None or now - last >= LOG_INTERVAL_SECONDS else logging.DEBUG
        if level == logging.WARNING:
            self._logged[key] = now
        self.logger.log(level, 'Meridian market context (observe-only) %s reasons=%s detail=%s',
                        record['state'], ','.join(record['reasons']), record['detail'])

    # ------------------------------------------------------------------ reporting / lifecycle
    def custom_info(self) -> dict:
        if not self.enabled:
            return {'mode': 'off', 'authority': AUTHORITY}
        client = self.client
        return {'mode': 'observe', 'authority': AUTHORITY, 'latest': self.latest,
                'recent_decisions': list(self.recent), 'counters': dict(self.counters),
                'client_counters': None if client is None else dict(getattr(client, 'counters', {}) or {})}

    def status_line(self) -> str:
        if not self.enabled:
            return 'market_context=off'
        latest = self.latest or {}
        return (f'market_context=observe state={latest.get("state", "NO_OBSERVATION")} '
                f'reasons={",".join(latest.get("reasons", ()))} age_ms={latest.get("age_ms")}')

    def close(self):
        """Close the client's refresh task; returns an awaitable task or None."""
        client = self.client
        if client is None:
            return None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return None
        closing = client.close()
        try:
            task = loop.create_task(closing)
        except BaseException:
            if inspect.getcoroutinestate(closing) == inspect.CORO_CREATED:
                closing.close()
            raise
        self.client = None
        return task
