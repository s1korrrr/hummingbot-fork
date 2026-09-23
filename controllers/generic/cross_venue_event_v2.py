"""Thin native no-order adapter for the v2 event recipe. Frozen RSI owners are not hosts."""
import asyncio
import json
import os
import stat
import sys
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from hummingbot.strategy_v2.components.events_v2.contracts import QualifiedEventSnapshotV2
from hummingbot.strategy_v2.components.events_v2.validation import validate_snapshot_sequence
from hummingbot.strategy_v2.components.recipe_contract import source_digest, verify_binding
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.recipes.cross_venue_events_v2 import bind_observe_recipe, step


def source_files():
    recipe = Path(__file__).resolve().parents[2] / 'hummingbot/strategy_v2/recipes/cross_venue_events_v2.py'
    return [Path(__file__).resolve(), recipe]


def behavioral_configuration(config):
    return {'wait_ms': config.wait_ms, 'lifetime_ms': config.lifetime_ms,
            'recipe_id': config.recipe_id, 'max_snapshot_age_ms': config.max_snapshot_age_ms,
            'snapshot_path': config.snapshot_path}


def load_v2_snapshot(path):
    path = Path(path).absolute()
    if (sys.platform == 'darwin' and path.is_relative_to('/var')
            and Path('/var').is_symlink() and Path('/var').resolve() == Path('/private/var')):
        path = Path('/private/var') / path.relative_to('/var')
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise OSError('symlink snapshot path forbidden')
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
    with os.fdopen(fd, 'rb') as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError('snapshot is not a regular file')
        raw = stream.read(2_000_001)
    if len(raw) > 2_000_000:
        raise ValueError('snapshot exceeds size limit')

    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError('duplicate JSON key')
            value[key] = item
        return value

    obj = json.loads(raw, object_pairs_hook=unique, parse_constant=lambda x: (_ for _ in ()).throw(ValueError('non-finite JSON')))
    snapshot = QualifiedEventSnapshotV2.model_validate(obj)
    if snapshot.execution_allowed:
        raise ValueError('v2 snapshot cannot admit execution')
    return snapshot


class CrossVenueEventV2Config(ControllerConfigBase):
    controller_name: Literal['cross_venue_event_v2'] = 'cross_venue_event_v2'
    controller_type: Literal['generic'] = 'generic'
    execution_enabled: Literal[False] = False
    total_amount_quote: Decimal = Field(default=Decimal('0'), ge=0, le=0)
    snapshot_path: str
    recipe_id: Literal['cve_observe_v2'] = 'cve_observe_v2'
    wait_ms: int = Field(default=60_000, ge=0, lt=1_200_000, strict=True)
    lifetime_ms: int = Field(default=1_200_000, ge=1, le=1_200_000, strict=True)
    max_snapshot_age_ms: int = Field(default=120_000, ge=1, le=86_400_000, strict=True)

    @model_validator(mode='after')
    def validate_observer(self):
        if self.initial_positions:
            raise ValueError('event observer cannot adopt inventory')
        if not Path(self.snapshot_path).is_absolute():
            raise ValueError('snapshot_path must be an absolute read-only mount path')
        if self.wait_ms >= self.lifetime_ms:
            raise ValueError('delay must be shorter than event lifetime')
        return self

    def get_controller_class(self):
        return CrossVenueEventV2Controller


class CrossVenueEventV2Controller(ControllerBase):
    def __init__(self, config, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self._state = None
        self._last_snapshot = None
        payload = behavioral_configuration(config)
        source_sha256 = source_digest(Path(__file__).resolve().parents[2], source_files())
        self._recipe = bind_observe_recipe(
            source_sha256=source_sha256, configuration=payload, wait_ms=config.wait_ms,
            lifetime_ms=config.lifetime_ms, recipe_id=config.recipe_id)
        self._configuration = payload

    async def control_task(self):
        await self.update_processed_data()

    async def update_processed_data(self):
        now_ms = int(self.market_data_provider.time() * 1000)
        if self.config.manual_kill_switch:
            self.processed_data = {'status': 'OBSERVER_PAUSED', 'execution_allowed': False, 'reasons': []}
            return
        try:
            current_configuration = behavioral_configuration(self.config)
            verify_binding(self._recipe.binding, current_configuration,
                           source_sha256=source_digest(Path(__file__).resolve().parents[2], source_files()))
            snapshot = await asyncio.to_thread(load_v2_snapshot, self.config.snapshot_path)
            if snapshot.asof_ms > now_ms or now_ms - snapshot.asof_ms > self.config.max_snapshot_age_ms:
                raise ValueError('snapshot is stale or from the future')
            if self._last_snapshot is not None:
                if snapshot.binding != self._last_snapshot.binding:
                    raise ValueError('snapshot source binding changed')
                if snapshot.sequence == self._last_snapshot.sequence:
                    if snapshot != self._last_snapshot:
                        raise ValueError('snapshot changed without advancing its sequence')
                else:
                    validate_snapshot_sequence(self._last_snapshot, snapshot)
            result = step(self._state, snapshot, now_ms=now_ms, recipe=self._recipe,
                          configuration=current_configuration)
            self._state = result.state
            self._last_snapshot = snapshot
            self.processed_data = {
                'status': 'NO_ORDER_OBSERVER_V2', 'execution_allowed': False, 'decision_ms': now_ms,
                'reasons': list(result.reasons), 'setups': [item.phase for item in result.state.setups]}
        except (OSError, ValueError, TypeError, KeyError, RecursionError) as exc:
            self.processed_data = {'status': 'UNAVAILABLE', 'execution_allowed': False,
                                   'error_type': type(exc).__name__, 'reasons': []}

    def determine_executor_actions(self):
        return []

    def validate_executor_config(self, config):
        return False

    async def send_actions(self, executor_actions):
        if executor_actions:
            raise PermissionError('CrossVenueEventV2Controller has no execution authority')

    def to_format_status(self):
        data = self.processed_data
        return [f"Cross-venue events v2: {data.get('status', 'UNAVAILABLE')}; orders disabled",
                f"Observed reasons: {len(data.get('reasons', []))}"]
