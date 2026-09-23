"""Isolated native no-order observer; never inherit RSI or adopt its inventory."""
import asyncio
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from controllers.generic.cross_venue_event_core import Policy, evaluate_batch, read_snapshot
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase


class CrossVenueEventConfig(ControllerConfigBase):
    controller_name: str = 'cross_venue_event'
    controller_type: str = 'generic'
    execution_enabled: Literal[False] = False
    total_amount_quote: Decimal = Field(default=Decimal('0'), ge=0, le=0)
    snapshot_path: str
    snapshot_ttl_ms: int = Field(default=5000, ge=1, le=60000)
    variant: Literal['delayed_entry', 'reaction_filter'] = 'delayed_entry'
    delay_ms: int = Field(default=60000, ge=0, lt=1200000)

    @model_validator(mode='after')
    def validate_observer(self):
        if self.initial_positions:
            raise ValueError('event observer cannot adopt inventory')
        if not Path(self.snapshot_path).is_absolute():
            raise ValueError('snapshot_path must be an absolute read-only mount path')
        return self


class CrossVenueEventController(ControllerBase):
    async def control_task(self):
        # An external evidence reader needs no connector-ready gate or orders.
        await self.update_processed_data()

    async def update_processed_data(self):
        now_ms = int(self.market_data_provider.time() * 1000)
        if self.config.manual_kill_switch:
            self.processed_data = {'status': 'OBSERVER_PAUSED', 'execution_allowed': False, 'decisions': []}
            return
        try:
            events, instruments, quotes = await asyncio.to_thread(
                read_snapshot, self.config.snapshot_path, now_ms, self.config.snapshot_ttl_ms)
            rows = evaluate_batch(events, instruments, quotes,
                                  Policy(variant=self.config.variant, delay_ms=self.config.delay_ms), now_ms)
            self.processed_data = {'status': 'NO_ORDER_OBSERVER', 'execution_allowed': False,
                                   'decision_ms': now_ms, 'decisions': rows}
        except (OSError, ValueError, TypeError, KeyError, RecursionError) as exc:
            # Do not leak raw payloads, filesystem contents or signed feed URLs.
            self.processed_data = {'status': 'UNAVAILABLE', 'execution_allowed': False,
                                   'error_type': type(exc).__name__, 'decisions': []}

    def determine_executor_actions(self):
        return []

    def validate_executor_config(self, config):
        return False

    async def send_actions(self, executor_actions):
        if executor_actions:
            raise PermissionError('CrossVenueEventController has no execution authority')

    def to_format_status(self):
        data = self.processed_data
        return [f"Cross-venue events: {data.get('status', 'UNAVAILABLE')}; orders disabled",
                f"Observed decisions: {len(data.get('decisions', []))}"]
