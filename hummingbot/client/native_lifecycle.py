"""Process-local identity observations; missing shutdown proof is always false."""
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ShutdownResult:
    reconciliation_complete: bool
    error: str | None = None


class NativeLifecycle:
    def __init__(self, instance_id):
        self.instance_id = instance_id
        self.boot_id = str(uuid.uuid4())
        self.pid = os.getpid()
        self.sequence = 0
        self.identity = None
        self.state = 'unknown'
        self.reconciliation_complete = False

    def capture(self, core):
        previous = (self.identity, self.state, self.reconciliation_complete)
        self._capture(core)
        if previous != (self.identity, self.state, self.reconciliation_complete):
            self.sequence += 1

    def _capture(self, core):
        strategy = core.strategy
        if strategy is not None:
            script = core.strategy_name
            config = core._config_source
            controllers = getattr(strategy, 'controllers', {})
            if isinstance(script, str) and isinstance(config, str) and controllers:
                identity = (Path(script).name.removesuffix('.py') + '.py', Path(config).name, tuple(sorted(controllers)))
                if self.identity is None:
                    self.identity = identity
                elif self.identity != identity:
                    self.state = 'unknown'
                    self.reconciliation_complete = False
                    return
            if self.state not in {'starting', 'stopping'}:
                self.state = 'running' if core._strategy_running and not getattr(strategy, '_is_stop_triggered', False) else 'unknown'
                self.reconciliation_complete = False
        elif self.state not in {'stopped', 'starting', 'stopping'}:
            self.state = 'unknown'
            self.reconciliation_complete = False

    def transition(self, state, reconciliation_complete=False):
        self.state = state
        self.reconciliation_complete = state == 'stopped' and reconciliation_complete is True
        self.sequence += 1

    def snapshot(self, core, *, advance=True):
        self.capture(core)
        if self.identity is None:
            return None
        if advance:
            self.sequence += 1
        script, config, controllers = self.identity
        return {'schema_version': 1, 'generated_at': time.time(), 'sequence': self.sequence,
                'boot_id': self.boot_id, 'pid': self.pid, 'instance_id': self.instance_id,
                'script_file': script, 'config_file': config, 'controller_ids': list(controllers),
                'state': self.state, 'reconciliation_complete': self.reconciliation_complete}


async def publish_lifecycle(observer, core, publisher, topic, logger):
    import asyncio

    while True:
        try:
            packet = observer.snapshot(core)
            if packet is not None:
                publisher.publish(packet, topic)
        except Exception:
            logger.exception('Native lifecycle publication failed')
        await asyncio.sleep(5)
