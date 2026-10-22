"""Bounded serial worker for native chart snapshot persistence."""

import asyncio
import logging
import queue
import threading
from typing import Any, Callable, Optional, Tuple


class BoundedChartSnapshotWriter:
    """Run recorder operations off-loop with bounded admission and owned shutdown."""

    def __init__(self, loop: asyncio.AbstractEventLoop, logger: logging.Logger,
                 on_complete: Callable[[Any, str, Optional[BaseException]], None]):
        self._loop = loop
        self._logger = logger
        self._on_complete = on_complete
        # One work item in flight or queued keeps each controller's write/ack/
        # prune sequence atomic with respect to other controller jobs.
        self._queue: "queue.Queue[Optional[Tuple[Any, str, Any]]]" = queue.Queue(maxsize=1)
        self._closing = threading.Event()
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._run, name="chart-snapshot-writer", daemon=True)
        self._thread.start()

    def submit(self, job: Any, phase: str, payload: Any) -> bool:
        if self._closing.is_set():
            return False
        try:
            self._queue.put_nowait((job, phase, payload))
        except queue.Full:
            return False
        return True

    def close(self) -> None:
        self._closing.set()

    def join(self, timeout: float) -> bool:
        self._thread.join(timeout=max(0.0, timeout))
        return self._stopped.is_set()

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def _run(self) -> None:
        try:
            while True:
                try:
                    item = self._queue.get(timeout=0.1)
                except queue.Empty:
                    if self._closing.is_set():
                        return
                    continue
                try:
                    if item is None:
                        return
                    job, phase, payload = item
                    error = None
                    try:
                        if phase == "write":
                            self._write_serialized(job.recorder, job.recorder.store_or_update_chart_snapshots, payload)
                        elif phase == "prune":
                            self._write_serialized(job.recorder, job.recorder.prune_chart_snapshots, **payload)
                        else:
                            raise ValueError("Unknown chart snapshot writer phase")
                    except Exception as exc:
                        error = exc
                    try:
                        self._loop.call_soon_threadsafe(self._on_complete, job, phase, error)
                    except RuntimeError:
                        self._logger.exception("Chart snapshot completion could not be returned to its owner loop")
                finally:
                    self._queue.task_done()
        finally:
            self._stopped.set()

    def _write_serialized(self, recorder, operation, *args, **kwargs):
        """Use the manager's single writer without blocking the strategy loop."""
        manager = recorder._sql_manager
        if manager is None:
            operation(*args, **kwargs)
            return
        queue = manager.write_queue
        future = asyncio.run_coroutine_threadsafe(
            queue.write(lambda: operation(*args, **kwargs)), self._loop
        )
        # This wait is confined to the owned chart worker thread. The strategy
        # loop remains free to process fills, timers, and the write completion.
        future.result()
