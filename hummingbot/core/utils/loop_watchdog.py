"""Event-loop stall evidence: name the code that held the loop instead of inferring it.

A daemon thread watches a heartbeat the loop refreshes every `interval` seconds. When
the heartbeat is older than `threshold`, the thread logs the loop thread's current
stack once for that stall; when the loop runs again it logs how long it was held.
Observation only: it never touches orders, state or the work the loop does.
"""
import logging
import sys
import threading
import time
import traceback
from typing import Optional

INTERVAL_SECONDS = 0.5
THRESHOLD_SECONDS = 3.0
STACK_FRAMES = 30

_watchdog: Optional["LoopWatchdog"] = None


class LoopWatchdog:
    def __init__(self, loop, *, interval=INTERVAL_SECONDS, threshold=THRESHOLD_SECONDS, logger=None):
        self.loop, self.interval, self.threshold = loop, interval, threshold
        self.logger = logger or logging.getLogger(__name__)
        self.beat = time.monotonic()
        self.loop_thread = None
        self.stall_reported = False
        self.stopped = threading.Event()

    def start(self):
        self.loop.call_soon_threadsafe(self._first_beat)
        threading.Thread(target=self._watch, name="loop-watchdog", daemon=True).start()
        return self

    def stop(self):
        self.stopped.set()

    def _first_beat(self):
        self.loop_thread = threading.get_ident()
        self._beat()

    def _beat(self):
        now = time.monotonic()
        if self.stall_reported:
            self.stall_reported = False
            self.logger.warning("Event loop resumed after %.1fs without running", now - self.beat)
        self.beat = now
        if not self.stopped.is_set():
            self.loop.call_later(self.interval, self._beat)

    def _watch(self):
        try:
            self._observe()
        except Exception:  # The watchdog only observes: its own failure must not reach the process.
            self.logger.exception("Event loop watchdog stopped")

    def _observe(self):
        while not self.stopped.wait(self.interval):
            if self.loop.is_closed():
                return
            if not self.loop.is_running():
                continue  # A loop that is not running is idle, not held.
            held = time.monotonic() - self.beat
            if held <= self.threshold or self.stall_reported or self.loop_thread is None:
                continue
            frame = sys._current_frames().get(self.loop_thread)
            stack = "".join(traceback.format_stack(frame)[-STACK_FRAMES:]) if frame is not None else "unavailable\n"
            self.stall_reported = True
            self.logger.warning("Event loop held for %.1fs; loop thread stack:\n%s", held, stack)


def start(loop) -> LoopWatchdog:
    """Watch `loop` once per process; a watchdog of another or closed loop is replaced."""
    global _watchdog
    if _watchdog is not None and _watchdog.loop is not loop:
        _watchdog.stop()
        _watchdog = None
    if _watchdog is None:
        _watchdog = LoopWatchdog(loop).start()
    return _watchdog
