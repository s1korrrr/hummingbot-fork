# distutils: language=c++

import asyncio
from enum import Enum
import logging
from typing import Optional

from hummingbot.core.clock cimport Clock
from hummingbot.logger import HummingbotLogger
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.time_iterator import TimeIterator

NaN = float("nan")
# Consecutive failed network checks before a connected network is stopped (about 30-45 s at the connector defaults).
NETWORK_FAILURE_THRESHOLD = 3
ni_logger = None


class NetworkStatus(Enum):
    STOPPED = 0
    NOT_CONNECTED = 1
    CONNECTED = 2


cdef class NetworkIterator(TimeIterator):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global ni_logger
        if ni_logger is None:
            ni_logger = logging.getLogger(__name__)
        return ni_logger

    def __init__(self):
        super().__init__()
        self._network_status = NetworkStatus.STOPPED
        self._last_connected_timestamp = NaN
        self._check_network_interval = 10.0
        self._check_network_timeout = 5.0
        self._network_error_wait_time = 60.0
        self._check_network_task = None

    @property
    def network_status(self) -> NetworkStatus:
        return self._network_status

    @property
    def last_connected_timestamp(self) -> float:
        return self._last_connected_timestamp

    @last_connected_timestamp.setter
    def last_connected_timestamp(self, value):
        self._last_connected_timestamp = value

    @property
    def check_network_task(self) -> Optional[asyncio.Task]:
        return self._check_network_task

    @property
    def check_network_interval(self) -> float:
        return self._check_network_interval

    @check_network_interval.setter
    def check_network_interval(self, double interval):
        self._check_network_interval = interval

    @property
    def network_error_wait_time(self) -> float:
        return self._network_error_wait_time

    @network_error_wait_time.setter
    def network_error_wait_time(self, double wait_time):
        self._network_error_wait_time = wait_time

    @property
    def check_network_timeout(self) -> float:
        return self._check_network_timeout

    @check_network_timeout.setter
    def check_network_timeout(self, double timeout):
        self._check_network_timeout = timeout

    async def start_network(self):
        pass

    async def stop_network(self):
        pass

    async def check_network(self) -> NetworkStatus:
        self.logger().warning("check_network() has not been implemented!")
        return NetworkStatus.NOT_CONNECTED

    async def _check_network_loop(self):
        # A connected network is stopped only after NETWORK_FAILURE_THRESHOLD consecutive failed checks. A single
        # slow or failed check (a starved event loop, one timed-out ping) used to mark the network NOT_CONNECTED
        # (silently, on a timeout) so the next good check ran start_network(), rebuilding every order book and
        # stream. The stop now also runs stop_network(), so status and networking agree.
        failures = 0
        while True:
            last_status = self._network_status
            wait = self._check_network_interval
            try:
                try:
                    new_status = await asyncio.wait_for(self.check_network(), timeout=self._check_network_timeout)
                except asyncio.CancelledError:
                    raise
                except asyncio.TimeoutError:
                    self.logger().debug("Check network call has timed out.")
                    new_status = NetworkStatus.NOT_CONNECTED
                except Exception as e:
                    self.logger().error(f"Unexpected error while checking for network status: {e}", exc_info=True)
                    new_status = NetworkStatus.NOT_CONNECTED
                    wait = self._network_error_wait_time
                if new_status is NetworkStatus.CONNECTED:
                    failures = 0
                elif last_status is NetworkStatus.CONNECTED:
                    failures += 1
                    if failures < NETWORK_FAILURE_THRESHOLD:
                        self.logger().info(f"Network check failed ({failures} of {NETWORK_FAILURE_THRESHOLD} consecutive); "
                                           "keeping networking running.")
                        new_status = last_status
                        wait = self._check_network_interval
                if new_status != last_status:
                    failures = 0
                    self._network_status = new_status
                    if self._network_status is NetworkStatus.CONNECTED:
                        self.logger().info(f"Network status has changed to {new_status}. Starting networking...")
                        await self.start_network()
                    else:
                        self.logger().info(f"Network status has changed to {new_status}. Stopping networking...")
                        await self.stop_network()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error while checking for network status: {e}", exc_info=True)
                self._network_status = NetworkStatus.NOT_CONNECTED
                wait = self._network_error_wait_time
            await asyncio.sleep(wait)


    cdef c_start(self, Clock clock, double timestamp):
        TimeIterator.c_start(self, clock, timestamp)
        self._check_network_task = safe_ensure_future(self._check_network_loop())
        self._network_status = NetworkStatus.NOT_CONNECTED

    cdef c_stop(self, Clock clock):
        TimeIterator.c_stop(self, clock)
        if self._check_network_task is not None:
            self._check_network_task.cancel()
            self._check_network_task = None
        self._network_status = NetworkStatus.STOPPED
        safe_ensure_future(self.stop_network())

    def start(self, clock: Clock, timestamp: float):
        self.c_start(clock, timestamp)

    def stop(self, clock: Clock):
        self.c_stop(clock)
