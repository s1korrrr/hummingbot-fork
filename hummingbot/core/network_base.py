import asyncio
import logging
from typing import Optional

from hummingbot.core.network_iterator import NETWORK_FAILURE_THRESHOLD, NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger

NaN = float("nan")
nb_logger = None


class NetworkBase:
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global nb_logger
        if nb_logger is None:
            nb_logger = logging.getLogger(__name__)
        return nb_logger

    def __init__(self):
        self._network_status = NetworkStatus.STOPPED
        self._last_connected_timestamp = NaN
        self._check_network_interval = 60.0
        self._check_network_timeout = 60.0
        self._network_error_wait_time = 60.0
        self._check_network_task = None
        self._started = False

    @property
    def network_status(self) -> NetworkStatus:
        return self._network_status

    @property
    def last_connected_timestamp(self) -> float:
        return self._last_connected_timestamp

    @property
    def check_network_task(self) -> Optional[asyncio.Task]:
        return self._check_network_task

    @property
    def check_network_interval(self) -> float:
        return self._check_network_interval

    @check_network_interval.setter
    def check_network_interval(self, interval):
        self._check_network_interval = interval

    @property
    def network_error_wait_time(self) -> float:
        return self._network_error_wait_time

    @network_error_wait_time.setter
    def network_error_wait_time(self, wait_time):
        self._network_error_wait_time = wait_time

    @property
    def check_network_timeout(self) -> float:
        return self._check_network_timeout

    @property
    def started(self) -> bool:
        return self._started

    @check_network_timeout.setter
    def check_network_timeout(self, timeout):
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
            await self._sleep(wait)

    def start(self):
        self._check_network_task = safe_ensure_future(self._check_network_loop())
        self._network_status = NetworkStatus.NOT_CONNECTED
        self._started = True

    def stop(self):
        if self._check_network_task is not None:
            self._check_network_task.cancel()
            self._check_network_task = None
        self._network_status = NetworkStatus.STOPPED
        safe_ensure_future(self.stop_network())
        self._started = False

    async def _sleep(self, seconds: float):
        await asyncio.sleep(seconds)
