import asyncio
import logging
from abc import ABC

from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy_v2.models.base import RunnableStatus


class RunnableBase(ABC):
    """
    Base class for smart components in the Hummingbot application.
    This class provides a basic structure for components that need to perform tasks at regular intervals.
    """
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, update_interval: float = 0.5):
        """
        Initialize a new instance of the SmartComponentBase class.

        :param update_interval: The interval at which the control loop should be executed, in seconds.
        """
        self.update_interval = update_interval
        self._status: RunnableStatus = RunnableStatus.NOT_STARTED
        self.terminated = asyncio.Event()
        self._control_loop_task = None

    @property
    def status(self):
        """
        Get the current status of the smart component.

        :return: The current status of the smart component.
        """
        return self._status

    def start(self):
        """
        Start the control loop of the smart component.
        If the component is not already started, it will start the control loop.
        """
        if self._status == RunnableStatus.NOT_STARTED:
            self._start_control_loop()

    def _assert_control_loop_stopped(self):
        if self._control_loop_task is not None and not self._control_loop_task.done():
            raise RuntimeError('The previous control loop is still stopping; retry after it finishes')

    def _start_control_loop(self):
        self._assert_control_loop_stopped()
        previous_status, was_terminated = self._status, self.terminated.is_set()
        self.terminated.clear()
        self._status = RunnableStatus.RUNNING
        try:
            self._control_loop_task = safe_ensure_future(self.control_loop())
        except BaseException:
            self._status = previous_status
            if was_terminated:
                self.terminated.set()
            else:
                self.terminated.clear()
            raise
        self._control_loop_task.add_done_callback(self._control_loop_finished)

    def _control_loop_finished(self, task):
        if task is self._control_loop_task:
            self._status = RunnableStatus.TERMINATED
            self.terminated.set()

    def stop(self):
        """
        Stop the control loop of the smart component.
        If the component is active or not started, it will stop the control loop.
        """
        if self._status != RunnableStatus.TERMINATED:
            self._status = RunnableStatus.TERMINATED
            self.terminated.set()

    async def control_loop(self):
        """
        The main control loop of the smart component.
        This method is responsible for executing the control task at the specified interval.
        """
        try:
            if not self.terminated.is_set():
                await self.on_start()
            while not self.terminated.is_set():
                try:
                    await self.control_task()
                except Exception as e:
                    self.logger().error(e, exc_info=True)
                await asyncio.sleep(self.update_interval)
        finally:
            self.on_stop()

    def on_stop(self):
        """
        Method to be executed when the control loop is stopped.
        This method should be overridden in subclasses to provide specific behavior.
        """
        pass

    async def on_start(self):
        """
        Method to be executed when the control loop is started.
        This method should be overridden in subclasses to provide specific behavior.
        """
        pass

    async def control_task(self):
        """
        The main task to be executed in the control loop.
        This method should be overridden in subclasses to provide specific behavior.
        """
        pass
