import asyncio
import logging
import time

from hummingbot.core.utils.loop_watchdog import LoopWatchdog


def blocking_candle_work():
    time.sleep(0.6)  # Holds the loop thread like synchronous indicator or SQLite work.


def test_stall_names_the_blocking_code_and_its_duration(caplog):
    loop = asyncio.new_event_loop()
    watchdog = LoopWatchdog(loop, interval=0.05, threshold=0.25, logger=logging.getLogger("watchdog-test"))

    async def scenario():
        watchdog.start()
        await asyncio.sleep(0.2)
        blocking_candle_work()
        await asyncio.sleep(0.2)

    try:
        with caplog.at_level(logging.WARNING, logger="watchdog-test"):
            loop.run_until_complete(scenario())
    finally:
        watchdog.stop()
        loop.close()
    held = [r.getMessage() for r in caplog.records if "Event loop held" in r.getMessage()]
    resumed = [r.getMessage() for r in caplog.records if "Event loop resumed" in r.getMessage()]
    assert len(held) == 1 and "blocking_candle_work" in held[0]
    assert len(resumed) == 1 and float(resumed[0].split("after ")[1].split("s")[0]) >= 0.5


def test_a_responsive_loop_logs_nothing(caplog):
    loop = asyncio.new_event_loop()
    watchdog = LoopWatchdog(loop, interval=0.05, threshold=0.25, logger=logging.getLogger("watchdog-quiet"))

    async def scenario():
        watchdog.start()
        for _ in range(10):
            await asyncio.sleep(0.03)

    try:
        with caplog.at_level(logging.WARNING, logger="watchdog-quiet"):
            loop.run_until_complete(scenario())
    finally:
        watchdog.stop()
        loop.close()
    assert caplog.records == []
