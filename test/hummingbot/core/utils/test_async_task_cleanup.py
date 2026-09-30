"""Cancellation must release an owned coroutine even before its wrapper runs."""
import asyncio
import inspect

import pytest

from hummingbot.core.utils.async_utils import safe_ensure_future


@pytest.mark.asyncio
async def test_cancellation_before_wrapper_start_closes_owned_coroutine():
    entered = False

    async def work():
        nonlocal entered
        entered = True

    coroutine = work()
    try:
        task = safe_ensure_future(coroutine)
        task.cancel()
        result = await asyncio.gather(task, return_exceptions=True)
        assert isinstance(result[0], asyncio.CancelledError)
        assert not entered
        assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED
    finally:
        coroutine.close()


@pytest.mark.asyncio
async def test_cancellation_after_start_preserves_coroutine_cleanup():
    entered, cleaned = asyncio.Event(), asyncio.Event()

    async def work():
        try:
            entered.set()
            await asyncio.Future()
        finally:
            cleaned.set()

    coroutine = work()
    task = safe_ensure_future(coroutine)
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    result = await asyncio.gather(task, return_exceptions=True)
    assert isinstance(result[0], asyncio.CancelledError)
    assert cleaned.is_set()
    assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED


@pytest.mark.asyncio
async def test_success_and_future_awaitables_retain_their_results():
    async def work():
        return 42

    assert await safe_ensure_future(work()) == 42
    future = asyncio.get_running_loop().create_future()
    future.set_result('native-result')
    assert await safe_ensure_future(future) == 'native-result'
