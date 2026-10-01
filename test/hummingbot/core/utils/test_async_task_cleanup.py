"""Cancellation and scheduling rejection must release owned coroutines."""
import asyncio
import inspect
import threading

import pytest

from hummingbot.core.utils.async_utils import safe_ensure_future


def test_missing_loop_rejection_closes_owned_coroutine():
    observed = []

    def schedule_without_loop():
        async def work():
            pass

        coroutine = work()
        try:
            with pytest.raises(RuntimeError, match='no current event loop'):
                safe_ensure_future(coroutine)
            observed.append(inspect.getcoroutinestate(coroutine))
        finally:
            coroutine.close()

    thread = threading.Thread(target=schedule_without_loop)
    thread.start()
    thread.join(timeout=1)
    assert not thread.is_alive()
    assert observed == [inspect.CORO_CLOSED]


def test_closed_loop_rejection_closes_owned_coroutine():
    async def work():
        pass

    loop = asyncio.new_event_loop()
    loop.close()
    coroutine = work()
    try:
        with pytest.raises(RuntimeError, match='closed'):
            safe_ensure_future(coroutine, loop=loop)
        assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED
    finally:
        coroutine.close()


@pytest.mark.parametrize('error', [RuntimeError, ValueError])
def test_rejected_task_factory_closes_owned_coroutine(error):
    async def work():
        pass

    loop = asyncio.new_event_loop()

    def reject_task_factory(_loop, _wrapped):
        raise error('task factory rejected')

    loop.set_task_factory(reject_task_factory)
    coroutine = work()
    try:
        with pytest.raises(error, match='task factory rejected'):
            safe_ensure_future(coroutine, loop=loop)
        assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED
    finally:
        coroutine.close()
        loop.close()


def test_rejected_schedule_does_not_take_ownership_of_future():
    loop = asyncio.new_event_loop()
    future = loop.create_future()
    loop.close()
    with pytest.raises(RuntimeError, match='closed'):
        safe_ensure_future(future, loop=loop)
    assert not future.done()


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
