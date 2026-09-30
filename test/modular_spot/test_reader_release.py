"""Rejected cleanup scheduling retains the last shared reader owner."""
import asyncio
from types import SimpleNamespace

import pytest

from controllers.generic.modular_spot_core import runtime as runtime_module
from hummingbot.strategy_v2.components import data_reader


class Provider:
    pass


class Client:
    def __init__(self, error=None):
        self.closed = False
        self.error = error

    async def close(self):
        self.closed = True
        if self.error:
            raise self.error


def owned_pool(client):
    provider = Provider()
    policy = data_reader.DataPolicy(mode='shared', origin='http://127.0.0.1:8092', token_file='/audit/token')
    pool = data_reader.DataPool(policy)
    pool.client, pool.references = client, 1
    data_reader._POOLS[provider] = {'fixture': pool}
    return provider, pool


@pytest.mark.asyncio
async def test_cleanup_factory_rejection_preserves_pool_ownership():
    client = Client()
    provider, pool = owned_pool(client)
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()

    def reject(*_args, **_kwargs):
        raise RuntimeError('cleanup scheduling rejected')

    loop.set_task_factory(reject)
    try:
        with pytest.raises(RuntimeError, match='cleanup scheduling rejected'):
            data_reader.release_pool(provider, pool)
        assert pool.references == 1
        assert data_reader._POOLS[provider]['fixture'] is pool
        assert pool.client is client and not client.closed
    finally:
        loop.set_task_factory(previous)
        await pool.close()
    # Reacquire a harmless stand-in client to prove release can be retried once.
    pool.client = client
    await data_reader.release_pool(provider, pool)
    assert pool.references == 0 and not data_reader._POOLS[provider]
    assert pool.client is None and client.closed


@pytest.mark.asyncio
async def test_close_task_preserves_errors_for_owner_latch():
    provider, pool = owned_pool(Client(ValueError('close failed')))
    task = data_reader.release_pool(provider, pool)
    with pytest.raises(ValueError, match='close failed'):
        await task
    assert isinstance(task.exception(), ValueError)
    assert pool.references == 0 and not data_reader._POOLS[provider]


def test_runtime_keeps_pool_when_release_is_rejected(monkeypatch):
    runtime = object.__new__(runtime_module.ModularRuntime)
    pool = runtime.pool = object()
    runtime.controller = SimpleNamespace(market_data_provider=Provider())

    def reject(*_args):
        raise RuntimeError('cleanup scheduling rejected')

    monkeypatch.setattr(runtime_module, 'release_pool', reject)
    with pytest.raises(RuntimeError, match='cleanup scheduling rejected'):
        runtime.close_data()
    assert runtime.pool is pool
