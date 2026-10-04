"""A funded BUY its own executor stopped before creating any order frees its claim.

`ExecutorBase.on_start` runs `validate_sufficient_balance()` before any order
exists; on a short wallet it closes INSUFFICIENT_BALANCE without an order and
without a terminal entry receipt. On a wallet shared with other bots this is an
ordinary race, and the claim must not stay reserved forever. Anything that ever
owned an order keeps its commitment until native reconciliation proves it.
"""
import asyncio
from decimal import Decimal
from test.acquisition_chain_v2.test_funding_edges import FEE_FRACTION, chain_config, make_store
from test.test_ok_rsi_down_only_entry_execution import make_executor
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from hummingbot.strategy_v2.models.executors import CloseType


def _stopped_on_insufficient_balance(config):
    """The real DownOnlyMaker executor failing its own pre-submission budget check."""
    executor = make_executor(config=config)[0]
    executor.adjust_order_candidates = MagicMock(return_value=[SimpleNamespace(amount=Decimal('0'))])
    asyncio.run(executor.validate_sufficient_balance())
    assert executor.close_type == CloseType.INSUFFICIENT_BALANCE
    executor.place_order.assert_not_called()
    return executor.executor_info


def _full_reserve(config):
    return config.amount * config.entry_price * (1 + FEE_FRACTION)


@pytest.mark.parametrize('chain', [True, False])
def test_insufficient_balance_before_any_order_releases_its_funding_claim(chain):
    config = chain_config() if chain else chain_config(entry_chain_contract=None)
    info = _stopped_on_insufficient_balance(config)
    assert not info.is_active and not info.custom_info.get('order_ids')
    store = make_store([info], [], [])

    first = store.reconcile()
    second = store.reconcile()

    assert Decimal(first['pending_quote']) == 0
    assert Decimal(second['pending_quote']) == 0
    assert first['released_intents'] == second['released_intents'] == 1
    store.admit(chain_config(id='next-entry'))  # the freed allocation admits the next full-size entry


@pytest.mark.parametrize('chain', [True, False])
def test_an_executor_that_owned_an_order_keeps_its_full_commitment(chain):
    config = chain_config() if chain else chain_config(entry_chain_contract=None)
    info = _stopped_on_insufficient_balance(config)
    info.custom_info['order_ids'] = ['submitted-entry']  # ambiguous: a submission may exist on the venue
    store = make_store([info], [], [])

    assert Decimal(store.assess()['pending_quote']) == _full_reserve(config)
