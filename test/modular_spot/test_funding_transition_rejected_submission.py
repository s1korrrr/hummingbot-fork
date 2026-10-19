"""A proven pre-ack OKX rejection has no native Order row to retain."""
import json
from decimal import Decimal as D
from test.test_ok_rsi_down_only_entry_execution import make_config
from types import SimpleNamespace

import pytest

from controllers.generic.modular_spot_core.native_funding import NativeFundingStore
from controllers.generic.modular_spot_core.native_funding_transition import _check_persisted_submissions
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.model.market_state import MarketState
from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType


def rejected_case():
    config = make_config(id='rejected-intent', controller_id='modular_spot_eth', trading_pair='ETH-USDC')
    pending = InFlightOrder('rejected-entry', 'ETH-USDC', OrderType.LIMIT_MAKER, TradeType.BUY,
                            D('1'), 1, D('100'))
    terminal = InFlightOrder.from_json(pending.to_json())
    terminal.current_state = OrderState.FAILED
    receipt = {'schema': 1, 'source': 'okx_submission_rejected_v1', 'order_id': pending.client_order_id,
               'exchange_order_id': None, 'trading_pair': 'ETH-USDC', 'side': 'BUY', 'amount': '1',
               'filled_base': '0', 'state': 'REJECTED', 'trade_ids': []}
    info = SimpleNamespace(id=config.id, config=config, connector_name='okx', trading_pair='ETH-USDC',
                           status=RunnableStatus.TERMINATED, close_type=CloseType.FAILED,
                           is_active=False, is_trading=False,
                           custom_info={'order_ids': [pending.client_order_id],
                                        'execution_order_snapshots': [terminal.to_json()],
                                        'entry_reconciliation': receipt,
                                        'entry_lifecycle': {'policy': 'single_attempt',
                                                            'state': 'ended_no_fill',
                                                            'stop_reason': 'entry_rejected'}})
    connector = SimpleNamespace(display_name='okx', _order_tracker=SimpleNamespace(all_orders={}))
    store = object.__new__(NativeFundingStore)
    store.owner = SimpleNamespace(config=SimpleNamespace(execution_mode='live', connector_name='okx'),
                                  market_data_provider=SimpleNamespace(connectors={'okx': connector}))
    store.recorder = SimpleNamespace(config_file_path='modular.yml', get_trades_for_config=lambda _: [])
    row = SimpleNamespace(key='v2-submitted-order:' + pending.client_order_id,
                          value=json.dumps({'owner': 'modular.yml', 'order': pending.to_json()}))

    class Session:
        def query(self, model):
            return SimpleNamespace(all=lambda: [row] if model is Metadata else [] if model is MarketState else None)

    return store, Session(), info, connector, row


def test_proven_terminal_preack_rejection_does_not_require_order_row():
    store, session, info, connector, _ = rejected_case()
    _check_persisted_submissions(store, session, [info], [], connector)


def change_submission(row, **changes):
    value = json.loads(row.value)
    value['order'].update(changes)
    row.value = json.dumps(value)


@pytest.mark.parametrize('mutate', [
    lambda info, row: info.custom_info.update(order_ids=[]),
    lambda info, row: info.custom_info.update(entry_reconciliation=None),
    lambda info, row: setattr(info, 'is_active', True),
    lambda info, row: (setattr(row, 'key', 'v2-supervised-orders:test'),
                       setattr(row, 'value', json.dumps({'owner': 'modular.yml',
                                                         'orders': [json.loads(row.value)['order']]}))),
    lambda info, row: change_submission(row, last_state=OrderState.OPEN.value),
    lambda info, row: change_submission(row, exchange_order_id='venue-ack'),
    lambda info, row: info.custom_info['entry_reconciliation'].update(source='okx_terminal_status_v1'),
    lambda info, row: info.custom_info['execution_order_snapshots'][0].update(
        last_state=OrderState.OPEN.value),
])
def test_unproved_preack_rejection_keeps_transition_blocked(mutate):
    store, session, info, connector, row = rejected_case()
    mutate(info, row)
    with pytest.raises(ValueError):
        _check_persisted_submissions(store, session, [info], [], connector)
