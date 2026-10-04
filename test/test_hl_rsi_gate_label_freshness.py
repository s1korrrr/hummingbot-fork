"""The published BUY gate must describe the current refusal, not an older one."""
from test.test_hl_rsi_entry_control import _controller
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType


@pytest.fixture(autouse=True)
def _complete_empty_native_history():
    recorder = MagicMock()
    recorder.get_executors_by_controller.return_value = []
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        yield recorder


def test_resume_clears_a_lingering_operator_pause_label():
    instance = _controller()
    instance.set_operator_entry_pause(True, reason='maintenance', scope='bot')
    instance._record_gate_reason(TradeType.BUY, 'operator_pause')

    instance.set_operator_entry_pause(False, reason='restored', scope='bot')

    assert instance._last_gate_reason[TradeType.BUY] is None
    assert instance.get_operator_state()['entry_paused'] is False


def test_resume_keeps_an_unrelated_current_refusal():
    instance = _controller()
    instance._record_gate_reason(TradeType.BUY, 'price_gap')

    instance.set_operator_entry_pause(False, reason='restored', scope='bot')

    assert instance._last_gate_reason[TradeType.BUY] == 'price_gap'


@pytest.mark.parametrize('atr_pct,reason', [(0.00005, 'low_vol'), (0.5, 'high_vol')])
def test_volatility_veto_of_a_buy_signal_replaces_the_stale_gate_label(atr_pct, reason):
    instance = _controller()
    instance._record_gate_reason(TradeType.BUY, 'operator_pause')

    assert instance._apply_buy_market_condition(1, atr_pct, None, None) == (0, False, reason)
    assert instance._last_gate_reason[TradeType.BUY] == reason


def test_tradable_volatility_keeps_signal_and_label():
    instance = _controller()
    instance._record_gate_reason(TradeType.BUY, 'price_gap')

    assert instance._apply_buy_market_condition(1, 0.001, None, None) == (1, True, 'ok')
    assert instance._last_gate_reason[TradeType.BUY] == 'price_gap'


@pytest.mark.parametrize('signal', [0, -1])
def test_non_buy_signal_is_not_volatility_vetoed_or_relabelled(signal):
    instance = _controller()
    instance._record_gate_reason(TradeType.BUY, 'price_gap')

    assert instance._apply_buy_market_condition(signal, 0.00005, None, None) == (signal, True, 'ok')
    assert instance._last_gate_reason[TradeType.BUY] == 'price_gap'


def test_strict_regime_error_zeroes_any_signal_and_labels_only_a_buy():
    instance = _controller()
    instance.config = type(instance.config).model_validate(dict(instance.config.model_dump(), strict_hmm_mode=True))
    instance._record_gate_reason(TradeType.BUY, 'price_gap')

    assert instance._apply_buy_market_condition(-1, 0.001, 'regime failed', None) == (0, False, 'strict_hmm')
    assert instance._last_gate_reason[TradeType.BUY] == 'price_gap'
    assert instance._apply_buy_market_condition(1, 0.001, 'regime failed', None) == (0, False, 'strict_hmm')
    assert instance._last_gate_reason[TradeType.BUY] == 'strict_hmm'


def test_volatility_veto_still_restores_a_pending_rebound_confirmation():
    instance = _controller(buy_confirmation_mode='rebound_confirm')
    setup = SimpleNamespace(name='pending-setup')
    instance._pending_buy_confirmation = None

    instance._apply_buy_market_condition(1, 0.00005, None, setup)

    assert instance._pending_buy_confirmation is setup
