from decimal import Decimal as D

import pytest

from controllers.directional_trading.ok_rsi_core.entry_pricing import (
    AdaptiveEntryPriceInputError,
    select_adaptive_entry_price,
)


def select(**overrides):
    values = dict(
        bid=D("100.00"),
        ask=D("100.09"),
        atr_1m=D("1"),
        price_step=D("0.01"),
        signal_confidence=D("0.8"),
        trend_alignment=D("0.2"),
    )
    return select_adaptive_entry_price(**(values | overrides))


def test_adaptive_entry_uses_larger_of_tick_two_spreads_and_atr_component():
    spread_limited = select()
    atr_limited = select(signal_confidence=D("0.2"), trend_alignment=D("-0.6"))

    assert spread_limited.reference_price == D("100.09")
    assert spread_limited.price == D("99.91")
    assert spread_limited.discount == D("0.18")
    assert spread_limited.discount_atr == D("0.18")
    assert atr_limited.price == D("99.39")
    assert atr_limited.discount == D("0.70")
    assert atr_limited.discount_atr == D("0.70")


def test_stronger_supportive_context_quotes_closer_but_remains_passive():
    strong = select(bid=D("100"), ask=D("100"), signal_confidence=D("1"), trend_alignment=D("1"))
    weak = select(bid=D("100"), ask=D("100"), signal_confidence=D("0"), trend_alignment=D("-1"))

    assert strong.price == D("99.99")
    assert strong.price < strong.reference_price
    assert weak.price == D("99.00")
    assert weak.price < strong.price


def test_reference_and_selected_price_are_quantized_down_to_native_tick():
    selection = select(
        bid=D("100.006"),
        ask=D("100.007"),
        atr_1m=D("0.013"),
        price_step=D("0.01"),
        signal_confidence=D("0.5"),
        trend_alignment=D("-0.2"),
    )

    assert selection.reference_price == D("100.00")
    assert selection.price == D("99.99")
    assert selection.discount == D("0.01")


@pytest.mark.parametrize(
    "change",
    [
        {"ask": D("NaN")},
        {"bid": D("101")},
        {"atr_1m": D("0")},
        {"price_step": D("0")},
        {"signal_confidence": D("1.01")},
        {"trend_alignment": D("-1.01")},
    ],
)
def test_invalid_adaptive_entry_inputs_fail_closed(change):
    with pytest.raises(AdaptiveEntryPriceInputError):
        select(**change)
