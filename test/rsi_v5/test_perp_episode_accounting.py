from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from controllers.directional_trading.rsi_v5_core.perp_episode_accounting import (
    EpisodeAccountingError,
    EpisodeAccountSnapshot,
    EpisodeTransitionAttribution,
    LinearPerpPositionSnapshot,
    ParityExpectation,
    ParityStatus,
    TerminalSettlementMode,
    liquidatable_nav,
    liquidatable_nav_reward,
    settle_terminal,
    verify_quants_lab_parity,
)

D = Decimal


def long_snapshot() -> EpisodeAccountSnapshot:
    return EpisodeAccountSnapshot(
        account_id="paper-account",
        as_of_ns=1_000,
        collateral_cash=D("9000"),
        accrued_funding_cashflow=D("-1"),
        positions=(
            LinearPerpPositionSnapshot(
                instrument_id="hyperliquid:perp:BTC",
                quantity=D("1"),
                entry_price=D("10000"),
                executable_bid=D("9990"),
                executable_ask=D("10010"),
                close_fee_rate=D("0.0005"),
                close_impact_rate=D("0.0002"),
                contract_multiplier=D("1"),
            ),
        ),
    )


def test_long_vector_matches_quants_lab_linear_perp_accounting() -> None:
    result = liquidatable_nav(long_snapshot())
    assert result.close_notional == D("9990")
    assert result.unrealized_pnl_at_close == D("-10")
    assert result.close_fee_cost == D("4.9950")
    assert result.close_impact_cost == D("1.9980")
    assert result.nav == D("8982.0070")

    parity = verify_quants_lab_parity(
        long_snapshot(),
        ParityExpectation(
            close_notional=D("9990"),
            unrealized_pnl_at_close=D("-10"),
            close_fee_cost=D("4.9950"),
            close_impact_cost=D("1.9980"),
            liquidatable_nav=D("8982.0070"),
            quants_lab_artifact_hash="1" * 64,
        ),
        tolerance=D("0.000000000000000001"),
    )
    assert parity.status is ParityStatus.MATCH
    assert parity.mismatches == ()
    assert parity.execution_authority is False
    assert not any(parity.authority.values())


def test_short_vector_uses_ask_without_short_sale_cash_proceeds() -> None:
    snapshot = EpisodeAccountSnapshot(
        account_id="paper-account",
        as_of_ns=2_000,
        collateral_cash=D("11000"),
        accrued_funding_cashflow=D("2"),
        positions=(
            LinearPerpPositionSnapshot(
                instrument_id="hyperliquid:perp:BTC",
                quantity=D("-1"),
                entry_price=D("10000"),
                executable_bid=D("9990"),
                executable_ask=D("10010"),
                close_fee_rate=D("0.0005"),
                close_impact_rate=D("0.0002"),
                contract_multiplier=D("1"),
            ),
        ),
    )
    result = liquidatable_nav(snapshot)
    assert result.close_notional == D("10010")
    assert result.unrealized_pnl_at_close == D("-10")
    assert result.close_fee_cost == D("5.0050")
    assert result.close_impact_cost == D("2.0020")
    assert result.nav == D("10984.9930")


def test_fractional_residual_position_and_multiplier_are_exact() -> None:
    snapshot = EpisodeAccountSnapshot(
        account_id="paper-account",
        as_of_ns=3_000,
        collateral_cash=D("1000"),
        accrued_funding_cashflow=D("0"),
        positions=(
            LinearPerpPositionSnapshot(
                instrument_id="hyperliquid:perp:BTC",
                quantity=D("0.5"),
                entry_price=D("9000"),
                executable_bid=D("10000"),
                executable_ask=D("10010"),
                close_fee_rate=D("0"),
                close_impact_rate=D("0"),
                contract_multiplier=D("2"),
            ),
        ),
    )
    result = liquidatable_nav(snapshot)
    assert result.close_notional == D("10000.0")
    assert result.unrealized_pnl_at_close == D("1000.0")
    assert result.nav == D("2000.0")


def test_force_flat_realizes_reserved_nav_once_and_clears_funding() -> None:
    before = liquidatable_nav(long_snapshot())
    settlement = settle_terminal(
        long_snapshot(),
        TerminalSettlementMode.FORCE_FLAT,
    )
    after = liquidatable_nav(settlement.ending_snapshot)
    assert settlement.terminal_nav == before.nav
    assert settlement.close_fee_cost == before.close_fee_cost
    assert settlement.close_impact_cost == before.close_impact_cost
    assert settlement.ending_snapshot.collateral_cash == before.nav
    assert settlement.ending_snapshot.accrued_funding_cashflow == D("0")
    assert settlement.ending_snapshot.positions == ()
    assert after.nav == before.nav


def test_reward_reconciles_nav_and_external_cashflow_exactly() -> None:
    before = EpisodeAccountSnapshot("paper-account", 1, D("100"), D("0"), ())
    after = EpisodeAccountSnapshot("paper-account", 2, D("111"), D("0"), ())
    attribution = EpisodeTransitionAttribution(
        gross_market_pnl=D("2"),
        funding_cashflow=D("0"),
        fee_cost=D("0.4"),
        spread_cost=D("0.2"),
        slippage_cost=D("0.1"),
        impact_cost=D("0.1"),
        borrow_cost=D("0.1"),
        liquidation_penalty=D("0.1"),
        close_cost_reserve_change=D("0"),
        ending_external_cashflow=D("10"),
    )
    result = liquidatable_nav_reward(before, after, attribution)
    assert result.performance_nav_after == D("101")
    assert result.log_return == D("1.01").ln()
    assert result.reconciliation_error == D("0")


def test_parity_mismatch_is_explicit_and_match_cannot_be_forged() -> None:
    expectation = ParityExpectation(
        close_notional=D("9990"),
        unrealized_pnl_at_close=D("-10"),
        close_fee_cost=D("4.9950"),
        close_impact_cost=D("1.9980"),
        liquidatable_nav=D("8983"),
        quants_lab_artifact_hash="2" * 64,
    )
    result = verify_quants_lab_parity(
        long_snapshot(),
        expectation,
        tolerance=D("0.000000000000000001"),
    )
    assert result.status is ParityStatus.MISMATCH
    assert result.mismatches == ("liquidatable_nav",)
    with pytest.raises(EpisodeAccountingError, match="MATCH parity result"):
        replace(result, status=ParityStatus.MATCH, mismatches=())
    with pytest.raises(EpisodeAccountingError, match="mismatch list"):
        replace(result, mismatches=("close_notional",))


def test_accounting_rejects_spot_style_cash_and_invalid_decimals() -> None:
    position = LinearPerpPositionSnapshot(
        instrument_id="hyperliquid:perp:BTC",
        quantity=D("0.01"),
        entry_price=D("10000"),
        executable_bid=D("10000"),
        executable_ask=D("10010"),
        close_fee_rate=D("0.001"),
        close_impact_rate=D("0"),
        contract_multiplier=D("1"),
    )
    with pytest.raises(EpisodeAccountingError, match="positive"):
        liquidatable_nav(
            EpisodeAccountSnapshot(
                "paper-account",
                2,
                D("0"),
                D("0"),
                (position,),
            )
        )
    with pytest.raises(EpisodeAccountingError, match="negative zero"):
        replace(position, quantity=D("-0"))
    with pytest.raises(TypeError, match="Decimal"):
        replace(position, entry_price=10000.0)  # type: ignore[arg-type]
