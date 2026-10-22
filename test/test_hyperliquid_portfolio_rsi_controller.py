import json
import unittest
from pathlib import Path

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "hyperliquid_portfolio_rsi"


def load_fixture(name: str):
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


class HyperliquidPortfolioRSIControllerTest(unittest.TestCase):
    def test_wallet_policy_builder_uses_buy_allowlist_and_defaults_wallet_assets_to_sell_only(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_pair_policies_from_wallet

        wallet = {
            "quote_asset": "USD",
            "balances": {
                "USD": {"total_units": "100", "available_units": "100"},
                "HYPE": {"total_units": "2", "available_units": "2"},
                "PURR": {"total_units": "50", "available_units": "50"},
                "DOGE": {"total_units": "1000", "available_units": "1000"},
            },
        }

        policies = build_pair_policies_from_wallet(
            portfolio_snapshot=wallet,
            buy_sell_assets=[{"asset": "HYPE", "max_symbol_exposure_quote": "500", "min_quote_reserve": "50"}],
            protected_assets=["PURR"],
            quote_asset="USD",
            tradable_pairs=["HYPE-USD", "DOGE-USD"],
        )

        by_asset = {policy["base_asset"]: policy for policy in policies}
        self.assertEqual(set(by_asset), {"DOGE", "HYPE"})
        self.assertEqual(by_asset["HYPE"]["mode"], "buy_sell")
        self.assertEqual(by_asset["HYPE"]["pair_id"], "HYPE-USD")
        self.assertEqual(by_asset["HYPE"]["max_symbol_exposure_quote"], "500")
        self.assertEqual(by_asset["DOGE"]["mode"], "sell_only")
        self.assertEqual(by_asset["DOGE"]["max_sell_fraction_per_tick"], "0.20")

    def test_wallet_policy_builder_accepts_usd_usdc_quote_aliases_from_connector_symbols(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_pair_policies_from_wallet

        wallet = {
            "quote_asset": "USDC",
            "balances": {
                "USDC": {"total_units": "100", "available_units": "100"},
                "HYPE": {"total_units": "2", "available_units": "2"},
            },
        }

        policies = build_pair_policies_from_wallet(
            portfolio_snapshot=wallet,
            buy_sell_assets=["HYPE"],
            quote_asset="USDC",
            tradable_pairs=["HYPE-USD"],
        )

        self.assertEqual(len(policies), 1)
        self.assertEqual(policies[0]["pair_id"], "HYPE-USD")
        self.assertEqual(policies[0]["quote_asset"], "USD")
        self.assertTrue(policies[0]["enabled"])
        self.assertEqual(policies[0]["symbol_mapping_status"], "resolved")

    def test_wallet_policy_builder_prefers_requested_quote_over_same_base_market(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_pair_policies_from_wallet

        policies = build_pair_policies_from_wallet(
            portfolio_snapshot={"quote_asset": "USDC", "balances": {}},
            buy_sell_assets=["HYPE"],
            quote_asset="USDC",
            symbol_map={
                "HYPE-USDC": "HYPE-USDC",
                "HYPE": "HYPE-USDE",
                "HYPE-USDE": "HYPE-USDE",
            },
            tradable_pairs=["HYPE-USDC", "HYPE-USDE"],
        )

        self.assertEqual(policies[0]["pair_id"], "HYPE-USDC")
        self.assertEqual(policies[0]["quote_asset"], "USDC")
        self.assertTrue(policies[0]["enabled"])

    def test_wallet_policy_builder_keeps_unresolved_wallet_assets_disabled(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_pair_policies_from_wallet

        wallet = {
            "quote_asset": "USDC",
            "balances": {
                "USDC": {"total_units": "100", "available_units": "100"},
                "UNKNOWN": {"total_units": "10", "available_units": "10"},
            },
        }

        policies = build_pair_policies_from_wallet(
            portfolio_snapshot=wallet,
            buy_sell_assets=[],
            quote_asset="USDC",
            tradable_pairs=["HYPE-USD"],
        )

        self.assertEqual(len(policies), 1)
        self.assertEqual(policies[0]["pair_id"], "UNKNOWN-USDC")
        self.assertEqual(policies[0]["mode"], "sell_only")
        self.assertFalse(policies[0]["enabled"])
        self.assertEqual(policies[0]["symbol_mapping_status"], "unresolved")

    def test_portfolio_plan_contract_shape_includes_replay_fields(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        features = load_fixture("feature_snapshot.json")

        plan = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=wallet,
            signal_snapshots=features,
            risk_gate_state={
                "request_id": "fixture-request-001",
                "shadow_mode": True,
                "manual_kill_switch": False,
            },
        )
        payload = plan.to_dict()

        self.assertEqual(payload["schema"], "rsibot.hyperliquid_portfolio_rsi.portfolio_plan.v1")
        for field_name in {
            "plan_id",
            "request_id",
            "as_of",
            "mode",
            "readiness",
            "created_order_count",
            "portfolio_reasons",
            "rows",
        }:
            self.assertIn(field_name, payload)
        self.assertEqual(payload["request_id"], "fixture-request-001")
        self.assertEqual(payload["as_of"], "2026-07-09T12:00:00Z")
        self.assertEqual(payload["rows"][0]["evidence_cutoff"], "2026-07-09T11:59:00Z")

    def test_shadow_plan_has_one_row_per_configured_pair_and_no_orders(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        features = load_fixture("feature_snapshot.json")

        plan = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=wallet,
            signal_snapshots=features,
            risk_gate_state={"shadow_mode": True, "manual_kill_switch": False},
        )

        self.assertEqual(plan.mode, "shadow")
        self.assertEqual(plan.created_order_count, 0)
        self.assertEqual(len(plan.rows), 25)
        self.assertEqual({row.pair_id for row in plan.rows}, {policy["pair_id"] for policy in policies})
        for row in plan.rows:
            self.assertEqual(row.target_action, "none")
            self.assertEqual(row.proposed_units, "0")
            self.assertTrue(row.skip_reasons)

    def test_sell_only_pair_caps_sell_and_never_buys_even_with_buy_score(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        features = load_fixture("feature_snapshot.json")
        features["pairs"] = {
            "SELL01-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "buy_score": "1.0",
                "sell_score": "1.0",
                "feature_cutoff": "2026-07-09T11:59:00Z",
            }
        }

        plan = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=wallet,
            signal_snapshots=features,
            risk_gate_state={"shadow_mode": True, "manual_kill_switch": False},
        )

        row = next(row for row in plan.rows if row.pair_id == "SELL01-USDC")
        self.assertEqual(row.policy_mode, "sell_only")
        self.assertEqual(row.target_action, "sell")
        self.assertEqual(row.proposed_units, "20")
        self.assertEqual(row.proposed_value_quote, "20")
        self.assertIn("max_sell_fraction_per_tick", row.caps_applied)
        self.assertEqual(row.skip_reasons, [])

    def test_sell_only_dust_below_min_trade_value_is_skipped(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        wallet["balances"]["SELL02"] = {
            "total_units": "2",
            "available_units": "2",
            "price_quote": "1.00",
            "precision": "0.0001",
            "min_trade_value_quote": "5",
        }
        features = load_fixture("feature_snapshot.json")
        features["pairs"] = {
            "SELL02-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "sell_score": "1.0",
            }
        }

        plan = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=wallet,
            signal_snapshots=features,
            risk_gate_state={"shadow_mode": True, "manual_kill_switch": False},
        )

        row = next(row for row in plan.rows if row.pair_id == "SELL02-USDC")
        self.assertEqual(row.target_action, "none")
        self.assertEqual(row.proposed_units, "0")
        self.assertIn("min_trade_value", row.skip_reasons)

    def test_sell_only_open_order_reservations_reduce_sellable_units(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        wallet["open_orders"] = [
            {
                "pair_id": "SELL01-USDC",
                "base_asset": "SELL01",
                "side": "sell",
                "remaining_units": "70",
            }
        ]
        features = load_fixture("feature_snapshot.json")
        features["pairs"] = {
            "SELL01-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "sell_score": "1.0",
            }
        }

        plan = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=wallet,
            signal_snapshots=features,
            risk_gate_state={"shadow_mode": True, "manual_kill_switch": False},
        )

        row = next(row for row in plan.rows if row.pair_id == "SELL01-USDC")
        self.assertEqual(row.target_action, "sell")
        self.assertEqual(row.available_units, "30")
        self.assertEqual(row.proposed_units, "6")
        self.assertIn("open_order_reservation", row.caps_applied)

    def test_external_signal_evidence_must_be_fresh_resolved_and_cutoff(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        cases = [
            (
                "stale",
                {
                    "freshness_status": "stale",
                    "symbol_mapping_status": "resolved",
                    "feature_cutoff": "2026-07-09T11:59:00Z",
                    "sell_score": "1.0",
                },
                "feature_stale",
            ),
            (
                "unresolved",
                {
                    "freshness_status": "fresh",
                    "symbol_mapping_status": "unresolved",
                    "feature_cutoff": "2026-07-09T11:59:00Z",
                    "sell_score": "1.0",
                },
                "symbol_unresolved",
            ),
            (
                "missing_cutoff",
                {
                    "freshness_status": "fresh",
                    "symbol_mapping_status": "resolved",
                    "sell_score": "1.0",
                },
                "feature_cutoff_missing",
            ),
        ]

        for case_name, pair_features, expected_reason in cases:
            with self.subTest(case_name=case_name):
                features = {
                    "as_of": "2026-07-09T12:00:00Z",
                    "freshness_status": "fresh",
                    "symbol_mapping_status": "resolved",
                    "pairs": {"SELL01-USDC": pair_features},
                }

                plan = build_portfolio_plan(
                    pair_policies=policies,
                    portfolio_snapshot=wallet,
                    signal_snapshots=features,
                    risk_gate_state={"shadow_mode": True, "manual_kill_switch": False},
                )

                row = next(row for row in plan.rows if row.pair_id == "SELL01-USDC")
                self.assertEqual(row.target_action, "none")
                self.assertEqual(row.proposed_units, "0")
                self.assertIn(expected_reason, row.skip_reasons)

    def test_buy_sell_pair_can_buy_when_reserve_and_exposure_pass(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        features = load_fixture("feature_snapshot.json")
        features["pairs"] = {
            "TRADE01-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "buy_score": "1.0",
                "price_quote": "10",
            }
        }

        plan = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=wallet,
            signal_snapshots=features,
            risk_gate_state={
                "shadow_mode": True,
                "manual_kill_switch": False,
                "gross_exposure_quote": "1000",
                "gross_exposure_cap_quote": "2000",
            },
        )

        row = next(row for row in plan.rows if row.pair_id == "TRADE01-USDC")
        self.assertEqual(row.policy_mode, "buy_sell")
        self.assertEqual(row.target_action, "buy")
        self.assertEqual(row.proposed_value_quote, "200")
        self.assertEqual(row.proposed_units, "20")
        self.assertIn("buy_score_threshold", row.signal_reasons)
        self.assertIn("max_symbol_exposure_quote", row.caps_applied)

    def test_buy_sell_pair_buy_is_blocked_by_quote_reserve_or_symbol_exposure(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        features = load_fixture("feature_snapshot.json")
        features["pairs"] = {
            "TRADE01-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "buy_score": "1.0",
                "price_quote": "10",
            }
        }

        low_quote_wallet = load_fixture("wallet_snapshot.json")
        low_quote_wallet["quote_available"] = "99"
        reserve_plan = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=low_quote_wallet,
            signal_snapshots=features,
            risk_gate_state={"shadow_mode": True, "manual_kill_switch": False},
        )
        reserve_row = next(row for row in reserve_plan.rows if row.pair_id == "TRADE01-USDC")
        self.assertEqual(reserve_row.target_action, "none")
        self.assertIn("quote_reserve", reserve_row.skip_reasons)

        full_exposure_wallet = load_fixture("wallet_snapshot.json")
        full_exposure_wallet["balances"]["TRADE01"] = {
            "total_units": "20",
            "available_units": "20",
            "price_quote": "10",
            "precision": "0.0001",
            "min_trade_value_quote": "5",
        }
        exposure_plan = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=full_exposure_wallet,
            signal_snapshots=features,
            risk_gate_state={"shadow_mode": True, "manual_kill_switch": False},
        )
        exposure_row = next(row for row in exposure_plan.rows if row.pair_id == "TRADE01-USDC")
        self.assertEqual(exposure_row.target_action, "none")
        self.assertIn("symbol_exposure_cap", exposure_row.skip_reasons)

    def test_manual_kill_switch_blocks_sell_and_buy_actions(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        features = load_fixture("feature_snapshot.json")
        features["pairs"] = {
            "SELL01-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "sell_score": "1.0",
            },
            "TRADE01-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "buy_score": "1.0",
                "price_quote": "10",
            },
        }

        plan = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=wallet,
            signal_snapshots=features,
            risk_gate_state={"shadow_mode": True, "manual_kill_switch": True},
        )

        sell_row = next(row for row in plan.rows if row.pair_id == "SELL01-USDC")
        buy_row = next(row for row in plan.rows if row.pair_id == "TRADE01-USDC")
        self.assertEqual(plan.readiness, "shadow_blocked")
        self.assertEqual(sell_row.target_action, "none")
        self.assertEqual(buy_row.target_action, "none")
        self.assertIn("manual_kill_switch", sell_row.skip_reasons)
        self.assertIn("manual_kill_switch", buy_row.skip_reasons)

    def test_turnover_cap_limits_buy_value_across_planning_tick(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        features = load_fixture("feature_snapshot.json")
        features["pairs"] = {
            "TRADE01-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "buy_score": "1.0",
                "price_quote": "10",
            },
            "TRADE02-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "buy_score": "1.0",
                "price_quote": "10",
            },
        }

        plan = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=wallet,
            signal_snapshots=features,
            risk_gate_state={
                "shadow_mode": True,
                "manual_kill_switch": False,
                "turnover_cap_quote": "250",
            },
        )

        first = next(row for row in plan.rows if row.pair_id == "TRADE01-USDC")
        second = next(row for row in plan.rows if row.pair_id == "TRADE02-USDC")
        self.assertEqual(first.target_action, "buy")
        self.assertEqual(first.proposed_value_quote, "200")
        self.assertEqual(second.target_action, "buy")
        self.assertEqual(second.proposed_value_quote, "50")
        self.assertIn("turnover_cap_quote", second.caps_applied)

    def test_max_orders_per_tick_blocks_additional_actions(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        features = load_fixture("feature_snapshot.json")
        features["pairs"] = {
            "TRADE01-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "buy_score": "1.0",
                "price_quote": "10",
            },
            "TRADE02-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "buy_score": "1.0",
                "price_quote": "10",
            },
        }

        plan = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=wallet,
            signal_snapshots=features,
            risk_gate_state={
                "shadow_mode": True,
                "manual_kill_switch": False,
                "max_orders_per_tick": 1,
            },
        )

        first = next(row for row in plan.rows if row.pair_id == "TRADE01-USDC")
        second = next(row for row in plan.rows if row.pair_id == "TRADE02-USDC")
        self.assertEqual(first.target_action, "buy")
        self.assertEqual(second.target_action, "none")
        self.assertEqual(second.proposed_value_quote, "0")
        self.assertIn("max_orders_per_tick", second.skip_reasons)

    def test_max_orders_per_tick_prefers_higher_signal_quality(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        features = load_fixture("feature_snapshot.json")
        features["pairs"] = {
            "TRADE01-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "buy_score": "0.6",
                "price_quote": "10",
            },
            "TRADE02-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "buy_score": "0.9",
                "price_quote": "10",
            },
        }

        plan = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=wallet,
            signal_snapshots=features,
            risk_gate_state={
                "shadow_mode": True,
                "manual_kill_switch": False,
                "max_orders_per_tick": 1,
            },
        )

        lower_quality = next(row for row in plan.rows if row.pair_id == "TRADE01-USDC")
        higher_quality = next(row for row in plan.rows if row.pair_id == "TRADE02-USDC")
        self.assertEqual(higher_quality.target_action, "buy")
        self.assertEqual(lower_quality.target_action, "none")
        self.assertIn("max_orders_per_tick", lower_quality.skip_reasons)

    def test_cooldown_blocks_churn_actions(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        features = load_fixture("feature_snapshot.json")
        features["pairs"] = {
            "TRADE01-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "buy_score": "1.0",
                "price_quote": "10",
            }
        }

        plan = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=wallet,
            signal_snapshots=features,
            risk_gate_state={
                "shadow_mode": True,
                "manual_kill_switch": False,
                "pair_cooldowns": {"TRADE01-USDC": 120},
            },
        )

        row = next(row for row in plan.rows if row.pair_id == "TRADE01-USDC")
        self.assertEqual(row.target_action, "none")
        self.assertEqual(row.proposed_units, "0")
        self.assertIn("pair_cooldown", row.skip_reasons)

    def test_loss_and_drawdown_pauses_block_buy_actions(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        features = load_fixture("feature_snapshot.json")
        features["pairs"] = {
            "TRADE01-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "buy_score": "1.0",
                "price_quote": "10",
            }
        }

        for gate_state, expected_reason in [
            ({"daily_loss_pct": "0.05", "daily_loss_limit_pct": "0.03"}, "daily_loss_limit"),
            ({"drawdown_pct": "0.12", "drawdown_limit_pct": "0.10"}, "drawdown_limit"),
        ]:
            with self.subTest(expected_reason=expected_reason):
                plan = build_portfolio_plan(
                    pair_policies=policies,
                    portfolio_snapshot=wallet,
                    signal_snapshots=features,
                    risk_gate_state={
                        "shadow_mode": True,
                        "manual_kill_switch": False,
                        **gate_state,
                    },
                )

                row = next(row for row in plan.rows if row.pair_id == "TRADE01-USDC")
                self.assertEqual(row.target_action, "none")
                self.assertEqual(row.proposed_units, "0")
                self.assertIn(expected_reason, row.skip_reasons)

    def test_partial_wallet_refresh_blocks_shadow_readiness_and_actions(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        wallet["refresh_status"] = "partial"
        features = load_fixture("feature_snapshot.json")
        features["pairs"] = {
            "SELL01-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "sell_score": "1.0",
            },
            "TRADE01-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "buy_score": "1.0",
                "price_quote": "10",
            },
        }

        plan = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=wallet,
            signal_snapshots=features,
            risk_gate_state={"shadow_mode": True, "manual_kill_switch": False},
        )

        self.assertEqual(plan.readiness, "shadow_blocked")
        sell_row = next(row for row in plan.rows if row.pair_id == "SELL01-USDC")
        buy_row = next(row for row in plan.rows if row.pair_id == "TRADE01-USDC")
        self.assertEqual(sell_row.target_action, "none")
        self.assertEqual(buy_row.target_action, "none")
        self.assertIn("wallet_partial", sell_row.skip_reasons)
        self.assertIn("wallet_partial", buy_row.skip_reasons)

    def test_paper_and_live_modes_remain_blocked_by_shadow_planner(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        features = load_fixture("feature_snapshot.json")

        for mode, expected_readiness in [("paper", "paper_blocked"), ("live", "live_blocked")]:
            with self.subTest(mode=mode):
                plan = build_portfolio_plan(
                    pair_policies=policies,
                    portfolio_snapshot=wallet,
                    signal_snapshots=features,
                    risk_gate_state={
                        "shadow_mode": False,
                        "mode": mode,
                        "manual_kill_switch": False,
                    },
                )

                self.assertEqual(plan.mode, mode)
                self.assertEqual(plan.readiness, expected_readiness)
                self.assertEqual(plan.created_order_count, 0)

    def test_explicit_paper_gate_allows_bounded_buy_plan(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        plan = build_portfolio_plan(
            pair_policies=[{
                "pair_id": "HYPE-USDC",
                "base_asset": "HYPE",
                "quote_asset": "USDC",
                "mode": "buy_sell",
                "max_symbol_exposure_quote": "500",
                "min_quote_reserve": "100",
                "min_trade_value_quote": "10",
            }],
            portfolio_snapshot={
                "snapshot_id": "paper-enabled",
                "as_of": "2026-07-09T12:00:00Z",
                "refresh_status": "fresh",
                "quote_available": "1000",
                "balances": {
                    "USDC": {"total_units": "1000", "available_units": "1000", "price_quote": "1"},
                    "HYPE": {"total_units": "0", "available_units": "0", "price_quote": "50"},
                },
            },
            signal_snapshots={
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "pairs": {
                    "HYPE-USDC": {
                        "freshness_status": "fresh",
                        "symbol_mapping_status": "resolved",
                        "feature_cutoff": "2026-07-09T11:59:00Z",
                        "buy_score": "1",
                        "sell_score": "0",
                        "price_quote": "50",
                    }
                },
            },
            risk_gate_state={
                "shadow_mode": False,
                "mode": "paper",
                "paper_actions_enabled": True,
                "usd_per_entry_quote": "30",
                "max_orders_per_tick": 1,
            },
        )

        self.assertEqual(plan.readiness, "paper_ready")
        self.assertEqual(plan.rows[0].target_action, "buy")
        self.assertEqual(plan.rows[0].proposed_value_quote, "30")
        self.assertEqual(plan.rows[0].proposed_units, "0.6")
        self.assertIn("usd_per_entry_quote", plan.rows[0].caps_applied)

    def test_buy_sell_policy_prefers_sell_signal_and_caps_wallet_units(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        plan = build_portfolio_plan(
            pair_policies=[{
                "pair_id": "HYPE-USDC",
                "base_asset": "HYPE",
                "quote_asset": "USDC",
                "mode": "buy_sell",
                "max_sell_fraction_per_tick": "0.25",
                "max_symbol_exposure_quote": "500",
                "min_quote_reserve": "100",
                "min_trade_value_quote": "10",
            }],
            portfolio_snapshot={
                "snapshot_id": "buy-sell-sell",
                "as_of": "2026-07-09T12:00:00Z",
                "refresh_status": "fresh",
                "quote_available": "1000",
                "balances": {
                    "HYPE": {
                        "total_units": "4",
                        "available_units": "4",
                        "price_quote": "50",
                        "min_trade_value_quote": "10",
                    }
                },
            },
            signal_snapshots={
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "pairs": {
                    "HYPE-USDC": {
                        "freshness_status": "fresh",
                        "symbol_mapping_status": "resolved",
                        "feature_cutoff": "2026-07-09T11:59:00Z",
                        "buy_score": "1",
                        "sell_score": "1",
                        "price_quote": "50",
                    }
                },
            },
            risk_gate_state={"shadow_mode": True, "manual_kill_switch": False},
        )

        self.assertEqual(plan.rows[0].target_action, "sell")
        self.assertEqual(plan.rows[0].proposed_units, "1")
        self.assertEqual(plan.rows[0].proposed_value_quote, "50")
        self.assertIn("max_sell_fraction_per_tick", plan.rows[0].caps_applied)

    def test_replaying_same_fixture_produces_identical_plan_payload(self):
        from controllers.directional_trading.hyperliquid_portfolio_rsi import build_portfolio_plan

        policies = load_fixture("policies.json")["pairs"]
        wallet = load_fixture("wallet_snapshot.json")
        features = load_fixture("feature_snapshot.json")
        features["pairs"] = {
            "SELL01-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "sell_score": "1.0",
            },
            "TRADE01-USDC": {
                "freshness_status": "fresh",
                "symbol_mapping_status": "resolved",
                "feature_cutoff": "2026-07-09T11:59:00Z",
                "buy_score": "1.0",
                "price_quote": "10",
            },
        }
        risk_gate_state = {
            "request_id": "replay-fixture-001",
            "shadow_mode": True,
            "manual_kill_switch": False,
            "turnover_cap_quote": "250",
            "max_orders_per_tick": 2,
        }

        first = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=wallet,
            signal_snapshots=features,
            risk_gate_state=risk_gate_state,
        )
        second = build_portfolio_plan(
            pair_policies=policies,
            portfolio_snapshot=wallet,
            signal_snapshots=features,
            risk_gate_state=risk_gate_state,
        )

        self.assertEqual(first.to_dict(), second.to_dict())


if __name__ == "__main__":
    unittest.main()
