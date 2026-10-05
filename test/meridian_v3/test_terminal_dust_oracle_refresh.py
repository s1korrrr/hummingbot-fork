"""A rate-oracle refresh must not hold Meridian entries through the terminal-dust proof.

Live V3 (2026-10-04/05): a closed tranche with sub-lot dust stays in the orchestrator's
active executors. Its order snapshots carry cumulative fees re-valued through the rate
oracle on every read, so each oracle refresh made the in-memory terminal-dust proof differ
from the recorder until the next 10 s sync. Every grant read then raised, all five symbols
held, and resting entries were revoked within seconds. Real controller, portfolio, symbol
runtimes, grant store, executors and recorder on the native paper engine; the only fixture
change from lifecycle_harness is that the BUY fee is taken in base, as on OKX.
"""
import tempfile
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v3 import lifecycle_harness
from test.meridian_v3.lifecycle_harness import Scenario, creates, rebound
from test.meridian_v3.test_unified_runtime import OWNER
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from controllers.generic.meridian_v3_recipe import PAIRS
from controllers.generic.modular_spot_core.domain import PolicyError
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import NativePaperTradeExchange
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.trade_fee import TradeFeeSchema
from hummingbot.core.rate_oracle.rate_oracle import RateOracle

BTC = 'BTC-USDC'


def okx_like_paper(pairs, cash='100'):
    """lifecycle_harness.paper, except the BUY fee is deducted from the received base (OKX spot)."""
    rules = {pair: TradingRule(pair, min_order_size=D('.001'), min_price_increment=D('.01'),
                               min_base_amount_increment=D('.001'), min_notional_size=D('1')) for pair in pairs}
    market = NativePaperTradeExchange(rules, fee_schema=TradeFeeSchema(
        maker_percent_fee_decimal=D('.001'), taker_percent_fee_decimal=D('.002'),
        buy_percent_fee_deducted_from_returns=True), trade_participation=D('1'))
    for pair in pairs:
        market.set_balance(pair.split('-')[0], D('0'))
    market.set_balance('USDC', D(cash))
    market.tick(1000)
    for pair in pairs:
        market.set_order_book(pair, 1, 1000, [(99.5, 10)], [(99.51, 10)])
    return market


def raise_site(exc):
    tb = exc.__traceback__
    while tb.tb_next is not None:
        tb = tb.tb_next
    return f'{Path(tb.tb_frame.f_code.co_filename).name}:{tb.tb_lineno} in {tb.tb_frame.f_code.co_name}'


class TerminalDustOracleRefreshTests(IsolatedAsyncioTestCase):
    setUp = session_tests.NativePaperSessionTests.setUp  # denies all network connections

    async def asyncSetUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.directory = Path(self._directory.name)
        self.scenario = None

    async def asyncTearDown(self):
        if self.scenario is not None and not self.scenario.session._closed:
            await self.scenario.close()
        self._directory.cleanup()

    def held(self, s):
        return {pair: s.controller.contexts[pair].runtime.reason for pair in PAIRS
                if s.controller.contexts[pair].runtime.reason.startswith('portfolio_grant_hold')}

    async def test_rate_oracle_refresh_keeps_admission_and_resting_entries(self):
        with patch.object(lifecycle_harness, 'paper', okx_like_paper):
            s = self.scenario = Scenario(self.directory)
        # One BTC tranche whose trailing exit leaves sub-lot dust and stays in active executors.
        await s.step()
        [create] = creates(await s.step(rsi={BTC: rebound()}), BTC)
        await s.fill(BTC, '1')
        [executor] = [e for e in s.executors(BTC) if e.config.id == create.executor_config.id]
        for mid in ('104.5', '105.5', '104.9'):
            await s.step(mids={BTC: mid})
        await s.lift(BTC)
        self.assertTrue(executor.is_closed)
        self.assertGreater(executor.get_custom_info()['execution_receipt']['residual_dust_base'], 0)
        self.assertIn(executor, s.executors(BTC))

        portfolio, oracle = s.controller.portfolio, RateOracle.get_instance()
        recorded = next(i for i in s.session.recorder.get_all_executors() if i.id == executor.config.id)
        oracle.set_price(BTC, oracle.get_pair_rate(BTC) * D('1.001'))  # one oracle refresh, before any sync
        s.session._refresh_reports(controller_ids={OWNER})
        live = next(i for i in s.controller.executors_info if i.id == executor.config.id)
        self.assertNotEqual(  # the valuation really moved between the two reads
            [o['cumulative_fee_paid_base'] for o in recorded.custom_info['execution_order_snapshots']],
            [o['cumulative_fee_paid_base'] for o in live.custom_info['execution_order_snapshots']])
        portfolio.begin_tick()
        portfolio.evidence()
        for pair in PAIRS:
            portfolio.begin_tick()
            s.controller.contexts[pair].runtime.acquisition_permitted()
        self.assertEqual(self.held(s), {})

        # A full native step that moves the oracle holds nothing.
        await s.step(minutes=0, seconds=4, mids={BTC: '104.8'})
        self.assertEqual(self.held(s), {})

        # A resting entry on another symbol keeps its permission across a refresh.
        await s.step(minutes=1)
        [entry] = creates(await s.step(rsi={'ETH-USDC': rebound()}), 'ETH-USDC')
        [eth] = [e for e in s.executors('ETH-USDC') if e.config.id == entry.executor_config.id]
        self.assertTrue(s.controller.entry_permission(eth.config, check_funds=False))
        oracle.set_price(BTC, oracle.get_pair_rate(BTC) * D('1.001'))
        s.session._refresh_reports(controller_ids={OWNER})
        self.assertTrue(s.controller.entry_permission(eth.config, check_funds=False))

        # The proof still guards receipts: a changed fill fee in the live executor is refused.
        order = executor._exit_orders()[0].order
        fill = next(iter(order.order_fills.values()))
        flat = fill.fee.flat_fees
        if flat:
            flat[0] = type(flat[0])(token=flat[0].token, amount=flat[0].amount + D('0.000001'))
        else:
            fill.fee.percent += D('0.0001')
        s.session._refresh_reports(controller_ids={OWNER})
        portfolio.begin_tick()
        with self.assertRaisesRegex(PolicyError, 'terminal dust proof'):
            portfolio.evidence()
