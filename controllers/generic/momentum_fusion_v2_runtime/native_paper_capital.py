"""Complete conservative capital evidence for one explicitly isolated native paper session."""
import threading

from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.strategy_v2.components.capital_policy import CapitalSnapshot, NativeExposure
from hummingbot.strategy_v2.components.domain import PolicyError, identity

PROGRAM = 'okx_spot_momentum_v2'


class NativePaperAccountEvidence:
    def __init__(self, session):
        from hummingbot.strategy_v2.paper_session import NativePaperSession
        if type(session) is not NativePaperSession or session._closed:
            raise PolicyError('a genuine isolated native paper session is required')
        self.session = session
        self.account_id = 'paper-' + identity(str(session._sql.db_path))[:32]

    def snapshot(self):
        from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import is_native_paper_connector
        s, market = self.session, self.session._connector
        if (threading.current_thread() is not threading.main_thread() or s._closed
                or not is_native_paper_connector(market) or market._terminal_recorder is not s.recorder):
            raise PolicyError('native paper account binding unavailable')
        # One synchronous event-loop turn plus the claim-store transaction is
        # the admission boundary. This is not a shared exchange-wallet snapshot.
        balances = market.get_all_balances()
        exposures, marks = [], {}
        for asset, quantity in sorted(balances.items()):
            if asset == 'USDC' or quantity == 0:
                continue
            pair = asset + '-USDC'
            if pair not in market.trading_pairs or not market.pair_ready(pair):
                raise PolicyError('complete native account valuation unavailable')
            bid = market.get_price_by_type(pair, PriceType.BestBid)
            if not bid.is_finite() or bid <= 0:
                raise PolicyError('native account bid unavailable')
            marks[pair] = str(bid)
            exposures.append(NativeExposure(executor_id='inventory-' + asset, program_id=PROGRAM,
                                            trading_pair=pair, marked_inventory_quote=quantity * bid,
                                            reserved_acquisition_quote=0))
        for oid, order in sorted(market.in_flight_orders.items()):
            if order.trade_type != TradeType.BUY:
                continue
            if not order.trading_pair.endswith('-USDC'):
                raise PolicyError('native account quote scope differs')
            candidate = market._candidate(order, order.amount - order.executed_amount_base, order.price,
                                          market._reservation_maker(order))
            reserve = candidate.collateral_dict.get('USDC')
            if reserve is None or any(asset != 'USDC' for asset in candidate.collateral_dict):
                raise PolicyError('native acquisition collateral unavailable')
            exposures.append(NativeExposure(executor_id=oid, program_id=PROGRAM, trading_pair=order.trading_pair,
                                            marked_inventory_quote=0, reserved_acquisition_quote=reserve))
        # Charge the entire paper account against this program as a conservative
        # envelope. This can overcount peer claims, never free their capacity.
        source = identity({'account': self.account_id, 'market': market.tracking_states})
        return CapitalSnapshot(account_id=self.account_id, valuation_currency='USDC',
                               observed_at_ms=int(s.current_timestamp * 1000), source_sha256=source,
                               valuation_sha256=identity(marks), complete=not market._terminal_orders,
                               reconciled=not market._terminal_orders and not market._pending,
                               risk_paused=s._failure is not None,
                               available_quote=market.get_available_balance('USDC'), exposures=tuple(exposures))

    def terminal_claim_receipt(self, claim):
        from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import _PaperInFlightOrder
        from hummingbot.connector.exchange.paper_trade.terminal_recorder import recorded_terminal_order

        # Executor absence, a callback's is_done, or a timer never proves release.
        infos = self.session.recorder.get_executors_by_controller(claim.controller_id)
        info = next((i for i in infos if i.id == (claim.seen_executor_id or claim.executor_identity)), None)
        if (info is None or not info.is_done or info.config.id != claim.executor_identity
                or info.config.controller_id != claim.controller_id):
            return False
        rows = (info.custom_info or {}).get('execution_order_snapshots')
        if not isinstance(rows, list) or not rows:
            return False
        proofs = []
        from hummingbot.core.data_type.terminal_reconciliation import terminal_reconciliation
        for row in rows:
            order = _PaperInFlightOrder.from_json(row)
            if order.trading_pair != claim.trading_pair:
                return False
            recorded = recorded_terminal_order(self.session.recorder, self.session._connector, order)
            if recorded is None:
                return None
            proofs.append(terminal_reconciliation(recorded, 'native_paper_terminal_v1',
                                                  recorded.current_state.name, recorded.executed_amount_base))
        return {'account_id': self.account_id, 'executor_id': info.id, 'orders': proofs}
