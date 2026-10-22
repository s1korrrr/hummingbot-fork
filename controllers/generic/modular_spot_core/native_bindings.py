"""Narrow native imports and effects. No orders are submitted by this adapter."""
from decimal import ROUND_FLOOR

from .domain import DataUnavailable, PolicyError, decimal


class NativeBindings:
    metadata_model = None

    @staticmethod
    def recorder():
        from hummingbot.connector.markets_recorder import MarketsRecorder
        return MarketsRecorder.get_existing_instance()

    # Owner dependency: ExecutorOrchestrator._claim_planned_positions constructs
    # PositionExecutor directly and never calls get_executor_class_for_config.
    UNSUPPORTED_PLANNED_RESUME = (
        'native planned resume bypasses local executor selection; not supported '
        '(ExecutorOrchestrator._claim_planned_positions constructs PositionExecutor directly)'
    )

    @staticmethod
    def guard_resume(controller, recorder):
        if recorder is None:
            return
        from hummingbot.strategy_v2.executors.planned_position_resume import pending_manifest
        path = getattr(getattr(recorder, 'sql_manager', None), 'db_path', None)
        try:
            manifest = pending_manifest(path) if path else None
        except (TypeError, ValueError, OSError) as exc:
            raise PolicyError(NativeBindings.UNSUPPORTED_PLANNED_RESUME) from exc
        if manifest and any(p['controller_id'] == controller.config.id for p in manifest['positions']):
            raise PolicyError(NativeBindings.UNSUPPORTED_PLANNED_RESUME)

    @staticmethod
    def trend(frame, **kwargs):
        from hummingbot.strategy_v2.utils.market_analysis import calculate_trend_state
        return calculate_trend_state(frame, **kwargs)

    @staticmethod
    def spot_connector(controller):
        """The registered native centralized spot connector; the only market snapshot owner."""
        from hummingbot.client.settings import AllConnectorSettings, ConnectorType
        c, p = controller.config, controller.market_data_provider
        setting = AllConnectorSettings.get_connector_settings().get(c.connector_name)
        if setting is None or setting.type != ConnectorType.Exchange:
            raise PolicyError('only native centralized spot connectors are supported')
        return p.connectors[c.connector_name]

    @classmethod
    def snapshot(cls, controller):
        from hummingbot.core.data_type.common import PriceType
        c, p = controller.config, controller.market_data_provider
        connector = cls.spot_connector(controller)
        if p.ready is not True or connector.ready is not True:
            raise DataUnavailable('native market/account provider not ready')
        if getattr(connector, 'supports_terminal_fill_reconciliation', False) is not True:
            raise PolicyError('authoritative terminal fill reconciliation is required')
        report = controller.performance_report
        if report is None or report.pnl_available is not True or report.global_pnl_quote is None:
            raise DataUnavailable('native marked performance unavailable')
        if controller.native_report_age() > c.max_native_report_age_seconds:
            raise DataUnavailable('native controller report is stale')
        pnl = decimal(report.global_pnl_quote, 'native marked pnl', minimum=None)
        bid = decimal(p.get_price_by_type(c.connector_name, c.trading_pair, PriceType.BestBid), 'bid', positive=True)
        ask = decimal(p.get_price_by_type(c.connector_name, c.trading_pair, PriceType.BestAsk), 'ask', positive=True)
        available = decimal(connector.get_available_balance(c.trading_pair.split('-')[1]), 'available quote')
        rules = connector.trading_rules[c.trading_pair]
        tick = decimal(rules.min_price_increment, 'price increment', positive=True)
        # A buy limit must not round above the price used to reserve quote capital.
        bid = (bid / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
        return dict(bid=bid, ask=ask, available_quote=available, pnl=pnl,
                    amount_step=rules.min_base_amount_increment, minimum_amount=rules.min_order_size,
                    minimum_notional=rules.min_notional_size)

    @staticmethod
    def fees_permit(controller, config):
        from hummingbot.core.data_type.common import OrderType, TradeType
        connector = controller.market_data_provider.connectors[config.connector_name]
        base, quote = config.trading_pair.split('-')
        fee = connector.get_fee(base_currency=base, quote_currency=quote, order_type=OrderType.LIMIT,
                                order_side=TradeType.BUY, amount=config.amount, price=config.entry_price, is_maker=False)
        if fee.percent and fee.percent_token not in (None, base, quote):
            return False
        rate = decimal(fee.percent, 'entry fee')
        for item in fee.flat_fees:
            amount = decimal(item.amount, 'flat entry fee')
            if item.token not in (base, quote):
                return False
            rate += amount * (config.entry_price if item.token == base else 1) / (config.amount * config.entry_price)
        return rate <= controller.config.risk.fee_reserve_fraction

    @staticmethod
    def config(controller_config, proposal, market=None):
        from .execution import native_config
        return native_config(controller_config, identifier=proposal.identifier, timestamp=proposal.timestamp,
                             price=proposal.price, amount=proposal.amount, cycle=proposal.cycle, stage=proposal.stage,
                             market=market)

    @staticmethod
    def action(controller_id, config):
        from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
        return CreateExecutorAction(controller_id=controller_id, executor_config=config)

    @staticmethod
    def executor_class():
        from .execution import ModularPositionExecutor
        return ModularPositionExecutor
