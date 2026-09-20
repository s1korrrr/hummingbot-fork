import os
import time
from decimal import Decimal
from typing import Dict, List, Optional

from pydantic import Field, field_validator

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.event.events import MarketOrderFailureEvent
from hummingbot.remote_iface.mqtt import ExternalEventFactory
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.controllers.native_policy_identity import is_modular_policy, is_ok_rsi_policy
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class V2WithControllersConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    max_global_drawdown_quote: Optional[float] = None
    max_controller_drawdown_quote: Optional[float] = None
    max_daily_loss_quote: Optional[Decimal] = Field(default=None, gt=0, allow_inf_nan=False)

    @field_validator('max_global_drawdown_quote', 'max_controller_drawdown_quote', 'max_daily_loss_quote', mode='before')
    @classmethod
    def reject_boolean_limits(cls, value):
        if isinstance(value, bool):
            raise ValueError('Loss limits must be numeric, not boolean')
        return value

    runtime_db_sync_enabled: bool = True
    runtime_db_sync_interval: int = 10
    runtime_status_snapshot_enabled: bool = True
    runtime_status_snapshot_interval: int = 10
    runtime_status_snapshot_dir: str = "data/runtime_status"
    chart_snapshots_enabled: bool = True
    chart_snapshot_base_retention_days: int = 30
    chart_snapshot_higher_interval_retention_days: int = 90
    chart_snapshot_prune_interval: int = 3600


class V2WithControllers(StrategyV2Base):
    """
    This script runs a generic strategy with cash out feature. Will also check if the controllers configs have been
    updated and apply the new settings.
    The cash out of the script can be set by the time_to_cash_out parameter in the config file. If set, the script will
    stop the controllers after the specified time has passed, and wait until the active executors finalize their
    execution.
    The controllers will also have a parameter to manually cash out. In that scenario, the main strategy will stop the
    specific controller and wait until the active executors finalize their execution. The rest of the executors will
    wait until the main strategy stops them.
    """
    performance_report_interval: int = 1

    def __init__(self, connectors: Dict[str, ConnectorBase], config: V2WithControllersConfig):
        super().__init__(connectors, config)
        self.config = config
        from controllers.generic.modular_spot_core.native_funding import bind_funding_scope
        self._funding_scope_owners = bind_funding_scope(self.controllers)
        self.max_pnl_by_controller = {}
        self.max_global_pnl = Decimal("0")
        self.drawdown_exited_controllers = []
        self.closed_executors_buffer: int = 30
        self._last_performance_report_timestamp = 0
        self._operator_event_queue = ExternalEventFactory.create_queue("rsibot.operator")
        self._risk_persistence_enabled = any(is_ok_rsi_policy(c.config) or is_modular_policy(c.config)
                                             for c in self.controllers.values())
        self._operator_started_at = time.time()
        self._operator_watermarks = {}
        self._last_risk_state = None
        self._risk_reconciliation_error: Optional[str] = None
        self._daily_entry_risk = None
        if self._risk_persistence_enabled:
            try:
                self._validate_modular_loss_limits()
                recorder = MarketsRecorder.get_instance()
                state = recorder.load_strategy_risk_state()
                if state is None:
                    if recorder.get_all_executors():
                        raise RuntimeError('Existing execution history has no durable risk state; reconcile before restart')
                else:
                    if state.get('version') != 1 or set(state['controllers']) != set(self.controllers):
                        raise ValueError('Risk state version or controller scope mismatch')
                    if (set(state['max_pnl_by_controller']) != set(self.controllers)
                            or set(state['operator_paused']) != set(self.controllers)
                            or not set(state['drawdown_exited_controllers']).issubset(self.controllers)
                            or type(state['stopped']) is not bool
                            or any(type(paused) is not bool for paused in state['operator_paused'].values())):
                        raise ValueError('Incomplete or invalid durable risk state')
                    self._operator_watermarks = self._validate_operator_watermarks(state.get('operator_watermarks', {}))
                    self.max_global_pnl = Decimal(state['max_global_pnl'])
                    self.max_pnl_by_controller = {key: Decimal(value) for key, value in state['max_pnl_by_controller'].items()}
                    if not self.max_global_pnl.is_finite() or any(not x.is_finite() for x in self.max_pnl_by_controller.values()):
                        raise ValueError('Non-finite persisted risk peak')
                    self.drawdown_exited_controllers = state['drawdown_exited_controllers']
                    self._is_stop_triggered = state['stopped']
                    for cid, paused in state['operator_paused'].items():
                        self.controllers[cid].set_operator_entry_pause(paused, reason='restored', scope='bot')
                    for cid in self.drawdown_exited_controllers:
                        self.controllers[cid].stop()
                if (state is not None and state.get('daily_entry_risk') is not None
                        and self.config.max_daily_loss_quote is None):
                    raise ValueError('Persisted daily loss limit was removed; explicit reconciliation required')
                if self.config.max_daily_loss_quote is not None:
                    from hummingbot.strategy_v2.models.daily_entry_risk import DailyEntryRisk
                    self._daily_entry_risk = (DailyEntryRisk.new(self.config.max_daily_loss_quote, time.time())
                                              if state is None else DailyEntryRisk.restore(
                                                  state.get('daily_entry_risk'), self.config.max_daily_loss_quote))
                    self._apply_daily_entry_pause()
                self._persist_risk_state()
            except Exception as error:
                self._is_stop_triggered = True
                self._risk_reconciliation_error = str(error) or type(error).__name__
                self.logger().exception('Risk state unavailable; execution blocked pending reconciliation')

    def _persist_risk_state(self, *, operator_receipts=()):
        if not getattr(self, '_risk_persistence_enabled', False):
            return
        if getattr(self, '_risk_reconciliation_error', None) is not None:
            # Preserve missing/malformed evidence; defaults cannot reconstruct account peaks.
            return
        state = {'version': 1, 'controllers': sorted(self.controllers),
                 'max_global_pnl': str(self.max_global_pnl),
                 'max_pnl_by_controller': {cid: str(self.max_pnl_by_controller.get(cid, Decimal('0'))) for cid in self.controllers},
                 'drawdown_exited_controllers': sorted(set(self.drawdown_exited_controllers)),
                 'stopped': self._is_stop_triggered,
                 'operator_paused': {cid: bool(getattr(c, '_operator_entry_paused', False)) for cid, c in self.controllers.items()}}
        if getattr(self, '_operator_watermarks', {}):
            state['operator_watermarks'] = self._operator_watermarks.copy()
        if self._daily_entry_risk is not None:
            state['daily_entry_risk'] = self._daily_entry_risk.snapshot()
        if state != self._last_risk_state or operator_receipts:
            recorder = MarketsRecorder.get_instance()
            if operator_receipts:
                recorder.store_strategy_risk_state(state, operator_receipts=operator_receipts)
            else:
                recorder.store_strategy_risk_state(state)
            self._last_risk_state = state

    def validate_create_action(self, action):
        # Called after fresh reports and immediately before executor creation.
        if getattr(self, '_risk_reconciliation_error', None) is not None:
            self._is_stop_triggered = True
            return False
        try:
            if self._funding_scope_owners and (
                    set(self.controllers) != {owner.config.id for owner in self._funding_scope_owners}
                    or any(self.controllers.get(owner.config.id) is not owner for owner in self._funding_scope_owners)):
                raise ValueError('Funded controller topology changed; restart and reconcile explicitly')
            self.process_operator_events()
            self.check_manual_kill_switch()
            self.control_max_drawdown()
            self._observe_daily_entry_risk()
            self._persist_risk_state()
            buying = getattr(getattr(action, 'executor_config', None), 'side', TradeType.BUY) == TradeType.BUY
            daily_block = self._daily_entry_risk is not None and self._daily_entry_risk.paused and buying
            return not daily_block and not self._is_stop_triggered and action.controller_id not in self.drawdown_exited_controllers
        except Exception:
            self._is_stop_triggered = True
            self.logger().exception('Risk check or persistence failed; new execution blocked')
            return False

    def format_status(self) -> str:
        status = super().format_status()
        error = getattr(self, '_risk_reconciliation_error', None)
        if error is not None:
            return f"EXECUTION BLOCKED: risk reconciliation required: {error}\n\n{status}"
        if self._is_stop_triggered:
            return f"EXECUTION BLOCKED: strategy stop is latched; verify durable risk state before restart.\n\n{status}"
        return status

    def on_tick(self):
        super().on_tick()
        self.process_operator_events()
        # The parent defers report initialization until planned ownership is restored.
        if not self._is_stop_triggered and not self.executor_orchestrator._planned_resume_pending:
            self.check_manual_kill_switch()
            self.control_max_drawdown()
            self._observe_daily_entry_risk()
            self.send_performance_report()
        try:
            self._persist_risk_state()
        except Exception:
            self._is_stop_triggered = True
            self.logger().exception('Unable to persist risk state; new execution blocked')

    def _apply_daily_entry_pause(self):
        if self._daily_entry_risk is not None and self._daily_entry_risk.paused:
            for controller in self.controllers.values():
                if not getattr(controller, '_operator_entry_paused', False):
                    controller.set_operator_entry_pause(True, reason='daily-loss-limit', scope='global')

    def _observe_daily_entry_risk(self):
        if self._daily_entry_risk is None:
            return
        values = [self._drawdown_pnl(cid) for cid in self.controllers]
        if any(value is None for value in values):
            raise ValueError('Native PnL unavailable for daily entry risk')
        self._daily_entry_risk.observe(time.time(), sum(values, Decimal('0')))
        self._apply_daily_entry_pause()

    def _build_runtime_status_snapshot(self):
        result = super()._build_runtime_status_snapshot()
        result['daily_entry_risk'] = self._daily_entry_risk.snapshot() if self._daily_entry_risk is not None else None
        return result

    def _validate_operator_watermarks(self, watermarks):
        if not isinstance(watermarks, dict) or not set(watermarks).issubset(self.controllers):
            raise ValueError('Invalid operator command scope')
        for value in watermarks.values():
            if (not isinstance(value, dict) or set(value) != {'requested_at', 'command_id'}
                    or type(value['requested_at']) not in (float, int)
                    or not Decimal(str(value['requested_at'])).is_finite() or value['requested_at'] < 0
                    or not isinstance(value['command_id'], str) or not 1 <= len(value['command_id']) <= 128):
                raise ValueError('Invalid persisted operator command receipt')
        return watermarks

    def _native_operator_command_is_fresh(self, payload, targets):
        native = [c for c in targets if is_modular_policy(c.config)]
        if not native:
            return True
        if getattr(self, '_risk_reconciliation_error', None):
            return False
        timestamp, command_id = payload.get('requested_at'), payload.get('command_id')
        bot_id = HummingbotApplication.main_application().instance_id
        now = time.time()
        if (not isinstance(bot_id, str) or not bot_id or payload.get('bot_name') != bot_id
                or type(timestamp) not in (float, int) or not Decimal(str(timestamp)).is_finite()
                or not self._operator_started_at <= timestamp <= now + 5 or now - timestamp > 60
                or not isinstance(command_id, str) or not 1 <= len(command_id) <= 128):
            return False
        for controller in native:
            previous = self._operator_watermarks.get(controller.config.id)
            if previous and (timestamp <= previous['requested_at'] or command_id == previous['command_id']):
                return False
            try:
                if MarketsRecorder.get_instance().has_operator_command(controller.config.id, command_id):
                    return False
            except Exception:
                self._is_stop_triggered = True
                self._risk_reconciliation_error = 'Operator receipt store unavailable'
                self.logger().exception(self._risk_reconciliation_error)
                return False
        return True

    def process_operator_events(self):
        while len(self._operator_event_queue) > 0:
            event_name, event = self._operator_event_queue.popleft()
            payload = getattr(event, "data", {}) or {}
            if not isinstance(payload, dict):
                self.logger().warning('Rejected non-mapping operator event')
                continue
            action = str(payload.get("action") or "").strip()
            controller_id = payload.get("controller_id")
            requested_at = payload.get("requested_at")
            command_id = payload.get("command_id")

            target_controllers = (
                [self.controllers.get(controller_id)]
                if controller_id
                else list(self.controllers.values())
            )
            target_controllers = [controller for controller in target_controllers if controller is not None]

            if action not in {'global_pause_entries', 'bot_pause_entries', 'global_resume_entries',
                              'bot_resume_entries', 'bot_cancel_pending_entries'}:
                self.logger().warning('Unsupported operator action: %s', action)
                continue
            if not self._native_operator_command_is_fresh(payload, target_controllers):
                self.logger().warning('Rejected stale, duplicate or incorrectly addressed native operator command')
                continue
            if action in {'global_resume_entries', 'bot_resume_entries'} and self._daily_entry_risk is not None:
                self._observe_daily_entry_risk()
                if self._daily_entry_risk.paused:
                    if (action != 'global_resume_entries' or controller_id is not None
                            or not self._daily_entry_risk.acknowledge(time.time(), self._daily_entry_risk.last_pnl_quote)):
                        self._persist_risk_state()
                        self.logger().warning('Daily loss entry pause requires global acknowledgement after UTC rollover')
                        continue
            if action in {"global_pause_entries", "bot_pause_entries"}:
                scope = "global" if action == "global_pause_entries" else "bot"
                for controller in target_controllers:
                    if hasattr(controller, "set_operator_entry_pause"):
                        controller.set_operator_entry_pause(
                            True,
                            reason="mobile-operator",
                            scope=scope,
                            command_id=command_id,
                            updated_at=requested_at,
                        )
            elif action in {"global_resume_entries", "bot_resume_entries"}:
                scope = "global" if action == "global_resume_entries" else "bot"
                for controller in target_controllers:
                    if hasattr(controller, "set_operator_entry_pause"):
                        controller.set_operator_entry_pause(
                            False,
                            reason="mobile-operator",
                            scope=scope,
                            command_id=command_id,
                            updated_at=requested_at,
                        )
            elif action == "bot_cancel_pending_entries":
                target_controller_ids = {controller.config.id for controller in target_controllers}
                actions = [
                    StopExecutorAction(executor_id=executor.id, controller_id=executor.controller_id, keep_position=True)
                    for executor in self.get_all_executors()
                    if executor.controller_id in target_controller_ids
                    and executor.side == TradeType.BUY
                    and executor.is_active
                    and not executor.is_trading
                ]
                if actions:
                    self.executor_orchestrator.execute_actions(actions)
            for controller in target_controllers:
                if is_modular_policy(controller.config):
                    self._operator_watermarks[controller.config.id] = {
                        'requested_at': requested_at, 'command_id': command_id}
            if any(is_modular_policy(c.config) for c in target_controllers):
                try:
                    receipts = tuple({'controller_id': c.config.id, 'command_id': command_id,
                                      'requested_at': requested_at} for c in target_controllers if is_modular_policy(c.config))
                    self._persist_risk_state(operator_receipts=receipts)
                except Exception as error:
                    self._is_stop_triggered = True
                    self._risk_reconciliation_error = 'Operator command persistence failed'
                    self.logger().exception(self._risk_reconciliation_error)
                    raise RuntimeError(self._risk_reconciliation_error) from error

    def _validate_modular_loss_limits(self):
        if not any(is_modular_policy(c.config) for c in self.controllers.values()):
            return
        limits = [self.config.max_global_drawdown_quote, self.config.max_controller_drawdown_quote]
        if any(value is None or isinstance(value, bool) for value in limits):
            raise ValueError('Native modular execution requires finite positive loss limits')
        limits = [Decimal(str(value)) for value in limits]
        if any(not value.is_finite() or value <= 0 for value in limits) or limits[1] > limits[0]:
            raise ValueError('Native modular loss limits must be finite, positive and controller <= global')
        if (self._daily_entry_risk is not None
                and self.config.max_daily_loss_quote != self._daily_entry_risk.limit_quote):
            raise ValueError('Daily loss limit changed; explicit reconciliation required')

    def control_max_drawdown(self):
        self._validate_modular_loss_limits()
        if self.config.max_controller_drawdown_quote:
            self.check_max_controller_drawdown()
        if self.config.max_global_drawdown_quote:
            self.check_max_global_drawdown()

    def _drawdown_pnl(self, controller_id):
        report = self.get_performance_report(controller_id)
        value = getattr(report, "global_pnl_quote", None)
        if getattr(report, "pnl_available", True) and value is not None:
            if not value.is_finite():
                raise ValueError("PnL unavailable: non-finite native report")
            return value
        config = self.controllers[controller_id].config
        if (getattr(report, "pnl_unavailable_reason", None) == "unknown_wallet_acquisition_cost"
                and is_ok_rsi_policy(config)
                and getattr(config, "trade_mode", None) == "sell_only"
                and getattr(config, "sell_only_strict_fraction", False)
                and not getattr(config, "sell_only_if_profitable", True)):
            return None
        raise ValueError("PnL unavailable: cannot evaluate drawdown")

    def check_max_controller_drawdown(self):
        for controller_id, controller in self.controllers.items():
            if controller.status != RunnableStatus.RUNNING:
                continue
            controller_pnl = self._drawdown_pnl(controller_id)
            if controller_pnl is None:
                continue
            last_max_pnl = self.max_pnl_by_controller.get(controller_id, Decimal("0"))
            if controller_pnl > last_max_pnl:
                self.max_pnl_by_controller[controller_id] = controller_pnl
            else:
                current_drawdown = last_max_pnl - controller_pnl
                if current_drawdown > self.config.max_controller_drawdown_quote:
                    self.logger().info(f"Controller {controller_id} reached max drawdown. Stopping the controller.")
                    controller.stop()
                    executors_order_placed = self.filter_executors(
                        executors=self.get_executors_by_controller(controller_id),
                        filter_func=lambda x: x.is_active and not x.is_trading,
                    )
                    self.executor_orchestrator.execute_actions(
                        actions=[StopExecutorAction(controller_id=controller_id, executor_id=executor.id) for executor in executors_order_placed]
                    )
                    self.drawdown_exited_controllers.append(controller_id)

    def check_max_global_drawdown(self):
        values = [self._drawdown_pnl(controller_id) for controller_id in self.controllers]
        if any(value is None for value in values):
            return  # Global drawdown is unavailable when any acquisition basis is unknown.
        current_global_pnl = sum(values)
        if current_global_pnl > self.max_global_pnl:
            self.max_global_pnl = current_global_pnl
        else:
            current_global_drawdown = self.max_global_pnl - current_global_pnl
            if current_global_drawdown > self.config.max_global_drawdown_quote:
                self.drawdown_exited_controllers.extend(list(self.controllers.keys()))
                self.logger().info("Global drawdown reached. Stopping the strategy.")
                self._is_stop_triggered = True
                HummingbotApplication.main_application().stop()

    def get_controller_report(self, controller_id: str) -> dict:
        """
        Get the full report for a controller including performance and custom info.
        """
        performance_report = self.controller_reports.get(controller_id, {}).get("performance")
        return {
            "performance": performance_report.dict() if performance_report else {},
            "custom_info": {
                **self.controllers[controller_id].get_custom_info(),
                "drawdown": {"available": getattr(performance_report, "pnl_available", True),
                             "unavailable_reason": getattr(performance_report, "pnl_unavailable_reason", None)},
            }
        }

    def send_performance_report(self):
        if not self._controller_reports_valid:
            return
        if self.current_timestamp - self._last_performance_report_timestamp >= self.performance_report_interval and self._pub:
            controller_reports = {controller_id: self.get_controller_report(controller_id) for controller_id in self.controllers.keys()}
            self._pub(controller_reports)
            self._last_performance_report_timestamp = self.current_timestamp

    def check_manual_kill_switch(self):
        for controller_id, controller in self.controllers.items():
            if controller.config.manual_kill_switch and controller.status == RunnableStatus.RUNNING:
                keep_position = bool(getattr(controller.config, "early_stop_keep_position", False))
                self.logger().info(
                    f"Manual stop for controller {controller_id} | keep_position={keep_position}."
                )
                controller.stop()
                executors_to_stop = self.get_executors_by_controller(controller_id)
                self.executor_orchestrator.execute_actions(
                    [StopExecutorAction(executor_id=executor.id,
                                        controller_id=executor.controller_id,
                                        keep_position=keep_position) for executor in executors_to_stop])
            if not controller.config.manual_kill_switch and controller.status == RunnableStatus.TERMINATED:
                if controller_id in self.drawdown_exited_controllers:
                    continue
                self.logger().info(f"Restarting controller {controller_id}.")
                controller.start()

    def check_executors_status(self):
        active_executors = self.filter_executors(
            executors=self.get_all_executors(),
            filter_func=lambda executor: executor.status == RunnableStatus.RUNNING
        )
        if not active_executors:
            self.logger().info("All executors have finalized their execution. Stopping the strategy.")
            HummingbotApplication.main_application().stop()
        else:
            non_trading_executors = self.filter_executors(
                executors=active_executors,
                filter_func=lambda executor: not executor.is_trading
            )
            self.executor_orchestrator.execute_actions(
                [StopExecutorAction(executor_id=executor.id,
                                    controller_id=executor.controller_id) for executor in non_trading_executors])

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        return []

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        return []

    def apply_initial_setting(self):
        connectors_position_mode = {}
        for controller_id, controller in self.controllers.items():
            self.max_pnl_by_controller.setdefault(controller_id, Decimal("0"))
            config_dict = controller.config.model_dump()
            if "connector_name" in config_dict:
                if self.is_perpetual(config_dict["connector_name"]):
                    if "position_mode" in config_dict:
                        connectors_position_mode[config_dict["connector_name"]] = config_dict["position_mode"]
                    if "leverage" in config_dict and "trading_pair" in config_dict:
                        self.connectors[config_dict["connector_name"]].set_leverage(
                            leverage=config_dict["leverage"],
                            trading_pair=config_dict["trading_pair"])
        for connector_name, position_mode in connectors_position_mode.items():
            self.connectors[connector_name].set_position_mode(position_mode)

    def did_fail_order(self, order_failed_event: MarketOrderFailureEvent):
        """
        Handle order failure events by logging the error and stopping the strategy if necessary.
        """
        if order_failed_event.error_message and "position side" in order_failed_event.error_message.lower():
            connectors_position_mode = {}
            for controller_id, controller in self.controllers.items():
                config_dict = controller.config.model_dump()
                if "connector_name" in config_dict:
                    if self.is_perpetual(config_dict["connector_name"]):
                        if "position_mode" in config_dict:
                            connectors_position_mode[config_dict["connector_name"]] = config_dict["position_mode"]
            for connector_name, position_mode in connectors_position_mode.items():
                self.connectors[connector_name].set_position_mode(position_mode)
