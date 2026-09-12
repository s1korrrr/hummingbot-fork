import os
from decimal import Decimal
from typing import Dict, List, Optional

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.event.events import MarketOrderFailureEvent
from hummingbot.remote_iface.mqtt import ExternalEventFactory
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class V2WithControllersConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    max_global_drawdown_quote: Optional[float] = None
    max_controller_drawdown_quote: Optional[float] = None
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
        self.max_pnl_by_controller = {}
        self.max_global_pnl = Decimal("0")
        self.drawdown_exited_controllers = []
        self.closed_executors_buffer: int = 30
        self._last_performance_report_timestamp = 0
        self._operator_event_queue = ExternalEventFactory.create_queue("rsibot.operator")
        self._risk_persistence_enabled = any(getattr(c.config, 'controller_name', '') == 'ok_rsi'
                                             for c in self.controllers.values())
        self._last_risk_state = None
        self._risk_reconciliation_error: Optional[str] = None
        if self._risk_persistence_enabled:
            try:
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
                self._persist_risk_state()
            except Exception as error:
                self._is_stop_triggered = True
                self._risk_reconciliation_error = str(error) or type(error).__name__
                self.logger().exception('Risk state unavailable; execution blocked pending reconciliation')

    def _persist_risk_state(self):
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
        if state != self._last_risk_state:
            MarketsRecorder.get_instance().store_strategy_risk_state(state)
            self._last_risk_state = state

    def validate_create_action(self, action):
        # Called after fresh reports and immediately before executor creation.
        if getattr(self, '_risk_reconciliation_error', None) is not None:
            self._is_stop_triggered = True
            return False
        try:
            self.process_operator_events()
            self.check_manual_kill_switch()
            self.control_max_drawdown()
            self._persist_risk_state()
            return not self._is_stop_triggered and action.controller_id not in self.drawdown_exited_controllers
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
            self.send_performance_report()
        try:
            self._persist_risk_state()
        except Exception:
            self._is_stop_triggered = True
            self.logger().exception('Unable to persist risk state; new execution blocked')

    def process_operator_events(self):
        while len(self._operator_event_queue) > 0:
            event_name, event = self._operator_event_queue.popleft()
            payload = getattr(event, "data", {}) or {}
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
            else:
                self.logger().warning(f"Unsupported operator event {event_name}: {payload}")

    def control_max_drawdown(self):
        if self.config.max_controller_drawdown_quote:
            self.check_max_controller_drawdown()
        if self.config.max_global_drawdown_quote:
            self.check_max_global_drawdown()

    def check_max_controller_drawdown(self):
        for controller_id, controller in self.controllers.items():
            if controller.status != RunnableStatus.RUNNING:
                continue
            controller_pnl = self.get_performance_report(controller_id).global_pnl_quote
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
        current_global_pnl = sum([self.get_performance_report(controller_id).global_pnl_quote for controller_id in self.controllers.keys()])
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
            "custom_info": self.controllers[controller_id].get_custom_info()
        }

    def send_performance_report(self):
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
