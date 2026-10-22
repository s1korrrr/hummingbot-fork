# --- controllers/directional_trading/rsi_v5_regime_switch.py ---
"""
RSI v5 Regime-Switch Controller.

Additive paper-candidate controller that keeps the RSI v5 execution/risk rails
and adds an explicit regime policy layer for defensive dip buying.
"""
from decimal import Decimal
from typing import Dict, List, Mapping, Optional, Tuple

import pandas as pd
from pydantic import model_validator

from controllers.directional_trading.rsi_v5 import Field, RSIv5Controller, RSIv5ControllerConfig
from hummingbot.client.config.config_data_types import ClientFieldData

_NUMERIC_ERRORS = (ArithmeticError, TypeError, ValueError)


class RSIv5RegimeSwitchControllerConfig(RSIv5ControllerConfig):
    controller_name: str = "rsi_v5_regime_switch"
    selected_candidate_label: str = Field(
        default="stateful_regime_switch_stack_rsi_atr_v1",
        client_data=ClientFieldData(
            prompt=lambda mi: "Selected evidence candidate label:",
            prompt_on_new=False,
            is_updatable=False,
        ),
    )
    healthy_source_label: str = Field(
        default="next50_stack_guardian_network_queue_k20",
        client_data=ClientFieldData(
            prompt=lambda mi: "Healthy-regime source label:",
            prompt_on_new=False,
            is_updatable=False,
        ),
    )
    defensive_source_label: str = Field(
        default="dip_candle_rsi_atr_decile_entry_k20",
        client_data=ClientFieldData(
            prompt=lambda mi: "Defensive-regime source label:",
            prompt_on_new=False,
            is_updatable=False,
        ),
    )
    promotion_stage: str = Field(
        default="paper_candidate_only",
        client_data=ClientFieldData(
            prompt=lambda mi: "Promotion stage:",
            prompt_on_new=False,
            is_updatable=False,
        ),
    )

    # Production-shaped defaults for the paper candidate. The existing RSI v5
    # rails stay active; these defaults only make entry confirmation and sizing
    # less aggressive for a first promoted controller package.
    max_executors_per_side: int = Field(
        default=8,
        ge=1,
        client_data=ClientFieldData(
            prompt=lambda mi: "Max executors per side:",
            prompt_on_new=True,
            is_updatable=True,
        ),
    )
    usd_per_entry: Decimal = Field(
        default=Decimal("300"),
        gt=Decimal("0"),
        client_data=ClientFieldData(
            prompt=lambda mi: "USD per entry:",
            prompt_on_new=True,
            is_updatable=True,
        ),
    )
    max_total_position_usd: Decimal = Field(
        default=Decimal("2500"),
        gt=Decimal("0"),
        client_data=ClientFieldData(
            prompt=lambda mi: "Max total position USD:",
            prompt_on_new=True,
            is_updatable=True,
        ),
    )
    buy_confirmation_mode: str = Field(
        default="rebound_confirm",
        client_data=ClientFieldData(
            prompt=lambda mi: "BUY confirmation mode (none/rebound_confirm):",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    buy_confirmation_rsi_delta: float = Field(
        default=0.25,
        ge=0.0,
        client_data=ClientFieldData(
            prompt=lambda mi: "BUY confirmation RSI rebound delta:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    buy_confirmation_max_wait_seconds: int = Field(
        default=90,
        ge=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "BUY confirmation max wait seconds (0=no timeout):",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    trailing_activation_pct: float = Field(
        default=0.035,
        ge=0.0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Trailing activation pct:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    trailing_delta_pct: float = Field(
        default=0.025,
        ge=0.0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Trailing delta pct:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    trend_confirmation_adx_threshold: float = Field(
        default=22.0,
        ge=0.0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Trend confirmation ADX threshold:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    sell_trend_hold_min_confidence: float = Field(
        default=0.35,
        ge=0.0,
        le=1.0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Sell trend-hold confidence:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    trend_sell_threshold_boost: float = Field(
        default=6.0,
        ge=0.0,
        le=10.0,
        client_data=ClientFieldData(
            prompt=lambda mi: "HTF uptrend sell-threshold boost:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    use_split_entries: bool = Field(
        default=False,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enable scout/runner BUY entries?:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    max_position_size_multiplier: float = Field(
        default=1.0,
        ge=0.5,
        le=1.0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Max size multiplier:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )

    regime_switch_enabled: bool = Field(
        default=True,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enable RSI v5 regime switch policy?:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    healthy_regime_keywords: str = Field(
        default="range,trend_up,normal_mean_reversion,strong_uptrend_distribution",
        client_data=ClientFieldData(
            prompt=lambda mi: "Healthy regime keywords:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    defensive_regime_keywords: str = Field(
        default="trend_down,defensive_downtrend,capitulation",
        client_data=ClientFieldData(
            prompt=lambda mi: "Defensive regime keywords:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    defensive_min_signal_score: int = Field(
        default=2,
        ge=1,
        le=4,
        client_data=ClientFieldData(
            prompt=lambda mi: "Defensive min signal score:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    defensive_size_multiplier: float = Field(
        default=0.5,
        gt=0.0,
        le=1.0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Defensive size multiplier:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    defensive_max_position_size_multiplier: float = Field(
        default=1.0,
        gt=0.0,
        le=1.0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Defensive max size multiplier:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    defensive_rebound_required: bool = Field(
        default=True,
        client_data=ClientFieldData(
            prompt=lambda mi: "Require rebound confirmation in defensive regimes?:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    local_low_overlay_enabled: bool = Field(
        default=False,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enable experimental local-low overlay?:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    local_low_overlay_acknowledgement: bool = Field(
        default=False,
        client_data=ClientFieldData(
            prompt=lambda mi: "Acknowledge local-low overlay drawdown risk?:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )

    @model_validator(mode="after")
    def validate_regime_switch_controls(self):
        expected_candidate = "stateful_regime_switch_stack_rsi_atr_v1"
        if self.selected_candidate_label != expected_candidate:
            raise ValueError(f"selected_candidate_label must remain {expected_candidate!r} for this controller")
        if self.local_low_overlay_enabled and not self.local_low_overlay_acknowledgement:
            raise ValueError(
                "local_low_overlay_enabled requires local_low_overlay_acknowledgement=True "
                "because prior tests showed materially worse drawdown"
            )
        if self.defensive_rebound_required and self.buy_confirmation_mode != "rebound_confirm":
            raise ValueError("defensive_rebound_required requires buy_confirmation_mode='rebound_confirm'")
        if self.defensive_max_position_size_multiplier > self.max_position_size_multiplier:
            raise ValueError(
                "defensive_max_position_size_multiplier must be less than or equal to max_position_size_multiplier"
            )
        return self

    def selected_profile_summary(self) -> Dict[str, object]:
        return {
            "selected_candidate_label": self.selected_candidate_label,
            "healthy_source_label": self.healthy_source_label,
            "defensive_source_label": self.defensive_source_label,
            "healthy_regimes": self._keyword_list_for_summary(self.healthy_regime_keywords),
            "defensive_regimes": self._keyword_list_for_summary(self.defensive_regime_keywords),
            "usd_per_entry": str(self.usd_per_entry),
            "max_total_position_usd": str(self.max_total_position_usd),
            "max_executors_per_side": self.max_executors_per_side,
            "defensive_size_multiplier": self.defensive_size_multiplier,
            "buy_confirmation_mode": self.buy_confirmation_mode,
            "buy_confirmation_rsi_delta": self.buy_confirmation_rsi_delta,
            "trailing_activation_pct": self.trailing_activation_pct,
            "trailing_delta_pct": self.trailing_delta_pct,
            "local_low_overlay": (
                "enabled_experimental"
                if self.local_low_overlay_enabled
                else "disabled_requires_acknowledgement"
            ),
            "promotion_stage": self.promotion_stage,
        }

    @staticmethod
    def _keyword_list_for_summary(value: str) -> List[str]:
        return [token.strip() for token in str(value or "").split(",") if token.strip()]


class RSIv5RegimeSwitchController(RSIv5Controller):
    """RSI v5 plus deterministic regime-specific BUY policy."""

    def __init__(self, config: RSIv5RegimeSwitchControllerConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)

    @staticmethod
    def _keyword_list(value: str) -> List[str]:
        return [token.strip().lower() for token in str(value or "").split(",") if token.strip()]

    def _policy_for_regime(
        self,
        regime_label: Optional[str],
        trend_confirmation: Optional[Mapping[str, object]] = None,
    ) -> str:
        if not self.config.regime_switch_enabled:
            return "base"

        label = str(regime_label or "").strip().lower()
        defensive_keywords = self._keyword_list(self.config.defensive_regime_keywords)
        healthy_keywords = self._keyword_list(self.config.healthy_regime_keywords)
        if label and any(keyword in label for keyword in defensive_keywords):
            return "defensive"
        if label and any(keyword in label for keyword in healthy_keywords):
            return "healthy"

        if isinstance(trend_confirmation, Mapping):
            direction = str(trend_confirmation.get("direction", "") or "").lower()
            try:
                confidence = float(trend_confirmation.get("confidence", 0.0) or 0.0)
            except _NUMERIC_ERRORS:
                confidence = 0.0
            if "down" in direction and confidence >= 0.35:
                return "defensive"
            if "up" in direction and confidence >= 0.35:
                return "healthy"

        return "neutral"

    def _apply_regime_policy_to_signal(
        self,
        *,
        signal: int,
        signal_state: Mapping[str, object],
        regime_label: Optional[str] = None,
        trend_confirmation: Optional[Mapping[str, object]] = None,
    ) -> Tuple[int, Dict[str, object]]:
        adjusted_state = dict(signal_state or {})
        effective_regime = regime_label if regime_label is not None else adjusted_state.get("regime_label")
        policy = self._policy_for_regime(str(effective_regime) if effective_regime is not None else None, trend_confirmation)
        adjusted_state["regime_switch_policy"] = policy
        adjusted_state["regime_switch_regime"] = effective_regime

        if signal <= 0 or policy != "defensive":
            return signal, adjusted_state

        signal_score_raw = adjusted_state.get("signal_score", 0)
        try:
            signal_score = int(signal_score_raw or 0)
        except _NUMERIC_ERRORS:
            signal_score = 0

        if signal_score < int(self.config.defensive_min_signal_score):
            adjusted_state["condition_ok"] = False
            adjusted_state["condition_reason"] = "regime_switch_defensive_score"
            adjusted_state["buy_decision"] = "blocked"
            adjusted_state["buy_reason"] = "regime-switch-defensive-score"
            adjusted_state["regime_switch_blocked"] = True
            return 0, adjusted_state

        try:
            current_size = float(adjusted_state.get("size_multiplier", 1.0) or 1.0)
        except _NUMERIC_ERRORS:
            current_size = 1.0

        capped_size = min(current_size, float(self.config.defensive_max_position_size_multiplier))
        adjusted_size = capped_size * float(self.config.defensive_size_multiplier)
        adjusted_state["size_multiplier"] = max(0.0, adjusted_size)
        adjusted_state["buy_context_bias"] = "hostile"
        adjusted_state["regime_switch_blocked"] = False
        adjusted_state["regime_switch_size_multiplier"] = adjusted_state["size_multiplier"]
        return signal, adjusted_state

    def _causal_replay_regime_labels(self, df: pd.DataFrame) -> pd.Series:
        close = pd.to_numeric(df.get("close"), errors="coerce")
        if close.empty:
            return pd.Series("normal_mean_reversion", index=df.index, dtype=object)

        ret_60 = close.pct_change(60)
        ret_240 = close.pct_change(240)
        ema_slow = close.ewm(span=240, adjust=False, min_periods=20).mean()

        labels = pd.Series("normal_mean_reversion", index=df.index, dtype=object)
        capitulation = ((ret_60 <= -0.025) | (ret_240 <= -0.05)).fillna(False)
        defensive = (((ret_60 <= -0.01) | (ret_240 <= -0.015)) & (close < ema_slow)).fillna(False)
        uptrend = ((ret_240 >= 0.025) & (close > ema_slow)).fillna(False)

        labels.loc[uptrend] = "strong_uptrend_distribution"
        labels.loc[defensive] = "defensive_downtrend"
        labels.loc[capitulation] = "capitulation"
        return labels

    def _apply_regime_policy_to_backtest_frame(self, df: pd.DataFrame, frame: pd.DataFrame) -> pd.DataFrame:
        adjusted = frame.copy()
        if "signal_state" not in adjusted.columns:
            adjusted["signal_state"] = [{} for _ in adjusted.index]

        if "regime_label" in df.columns:
            regime_labels = df["regime_label"].reindex(adjusted.index)
        else:
            regime_labels = self._causal_replay_regime_labels(df).reindex(adjusted.index)

        policies: List[str] = []
        regimes: List[object] = []
        blocked_flags: List[bool] = []
        for idx in adjusted.index:
            regime_label = regime_labels.loc[idx] if idx in regime_labels.index else None
            raw_signal = adjusted.loc[idx, "signal"] if "signal" in adjusted.columns else 0
            try:
                signal = int(raw_signal or 0)
            except _NUMERIC_ERRORS:
                signal = 0
            state_value = adjusted.loc[idx, "signal_state"]
            state = state_value if isinstance(state_value, Mapping) else {}
            adjusted_signal, adjusted_state = self._apply_regime_policy_to_signal(
                signal=signal,
                signal_state=state,
                regime_label=str(regime_label) if pd.notna(regime_label) else None,
            )
            adjusted.loc[idx, "signal"] = adjusted_signal
            adjusted.at[idx, "signal_state"] = adjusted_state
            policies.append(str(adjusted_state.get("regime_switch_policy", "neutral")))
            regimes.append(adjusted_state.get("regime_switch_regime"))
            blocked_flags.append(bool(adjusted_state.get("regime_switch_blocked", False)))

        adjusted["regime_switch_policy"] = policies
        adjusted["regime_switch_regime"] = regimes
        adjusted["regime_switch_blocked"] = blocked_flags
        return adjusted

    def _build_backtest_signal_frame(
        self,
        df: pd.DataFrame,
        *,
        rsi_buy_eff: float,
        rsi_sell_eff: float,
    ) -> pd.DataFrame:
        frame = super()._build_backtest_signal_frame(df, rsi_buy_eff=rsi_buy_eff, rsi_sell_eff=rsi_sell_eff)
        return self._apply_regime_policy_to_backtest_frame(df, frame)

    async def update_processed_data(self):
        await super().update_processed_data()
        if not isinstance(self.processed_data, dict):
            return

        signal = self.processed_data.get("signal", 0)
        state = self.processed_data.get("signal_state", {})
        if not isinstance(state, Mapping):
            return

        trend_confirmation = self.processed_data.get("trend_confirmation")
        trend = trend_confirmation if isinstance(trend_confirmation, Mapping) else None
        adjusted_signal, adjusted_state = self._apply_regime_policy_to_signal(
            signal=int(signal or 0),
            signal_state=state,
            regime_label=str(state.get("regime_label")) if state.get("regime_label") is not None else None,
            trend_confirmation=trend,
        )
        self.processed_data["signal"] = adjusted_signal
        self.processed_data["signal_state"] = adjusted_state
