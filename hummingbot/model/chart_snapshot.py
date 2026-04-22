from sqlalchemy import Column, Float, Index, Integer, Text, UniqueConstraint

from . import HummingbotBase


class ChartSnapshot(HummingbotBase):
    """
    Normalized per-candle chart state used by the reporting desk.
    """

    __tablename__ = "ChartSnapshot"
    __table_args__ = (
        Index("cs_pair_interval_timestamp", "pair", "interval", "candle_timestamp"),
        Index("cs_controller_interval_timestamp", "controller_id", "interval", "candle_timestamp"),
        Index("cs_bot_pair_timestamp", "bot_name", "pair", "candle_timestamp"),
        UniqueConstraint("controller_id", "pair", "interval", "candle_timestamp", name="uq_chart_snapshot_controller_pair_interval_ts"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    bot_name = Column(Text, nullable=False)
    controller_id = Column(Text, nullable=False)
    pair = Column(Text, nullable=False)
    interval = Column(Text, nullable=False)
    candle_timestamp = Column(Integer, nullable=False)
    open_price = Column(Float, nullable=False)
    high_price = Column(Float, nullable=False)
    low_price = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)
    volume = Column(Float, nullable=True)
    rsi = Column(Float, nullable=True)
    atr = Column(Float, nullable=True)
    atr_pct = Column(Float, nullable=True)
    ema_fast = Column(Float, nullable=True)
    ema_slow = Column(Float, nullable=True)
    bb_lower = Column(Float, nullable=True)
    bb_mid = Column(Float, nullable=True)
    bb_upper = Column(Float, nullable=True)
    macd_hist = Column(Float, nullable=True)
    regime_label = Column(Text, nullable=True)
    regime_confidence = Column(Float, nullable=True)
    trend_direction = Column(Text, nullable=True)
    trend_confidence = Column(Float, nullable=True)
    signal = Column(Integer, nullable=True)
    signal_score = Column(Integer, nullable=True)
    buy_decision = Column(Text, nullable=True)
    sell_decision = Column(Text, nullable=True)
    buy_entry_role = Column(Text, nullable=True)
    buy_context_bias = Column(Text, nullable=True)
    rsi_buy_threshold = Column(Float, nullable=True)
    rsi_sell_threshold = Column(Float, nullable=True)
    cost_basis = Column(Float, nullable=True)
    exposure_base = Column(Float, nullable=True)
    exposure_quote = Column(Float, nullable=True)
    breakeven_price = Column(Float, nullable=True)
