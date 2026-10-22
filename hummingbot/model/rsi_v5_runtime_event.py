from sqlalchemy import Column, Float, Index, Integer, Text, UniqueConstraint

from . import HummingbotBase


class RSIV5RuntimeEvent(HummingbotBase):
    """Immutable controller-local RSI runtime fact."""

    __tablename__ = "RSIV5RuntimeEvent"
    __table_args__ = (
        UniqueConstraint(
            "controller_id",
            "sequence",
            name="uq_rsi_v5_runtime_controller_sequence",
        ),
        Index(
            "rsi_v5_runtime_controller_sequence_index",
            "controller_id",
            "sequence",
        ),
    )

    event_id = Column(Text, primary_key=True, nullable=False)
    controller_id = Column(Text, nullable=False)
    sequence = Column(Integer, nullable=False)
    episode_id = Column(Text, nullable=False)
    event_type = Column(Text, nullable=False)
    payload_json = Column(Text, nullable=False)
    created_at = Column(Float, nullable=False)
