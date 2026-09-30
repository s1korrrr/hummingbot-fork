"""Timing of prices actually installed in a native book, never the time of a read."""
from dataclasses import dataclass


@dataclass(frozen=True)
class OrderBookObservation:
    event_timestamp: float
    applied_timestamp: float
