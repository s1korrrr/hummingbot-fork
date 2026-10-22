"""Bounded builder state. The first release recomputes fits from the panel (batch reference); no learned weights."""
from typing import Literal

from pydantic import Field

from hummingbot.strategy_v2.components.config import PolicyModel

from .contracts import EPOCH_PATTERN, MAX_TIMESTAMP_MS, SHA256_PATTERN


class ContextState(PolicyModel):
    """Identity and monotonic cursor of one context stream's builder."""
    state_version: Literal[1]
    initialization_id: str = Field(pattern=EPOCH_PATTERN)
    code_digest: str = Field(pattern=SHA256_PATTERN)
    config_digest: str = Field(pattern=SHA256_PATTERN)
    last_cutoff_ms: int | None = Field(ge=0, le=MAX_TIMESTAMP_MS, strict=True)
    builds: int = Field(ge=0, strict=True)


def initial_state(config, *, initialization_id: str, code_digest: str) -> ContextState:
    return ContextState(state_version=1, initialization_id=initialization_id, code_digest=code_digest,
                        config_digest=config.digest, last_cutoff_ms=None, builds=0)
