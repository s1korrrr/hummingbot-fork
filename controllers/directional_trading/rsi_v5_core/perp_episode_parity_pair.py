from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any

from controllers.directional_trading.rsi_v5_core.perp_episode_accounting import (
    ParityStatus,
)
from controllers.directional_trading.rsi_v5_core.perp_episode_parity_io import (
    ParityWireError,
    evaluate_parity_payload,
    parity_result_to_wire,
    read_parity_payload,
)
from controllers.directional_trading.rsi_v5_core.perp_episode_parity_result import (
    ParityResultVerificationError,
    read_verified_parity_result,
)

_AUTHORITY_KEYS = (
    "paper_allowed",
    "canary_allowed",
    "live_allowed",
    "live_orders_allowed",
    "capital_authority",
    "risk_authority",
    "execution_authority",
    "trading_authority",
)


class ParityPairVerificationError(ValueError):
    pass


def _false_authority(value: Mapping[str, bool] | None = None) -> Mapping[str, bool]:
    supplied = dict(value or {})
    unknown = set(supplied) - set(_AUTHORITY_KEYS)
    if unknown:
        raise ParityPairVerificationError(
            f"unknown parity-pair authority keys: {sorted(unknown)}"
        )
    if any(type(item) is not bool for item in supplied.values()):
        raise ParityPairVerificationError(
            "parity-pair authority values must be booleans"
        )
    if any(supplied.values()):
        raise ParityPairVerificationError("parity pair cannot grant authority")
    return MappingProxyType({key: False for key in _AUTHORITY_KEYS})


def _canonical_hash(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class ParityPairVerification:
    status: ParityStatus
    pair_match: bool
    input_hash: str
    result_hash: str
    authority: Mapping[str, bool] = field(default_factory=dict)
    pair_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.status, ParityStatus):
            raise ParityPairVerificationError("pair status is invalid")
        if self.pair_match is not True:
            raise ParityPairVerificationError(
                "a persisted parity-pair object must represent an exact match"
            )
        for value, label in (
            (self.input_hash, "input_hash"),
            (self.result_hash, "result_hash"),
        ):
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise ParityPairVerificationError(
                    f"{label} must be a lowercase SHA-256 hash"
                )
        object.__setattr__(self, "authority", _false_authority(self.authority))
        object.__setattr__(self, "pair_hash", _canonical_hash(self.as_dict()))

    @property
    def execution_authority(self) -> bool:
        return False

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "hummingbot.perp_episode_parity_pair.v1",
            "status": self.status.value,
            "pair_match": self.pair_match,
            "input_hash": self.input_hash,
            "result_hash": self.result_hash,
            "execution_authority": self.execution_authority,
            "authority": dict(self.authority),
        }


def verify_parity_pair(
    *,
    input_path: Path,
    result_path: Path,
) -> ParityPairVerification:
    source = Path(input_path)
    result_file = Path(result_path)
    if source.resolve(strict=False) == result_file.resolve(strict=False):
        raise ParityPairVerificationError("input and result paths must differ")
    try:
        input_payload = read_parity_payload(source)
        expected_wire = evaluate_parity_payload(input_payload)
        recorded = read_verified_parity_result(result_file)
        recorded_wire = parity_result_to_wire(recorded)
    except (ParityWireError, ParityResultVerificationError) as error:
        raise ParityPairVerificationError(str(error)) from error
    if expected_wire != recorded_wire:
        raise ParityPairVerificationError(
            "persisted parity result does not match exact input re-evaluation"
        )
    return ParityPairVerification(
        status=recorded.status,
        pair_match=True,
        input_hash=_canonical_hash(input_payload),
        result_hash=recorded.result_hash,
        authority={},
    )
