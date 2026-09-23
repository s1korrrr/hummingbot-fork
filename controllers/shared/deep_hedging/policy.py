"""Strict, immutable, dependency-free inference for an unvalidated band policy.

A supplied digest proves identity, not approval. This format is deliberately
shadow-only. Loading a model performs no file, network or account operations.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from typing import Mapping

SCHEMA = "deep_hedging.band_policy.v1"
MAX_BYTES = 131072


def _number(value: object) -> float:
    if type(value) not in (int, float):
        raise ValueError("expected a finite JSON number, not a boolean")
    try:
        number = float(value)
    except (OverflowError, ValueError) as error:
        raise ValueError("number outside finite range") from error
    if not math.isfinite(number):
        raise ValueError("non-finite number")
    return number


def _keys(value: object, keys: set[str]) -> dict:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError("missing or unknown schema fields")
    return value


def _text(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,128}", value):
        raise ValueError("invalid bounded identifier")
    return value


def _hash(value: object, size: int = 64) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{%d}" % size, value):
        raise ValueError("invalid digest")
    return value


def _pairs(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError("non-finite JSON literal: " + value)


@dataclass(frozen=True)
class Context:
    program_id: str
    capital_model: str
    instrument: str
    book_id: str

    def __post_init__(self) -> None:
        for value in (self.program_id, self.capital_model, self.instrument, self.book_id):
            _text(value)


@dataclass(frozen=True)
class Feature:
    name: str
    unit: str
    mean: float
    scale: float
    lower: float
    upper: float


@dataclass(frozen=True)
class FrozenBandPolicy:
    context: Context
    features: tuple[Feature, ...]
    position_bounds: tuple[float, float]
    hidden_weight: tuple[tuple[float, ...], ...]
    hidden_bias: tuple[float, ...]
    output_weight: tuple[tuple[float, ...], ...]
    output_bias: tuple[float, ...]
    sha256: str

    def band(self, values: Mapping[str, float], units: Mapping[str, str]) -> tuple[float, float]:
        names = {feature.name for feature in self.features}
        if set(values) != names or set(units) != names:
            raise ValueError("feature names must match the admitted contract exactly")
        normalized = []
        for feature in self.features:
            value = _number(values[feature.name])
            if units[feature.name] != feature.unit or not feature.lower <= value <= feature.upper:
                raise ValueError("feature unit or admitted domain mismatch")
            item = (value - feature.mean) / feature.scale
            if not math.isfinite(item):
                raise ValueError("normalization overflow")
            normalized.append(item)
        try:
            affine = tuple(math.fsum(w * x for w, x in zip(row, normalized)) + bias
                           for row, bias in zip(self.hidden_weight, self.hidden_bias))
            if not all(math.isfinite(value) for value in affine):
                raise ValueError("non-finite hidden affine output")
            hidden = tuple(math.tanh(value) for value in affine)
            logits = tuple(math.fsum(w * x for w, x in zip(row, hidden)) + bias
                           for row, bias in zip(self.output_weight, self.output_bias))
        except (OverflowError, ValueError) as error:
            raise ValueError("non-finite network intermediate") from error
        if not all(math.isfinite(value) for value in logits):
            raise ValueError("non-finite policy output")
        lower = _sigmoid(logits[0])
        upper = lower + (1.0 - lower) * _sigmoid(logits[1])
        lo, hi = self.position_bounds
        return lo + (hi - lo) * lower, lo + (hi - lo) * upper

    def target(self, values: Mapping[str, float], units: Mapping[str, str], current: float) -> float:
        current = _number(current)
        lo, hi = self.position_bounds
        if not lo <= current <= hi:
            raise ValueError("current position is outside the admitted bounds")
        lower, upper = self.band(values, units)
        return min(max(current, lower), upper)


def _sigmoid(value: float) -> float:
    if value >= 0:
        return 1.0 / (1.0 + math.exp(-value))
    exp = math.exp(value)
    return exp / (1.0 + exp)


def load_policy(raw: bytes, expected_sha256: str, expected_context: Context) -> FrozenBandPolicy:
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError("model must be bounded UTF-8 JSON bytes")
    if hashlib.sha256(raw).hexdigest() != _hash(expected_sha256):
        raise ValueError("model bytes do not match the externally supplied digest")
    try:
        data = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_reject_constant)
    except (UnicodeError, json.JSONDecodeError, RecursionError) as error:
        raise ValueError("invalid model JSON") from error
    _keys(data, {"schema", "mode", "context", "features", "position_bounds", "network", "provenance"})
    if data["schema"] != SCHEMA or data["mode"] != "shadow_only":
        raise ValueError("unsupported or non-shadow policy")
    context = Context(**_keys(data["context"], {"program_id", "capital_model", "instrument", "book_id"}))
    if context != expected_context:
        raise ValueError("policy context differs from the admitted context")
    source = _keys(data["provenance"], {"code_sha", "data_sha256", "config_sha256", "seed",
                                       "framework", "dtype", "evidence_state"})
    _hash(source["code_sha"], 40)
    _hash(source["data_sha256"])
    _hash(source["config_sha256"])
    _text(source["framework"])
    if type(source["seed"]) is not int or not 0 <= source["seed"] < 2**63:
        raise ValueError("invalid seed")
    if source["dtype"] != "float64" or source["evidence_state"] != "ENGINEERING_ONLY":
        raise ValueError("v1 admits only float64 engineering artifacts, not economic support")
    rows = data["features"]
    if not isinstance(rows, list) or not 1 <= len(rows) <= 32:
        raise ValueError("invalid feature count")
    features = []
    for row in rows:
        _keys(row, {"name", "unit", "mean", "scale", "lower", "upper"})
        f = Feature(_text(row["name"]), _text(row["unit"]), *(_number(row[k]) for k in ("mean", "scale", "lower", "upper")))
        if f.scale <= 0 or f.lower >= f.upper:
            raise ValueError("invalid feature normalization or domain")
        features.append(f)
    if len({feature.name for feature in features}) != len(features):
        raise ValueError("duplicate feature name")
    bounds = data["position_bounds"]
    if not isinstance(bounds, list) or len(bounds) != 2:
        raise ValueError("invalid position bounds")
    lo, hi = map(_number, bounds)
    if not lo < hi or not math.isfinite(hi - lo):
        raise ValueError("invalid position interval")
    net = _keys(data["network"], {"hidden_weight", "hidden_bias", "output_weight", "output_bias"})
    hidden = net["hidden_bias"]
    if not isinstance(hidden, list) or not 1 <= len(hidden) <= 64:
        raise ValueError("invalid hidden layer width")

    def vector(values: object, length: int) -> tuple[float, ...]:
        if not isinstance(values, list) or len(values) != length:
            raise ValueError("network vector shape mismatch")
        result = tuple(_number(value) for value in values)
        if any(abs(value) > 1e6 for value in result):
            raise ValueError("network parameter exceeds the bounded format")
        return result

    def matrix(values: object, n: int, m: int) -> tuple[tuple[float, ...], ...]:
        if not isinstance(values, list) or len(values) != n:
            raise ValueError("network matrix shape mismatch")
        return tuple(vector(row, m) for row in values)

    width = len(hidden)
    return FrozenBandPolicy(context, tuple(features), (lo, hi),
                            matrix(net["hidden_weight"], width, len(features)), vector(hidden, width),
                            matrix(net["output_weight"], 2, width), vector(net["output_bias"], 2), expected_sha256)
