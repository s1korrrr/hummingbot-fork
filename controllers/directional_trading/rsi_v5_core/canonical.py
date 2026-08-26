from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel

LEGACY_DECIMAL_SCHEMA_VERSIONS = frozenset({"1.0"})


def canonical_decimal_string(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("non-finite Decimal is not canonicalizable")
    if value.is_zero():
        return "0"
    fixed = format(value, "f")
    if "." in fixed:
        fixed = fixed.rstrip("0").rstrip(".")
    return fixed


def canonical_value(value: Any, *, schema_version: str | None = None) -> Any:
    if isinstance(value, BaseModel):
        return canonical_value(
            value.model_dump(mode="python", exclude_none=False),
            schema_version=schema_version,
        )
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("non-finite Decimal is not canonicalizable")
        if schema_version is None or schema_version in LEGACY_DECIMAL_SCHEMA_VERSIONS:
            return str(value)
        return canonical_decimal_string(value)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("canonical datetimes must be timezone-aware")
        return value.astimezone(timezone.utc).isoformat(timespec="auto").replace("+00:00", "Z")
    if isinstance(value, Enum):
        return canonical_value(value.value, schema_version=schema_version)
    if isinstance(value, dict):
        nested_schema_version = value.get("schema_version", schema_version)
        if nested_schema_version is not None:
            nested_schema_version = str(nested_schema_version)
        normalized = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("canonical JSON object keys must be strings")
            normalized[key] = canonical_value(item, schema_version=nested_schema_version)
        return normalized
    if isinstance(value, (list, tuple)):
        return [canonical_value(item, schema_version=schema_version) for item in value]
    if isinstance(value, (set, frozenset)):
        items = [canonical_value(item, schema_version=schema_version) for item in value]
        return sorted(items, key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")))
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite float is not canonicalizable")
        return value
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported canonical JSON value: {type(value).__name__}")


def canonical_json(value: Any) -> str:
    return json.dumps(
        canonical_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def canonical_json_bytes(value: Any) -> bytes:
    return canonical_json(value).encode("utf-8")


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()
