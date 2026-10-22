"""Bounded declarative model loading and deterministic inference; no I/O or fitting."""
import hashlib
import json
import math
import re
from dataclasses import dataclass
from decimal import Decimal

from .contracts import ContinuationEstimateV2, ExitAContextV2

MAX_BYTES = 1048576
MAX_DEPTH = 16


def _vector(values, size, *, positive=False):
    if not isinstance(values, (list, tuple)) or len(values) != size:
        raise ValueError('wrong coefficient dimension')
    try:
        valid = all(type(v) in (int, float) and math.isfinite(v) and (not positive or v > 0) for v in values)
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError('finite numeric coefficients required')
    return tuple(float(v) for v in values)


def _integer(value, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError('invalid bounded integer')
    return value


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON key')
        result[key] = value
    return result


def _nonfinite(value):
    raise ValueError('nonfinite JSON')


def _depth(text):
    depth = 0
    quoted = escaped = False
    for c in text:
        if quoted:
            if escaped:
                escaped = False
            elif c == '\\':
                escaped = True
            elif c == '"':
                quoted = False
        elif c == '"':
            quoted = True
        elif c in '[{':
            depth += 1
            if depth > MAX_DEPTH:
                raise ValueError('JSON nesting exceeds bound')
        elif c in ']}':
            depth -= 1


@dataclass(frozen=True)
class LoadedModel:
    sha256: str
    training_manifest_sha256: str
    feature_names: tuple[str, ...]
    means: tuple[float, ...]
    scales: tuple[float, ...]
    horizon_bins: int
    hazard_weights: tuple[tuple[float, ...], ...] | None
    payoff_weights: tuple[float, ...]
    payoff_intercept: float
    label_cutoff_ms: int
    trained_at_ms: int
    valid_until_ms: int


def load_model(raw: bytes, *, expected_sha256: str) -> LoadedModel:
    if type(raw) is not bytes or not 1 <= len(raw) <= MAX_BYTES:
        raise ValueError('bounded model bytes required')
    if type(expected_sha256) is not str or re.fullmatch(r'[0-9a-f]{64}', expected_sha256) is None:
        raise ValueError('external model pin required')
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError('model bytes differ from pin')
    try:
        text = raw.decode('utf-8')
        _depth(text)
        data = json.loads(text, object_pairs_hook=_object, parse_constant=_nonfinite)
    except (UnicodeError, RecursionError) as exc:
        raise ValueError('invalid model JSON') from exc
    fields = {'schema', 'training_manifest_sha256', 'feature_names', 'means', 'scales', 'horizon_bins',
              'hazard_weights', 'payoff_weights', 'payoff_intercept', 'label_cutoff_ms', 'trained_at_ms', 'valid_until_ms'}
    if not isinstance(data, dict) or set(data) != fields or data['schema'] != 'exit_a_linear_v2':
        raise ValueError('unsupported model schema')
    manifest = data['training_manifest_sha256']
    if type(manifest) is not str or re.fullmatch(r'[0-9a-f]{64}', manifest) is None:
        raise ValueError('invalid training manifest identity')
    names = data['feature_names']
    if not isinstance(names, list) or not 1 <= len(names) <= 32 or any(
        type(n) is not str or re.fullmatch(r'[A-Za-z0-9_.:-]{1,64}', n) is None for n in names
    ) or len(set(names)) != len(names):
        raise ValueError('ordered bounded unique feature names required')
    n = len(names)
    means, scales = _vector(data['means'], n), _vector(data['scales'], n, positive=True)
    bins = _integer(data['horizon_bins'], 1, 64)
    hazards = data['hazard_weights']
    if hazards is not None:
        if not isinstance(hazards, list) or len(hazards) != 3:
            raise ValueError('three competing outcomes required')
        hazards = tuple(_vector(row, n + 2) for row in hazards)
    weights = _vector(data['payoff_weights'], n + (3 if hazards is not None else 0))
    intercept = _vector([data['payoff_intercept']], 1)[0]
    cutoff, trained, end = (_integer(data[key], 0, 2**63 - 1)
                            for key in ('label_cutoff_ms', 'trained_at_ms', 'valid_until_ms'))
    if not cutoff <= trained < end:
        raise ValueError('invalid training/availability interval')
    return LoadedModel(expected_sha256, manifest, tuple(names), means, scales, bins, hazards,
                       weights, intercept, cutoff, trained, end)


def predict_values(model: LoadedModel, feature_names, values) -> tuple[float, tuple[float, ...]]:
    if tuple(feature_names) != model.feature_names:
        raise ValueError('feature order mismatch')
    features = _vector(values, len(model.feature_names))
    try:
        x = tuple((v - mean) / scale for v, mean, scale in zip(features, model.means, model.scales))
        if not all(math.isfinite(v) for v in x):
            raise ValueError('nonfinite standardized features')
        up = down = 0.0
        survival = 1.0
        if model.hazard_weights is not None:
            for k in range(model.horizon_bins):
                row = (1.0, *x, (k + 1) / model.horizon_bins)
                logits = [math.fsum(w * v for w, v in zip(weights, row)) for weights in model.hazard_weights]
                if not all(math.isfinite(v) for v in logits):
                    raise ValueError('nonfinite logits')
                weights = [math.exp(v - max(logits)) for v in logits]
                total = math.fsum(weights)
                u, d, s = (v / total for v in weights)
                up += survival * u
                down += survival * d
                survival *= s
            x = (*x, up, down, survival)
        result = model.payoff_intercept + math.fsum(w * v for w, v in zip(model.payoff_weights, x))
        if not math.isfinite(result):
            raise ValueError('nonfinite payoff')
    except (ArithmeticError, OverflowError) as exc:
        raise ValueError('model arithmetic is unavailable') from exc
    return result, (up, down, survival)


def estimate(context: ExitAContextV2, model: LoadedModel) -> ContinuationEstimateV2:
    context = ExitAContextV2.model_validate(context.model_dump(mode='json'))
    available = model.trained_at_ms <= context.decision_time_ms < model.valid_until_ms
    value, reason = None, 'model_not_available'
    if available:
        try:
            value = Decimal(str(predict_values(model, context.feature_names, context.feature_values)[0]))
            reason = 'estimated'
        except ValueError:
            available, reason = False, 'inference_unavailable'
    return ContinuationEstimateV2(context_sha256=context.context_sha256, feature_sha256=context.feature_sha256,
        model_sha256=model.sha256, training_manifest_sha256=model.training_manifest_sha256,
        available=available, value_bps=value, label_cutoff_ms=model.label_cutoff_ms,
        trained_at_ms=model.trained_at_ms, expires_at_ms=model.valid_until_ms, reason=reason)
