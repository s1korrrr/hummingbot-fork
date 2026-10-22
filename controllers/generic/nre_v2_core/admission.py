"""Initial-entry evidence only. Forecast production, role authority and orders stay outside."""
from dataclasses import asdict, dataclass
from decimal import Decimal, localcontext

from hummingbot.strategy_v2.components.decision_protocol import Opportunity, Prerequisite
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding, digest

from .binding import BoundPath, _hash, _native, evidence, milliseconds, observation_ref

FEATURES = frozenset({'extension_atr', 'rebound_atr', 'retention', 'setup_age_bars', 'bars_since_low'})


def _bps(value, *, nonnegative=False) -> Decimal:
    if type(value) not in (str, int, float, Decimal) or len(str(value)) > 96:
        raise PolicyError('bounded finite basis-point value required')
    try:
        result = Decimal(str(value))
        if (not result.is_finite() or len(result.as_tuple().digits) > 32 or result.as_tuple().exponent < -18
                or result.copy_abs() > Decimal('1000000') or (nonnegative and result < 0)):
            raise ValueError('basis-point magnitude/precision outside bounds')
        return result
    except (ArithmeticError, ValueError) as exc:
        raise PolicyError('invalid basis-point value') from exc


@dataclass(frozen=True)
class ActionRole:
    opportunity_id: str
    role: str
    evidence_sha256: str
    available_at_ms: int

    def __post_init__(self):
        _hash(self.opportunity_id)
        _hash(self.evidence_sha256)
        milliseconds(self.available_at_ms)
        if self.role not in ('INITIAL_ENTRY', 'SCALE_IN', 'PYRAMID', 'REDUCE', 'UNKNOWN'):
            raise PolicyError('unknown action-role spelling')


@dataclass(frozen=True)
class GatePolicy:
    recipe: RecipeBinding
    model_sha256: str
    scaler_sha256: str
    horizon_ms: int
    required_features: tuple[str, ...]
    minimum_net_bps: Decimal

    def __post_init__(self):
        object.__setattr__(self, 'recipe', _native(self.recipe, RecipeBinding))
        for value in (self.model_sha256, self.scaler_sha256):
            _hash(value)
        milliseconds(self.horizon_ms)
        if not 1 <= self.horizon_ms <= 604800000:
            raise PolicyError('bounded positive prediction horizon required')
        if (type(self.required_features) is not tuple or not self.required_features
                or len(self.required_features) != len(set(self.required_features))
                or not set(self.required_features) <= FEATURES):
            raise PolicyError('explicit supported feature requirements needed')
        object.__setattr__(self, 'minimum_net_bps', _bps(self.minimum_net_bps))


@dataclass(frozen=True)
class Prediction:
    opportunity_id: str
    path_state_sha256: str
    model_sha256: str
    scaler_sha256: str
    training_cutoff_ms: int
    model_available_at_ms: int
    generated_at_ms: int
    valid_until_ms: int
    horizon_ms: int
    gross_return_bps: Decimal
    cost_bps: Decimal
    mean_uncertainty_bps: Decimal

    def __post_init__(self):
        for value in (self.opportunity_id, self.path_state_sha256, self.model_sha256, self.scaler_sha256):
            _hash(value)
        for value in (self.training_cutoff_ms, self.model_available_at_ms, self.generated_at_ms,
                      self.valid_until_ms, self.horizon_ms):
            milliseconds(value)
        if not (self.training_cutoff_ms <= self.model_available_at_ms <= self.generated_at_ms < self.valid_until_ms):
            raise PolicyError('prediction timing is inconsistent')
        if not 1 <= self.horizon_ms <= 604800000:
            raise PolicyError('bounded positive prediction horizon required')
        for field in ('gross_return_bps', 'cost_bps', 'mean_uncertainty_bps'):
            object.__setattr__(self, field, _bps(getattr(self, field), nonnegative=field != 'gross_return_bps'))


@dataclass(frozen=True)
class GateResult:
    applicable: bool
    reason: str
    prerequisite: Prerequisite | None
    net_edge_bps: str | None = None

    @property
    def execution_authorized(self) -> bool:
        return False


def classify_role(*, action: str, stage: int) -> str:
    if type(action) is not str or type(stage) is not int:
        raise PolicyError('explicit action and integer stage required')
    if action == 'reduce':
        return 'REDUCE'
    if action == 'acquire' and stage == 0:
        return 'INITIAL_ENTRY'
    if action == 'acquire' and stage > 0:
        return 'SCALE_IN'
    return 'UNKNOWN'


def acquisition_dispatch_allowed(result: GateResult) -> bool:
    if type(result) is not GateResult:
        raise PolicyError('gate result required')
    if not result.applicable:
        return True
    return result.prerequisite is not None and result.prerequisite.status == 'PASS'


def evaluate(*, opportunity: Opportunity, role: ActionRole | None, state: BoundPath | None,
             prediction: Prediction | None, policy: GatePolicy | None, now_ms: int) -> GateResult:
    """Return one source-bound prerequisite; callers retain native admission and cursor semantics."""
    opportunity = _native(opportunity, Opportunity)
    # Management never waits for NRE data, a model, a role annotation or NRE timing.
    if opportunity.action == 'reduce':
        return GateResult(False, 'management_not_filtered', None)

    def hold(reason, status='UNAVAILABLE'):
        return GateResult(True, reason, Prerequisite(name='nre_initial_entry', status=status))

    if type(role) is not ActionRole or role.opportunity_id != opportunity.opportunity_id:
        return hold('unbound_action_role')
    if role.available_at_ms > opportunity.created_at_ms:
        return hold('action_role_not_available')
    if role.role in ('SCALE_IN', 'PYRAMID'):
        return GateResult(False, 'noninitial_acquisition_not_filtered', None)
    if role.role != 'INITIAL_ENTRY':
        return hold('initial_entry_role_unavailable')
    milliseconds(now_ms)
    if now_ms < opportunity.created_at_ms:
        return hold('decision_clock_regressed')
    if now_ms >= opportunity.expires_at_ms:
        return hold('opportunity_expired', 'STALE')
    if type(state) is not BoundPath or type(prediction) is not Prediction or type(policy) is not GatePolicy:
        return hold('path_model_or_policy_unavailable')
    if (state.contract.scope != opportunity.scope or state.contract.setup != opportunity.setup
            or state.contract.recipe != policy.recipe or observation_ref(state.snapshot) != opportunity.primary):
        return hold('scope_recipe_or_observation_mismatch')
    try:
        row = evidence(state, now_ms=now_ms)
    except ValueError:
        return hold('path_evidence_stale_or_invalid', 'STALE')
    if row['available_at_ms'] > opportunity.created_at_ms or state.evaluated_at_ms > opportunity.created_at_ms:
        return hold('path_evidence_not_available_at_proposal')
    values = row['features']
    if values['status'] in ('FAILED', 'EXPIRED'):
        return hold('terminal_path', 'HOLD')
    if not values['feature_valid'] or any(values[name] is None for name in policy.required_features):
        return hold('required_path_feature_unavailable')
    if (prediction.opportunity_id != opportunity.opportunity_id or prediction.path_state_sha256 != state.state_sha256
            or prediction.model_sha256 != policy.model_sha256 or prediction.scaler_sha256 != policy.scaler_sha256
            or prediction.horizon_ms != policy.horizon_ms):
        return hold('prediction_binding_mismatch')
    if (prediction.training_cutoff_ms >= state.path_state.last.timestamp_ms
            or prediction.generated_at_ms < state.evaluated_at_ms
            or prediction.generated_at_ms > opportunity.created_at_ms):
        return hold('prediction_not_causal_for_proposal')
    if now_ms >= prediction.valid_until_ms:
        return hold('prediction_expired', 'STALE')
    # Input bounds make this subtraction exact independently of ambient Decimal precision.
    with localcontext() as ctx:
        ctx.prec = 64
        net = prediction.gross_return_bps - prediction.cost_bps - prediction.mean_uncertainty_bps
    payload = {'schema': 'rsibot.nre_initial_entry_prerequisite.v1', 'opportunity_id': opportunity.opportunity_id,
               'path_evidence_sha256': row['evidence_sha256'], 'role': asdict(role), 'policy': asdict(policy),
               'prediction': asdict(prediction), 'now_ms': now_ms, 'net_edge_bps': str(net),
               'execution_authorized': False}
    status = 'PASS' if net >= policy.minimum_net_bps else 'HOLD'
    return GateResult(True, 'net_edge_sufficient' if status == 'PASS' else 'net_edge_below_threshold',
                      Prerequisite(name='nre_initial_entry', status=status, evidence_sha256=digest(payload)), str(net))
