"""Identity witness and the explicit, operator-invoked identity migration of one owner.

The owner's recorder state is bound to identities derived from source and
configuration: the p23 decision checkpoints (symbol fingerprint), the target slot
(writer epoch and portfolio recipe) and the grant row (grant policy). A code-only
image upgrade changes the first two; an explicit `capital_stage` change (between
any two of canary, full and full_2000) changes all three. Without a migration the owner holds new
acquisitions (fail closed, reported as IDENTITY_MIGRATION_REQUIRED) while held-bag
stops and recovery sells continue; it never rewrites or deletes its checkpoints.

The owner records the identity its state is written under (the witness) the
first time it runs with state it can prove is its own. `plan_identity_migration`
recomputes the witnessed fingerprints from their recorded configuration and
sources, checks every persisted row is bound to the witness, and compares the
witnessed configuration with the current one. Without a stage change any
behavioural configuration change is refused. A `capital_stage` change A -> B is
carried only when the witness is exactly the running configuration projected onto
stage A by the stage table (`CAPITAL_STAGES`): every value the table derives
(envelope, per-asset and per-order quote, tranche count, recipe sizing, capital
policy, mandate and grant policy) must equal the table's A value and every other
value must be unchanged, so neither a hand-edited size nor a second behavioural
change rides along. It is refused while the persisted grant reservations exceed the
target stage's envelope or per-asset limit, or a symbol holds more open grants
than the target stage's concurrent tranches. `apply_identity_migration` re-plans
inside one SQLite write transaction, archives every row it changes in a
content-addressed receipt, and carries decision checkpoints (claims, cycle,
clocks), the target chain and the grants forward unchanged except for their
identity binding. Native orders, fills, executors, positions and held-bag trails
are never touched: nothing is sold, closed or dropped to fit a stage.
"""
import json
from decimal import Decimal as D
from types import SimpleNamespace

from controllers.generic.meridian_v3_portfolio import MeridianPortfolioRecipe, source_identity
from controllers.generic.modular_spot_core.domain import PolicyError, identity
from controllers.generic.modular_spot_core.state import DecisionStore, decode, encode
from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.components.recipe_contract import CONTROL_FIELDS, canonical_json, digest
from hummingbot.strategy_v2.controllers.native_portfolio_bridge_v3 import PortfolioGrantStore
from hummingbot.strategy_v2.controllers.portfolio_target_store_v3 import NativeTargetStore, ProposalWriter
from hummingbot.strategy_v2.portfolio_v3.contracts import P0Policy, PortfolioMandate
from hummingbot.strategy_v2.portfolio_v3.runtime_contracts_v1 import GrantPolicy, PortfolioGrant

from .config import CAPITAL_STAGES, PAIRS, PORTFOLIO_ID, RECIPE_ID

WITNESS_PREFIX = 'meridian-v3-identity:v1:'
RECEIPT_PREFIX = 'meridian-v3-migration:v1:'
KIND = 'meridian_v3_owner_identity_v1'
IDENTITY_MIGRATION_REQUIRED = 'IDENTITY_MIGRATION_REQUIRED'
IDENTITY_WITNESS_MISSING = 'IDENTITY_WITNESS_MISSING'
IDENTITY_UNVERIFIED = 'IDENTITY_UNVERIFIED'
FINGERPRINT_EXCLUDED = frozenset({'manual_kill_switch', 'execution_enabled', 'acquisition_enabled'})


class MigrationRefused(PolicyError):
    """The migration would carry state across a change it cannot prove is code-only."""


def _json(value):
    return json.loads(canonical_json(value))


# ---------------------------------------------------------------------- identities
def grant_policy(config, recipe):
    mandate = recipe.mandate
    return GrantPolicy(portfolio_id=PORTFOLIO_ID, account_id=config.account_id, controller_id=config.id,
                       connector=config.connector_name, mandate_sha256=mandate.identity,
                       policy_sha256=recipe.policy.identity, pairs=mandate.pairs,
                       envelope_quote=mandate.funding_envelope_quote, per_asset_quote=mandate.per_asset_quote,
                       order_quote=mandate.order_quote)


def epoch(config, recipe):
    return f'{config.execution_mode}.{recipe.recipe_sha256[:16]}'


def proposal_writer(config, recipe, *, fence=1):
    return ProposalWriter(portfolio_id=PORTFOLIO_ID, account_id=config.account_id, writer_id=config.id,
                          epoch=epoch(config, recipe), fence=fence, mandate_sha256=recipe.mandate.identity,
                          recipe_sha256=recipe.recipe_sha256)


def symbol_configuration(symbol_config):
    """The configuration half of `ModularSpotControllerConfig.fingerprint`, verified by `owner_identity`."""
    values = symbol_config.model_dump(mode='json', exclude=set(FINGERPRINT_EXCLUDED))
    values['universe'] = {**values['universe'], 'symbols': [], 'benchmarks': []}
    values['data'] = {**values['data'], 'symbols': []}
    return values


def symbol_fingerprint(configuration, sources):
    return identity({'configuration': configuration, 'sources': sources, 'policy_version': 2})


def owner_identity(config):
    """Every identity this owner binds its recorder state to, under the running code."""
    recipe = MeridianPortfolioRecipe(config.mandate(), config.p0_policy())
    symbols, sources = {}, None
    for pair, symbol in sorted(config.symbol_configs().items()):
        manifest = _json(symbol.source_manifest())
        if sources is not None and manifest != sources:
            raise PolicyError('Meridian symbol runtimes do not share one source manifest')
        sources = manifest
        values = _json(symbol_configuration(symbol))
        fingerprint = symbol.fingerprint()
        if symbol_fingerprint(values, manifest) != fingerprint:
            raise PolicyError('p23 fingerprint composition changed; identity migration cannot verify it')
        symbols[pair] = {'fingerprint': fingerprint, 'configuration': values}
    policy = grant_policy(config, recipe)
    return _json({
        'schema_version': 1, 'kind': KIND, 'controller_id': config.id, 'recipe_id': RECIPE_ID,
        'configuration': {k: v for k, v in config.sealed_payload().items() if k not in CONTROL_FIELDS},
        'recipe_sha256': config.recipe_digest(), 'source_sha256': digest(config.source_manifest()),
        'portfolio_source_sha256': source_identity(), 'mandate': recipe.mandate.model_dump(mode='json'),
        'policy': recipe.policy.model_dump(mode='json'), 'writer': proposal_writer(config, recipe).model_dump(mode='json'),
        'grant_policy': policy.model_dump(mode='json'), 'grant_policy_sha256': policy.identity,
        'symbol_sources': sources, 'symbols': symbols})


# ---------------------------------------------------------------------- recorder rows
def _recorder(sessions, config_file_path):
    return SimpleNamespace(config_file_path=config_file_path, sql_manager=SimpleNamespace(get_new_session=sessions))


def witness_key(config, config_file_path):
    return WITNESS_PREFIX + identity([config_file_path, config.id])


def _stores(config, sessions, config_file_path, current):
    recorder = _recorder(sessions, config_file_path)
    return (NativeTargetStore(sessions, ProposalWriter.model_validate(current['writer'])),
            PortfolioGrantStore(recorder, GrantPolicy.model_validate(current['grant_policy'])),
            {pair: DecisionStore(recorder, config.id, config.connector_name, pair).key for pair in PAIRS})


def _rows(session, config, config_file_path, targets, grants, decisions):
    def raw(key):
        row = session.get(Metadata, key)
        return None if row is None else row.value
    return {'witness': raw(witness_key(config, config_file_path)), 'target': raw(targets.key),
            'grants': raw(grants.key), 'decisions': {pair: raw(key) for pair, key in decisions.items()}}


def _head_writer(raw):
    if raw is None:
        return None
    return json.loads(raw).get('writer')


def _consistent(rows, expected):
    """Names of persisted rows not bound to `expected` (an owner identity)."""
    bad = []
    if rows['target'] is not None and _head_writer(rows['target']) != expected['writer']:
        bad.append('target')
    if rows['grants'] is not None and json.loads(rows['grants']).get('policy_hash') != expected['grant_policy_sha256']:
        bad.append('grants')
    for pair, raw in sorted(rows['decisions'].items()):
        if raw is not None and decode(raw).fingerprint != expected['symbols'][pair]['fingerprint']:
            bad.append(f'decision:{pair}')
    return bad


# ---------------------------------------------------------------------- runtime guard
class IdentityGuard:
    """Checked every tick until bound. Writes only the first witness of a proven owner."""

    def __init__(self, config):
        self.config = config
        self.current = None
        self.bound = False

    def check(self, sessions, config_file_path):
        """None when bound, otherwise the explicit hold reason."""
        if self.bound:
            return None
        if self.current is None:
            self.current = owner_identity(self.config)
        current = self.current
        targets, grants, decisions = _stores(self.config, sessions, config_file_path, current)
        with sessions() as session, session.begin():
            session.connection().exec_driver_sql('BEGIN IMMEDIATE')
            rows = _rows(session, self.config, config_file_path, targets, grants, decisions)
            if rows['witness'] is None:
                bad = _consistent(rows, current)
                if bad:
                    return f'{IDENTITY_WITNESS_MISSING}:state_not_bound_to_running_identity:{",".join(bad)}'
                session.add(Metadata(key=witness_key(self.config, config_file_path), value=canonical_json(current)))
                self.bound = True
                return None
        witness = json.loads(rows['witness'])
        if witness == current:
            self.bound = True
            return None
        try:
            plan = _plan(self.config, current, rows)
            outcome = f'migration_available:{"+".join(plan["kinds"])}:{plan["transition_id"]}'
        except PolicyError as exc:
            outcome = f'migration_refused:{exc}'
        return f'{IDENTITY_MIGRATION_REQUIRED}:{outcome}'[:300]


# ---------------------------------------------------------------------- migration
ZERO = D('0')
MISSING = object()


def _stage_change(witness, current):
    """(from, to) when the explicit capital stage changed; its live binding may move only its envelope."""
    old, new = witness['configuration'], current['configuration']
    if old.get('capital_stage') == new.get('capital_stage'):
        return None
    if old.get('capital_stage') not in CAPITAL_STAGES or new.get('capital_stage') not in CAPITAL_STAGES:
        raise MigrationRefused('unknown capital stage; identity migration refused')
    old_live, new_live = old.get('live_capital'), new.get('live_capital')
    if (old_live is None) != (new_live is None) or (old_live is not None and (
            {k: v for k, v in old_live.items() if k != 'envelope_quote'}
            != {k: v for k, v in new_live.items() if k != 'envelope_quote'})):
        raise MigrationRefused('live capital binding changed beyond its stage envelope; identity migration refused')
    return old['capital_stage'], new['capital_stage']


def _flatten(value, prefix=''):
    """Dotted path -> JSON leaf. Lists are leaves: a reordered priority is one changed field."""
    if isinstance(value, dict) and value:
        result = {}
        for key, item in value.items():
            result.update(_flatten(item, f'{prefix}{key}.'))
        return result
    return {prefix[:-1]: value}


def _behaviour(owner):
    """Every behavioural value of an owner identity: no source digest, no identity derived from code."""
    return _flatten({**{k: owner[k] for k in ('configuration', 'mandate', 'policy', 'grant_policy')},
                     'symbols': {pair: owner['symbols'][pair]['configuration'] for pair in PAIRS}})


def stage_projection(config, stage):
    """`_behaviour` of `config` with only its capital stage replaced, under the running code.

    The stage's values come from `CAPITAL_STAGES` through the same config methods the
    runtime uses (mandate, recipe sizing, capital policy, budget, grant policy); this
    module holds no copy of any stage number. Control fields and the seal are dropped:
    they are not behavioural and the seal binds one stage only.
    """
    values = config.model_dump(mode='json')
    envelope = str(CAPITAL_STAGES[stage].envelope_quote)
    values.update(capital_stage=stage, total_amount_quote=envelope, recipe_binding=None, execution_enabled=False)
    if values.get('live_capital') is not None:
        values['live_capital'] = {**values['live_capital'], 'envelope_quote': envelope}
    projected = type(config).model_validate(values)
    mandate, policy = projected.mandate(), projected.p0_policy()
    return _behaviour(_json({
        'configuration': {k: v for k, v in projected.sealed_payload().items() if k not in CONTROL_FIELDS},
        'mandate': mandate.model_dump(mode='json'), 'policy': policy.model_dump(mode='json'),
        'grant_policy': grant_policy(projected, SimpleNamespace(mandate=mandate, policy=policy)).model_dump(mode='json'),
        'symbols': {pair: {'configuration': symbol_configuration(symbol)}
                    for pair, symbol in projected.symbol_configs().items()}}))


def _differs(a, b):
    return sorted(k for k in set(a) | set(b) if a.get(k, MISSING) != b.get(k, MISSING))


def _check_stage_projection(config, witness, current, stage):
    """The stage-derived values that change from `stage[0]` to `stage[1]`, as {path: [from, to]}.

    A field is stage-derived when the stage table gives it different values in any two
    stages. The witness must be the running configuration projected onto the source
    stage exactly; a differing stage-derived value means sizing that is not the table's.
    """
    projections = {name: stage_projection(config, name) for name in CAPITAL_STAGES}
    derived = set()
    for name in CAPITAL_STAGES:
        derived.update(_differs(projections[name], projections[stage[1]]))
    if _differs(_behaviour(current), projections[stage[1]]):
        raise MigrationRefused('running configuration is not its own capital stage projection; identity migration refused')
    changed = _differs(_behaviour(witness), projections[stage[0]])
    other = [k for k in changed if k not in derived]
    if other:
        raise MigrationRefused(f'behavioural configuration changed ({",".join(other)}); only code '
                               'and an explicit capital_stage change can be carried')
    if changed:
        raise MigrationRefused(f'stage-derived values differ from the {stage[0]} stage table ({",".join(changed)}); '
                               'hand-edited sizing cannot be carried; identity migration refused')
    source, target = projections[stage[0]], projections[stage[1]]
    return {k: [source.get(k), target.get(k)] for k in sorted(derived) if source.get(k, MISSING) != target.get(k, MISSING)}


def _grant_exposure(grants):
    """Reserved quote and open grant count of every persisted grant, before native reconciliation drops any.

    Every Meridian acquisition spends inside a grant, and a grant is released only by
    native proof, so this bounds the owner's capital at cost from above and its open
    tranches per symbol from above.
    """
    by_asset, open_grants = {}, {}
    for grant in (grants or {}).get('grants', ()):
        value = PortfolioGrant.model_validate(grant)
        by_asset[value.symbol] = by_asset.get(value.symbol, ZERO) + value.reserved_quote
        open_grants[value.symbol] = open_grants.get(value.symbol, 0) + 1
    return {'total': sum(by_asset.values(), ZERO), 'by_asset': by_asset, 'open_grants': open_grants}


def _check_target_stage_state(stage, exposure):
    """Carried state must satisfy the target stage's limits; the migration never sells or drops to fit."""
    target = CAPITAL_STAGES[stage[1]]
    worst = max(exposure['by_asset'].values(), default=ZERO)
    if exposure['total'] > target.envelope_quote or worst > target.per_asset_quote:
        raise MigrationRefused(
            f'capital stage {stage[0]} -> {stage[1]} refused: reserved exposure {exposure["total"]} quote '
            f'(largest asset {worst}) exceeds the {stage[1]} envelope {target.envelope_quote} / per-asset '
            f'{target.per_asset_quote}; let exposure fall below the target stage first')
    for pair, count in sorted(exposure['open_grants'].items()):
        if count > target.max_active_tranches:
            raise MigrationRefused(
                f'capital stage {stage[0]} -> {stage[1]} refused: {pair} holds {count} open grants but the '
                f'{stage[1]} stage admits {target.max_active_tranches} concurrent tranches per symbol; '
                'let open tranches fall to the target stage first')


def _changes(old, new):
    keys = set(old) | set(new)
    return sorted(k for k in keys if old.get(k) != new.get(k))


def _plan(config, current, rows):
    raw = rows['witness']
    if raw is None:
        raise MigrationRefused('no identity witness for this owner; explicit reconciliation required')
    witness = json.loads(raw)
    if not isinstance(witness, dict) or witness.get('schema_version') != 1 or witness.get('kind') != KIND:
        raise MigrationRefused('identity witness schema unsupported')
    if witness == current:
        raise MigrationRefused('owner identity is unchanged; nothing to migrate')
    if witness['controller_id'] != current['controller_id'] or witness['recipe_id'] != current['recipe_id']:
        raise MigrationRefused('another owner or recipe; identity migration refused')
    # The witness must reproduce its own identities before it can vouch for persisted state.
    for pair in PAIRS:
        entry = witness['symbols'][pair]
        if symbol_fingerprint(entry['configuration'], witness['symbol_sources']) != entry['fingerprint']:
            raise MigrationRefused(f'identity witness for {pair} does not reproduce its fingerprint')
    if (PortfolioMandate.model_validate(witness['mandate']).identity != witness['writer']['mandate_sha256']
            or GrantPolicy.model_validate(witness['grant_policy']).identity != witness['grant_policy_sha256']
            or P0Policy.model_validate(witness['policy']).identity != witness['grant_policy']['policy_sha256']):
        raise MigrationRefused('identity witness is internally inconsistent')
    bad = _consistent(rows, witness)
    if bad:
        raise MigrationRefused(f'persisted state is not bound to the witnessed identity: {",".join(bad)}')
    stage = _stage_change(witness, current)
    if stage:
        stage_changes = _check_stage_projection(config, witness, current, stage)
    else:
        stage_changes = {}
        changed = _changes(witness['configuration'], current['configuration'])
        if changed:
            raise MigrationRefused(f'behavioural configuration changed ({",".join(changed)}); only code '
                                   'and an explicit capital_stage change can be carried')
        if witness['policy'] != current['policy']:
            raise MigrationRefused('P0 policy changed; identity migration refused')
        if witness['mandate'] != current['mandate']:
            raise MigrationRefused('portfolio mandate changed beyond the capital stage; identity migration refused')
        for pair in PAIRS:
            if witness['symbols'][pair]['configuration'] != current['symbols'][pair]['configuration']:
                raise MigrationRefused(f'{pair} effective p23 configuration changed; identity migration refused')
        if witness['grant_policy'] != current['grant_policy']:
            raise MigrationRefused('grant policy changed beyond the capital stage; identity migration refused')
    decisions = {pair: decode(value) for pair, value in rows['decisions'].items() if value is not None}
    target = None if rows['target'] is None else json.loads(rows['target'])
    grants = None if rows['grants'] is None else json.loads(rows['grants'])
    code = (witness['source_sha256'] != current['source_sha256']
            or witness['portfolio_source_sha256'] != current['portfolio_source_sha256']
            or witness['symbol_sources'] != current['symbol_sources'])
    kinds = [*(['code_upgrade'] if code else []), *([f'capital_stage_{stage[0]}_to_{stage[1]}'] if stage else [])]
    if not kinds:
        raise MigrationRefused('identity changed without a code or capital stage change; explicit reconciliation required')
    exposure = _grant_exposure(grants)
    if stage:
        _check_target_stage_state(stage, exposure)
    transition_id = identity([witness, current, rows])
    return {
        'transition_id': transition_id, 'kinds': kinds, 'controller_id': current['controller_id'],
        'stage_fields': list(stage_changes), 'stage_changes': stage_changes,
        'from': {k: witness[k] for k in ('source_sha256', 'portfolio_source_sha256', 'recipe_sha256')}
        | {'epoch': witness['writer']['epoch'], 'grant_policy_sha256': witness['grant_policy_sha256']},
        'to': {k: current[k] for k in ('source_sha256', 'portfolio_source_sha256', 'recipe_sha256')}
        | {'epoch': current['writer']['epoch'], 'grant_policy_sha256': current['grant_policy_sha256']},
        'carried': {
            'decisions': {pair: {'claims': len(state.claims), 'cycle': state.cycle,
                                 'committed_quote': str(state.committed_quote)} for pair, state in sorted(decisions.items())},
            'target_head': None if target is None else target['target_sha256'],
            'grants': None if grants is None else len(grants['grants']),
            'reserved_quote': str(exposure['total']),
            'released': None if grants is None else len(grants['released']),
        }}


def plan_identity_migration(config, sessions, config_file_path):
    """Read-only. The reviewed plan an operator applies with its `transition_id`."""
    current = owner_identity(config)
    targets, grants, decisions = _stores(config, sessions, config_file_path, current)
    with sessions() as session:
        rows = _rows(session, config, config_file_path, targets, grants, decisions)
    return _plan(config, current, rows)


def apply_identity_migration(config, sessions, config_file_path, transition_id):
    """Apply one reviewed plan atomically. Changed evidence since planning refuses it."""
    current = owner_identity(config)
    targets, grants, decisions = _stores(config, sessions, config_file_path, current)
    with sessions() as session, session.begin():
        session.connection().exec_driver_sql('BEGIN IMMEDIATE')
        rows = _rows(session, config, config_file_path, targets, grants, decisions)
        plan = _plan(config, current, rows)
        if plan['transition_id'] != transition_id:
            raise MigrationRefused('recorder state changed since the reviewed plan; plan again')
        witness = json.loads(rows['witness'])
        before, after = {witness_key(config, config_file_path): rows['witness']}, {}
        for pair, key in decisions.items():
            raw = rows['decisions'][pair]
            if raw is None:
                continue
            state = decode(raw).model_copy(update={'fingerprint': current['symbols'][pair]['fingerprint']})
            row = session.get(Metadata, key)
            before[key], row.value = raw, encode(state)
            after[key] = row.value
        carried = targets.carry_forward(session, ProposalWriter.model_validate(witness['writer']),
                                        migration_sha256=transition_id)
        if carried is not None:
            before[targets.key], after[targets.key] = carried
        if witness['grant_policy_sha256'] != current['grant_policy_sha256']:
            rebound = grants.rebind_policy(session, witness['grant_policy_sha256'])
            if rebound is not None:
                before[grants.key], after[grants.key] = rebound
        session.get(Metadata, witness_key(config, config_file_path)).value = canonical_json(current)
        after[witness_key(config, config_file_path)] = canonical_json(current)
        receipt = {'schema_version': 1, 'kind': 'meridian_v3_identity_migration_v1', 'plan': plan,
                   'before': before, 'after_sha256': identity(after)}
        key = RECEIPT_PREFIX + transition_id
        if session.get(Metadata, key) is not None:
            raise MigrationRefused('this transition was already applied')
        session.add(Metadata(key=key, value=canonical_json(receipt)))
    return {'receipt_key': key, **plan}
