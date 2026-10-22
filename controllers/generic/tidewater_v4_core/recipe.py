"""Tidewater's P0 recipe and owner identities.

The P0 composition is Meridian V3's (`MeridianPortfolioRecipe.propose`: the frozen BNB p23
MR proposal and `propose_target`) under Tidewater's own portfolio id, stage and source
identity. The owner identity binds recorder state to Tidewater's code and configuration.
Tidewater uses its own identity kind, so Meridian's migration planner refuses it
explicitly; Tidewater stage changes and code-upgrade migrations are not supported in
slice 1 (paper and replay only).
"""
import hashlib

from controllers.generic import meridian_v3_recipe
from controllers.generic.meridian_v3_portfolio import MeridianPortfolioRecipe, portfolio_source_files
from controllers.generic.meridian_v3_unified_core.migration import _json, symbol_configuration, symbol_fingerprint
from controllers.generic.modular_spot_core.domain import PolicyError
from hummingbot.strategy_v2.components.recipe_contract import CONTROL_FIELDS, digest
from hummingbot.strategy_v2.controllers.portfolio_target_store_v3 import ProposalWriter
from hummingbot.strategy_v2.portfolio_v3.contracts import P0Policy, PortfolioMandate
from hummingbot.strategy_v2.portfolio_v3.runtime_contracts_v1 import GrantPolicy

from .config import CAPITAL_STAGES, PAIRS, PORTFOLIO_ID, PRODUCER, RECIPE_ID, source_files

KIND = 'tidewater_v4_owner_identity_v1'


def source_identity():
    """Digest of Meridian's portfolio closure, Tidewater's files and the neutral components."""
    from pathlib import Path

    from hummingbot.strategy_v2.components.source_identity import checked_source, component_source_files
    root = Path(meridian_v3_recipe.__file__).resolve().parents[2]
    paths = {*[p.resolve() for p in portfolio_source_files()], *[p.resolve() for p in source_files()],
             *[p.resolve() for p in component_source_files()]}
    return digest({str(p.relative_to(root)): hashlib.sha256(checked_source(p)).hexdigest() for p in sorted(paths)})


class TidewaterPortfolioRecipe(MeridianPortfolioRecipe):
    """Meridian's P0 graph under Tidewater's mandate; only the identity checks differ."""

    def __init__(self, mandate: PortfolioMandate, policy: P0Policy):
        self.mandate = PortfolioMandate.model_validate(mandate.model_dump(mode='json'))
        self.policy = P0Policy.model_validate(policy.model_dump(mode='json'))
        allowed = {(s.envelope_quote, s.envelope_quote, s.order_quote) for s in CAPITAL_STAGES.values()}
        stage = (mandate.funding_envelope_quote, mandate.per_asset_quote, mandate.order_quote)
        if (mandate.portfolio_id != PORTFOLIO_ID or set(mandate.pairs) != set(PAIRS) or stage not in allowed
                or mandate.reference_quote != mandate.funding_envelope_quote):
            raise ValueError('Tidewater requires its five pairs and a stage mandate with per-asset = envelope')
        if set(policy.pair_priority) != set(mandate.pairs) or policy.producers != (PRODUCER,):
            raise ValueError('explicit five-pair priority and the BNB MR producer required')
        self.recipe_sha256 = digest(dict(recipe_id=RECIPE_ID, source_sha256=source_identity(),
                                         mandate_sha256=self.mandate.identity, policy_sha256=self.policy.identity,
                                         bnb_candidate_sha256=meridian_v3_recipe.CANDIDATE_SHA256))


def epoch(config, recipe):
    return f'{config.execution_mode}.{recipe.recipe_sha256[:16]}'


def grant_policy(config, recipe):
    mandate = recipe.mandate
    return GrantPolicy(portfolio_id=PORTFOLIO_ID, account_id=config.account_id, controller_id=config.id,
                       connector=config.connector_name, mandate_sha256=mandate.identity,
                       policy_sha256=recipe.policy.identity, pairs=mandate.pairs,
                       envelope_quote=mandate.funding_envelope_quote, per_asset_quote=mandate.per_asset_quote,
                       order_quote=mandate.order_quote)


def proposal_writer(config, recipe, *, fence=1):
    return ProposalWriter(portfolio_id=PORTFOLIO_ID, account_id=config.account_id, writer_id=config.id,
                          epoch=epoch(config, recipe), fence=fence, mandate_sha256=recipe.mandate.identity,
                          recipe_sha256=recipe.recipe_sha256)


def owner_identity(config, recipe):
    """Every identity this owner binds its recorder state to, under the running code.

    Same fields as Meridian's owner identity, so Meridian's guard reads and compares it
    unchanged; the kind and recipe differ."""
    symbols, sources = {}, None
    for pair, symbol in sorted(config.symbol_configs().items()):
        manifest = _json(symbol.source_manifest())
        if sources is not None and manifest != sources:
            raise PolicyError('Tidewater symbol runtimes do not share one source manifest')
        sources = manifest
        values = _json(symbol_configuration(symbol))
        fingerprint = symbol.fingerprint()
        if symbol_fingerprint(values, manifest) != fingerprint:
            raise PolicyError('p23 fingerprint composition changed; identity cannot be verified')
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
