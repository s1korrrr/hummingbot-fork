import unittest
from test.cross_venue_events_v2.test_contracts import T, evidence, snapshot

from hummingbot.strategy_v2.components.decision_protocol import (
    DecisionScope,
    ObservationRef,
    Opportunity,
    Prerequisite,
    decide,
)
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.events_v2.contracts import CoverageScope
from hummingbot.strategy_v2.recipes.cross_venue_events_v2 import bind_observe_recipe, step

CONFIG = {'wait_ms': 60_000}


def recipe(**changes):
    return bind_observe_recipe(source_sha256='e' * 64, configuration=CONFIG, **changes)


class RecipeTests(unittest.TestCase):
    def test_existing_hold_advances_cursor_not_wait_scheduler(self):
        scope = DecisionScope(program_id='fixture', owner_id='observer', connector='okx',
                              trading_pair='BTC-USDC', recipe_sha256='a' * 64)

        def proposal(at):
            ref = ObservationRef(dataset_sha256='b' * 64, epoch='fixture', revision=1,
                                 content_sha256='c' * 64, event_time_ms=at, available_time_ms=at)
            return Opportunity(scope=scope, setup='listing', action='acquire', primary=ref,
                               created_at_ms=at, lifetime_ms=60000)
        cursor, seed = decide(None, proposal(1000), now_ms=1000, prerequisites=())
        cursor, hold = decide(cursor, proposal(2000), now_ms=2000,
                              prerequisites=(Prerequisite(name='event', status='HOLD'),))
        _, retry = decide(cursor, proposal(2000), now_ms=2500,
                          prerequisites=(Prerequisite(name='event', status='PASS', evidence_sha256='d' * 64),))
        self.assertEqual(seed.reason, 'seeded')
        self.assertEqual(hold.reason, 'prerequisite_hold')
        self.assertEqual(retry.reason, 'already_observed')
        self.assertIs(retry.execution_authorized, False)

    def test_observe_waits_until_fixed_lifetime_elapses(self):
        item = evidence()
        bound = recipe()
        view = snapshot((item,))
        waiting = step(None, view, now_ms=item.event_available_ms + 59_000, recipe=bound, configuration=CONFIG)
        self.assertEqual(waiting.state.setups[0].phase, 'WAIT')
        self.assertEqual(waiting.reasons, ('event_wait',))
        self.assertEqual(waiting.opportunities, ())
        ready = step(waiting.state, view, now_ms=item.event_available_ms + 60_000, recipe=bound, configuration=CONFIG)
        self.assertEqual(ready.state.setups[0].phase, 'READY_RESEARCH_ONLY')
        self.assertEqual(ready.reasons, ('ready_research_only',))
        self.assertIs(ready.execution_authorized, False)
        self.assertIsNone(ready.opportunities[0].expected_return)
        self.assertEqual(len(ready.traces), 1)
        self.assertEqual(ready.traces[0].status, 'HOLD')
        self.assertEqual(ready.traces[0].reason, 'acquisition_forbidden')
        self.assertIs(ready.traces[0].execution_authorized, False)

    def test_same_millisecond_occurrences_stay_distinct(self):
        first = evidence(occurrence_id='notice-a', content_sha256='1' * 64)
        second = evidence(occurrence_id='notice-b', content_sha256='2' * 64, asset_id='eip155:1/erc20:0xdef')
        view = snapshot((first, second))
        result = step(None, view, now_ms=T + 1, recipe=recipe(), configuration=CONFIG)
        self.assertEqual({item.occurrence_id for item in result.state.setups}, {'notice-a', 'notice-b'})

    def test_negative_veto_does_not_expire_into_clearance(self):
        item = evidence(kind='TOKEN_DELISTING')
        view = snapshot((item,))
        bound = recipe()
        first = step(None, view, now_ms=T + 1, recipe=bound, configuration=CONFIG)
        self.assertEqual(first.state.setups[0].phase, 'VETO_HOLD')
        later = step(first.state, snapshot((), asof_ms=T + 1, sequence=2, predecessor_sha256=view.dataset_sha256),
                     now_ms=T + 2_000_000, recipe=bound, configuration=CONFIG)
        self.assertEqual(later.state.setups[0].phase, 'VETO_HOLD')
        self.assertTrue(any(item.status == 'HOLD' and item.name.startswith('veto.') for item in later.prerequisites))

    def test_removed_asset_keeps_risk_veto_outside_entry_and_observation_universes(self):
        item = evidence(kind='PAIR_REMOVAL')
        view = snapshot((item,), observation_universe=(), entry_universe=(),
                        management_universe=(item.asset_id,))
        result = step(None, view, now_ms=T + 1, recipe=recipe(), configuration=CONFIG)
        self.assertEqual(result.state.setups[0].phase, 'VETO_HOLD')
        self.assertEqual(result.reasons, ('negative_event_veto',))
        self.assertEqual(result.opportunities, ())
        self.assertIs(result.execution_authorized, False)

    def test_empty_veto_set_is_not_an_admission(self):
        result = step(None, snapshot(()), now_ms=T + 1, recipe=recipe(), configuration=CONFIG)
        self.assertEqual(result.prerequisites, ())
        self.assertEqual(result.opportunities, ())
        self.assertIs(result.execution_authorized, False)

    def test_restored_observation_is_not_re_reported(self):
        item = evidence()
        view = snapshot((item,))
        bound = recipe()
        first = step(None, view, now_ms=item.event_available_ms + 60_000, recipe=bound, configuration=CONFIG)
        self.assertEqual(first.reasons, ('ready_research_only',))
        again = step(first.state, view, now_ms=item.event_available_ms + 61_000, recipe=bound, configuration=CONFIG)
        self.assertEqual(again.reasons, ())
        self.assertEqual(again.opportunities, ())

    def test_coverage_recovery_does_not_reemit_consumed_opportunity(self):
        item = evidence()
        bound = recipe()
        initial = snapshot((item,))
        first = step(None, initial, now_ms=T + 60_000, recipe=bound, configuration=CONFIG)
        self.assertEqual(len(first.opportunities), 1)
        unavailable = CoverageScope(source_id=item.source_id, coverage_state='UNAVAILABLE',
                                    asof_ms=T + 61_000, reason='temporary_outage')
        outage = snapshot((item,), completeness=(unavailable,), asof_ms=T + 61_000,
                          sequence=2, predecessor_sha256=initial.dataset_sha256,
                          dataset_sha256='1' * 64)
        held = step(first.state, outage, now_ms=T + 61_000, recipe=bound, configuration=CONFIG)
        self.assertEqual(held.state.setups[0].phase, 'HOLD')
        self.assertEqual(held.reasons, ('source_coverage_unavailable',))
        self.assertTrue(held.state.setups[0].reported)
        restored = type(held.state).model_validate_json(held.state.model_dump_json())
        recovered = snapshot((item,), asof_ms=T + 62_000, sequence=3,
                             predecessor_sha256=outage.dataset_sha256, dataset_sha256='2' * 64)
        again = step(restored, recovered, now_ms=T + 62_000, recipe=bound, configuration=CONFIG)
        self.assertEqual(again.state.setups[0].phase, 'READY_RESEARCH_ONLY')
        self.assertEqual(again.state.setups[0].consumption_id, first.state.setups[0].consumption_id)
        self.assertEqual(again.opportunities, ())
        self.assertEqual(again.traces, ())
        self.assertEqual(again.reasons, ())

    def test_correction_does_not_extend_wait_or_rearm_ready_setup(self):
        item = evidence()
        bound = recipe()
        initial = snapshot((item,))
        waiting = step(None, initial, now_ms=T + 10_000, recipe=bound, configuration=CONFIG)
        corrected = evidence(stage='CORRECTION', revision=2, predecessor_sha256=item.content_sha256,
                             content_sha256='f' * 64, parsed_ms=T + 30_000, processed_ms=T + 30_000,
                             label_available_ms=T + 30_000, mapping_available_ms=T + 30_000)
        revised = snapshot((corrected,), asof_ms=T + 30_000, sequence=2,
                           predecessor_sha256=initial.dataset_sha256)
        ready = step(waiting.state, revised, now_ms=T + 60_000, recipe=bound, configuration=CONFIG)
        self.assertEqual(ready.state.setups[0].wait_until_ms, T + 60_000)
        self.assertEqual(ready.state.setups[0].phase, 'READY_RESEARCH_ONLY')
        again = step(ready.state, revised, now_ms=T + 61_000, recipe=bound, configuration=CONFIG)
        self.assertEqual(again.opportunities, ())
        self.assertEqual(again.reasons, ())

    def test_source_identity_change_is_rejected(self):
        item = evidence()
        view = snapshot((item,))
        first = step(None, view, now_ms=T + 1, recipe=recipe(), configuration=CONFIG)
        other = bind_observe_recipe(source_sha256='f' * 64, configuration=CONFIG)
        with self.assertRaisesRegex(PolicyError, 'identity changed'):
            step(first.state, view, now_ms=T + 2, recipe=other, configuration=CONFIG)

    def test_clock_regression_is_rejected(self):
        item = evidence()
        view = snapshot((item,))
        bound = recipe()
        first = step(None, view, now_ms=T + 10, recipe=bound, configuration=CONFIG)
        with self.assertRaisesRegex(PolicyError, 'regressed'):
            step(first.state, view, now_ms=T + 9, recipe=bound, configuration=CONFIG)

    def test_listing_rejects_market_that_opened_after_publication(self):
        item = evidence(listed_ms=T - 500, first_public_ms=T - 1_000)
        bound = recipe(recipe_id='cve_listing_v2', wait_ms=0, lifetime_ms=60_000)
        result = step(None, snapshot((item,), entry_universe=(item.asset_id,)),
                      now_ms=T + 1, recipe=bound, configuration=CONFIG)
        self.assertEqual(result.state.setups[0].phase, 'REJECTED')
        self.assertEqual(result.reasons, ('okx_market_not_preexisting',))

    def test_entry_guard_holds_when_entry_coverage_is_incomplete(self):
        item = evidence()
        bound = recipe(recipe_id='cve_entry_guard_v2', wait_ms=0, lifetime_ms=60_000)
        result = step(None, snapshot((item,), entry_universe=(item.asset_id,)),
                      now_ms=T + 1, recipe=bound, configuration=CONFIG)
        self.assertEqual(result.state.setups[0].phase, 'HOLD')
        self.assertEqual(result.reasons, ('entry_coverage_incomplete',))

    def test_entry_guard_ready_when_entry_coverage_is_complete(self):
        item = evidence()
        bound = recipe(recipe_id='cve_entry_guard_v2', wait_ms=0, lifetime_ms=60_000)
        coverage = CoverageScope(source_id='okx_announcements', coverage_state='PARTIAL', asof_ms=T,
                                 complete_for_entry=True, complete_for_risk=False, reason='entry_scope_ready')
        view = snapshot((item,), completeness=(coverage,), entry_universe=(item.asset_id,))
        result = step(None, view, now_ms=T + 1, recipe=bound, configuration=CONFIG)
        self.assertEqual(result.state.setups[0].phase, 'READY_RESEARCH_ONLY')

    def test_entry_guard_holds_asset_outside_entry_universe(self):
        item = evidence()
        bound = recipe(recipe_id='cve_entry_guard_v2', wait_ms=0, lifetime_ms=60_000)
        coverage = CoverageScope(source_id='okx_announcements', coverage_state='PARTIAL', asof_ms=T,
                                 complete_for_entry=True, complete_for_risk=False, reason='entry_scope_ready')
        result = step(None, snapshot((item,), completeness=(coverage,), entry_universe=()),
                      now_ms=T + 1, recipe=bound, configuration=CONFIG)
        self.assertEqual(result.state.setups[0].phase, 'HOLD')
        self.assertEqual(result.opportunities, ())
