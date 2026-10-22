import unittest
from test.cross_venue_events_v2.test_contracts import T, evidence, snapshot
from test.cross_venue_events_v2.test_recipe import CONFIG, recipe

from hummingbot.strategy_v2.recipes.cross_venue_events_v2 import bind_observe_recipe, consumption_id, step


class EventSetupTests(unittest.TestCase):
    def test_event_consumption_id_is_not_the_native_timestamp_key(self):
        first = evidence(occurrence_id='a', asset_id='eip155:1/erc20:0xaaa')
        second = evidence(occurrence_id='b', asset_id='eip155:1/erc20:0xbbb', content_sha256='2' * 64)
        bound = recipe()
        self.assertNotEqual(consumption_id(bound, first), consumption_id(bound, second))
        view = snapshot((first, second))
        result = step(None, view, now_ms=T + 1, recipe=bound, configuration=CONFIG)
        self.assertEqual(len({item.consumption_id for item in result.state.setups}), 2)

    def test_expired_setup_rejects_without_rearming(self):
        item = evidence()
        bound = bind_observe_recipe(source_sha256='e' * 64, configuration=CONFIG, wait_ms=0, lifetime_ms=1)
        result = step(None, snapshot((item,)), now_ms=item.event_available_ms + 1, recipe=bound, configuration=CONFIG)
        self.assertEqual(result.state.setups[0].phase, 'REJECTED')
        self.assertEqual(result.opportunities, ())
