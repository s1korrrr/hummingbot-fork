import dataclasses
import importlib.util
import unittest
from decimal import Decimal


class ImportContract(unittest.TestCase):
    def test_policy_module_exists(self):
        self.assertIsNotNone(importlib.util.find_spec('controllers.generic.cross_venue_event_core'))


if importlib.util.find_spec('controllers.generic.cross_venue_event_core'):
    from controllers.generic.cross_venue_event_core import Event, Instrument, Policy, Quote, evaluate, evaluate_batch
    T = 1780000000000

    def event(**changes):
        values = dict(source='BINANCE', source_event_id='article-1', asset_id='eip155:1/erc20:0xabc',
                      source_url='https://www.binance.com/en/support/announcement/example', raw_sha256='a' * 64,
                      kind='SPOT_LISTING_ANNOUNCED', published_ms=T - 1000, received_ms=T, parsed_ms=T + 100,
                      label_known_ms=T + 100, identity_known_ms=T - 10000, novel=True, revision=False)
        values.update(changes)
        return Event(**values)

    def instrument(**changes):
        values = dict(asset_id=event().asset_id, pair='ABC-USDC', venue='OKX', region='EEA', capital_model='SPOT_CASH',
                      listed_ms=T - 100000, known_ms=T - 10000, valid_until_ms=T + 3600000,
                      account_eligible=True, state='live', evidence_sha256='b' * 64)
        values.update(changes)
        return Instrument(**values)

    def quote(**changes):
        values = dict(pair='ABC-USDC', exchange_ms=T + 60100, received_ms=T + 60100,
                      bid='100.10', ask='100.20', ask_size='10', reference_mid='100',
                      reference_known_ms=T - 1000, feature_known_ms=T + 60100,
                      buy_imbalance='0.4', relative_turnover='2.0')
        values.update(changes)
        return Quote(**values)

    def decision(e=None, i=None, q=None, p=None, now=T + 60100):
        return evaluate(e or event(), i or instrument(), q or quote(), p or Policy(), now)

    class PolicyTests(unittest.TestCase):
        def test_availability_uses_latest_required_input(self):
            self.assertEqual(event(label_known_ms=T + 70000).available_ms, T + 70000)

        def test_delayed_entry_is_research_not_order(self):
            d = decision()
            self.assertEqual(d['reason'], 'ELIGIBLE_RESEARCH_ONLY')
            self.assertFalse(d['execution_allowed'])
            self.assertNotIn('order', d)

        def test_publication_does_not_replace_receipt(self):
            self.assertEqual(decision(now=T + 60099)['reason'], 'WAIT_DELAY')

        def test_late_labels_do_not_backdate(self):
            self.assertEqual(decision(e=event(label_known_ms=T + 60000))['reason'], 'WAIT_DELAY')

        def test_future_event_is_not_known(self):
            self.assertEqual(decision(e=event(label_known_ms=T + 90000))['reason'], 'EVENT_NOT_AVAILABLE')

        def test_delayed_old_news_rejected(self):
            self.assertEqual(decision(e=event(published_ms=T - 400000))['reason'], 'SOURCE_DELAY_EXCEEDED')

        def test_unknown_publication_is_not_fast_news(self):
            self.assertEqual(decision(e=event(published_ms=None))['reason'], 'PUBLICATION_TIME_UNAVAILABLE')

        def test_future_publication_is_clock_fault(self):
            self.assertEqual(decision(e=event(published_ms=T + 1))['reason'], 'SOURCE_CLOCK_FAULT')

        def test_quote_stale(self):
            self.assertEqual(decision(q=quote(received_ms=T, exchange_ms=T))['reason'], 'QUOTE_STALE')

        def test_old_exchange_quote_cannot_be_freshened_by_receipt(self):
            self.assertEqual(decision(q=quote(exchange_ms=T))['reason'], 'QUOTE_STALE')

        def test_future_quote_or_features_fail(self):
            for field in ('received_ms', 'exchange_ms', 'feature_known_ms'):
                self.assertEqual(decision(q=quote(**{field: T + 61000}))['reason'], 'MARKET_NOT_AVAILABLE')

        def test_invalid_number_and_crossed_book(self):
            for value in ('NaN', 'Infinity', '-1', '0'):
                with self.assertRaises(ValueError):
                    quote(ask=value)
            with self.assertRaises(ValueError):
                quote(bid='101')

        def test_strict_boolean_and_timestamp(self):
            with self.assertRaises(ValueError):
                event(novel='true')
            with self.assertRaises(ValueError):
                event(received_ms=True)
            with self.assertRaises(ValueError):
                event(parsed_ms=T - 1)

        def test_issuer_host_must_match(self):
            with self.assertRaises(ValueError):
                event(source_url='https://binance.com.attacker.invalid/example')

        def test_identity_not_symbol(self):
            self.assertEqual(decision(i=instrument(asset_id='solana:mainnet/abc'))['reason'], 'ASSET_IDENTITY_MISMATCH')

        def test_eea_eligibility_not_global_listing(self):
            for changes in ({'account_eligible': False}, {'region': 'GLOBAL'}, {'capital_model': 'PERPS'}, {'state': 'suspend'}):
                self.assertEqual(decision(i=instrument(**changes))['reason'], 'INSTRUMENT_INELIGIBLE')

        def test_instrument_must_preexist_information(self):
            self.assertEqual(decision(i=instrument(listed_ms=T + 1000))['reason'], 'NOT_PREEXISTING_OKX_MARKET')

        def test_future_or_expired_metadata(self):
            self.assertEqual(decision(i=instrument(known_ms=T + 61000))['reason'], 'INSTRUMENT_NOT_AVAILABLE')
            self.assertEqual(decision(i=instrument(valid_until_ms=T + 50000))['reason'], 'INSTRUMENT_NOT_AVAILABLE')

        def test_quote_currency_policy(self):
            self.assertEqual(decision(i=instrument(pair='ABC-USDT'), q=quote(pair='ABC-USDT'))['reason'], 'QUOTE_NOT_ADMITTED')

        def test_spread_and_chase(self):
            self.assertEqual(decision(q=quote(ask='101'))['reason'], 'SPREAD_TOO_WIDE')
            self.assertEqual(decision(q=quote(bid='104', ask='104.1'))['reason'], 'MOVE_ALREADY_EXTENDED')

        def test_non_events_reminders_and_perps_not_spot_alpha(self):
            for kind in ('ROADMAP', 'TRADING_LAUNCH', 'PERP_LISTING', 'PAIR_REMOVAL', 'STATUS', 'OTHER'):
                self.assertEqual(decision(e=event(kind=kind))['reason'], 'EVENT_TYPE_NOT_ADMITTED')
            for changes in ({'novel': False}, {'revision': True}):
                self.assertEqual(decision(e=event(**changes))['reason'], 'NOT_NOVEL')

        def test_same_venue_positive_is_not_cross_venue(self):
            e = event(source='OKX', source_url='https://www.okx.com/help/example')
            self.assertEqual(decision(e=e)['reason'], 'NOT_CROSS_VENUE')

        def test_filter_requires_available_features(self):
            self.assertEqual(decision(q=quote(buy_imbalance=None), p=Policy(variant='reaction_filter'))['reason'], 'FEATURES_UNAVAILABLE')
            self.assertEqual(decision(p=Policy(variant='reaction_filter'))['reason'], 'ELIGIBLE_RESEARCH_ONLY')

        def test_negative_news_latches_without_short_or_loss_sell(self):
            negative = event(source_event_id='bad-1', kind='TOKEN_DELISTING', published_ms=T - 10000000,
                             received_ms=T - 9000000, parsed_ms=T - 9000000, label_known_ms=T - 9000000)
            rows = evaluate_batch([event(), negative], [instrument()], [quote()], Policy(), T + 60100)
            self.assertTrue(all(r['reason'] == 'NEGATIVE_EVENT_LATCH' for r in rows))
            self.assertTrue(all(not r['execution_allowed'] for r in rows))

        def test_future_negative_does_not_leak(self):
            future = event(source_event_id='bad-2', kind='SECURITY_INCIDENT', label_known_ms=T + 70000)
            rows = evaluate_batch([event(), future], [instrument()], [quote()], Policy(), T + 60100)
            self.assertEqual(rows[0]['reason'], 'ELIGIBLE_RESEARCH_ONLY')

        def test_duplicate_event_identity_and_metadata_rejected(self):
            with self.assertRaises(ValueError):
                evaluate_batch([event(), event()], [instrument()], [quote()], Policy(), T + 60100)
            with self.assertRaises(ValueError):
                evaluate_batch([event()], [instrument(), instrument()], [quote()], Policy(), T + 60100)

        def test_config_is_frozen_validated_and_hash_bound(self):
            with self.assertRaises(ValueError):
                Policy(delay_ms=-1)
            with self.assertRaises(ValueError):
                Policy(variant='best_in_hindsight')
            with self.assertRaises(dataclasses.FrozenInstanceError):
                Policy().delay_ms = 0
            self.assertNotEqual(decision()['policy_sha256'], decision(p=Policy(delay_ms=1))['policy_sha256'])
