import asyncio
import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from hummingbot.connector.markets_recorder import MarketsRecorder
from scripts.component_paper_runtime import PaperConfig, PaperRuntime, PublicLiquidity, parse_rules


def config(tmp_path, **kwargs):
    return PaperConfig(
        bot_name="breakout_paper_v2",
        pairs=["BTC-USDC"],
        state_path=tmp_path / "paper.sqlite",
        status_path=tmp_path / "status.json",
        candle_origin="http://127.0.0.1:8091",
        **kwargs,
    )


def instrument(pair="BTC-USDC"):
    return {
        "instId": pair,
        "state": "live",
        "instType": "SPOT",
        "quoteCcy": "USDC",
        "minSz": ".001",
        "tickSz": ".01",
        "lotSz": ".001",
    }


def rules(pairs=("BTC-USDC",)):
    return parse_rules({"code": "0", "data": [instrument(p) for p in pairs]}, pairs)


def book(now, pair="BTC-USDC"):
    return {
        "arg": {"channel": "books5", "instId": pair},
        "action": "snapshot",
        "data": [
            {
                "ts": str(int(now * 1000)),
                "bids": [["100.9", "100", "0", "2"]],
                "asks": [["101.1", "100", "0", "2"]],
            }
        ],
    }


def row(timestamp, close="100"):
    return dict(
        open_time=timestamp,
        open=close,
        close=close,
        high=str(Decimal(close) + Decimal(".1")),
        low=str(Decimal(close) - Decimal(".1")),
        volume="100",
        quote_volume="10000",
        confirmed=True,
    )


async def ack(feed):
    for pair in feed.pairs:
        for channel in ('books5', 'trades'):
            await feed.accept({'event': 'subscribe', 'arg': {'instId': pair, 'channel': channel}}, 600000)


async def seed(runtime, now):
    await ack(runtime.liquidity)
    runtime.candles.clock = lambda: now
    runtime.candles.begin_session()
    runtime.candles.apply(
        dict(
            v=1,
            type="snapshot",
            epoch="fixture",
            published_at=now,
            received_at=now,
            pair="BTC-USDC",
            sequence=1,
            rows=[row(t) for t in range(now - 3600, now, 60)],
        )
    )


def test_config_and_current_instrument_admission(tmp_path):
    with pytest.raises(ValueError):
        config(tmp_path, exchange_secret="forbidden")
    with pytest.raises(ValueError):
        PaperConfig(
            **{**config(tmp_path).model_dump(), "pairs": ["BTC-USDC", "BTC-USDC"]}
        )
    with pytest.raises(ValueError, match="missing"):
        parse_rules({"code": "0", "data": []}, ["BTC-USDC"])
    with pytest.raises(ValueError, match="non-live"):
        parse_rules(
            {"code": "0", "data": [{**instrument(), "state": "suspend"}]}, ["BTC-USDC"]
        )


@pytest.mark.asyncio
async def test_native_public_parser_duplicate_depth_and_trade_identity():
    feed = PublicLiquidity(["BTC-USDC"])
    try:
        await ack(feed)
        await feed.accept(book(600000), 600000)
        assert feed.messages == 1
        await feed.accept(book(600000), 600000)
        assert feed.messages == 1
        trade = {
            "arg": {"channel": "trades", "instId": "BTC-USDC"},
            "data": [
                {
                    "instId": "BTC-USDC",
                    "tradeId": "77",
                    "ts": "600000000",
                    "side": "sell",
                    "px": "100",
                    "sz": "1",
                }
            ],
        }
        await feed.accept(trade, 600000)
        await feed.accept(trade, 600000)
        assert len(feed.trades) == 1
        await feed.accept(book(599990), 600000)
        assert feed.stale_inputs == 1
        with pytest.raises(ValueError, match="Unsubscribed"):
            await feed.accept(book(600000, "ETH-USDC"), 600000)
        feed.trades.extend([{}] * 4095)
        trade["data"][0]["tradeId"] = "78"
        with pytest.raises(RuntimeError, match="overflow"):
            await feed.accept(trade, 600000)
        assert feed.fatal == "public_trade_queue_overflow"
    finally:
        await feed.close()


@pytest.mark.asyncio
async def test_public_data_without_subscription_ack_rejected():
    feed = PublicLiquidity(['BTC-USDC'])
    try:
        with pytest.raises(ValueError, match='acknowledg'):
            await feed.accept(book(600000), 600000)
        assert not feed.books
    finally:
        await feed.close()


@pytest.mark.asyncio
async def test_captured_public_books5_wire_without_action_is_snapshot():
    captured = json.loads((Path(__file__).parent / 'fixtures/okx_books5_public_snapshot.json').read_text())
    feed = PublicLiquidity(['BTC-USDC'])
    try:
        for observation in captured:
            await feed.accept(observation['frame'], observation['received_at'])
        assert feed.subscribed('BTC-USDC')
        assert feed.messages == 1
        assert feed.books['BTC-USDC']['bids'] and feed.books['BTC-USDC']['asks']
        latency = feed.latency_summary()['books5']
        assert latency['count'] == 1 and 0 <= latency['max_seconds'] <= 5
        assert latency['rolling_p95_seconds'] == latency['max_seconds']
        bad = dict(captured[-1]['frame'], action='update')
        with pytest.raises(ValueError, match='snapshot'):
            await feed.accept(bad, captured[-1]['received_at'])
    finally:
        await feed.close()


@pytest.mark.asyncio
async def test_public_error_diagnostics_remain_bounded():
    feed = PublicLiquidity(['BTC-USDC'])
    try:
        for _ in range(20):
            feed.record_error('decode', ValueError('malformed public frame ' * 100), feed.pairs)
        assert feed.failures == 20 and feed.failures_by_stage == {'decode': 20}
        assert len(feed.recent_errors) == 8
        assert all(len(row['reason']) <= 240 for row in feed.recent_errors)
    finally:
        await feed.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['crossed', 'empty', 'negative', 'nonfinite', 'unordered'])
async def test_invalid_native_book_isolates_pair_and_fresh_peer_keeps_trading(tmp_path, kind):
    conf = PaperConfig(**{**config(tmp_path).model_dump(), 'pairs': ['BTC-USDC', 'ETH-USDC']})
    runtime = PaperRuntime(conf, rules(conf.pairs))
    try:
        await seed(runtime, 600000)
        for pair in conf.pairs:
            await runtime.liquidity.accept(book(600000, pair), 600000)
        await runtime.step(600000)
        runtime.candles.clock = lambda: 600060
        runtime.candles.apply(dict(v=1, type='candle', epoch='fixture', published_at=600060,
                                   received_at=600060, pair='BTC-USDC', sequence=2, row=row(600000, '101')))
        await runtime.liquidity.accept(book(600060), 600060)
        invalid = book(600060, 'ETH-USDC')
        depth = invalid['data'][0]
        if kind == 'crossed':
            depth['bids'][0][0] = '102'
        elif kind == 'empty':
            depth['asks'] = []
        elif kind == 'negative':
            depth['bids'][0][1] = '-1'
        elif kind == 'nonfinite':
            depth['bids'][0][0] = 'NaN'
        else:
            depth['bids'].append(['101', '1', '0', '1'])
        await runtime.liquidity.accept(invalid, 600060)
        assert 'ETH-USDC' not in runtime.liquidity.books
        result = await runtime.step(600060)
        assert len(result['actions']) == 1
        assert result['actions'][0].executor_config.trading_pair == 'BTC-USDC'
        assert not runtime.session._connector.pair_ready('ETH-USDC')
        assert runtime.liquidity.book_rejections == 1
        assert runtime.liquidity.invalid_book_reasons['ETH-USDC']
        await runtime.liquidity.accept(book(600061, 'ETH-USDC'), 600061)
        await runtime.step(600061)
        assert runtime.session._connector.pair_ready('ETH-USDC')
        assert 'ETH-USDC' not in runtime.liquidity.invalid_book_reasons
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_candle_arriving_during_native_step_is_not_marked_consumed(tmp_path):
    runtime = PaperRuntime(config(tmp_path), rules())
    try:
        await seed(runtime, 600000)
        await runtime.liquidity.accept(book(600000), 600000)
        native_step = runtime.session.step

        async def receive_during_step(*args, **kwargs):
            result = await native_step(*args, **kwargs)
            runtime.candles.clock = lambda: 600060
            runtime.candles.apply(dict(v=1, type='candle', epoch='fixture', published_at=600060,
                                       received_at=600060, pair='BTC-USDC', sequence=2, row=row(600000, '101')))
            await runtime.liquidity.accept(book(600060), 600060)
            return result

        with patch.object(runtime.session, 'step', side_effect=receive_during_step):
            assert (await runtime.step(600000))['actions'] == []
        next_step = await runtime.step(600060)
        assert len(next_step['actions']) == 1
        frame = runtime.session.market_data_provider.get_candles_df('okx', 'BTC-USDC', '1m', 60)
        assert frame.iloc[-1].timestamp == 600000
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_ack_timeout_disconnects_and_revokes_subscription():
    feed = PublicLiquidity(['BTC-USDC'])
    feed.ACK_TIMEOUT_SECONDS = .01
    ws = AsyncMock()

    async def no_response():
        await asyncio.Future()

    ws.receive.side_effect = no_response
    with patch.object(feed.factory, 'get_ws_assistant', return_value=ws):
        task = asyncio.create_task(feed._run(feed.pairs))
        try:
            for _ in range(100):
                if feed.failures:
                    break
                await asyncio.sleep(.005)
            assert feed.failures == 1
            assert not feed.subscribed('BTC-USDC')
            assert feed.failures_by_stage == {'acknowledgement': 1}
            assert feed.recent_errors[-1]['reason'] == 'Public subscription acknowledgement timeout'
            ws.disconnect.assert_awaited_once()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await feed.close()


@pytest.mark.asyncio
async def test_genuine_breakout_native_executor_partial_fill_fees_and_restore_hold(
    tmp_path,
):
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    runtime = None
    try:
        with patch("socket.getaddrinfo", side_effect=AssertionError("no networking")):
            runtime = PaperRuntime(config(tmp_path), rules())
            await seed(runtime, 600000)
            await runtime.liquidity.accept(book(600000), 600000)
            first = await runtime.step(600000)
            assert first["actions"] == []  # Seeded history never enters.
            runtime.candles.clock = lambda: 600060
            runtime.candles.apply(
                dict(
                    v=1,
                    type="candle",
                    epoch="fixture",
                    published_at=600060,
                    received_at=600060,
                    pair="BTC-USDC",
                    sequence=2,
                    row=row(600000, "101"),
                )
            )
            await runtime.liquidity.accept(book(600060), 600060)
            decision = await runtime.step(600060)
            assert len(decision["actions"]) == 1
            assert decision["actions"][0].executor_config.type == "position_executor"
            await runtime.liquidity.accept(
                {
                    "arg": {"channel": "trades", "instId": "BTC-USDC"},
                    "data": [
                        {
                            "instId": "BTC-USDC",
                            "tradeId": "1",
                            "ts": "600061000",
                            "side": "sell",
                            "px": "100.9",
                            "sz": ".1",
                        }
                    ],
                },
                600061,
            )
            filled = await runtime.step(600061)
            orders = runtime.session._connector._order_tracker.all_orders
            order = next(iter(orders.values()))
            assert order.executed_amount_base == Decimal(".01")
            assert order.cumulative_fee_paid("USDC") == Decimal(".00101")
            assert runtime.session._connector.get_balance("USDC") == Decimal(
                "4998.98899"
            )
            assert filled["actions"] == []
            status = json.loads((tmp_path / "status.json").read_text())
            assert status["execution_mode"] == "paper" and status["actions"] == 1
            assert status['report_observed_at'] == 600061
            assert status['unavailable_controller_ids'] == []
            assert isinstance(status['native_reports']['paper_BTC_USDC']['performance'], dict)
            (tmp_path / 'observed.json').write_text(json.dumps(status))
            await runtime.close()
            await runtime.liquidity.close()
            restored = PaperRuntime(config(tmp_path), rules())
            runtime = restored
            assert restored.restored
            assert next(iter(restored.session.controllers.values())).recovery_hold
            assert restored.session._connector.get_balance("BTC") == Decimal(".01")
    finally:
        if runtime is not None:
            await runtime.close()
            await runtime.liquidity.close()
        MarketsRecorder._shared_instance = prior


@pytest.mark.asyncio
async def test_missing_book_and_transport_fatal_never_create_actions(tmp_path):
    runtime = PaperRuntime(config(tmp_path), rules())
    try:
        await seed(runtime, 600000)
        assert (await runtime.step(600000))["actions"] == []
        assert runtime.actions == 0
        status = json.loads((tmp_path / "status.json").read_text())
        assert not status["ready"]
        runtime.liquidity.fatal = "public_trade_queue_overflow"
        with pytest.raises(RuntimeError, match="overflow"):
            await runtime.step(600001)
        with pytest.raises(RuntimeError, match="held"):
            await runtime.step(600002)
    finally:
        await runtime.close()
        await runtime.liquidity.close()


@pytest.mark.asyncio
async def test_same_database_requires_exclusive_process_lock(tmp_path):
    runtime = PaperRuntime(config(tmp_path), rules())
    try:
        with pytest.raises(BlockingIOError):
            PaperRuntime(config(tmp_path), rules())
        assert runtime.session._closed is False
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_restore_rejects_changed_driver_identity(tmp_path):
    runtime = PaperRuntime(config(tmp_path), rules())
    await runtime.close()
    receipt = tmp_path / 'paper.sqlite.runtime.json'
    value = json.loads(receipt.read_text())
    value['source_sha256'] = '0' * 64
    receipt.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='source/config identity'):
        PaperRuntime(config(tmp_path), rules())


@pytest.mark.asyncio
async def test_missing_idle_peer_does_not_block_but_exposed_stale_book_holds(tmp_path):
    conf = PaperConfig(
        **{**config(tmp_path).model_dump(), "pairs": ["BTC-USDC", "ETH-USDC"]}
    )
    runtime = PaperRuntime(conf, rules(conf.pairs))
    try:
        await seed(runtime, 600000)
        await runtime.liquidity.accept(book(600000), 600000)
        await runtime.step(600000)
        runtime.candles.clock = lambda: 600060
        runtime.candles.apply(
            dict(
                v=1,
                type="candle",
                epoch="fixture",
                published_at=600060,
                received_at=600060,
                pair="BTC-USDC",
                sequence=2,
                row=row(600000, "101"),
            )
        )
        await runtime.liquidity.accept(book(600060), 600060)
        result = await runtime.step(600060)
        assert len(result["actions"]) == 1
        assert not runtime.session._connector.ready
        assert runtime.session._connector.pair_ready("BTC-USDC")
        assert not runtime.session._connector.pair_ready("ETH-USDC")
        executor = runtime.session.executor_orchestrator.active_executors[
            "paper_BTC_USDC"
        ][0]
        runtime.liquidity.disconnect_pairs(['BTC-USDC'])  # Native cache is only 1s old.
        with patch.object(
            executor,
            "control_task",
            side_effect=AssertionError("stale exit must not run"),
        ):
            held = await runtime.step(600061)
        assert held["actions"] == [] and held["reports"] == {}
        assert "exposed_pair_book_unavailable:BTC-USDC" in held["readiness"]["reasons"]
        await ack(runtime.liquidity)
        await runtime.liquidity.accept(book(600067), 600067)
        resumed = await runtime.step(600067)
        assert resumed["reports"]
        assert runtime.actions == 1
    finally:
        await runtime.close()
        await runtime.liquidity.close()


@pytest.mark.asyncio
async def test_fresh_peer_protective_market_exit_survives_exposed_peer_disconnect(tmp_path):
    conf = PaperConfig(**{**config(tmp_path).model_dump(), 'pairs': ['BTC-USDC', 'ETH-USDC']})
    runtime = PaperRuntime(conf, rules(conf.pairs))
    try:
        await seed(runtime, 600000)
        runtime.candles.apply(dict(v=1, type='snapshot', epoch='fixture', published_at=600000,
                                   received_at=600000, pair='ETH-USDC', sequence=1,
                                   rows=[row(t) for t in range(596400, 600000, 60)]))
        for pair in conf.pairs:
            await runtime.liquidity.accept(book(600000, pair), 600000)
        await runtime.step(600000)
        runtime.candles.clock = lambda: 600060
        for pair in conf.pairs:
            runtime.candles.apply(dict(v=1, type='candle', epoch='fixture', published_at=600060,
                                       received_at=600060, pair=pair, sequence=2, row=row(600000, '101')))
            await runtime.liquidity.accept(book(600060, pair), 600060)
        created = await runtime.step(600060)
        assert len(created['actions']) == 2
        for pair in conf.pairs:
            await runtime.liquidity.accept({'arg': {'channel': 'trades', 'instId': pair},
                                            'data': [{'instId': pair, 'tradeId': '1', 'ts': '600061000',
                                                      'side': 'sell', 'px': '100.9', 'sz': '10'}]}, 600061)
        await runtime.step(600061)
        btc = runtime.session._connector.get_balance('BTC')
        assert btc > 0 and runtime.session._connector.get_balance('ETH') > 0
        runtime.liquidity.disconnect_pairs(['BTC-USDC'])
        stale_executor = runtime.session.executor_orchestrator.active_executors['paper_BTC_USDC'][0]
        with patch.object(stale_executor, 'control_task', side_effect=AssertionError('stale peer must be preserved')):
            for now in range(600062, 600066):
                observation = book(now, 'ETH-USDC')
                observation['data'][0]['bids'][0][0] = '90'
                observation['data'][0]['asks'][0][0] = '90.1'
                await runtime.liquidity.accept(observation, now)
                result = await runtime.step(now)
        assert runtime.session._connector.get_balance('ETH') == 0
        assert runtime.session._connector.get_balance('BTC') == btc
        assert 'paper_BTC_USDC' not in result['reports']
        assert 'paper_ETH_USDC' in result['reports']
        orders = runtime.session._connector._order_tracker.all_orders.values()
        assert any(o.trading_pair == 'ETH-USDC' and o.trade_type.name == 'SELL'
                   and o.order_type.name == 'MARKET' and o.executed_amount_base > 0 for o in orders)
        status = json.loads(conf.status_path.read_text())
        assert status['unavailable_controller_ids'] == ['paper_BTC_USDC']
        (tmp_path / 'observed.json').write_text(json.dumps(status))
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_stale_exposure_shutdown_is_durable_hold(tmp_path):
    runtime = PaperRuntime(config(tmp_path), rules())
    try:
        await seed(runtime, 600000)
        await runtime.liquidity.accept(book(600000), 600000)
        await runtime.step(600000)
        runtime.candles.clock = lambda: 600060
        runtime.candles.apply(
            dict(
                v=1,
                type="candle",
                epoch="fixture",
                published_at=600060,
                received_at=600060,
                pair="BTC-USDC",
                sequence=2,
                row=row(600000, "101"),
            )
        )
        await runtime.liquidity.accept(book(600060), 600060)
        await runtime.step(600060)
        await runtime.step(600066)
        await runtime.close()
        held = json.loads(
            runtime.config.state_path.with_suffix(".sqlite.paper-step.json").read_text()
        )
        assert held["status"] == "shutdown_without_fresh_exposed_book"
        await runtime.liquidity.close()
        runtime = PaperRuntime(config(tmp_path), rules())
        assert runtime.session._failure is not None
        with pytest.raises(RuntimeError, match="HELD"):
            await runtime.session.step(600067)
    finally:
        await runtime.close()
        await runtime.liquidity.close()


# ---------------------------------------------------------------- rsi_modular paper twin
TWIN_FIELDS = {"id", "execution_mode", "connector_name", "candles_connector", "data", "shared_history"}


def live_rsi_file(tmp_path, pair="BTC-USDC", name=None):
    import yaml

    from controllers.generic.rsi_modular import get_config_class
    suffix = pair.split("-")[0].lower()
    live = get_config_class("ok_rsi")(
        id="modular_spot_v2_" + suffix, profile="ok_rsi", execution_mode="live", connector_name="okx",
        trading_pair=pair, execution_enabled=True, strict_hmm_mode=False, trend_confirmation_enabled=False,
        min_signal_score=1, dynamic_position_sizing=False,
        data={"mode": "shared", "transport": "stream", "origin": "http://127.0.0.1:8092",
              "token_file": "/run/candles/token"}, shared_history=7514,
        funding={"allocation_quote": "50", "fee_reserve_fraction": ".003"})
    path = tmp_path / (name or f"rsi_modular_v2_{suffix}.yml")
    path.write_text(yaml.safe_dump(live.model_dump(mode="json"), sort_keys=False))
    return path, live


def twin_config(tmp_path, files, pairs=("BTC-USDC",), **kwargs):
    return PaperConfig(bot_name="rsi_modular_paper_v2", pairs=list(pairs), state_path=tmp_path / "paper.sqlite",
                       status_path=tmp_path / "status.json", candle_origin="http://127.0.0.1:8092",
                       controller_kind="rsi_modular", controller_files=[str(f) for f in files],
                       simulated_quote="50", **kwargs)


def test_rsi_twin_differs_from_the_live_controller_only_in_declared_fields(tmp_path):
    from scripts.component_paper_runtime import rsi_modular_paper_configs
    path, live = live_rsi_file(tmp_path)
    (paper,), divergence = rsi_modular_paper_configs([path], ("BTC-USDC",))
    live_values, paper_values = live.model_dump(mode="json"), paper.model_dump(mode="json")
    changed = {k for k in set(live_values) | set(paper_values) if live_values.get(k) != paper_values.get(k)}
    assert changed <= TWIN_FIELDS and {"id", "execution_mode", "connector_name"} <= changed
    assert set(divergence[paper.id]) == changed
    assert paper.id == "modular_spot_paper_v2_btc" and paper.execution_mode == "paper"
    assert paper.connector_name == "okx_paper_trade" and paper.data.mode == "native"
    assert paper.funding == live.funding and paper.execution_enabled is True


def test_rsi_twin_refuses_non_live_files_pair_mismatch_and_breakout_file_mixing(tmp_path):
    import yaml

    from scripts.component_paper_runtime import rsi_modular_paper_configs
    path, _ = live_rsi_file(tmp_path)
    with pytest.raises(ValueError, match="pairs"):
        rsi_modular_paper_configs([path], ("ETH-USDC",))
    values = yaml.safe_load(path.read_text())
    bad = tmp_path / "paper.yml"
    bad.write_text(yaml.safe_dump({**values, "execution_mode": "paper", "connector_name": "okx_paper_trade"}))
    with pytest.raises(ValueError, match="live rsi_modular"):
        rsi_modular_paper_configs([bad], ("BTC-USDC",))
    with pytest.raises(ValueError, match="controller_files"):
        config(tmp_path, controller_files=[str(path)])
    with pytest.raises(ValueError, match="controller_files"):
        PaperConfig(bot_name="x", pairs=["BTC-USDC"], state_path=tmp_path / "p.sqlite", status_path=tmp_path / "s.json",
                    candle_origin="http://127.0.0.1:8092", controller_kind="rsi_modular", controller_files=[])


@pytest.mark.asyncio
async def test_rsi_twin_runtime_injects_the_controller_history_and_reports_the_twin(tmp_path):
    path, _ = live_rsi_file(tmp_path)
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    runtime = None
    try:
        with patch("socket.getaddrinfo", side_effect=AssertionError("no networking")):
            runtime = PaperRuntime(twin_config(tmp_path, [path]), rules())
            (controller,) = runtime.session.controllers.values()
            need = max(c.max_records for c in controller.config.candles_config)
            assert runtime.datasets[0].history == need and need > 60
            now = 600000
            await ack(runtime.liquidity)
            runtime.candles.clock = lambda: now
            runtime.candles.begin_session()
            runtime.candles.apply(dict(v=1, type="snapshot", epoch="fixture", published_at=now, received_at=now,
                                       pair="BTC-USDC", sequence=1,
                                       rows=[row(t) for t in range(now - 60 * need, now, 60)]))
            await runtime.liquidity.accept(book(now), now)
            await runtime.step(now)
            warming = json.loads((tmp_path / "status.json").read_text())
            assert "BTC-USDC:candles_warming" in warming["reasons"]  # boot snapshot alone never admits the twin
            later = now + 60
            runtime.candles.clock = lambda: later
            runtime.candles.apply(dict(v=1, type="candle", epoch="fixture", published_at=later, received_at=later,
                                       pair="BTC-USDC", sequence=2, row=row(now, "100")))
            await runtime.liquidity.accept(book(later), later)
            await runtime.step(later)
            frame = runtime.session.market_data_provider.get_candles_df("okx", "BTC-USDC", "1m", need)
            assert len(frame) == need and int(frame.iloc[-1].timestamp) == now
            status = json.loads((tmp_path / "status.json").read_text())
            assert status["controller_kind"] == "rsi_modular"
            assert set(status["twin_divergence"]["modular_spot_paper_v2_btc"]) <= TWIN_FIELDS
            assert [c["id"] for c in status["controllers"]] == ["modular_spot_paper_v2_btc"]
            (status_row,) = status["controllers"]  # the reporting paper reader binds these identities
            assert (status_row["controller_name"], status_row["pair"], status_row["execution_mode"]) == (
                "rsi_modular", "BTC-USDC", "paper")
            assert not any("candles_" in r for r in status["reasons"]), status["reasons"]
    finally:
        if runtime is not None:
            await runtime.close()
        MarketsRecorder._shared_instance = prior


# ---------------------------------------------------------------- modular_spot_research paper
def research_file(tmp_path, pair="BTC-USDC", **overrides):
    import yaml

    from controllers.generic.modular_spot_core.program_budget import BudgetPolicy
    from controllers.generic.modular_spot_research import ModularSpotResearchConfig
    config = ModularSpotResearchConfig(
        id="modular_spot_research_paper_" + pair.split("-")[0].lower(), trading_pair=pair, execution_enabled=True,
        total_amount_quote=Decimal("5000"), account="research-paper",
        budget=BudgetPolicy(group="research-paper", max_quote=Decimal("5000"), max_assets=1), **overrides)
    path = tmp_path / f"modular_spot_research_{pair.split('-')[0].lower()}.yml"
    path.write_text(yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False))
    return path, config


def test_modular_spot_research_paper_files_load_strictly(tmp_path):
    import yaml

    from scripts.component_paper_runtime import modular_spot_research_paper_configs
    path, reference = research_file(tmp_path)
    (loaded,) = modular_spot_research_paper_configs([path], ("BTC-USDC",))
    assert loaded.model_dump(mode="json") == reference.model_dump(mode="json")
    assert loaded.connector_name == "okx_paper_trade" and loaded.data.mode == "native"
    with pytest.raises(ValueError, match="pairs"):
        modular_spot_research_paper_configs([path], ("ETH-USDC",))
    other = tmp_path / "other.yml"
    other.write_text(yaml.safe_dump({**yaml.safe_load(path.read_text()), "controller_name": "rsi_modular"}))
    with pytest.raises(ValueError, match="modular_spot_research controller file"):
        modular_spot_research_paper_configs([other], ("BTC-USDC",))
    with pytest.raises(ValueError, match="controller_files"):
        PaperConfig(bot_name="x", pairs=["BTC-USDC"], state_path=tmp_path / "p.sqlite", status_path=tmp_path / "s.json",
                    candle_origin="http://127.0.0.1:8092", controller_kind="modular_spot_research", controller_files=[])


@pytest.mark.asyncio
async def test_modular_spot_research_runtime_injects_max_records_and_reports_the_controller(tmp_path):
    path, reference = research_file(tmp_path)
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    runtime = None
    try:
        with patch("socket.getaddrinfo", side_effect=AssertionError("no networking")):
            runtime = PaperRuntime(PaperConfig(
                bot_name="spot_research_paper_v2", pairs=["BTC-USDC"], state_path=tmp_path / "paper.sqlite",
                status_path=tmp_path / "status.json", candle_origin="http://127.0.0.1:8092",
                controller_kind="modular_spot_research", controller_files=[str(path)], simulated_quote="5000"), rules())
            (controller,) = runtime.session.controllers.values()
            assert runtime.datasets[0].history == reference.max_records
            now = 600000
            await ack(runtime.liquidity)
            runtime.candles.clock = lambda: now
            runtime.candles.begin_session()
            runtime.candles.apply(dict(v=1, type="snapshot", epoch="fixture", published_at=now, received_at=now,
                                       pair="BTC-USDC", sequence=1,
                                       rows=[row(t) for t in range(now - 60 * reference.max_records, now, 60)]))
            await runtime.liquidity.accept(book(now), now)
            await runtime.step(now)
            status = json.loads((tmp_path / "status.json").read_text())
            assert status["controller_kind"] == "modular_spot_research"
            assert [c["id"] for c in status["controllers"]] == [reference.id]
    finally:
        if runtime is not None:
            await runtime.close()
        MarketsRecorder._shared_instance = prior


MERIDIAN_PAIRS = ("BNB-USDC", "BTC-USDC", "ETH-USDC", "SOL-USDC", "XRP-USDC")


def meridian_file(tmp_path, **changes):
    from test.meridian_v3.test_unified_runtime import unified

    import yaml
    config = unified(**changes)
    path = tmp_path / "meridian_v3_unified_paper.yml"
    path.write_text(yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False))
    return path, config


def test_meridian_unified_paper_file_loads_strictly(tmp_path):
    from scripts.component_paper_runtime import meridian_unified_paper_configs
    path, reference = meridian_file(tmp_path, execution_enabled=True)
    (loaded,) = meridian_unified_paper_configs([path], MERIDIAN_PAIRS)
    assert loaded.model_dump(mode="json") == reference.model_dump(mode="json")
    with pytest.raises(ValueError, match="pairs"):
        meridian_unified_paper_configs([path], MERIDIAN_PAIRS[:4])
    with pytest.raises(ValueError, match="one portfolio controller file"):
        PaperConfig(bot_name="x", pairs=list(MERIDIAN_PAIRS), state_path=tmp_path / "p.sqlite",
                    status_path=tmp_path / "s.json", candle_origin="http://127.0.0.1:8092",
                    controller_kind="meridian_v3_unified", controller_files=[str(path), str(tmp_path / "b.yml")])


@pytest.mark.asyncio
async def test_meridian_unified_runtime_hosts_one_owner_for_five_pairs(tmp_path):
    path, reference = meridian_file(tmp_path, execution_enabled=True)
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    runtime = None
    try:
        with patch("socket.getaddrinfo", side_effect=AssertionError("no networking")):
            runtime = PaperRuntime(PaperConfig(
                bot_name="meridian_paper_v3", pairs=list(MERIDIAN_PAIRS), state_path=tmp_path / "paper.sqlite",
                status_path=tmp_path / "status.json", candle_origin="http://127.0.0.1:8092",
                controller_kind="meridian_v3_unified", controller_files=[str(path)], simulated_quote="500"),
                rules(MERIDIAN_PAIRS))
            (controller,) = runtime.session.controllers.values()
            assert controller.config.id == reference.id
            assert {d.trading_pair: d.history for d in runtime.datasets} == {p: 1500 for p in MERIDIAN_PAIRS}
            assert set(runtime.controller_ids.values()) == {reference.id}
            now = 600000
            await ack(runtime.liquidity)
            runtime.candles.clock = lambda: now
            runtime.candles.begin_session()
            for pair in MERIDIAN_PAIRS:
                runtime.candles.apply(dict(v=1, type="snapshot", epoch="fixture", published_at=now, received_at=now,
                                           pair=pair, sequence=1,
                                           rows=[row(t) for t in range(now - 60 * 1500, now, 60)]))
                await runtime.liquidity.accept(book(now, pair), now)
            result = await runtime.step(now)
            assert result["actions"] == []  # the first observed cut only seeds each symbol
            status = json.loads((tmp_path / "status.json").read_text())
            assert status["controller_kind"] == "meridian_v3_unified"
            (row_,) = status["controllers"]
            assert row_["id"] == reference.id and sorted(row_["pair"]) == sorted(MERIDIAN_PAIRS)
            assert status["unavailable_controller_ids"] == []
    finally:
        if runtime is not None:
            await runtime.close()
        MarketsRecorder._shared_instance = prior


# ---------------------------------------------------------------- V4 / V4 D2h paper (forward A/B arms)
def v4_files(tmp_path, program="meridian_v4", pair_quote="40000", pairs=MERIDIAN_PAIRS):
    import yaml

    from controllers.generic.meridian_v4 import MeridianV4Config
    from controllers.generic.meridian_v4_d2h import MeridianV4D2hConfig

    cls = {"meridian_v4": MeridianV4Config, "meridian_v4_d2h": MeridianV4D2hConfig}[program]
    paths, configs = [], []
    for pair in pairs:
        values = dict(id=f"modular_spot_{program}_paper_{pair.split('-')[0].lower()}", trading_pair=pair,
                      execution_mode="paper", connector_name="okx_paper_trade", account=f"{program}_paper",
                      risk=dict(max_drawdown_fraction=".10", max_daily_loss_fraction=".05", max_spread_fraction=".003",
                                fee_reserve_fraction=".003", cash_reserve_quote="0"),
                      budget=dict(group=program, max_quote=str(5 * int(pair_quote)), max_assets=5),
                      total_amount_quote=pair_quote)
        unsealed = cls(**values)
        config = cls(**values, execution_enabled=True, recipe_binding=unsealed.seal())
        path = tmp_path / f"{program}_{pair.split('-')[0].lower()}.yml"
        path.write_text(yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False))
        paths.append(path)
        configs.append(config)
    return paths, configs


@pytest.mark.parametrize("program", ["meridian_v4", "meridian_v4_d2h"])
def test_v4_paper_files_load_strictly_one_program_per_runtime(tmp_path, program):
    import yaml

    from scripts.component_paper_runtime import meridian_v4_paper_configs
    paths, references = v4_files(tmp_path, program)
    loaded = meridian_v4_paper_configs(paths, MERIDIAN_PAIRS)
    assert [c.model_dump(mode="json") for c in loaded] == [c.model_dump(mode="json") for c in references]
    with pytest.raises(ValueError, match="pairs"):
        meridian_v4_paper_configs(paths[:4], MERIDIAN_PAIRS)
    other = "meridian_v4_d2h" if program == "meridian_v4" else "meridian_v4"
    (tmp_path / "other").mkdir()
    other_paths, _ = v4_files(tmp_path / "other", other)
    with pytest.raises(ValueError, match="One V4 program"):
        meridian_v4_paper_configs([*paths[:4], other_paths[4]], MERIDIAN_PAIRS)
    unsealed = yaml.safe_load(paths[0].read_text())
    unsealed.update(execution_enabled=False, recipe_binding=None)
    paths[0].write_text(yaml.safe_dump(unsealed, sort_keys=False))
    with pytest.raises(ValueError, match="sealed, enabled"):
        meridian_v4_paper_configs(paths, MERIDIAN_PAIRS)


def test_v4_paper_budgets_must_fit_the_simulated_wallet(tmp_path):
    paths, _ = v4_files(tmp_path)
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    try:
        with pytest.raises(ValueError, match="simulated paper wallet"):
            PaperRuntime(PaperConfig(
                bot_name="meridian_v4_paper", pairs=list(MERIDIAN_PAIRS), state_path=tmp_path / "paper.sqlite",
                status_path=tmp_path / "status.json", candle_origin="http://127.0.0.1:8092",
                controller_kind="meridian_v4", controller_files=[str(p) for p in paths], simulated_quote="199999"),
                rules(MERIDIAN_PAIRS))
    finally:
        MarketsRecorder._shared_instance = prior


@pytest.mark.asyncio
@pytest.mark.parametrize("program", ["meridian_v4", "meridian_v4_d2h"])
async def test_v4_runtime_hosts_five_sealed_owners_and_reports_them(tmp_path, program):
    paths, references = v4_files(tmp_path, program)
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    runtime = None
    try:
        with patch("socket.getaddrinfo", side_effect=AssertionError("no networking")):
            runtime = PaperRuntime(PaperConfig(
                bot_name=f"{program}_paper", pairs=list(MERIDIAN_PAIRS), state_path=tmp_path / "paper.sqlite",
                status_path=tmp_path / "status.json", candle_origin="http://127.0.0.1:8092",
                controller_kind="meridian_v4", controller_files=[str(p) for p in paths], simulated_quote="200000"),
                rules(MERIDIAN_PAIRS))
            assert {d.trading_pair: d.history for d in runtime.datasets} == {p: 1500 for p in MERIDIAN_PAIRS}
            assert runtime.controller_ids == {c.trading_pair: c.id for c in references}
            now = 600000
            await ack(runtime.liquidity)
            runtime.candles.clock = lambda: now
            runtime.candles.begin_session()
            for pair in MERIDIAN_PAIRS:
                runtime.candles.apply(dict(v=1, type="snapshot", epoch="fixture", published_at=now, received_at=now,
                                           pair=pair, sequence=1,
                                           rows=[row(t) for t in range(now - 60 * 1500, now, 60)]))
                await runtime.liquidity.accept(book(now, pair), now)
            result = await runtime.step(now)
            assert result["actions"] == []  # the first observed cut only seeds each owner
            status = json.loads((tmp_path / "status.json").read_text())
            assert status["controller_kind"] == "meridian_v4"
            assert sorted(r["id"] for r in status["controllers"]) == sorted(c.id for c in references)
            assert status["failure"] is None
            assert status["unavailable_controller_ids"] == []  # every owner reports natively
            assert status["public_liquidity"]["fresh_book_pairs"] == 5
            assert status["simulation_assumptions"]["quote"] == "200000"
            assert {r["custom_info"]["strategy_name"] for r in status["controllers"]} == {
                "V4" if program == "meridian_v4" else "V4 D2h"}
            assert status["decision_journal"] == {"failures": 0, "last_error": None}
            journal = tmp_path / f"{program}_paper.decisions.jsonl"
            assert not journal.exists() or journal.read_text() == ""  # no closed candle yet
            # One line per controller per closed candle with step counts per status; same_candle is not counted.
            c1, c2 = 10 ** 10, 10 ** 10 + 60  # far after the real checkpoint, so not decided
            script = iter([(c1, "acquisition_not_eligible"), (c1, "same_candle"),
                           (c1, "data_hold:native market/account provider not ready"), (c2, "acquisition_not_eligible")])
            for candle, reason in script:
                for controller in runtime.session.controllers.values():
                    controller.get_custom_info = (lambda c=candle, r=reason: {
                        "modular_status": r.split(":", 1)[0], "candle_open": c, "bag": {"held_amount": "0"}})
                    controller.runtime.reason = reason
                runtime.journal(now, {"actions": []})
            lines = [json.loads(line) for line in journal.read_text().splitlines()]
            assert all(e["kind"] == "candle" for e in lines)  # the real first candle closes too
            lines = [e for e in lines if e["candle_open"] == c1]
            assert sorted(e["controller"] for e in lines) == sorted(c.id for c in references)
            assert all(e["kind"] == "candle" and e["candle_open"] == c1 and e["final"] == "data_hold"
                       and e["statuses"] == {"acquisition_not_eligible": 1, "same_candle": 1, "data_hold": 1}
                       and e["decided"] is False  # the scripted candle never reached the real checkpoint
                       and e["detail"] == "data_hold:native market/account provider not ready" and "bag" in e
                       for e in lines)
    finally:
        if runtime is not None:
            await runtime.close()
        MarketsRecorder._shared_instance = prior


@pytest.mark.asyncio
async def test_v4_journal_failure_is_reported_not_a_session_failure(tmp_path):
    paths, _ = v4_files(tmp_path)
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    runtime = None
    try:
        with patch("socket.getaddrinfo", side_effect=AssertionError("no networking")):
            runtime = PaperRuntime(PaperConfig(
                bot_name="meridian_v4_paper", pairs=list(MERIDIAN_PAIRS), state_path=tmp_path / "paper.sqlite",
                status_path=tmp_path / "status.json", candle_origin="http://127.0.0.1:8092",
                controller_kind="meridian_v4", controller_files=[str(p) for p in paths], simulated_quote="200000"),
                rules(MERIDIAN_PAIRS))
            runtime.journal_path = tmp_path  # a directory: every append fails
            runtime.journal = lambda now, result: open(runtime.journal_path, "a")
            now = 600000
            await ack(runtime.liquidity)
            runtime.candles.clock = lambda: now
            runtime.candles.begin_session()
            for pair in MERIDIAN_PAIRS:
                runtime.candles.apply(dict(v=1, type="snapshot", epoch="fixture", published_at=now, received_at=now,
                                           pair=pair, sequence=1, rows=[row(t) for t in range(now - 60 * 1500, now, 60)]))
                await runtime.liquidity.accept(book(now, pair), now)
            await runtime.step(now)
            await runtime.step(now + 1)  # a failed journal never holds the session
            assert runtime.failure is None and runtime.journal_failures == 2
            assert runtime.journal_error.startswith("IsADirectoryError")
            status = json.loads((tmp_path / "status.json").read_text())
            assert status["decision_journal"]["failures"] == 1 and status["failure"] is None  # reported next step
    finally:
        if runtime is not None:
            await runtime.close()
        MarketsRecorder._shared_instance = prior


def test_decided_reads_the_owner_checkpoint():
    from types import SimpleNamespace

    def owner(candle):
        state = None if candle is None else SimpleNamespace(candle=candle)
        return SimpleNamespace(runtime=SimpleNamespace(store=SimpleNamespace(load=lambda: (state, "raw"))))
    assert PaperRuntime._decided(owner(120), 120) is True
    assert PaperRuntime._decided(owner(60), 120) is False
    assert PaperRuntime._decided(owner(None), 120) is False
    assert PaperRuntime._decided(SimpleNamespace(), 120) is None  # no modular decision store
