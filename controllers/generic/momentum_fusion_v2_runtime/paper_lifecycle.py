"""Explicit offline momentum qualification using the native recorder and executors.

This adapter is constructible only by a genuine NativePaperSession. It is not a
production execution mode, a wallet owner, an economic runner, or a second ledger.
"""
import json
from decimal import Decimal
from types import SimpleNamespace

from pydantic import Field
from sqlalchemy import text

from controllers.generic.momentum_fusion_v2_core.recipe import scope_for
from controllers.generic.momentum_fusion_v2_core.setups import PolicyState, step
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.model.executors import Executors
from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.components.capital_policy import CapitalLimits, CapitalRequest, assess_acquisition
from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import Prerequisite
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.recipe_contract import digest, source_digest
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TrailingStop,
    TripleBarrierConfig,
)
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction

from .native_paper_capital import PROGRAM, NativePaperAccountEvidence


class MomentumPaperPolicy(PolicyModel):
    """Bounded fixture-only native mapping; every field is bound into the recipe."""
    trade_quote: Decimal = Field(gt=0, le=1000)
    account_quote: Decimal = Field(gt=0, le=10000)
    max_loss_quote: Decimal = Field(gt=0, le=1000)
    stop_loss: Decimal = Field(gt=0, lt=1)
    trailing_activation: Decimal = Field(gt=0, lt=1)
    trailing_delta: Decimal = Field(gt=0, lt=1)
    entry_ttl_seconds: int = Field(ge=1, le=60, strict=True)

    def recipe_bindings(self, initialization_sha256):
        return dict(initialization_sha256=initialization_sha256,
                    sizing_policy_sha256=digest({'trade_quote': self.trade_quote}),
                    capital_policy_sha256=digest({'account_quote': self.account_quote}),
                    loss_policy_sha256=digest({'max_loss_quote': self.max_loss_quote}),
                    exit_policy_sha256=digest(self.model_dump(exclude={'trade_quote', 'account_quote', 'max_loss_quote'})))


class MomentumPaperLifecycle:
    def __init__(self, session, policy):
        from controllers.generic.momentum_fusion_v2 import MomentumFusionV2Config, source_files, source_root
        from hummingbot.strategy_v2.paper_session import NativePaperSession
        if type(session) is not NativePaperSession or type(policy) is not MomentumPaperPolicy:
            raise PolicyError('exact offline native paper fixture required')
        if len(session.controllers) != 1:
            raise PolicyError('momentum qualification requires one isolated controller')
        host = next(iter(session.controllers.values()))
        if type(host.config) is not MomentumFusionV2Config or host.config.execution_mode != 'paper':
            raise PolicyError('exact nonexecuting momentum paper configuration required')
        policy = MomentumPaperPolicy.model_validate(policy.model_dump(mode='json'))
        expected = policy.recipe_bindings(session._identity['initial_balances_sha256'])
        if any(getattr(host.config.recipe, key) != value for key, value in expected.items()):
            raise PolicyError('fixture policy or initialization differs from the recipe binding')
        self.session, self.host, self.policy = session, host, policy
        self.evidence = NativePaperAccountEvidence(session)
        self.source = source_digest(source_root(), source_files())
        self.binding = {'source_sha256': self.source, 'recipe_sha256': host.config.recipe.digest,
                        'policy_sha256': digest(policy), 'controller_id': host.config.id,
                        'account_id': self.evidence.account_id, 'coordinator': 'native_flat_reseed_v1'}
        self.key = 'momentum-paper-v1:' + host.config.id
        self.pending = None
        self._emitted = False
        self._raw = None
        self._checkpoint = None
        self._load()
        if any(quantity != 0 for asset, quantity in session._initial_balances.items() if asset != 'USDC'):
            raise PolicyError('fixture cannot adopt preexisting inventory')
        self.initial_quote = session._initial_balances.get('USDC', Decimal(0))
        if not self.initial_quote.is_finite() or self.initial_quote <= 0:
            raise PolicyError('positive native initial cash required')
        host._paper_fixture = self

    def _load(self):
        with self.session._sql.get_new_session() as db:
            row = db.get(Metadata, self.key)
            self._raw = row.value if row else None
        self._checkpoint = None if self._raw is None else json.loads(self._raw)
        if self._checkpoint is not None:
            if (self._checkpoint['binding'] != self.binding
                    or type(self._checkpoint['episode']) is not int or self._checkpoint['episode'] < 0
                    or Decimal(self._checkpoint['initial_quote']) != self.session._initial_balances.get('USDC', Decimal(0))):
                raise PolicyError('native momentum checkpoint identity or initial cash changed')

    def acquisition_paused(self):
        with self.session._sql.get_new_session() as db:
            row = db.get(Metadata, self.key + ':entry-pause')
            value = False if row is None else json.loads(row.value)
        if type(value) is not bool:
            raise PolicyError('invalid native acquisition pause state')
        return value

    def pause_acquisition(self, paused):
        if type(paused) is not bool or self.session._closed:
            raise PolicyError('explicit boolean pause on an open native fixture required')
        with self.session._sql.get_new_session() as db, db.begin():
            key = self.key + ':entry-pause'
            row = db.get(Metadata, key)
            if row is None:
                db.add(Metadata(key=key, value=json.dumps(paused)))
            else:
                row.value = json.dumps(paused)

    def _terminal_history_proven(self, infos, current_id=None):
        from hummingbot.strategy_v2.models.executors import CloseType

        # Bound replay/admission work. Reaching this lifetime fixture capacity
        # blocks new acquisitions, never the current executor's protective exit.
        if len(infos) > 10000:
            return False
        current, order_ids = [], set()
        for info in infos:
            if (info.controller_id != self.host.config.id or info.config.connector_name != self.session._connector.name
                    or info.config.trading_pair != self.host.config.trading_pair):
                return False
            with self.session._sql.get_new_session() as db:
                receipt = db.get(Metadata, self.key + ':consumed:' + info.id.removeprefix('momentum-paper-'))
            if receipt is None:
                return False
            admission = json.loads(receipt.value)
            if (admission['executor_id'] != info.id
                    or admission['config_sha256'] != digest(info.config.model_dump(mode='json'))):
                return False
            order_ids.update(row['client_order_id'] for row in (info.custom_info or {}).get('execution_order_snapshots', []))
            if info.id == current_id:
                current.append(info)
                continue
            if not info.is_done or info.close_type == CloseType.POSITION_HOLD:
                return False
            claim = SimpleNamespace(controller_id=info.controller_id, executor_identity=info.id,
                                    seen_executor_id=info.id, trading_pair=info.config.trading_pair)
            if not self.evidence.terminal_claim_receipt(claim):
                return False
        orders = self.session.recorder.get_orders_for_config_and_market('native-paper-session', self.session._connector)
        return len(current) == (0 if current_id is None else 1) and order_ids == {order.id for order in orders}

    def observe(self):
        from controllers.generic.momentum_fusion_v2 import source_files, source_root
        s, market, host = self.session, self.session._connector, self.host
        self.pending, self._emitted = None, False
        if (s._closed or not s._step_lock.locked() or s.controllers.get(host.config.id) is not host
                or source_digest(source_root(), source_files()) != self.source):
            raise PolicyError('native fixture binding or source changed')
        self._load()
        now = s.current_timestamp
        observed = host._pipeline.evaluate({d.key: s.market_data_provider.get_candle_snapshot(d)
                                            for d in host.config.recipe.datasets}, now_ms=int(now * 1000))
        snapshot = self.evidence.snapshot()
        equity = market.get_balance('USDC') + sum((e.marked_inventory_quote or Decimal(0)
                                                   for e in snapshot.exposures), Decimal(0))
        native = s.recorder.get_all_executors()
        flat = not market.in_flight_orders and all(quantity == 0 for asset, quantity in market.get_all_balances().items()
                                                   if asset != 'USDC')
        owned = (flat and self._terminal_history_proven(native)
                 and not s.executor_orchestrator.recovery_required.get(host.config.id))
        paused = self.acquisition_paused()

        risk_ok = self.initial_quote - equity < self.policy.max_loss_quote
        assessment = assess_acquisition(
            CapitalRequest(account_id=snapshot.account_id, program_id=PROGRAM, trading_pair=host.config.trading_pair,
                           valuation_currency='USDC', requested_quote=self.policy.trade_quote), snapshot,
            CapitalLimits(account_quote=self.policy.account_quote, program_quote=self.policy.account_quote,
                          base_quote=self.policy.account_quote, order_quote=self.policy.trade_quote, max_age_ms=100),
            now_ms=int(now * 1000))
        checks = (
            Prerequisite(name='owner_ready', status='PASS' if owned and not paused else 'HOLD',
                         evidence_sha256=digest({'native_executor_ids': [e.id for e in native], 'binding': self.binding, 'paused': paused})),
            Prerequisite(name='risk', status='PASS' if risk_ok else 'HOLD',
                         evidence_sha256=digest({'equity': equity, 'initial': self.initial_quote, 'policy': self.policy})),
            Prerequisite(name='capital', status='PASS' if assessment.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION' else 'HOLD',
                         evidence_sha256=assessment.input_sha256))
        previous = None if self._checkpoint is None else PolicyState.model_validate(self._checkpoint['state'])
        episode = 0 if self._checkpoint is None else self._checkpoint['episode']
        if previous is not None and previous.issued and owned and observed.primary.event_time_ms > previous.cursor.last_event_ms:
            # The pure policy deliberately consumes one episode. Only proven
            # native flatness can seed another, on a strictly later event. The
            # immutable consumption receipt below still forbids event replay.
            previous = None
            episode += 1
        scope = scope_for(host.config.recipe, program_id='momentum_v2_offline_fixture', owner_id=host.config.id, mode='shadow')
        state, decision = step(host.config.recipe, previous, observed, scope=scope, now_ms=int(now * 1000), prerequisites=checks)
        payload = dict(binding=self.binding, initial_quote=str(self.initial_quote), episode=episode, state=state.model_dump(mode='json'),
                       decision=decision.model_dump(mode='json'), prerequisites=[p.model_dump(mode='json') for p in checks],
                       admission=None if self._checkpoint is None else self._checkpoint['admission'])
        config = None
        if decision.trace.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION':
            config = self._configuration(decision.trace.consumption_key)
            payload['admission'] = dict(executor_id=config.id, config_sha256=digest(config.model_dump(mode='json')),
                                        opportunity_id=decision.trace.opportunity_id,
                                        consumption_key=decision.trace.consumption_key,
                                        prerequisite_sha256=decision.trace.prerequisite_sha256)
        self._persist(payload, config)
        self.pending = config
        host._state = state
        host.processed_data.update(data_health={'ready': True}, decision=decision.model_dump(mode='json'),
                                   offline_fixture=True, action_reason=decision.reason)

    def _configuration(self, consumption_key):
        market, cfg = self.session._connector, self.host.config
        price = market.get_price_by_type(cfg.trading_pair, PriceType.BestBid)
        # Reserve the worst supported native fee using the connector's own
        # budget checker, never a caller's free-quote value or an assumed fee.
        fee = max(market._fee_policy.maker_percent_fee_decimal, market._fee_policy.taker_percent_fee_decimal)
        amount = market.quantize_order_amount(cfg.trading_pair, self.policy.trade_quote / (price * (1 + fee)))
        config = PositionExecutorConfig(id='momentum-paper-' + consumption_key, timestamp=self.session.current_timestamp,
                                        controller_id=cfg.id, connector_name=market.name, trading_pair=cfg.trading_pair, side=TradeType.BUY,
                                        amount=amount, entry_price=price, entry_order_policy='single_attempt',
                                        triple_barrier_config=TripleBarrierConfig(stop_loss=self.policy.stop_loss,
                                                                                  trailing_stop=TrailingStop(activation_price=self.policy.trailing_activation, trailing_delta=self.policy.trailing_delta),
                                                                                  open_order_type=OrderType.LIMIT, open_order_limit_max_age_seconds=self.policy.entry_ttl_seconds,
                                                                                  stop_loss_order_type=OrderType.MARKET, trailing_stop_order_type=OrderType.MARKET))
        candidate = market._paper_budget_checker.populate_collateral_entries(OrderCandidate(
            cfg.trading_pair, fee == market._fee_policy.maker_percent_fee_decimal, OrderType.LIMIT, TradeType.BUY, amount, price))
        collateral = candidate.collateral_dict
        rule = market.trading_rules[cfg.trading_pair]
        if (amount <= 0 or amount < rule.min_order_size or amount * price < rule.min_notional_size
                or any(asset != 'USDC' for asset in collateral)
                or collateral.get('USDC', Decimal(0)) > min(self.policy.trade_quote, market.get_available_balance('USDC'))):
            raise PolicyError('native paper sizing or exact fee collateral unavailable')
        return config

    def _persist(self, payload, config):
        raw = json.dumps(payload, sort_keys=True)
        # The existing native Metadata and Executors rows commit together before
        # start. A repeated event has one durable consumption identity, including
        # crashes between intent persistence and the first order event.
        executor = None if config is None else PositionExecutor(self.session, config)
        with self.session._sql.get_new_session() as db:
            db.execute(text('BEGIN IMMEDIATE'))
            row = db.get(Metadata, self.key)
            if (None if row is None else row.value) != self._raw:
                raise PolicyError('native momentum checkpoint changed concurrently')
            if config is not None:
                consumed_key = self.key + ':consumed:' + payload['admission']['consumption_key']
                if db.get(Executors, config.id) is not None or db.get(Metadata, consumed_key) is not None:
                    raise PolicyError('native opportunity already has an execution owner')
                db.add(Executors(**json.loads(executor.executor_info.model_dump_json())))
                db.add(Metadata(key=consumed_key, value=json.dumps(payload['admission'], sort_keys=True)))
            if row is None:
                db.add(Metadata(key=self.key, value=raw))
            else:
                row.value = raw
            db.commit()
        self._raw, self._checkpoint = raw, payload

    def actions(self):
        if self.pending is None or self._emitted:
            return []
        self._emitted = True
        return [CreateExecutorAction(controller_id=self.host.config.id, executor_config=self.pending)]

    def validate(self, config):
        if self.pending is None or not self.session._step_lock.locked() or self.session._failure is not None:
            return False
        return type(config) is PositionExecutorConfig and config == self.pending

    def committed(self, config):
        if not self.validate(config):
            raise PolicyError('native fixture admission changed before execution')
        self.pending = None

    def restore_executor(self, info):
        """Restore only proven singleton entry ownership; every other state holds."""
        try:
            return self._restore_executor(info)
        except (ValueError, TypeError, KeyError, AttributeError) as error:
            self.host._recovery_hold = 'momentum_fixture_restore_requires_reconciliation:' + str(error)
            return None

    def _restore_executor(self, info):
        from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import _PaperInFlightOrder
        from hummingbot.connector.exchange.paper_trade.terminal_recorder import recorded_terminal_order
        from hummingbot.core.data_type.in_flight_order import OrderState
        from hummingbot.strategy_v2.models.base import RunnableStatus
        from hummingbot.strategy_v2.models.executors import TrackedOrder
        market = self.session._connector
        admission = None if self._checkpoint is None else self._checkpoint['admission']
        if (self.session._failure is not None or not admission or info.id != admission['executor_id']
                or info.controller_id != self.host.config.id or info.config.controller_id != self.host.config.id
                or digest(info.config.model_dump(mode='json')) != admission['config_sha256']
                or market.in_flight_orders or not self._terminal_history_proven(
                    self.session.recorder.get_all_executors(), current_id=info.id)
                or self.session.recorder.get_all_position_hold_ledger_entries()):
            raise PolicyError('native intent binding or outstanding order ownership unavailable')
        rows = (info.custom_info or {}).get('execution_order_snapshots')
        executor = PositionExecutor(self.session, info.config)
        if info.status == RunnableStatus.NOT_STARTED and rows == []:
            if (self.session.recorder.get_trades_for_config('native-paper-session')
                    or self.session.recorder.get_orders_for_config_and_market('native-paper-session', market)):
                raise PolicyError('unstarted intent has native order history')
            return executor
        custom = info.custom_info
        if (info.status != RunnableStatus.RUNNING or info.close_type is not None or custom.get('exit_trigger_reason')
                or not isinstance(rows, list) or len(rows) != 1 or custom.get('held_position_orders')
                or Decimal(str(custom['close_order_filled_amount_base'])) != 0):
            raise PolicyError('only unclosed native singleton entry ownership is qualified')
        order = _PaperInFlightOrder.from_json(rows[0])
        recorded = recorded_terminal_order(self.session.recorder, market, order)
        if (recorded is None or order.current_state not in (OrderState.FILLED, OrderState.CANCELED)
                or recorded.current_state != order.current_state or order.trade_type != TradeType.BUY
                or order.trading_pair != info.config.trading_pair or order.amount != info.config.amount
                or order.executed_amount_base <= 0):
            raise PolicyError('positive terminal native entry receipt required')
        executor._open_order = TrackedOrder(order_id=order.client_order_id)
        executor._open_order.order = order
        executor._resume_entry_sealed = True
        executor._entry_submitted = True
        executor._entry_lifecycle = {'state': 'managing_fill', 'stop_reason': 'native_paper_checkpoint'}
        executor._open_order_entry_expired = custom['open_order_entry_expired']
        executor._open_order_expiry_reconciled = True
        executor._entry_reconciliation = custom.get('entry_reconciliation') or {}
        state, trigger = custom['trailing_state'], custom['trailing_stop_trigger_pct']
        moves = custom['trailing_move_count']
        armed, updated = custom['trailing_armed_timestamp'], custom['trailing_last_update_timestamp']
        if type(moves) is not int or moves < 0:
            raise PolicyError('invalid native trailing move count')
        if state == 'armed':
            trigger = Decimal(str(trigger))
            if (not trigger.is_finite() or trigger < 0 or type(armed) not in (int, float)
                    or type(updated) not in (int, float) or not info.timestamp <= armed <= updated <= market.current_timestamp):
                raise PolicyError('invalid native trailing checkpoint')
        elif state not in ('waiting', 'pending') or any(x is not None for x in (trigger, armed, updated)) or moves:
            raise PolicyError('incomplete native trailing checkpoint')
        executor._trailing_stop_trigger_pct = trigger
        executor._trailing_stop_move_count = moves
        executor._trailing_stop_armed_timestamp = armed
        executor._trailing_stop_last_update_timestamp = updated
        net = Decimal(str(custom['remaining_position_amount_base']))
        if (executor.amount_to_close != net or net <= 0
                or market.get_balance(info.config.trading_pair.split('-')[0]) != net
                or executor.cum_fees_quote != Decimal(str(custom['execution_receipt']['fees_quote']))):
            raise PolicyError('native exact inventory or fee ownership changed')
        return executor
