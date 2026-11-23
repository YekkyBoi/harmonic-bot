"""The friend's tickmill takes gold / forex from the fx topic only (the owner's decision, 23.11: "Xau zaten kripto değil fx
e bağlı olacak ... marketse market limit emirse kuralımızı uygulayacaksın tickmill için").

The incident: the same XAU short chart (entry 3610.49, stop 3636.92, price 3602) was posted in both topics. The kripto
topic reads every ticker as a USDT perpetual (XAUUSDT, market type perp) and its 0.5% near-entry rule made it a market
order: tickmill sold at 3601.7. With ``fx_only_from_fx_topic`` tickmill leaves the kripto topic's gold / forex out (the
owner's accounts and the coins as before) and takes the fx topic's signal: a limit there waits for FundingPips' fill or
price (reference_broker), a market signal opens at market.

These run the REAL ClaudeSignalParser (scripted model): the topic kind sets the market type, as on the server.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from copytrader.config import AppConfig
from copytrader.engine.monitor import is_virtual_pending
from copytrader.models import EntryType, MarketType, PositionStatus

import test_preflight_scenarios as tps
from test_llm_parser import wire_action
from test_preflight_scenarios import FX, KRIPTO, Prod, ScriptedModel, _isolate  # noqa: F401  (_isolate: autouse)

ENTRY, STOP, TP = 3610.49, 3636.92, 3500.11


def _setup(*, on: bool = True):
    def edit(cfg: AppConfig) -> None:
        tickmill = next(b for b in cfg.brokers if b.name == "tickmill")
        tickmill.reference_broker = "fx"
        tickmill.confirm_open = False
        tickmill.virtual_trigger_max_hours = 24
        tickmill.fx_only_from_fx_topic = on
        for route in cfg.telegram.topics:          # (the server: fx trades the kripto topic too)
            if route.name == "kripto" and "fx" not in route.brokers:
                route.brokers.append("fx")
    return edit


class Env:
    def __init__(self, p: Prod, model: ScriptedModel) -> None:
        self.p, self.model, self.mid = p, model, 9000

    async def gold(self, topic: int, text: str = "Xau short\n1R risk", **kw: Any) -> None:
        """The trader posts the chart in ``topic``; the model reads it with null market type / quote (the topic
        decides them)."""
        fields: dict[str, Any] = dict(
            action="open", base_asset="XAU", side="short", entry_price=ENTRY, stop_loss=STOP,
            take_profits=[{"price": TP, "close_fraction": 1.0}], risk_r=1.0, confidence=0.95,
            summary_tr="XAU SHORT, giriş 3610.49, SL 3636.92, TP 3500.11, risk 1R")
        fields.update(kw)
        self.model.queue.append([wire_action(**fields)])
        self.mid += 1
        await self.p._post(self.mid, topic, text)


async def _make(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, on: bool = True) -> Env:
    # Binance lists the XAUUSDT perpetual, as on the server ("Bazı kripto borsalarında da var").
    plain = tps.Binance.__init__

    def with_gold(self: tps.Binance) -> None:
        plain(self)
        self.markets["XAU/USDT:USDT"] = tps._swap("XAU", amount=0.001, price=0.01, min_qty=0.001, min_cost=5.0)
        self.set_price("XAU", 3602.5, 3602.7)
    monkeypatch.setattr(tps.Binance, "__init__", with_gold)
    model = ScriptedModel()
    p = await Prod.create(tmp_path, monkeypatch, edit=_setup(on=on), model=model, replica="live")
    _gold(p, 3601.63, 3601.83)
    return Env(p, model)


def _gold(p: Prod, bid: float, ask: float) -> None:
    p.terminal.set_price("XAUUSD", bid, ask)
    p.tickmill.set_price("XAUUSD", bid, ask)


@pytest.fixture
async def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    e = await _make(tmp_path, monkeypatch)
    yield e
    await e.p.close()


async def _tickmill(p: Prod, trade_id: int):
    return (await p.positions(trade_id)).get("tickmill")


# --------------------------------------------------------------------------- #
async def test_kripto_topic_gold_is_read_as_a_perpetual_and_left_out_on_tickmill(env) -> None:
    p = env.p
    await env.gold(KRIPTO)
    (trade,) = await p.trades()
    assert (trade.plan.route, trade.plan.market_type, trade.plan.quote_asset) == ("kripto", MarketType.PERP, "USDT")
    assert trade.plan.entry_type is EntryType.MARKET                # the kripto rule, as before
    assert p.tickmill_orders() == [] and await _tickmill(p, trade.id) is None
    assert p.fx_orders()[0][:2] == ("SELL", "XAUUSD")               # the owner's fx: unchanged
    assert any("tickmill: XAU kripto konusundan alınmaz; bu hesap altın / forex sinyalini Fx konusundan alır" in t
               for t in p.texts())


async def test_the_incident_both_topics_tickmill_waits_for_fundingpips_at_the_charts_entry(env) -> None:
    p = env.p
    await env.gold(KRIPTO)
    await env.gold(FX, "Xau short\n1R risk")
    await env.gold(FX, "Limit emir", entry_type="limit")             # (a repost of the same signal: asked, not opened)
    kripto, fx = (await p.trades())[:2]
    assert await _tickmill(p, kripto.id) is None
    assert fx.plan.entry_type is EntryType.LIMIT and fx.plan.market_type is MarketType.CFD
    pos = await _tickmill(p, fx.id)
    assert is_virtual_pending(pos) and pos.data["reference_broker"] == "fx"
    assert p.tickmill_orders() == []                                  # nothing at 3601.7
    for _ in range(3):                                                # FundingPips' price under the entry: waits
        await p.engine.monitor.run_once()
        await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == []
    # FundingPips' sell limit at 3610.49 fills: tickmill sells at market then
    _gold(p, 3610.55, 3610.75)
    ticket = next(t for t, o in p.terminal.orders.items() if o.price_open == pytest.approx(ENTRY))
    p.terminal.fill(ticket, ENTRY)
    await p.engine.monitor.run_once()
    await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()
    assert order[:2] == ("market_sell", "XAUUSD")
    assert (await _tickmill(p, fx.id)).status is PositionStatus.OPEN


async def test_a_market_signal_of_the_fx_topic_opens_at_market_on_tickmill(env) -> None:
    p = env.p
    _gold(p, 3610.40, 3610.60)                                        # at the entry
    await env.gold(FX, "Xau short 1R risk")
    (trade,) = await p.trades()
    assert trade.plan.entry_type is EntryType.MARKET
    (order,) = p.tickmill_orders()
    assert order[:2] == ("market_sell", "XAUUSD")


async def test_forex_of_the_kripto_topic_is_left_out_too(env) -> None:
    p = env.p
    p.terminal.set_price("EURUSD", 1.16195, 1.16205)
    p.tickmill.set_price("EURUSD", 1.16195, 1.16205)
    await env.gold(KRIPTO, "Eur short", base_asset="EUR", entry_price=1.1620, stop_loss=1.1640,
                   take_profits=[{"price": 1.1580, "close_fraction": 1.0}], summary_tr="EUR SHORT")
    for trade in await p.trades():
        assert await _tickmill(p, trade.id) is None
    assert p.tickmill_orders() == []


async def test_coins_of_the_kripto_topic_are_unchanged_on_tickmill(env) -> None:
    p = env.p
    await env.gold(KRIPTO, "Btc long 1R", base_asset="BTC", side="long", entry_price=74000.0, stop_loss=73000.0,
                   take_profits=[{"price": 76000.0, "close_fraction": 1.0}], summary_tr="BTC LONG")
    (trade,) = await p.trades()
    assert await _tickmill(p, trade.id) is not None


async def test_without_the_setting_tickmill_sells_kripto_gold_at_market_as_before(tmp_path, monkeypatch) -> None:
    e = await _make(tmp_path, monkeypatch, on=False)
    try:
        await e.gold(KRIPTO)
        (order,) = e.p.tickmill_orders()
        assert order[:2] == ("market_sell", "XAUUSD")
    finally:
        await e.p.close()


async def test_the_dry_check_says_why_tickmill_is_left_out(env) -> None:
    p = env.p
    from copytrader.models import Side
    from test_preflight_scenarios import signal
    plan_action = signal("XAUUSDT", "XAU", "USDT", MarketType.PERP, Side.SHORT, ENTRY, STOP, TP)
    route = next(r for r in p.cfg.telegram.topics if r.name == "kripto")
    ev = await p.engine.evaluate(plan_action, -1003782294463, route=route)
    assert ev.plan is not None
    tickmill = next(b for b in p.engine.brokers if b.name == "tickmill")
    assert not p.engine._takes(tickmill, ev.plan)


# --------------------------------------------------------------------------- #
# the update (migrate step 13)
# --------------------------------------------------------------------------- #
def test_the_update_turns_it_on_for_tickmill_only_and_keeps_a_hand_written_value(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from test_preflight2_tools import PROD_YAML, broker_of, migrate_mod, prod_file, tickmill_block

    from copytrader.config import load_config
    for name in list(os.environ):                     # (load_config copies .env into os.environ: undone afterwards)
        monkeypatch.setenv(name, os.environ[name])
    m = migrate_mod()
    none = (None, None)
    path = prod_file(tmp_path)
    lines = m.migrate_config(path, terminal=none)
    assert any(line.startswith("tickmill:") and line.endswith("(fx_only_from_fx_topic: true)") for line in lines)
    cfg = load_config(path)
    assert tickmill_block(cfg).fx_only_from_fx_topic is True
    for name in ("binance", "breakout", "fx"):
        assert broker_of(cfg, name).fx_only_from_fx_topic is False
    assert m.migrate_config(path, terminal=none) == []                                 # idempotent
    (tmp_path / "hand").mkdir()
    hand = prod_file(tmp_path / "hand", PROD_YAML.replace("    risk_usd: 45\n",
                                                          "    risk_usd: 45\n    fx_only_from_fx_topic: false\n"))
    m.migrate_config(hand, terminal=none)
    assert tickmill_block(load_config(hand)).fx_only_from_fx_topic is False


def test_the_update_leaves_it_off_when_tickmill_has_no_fx_topic(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from test_preflight2_tools import PROD_YAML, migrate_mod, prod_file, tickmill_block

    from copytrader.config import load_config
    for name in list(os.environ):
        monkeypatch.setenv(name, os.environ[name])
    m = migrate_mod()
    text = PROD_YAML.replace("      brokers: [fx, tickmill]\n", "      brokers: [fx]\n")
    assert text != PROD_YAML
    path = prod_file(tmp_path, text)
    notes: list[str] = []
    m.migrate_config(path, notes=notes, terminal=(None, None))
    assert tickmill_block(load_config(path)).fx_only_from_fx_topic is False
    assert any("fx_only_from_fx_topic yazılmadı" in n for n in notes)


# --------------------------------------------------------------------------- #
# review round: leftovers, questions, notices, unknown routes
# --------------------------------------------------------------------------- #
async def test_a_waiting_entry_an_older_version_left_on_kripto_gold_never_fires(tmp_path, monkeypatch) -> None:
    e = await _make(tmp_path, monkeypatch, on=False)
    p = e.p
    try:
        await e.gold(KRIPTO, "Xau short Limit emir", entry_type="limit")
        (trade,) = await p.trades()
        assert is_virtual_pending(await _tickmill(p, trade.id))         # (the old behaviour: Binance's touch)
        next(b for b in p.engine.configured_brokers if b.name == "tickmill").config.fx_only_from_fx_topic = True
        p.binance.set_price("XAU", 3610.50, 3610.70)                    # Binance touches the entry
        await p.engine.monitor.run_virtual_once()
        assert p.tickmill_orders() == []
        pos = await _tickmill(p, trade.id)
        assert pos.status is PositionStatus.CANCELLED
        assert any("bu hesapta artık açılmıyor (Fx konusundan alınır)" in t for t in p.texts())
    finally:
        await p.close()


async def test_a_doubtful_kripto_gold_signal_never_names_tickmill_and_evet_leaves_it_out(env) -> None:
    p = env.p
    await env.gold(KRIPTO, confidence=0.5)
    (conf,) = p.notifier.confirmations
    assert conf.early is None                                         # tickmill takes nothing early
    await p.engine.handle_confirmation(conf.id, True)
    assert p.tickmill_orders() == []
    for trade in await p.trades():
        assert await _tickmill(p, trade.id) is None


async def test_no_not_connected_notice_for_tickmill_on_kripto_gold(env) -> None:
    p = env.p
    tickmill = next(b for b in p.engine.brokers if b.name == "tickmill")
    p.engine.brokers = [b for b in p.engine.brokers if b is not tickmill]
    p.engine.optional_down["tickmill"] = "test"
    await env.gold(KRIPTO)
    assert not any("tickmill bağlı değil" in t for t in p.texts())


async def test_unknown_route_and_a_base_carrying_its_quote_are_still_left_out(env) -> None:
    p = env.p
    await env.gold(KRIPTO)
    (trade,) = await p.trades()
    tickmill = next(b for b in p.engine.configured_brokers if b.name == "tickmill")
    renamed = trade.plan.model_copy(update={"route": "eski-konu"})
    assert renamed.market_type is MarketType.PERP and not p.engine._takes(tickmill, renamed)
    assert not p.engine._takes(tickmill, trade.plan.model_copy(update={"base_asset": "XAUUSDT"}))
    assert p.engine._takes(tickmill, trade.plan.model_copy(update={"base_asset": "BTC"}))
    fx_renamed = trade.plan.model_copy(update={"route": "eski-konu", "market_type": MarketType.CFD})
    assert p.engine._takes(tickmill, fx_renamed)


def test_a_config_written_by_the_unreleased_build_still_loads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from test_preflight2_tools import PROD_YAML, prod_file, tickmill_block

    from copytrader.config import load_config
    for name in list(os.environ):
        monkeypatch.setenv(name, os.environ[name])
    text = PROD_YAML.replace("    risk_usd: 45\n", "    risk_usd: 45\n    fx_entry_rule_everywhere: true\n"
                                                    "    one_position_per_signal: true\n")
    t = tickmill_block(load_config(prod_file(tmp_path, text)))
    assert t.fx_only_from_fx_topic is False
