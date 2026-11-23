"""The friend's tickmill, gold / forex of the CRYPTO topic (the owner's decision, 23.11: "XAU kripto mu? ... grupta hangi
işlem geliyorsa ona göre hareket et, tickmill için"):

    fx_entry_rule_everywhere   a forex / metal plan of the kripto topic is entered on tickmill by the fx topic's rule:
                               the XAU short of 23.11 (entry 3610.49, stop 3636.92, price 3601.7) waits for the entry
                               instead of selling at the market (the kripto rule: within 0.5% -> market). It waits for
                               the fx account's PRICE (fx's own leg of that trade is a market fill: no touch of it).
                               Coins and the other accounts: as before.
    one_position_per_signal    the trader posted the same chart in both topics: tickmill takes it once (the first to
                               arrive); a second one is taken again once the first has ended there.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from copytrader.config import AppConfig
from copytrader.engine.monitor import is_virtual_pending
from copytrader.models import EntryType, MarketType, PositionStatus, Side, TradeStatus

from test_preflight_scenarios import FX, KRIPTO, Prod, _isolate, btc, signal  # noqa: F401  (_isolate: autouse)

ENTRY, STOP, TP = 3610.49, 3636.92, 3500.11
FRIEND_LEAK = ("'fx'", "fx hesab", "fundingpips")


def _setup(*, fx_rule: bool = True, one: bool = True):
    def edit(cfg: AppConfig) -> None:
        tickmill = next(b for b in cfg.brokers if b.name == "tickmill")
        tickmill.reference_broker = "fx"
        tickmill.confirm_open = False
        tickmill.virtual_trigger_max_hours = 24
        tickmill.fx_entry_rule_everywhere = fx_rule
        tickmill.one_position_per_signal = one
        for route in cfg.telegram.topics:          # (the server: fx trades the kripto topic too)
            if route.name == "kripto" and "fx" not in route.brokers:
                route.brokers.append("fx")
    return edit


async def _make(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **kw) -> Prod:
    p = await Prod.create(tmp_path, monkeypatch, edit=_setup(**kw), replica="live")
    _gold(p, 3601.63, 3601.83)
    return p


def _gold(p: Prod, bid: float, ask: float) -> None:
    p.terminal.set_price("XAUUSD", bid, ask)
    p.tickmill.set_price("XAUUSD", bid, ask)


def _xau_short(entry: float = ENTRY, entry_type: EntryType | None = None):
    return signal("XAUUSD", "XAU", "USD", MarketType.CFD, Side.SHORT, entry, STOP, TP, entry_type=entry_type)


async def _say_gold(p: Prod, topic: int, **kw) -> int:
    return await p.say(_xau_short(**kw), topic, "Xau short 1R risk")


async def _tickmill(p: Prod, trade_id: int):
    return (await p.positions(trade_id)).get("tickmill")


@pytest.fixture
async def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    p = await _make(tmp_path, monkeypatch)
    yield p
    await p.close()


# --------------------------------------------------------------------------- #
# fx_entry_rule_everywhere
# --------------------------------------------------------------------------- #
async def test_kripto_gold_away_from_its_entry_waits_on_tickmill_the_others_unchanged(env) -> None:
    p = env
    await _say_gold(p, KRIPTO)
    (trade,) = await p.trades()
    # the plan stays the kripto topic's (market); the fx rule's type is carried for tickmill
    assert (trade.plan.route, trade.plan.entry_type, trade.plan.fx_entry_type) == ("kripto", EntryType.MARKET,
                                                                                   EntryType.LIMIT)
    assert p.tickmill_orders() == []                                  # no market sell at 3601.7
    pos = await _tickmill(p, trade.id)
    assert is_virtual_pending(pos)
    assert (pos.data["reference_broker"], pos.data["reference_price_only"], pos.data["trigger_price"]) == (
        "fx", True, ENTRY)
    assert (pos.stop_loss, [t.price for t in pos.take_profits]) == (STOP, [TP])
    assert p.fx_orders()[0][:2] == ("SELL", "XAUUSD")                 # the owner's fx: as before (market)
    assert any("tickmill: sanal tetik kuruldu (limit emir konmadı): fx fiyatı 3610.49 seviyesine gelince" in t
               for t in p.texts())


async def test_fx_market_fill_is_no_touch_tickmill_opens_when_fx_price_reaches_the_entry(env) -> None:
    p = env
    await _say_gold(p, KRIPTO)
    for _ in range(3):                                # fx's leg is open (a market fill), its price under the entry
        await p.engine.monitor.run_once()
        await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == []
    assert is_virtual_pending(await _tickmill(p, 1))
    _gold(p, 3610.55, 3610.75)                        # fx's bid reaches the short's entry
    await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()
    assert order[:2] == ("market_sell", "XAUUSD")
    assert order[-2:] == (pytest.approx(STOP), pytest.approx(TP))
    assert (await _tickmill(p, 1)).status is PositionStatus.OPEN
    assert len(p.fx_orders()) == 1                    # fx never twice


async def test_kripto_gold_near_its_entry_opens_at_market_on_tickmill(env) -> None:
    p = env
    _gold(p, 3608.60, 3608.80)                        # 1.9 from the entry: within a quarter of the stop distance (6.6)
    await _say_gold(p, KRIPTO)
    (trade,) = await p.trades()
    assert (trade.plan.entry_type, trade.plan.fx_entry_type) == (EntryType.MARKET, EntryType.MARKET)
    (order,) = p.tickmill_orders()
    assert order[:2] == ("market_sell", "XAUUSD")


async def test_explicit_limit_of_the_kripto_topic_still_waits(env) -> None:
    p = env
    await _say_gold(p, KRIPTO, entry_type=EntryType.LIMIT)
    (trade,) = await p.trades()
    assert (trade.plan.entry_type, trade.plan.fx_entry_type) == (EntryType.LIMIT, EntryType.LIMIT)
    pos = await _tickmill(p, trade.id)
    assert is_virtual_pending(pos) and not pos.data.get("reference_price_only")   # fx's own limit: its fill decides
    assert p.tickmill_orders() == []


async def test_without_the_setting_tickmill_sells_kripto_gold_at_market_as_before(tmp_path, monkeypatch) -> None:
    p = await _make(tmp_path, monkeypatch, fx_rule=False, one=False)
    try:
        await _say_gold(p, KRIPTO)
        (trade,) = await p.trades()
        assert trade.plan.fx_entry_type is None
        (order,) = p.tickmill_orders()
        assert order[:2] == ("market_sell", "XAUUSD")
    finally:
        await p.close()


async def test_coins_of_the_kripto_topic_are_unchanged(env) -> None:
    p = env
    await p.say(btc(EntryType.LIMIT, 73000.0, 72000.0, 76000.0), KRIPTO, "BTC LONG Limit")
    (trade,) = await p.trades()
    assert trade.plan.fx_entry_type is None
    pos = await _tickmill(p, trade.id)
    assert is_virtual_pending(pos) and pos.data.get("reference_exchange") == "binanceusdm"
    assert "reference_broker" not in pos.data


async def test_the_fx_topic_is_unchanged(env) -> None:
    p = env
    await _say_gold(p, FX)
    (trade,) = await p.trades()
    assert (trade.plan.entry_type, trade.plan.fx_entry_type) == (EntryType.LIMIT, None)
    pos = await _tickmill(p, trade.id)
    assert is_virtual_pending(pos) and not pos.data.get("reference_price_only")
    assert p.fx_orders()[0][:2] == ("SELL_LIMIT", "XAUUSD")


# --------------------------------------------------------------------------- #
# one_position_per_signal
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("first, second", [(KRIPTO, FX), (FX, KRIPTO)])
async def test_the_same_chart_in_both_topics_is_one_tickmill_position(env, first, second) -> None:
    p = env
    await _say_gold(p, first)
    await _say_gold(p, second)
    one, two = await p.trades()
    assert is_virtual_pending(await _tickmill(p, one.id))
    assert await _tickmill(p, two.id) is None                      # skipped: nothing stored for it
    assert any("tickmill: aynı sinyal diğer konudan zaten alındı (#1); bu hesapta ikinci kez açılmadı" in t
               for t in p.texts())
    # the price reaches the entry (fx's resting limit of the fx topic's trade fills too): still one tickmill order
    _gold(p, 3610.55, 3610.75)
    (ticket,) = list(p.terminal.orders)
    p.terminal.fill(ticket, ENTRY)
    for _ in range(2):
        await p.engine.monitor.run_once()
        await p.engine.monitor.run_virtual_once()
    assert len(p.tickmill_orders()) == 1


async def test_friend_is_told_without_the_owners_names(env) -> None:
    p = env
    notes: list[str] = []

    async def note(text: str) -> None:
        notes.append(text)
    p.notifier.friend_note = note                                  # type: ignore[attr-defined]
    await _say_gold(p, FX)
    await _say_gold(p, KRIPTO)
    import asyncio
    for _ in range(5):
        await asyncio.sleep(0)
    told = [t for t in notes if "ikinci kez açılmadı" in t]
    assert told, notes
    for text in told:
        assert not any(word in text.casefold() for word in FRIEND_LEAK), text


async def test_once_the_first_has_ended_on_tickmill_the_second_is_taken(env) -> None:
    p = env
    await _say_gold(p, KRIPTO)
    pos = await _tickmill(p, 1)
    ended = pos.model_copy(deep=True)
    ended.status = PositionStatus.CANCELLED
    ended.data["virtual_state"] = "cancelled"
    await p.db.save_broker_position(1, ended)
    await _say_gold(p, FX)
    assert is_virtual_pending(await _tickmill(p, 2))


async def test_a_repost_in_the_same_topic_is_left_to_the_repost_question(env) -> None:
    p = env
    await _say_gold(p, FX)
    await _say_gold(p, FX)
    assert p.asked() and "duplicate_signal" in p.asked()[-1]
    trades = await p.trades()
    assert trades[0].status is TradeStatus.PENDING


async def test_another_instrument_or_side_is_never_a_repost(env) -> None:
    p = env
    await _say_gold(p, KRIPTO)
    await p.say(signal("XAUUSD", "XAU", "USD", MarketType.CFD, Side.LONG, 3590.0, 3570.0, 3650.0), FX, "Xau long")
    p.terminal.set_price("EURUSD", 1.16195, 1.16205)
    await p.say(signal("EURUSD", "EUR", "USD", MarketType.FOREX, Side.SHORT, 1.1640, 1.1660, 1.1580), FX, "Eur short")
    assert not any("ikinci kez açılmadı" in t for t in p.texts())     # (the long: tickmill's no_hedge rule says why)
    eur = (await p.trades())[2]
    assert is_virtual_pending(await _tickmill(p, eur.id))


# --------------------------------------------------------------------------- #
# the update (migrate step 13)
# --------------------------------------------------------------------------- #
def test_the_update_turns_both_on_for_tickmill_only_and_keeps_a_hand_written_value(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from test_preflight2_tools import PROD_YAML, broker_of, migrate_mod, prod_file, tickmill_block

    from copytrader.config import load_config
    for name in list(os.environ):                     # (load_config copies .env into os.environ: undone afterwards)
        monkeypatch.setenv(name, os.environ[name])
    m = migrate_mod()
    none = (None, None)
    path = prod_file(tmp_path)
    lines = m.migrate_config(path, terminal=none)
    assert any(line.endswith("(fx_entry_rule_everywhere: true)") and line.startswith("tickmill:") for line in lines)
    assert any(line.endswith("(one_position_per_signal: true)") and line.startswith("tickmill:") for line in lines)
    cfg = load_config(path)
    t = tickmill_block(cfg)
    assert (t.fx_entry_rule_everywhere, t.one_position_per_signal) == (True, True)
    for name in ("binance", "breakout", "fx"):
        b = broker_of(cfg, name)
        assert (b.fx_entry_rule_everywhere, b.one_position_per_signal) == (False, False)
    assert m.migrate_config(path, terminal=none) == []                                 # idempotent
    hand = PROD_YAML.replace("    risk_usd: 45\n", "    risk_usd: 45\n    fx_entry_rule_everywhere: false\n")
    (tmp_path / "hand").mkdir()
    path2 = prod_file(tmp_path / "hand", hand)
    m.migrate_config(path2, terminal=none)
    t2 = tickmill_block(load_config(path2))
    assert (t2.fx_entry_rule_everywhere, t2.one_position_per_signal) == (False, True)
