"""The owner's decision: his own accounts Binance, FundingPips (fx) and BEM (fx2) open a signal the bot is unsure
about at once, as read (confirm_open: false, written by migrate step 11); the manual breakout account is still asked.
"TP'si belli olmayanı da aç": a signal without a (readable) target is such a signal too.

On the production replica of tests/test_preflight_scenarios.py as the update leaves it: the server's config.yaml
(LIVE_YAML) with an fx2 block and a "challenge2" topic (fx2 only) added - neither is in the replica, their settings
here are assumptions - migrated by copytrader.migrate (steps 9-11), then loaded as the bot loads it. fx2 runs the
MetaTrader adapter its helper process runs, in process, on a terminal of its own.

  (a) challenge2 (fx2 only)   opens on fx2 at once; nobody is asked
  (b) fx route                fx and tickmill at once; a limit rests on fx, tickmill waits for its fill (never a limit)
  (c) kripto route            binance and tickmill at once, breakout asked; "Evet" opens breakout only
  (d) hard problems           no stop, a repost, an opposite trade, a target on the wrong side: never opened early
  (e) the accounts' own rules daily loss with the open risk (fx, fx2) still refuse a misread size; binance has none
  (f) no target               opens without one everywhere; the trader's later "TP 1.1030" reaches every position
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from copytrader import migrate
from copytrader.brokers import mt5_broker
from copytrader.brokers.mt5_broker import MT5, Mt5Broker
from copytrader.engine.monitor import is_virtual_pending
from copytrader.models import ActionType, EntryType, MarketType, ParsedAction, PositionStatus, Side, TakeProfit, TradeStatus

import test_preflight_scenarios as tps
from test_llm_parser import wire_action
from test_preflight_scenarios import (  # noqa: F401  (_isolate: autouse)
    DUMMY_ENV,
    FX,
    KRIPTO,
    FxTerminal,
    Prod,
    ScriptedModel,
    _isolate,
    btc,
    fx_pair,
    signal,
)

CHALLENGE2 = 13
BEM_EXE = r"C:\Program Files\BEM Funding MT5 Terminal\terminal64.exe"
# (assumed: a BEM challenge of 100,000 USD, 1R = 1,000 USD, daily loss limit 2.5% with the open risk counted)
FX2_BLOCK = (
    "  - name: fx2\n    type: mt5_bridge\n    enabled: true\n    optional: true\n    friend_account: false\n"
    f"    terminal_path: {BEM_EXE}\n    broker_company: BEM\n    risk_usd: 1000\n    no_hedge: true\n"
    "    daily_loss_limit_pct: 2.5\n    daily_loss_include_open_risk: true\n"
)
CHALLENGE2_ROUTE = ('    - name: challenge2\n      title_contains: ["Challenge"]\n      brokers: [fx2]\n'
                    "      kind: fx\n")
ALONE = "başka hesap yok, soru sorulmadı."


def server_config_after_update(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, kripto_fx: bool = False) -> str:
    """LIVE_YAML + fx2 + challenge2 (``kripto_fx``: fx on the kripto topic too), migrated as the update does."""
    for name, value in DUMMY_ENV.items():
        monkeypatch.setenv(name, value)
    text = tps.LIVE_YAML.replace("  - name: tickmill\n", FX2_BLOCK + "  - name: tickmill\n", 1)
    text = text.replace("notifier:\n", CHALLENGE2_ROUTE + "notifier:\n", 1)
    if kripto_fx:
        text = text.replace("brokers: [binance, breakout, tickmill]", "brokers: [binance, breakout, fx, tickmill]", 1)
    folder = tmp_path / "guncelleme"
    folder.mkdir()
    path = folder / "config.yaml"
    path.write_text(text, encoding="utf-8")
    monkeypatch.chdir(folder)              # (load_config also reads a ".env" of the current directory: none here)
    lines = migrate.migrate_config(path, terminal=(None, None))
    assert [ln.split(":")[0] for ln in lines if ln.endswith("(confirm_open: false)")] == [
        "tickmill", "binance", "fx", "fx2"]
    return path.read_text(encoding="utf-8")


def bem_terminal() -> FxTerminal:
    term = FxTerminal()
    term.terminal.path = BEM_EXE.rsplit("\\", 1)[0]
    term.terminal.company = "BEM Funding Ltd"
    term.account.login = 7001
    term.account.company, term.account.server = "BEM Funding Ltd", "BEMFunding-Server"
    term.account.equity = term.account.balance = term.account.margin_free = 100_000.0
    return term


async def own(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, kripto_fx: bool = False,
              model: ScriptedModel | None = None) -> Prod:
    monkeypatch.setattr(tps, "LIVE_YAML", server_config_after_update(tmp_path, monkeypatch, kripto_fx=kripto_fx))
    monkeypatch.setitem(tps.TITLES, CHALLENGE2, "Kasım Ayı Challenge İşlemleri")
    term = bem_terminal()
    # (one MetaTrader module per process, keyed by the module object's id: an id another test's module had - and
    # left behind - would read as a second account in the same package; production runs fx2 in a process of its own)
    monkeypatch.setattr(mt5_broker, "_ACTIVE_LOGINS", {})

    def fx2(_p: Prod, by: dict[str, Any]) -> dict[str, Any]:
        broker = Mt5Broker(by["fx2"], mt5_module=term)
        broker.retry_delay_sec = 0
        return {"fx2": broker}

    p = await Prod.create(tmp_path, monkeypatch, replica="live", more=fx2, model=model)
    p.fx2 = term                                                   # type: ignore[attr-defined]
    return p


@pytest.fixture
async def world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    p = await own(tmp_path, monkeypatch)
    yield p
    await p.close()


# ---- helpers ----------------------------------------------------------------------------------------------------
KINDS = {(MT5.TRADE_ACTION_DEAL, MT5.ORDER_TYPE_BUY): "BUY", (MT5.TRADE_ACTION_DEAL, MT5.ORDER_TYPE_SELL): "SELL",
         (MT5.TRADE_ACTION_PENDING, MT5.ORDER_TYPE_BUY_LIMIT): "BUY_LIMIT"}


def orders(term: FxTerminal) -> list[tuple[Any, ...]]:
    """Orders sent to a MetaTrader terminal: (kind, symbol, lots, price, sl, tp); tp 0.0 = none."""
    return [(KINDS[(r["action"], r["type"])], r["symbol"], r["volume"], r["price"], r["sl"], r["tp"])
            for r in term.sent if r["action"] in (MT5.TRADE_ACTION_DEAL, MT5.TRADE_ACTION_PENDING)]


def level_changes(term: FxTerminal) -> list[tuple[str, int, float, float]]:
    """Stop / target changes sent to a terminal: ("position" | "order", ticket, sl, tp)."""
    return [("position", r["position"], r["sl"], r["tp"]) if r["action"] == MT5.TRADE_ACTION_SLTP
            else ("order", r["order"], r["sl"], r["tp"])
            for r in term.sent if r["action"] in (MT5.TRADE_ACTION_SLTP, MT5.TRADE_ACTION_MODIFY)]


def tickmill_changes(p: Prod) -> list[tuple[Any, ...]]:
    return [(*args, kw["stop_loss"], kw["take_profit"]) for name, args, kw in p.tickmill.calls
            if name == "modify_position"]


def unsure(action: ParsedAction, **kw: Any) -> ParsedAction:
    return action.model_copy(update={"confidence": 0.75, **kw})


def eur_at(p: Prod, bid: float, ask: float, *, tickmill: float = 0.0) -> None:
    """EURUSD on fx and fx2; tickmill ``tickmill`` higher."""
    p.terminal.set_price("EURUSD", bid, ask)
    p.fx2.set_price("EURUSD", bid, ask)                            # type: ignore[attr-defined]
    p.tickmill.set_price("EURUSD", bid + tickmill, ask + tickmill)


def no_tp(entry_type: EntryType = EntryType.MARKET, entry: float = 1.1000) -> ParsedAction:
    """As the live EUR #16 was read: no target, confidence 75%."""
    return fx_pair("EURUSD", Side.LONG, entry, 1.0980, 1.2, entry_type=entry_type).model_copy(
        update={"take_profits": [], "confidence": 0.75, "summary_tr": "EUR LONG, TP yok"})


def move_tp(mid: int, price: float, confidence: float = 0.95) -> ParsedAction:
    return ParsedAction(action=ActionType.MOVE_TP, take_profits=[TakeProfit(price=price)], refers_to_message_id=mid,
                        confidence=confidence, summary_tr=f"TP {price:g}")


def errors(p: Prod) -> list[str]:
    return [t for t in p.texts() if t.startswith(("🚨", "ℹ️"))]


# =================================================================================================================
# the config the update leaves
# =================================================================================================================
async def test_the_update_leaves_breakout_the_only_account_that_asks(world) -> None:
    by = {b.name: b for b in world.cfg.brokers}
    assert {n: b.confirm_open for n, b in by.items()} == {
        "binance": False, "breakout": True, "fx": False, "fx2": False, "tickmill": False}
    assert (by["fx2"].type, by["fx2"].friend_account, by["tickmill"].reference_broker) == ("mt5_bridge", False, "fx")
    assert [(r.name, r.brokers) for r in world.cfg.telegram.topics][-1] == ("challenge2", ["fx2"])


# =================================================================================================================
# (a) challenge2: fx2 is the topic's only account
# =================================================================================================================
async def test_a_challenge2_unsure_signal_opens_on_fx2_at_once_and_nobody_is_asked(world) -> None:
    p = world
    await p.say(unsure(fx_pair("EURUSD", Side.LONG, 1.1620, 1.1600, 1.1660)), CHALLENGE2, "EUR LONG")
    assert p.asked() == []
    assert orders(p.fx2) == [("BUY", "EURUSD", 4.87, 1.16205, 1.16, 1.166)]
    assert orders(p.terminal) == [] and p.tickmill_orders() == [] and p.binance.created() == []
    (trade,) = await p.trades()
    assert trade.status is TradeStatus.OPEN and list(await p.positions(trade.id)) == ["fx2"]
    (report,) = p.reports()
    assert report.startswith("✅ <b>#1 EUR LONG açıldı</b>")
    assert "• fx2: 4.87 EURUSD (açık), risk 1 000.00 USD, giriş ~1.16205" in report
    assert report.endswith(f"⚡ fx2 için onay beklenmedi (confirm_open: false); {ALONE}")
    # What the owner sees today: the report does not say WHY the bot was unsure (here: confidence 75%).
    assert "Güven" not in report and "%75" not in report


# =================================================================================================================
# (b) fx route: fx and tickmill, both confirm_open: false
# =================================================================================================================
async def test_b_fx_route_market_opens_on_fx_and_tickmill_at_once(world) -> None:
    p = world
    await p.say(unsure(fx_pair("EURUSD", Side.LONG, 1.1620, 1.1600, 1.1660)), FX, "EUR LONG")
    assert p.asked() == []
    assert p.fx_orders() == [("BUY", "EURUSD", 0.48, 1.16205, 1.16, 1.166)]
    assert p.tickmill_orders() == [("market_buy", "EURUSD", 0.25, 1.16, 1.166)]
    assert orders(p.fx2) == []
    (report,) = p.reports()
    assert f"⚡ fx ve tickmill için onay beklenmedi (confirm_open: false); {ALONE}" in report


async def test_b_fx_route_limit_rests_on_fx_at_once_and_tickmill_waits_for_its_fill_never_a_limit(world) -> None:
    p = world
    entry, stop, tp = 1.1610, 1.1590, 1.1650
    await p.say(unsure(fx_pair("EURUSD", Side.LONG, entry, stop, tp, entry_type=EntryType.LIMIT)), FX,
                "EUR LONG 1R Limit Emir")
    assert p.asked() == []
    assert p.fx_orders() == [("BUY_LIMIT", "EURUSD", 0.5, entry, stop, tp)]         # placed now, not after an answer
    assert p.tickmill_orders() == []                                                 # no limit at tickmill
    (report,) = p.reports()
    assert "fx hesabında limit emri yerleştirildi, tickmill hesabında sanal tetik kuruldu" in report
    assert report.endswith(ALONE)
    tm = (await p.positions(1))["tickmill"]
    assert is_virtual_pending(tm) and tm.data["reference_broker"] == "fx" and tm.data["trigger_price"] == entry
    # while fx's order waits, tickmill waits for its fill: fx's price is not even read
    reads = sum(1 for name, _a, _k in p.terminal.calls if name == "symbol_info_tick")
    for _ in range(3):
        await p.engine.monitor.run_virtual_once()
    assert sum(1 for name, _a, _k in p.terminal.calls if name == "symbol_info_tick") == reads
    assert p.tickmill_orders() == []
    # fx fills (fx mid 1.16100, tickmill mid 1.16115): tickmill opens at market, levels shifted by +0.00015, once
    p.terminal.set_price("EURUSD", 1.16095, 1.16105)
    p.tickmill.set_price("EURUSD", 1.16110, 1.16120)
    (ticket,) = list(p.terminal.orders)
    p.terminal.fill(ticket, entry)
    for _ in range(3):
        await p.engine.monitor.run_once()
        await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()
    assert order[:2] == ("market_buy", "EURUSD")
    assert order[-2:] == (pytest.approx(stop + 0.00015), pytest.approx(tp + 0.00015))
    assert len(p.fx_orders()) == 1 and p.asked() == []


# =================================================================================================================
# (c) kripto route: binance and tickmill at once, the manual breakout account asked
# =================================================================================================================
async def test_c_kripto_binance_and_tickmill_at_once_breakout_asked_and_yes_opens_breakout_only(world) -> None:
    p = world
    await p.say(unsure(btc(EntryType.LIMIT, 73500.0, 73000.0, 75000.0)), KRIPTO, "BTC LONG limit")
    (conf,) = p.notifier.confirmations
    assert p.asked() == [["low_confidence"]]
    assert conf.early == {
        "trade_id": 1, "brokers": ["binance", "tickmill"], "done": ["binance", "tickmill"],
        "line": "⚡ <b>#1</b> onay beklenmeden açılışa gönderildi — binance: limit emri yerleştirildi; tickmill: sanal "
                "tetiği kuruldu. Bu soru yalnızca breakout için."}
    assert [(o["type"], o["side"], o["amount"], o["price"]) for o in p.binance.entry_orders()] == [
        ("limit", "buy", 0.017, 73500.0)]
    assert p.tickmill_orders() == [] and is_virtual_pending((await p.positions(1))["tickmill"])
    assert "breakout" not in await p.positions(1)
    (report,) = p.reports()
    assert f"⚡ binance ve tickmill için onay beklenmedi (confirm_open: false); diğer hesaplar için onay soruldu " \
           f"(#{conf.id})." in report
    p.notifier.notifications.clear()
    await p.engine.handle_confirmation(conf.id, True)
    rows = await p.positions(1)
    assert rows["breakout"].status is PositionStatus.PENDING                         # the manual instruction
    assert any("✋ Elle işlem (breakout)" in t for t in p.texts())
    assert "• binance ve tickmill: onay beklenmeden önceden işleme alınmıştı; tekrar gönderilmedi" in \
        p.notifier.closed_text(conf.id)
    assert len(p.binance.entry_orders()) == 1 and p.tickmill_orders() == []         # nothing twice
    # tickmill's virtual entry still fires once on Binance's touch
    p.binance.set_price("BTC", 73499.0, 73501.0)
    p.tickmill.set_price("BTCUSD", 73449.0, 73451.0)
    for _ in range(2):
        await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == [("market_buy", "BTCUSD", 0.1, 72950.0, 74950.0)]
    assert len(await p.trades()) == 1


async def test_c_kripto_with_fx_on_it_too_binance_fx_and_tickmill_at_once(tmp_path: Path,
                                                                          monkeypatch: pytest.MonkeyPatch) -> None:
    """(The replica's kripto topic is [binance, breakout, tickmill]; were fx on it, it would open at once too.)"""
    p = await own(tmp_path, monkeypatch, kripto_fx=True)
    try:
        await p.say(unsure(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0)), KRIPTO, "BTC LONG")
        (conf,) = p.notifier.confirmations
        assert conf.early is not None and conf.early["brokers"] == ["binance", "fx", "tickmill"]
        assert conf.early["line"].endswith("Bu soru yalnızca breakout için.")
        assert [(o["type"], o["amount"]) for o in p.binance.entry_orders()] == [("market", 0.012)]
        assert p.fx_orders() == [("BUY", "BTCUSD", 0.14, 74010.0, 73300.0, 75500.0)]
        assert p.tickmill_orders() == [("market_buy", "BTCUSD", 0.08, 73250.0, 75450.0)]
        await p.engine.handle_confirmation(conf.id, True)
        assert (await p.positions(1))["breakout"].status is PositionStatus.OPEN
        assert (len(p.binance.entry_orders()), len(p.fx_orders()), len(p.tickmill_orders())) == (1, 1, 1)
    finally:
        await p.close()


# =================================================================================================================
# (d) what is never opened early
# =================================================================================================================
@pytest.mark.parametrize("topic", [FX, CHALLENGE2])
async def test_d_a_signal_without_a_stop_is_never_opened(world, topic: int) -> None:
    p = world
    await p.say(unsure(fx_pair("EURUSD", Side.LONG, 1.1620, 1.1600, 1.1660), stop_loss=None), topic, "EUR LONG")
    (conf,) = p.notifier.confirmations
    assert p.asked() == [["missing_stop_loss", "low_confidence"]] and conf.early is None
    assert (p.fx_orders(), p.tickmill_orders(), orders(p.fx2)) == ([], [], [])
    assert await p.trades() == []


async def test_d_a_target_read_on_the_wrong_side_is_never_opened(world) -> None:
    p = world
    await p.say(unsure(fx_pair("GBPUSD", Side.LONG, 1.3150, 1.3130, 1.3100)), FX, "GBP LONG")
    (conf,) = p.notifier.confirmations
    assert p.asked() == [["tp_wrong_side", "market_beyond_tp", "low_confidence"]] and conf.early is None
    assert (p.fx_orders(), p.tickmill_orders()) == ([], []) and await p.trades() == []


async def test_d_a_repost_is_asked_never_opened_twice(world) -> None:
    p = world
    signal_ = fx_pair("EURUSD", Side.LONG, 1.1620, 1.1600, 1.1660)
    await p.say(signal_, FX, "EUR LONG")
    assert len(p.fx_orders()) == len(p.tickmill_orders()) == 1
    await p.say(unsure(signal_), FX, "EUR LONG")
    (conf,) = p.notifier.confirmations
    assert set(p.asked()[0]) == {"duplicate_signal", "low_confidence"} and conf.early is None
    assert len(p.fx_orders()) == len(p.tickmill_orders()) == 1 and len(await p.trades()) == 1


async def test_d_an_opposite_trade_is_asked_on_kripto_and_refused_by_no_hedge_on_fx(world) -> None:
    p = world
    await p.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), KRIPTO, "BTC LONG")
    sent = (len(p.binance.created()), len(p.tickmill_orders()))
    short = signal("BTCUSDT.P", "BTC", "USDT", MarketType.PERP, Side.SHORT, 74000.0, 74700.0, 72500.0,
                   entry_type=EntryType.MARKET, confidence=0.75)
    await p.say(short, KRIPTO, "BTC SHORT")
    (conf,) = p.notifier.confirmations                       # (breakout may hedge: the owner decides)
    assert set(p.asked()[0]) == {"opposite_trade_open", "low_confidence"} and conf.early is None
    assert (len(p.binance.created()), len(p.tickmill_orders())) == sent
    # fx route: both accounts refuse hedging themselves - no question, nothing sent
    await p.say(fx_pair("EURUSD", Side.LONG, 1.1620, 1.1600, 1.1660), FX, "EUR LONG")
    p.notifier.notifications.clear()
    await p.say(unsure(fx_pair("EURUSD", Side.SHORT, 1.1620, 1.1640, 1.1580)), FX, "EUR SHORT")
    assert len(p.notifier.confirmations) == 1
    (text,) = errors(p)
    assert text.startswith("ℹ️ <b>#3 EUR SHORT açılmadı</b> (hedging yasağı)")
    assert "• fx: EUR: açık LONG var; ters yönlü SHORT sinyali hedging yasağı nedeniyle açılmadı" in text
    assert len(p.fx_orders()) == 1 and [o[0] for o in p.tickmill_orders()] == ["market_buy", "market_buy"]


# =================================================================================================================
# (e) the accounts' own rules
# =================================================================================================================
async def test_e_a_misread_5r_on_fx2_is_refused_by_its_daily_limit_and_the_owner_is_told(world) -> None:
    p = world
    await p.say(fx_pair("EURUSD", Side.LONG, 1.1620, 1.1600, 1.1660).model_copy(update={"risk_r": 5.0}),
                CHALLENGE2, "EUR LONG 5R")
    assert p.asked() == [] and orders(p.fx2) == []
    (trade,) = await p.trades()
    assert trade.status is TradeStatus.FAILED
    (text,) = errors(p)
    assert text == (
        "🚨 <b>#1 EUR LONG fx2 hesabında açılamadı</b>\n"
        "• fx2: günlük zarar sınırı (%2.5 = 2 500.00 USD) aşılırdı: bugün gerçekleşen zarar 0.00 + açık ve bekleyen "
        "pozisyonların stoptaki riski 0.00 + bu işlemin riski 5 000.00 = 5 000.00 USD, yani sınırı 2 500.00 USD "
        f"aşardı; yeni işlem açılmadı\n⚡ fx2 için onay beklenmedi (confirm_open: false); {ALONE}")


async def test_e_fx2_takes_up_to_its_limit_without_asking_then_nothing_more_while_it_is_open(world) -> None:
    p = world
    await p.say(fx_pair("EURUSD", Side.LONG, 1.1620, 1.1600, 1.1660).model_copy(update={"risk_r": 2.5}),
                CHALLENGE2, "EUR LONG 2.5R")                   # 2.5R > risk.max_r_per_trade: was asked before
    assert p.asked() == []
    assert orders(p.fx2) == [("BUY", "EURUSD", 12.19, 1.16205, 1.16, 1.166)]
    (report,) = p.reports()
    assert "• fx2: 12.19 EURUSD (açık), risk 2 500.00 USD" in report
    await p.say(fx_pair("GBPUSD", Side.LONG, 1.3150, 1.3130, 1.3190), CHALLENGE2, "GBP LONG 1R")
    assert len(orders(p.fx2)) == 1
    assert any("açık ve bekleyen pozisyonların stoptaki riski 2 500.00 + bu işlemin riski 1 000.00 = 3 500.00 USD"
               in t for t in errors(p))


async def test_e_fx_refuses_a_misread_5r_and_takes_3r_its_whole_daily_limit(world) -> None:
    p = world
    await p.say(fx_pair("GBPUSD", Side.LONG, 1.3150, 1.3130, 1.3190).model_copy(update={"risk_r": 5.0}), FX,
                "GBP LONG 5R")
    assert p.asked() == [] and p.fx_orders() == [] and p.tickmill_orders() == []
    (text,) = [t for t in errors(p) if t.startswith("🚨 <b>#1")]
    assert "• fx: günlük zarar sınırı (%3 = 300.00 USD) aşılırdı" in text and "bu işlemin riski 500.00" in text
    assert "• tickmill: <code>Yetersiz teminat" in text      # (tickmill has no daily limit: its margin stopped it)
    await p.say(fx_pair("GBPUSD", Side.LONG, 1.3150, 1.3130, 1.3190).model_copy(update={"risk_r": 3.0}), FX,
                "GBP LONG 3R")
    assert p.asked() == [] and p.fx_orders() == [("BUY", "GBPUSD", 1.46, 1.31505, 1.313, 1.319)]


async def test_e_binance_has_no_daily_limit_a_misread_5r_opens_there_at_once(world) -> None:
    p = world
    await p.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0).model_copy(update={"risk_r": 5.0}), KRIPTO,
                "BTC LONG 5R")
    (conf,) = p.notifier.confirmations                        # (asked for breakout only)
    assert p.asked() == [["risk_r_too_high"]] and conf.early is not None
    assert [(o["type"], o["amount"]) for o in p.binance.entry_orders()] == [("market", 0.064)]
    (report,) = p.reports()
    assert "• binance: 0.064 BTC/USDT:USDT (açık), risk 50.00 USDT" in report      # 5 x 10 USDT
    assert p.binance.balance["USDT"]["total"] == 46.90                             # the replica's whole balance


# =================================================================================================================
# (f) no target
# =================================================================================================================
async def test_f_no_target_opens_on_fx_tickmill_and_fx2_at_once_and_tp_1_1030_reaches_every_position(world) -> None:
    p = world
    eur_at(p, 1.09995, 1.10005, tickmill=0.00010)
    mid = await p.say(no_tp(), FX, "EUR LONG")
    mid2 = await p.say(no_tp(), CHALLENGE2, "EUR LONG")
    assert p.asked() == []
    assert p.fx_orders() == [("BUY", "EURUSD", 0.48, 1.10005, 1.098, 0.0)]           # tp 0.0: none at MetaTrader
    assert p.tickmill_orders() == [("market_buy", "EURUSD", 0.25, 1.098, None)]
    assert orders(p.fx2) == [("BUY", "EURUSD", 4.87, 1.10005, 1.098, 0.0)]
    first, second = p.reports()
    assert "🎯 TP: ❓ <i>yok</i>" in first and first.endswith(ALONE) and second.endswith(ALONE)
    p.notifier.notifications.clear()
    # the trader's target, in the fx topic: fx and tickmill (one trade); fx2's trade is the other topic's
    await p.say(move_tp(mid, 1.1030), FX, "TP 1.1030", reply_to=mid)
    assert p.asked() == []
    (fx_pos,) = list(p.terminal.positions)
    assert level_changes(p.terminal) == [("position", fx_pos, 1.098, 1.103)]
    assert [c[-1] for c in tickmill_changes(p)] == [1.103]
    assert level_changes(p.fx2) == []
    (applied,) = p.texts()
    assert applied.startswith("✅ #1 EUR LONG: uygulandı") and "• tickmill: TP 1.103" in applied
    assert "• fx: TP 1.103" in applied
    await p.say(move_tp(mid2, 1.1030), CHALLENGE2, "TP 1.1030", reply_to=mid2)
    (fx2_pos,) = list(p.fx2.positions)                                 # type: ignore[attr-defined]
    assert level_changes(p.fx2) == [("position", fx2_pos, 1.098, 1.103)]
    for trade_id in (1, 2):
        assert {n: [tp.price for tp in r.take_profits] for n, r in (await p.positions(trade_id)).items()} == (
            {"fx": [1.103], "tickmill": [1.103]} if trade_id == 1 else {"fx2": [1.103]})


async def test_f_no_target_limit_tickmills_reference_entry_takes_the_new_target_and_opens_with_it(world) -> None:
    p = world
    eur_at(p, 1.09995, 1.10005, tickmill=0.00010)
    mid = await p.say(no_tp(EntryType.LIMIT, 1.0990), FX, "EUR LONG limit")
    assert p.asked() == []
    assert p.fx_orders() == [("BUY_LIMIT", "EURUSD", 1.0, 1.099, 1.098, 0.0)]
    tm = (await p.positions(1))["tickmill"]
    assert is_virtual_pending(tm) and tm.data["reference_broker"] == "fx" and tm.take_profits == []
    for _ in range(2):
        await p.engine.monitor.run_once()
        await p.engine.monitor.run_virtual_once()
    await p.say(move_tp(mid, 1.1030), FX, "TP 1.1030", reply_to=mid)
    (ticket,) = list(p.terminal.orders)
    assert level_changes(p.terminal) == [("order", ticket, 1.098, 1.103)]           # fx's waiting order
    tm = (await p.positions(1))["tickmill"]
    assert is_virtual_pending(tm) and [tp.price for tp in tm.take_profits] == [1.103]
    assert tm.data["virtual_tp_armed"] is False             # prices before the new target never count for it
    assert p.tickmill_orders() == []
    eur_at(p, 1.09895, 1.09905, tickmill=0.00010)
    p.terminal.fill(ticket, 1.0990)
    for _ in range(2):
        await p.engine.monitor.run_once()
        await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()
    assert order[:3] == ("market_buy", "EURUSD", 0.5)
    assert order[-2:] == (pytest.approx(1.0981), pytest.approx(1.1031))             # + the offset at the fill


async def test_f_no_target_kripto_limit_binance_tickmills_virtual_entry_and_breakout_after_yes(world) -> None:
    p = world
    mid = await p.say(btc(EntryType.LIMIT, 73500.0, 73000.0, 75000.0).model_copy(
        update={"take_profits": [], "confidence": 0.75}), KRIPTO, "BTC LONG limit")
    (conf,) = p.notifier.confirmations
    assert p.asked() == [["no_take_profit", "low_confidence"]]
    assert conf.early is not None and conf.early["brokers"] == ["binance", "tickmill"]
    assert len(p.binance.entry_orders()) == 1 and p.tickmill_orders() == []
    await p.say(move_tp(mid, 76000.0), KRIPTO, "TP 76000", reply_to=mid)
    assert len(p.notifier.confirmations) == 1
    rows = await p.positions(1)
    assert {n: [tp.price for tp in r.take_profits] for n, r in rows.items()} == {
        "binance": [76000.0], "tickmill": [76000.0]}
    # Binance touches the entry: tickmill opens with the target (shifted by its offset, -50)
    p.binance.set_price("BTC", 73499.0, 73501.0)
    p.tickmill.set_price("BTCUSD", 73449.0, 73451.0)
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == [("market_buy", "BTCUSD", 0.1, 72950.0, 75950.0)]
    # binance's limit fills: its target order goes out with the new target
    (entry,) = p.binance.entry_orders()
    p.binance.fill(entry["id"], price=73500.0)
    await p.engine.monitor.run_once()
    assert [o["params"]["takeProfitPrice"] for o in p.binance.trigger_orders("takeProfitPrice")] == [76000.0]
    # "Evet" for breakout: its instruction carries the target; nothing is sent twice
    p.notifier.notifications.clear()
    await p.engine.handle_confirmation(conf.id, True)
    (note,) = [t for t in p.texts() if "Elle işlem (breakout)" in t]
    assert "TP: 76000" in note
    assert (len(p.binance.entry_orders()), len(p.tickmill_orders())) == (1, 1)


async def test_f_no_target_kripto_market_binance_gets_one_target_order_later_tickmill_its_shifted_target(
    world
) -> None:
    p = world
    mid = await p.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0).model_copy(
        update={"take_profits": [], "confidence": 0.75}), KRIPTO, "BTC LONG")
    (conf,) = p.notifier.confirmations
    assert p.asked() == [["no_take_profit", "low_confidence"]]
    assert conf.early is not None and conf.early["brokers"] == ["binance", "tickmill"]
    assert [(o["type"], o["amount"]) for o in p.binance.entry_orders()] == [("market", 0.012)]
    assert [o["params"]["stopLossPrice"] for o in p.binance.trigger_orders("stopLossPrice")] == [73300.0]
    assert p.binance.trigger_orders("takeProfitPrice") == []                        # no target order
    assert p.tickmill_orders() == [("market_buy", "BTCUSD", 0.08, 73250.0, None)]   # (offset -50)
    await p.say(move_tp(mid, 76000.0), KRIPTO, "TP 76000", reply_to=mid)
    assert len(p.notifier.confirmations) == 1
    assert [(o["amount"], o["params"]["takeProfitPrice"]) for o in p.binance.trigger_orders("takeProfitPrice")] == [
        (0.012, 76000.0)]                                                          # the whole position, once
    assert len(p.binance.trigger_orders("stopLossPrice")) == 1                      # the stop is not sent again
    assert tickmill_changes(p) == [("70001", 73250.0, 75950.0)]                     # shifted by its offset
    rows = await p.positions(1)
    assert {n: [tp.price for tp in r.take_profits] for n, r in rows.items()} == {
        "binance": [76000.0], "tickmill": [75950.0]}


async def test_f_the_real_parser_drops_an_unreadable_target_and_the_signal_opens_without_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    model = ScriptedModel()
    p = await own(tmp_path, monkeypatch, model=model)
    try:
        model.queue.append([wire_action(
            action="open", symbol="EURUSD", base_asset="EUR", quote_asset="USD", side="long", entry_type="market",
            entry_price=1.1620, stop_loss=1.1600, take_profits=[{"price": 0, "close_fraction": 1.0}], risk_r=1.0,
            confidence=0.9, summary_tr="EUR LONG")])
        await p._post(8001, FX, "EUR LONG TP ?")
        assert p.asked() == []
        assert p.fx_orders() == [("BUY", "EURUSD", 0.48, 1.16205, 1.16, 0.0)]
        assert p.tickmill_orders() == [("market_buy", "EURUSD", 0.25, 1.16, None)]
    finally:
        await p.close()


async def test_f_a_target_message_the_bot_is_unsure_about_is_still_asked(world) -> None:
    p = world
    eur_at(p, 1.09995, 1.10005)
    mid = await p.say(no_tp(), FX, "EUR LONG")
    await p.say(move_tp(mid, 1.1030, confidence=0.75), FX, "TP 1.1030?", reply_to=mid)
    (conf,) = p.notifier.confirmations                        # (confirm_open is about opening only)
    assert conf.kind == "manage" and p.asked() == [["low_confidence"]]
    assert level_changes(p.terminal) == [] and tickmill_changes(p) == []
    await p.engine.handle_confirmation(conf.id, True)
    assert [c[-1] for c in level_changes(p.terminal)] == [1.103] and [c[-1] for c in tickmill_changes(p)] == [1.103]
