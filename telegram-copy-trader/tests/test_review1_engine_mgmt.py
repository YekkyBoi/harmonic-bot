"""Review 1 (lane engine_mgmt): trade management findings of the review of release 62e0719.

The production replica of tests/test_preflight_scenarios (REAL Engine, Monitor, validator and the CcxtBroker,
ManualBroker, Mt5Broker and MetaApiBroker adapters on the fake venues), loaded with the server's config of today
(PROD_NOW_YAML below: kripto -> binance, breakout, tickmill; fx -> fx, tickmill; notify_on_execution; fx daily loss
3 % with open risk; tickmill optional with a virtual reference-touch entry).

  M1  a close / cancel that fails on one broker is remembered on the position and retried by the monitor; the
      trade counts as ended for the next signal; a close that went through unseen is never sent twice
  M2  a management message that ends the trade gives the same closing summary (realized result of every
      position) as the monitor
  M3  a waiting virtual entry ends with the real positions of its trade and never outlives pending_entry_expiry_min
  M4  the daily-loss refusal text names the exact excess
  M5  the reply context of each action is its own message's reply, not the newest reply of the batch
  M6  an edited signal is compared with its own levels, not with a stop moved since
  M7  "entry stop attık" while entries are still resting cancels them (and moves the stop of the filled ones)
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import ccxt
import pytest
from ccxt.base import errors as cx

import test_preflight_scenarios as tps
from test_preflight_scenarios import KRIPTO, Prod, _isolate, btc, signal  # noqa: F401  (_isolate: autouse)

import copytrader.brokers.metaapi_broker as mab
from copytrader.engine.monitor import CLOSE_WANTED
from copytrader.models import ActionType, EntryType, MarketType, ParsedAction, PositionStatus, Severity, Side, TradeStatus

# The owner's config.yaml as it is on the server now (secrets as ${VAR}; tps.DUMMY_ENV gives dummy values).
PROD_NOW_YAML = """\
dry_run: false
data_dir: data
timezone: Europe/Istanbul
telegram:
  api_id: ${TG_API_ID}
  api_hash: ${TG_API_HASH}
  source_chats: [-1003782294463]
  topics:
    - name: kripto
      title_contains: ["Kripto İşlemleri"]
      brokers: [binance, breakout, tickmill]
      kind: crypto
      explicit_market_drift: follow
    - name: fx
      title_contains: ["Fx İşlemleri"]
      brokers: [fx, tickmill]
      kind: fx
notifier:
  bot_token: ${TG_BOT_TOKEN}
  owner_user_id: ${TG_OWNER_ID}
  notify_on_execution: true
  notify_on_ignored: true
llm:
  api_key: ${ANTHROPIC_API_KEY}
brokers:
  - name: binance
    type: ccxt
    exchange: binanceusdm
    api_key: ${BINANCE_API_KEY}
    secret: ${BINANCE_API_SECRET}
    risk_usd: 10
    margin_mode: cross
    leverage: 125
    max_leverage: 125
    daily_loss_limit_pct: 0
    no_hedge: true
  - name: breakout
    type: manual
    risk_usd: 50
    allowed_assets: [BTC, ETH, SOL, XRP, DOGE, ADA, AVAX, LINK, BNB, LTC, SUI, TON, DOT, NEAR, APT, ARB, OP, PEPE, WIF, HYPE]
  - name: fx
    type: mt5
    login: ${MT5_LOGIN}
    password: ${MT5_PASSWORD}
    server: ${MT5_SERVER}
    risk_usd: 100
    no_hedge: true
    daily_loss_limit_pct: 3
    daily_loss_include_open_risk: true
  - name: tickmill
    type: metaapi
    enabled: true
    optional: true
    token: ${METAAPI_TOKEN:-}
    account_id: ${METAAPI_ACCOUNT_ID:-}
    risk_usd: 45
    reference_exchange: binanceusdm
    offset_mode: additive
    max_offset_pct: 1.0
    entry_trigger: reference_touch
    virtual_trigger_max_hours: 48
    symbol_map:
      BTC: BTCUSD
      ETH: ETHUSD
    allowed_assets: [EUR, GBP, USD, AUD, NZD, BTC, ETH, XAU, XAG]
    no_hedge: true
    daily_loss_limit_pct: 0
risk:
  max_open_trades: 25
  allow_min_qty_round_up: true
"""


@pytest.fixture
async def prod(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(tps, "PROD_YAML", PROD_NOW_YAML)
    p = await Prod.create(tmp_path, monkeypatch)
    by = {b.name: b for b in p.cfg.brokers}
    assert (by["binance"].no_hedge, by["tickmill"].optional, p.cfg.notifier.notify_on_execution) == (True, True, True)
    yield p
    await p.close()


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def btc_long() -> ParsedAction:
    return btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0)


def btc_short() -> ParsedAction:
    return signal("BTCUSDT.P", "BTC", "USDT", MarketType.PERP, Side.SHORT, 74000.0, 74700.0, 72500.0,
                  entry_type=EntryType.MARKET)


def manage(action: ActionType, trade_id: int | None, sig: int | None, base: str | None = "BTC",
           **kw: Any) -> ParsedAction:
    return ParsedAction(action=action, target_trade_id=trade_id, refers_to_message_id=sig, base_asset=base,
                        confidence=0.95, summary_tr=kw.pop("summary_tr", action.value), **kw)


def close_all_of(trade_id: int, sig: int) -> ParsedAction:
    return manage(ActionType.CLOSE, trade_id, sig, close_fraction=1.0, summary_tr="pozisyonu kapat")


def is_close_order(method: str, args: tuple[Any, ...]) -> bool:
    """The reduce-only MARKET order of close_position (not a stop / take profit)."""
    if method != "create_order":
        return False
    params = args[5] or {}
    return (args[1] == "market" and bool(params.get("reduceOnly")) and not params.get("stopLossPrice")
            and not params.get("takeProfitPrice"))


def close_orders(p: Prod) -> int:
    return sum(1 for m, a in p.binance.calls if is_close_order(m, a))


def real_ccxt_error(body: str, status: int) -> cx.BaseError:
    """The exception REAL ccxt (binanceusdm) raises for this answer of POST /fapi/v1/order (its handle_errors,
    then its HTTP status mapping, in the order ccxt's fetch() calls them). No network."""
    ex = ccxt.binanceusdm()
    url = "https://fapi.binance.com/fapi/v1/order"
    try:
        ex.handle_errors(status, "x", url, "POST", {}, body, json.loads(body), {}, "")
        ex.handle_http_status_code(status, "x", url, "POST", body)
    except cx.BaseError as exc:
        return exc
    raise AssertionError("ccxt raised nothing")


async def statuses(p: Prod, trade_id: int) -> dict[str, str]:
    return {b: pos.status.value for b, pos in (await p.positions(trade_id)).items()}


async def passes(p: Prod, n: int) -> None:
    for _ in range(n):
        await p.engine.monitor.run_once()


# =========================================================================== #
# M1 (YONETIM-1): a failed close is remembered and retried
# =========================================================================== #
async def test_m1_binance_close_refused_once_is_retried_by_the_monitor_and_the_owner_is_told_twice(
        prod: Prod) -> None:
    """Reviewer's test_y10 case 2 / verifier's case A: one transient Binance refusal (-1001) of the reduce-only
    close on "2R kapattık". 62e0719: never retried, the owner's Binance stays long."""
    sig = await prod.say(btc_long(), KRIPTO, "btc 1R long piyasadan")
    prod.binance.fail_when(is_close_order, real_ccxt_error(
        '{"code":-1001,"msg":"Internal error; unable to process your request. Please try again."}', 500))
    prod.notifier.notifications.clear()
    await prod.say(close_all_of(1, sig), KRIPTO, "2R kapattık", reply_to=sig)
    # Review 3b (F2): the binance adapter (R3, X2) reports -1001 as an unknown outcome (OrderOutcomeUnknownError: the
    # order may exist), so the engine no longer calls it "kapatılamadı"; the retry is unchanged.
    told = [t for t in prod.texts(Severity.ERROR) if "binance" in t and "kapatma emrinin sonucu alınamadı" in t]
    assert len(told) == 1 and "her kontrolde okuyup gerekirse yeniden deneyecek" in told[0], prod.texts()
    stored = (await prod.positions(1))["binance"]
    assert stored.status is PositionStatus.OPEN and stored.data[CLOSE_WANTED]["attempts"] == 1
    assert prod.binance.net.get("BTC/USDT:USDT") == pytest.approx(0.012)
    prod.notifier.notifications.clear()
    await passes(prod, 3)
    (trade,) = await prod.trades()
    assert prod.binance.net.get("BTC/USDT:USDT") == 0.0                 # flat like the trader
    assert await statuses(prod, 1) == {"binance": "closed", "breakout": "closed", "tickmill": "closed"}
    assert trade.status is TradeStatus.CLOSED
    assert close_orders(prod) == 2                                      # refused once, retried once
    assert CLOSE_WANTED not in (await prod.positions(1))["binance"].data
    # (Review 3b, F2: the end of a close whose outcome was unknown is worded from what is known now)
    worked = [t for t in prod.texts() if "binance" in t and "kapatma emrinin sonucu alınamamıştı; pozisyon artık "
              "kapalı" in t]
    assert len(worked) == 1, prod.texts()
    assert not any("kapatılamadı" in t for t in prod.texts())          # the retries themselves are quiet


async def test_m1_a_close_refused_again_is_retried_with_back_off_until_it_works(prod: Prod) -> None:
    sig = await prod.say(btc_long(), KRIPTO, "btc 1R long piyasadan")
    refusal = real_ccxt_error('{"code":-1001,"msg":"Internal error; unable to process your request."}', 500)
    prod.binance.fail_when(is_close_order, refusal, times=3)
    await prod.say(close_all_of(1, sig), KRIPTO, "kapattık", reply_to=sig)
    prod.notifier.notifications.clear()
    await passes(prod, 1)                                               # retry 1 (due at once): refused
    wish = (await prod.positions(1))["binance"].data[CLOSE_WANTED]
    assert wish["attempts"] == 2 and close_orders(prod) == 2
    assert datetime.fromisoformat(wish["next_at"]) > prod.engine._now() + timedelta(seconds=20)   # back-off
    await passes(prod, 2)                                               # not due yet: nothing sent
    assert close_orders(prod) == 2
    wish["next_at"] = (prod.engine._now() - timedelta(seconds=1)).isoformat()      # time passes
    pos = (await prod.positions(1))["binance"]
    pos.data[CLOSE_WANTED] = wish
    await prod.db.save_broker_position(1, pos)
    await passes(prod, 1)                                               # retry 2: refused (3rd refusal)
    assert close_orders(prod) == 3
    wish = (await prod.positions(1))["binance"].data[CLOSE_WANTED]
    wish["next_at"] = (prod.engine._now() - timedelta(seconds=1)).isoformat()
    pos = (await prod.positions(1))["binance"]
    pos.data[CLOSE_WANTED] = wish
    await prod.db.save_broker_position(1, pos)
    await passes(prod, 1)                                               # retry 3: works
    assert close_orders(prod) == 4 and prod.binance.net.get("BTC/USDT:USDT") == 0.0
    assert (await prod.trades())[0].status is TradeStatus.CLOSED
    assert not any("kapatılamadı" in t for t in prod.texts()), prod.texts()
    # (Review 3b, F2: -1001 is an unknown outcome since R3: the end is worded from what is known now)
    assert sum("kapatma emrinin sonucu alınamamıştı; pozisyon artık kapalı" in t for t in prod.texts()) == 1


@pytest.mark.parametrize("outage", ["no_answer", "reconnecting"])
async def test_m1_tickmill_down_during_kapattik_is_closed_when_it_answers_and_the_next_short_is_not_asked(
        prod: Prod, monkeypatch: pytest.MonkeyPatch, outage: str) -> None:
    """Reviewer's test_y10 case 1 / verifier's case B: the friend's MetaApi does not answer (or is rebuilding its
    connection) while "kapattık" is processed, then answers again. 62e0719: the friend's copy stays open, the trade
    stays active and the trader's next BTC short is turned into a question (binance does not open it)."""
    sig = await prod.say(btc_long(), KRIPTO, "btc 1R long piyasadan")
    tm = prod.brokers["tickmill"]
    saved: dict[str, Any] = {}
    if outage == "no_answer":
        monkeypatch.setattr(mab, "READ_TIMEOUT_SEC", 0.5)
        monkeypatch.setattr(mab, "RPC_TIMEOUT_SEC", 1.0)

        async def forever(*_a: Any, **_k: Any) -> Any:
            import asyncio
            await asyncio.sleep(3600)
        for name in ("get_positions", "get_orders", "close_position"):
            saved[name] = getattr(prod.tickmill, name)
            setattr(prod.tickmill, name, forever)
    else:
        tm._reconnecting = True
    prod.notifier.notifications.clear()
    await prod.say(close_all_of(1, sig), KRIPTO, "kapattık", reply_to=sig)
    told = [t for t in prod.texts(Severity.ERROR) if "tickmill" in t and "kapatılamadı" in t]
    assert told and "bot her kontrolde yeniden deneyecek" in told[0], prod.texts()
    assert await statuses(prod, 1) == {"binance": "closed", "breakout": "closed", "tickmill": "open"}
    for name, fn in saved.items():                                     # MetaApi answers again
        setattr(prod.tickmill, name, fn)
    tm._reconnecting = False
    prod.notifier.notifications.clear()
    await passes(prod, 3)
    assert prod.tickmill.positions == {}                               # the friend's copy follows the trader's exit
    assert (await prod.trades())[0].status is TradeStatus.CLOSED
    assert sum("tickmill" in t and "yeniden denemede kapatıldı" in t for t in prod.texts()) == 1, prod.texts()
    await prod.say(btc_short(), KRIPTO, "btc 1R short piyasadan")
    assert prod.asked() == []                                          # the owner's next signal is not held back
    assert prod.binance.net.get("BTC/USDT:USDT") == pytest.approx(-0.012)
    assert {b: p.status for b, p in (await prod.positions(2)).items()} == {
        "binance": PositionStatus.OPEN, "breakout": PositionStatus.OPEN, "tickmill": PositionStatus.OPEN}


@pytest.mark.parametrize("stuck", ["binance_refuses", "tickmill_rebuilding"])
async def test_m1_while_a_close_is_still_owed_the_trade_counts_as_ended_for_the_next_signal(
        prod: Prod, stuck: str) -> None:
    """The close keeps failing on one broker (binance keeps refusing it / the friend's MetaApi keeps rebuilding its
    connection): its long is still open and the bot keeps trying. The trader's next BTC short is no question
    (62e0719: opposite_trade_open, the owner's tap needed). The brokers that are flat open it at once; binance,
    which still holds the long, skips it by its own no_hedge rule (tickmill is not reachable at all). A plain "BTC
    stop girişe" then finds the new trade without a question: the closing one is not a candidate any more."""
    sig = await prod.say(btc_long(), KRIPTO, "btc 1R long piyasadan")
    if stuck == "binance_refuses":
        prod.binance.fail_when(is_close_order, real_ccxt_error(
            '{"code":-1001,"msg":"Internal error; unable to process your request."}', 500), times=50)
    else:
        prod.brokers["tickmill"]._reconnecting = True
    held = "binance" if stuck == "binance_refuses" else "tickmill"
    await prod.say(close_all_of(1, sig), KRIPTO, "kapattık", reply_to=sig)
    await passes(prod, 2)
    prod.notifier.notifications.clear()
    await prod.say(btc_short(), KRIPTO, "btc 1R short piyasadan")
    assert prod.asked() == []
    assert (await prod.positions(1))[held].status is PositionStatus.OPEN       # still owed ...
    assert (await prod.positions(1))[held].data[CLOSE_WANTED]["attempts"] >= 1  # ... and remembered
    two = await prod.positions(2)
    assert held not in two or two[held].status not in (PositionStatus.OPEN, PositionStatus.PENDING)
    assert all(p.status is PositionStatus.OPEN for b, p in two.items() if b != held) and len(two) >= 2, two
    if held == "binance":
        assert any("binance" in t and "hedging yasağı" in t for t in prod.texts()), prod.texts()
    summaries = await prod.engine._open_trade_summaries(prod.engine._route_named("kripto"))
    assert [s["trade_id"] for s in summaries] == [2]                    # the parser sees only the live trade
    prod.binance.set_price("BTC", 73400.0, 73402.0)                    # the short is in profit
    prod.tickmill.set_price("BTCUSD", 73350.0, 73352.0)
    await prod.say(manage(ActionType.MOVE_SL, None, None, move_sl_to_entry=True, summary_tr="stop girişe"),
                   KRIPTO, "BTC stop girişe")
    assert prod.asked() == [], [c.issues for c in prod.notifier.confirmations]
    (_one, two_trade) = await prod.trades()
    assert two_trade.current_stop_loss == pytest.approx(74000.0)


async def test_m1_a_close_that_went_through_unseen_is_never_sent_twice(prod: Prod) -> None:
    """The close order executes at Binance but its answer is lost (request timeout). The monitor reads the venue
    before it tries again: flat there -> the position is booked closed, no second close order (on Binance's one-way
    account a second reduce-only order could close another trade of the same coin)."""
    sig = await prod.say(btc_long(), KRIPTO, "btc 1R long piyasadan")
    original = prod.binance.create_order
    lost = {"n": 0}

    async def lossy(symbol: str, type: str, side: str, amount: float, price: float | None = None,
                    params: dict | None = None) -> Any:
        order = await original(symbol, type, side, amount, price, params)
        if lost["n"] == 0 and is_close_order("create_order", (symbol, type, side, amount, price, params)):
            lost["n"] += 1
            raise cx.RequestTimeout("binanceusdm POST https://fapi.binance.com/fapi/v1/order 10000ms timeout")
        return order

    prod.binance.create_order = lossy                                  # type: ignore[method-assign]
    # (Repair 3: the adapter now looks a close whose answer was lost up by its client id and books it at once. This
    # test is about the monitor's guard for one it cannot look up, so here that look-up gets no answer.)
    prod.binance.fail_when(lambda m, a: m == "fetch_order" and bool((a[2] or {}).get("clientOrderId")),
                           cx.NetworkError("binanceusdm GET order connection reset"), times=1000)
    await prod.say(close_all_of(1, sig), KRIPTO, "kapattık", reply_to=sig)
    assert prod.binance.net.get("BTC/USDT:USDT") == 0.0                # it did go through
    assert (await prod.positions(1))["binance"].status is PositionStatus.OPEN
    prod.notifier.notifications.clear()
    await passes(prod, 3)
    assert close_orders(prod) == 1                                     # never a second one
    assert prod.binance.net.get("BTC/USDT:USDT") == 0.0                # not turned short
    assert (await prod.positions(1))["binance"].status is PositionStatus.CLOSED
    assert (await prod.trades())[0].status is TradeStatus.CLOSED
    assert sum("binance" in t and "artık kapalı" in t for t in prod.texts()) == 1, prod.texts()


async def test_m1_a_second_trade_of_the_same_coin_is_never_closed_by_a_retry(prod: Prod) -> None:
    """Two BTC longs on the owner's Binance (one-way account: one net position). The close of #1 executes but its
    answer is lost; the venue still holds #2's size, so it does not look flat. The bot must not send another
    reduce-only close (it would close #2): it tells the owner once and leaves #2 alone."""
    sig = await prod.say(btc_long(), KRIPTO, "btc 1R long piyasadan")
    prod.binance.set_price("BTC", 74999.0, 75001.0)
    prod.tickmill.set_price("BTCUSD", 74949.0, 74951.0)
    await prod.say(btc(EntryType.MARKET, 75000.0, 74300.0, 76500.0), KRIPTO, "btc 1R long piyasadan (2)")
    assert prod.asked() == []
    q1 = (await prod.positions(1))["binance"].remaining_quantity
    q2 = (await prod.positions(2))["binance"].remaining_quantity
    assert q1 > 0 and q2 > 0 and prod.binance.net.get("BTC/USDT:USDT") == pytest.approx(q1 + q2)
    original = prod.binance.create_order
    lost = {"n": 0}

    async def lossy(symbol: str, type: str, side: str, amount: float, price: float | None = None,
                    params: dict | None = None) -> Any:
        order = await original(symbol, type, side, amount, price, params)
        if lost["n"] == 0 and is_close_order("create_order", (symbol, type, side, amount, price, params)):
            lost["n"] += 1
            raise cx.RequestTimeout("binanceusdm POST https://fapi.binance.com/fapi/v1/order 10000ms timeout")
        return order

    prod.binance.create_order = lossy                                  # type: ignore[method-assign]
    # (Repair 3: the adapter now looks a close whose answer was lost up by its client id and books it at once. This
    # test is about the monitor's guard for one it cannot look up, so here that look-up gets no answer.)
    prod.binance.fail_when(lambda m, a: m == "fetch_order" and bool((a[2] or {}).get("clientOrderId")),
                           cx.NetworkError("binanceusdm GET order connection reset"), times=1000)
    await prod.say(close_all_of(1, sig), KRIPTO, "1. işlemi kapattık", reply_to=sig)
    assert prod.binance.net.get("BTC/USDT:USDT") == pytest.approx(q2)   # #1's size went; #2's is there
    prod.notifier.notifications.clear()
    await passes(prod, 3)
    assert close_orders(prod) == 1
    assert prod.binance.net.get("BTC/USDT:USDT") == pytest.approx(q2)   # #2 untouched
    held = [t for t in prod.texts(Severity.WARNING) if "#1 BTC LONG binance" in t and "GÖNDERMEDİ" in t]
    assert len(held) == 1, prod.texts()
    assert (await prod.positions(2))["binance"].status is PositionStatus.OPEN


# =========================================================================== #
# M2 (YONETIM-3): the closing summary when a management message ends the trade
# =========================================================================== #
def owner_texts(p: Prod) -> list[str]:
    """Everything the owner's bot shows: notices and the result texts of questions."""
    return [t for t, _s in p.notifier.notifications] + [t for _c, t in p.notifier.closed]


def reported(texts: list[str], broker: str, pnl: float | None) -> bool:
    """Does a message give this broker's realized result as the bot formats it (+17.79)?"""
    return pnl is not None and any(broker in t and f"{pnl:+.2f}" in t for t in texts)


def finals(texts: list[str]) -> list[str]:
    return [t for t in texts if t.startswith("🏁")]


@pytest.mark.parametrize("which", ["takeProfitPrice", "stopLossPrice"])
async def test_m2_binance_exit_before_the_traders_close_is_in_the_one_closing_summary(prod: Prod, which: str) -> None:
    """Binance's TP (SL) fills while the friend's shifted level is not reached; then the trader's "2R kapattık"
    ("Stop") closes the rest. 62e0719: the owner never read his Binance (and breakout) result."""
    sig = await prod.say(btc_long(), KRIPTO, "btc 1R long piyasadan")
    (order,) = prod.binance.trigger_orders(which)
    if which == "takeProfitPrice":       # Binance at its TP, the CFD (50 lower) 10 short of its shifted TP 75450
        prod.binance.set_price("BTC", 75500.0, 75502.0)
        prod.tickmill.set_price("BTCUSD", 75440.0, 75442.0)
        text = "2R kapattık"
    else:                                # Binance at its SL, the CFD 5 above its shifted SL 73250
        prod.binance.set_price("BTC", 73299.0, 73301.0)
        prod.tickmill.set_price("BTCUSD", 73255.0, 73257.0)
        text = "Stop"
    prod.binance.fill(order["id"])
    await passes(prod, 2)
    assert (await prod.positions(1))["binance"].status is PositionStatus.CLOSED
    assert (await prod.trades())[0].status is TradeStatus.OPEN
    prod.notifier.notifications.clear()
    await prod.say(close_all_of(1, sig), KRIPTO, text, reply_to=sig)
    await passes(prod, 1)
    pos = await prod.positions(1)
    texts = owner_texts(prod)
    assert (await prod.trades())[0].status is TradeStatus.CLOSED
    (summary,) = finals(texts)
    assert summary.startswith("🏁 #1 BTC LONG (giriş 74000) kapandı. Gerçekleşen K/Z: ")
    for broker in ("binance", "breakout", "tickmill"):
        assert pos[broker].realized_pnl is not None and reported([summary], broker, pos[broker].realized_pnl), (
            broker, summary)
    # it comes after the management report
    assert texts.index(summary) > next(i for i, t in enumerate(texts) if "#1 BTC LONG: uygulandı" in t)


async def test_m2_prop_account_stop_before_the_traders_stop_is_in_the_closing_summary(prod: Prod) -> None:
    from copytrader.brokers.mt5_broker import MT5
    from test_preflight_scenarios import FX, fx_pair, set_eurusd
    set_eurusd(prod, 1.10872, 1.10874)
    sig = await prod.say(fx_pair("EURUSD", Side.SHORT, 1.10872, 1.11005, 1.10654, entry_type=EntryType.MARKET), FX,
                         "Eu short")
    (ticket,) = list(prod.terminal.positions)
    prod.terminal.set_price("EURUSD", 1.11003, 1.11005)
    prod.tickmill.set_price("EURUSD", 1.11001, 1.11003)
    prod.terminal.hit(ticket, MT5.DEAL_REASON_SL, 1.11005)
    await passes(prod, 1)
    prod.notifier.notifications.clear()
    await prod.say(manage(ActionType.CLOSE, 1, sig, "EUR", close_fraction=1.0, summary_tr="stop oldu"), FX, "Stop",
                   reply_to=sig)
    pos = await prod.positions(1)
    (summary,) = finals(owner_texts(prod))
    assert reported([summary], "fx", pos["fx"].realized_pnl), summary
    assert pos["fx"].realized_pnl < 0


async def test_m2_control_every_copy_exits_at_its_venue_one_summary_only(prod: Prod) -> None:
    sig = await prod.say(btc_long(), KRIPTO, "btc 1R long piyasadan")
    (order,) = prod.binance.trigger_orders("takeProfitPrice")
    prod.binance.set_price("BTC", 75500.0, 75502.0)
    prod.tickmill.set_price("BTCUSD", 75440.0, 75442.0)
    prod.binance.fill(order["id"])
    await passes(prod, 1)
    (pid,) = list(prod.tickmill.positions)
    prod.tickmill.hit(pid, "DEAL_REASON_TP", 75450.0)
    await passes(prod, 2)                       # the monitor ends the trade: its summary
    await prod.say(close_all_of(1, sig), KRIPTO, "2R kapattık", reply_to=sig)     # ignored: already over
    await passes(prod, 1)
    texts = owner_texts(prod)
    pos = await prod.positions(1)
    assert len(finals(texts)) == 1, finals(texts)
    assert all(reported(finals(texts), b, pos[b].realized_pnl) for b in ("binance", "breakout", "tickmill"))


async def test_m2_close_all_reply_carries_the_summary_and_no_second_one_is_sent(prod: Prod) -> None:
    await prod.say(btc_long(), KRIPTO, "btc 1R long piyasadan")
    (order,) = prod.binance.trigger_orders("takeProfitPrice")
    prod.binance.set_price("BTC", 75500.0, 75502.0)
    prod.tickmill.set_price("BTCUSD", 75440.0, 75442.0)
    prod.binance.fill(order["id"])
    await passes(prod, 2)
    prod.notifier.notifications.clear()
    reply = await prod.engine.close_all()                          # /hepsini_kapat
    pos = await prod.positions(1)
    assert "🏁 #1 BTC LONG (giriş 74000) kapandı." in reply
    assert reported([reply], "binance", pos["binance"].realized_pnl)
    await passes(prod, 1)
    assert finals(owner_texts(prod)) == []                          # the reply is the only one


async def test_m2_a_late_answer_of_the_friends_broker_that_ends_the_trade_gives_the_summary(
        prod: Prod, monkeypatch: pytest.MonkeyPatch) -> None:
    """The friend's MetaApi answers the close only after the flow's grace: its late result ends the trade, and the
    owner then gets the closing summary with every result (binance's TP included)."""
    import asyncio

    from copytrader.engine import orchestrator as orch
    monkeypatch.setattr(orch, "OPTIONAL_LEG_GRACE_SEC", 0.2)
    sig = await prod.say(btc_long(), KRIPTO, "btc 1R long piyasadan")
    (order,) = prod.binance.trigger_orders("takeProfitPrice")
    prod.binance.set_price("BTC", 75500.0, 75502.0)
    prod.tickmill.set_price("BTCUSD", 75440.0, 75442.0)
    prod.binance.fill(order["id"])
    await passes(prod, 2)
    gate = asyncio.Event()
    original = prod.tickmill.close_position

    async def slow_close(*a: Any, **k: Any) -> Any:
        await gate.wait()
        return await original(*a, **k)

    prod.tickmill.close_position = slow_close                      # type: ignore[method-assign]
    prod.notifier.notifications.clear()
    await prod.say(close_all_of(1, sig), KRIPTO, "2R kapattık", reply_to=sig)
    assert (await prod.trades())[0].status is TradeStatus.OPEN    # tickmill has not answered yet
    assert finals(owner_texts(prod)) == []
    gate.set()
    for _ in range(100):
        if finals(owner_texts(prod)):
            break
        await asyncio.sleep(0.05)
    await passes(prod, 1)
    texts = owner_texts(prod)
    pos = await prod.positions(1)
    (summary,) = finals(texts)
    assert (await prod.trades())[0].status is TradeStatus.CLOSED
    assert all(reported([summary], b, pos[b].realized_pnl) for b in ("binance", "breakout", "tickmill")), summary
    late = next(i for i, t in enumerate(texts) if "tickmill geç yanıt verdi" in t)
    assert texts.index(summary) > late


# =========================================================================== #
# M3 (ACILIS-4): a waiting virtual entry ends with the owner's limits
# =========================================================================== #
def at(p: Prod, when: datetime) -> None:
    p.engine._clock = lambda: when
    p.engine.monitor._clock = lambda: when


async def btc_limit(p: Prod) -> None:
    await p.say(btc(EntryType.LIMIT, 73000.0, 72300.0, 75000.0), KRIPTO, "btc 1R long limit emir")
    assert {b: x.status for b, x in (await p.positions(1)).items()} == {
        "binance": PositionStatus.PENDING, "breakout": PositionStatus.PENDING, "tickmill": PositionStatus.PENDING}


async def test_m3_virtual_entry_lives_no_longer_than_the_owners_limits(prod: Prod) -> None:
    """Reviewer's test_i_virtual_outlives: binance's and breakout's limits expire after pending_entry_expiry_min
    (1440 min); 62e0719 kept tickmill's virtual entry 48 h and opened the trade on the friend's account alone when
    Binance touched the level at +30 h."""
    assert prod.cfg.risk.pending_entry_expiry_min == 1440
    await btc_limit(prod)
    registered = [t for t in owner_texts(prod) if "sanal tetik kuruldu" in t]
    prod.notifier.notifications.clear()
    at(prod, datetime.now(UTC) + timedelta(hours=25))
    await passes(prod, 1)
    assert {b: x.status for b, x in (await prod.positions(1)).items()} == {
        "binance": PositionStatus.CANCELLED, "breakout": PositionStatus.CANCELLED, "tickmill": PositionStatus.CANCELLED}
    ended = [t for t in owner_texts(prod) if "tickmill" in t and "🧭" in t]
    assert len(ended) == 1 and "sanal tetik 24 saatte tetiklenmedi, iptal edildi" in ended[0], owner_texts(prod)
    at(prod, datetime.now(UTC) + timedelta(hours=30))                  # Binance touches 73000
    prod.binance.set_price("BTC", 72989.0, 72991.0)
    prod.tickmill.set_price("BTCUSD", 72939.0, 72941.0)
    await prod.engine.monitor.run_virtual_once()
    assert prod.tickmill_orders() == []                                # the friend's account takes nothing
    assert (await prod.trades())[0].status is TradeStatus.CANCELLED
    assert registered and "en çok 24 saat beklenir" in registered[0], registered   # the owner was told the limit


async def test_m3_virtual_entry_is_cancelled_when_the_owners_entries_end_without_a_fill(prod: Prod) -> None:
    """The real limits end unfilled before the virtual entry's own time is up (here: expired early, the limit of a
    shorter pending_entry_expiry_min after the signal): the waiting virtual entry ends with them, one notice."""
    await btc_limit(prod)
    for broker in ("binance", "breakout"):               # their limits ended unfilled (expired / cancelled)
        pos = (await prod.positions(1))[broker]
        await prod.db.save_broker_position(1, pos.model_copy(update={"status": PositionStatus.CANCELLED}))
    prod.notifier.notifications.clear()
    await prod.engine.monitor.run_virtual_once()
    assert (await prod.positions(1))["tickmill"].status is PositionStatus.CANCELLED
    (notice,) = [t for t in owner_texts(prod) if "🧭" in t]
    assert "tickmill" in notice and "dolmadan sona erdi" in notice and "AÇILMADI" in notice
    prod.binance.set_price("BTC", 72989.0, 72991.0)
    prod.tickmill.set_price("BTCUSD", 72939.0, 72941.0)
    await prod.engine.monitor.run_virtual_once()
    assert prod.tickmill_orders() == []


async def test_m3_virtual_fire_headline_names_the_account(prod: Prod) -> None:
    await btc_limit(prod)
    prod.notifier.notifications.clear()
    prod.binance.set_price("BTC", 72989.0, 72991.0)
    prod.tickmill.set_price("BTCUSD", 72939.0, 72941.0)
    await prod.engine.monitor.run_virtual_once()
    (fired,) = [t for t in owner_texts(prod) if "sanal tetik tetiklendi" in t]
    assert fired.startswith("✅ <b>#1 BTC LONG tickmill hesabında açıldı</b> — sanal tetik tetiklendi"), fired


# =========================================================================== #
# M4 (ACILIS-3): the daily-loss refusal text names the exact excess
# =========================================================================== #
FX_PAIRS = [
    ("EURUSD", Side.LONG, 1.1620, 1.1600, 1.1660),
    ("GBPUSD", Side.LONG, 1.3150, 1.3120, 1.3210),
    ("USDJPY", Side.SHORT, 154.20, 154.50, 153.60),
    ("AUDUSD", Side.LONG, 0.6520, 0.6500, 0.6560),
]


@pytest.mark.parametrize(("equity", "opened", "limit", "total", "excess"), [
    (9_999.99, 2, "299.9997", "300.0000", "0.0003"),     # 62e0719: "%3 = 300.00 USD ... = 300.00 USD"
    (10_000.0, 3, "300.00", "400.00", "100.00"),
    (9_900.0, 2, "297.00", "300.00", "3.00"),
])
async def test_m4_daily_loss_refusal_never_shows_the_limit_and_the_total_as_equal(
        prod: Prod, equity: float, opened: int, limit: str, total: str, excess: str) -> None:
    """FundingPips 10k, risk_usd 100, 3 % daily loss with open risk: the rule is unchanged (2 or 3 concurrent 1R
    trades fit), only the text tells the truth to the cent."""
    from test_preflight_scenarios import FX, fx_pair
    prod.terminal.account.equity = equity
    taken = []
    for pair in FX_PAIRS:
        before = len(prod.fx_orders())
        await prod.say(fx_pair(*pair), FX, pair[0])
        taken.append(len(prod.fx_orders()) > before)
    assert taken == [True] * opened + [False] * (len(FX_PAIRS) - opened)        # behaviour as before
    (refusal, *_rest) = [t for t in prod.texts(Severity.WARNING) if "günlük zarar sınırı" in t]
    assert f"(%3 = {limit} USD) aşılırdı" in refusal, refusal
    assert f"= {total} USD, yani sınırı {excess} USD aşardı; yeni işlem açılmadı" in refusal, refusal
    assert limit != total


# =========================================================================== #
# M5 (YONETIM-4): the reply context of each action of a batch
# =========================================================================== #
async def two_trades(p: Prod) -> tuple[int, int]:
    from test_preflight_scenarios import CHAT, TITLES, TRADER  # noqa: F401
    s_btc = await p.say(btc_long(), KRIPTO, "btc 1R long")
    eth = signal("ETHUSDT.P", "ETH", "USDT", MarketType.PERP, Side.LONG, 2232.0, 2200.0, 2300.0,
                 entry_type=EntryType.MARKET)
    s_eth = await p.say(eth, KRIPTO, "eth 1R long")
    p.binance.set_price("BTC", 74700.0, 74702.0)                       # BTC 1R in profit: its stop may go to the entry
    p.tickmill.set_price("BTCUSD", 74650.0, 74652.0)
    p.notifier.notifications.clear()
    return s_btc, s_eth


async def batch(p: Prod, posts: list[tuple[int, str, int | None]], actions: list[ParsedAction]) -> None:
    """The trader's messages ``posts`` = (id, text, replied-to id or None) arrive within one debounce and are read
    in one batch as ``actions``."""
    from test_preflight_scenarios import CHAT, TITLES, TRADER
    from copytrader.models import ParseResult, StoredMessage
    for mid, text, target in posts:
        await p.db.save_message(StoredMessage(
            chat_id=CHAT, message_id=mid, sender_id=TRADER, sender_name="Hoca", date=datetime.now(UTC), text=text,
            reply_to_message_id=target if target is not None else KRIPTO, topic_id=KRIPTO,
            topic_title=TITLES[KRIPTO]))
    p.parser.push(ParseResult(model="fake", actions=actions))
    await p.engine.on_messages_ready(CHAT, tps.TRADER, KRIPTO)


async def test_m5_two_replies_in_one_batch_each_instruction_manages_its_own_trade(prod: Prod) -> None:
    """Reviewer's test_y8: "1R kâr var entry stop attık" replies to the BTC signal, "kapattık" to the ETH signal.
    62e0719: the BTC stop move was asked (reply_trade_mismatch, "Mesaj #2 ETH işleminin sinyaline yanıt ...", which
    is false)."""
    s_btc, s_eth = await two_trades(prod)
    await batch(prod, [(9001, "1R kâr var entry stop attık", s_btc), (9002, "kapattık", s_eth)], [
        manage(ActionType.MOVE_SL, 1, s_btc, "BTC", move_sl_to_entry=True, used_message_ids=[9001],
               summary_tr="BTC stop girişe"),
        manage(ActionType.CLOSE, 2, s_eth, "ETH", close_fraction=1.0, used_message_ids=[9002],
               summary_tr="ETH kapatılacak"),
    ])
    assert prod.asked() == [], [(c.action.summary_tr, [i.code for i in c.issues]) for c in prod.notifier.confirmations]
    t_btc, t_eth = await prod.trades()
    assert t_btc.current_stop_loss == pytest.approx(74000.0)
    assert t_eth.status is TradeStatus.CLOSED


async def test_m5_a_plain_message_next_to_a_reply_is_not_read_as_that_reply(prod: Prod) -> None:
    """"Aktif" replies to the BTC signal; "eth stop girişe" is a plain message about ETH (read in the same batch).
    62e0719: the ETH instruction took the BTC reply as its own and was asked."""
    s_btc, s_eth = await two_trades(prod)
    prod.binance.set_price("ETH", 2270.0, 2270.2)                      # ETH in profit too
    prod.tickmill.set_price("ETHUSD", 2269.0, 2269.2)
    await batch(prod, [(9011, "Aktif", s_btc), (9012, "eth stop girişe", None)], [
        ParsedAction(action=ActionType.NONE, confidence=0.9, used_message_ids=[9011], summary_tr="bilgi"),
        manage(ActionType.MOVE_SL, 2, None, "ETH", move_sl_to_entry=True, used_message_ids=[9012],
               summary_tr="ETH stop girişe"),
    ])
    assert prod.asked() == [], [(c.action.summary_tr, [i.code for i in c.issues]) for c in prod.notifier.confirmations]
    t_btc, t_eth = await prod.trades()
    assert t_eth.current_stop_loss == pytest.approx(2232.0) and t_btc.current_stop_loss == pytest.approx(73300.0)


async def test_m5_replies_without_any_trade_named_each_use_their_own_reply(prod: Prod) -> None:
    """The model names no trade, message or asset and relies on the reply headers: each instruction's own reply
    decides (62e0719: both took the newest reply, the ETH signal - the BTC stop move went to ETH silently)."""
    s_btc, s_eth = await two_trades(prod)
    prod.binance.set_price("ETH", 2270.0, 2270.2)                      # ETH in profit too: a stop move fits both
    prod.tickmill.set_price("ETHUSD", 2269.0, 2269.2)
    await batch(prod, [(9021, "1R kâr var entry stop attık", s_btc), (9022, "kapattık", s_eth)], [
        manage(ActionType.MOVE_SL, None, None, None, move_sl_to_entry=True, used_message_ids=[9021],
               summary_tr="stop girişe"),
        manage(ActionType.CLOSE, None, None, None, close_fraction=1.0, used_message_ids=[9022], summary_tr="kapat"),
    ])
    assert prod.asked() == [], [(c.action.summary_tr, [i.code for i in c.issues]) for c in prod.notifier.confirmations]
    t_btc, t_eth = await prod.trades()
    assert t_btc.current_stop_loss == pytest.approx(74000.0) and t_btc.status is TradeStatus.OPEN
    assert t_eth.status is TradeStatus.CLOSED and t_eth.current_stop_loss == pytest.approx(2200.0)


async def test_m5_an_instruction_whose_messages_reply_to_two_signals_and_names_no_trade_is_asked(prod: Prod) -> None:
    """Reviewer's suspicion: the model leaves out the trade, the message and the asset AND the message ids, so each
    instruction covers both replies. Which trade is meant cannot be told: asked, nothing changed silently
    (62e0719: the stop move was applied to the ETH trade, whose signal was the newest reply)."""
    s_btc, s_eth = await two_trades(prod)
    prod.binance.set_price("ETH", 2270.0, 2270.2)                      # ETH in profit too: a stop move fits both
    prod.tickmill.set_price("ETHUSD", 2269.0, 2269.2)
    await batch(prod, [(9031, "1R kâr var entry stop attık", s_btc), (9032, "kapattık", s_eth)], [
        manage(ActionType.MOVE_SL, None, None, None, move_sl_to_entry=True, summary_tr="stop girişe"),
        manage(ActionType.CLOSE, None, None, None, close_fraction=1.0, summary_tr="kapat"),
    ])
    t_btc, t_eth = await prod.trades()
    assert t_btc.current_stop_loss == pytest.approx(73300.0) and t_eth.current_stop_loss == pytest.approx(2200.0)
    assert t_eth.status is TradeStatus.OPEN and t_btc.status is TradeStatus.OPEN       # nothing done on a guess
    assert len(prod.notifier.confirmations) == 2
    for conf in prod.notifier.confirmations:
        assert any(i.code == "ambiguity" and "farklı mesajlara yanıt" in i.message_tr for i in conf.issues), conf.issues


# =========================================================================== #
# M6 (YONETIM-5): an edited signal is compared with its own levels
# =========================================================================== #
EU_TEXT = "Eu short\n1R risk\n100 risk için 0.75 lot"


async def eu_short_at_break_even(p: Prod) -> tuple[int, ParsedAction]:
    from test_preflight_scenarios import FX, fx_pair, set_eurusd
    set_eurusd(p, 1.10872, 1.10874)
    opening = fx_pair("EURUSD", Side.SHORT, 1.10872, 1.11005, 1.10654, entry_type=EntryType.MARKET)
    sig = await p.say(opening, FX, EU_TEXT)
    set_eurusd(p, 1.10740, 1.10742)                                    # 1R in profit
    await p.say(manage(ActionType.MOVE_SL, 1, sig, "EUR", move_sl_to_entry=True, summary_tr="stop girişe"), FX,
                "1R kâr var entry stop attık", reply_to=sig)
    (trade,) = await p.trades()
    assert trade.current_stop_loss == pytest.approx(1.10872) and p.asked() == []
    p.notifier.notifications.clear()
    return sig, opening


async def edit_signal(p: Prod, sig: int, text: str, reading: ParsedAction) -> None:
    from test_preflight_scenarios import CHAT, FX, TITLES, TRADER
    from copytrader.models import ParseResult, StoredMessage
    edited = StoredMessage(chat_id=CHAT, message_id=sig, sender_id=TRADER, sender_name="Hoca", date=datetime.now(UTC),
                           text=text, edited=True, reply_to_message_id=FX, topic_id=FX, topic_title=TITLES[FX])
    await p.db.save_message(edited)
    p.parser.push(ParseResult(actions=[reading.model_copy(update={"used_message_ids": [sig]})], model="fake"))
    await p.engine.on_message_edited(edited)


async def test_m6_typo_fix_of_the_signal_after_break_even_proposes_nothing(prod: Prod) -> None:
    """Reviewer's test_y7::test_a_edit_after_be. 62e0719 asked "Sinyal düzenlendi: EUR stop 1.10872 → 1.11005"
    (sl_widened): one tap would have put the full 1R back on a trade the trader treats as risk-free."""
    sig, opening = await eu_short_at_break_even(prod)
    await edit_signal(prod, sig, EU_TEXT.replace("için", "icin"), opening)
    assert prod.notifier.confirmations == [], [c.action.summary_tr for c in prod.notifier.confirmations]
    (trade,) = await prod.trades()
    assert trade.current_stop_loss == pytest.approx(1.10872)


async def test_m6_a_real_change_of_the_signals_stop_is_still_proposed(prod: Prod) -> None:
    sig, opening = await eu_short_at_break_even(prod)
    changed = opening.model_copy(update={"stop_loss": 1.10950})       # the trader really changed the stop
    await edit_signal(prod, sig, EU_TEXT + "\nSL 1.10950", changed)
    (conf,) = prod.notifier.confirmations
    assert "stop 1.10872 → 1.1095" in (conf.action.summary_tr or "")
    assert any(i.code == "signal_edited" for i in conf.issues)


async def test_m6_an_edit_that_says_what_was_done_already_proposes_nothing(prod: Prod) -> None:
    """The trader adds "stop girişe çekildi" to the signal text after the break-even: read as a stop move to the
    entry, which the trade already has - nothing to propose."""
    sig, _opening = await eu_short_at_break_even(prod)
    note = manage(ActionType.MOVE_SL, 1, sig, "EUR", move_sl_to_entry=True, summary_tr="stop girişe")
    await edit_signal(prod, sig, EU_TEXT + "\nstop girişe çekildi", note)
    assert prod.notifier.confirmations == [], [c.action.summary_tr for c in prod.notifier.confirmations]


async def test_m6_targets_moved_since_are_no_edit(prod: Prod) -> None:
    sig, opening = await eu_short_at_break_even(prod)
    from test_preflight_scenarios import FX
    from copytrader.models import TakeProfit
    await prod.say(manage(ActionType.MOVE_TP, 1, sig, "EUR", take_profits=[TakeProfit(price=1.10500)],
                          summary_tr="yeni hedef"), FX, "yeni hedef 1.10500", reply_to=sig)
    (trade,) = await prod.trades()
    assert [tp.price for tp in trade.current_take_profits] == [pytest.approx(1.10500)]
    prod.notifier.notifications.clear()
    prod.notifier.confirmations.clear()
    await edit_signal(prod, sig, EU_TEXT.replace("için", "icin"), opening)    # levels as in the signal
    assert prod.notifier.confirmations == [], [c.action.summary_tr for c in prod.notifier.confirmations]


# =========================================================================== #
# M7 (YONETIM-6): "entry stop attık" while entries are still resting
# =========================================================================== #
async def test_m7_fx_break_even_while_both_limits_rest_cancels_them_without_a_question(prod: Prod) -> None:
    """Reviewer's test_y9 (fx): the trader's own limit filled ("Aktif"), ours did not (other quotes); he writes
    "1R kâr var entry stop attık". 62e0719: a question that could not be approved (sl_wrong_side) and both limits
    kept resting with the full original stop."""
    from test_preflight_scenarios import FX, fx_pair, set_eurusd
    set_eurusd(prod, 1.10840, 1.10842)                    # below a short limit at 1.10872
    sig = await prod.say(fx_pair("EURUSD", Side.SHORT, 1.10872, 1.11005, 1.10654, entry_type=EntryType.LIMIT), FX,
                         "EUR SHORT 1R Limit Emir")
    assert {b: x.status for b, x in (await prod.positions(1)).items()} == {
        "fx": PositionStatus.PENDING, "tickmill": PositionStatus.PENDING}
    prod.notifier.notifications.clear()
    set_eurusd(prod, 1.10730, 1.10732)                    # the trader's fill ran to +1R; ours never filled
    await prod.say(manage(ActionType.MOVE_SL, 1, sig, "EUR", move_sl_to_entry=True, summary_tr="EUR stop girişe"), FX,
                   "1R kâr var entry stop attık", reply_to=sig)
    assert prod.asked() == []
    assert {b: x.status for b, x in (await prod.positions(1)).items()} == {
        "fx": PositionStatus.CANCELLED, "tickmill": PositionStatus.CANCELLED}
    assert prod.terminal.orders == {} and prod.tickmill.orders == {}       # nothing rests at the brokers
    (report,) = [t for t in prod.texts() if "#1 EUR SHORT: uygulandı" in t]
    for broker in ("fx", "tickmill"):
        assert f"{broker}: stop girişe çekildiği için bekleyen giriş iptal edildi" in report, report
    assert (await prod.trades())[0].status is TradeStatus.CANCELLED
    (summary,) = finals(prod.texts())
    assert "iptal edildi" in summary and "giriş hiçbir hesapta dolmadı" in summary


async def test_m7_crypto_break_even_while_every_entry_rests_cancels_all_of_them(prod: Prod) -> None:
    """Reviewer's test_y9 (crypto): binance's limit, breakout's limit instruction and tickmill's waiting virtual entry.
    62e0719: a question with nothing to approve; the virtual entry stayed "waiting"."""
    sig = await prod.say(btc(EntryType.LIMIT, 73000.0, 72300.0, 75000.0), KRIPTO, "btc 1R long limit")
    prod.notifier.notifications.clear()
    prod.binance.set_price("BTC", 73700.0, 73702.0)
    prod.tickmill.set_price("BTCUSD", 73650.0, 73652.0)
    await prod.say(manage(ActionType.MOVE_SL, 1, sig, "BTC", move_sl_to_entry=True, summary_tr="BTC stop girişe"),
                   KRIPTO, "1R kâr var entry stop attık", reply_to=sig)
    assert prod.asked() == []
    assert {b: x.status for b, x in (await prod.positions(1)).items()} == {
        "binance": PositionStatus.CANCELLED, "breakout": PositionStatus.CANCELLED, "tickmill": PositionStatus.CANCELLED}
    entry = next(o for o in prod.binance.orders.values() if not o.get("triggerPrice") and not o.get("reduceOnly"))
    assert entry["status"] == "canceled"
    assert any("BEKLEYEN LİMİT EMRİ İPTAL ET" in t for t in prod.texts())     # the owner's manual broker
    (report,) = [t for t in prod.texts() if "#1 BTC LONG: uygulandı" in t]
    assert "tickmill: stop girişe çekildiği için bekleyen sanal giriş iptal edildi" in report
    assert "binance: stop girişe çekildiği için bekleyen giriş iptal edildi" in report
    prod.binance.set_price("BTC", 72999.0, 73001.0)       # the price comes back to the entry later: nothing opens
    prod.tickmill.set_price("BTCUSD", 72949.0, 72951.0)
    await prod.engine.monitor.run_virtual_once()
    assert prod.tickmill_orders() == [] and len(prod.binance.entry_orders()) == 1


async def test_m7_some_entries_filled_some_resting_moves_the_filled_stop_and_cancels_the_rest(prod: Prod) -> None:
    """binance's limit filled (it has its stop); breakout's limit instruction and tickmill's virtual entry still
    wait. "entry stop attık": binance's stop goes to its entry, the waiting ones are cancelled - no question."""
    sig = await prod.say(btc(EntryType.LIMIT, 73000.0, 72300.0, 75000.0), KRIPTO, "btc 1R long limit")
    entry = next(o for o in prod.binance.orders.values() if not o.get("triggerPrice") and not o.get("reduceOnly"))
    prod.binance.fill(entry["id"], price=73000.0)        # filled on a wick the virtual entry did not act on
    await prod.engine.monitor.run_virtual_once()
    assert (await prod.positions(1))["binance"].status is PositionStatus.OPEN
    prod.binance.set_price("BTC", 74400.0, 74402.0)       # 2R in profit
    prod.tickmill.set_price("BTCUSD", 74350.0, 74352.0)
    prod.notifier.notifications.clear()
    await prod.say(manage(ActionType.MOVE_SL, 1, sig, "BTC", move_sl_to_entry=True, summary_tr="BTC stop girişe"),
                   KRIPTO, "1R kâr var entry stop attık", reply_to=sig)
    assert prod.asked() == []
    pos = await prod.positions(1)
    assert pos["binance"].status is PositionStatus.OPEN and pos["binance"].stop_loss == pytest.approx(73000.0)
    assert pos["breakout"].status is PositionStatus.CANCELLED and pos["tickmill"].status is PositionStatus.CANCELLED
    stops = [o for o in prod.binance.orders.values() if o.get("stopLossPrice") and o["status"] == "open"]
    assert [o["stopLossPrice"] for o in stops] == [pytest.approx(73000.0)]
    (trade,) = await prod.trades()
    assert trade.status is TradeStatus.OPEN and trade.current_stop_loss == pytest.approx(73000.0)


async def test_m7_a_partly_filled_limit_keeps_its_filled_part_at_entry_and_drops_its_unfilled_rest(prod: Prod) -> None:
    sig = await prod.say(btc(EntryType.LIMIT, 73000.0, 72300.0, 75000.0), KRIPTO, "btc 1R long limit")
    entry = next(o for o in prod.binance.orders.values() if not o.get("triggerPrice") and not o.get("reduceOnly"))
    prod.binance.fill(entry["id"], amount=0.006, price=73000.0)          # half of 0.012 filled
    await prod.engine.monitor.run_virtual_once()
    before = (await prod.positions(1))["binance"]
    assert before.status is PositionStatus.OPEN and before.filled_quantity == pytest.approx(0.006)
    prod.binance.set_price("BTC", 74400.0, 74402.0)
    prod.tickmill.set_price("BTCUSD", 74350.0, 74352.0)
    prod.notifier.notifications.clear()
    await prod.say(manage(ActionType.MOVE_SL, 1, sig, "BTC", move_sl_to_entry=True, summary_tr="BTC stop girişe"),
                   KRIPTO, "1R kâr var entry stop attık", reply_to=sig)
    assert prod.asked() == []
    after = (await prod.positions(1))["binance"]
    assert after.stop_loss == pytest.approx(73000.0) and after.remaining_quantity == pytest.approx(0.006)
    assert prod.binance.orders[entry["id"]]["status"] == "canceled"   # the unfilled rest no longer rests
    (report,) = [t for t in prod.texts() if "#1 BTC LONG: uygulandı" in t]
    assert "binance: stop 73000; dolmayan kalan giriş iptal edildi" in report, report


@pytest.mark.parametrize("market", [None, 2260.0, 2220.0])
def test_m7_validator_break_even_of_a_pending_trade_is_a_plan_whatever_the_market(market: float | None) -> None:
    """No stop is placed for it (the resting entries are cancelled): neither the market price (unknown, or already
    back below the entry) nor the entry side may turn it into a question that cannot be approved. A real stop price
    of a pending trade is still checked."""
    from test_validator import codes, mact, manage as vmanage, trade as vtrade
    t = vtrade(status=TradeStatus.PENDING)
    plan, issues = vmanage(mact(ActionType.MOVE_SL, move_sl_to_entry=True), t, market=market)
    assert plan is not None and plan.move_sl_to_entry and issues == [], codes(issues)
    plan, issues = vmanage(mact(ActionType.MOVE_SL, new_stop_loss=2240.0), t, market=market)
    assert plan is None and "sl_wrong_side" in codes(issues)
    # an OPEN trade's stop to the entry is still checked against the market
    plan, issues = vmanage(mact(ActionType.MOVE_SL, move_sl_to_entry=True), vtrade(), market=2220.0)
    assert plan is None and "sl_wrong_side" in codes(issues)
