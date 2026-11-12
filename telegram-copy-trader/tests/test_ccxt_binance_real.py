"""CcxtBroker against the REAL ccxt binanceusdm class (only the HTTP layer is faked, see binance_rest_fake.py):
request routing to the Algo Order endpoints, leverage refusals, leverage brackets and margin, one-way netting,
order-not-found answers, lost replies, flat positions, partial stop fills, the clock offset.

What these tests prove is the bot's logic on ccxt's real request / parsing / error-mapping code. The exchange's
side (fills, margin rule, which code it really sends) is a simulation written from Binance's documentation.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import ccxt
import pytest

from binance_rest_fake import ApiError, FakeBinance, make_binance
from copytrader.brokers.base import BrokerError
from copytrader.brokers.ccxt_broker import CcxtBroker
from copytrader.models import (
    BrokerPosition,
    EntryType,
    MarketType,
    OpenRequest,
    PositionStatus,
    Side,
    SizingResult,
    TakeProfit,
    TradePlan,
)
from copytrader.risk.sizing import SizingError


def _plan(base: str, side: Side, sl: float, tps: list[tuple[float, float]], *, entry: float | None = None,
          entry_type: EntryType = EntryType.MARKET, plan_id: str = "12ab34cd") -> TradePlan:
    return TradePlan(
        plan_id=plan_id, base_asset=base, quote_asset=None, market_type=MarketType.PERP, side=side,
        entry_type=entry_type, entry_price=entry, stop_loss=sl,
        take_profits=[TakeProfit(price=p, close_fraction=f) for p, f in tps],
        risk_r=1.0, risk_pct=2.0, source_chat_id=-100, source_message_ids=[1],
        created_at=datetime(2026, 11, 12, tzinfo=timezone.utc),
    )


async def _size(broker: CcxtBroker, plan: TradePlan, risk: float = 10.0) -> tuple[Any, SizingResult]:
    sym = await broker.resolve_symbol(plan.base_asset, plan.quote_asset, plan.market_type, None)
    assert sym is not None
    quote = await broker.get_quote(sym)
    entry = quote.entry_price_for(plan.side) if plan.entry_type is EntryType.MARKET else float(plan.entry_price or 0)
    return sym, await broker.size_position(sym, plan.side, entry, plan.stop_loss, risk)


async def _open(broker: CcxtBroker, plan: TradePlan, risk: float = 10.0) -> tuple[SizingResult, BrokerPosition]:
    """Same call order as Engine._open_on_broker."""
    sym, size = await _size(broker, plan, risk)
    req = OpenRequest(plan=plan, symbol=sym, quantity=size.quantity, entry_type=plan.entry_type,
                      entry_price=plan.entry_price if plan.entry_type is EntryType.LIMIT else None,
                      stop_loss=plan.stop_loss, take_profits=plan.take_profits, leverage=size.leverage,
                      client_tag=f"ct{plan.plan_id}")
    return size, await broker.open_position(req)


def _live(fake: FakeBinance) -> list[tuple[str, str, str]]:
    """Conditional orders that are live on the venue: (type, quantity, trigger)."""
    return sorted((a["orderType"], a["quantity"], a["triggerPrice"]) for a in fake.algos.values()
                  if a["algoStatus"] == "NEW")


def _posts(fake: FakeBinance, start: int = 0) -> list[tuple[str, dict[str, str]]]:
    return [(p, q) for m, p, q in fake.log[start:] if m == "POST"]


def _writes(fake: FakeBinance, start: int = 0) -> list[tuple[str, str]]:
    """Every request that changes something on the venue."""
    return [(m, p) for m, p, _ in fake.log[start:] if m != "GET"]


def _leverage_requests(fake: FakeBinance, start: int = 0) -> list[str]:
    return [q["leverage"] for p, q in _posts(fake, start) if p == "/fapi/v1/leverage"]


BTC_LONG = dict(base="BTC", side=Side.LONG, sl=73300.0, tps=[(75500.0, 1.0)])
BTC_STOP_AND_TP = [("STOP_MARKET", "0.012", "73300"), ("TAKE_PROFIT_MARKET", "0.012", "75500")]


async def _connected(**kw: Any) -> tuple[CcxtBroker, FakeBinance]:
    broker, fake = make_binance(**kw)
    await broker.connect()
    return broker, fake


def _refuse_leverage_above(fake: FakeBinance, limit: int, code: int, msg: str, symbol: str | None = None) -> None:
    def refuse(method: str, path: str, q: dict[str, str]) -> None:
        if int(q["leverage"]) > limit:
            raise ApiError(code, msg.format(lev=q["leverage"]))

    fake.fail(lambda m, p, q: p == "/fapi/v1/leverage" and symbol in (None, q["symbol"]), refuse, times=100)


# --------------------------------------------------------------------------- routing
async def test_market_open_uses_order_endpoint_and_algo_endpoints() -> None:
    broker, fake = await _connected()
    n = len(fake.log)
    size, pos = await _open(broker, _plan(**BTC_LONG))
    assert (size.quantity, size.leverage) == (0.012, 125.0) and pos.status is PositionStatus.OPEN
    posts = _posts(fake, n)
    assert [p for p, _ in posts] == ["/fapi/v1/marginType", "/fapi/v1/leverage", "/fapi/v1/order",
                                     "/fapi/v1/algoOrder", "/fapi/v1/algoOrder"]
    entry, sl, tp = posts[2][1], posts[3][1], posts[4][1]
    assert entry["type"] == "MARKET" and entry["newClientOrderId"] == "ct12ab34cde" and "positionSide" not in entry
    assert (sl["type"], sl["triggerPrice"], sl["reduceOnly"], sl["algoType"]) == ("STOP_MARKET", "73300", "true", "CONDITIONAL")
    assert (tp["type"], tp["triggerPrice"], tp["reduceOnly"]) == ("TAKE_PROFIT_MARKET", "75500", "true")
    # N4: SL and TP carry client ids of our own (one per request), so a lost reply can be found again
    assert (sl["clientAlgoId"], tp["clientAlgoId"]) == ("ct12ab34cds1", "ct12ab34cdt2")
    assert pos.data["leverage"] == 125 and pos.data["warnings"] == []
    await broker.close()


async def test_stop_fill_is_read_from_the_algo_order_and_its_child() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    fake.price["BTCUSDT"] = (73290.0, 73290.1)
    fake.trigger(pos.data["sl_order_id"], price=73288.0)
    n = len(fake.log)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["closed_reason"] == "stop"
    assert pos.realized_pnl == pytest.approx(-9.6078, abs=1e-3)
    calls = [(m, p) for m, p, _ in fake.log[n:]]
    assert ("GET", "/fapi/v1/algoOrder") in calls and ("DELETE", "/fapi/v1/algoOrder") in calls
    assert _live(fake) == []
    await broker.close()


async def test_move_stop_places_new_algo_then_cancels_old() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    fake.price["BTCUSDT"] = (74800.0, 74800.1)
    n = len(fake.log)
    pos = await broker.update_stop_loss(pos, 74015.0)
    assert [(m, p) for m, p, _ in fake.log[n:]] == [("POST", "/fapi/v1/algoOrder"), ("DELETE", "/fapi/v1/algoOrder")]
    assert _live(fake) == [("STOP_MARKET", "0.012", "74015"), ("TAKE_PROFIT_MARKET", "0.012", "75500")]
    await broker.close()


# --------------------------------------------------------------------------- N1: leverage refusals
@pytest.mark.parametrize("code,msg,requests", [
    (-4028, "Leverage {lev} is not valid", ["125", "100", "75", "50", "40", "25", "20"]),
    (-4209, "The max leverage of Symbol is 20x", ["125", "20"]),
    (-4209, "Leverage adjustment failed. Current symbol max leverage limit is 20x", ["125", "20"]),
    (-4208, "Current symbol leverage cannot exceed 20 when using position limit adjustment service.", ["125", "20"]),
    (-4203, "More than 20x leverage is available one month after account registration.", ["125", "20"]),
    (-4205, "More than 20x leverage is available 30 days after Futures account registration.", ["125", "20"]),
    (-4202, "Intermediate Personal Verification is required for adjusting leverage over 20x", ["125", "20"]),
    (-4206, "Users in your location/country can only access a maximum leverage of 20", ["125", "20"]),
    (-4206, "Users in this country has limited adjust leverage.", ["125", "100", "75", "50", "40", "25", "20"]),
    (-4403, "Dear user, as per our Terms of Use and compliance with local regulations, the leverage can only up to 20x in your region", ["125", "20"]),
])
async def test_every_documented_leverage_refusal_steps_down_and_the_trade_opens(
        code: int, msg: str, requests: list[str]) -> None:
    broker, fake = await _connected()
    _refuse_leverage_above(fake, 20, code, msg)
    n = len(fake.log)
    _, pos = await _open(broker, _plan("BTC", Side.LONG, 72500.0, [(77000.0, 1.0)]))
    assert pos.status is PositionStatus.OPEN and fake.lev["BTCUSDT"] == 20
    assert _leverage_requests(fake, n) == requests        # the limit named in the message is used at once
    assert pos.data["leverage"] == 20                     # N8: the leverage the exchange accepted is stored
    assert any("125x yerine 20x" in w for w in pos.data["warnings"])
    assert len(_live(fake)) == 2
    await broker.close()


async def test_account_wide_leverage_limit_is_remembered_for_the_next_coin() -> None:
    broker, fake = await _connected()
    _refuse_leverage_above(fake, 20, -4203, "More than 20x leverage is available one month after account registration.")
    await _open(broker, _plan(**BTC_LONG))
    n = len(fake.log)
    size, pos = await _open(broker, _plan("DOGE", Side.LONG, 0.19, [(0.22, 1.0)], plan_id="dddd0001"))
    assert size.leverage == 20.0 and pos.status is PositionStatus.OPEN
    assert _leverage_requests(fake, n) == ["20"]          # DOGE allows 75x, the account only 20x: no refused request
    await broker.close()


async def test_coin_limit_from_a_refusal_is_not_applied_to_other_coins() -> None:
    broker, fake = await _connected()
    fake.no_brackets.update(fake.symbols)         # limits are only learned from refusals here
    _refuse_leverage_above(fake, 20, -4209, "The max leverage of Symbol is 20x", symbol="DOGEUSDT")
    _, doge = await _open(broker, _plan("DOGE", Side.LONG, 0.19, [(0.22, 1.0)], plan_id="dddd0001"))
    assert doge.data["leverage"] == 20
    n = len(fake.log)
    size, btc = await _open(broker, _plan(**BTC_LONG))
    assert (size.leverage, btc.data["leverage"]) == (125.0, 125) and _leverage_requests(fake, n) == ["125"]
    n = len(fake.log)
    _, size = await _size(broker, _plan("DOGE", Side.LONG, 0.19, [(0.22, 1.0)], plan_id="dddd0002"))
    assert size.leverage == 20.0                          # the coin's own limit is remembered for the coin
    await broker.close()


async def test_other_leverage_refusal_continues_with_the_leverage_the_symbol_has() -> None:
    broker, fake = await _connected()
    fake.fail(lambda m, p, q: p == "/fapi/v1/leverage",
              ApiError(-2028, "Leverage is smaller than permitted: insufficient margin balance."), times=100)
    n = len(fake.log)
    _, pos = await _open(broker, _plan(**BTC_LONG))
    assert pos.status is PositionStatus.OPEN and pos.data["leverage"] == 20       # the fake's symbols start at 20x
    assert _leverage_requests(fake, n) == ["125"]
    assert any("mevcut kaldıraç 20x ile devam edildi" in w for w in pos.data["warnings"])
    # at 20x the planned 0.012 BTC needs 44.41 margin, more than 90% of 46.90: the largest size that fits is opened
    assert pos.quantity == 0.011 and "planlanan 0.012 yerine 0.011 açıldı" in pos.data["warnings"][-1]
    assert _live(fake) == [("STOP_MARKET", "0.011", "73300"), ("TAKE_PROFIT_MARKET", "0.011", "75500")]
    await broker.close()


async def test_leverage_refused_and_current_leverage_unknown_refuses_the_trade() -> None:
    broker, fake = await _connected()
    fake.fail(lambda m, p, q: p == "/fapi/v1/leverage",
              ApiError(-2028, "Leverage is smaller than permitted: insufficient margin balance."), times=100)
    fake.symbol_config_works = False
    with pytest.raises(BrokerError, match="kaldıraç.*ayarlanamadı"):
        await _open(broker, _plan(**BTC_LONG))
    assert fake.orders == {} and fake.algos == {}
    await broker.close()


async def test_isolated_margin_never_trades_with_a_higher_leverage_than_planned() -> None:
    broker, fake = await _connected(margin_mode="isolated", leverage=10, max_leverage=50)
    fake.lev["BTCUSDT"] = 50                              # what the symbol has on the exchange
    fake.fail(lambda m, p, q: p == "/fapi/v1/leverage",
              ApiError(-4161, "Leverage reduction is not supported in Isolated Margin Mode with open positions"), times=100)
    with pytest.raises(BrokerError, match="kaldıraç.*ayarlanamadı"):
        await _open(broker, _plan(**BTC_LONG))
    assert fake.orders == {}
    await broker.close()


async def test_set_leverage_timeout_is_retried_once() -> None:
    broker, fake = await _connected()
    fake.fail(lambda m, p, q: p == "/fapi/v1/leverage", ccxt.RequestTimeout("binanceusdm POST leverage"))
    n = len(fake.log)
    _, pos = await _open(broker, _plan(**BTC_LONG))
    assert pos.status is PositionStatus.OPEN and _leverage_requests(fake, n) == ["125", "125"]
    assert pos.data["warnings"] == []
    await broker.close()


async def test_set_leverage_refused_for_the_clock_measures_the_offset_again() -> None:
    broker, fake = await _connected()
    fake.fail(lambda m, p, q: p == "/fapi/v1/leverage",
              ApiError(-1021, "Timestamp for this request is outside of the recvWindow."))
    n = len(fake.log)
    _, pos = await _open(broker, _plan(**BTC_LONG))
    calls = [(m, p) for m, p, _ in fake.log[n:]]
    first = calls.index(("POST", "/fapi/v1/leverage"))
    assert calls[first:first + 3] == [("POST", "/fapi/v1/leverage"), ("GET", "/fapi/v1/time"), ("POST", "/fapi/v1/leverage")]
    assert pos.status is PositionStatus.OPEN and pos.data["leverage"] == 125
    await broker.close()


# --------------------------------------------------------------------------- N2: leverage brackets and margin
async def test_low_leverage_coin_is_sized_to_the_margin_it_really_needs() -> None:
    broker, fake = make_binance()
    fake.add_symbol("ALT20USDT", "DOGEUSDT", bid=0.19999, ask=0.2, max_lev=20)
    await broker.connect()
    n = len(fake.log)
    size, pos = await _open(broker, _plan("ALT20", Side.LONG, 0.199, [(0.21, 1.0)]))      # 0.5% stop
    # planned 8336 (notional 1667 = 83 margin at 20x); 90% of the 46.90 free balance carries 844 -> 4216
    assert (size.quantity, size.leverage) == (4216.0, 20.0) and pos.status is PositionStatus.OPEN
    assert size.margin_required == pytest.approx(42.16) and size.margin_required <= 46.90 * 0.9
    assert size.est_loss_at_sl == pytest.approx(5.057, abs=1e-3) and size.risk_amount == 10.0
    assert size.warnings == ["Teminat yetmediği için pozisyon küçültüldü: planlanan 8336 yerine 4216 açılıyor; stopta "
                             "kayıp ~5.06 (hedef 10.00). Serbest bakiye 46.90, bu büyüklükte kullanılabilen en yüksek "
                             "kaldıraç 20x."]
    assert _leverage_requests(fake, n) == ["20"]          # no blind 125x try
    assert [q["quantity"] for p, q in _posts(fake, n) if p == "/fapi/v1/order"] == ["4216"]
    assert pos.quantity == 4216.0 and pos.data["warnings"] == []
    # the engine tells the owner about a position marked like this even when execution reports are off
    assert pos.data["size_capped"] is True
    await broker.close()


async def test_margin_that_fits_is_not_touched() -> None:
    broker, fake = await _connected()
    size, pos = await _open(broker, _plan(**BTC_LONG))
    assert (size.quantity, size.leverage, size.warnings) == (0.012, 125.0, [])
    assert size.margin_required == pytest.approx(888.18 / 125)
    assert pos.quantity == 0.012 and "size_capped" not in pos.data
    await broker.close()


async def test_shrunk_preview_does_not_mark_a_later_full_size_open() -> None:
    """The mark belongs to the sizing that directly precedes the open (same coin, same quantity, recent)."""
    broker, fake = make_binance(wallet=5.0)
    await broker.connect()
    _, preview = await _size(broker, _plan(**BTC_LONG))           # e.g. the "parse" preview: 0.012 -> 0.007
    assert preview.quantity == 0.007 and "planlanan 0.012 yerine 0.007" in preview.warnings[0]
    fake.wallet = 46.90                                           # money arrives; the real signal is sized again
    size, pos = await _open(broker, _plan(**BTC_LONG))
    assert (size.quantity, size.warnings, pos.quantity) == (0.012, [], 0.012)
    assert "size_capped" not in pos.data
    await broker.close()


async def test_old_shrink_note_is_not_applied_to_an_open_without_its_own_sizing() -> None:
    broker, fake = make_binance(wallet=5.0)
    await broker.connect()
    sym, preview = await _size(broker, _plan(**BTC_LONG))
    assert preview.quantity == 0.007
    plan = _plan(**BTC_LONG)

    def request() -> OpenRequest:
        return OpenRequest(plan=plan, symbol=sym, quantity=0.007, entry_type=EntryType.MARKET, stop_loss=plan.stop_loss,
                           take_profits=plan.take_profits, leverage=125, client_tag="ct12ab34cd")

    fake.now += 10 * 60 * 1000                                    # ten minutes later, no new sizing
    late = await broker.open_position(request())
    assert late.status is PositionStatus.OPEN and "size_capped" not in late.data
    await broker.close()


async def test_not_even_the_smallest_order_fits_refuses_before_any_request() -> None:
    broker, fake = make_binance(wallet=0.2)
    fake.add_symbol("ALT20USDT", "DOGEUSDT", bid=0.19999, ask=0.2, max_lev=20)
    await broker.connect()
    n = len(fake.log)
    with pytest.raises(SizingError, match="Yetersiz teminat") as refused:
        await _open(broker, _plan("ALT20", Side.LONG, 0.199, [(0.21, 1.0)]))
    assert _writes(fake, n) == []
    assert str(refused.value) == ("Yetersiz teminat: en küçük emir (~5.00 USDT) için bile 20x kaldıraçla ~0.25 teminat "
                                  "gerekiyor; kullanılabilir 0.18 (serbest bakiye 0.20). İşlem açılmadı.")
    await broker.close()


async def test_larger_position_uses_the_bracket_that_allows_its_size() -> None:
    # 75x only up to 5000 USDT, 50x up to 25000: an 8000 USDT position needs the second bracket
    broker, fake = make_binance(wallet=500.0)
    fake.add_symbol("TIERUSDT", "DOGEUSDT", bid=0.19999, ask=0.2, max_lev=75)
    fake.tiers["TIERUSDT"] = [(75, 5000.0), (50, 25000.0)]
    await broker.connect()
    n = len(fake.log)
    size, pos = await _open(broker, _plan("TIER", Side.LONG, 0.1992, [(0.21, 1.0)]), risk=40.0)   # 0.4% stop
    assert size.notional == pytest.approx(8003.2) and size.leverage == 50.0 and size.warnings == []
    assert pos.status is PositionStatus.OPEN and _leverage_requests(fake, n) == ["50"]
    await broker.close()


async def test_bracket_limits_the_size_when_margin_would_allow_more() -> None:
    # free 46.90 -> 42.21 usable: at 75x that is 3166 USDT, but the 75x bracket ends at 2000 and 20x carries 844
    broker, fake = make_binance()
    fake.add_symbol("TIERUSDT", "DOGEUSDT", bid=0.19999, ask=0.2, max_lev=75)
    fake.tiers["TIERUSDT"] = [(75, 2000.0), (20, 25000.0)]
    await broker.connect()
    size, pos = await _open(broker, _plan("TIER", Side.LONG, 0.1994, [(0.21, 1.0)]))      # 0.3% stop: planned 2500
    assert size.quantity == 9990.0 and size.notional == pytest.approx(1998.0) and size.leverage == 75.0
    assert "planlanan 12504 yerine 9990" in size.warnings[0] and pos.status is PositionStatus.OPEN
    await broker.close()


async def test_step_down_without_brackets_shrinks_to_the_margin_and_tells_the_owner() -> None:
    broker, fake = make_binance()
    fake.add_symbol("ALT20USDT", "DOGEUSDT", bid=0.19999, ask=0.2, max_lev=20)
    fake.no_brackets.add("ALT20USDT")             # the maximum of this coin is not known up front
    await broker.connect()
    n = len(fake.log)
    size, pos = await _open(broker, _plan("ALT20", Side.LONG, 0.199, [(0.21, 1.0)]))
    assert (size.quantity, size.leverage) == (8336.0, 125.0)         # planned blind, as before
    assert pos.status is PositionStatus.OPEN and pos.quantity == 4216.0 and pos.data["leverage"] == 20
    assert [q["quantity"] for p, q in _posts(fake, n) if p == "/fapi/v1/order"] == ["4216"]
    assert pos.data["warnings"] == [
        "Kaldıraç 125x yerine 20x (borsanın bu coin / hesap için izin verdiği en yüksek).",
        "Kaldıraç 20x ile teminat yetmediği için pozisyon küçültüldü: planlanan 8336 yerine 4216 açıldı; stopta "
        "kayıp ~5.06.",
    ]
    assert pos.data["size_capped"] is True
    assert _live(fake) == [("STOP_MARKET", "4216", "0.199"), ("TAKE_PROFIT_MARKET", "4216", "0.21")]
    await broker.close()


async def test_lower_leverage_than_the_size_was_planned_with_is_checked_against_the_margin_rule() -> None:
    """The size was planned at 125x; when the order is opened the adapter already knows that only 20x is allowed
    (learned in between). The 90% margin rule must be applied to the leverage really used."""
    broker, fake = await _connected()
    plan = _plan(**BTC_LONG)
    sym, size = await _size(broker, plan)
    assert (size.quantity, size.leverage) == (0.012, 125.0)
    broker._leverage_caps[sym.broker_symbol] = (20, fake.now)       # e.g. another trade's refusal a moment ago
    n = len(fake.log)
    pos = await broker.open_position(OpenRequest(
        plan=plan, symbol=sym, quantity=size.quantity, entry_type=EntryType.MARKET, stop_loss=plan.stop_loss,
        take_profits=plan.take_profits, leverage=size.leverage, client_tag="ct12ab34cd"))
    assert _leverage_requests(fake, n) == ["20"] and pos.data["leverage"] == 20
    # 0.012 BTC at 20x needs 44.41, more than 90% of 46.90: the largest size that fits is opened
    assert pos.status is PositionStatus.OPEN and pos.quantity == 0.011
    assert [q["quantity"] for p, q in _posts(fake, n) if p == "/fapi/v1/order"] == ["0.011"]
    assert pos.data["warnings"] == ["Kaldıraç 20x ile teminat yetmediği için pozisyon küçültüldü: planlanan 0.012 "
                                    "yerine 0.011 açıldı; stopta kayıp ~8.68."]
    assert pos.data["size_capped"] is True
    await broker.close()


async def test_balance_unreadable_after_a_step_down_leaves_the_margin_to_the_exchange_and_still_opens() -> None:
    broker, fake = make_binance()
    fake.add_symbol("ALT20USDT", "DOGEUSDT", bid=0.19999, ask=0.2, max_lev=20)
    fake.no_brackets.add("ALT20USDT")
    await broker.connect()
    stepped_down = lambda: any(p == "/fapi/v1/leverage" for _, p, _ in fake.log)        # noqa: E731
    fake.fail(lambda m, p, q: p == "/fapi/v3/account" and stepped_down(), ApiError(-1000, "An unknown error occured."))
    n = len(fake.log)
    size, pos = await _open(broker, _plan("ALT20", Side.LONG, 0.199, [(0.21, 1.0)]))
    assert size.quantity == 8336.0 and pos.status is PositionStatus.OPEN and pos.data["leverage"] == 20
    # the exchange refuses the planned size and three quarters of it for margin; half of it fits
    assert [q["quantity"] for p, q in _posts(fake, n) if p == "/fapi/v1/order"] == ["8336", "6252", "4168"]
    assert pos.quantity == 4168.0 and pos.data["size_capped"] is True
    assert pos.data["warnings"][-1] == ("Borsa teminatı yetersiz bulduğu için pozisyon küçültüldü: planlanan 8336 yerine "
                                        "4168 açıldı; stopta kayıp ~5.00.")
    assert _live(fake) == [("STOP_MARKET", "4168", "0.199"), ("TAKE_PROFIT_MARKET", "4168", "0.21")]
    await broker.close()


async def test_step_down_where_the_smallest_order_does_not_fit_sends_no_order() -> None:
    broker, fake = make_binance(wallet=0.2)
    fake.add_symbol("ALT20USDT", "DOGEUSDT", bid=0.19999, ask=0.2, max_lev=20)
    fake.no_brackets.add("ALT20USDT")
    await broker.connect()
    sym = await broker.resolve_symbol("ALT20")
    assert sym is not None
    plan = _plan("ALT20", Side.LONG, 0.199, [(0.21, 1.0)])
    req = OpenRequest(plan=plan, symbol=sym, quantity=100.0, entry_type=EntryType.MARKET, stop_loss=0.199,
                      take_profits=plan.take_profits, leverage=125, client_tag="ct12ab34cd")
    n = len(fake.log)
    with pytest.raises(BrokerError) as refused:
        await broker.open_position(req)
    assert [p for p, _ in _posts(fake, n) if p == "/fapi/v1/order"] == []       # refused before any order request
    assert "yetersiz teminat" in str(refused.value) and "İşlem açılmadı" in str(refused.value)
    assert fake.orders == {} and fake.algos == {}
    await broker.close()


async def test_exchange_says_margin_is_insufficient_then_a_smaller_entry_is_sent() -> None:
    broker, fake = await _connected(wallet=8.0)
    fake.margin_factor = 1.2                 # SIMULATED: the exchange wants 20% more than notional / leverage
    n = len(fake.log)
    size, pos = await _open(broker, _plan(**BTC_LONG))
    assert size.quantity == 0.012 and pos.status is PositionStatus.OPEN
    entries = [(q["quantity"], q["newClientOrderId"]) for p, q in _posts(fake, n) if p == "/fapi/v1/order"]
    assert entries == [("0.012", "ct12ab34cde"), ("0.009", "ct12ab34cde2")]
    assert (pos.quantity, pos.filled_quantity, fake.pos["BTCUSDT"]["amt"]) == (0.009, 0.009, 0.009)
    assert pos.data["entry_client_id"] == "ct12ab34cde2"
    assert pos.data["warnings"] == ["Borsa teminatı yetersiz bulduğu için pozisyon küçültüldü: planlanan 0.012 yerine "
                                    "0.009 açıldı; stopta kayıp ~7.10."]
    assert pos.data["size_capped"] is True
    assert _live(fake) == [("STOP_MARKET", "0.009", "73300"), ("TAKE_PROFIT_MARKET", "0.009", "75500")]
    await broker.close()


async def test_margin_refusal_that_no_smaller_size_cures_is_reported() -> None:
    broker, fake = await _connected(wallet=8.0)
    fake.margin_factor = 5.0
    with pytest.raises(BrokerError, match="yetersiz bakiye"):
        await _open(broker, _plan(**BTC_LONG))
    assert fake.pos.get("BTCUSDT", {"amt": 0.0})["amt"] == 0.0 and fake.algos == {}
    await broker.close()


# --------------------------------------------------------------------------- N3: one-way netting
async def test_one_way_mode_refuses_an_entry_against_an_opposite_position_and_sends_nothing() -> None:
    broker, fake = await _connected()
    await _open(broker, _plan(**BTC_LONG, plan_id="aaaa1111"))
    n = len(fake.log)
    with pytest.raises(BrokerError, match=r"ters yönde açık pozisyon var \(LONG 0\.012\).*işlem açılmadı"):
        await _open(broker, _plan("BTC", Side.SHORT, 74700.0, [(72500.0, 1.0)], plan_id="bbbb2222"))
    assert _writes(fake, n) == []                         # not even a leverage / margin-mode request
    assert fake.pos["BTCUSDT"]["amt"] == 0.012 and _live(fake) == BTC_STOP_AND_TP
    await broker.close()


async def test_one_way_mode_refuses_an_entry_against_a_position_opened_by_hand() -> None:
    broker, fake = await _connected()
    fake.pos["BTCUSDT"] = {"amt": -0.002, "entry": 74000.0}
    n = len(fake.log)
    with pytest.raises(BrokerError, match=r"ters yönde açık pozisyon var \(SHORT 0\.002\)"):
        await _open(broker, _plan(**BTC_LONG))
    assert _writes(fake, n) == [] and fake.pos["BTCUSDT"]["amt"] == -0.002
    await broker.close()


async def test_one_way_mode_refuses_an_entry_against_a_resting_opposite_entry() -> None:
    broker, fake = await _connected()
    _, pending = await _open(broker, _plan(**BTC_LONG, entry=73900.0, entry_type=EntryType.LIMIT, plan_id="aaaa1111"))
    assert pending.status is PositionStatus.PENDING
    n = len(fake.log)
    with pytest.raises(BrokerError, match=r"ters yönde bekleyen giriş emri var \(LONG 0\.014\)"):
        await _open(broker, _plan("BTC", Side.SHORT, 74700.0, [(72500.0, 1.0)], plan_id="bbbb2222"))
    assert _writes(fake, n) == []
    await broker.close()


async def test_a_second_trade_in_the_same_direction_is_still_opened() -> None:
    broker, fake = await _connected()
    await _open(broker, _plan(**BTC_LONG, plan_id="aaaa1111"))
    _, second = await _open(broker, _plan(**BTC_LONG, plan_id="bbbb2222"))
    assert second.status is PositionStatus.OPEN and fake.pos["BTCUSDT"]["amt"] == 0.024
    await broker.close()


async def test_resting_sell_that_only_closes_what_is_held_does_not_block_another_long() -> None:
    broker, fake = await _connected()
    await _open(broker, _plan(**BTC_LONG, plan_id="aaaa1111"))
    fake.new_order({"symbol": "BTCUSDT", "side": "SELL", "type": "LIMIT", "quantity": "0.012", "price": "76000",
                    "timeInForce": "GTC"})                # e.g. a take profit placed by hand, not reduce-only
    _, second = await _open(broker, _plan(**BTC_LONG, plan_id="bbbb2222"))
    assert second.status is PositionStatus.OPEN and fake.pos["BTCUSDT"]["amt"] == 0.024
    await broker.close()


async def test_unreadable_positions_do_not_stop_the_entry_but_the_owner_is_told() -> None:
    broker, fake = await _connected()
    fake.fail(lambda m, p, q: p == "/fapi/v3/positionRisk", ApiError(-1000, "unknown"))
    _, pos = await _open(broker, _plan(**BTC_LONG))
    assert pos.status is PositionStatus.OPEN
    assert pos.data["warnings"] == ["Borsadaki açık pozisyonlar okunamadı; ters yönde pozisyon kontrolü yapılamadı."]
    await broker.close()


@pytest.mark.parametrize("path,warning", [
    ("/fapi/v3/positionRisk", "Borsadaki açık pozisyonlar okunamadı; ters yönde pozisyon kontrolü yapılamadı."),
    ("/fapi/v1/openOrders", "Borsadaki bekleyen emirler okunamadı; ters yönde emir kontrolü yapılamadı."),
], ids=["positions", "open-orders"])
async def test_a_check_that_breaks_in_an_unexpected_way_never_costs_the_entry(path: str, warning: str) -> None:
    """The two reads of the netting check run before EVERY entry: whatever goes wrong in them (here: not even a
    ccxt error), the trade must still open and the owner gets the warning."""
    broker, fake = await _connected()
    fake.fail(lambda m, p, q: p == path, ValueError("unexpected answer"))
    _, pos = await _open(broker, _plan(**BTC_LONG))
    assert pos.status is PositionStatus.OPEN and _live(fake) == BTC_STOP_AND_TP
    assert pos.data["warnings"] == [warning]
    await broker.close()


# --------------------------------------------------------------------------- N4: no duplicate / orphan stops
async def test_transient_not_found_for_a_live_stop_places_no_second_stop() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    sl_id = pos.data["sl_order_id"]
    fake.fail(lambda m, p, q: m == "GET" and p == "/fapi/v1/algoOrder" and q.get("algoId") == sl_id,
              ApiError(-2013, "Order does not exist."))
    n = len(fake.log)
    pos = await broker.sync_position(pos)
    assert pos.data["sl_order_id"] == sl_id and pos.error is None and _writes(fake, n) == []
    assert _live(fake) == BTC_STOP_AND_TP
    await broker.close()


async def test_stop_unknown_by_id_and_in_the_history_but_listed_as_open_is_left_alone() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    sl_id = pos.data["sl_order_id"]
    fake.fail(lambda m, p, q: m == "GET" and p == "/fapi/v1/algoOrder" and q.get("algoId") == sl_id,
              ApiError(-2013, "Order does not exist."))
    fake.algo_history_works = False
    n = len(fake.log)
    pos = await broker.sync_position(pos)
    assert ("GET", "/fapi/v1/openAlgoOrders") in [(m, p) for m, p, _ in fake.log[n:]]
    assert pos.data["sl_order_id"] == sl_id and _writes(fake, n) == [] and _live(fake) == BTC_STOP_AND_TP
    await broker.close()


async def test_stop_that_stays_unknown_by_id_does_not_stop_the_take_profit_from_being_booked() -> None:
    """The exchange keeps answering "order does not exist" for a stop that is listed as open. The rest of the
    pass must still run: the take profit fill is booked, the trade closes and the stop is cancelled."""
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    sl_id = pos.data["sl_order_id"]
    fake.fail(lambda m, p, q: m == "GET" and p == "/fapi/v1/algoOrder" and q.get("algoId") == sl_id,
              ApiError(-2013, "Order does not exist."), times=100)
    fake.algo_history_works = False
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.data["sl_order_id"] == sl_id and _live(fake) == BTC_STOP_AND_TP
    fake.price["BTCUSDT"] = (75510.0, 75510.1)
    fake.trigger(pos.data["tp_order_ids"][0]["id"], price=75505.0)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["closed_reason"] == "take_profit"
    assert [(e["kind"], e["qty"]) for e in pos.data["exits"]] == [("tp", 0.012)]
    assert pos.realized_pnl == pytest.approx(16.98, abs=0.02) and pos.error is None
    assert _live(fake) == []                               # the stop did not stay behind
    await broker.close()


async def test_stop_that_stays_unknown_by_id_does_not_hide_a_position_closed_on_the_exchange() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    sl_id = pos.data["sl_order_id"]
    fake.fail(lambda m, p, q: m == "GET" and p == "/fapi/v1/algoOrder" and q.get("algoId") == sl_id,
              ApiError(-2013, "Order does not exist."), times=100)
    fake.algo_history_works = False
    fake.pos["BTCUSDT"]["amt"] = 0.0                       # closed by hand on the exchange; stop and TP still live
    n = len(fake.log)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.data["flat_seen"] == 1
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["closed_reason"] == "flat_on_exchange"
    assert _posts(fake, n) == [] and _live(fake) == [] and pos.realized_pnl is None
    await broker.close()


async def test_replaced_stop_the_exchange_claims_not_to_know_is_not_forgotten_while_it_is_listed_as_open() -> None:
    """After a partial take profit the stop is re-sized: new stop first, then the old one is cancelled. If the
    exchange answers "unknown order" to the cancel AND to the lookup while the old stop is still listed as open,
    it must stay tracked until it is really gone (else two stops stay on the coin)."""
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan("BTC", Side.LONG, 73300.0, [(75000.0, 0.5), (76000.0, 0.5)]))
    old = pos.data["sl_order_id"]
    fake.price["BTCUSDT"] = (75010.0, 75010.1)
    fake.trigger(pos.data["tp_order_ids"][0]["id"], price=75005.0)
    unknown = lambda m, p, q: p == "/fapi/v1/algoOrder" and q.get("algoId") == old and m in ("GET", "DELETE")  # noqa: E731
    fake.fail(lambda m, p, q: unknown(m, p, q) and m == "GET", ApiError(-2013, "Order does not exist."), times=100)
    fake.fail(lambda m, p, q: unknown(m, p, q) and m == "DELETE", ApiError(-2011, "Unknown order sent."), times=100)
    fake.algo_history_works = False
    pos = await broker.sync_position(pos)
    assert pos.remaining_quantity == 0.006 and pos.data["sl_order_id"] != old and pos.data["protected_qty"] == 0.006
    assert [a for a in _live(fake) if a[0] == "STOP_MARKET"] == [("STOP_MARKET", "0.006", "73300"),
                                                                  ("STOP_MARKET", "0.012", "73300")]
    assert [s["id"] for s in pos.data["stale_orders"]] == [old]            # still ours to cancel
    assert "eski stop emri" in (pos.error or "")                            # and the owner is told
    fake.hooks.clear()                                                      # the exchange answers normally again
    pos = await broker.sync_position(pos)
    assert pos.data["stale_orders"] == [] and pos.error is None
    assert _live(fake) == [("STOP_MARKET", "0.006", "73300"), ("TAKE_PROFIT_MARKET", "0.006", "76000")]
    await broker.close()


async def test_stop_that_cannot_be_confirmed_for_three_passes_is_reported_and_nothing_is_sent() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    sl_id = pos.data["sl_order_id"]
    fake.fail(lambda m, p, q: m == "GET" and p == "/fapi/v1/algoOrder" and q.get("algoId") == sl_id,
              ApiError(-2013, "Order does not exist."), times=10)
    fake.algo_history_works = False
    fake.fail(lambda m, p, q: p == "/fapi/v1/openAlgoOrders", ApiError(-1000, "An unknown error occured."), times=10)
    n = len(fake.log)
    for expected in (None, None, "binance: stop emri (9001) borsada doğrulanamıyor: emir numarasıyla bulunamadı ve "
                                 "açık emirler okunamıyor. BTC/USDT:USDT stopunu borsada kontrol edin."):
        pos = await broker.sync_position(pos)
        assert pos.error == expected
    assert pos.data["sl_order_id"] == sl_id and _writes(fake, n) == [] and _live(fake) == BTC_STOP_AND_TP
    fake.hooks.clear()
    fake.algo_history_works = True
    pos = await broker.sync_position(pos)
    assert pos.error is None and "sl_unsure" not in pos.data
    await broker.close()


async def test_stop_cancelled_on_the_exchange_is_replaced_exactly_once() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    old = pos.data["sl_order_id"]
    fake.algos[old]["algoStatus"] = "CANCELED"            # e.g. cancelled by hand
    pos = await broker.sync_position(pos)
    assert pos.data["sl_order_id"] != old and not pos.data.get("unprotected") and not pos.data.get("stale_orders")
    assert _live(fake) == BTC_STOP_AND_TP
    pos = await broker.sync_position(pos)
    assert _live(fake) == BTC_STOP_AND_TP
    await broker.close()


async def test_no_stop_at_all_is_never_described_as_the_old_stop_being_active() -> None:
    """The stop was cancelled on the exchange, a new one cannot be placed and the emergency close failed: the
    position is open WITHOUT a stop. A later partial close must not report 'the old stop is active'."""
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    fake.algos[pos.data["sl_order_id"]]["algoStatus"] = "CANCELED"            # e.g. cancelled by hand
    fake.fail(lambda m, p, q: m == "POST" and p == "/fapi/v1/algoOrder" and q.get("type") == "STOP_MARKET",
              ApiError(-4014, "Price not increased by tick size."), times=100)
    fake.fail(lambda m, p, q: m == "POST" and p == "/fapi/v1/order" and q.get("reduceOnly") == "true",
              ApiError(-4003, "Quantity less than zero."))
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.data["unprotected"] and "KRİTİK" in (pos.error or "")
    assert [a for a in _live(fake) if a[0] == "STOP_MARKET"] == []
    pos = await broker.close_position(pos, 0.5)                               # the owner closes half
    assert pos.remaining_quantity == 0.006 and pos.data["unprotected"]
    assert "eski stop aktif" not in (pos.error or "")
    assert "KRİTİK" in (pos.error or "") and "0.006 STOPSUZ açık" in (pos.error or "")
    await broker.close()


async def test_stop_whose_reply_was_lost_is_adopted_not_duplicated() -> None:
    broker, fake = await _connected()

    def accepted_then_timeout(method: str, path: str, q: dict[str, str]) -> None:
        fake.new_algo(q)
        raise ccxt.RequestTimeout("binanceusdm POST https://fapi.binance.com/fapi/v1/algoOrder")

    fake.fail(lambda m, p, q: m == "POST" and p == "/fapi/v1/algoOrder" and q.get("type") == "STOP_MARKET",
              accepted_then_timeout)
    _, pos = await _open(broker, _plan(**BTC_LONG))
    assert _live(fake) == BTC_STOP_AND_TP
    assert pos.data["sl_order_id"] == str(next(a["algoId"] for a in fake.algos.values() if a["orderType"] == "STOP_MARKET"))
    assert not pos.data.get("lost_orders") and not pos.data.get("unprotected")
    await broker.close()


async def test_take_profit_whose_reply_was_lost_is_adopted_not_duplicated() -> None:
    broker, fake = await _connected()

    def accepted_then_timeout(method: str, path: str, q: dict[str, str]) -> None:
        fake.new_algo(q)
        raise ccxt.RequestTimeout("binanceusdm POST https://fapi.binance.com/fapi/v1/algoOrder")

    fake.fail(lambda m, p, q: m == "POST" and p == "/fapi/v1/algoOrder" and q.get("type") == "TAKE_PROFIT_MARKET",
              accepted_then_timeout)
    _, pos = await _open(broker, _plan(**BTC_LONG))
    assert _live(fake) == BTC_STOP_AND_TP and pos.error is None
    assert [t["status"] for t in pos.data["tp_order_ids"]] == ["open"]
    pos = await broker.sync_position(pos)
    assert _live(fake) == BTC_STOP_AND_TP
    await broker.close()


async def test_stop_placed_but_unconfirmed_is_cancelled_once_the_retry_has_placed_another() -> None:
    """Reply lost AND the lookup fails (an outage): the retry places a second stop; the first one is found by
    its client id on the next sync and cancelled, so one stop is left."""
    broker, fake = await _connected()

    def accepted_then_timeout(method: str, path: str, q: dict[str, str]) -> None:
        fake.new_algo(q)
        raise ccxt.RequestTimeout("binanceusdm POST https://fapi.binance.com/fapi/v1/algoOrder")

    fake.fail(lambda m, p, q: m == "POST" and p == "/fapi/v1/algoOrder" and q.get("type") == "STOP_MARKET",
              accepted_then_timeout)
    fake.fail(lambda m, p, q: m == "GET" and p == "/fapi/v1/algoOrder" and "clientAlgoId" in q,
              ccxt.NetworkError("binanceusdm GET algoOrder connection reset"))
    _, pos = await _open(broker, _plan(**BTC_LONG))
    assert pos.status is PositionStatus.OPEN and not pos.data.get("unprotected")
    assert [lo["cid"] for lo in pos.data["lost_orders"]] == ["ct12ab34cds1"]
    assert len([a for a in _live(fake) if a[0] == "STOP_MARKET"]) == 2          # for one monitor pass
    pos = await broker.sync_position(pos)
    assert pos.data["lost_orders"] == [] and _live(fake) == BTC_STOP_AND_TP
    tracked = next(a for a in fake.algos.values() if a["algoStatus"] == "NEW" and a["orderType"] == "STOP_MARKET")
    assert pos.data["sl_order_id"] == str(tracked["algoId"])
    await broker.close()


async def test_stop_that_never_reached_the_exchange_is_forgotten_after_three_passes() -> None:
    broker, fake = await _connected()
    fake.fail(lambda m, p, q: m == "POST" and p == "/fapi/v1/algoOrder" and q.get("type") == "STOP_MARKET",
              ccxt.RequestTimeout("binanceusdm POST https://fapi.binance.com/fapi/v1/algoOrder"))
    _, pos = await _open(broker, _plan(**BTC_LONG))
    assert [lo["cid"] for lo in pos.data["lost_orders"]] == ["ct12ab34cds1"] and _live(fake) == BTC_STOP_AND_TP
    for left in (1, 1, 0):
        pos = await broker.sync_position(pos)
        assert len(pos.data["lost_orders"]) == left
    assert _live(fake) == BTC_STOP_AND_TP
    await broker.close()


async def test_unconfirmed_stop_is_adopted_when_the_position_has_no_other() -> None:
    """Both attempts end without an answer and the emergency close fails too (outage). When the exchange answers
    again the stop that did arrive is adopted: no second stop, no market close."""
    broker, fake = await _connected()

    def accepted_then_timeout(method: str, path: str, q: dict[str, str]) -> None:
        fake.new_algo(q)
        raise ccxt.RequestTimeout("binanceusdm POST https://fapi.binance.com/fapi/v1/algoOrder")

    is_stop = lambda m, p, q: m == "POST" and p == "/fapi/v1/algoOrder" and q.get("type") == "STOP_MARKET"  # noqa: E731
    fake.fail(is_stop, accepted_then_timeout)
    fake.fail(is_stop, ccxt.RequestTimeout("binanceusdm POST algoOrder"))
    fake.fail(lambda m, p, q: m == "GET" and p == "/fapi/v1/algoOrder" and "clientAlgoId" in q,
              ccxt.NetworkError("binanceusdm GET algoOrder connection reset"), times=2)
    fake.fail(lambda m, p, q: m == "POST" and p == "/fapi/v1/order" and q.get("reduceOnly") == "true",
              ccxt.NetworkError("binanceusdm POST order connection reset"))
    _, pos = await _open(broker, _plan(**BTC_LONG))
    assert pos.status is PositionStatus.OPEN and pos.data["unprotected"] and "KRİTİK" in (pos.error or "")
    n = len(fake.log)
    pos = await broker.sync_position(pos)
    assert not pos.data["unprotected"] and "KRİTİK" not in (pos.error or "")
    assert [p for p, q in _posts(fake, n) if q.get("type") == "STOP_MARKET"] == []      # adopted, not re-sent
    assert fake.pos["BTCUSDT"]["amt"] == 0.012 and _live(fake) == BTC_STOP_AND_TP
    await broker.close()


@pytest.mark.parametrize("code", [-1008, -1001])
async def test_server_busy_after_protection_was_placed_keeps_the_new_state(code: int) -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG, entry=73900.0, entry_type=EntryType.LIMIT))
    fake.price["BTCUSDT"] = (73899.9, 73900.0)
    fake.fill_limit(pos.data["entry_order_id"])
    fake.fail(lambda m, p, q: m == "GET" and p == "/fapi/v1/algoOrder", ApiError(code, "server busy"))
    pos = await broker.sync_position(pos)         # must not raise: the monitor would keep the old position
    assert pos.status is PositionStatus.OPEN and pos.data["sl_order_id"]
    pos = await broker.sync_position(pos)
    assert _live(fake) == [("STOP_MARKET", "0.014", "73300"), ("TAKE_PROFIT_MARKET", "0.014", "75500")]
    await broker.close()


async def test_any_error_after_protection_was_placed_keeps_the_new_state_and_is_reported() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG, entry=73900.0, entry_type=EntryType.LIMIT))
    fake.price["BTCUSDT"] = (73899.9, 73900.0)
    fake.fill_limit(pos.data["entry_order_id"])
    fake.fail(lambda m, p, q: m == "GET" and p == "/fapi/v1/algoOrder",
              ApiError(-1102, "Mandatory parameter 'algoid' was not sent, was empty/null, or malformed."))
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.data["sl_order_id"]
    assert pos.error is not None and pos.error.startswith("binance: durum güncellenemedi: binanceusdm")
    pos = await broker.sync_position(pos)
    assert pos.error is None
    assert _live(fake) == [("STOP_MARKET", "0.014", "73300"), ("TAKE_PROFIT_MARKET", "0.014", "75500")]
    await broker.close()


async def test_error_without_any_change_is_still_raised_for_the_monitor() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    fake.fail(lambda m, p, q: m == "GET" and p == "/fapi/v1/algoOrder",
              ApiError(-1102, "Mandatory parameter 'algoid' was not sent, was empty/null, or malformed."))
    with pytest.raises(BrokerError, match="durum güncellenemedi"):
        await broker.sync_position(pos)
    await broker.close()


async def test_cancel_answered_unknown_order_for_a_live_stop_is_retried_until_it_is_gone() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    tp_id = pos.data["tp_order_ids"][0]["id"]
    fake.price["BTCUSDT"] = (75510.0, 75510.1)
    fake.trigger(tp_id, price=75505.0)
    fake.fail(lambda m, p, q: m == "DELETE" and p == "/fapi/v1/algoOrder", ApiError(-2011, "Unknown order sent."))
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.data["closing"] and pos.remaining_quantity == 0.0
    assert _live(fake) == [("STOP_MARKET", "0.012", "73300")]          # still there, and not forgotten
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.error is None and _live(fake) == []
    assert pos.realized_pnl == pytest.approx(16.98, abs=0.02)
    await broker.close()


# --------------------------------------------------------------------------- N5: flat on the exchange
async def test_flat_on_venue_without_a_readable_stop_closes_instead_of_alarming_forever() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    fake.price["BTCUSDT"] = (73290.0, 73290.1)
    fake.trigger(pos.data["sl_order_id"], price=73288.0)
    fake.completed_algo_query_missing = True
    fake.algo_history_works = False               # the stop cannot be read at all, the position is gone
    n = len(fake.log)
    for _ in range(3):
        pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["closed_reason"] == "flat_on_exchange"
    assert pos.realized_pnl is None               # N8: unknown, not 0.0
    assert _posts(fake, n) == []                  # no stop / emergency-close attempts for a position that is gone
    assert pos.error == ("binance: pozisyon borsada kapalı bulundu; kalan emirler iptal edildi. Kapanış botun "
                         "emirleriyle olmadığı için kâr/zarar bilinmiyor.")
    assert _live(fake) == []
    await broker.close()


async def test_position_and_stop_removed_by_hand_close_the_trade_without_new_orders() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    fake.algos[pos.data["sl_order_id"]]["algoStatus"] = "CANCELED"
    fake.pos["BTCUSDT"]["amt"] = 0.0
    n = len(fake.log)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.data["flat_seen"] == 1 and _posts(fake, n) == []
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["closed_reason"] == "flat_on_exchange"
    assert pos.realized_pnl is None and _posts(fake, n) == [] and _live(fake) == []
    await broker.close()


async def test_flat_close_that_needs_a_second_pass_for_the_leftover_orders_keeps_its_note() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    fake.pos["BTCUSDT"]["amt"] = 0.0
    pos = await broker.sync_position(pos)
    fake.fail(lambda m, p, q: m == "DELETE" and p == "/fapi/v1/algoOrder", ApiError(-4003, "Quantity less than zero."))
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.data["closing"] and len(_live(fake)) == 1
    assert pos.error == "binance: pozisyon kapandı ama kalan emirler iptal edilemedi; tekrar denenecek."
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.realized_pnl is None and _live(fake) == []
    assert pos.error == ("binance: pozisyon borsada kapalı bulundu; kalan emirler iptal edildi. Kapanış botun "
                         "emirleriyle olmadığı için kâr/zarar bilinmiyor.")
    await broker.close()


async def test_unprotected_position_that_is_gone_is_closed_not_alarmed_about() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    pos.data.update(unprotected=True, sl_order_id=None)       # e.g. after a failed stop placement
    pos.error = "KRİTİK binance: stop emri yerleştirilemedi ve pozisyon kapatılamadı!"
    fake.pos["BTCUSDT"]["amt"] = 0.0
    n = len(fake.log)
    pos = await broker.sync_position(pos)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and not pos.data["unprotected"] and "KRİTİK" not in (pos.error or "")
    assert _posts(fake, n) == []
    await broker.close()


async def test_position_still_there_gets_its_stop_back() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    fake.algos[pos.data["sl_order_id"]]["algoStatus"] = "CANCELED"
    pos.data.update(unprotected=True, sl_order_id=None)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and not pos.data["unprotected"] and _live(fake) == BTC_STOP_AND_TP
    await broker.close()


# --------------------------------------------------------------------------- N6: partial stop fill
async def test_partially_executed_stop_books_what_the_child_executed_and_closes_the_rest() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    fake.price["BTCUSDT"] = (73290.0, 73290.1)
    fake.trigger(pos.data["sl_order_id"], price=73288.0, fill_fraction=0.5)
    pos = await broker.sync_position(pos)
    exits = [(e["kind"], e["qty"]) for e in pos.data["exits"]]
    assert exits == [("sl", 0.006), ("emergency", 0.006)]
    assert fake.pos["BTCUSDT"]["amt"] == 0.0      # the remaining half was closed, not forgotten
    assert pos.status is PositionStatus.CLOSED and _live(fake) == []
    await broker.close()


async def test_partially_executed_stop_with_the_price_back_above_it_leaves_the_rest_open_with_a_new_stop() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    first_stop = pos.data["sl_order_id"]
    fake.price["BTCUSDT"] = (73290.0, 73290.1)
    fake.trigger(first_stop, price=73288.0, fill_fraction=0.5)
    fake.price["BTCUSDT"] = (73600.0, 73600.1)            # the market is back above the stop before the next pass
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == 0.006
    assert [(e["kind"], e["qty"]) for e in pos.data["exits"]] == [("sl", 0.006)]
    assert pos.data["sl_order_id"] not in (None, first_stop) and pos.data["protected_qty"] == 0.006
    assert not pos.data.get("unprotected") and fake.pos["BTCUSDT"]["amt"] == 0.006
    assert _live(fake) == [("STOP_MARKET", "0.006", "73300"), ("TAKE_PROFIT_MARKET", "0.006", "75500")]
    assert pos.error == "binance: stop emri kısmen gerçekleşti (0.006); kalan 0.006 için yeni stop konuldu."
    # the rest is tracked: its own stop closes it later and both fills are in the result
    fake.price["BTCUSDT"] = (73290.0, 73290.1)
    fake.trigger(pos.data["sl_order_id"], price=73285.0)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["closed_reason"] == "stop" and _live(fake) == []
    assert [(e["kind"], e["qty"]) for e in pos.data["exits"]] == [("sl", 0.006), ("sl", 0.006)]
    assert pos.realized_pnl == pytest.approx(-9.626, abs=5e-3)
    await broker.close()


async def test_stop_whose_market_order_is_still_working_is_not_booked_yet() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    sl_id = pos.data["sl_order_id"]
    fake.price["BTCUSDT"] = (73290.0, 73290.1)
    algo = fake.trigger(sl_id, price=73288.0, fill_fraction=0.5)
    child = fake.orders[algo["actualOrderId"]]
    child["status"] = "PARTIALLY_FILLED"          # the triggered market order has not finished
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.data["exits"] == [] and pos.data["sl_order_id"] == sl_id
    fake._fill(child, 0.006, 73288.0)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and [(e["kind"], e["qty"]) for e in pos.data["exits"]] == [("sl", 0.012)]
    await broker.close()


async def test_stop_whose_market_order_cannot_be_read_right_now_is_not_booked_on_a_guess() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    fake.price["BTCUSDT"] = (73290.0, 73290.1)
    algo = fake.trigger(pos.data["sl_order_id"], price=73288.0, fill_fraction=0.5)
    fake.fail(lambda m, p, q: m == "GET" and p == "/fapi/v1/order" and q.get("orderId") == algo["actualOrderId"],
              ccxt.RequestTimeout("binanceusdm GET order"))
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.data["exits"] == []
    pos = await broker.sync_position(pos)
    assert [(e["kind"], e["qty"]) for e in pos.data["exits"]][0] == ("sl", 0.006)
    await broker.close()


# --------------------------------------------------------------------------- N7: clock offset
@pytest.mark.parametrize("ahead_ms", [4000.0, -15000.0])
async def test_clock_offset_is_measured_before_the_first_signed_request_and_applied(ahead_ms: float) -> None:
    broker, fake = make_binance()
    fake.local_clock_ahead_ms = ahead_ms          # this machine's clock minus the exchange's
    await broker.connect()
    first_signed = next(i for i, (m, p, _) in enumerate(fake.log) if (m, p) == fake.signed_log[0])
    assert ("GET", "/fapi/v1/time") in [(m, p) for m, p, _ in fake.log[:first_signed]]
    assert broker.exchange.options["timeDifference"] == pytest.approx(ahead_ms, abs=500)
    assert await broker.get_equity() == pytest.approx(46.90)
    _, pos = await _open(broker, _plan(**BTC_LONG))        # every signed request of an open passes the check
    assert pos.status is PositionStatus.OPEN and _live(fake) == BTC_STOP_AND_TP
    await broker.close()


async def test_explicit_option_in_the_config_wins_over_the_default() -> None:
    broker, fake = make_binance(options={"adjustForTimeDifference": False})
    assert broker.exchange.options["adjustForTimeDifference"] is False
    fake.local_clock_ahead_ms = 4000.0
    with pytest.raises(BrokerError, match="-1021"):
        await broker.connect()
    assert ("GET", "/fapi/v1/time") not in [(m, p) for m, p, _ in fake.log]
    await broker.close()


async def test_clock_that_changes_while_running_is_measured_again() -> None:
    broker, fake = make_binance()
    fake.local_clock_ahead_ms = 200.0
    await broker.connect()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    fake.local_clock_ahead_ms = 5000.0            # the clock ran away: the offset measured at connect is 4.8 s off
    n = len(fake.log)
    assert await broker.get_equity() == pytest.approx(46.90, abs=1.0)
    assert [(m, p) for m, p, _ in fake.log[n:]] == [("GET", "/fapi/v3/account"), ("GET", "/fapi/v1/time"),
                                                    ("GET", "/fapi/v3/account")]
    fake.local_clock_ahead_ms = -12000.0          # e.g. Windows set the clock back
    n = len(fake.log)
    assert (await broker.sync_position(pos)).status is PositionStatus.OPEN      # -1021: transient, offset re-measured
    assert ("GET", "/fapi/v1/time") in [(m, p) for m, p, _ in fake.log[n:]]
    fake.price["BTCUSDT"] = (75510.0, 75510.1)
    fake.trigger(pos.data["tp_order_ids"][0]["id"], price=75505.0)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["closed_reason"] == "take_profit"
    await broker.close()


# --------------------------------------------------------------------------- N8: unknown PnL
async def test_position_closed_by_hand_is_recorded_with_unknown_pnl_not_zero() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    fake.pos["BTCUSDT"]["amt"] = 0.0              # closed on the exchange by hand; our stop and TP are still there
    pos = await broker.sync_position(pos)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["closed_reason"] == "flat_on_exchange"
    assert pos.realized_pnl is None
    assert _live(fake) == []                      # the leftover stop and take profit are cancelled
    await broker.close()


async def test_rest_closed_outside_the_bot_keeps_the_known_part_of_the_pnl_and_says_so() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan("BTC", Side.LONG, 73300.0, [(75000.0, 0.5), (76000.0, 0.5)]))
    fake.price["BTCUSDT"] = (75010.0, 75010.1)
    fake.trigger(pos.data["tp_order_ids"][0]["id"], price=75005.0)
    pos = await broker.sync_position(pos)
    assert pos.remaining_quantity == 0.006
    fake.pos["BTCUSDT"]["amt"] = 0.0              # the rest is closed by hand on the exchange
    pos = await broker.sync_position(pos)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["closed_reason"] == "flat_on_exchange"
    assert pos.realized_pnl == pytest.approx(5.27, abs=0.05) and pos.data["closed_outside"] == 0.006
    assert pos.error is not None and "0.006 botun emirleri dışında kapandı" in pos.error
    assert _live(fake) == []
    await broker.close()
