"""Pre-flight batch 2, Binance adapter (see also test_ccxt_binance_real.py for N1..N8):

N9   contract prefixes: "1M" (1MBABYDOGE) next to 1000 ... 1000000
N11  the read-only helpers health_problem() and preflight_report(), on the real ccxt code (HTTP faked)
     pure helpers of N1 / N2, other exchange families, and what the storage does with an unknown PnL (N8)
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import ccxt
import pytest

from binance_rest_fake import ApiError, FakeBinance, make_binance
from copytrader.brokers import ccxt_broker
from copytrader.brokers.base import BrokerError
from copytrader.brokers.ccxt_broker import CcxtBroker, _base_variants, find_market
from copytrader.models import (
    BrokerPosition,
    EntryType,
    MarketType,
    OpenRequest,
    PositionStatus,
    Severity,
    Side,
    TakeProfit,
    TradePlan,
)
from copytrader.risk import guards
from copytrader.storage.db import Database
from test_ccxt_broker import make_broker, make_plan, open_limit, open_market


def _swap(base: str) -> dict[str, Any]:
    symbol = f"{base}/USDT:USDT"
    return {"id": f"{base}USDT", "symbol": symbol, "base": base, "quote": "USDT", "settle": "USDT", "type": "swap",
            "swap": True, "spot": False, "linear": True, "active": True, "contractSize": 1.0,
            "precision": {"amount": 1, "price": 0.0000001}, "limits": {"amount": {"min": 1}, "cost": {"min": 5.0}}}


def _find(markets: dict[str, Any], base: str) -> tuple[str, float] | None:
    found = find_market(markets, base_asset=base, quote_asset=None, default_quote="USDT", market_type="swap")
    return (found[0]["symbol"], found[1]) if found else None


def _writes(fake: FakeBinance, start: int = 0) -> list[tuple[str, str]]:
    return [(m, p) for m, p, _ in fake.log[start:] if m != "GET"]


async def _connected(**kw: Any) -> tuple[CcxtBroker, FakeBinance]:
    broker, fake = make_binance(**kw)
    fake.local_clock_ahead_ms = 50.0                # a healthy clock (the fake then checks every signed request)
    await broker.connect()
    return broker, fake


def _btc_long(plan_id: str = "12ab34cd") -> TradePlan:
    return TradePlan(
        plan_id=plan_id, base_asset="BTC", quote_asset=None, market_type=MarketType.PERP, side=Side.LONG,
        entry_type=EntryType.MARKET, entry_price=None, stop_loss=73300.0,
        take_profits=[TakeProfit(price=75500.0, close_fraction=1.0)], risk_r=1.0, risk_pct=2.0,
        source_chat_id=-100, source_message_ids=[1], created_at=datetime(2026, 11, 12, tzinfo=timezone.utc),
    )


async def _open_btc(broker: CcxtBroker) -> BrokerPosition:
    plan = _btc_long()
    sym = await broker.resolve_symbol("BTC")
    assert sym is not None
    quote = await broker.get_quote(sym)
    size = await broker.size_position(sym, plan.side, quote.ask, plan.stop_loss, 10.0)
    return await broker.open_position(OpenRequest(
        plan=plan, symbol=sym, quantity=size.quantity, entry_type=EntryType.MARKET, stop_loss=plan.stop_loss,
        take_profits=plan.take_profits, leverage=size.leverage, client_tag="ct12ab34cd"))


# --------------------------------------------------------------------------- N9: contract prefixes
def test_1m_prefix_is_found_from_the_plain_coin_name_with_its_price_multiplier() -> None:
    markets = {m["symbol"]: m for m in (_swap("1MBABYDOGE"), _swap("1000PEPE"), _swap("1000000MOG"), _swap("ETH"))}
    assert _find(markets, "BABYDOGE") == ("1MBABYDOGE/USDT:USDT", 1_000_000.0)
    assert _find(markets, "1MBABYDOGE") == ("1MBABYDOGE/USDT:USDT", 1.0)
    assert _find(markets, "1000000BABYDOGE") == ("1MBABYDOGE/USDT:USDT", 1.0)      # the same contract, spelled in digits
    assert _find(markets, "1000BABYDOGE") == ("1MBABYDOGE/USDT:USDT", 1000.0)
    assert _find(markets, "1MMOG") == ("1000000MOG/USDT:USDT", 1.0)
    # unchanged behaviour
    assert _find(markets, "PEPE") == ("1000PEPE/USDT:USDT", 1000.0)
    assert _find(markets, "MOG") == ("1000000MOG/USDT:USDT", 1_000_000.0)
    assert _find(markets, "ETH") == ("ETH/USDT:USDT", 1.0)
    assert _find(markets, "DOGE") is None                                          # BABYDOGE is another coin


def test_prefixed_signal_resolves_to_the_plain_contract_with_the_inverse_multiplier() -> None:
    markets = {m["symbol"]: m for m in (_swap("BABYDOGE"), _swap("PEPE"), _swap("1INCH"), _swap("1MIL"))}
    assert _find(markets, "1MBABYDOGE") == ("BABYDOGE/USDT:USDT", pytest.approx(1e-6))
    assert _find(markets, "1000PEPE") == ("PEPE/USDT:USDT", pytest.approx(1e-3))
    assert _find(markets, "1INCH") == ("1INCH/USDT:USDT", 1.0)                     # a name that starts with a digit
    assert _find(markets, "1MIL") == ("1MIL/USDT:USDT", 1.0)                       # the name as written always wins


def test_base_variants_order_and_values() -> None:
    assert _base_variants("PEPE") == [("PEPE", 1.0), ("1000PEPE", 1000.0), ("10000PEPE", 10000.0),
                                      ("100000PEPE", 100000.0), ("1000000PEPE", 1000000.0), ("1MPEPE", 1000000.0)]
    assert _base_variants("1MBABYDOGE") == [
        ("1MBABYDOGE", 1.0), ("BABYDOGE", 1e-6), ("1000BABYDOGE", 1e-3), ("10000BABYDOGE", 1e-2),
        ("100000BABYDOGE", 1e-1), ("1000000BABYDOGE", 1.0)]
    assert ("1M0X", 1_000_000.0) in _base_variants("0X") and _base_variants("1M0X")[1][0] != "0X"


async def test_1m_contract_is_resolved_on_real_ccxt_markets() -> None:
    broker, fake = make_binance()
    fake.add_symbol("1MBABYDOGEUSDT", "DOGEUSDT", bid=0.0012, ask=0.00121, max_lev=50)
    await broker.connect()
    sym = await broker.resolve_symbol("BABYDOGE", None, MarketType.PERP, "BABYDOGEUSDT.P")
    assert sym is not None and sym.broker_symbol == "1MBABYDOGE/USDT:USDT"
    assert sym.extra["price_multiplier"] == 1_000_000.0 and sym.base_asset == "BABYDOGE"
    same = await broker.resolve_symbol("1MBABYDOGE")
    assert same is not None and same.broker_symbol == sym.broker_symbol and same.extra["price_multiplier"] == 1.0
    await broker.close()


# --------------------------------------------------------------------------- N11: health_problem
async def test_health_is_none_after_one_signed_read_and_changes_nothing() -> None:
    broker, fake = await _connected()
    n, signed = len(fake.log), len(fake.signed_log)
    assert await broker.health_problem() is None
    assert [(m, p) for m, p, _ in fake.log[n:]] == [("GET", "/fapi/v3/account")]
    assert fake.signed_log[signed:] == [("GET", "/fapi/v3/account")]
    await broker.close()


@pytest.mark.parametrize("error,expected", [
    (ApiError(-2015, "Invalid API-key, IP, or permissions for action, request ip: 203.0.113.7", 401),
     "API anahtarı reddedildi: anahtar silinmiş / geçersiz, bu bilgisayarın IP adresi izinli değil ya da vadeli işlem "
     "yetkisi yok"),
    (ApiError(-1022, "Signature for this request is not valid."), "API gizli anahtarı (secret) yanlış: imza reddedildi"),
    (ApiError(-1021, "Timestamp for this request is outside of the recvWindow."),
     "bu bilgisayarın saati borsa saatiyle uyuşmuyor (Windows saat eşitlemesini kontrol edin)"),
    (ApiError(-1003, "Too many requests; current limit of IP is 2400 requests per minute.", 429),
     "borsa çok fazla istek nedeniyle geçici olarak engelledi"),
    (ccxt.RequestTimeout("binanceusdm GET https://fapi.binance.com/fapi/v3/account"),
     "borsa zamanında yanıt vermedi (bağlantı çok yavaş)"),
    (ccxt.OnMaintenance("binanceusdm System is under maintenance."), "borsa bakımda ya da ulaşılamıyor"),
])
async def test_health_names_the_problem_in_turkish(error: Exception, expected: str) -> None:
    broker, fake = await _connected()               # connect has already made a signed call that worked
    fake.fail(lambda m, p, q: p == "/fapi/v3/account", error, times=5)
    assert await broker.health_problem() == expected
    await broker.close()


async def test_health_reports_a_refused_key_also_before_any_signed_call_has_worked() -> None:
    broker, fake = make_binance()                   # not connected: ccxt maps -2015 to another class here
    fake.fail(lambda m, p, q: p == "/fapi/v3/account",
              ApiError(-2015, "Invalid API-key, IP, or permissions for action.", 401), times=5)
    assert (await broker.health_problem() or "").startswith("API anahtarı reddedildi")
    await broker.close()


async def test_health_network_error_and_unexpected_error_never_raise() -> None:
    broker, fake = await _connected()
    fake.fail(lambda m, p, q: p == "/fapi/v3/account", ccxt.NetworkError("binanceusdm GET account connection reset"))
    assert (await broker.health_problem() or "").startswith("borsaya bağlanılamıyor (internet / ağ sorunu)")
    fake.fail(lambda m, p, q: p == "/fapi/v3/account", ValueError("boom"))
    assert await broker.health_problem() == "beklenmeyen hata: boom"
    assert await broker.health_problem() is None
    await broker.close()


async def test_health_gives_up_after_the_time_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    broker, fake = await _connected()
    monkeypatch.setattr(ccxt_broker, "_HEALTH_TIMEOUT_SEC", 0.05)

    async def hang(*_: Any, **__: Any) -> Any:
        await asyncio.sleep(30)

    monkeypatch.setattr(broker.exchange, "fetch_balance", hang)
    started = asyncio.get_running_loop().time()
    text = await broker.health_problem()
    assert asyncio.get_running_loop().time() - started < 2
    assert text == "borsa 0 saniye içinde yanıt vermedi (bağlantı yok ya da çok yavaş)"
    await broker.close()


async def test_health_measures_the_clock_again_before_calling_it_a_clock_problem() -> None:
    broker, fake = make_binance()
    fake.local_clock_ahead_ms = 100.0
    await broker.connect()
    fake.local_clock_ahead_ms = 7000.0              # the clock changed while the bot was running
    n = len(fake.log)
    assert await broker.health_problem() is None
    assert [(m, p) for m, p, _ in fake.log[n:]] == [("GET", "/fapi/v3/account"), ("GET", "/fapi/v1/time"),
                                                    ("GET", "/fapi/v3/account")]
    await broker.close()


async def test_health_without_a_connection_and_on_other_exchanges() -> None:
    broker, _ = make_binance()
    broker.exchange = None
    assert await broker.health_problem() == "borsa bağlantısı kurulmamış"
    other, fake = make_broker("bybit")              # the in-memory exchange of test_ccxt_broker.py
    assert await other.health_problem() is None
    fake.fail_when(lambda method, _: method == "fetch_balance", ccxt.AuthenticationError("bybit invalid api key"))
    assert (await other.health_problem() or "").startswith("API anahtarı reddedildi (yetki ya da IP kısıtı)")


# --------------------------------------------------------------------------- N11: preflight_report
async def test_preflight_report_of_a_clean_account_is_all_tamam_and_read_only() -> None:
    broker, fake = make_binance()
    fake.add_symbol("1MBABYDOGEUSDT", "DOGEUSDT", bid=0.0012, ask=0.00121, max_lev=50)
    fake.local_clock_ahead_ms = 120.0
    await broker.connect()
    n = len(fake.log)
    lines = await broker.preflight_report(["BTC", "DOGE", "BABYDOGE"])
    assert all(line.startswith("TAMAM ") for line in lines), lines
    assert lines[1:] == [
        "TAMAM Pozisyon modu: tek yönlü (ayarla uyumlu).",
        "TAMAM Çoklu varlık modu kapalı.",
        "TAMAM BTC: BTC/USDT:USDT sözleşmesi; en küçük emir 0.001 BTC ve 50 USDT; borsanın kaldıraç tablosunda en yüksek 125x "
        "(50000 USDT büyüklüğe kadar).",
        "TAMAM DOGE: DOGE/USDT:USDT sözleşmesi; en küçük emir 1 DOGE ve 5 USDT; borsanın kaldıraç tablosunda en yüksek 75x "
        "(50000 USDT büyüklüğe kadar), ayardaki 125x yerine bu kullanılır (aynı işlem daha çok teminat ister).",
        "TAMAM BABYDOGE: 1MBABYDOGE/USDT:USDT sözleşmesi (fiyatlar 1000000 ile çarpılır); en küçük emir 1 1MBABYDOGE "
        "ve 5 USDT; borsanın kaldıraç tablosunda en yüksek 50x (50000 USDT büyüklüğe kadar), ayardaki 125x yerine bu kullanılır (aynı "
        "işlem daha çok teminat ister).",
        "TAMAM Not: kaldıraç tablosu hesaba özel sınırları (yeni açılmış hesap, kimlik doğrulama, ülke) göstermez. "
        "Borsa daha azına izin verirse bot ilk işlemde kaldıracı kendisi düşürür; bu, işlem raporundaki uyarıda "
        "yazar.",
        "TAMAM Hesapta açık pozisyon yok.",
        "TAMAM Hesapta bekleyen emir yok (normal ve koşullu).",
    ]
    assert lines[0].startswith("TAMAM Saat farkı: bu bilgisayarın saati borsadan ") and lines[0].endswith(
        " ms ileride (bot bu farkı kendisi düzeltiyor).")
    # strictly read-only: GET requests only (no leverage / margin-mode change, no order), on these endpoints
    assert _writes(fake, n) == []
    assert {p for _, p, _ in fake.log[n:]} == {
        "/fapi/v1/time", "/fapi/v1/positionSide/dual", "/fapi/v1/multiAssetsMargin", "/fapi/v1/leverageBracket",
        "/fapi/v3/positionRisk", "/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders"}
    assert fake.lev == {} and fake.mtype == {}
    assert "warnOnFetchOpenOrdersWithoutSymbol" not in broker.exchange.options      # ccxt's own option is put back
    await broker.close()


async def test_preflight_report_flags_what_needs_the_owners_attention() -> None:
    broker, fake = make_binance()
    fake.local_clock_ahead_ms = 100.0
    await broker.connect()
    fake.local_clock_ahead_ms = 4200.0
    fake.multi_assets = True
    fake.pos["BTCUSDT"] = {"amt": -0.002, "entry": 74000.0}             # opened by hand
    fake.new_order({"symbol": "DOGEUSDT", "side": "BUY", "type": "LIMIT", "quantity": "100", "price": "0.15",
                    "timeInForce": "GTC"})
    fake.new_algo({"algoType": "CONDITIONAL", "symbol": "BTCUSDT", "side": "BUY", "type": "STOP_MARKET",
                   "triggerPrice": "76000", "quantity": "0.002", "reduceOnly": "true"})
    fake.hedge_mode = True                                              # the account, while the config says one-way
    n = len(fake.log)
    lines = await broker.preflight_report(["BTC", "XYZ"])
    assert _writes(fake, n) == []
    assert all(line.startswith(("TAMAM ", "DİKKAT ")) for line in lines)
    assert lines[0] == ("DİKKAT Saat farkı büyük: bu bilgisayarın saati borsadan 4.2 saniye ileride. Bot farkı "
                        "kendisi düzeltiyor; yine de Windows saat eşitlemesini açın.")
    assert lines[1:] == [
        "DİKKAT Hesabın pozisyon modu çift yönlü (hedge), ayarda ise tek yönlü yazıyor: bu hâlde borsa giriş "
        "emirlerini reddeder. Binance'te Tercihler > Pozisyon Modu'nu tek yönlü yapın.",
        "DİKKAT Çoklu varlık modu açık: bot yalnızca USDT bakiyesine göre hesap yapar ve izole marjin kullanılamaz. "
        "Binance'te kapatmanız önerilir.",
        "TAMAM BTC: BTC/USDT:USDT sözleşmesi; en küçük emir 0.001 BTC ve 50 USDT; borsanın kaldıraç tablosunda en yüksek 125x "
        "(50000 USDT büyüklüğe kadar).",
        "DİKKAT XYZ: borsada USDT vadeli sözleşmesi bulunamadı; bu coinin sinyalleri bu hesapta açılmaz.",
        "TAMAM Not: kaldıraç tablosu hesaba özel sınırları (yeni açılmış hesap, kimlik doğrulama, ülke) göstermez. "
        "Borsa daha azına izin verirse bot ilk işlemde kaldıracı kendisi düşürür; bu, işlem raporundaki uyarıda "
        "yazar.",
        "DİKKAT Hesapta 1 açık pozisyon var (botun açtıkları dahil; tek yönlü modda aynı coindeki bot işlemleriyle "
        "birleşir):",
        "DİKKAT   BTC/USDT:USDT SHORT 0.002, giriş 74000, cross",
        "DİKKAT Hesapta bekleyen emir var: 1 normal, 1 koşullu (stop / kâr al). Botun açık işlemlerine ait "
        "değillerse borsada iptal edin:",
        "DİKKAT   DOGE/USDT:USDT ALIŞ limit emri, miktar 100, fiyat 0.15 (emir no 5000001)",
        "DİKKAT   BTC/USDT:USDT ALIŞ koşullu emir, miktar 0.002, tetik fiyatı 76000, yalnızca pozisyon azaltır "
        "(emir no 9001)",
    ]
    await broker.close()


async def test_preflight_report_lists_the_bots_own_trade_and_survives_an_unreadable_check() -> None:
    broker, fake = make_binance()
    fake.local_clock_ahead_ms = 50.0
    fake.no_brackets.add("DOGEUSDT")
    await broker.connect()
    await _open_btc(broker)
    fake.fail(lambda m, p, q: p == "/fapi/v1/multiAssetsMargin", ApiError(-1000, "An unknown error occured."))
    n = len(fake.log)
    lines = await broker.preflight_report(["DOGE"])
    assert _writes(fake, n) == []
    assert 'DİKKAT Çoklu varlık modu okunamadı: binanceusdm {"code": -1000, "msg": "An unknown error occured."}' in lines
    assert ("DİKKAT DOGE: DOGE/USDT:USDT sözleşmesi; en küçük emir 1 DOGE ve 5 USDT. Sorun: en yüksek kaldıraç "
            "okunamadı (ilk işlemde deneyerek bulunur).") in lines
    assert "DİKKAT   BTC/USDT:USDT LONG 0.012, giriş 74015, cross" in lines
    assert sum("koşullu emir" in line for line in lines) == 2           # the bot's stop and take profit
    await broker.close()


async def test_preflight_report_without_connection_and_on_another_exchange() -> None:
    broker, _ = make_binance()
    broker.exchange = None
    assert await broker.preflight_report(["BTC"]) == ["DİKKAT Borsa bağlantısı kurulmamış; kontroller yapılamadı."]
    other, fake = make_broker("bybit")
    await open_market(other)
    writes = [c[0] for c in fake.calls if c[0] in ("create_order", "cancel_order", "set_margin_mode", "set_leverage")]
    lines = await other.preflight_report(["ETH", "BTC"])
    assert lines[0].startswith("TAMAM ETH: ETH/USDT:USDT sözleşmesi; en küçük emir 0.001 ETH ve 5 USDT; borsanın "
                               "kaldıraç tablosunda en yüksek 100x")
    assert lines[1].startswith("DİKKAT BTC: borsada USDT vadeli sözleşmesi bulunamadı")      # inactive in that fake
    assert lines[2].startswith("DİKKAT Hesapta 1 açık pozisyon var") and "ETH/USDT:USDT LONG 6.828" in lines[3]
    assert not any(line.startswith("TAMAM Not:") for line in lines)     # the note is about Binance's bracket table
    assert writes == ["set_margin_mode", "set_leverage", "create_order", "create_order", "create_order"]
    assert [c[0] for c in fake.calls if c[0] in ("create_order", "cancel_order", "set_margin_mode", "set_leverage")] == writes


# --------------------------------------------------------------------------- pure helpers (N1 / N2)
@pytest.mark.parametrize("message,hint", [
    ("The max leverage of Symbol is 20x", [20]),
    ("Leverage adjustment failed. Current symbol max leverage limit is 25x", [25]),
    ("More than 20x leverage is available 30 days after Futures account registration.", [20]),
    ("Intermediate Personal Verification is required for adjusting leverage over 20x", [20]),
    ("Users in your location/country can only access a maximum leverage of 20", [20]),
    ("Current symbol leverage cannot exceed 20 when using position limit adjustment service.", [20]),
    ("the leverage can only up to 10x in your region", [10]),
    ("Leverage 125 is not valid", []),
    ("Users in this country has limited adjust leverage.", []),
    ("Leverage 125 is not valid for symbol 1000XUSDT", []),
])
def test_leverage_limit_named_in_a_refusal(message: str, hint: list[int]) -> None:
    text = f'binanceusdm {{"code":-4209,"msg":"{message}"}}'
    found = sorted({int(h) for pair in ccxt_broker._LEVERAGE_HINT_RE.findall(text) for h in pair if h})
    assert found == hint
    assert ccxt_broker._LEVERAGE_INVALID_RE.search(text)


def test_bracket_ladder_picks_leverage_by_position_value() -> None:
    ladder = [(75, 5000.0), (50, 25000.0), (20, None)]
    assert [CcxtBroker._ladder_leverage(ladder, v) for v in (100.0, 5000.0, 5000.01, 25000.0, 9e9)] == [75, 75, 50, 50, 20]
    assert CcxtBroker._largest_fit(ladder, 42.21) == (pytest.approx(3165.75), 75)     # margin is the limit
    assert CcxtBroker._largest_fit(ladder, 450.0) == (22500.0, 50)                    # 75x would stop at 5000
    assert CcxtBroker._largest_fit([(20, 300.0)], 450.0) == (300.0, 20)               # the bracket is the limit
    assert CcxtBroker._ladder_leverage([(10, 100.0)], 500.0) == 10                    # beyond every bracket: the last


# --------------------------------------------------------------------------- other exchange families
async def test_other_families_keep_their_plain_stop_and_take_profit_requests() -> None:
    broker, fake = make_broker("bybit")
    pos = await open_market(broker)
    created = [c[1] for c in fake.calls if c[0] == "create_order"]
    assert "clientOrderId" in created[0][5] and all("clientOrderId" not in c[5] for c in created[1:])
    assert "prot_seq" not in pos.data and not pos.data.get("lost_orders")


async def test_netting_guard_and_margin_shrink_also_apply_to_other_families() -> None:
    broker, fake = make_broker("bybit", margin_mode="cross", leverage=10, max_leverage=10)
    await open_limit(broker, qty=1.0, limit=2220.0)                     # a resting LONG entry
    info = await broker.resolve_symbol("ETH", "USDT")
    assert info is not None
    short = make_plan(Side.SHORT, sl=2260.0, tps=[TakeProfit(price=2100.0)])
    req = OpenRequest(plan=short, symbol=info, quantity=1.0, entry_type=EntryType.MARKET, stop_loss=short.stop_loss,
                      take_profits=short.take_profits, leverage=10, client_tag="ct99")
    with pytest.raises(BrokerError, match="ters yönde bekleyen giriş emri var"):
        await broker.open_position(req)
    fake.balance = {"USDT": {"total": 100.0, "free": 100.0}, "info": {}}
    size = await broker.size_position(info, Side.LONG, 2232.85, 2205.11, 20.0)        # planned 0.667 ETH = 149 margin
    assert size.quantity == 0.402 and size.leverage == 10.0 and size.margin_required <= 90.0
    assert "planlanan 0.667 yerine 0.402 açılıyor; stopta kayıp ~12.04 (hedef 20.00)" in size.warnings[0]


# --------------------------------------------------------------------------- N2: the owner is told (engine level)
async def test_owner_is_told_about_a_margin_shrunk_binance_position_although_execution_reports_are_off(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The real Engine on the production replica of test_preflight_scenarios.py (that module's harness; the
    in-memory exchange of test_ccxt_broker.py plays Binance). Execution reports are off there, so an ordinary
    open sends nothing: a position the adapter cut down for margin must still be reported."""
    scen = pytest.importorskip("test_preflight_scenarios")
    from copytrader.brokers.mt5_broker import Mt5Broker
    from copytrader.telegram import formatting as fmt

    monkeypatch.setattr(Mt5Broker, "_now_epoch", staticmethod(lambda: scen.SERVER_TIME - 7200.0))   # as scen._isolate
    monkeypatch.setattr(Mt5Broker, "tick_wait_sec", 0.0)
    monkeypatch.setattr(fmt, "RISK_PER_R_NOTE", fmt.RISK_PER_R_NOTE)
    p = await scen.Prod.create(tmp_path, monkeypatch)
    try:
        assert p.cfg.notifier.notify_on_execution is False
        p.binance.balance = {"USDT": {"total": 5.0, "free": 5.0}, "info": {}}      # 0.012 BTC needs 7.10 at 125x
        await p.say(scen.btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), scen.KRIPTO, "btc 1R long")
        (entry,) = p.binance.entry_orders()
        assert entry["amount"] == 0.007                                 # 90% of 5.00 carries 562 USDT at 125x
        (trade,) = await p.trades()
        pos = (await p.positions(trade.id))["binance"]
        assert pos.status is PositionStatus.OPEN and pos.quantity == 0.007 and pos.data["size_capped"] is True
        (note,) = p.texts(Severity.WARNING)
        assert "miktar sınırlandı, gerçek risk hedefin altında" in note
        assert "• binance: 0.007 BTC/USDT:USDT" in note
        assert "Teminat yetmediği için pozisyon küçültüldü: planlanan 0.012 yerine 0.007 açılıyor; stopta kayıp" in note
        assert "tickmill" not in note and p.texts(Severity.ERROR) == []     # only the account whose size was cut
    finally:
        await p.close()


def test_flat_note_says_unknown_only_when_the_result_really_is_unknown() -> None:
    broker, _ = make_binance()

    def closed(pnl: float | None, exits: list[dict[str, Any]]) -> BrokerPosition:
        return BrokerPosition(broker="binance", broker_symbol="BTC/USDT:USDT", side=Side.LONG,
                              status=PositionStatus.CLOSED, quantity=0.012, realized_pnl=pnl,
                              data={"closed_outside": 0.012, "exits": exits})

    head = "binance: pozisyon borsada kapalı bulundu; kalan emirler iptal edildi."
    unknown = head + " Kapanış botun emirleriyle olmadığı için kâr/zarar bilinmiyor."
    partial = head + " 0.012 botun emirleri dışında kapandı; kâr/zarar yalnızca botun kendi emirlerini içerir."
    assert broker._flat_note(closed(None, [])) == unknown
    # a result is recorded (e.g. from the trades of an order of ours that was found late): never "unknown" then
    assert broker._flat_note(closed(-9.61, [])) == partial
    assert broker._flat_note(closed(5.27, [{"order_id": "9002", "qty": 0.006, "price": 75005.0, "kind": "tp"}])) == partial
    # booked exits but no result could be computed: the text must not promise one
    assert broker._flat_note(closed(None, [{"order_id": "9002", "qty": 0.006, "price": None, "kind": "tp"}])) == unknown


# --------------------------------------------------------------------------- N8: consumers of an unknown PnL
async def test_storage_leaves_a_position_with_unknown_pnl_out_of_the_daily_sum() -> None:
    """Characterisation (unchanged code): None is stored as NULL and simply not summed, so a loss of unknown size
    does not count towards the daily-loss limit. Before, such a close was stored as 0.0: the sum is the same."""
    db = Database(":memory:")
    await db.open()
    try:
        plan = _btc_long()
        start = datetime(2020, 1, 1, tzinfo=timezone.utc)

        def closed(pnl: float | None) -> BrokerPosition:
            return BrokerPosition(broker="binance", broker_symbol="BTC/USDT:USDT", side=Side.LONG,
                                  status=PositionStatus.CLOSED, quantity=0.012, realized_pnl=pnl)

        await db.save_broker_position((await db.create_trade(plan)).id, closed(-4.0))
        unknown = await db.create_trade(plan)
        await db.save_broker_position(unknown.id, closed(None))
        assert [p.realized_pnl for p in await db.broker_positions(unknown.id)] == [None]
        total = await db.realized_pnl_since("binance", start)
        assert total == -4.0
        assert guards.daily_loss_exceeded(total, 46.9, 10.0) is False
    finally:
        await db.close()
