"""``python -m copytrader canli-test --broker <ad> [--sembol EURUSD] [--bekle N]``: the ONLY command that trades.

Strictly opt-in: nothing else in the program (the updater included) runs it. On ONE broker it sends the
smallest possible position through the adapter's real order path, exactly the calls the engine makes:

    resolve_symbol -> get_quote -> open_position (market, stop + take profit) -> sync_position
    -> update_stop_loss -> sync_position -> [--bekle seconds] -> close_position -> sync_position

Before anything is sent it shows broker, symbol, size and the worst-case loss in the account currency and
waits for the typed word ``EVET``. Brokers of kind ``manual`` are refused. Whatever happens after the order
was sent (an error, Ctrl+C), the position is closed again; if that cannot be confirmed, a loud message names
the broker, symbol and size so the owner can close it by hand.

What "stop and take profit are at the broker" means here: the adapter's own ``sync_position`` (which reads the
venue) reports the position OPEN, without an error and without its "no working stop" flag, and carries the
requested levels. It is the adapter's view of the venue, not a second, independent query.
"""

from __future__ import annotations

import asyncio
import secrets
import signal
import threading
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from decimal import ROUND_CEILING, Decimal
from pathlib import Path
from typing import Any

from .brokers.base import Broker
from .config import AppConfig
from .engine.monitor import protection_problem
from .models import (
    BrokerPosition,
    EntryType,
    MarketType,
    OpenRequest,
    PositionStatus,
    Quote,
    Side,
    SymbolInfo,
    TakeProfit,
    TradePlan,
)
from .risk.sizing import round_to_tick
from .selftest import PROBE_ASSETS, asset_spec
from .telegram.formatting import fmt_price

CONFIRM_WORD = "EVET"
CRYPTO_STOP_PCT = 1.0          # stop / take-profit distance for coins: percent of the price
CFD_STOP_PCT = 0.5             # metals, indices
FX_STOP_PIPS = 50.0            # currency pairs
MIN_NOTIONAL_MARGIN = 1.1      # stay this far above the venue's minimum order value
STOP_MOVE_FRACTION = 0.2       # the stop is moved this share of its distance towards the price
MAX_DRIFT_FRACTION = 0.25      # price moved more than this share of the stop distance while the owner read: stop
CONNECT_TIMEOUT_SEC = 180.0
CALL_TIMEOUT_SEC = 120.0
EXIT_OK, EXIT_FAILED, EXIT_REFUSED = 0, 1, 2

_DONE = (PositionStatus.CLOSED, PositionStatus.CANCELLED, PositionStatus.FAILED)
_FX_MARKETS = (MarketType.FOREX, MarketType.CFD)


def _reason(exc: BaseException) -> str:
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)) and not str(exc):
        return "yanıt gelmedi (zaman aşımı)"
    return str(exc) or type(exc).__name__


def _qty(x: float) -> str:
    return f"{x:.8f}".rstrip("0").rstrip(".") or "0"


def default_symbol(config: AppConfig, cfg: Any) -> str:
    """First probe instrument of the first topic route that trades on this broker; without routes a
    MetaTrader account gets a currency pair and everything else a coin."""
    for route in config.telegram.topics:
        if (not route.brokers or cfg.name in route.brokers) and route.kind in PROBE_ASSETS:
            return PROBE_ASSETS[route.kind][0]
    return PROBE_ASSETS["fx" if getattr(cfg, "type", "") in ("mt5", "metaapi") else "crypto"][0]


def minimal_quantity(sym: SymbolInfo, price: float) -> float:
    """Smallest order the venue accepts: the minimum quantity, raised to the minimum order value (plus a
    margin, so a small price move does not push it below)."""
    step = sym.qty_step if sym.qty_step > 0 else sym.min_qty
    qty = max(sym.min_qty, step)
    unit = price * sym.contract_size
    if sym.min_notional and unit > 0 and step > 0:
        need = sym.min_notional * MIN_NOTIONAL_MARGIN / unit
        if need > qty:
            d_step = Decimal(str(step))
            qty = float((Decimal(str(need)) / d_step).to_integral_value(rounding=ROUND_CEILING) * d_step)
    return qty


def stop_distance(market_type: MarketType | None, quote_asset: str | None, quote: Quote, sym: SymbolInfo) -> float:
    """About 1% (coins), 0.5% (metals / indices) or 50 pips (currency pairs); never inside the spread or the
    broker's minimum stop distance."""
    price = (quote.bid + quote.ask) / 2.0
    if market_type is MarketType.FOREX:
        distance = FX_STOP_PIPS * (0.01 if (quote_asset or "").upper() == "JPY" else 0.0001)
    elif market_type is MarketType.CFD:
        distance = price * CFD_STOP_PCT / 100.0
    else:
        distance = price * CRYPTO_STOP_PCT / 100.0
    try:
        stops_level = float((sym.extra or {}).get("stops_level") or 0.0)
    except (TypeError, ValueError):
        stops_level = 0.0
    return max(distance, 5.0 * (quote.ask - quote.bid), 2.0 * stops_level)


async def loss_at_stop(broker: Broker, sym: SymbolInfo, entry: float, stop: float, qty: float) -> tuple[float, bool]:
    """(loss of ``qty`` at ``stop``, is it in the account currency?). The adapter's own sizing gives the
    loss per unit in the account currency (with its fee estimate); when that fails the plain price
    difference is used, which is in the instrument's quote currency."""
    plain = qty * sym.contract_size * abs(entry - stop)
    amount = plain / 64.0
    for _ in range(12):
        try:
            size = await asyncio.wait_for(broker.size_position(sym, Side.LONG, entry, stop, amount), CALL_TIMEOUT_SEC)
            if size.quantity > 0 and size.est_loss_at_sl > 0:
                return size.est_loss_at_sl / size.quantity * qty, True
        except Exception:      # noqa: BLE001 - below the minimum size / no margin for this probe amount
            pass
        amount *= 4.0
    return plain, False


async def _bot_trade_on(config: AppConfig, broker_name: str, broker_symbol: str) -> str | None:
    """'#12' when the bot's own database has an active position of this broker on this symbol."""
    path = Path(config.data_dir).expanduser() / "copytrader.db"
    if not path.is_file():
        return None
    from .storage.db import Database

    db = Database(path)
    await db.open()
    try:
        for trade_id, pos in await db.active_broker_positions():
            if pos.broker == broker_name and pos.broker_symbol.upper() == broker_symbol.upper():
                return f"#{trade_id}"
    finally:
        await db.close()
    return None


def _same_price(a: float | None, b: float, tick: float) -> bool:
    return a is not None and abs(a - b) <= max(2.0 * tick, abs(b) * 1e-9)


def _venue_orders_problem(pos: BrokerPosition, tp: float | None, tick: float) -> str | None:
    """What the adapter's own records of the venue say about the protective orders, where it keeps such
    records (best effort: a key this version of an adapter does not have is simply not checked).
    MetaTrader legs: ``venue_tp`` is the take profit read back from the terminal / MetaApi.
    ccxt: ``sl_order_id`` and ``tp_order_ids`` are the live stop / take-profit orders on the exchange."""
    data = pos.data or {}
    if tp is not None:
        legs = [leg for leg in data.get("legs") or []
                if isinstance(leg, dict) and leg.get("state") == "open" and "venue_tp" in leg]
        bad = [leg for leg in legs if not _same_price(leg.get("venue_tp") or None, tp, tick)]
        if bad:
            return f"kâr al brokerda görünmüyor (pozisyon #{bad[0].get('ticket')})"
        orders = data.get("tp_order_ids")
        if isinstance(orders, list) and not any(
                isinstance(o, dict) and o.get("id") and o.get("status") == "open" for o in orders):
            return "kâr al emri borsada görünmüyor"
    if "sl_order_id" in data and not data.get("sl_order_id") and not data.get("attached"):
        return "stop emri borsada görünmüyor"
    return None


def _position_problem(pos: BrokerPosition, stop: float, tp: float | None, tick: float) -> str | None:
    """Why an OPEN position is not what was asked for (no stop, wrong levels, adapter error), else None."""
    if pos.error:
        return str(pos.error)
    naked = protection_problem(pos)
    if naked:
        return naked
    if not _same_price(pos.stop_loss, stop, tick):
        return f"stop brokerda {fmt_price(pos.stop_loss)} görünüyor, istenen {fmt_price(stop)}"
    if tp is not None and not any(_same_price(level.price, tp, tick) for level in pos.take_profits):
        shown = ", ".join(fmt_price(level.price) for level in pos.take_profits) or "yok"
        return f"kâr al brokerda {shown} görünüyor, istenen {fmt_price(tp)}"
    return _venue_orders_problem(pos, tp, tick)


class _InterruptGuard:
    """While an order may be open, Ctrl+C must not end the program: it is noted (``requested``), a running
    wait ends early, and the caller closes the position before it stops."""

    def __init__(self, out: Callable[[str], None]) -> None:
        self.requested = False
        self._out = out
        self._event = asyncio.Event()
        self._previous: Any = None
        self._installed = False

    def __enter__(self) -> "_InterruptGuard":
        loop = asyncio.get_running_loop()

        def handler(signum: int, frame: Any) -> None:
            if not self.requested:
                self._out("   Ctrl+C alındı: önce pozisyon kapatılacak, sonra çıkılacak. Lütfen bekleyin.")
            self.requested = True
            loop.call_soon_threadsafe(self._event.set)

        try:
            self._previous = signal.signal(signal.SIGINT, handler)
            self._installed = True
        except (ValueError, OSError):       # not the main thread: Ctrl+C then reaches the except path below
            pass
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._installed:
            try:
                signal.signal(signal.SIGINT, self._previous)
            except (ValueError, OSError, TypeError):
                pass

    async def hold(self, seconds: float, sleep: Callable[[float], Awaitable[Any]]) -> None:
        """Wait ``seconds``; return at once when Ctrl+C was pressed."""
        waiter = asyncio.ensure_future(self._event.wait())
        sleeper = asyncio.ensure_future(sleep(seconds))
        try:
            await asyncio.wait({waiter, sleeper}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (waiter, sleeper):
                if not task.done():
                    task.cancel()
            await asyncio.gather(waiter, sleeper, return_exceptions=True)


async def _ask(confirm: Callable[[str], str], prompt: str) -> str:
    """The owner's typed answer. It is read in a helper thread, so the event loop keeps running while the
    owner reads the summary: a blocked loop cannot answer a websocket's keep-alive (MetaApi), and the order
    would then be sent over a connection that is just being rebuilt. EOF (no keyboard) counts as "no"."""
    loop = asyncio.get_running_loop()
    answer: asyncio.Future[str] = loop.create_future()

    def deliver(value: str | None, error: BaseException | None) -> None:
        if answer.done():                       # the wait was given up (Ctrl+C): nobody is listening any more
            return
        if error is not None:
            answer.set_exception(error)
        else:
            answer.set_result(value or "")

    def work() -> None:
        value: str | None = None
        error: BaseException | None = None
        try:
            value = confirm(prompt)
        except EOFError:
            value = ""
        except BaseException as exc:            # noqa: BLE001 - handed to the waiting coroutine
            error = exc
        try:
            loop.call_soon_threadsafe(deliver, value, error)
        except RuntimeError:                    # the loop is closed: the program is already ending
            pass

    threading.Thread(target=work, name="canli-test-onay", daemon=True).start()
    return await answer


async def _flatten(broker: Broker, pos: BrokerPosition) -> tuple[BrokerPosition, str | None]:
    """Close / cancel whatever is left of ``pos``. (final position, None) when it is certainly flat, otherwise
    (last known position, why it could not be confirmed)."""
    problem: str | None = None
    for _attempt in range(2):
        try:
            if pos.status is PositionStatus.PENDING:
                pos = await asyncio.wait_for(broker.cancel_entry(pos), CALL_TIMEOUT_SEC)
            elif pos.status is PositionStatus.OPEN:
                pos = await asyncio.wait_for(broker.close_position(pos, 1.0), CALL_TIMEOUT_SEC)
        except Exception as exc:      # noqa: BLE001
            problem = _reason(exc)
        if pos.status in _DONE and not pos.remaining_quantity > 0:
            return pos, None
        try:
            pos = await asyncio.wait_for(broker.sync_position(pos), CALL_TIMEOUT_SEC)
        except Exception as exc:      # noqa: BLE001
            problem = problem or _reason(exc)
        if pos.status in _DONE and not pos.remaining_quantity > 0:
            return pos, None
    return pos, problem or f"pozisyon hâlâ '{pos.status.value}' görünüyor (kalan {_qty(pos.remaining_quantity)})"


async def run_live_test(
    config: AppConfig,
    broker_name: str,
    *,
    symbol: str | None = None,
    hold_sec: float = 0.0,
    broker_factory: Callable[[AppConfig, Any], Broker],
    confirm: Callable[[str], str] | None = None,
    out: Callable[[str], None] = print,
    sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
) -> int:
    """Run the round trip. Returns 0 (every step fine, or cancelled before anything was sent), 1 (a step
    failed) or 2 (refused: unknown / manual broker, the bot's own trade on the symbol).

    ``broker_factory(config, broker_config)`` builds the REAL adapter; ``confirm`` asks for the typed word
    (default: the keyboard); ``sleep`` waits (tests)."""
    cfg = next((b for b in config.brokers if b.name == broker_name), None)
    if cfg is None:
        names = ", ".join(b.name for b in config.brokers) or "yok"
        out(f"'{broker_name}' adlı broker config dosyasında yok. Tanımlı brokerlar: {names}")
        return EXIT_REFUSED
    if getattr(cfg, "type", "") == "manual":
        out(f"'{broker_name}' elle işlem hesabı: bot bu hesaba emir göndermez, yalnızca size mesaj atar. "
            "Canlı test yapılamaz.")
        return EXIT_REFUSED
    simulated = getattr(cfg, "type", "") == "paper"
    text = (symbol or default_symbol(config, cfg)).strip().upper()
    spec = asset_spec(text, "crypto")          # coins as USDT perpetuals, like a crypto-topic signal
    if spec is None:
        out(f"'{text}' bir varlık adı olarak anlaşılamadı (ör. EURUSD, XAUUSD, BTC).")
        return EXIT_REFUSED
    base, quote_asset, market_type, _pct = spec
    if market_type in _FX_MARKETS:
        base, quote_asset, market_type, _pct = asset_spec(text, "fx") or spec

    broker = broker_factory(config, cfg)
    pos: BrokerPosition | None = None
    sent = False
    label = text
    quantity = 0.0
    try:
        broker.set_risk_config(config.risk)
        out(f"{broker_name} hesabına bağlanılıyor...")
        await asyncio.wait_for(broker.connect(), CONNECT_TIMEOUT_SEC)
        ccy = await asyncio.wait_for(broker.account_currency(), CALL_TIMEOUT_SEC)
        equity_before = await asyncio.wait_for(broker.get_equity(), CALL_TIMEOUT_SEC)
        sym = await asyncio.wait_for(broker.resolve_symbol(base, quote_asset, market_type, text), CALL_TIMEOUT_SEC)
        if sym is None:
            out(f"{text} bu hesapta bulunamadı (sembol adı farklıysa config.yaml'da symbol_map gerekir). "
                "Hiçbir emir gönderilmedi.")
            return EXIT_FAILED
        label = sym.broker_symbol
        try:
            held = await _bot_trade_on(config, broker_name, sym.broker_symbol)
        except Exception as exc:      # noqa: BLE001 - an unreadable database must not hide the test
            held = None
            out(f"Uyarı: botun kayıtları okunamadı ({_reason(exc)}); bu sembolde bota ait açık işlem olup "
                "olmadığı kontrol edilemedi.")
        if held:
            out(f"{sym.broker_symbol} sembolünde bota ait açık işlem var ({held}). Test onunla karışmasın diye "
                "yapılmadı; başka bir sembol seçin (--sembol ...). Hiçbir emir gönderilmedi.")
            return EXIT_REFUSED
        quote = await asyncio.wait_for(broker.get_quote(sym), CALL_TIMEOUT_SEC)
        if not (quote.bid > 0 and quote.ask > 0):
            out(f"{sym.broker_symbol} için geçerli fiyat yok (alış {quote.bid}, satış {quote.ask}); piyasa "
                "kapalı olabilir. Hiçbir emir gönderilmedi.")
            return EXIT_FAILED
        quantity = minimal_quantity(sym, quote.ask)
        if not quantity > 0:
            out(f"{sym.broker_symbol} için en küçük emir miktarı okunamadı. Hiçbir emir gönderilmedi.")
            return EXIT_FAILED
        distance = stop_distance(market_type, sym.quote_asset or quote_asset, quote, sym)
        stop = round_to_tick(quote.bid - distance, sym.price_tick)
        target = round_to_tick(quote.ask + distance, sym.price_tick)
        loss, in_account_ccy = await loss_at_stop(broker, sym, quote.ask, stop, quantity)
        loss_ccy = ccy if in_account_ccy else (sym.quote_asset or ccy)

        out("")
        out("SANAL HESAP TESTİ - gerçek para kullanılmaz" if simulated
            else "CANLI TEST - GERÇEK HESAPTA GERÇEK EMİR GÖNDERİLİR")
        out(f"  Hesap  : {broker_name} ({broker.kind}), hesap değeri {equity_before:,.2f} {ccy}")
        out(f"  Sembol : {sym.broker_symbol}   Yön: LONG (alış)")
        out(f"  Miktar : {_qty(quantity)} (bu hesapta açılabilecek en küçük miktar)")
        out(f"  Giriş  : piyasa fiyatından, yaklaşık {fmt_price(quote.ask)}")
        out(f"  Stop   : {fmt_price(stop)}   Kâr al: {fmt_price(target)}")
        out(f"  En kötü durumda kayıp (stop çalışırsa): yaklaşık {loss:,.2f} {loss_ccy}"
            + ("" if in_account_ccy else " (hesap para birimine çevrilemedi)") + " + fiyat kayması")
        steps = "aç -> stop ve kâr al brokerda mı bak -> stopu bir kez taşı -> "
        steps += f"{hold_sec:g} sn bekle -> " if hold_sec > 0 else ""
        out(f"  Adımlar: {steps}kapat")
        out("  Not: bu sembolde hesapta başka açık pozisyon veya emir OLMAMALI.")
        if config.dry_run and not simulated:
            out("  Dikkat: config.yaml'da dry_run açık olsa da BU KOMUT gerçek emir gönderir.")
        answer = await _ask(confirm if confirm is not None else input,
                            f"Onaylıyorsanız büyük harflerle {CONFIRM_WORD} yazıp Enter'a basın "
                            "(başka her şey iptal eder): ")
        if answer.strip() != CONFIRM_WORD:
            out("İptal edildi; hiçbir emir gönderilmedi.")
            return EXIT_OK
        # Only what was shown is sent, and only while the market is still where it was shown.
        fresh = await asyncio.wait_for(broker.get_quote(sym), CALL_TIMEOUT_SEC)
        drift = abs((fresh.bid + fresh.ask) - (quote.bid + quote.ask)) / 2.0
        if not (fresh.bid > 0 and fresh.ask > 0) or drift > MAX_DRIFT_FRACTION * distance:
            out(f"Siz onaylarken fiyat değişti (gösterilen {fmt_price(quote.ask)}, şimdi {fmt_price(fresh.ask)}); "
                "gösterilen stop ve kâr al artık uygun değil. Hiçbir emir gönderilmedi; komutu yeniden çalıştırın.")
            return EXIT_FAILED

        plan = TradePlan(
            plan_id="lt" + secrets.token_hex(4), base_asset=base, quote_asset=quote_asset, market_type=market_type,
            side=Side.LONG, entry_type=EntryType.MARKET, stop_loss=stop,
            take_profits=[TakeProfit(price=target, close_fraction=1.0)], risk_r=1.0,
            risk_pct=config.risk.risk_per_r_pct, source_chat_id=0, summary_tr="canlı test",
            created_at=datetime.now(timezone.utc), broker_names=[broker_name],
        )
        request = OpenRequest(plan=plan, symbol=sym, quantity=quantity, entry_type=EntryType.MARKET,
                              stop_loss=stop, take_profits=list(plan.take_profits), client_tag=f"ct{plan.plan_id}")
        failures: list[str] = []
        tick = sym.price_tick

        out("")
        out(f"1. Emir gönderiliyor: LONG {_qty(quantity)} {sym.broker_symbol}, stop {fmt_price(stop)}, "
            f"kâr al {fmt_price(target)}")
        with _InterruptGuard(out) as guard:
            sent = True
            pos = await asyncio.wait_for(broker.open_position(request), CALL_TIMEOUT_SEC)
            if pos.status is not PositionStatus.OPEN:
                failures.append(f"pozisyon açılmadı (durum: {pos.status.value}): {pos.error or 'sebep bildirilmedi'}")
                out(f"   HATA: {failures[-1]}")
            else:
                out(f"   TAMAM: pozisyon açıldı, miktar {_qty(pos.filled_quantity or pos.quantity)}, "
                    f"giriş {fmt_price(pos.avg_entry_price)}")

            if pos.status is PositionStatus.OPEN and not guard.requested:
                out("2. Stop ve kâr al brokerda duruyor mu?")
                pos = await asyncio.wait_for(broker.sync_position(pos), CALL_TIMEOUT_SEC)
                problem = (None if pos.status is PositionStatus.OPEN
                           else f"pozisyon artık açık değil ({pos.status.value})")
                problem = problem or _position_problem(pos, stop, target, tick)
                if problem:
                    failures.append(f"koruma emirleri doğrulanamadı: {problem}")
                    out(f"   HATA: {failures[-1]}")
                else:
                    out(f"   TAMAM: stop {fmt_price(pos.stop_loss)} ve kâr al {fmt_price(target)} brokerda")

            if pos.status is PositionStatus.OPEN and not guard.requested:
                moved = round_to_tick(stop + STOP_MOVE_FRACTION * (quote.bid - stop), tick)
                out(f"3. Stop taşınıyor: {fmt_price(stop)} -> {fmt_price(moved)}")
                try:
                    pos = await asyncio.wait_for(broker.update_stop_loss(pos, moved), CALL_TIMEOUT_SEC)
                    pos = await asyncio.wait_for(broker.sync_position(pos), CALL_TIMEOUT_SEC)
                    problem = (None if pos.status is PositionStatus.OPEN
                               else f"pozisyon artık açık değil ({pos.status.value})")
                    problem = problem or _position_problem(pos, moved, None, tick)
                except Exception as exc:      # noqa: BLE001 - reported; the position is still closed below
                    problem = _reason(exc)
                if problem:
                    failures.append(f"stop taşınamadı: {problem}")
                    out(f"   HATA: {failures[-1]}")
                else:
                    out(f"   TAMAM: yeni stop {fmt_price(pos.stop_loss)} brokerda")

            if hold_sec > 0 and pos.status is PositionStatus.OPEN and not guard.requested:
                out(f"   Pozisyon {hold_sec:g} sn açık tutuluyor (erken kapatmak için Ctrl+C)...")
                await guard.hold(hold_sec, sleep)
                pos = await asyncio.wait_for(broker.sync_position(pos), CALL_TIMEOUT_SEC)
                if pos.status is not PositionStatus.OPEN:
                    out(f"   Not: pozisyon beklerken kapandı ({pos.status.value}); stop ya da kâr al çalışmış "
                        "olabilir.")

            if pos.status not in _DONE or pos.remaining_quantity > 0:
                out("4. Pozisyon kapatılıyor")
                pos, problem = await _flatten(broker, pos)
                if problem:
                    failures.append(f"pozisyon kapatılamadı: {problem}")
                    out(f"   HATA: {failures[-1]}")
                else:
                    out("   TAMAM: pozisyon kapandı, hesapta bu testten açık bir şey kalmadı")
                    await sleep(1.0)        # the venue books the closing deal a moment later
                    try:
                        pos = await asyncio.wait_for(broker.sync_position(pos), CALL_TIMEOUT_SEC)
                    except Exception:      # noqa: BLE001 - only the booked result is refreshed here
                        pass
                    if pos.error:
                        failures.append(f"kapanıştan sonra broker uyarısı: {pos.error}")
                        out(f"   HATA: {failures[-1]}")
            if guard.requested:
                failures.append("test Ctrl+C ile yarıda kesildi")

        out("")
        cost = [f"brokerın bildirdiği sonuç {pos.realized_pnl:+,.2f} {ccy}"] if pos.realized_pnl is not None else []
        try:
            equity_after = await asyncio.wait_for(broker.get_equity(), CALL_TIMEOUT_SEC)
            cost.append(f"hesap değeri {equity_before:,.2f} -> {equity_after:,.2f} {ccy} "
                        f"(fark {equity_after - equity_before:+,.2f})")
        except Exception as exc:      # noqa: BLE001
            cost.append(f"hesap değeri okunamadı ({_reason(exc)})")
        out("Gerçekleşen maliyet: " + "; ".join(cost))
        if pos.status not in _DONE or pos.remaining_quantity > 0:
            _loud(out, broker_name, label, quantity, failures[-1] if failures else "pozisyon kapalı görünmüyor")
            return EXIT_FAILED
        if failures:
            out(f"SONUÇ: TEST BAŞARISIZ ({len(failures)} sorun). Hesapta bu testten açık pozisyon kalmadı.")
            for i, failure in enumerate(failures, start=1):
                out(f"  {i}. {failure}")
            return EXIT_FAILED
        out(f"SONUÇ: TEST BAŞARILI. {broker_name} hesabında işlem açma, stop / kâr al koyma, stop taşıma ve "
            "kapatma çalışıyor.")
        return EXIT_OK
    except BaseException as exc:      # also Ctrl+C: never walk away from an order that may be open
        if not sent:
            if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)):
                out("Durduruldu; hiçbir emir gönderilmedi.")
                raise
            out(f"HATA: {_reason(exc)}. Hiçbir emir gönderilmedi.")
            return EXIT_FAILED
        why = "test yarıda kesildi" if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)) else _reason(exc)
        out(f"   HATA: {why}")
        if pos is None:
            # The order call itself failed: the adapter could not tell whether the venue took the order.
            _loud(out, broker_name, label, quantity, f"emir gönderilirken hata oluştu ({why}); emrin brokera "
                                                     "ulaşıp ulaşmadığı bilinmiyor")
            return EXIT_FAILED
        out("   Pozisyon kapatılmaya çalışılıyor...")
        pos, problem = await _flatten(broker, pos)
        if problem:
            _loud(out, broker_name, label, quantity, f"{why}; ardından kapatma doğrulanamadı: {problem}")
        else:
            out(f"SONUÇ: TEST BAŞARISIZ ({why}). Pozisyon kapatıldı; hesapta bu testten açık bir şey kalmadı.")
        return EXIT_FAILED
    finally:
        try:
            await broker.close()
        except Exception:      # noqa: BLE001
            pass


def _loud(out: Callable[[str], None], broker_name: str, symbol: str, quantity: float, why: str) -> None:
    bar = "!" * 78
    out(bar)
    out(f"!!! DİKKAT: {broker_name} hesabında {symbol} LONG {_qty(quantity)} pozisyonu AÇIK KALMIŞ OLABİLİR !!!")
    out(f"!!! Sebep: {why}")
    out("!!! HEMEN brokerın kendi ekranından (uygulama / web) bakın; pozisyon ve ona ait bekleyen emir")
    out("!!! varsa ELLE KAPATIN / İPTAL EDİN. Bot bu pozisyonu takip etmiyor.")
    out(bar)


__all__ = ["CONFIRM_WORD", "default_symbol", "loss_at_stop", "minimal_quantity", "run_live_test", "stop_distance"]
