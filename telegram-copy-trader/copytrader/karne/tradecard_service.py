"""The owner's trade card on Telegram: ``/<hesap>kart`` (``/binancekart``, ``/fxkart`` - or an alias, ``/fx1kart`` -,
``/fx2kart``, ``/breakoutkart``...) lists THIS WEEK's trades of that account, numbered ("1- BTC LONG · #12 · ..."); the
owner answers with a number and gets one picture: the ENTRY card while that account's position is open or pending,
the RESULT card once it is closed (see :mod:`.tradecard` for what the card says). Only on command, never by itself.

* **Owner only.** The accounts are the owner's own (real money and his hand-traded ones; never the friend's); the
  friend's chat has its own commands (telegram.notifier.FRIEND_COMMANDS) and this is not one of them; no reply, no
  card and no error of this feature is ever copied to the friend.
* **The numbered list** works like /pozkapat's: one per owner, CARD_LIST_TTL_SEC long, one use; a plain number
  answers it. A number is never taken from it while a correction is awaited (the number is the correction), and
  never while a /pozkapat list is waiting too (the bot then asks for ``/pozkapat 2`` or ``/binancekart 2``: a number
  meant for a card must never close a position). ``/binancekart 2`` picks from that account's list explicitly.
* **Never in the way of trading.** Database reads; candles from the public exchange (ccxt) or - read only - from
  the account's own MetaTrader terminal through the adapter's worker thread with its time limit
  (``trade_chart``). That worker thread is the one order_send uses, so the MetaTrader read is made only while the
  trading path is idle (``busy``, :func:`copytrader.telegram.group_mirror.trading_path_busy`: open / management
  flows, late legs, monitor syncs - virtual entries fire inside them -, owner commands, the listener), waited for
  at most CHART_IDLE_WAIT_SEC (the owner's own number is still being answered when the card starts), else the owner
  is told BUSY_TRADING; it asks only for the candles the card draws (:func:`.tradecard.window`) and stops as soon as
  the terminal gets another call (``trade_chart``'s "busy"). The picture is drawn by ANOTHER process
  (:mod:`.render`: time limit, below normal priority, killed on overrun); the upload through the notifier's bounded
  file request (paused while the trading path is busy). The command answers at once; the card is made by a task of
  this service, one card at a time. Every failure is a short text for the owner; nothing raises into the bot.
  :meth:`stop` ends within STOP_WAIT_SEC.
* **A ccxt position closed by hand on the exchange** (closed, no exit price of the bot's on record, or part of it
  closed by the bot and the rest by hand: :func:`.tradecard.venue_wanted`): its close
  and the net money it made (fees in the account's quote currency included) are read - read only, within
  FILLS_TIMEOUT_SEC - from the account's own fills of the symbol (the live adapter's exchange, ``fetch_my_trades``;
  :func:`.tradecard.venue_close`), for the card only (the bot's own records, its result, the report card and the
  closing notice stay as they are). Not found or not readable: the card is not made (the owner reads why), never a
  guessed price; a position whose bot result is complete reads no fills.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ..config import fold_ascii
from ..engine.prices import stray_cancel
from ..models import PositionStatus
from ..telegram.formatting import esc
from . import tradecard as tc
from .render import RENDER_TIMEOUT_SEC, TIMEOUT, RenderResult, pillow_installed, remove_picture, render_card
from .uktime import UTC, as_utc

logger = logging.getLogger(__name__)

CARD_LIST_TTL_SEC = 300.0          # = telegram.notifier.POZKAPAT_TTL_SEC (tests keep the two equal)
COMMAND_SUFFIX = "kart"            # /<account>kart
TEMP_DIR = "kart"                  # under data_dir: the card's temporary files (removed after each card)
STOP_WAIT_SEC = 5.0
CANDLE_TIMEOUT_SEC = 60.0          # one candle read (the MetaTrader adapter's own limits are inside it)
CHART_IDLE_WAIT_SEC = 10.0         # a MetaTrader read waits at most this long for an idle trading path, then refused
CHART_IDLE_POLL_SEC = 0.2
FILLS_TIMEOUT_SEC = 20.0           # one read of a ccxt account's own fills (a close made by hand on the exchange)
FILLS_SINCE_SLACK_MS = 60_000      # the fills are asked for from this long before the position's open (as the adapter)
LIST_MAX = 60                      # rows of one list (a week of one account: never near it)
# ccxt's bar sizes the card may use (minutes -> ccxt timeframe); MetaTrader's: brokers.mt5_broker.TIMEFRAMES
CCXT_TIMEFRAMES = {1: "1m", 3: "3m", 5: "5m", 15: "15m", 30: "30m", 60: "1h", 120: "2h", 240: "4h", 360: "6h",
                   480: "8h", 720: "12h", 1440: "1d"}
MT5_TIMEFRAMES = (1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60, 120, 180, 240, 360, 480, 720, 1440)
_ONE_NUMBER = re.compile(r"^\s*(\d{1,3})\s*$")

STARTED = "⏳ Kart hazırlanıyor; birkaç saniye içinde gelecek."
BUSY = "⏳ Bir kart zaten hazırlanıyor; o gelince tekrar seçebilirsiniz (liste duruyor)."
STOPPING = "⏳ Bot kapanıyor; kart şu an hazırlanamıyor."
NO_LIST = ("Seçilecek bir kart listesi yok ya da süresi doldu: önce <code>/{cmd}</code> yazıp listeyi alın.")
ONE_NUMBER = "Kart için tek bir numara yazın (ör. <code>2</code>)."
NOT_ON_LIST = "{n} listede yok (1–{count}); listeden bir numara yazın."
NO_TRADES = "📂 <b>{account}</b>: bu hafta (pazar 22:00'den beri, İngiltere saati) işlem yok."
FAILED = "⚠️ Kart hazırlanamadı ({why})."
NO_PICTURE = "⚠️ Kart görseli hazırlanamadı ({why})."
NOT_SENT = "⚠️ Kart gönderilemedi ({why})."
NO_PILLOW = "ℹ️ Kart görseli için Pillow paketi kurulu değil (karne ile aynı paket); kart gönderilemedi."
CANDLES_FAILED = "mumlar okunamadı"
BUSY_TRADING = "⏳ Şu an hesapta işlem yürütülüyor, kartı birazdan tekrar isteyin."


class _TradingBusy(Exception):
    """The MetaTrader read was not made: the trading path is working (the owner reads BUSY_TRADING)."""


class TradeCardService:
    def __init__(
        self,
        db: Any,
        notifier: Any,
        data_dir: str | Path,
        *,
        accounts: Sequence[str],
        aliases: Mapping[str, Sequence[str]] | None = None,
        sources: Mapping[str, tuple[str, str | None]] | None = None,
        broker: Callable[[str], Any] | None = None,
        oracle: Any = None,
        manual: Sequence[str] = (),
        busy: Callable[[], bool] | None = None,
        clock: Callable[[], datetime] | None = None,
        renderer: Callable[..., Awaitable[RenderResult]] = render_card,
        pillow: Callable[[], bool] = pillow_installed,
        monotonic: Callable[[], float] = time.monotonic,
        idle_wait_sec: float = CHART_IDLE_WAIT_SEC,
    ) -> None:
        """``accounts``: the owner's accounts a card is made for, in the order they are shown (never the friend's);
        ``aliases``: their other names (config ``aliases``); ``sources``: where each one's candles come from -
        ("ccxt", exchange id) or ("mt5", None) (the adapter's own read-only ``trade_chart``); ``broker(name)``: the
        live adapter of an account; ``oracle``: the engine's ReferencePriceOracle (public candles); ``manual``: the
        hand-traded accounts (no stop on a broker); ``busy``: true while the trading path is working (the upload
        pauses; a MetaTrader read waits for it at most ``idle_wait_sec``, then is refused). The rest replaces the
        outside world in tests."""
        self._db = db
        self._notifier = notifier
        self._folder = Path(data_dir) / TEMP_DIR
        self._accounts = list(dict.fromkeys(accounts))
        self._aliases = {name: list((aliases or {}).get(name, [])) for name in self._accounts}
        self._sources = dict(sources or {})
        self._broker = broker or (lambda _name: None)
        self._oracle = oracle
        self._manual = set(manual)
        self._busy = busy
        self._clock = clock or (lambda: datetime.now(UTC))
        self._renderer = renderer
        self._pillow = pillow
        self._monotonic = monotonic
        self._idle_wait = float(idle_wait_sec)
        # the owner's numbered list: (made at, account, [(trade id, label)]); in memory only, one use
        self._list: tuple[float, str, list[tuple[int, str]]] | None = None
        self._job: asyncio.Task[None] | None = None
        self._stopping = False

    # ---- names ----------------------------------------------------------------------------------------
    @property
    def accounts(self) -> list[str]:
        return list(self._accounts)

    def account_for(self, word: str) -> str | None:
        """The owner's account a command word names ("fx1" -> "fx"), compared without Turkish letters and case;
        None: not one of the accounts a card is made for (the friend's, an unknown one)."""
        key = fold_ascii(word or "")
        if not key:
            return None
        for name in self._accounts:
            if key == fold_ascii(name) or any(key == fold_ascii(a) for a in self._aliases.get(name, [])):
                return name
        return None

    def command_account(self, command: str) -> str | None:
        """The account of a command name ("binancekart" -> "binance"); None when it is not a card command of an
        account of the owner."""
        key = fold_ascii(command or "")
        if not key.endswith(COMMAND_SUFFIX) or len(key) <= len(COMMAND_SUFFIX):
            return None
        return self.account_for(key[: -len(COMMAND_SUFFIX)])

    def is_card_command(self, command: str) -> bool:
        """A command of the form /<word>kart (the owner's or not)."""
        key = fold_ascii(command or "")
        return key.endswith(COMMAND_SUFFIX) and len(key) > len(COMMAND_SUFFIX)

    def commands_text(self) -> str:
        return " · ".join(f"/{esc(name)}{COMMAND_SUFFIX}" for name in self._accounts)

    def not_an_account(self, command: str) -> str:
        """The answer to /<word>kart whose word is not one of the owner's accounts."""
        if not self._accounts:
            return "ℹ️ İşlem kartı için hesabınız yok."
        return f"❓ /{esc(command)}: işlem kartı yalnızca sizin hesaplarınız için: {self.commands_text()}"

    # ---- the numbered list ----------------------------------------------------------------------------
    def list_live(self) -> bool:
        """A numbered card list waits for a number (and is not older than CARD_LIST_TTL_SEC)."""
        if self._list is not None and self._monotonic() - self._list[0] > CARD_LIST_TTL_SEC:
            self._list = None
        return self._list is not None

    def list_command(self) -> str:
        """The command of the live list ("binancekart"), for the owner's explicit choice."""
        account = self._list[1] if self._list is not None else (self._accounts[0] if self._accounts else "hesap")
        return f"{account}{COMMAND_SUFFIX}"

    async def command(self, account: str, args: str = "") -> str:
        """``/<account>kart`` (args empty): the numbered list of the account's trades this week; ``/<account>kart
        N``: the card of number N of that account's list. HTML for the owner; never raises."""
        try:
            if self._stopping:
                return STOPPING
            words = (args or "").strip()
            if words:
                if not _ONE_NUMBER.match(words):
                    return (f"Kullanım: <code>/{esc(account)}{COMMAND_SUFFIX}</code> (bu haftanın listesi), "
                            "sonra numara.")
                if not self.list_live() or self._list is None or self._list[1] != account:
                    return NO_LIST.format(cmd=esc(f"{account}{COMMAND_SUFFIX}"))
                return await self.pick(words)
            now = self._now()
            since = tc.week_start(now)
            rows = tc.listed(await self._db.account_trades(account, since))
            if not rows:
                self._list = None
                return NO_TRADES.format(account=esc(account))
            rows = rows[-LIST_MAX:]
            items = [(trade.id, tc.list_label(trade, pos)) for trade, pos, _closed in rows]
            self._list = (self._monotonic(), account, items)
            lines = [f"{i}- {esc(label)}" for i, (_tid, label) in enumerate(items, start=1)]
            return (f"🃏 <b>{esc(account)}</b> · bu haftanın işlemleri (pazar 22:00'den beri, İngiltere saati):\n"
                    + "\n".join(lines)
                    + f"\n\nKartı için numarasını yazın: ör. <code>1</code>. Açık / bekleyen işlemde giriş kartı, "
                    f"kapanmışta sonuç kartı gelir. Liste {int(CARD_LIST_TTL_SEC // 60)} dk geçerli.")
        except asyncio.CancelledError:
            raise
        except Exception as exc:      # noqa: BLE001 - told to the owner, never raised into the bot
            logger.exception("Trade card: /%skart failed", account)
            return FAILED.format(why=esc(str(exc) or type(exc).__name__))

    async def pick(self, text: str) -> str:
        """A number of the live list: that trade's card is made (by a task of its own; the answer is at once). One
        use: the list is gone once a card was started from it. Never raises."""
        try:
            if self._stopping:
                return STOPPING
            if not self.list_live() or self._list is None:
                first = self._accounts[0] if self._accounts else "hesap"
                return NO_LIST.format(cmd=esc(f"{first}{COMMAND_SUFFIX}"))
            made, account, items = self._list
            m = _ONE_NUMBER.match(text or "")
            if m is None:
                return ONE_NUMBER
            n = int(m.group(1))
            if not 1 <= n <= len(items):
                return NOT_ON_LIST.format(n=n, count=len(items))
            if self._job is not None and not self._job.done():
                return BUSY                     # (nothing was done: the list stays for the rest of its time)
            self._list = None                   # one use only (before anything else)
            trade_id = items[n - 1][0]
            job = asyncio.ensure_future(self._make(account, trade_id))
            job.set_name(f"tradecard-{account}-{trade_id}")
            job.add_done_callback(self._job_done)
            self._job = job
            return STARTED
        except asyncio.CancelledError:
            raise
        except Exception as exc:      # noqa: BLE001
            logger.exception("Trade card: picking from the list failed")
            return FAILED.format(why=esc(str(exc) or type(exc).__name__))

    def _job_done(self, job: asyncio.Task[None]) -> None:
        if not job.cancelled() and job.exception() is not None:
            logger.error("Trade card: a card failed", exc_info=job.exception())

    async def stop(self) -> None:
        """No more cards; a card that is being made is given up (its drawing process is killed)."""
        self._stopping = True
        self._list = None
        job = self._job
        if job is not None and not job.done():
            job.cancel()
            await asyncio.wait([job], timeout=STOP_WAIT_SEC)

    # ---- making one card ------------------------------------------------------------------------------
    def _now(self) -> datetime:
        return as_utc(self._clock())

    async def _make(self, account: str, trade_id: int) -> None:
        try:
            data, caption = await self.build(account, trade_id)
            await self._draw_and_send(data, caption)
        except _TradingBusy:
            await self._tell(BUSY_TRADING)
        except tc.CardProblem as exc:
            await self._tell(FAILED.format(why=str(exc)))
        except asyncio.CancelledError:
            if self._stopping or not stray_cancel():
                raise
            logger.error("Trade card: a CancelledError nobody asked for ended the card", exc_info=True)
            await self._tell(FAILED.format(why="CancelledError"))
        except Exception as exc:      # noqa: BLE001 - told to the owner, never raised into the bot
            logger.exception("Trade card of trade %s on %s could not be made", trade_id, account)
            await self._tell(FAILED.format(why=esc(str(exc) or type(exc).__name__)))

    async def build(self, account: str, trade_id: int) -> tuple[dict[str, Any], str]:
        """The card's data (tradecard.card_data) and the photo's caption, from a FRESH read of the account's
        position (entry or result card is decided now). Raises CardProblem with the owner's reason."""
        now = self._now()
        rows = await self._db.account_trades(account, now, trade_id=trade_id)
        if not rows:
            raise tc.CardProblem("bu işlem bu hesapta artık bulunamadı")
        trade, pos, closed_at = rows[0]
        active = pos.status in (PositionStatus.PENDING, PositionStatus.OPEN)
        if not active and not tc.traded(pos):
            raise tc.CardProblem("bu işlemde bu hesapta giriş hiç dolmadı; gösterilecek bir işlem yok")
        source = self._sources.get(account) or ("", None)
        supported = MT5_TIMEFRAMES if source[0] == "mt5" else tuple(CCXT_TIMEFRAMES)
        tf = tc.timeframe_of(trade, supported)
        end_events = (as_utc(closed_at) if closed_at is not None else now) + timedelta(hours=2)
        events = await self._db.events_between(tc.EVENT_KINDS, as_utc(trade.created_at) - timedelta(hours=1),
                                               min(end_events, now + timedelta(minutes=1)))
        if source[0] == "mt5":
            bars, rec = await self._mt5_candles(account, trade, pos, closed_at, events, tf, now)
        elif source[0] == "ccxt" and source[1]:
            rec = tc.read_records(trade, pos, closed_at, events)
            if tc.venue_wanted(pos, rec):
                rec = await self._closed_on_venue(account, trade.id, pos, rec, now)
            bars = await self._ccxt_candles(str(source[1]), pos.broker_symbol, rec, pos, tf, now)
        else:
            raise tc.CardProblem("bu hesabın mumları okunamıyor")
        data = tc.card_data(trade, pos, account, rec, bars, tf, manual=account in self._manual, now=now)
        return data, tc.caption(trade, account, data["kind"])

    async def _closed_on_venue(self, account: str, trade_id: int, pos: Any, rec: tc.Records, now: datetime
                               ) -> tc.Records:
        """A closed position of a ccxt account closed by hand on the exchange, in full or in part
        (tradecard.venue_wanted): the records with the close and the net money read from the account's own fills
        (tradecard.venue_close; the fills of the account's other positions of the bot's are never this one's:
        tradecard.venue_neighbours, from the database). The fills do not tell (the account is not a live ccxt one,
        the position not real money, the read fails or the fills do not cover it): ``rec`` as it is, but without
        the bot's own exit price when part of the position was closed outside the bot's orders (that price is not
        the position's close: the card then says the close price is not on record, never a part shown as the
        whole). Read only, bounded; raises only CardProblem (tradecard.VENUE_SHARED: a close by hand that cannot be
        told to be this position's) and CancelledError."""
        unread = replace(rec, exit_price=None) if tc.venue_partial(pos) else rec
        broker = self._broker(account)
        if getattr(broker, "kind", None) != "ccxt" or pos.dry_run or (pos.data or {}).get("simulated"):
            return unread
        fetch = getattr(getattr(broker, "exchange", None), "fetch_my_trades", None)
        opened = tc.venue_opened(pos, rec)
        if not callable(fetch) or opened is None:
            return unread
        quote = getattr(getattr(broker, "cfg", None), "quote_currency", None)
        try:
            since = int(as_utc(opened).timestamp() * 1000) - FILLS_SINCE_SLACK_MS
            rows = await self._db.account_trades(account, as_utc(opened) - timedelta(milliseconds=FILLS_SINCE_SLACK_MS))
            foreign, busy = tc.venue_neighbours(pos, rec, trade_id, rows, now)
            trades = await asyncio.wait_for(fetch(pos.broker_symbol, since), FILLS_TIMEOUT_SEC)
            found = tc.venue_close(pos, rec, list(trades or []), now, str(quote) if quote else None,
                                   foreign=foreign, busy=busy)
        except (asyncio.CancelledError, tc.CardProblem):
            raise
        except Exception as exc:      # noqa: BLE001 - the card is refused as before (no close price on record)
            logger.warning("Trade card: %s's fills of %s could not be read: %s", account, pos.broker_symbol,
                           str(exc) or type(exc).__name__)
            return unread
        if found is None:
            logger.info("Trade card: %s's fills of %s do not cover the closed position", account, pos.broker_symbol)
            return unread
        return found

    def _span(self, rec: tc.Records, pos: Any, tf: int, now: datetime, *, closed_market: bool
              ) -> tuple[datetime, datetime]:
        anchor = rec.placed if pos.status is PositionStatus.PENDING else (rec.filled or rec.placed)
        if anchor is None:
            raise tc.CardProblem("girişin zamanı kayıtlarda yok")
        if pos.status in (PositionStatus.PENDING, PositionStatus.OPEN):
            end = now
        else:
            if rec.closed is None:
                raise tc.CardProblem("kapanışın zamanı kayıtlarda yok")
            end = tc.search_end(rec.closed, tf, now)
        return tc.window(anchor, end, tf, closed_market=closed_market)

    async def _ccxt_candles(self, exchange: str, symbol: str, rec: tc.Records, pos: Any, tf: int,
                            now: datetime) -> list[tuple[float, ...]]:
        if self._oracle is None or not callable(getattr(self._oracle, "ohlcv", None)):
            raise tc.CardProblem(CANDLES_FAILED)
        start, end = self._span(rec, pos, tf, now, closed_market=False)
        rows = await asyncio.wait_for(
            self._oracle.ohlcv(exchange, symbol, CCXT_TIMEFRAMES[tf], int(start.timestamp() * 1000),
                               int(end.timestamp() * 1000)), CANDLE_TIMEOUT_SEC)
        if not rows:
            raise tc.CardProblem(CANDLES_FAILED)
        return [(r[0] / 1000.0, r[1], r[2], r[3], r[4]) for r in rows]

    async def _mt5_candles(self, account: str, trade: Any, pos: Any, closed_at: datetime | None,
                           events: Sequence[Mapping[str, Any]], tf: int, now: datetime
                           ) -> tuple[list[tuple[float, ...]], tc.Records]:
        broker = self._broker(account)
        chart = getattr(broker, "trade_chart", None)
        if not callable(chart):
            raise tc.CardProblem("bu hesabın terminaline şu an ulaşılamıyor")
        plain = tc.read_records(trade, pos, closed_at, events)      # (the bot's own times: the window, the anchor)
        start, end = self._span(plain, pos, tf, now, closed_market=True)
        tickets = [leg.get("ticket") for leg in (pos.data or {}).get("legs") or []
                   if isinstance(leg, dict) and float(leg.get("filled") or 0.0) > 0]
        # The bot's own UTC record of the FILL only (its notice of a limit fill, a market order's send): the time a
        # limit order was placed may be hours before it filled and would give the server's clock wrong by hours.
        anchor = plain.filled
        await self._wait_idle()
        # (the read follows the look at once; an order that comes after it: trade_chart stops before its next call)
        try:
            answer = await asyncio.wait_for(
                chart(pos.broker_symbol, tf, start.timestamp(), end.timestamp(), tickets,
                      anchor.timestamp() if anchor is not None else None), CANDLE_TIMEOUT_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as exc:      # noqa: BLE001 - the terminal's answer: the owner reads why
            logger.warning("Trade card: %s candles could not be read: %s", account, exc)
            raise tc.CardProblem(f"{CANDLES_FAILED}: {esc(str(exc) or type(exc).__name__)}") from exc
        if isinstance(answer, dict) and answer.get("busy"):
            logger.info("Trade card: %s's terminal got another call during the card's read; not read", account)
            raise _TradingBusy()
        offset = answer.get("offset") if isinstance(answer, dict) else None
        if not isinstance(offset, (int, float)):
            raise tc.CardProblem("MetaTrader sunucusunun saati belirlenemedi (piyasa kapalı ve giriş kaydı yok)")
        deals = [tc.Deal(time=datetime.fromtimestamp(float(d["time"]) - offset, tz=timezone.utc),
                         price=float(d["price"]), volume=float(d["volume"]), entry=d.get("entry") == "in",
                         reason=str(d.get("reason") or "manual"))
                 for d in answer.get("deals") or [] if isinstance(d, dict) and d.get("time")]
        rec = tc.read_records(trade, pos, closed_at, events, deals=deals or None)
        bars = [(float(b[0]) - offset, b[1], b[2], b[3], b[4]) for b in answer.get("bars") or [] if len(b) >= 5]
        if not bars:
            raise tc.CardProblem(CANDLES_FAILED)
        return bars, rec

    def _path_busy(self) -> bool:
        if self._busy is None:
            return False
        try:
            return bool(self._busy())
        except Exception:      # noqa: BLE001 - what cannot be looked at counts as busy
            return True

    async def _wait_idle(self) -> None:
        """Return when the trading path is idle (at once when it is); raise _TradingBusy when it stays busy for
        ``idle_wait_sec``."""
        deadline = time.monotonic() + self._idle_wait
        while self._path_busy():
            if self._stopping:
                raise tc.CardProblem("bot kapanıyor")
            left = deadline - time.monotonic()
            if left <= 0:
                logger.info("Trade card: the trading path stayed busy for %.0f s; the terminal was not read",
                            self._idle_wait)
                raise _TradingBusy()
            await asyncio.sleep(min(CHART_IDLE_POLL_SEC, left))

    async def _draw_and_send(self, data: dict[str, Any], caption: str) -> None:
        if not self._pillow():
            await self._tell(NO_PILLOW)
            return
        picture = await self._renderer(data, self._folder)
        if not picture.ok:
            why = (f"{RENDER_TIMEOUT_SEC:.0f} sn içinde bitmedi" if picture.status == TIMEOUT
                   else "Pillow kurulu değil" if picture.status == "no_pillow"
                   else f"çizim hatası: <code>{esc(picture.detail[:200])}</code>")
            await self._tell(NO_PICTURE.format(why=why))
            return
        send = getattr(self._notifier, "send_owner_photo", None)
        problem: str | None
        try:
            if not callable(send):
                problem = "bildirim botu fotoğraf gönderemiyor"
            else:
                problem = await send(str(picture.path), caption=caption, busy=self._busy)
        except asyncio.CancelledError:
            raise
        except Exception as exc:      # noqa: BLE001
            logger.exception("Trade card: sending the picture failed unexpectedly")
            problem = str(exc) or type(exc).__name__
        finally:
            remove_picture(picture.path)
        if problem is not None:
            await self._tell(NOT_SENT.format(why=esc(problem)))

    async def _tell(self, text: str) -> None:
        """A line for the owner alone (never the friend); never raises."""
        with contextlib.suppress(Exception):
            tell = getattr(self._notifier, "tell_owner", None)
            if callable(tell):
                await tell(text)


__all__ = ["BUSY_TRADING", "CARD_LIST_TTL_SEC", "CCXT_TIMEFRAMES", "COMMAND_SUFFIX", "MT5_TIMEFRAMES",
           "TradeCardService"]
