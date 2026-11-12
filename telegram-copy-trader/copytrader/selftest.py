"""``python -m copytrader selftest``: a READ-ONLY health report in plain Turkish, made to be screenshotted.

It never places, changes or cancels an order: the brokers come from the dry-run factory that ``check`` and
``parse`` use and only read methods are called. (Native MT5: resolving a symbol can add it to the terminal's
Market Watch, exactly as the bot does when a signal arrives. That is not an order.)

What is checked
* every enabled broker: connect, account currency / equity / free margin, and where the adapter offers it:
  - ``health_problem()`` (native MT5: terminal connected, trading allowed, Python API not disabled; ccxt: a
    signed read answers);
  - ``preflight_report(bases)`` (ccxt: the adapter's own read-only account report: clock, position mode,
    contracts, what is already on the account). Its DİKKAT lines are listed as warnings;
  - native MT5: the installed MetaTrader5 package version;
  - ccxt: position mode against ``position_mode`` (a mismatch is a problem: orders would be refused);
    Binance: the API key's restrictions (withdrawals must be OFF, IP restriction, futures permission).
    Both are GET requests (checked against the installed ccxt in tests/test_preflight2_tools.py).
* every topic route of the config (all brokers when no topics are set): for each instrument of
  ``PROBE_ASSETS`` an example "1R long at market" signal with a typical stop goes through the engine's own
  ``evaluate`` and ``preview_open`` (as ``parse`` does), so symbol, price and size are what a real signal gets.
* the Telegram source group (only when the caller hands in a client factory; the CLI does, and it refuses to
  run while the bot is running): with the owner's user session, exactly as the listener and ``chats`` open it,
  the group's default member rights and the open / closed state of the followed topics are READ, to say
  whether ordinary members can post where the bot copies from. Nothing is sent, forwarded or marked as read.
  What it finds is a warning at most, never a problem.

Exit code 0: every enabled broker of every route could price and size every probe instrument it is expected
to trade and no blocker was found on an account; otherwise 1. "Not expected" (reported, never a problem): an
asset the broker's ``allowed_assets`` excludes, and on a route without a kind an instrument the broker does
not list as long as another broker of the route trades it.
"""

from __future__ import annotations

import asyncio
import html
import inspect
import logging
import re
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from .brokers.base import Broker
from .config import AppConfig, match_topic
from .models import ActionType, EntryType, MarketType, ParsedAction, Severity, Side, TakeProfit, TradePlan
from .parsing.symbols import normalize_symbol
from .telegram.formatting import fmt_price

# The instruments the owner trades, per route kind. ``--varlik BTC,EURUSD`` replaces the table.
PROBE_ASSETS: dict[str, tuple[str, ...]] = {
    "crypto": ("BTC", "ETH", "SOL"),
    "fx": ("EURUSD", "GBPUSD", "USDJPY", "XAUUSD"),
}
# Distance of the example stop from the price, in percent ("a typical stop").
PROBE_STOP_PCT: dict[str, float] = {"crypto": 1.0, "fx": 0.3}
CALL_TIMEOUT_SEC = 60.0            # one read call on one broker
PREVIEW_TIMEOUT_SEC = 180.0        # the engine's size preview of one instrument (all brokers of the route)
TELEGRAM_TIMEOUT_SEC = 45.0        # one Telegram request (connecting included)
TELEGRAM_TOPIC_LIMIT = 100         # topics read per forum (the listener lists as many at its start)
ONLY_ADMINS_POST = "TAMAM: bu konulara sadece yöneticiler yazabiliyor"
MEMBERS_POST = "DİKKAT: üyeler de yazabiliyor; allowed_sender_ids boş olduğu için yazan herkes kopyalanır"

_TAG = re.compile(r"<[^>]+>")
_FX_MARKETS = (MarketType.FOREX, MarketType.CFD)
_KIND_TR = {"manual": "elle işlem", "paper": "sanal hesap"}
_CRYPTO_TYPES = frozenset({"ccxt", "paper", "manual"})
_MT_TYPES = frozenset({"mt5", "metaapi"})

OK, BAD, WARN, SKIP = "[TAMAM]", "[SORUN]", "[UYARI]", "[  -- ]"


def _plain(text: object) -> str:
    return html.unescape(_TAG.sub("", str(text)))


def _reason(exc: BaseException, timeout: float | None = None) -> str:
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)) and not str(exc):
        return f"{timeout:.0f} sn içinde yanıt gelmedi" if timeout else "yanıt gelmedi (zaman aşımı)"
    return _plain(str(exc) or type(exc).__name__)


async def _read(awaitable: Any, timeout: float = CALL_TIMEOUT_SEC) -> tuple[Any, str | None]:
    """(value, None) or (None, Turkish reason). Never raises (a failing broker must not stop the report)."""
    try:
        return await asyncio.wait_for(awaitable, timeout), None
    except Exception as exc:      # noqa: BLE001 - the reason is the point of the report
        return None, _reason(exc, timeout)


def _package_version(name: str) -> str | None:
    from importlib import metadata

    try:
        return metadata.version(name)
    except Exception:             # noqa: BLE001 - not installed / broken metadata: unknown
        return None


class _Collector:
    """Notifier for the engine: keeps what it is told (nothing is sent anywhere, nothing is asked)."""

    def __init__(self) -> None:
        self.items: list[tuple[str, Severity]] = []

    async def start(self, engine: Any) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def ask_confirmation(self, conf: Any) -> int | None:
        return None

    async def close_confirmation(self, conf: Any, result_text: str) -> None:
        return None

    async def notify(self, text: str, severity: Severity = Severity.INFO) -> None:
        self.items.append((_plain(text), severity))


class _Report:
    def __init__(self, out: Callable[[str], None]) -> None:
        self.out = out
        self.problems: list[str] = []
        self.warnings: list[str] = []

    def say(self, text: str = "") -> None:
        self.out(text)

    def problem(self, where: str, text: str) -> None:
        self.problems.append(f"{where}: {text}")

    def warning(self, where: str, text: str) -> None:
        self.warnings.append(f"{where}: {text}")


# --------------------------------------------------------------------------- #
# Probe instruments
# --------------------------------------------------------------------------- #
def parse_assets(assets: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if not assets:
        return []
    items = assets.split(",") if isinstance(assets, str) else list(assets)
    return [a.strip().upper() for a in items if a and a.strip()]


def asset_spec(asset: str, kind: str) -> tuple[str, str | None, MarketType | None, float] | None:
    """(base, quote, market type, stop distance %) the way a signal of a ``kind`` topic is normalised, or None
    when the text is not an instrument."""
    base, quote, market_type = normalize_symbol(asset)
    if not base:
        return None
    if kind == "crypto":                       # crypto topic: USDT perpetual
        market_type = market_type or MarketType.PERP
        quote = quote or "USDT"
    fx = market_type in _FX_MARKETS
    return base, quote, market_type, PROBE_STOP_PCT["fx" if fx else "crypto"]


def assets_for(kind: str, override: list[str], broker_types: set[str] | None = None) -> list[str]:
    """Probe instruments of a route kind. A route without a kind gets the coins when it has a crypto venue
    (``broker_types``: ccxt / paper / manual) and the currency pairs when it has a MetaTrader account.
    An override list is split by what each text is: currency pairs and metals go to fx routes, everything
    else to crypto routes; a route without a kind gets all of them."""
    if not override:
        if kind in PROBE_ASSETS:
            return list(PROBE_ASSETS[kind])
        types = broker_types or set()
        groups = [k for k, wanted in (("crypto", _CRYPTO_TYPES), ("fx", _MT_TYPES)) if types & wanted]
        return [a for k in (groups or list(PROBE_ASSETS)) for a in PROBE_ASSETS[k]]
    if kind not in PROBE_ASSETS:
        return list(override)
    out = []
    for asset in override:
        is_fx = normalize_symbol(asset)[2] in _FX_MARKETS
        if is_fx == (kind == "fx"):
            out.append(asset)
    return out


# --------------------------------------------------------------------------- #
# Accounts
# --------------------------------------------------------------------------- #
def _inner(broker: Broker) -> Any:
    return getattr(broker, "inner", broker)        # behind the dry-run wrapper


def _kind(broker: Broker) -> str:
    return str(getattr(_inner(broker), "kind", broker.kind))


def _risk_text(config: AppConfig) -> str:
    parts = []
    for b in config.enabled_brokers:
        fixed = getattr(b, "risk_usd", None)
        if fixed:
            parts.append(f"{b.name} {fixed:g} USD")
        else:
            pct = config.risk.risk_per_r_pct * float(getattr(b, "risk_multiplier", 1.0) or 1.0)
            parts.append(f"{b.name} bakiyenin %{pct:g} kadarı")
    return ", ".join(parts) or "—"


def _brake_text(config: AppConfig) -> str:
    parts = []
    for b in config.enabled_brokers:
        limit = getattr(b, "daily_loss_limit_pct", None)
        if limit is None:
            limit = config.risk.daily_loss_limit_pct
        text = f"%{limit:g}" if limit and limit > 0 else "kapalı"
        if limit and limit > 0 and getattr(b, "daily_loss_include_open_risk", False):
            text += " (açık işlemlerin riski dahil)"
        parts.append(f"{b.name} {text}")
    return ", ".join(parts) or "—"


def _yes(value: Any) -> str:
    return "EVET" if value else "HAYIR"


async def _health_lines(broker: Broker, report: _Report) -> list[str] | None:
    """The adapter's own health check; None when this version of the adapter has none."""
    health = getattr(_inner(broker), "health_problem", None)
    if not callable(health):
        return None
    try:
        result = health()
        if inspect.isawaitable(result):
            result = await asyncio.wait_for(result, CALL_TIMEOUT_SEC)
    except Exception as exc:      # noqa: BLE001
        result = f"sağlık kontrolü yapılamadı: {_reason(exc, CALL_TIMEOUT_SEC)}"
    if result:
        report.problem(broker.name, _plain(result))
        return [f"{BAD} bağlantı kontrolü: {_plain(result)}"]
    return ["bağlantı kontrolü: TAMAM"]


async def _report_lines(broker: Broker, bases: list[str], report: _Report) -> list[str] | None:
    """The adapter's own read-only account report, when this version of the adapter has one (lines that start
    with TAMAM or DİKKAT). None: the adapter has none."""
    preflight = getattr(_inner(broker), "preflight_report", None)
    if not callable(preflight):
        return None
    try:
        result = preflight(list(bases))
        if inspect.isawaitable(result):
            result = await asyncio.wait_for(result, PREVIEW_TIMEOUT_SEC)
        lines = [_plain(line) for line in result or []]
    except Exception as exc:      # noqa: BLE001
        return [f"hesap raporu alınamadı: {_reason(exc, PREVIEW_TIMEOUT_SEC)}"]
    out = []
    own: int | None = None                          # index of this report's latest warning in the summary
    for line in lines:
        if line.startswith("DİKKAT"):
            text = line[len("DİKKAT"):].strip()
            detail = line.startswith("DİKKAT  ")              # an indented line continues the one above
            if detail and own is not None:
                report.warnings[own] += (" " if report.warnings[own].endswith(":") else "; ") + text
            elif text:
                report.warning(broker.name, text)
                own = len(report.warnings) - 1
            out.append(f"{WARN} {'   ' if detail else ''}{text}")
        else:
            out.append(line)
    return out


async def _ccxt_lines(broker: Broker, report: _Report, *, has_report: bool) -> list[str]:
    """Read-only extras of a ccxt account: position mode; Binance: the API key's restrictions.
    ``has_report``: the adapter printed its own report (a matching position mode is then not repeated)."""
    inner = _inner(broker)
    ex = getattr(inner, "exchange", None)
    cfg = broker.config
    if ex is None:
        return []
    lines: list[str] = []
    has = getattr(ex, "has", None) or {}
    if has.get("fetchPositionMode") and callable(getattr(ex, "fetch_position_mode", None)):
        mode, err = await _read(ex.fetch_position_mode())
        if err is not None or not isinstance(mode, dict) or mode.get("hedged") is None:
            lines.append(f"pozisyon modu okunamadı ({err or 'yanıt anlaşılamadı'})")
        else:
            actual = "hedge (çift yönlü)" if mode["hedged"] else "tek yönlü"
            want_hedge = getattr(cfg, "position_mode", "oneway") == "hedge"
            if bool(mode["hedged"]) != want_hedge:
                text = (f"hesabın pozisyon modu {actual}, ayarda position_mode: "
                        f"{getattr(cfg, 'position_mode', 'oneway')}. Bu haliyle emirler reddedilir; borsada "
                        f"pozisyon modunu değiştirin ya da ayarı düzeltin")
                report.problem(broker.name, text)
                lines.append(f"{BAD} {text}")
            elif not has_report:
                lines.append(f"pozisyon modu: {actual} (ayarla aynı)")
    elif not has_report:
        lines.append("pozisyon modu: bu borsada okunamıyor (kontrol edilmedi)")

    if not str(getattr(ex, "id", "")).lower().startswith("binance"):
        return lines
    if getattr(cfg, "sandbox", False) or getattr(cfg, "demo", False):
        lines.append("API anahtarı izinleri: test/demo hesabında okunamaz (kontrol edilmedi)")
        return lines
    call = getattr(ex, "sapiGetAccountApiRestrictions", None)
    if not callable(call):
        lines.append("API anahtarı izinleri: bu ccxt sürümünde okunamıyor (kontrol edilmedi)")
        return lines
    info, err = await _read(call())
    if err is not None or not isinstance(info, dict):
        lines.append(f"API anahtarı izinleri okunamadı ({err or 'yanıt anlaşılamadı'}); kontrol edilmedi")
        return lines
    withdrawals, ip, futures = info.get("enableWithdrawals"), info.get("ipRestrict"), info.get("enableFutures")
    lines.append("API anahtarı: çekim izni " + ("AÇIK" if withdrawals else "kapalı")
                 + ", IP kısıtı " + ("var" if ip else "YOK")
                 + ", vadeli işlem izni " + ("açık" if futures else "KAPALI"))
    if withdrawals:
        report.warning(broker.name, "API anahtarında para çekme izni AÇIK. Bot bunu kullanmaz; Binance'te "
                                    "API Yönetimi'nden kapatın")
    if ip is False:
        report.warning(broker.name, "API anahtarında IP kısıtı yok; güvenlik için anahtarı sunucunun IP "
                                    "adresiyle sınırlayın")
    if futures is False:
        report.problem(broker.name, "API anahtarında vadeli işlem (Futures) izni kapalı; emirler reddedilir")
    return lines


async def _account(broker: Broker, config: AppConfig, report: _Report, bases: list[str]) -> None:
    kind = _kind(broker)
    label = f"{broker.name} ({_KIND_TR.get(kind, kind)}"
    label += ", isteğe bağlı)" if getattr(broker.config, "optional", False) else ")"
    if kind == "manual":
        report.say(f"  {OK} {label}: emir gönderilmez, size Telegram'dan mesaj gelir; "
                   f"1R = {getattr(broker.config, 'risk_usd', None) or 50:g} USD")
        return
    values = []
    for call in (broker.get_equity, broker.get_free_margin, broker.account_currency):
        value, err = await _read(call())
        if err is not None:
            report.problem(broker.name, f"hesap bilgisi okunamadı: {err}")
            report.say(f"  {BAD} {label}: hesap bilgisi okunamadı: {err}")
            return
        values.append(value)
    equity, free, ccy = values
    report.say(f"  {OK} {label}: hesap değeri {equity:,.2f} {ccy}, kullanılabilir {free:,.2f} {ccy}")
    if not equity > 0:
        report.problem(broker.name, f"hesap değeri sıfır ({equity:,.2f} {ccy}); işlem açılamaz")
    details: list[str] = []
    health = await _health_lines(broker, report)
    if health is None:
        # No health check in this adapter: what it noticed while connecting is all there is. (With a health
        # check the same findings come from it, fresh; listing both would count one cause twice.)
        for warning in getattr(_inner(broker), "startup_warnings", None) or []:
            report.problem(broker.name, _plain(warning))
            details.append(f"{BAD} {_plain(warning)}")
    details += health or []
    if kind == "mt5":
        version = _package_version("MetaTrader5")
        details.append(f"MetaTrader5 paketi: {version or 'sürümü okunamadı'}")
    own = await _report_lines(broker, bases, report)
    details += own or []
    if kind == "ccxt":
        details += await _ccxt_lines(broker, report, has_report=own is not None)
    for line in details:
        report.say(f"          {line}")


# --------------------------------------------------------------------------- #
# Telegram: can ordinary members post where the bot copies from? (read-only)
# --------------------------------------------------------------------------- #
def members_can_post(entity: Any) -> bool | None:
    """Can an ordinary member post a text or a picture in this chat, by the chat's own settings (the default
    rights of its members)? None: what Telegram returned does not tell."""
    if getattr(entity, "broadcast", False) or getattr(entity, "gigagroup", False):
        return False                                # a channel / broadcast group: only admins post
    rights = getattr(entity, "default_banned_rights", None)
    if rights is None:
        return None
    if getattr(rights, "send_messages", False):
        return False
    text = not getattr(rights, "send_plain", False)
    picture = not (getattr(rights, "send_media", False)
                   or (getattr(rights, "send_photos", False) and getattr(rights, "send_docs", False)))
    return text or picture                          # a screenshot alone is a signal for the bot


async def _telegram_report(config: AppConfig, report: _Report, client_factory: Callable[[Any], Any]) -> None:
    """The TELEGRAM section. Uses the owner's user session like the listener does (``client_factory`` is the
    listener's ``make_user_client``) and only these calls: connect, is_user_authorized, get_entity,
    get_dialogs (when the session does not know the chat yet), messages.GetForumTopics, disconnect."""
    tg = config.telegram
    report.say()
    report.say("TELEGRAM GRUBU  (yalnızca okunur: hiçbir mesaj gönderilmez, hiçbir şey okundu sayılmaz)")

    def unread(reason: str) -> None:
        text = f"grup ayarları okunamadı ({reason}); üyelerin yazıp yazamadığı kontrol edilmedi"
        report.say(f"  {WARN} {text}")
        report.warning("Telegram", text)

    if not (tg.session_string or Path(f"{tg.session_path}.session").is_file()):
        unread("Telegram kullanıcı oturumu yok; 'python -m copytrader login' çalıştırın")
        return                                      # (never creates an empty session file)

    async def wait(awaitable: Any) -> Any:
        return await asyncio.wait_for(awaitable, TELEGRAM_TIMEOUT_SEC)

    places: list[tuple[str, bool | None]] = []      # (where the bot copies from, can ordinary members post there?)
    topics_followed = False
    client: Any = None
    try:
        from telethon import functions

        from .telegram.listener import normalize_chat_ref

        client = client_factory(tg)
        await wait(client.connect())
        if not await wait(client.is_user_authorized()):
            unread("Telegram oturumu geçersiz; 'python -m copytrader login' çalıştırın")
            return
        for ref in tg.source_chats:
            target = normalize_chat_ref(ref)
            try:
                entity = await wait(client.get_entity(target))
            except (ValueError, TypeError):
                await wait(client.get_dialogs())    # a fresh session knows no chat yet (the listener does the same)
                entity = await wait(client.get_entity(target))
            name = f'"{getattr(entity, "title", None) or ref}"'
            group = members_can_post(entity)
            report.say(f"  Grup {name}: grup ayarında " + {
                True: "üyeler mesaj yazabiliyor", False: "üyelerin mesaj yazması kapalı (yalnızca yöneticiler yazabilir)",
                None: "üyelerin yazma izni okunamadı"}[group])
            if not tg.topics:
                places.append((f"grup {name}", group))
                continue
            if not getattr(entity, "forum", False):
                text = (f"config.yaml'da konular tanımlı ama {name} konulara ayrılmış bir grup değil; bot bu gruptan "
                        "hiçbir mesajı kopyalamaz")
                report.say(f"    {WARN} {text}")
                report.warning("Telegram", text)
                continue
            topics_followed = True
            result = await wait(client(functions.messages.GetForumTopicsRequest(
                peer=entity, offset_date=None, offset_id=0, offset_topic=0, limit=TELEGRAM_TOPIC_LIMIT)))
            listed = [t for t in getattr(result, "topics", None) or [] if getattr(t, "title", None)]
            found: set[str] = set()
            for topic in listed:
                route = match_topic(tg.topics, topic.title)       # the listener's own rule
                if route is None:
                    continue
                found.add(route.name)
                closed = bool(getattr(topic, "closed", False))
                can = False if closed else group
                places.append((f'"{topic.title}"', can))
                report.say(f'    konu "{topic.title}" ({route.name}): ' + (
                    "kapalı konu; üyeler yazamaz" if closed else
                    {True: "açık konu; üyeler yazabilir", False: "açık konu, ama grupta üyelerin yazması kapalı",
                     None: "açık konu; üyelerin yazma izni okunamadı"}[group]))
            more = int(getattr(result, "count", 0) or 0) > len(getattr(result, "topics", None) or [])
            for route in tg.topics:
                if route.name not in found:
                    text = (f"'{route.name}' rotasına uyan bir konu " + ("ilk 100 konu arasında" if more else "grupta")
                            + f" bulunamadı (aranan: {', '.join(route.title_contains)})")
                    report.say(f"    {WARN} {text}")
                    report.warning("Telegram", text)
    except Exception as exc:      # noqa: BLE001 - this section informs; it must never break the report
        unread(_reason(exc, TELEGRAM_TIMEOUT_SEC))
        return
    finally:
        if client is not None:
            try:
                await asyncio.wait_for(client.disconnect(), 15.0)
            except Exception:     # noqa: BLE001
                pass

    open_places = [where for where, can in places if can]
    unknown = [where for where, can in places if can is None]
    if open_places and tg.allowed_sender_ids:
        report.say(f"  TAMAM: üyeler de yazabiliyor ({', '.join(open_places)}), ama allowed_sender_ids ayarlı: yalnızca "
                   f"listedeki {len(tg.allowed_sender_ids)} gönderenin mesajları kopyalanır")
    elif open_places:
        report.say(f"  {MEMBERS_POST}")
        report.warning("Telegram", f"{', '.join(open_places)}: üyeler de yazabiliyor; allowed_sender_ids boş olduğu için "
                                   "yazan herkes kopyalanır. Yalnızca yöneticiler kopyalansın isterseniz config.yaml'da "
                                   "telegram.allowed_sender_ids listesine onların kullanıcı ID'lerini yazın")
    elif unknown:
        unread(f"{', '.join(unknown)} için üyelerin yazma izni okunamadı")
    elif places:
        report.say(f"  {ONLY_ADMINS_POST}" if topics_followed else
                   "  TAMAM: bu gruba sadece yöneticiler yazabiliyor")


# --------------------------------------------------------------------------- #
# Routes and instruments
# --------------------------------------------------------------------------- #
def _accepts(broker: Broker | None, broker_cfg: Any, base: str) -> bool:
    """``allowed_assets`` of a broker; for one that is not connected the same rule on its config."""
    try:
        if broker is not None:
            return bool(broker.accepts_asset(base))
        return bool(Broker.accepts_asset(SimpleNamespace(config=broker_cfg), base))   # type: ignore[arg-type]
    except Exception:             # noqa: BLE001 - cannot tell: treat the asset as expected on this account
        return True


async def _probe(engine: Any, route: Any, kind: str, names: list[str], asset: str, connected: dict[str, Broker],
                 down: dict[str, str], where: str, report: _Report) -> None:
    spec = asset_spec(asset, kind)
    width = max((len(n) for n in names), default=0)
    cfgs = {b.name: b for b in engine.config.enabled_brokers}

    def line(mark: str, name: str, text: str) -> None:
        report.say(f"    {mark} {name.ljust(width)}: {text}")

    def not_traded(name: str, base: str) -> bool:
        """Prints why ``name`` is not probed for this asset (excluded by its config, or the account is down)."""
        if not _accepts(connected.get(name), cfgs.get(name), base):
            line(SKIP, name, f"{base} bu hesabın izin listesinde yok (allowed_assets); atlandı")
            return True
        if name not in connected:               # the reason is listed once, under HESAPLAR
            line(BAD, name, "hesap bağlı değil")
            return True
        return False

    def all_fail(text: str) -> None:
        for name in names:
            if not not_traded(name, spec[0] if spec else asset):
                line(BAD, name, text)
                report.problem(f"{where} / {asset} / {name}", text)

    if spec is None:
        report.say(f"  {asset}")
        all_fail("varlık adı anlaşılamadı")
        return
    base, quote, market_type, stop_pct = spec
    price, err = await _read(engine.prices.price(base, quote, market_type, asset))
    if not price or not price > 0:
        report.say(f"  {asset}")
        all_fail(f"güncel fiyat alınamadı ({err or 'hiçbir hesap ya da borsa fiyat vermedi'})")
        return
    report.say(f"  {asset}  (güncel fiyat {fmt_price(price)}, örnek stop {fmt_price(price * (1 - stop_pct / 100))})")
    action = ParsedAction(
        action=ActionType.OPEN, symbol=asset, base_asset=base, quote_asset=quote, market_type=market_type,
        side=Side.LONG, entry_type=EntryType.MARKET, stop_loss=price * (1.0 - stop_pct / 100.0),
        take_profits=[TakeProfit(price=price * (1.0 + 2.0 * stop_pct / 100.0))], risk_r=1.0, confidence=1.0,
        summary_tr=f"{asset} sağlık kontrolü örneği",
    )
    ev, err = await _read(engine.evaluate(action, 0, route=route))
    plan = getattr(ev, "plan", None)
    if not isinstance(plan, TradePlan):
        why = err or "; ".join(_plain(i.message_tr) for i in getattr(ev, "issues", [])) or "plan oluşturulamadı"
        all_fail(f"örnek sinyal bu ayarlarla işlenemedi: {why}")
        return
    if ev.issues:
        asked = "; ".join(_plain(i.message_tr) for i in ev.issues)
        report.say(f"    {WARN} bot bu örnek sinyali otomatik açmaz, size sorardı: {asked}")
        report.warning(f"{where} / {asset}", f"örnek sinyal için onay sorulurdu: {asked}")
    lines, err = await _read(engine.preview_open(plan, raw_symbol=asset), PREVIEW_TIMEOUT_SEC)
    preview = {name: next((ln[len(name) + 2:] for ln in lines or [] if ln.startswith(f"{name}: ")), None)
               for name in names}

    unlisted: list[str] = []
    traded = 0
    for name in names:
        if not_traded(name, base):
            continue
        broker = connected[name]
        sym, s_err = await _read(broker.resolve_symbol(base, quote, market_type, asset))
        if sym is None:
            if s_err is None and kind not in PROBE_ASSETS:      # a route without a kind: see the module docstring
                unlisted.append(name)
                line(SKIP, name, f"{asset} bu hesapta yok; bu hesapta açılmaz")
                continue
            text = s_err or (f"{asset} bu hesapta bulunamadı (sembol adı farklı olabilir: brokerın ayarına "
                             f"symbol_map ekleyin)")
            line(BAD, name, text)
            report.problem(f"{where} / {asset} / {name}", text)
            continue
        quote_now, q_err = await _read(broker.get_quote(sym))
        if q_err is not None or not (quote_now.bid > 0 and quote_now.ask > 0):
            text = q_err or f"geçersiz fiyat (alış {quote_now.bid}, satış {quote_now.ask})"
            line(BAD, name, f"{sym.broker_symbol}: {text}")
            report.problem(f"{where} / {asset} / {name}", f"{sym.broker_symbol}: {text}")
            continue
        shown = fmt_price(quote_now.entry_price_for(Side.LONG))
        size = preview.get(name)
        sized = size is not None and size.startswith(f"{sym.broker_symbol} miktar ")
        if not sized:
            text = _plain(size) if size else (err or "miktar hesabı sonuç vermedi")
            line(BAD, name, f"{sym.broker_symbol} fiyat {shown} | {text}")
            report.problem(f"{where} / {asset} / {name}", f"{sym.broker_symbol}: {text}")
            continue
        traded += 1
        line(OK, name, f"{sym.broker_symbol} fiyat {shown} | {_plain(size[len(sym.broker_symbol) + 1:])}")
    if unlisted and not traded:
        report.problem(f"{where} / {asset}", "bu varlığı işleyebilen hesap yok")


async def run_selftest(
    config: AppConfig,
    brokers: list[Broker],
    broker_errors: list[str],
    *,
    config_path: str = "config.yaml",
    assets: str | list[str] | None = None,
    oracle_factory: Callable[[AppConfig, Any], Any] | None = None,
    reference_prices: Any | None = None,
    out: Callable[[str], None] = print,
    now: datetime | None = None,
    telegram_client: Callable[[Any], Any] | None = None,
) -> int:
    """Print the report through ``out`` and return the exit code (0 = no problem, 1 = problems listed).

    ``brokers`` / ``broker_errors``: what the CLI's dry-run broker factory returned (adapters and
    "name: reason" texts of brokers that could not even be created). ``oracle_factory(config, brokers_fn)``
    and ``reference_prices`` replace the engine's price sources (tests). ``telegram_client(telegram_config)``
    returns the owner's user client (the listener's ``make_user_client``); None: the Telegram section is left
    out. The caller must make sure that the bot is not running (the same session would be opened twice)."""
    from .engine.orchestrator import Engine
    from .storage.db import Database

    report = _Report(out)
    override = parse_assets(assets)
    stamp = (now or datetime.now()).strftime("%d.%m.%Y %H:%M")
    report.say(f"BOT SAĞLIK KONTROLÜ  ({stamp})  -  hiçbir emir gönderilmez")
    report.say(f"Ayar dosyası: {config_path} | Mod: "
               + ("DRY-RUN (bot gerçek emir göndermez)" if config.dry_run else "CANLI (bot gerçek emir gönderir)"))
    report.say()
    report.say("AYARLAR")
    report.say(f"  1R (işlem başına risk): {_risk_text(config)}")
    report.say(f"  Her işlem bildirilir: {_yes(config.notifier.notify_on_execution)} | işlem sayılmayan "
               f"mesajlar bildirilir: {_yes(config.notifier.notify_on_ignored)}")
    report.say(f"  En fazla açık işlem: {config.risk.max_open_trades} | en küçük miktara yuvarlama: "
               f"{'AÇIK' if config.risk.allow_min_qty_round_up else 'KAPALI'}")
    report.say(f"  Günlük zarar freni: {_brake_text(config)}")
    report.say()

    db = Database(":memory:")              # the bot's own database is not opened
    await db.open()
    oracle = None
    engine: Any = None
    try:
        if oracle_factory is not None:
            oracle = oracle_factory(config, lambda: engine.brokers if engine is not None else [])
        engine = Engine(config, db, None, brokers, _Collector(), price_oracle=oracle,  # type: ignore[arg-type]
                        reference_prices=reference_prices)
        report.say("Hesaplara bağlanılıyor (1-2 dakika sürebilir)...")
        await engine.connect_brokers()
        connected: dict[str, Broker] = {b.name: b for b in engine.brokers}
        down: dict[str, str] = {name: _plain(reason) for name, reason in engine.failed_brokers.items()}
        for err in broker_errors:
            name, _, reason = err.partition(":")
            down[name.strip()] = _plain(reason.strip() or err)

        routes = list(config.telegram.topics)
        enabled = [b.name for b in config.enabled_brokers]
        routed = {n for r in routes for n in (r.brokers or enabled)} if routes else set(enabled)

        # Coins each broker is expected to trade (for an adapter's own account report).
        bases_of: dict[str, list[str]] = {}
        for route_kind, route_names in ([(r.kind, list(r.brokers or enabled)) for r in routes] or [("any", enabled)]):
            types = {str(getattr(b, "type", "")) for b in config.enabled_brokers if b.name in route_names}
            for asset in assets_for(route_kind, override, types):
                spec = asset_spec(asset, route_kind)
                if spec is None or spec[2] in _FX_MARKETS:
                    continue
                for name in route_names:
                    if spec[0] not in bases_of.setdefault(name, []):
                        bases_of[name].append(spec[0])

        report.say()
        report.say("HESAPLAR")
        if not enabled:
            report.say(f"  {BAD} etkin hesap yok (config.yaml'da enabled: true olan broker yok)")
            report.problem("ayar", "etkin hesap yok")
        for name in enabled:
            broker = connected.get(name)
            if broker is None:
                optional = bool(getattr(next(b for b in config.enabled_brokers if b.name == name), "optional", False))
                text = f"bağlanılamadı: {down.get(name) or 'sebep bilinmiyor'}"
                report.say(f"  {BAD} {name}{' (isteğe bağlı)' if optional else ''}: {text}")
                report.problem(name, text)
                continue
            await _account(broker, config, report, bases_of.get(name, []))
            if name not in routed:
                report.say("          not: hiçbir konuya bağlı değil; bu hesapta işlem açılmaz")

        pseudo = [(None, "any", enabled, "Tüm mesajlar (konu ayarı yok)")]
        for route, kind, names, title in ([(r, r.kind, list(r.brokers or enabled), f'KONU "{r.name}"')
                                           for r in routes] or pseudo):
            report.say()
            active = [n for n in names if n in enabled]
            off = [n for n in names if n not in enabled]
            stops = " / ".join(f"%{PROBE_STOP_PCT[k]:g} ({'kripto' if k == 'crypto' else 'fx'})"
                               for k in PROBE_STOP_PCT if kind in (k, "any"))
            report.say(f"{title} -> {', '.join(names) or '—'}   (örnek: 1R long, stop fiyattan {stops} uzakta)")
            for name in off:
                report.say(f"    {SKIP} {name}: kapalı (enabled: false); bu hesapta işlem açılmaz")
            if not active:
                report.say(f"    {BAD} bu konunun etkin hesabı yok; sinyalleri hiçbir hesapta açılmaz")
                report.problem(title, "etkin hesap yok")
                continue
            types = {str(getattr(b, "type", "")) for b in config.enabled_brokers if b.name in active}
            for asset in assets_for(kind, override, types):
                await _probe(engine, route, kind, active, asset, connected, down,
                             route.name if route is not None else "tüm mesajlar", report)
    finally:
        if engine is not None:
            try:
                await engine.stop(grace_sec=5.0)
            except Exception:     # noqa: BLE001 - closing must never hide the report
                logging.getLogger(__name__).debug("engine stop failed", exc_info=True)
        close = getattr(oracle, "close", None)
        if close is not None:
            try:
                await close()
            except Exception:     # noqa: BLE001
                pass
        await db.close()

    if telegram_client is not None:
        await _telegram_report(config, report, telegram_client)

    report.say()
    if report.warnings:
        report.say("UYARILAR (işlem açmayı engellemez)")
        for i, text in enumerate(report.warnings, start=1):
            report.say(f"  {i}. {text}")
        report.say()
    if not report.problems:
        report.say("SONUÇ: SORUN YOK. Her hesap, işlem açması beklenen her varlıkta fiyat alıp miktar hesaplayabildi.")
        return 0
    report.say(f"SONUÇ: {len(report.problems)} SORUN VAR")
    for i, text in enumerate(report.problems, start=1):
        report.say(f"  {i}. {text}")
    return 1


__all__ = ["PROBE_ASSETS", "PROBE_STOP_PCT", "asset_spec", "assets_for", "members_can_post", "parse_assets",
           "run_selftest"]
