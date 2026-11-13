"""MT4/MT5 through the MetaApi cloud (metaapi-cloud-sdk 29.x). Works on Linux/Docker.

Uses an RPC connection (request/response, no local terminal state). The
leg-per-TP trading logic is shared with Mt5Broker (``MetaTraderLegBroker``);
this module only implements the venue primitives.
"""

from __future__ import annotations

import asyncio
import contextlib
import html
import logging
import re
import weakref
from collections.abc import Awaitable, Callable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from ..config import MetaApiBrokerConfig
from ..models import BrokerPosition, MarketType, OpenRequest, Quote, Severity, Side, SymbolInfo, TakeProfit
from .base import BrokerError, BrokerUnavailableError
from .mt5_broker import (
    MT5,
    RETCODE_TR,
    MetaTraderLegBroker,
    OrderReplyLostError,
    _snap,
    mapped_symbol,
    new_leg,
    rank_symbol,
    retcode_text,
    symbol_candidates,
)

logger = logging.getLogger(__name__)

_EPS = 1e-9
_NETTING_MODES = ("ACCOUNT_MARGIN_MODE_RETAIL_NETTING", "ACCOUNT_MARGIN_MODE_EXCHANGE")
_PENDING_TYPES = (
    "ORDER_TYPE_BUY_LIMIT", "ORDER_TYPE_SELL_LIMIT", "ORDER_TYPE_BUY_STOP", "ORDER_TYPE_SELL_STOP",
    "ORDER_TYPE_BUY_STOP_LIMIT", "ORDER_TYPE_SELL_STOP_LIMIT",
)
_TRADE_MODES = {
    "SYMBOL_TRADE_MODE_DISABLED": MT5.SYMBOL_TRADE_MODE_DISABLED,
    "SYMBOL_TRADE_MODE_LONGONLY": MT5.SYMBOL_TRADE_MODE_LONGONLY,
    "SYMBOL_TRADE_MODE_SHORTONLY": MT5.SYMBOL_TRADE_MODE_SHORTONLY,
    "SYMBOL_TRADE_MODE_CLOSEONLY": MT5.SYMBOL_TRADE_MODE_CLOSEONLY,
    "SYMBOL_TRADE_MODE_FULL": MT5.SYMBOL_TRADE_MODE_FULL,
}
_DEAL_REASONS = {"DEAL_REASON_SL": "sl", "DEAL_REASON_TP": "tp", "DEAL_REASON_SO": "stop_out"}
_MAX_TAG_PLUS_COMMENT = 26      # SDK: len(comment) + len(clientId) <= 26
# clientId as MetaApi requires it (https://metaapi.cloud/docs/client/clientIdUsage/, linked from the SDK's
# TradeOptions): ${strategyId}_${positionId}_${orderId}, e.g. "RF_EURUSD_GjCy5lk" there and "TE_GBPUSD_7hyINWqAl" in
# MetaApi's own SDK examples. The live service refused the bare internal tag ("ctd7c454ab04": "Value must match
# required pattern"). This bot writes the tag "ct<body>[-<leg>]" as "CT_<body>[L<leg>]_0": ASCII letters and digits
# only inside each part. The third part may be replaced by MetaApi (trades from a stop loss / take profit), so it
# is a fixed "0" and is ignored when read back.
_CID_STRATEGY = "CT"
_CID_ORDER = "0"
_CID_RE = re.compile(r"([A-Za-z0-9]+)_([A-Za-z0-9]+)_([A-Za-z0-9]+)")
_CID_POSITION_RE = re.compile(r"([A-Za-z0-9]+?)(?:L([1-9][0-9]*))?")
_TAG_RE = re.compile(r"ct([A-Za-z0-9]+)(?:-([1-9][0-9]*))?")       # internal tag: "ct" + body [+ "-<leg>"]
_LABEL_FIELDS = ("clientId", "comment")      # trade request fields that only label the order
# SDK exceptions (clients/error_handler.py) that MetaApi raises BEFORE a request reaches the terminal: the request
# was refused as such (400 invalid input, 401 / 403 not allowed, 429 rate limit), so nothing was executed. Every
# other error of an order request (timeout, not connected / synchronized, internal, not found) leaves its outcome
# unknown and stays a lost reply.
_REFUSED_ERRORS = ("ValidationException", "UnauthorizedException", "ForbiddenException", "TooManyRequestsException")
_REASON_MAX = 160                # characters of MetaApi's own words in a refusal reason
RPC_TIMEOUT_SEC = 120.0          # trade requests + connect chain. SDK calls have their own timeouts; last resort
READ_TIMEOUT_SEC = 30.0          # pure reads (account, price, specification, positions, orders, history): one
                                 # that is never answered must not hold the shared loops for two minutes
HEALTH_TIMEOUT_SEC = 20.0        # health_problem(): everything it does must fit in this
HEALTH_SYNC_GRACE_SEC = 5.0      # health_problem(): a reconnect shorter than this is not reported
CLOSE_WAIT_SEC = 10.0            # close(): how long to wait for the SDK's own (scheduled) connection close
SOCKET_CLOSE_SEC = 3.0           # dropping an SDK client: how long each of its sockets gets for a polite goodbye
RECONNECT_AFTER_PROBLEMS = 3     # failed health checks in a row before the connection is rebuilt from scratch
RECONNECT_BACKOFF_SEC = (60.0, 120.0, 300.0, 600.0)     # pause after the 1st, 2nd, 3rd and every later attempt
RECONNECT_SLACK_SEC = 5.0        # a health check this much before the end of a pause still starts the attempt
RECONNECT_TIMEOUT_SEC = 180.0    # one reconnect attempt: the same limit the engine gives the first connect()
LINK_WATCH_SEC = 0.5             # the adapter looks at the SDK's own view of the link this often (no request is made);
                                 # a socket that ends wakes it at once (_handle_eio_disconnect), this is the fallback
LINK_CONFIRM_SEC = 0.0           # ... and rebuilds the connection when the SDK has reported its sockets ended this long
                                 # without bringing them back itself: at once. Production (2026-11-13): MetaApi closed
                                 # both sockets every 10-15 min and the SDK's own socket reconnect never brought them
                                 # back within the 20 s this used to wait (it made one attempt and then none for hours
                                 # on an earlier day); every drop was rebuilt after those 20 s. "Not synchronized" on
                                 # live sockets (the terminal lost the broker) is NOT rebuilt fast: a new MetaApi
                                 # client does not bring the broker back; the health checks' three-check rule decides
                                 # there, as before
SYNC_POLL_SEC = 0.05             # connect(): how often "synchronized" is looked at (the SDK itself: once a second)
SDK_SYNC_TIMEOUT_SEC = 300.0     # RpcMetaApiConnectionInstance.wait_synchronized(): the SDK's default limit
LINK_SETTLE_SEC = 90.0           # a rebuilt link that stayed up this long ends the outage without a health check, but
                                 # only when the account answered a request after the rebuild and no check failed
                                 # since (longer than one heartbeat: with the heartbeat running, its check decides)
LINK_NOTICE_QUIET_SEC = 1800.0   # at most one "short drop" notice in this time; the drops in between are counted
HEARTBEAT_TELLS_AFTER = 3        # engine/health.py HEARTBEAT_ALERT_AFTER: from this many failed checks in a row the
                                 # heartbeat itself tells the owner "down" and later "✅"; the adapter then does not
PING_MISSES_ALLOWED = 1          # engine.io 3 keep-alive: unanswered PINGs in a row a socket survives (library: none)
UNHEALTHY_TTL_SEC = 180.0        # a failed health check older than this no longer refuses new signals
UNHEALTHY_AFTER_CHECKS = 2       # failed health checks in a row before new signals are refused (one late answer is
                                 # not enough; the SDK reporting the link down is: see _refuse_while_unhealthy)
DEGRADED_READ_TIMEOUT_SEC = 3.0  # pure reads while the latest request of this account got no answer, or the SDK
                                 # reports the link down (see _degraded): a dead link must not hold the shared
                                 # flows (monitor pass, management, the open flow) for READ_TIMEOUT_SEC each time
_NO_ANSWER_ERRORS = ("TimeoutException", "NotSynchronizedException", "NotConnectedException")   # SDK: no answer
REQUOTE_RETRIES = 2              # extra attempts of a market order after a requote / price-changed answer
_REQUOTE_RETCODES = (MT5.TRADE_RETCODE_REQUOTE, MT5.TRADE_RETCODE_PRICE_CHANGED, MT5.TRADE_RETCODE_PRICE_OFF)
LOST_REPLY_CHECKS = 3            # look-ups by tag after an order request that got no trade answer
LOST_REPLY_DELAY_SEC = 2.0       # pause between them (the order may still be on its way to the trade server)
SOCKET_RETRY_PAUSE_SEC = 1.0     # pause after a failed connection attempt of an SDK socket (the SDK has none)
_DETAIL_MAX = 300                # characters of ValidationException.details / TooManyRequests metadata shown
_RATE_SYMBOL_TRIES = 3           # sizing without a tick value: listings of the conversion pair tried for a price
# priceCalculationMode values whose profit MetaTrader computes from the tick value, not from the contract size
_TICK_ONLY_CALC_MODES = ("FUTURES", "BONDS", "OPTIONS", "COLLATERAL")
_CUT_TR = "bağlantı kopuk; yeniden bağlanması bekleniyor"                      # after "MetaApi <what>: "
_DISCONNECTED_TR = "MetaApi bağlantısı kopuk; yeniden bağlanması bekleniyor."   # on its own
_TRADE_OFF_TR = "MetaApi hesabında işlem izni kapalı görünüyor."     # the account answers: nothing to rebuild
_RECONNECTING_TR = "MetaApi bağlantısı yenileniyor."
_SOCKETS_DOWN_TR = "MetaApi websocket bağlantısı kapandı."     # the SDK's sockets ended and are not back
_WHY_TR = {_SOCKETS_DOWN_TR: "websocket kapandı"}     # the only reason the link watch rebuilds fast (with "no client")
_DROPPED_TR = "bağlantı yenilenirken istek yanıtsız kaldı"           # a call that was waiting on a dropped client
_TOKEN_RE = re.compile(r"(auth-token=)[^&\s\"'<>]+")                 # never in a notice or a log line of ours
# ReconnectPolicy events
_DOWN, _RECONNECT, _RECONNECT_FAILED, _RECOVERED = "down", "reconnect", "reconnect_failed", "recovered"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class MetaApiError(BrokerError):
    """BrokerError carrying the MetaTrader trade return code (TradeException.numericCode) if any."""

    def __init__(self, message: str, code: int | None = None, string_code: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.string_code = string_code
        self.no_answer = False          # the request got no answer (timeout, link cut): nothing is known
        self.refused = False            # MetaApi refused the request before executing it (see _REFUSED_ERRORS)
        self.reason = ""                # ... why, in a few words for the owner
        self.fields: tuple[str, ...] = ()     # ... the request fields its ValidationException names


def encode_client_id(tag: str) -> str | None:
    """The clientId for an internal order tag: "ctd7c454ab04" -> "CT_d7c454ab04_0", "ctd7c454ab04-2" ->
    "CT_d7c454ab04L2_0". None when the tag cannot be written so that :func:`decode_client_id` gives it back
    exactly (the order then goes without a clientId; its comment carries the tag)."""
    m = _TAG_RE.fullmatch(tag or "")
    if m is None:
        return None
    body, leg = m.groups()
    cid = f"{_CID_STRATEGY}_{body}{'L' + leg if leg else ''}_{_CID_ORDER}"
    return cid if decode_client_id(cid) == tag else None


def decode_client_id(value: Any) -> str:
    """The internal tag a clientId of this bot carries ("CT_d7c454ab04L2_<anything>" -> "ctd7c454ab04-2"; the
    third part is ignored). A value that is itself an internal tag (the form 62e0719 sent) counts as that tag.
    "" for anything else: a foreign clientId."""
    text = str(value or "")
    m = _CID_RE.fullmatch(text)
    if m is not None:
        strategy, position, _order = m.groups()
        p = _CID_POSITION_RE.fullmatch(position)
        if strategy != _CID_STRATEGY or p is None:
            return ""
        body, leg = p.groups()
        return f"ct{body}" + (f"-{leg}" if leg else "")
    return text if _TAG_RE.fullmatch(text) else ""


def _is_sdk_error(exc: BaseException, name: str) -> bool:
    return any(cls.__name__ == name for cls in type(exc).__mro__)


def _refused_fields(exc: BaseException) -> tuple[str, ...]:
    """The request fields a ValidationException names (details: [{"parameter": ..., "message": ...}, ...])."""
    if not _is_sdk_error(exc, "ValidationException"):
        return ()
    try:
        details = getattr(exc, "details", None)
    except Exception:  # noqa: BLE001 - a property of a foreign exception class
        return ()
    names: list[str] = []
    for item in details if isinstance(details, (list, tuple)) else ():
        name = item.get("parameter") if isinstance(item, dict) else None
        if isinstance(name, str) and name and name not in names:
            names.append(name)
    if not names:       # no structured details: a label field named in the text still counts
        text = f"{exc} {details or ''}"
        names = [f for f in _LABEL_FIELDS if f in text]
    return tuple(names)


def _short(text: Any) -> str:
    text = _TOKEN_RE.sub(r"\1***", " ".join(str(text or "").split()))     # never a token in an owner's text
    return text if len(text) <= _REASON_MAX else text[: _REASON_MAX - 1] + "…"


def _refusal_reason(exc: BaseException, fields: tuple[str, ...]) -> str:
    """Why MetaApi refused a request, in a few words for the owner (MetaApi's own words where they help)."""
    if _is_sdk_error(exc, "ValidationException"):
        said = ""
        details = getattr(exc, "details", None)
        for item in details if isinstance(details, (list, tuple)) else ():
            if isinstance(item, dict) and item.get("message"):
                said = str(item["message"])
                break
        what = f"geçersiz alan: {', '.join(fields)}" if fields else "geçersiz istek"
        return _short(f"{what} – {said}" if said else f"{what} – {exc}")
    if _is_sdk_error(exc, "TooManyRequestsException"):
        return "MetaApi istek sınırı aşıldı, kısa sürede çok fazla istek gönderildi"
    if _is_sdk_error(exc, "UnauthorizedException"):
        return _short(f"MetaApi erişim anahtarı (METAAPI_TOKEN) kabul edilmedi – {exc}")
    return _short(f"MetaApi bu hesapta bu işleme izin vermiyor – {exc}")


def _error_detail(exc: BaseException) -> str:
    """What exactly was refused: ValidationException.details / TooManyRequestsException.metadata, trimmed."""
    try:
        extra = getattr(exc, "details", None) or getattr(exc, "metadata", None)
    except Exception:  # noqa: BLE001 - a property of a foreign exception class; diagnostics only
        return ""
    if not extra or not isinstance(extra, (list, tuple, dict, str)):
        return ""
    text = str(extra)
    return text if len(text) <= _DETAIL_MAX else text[: _DETAIL_MAX - 1] + "…"


def _cancel_requested() -> bool:
    """True when the running task itself was asked to stop (shutdown, an outer timeout): that CancelledError must
    go on. False: nobody cancelled us, the SDK only awaited one of its own futures that somebody else cancelled."""
    task = asyncio.current_task()
    cancelling = getattr(task, "cancelling", None)      # Python 3.11+
    return task is None or cancelling is None or cancelling() > 0


_LINK_WATCHERS: weakref.WeakSet[MetaApiBroker] = weakref.WeakSet()    # adapters whose link watch runs


def _socket_dropped() -> None:
    """An SDK socket's engine.io connection just ended: every link watch of this event loop looks at once (cheap,
    no request; each looks at its own client's sockets only)."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    for broker in list(_LINK_WATCHERS):
        task = broker._watch_task
        if task is not None and not task.done() and task.get_loop() is loop:
            broker._wake_link_watch()


def _pausing_socketio(real: Any) -> Any:
    """``socketio`` as an SDK module sees it, with an AsyncClient that waits SOCKET_RETRY_PAUSE_SEC after a FAILED
    connect() before it hands the error on, and that says at once when its engine.io connection ended. Everything
    else is the real module."""

    class _PausingClient(real.AsyncClient):  # type: ignore[name-defined,misc]
        copytrader_dropped = False      # the engine.io connection ended and connect() did not succeed since

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            _patient_keepalive(getattr(self, "eio", None))

        async def connect(self, *args: Any, **kwargs: Any) -> Any:
            try:
                result = await super().connect(*args, **kwargs)
            except Exception:
                await asyncio.sleep(SOCKET_RETRY_PAUSE_SEC)
                raise
            self.copytrader_dropped = False
            return result

        async def _handle_eio_disconnect(self) -> None:
            # python-socketio 4.6.1 runs the SDK's 'disconnect' handler INSIDE this call, and that handler is the
            # SDK's whole socket reconnect loop (metaapi_websocket_client.py _reconnect: it sleeps first, then
            # calls disconnect()). Until then the socket still reads as connected, so the link watch would see
            # the drop only a second or more later. Marked and told here, before that handler runs.
            self.copytrader_dropped = True
            _socket_dropped()
            result = super()._handle_eio_disconnect()
            if asyncio.iscoroutine(result):     # python-socketio 4.x / 5.x: a coroutine
                await result

    class _Socketio:
        copytrader_paused = True
        AsyncClient = _PausingClient

        def __getattr__(self, name: str) -> Any:
            return getattr(real, name)

    return _Socketio()


def _patient_keepalive(eio: Any) -> bool:
    """python-engineio 3.x client (Engine.IO protocol 3, used by the SDK's socket.io 4.x client): the CLIENT sends a
    PING every ping_interval and cuts the socket when the previous PING has no PONG by the next tick, so ONE late or
    lost answer kills the socket. Here a socket survives PING_MISSES_ALLOWED unanswered PINGs in a row: a dead link
    is still given up one ping interval later at the latest, and the PINGs keep going meanwhile, so the server keeps
    its side too. A ping loop that is not the client's current one ends at its next tick: disconnect(abort=True)
    leaves the loop of the old connection running, and when the client connects again the two loops share
    pong_received and one cuts the new socket for no reason (seen in the forensics rig with 0.8 s round trips). And
    when the read loop ends first (the peer closed the socket, or it broke: the read loop logs why) the loop just
    ends: the library logs "PONG response has not been received, aborting" in that case too, so in its log that
    line does not prove a missing PONG (production: both MetaApi sockets logged it in the same second, three
    times). A real abort logs the library's own line. Returns True when the loop of ``eio`` is (now) this one."""
    try:
        import engineio
        from engineio import packet
    except Exception:  # noqa: BLE001 - no engineio: nothing to change
        return False
    if not str(getattr(engineio, "__version__", "")).startswith("3.") or not isinstance(eio, engineio.AsyncClient):
        return False
    if getattr(getattr(eio, "_ping_loop", None), "copytrader_patient", False):
        return True

    async def ping_loop() -> None:
        me = asyncio.current_task()
        eio.pong_received = True
        if eio.ping_loop_event is None:
            eio.ping_loop_event = eio.create_event()
        else:
            eio.ping_loop_event.clear()
        missed = 0
        while eio.state == "connected" and eio.ping_loop_task is me:
            if eio.pong_received:
                missed = 0
            else:
                missed += 1
                if missed > PING_MISSES_ALLOWED:
                    eio.logger.info("PONG response has not been received, aborting")
                    if eio.ws:
                        await eio.ws.close()
                    await eio.queue.put(None)
                    break
                logger.warning("MetaApi websocket: no answer to a keep-alive PING within %.0f s; the socket is kept "
                               "for one more PING", eio.ping_interval)
            eio.pong_received = False
            await eio._send_packet(packet.Packet(packet.PING))
            try:
                await asyncio.wait_for(eio.ping_loop_event.wait(), eio.ping_interval)
                # Set by a read loop that is ending (the socket was closed or broke; it logged why) or by a tear
                # down: nothing to ping any more. The library loops once more here and, since a PING is out,
                # logs "PONG response has not been received" for a socket that was in fact closed by the peer.
                break
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                if _cancel_requested():
                    break                   # the socket is being torn down (the library loops on here)
        eio.logger.info("Exiting ping task")

    ping_loop.copytrader_patient = True     # type: ignore[attr-defined]
    eio._ping_loop = ping_loop
    return True


def _pause_socket_retries() -> None:
    """SDK 29.x retries a failed socket connection in two loops that have no pause at all: the latency probe
    (latency_service.py:272-276; every 15 minutes and when a region connects for the first time) and the first
    connection of each trading socket (metaapi_websocket_client.py:650-659; while the adapter connects). While
    MetaApi cannot be reached each of them keeps one CPU core of the bot busy, and the second one also prints two
    console lines per attempt (measured: 1 800 attempts a second). The sockets of both now wait
    SOCKET_RETRY_PAUSE_SEC after a failed attempt before the SDK tries again; a connection that succeeds, and
    everything else the SDK does, is unchanged."""
    try:
        from metaapi_cloud_sdk.clients.metaapi import latency_service, metaapi_websocket_client
    except Exception as exc:  # noqa: BLE001 - another SDK layout: leave the SDK exactly as it is
        logger.debug("MetaApi socket retries left unchanged: %s", exc)
        return
    for module in (latency_service, metaapi_websocket_client):
        try:
            if not getattr(module.socketio, "copytrader_paused", False):
                module.socketio = _pausing_socketio(module.socketio)
        except Exception as exc:  # noqa: BLE001 - as above
            logger.debug("MetaApi socket retries of %s left unchanged: %s", module.__name__, exc)


def _broker_connected(account: Any) -> bool:
    """What MetatraderAccount.wait_connected() waits for, read from the account as it was last loaded: its API
    server (or one of its replicas) is connected to the terminal and the broker. False when it cannot tell
    (another SDK layout, a test double): wait_connected() is then called as before."""
    try:
        statuses = [account.connection_status] + [replica.connection_status for replica in account.replicas]
    except Exception:  # noqa: BLE001 - not the SDK's account object
        return False
    return "CONNECTED" in statuses


async def _wait_synchronized(conn: Any) -> None:
    """conn.wait_synchronized() without its one-second steps. SDK 29.x (rpc_metaapi_connection.py
    wait_synchronized) looks at "synchronized" once a second, so a subscription that MetaApi answered after 0.05 s
    still waited a whole second (measured in the real-SDK rig: all of the 1.0 s a rebuild took). The same flag
    is looked at every SYNC_POLL_SEC first; the SDK call then finds it set and only makes its waitSynchronized
    request. Together they wait as long as the SDK call alone would (its default, SDK_SYNC_TIMEOUT_SEC; the
    callers' own limits are shorter). Without that private flag (another SDK layout, a test double) the SDK call
    does everything."""
    is_synchronized = getattr(getattr(conn, "_meta_api_connection", None), "is_synchronized", None)
    if not callable(is_synchronized):
        await conn.wait_synchronized()
        return
    loop = asyncio.get_running_loop()
    deadline = loop.time() + SDK_SYNC_TIMEOUT_SEC
    try:
        while not is_synchronized() and loop.time() < deadline:
            await asyncio.sleep(SYNC_POLL_SEC)
    except Exception:  # noqa: BLE001 - private SDK attribute: the SDK call below decides
        pass
    # The SDK handles MetaApi's 'authenticated' packet by first starting LatencyService.on_connected as a task of
    # its own (it marks the instance connected, then measures the region's latency with one more socket) and then
    # setting the flag above. Seen in the same turn of the event loop, the request below would find no connected
    # instance and wait for that whole latency measurement (real-SDK rig: +0.6 s); one turn lets the task mark it.
    await asyncio.sleep(0)
    await conn.wait_synchronized(timeout_in_seconds=max(deadline - loop.time(), SYNC_POLL_SEC))


def map_error(what: str, exc: BaseException) -> MetaApiError:
    """SDK exception (TradeException, NotFoundException, TimeoutException...) -> BrokerError."""
    code = getattr(exc, "numericCode", None)
    scode = getattr(exc, "stringCode", None)
    msg = str(exc) or exc.__class__.__name__
    detail = _error_detail(exc)
    if detail:
        msg = f"{msg} {detail}"
    if isinstance(code, int) and code in RETCODE_TR:
        text = f"MetaApi {what} başarısız: {retcode_text(code, msg)}"
    else:
        text = f"MetaApi {what} başarısız: {msg}" + (f" [{scode}]" if scode else "")
    err = MetaApiError(text, code if isinstance(code, int) else None, scode)
    if any(_is_sdk_error(exc, name) for name in _REFUSED_ERRORS):
        err.refused = True
        err.fields = _refused_fields(exc)
        err.reason = _refusal_reason(exc, err.fields)
    return err


def _f(x: Any) -> float:
    try:
        return float(x or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _ts(x: Any) -> float:
    if isinstance(x, datetime):
        return (x if x.tzinfo else x.replace(tzinfo=UTC)).timestamp()
    return _EPOCH.timestamp()


def _lots(x: float) -> str:
    return f"{x:.10g}"


def _partly_done(res: Any) -> bool:
    """A successful trade answer that says only a part of the volume was executed."""
    res = res or {}
    return (res.get("numericCode") == MT5.TRADE_RETCODE_DONE_PARTIAL
            or res.get("stringCode") == "TRADE_RETCODE_DONE_PARTIAL")


class ReconnectPolicy:
    """When a connection that stopped answering is rebuilt, and what the owner hears about it. Pure: no clock of
    its own, no I/O. The adapter reports every health check (:meth:`check`) and every reconnect attempt
    (:meth:`attempt_started`, :meth:`attempt_finished`); the answers are lists of events:

    "down"              the ``after``-th failed check in a row (once per outage)
    "reconnect"         start an attempt now: with "down", and later whenever a failed check finds that
                        ``backoff[n - 1]`` seconds have passed since attempt ``n`` ENDED (the last value repeats)
                        and no attempt is running. The checks come on the heartbeat's own grid, so one that is
                        up to ``slack`` seconds early counts: an attempt that ran into its time limit ends a
                        moment after a check, and its pause would otherwise last one heartbeat longer.
    "reconnect_failed"  an attempt did not help: it failed, or it connected and the next check failed all the
                        same (once per outage, not once per attempt)
    "recovered"         the first healthy check after "down" (once per outage); it resets everything

    An attempt that could not be started (an order request was in flight) is simply not reported with
    attempt_started(): the next failed check asks for it again."""

    def __init__(self, after: int = RECONNECT_AFTER_PROBLEMS, backoff: Sequence[float] = RECONNECT_BACKOFF_SEC,
                 slack: float = RECONNECT_SLACK_SEC) -> None:
        if after < 1 or not backoff:
            raise ValueError("after must be >= 1 and backoff must not be empty")
        self.after = int(after)
        self.backoff = tuple(float(x) for x in backoff)
        self.slack = float(slack)
        self.running = False            # an attempt is in progress
        self.reset()

    def reset(self) -> None:
        self.problems = 0               # failed health checks in a row
        self.attempts = 0               # attempts started in this outage
        self.down = False               # "down" was reported
        self.failed_told = False        # "reconnect_failed" was reported
        self.retry_at: float | None = None      # earliest time of the next attempt
        self._verdict_due = False       # an attempt connected: the next check says whether it helped

    def check(self, healthy: bool, now: float) -> list[str]:
        if healthy:
            events = [_RECOVERED] if self.down else []
            self.reset()
            return events
        events = []
        self.problems += 1
        if self.problems >= self.after and not self.down:
            self.down = True
            events.append(_DOWN)
        if not self.down:
            return events
        return events + self._next_steps(now)

    def link_down(self, now: float) -> list[str]:
        """The SDK itself reports the link down and did not bring it back (the adapter's link watch, see
        LINK_CONFIRM_SEC): an outage at once, without waiting for ``after`` failed checks. Same back-off and the
        same once-per-outage events as :meth:`check`; the outage ends with the next healthy check."""
        events = [] if self.down else [_DOWN]
        self.down = True
        return events + self._next_steps(now)

    def _next_steps(self, now: float) -> list[str]:
        if self.running:
            return []
        events = []
        if self._verdict_due:
            self._verdict_due = False
            if not self.failed_told:
                self.failed_told = True
                events.append(_RECONNECT_FAILED)
        if self.retry_at is None or now + self.slack >= self.retry_at:
            events.append(_RECONNECT)
        return events

    def attempt_started(self) -> int:
        self.attempts += 1
        self.running = True
        return self.attempts

    def attempt_finished(self, ok: bool, now: float) -> list[str]:
        self.running = False
        if self.attempts == 0:          # the outage was over (reset) before the attempt ended
            return []
        self.retry_at = now + self.pause_after(self.attempts)
        if ok:
            self._verdict_due = True
            return []
        if self.failed_told:
            return []
        self.failed_told = True
        return [_RECONNECT_FAILED]

    def pause_after(self, attempt: int) -> float:
        """Seconds between the end of attempt ``attempt`` (1, 2, ...) and the earliest next one."""
        return self.backoff[min(max(attempt, 1), len(self.backoff)) - 1]


def _num_sec(seconds: float) -> str:
    """Seconds as the owner reads them: 30 -> "30", 19.99 -> "20", 3 -> "3", 0.05 -> "0.05" (never "0")."""
    return f"{seconds:.2g}" if seconds < 0.95 else str(round(seconds))


def _span_tr(seconds: float) -> str:
    return f"{seconds / 60.0:g} dakika" if seconds >= 60 else f"{seconds:g} saniye"


def _took_tr(seconds: float) -> str:
    """How long an outage lasted, as the owner reads it: "25 sn", "4 dakika"."""
    return f"{max(1, round(seconds))} sn" if seconds < 90 else f"{max(2, round(seconds / 60))} dakika"


class MetaApiBroker(MetaTraderLegBroker):
    """One MetaApi-hosted MT4/MT5 account. ``connection`` / ``api_factory`` are test seams.

    Self-healing: the heartbeat (engine/health.py) calls :meth:`health_problem` every minute. After
    RECONNECT_AFTER_PROBLEMS failed checks in a row the adapter drops its whole SDK client and connects from
    scratch (:meth:`reconnect`) in a task of its own, again after 1, 2, 5, 10, 10 ... minutes while that does not
    help (:class:`ReconnectPolicy`). After UNHEALTHY_AFTER_CHECKS failed checks in a row (or one, when the SDK
    itself reports the link down), the calls a NEW signal makes are refused at once instead of waiting for their
    timeouts. Nothing of this is active for a ``connection`` handed in from outside (tests).

    Degraded link (any connection): while the latest request of this adapter got no answer in time, or the SDK
    reports the link down, pure reads give up after DEGRADED_READ_TIMEOUT_SEC instead of READ_TIMEOUT_SEC and a
    management call (stop / TP move, close, cancel) first makes sure the account answers; if it does not, the call
    ends at once with a clear text and sends nothing. The first answered request ends that state. Trade requests
    keep RPC_TIMEOUT_SEC and are never cut short."""

    kind = "metaapi"
    tag_limit = 16      # internal tag length; its clientId is 3 longer (encode_client_id): room for a short comment

    def __init__(
        self,
        config: MetaApiBrokerConfig,
        *,
        connection: Any | None = None,
        api_factory: Callable[..., Any] | None = None,
    ) -> None:
        super().__init__(config)
        self.cfg = config
        self._conn: Any | None = connection
        self._owns_conn = connection is None     # created by connect(): dropped on close so a retry starts clean
        self._api_factory = api_factory
        self._api: Any | None = None
        self._domain_cache: dict[str, Any] | None = None     # server names a dropped SDK client knew (_reuse_domain)
        self._half_open: Any | None = None       # SDK connection of a connect() that did not finish
        self._lost: dict[tuple[str, str], tuple[OrderReplyLostError, str]] = {}   # (symbol, leg tag) -> unanswered
        self._refused: dict[tuple[str, str], MetaApiError] = {}      # (symbol, leg tag) -> refused, not executed
        self._snapshot_failures = 0
        self._connected = False
        self._platform = "mt5"
        self._symbols: list[str] | None = None
        self._specs: dict[str, dict[str, Any]] = {}
        self._notifier: Any | None = None        # set_notifier(): for the one notice the heartbeat cannot send
        self._policy = ReconnectPolicy()
        self._wanted = False                     # connect() succeeded and close() was not called since
        self._reconnecting = False               # reconnect() is tearing down / connecting right now
        self._heal_task: asyncio.Task[None] | None = None
        self._notices: set[asyncio.Task[None]] = set()
        self._orders_in_flight = 0               # trade requests waiting for their answer
        self._flows = 0                          # open / close / modify / cancel flows in progress
        self._unhealthy: str | None = None       # what the latest health check found (None: the link works)
        self._unhealthy_at = 0.0                 # ... and when (loop time)
        self._stale: str | None = None           # the latest request got no answer in time (its error text)
        self._stale_at = 0.0                     # ... and when (loop time)
        self._answered_at = float("-inf")        # loop time of the latest answered request
        self._watch_task: asyncio.Task[None] | None = None     # the link watch (see _look_at_link)
        self._link_wake: asyncio.Event | None = None             # ... looks at once when this is set
        self._link_down_since: float | None = None   # loop time the SDK started to report the link down
        self._link_reason = ""                       # ... and what it reported
        self._outage_since: float | None = None      # loop time the present outage began
        self._outage_fast = False                    # ... the link watch confirmed it (not three failed checks)
        self._back_at: float | None = None           # ... and a rebuild of it connected at this loop time
        self._put_off_told = False                   # "reconnect put off" was logged for the present outage
        self._quiet_drops = 0                        # short drops not told since the latest notice (flapping link)
        self._short_told_at = float("-inf")          # loop time of the latest "short drop" notice
        self._order_in_outage = False                # an order request was waiting while the link was down

    # ---- plumbing ------------------------------------------------------------
    def _latency_service(self) -> Any | None:
        """The SDK object that gates every RPC on "is a terminal instance connected" (private; best effort)."""
        for holder, attr in ((self._api, "_metaapi_websocket_client"), (self._conn, "_websocket_client"),
                             (self._half_open, "_websocket_client")):
            try:
                service = getattr(getattr(holder, attr), "_latency_service")
            except Exception:  # noqa: BLE001 - private SDK attributes, a test fake, or nothing there yet
                continue
            if service is not None:
                return service
        return None

    def _drop_cancelled_wait_futures(self) -> None:
        """SDK 29.x: while no terminal instance is connected every RPC awaits ONE shared future
        (LatencyService._wait_connect_promises[account], latency_service.py:187-202). A caller that gives up (our
        timeout, the engine's) cancels that future and the SDK never replaces it: every later call made while
        disconnected then raises CancelledError at once, also after a reconnect followed by a new outage."""
        try:
            waiting = self._latency_service()._wait_connect_promises    # type: ignore[union-attr]
            for key, fut in list(waiting.items()):
                if fut.cancelled():
                    del waiting[key]
        except Exception:  # noqa: BLE001 - private SDK attributes; best effort
            pass

    def _synchronized(self) -> bool | None:
        """What the SDK says about the RPC connection: True = synchronized with the terminal, False = not (the
        terminal lost the broker, or the link to MetaApi is being re-established), None = cannot tell."""
        try:
            return bool(self._conn._meta_api_connection.is_synchronized())     # type: ignore[union-attr]
        except Exception:  # noqa: BLE001 - private SDK attribute or a test fake
            return None

    def _sockets_down(self) -> bool | None:
        """What the SDK's own socket list says (private; best effort, no request): True = every socket of this
        client that the SDK keeps open has ended (the engine.io keep-alive abort, a closed or broken socket) and is
        not back yet; False = at least one is connected; None = cannot tell (no real SDK client). A socket whose
        engine.io connection ended counts as ended at once, although socket.io still calls it connected while the
        SDK's own reconnect handler runs (see _pausing_socketio)."""
        try:
            by_region = self._api._metaapi_websocket_client._socket_instances     # type: ignore[union-attr]
            states = [bool(inst["socket"].connected) and not getattr(inst["socket"], "copytrader_dropped", False)
                      for by_number in by_region.values()
                      for insts in by_number.values() for inst in insts if inst.get("connected")]
        except Exception:  # noqa: BLE001 - another SDK layout, a test fake, or no client
            return None
        return (not any(states)) if states else None

    def _link_down_reason(self) -> str | None:
        """Why the SDK reports this adapter's link down right now (no request is made), or None."""
        if not self._connected or self._conn is None:
            return "MetaApi bağlı değil."
        if self._sockets_down():
            return _SOCKETS_DOWN_TR
        if self._synchronized() is False:
            return _DISCONNECTED_TR
        return None

    async def _call(self, what: str, timeout: float, fn: Callable[..., Awaitable[Any]], *args: Any,
                    **kwargs: Any) -> Any:
        """Every SDK request of this adapter. Raises BrokerError only, or a CancelledError that was really meant
        for the running task."""
        self._drop_cancelled_wait_futures()
        try:
            result = await asyncio.wait_for(fn(*args, **kwargs), timeout)
        except BrokerError:
            raise
        except asyncio.TimeoutError as exc:      # backstop: a hung cloud call must not block the shared loops
            self._drop_cancelled_wait_futures()
            hint = " (MetaApi bağlantısı kopuk görünüyor)" if self._synchronized() is False else ""
            err = MetaApiError(f"MetaApi {what}: {_num_sec(timeout)} sn içinde yanıt gelmedi{hint}.")
            logger.warning("%s: %s", self.name, err)
            self._no_answer(err)
            raise err from exc
        except asyncio.CancelledError as exc:
            self._drop_cancelled_wait_futures()
            if _cancel_requested():
                raise                             # a real cancellation (shutdown, the caller's own timeout)
            # Nobody cancelled this task: the SDK's shared future was (see above). It must never end the
            # caller's task (the monitor loop of ALL brokers, an open / close flow half-way).
            err = MetaApiError(f"MetaApi {what}: {_CUT_TR}.")
            logger.warning("%s: %s", self.name, err)
            self._no_answer(err)
            raise err from exc
        except Exception as exc:
            err = map_error(what, exc)
            logger.warning("%s: %s", self.name, err)
            if type(exc).__name__ in _NO_ANSWER_ERRORS:
                self._no_answer(err)
            raise err from exc
        self._stale = None                        # an answer: the link works
        self._answered_at = asyncio.get_running_loop().time()
        return result

    def _no_answer(self, err: MetaApiError) -> None:
        err.no_answer = True
        self._stale, self._stale_at = str(err), asyncio.get_running_loop().time()

    def _degraded(self) -> bool:
        """The latest request of this adapter got no answer in time, or the SDK reports the link down."""
        return self._stale is not None or self._synchronized() is False or self._sockets_down() is True

    async def _rpc(self, what: str, fn: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
        """A trade request or a step of the connect chain. Counted while it waits for its answer: the connection
        is never rebuilt under an order request (see reconnect())."""
        self._orders_in_flight += 1
        self._note_order_in_outage()
        try:
            return await self._call(what, RPC_TIMEOUT_SEC, fn, *args, **kwargs)
        finally:
            self._orders_in_flight -= 1
            self._note_order_in_outage()    # an order sent before the drop that ended during it
            self._wake_if_put_off()

    def _note_order_in_outage(self) -> None:
        """A trade request (not a step of a rebuild) is waiting while the link is down: it may have executed, so
        the "short drop" notice must not say that nothing was opened (see _outage_over). The outage that notice
        names ends when the rebuild connects (_back_at): a request on the rebuilt link is reported by itself."""
        if self._reconnecting:
            return
        if self._link_down_since is not None or (self._outage_since is not None and self._back_at is None):
            self._order_in_outage = True

    async def _read(self, what: str, fn: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
        """A pure read: nothing changes on the account, so giving up early is safe. While the link is degraded
        (see _degraded) it gives up after DEGRADED_READ_TIMEOUT_SEC."""
        timeout = min(READ_TIMEOUT_SEC, RPC_TIMEOUT_SEC)
        if self._degraded():
            timeout = min(timeout, DEGRADED_READ_TIMEOUT_SEC)
        return await self._call(what, timeout, fn, *args, **kwargs)

    def _require(self) -> Any:
        """The connection for a call that is about to be made; refused (BrokerUnavailableError: nothing is sent)
        while there is none."""
        if self._reconnecting:
            raise BrokerUnavailableError("MetaApi bağlantısı şu an yenileniyor (hesap yanıt vermediği için bot "
                                         "bağlantıyı sıfırdan kuruyor); bu işlem yapılamadı.")
        if not self._connected or self._conn is None:
            if self._wanted:
                raise BrokerUnavailableError("MetaApi bağlantısı yok (yeniden bağlanma denemesi sonuç vermedi; bot "
                                             "aralıklarla tekrar deniyor).")
            raise BrokerUnavailableError(f"{self.name}: MetaApi bağlı değil (connect çağrılmadı).")
        return self._conn

    def _refuse_while_unhealthy(self) -> None:
        """For the calls a NEW signal makes (symbol, balance, the order itself): health checks found the link
        dead, so they would only wait for their timeouts. Refused after UNHEALTHY_AFTER_CHECKS failed checks in a
        row, or after one when the SDK itself reports the link down (now, or in that check); never on a single
        late answer, and not when a request was answered after the failed check. Calls that manage an EXISTING
        position are not refused here (see _before_management)."""
        reason = self._unhealthy
        if reason is None or self._reconnecting or not self._connected:
            return                          # healthy, or _require() has the better text
        now = asyncio.get_running_loop().time()
        if now - self._unhealthy_at > UNHEALTHY_TTL_SEC:
            return                          # no health check ran since (no heartbeat): not known any more
        if self._answered_at > self._unhealthy_at:
            return                          # the account answered a request after that check: try
        sync = self._synchronized()
        sockets_down = self._sockets_down()
        checks = self._policy.problems
        reason = reason.rstrip(". ")
        if reason in (_DISCONNECTED_TR.rstrip(". "), _SOCKETS_DOWN_TR.rstrip(". ")) or sync is False or sockets_down:
            if sync is True and not sockets_down:
                return                      # the SDK reports that the link is back: try (the next check confirms)
            known = f"MetaApi bu hesabın bağlantısının koptuğunu bildiriyor ({reason})"
        elif checks >= UNHEALTHY_AFTER_CHECKS:
            known = f"son {checks} bağlantı kontrolü yanıt almadı ({reason})"
        else:
            return                          # one late answer: try (the calls' own reads are bounded)
        if self._policy.attempts:
            then = "Bot bağlantıyı aralıklarla sıfırdan kurmayı deniyor."
        elif checks >= RECONNECT_AFTER_PROBLEMS:
            then = "Bot bağlantıyı sıfırdan kuracak."
        else:
            then = (f"Bağlantı kontrolü üst üste {RECONNECT_AFTER_PROBLEMS} kez yanıtsız kalırsa bot bağlantıyı "
                    "sıfırdan kurar.")
        raise BrokerUnavailableError(f"MetaApi bağlantısı sorunlu: {known}; bu hesapta yeni işlem açılmadı. {then} "
                                     "Bağlantı kontrolü yeniden yanıt alınca yeni sinyaller bu hesapta yine açılır.")

    async def _before_management(self, not_done: str) -> None:
        """A management call (stop / TP move, close, cancel) while the link is degraded: make sure the account
        answers (one cheap read, short limit) before any trade request, which could wait RPC_TIMEOUT_SEC; when a
        request got no answer only a moment ago, do not even ask again. Raises a clear BrokerError (nothing was
        sent) when the account does not answer."""
        if not self._degraded():
            return
        conn = self._require()
        reason = self._stale
        if reason is None or asyncio.get_running_loop().time() - self._stale_at > DEGRADED_READ_TIMEOUT_SEC:
            try:
                await self._read("bağlantı kontrolü", conn.get_account_information)
                return                      # answered: the link works for this call
            except BrokerError as exc:
                reason = str(exc)
        raise BrokerError(
            f"{self.name} hesabı son isteğe yanıt vermedi ({reason.rstrip('. ')}). {not_done} Bot bu işlem için "
            f"hesaba emir göndermedi; {self.name} hesabındaki pozisyonu MetaTrader'dan kendiniz kontrol edin."
        )

    # ---- lifecycle -------------------------------------------------------------
    async def connect(self) -> None:
        if self._connected:
            return
        if self._conn is None:
            if self._api is not None or self._half_open is not None:
                await self._teardown()            # leftovers of an attempt that failed without close()
            if not (self.cfg.token and self.cfg.account_id):
                raise BrokerError("MetaApi token veya hesap kimliği (account_id) boş. .env dosyasında "
                                  "METAAPI_TOKEN ve METAAPI_ACCOUNT_ID satırlarını doldurun.")
            factory = self._api_factory
            if factory is None:
                try:
                    from metaapi_cloud_sdk import MetaApi
                except ImportError as exc:
                    raise BrokerError(
                        "metaapi-cloud-sdk paketi kurulu değil. Sunucuda şunu çalıştırın: "
                        "pip install metaapi-cloud-sdk"
                    ) from exc
                factory = MetaApi
            opts: dict[str, Any] = {"region": self.cfg.region} if self.cfg.region else {}
            try:
                self._api = factory(self.cfg.token, opts)
                if hasattr(self._api, "_metaapi_websocket_client"):      # the real SDK object
                    _pause_socket_retries()
                    self._reuse_domain(self._api)
                account = await self._api.metatrader_account_api.get_account(self.cfg.account_id)
                if account.state not in ("DEPLOYING", "DEPLOYED"):
                    logger.info("%s: deploying MetaApi account %s", self.name, self.cfg.account_id)
                    await account.deploy()
                # wait_deployed() / wait_connected() each read the account AGAIN first (one more REST call each,
                # SDK 29.x opens a new HTTPS connection for every call) and then poll once a second. The account
                # was read a moment ago: when that answer already says what they wait for, they are not called.
                if account.state != "DEPLOYED":
                    await account.wait_deployed()
                if not _broker_connected(account):
                    await account.wait_connected()
                conn = account.get_rpc_connection()
                self._half_open = conn            # close() must reach it even if we never get further
                await conn.connect()
                await _wait_synchronized(conn)
            except BrokerError:
                raise
            except asyncio.CancelledError as exc:
                self._drop_cancelled_wait_futures()
                if _cancel_requested():
                    raise                         # the engine's connect timeout / shutdown; close() cleans up
                raise MetaApiError(_DISCONNECTED_TR) from exc
            except Exception as exc:
                raise map_error("bağlantı", exc) from exc
            self._conn = conn
            self._half_open = None
        info = await self._rpc("hesap bilgisi", self._conn.get_account_information)
        if info.get("investorMode") or info.get("tradeAllowed") is False:
            raise BrokerError("MetaApi hesabında işlem izni yok (yatırımcı şifresi mi girildi?).")
        self._platform = str(info.get("platform") or "mt5").lower()
        self._netting = info.get("marginMode") in _NETTING_MODES
        self._connected = True
        self._wanted = True
        logger.info(
            "%s: MetaApi connected platform=%s broker=%s currency=%s netting=%s",
            self.name, self._platform, info.get("broker"), info.get("currency"), self._netting,
        )
        if self._owns_conn and (self._watch_task is None or self._watch_task.done()):
            self._link_wake = asyncio.Event()
            self._watch_task = asyncio.ensure_future(self._watch_link())
            _LINK_WATCHERS.add(self)

    async def _close_connection(self, conn: Any, wait: float = CLOSE_WAIT_SEC) -> None:
        try:
            await conn.close()
            # The SDK's close() only SCHEDULES the real close (unsubscribe; that also stops the SDK's subscribe
            # loops). Wait for it before the api object is torn down: otherwise those loops keep reconnecting and
            # the scheduled task dies with a KeyError ("Task exception was never retrieved").
            inner = getattr(conn, "_meta_api_connection", None)
            if inner is None or not getattr(inner, "_opened", False):
                return
            loop = asyncio.get_running_loop()
            deadline = loop.time() + wait
            while not getattr(inner, "_closed", True) and loop.time() < deadline:
                await asyncio.sleep(0.1)
            if not getattr(inner, "_closed", True):
                # MetaApi does not answer (unreachable): the api object is torn down anyway. The SDK's close task
                # then fails on what that removes; nobody holds that task, so its error would only reach the log
                # as "Task exception was never retrieved".
                if wait > 0:                    # wait 0: every socket ended, nothing to wait for (see _teardown)
                    logger.warning("%s: MetaApi connection close did not finish in %.0f s; closing the client "
                                   "anyway", self.name, wait)
                for task in asyncio.all_tasks():
                    frame = getattr(task.get_coro(), "cr_frame", None)
                    if frame is not None and frame.f_code.co_name == "close" and frame.f_locals.get("self") is inner:
                        task.add_done_callback(lambda t: t.cancelled() or t.exception())
        except asyncio.CancelledError:
            if _cancel_requested():
                raise
            logger.warning("%s: MetaApi connection close was interrupted by the SDK", self.name)
        except Exception as exc:  # noqa: BLE001 - best effort on shutdown
            logger.warning("%s: MetaApi connection close failed: %s", self.name, exc)

    async def _teardown(self) -> None:
        """Close the SDK connection(s) and the api object of this adapter; nothing of them may stay alive."""
        half_open, self._half_open = self._half_open, None
        conn = self._conn
        if self._owns_conn:
            self._conn = None
        # every socket of the client has ended (the link watch's case): an unsubscribe cannot reach MetaApi, so
        # waiting CLOSE_WAIT_SEC for it would only make the outage longer (measured: 10 of 11 s of a rebuild)
        wait = 0.0 if self._sockets_down() else CLOSE_WAIT_SEC
        self._remember_domain(self._api)
        api, self._api = self._api, None
        try:
            if half_open is not None and half_open is not conn:
                await self._close_connection(half_open, wait)
            if conn is not None:
                await self._close_connection(conn, wait)
        finally:
            if api is not None:
                try:
                    await self._drop_client(api)
                finally:
                    try:
                        api.close()
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("%s: MetaApi close failed: %s", self.name, exc)

    def _remember_domain(self, api: Any) -> None:
        """What the SDK client being dropped knows about MetaApi's server names (DomainClient._url_cache: the
        domain and host name every socket URL is built from, and when they were read)."""
        with contextlib.suppress(Exception):    # private SDK attributes, a test double, or no client: nothing kept
            cache = api._metaapi_websocket_client._domain_client._url_cache
            if cache.get("domain") and cache.get("hostname") and cache.get("lastUpdated"):
                self._domain_cache = {key: cache[key] for key in ("domain", "hostname", "lastUpdated")}

    def _reuse_domain(self, api: Any) -> None:
        """A new SDK client asks MetaApi for those server names before its first socket can connect: one more
        REST call over a new HTTPS connection on the way back from a drop. It gets what the dropped client knew,
        with its time, so the SDK's own rule still applies (read again when older than 10 minutes)."""
        cache = self._domain_cache
        if not cache:
            return
        with contextlib.suppress(Exception):    # another SDK layout: the SDK asks, as before
            domain_client = api._metaapi_websocket_client._domain_client
            current = domain_client._url_cache
            if not current.get("domain") and current.get("requestPromise") is None:
                domain_client._url_cache = {**cache, "requestPromise": None}

    async def _drop_client(self, api: Any) -> None:
        """SDK 29.x: MetaApi.close() only SCHEDULES the close of its sockets (one after the other, each waits up
        to 10 s for the server's goodbye) and never stops the two interval jobs of every socket, the 60 s status
        timers or a latency probe that is still looking for the server. A client that is dropped while MetaApi
        does not answer therefore lives on, and its loops can open sockets and subscribe again when MetaApi comes
        back. Here every part is stopped and the object is made unable to start anything again; api.close()
        follows. Private attributes of the SDK, best effort: never raises (a real cancellation excepted)."""
        try:
            ws = api._metaapi_websocket_client
            instances = [inst for by_number in ws._socket_instances.values() for insts in by_number.values()
                         for inst in insts]
        except Exception:  # noqa: BLE001 - another SDK layout or a test fake: api.close() is all there is
            return

        async def never_again(*_args: Any, **_kwargs: Any) -> None:
            raise ConnectionError(_DROPPED_TR)

        async def no_subscribe(*_args: Any, **_kwargs: Any) -> None:
            return None

        with contextlib.suppress(Exception):
            manager = ws._subscription_manager
            for key in list(manager._subscriptions):
                manager.cancel_subscribe(key)               # the SDK's "subscribe until authenticated" loops
            for waiting in manager._awaiting_resubscribe.values():
                waiting.clear()
            manager.schedule_subscribe = no_subscribe       # ... and none may start later
            ws.connect = never_again                        # no new socket either
        with contextlib.suppress(Exception):
            promises = ws._latency_service._wait_connect_promises
            for fut in list(promises.values()):
                if not fut.done():                          # calls still waiting for "a terminal is connected"
                    fut.set_exception(ConnectionError(_DROPPED_TR))
                    fut.exception()
            promises.clear()
        for inst in instances:
            inst["connected"] = False                       # ends the SDK's reconnect loop of that socket
        try:
            await asyncio.gather(*(self._close_socket(inst.get("socket")) for inst in instances))
        except asyncio.CancelledError:
            if _cancel_requested():
                raise
        for inst in instances:
            with contextlib.suppress(Exception):
                inst["socket"].connect = never_again
                pending, inst["requestResolves"] = inst["requestResolves"], {}
                for fut in pending.values():
                    if not fut.done():                      # requests that were sent and never answered
                        fut.set_exception(ConnectionError(_DROPPED_TR))
                        fut.exception()
            with contextlib.suppress(Exception):
                result = inst.get("connectResult")
                if result is not None:                      # the SDK puts a connection error there; nobody reads it
                    result.add_done_callback(lambda f: f.cancelled() or f.exception())
            with contextlib.suppress(Exception):
                throttler = inst["synchronizationThrottler"]
                throttler.start = lambda: None              # the socket's disconnect handler restarts it otherwise
                throttler.stop()
        with contextlib.suppress(Exception):
            for timer in list(ws._status_timers.values()):
                timer.cancel()
        with contextlib.suppress(Exception):
            service = ws._latency_service
            for task in asyncio.all_tasks():
                coro = task.get_coro()
                frame = getattr(coro, "cr_frame", None)
                owner = frame.f_locals.get("self") if frame is not None else None
                name = getattr(coro, "__qualname__", "")
                if name == "LatencyService.on_connected" and owner is service:
                    task.cancel()                           # a latency probe of this client, still connecting
                elif name == "RpcMetaApiConnection.close" and getattr(owner, "_websocket_client", None) is ws:
                    task.cancel()                           # the SDK's own close of a connection of this client:
                    #                                         it waits (up to a minute) to unsubscribe through
                    #                                         sockets that no longer exist

    async def _close_socket(self, sock: Any) -> None:
        """One socket.io client of a dropped SDK client: a polite disconnect, then whatever is left of it."""
        if sock is None:
            return
        try:
            await asyncio.wait_for(sock.disconnect(), SOCKET_CLOSE_SEC)
        except asyncio.CancelledError:
            if _cancel_requested():
                raise
        except Exception as exc:  # noqa: BLE001 - also the timeout: the peer does not answer
            logger.debug("%s: a MetaApi socket did not close politely: %s", self.name, exc or type(exc).__name__)
        with contextlib.suppress(Exception):
            eio = sock.eio
            eio.state = "disconnected"                      # its three loops run "while connected"
            with contextlib.suppress(Exception):
                eio.queue.put_nowait(None)
            if getattr(eio, "ping_loop_event", None) is not None:
                eio.ping_loop_event.set()
            http = getattr(eio, "http", None)
            if http is not None and not getattr(http, "closed", True):
                await http.close()                          # the TCP connection itself, without waiting for the peer
            await asyncio.sleep(0)
            for name in ("read_loop_task", "write_loop_task", "ping_loop_task"):
                task = getattr(eio, name, None)
                if task is not None and not task.done():
                    task.cancel()

    async def close(self) -> None:
        self._wanted = False
        await self._stop_healing()
        self._connected = False
        await self._teardown()

    # ---- self-healing ----------------------------------------------------------
    def set_notifier(self, notifier: Any) -> None:
        """The engine hands its notifier to every adapter that has this method, before connect(). Used for the
        one notice the heartbeat cannot send: a reconnect was tried and did not help."""
        self._notifier = notifier

    @property
    def busy(self) -> bool:
        """An order request is waiting for its answer, or an open / close / modify / cancel flow is between two
        of its requests: the connection must not be rebuilt under it."""
        return self._orders_in_flight > 0 or self._flows > 0

    @contextlib.contextmanager
    def _flow(self) -> Iterator[None]:
        self._flows += 1
        try:
            yield
        finally:
            self._flows -= 1
            self._wake_if_put_off()

    def _wake_link_watch(self) -> None:
        if self._link_wake is not None:
            self._link_wake.set()

    def _wake_if_put_off(self) -> None:
        """An order request or a flow just ended: a rebuild that the link watch put off for it starts at once."""
        if self._link_down_since is not None and not self.busy:
            self._wake_link_watch()

    def _begin_reconnect(self) -> None:
        """The checks and the flag in one step, without an await between them: from here on _require() refuses
        every call, so no order request can start under the reconnect."""
        if self._reconnecting:
            raise BrokerError("MetaApi bağlantısı zaten yenileniyor.")
        if self.busy:
            raise BrokerError("Bir emir isteği sürerken MetaApi bağlantısı yenilenmez.")
        if not self._owns_conn:
            raise BrokerError("Bu MetaApi bağlantısı dışarıdan verildi; yeniden kurulamaz.")
        self._reconnecting = True

    async def reconnect(self) -> None:
        """Drop the present SDK client completely and connect from scratch, exactly as connect() does (deploy if
        needed, wait until synchronized, read the account). Raises BrokerError when it cannot start now (it is
        already running, an order request is in flight) or when the new connection could not be made: the
        adapter is then cleanly "not connected" (no SDK object left) and the next call can try again."""
        self._begin_reconnect()
        await self._reconnect_now()

    async def _reconnect_now(self) -> None:
        try:
            self._connected = False
            await self._teardown()
            await asyncio.wait_for(self.connect(), RECONNECT_TIMEOUT_SEC)
            self._unhealthy = None
        except BaseException as exc:
            self._connected = False
            try:
                await self._teardown()          # also the half-open client of the attempt that just failed
            except Exception:
                logger.exception("%s: cleaning up after a failed MetaApi reconnect", self.name)
            if isinstance(exc, asyncio.TimeoutError):
                raise MetaApiError(
                    f"MetaApi yeniden bağlanma {int(RECONNECT_TIMEOUT_SEC)} sn içinde tamamlanamadı.") from exc
            raise
        finally:
            self._reconnecting = False

    def _checked(self, problem: str | None, *, heal: bool = True, old_link: bool = False) -> str | None:
        """Book one health check: the fail-fast flag, the reconnect policy, and what the heartbeat is told.
        ``old_link``: the check began before a rebuild that ended while it ran (the link watch rebuilds on its
        own), so its answer is about the connection that was dropped: it counts as one more check of the outage
        (the heartbeat counts it too), but it is neither the verdict on the rebuilt link ("rebuilding did not
        help") nor a reason to refuse new signals on it."""
        dead = problem is not None and problem != _TRADE_OFF_TR
        now = asyncio.get_running_loop().time()
        if not self._reconnecting and not old_link:
            self._unhealthy, self._unhealthy_at = (problem if dead else None), now
        if dead and self._outage_since is None:
            self._outage_since = now
        problems, failed_told = self._policy.problems, self._policy.failed_told
        verdict_due = self._policy._verdict_due
        if old_link:
            self._policy._verdict_due = False
        events = self._policy.check(not dead, now)
        if old_link:
            self._policy._verdict_due = verdict_due and self._policy.down
            heal = False
        if _RECOVERED in events:
            logger.info("%s: MetaApi answers again", self.name)
            self._outage_over(now, problems, failed_told)
        elif not dead and not self._policy.down:
            self._outage_since = None       # one or two failed checks, over by themselves: a hiccup (not told)
        if _DOWN in events:
            logger.warning("%s: MetaApi failed %d health checks in a row: %s", self.name, self._policy.problems,
                           problem)
        if _RECONNECT_FAILED in events:
            self._tell_reconnect_failed(problem or "", connected=True)
        if _RECONNECT in events and heal:
            self._start_heal()
        if dead and problem is not None and self._policy.attempts:
            return (f"{problem.rstrip('. ')}. Bot bağlantıyı kendisi sıfırdan kuruyor "
                    f"({self._policy.attempts}. deneme).")
        return problem

    def _start_heal(self) -> None:
        if self._heal_task is not None and not self._heal_task.done():
            return
        try:
            self._begin_reconnect()
        except BrokerError as exc:
            if not self._put_off_told:      # the link watch asks again every LINK_WATCH_SEC: one line is enough
                self._put_off_told = True
                logger.warning("%s: MetaApi reconnect put off until it may start: %s", self.name, exc)
            return
        self._put_off_told = False
        attempt = self._policy.attempt_started()
        self._heal_task = asyncio.ensure_future(self._heal(attempt))

    async def _heal(self, attempt: int) -> None:
        loop = asyncio.get_running_loop()
        logger.warning("%s: MetaApi does not answer; dropping the connection and connecting from scratch "
                       "(attempt %d)", self.name, attempt)
        try:
            await self._reconnect_now()
        except asyncio.CancelledError:
            if _cancel_requested():
                self._policy.attempt_finished(False, loop.time())
                raise
            reason = _DISCONNECTED_TR
        except BrokerError as exc:
            reason = str(exc)
        except Exception as exc:        # whatever it was, this attempt failed
            logger.exception("%s: MetaApi reconnect failed unexpectedly", self.name)
            reason = str(exc) or type(exc).__name__
        else:
            self._policy.attempt_finished(True, loop.time())
            self._back_at, self._link_down_since = loop.time(), None    # a new link: the watch starts afresh
            logger.warning("%s: MetaApi connection rebuilt (attempt %d); the next health check confirms it",
                           self.name, attempt)
            return
        logger.warning("%s: MetaApi reconnect attempt %d failed: %s", self.name, attempt,
                       _TOKEN_RE.sub(r"\1***", reason))
        if _RECONNECT_FAILED in self._policy.attempt_finished(False, loop.time()):
            self._tell_reconnect_failed(reason, connected=False)

    def _tell_reconnect_failed(self, reason: str, *, connected: bool) -> None:
        """Once per outage. Never waits (the heartbeat bounds health_problem()), never raises."""
        reason = _TOKEN_RE.sub(r"\1***", reason).rstrip(". ") or "?"
        logger.warning("%s: rebuilding the MetaApi connection did not help: %s", self.name, reason)
        if self._notifier is None:
            return
        what = ("MetaApi bağlantısı sıfırdan kuruldu ama hesap hâlâ yanıt vermiyor" if connected
                else "MetaApi bağlantısı sıfırdan kurulamadı")
        pause = _span_tr(self._policy.pause_after(max(self._policy.attempts, 1)))
        text = (f"🔌 <b>{html.escape(self.name, quote=False)}</b>: {what}: "
                f"<code>{html.escape(reason, quote=False)}</code>. Bot yaklaşık {pause} sonra yeniden dener ve "
                f"denemeleri, aralarını {_span_tr(self._policy.backoff[-1])} kadar açarak sürdürür. Bu sırada bu "
                "hesapta yeni işlem açılmaz; diğer hesaplar etkilenmez. Hesap yanıt verince ayrıca haber verilir "
                "(bu uyarı tekrarlanmaz).")
        self._send_notice(text, Severity.WARNING)

    def _send_notice(self, text: str, severity: Severity) -> None:
        """In a task of its own: never waits, never raises."""
        notifier = self._notifier
        if notifier is None:
            return

        async def send() -> None:
            try:
                await notifier.notify(text, severity)
            except Exception:           # a notice must never break the adapter
                logger.exception("%s: could not send a notice to the owner", self.name)

        task = asyncio.ensure_future(send())
        self._notices.add(task)
        task.add_done_callback(self._notices.discard)

    def _outage_over(self, now: float, problems: int, failed_told: bool) -> None:
        """The first healthy check after an outage (``problems``: failed checks in a row right before it,
        ``failed_told``: the owner got the "rebuilding did not help" notice). One notice per outage, no second one
        for what the heartbeat tells itself (HEARTBEAT_TELLS_AFTER), at most one "short drop" notice per
        LINK_NOTICE_QUIET_SEC on a flapping link (the drops in between are counted into the next notice)."""
        since, self._outage_since = self._outage_since, None
        fast, self._outage_fast = self._outage_fast, False
        back_at, self._back_at = self._back_at, None
        order_waited, self._order_in_outage = self._order_in_outage, False
        if problems >= HEARTBEAT_TELLS_AFTER:
            return                          # the heartbeat told "down" and tells "✅" in this same round
        took = _took_tr(max(0.0, (back_at if back_at is not None else now) - (since if since is not None else now)))
        name = html.escape(self.name, quote=False)
        more = (f" Son bildirimden beri {self._quiet_drops} kısa kopma daha oldu; bot hepsini kendisi giderdi."
                if self._quiet_drops else "")
        if failed_told:                     # "Hesap yanıt verince ayrıca haber verilir" was promised
            self._quiet_drops = 0
            logger.info("%s: MetaApi outage over after %s; the owner is told", self.name, took)
            self._send_notice(f"✅ <b>{name}</b>: MetaApi bağlantısı yeniden kuruldu, hesap yanıt veriyor (kesinti "
                              f"yaklaşık {took}). Yeni sinyaller bu hesapta yine açılır.{more}", Severity.INFO)
            return
        if not fast:
            return
        if now - self._short_told_at < LINK_NOTICE_QUIET_SEC:
            self._quiet_drops += 1
            logger.info("%s: short MetaApi drop over after %s; not told (a notice went out less than %.0f min ago)",
                        self.name, took, LINK_NOTICE_QUIET_SEC / 60.0)
            return
        why = _WHY_TR.get(self._link_reason, self._link_reason.rstrip(". ")) or "?"
        self._quiet_drops, self._short_told_at = 0, now
        logger.info("%s: short MetaApi drop over after %s; the owner is told", self.name, took)
        if order_waited:                    # its answer may never have come: it may have executed all the same
            during = ("Kesinti sırasında bu hesaba gönderilmiş bir emir yanıt bekliyordu; o emir açılmış olabilir, "
                      "bu hesaptaki işlemleri kontrol edin. Diğer hesaplar etkilenmedi.")
        else:
            during = "Kesinti sırasında gelen yeni sinyaller bu hesapta açılmadı; diğer hesaplar etkilenmedi."
        self._send_notice(f"✅ <b>{name}</b>: MetaApi bağlantısı koptu ({html.escape(why, quote=False)}); bot "
                          f"bağlantıyı yeniden kurdu, kesinti yaklaşık {took} sürdü. {during}{more}", Severity.INFO)

    async def _watch_link(self) -> None:
        """Runs from the first connect() of a connection this adapter made itself until close(). Looks every
        LINK_WATCH_SEC, and at once when an SDK socket ended (_socket_dropped) or a request that put a rebuild off
        ended (_wake_if_put_off)."""
        loop = asyncio.get_running_loop()
        wake = self._link_wake
        while True:
            if wake is None:
                await asyncio.sleep(LINK_WATCH_SEC)
            else:
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(wake.wait(), LINK_WATCH_SEC)
                wake.clear()
            if not self._wanted:
                return
            try:
                self._look_at_link(loop.time())
            except Exception:  # noqa: BLE001 - the watch must outlive anything it finds
                logger.exception("%s: MetaApi link watch failed; it goes on", self.name)

    def _look_at_link(self, now: float) -> None:
        """One look of the link watch (no request is made). The SDK reports the link down (all its sockets ended,
        e.g. by the keep-alive abort; or no connection after a failed rebuild) and does not bring it back within
        LINK_CONFIRM_SEC: an outage, rebuilt at once (ReconnectPolicy.link_down: the same back-off, never two at
        once, never under an order request in flight). A rebuilt link that stayed up LINK_SETTLE_SEC ends the
        outage when no health check did, but only once the account answered a request after the rebuild and no
        check failed since. Slow answers on a link the SDK reports as up, and "not synchronized" on live sockets
        (the terminal lost the broker: a new client does not fix that), are left to the health checks (three
        failed ones in a row)."""
        if self._reconnecting or (self._heal_task is not None and not self._heal_task.done()):
            return                          # a rebuild owns the link right now
        reason = self._link_down_reason()
        if self._orders_in_flight and reason is not None:
            self._note_order_in_outage()
        if reason == _DISCONNECTED_TR:
            if self._link_down_since is not None:
                logger.info("%s: MetaApi sockets are up again but the account is not synchronized; left to the "
                            "health checks", self.name)
                self._link_down_since = None
            return
        if reason is None:
            if self._link_down_since is not None:
                if self._back_at is None:
                    logger.info("%s: MetaApi link back by itself after %.1f s", self.name,
                                now - self._link_down_since)
                self._link_down_since = None
            if (self._policy.down and self._back_at is not None and now - self._back_at >= LINK_SETTLE_SEC
                    and not self._policy.running and self._answered_at > self._back_at
                    and (self._unhealthy is None or self._unhealthy_at < self._back_at)):
                # The rebuilt link held AND the account answered a request since, with no failed check after
                # the rebuild: as a healthy check would say. A link that is up but whose account does not answer
                # is left to the health checks (never a false "back" notice).
                self._checked(None)
            return
        if self._link_down_since is None:
            self._link_down_since, self._link_reason = now, reason
            if LINK_CONFIRM_SEC > 0:
                logger.warning("%s: MetaApi reports the link down (%s); rebuilt in %.0f s unless it comes back by "
                               "itself", self.name, reason, LINK_CONFIRM_SEC)
            else:
                logger.warning("%s: MetaApi reports the link down (%s)", self.name, reason)
        if now - self._link_down_since < LINK_CONFIRM_SEC:
            return
        events = self._policy.link_down(now)
        if _DOWN in events:
            self._outage_fast = True
            if self._outage_since is None or self._outage_since > self._link_down_since:
                self._outage_since = self._link_down_since
            logger.warning("%s: MetaApi link down for %.1f s (%s); rebuilding the connection now", self.name,
                           now - self._link_down_since, reason)
        if _RECONNECT_FAILED in events:
            self._tell_reconnect_failed(reason, connected=True)
        if _RECONNECT in events:
            self._start_heal()

    async def _stop_healing(self) -> None:
        _LINK_WATCHERS.discard(self)
        watch, self._watch_task = self._watch_task, None
        if watch is not None and not watch.done():
            watch.cancel()
            await asyncio.wait({watch})
        task, self._heal_task = self._heal_task, None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.wait({task})          # its own clean-up is over before close() tears down
        for notice in list(self._notices):
            notice.cancel()
        self._reconnecting = False
        self._unhealthy = None
        self._policy = ReconnectPolicy()
        self._link_down_since = self._outage_since = self._back_at = None
        self._outage_fast = self._put_off_told = self._order_in_outage = False

    async def health_problem(self) -> str | None:
        """None while the connection works: the SDK reports the RPC connection as synchronized with the terminal
        and one cheap read is answered, all within HEALTH_TIMEOUT_SEC. Otherwise a short text for the owner.
        Never raises a CancelledError that nobody asked for (see _call); a real cancellation goes on.

        Called by the heartbeat every minute. For a connection this adapter made itself the result also drives
        the self-healing (see the class text): it never waits for a reconnect, that runs in its own task."""
        if not (self._wanted and self._owns_conn):
            return await self._health_check()
        if self._reconnecting:
            return self._checked(_RECONNECTING_TR)
        conn = self._conn
        try:
            problem = await self._health_check()
        except asyncio.CancelledError:
            self._checked("MetaApi bağlantı kontrolü yarıda kesildi.", heal=False,      # the caller's time limit
                          old_link=self._conn is not conn and not self._reconnecting)
            raise
        if self._reconnecting:
            return self._checked(_RECONNECTING_TR)      # a rebuild began while this check ran
        if self._conn is not conn:                      # ... and ended: the answer is about the dropped link
            return self._checked(_RECONNECTING_TR, old_link=True)
        return self._checked(problem)

    async def _health_check(self) -> str | None:
        conn = self._conn
        if not self._connected or conn is None:
            return "MetaApi bağlı değil."
        try:
            loop = asyncio.get_running_loop()
            start = loop.time()
            while self._synchronized() is False or self._sockets_down():
                if loop.time() - start >= min(HEALTH_SYNC_GRACE_SEC, HEALTH_TIMEOUT_SEC):
                    return _SOCKETS_DOWN_TR if self._sockets_down() else _DISCONNECTED_TR
                await asyncio.sleep(0.25)
            left = max(0.1, HEALTH_TIMEOUT_SEC - (loop.time() - start))
            info = await self._call("bağlantı kontrolü", left, conn.get_account_information) or {}
            if info.get("investorMode") or info.get("tradeAllowed") is False:
                return _TRADE_OFF_TR
        except BrokerError as exc:
            return str(exc)
        except asyncio.CancelledError:
            if _cancel_requested():
                raise
            return _DISCONNECTED_TR
        except Exception as exc:  # noqa: BLE001 - whatever it is, the owner should hear about it
            return f"MetaApi bağlantı kontrolü başarısız: {str(exc) or type(exc).__name__}"
        return None

    # ---- account -----------------------------------------------------------------
    async def _info(self) -> dict[str, Any]:
        self._refuse_while_unhealthy()
        conn = self._require()
        return await self._read("hesap bilgisi", conn.get_account_information)

    async def get_equity(self) -> float:
        info = await self._info()
        return _f(info.get("equity") if self.risk_config.use_equity else info.get("freeMargin"))

    async def get_free_margin(self) -> float:
        return _f((await self._info()).get("freeMargin"))

    async def account_currency(self) -> str:
        return str((await self._info()).get("currency") or "")

    # ---- market data ---------------------------------------------------------------
    async def _spec(self, symbol: str) -> dict[str, Any]:
        if symbol not in self._specs:
            conn = self._require()
            spec = await self._read(f"{symbol} sembol bilgisi", conn.get_symbol_specification, symbol)
            if not spec:
                raise BrokerError(f"{symbol} sembol bilgisi alınamadı.")
            self._specs[symbol] = spec
        return self._specs[symbol]

    @staticmethod
    def _tradable(spec: dict[str, Any]) -> bool:
        return spec.get("tradeMode") not in ("SYMBOL_TRADE_MODE_DISABLED", "SYMBOL_TRADE_MODE_CLOSEONLY")

    async def resolve_symbol(
        self,
        base_asset: str,
        quote_asset: str | None = None,
        market_type: MarketType | None = None,
        raw_symbol: str | None = None,
    ) -> SymbolInfo | None:
        self._refuse_while_unhealthy()
        conn = self._require()
        mapped = mapped_symbol(self.cfg.symbol_map, base_asset, raw_symbol, quote_asset)
        if mapped:
            try:
                spec = await self._spec(mapped)
            except BrokerError as exc:
                if getattr(exc, "no_answer", False):    # not "not found": the account did not answer the read
                    raise BrokerError(f"'{mapped}' sembol bilgisi {self.name} hesabından okunamadı "
                                      f"({str(exc).rstrip('. ')}).") from exc
                raise BrokerError(f"symbol_map'teki '{mapped}' sembolü {self.name} hesabında bulunamadı ({exc}).") from exc
            return self._symbol_info_model(mapped, spec, base_asset, quote_asset)

        if self._symbols is None:
            self._symbols = list(await self._read("sembol listesi", conn.get_symbols) or [])
        by_upper = {s.upper(): s for s in self._symbols}
        candidates = symbol_candidates(base_asset, quote_asset, market_type)
        names = [by_upper[c] for c in candidates if c in by_upper]
        whole = (base_asset.upper(),)       # the bare CFD name, if it is a candidate: no longer names
        ranked = sorted(
            (r, s) for s in self._symbols
            if s not in names and (r := rank_symbol(s, candidates, whole)) is not None
        )
        names += [s for _, s in ranked]
        for name in names:
            spec = await self._spec(name)
            if self._tradable(spec):
                return self._symbol_info_model(name, spec, base_asset, quote_asset)
        logger.info("%s: no MetaApi symbol for %s/%s", self.name, base_asset, quote_asset)
        return None

    def _symbol_info_model(self, name: str, spec: dict[str, Any], base: str, quote: str | None) -> SymbolInfo:
        point = _f(spec.get("point"))
        tick = _f(spec.get("tickSize")) or point
        logger.info("%s: %s resolved to %s", self.name, base, name)
        return SymbolInfo(
            broker_symbol=name,
            base_asset=base.upper(),
            quote_asset=str(spec.get("profitCurrency") or quote or "USD").upper(),
            qty_step=_f(spec.get("volumeStep")),
            min_qty=_f(spec.get("minVolume")),
            max_qty=_f(spec.get("maxVolume")) or None,
            price_tick=tick,
            contract_size=_f(spec.get("contractSize")) or 1.0,
            extra={
                "digits": int(spec.get("digits") or 0),
                "point": point,
                "stops_level": _f(spec.get("stopsLevel")) * point,
                "trade_mode": _TRADE_MODES.get(str(spec.get("tradeMode")), MT5.SYMBOL_TRADE_MODE_FULL),
                "filling_modes": list(spec.get("fillingModes") or []),
                "expiration_modes": list(spec.get("allowedExpirationModes") or []),
                "order_types": list(spec.get("allowedOrderTypes") or []),
                "currency_margin": str(spec.get("marginCurrency") or ""),
            },
        )

    async def _price(self, symbol: str) -> dict[str, Any]:
        conn = self._require()
        price = await self._read(f"{symbol} fiyatı", conn.get_symbol_price, symbol)
        if not price or _f(price.get("bid")) <= 0 or _f(price.get("ask")) <= 0:
            raise BrokerError(f"{symbol} için fiyat yok (piyasa kapalı olabilir).")
        return price

    async def _quote(self, broker_symbol: str) -> Quote:
        p = await self._price(broker_symbol)
        bid, ask = _f(p["bid"]), _f(p["ask"])
        return Quote(bid=bid, ask=ask, last=(bid + ask) / 2)

    # ---- sizing primitives -------------------------------------------------------------
    async def _loss_per_lot(self, symbol: SymbolInfo, side: Side, entry: float, stop: float) -> float:
        name = symbol.broker_symbol
        price = await self._price(name)
        spec = await self._spec(name)
        tick = _f(spec.get("tickSize")) or _f(spec.get("point"))
        distance = abs(entry - stop)
        loss_tick_value = _f(price.get("lossTickValue"))
        if loss_tick_value > 0 and tick > 0:
            # lossTickValue = account-currency value of one tick for 1 lot on a losing position at the
            # CURRENT conversion rate. If the quote currency is not the account currency the real loss at the
            # stop drifts with that FX rate (small for normal stops).
            return distance / tick * loss_tick_value
        # No tick value in the price. For forex / CFD contracts MetaTrader's loss is distance x contract size in
        # the profit currency; futures-type contracts are computed from the tick value only: never guessed.
        mode = str(spec.get("priceCalculationMode") or "").upper()
        if any(word in mode for word in _TICK_ONLY_CALC_MODES):
            raise BrokerError(
                f"{name}: tick değeri alınamadı; bu sözleşme türünde ({mode}) zarar yalnızca tick değeriyle "
                f"hesaplanabildiği için lot güvenle hesaplanamıyor."
            )
        profit_ccy = str(spec.get("profitCurrency") or "").upper()
        account_ccy = (await self.account_currency()).upper()
        contract = _f(spec.get("contractSize"))
        if profit_ccy and profit_ccy == account_ccy:
            return distance * (contract or 1.0)
        if profit_ccy and account_ccy and contract > 0:
            rate = await self._loss_rate(name, price, profit_ccy, account_ccy)
            if rate is not None:
                return distance * contract * rate
            raise BrokerError(
                f"{name}: tick değeri alınamadı ve kâr para birimi ({profit_ccy}) hesap para biriminden "
                f"({account_ccy}) farklı; çevirmek için gereken {account_ccy}{profit_ccy} ya da "
                f"{profit_ccy}{account_ccy} fiyatı da okunamadı. Lot güvenle hesaplanamıyor."
            )
        raise BrokerError(
            f"{name}: tick değeri alınamadı ve kâr para birimi ({profit_ccy or '?'}) hesap para biriminden farklı; "
            f"lot güvenle hesaplanamıyor."
        )

    async def _loss_rate(self, name: str, price: dict[str, Any], profit_ccy: str, account_ccy: str) -> float | None:
        """Account-currency value of ONE unit of the profit currency when a LOSS is converted, read from the
        current price of the pair between the two (USDJPY for a JPY loss on a USD account: 1 / bid; GBPUSD for a
        GBP loss: ask; always the dearer side of the quote, so the loss is never understated by the spread).
        None: the account lists no such pair, or it has no price right now."""
        for pair, inverse in ((account_ccy + profit_ccy, True), (profit_ccy + account_ccy, False)):
            try:
                quotes: list[tuple[str, dict[str, Any] | None]]
                if rank_symbol(name, (pair,)) is not None:
                    quotes = [(name, price)]          # the instrument itself is the pair (USDJPY, USD account)
                else:
                    if self._symbols is None:
                        self._symbols = list(await self._read("sembol listesi", self._require().get_symbols) or [])
                    ranked = sorted((r, s) for s in self._symbols if (r := rank_symbol(s, (pair,))) is not None)
                    quotes = [(s, None) for _, s in ranked[:_RATE_SYMBOL_TRIES]]
                for symbol, quote in quotes:
                    try:
                        quote = quote or await self._price(symbol)
                    except BrokerError as exc:
                        logger.warning("%s: no %s rate from %s: %s", self.name, pair, symbol, exc)
                        continue
                    rate = 1.0 / _f(quote["bid"]) if inverse else _f(quote["ask"])
                    logger.info("%s: %s has no tick value; its %s loss is converted to %s with %s (%.10g %s per %s)",
                                self.name, name, profit_ccy, account_ccy, symbol, rate, account_ccy, profit_ccy)
                    return rate
            except BrokerError as exc:
                logger.warning("%s: cannot look for the %s rate: %s", self.name, pair, exc)
        return None

    async def _margin_for(self, symbol: SymbolInfo, side: Side, lots: float, entry: float) -> float | None:
        conn = self._require()
        order = {
            "symbol": symbol.broker_symbol,
            "type": "ORDER_TYPE_BUY" if side is Side.LONG else "ORDER_TYPE_SELL",
            "volume": float(lots),
            "openPrice": float(entry),
        }
        try:
            res = await self._read("teminat hesabı", conn.calculate_margin, order)
        except BrokerError as exc:
            logger.warning("%s: margin calculation failed: %s", self.name, exc)
            return None
        margin = (res or {}).get("margin")
        return None if margin is None else float(margin)

    # ---- flows a reconnect must wait for ------------------------------------------------
    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        self._refuse_while_unhealthy()
        with self._flow():
            return await super().open_position(req)

    async def update_stop_loss(self, pos: BrokerPosition, new_stop_loss: float) -> BrokerPosition:
        with self._flow():
            await self._before_management("Stop değiştirilmedi.")
            return await super().update_stop_loss(pos, new_stop_loss)

    async def update_take_profits(self, pos: BrokerPosition, take_profits: list[TakeProfit]) -> BrokerPosition:
        with self._flow():
            await self._before_management("TP'ler değiştirilmedi.")
            return await super().update_take_profits(pos, take_profits)

    async def close_position(self, pos: BrokerPosition, fraction: float = 1.0) -> BrokerPosition:
        with self._flow():
            await self._before_management("Pozisyon kapatılmadı." if fraction >= 1 - 1e-9
                                          else "Kısmi kapatma yapılmadı.")
            return await super().close_position(pos, fraction)

    async def cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        with self._flow():
            await self._before_management("Bekleyen giriş iptal edilmedi.")
            return await super().cancel_entry(pos)

    async def cancel_entry_remainder(self, pos: BrokerPosition) -> BrokerPosition:
        with self._flow():
            await self._before_management("Girişin dolmamış kalan kısmı iptal edilmedi.")
            return await super().cancel_entry_remainder(pos)

    @property
    def link_degraded(self) -> bool:
        """For the engine: the latest request of this account got no answer in time, or the SDK reports the link
        down (reads give up after DEGRADED_READ_TIMEOUT_SEC; management calls first check that it answers)."""
        return self._degraded()

    # ---- order primitives -------------------------------------------------------------
    def _options(self, tag: str) -> dict[str, Any]:
        opts: dict[str, Any] = {
            "magic": int(self.cfg.magic),
            "slippage": int(self.cfg.slippage_points),
        }
        cid = encode_client_id(tag)
        if cid is None:
            # Not writable in MetaApi's clientId format (never for the engine's "ct" + hex tags): the comment
            # carries the tag (see _own_tag).
            if len(tag) <= _MAX_TAG_PLUS_COMMENT:
                opts["comment"] = tag
            return opts
        opts["clientId"] = cid
        room = _MAX_TAG_PLUS_COMMENT - len(cid)
        if room >= 2:
            opts["comment"] = "copytrader"[:room]
        return opts

    @staticmethod
    def _without_labels(opts: dict[str, Any], tag: str, fields: Sequence[str]) -> None:
        """``opts`` (in place) for the one repeat of an order whose label MetaApi refused: without the refused
        label field(s). Without a clientId the comment carries the internal tag, unless the comment was refused."""
        if "clientId" in fields:
            opts.pop("clientId", None)
            if "comment" not in fields and len(tag) <= _MAX_TAG_PLUS_COMMENT:
                opts["comment"] = tag
        if "comment" in fields:
            opts.pop("comment", None)

    async def _find_tagged_position(self, symbol: str, tag: str) -> str | None:
        positions, _ = await self._snapshot(symbol)
        for p in positions.values():
            if p.get("tag") == tag:
                return str(p["ticket"])
        return None

    def _lost_text(self, problem: str, tag: str, *, readable: bool = True) -> str:
        """What the owner reads when an order got no trade answer AND the look-up by tag found nothing."""
        looked = (f"Hesapta '{tag}' etiketli emir ya da pozisyon şu an görünmüyor; yine de" if readable else
                  f"Hesap şu an okunamadığı için '{tag}' etiketli emrin hesaba ulaşıp ulaşmadığı doğrulanamadı;")
        return (f"{problem} Emir GÖNDERİLMİŞ OLABİLİR; aynı emir ikinci kez gönderilmedi. {looked} "
                f"{self.name} hesabını MetaTrader'dan kontrol edin.")

    def _reply_lost(self, symbol: str, tag: str, problem: str) -> OrderReplyLostError:
        """The order MAY exist on the account: MetaTraderLegBroker looks it up by its tag and adopts it."""
        err = OrderReplyLostError(self._lost_text(problem, tag))
        self._lost[(symbol, tag)] = (err, problem)
        logger.error("%s: no trade answer for the %s order tagged %s (%s); looking it up, not sending it again",
                     self.name, symbol, tag, problem)
        return err

    async def _send_order(
        self, what: str, symbol: str, tag: str, fn: Callable[..., Awaitable[Any]], *args: Any,
        requote_retries: int = 0, **kwargs: Any,
    ) -> Any:
        """A request that CREATES an order. An answer with a trade return code is final (MetaApiError; a requote
        of a market order is sent again, the price is the market's at that moment). So is a refusal MetaApi gives
        before executing anything (map_error: ``refused``): nothing was executed, never a lost reply. When that
        refusal names a label field (clientId, comment), the order is sent ONCE more without it (safe: the first
        request was not executed; ``options`` is changed in place, so the caller sees how the order went out).
        Anything else (timeout, connection closed, an answer without a return code) means the order MAY have been
        executed: OrderReplyLostError, never sent again. So does the one return code that says the trade server
        did not answer in time (10012, "request cancelled by timeout"; Mt5Broker treats it the same way)."""
        attempt = 0
        relabelled = False
        while True:
            try:
                return await self._rpc(what, fn, *args, **kwargs)
            except MetaApiError as exc:
                if exc.refused:
                    opts = kwargs.get("options")
                    labels = [f for f in exc.fields if f in _LABEL_FIELDS and isinstance(opts, dict) and f in opts]
                    if labels and not relabelled:
                        relabelled = True
                        self._without_labels(opts, tag, labels)      # type: ignore[arg-type]
                        logger.warning("%s: MetaApi refused the %s of the %s order tagged %s (%s); nothing was "
                                       "executed, sending it once more without it", self.name, "/".join(labels),
                                       symbol, tag, exc.reason)
                        continue
                    err = MetaApiError(f"MetaApi emri reddetti ({exc.reason})", exc.code, exc.string_code)
                    err.refused, err.reason, err.fields = True, exc.reason, exc.fields
                    self._refused[(symbol, tag)] = err
                    raise err from exc
                if (exc.code is None and exc.string_code is None) or exc.code == MT5.TRADE_RETCODE_TIMEOUT:
                    raise self._reply_lost(symbol, tag, str(exc)) from exc
                if exc.code not in _REQUOTE_RETCODES or attempt >= requote_retries:
                    raise
                attempt += 1
                logger.info("%s: %s for %s answered %s, sending again (%d of %d)",
                            self.name, what, symbol, exc.code, attempt, requote_retries)
                if self.retry_delay_sec:
                    await asyncio.sleep(self.retry_delay_sec)

    async def _adopt_lost_leg(
        self, symbol: str, tag: str, volume: float, sl: float, tp: float | None
    ) -> dict[str, Any] | None:
        """Look the unanswered order up by its tag a few times (it may still be on its way to the trade server)."""
        for i in range(LOST_REPLY_CHECKS):
            if i:
                await asyncio.sleep(LOST_REPLY_DELAY_SEC)
            failures = self._snapshot_failures
            leg = await super()._adopt_lost_leg(symbol, tag, volume, sl, tp)
            if leg is not None:
                self._lost.pop((symbol, tag), None)
                if leg["state"] == "open" and float(leg["filled"]) < float(volume) - _EPS:
                    leg["unfilled"] = float(volume) - float(leg["filled"])     # reported once by _open_position
                return leg
            if self._snapshot_failures != failures:
                # The account cannot be read right now: more tries would only keep the shared flows waiting.
                if (symbol, tag) in self._lost:
                    err, problem = self._lost[(symbol, tag)]
                    err.args = (self._lost_text(problem, tag, readable=False),)
                break
        return None

    async def _open_position(self, req: OpenRequest) -> BrokerPosition:
        symbol = req.symbol.broker_symbol
        try:
            pos = await super()._open_position(req)
        except BrokerError as exc:
            text = self._unsure_not_failed(symbol, str(exc))
            for key, err in self._refused.items():
                # Nothing was opened and MetaApi refused the order before executing it: say exactly that.
                if key[0] == symbol and re.fullmatch(rf"(?:Emir|\d+/\d+\. bacak) açılamadı: {re.escape(str(err))}",
                                                     text):
                    text = f"{err}; bu hesapta işlem açılmadı."
            if text != str(exc):
                raise BrokerError(text) from exc
            raise
        finally:
            lost = [self._lost.pop(key)[0] for key in list(self._lost) if key[0] == symbol]
            for key in [k for k in self._refused if k[0] == symbol]:
                del self._refused[key]
        if pos.error:
            pos.error = self._unsure_not_failed(symbol, pos.error, lost)
        legs = pos.data.get("legs") or []
        notes = pos.data.setdefault("warnings", [])
        unlabelled = [f"#{leg['ticket']}" for leg in legs if leg.get("unlabelled")]
        if unlabelled:
            one = len(unlabelled) == 1
            what = "emir etiketsiz gönderildi" if one else f"{len(unlabelled)} emir etiketsiz gönderildi"
            how = "bu emri MetaApi'nin verdiği bu numarayla" if one else "bu emirleri MetaApi'nin verdiği bu numaralarla"
            notes.append(f"MetaApi emir etiketini (clientId) kabul etmedi; {what} ({', '.join(unlabelled)}). Bot "
                         f"{how} takip ediyor.")
        for leg in legs:
            label = f"#{leg['ticket']}" if len(legs) > 1 else "Emir"
            if leg.get("unfilled"):
                notes.append(
                    f"{label} kısmen gerçekleşti: {_lots(float(leg['filled']) + float(leg['unfilled']))} lotun "
                    f"{_lots(float(leg['filled']))} lotu açıldı, {_lots(float(leg['unfilled']))} lot açılmadı "
                    f"(gerçek risk hedefin altında)."
                )
            elif leg.get("partial_fill"):
                notes.append(
                    f"{label}: broker emrin yalnızca bir kısmını gerçekleştirdiğini bildirdi ama açılan lot henüz "
                    f"okunamadı; {self.name} hesabını kontrol edin."
                )
        return pos

    def _unsure_not_failed(self, symbol: str, text: str, lost: list[OrderReplyLostError] | None = None) -> str:
        """MetaTraderLegBroker words every leg error as "... açılamadı: <reason>". For an order whose answer was
        lost that is not known: the owner must read that its state is unknown."""
        if lost is None:
            lost = [err for key, (err, _) in self._lost.items() if key[0] == symbol]
        for err in lost:
            text = text.replace(f"açılamadı: {err}", f"durumu belirsiz: {err}")
        return text

    async def _refresh_open(self, leg: dict[str, Any], positions: dict[str, dict[str, Any]]) -> None:
        p = positions.get(str(leg["ticket"]))
        if p is not None and leg.get("partial_fill"):
            # First look at a market order the venue reported as partly executed (TRADE_RETCODE_DONE_PARTIAL):
            # what the position holds is what was filled. Without this the missing part would be booked as an
            # already closed part with 0 PnL.
            del leg["partial_fill"]
            asked, got = float(leg["filled"]), float(p["volume"])
            if got < asked - _EPS:
                leg["unfilled"] = asked - got
                leg["filled"] = leg["open_volume"] = got
                logger.warning("%s: position %s was only partly filled: %s of %s lots", self.name, leg["ticket"],
                               _lots(got), _lots(asked))
        await super()._refresh_open(leg, positions)

    async def _open_market_leg(
        self, symbol: SymbolInfo, side: Side, volume: float, sl: float, tp: float | None, tag: str
    ) -> dict[str, Any]:
        conn = self._require()
        name = symbol.broker_symbol
        fn = conn.create_market_buy_order if side is Side.LONG else conn.create_market_sell_order
        opts = self._options(tag)
        labelled = "clientId" in opts
        naked = False
        try:
            res = await self._send_order("piyasa emri", name, tag, fn, name, float(volume), stop_loss=float(sl),
                                         take_profit=float(tp) if tp else None, options=opts,
                                         requote_retries=REQUOTE_RETRIES)
        except MetaApiError as exc:
            if exc.code != MT5.TRADE_RETCODE_INVALID_STOPS:
                raise
            # Stops refused on the entry: open without, attach right after (rule 1, handled by the caller).
            logger.warning("%s: stops rejected on entry, opening without and attaching", self.name)
            res = await self._send_order("piyasa emri", name, tag, fn, name, float(volume), stop_loss=None,
                                         take_profit=None, options=opts, requote_retries=REQUOTE_RETRIES)
            naked = True
        pid = (res or {}).get("positionId") or (res or {}).get("orderId")
        if not pid:
            try:
                pid = await self._find_tagged_position(name, tag)
            except BrokerError:
                pid = None
        if not pid:
            # Accepted, but which position it is is not known yet: same look-up as for a lost answer.
            raise self._reply_lost(name, tag, f"{name} piyasa emri gönderildi ama pozisyon numarası alınamadı.")
        leg = new_leg(ticket=str(pid), state="open", volume=float(volume), filled=float(volume),
                      sl=None if naked else float(sl), tp=tp, tag=tag)
        if _partly_done(res):
            leg["partial_fill"] = True        # the real volume is read from the position (_refresh_open)
        if labelled and "clientId" not in opts:
            leg["unlabelled"] = True          # MetaApi refused its clientId: sent without (_send_order)
        return leg

    async def _open_pending_leg(
        self, symbol: SymbolInfo, side: Side, volume: float, price: float, sl: float,
        tp: float | None, tag: str, quote: Quote,
    ) -> dict[str, Any]:
        conn = self._require()
        name = symbol.broker_symbol
        if side is Side.LONG:
            limit = price < quote.ask
            fn = conn.create_limit_buy_order if limit else conn.create_stop_buy_order
        else:
            limit = price > quote.bid
            fn = conn.create_limit_sell_order if limit else conn.create_stop_sell_order
        allowed = symbol.extra.get("order_types") or []
        need = "SYMBOL_ORDER_LIMIT" if limit else "SYMBOL_ORDER_STOP"
        if allowed and need not in allowed:
            raise BrokerError(f"{name} için {'limit' if limit else 'stop'} emir türüne broker izin vermiyor.")
        opts = self._options(tag)
        labelled = "clientId" in opts
        minutes = int(self.risk_config.pending_entry_expiry_min or 0)
        modes = symbol.extra.get("expiration_modes") or []
        if minutes > 0 and ("SYMBOL_EXPIRATION_SPECIFIED" in modes or self._platform == "mt4"):
            opts["expiration"] = {
                "type": "ORDER_TIME_SPECIFIED",
                "time": datetime.now(UTC) + timedelta(minutes=minutes),
            }
        what = "limit emri" if limit else "stop emri"
        try:
            res = await self._send_order(what, name, tag, fn, name, float(volume), float(price),
                                         stop_loss=float(sl), take_profit=float(tp) if tp else None, options=opts)
        except MetaApiError as exc:
            if exc.code != MT5.TRADE_RETCODE_INVALID_EXPIRATION or "expiration" not in opts:
                raise
            opts.pop("expiration")      # engine still cancels after pending_entry_expiry_min
            res = await self._send_order(what, name, tag, fn, name, float(volume), float(price),
                                         stop_loss=float(sl), take_profit=float(tp) if tp else None, options=opts)
        oid = (res or {}).get("orderId")
        if not oid:
            # Accepted, but which order it is is not known: same look-up as for a lost answer.
            raise self._reply_lost(name, tag, f"{name} bekleyen emri gönderildi ama emir numarası alınamadı.")
        leg = new_leg(ticket=str(oid), order_ticket=str(oid), state="pending", volume=float(volume),
                      entry=float(price), sl=float(sl), tp=tp, tag=tag)
        leg["order_type"] = ("BUY_" if side is Side.LONG else "SELL_") + ("LIMIT" if limit else "STOP")
        if labelled and "clientId" not in opts:
            leg["unlabelled"] = True          # MetaApi refused its clientId: sent without (_send_order)
        return leg

    async def _modify_leg(self, symbol: str, side: Side, leg: dict[str, Any], sl: float, tp: float | None) -> None:
        conn = self._require()
        take_profit = float(tp) if tp else None
        if leg["state"] == "open":
            await self._rpc("SL/TP değişikliği", conn.modify_position, str(leg["ticket"]),
                            stop_loss=float(sl), take_profit=take_profit)
        elif leg["state"] == "pending":
            if not leg.get("entry"):
                raise BrokerError(f"Bekleyen emir #{leg['order_ticket']} fiyatı bilinmiyor.")
            await self._rpc("emir değişikliği", conn.modify_order, str(leg["order_ticket"]), float(leg["entry"]),
                            stop_loss=float(sl), take_profit=take_profit)
        else:
            raise BrokerError(f"#{leg['ticket']} artık açık değil.")

    async def _close_leg(self, symbol: str, side: Side, leg: dict[str, Any], volume: float | None) -> float:
        conn = self._require()
        if leg["state"] != "open":
            return 0.0
        ov = float(leg["open_volume"] or leg["filled"])
        opts = {"slippage": int(self.cfg.slippage_points)}
        if volume is None or volume >= ov - _EPS:
            await self._rpc("pozisyon kapatma", conn.close_position, str(leg["ticket"]), opts)
            leg.update(open_volume=0.0, state="closed", reason="manual")
            self._mark_closed_lots(leg, ov)
            return ov
        await self._rpc("kısmi kapatma", conn.close_position_partially, str(leg["ticket"]), float(volume), opts)
        self._mark_closed_lots(leg, float(volume))
        step = _f((await self._spec(symbol)).get("volumeStep"))
        leg["open_volume"] = _snap(ov - float(volume), step)
        return float(volume)

    async def _cancel_leg(self, symbol: str, leg: dict[str, Any]) -> None:
        conn = self._require()
        await self._rpc("emir iptali", conn.cancel_order, str(leg["order_ticket"]))

    # ---- state primitives ---------------------------------------------------------------
    def _own_tag(self, item: dict[str, Any]) -> str:
        """The internal tag of a position / order of this bot ("" for anything else). Only our own magic counts
        for idempotency matching. The clientId decides when there is one (decode_client_id); an order sent without
        one (MetaApi refused it, see _send_order) carries the tag in its comment."""
        if int(item.get("magic") or 0) != int(self.cfg.magic):
            return ""
        if item.get("clientId"):
            return decode_client_id(item.get("clientId"))
        comment = str(item.get("comment") or "")
        return comment if _TAG_RE.fullmatch(comment) else ""

    def _positions_of(self, raw_positions: Sequence[dict[str, Any]], symbol: str) -> dict[str, dict[str, Any]]:
        return {
            str(p["id"]): {
                "ticket": str(p["id"]), "volume": _f(p.get("volume")), "entry": _f(p.get("openPrice")) or None,
                "sl": _f(p.get("stopLoss")) or None, "tp": _f(p.get("takeProfit")) or None,
                "tag": self._own_tag(p), "magic": int(p.get("magic") or 0),
                "realized": p.get("realizedProfit"),
            }
            for p in raw_positions if p.get("symbol") == symbol
        }

    async def _position_now(self, symbol: str, ticket: Any) -> dict[str, Any] | None:
        conn = self._require()
        raw = await self._read("pozisyon listesi", conn.get_positions) or []
        return self._positions_of(raw, symbol).get(str(ticket))

    async def _snapshot(self, symbol: str) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        conn = self._require()
        try:
            raw_positions = await self._read("pozisyon listesi", conn.get_positions) or []
            raw_orders = await self._read("emir listesi", conn.get_orders) or []
        except BrokerError:
            self._snapshot_failures += 1      # _adopt_lost_leg: "not found" and "could not look" are different
            raise
        positions = self._positions_of(raw_positions, symbol)
        orders = {
            str(o["id"]): {
                "ticket": str(o["id"]), "volume": _f(o.get("currentVolume") or o.get("volume")),
                "price": _f(o.get("openPrice")) or None, "sl": _f(o.get("stopLoss")) or None,
                "tp": _f(o.get("takeProfit")) or None, "tag": self._own_tag(o), "magic": int(o.get("magic") or 0),
            }
            for o in raw_orders if o.get("symbol") == symbol and o.get("type") in _PENDING_TYPES
        }
        return positions, orders

    async def _history_order(self, order_ticket: Any) -> dict[str, Any] | None:
        conn = self._require()
        res = await self._read("emir geçmişi", conn.get_history_orders_by_ticket, str(order_ticket)) or {}
        items = res.get("historyOrders") or []
        if not items:
            return None
        h = items[-1]
        state = str(h.get("state") or "")
        if state in ("ORDER_STATE_FILLED", "ORDER_STATE_PARTIAL"):
            kind = "filled"
        elif state in ("ORDER_STATE_CANCELED", "ORDER_STATE_EXPIRED", "ORDER_STATE_REJECTED"):
            kind = "cancelled"
        else:
            kind = "other"
        reason = {"ORDER_STATE_EXPIRED": "expired", "ORDER_STATE_REJECTED": "rejected"}.get(state, "cancelled")
        pid = h.get("positionId")
        return {"state": kind, "position_id": str(pid) if pid else None, "reason": reason}

    async def _deal_summary(self, position_id: Any) -> dict[str, Any] | None:
        conn = self._require()
        res = await self._read("işlem geçmişi", conn.get_deals_by_position, str(position_id)) or {}
        deals = res.get("deals") or []
        if not deals:
            return None
        trades = [d for d in deals if d.get("type") in ("DEAL_TYPE_BUY", "DEAL_TYPE_SELL")]
        ins = [d for d in trades if d.get("entryType") == "DEAL_ENTRY_IN"]
        outs = [d for d in trades if d.get("entryType") in ("DEAL_ENTRY_OUT", "DEAL_ENTRY_OUT_BY", "DEAL_ENTRY_INOUT")]
        in_vol = sum(_f(d.get("volume")) for d in ins)
        reason = None
        if outs:
            last = max(outs, key=lambda d: (_ts(d.get("time")), str(d.get("id") or "")))
            reason = _DEAL_REASONS.get(str(last.get("reason") or ""), "manual")
        return {
            "realized": sum(_f(d.get("profit")) + _f(d.get("commission")) + _f(d.get("swap")) for d in deals),
            "in_volume": in_vol,
            "out_volume": sum(_f(d.get("volume")) for d in outs),
            "entry": (sum(_f(d.get("price")) * _f(d.get("volume")) for d in ins) / in_vol) if in_vol else None,
            "reason": reason,
        }


__all__ = ["MetaApiBroker", "MetaApiError", "ReconnectPolicy", "map_error"]
