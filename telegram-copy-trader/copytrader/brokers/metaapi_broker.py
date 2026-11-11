"""MT4/MT5 through the MetaApi cloud (metaapi-cloud-sdk 29.x). Works on Linux/Docker.

Uses an RPC connection (request/response, no local terminal state). The
leg-per-TP trading logic is shared with Mt5Broker (``MetaTraderLegBroker``);
this module only implements the venue primitives.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from ..config import MetaApiBrokerConfig
from ..models import MarketType, Quote, Side, SymbolInfo
from .base import BrokerError
from .mt5_broker import (
    MT5,
    RETCODE_TR,
    MetaTraderLegBroker,
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
RPC_TIMEOUT_SEC = 120.0          # SDK calls have their own timeouts; this is only the last resort
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class MetaApiError(BrokerError):
    """BrokerError carrying the MetaTrader trade return code (TradeException.numericCode) if any."""

    def __init__(self, message: str, code: int | None = None, string_code: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.string_code = string_code


def map_error(what: str, exc: BaseException) -> MetaApiError:
    """SDK exception (TradeException, NotFoundException, TimeoutException...) -> BrokerError."""
    code = getattr(exc, "numericCode", None)
    scode = getattr(exc, "stringCode", None)
    msg = str(exc) or exc.__class__.__name__
    if isinstance(code, int) and code in RETCODE_TR:
        text = f"MetaApi {what} başarısız: {retcode_text(code, msg)}"
    else:
        text = f"MetaApi {what} başarısız: {msg}" + (f" [{scode}]" if scode else "")
    return MetaApiError(text, code if isinstance(code, int) else None, scode)


def _f(x: Any) -> float:
    try:
        return float(x or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _ts(x: Any) -> float:
    if isinstance(x, datetime):
        return (x if x.tzinfo else x.replace(tzinfo=UTC)).timestamp()
    return _EPOCH.timestamp()


class MetaApiBroker(MetaTraderLegBroker):
    """One MetaApi-hosted MT4/MT5 account. ``connection`` / ``api_factory`` are test seams."""

    kind = "metaapi"
    tag_limit = 16      # clientId length; leaves room for a short comment

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
        self._connected = False
        self._platform = "mt5"
        self._symbols: list[str] | None = None
        self._specs: dict[str, dict[str, Any]] = {}

    # ---- plumbing ------------------------------------------------------------
    async def _rpc(self, what: str, fn: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
        try:
            return await asyncio.wait_for(fn(*args, **kwargs), RPC_TIMEOUT_SEC)
        except BrokerError:
            raise
        except asyncio.TimeoutError as exc:      # backstop: a hung cloud call must not block the shared loops
            err = MetaApiError(f"MetaApi {what}: {int(RPC_TIMEOUT_SEC)} sn içinde yanıt gelmedi.")
            logger.warning("%s: %s", self.name, err)
            raise err from exc
        except Exception as exc:
            err = map_error(what, exc)
            logger.warning("%s: %s", self.name, err)
            raise err from exc

    def _require(self) -> Any:
        if not self._connected or self._conn is None:
            raise BrokerError(f"{self.name}: MetaApi bağlı değil (connect çağrılmadı).")
        return self._conn

    # ---- lifecycle -------------------------------------------------------------
    async def connect(self) -> None:
        if self._connected:
            return
        if self._conn is None:
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
                account = await self._api.metatrader_account_api.get_account(self.cfg.account_id)
                if account.state not in ("DEPLOYING", "DEPLOYED"):
                    logger.info("%s: deploying MetaApi account %s", self.name, self.cfg.account_id)
                    await account.deploy()
                await account.wait_deployed()
                await account.wait_connected()
                conn = account.get_rpc_connection()
                await conn.connect()
                await conn.wait_synchronized()
            except BrokerError:
                raise
            except Exception as exc:
                raise map_error("bağlantı", exc) from exc
            self._conn = conn
        info = await self._rpc("hesap bilgisi", self._conn.get_account_information)
        if info.get("investorMode") or info.get("tradeAllowed") is False:
            raise BrokerError("MetaApi hesabında işlem izni yok (yatırımcı şifresi mi girildi?).")
        self._platform = str(info.get("platform") or "mt5").lower()
        self._netting = info.get("marginMode") in _NETTING_MODES
        self._connected = True
        logger.info(
            "%s: MetaApi connected platform=%s broker=%s currency=%s netting=%s",
            self.name, self._platform, info.get("broker"), info.get("currency"), self._netting,
        )

    async def close(self) -> None:
        self._connected = False
        if self._conn is not None:
            try:
                await self._conn.close()
            except Exception as exc:  # noqa: BLE001 - best effort on shutdown
                logger.warning("%s: MetaApi connection close failed: %s", self.name, exc)
            if self._owns_conn:
                self._conn = None
        if self._api is not None:
            try:
                self._api.close()
            except Exception as exc:  # noqa: BLE001
                logger.warning("%s: MetaApi close failed: %s", self.name, exc)
            self._api = None

    # ---- account -----------------------------------------------------------------
    async def _info(self) -> dict[str, Any]:
        conn = self._require()
        return await self._rpc("hesap bilgisi", conn.get_account_information)

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
            spec = await self._rpc(f"{symbol} sembol bilgisi", conn.get_symbol_specification, symbol)
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
        conn = self._require()
        mapped = mapped_symbol(self.cfg.symbol_map, base_asset, raw_symbol)
        if mapped:
            try:
                spec = await self._spec(mapped)
            except BrokerError as exc:
                raise BrokerError(f"symbol_map'teki '{mapped}' sembolü {self.name} hesabında bulunamadı ({exc}).") from exc
            return self._symbol_info_model(mapped, spec, base_asset, quote_asset)

        if self._symbols is None:
            self._symbols = list(await self._rpc("sembol listesi", conn.get_symbols) or [])
        by_upper = {s.upper(): s for s in self._symbols}
        candidates = symbol_candidates(base_asset, quote_asset)
        names = [by_upper[c] for c in candidates if c in by_upper]
        ranked = sorted(
            (r, s) for s in self._symbols if s not in names and (r := rank_symbol(s, candidates)) is not None
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
        price = await self._rpc(f"{symbol} fiyatı", conn.get_symbol_price, symbol)
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
        profit_ccy = str(spec.get("profitCurrency") or "").upper()
        if profit_ccy and profit_ccy == (await self.account_currency()).upper():
            return distance * (_f(spec.get("contractSize")) or 1.0)
        raise BrokerError(
            f"{name}: tick değeri alınamadı ve kâr para birimi ({profit_ccy or '?'}) hesap para biriminden farklı; "
            f"lot güvenle hesaplanamıyor."
        )

    async def _margin_for(self, symbol: SymbolInfo, side: Side, lots: float, entry: float) -> float | None:
        conn = self._require()
        order = {
            "symbol": symbol.broker_symbol,
            "type": "ORDER_TYPE_BUY" if side is Side.LONG else "ORDER_TYPE_SELL",
            "volume": float(lots),
            "openPrice": float(entry),
        }
        try:
            res = await self._rpc("teminat hesabı", conn.calculate_margin, order)
        except BrokerError as exc:
            logger.warning("%s: margin calculation failed: %s", self.name, exc)
            return None
        margin = (res or {}).get("margin")
        return None if margin is None else float(margin)

    # ---- order primitives -------------------------------------------------------------
    def _options(self, tag: str) -> dict[str, Any]:
        opts: dict[str, Any] = {
            "clientId": tag,
            "magic": int(self.cfg.magic),
            "slippage": int(self.cfg.slippage_points),
        }
        room = _MAX_TAG_PLUS_COMMENT - len(tag)
        if room >= 2:
            opts["comment"] = "copytrader"[:room]
        return opts

    async def _find_tagged_position(self, symbol: str, tag: str) -> str | None:
        positions, _ = await self._snapshot(symbol)
        for p in positions.values():
            if p.get("tag") == tag:
                return str(p["ticket"])
        return None

    async def _open_market_leg(
        self, symbol: SymbolInfo, side: Side, volume: float, sl: float, tp: float | None, tag: str
    ) -> dict[str, Any]:
        conn = self._require()
        name = symbol.broker_symbol
        fn = conn.create_market_buy_order if side is Side.LONG else conn.create_market_sell_order
        opts = self._options(tag)
        naked = False
        try:
            res = await self._rpc("piyasa emri", fn, name, float(volume),
                                  stop_loss=float(sl), take_profit=float(tp) if tp else None, options=opts)
        except MetaApiError as exc:
            if exc.code != MT5.TRADE_RETCODE_INVALID_STOPS:
                raise
            # Stops refused on the entry: open without, attach right after (rule 1, handled by the caller).
            logger.warning("%s: stops rejected on entry, opening without and attaching", self.name)
            res = await self._rpc("piyasa emri", fn, name, float(volume), stop_loss=None, take_profit=None, options=opts)
            naked = True
        pid = (res or {}).get("positionId") or (res or {}).get("orderId")
        if not pid:
            pid = await self._find_tagged_position(name, tag)
        if not pid:
            raise BrokerError(
                f"{name} emri gönderildi ama pozisyon numarası alınamadı; MetaTrader'da '{tag}' etiketli "
                f"pozisyonu kontrol edin."
            )
        return new_leg(ticket=str(pid), state="open", volume=float(volume), filled=float(volume),
                       sl=None if naked else float(sl), tp=tp, tag=tag)

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
        minutes = int(self.risk_config.pending_entry_expiry_min or 0)
        modes = symbol.extra.get("expiration_modes") or []
        if minutes > 0 and ("SYMBOL_EXPIRATION_SPECIFIED" in modes or self._platform == "mt4"):
            opts["expiration"] = {
                "type": "ORDER_TIME_SPECIFIED",
                "time": datetime.now(UTC) + timedelta(minutes=minutes),
            }
        what = "limit emri" if limit else "stop emri"
        try:
            res = await self._rpc(what, fn, name, float(volume), float(price),
                                  stop_loss=float(sl), take_profit=float(tp) if tp else None, options=opts)
        except MetaApiError as exc:
            if exc.code != MT5.TRADE_RETCODE_INVALID_EXPIRATION or "expiration" not in opts:
                raise
            opts.pop("expiration")      # engine still cancels after pending_entry_expiry_min
            res = await self._rpc(what, fn, name, float(volume), float(price),
                                  stop_loss=float(sl), take_profit=float(tp) if tp else None, options=opts)
        oid = (res or {}).get("orderId")
        if not oid:
            raise BrokerError(f"{name} bekleyen emri gönderildi ama emir numarası alınamadı; '{tag}' etiketini kontrol edin.")
        leg = new_leg(ticket=str(oid), order_ticket=str(oid), state="pending", volume=float(volume),
                      entry=float(price), sl=float(sl), tp=tp, tag=tag)
        leg["order_type"] = ("BUY_" if side is Side.LONG else "SELL_") + ("LIMIT" if limit else "STOP")
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
            return ov
        await self._rpc("kısmi kapatma", conn.close_position_partially, str(leg["ticket"]), float(volume), opts)
        step = _f((await self._spec(symbol)).get("volumeStep"))
        leg["open_volume"] = _snap(ov - float(volume), step)
        return float(volume)

    async def _cancel_leg(self, symbol: str, leg: dict[str, Any]) -> None:
        conn = self._require()
        await self._rpc("emir iptali", conn.cancel_order, str(leg["order_ticket"]))

    # ---- state primitives ---------------------------------------------------------------
    async def _snapshot(self, symbol: str) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        conn = self._require()
        raw_positions = await self._rpc("pozisyon listesi", conn.get_positions) or []
        raw_orders = await self._rpc("emir listesi", conn.get_orders) or []
        magic = int(self.cfg.magic)

        def own_tag(item: dict[str, Any]) -> str:
            # only our own magic counts for idempotency matching
            return str(item.get("clientId") or "") if int(item.get("magic") or 0) == magic else ""

        positions = {
            str(p["id"]): {
                "ticket": str(p["id"]), "volume": _f(p.get("volume")), "entry": _f(p.get("openPrice")) or None,
                "sl": _f(p.get("stopLoss")) or None, "tp": _f(p.get("takeProfit")) or None,
                "tag": own_tag(p), "magic": int(p.get("magic") or 0),
                "realized": p.get("realizedProfit"),
            }
            for p in raw_positions if p.get("symbol") == symbol
        }
        orders = {
            str(o["id"]): {
                "ticket": str(o["id"]), "volume": _f(o.get("currentVolume") or o.get("volume")),
                "price": _f(o.get("openPrice")) or None, "sl": _f(o.get("stopLoss")) or None,
                "tp": _f(o.get("takeProfit")) or None, "tag": own_tag(o), "magic": int(o.get("magic") or 0),
            }
            for o in raw_orders if o.get("symbol") == symbol and o.get("type") in _PENDING_TYPES
        }
        return positions, orders

    async def _history_order(self, order_ticket: Any) -> dict[str, Any] | None:
        conn = self._require()
        res = await self._rpc("emir geçmişi", conn.get_history_orders_by_ticket, str(order_ticket)) or {}
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
        res = await self._rpc("işlem geçmişi", conn.get_deals_by_position, str(position_id)) or {}
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


__all__ = ["MetaApiBroker", "MetaApiError", "map_error"]
