"""Paper trading: a simulated account priced from a public ccxt exchange.

``PaperSimulator`` holds the fill / stop / take-profit simulation and the persisted
account state. ``PaperBroker`` feeds it public prices; ``DryRunBroker`` (dry_run.py)
feeds it quotes from a real adapter.

Simulation rules (conservative):
  * market orders fill at the quote (ask for buys, bid for sells) plus ``fee_rate``,
  * limit entries fill at the limit price once a 1m candle (or the quote) crosses it,
  * SL / TP are checked on CLOSED 1m candles since the last check (high/low), then on the
    current quote; if SL and TP are both inside one candle the SL is assumed first,
  * an SL that gaps (candle opens beyond it) fills at the candle open.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import ccxt.async_support as ccxt_async
from ccxt.base import errors as cx

from ..config import PaperBrokerConfig, RiskConfig
from ..models import (
    BrokerPosition,
    EntryType,
    MarketType,
    OpenRequest,
    PositionStatus,
    Quote,
    Side,
    SizingResult,
    SymbolInfo,
    TakeProfit,
)
from ..risk.sizing import compute_linear_size, floor_to_step, round_to_tick
from .base import Broker, BrokerError
from .ccxt_broker import (
    _lookup_symbol_map,
    find_market,
    num,
    quote_from_ticker,
    split_quantities,
    symbol_info_from_market,
)

logger = logging.getLogger(__name__)

_MINUTE_MS = 60_000
QuoteFn = Callable[[SymbolInfo], Awaitable[Quote]]
CandlesFn = Callable[[str, int], Awaitable[list[list[float]]]]


def _fmt(x: float | None) -> str:
    if x is None:
        return "-"
    return format(Decimal(f"{x:.10g}").normalize(), "f")


def _dec_sub(a: float, b: float) -> float:
    return float(Decimal(str(a)) - Decimal(str(b)))


def safe_file_part(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name) or "broker"


class PaperSimulator:
    """Simulated positions + account state persisted as JSON."""

    def __init__(
        self,
        *,
        name: str,
        state_path: Path,
        starting_balance: float,
        quote_fn: QuoteFn,
        fee_rate_fn: Callable[[SymbolInfo], float],
        candles_fn: CandlesFn | None = None,
        dry_run: bool = False,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.name = name
        self.state_path = Path(state_path)
        self.starting_balance = starting_balance
        self.quote_fn = quote_fn
        self.fee_rate_fn = fee_rate_fn
        self.candles_fn = candles_fn
        self.dry_run = dry_run
        self.now_ms = clock_ms or (lambda: int(time.time() * 1000))
        self.state: dict[str, Any] = {}
        self._loaded = False

    # ------------------------------------------------------------------ state
    def load(self) -> None:
        state: dict[str, Any] = {}
        if self.state_path.is_file():
            try:
                state = json.loads(self.state_path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as e:
                raise BrokerError(f"{self.name}: simülasyon durumu okunamadı ({self.state_path}): {e}") from e
        state.setdefault("balance", self.starting_balance)
        state.setdefault("realized_pnl", 0.0)
        state.setdefault("fees", 0.0)
        state.setdefault("positions", {})
        self.state = state
        self._loaded = True

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            self.load()

    def save(self) -> None:
        self.state["updated_at"] = self.now_ms()
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.state, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.state_path)

    @property
    def balance(self) -> float:
        self._ensure_loaded()
        return float(self.state["balance"])

    async def unrealized_pnl(self) -> float:
        self._ensure_loaded()
        total = 0.0
        for entry in self.state["positions"].values():
            if entry.get("remaining", 0) <= 0 or entry.get("entry") is None:
                continue
            try:
                quote = await self.quote_fn(SymbolInfo(**entry["symbol_info"]))
            except Exception as e:  # noqa: BLE001 - one bad quote must not break equity
                logger.warning("%s: no quote for %s, unrealized PnL ignored: %s", self.name, entry.get("symbol"), e)
                continue
            side = Side(entry["side"])
            direction = 1.0 if side is Side.LONG else -1.0
            total += (quote.exit_price_for(side) - entry["entry"]) * entry["remaining"] * entry["contract_size"] * direction
        return total

    def _track(self, pos: BrokerPosition) -> None:
        pid = pos.data["paper_id"]
        if pos.status in (PositionStatus.OPEN,) and pos.remaining_quantity > 0:
            self.state["positions"][pid] = {
                "symbol": pos.broker_symbol,
                "side": pos.side.value,
                "remaining": pos.remaining_quantity,
                "entry": pos.avg_entry_price,
                "contract_size": pos.data["symbol_info"]["contract_size"],
                "symbol_info": pos.data["symbol_info"],
            }
        else:
            self.state["positions"].pop(pid, None)
        self.save()

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _info(pos: BrokerPosition) -> SymbolInfo:
        return SymbolInfo(**pos.data["symbol_info"])

    def _fee(self, pos: BrokerPosition, price: float, qty: float) -> float:
        return self.fee_rate_fn(self._info(pos)) * price * qty * self._info(pos).contract_size

    def _book_fee(self, pos: BrokerPosition, fee: float) -> None:
        pos.data["fees"] = float(pos.data.get("fees", 0.0)) + fee
        self.state["balance"] = float(self.state["balance"]) - fee
        self.state["realized_pnl"] = float(self.state["realized_pnl"]) - fee
        self.state["fees"] = float(self.state["fees"]) + fee

    def _fill_entry(self, pos: BrokerPosition, price: float, at_ms: int) -> None:
        pos.filled_quantity = pos.quantity
        pos.remaining_quantity = pos.quantity
        pos.avg_entry_price = price
        pos.status = PositionStatus.OPEN
        pos.data["opened_at"] = at_ms
        pos.data["next_candle_ms"] = (at_ms // _MINUTE_MS + 1) * _MINUTE_MS
        self._book_fee(pos, self._fee(pos, price, pos.quantity))
        info = self._info(pos)
        qtys = split_quantities(pos.quantity, [tp["fraction"] for tp in pos.data["tps"]], info.qty_step, info.min_qty)
        for tp, q in zip(pos.data["tps"], qtys):
            tp.update(qty=q, status="open" if q > 0 else "skipped")
        pos.realized_pnl = -float(pos.data["fees"])

    def _exit(self, pos: BrokerPosition, qty: float, price: float, kind: str) -> None:
        qty = min(qty, pos.remaining_quantity)
        if qty <= 0:
            return
        cs = self._info(pos).contract_size
        direction = 1.0 if pos.side is Side.LONG else -1.0
        gross = (price - float(pos.avg_entry_price or price)) * qty * cs * direction
        self._book_fee(pos, self._fee(pos, price, qty))
        self.state["balance"] = float(self.state["balance"]) + gross
        self.state["realized_pnl"] = float(self.state["realized_pnl"]) + gross
        pos.data["gross_pnl"] = float(pos.data.get("gross_pnl", 0.0)) + gross
        pos.data.setdefault("exits", []).append({"qty": qty, "price": price, "kind": kind, "at": self.now_ms()})
        remaining = _dec_sub(pos.remaining_quantity, qty)
        step = self._info(pos).qty_step
        pos.remaining_quantity = 0.0 if remaining <= step / 2 else remaining
        pos.realized_pnl = float(pos.data["gross_pnl"]) - float(pos.data["fees"])
        if pos.remaining_quantity <= 0:
            pos.status = PositionStatus.CLOSED
            pos.data["closed_reason"] = kind
            for tp in pos.data["tps"]:
                if tp.get("status") == "open":
                    tp["status"] = "canceled"
        logger.info("%s: simulated %s %s qty=%s @ %s (pnl so far %.2f)", self.name, kind, pos.broker_symbol,
                    qty, price, pos.realized_pnl)

    def _new_tps(self, pos: BrokerPosition, levels: list[TakeProfit], total: float) -> None:
        info = self._info(pos)
        kept = [tp for tp in pos.data.get("tps", []) if tp.get("status") == "filled"]
        qtys = split_quantities(total, [lv.close_fraction for lv in levels], info.qty_step, info.min_qty)
        pos.data["tps"] = kept + [
            {"price": lv.price, "fraction": lv.close_fraction, "qty": q, "status": "open" if q > 0 else "skipped"}
            for lv, q in zip(levels, qtys)
        ]

    @staticmethod
    def _sl_hit(side: Side, sl: float, low: float, high: float) -> bool:
        return low <= sl if side is Side.LONG else high >= sl

    @staticmethod
    def _tp_hit(side: Side, tp: float, low: float, high: float) -> bool:
        return high >= tp if side is Side.LONG else low <= tp

    # ------------------------------------------------------------------ trading
    async def open(self, req: OpenRequest) -> BrokerPosition:
        self._ensure_loaded()
        info, side = req.symbol, req.plan.side
        qty = floor_to_step(req.quantity, info.qty_step)
        if qty <= 0 or qty < info.min_qty:
            raise BrokerError(f"{self.name}: miktar ({_fmt(qty)}) minimum emir miktarının ({_fmt(info.min_qty)}) altında.")
        sl = round_to_tick(req.stop_loss, info.price_tick)
        levels = [TakeProfit(price=round_to_tick(tp.price, info.price_tick), close_fraction=tp.close_fraction)
                  for tp in req.take_profits]
        long = side is Side.LONG
        quote = await self.quote_fn(info)
        now = self.now_ms()
        limit_price = None
        if req.entry_type is EntryType.MARKET:
            if (long and sl >= quote.bid) or (not long and sl <= quote.ask):
                raise BrokerError(f"{self.name}: stop ({_fmt(sl)}) mevcut fiyatın ({_fmt(quote.last)}) yanlış tarafında; işlem açılmadı.")
        else:
            if not req.entry_price:
                raise BrokerError(f"{self.name}: limit emri için giriş fiyatı yok.")
            limit_price = round_to_tick(req.entry_price, info.price_tick)
            if (long and sl >= limit_price) or (not long and sl <= limit_price):
                raise BrokerError(f"{self.name}: stop ({_fmt(sl)}) limit girişin ({_fmt(limit_price)}) yanlış tarafında.")
        pos = BrokerPosition(
            broker=self.name,
            broker_symbol=info.broker_symbol,
            side=side,
            status=PositionStatus.PENDING,
            quantity=qty,
            stop_loss=sl,
            take_profits=levels,
            dry_run=self.dry_run,
            data={
                "simulated": True,
                "paper_id": uuid.uuid4().hex[:12],
                "client_tag": req.client_tag,
                "symbol_info": info.model_dump(),
                "entry_type": req.entry_type.value,
                "limit_price": limit_price,
                "leverage": req.leverage,
                "tps": [{"price": lv.price, "fraction": lv.close_fraction, "qty": 0.0, "status": "pending"} for lv in levels],
                "created_at": now,
                "next_candle_ms": (now // _MINUTE_MS + 1) * _MINUTE_MS,
                "fees": 0.0,
                "gross_pnl": 0.0,
                "exits": [],
            },
        )
        if req.entry_type is EntryType.MARKET:
            self._fill_entry(pos, quote.entry_price_for(side), now)
        elif (long and quote.ask <= limit_price) or (not long and quote.bid >= limit_price):
            # marketable limit: fills at once at the better market price
            self._fill_entry(pos, min(quote.ask, limit_price) if long else max(quote.bid, limit_price), now)
        self._track(pos)
        return pos

    async def update_stop_loss(self, pos: BrokerPosition, new_stop_loss: float) -> BrokerPosition:
        pos = pos.model_copy(deep=True)
        info = self._info(pos)
        price = round_to_tick(new_stop_loss, info.price_tick)
        if pos.status is PositionStatus.OPEN:
            quote = await self.quote_fn(info)
            if (pos.side is Side.LONG and price >= quote.bid) or (pos.side is Side.SHORT and price <= quote.ask):
                # a real exchange rejects a stop that would trigger immediately
                raise BrokerError(f"{self.name}: yeni stop ({_fmt(price)}) mevcut fiyatın ({_fmt(quote.last)}) yanlış tarafında.")
        elif pos.status is not PositionStatus.PENDING:
            raise BrokerError(f"{self.name}: pozisyon açık değil ({pos.status.value}); stop taşınamadı.")
        pos.stop_loss = price
        self._track(pos)
        return pos

    async def update_take_profits(self, pos: BrokerPosition, take_profits: list[TakeProfit]) -> BrokerPosition:
        pos = pos.model_copy(deep=True)
        if pos.status not in (PositionStatus.PENDING, PositionStatus.OPEN):
            raise BrokerError(f"{self.name}: pozisyon açık değil ({pos.status.value}); kâr al güncellenemedi.")
        info = self._info(pos)
        levels = [TakeProfit(price=round_to_tick(tp.price, info.price_tick), close_fraction=tp.close_fraction)
                  for tp in take_profits]
        pos.take_profits = levels
        if pos.status is PositionStatus.PENDING:
            pos.data["tps"] = [{"price": lv.price, "fraction": lv.close_fraction, "qty": 0.0, "status": "pending"}
                               for lv in levels]
        else:
            self._new_tps(pos, levels, pos.remaining_quantity)
        self._track(pos)
        return pos

    async def close(self, pos: BrokerPosition, fraction: float = 1.0) -> BrokerPosition:
        pos = pos.model_copy(deep=True)
        if pos.status is PositionStatus.PENDING:
            return await self.cancel_entry(pos)
        if pos.status is not PositionStatus.OPEN:
            return pos
        info = self._info(pos)
        full = fraction >= 0.999
        qty = pos.remaining_quantity if full else floor_to_step(
            min(pos.remaining_quantity, pos.filled_quantity * max(0.0, fraction)), info.qty_step)
        if not full and pos.remaining_quantity - qty < max(info.min_qty, info.qty_step):
            full, qty = True, pos.remaining_quantity
        if qty <= 0 or (not full and qty < info.min_qty):
            raise BrokerError(f"{self.name}: kapatılacak miktar ({_fmt(qty)}) minimum emir miktarının altında.")
        quote = await self.quote_fn(info)
        self._exit(pos, qty, quote.exit_price_for(pos.side), "manual_close")
        if pos.status is PositionStatus.OPEN:
            levels = [TakeProfit(price=tp["price"], close_fraction=tp["fraction"])
                      for tp in pos.data["tps"] if tp.get("status") in ("open", "skipped")]
            if levels:
                self._new_tps(pos, levels, pos.remaining_quantity)
        self._track(pos)
        return pos

    async def cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        pos = pos.model_copy(deep=True)
        if pos.status is PositionStatus.OPEN:
            return await self.close(pos, 1.0)
        if pos.status is PositionStatus.PENDING:
            pos.status = PositionStatus.CANCELLED
            pos.remaining_quantity = 0.0
            self._track(pos)
        return pos

    # ------------------------------------------------------------------ sync
    async def _closed_candles(self, symbol: str, since: int) -> list[list[float]]:
        if self.candles_fn is None:
            return []
        try:
            candles = await self.candles_fn(symbol, since)
        except Exception as e:  # noqa: BLE001 - fall back to the quote
            logger.warning("%s: candles for %s unavailable: %s", self.name, symbol, e)
            return []
        now = self.now_ms()
        out = [c for c in candles or [] if len(c) >= 5 and c[0] >= since and c[0] + _MINUTE_MS <= now]
        return sorted(out, key=lambda c: c[0])

    async def sync(self, pos: BrokerPosition) -> BrokerPosition:
        if pos.status not in (PositionStatus.PENDING, PositionStatus.OPEN):
            return pos
        if not pos.data.get("simulated"):
            logger.warning("%s: %s is not a simulated position; not synced", self.name, pos.broker_symbol)
            return pos
        self._ensure_loaded()
        pos = pos.model_copy(deep=True)
        d, side = pos.data, pos.side
        sl = float(pos.stop_loss) if pos.stop_loss is not None else None
        candles = await self._closed_candles(pos.broker_symbol, int(d.get("next_candle_ms", 0)))
        for ts, op, high, low, *_rest in candles:
            op, high, low = float(op), float(high), float(low)
            d["next_candle_ms"] = int(ts) + _MINUTE_MS
            if pos.status is PositionStatus.PENDING:
                limit = float(d["limit_price"])
                if (side is Side.LONG and low <= limit) or (side is Side.SHORT and high >= limit):
                    gapped = (op < limit) if side is Side.LONG else (op > limit)
                    self._fill_entry(pos, op if gapped else limit, int(ts))
                    d["next_candle_ms"] = int(ts) + _MINUTE_MS
                    if sl is not None and self._sl_hit(side, sl, low, high):   # same candle: assume the worst
                        self._exit(pos, pos.remaining_quantity, sl, "stop")
                continue
            if pos.status is not PositionStatus.OPEN:
                break
            if sl is not None and self._sl_hit(side, sl, low, high):
                gapped = (op < sl) if side is Side.LONG else (op > sl)
                self._exit(pos, pos.remaining_quantity, op if gapped else sl, "stop")   # SL first if both hit
                break
            for tp in sorted((t for t in d["tps"] if t.get("status") == "open"),
                             key=lambda t: t["price"], reverse=side is Side.SHORT):
                if self._tp_hit(side, float(tp["price"]), low, high):
                    tp["status"] = "filled"
                    self._exit(pos, float(tp["qty"]), float(tp["price"]), "take_profit")
            if pos.status is not PositionStatus.OPEN:
                break

        if pos.status in (PositionStatus.PENDING, PositionStatus.OPEN):
            try:
                quote = await self.quote_fn(self._info(pos))
            except BrokerError as e:
                logger.warning("%s: no quote during sync: %s", self.name, e)
                quote = None
            if quote is not None:
                self._apply_quote(pos, quote)
        self._track(pos)
        return pos

    def _apply_quote(self, pos: BrokerPosition, quote: Quote) -> None:
        d, side = pos.data, pos.side
        long = side is Side.LONG
        if pos.status is PositionStatus.PENDING:
            limit = float(d["limit_price"])
            if (long and quote.ask <= limit) or (not long and quote.bid >= limit):
                self._fill_entry(pos, limit, self.now_ms())
            else:
                return
        exit_px = quote.exit_price_for(side)
        if pos.stop_loss is not None and ((long and exit_px <= pos.stop_loss) or (not long and exit_px >= pos.stop_loss)):
            self._exit(pos, pos.remaining_quantity, exit_px, "stop")
            return
        for tp in d["tps"]:
            if tp.get("status") == "open" and ((long and exit_px >= tp["price"]) or (not long and exit_px <= tp["price"])):
                tp["status"] = "filled"
                self._exit(pos, float(tp["qty"]), float(tp["price"]), "take_profit")
                if pos.status is not PositionStatus.OPEN:
                    return


class PaperBroker(Broker):
    kind = "paper"

    def __init__(
        self,
        cfg: PaperBrokerConfig,
        data_dir: str,
        *,
        price_source: Any | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        super().__init__(cfg)
        self.cfg: PaperBrokerConfig = cfg
        self.risk_config: RiskConfig = RiskConfig()
        self._exchange: Any = price_source     # public ccxt exchange; injectable for tests
        self._owns_exchange = price_source is None
        self._markets_loaded = False
        self.sim = PaperSimulator(
            name=cfg.name,
            state_path=Path(data_dir) / f"paper_{safe_file_part(cfg.name)}.json",
            starting_balance=cfg.starting_balance,
            quote_fn=self.get_quote,
            fee_rate_fn=lambda _info: cfg.fee_rate,
            candles_fn=self._candles,
            dry_run=False,
            clock_ms=clock_ms,
        )

    # ------------------------------------------------------------------ lifecycle
    async def connect(self) -> None:
        self.sim.load()
        logger.info("%s: paper account, balance %.2f %s", self.name, self.sim.balance, self.cfg.quote_currency)

    async def close(self) -> None:
        if self._exchange is not None and self._owns_exchange:
            try:
                await self._exchange.close()
            except Exception:
                logger.exception("%s: error closing price source", self.name)

    async def _prices(self) -> Any:
        if self._exchange is None:
            klass = getattr(ccxt_async, self.cfg.price_source_exchange, None)
            if klass is None:
                raise BrokerError(f"{self.name}: bilinmeyen fiyat kaynağı '{self.cfg.price_source_exchange}'.")
            self._exchange = klass({"enableRateLimit": True, "options": {"defaultType": "swap"}})
        if not self._markets_loaded:
            try:
                await self._exchange.load_markets()
            except cx.BaseError as e:
                raise BrokerError(f"{self.name}: fiyat kaynağı piyasaları yüklenemedi: {e}") from e
            self._markets_loaded = True
        return self._exchange

    # ------------------------------------------------------------------ account
    async def get_equity(self) -> float:
        if not getattr(self.risk_config, "use_equity", True):
            return self.sim.balance
        return self.sim.balance + await self.sim.unrealized_pnl()

    async def get_free_margin(self) -> float:
        return self.sim.balance + min(0.0, await self.sim.unrealized_pnl())

    async def account_currency(self) -> str:
        return self.cfg.quote_currency.upper()

    # ------------------------------------------------------------------ market data
    async def resolve_symbol(
        self,
        base_asset: str,
        quote_asset: str | None = None,
        market_type: MarketType | None = None,
        raw_symbol: str | None = None,
    ) -> SymbolInfo | None:
        ex = await self._prices()
        mapped = _lookup_symbol_map(self.cfg.symbol_map, [raw_symbol, base_asset])
        if market_type in (MarketType.FOREX, MarketType.CFD) and not mapped:
            return None
        found = find_market(
            ex.markets or {}, base_asset=base_asset, quote_asset=quote_asset,
            default_quote=self.cfg.quote_currency, market_type="swap",
            symbol_map=self.cfg.symbol_map, raw_symbol=raw_symbol,
        )
        if found is not None:
            market, multiplier = found
            return symbol_info_from_market(market, ex.precisionMode, base_asset=base_asset, price_multiplier=multiplier)
        if not mapped:
            return None
        # mapped symbol unknown to the market list: accept it if a ticker exists
        try:
            quote = quote_from_ticker(await ex.fetch_ticker(mapped))
        except cx.BaseError:
            quote = None
        if quote is None:
            return None
        return SymbolInfo(broker_symbol=mapped, base_asset=base_asset.upper(), quote_asset=self.cfg.quote_currency.upper(),
                          qty_step=1e-8, min_qty=0.0, price_tick=1e-8, extra={"price_multiplier": 1.0})

    async def get_quote(self, symbol: SymbolInfo) -> Quote:
        ex = await self._prices()
        try:
            quote = quote_from_ticker(await ex.fetch_ticker(symbol.broker_symbol))
        except cx.BaseError as e:
            raise BrokerError(f"{self.name}: {symbol.broker_symbol} fiyatı alınamadı: {e}") from e
        if quote is None:
            raise BrokerError(f"{self.name}: {symbol.broker_symbol} için fiyat yok.")
        return quote

    async def _candles(self, broker_symbol: str, since_ms: int) -> list[list[float]]:
        ex = await self._prices()
        return await ex.fetch_ohlcv(broker_symbol, "1m", since_ms, 500)

    # ------------------------------------------------------------------ sizing
    async def size_position(
        self,
        symbol: SymbolInfo,
        side: Side,
        entry_price: float,
        stop_loss: float,
        risk_amount: float,
    ) -> SizingResult:
        if (side is Side.LONG and stop_loss >= entry_price) or (side is Side.SHORT and stop_loss <= entry_price):
            raise BrokerError(f"Stop ({_fmt(stop_loss)}) {side.value} işlem için girişin ({_fmt(entry_price)}) yanlış tarafında.")
        risk = self.risk_config or RiskConfig()
        result = compute_linear_size(
            risk_amount=risk_amount,
            entry=entry_price,
            stop=stop_loss,
            contract_size=symbol.contract_size,
            qty_step=symbol.qty_step,
            min_qty=symbol.min_qty,
            max_qty=symbol.max_qty,
            min_notional=symbol.min_notional,
            fee_rate=self.cfg.fee_rate,
            include_fees=risk.include_fees_in_risk,
            allow_min_qty_round_up=risk.allow_min_qty_round_up,
        )
        # Leverage is informational on paper: the smallest that fits the balance.
        equity = max(await self.get_equity(), 1e-9)
        leverage = max(1, math.ceil(result.notional / equity - 1e-9))
        return result.model_copy(update={"leverage": float(leverage), "margin_required": result.notional / leverage})

    # ------------------------------------------------------------------ trading
    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        return await self.sim.open(req)

    async def update_stop_loss(self, pos: BrokerPosition, new_stop_loss: float) -> BrokerPosition:
        return await self.sim.update_stop_loss(pos, new_stop_loss)

    async def update_take_profits(self, pos: BrokerPosition, take_profits: list[TakeProfit]) -> BrokerPosition:
        return await self.sim.update_take_profits(pos, take_profits)

    async def close_position(self, pos: BrokerPosition, fraction: float = 1.0) -> BrokerPosition:
        return await self.sim.close(pos, fraction)

    async def cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        return await self.sim.cancel_entry(pos)

    async def sync_position(self, pos: BrokerPosition) -> BrokerPosition:
        return await self.sim.sync(pos)


__all__ = ["PaperBroker", "PaperSimulator", "num", "safe_file_part"]
