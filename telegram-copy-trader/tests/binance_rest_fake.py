"""A fake Binance USD-M REST server plugged UNDER the real ccxt binanceusdm class (tests only).

Only ``exchange.fetch`` (the HTTP layer) is replaced, so request building (create_order_request, sign), endpoint
routing (order vs algoOrder), response parsing and error mapping are the real ccxt code. Response shapes follow
ccxt's recorded fixtures (ts/src/test/static/response/binance.json) and Binance's SDK field lists; error bodies
follow Binance's error-code page. The matching engine (fills, margin checks, triggers) and the clock check are a
SIMULATION: they prove the bot's logic, not the real exchange.
"""
from __future__ import annotations

import json
import time
import urllib.parse
from typing import Any, Callable

from copytrader.brokers.ccxt_broker import CcxtBroker
from copytrader.config import CcxtBrokerConfig, RiskConfig

# raw /fapi/v1/exchangeInfo entries (from ccxt's fixture markets)
_BTC = {"symbol": "BTCUSDT", "pair": "BTCUSDT", "contractType": "PERPETUAL", "deliveryDate": "4133404800000", "onboardDate": "1567965300000", "status": "TRADING", "maintMarginPercent": "2.5000", "requiredMarginPercent": "5.0000", "baseAsset": "BTC", "quoteAsset": "USDT", "marginAsset": "USDT", "pricePrecision": "2", "quantityPrecision": "3", "baseAssetPrecision": "8", "quotePrecision": "8", "underlyingType": "COIN", "underlyingSubType": ["PoW"], "triggerProtect": "0.0500", "liquidationFee": "0.012500", "marketTakeBound": "0.05", "maxMoveOrderLimit": "10000", "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.10", "maxPrice": "4529764", "minPrice": "556.80"}, {"stepSize": "0.001", "filterType": "LOT_SIZE", "maxQty": "1000", "minQty": "0.001"}, {"minQty": "0.001", "filterType": "MARKET_LOT_SIZE", "maxQty": "120", "stepSize": "0.001"}, {"filterType": "MAX_NUM_ORDERS", "limit": "200"}, {"filterType": "MIN_NOTIONAL", "notional": "50"}, {"multiplierDown": "0.9500", "multiplierUp": "1.0500", "multiplierDecimal": "4", "filterType": "PERCENT_PRICE"}, {"positionControlSide": "NONE", "filterType": "POSITION_RISK_CONTROL"}], "orderTypes": ["LIMIT", "MARKET", "STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"], "timeInForce": ["GTC", "IOC", "FOK", "GTX", "GTD"], "permissionSets": ["GRID", "COPY", "DCA", "PSB"]}
_DOGE = {"symbol": "DOGEUSDT", "pair": "DOGEUSDT", "contractType": "PERPETUAL", "deliveryDate": "4133404800000", "onboardDate": "1594371600000", "status": "TRADING", "maintMarginPercent": "2.5000", "requiredMarginPercent": "5.0000", "baseAsset": "DOGE", "quoteAsset": "USDT", "marginAsset": "USDT", "pricePrecision": "6", "quantityPrecision": "0", "baseAssetPrecision": "8", "quotePrecision": "8", "underlyingType": "COIN", "underlyingSubType": ["Meme"], "triggerProtect": "0.1000", "liquidationFee": "0.015000", "marketTakeBound": "0.10", "maxMoveOrderLimit": "10000", "filters": [{"maxPrice": "30", "minPrice": "0.002440", "tickSize": "0.000010", "filterType": "PRICE_FILTER"}, {"maxQty": "300000000", "minQty": "1", "stepSize": "1", "filterType": "LOT_SIZE"}, {"maxQty": "30000000", "filterType": "MARKET_LOT_SIZE", "stepSize": "1", "minQty": "1"}, {"limit": "200", "filterType": "MAX_NUM_ORDERS"}, {"notional": "5", "filterType": "MIN_NOTIONAL"}, {"multiplierUp": "1.1000", "multiplierDown": "0.9000", "multiplierDecimal": "4", "filterType": "PERCENT_PRICE"}, {"filterType": "POSITION_RISK_CONTROL", "positionControlSide": "NONE"}], "orderTypes": ["LIMIT", "MARKET", "STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"], "timeInForce": ["GTC", "IOC", "FOK", "GTX", "GTD"], "permissionSets": ["GRID", "COPY", "RPI", "DCA", "PSB"]}


class ApiError(Exception):
    def __init__(self, code: int, msg: str, status: int = 400) -> None:
        super().__init__(f"{code} {msg}")
        self.code, self.msg, self.status = code, msg, status


def _f(x: float, nd: int = 8) -> str:
    return f"{x:.{nd}f}"


class FakeBinance:
    def __init__(self, ex: Any, *, wallet: float = 46.90) -> None:
        self.ex = ex
        self.now = 1_794_000_000_000
        self.log: list[tuple[str, str, dict[str, str]]] = []      # (method, path, params)
        self.signed_log: list[tuple[str, str]] = []
        self.wallet = wallet
        self.symbols: dict[str, dict[str, Any]] = {"BTCUSDT": json.loads(json.dumps(_BTC)),
                                                   "DOGEUSDT": json.loads(json.dumps(_DOGE))}
        self.price: dict[str, tuple[float, float]] = {"BTCUSDT": (74014.9, 74015.0), "DOGEUSDT": (0.19999, 0.2)}
        self.max_lev: dict[str, int] = {"BTCUSDT": 125, "DOGEUSDT": 75}
        self.lev: dict[str, int] = {}
        self.mtype: dict[str, str] = {}
        self.pos: dict[str, dict[str, float]] = {}
        self.orders: dict[str, dict[str, Any]] = {}
        self.algos: dict[str, dict[str, Any]] = {}
        self.trades: list[dict[str, Any]] = []
        self._oid = 5_000_000
        self._aid = 9_000
        self._tid = 100
        self.hedge_mode = False
        # hooks: list of (predicate(method, path, params) -> bool, action) ; action = ApiError | Exception | callable
        self.hooks: list[tuple[Callable[[str, str, dict[str, str]], bool], Any, list[int]]] = []
        self.completed_algo_query_missing = False   # nautilus_trader#4448 claim: GET algoOrder -2013 after completion
        self.algo_orders_block_margin_type = False
        self.algo_history_works = True              # GET allAlgoOrders still lists the order (nautilus_trader#4448)
        # Clock: None = timestamps are not checked and the server time is ``now``. A number = this machine's
        # clock minus the exchange's clock in ms; signed requests are then checked like Binance does (-1021).
        self.local_clock_ahead_ms: float | None = None
        self.multi_assets = False
        self.margin_factor = 1.0                    # SIMULATED: margin an order needs / (notional / leverage)
        self.tiers: dict[str, list[tuple[int, float]]] = {}   # leverage brackets [(leverage, notional cap)]; else one
        self.no_brackets: set[str] = set()          # symbols missing from the leverage bracket answer
        self.symbol_config_works = True

    # ------------------------------------------------------------------ helpers
    def add_symbol(self, sym: str, like: str, *, bid: float, ask: float, max_lev: int, **over: Any) -> None:
        info = json.loads(json.dumps(self.symbols[like]))
        info.update(symbol=sym, pair=sym, baseAsset=sym[:-4])
        for flt in info["filters"]:
            for k, v in over.get(flt["filterType"], {}).items():
                flt[k] = v
        self.symbols[sym] = info
        self.price[sym] = (bid, ask)
        self.max_lev[sym] = max_lev

    def fail(self, pred: Callable[[str, str, dict[str, str]], bool], action: Any, times: int = 1) -> None:
        self.hooks.append((pred, action, [times]))

    def paths(self) -> list[str]:
        return [f"{m} {p}" for m, p, _ in self.log]

    def _p(self, sym: str) -> dict[str, float]:
        return self.pos.setdefault(sym, {"amt": 0.0, "entry": 0.0})

    def _mark(self, sym: str) -> float:
        b, a = self.price[sym]
        return (b + a) / 2

    def _unreal(self) -> float:
        return sum(p["amt"] * (self._mark(s) - p["entry"]) for s, p in self.pos.items() if abs(p["amt"]) > 1e-12)

    def _init_margin(self) -> float:
        return sum(abs(p["amt"]) * self._mark(s) / self.lev.get(s, 20) for s, p in self.pos.items() if abs(p["amt"]) > 1e-12)

    def available(self) -> float:
        return max(0.0, self.wallet + self._unreal() - self._init_margin())

    def server_ms(self) -> float:
        if self.local_clock_ahead_ms is None:
            return float(self.now)
        return time.time() * 1000 - self.local_clock_ahead_ms

    def signed(self, method: str | None = None, path: str | None = None) -> list[tuple[str, str]]:
        """(method, path) of the signed requests seen so far."""
        return [(m, p) for m, p in self.signed_log if method in (None, m) and path in (None, p)]

    # ------------------------------------------------------------------ transport
    async def fetch(self, url: str, method: str = "GET", headers: Any = None, body: Any = None) -> Any:
        parts = urllib.parse.urlsplit(url)
        query = dict(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
        if body:
            query.update(dict(urllib.parse.parse_qsl(body, keep_blank_values=True)))
        path = parts.path
        params = {k: v for k, v in query.items() if k not in ("timestamp", "signature", "recvWindow")}
        self.log.append((method, path, params))
        self.now += 50
        status, payload = 200, None
        try:
            if "signature" in query:
                self.signed_log.append((method, path))
                if self.local_clock_ahead_ms is not None:      # SIMULATED after Binance's rule for -1021
                    ahead = float(query["timestamp"]) - self.server_ms()
                    if ahead >= 1000:
                        raise ApiError(-1021, "Timestamp for this request was 1000ms ahead of the server's time.")
                    if ahead < -float(query.get("recvWindow", 5000)):
                        raise ApiError(-1021, "Timestamp for this request is outside of the recvWindow.")
            for i, (pred, action, left) in enumerate(self.hooks):
                if left[0] > 0 and pred(method, path, params):
                    left[0] -= 1
                    if isinstance(action, ApiError):
                        raise action
                    if isinstance(action, Exception):
                        raise action
                    out = action(method, path, params)
                    if out is not None:
                        payload = out
                    break
            if payload is None:
                payload = self._route(method, path, params)
        except ApiError as e:
            status, payload = e.status, {"code": e.code, "msg": e.msg}
        text = json.dumps(payload)
        reason = "OK" if status == 200 else "Bad Request"
        # same two calls ccxt's own fetch() makes after reading the response (async_support/base/exchange.py:290-291)
        self.ex.handle_errors(status, reason, url, method, {}, text, payload, headers, body)
        self.ex.handle_http_status_code(status, reason, url, method, text)
        return payload

    def _route(self, method: str, path: str, q: dict[str, str]) -> Any:
        table = {
            ("GET", "/fapi/v1/exchangeInfo"): self.exchange_info,
            ("GET", "/sapi/v1/capital/config/getall"): lambda q: [],
            ("GET", "/sapi/v1/margin/allPairs"): lambda q: [],
            ("GET", "/fapi/v1/time"): lambda q: {"serverTime": int(self.server_ms())},
            ("GET", "/fapi/v1/positionSide/dual"): lambda q: {"dualSidePosition": self.hedge_mode},
            ("GET", "/fapi/v1/multiAssetsMargin"): lambda q: {"multiAssetsMargin": self.multi_assets},
            ("GET", "/fapi/v1/symbolConfig"): self.symbol_config,
            ("GET", "/fapi/v3/account"): self.account,
            ("GET", "/fapi/v1/ticker/24hr"): self.ticker,
            ("GET", "/fapi/v1/depth"): self.depth,
            ("POST", "/fapi/v1/marginType"): self.margin_type,
            ("POST", "/fapi/v1/leverage"): self.leverage,
            ("POST", "/fapi/v1/order"): self.new_order,
            ("GET", "/fapi/v1/order"): self.get_order,
            ("DELETE", "/fapi/v1/order"): self.cancel_order,
            ("GET", "/fapi/v1/openOrders"): self.open_orders,
            ("GET", "/fapi/v1/allOrders"): self.all_orders,
            ("POST", "/fapi/v1/algoOrder"): self.new_algo,
            ("GET", "/fapi/v1/algoOrder"): self.get_algo,
            ("DELETE", "/fapi/v1/algoOrder"): self.cancel_algo,
            ("GET", "/fapi/v1/openAlgoOrders"): self.open_algos,
            ("GET", "/fapi/v1/allAlgoOrders"): self.all_algos,
            ("GET", "/fapi/v3/positionRisk"): self.position_risk,
            ("GET", "/fapi/v1/leverageBracket"): self.brackets,
            ("GET", "/fapi/v1/userTrades"): self.user_trades,
        }
        handler = table.get((method, path))
        if handler is None:
            raise AssertionError(f"fake has no handler for {method} {path} {q}")
        return handler(q)

    # ------------------------------------------------------------------ public
    def exchange_info(self, q: dict[str, str]) -> Any:
        return {"timezone": "UTC", "serverTime": self.now, "rateLimits": [], "exchangeFilters": [],
                "assets": [], "symbols": list(self.symbols.values())}

    def ticker(self, q: dict[str, str]) -> Any:
        b, a = self.price[q["symbol"]]
        return {"symbol": q["symbol"], "priceChange": "0", "priceChangePercent": "0", "weightedAvgPrice": _f(a, 4),
                "lastPrice": _f(a, 6), "lastQty": "1", "openPrice": _f(a, 6), "highPrice": _f(a, 6), "lowPrice": _f(b, 6),
                "volume": "1000", "quoteVolume": "1000", "openTime": self.now - 86_400_000, "closeTime": self.now,
                "firstId": 1, "lastId": 2, "count": 2}

    def depth(self, q: dict[str, str]) -> Any:
        b, a = self.price[q["symbol"]]
        return {"lastUpdateId": 1, "E": self.now, "T": self.now, "bids": [[_f(b, 6), "10"]], "asks": [[_f(a, 6), "10"]]}

    # ------------------------------------------------------------------ account
    def account(self, q: dict[str, str]) -> Any:
        un, im = self._unreal(), self._init_margin()
        usdt = {"asset": "USDT", "walletBalance": _f(self.wallet), "unrealizedProfit": _f(un),
                "marginBalance": _f(self.wallet + un), "maintMargin": "0.00000000", "initialMargin": _f(im),
                "positionInitialMargin": _f(im), "openOrderInitialMargin": "0.00000000",
                "maxWithdrawAmount": _f(self.available()), "crossWalletBalance": _f(self.wallet),
                "crossUnPnl": _f(un), "availableBalance": _f(self.available()), "marginAvailable": True,
                "updateTime": str(self.now)}
        return {"feeTier": "0", "canTrade": True, "multiAssetsMargin": False, "totalWalletBalance": _f(self.wallet),
                "totalUnrealizedProfit": _f(un), "totalMarginBalance": _f(self.wallet + un),
                "availableBalance": _f(self.available()), "assets": [usdt], "positions": []}

    def margin_type(self, q: dict[str, str]) -> Any:
        sym, want = q["symbol"], q["marginType"]
        cur = self.mtype.get(sym, "CROSSED")
        if cur == want:
            raise ApiError(-4046, "No need to change margin type.")
        if any(o["symbol"] == sym and o["status"] in ("NEW", "PARTIALLY_FILLED") for o in self.orders.values()):
            raise ApiError(-4047, "Margin type cannot be changed if there exists open orders.")
        if self.algo_orders_block_margin_type and any(a["symbol"] == sym and a["algoStatus"] == "NEW" for a in self.algos.values()):
            raise ApiError(-4047, "Margin type cannot be changed if there exists open orders.")
        if abs(self._p(sym)["amt"]) > 1e-12:
            raise ApiError(-4048, "Margin type cannot be changed if there exists position.")
        self.mtype[sym] = want
        return {"code": 200, "msg": "success"}

    def leverage(self, q: dict[str, str]) -> Any:
        sym, lev = q["symbol"], int(q["leverage"])
        if lev > self.max_lev[sym]:
            raise ApiError(-4028, f"Leverage {lev} is not valid")
        self.lev[sym] = lev
        return {"symbol": sym, "leverage": lev, "maxNotionalValue": "1000000"}

    def brackets(self, q: dict[str, str]) -> Any:
        out = []
        for s in self.symbols:
            if s in self.no_brackets:
                continue
            rows, floor = [], 0.0
            for i, (lev, cap) in enumerate(self.tiers.get(s) or [(self.max_lev[s], 50000.0)], start=1):
                rows.append({"bracket": i, "initialLeverage": lev, "notionalCap": cap, "notionalFloor": floor,
                             "maintMarginRatio": 0.005 * i, "cum": 0.0})
                floor = cap
            out.append({"symbol": s, "notionalCoef": 1.0, "brackets": rows})
        return out

    def symbol_config(self, q: dict[str, str]) -> Any:
        if not self.symbol_config_works:
            raise ApiError(-1000, "An unknown error occured while processing the request.")
        return [{"symbol": s, "marginType": self.mtype.get(s, "CROSSED"), "isAutoAddMargin": "false",
                 "leverage": self.lev.get(s, 20), "maxNotionalValue": "1000000"}
                for s in self.symbols if q.get("symbol") in (None, s)]

    def position_risk(self, q: dict[str, str]) -> Any:
        out = []
        for s in self.symbols:
            p = self._p(s)
            mark = self._mark(s)
            out.append({"symbol": s, "positionSide": "BOTH", "positionAmt": _f(p["amt"], 3),
                        "entryPrice": _f(p["entry"], 4) if abs(p["amt"]) > 1e-12 else "0.0",
                        "breakEvenPrice": _f(p["entry"], 4), "markPrice": _f(mark), "unRealizedProfit": _f(p["amt"] * (mark - p["entry"])),
                        "liquidationPrice": "0", "isolatedMargin": "0", "notional": _f(p["amt"] * mark),
                        "marginAsset": "USDT", "isolatedWallet": "0", "initialMargin": _f(abs(p["amt"]) * mark / self.lev.get(s, 20)),
                        "maintMargin": "0", "positionInitialMargin": "0", "openOrderInitialMargin": "0", "adl": "0",
                        "bidNotional": "0", "askNotional": "0", "updateTime": str(self.now)})
        return out

    # ------------------------------------------------------------------ matching (SIMULATED)
    def _fill(self, order: dict[str, Any], qty: float, price: float) -> None:
        sym = order["symbol"]
        p = self._p(sym)
        sign = 1.0 if order["side"] == "BUY" else -1.0
        old = p["amt"]
        new = round(old + sign * qty, 10)
        if abs(old) < 1e-12 or (old > 0) == (sign > 0):          # opening / adding
            p["entry"] = (abs(old) * p["entry"] + qty * price) / (abs(old) + qty)
        else:                                                    # reducing
            closed = min(qty, abs(old))
            self.wallet += closed * (price - p["entry"]) * (1.0 if old > 0 else -1.0)
            if abs(new) > 1e-12 and (new > 0) != (old > 0):
                p["entry"] = price
        p["amt"] = new
        fee = qty * price * 0.0005
        self.wallet -= fee
        prev = float(order["executedQty"])
        cum = float(order["cumQuote"]) + qty * price
        order["executedQty"] = _f(prev + qty, 3)
        order["cumQuote"] = _f(cum, 5)
        order["avgPrice"] = _f(cum / (prev + qty), 5)
        order["status"] = "FILLED" if prev + qty >= float(order["origQty"]) - 1e-12 else "PARTIALLY_FILLED"
        order["updateTime"] = self.now
        self._tid += 1
        self.trades.append({"symbol": sym, "id": self._tid, "orderId": order["orderId"], "side": order["side"],
                            "price": _f(price, 5), "qty": _f(qty, 3), "realizedPnl": "0", "marginAsset": "USDT",
                            "quoteQty": _f(qty * price, 5), "commission": _f(fee), "commissionAsset": "USDT",
                            "time": self.now, "positionSide": "BOTH", "maker": False, "buyer": order["side"] == "BUY"})

    def _order_json(self, o: dict[str, Any], *, create: bool = False) -> dict[str, Any]:
        out = {k: v for k, v in o.items() if not k.startswith("_")}
        if create:
            out["cumQty"] = out["executedQty"]
            out.pop("time", None)
        return out

    def new_order(self, q: dict[str, str]) -> Any:
        sym, side, typ = q["symbol"], q["side"], q["type"]
        if typ in ("STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"):
            raise ApiError(-4120, "Order type not supported for this endpoint. Please use the Algo Order API endpoints instead.")
        if self.hedge_mode and "positionSide" not in q:
            raise ApiError(-4061, "Order's position side does not match user's setting.")
        qty = float(q["quantity"])
        reduce_only = q.get("reduceOnly") == "true"
        b, a = self.price[sym]
        px = a if side == "BUY" else b
        info = self.symbols[sym]
        min_notional = float(next(f["notional"] for f in info["filters"] if f["filterType"] == "MIN_NOTIONAL"))
        p = self._p(sym)
        ref = float(q["price"]) if typ == "LIMIT" else px
        if not reduce_only and qty * ref < min_notional:
            raise ApiError(-4164, f"Order's notional must be no smaller than {min_notional:g} (unless you choose reduce only).")
        if reduce_only:
            closing = (p["amt"] > 0 and side == "SELL") or (p["amt"] < 0 and side == "BUY")
            if not closing:
                raise ApiError(-2022, "ReduceOnly Order is rejected.")
            qty_exec = min(qty, abs(p["amt"]))
        else:
            qty_exec = qty
            lev = self.lev.get(sym, 20)
            cap = max((c for l, c in self.tiers.get(sym) or [(lev, 1e12)] if l >= lev), default=0.0)
            if abs(p["amt"]) * ref + qty * ref > cap + 1e-9:        # SIMULATED bracket rule
                raise ApiError(-2027, "Exceeded the maximum allowable position at current leverage.")
            need = qty * ref / lev * self.margin_factor
            if need > self.available() + 1e-9:
                raise ApiError(-2019, "Margin is insufficient.")
        self._oid += 1
        o = {"orderId": self._oid, "symbol": sym, "status": "NEW", "clientOrderId": q.get("newClientOrderId", "auto"),
             "price": q.get("price", "0.00"), "avgPrice": "0.00", "origQty": _f(qty, 3), "executedQty": "0.000",
             "cumQuote": "0.00000", "timeInForce": q.get("timeInForce", "GTC"), "type": typ, "reduceOnly": reduce_only,
             "closePosition": False, "side": side, "positionSide": "BOTH", "stopPrice": "0.00", "workingType": "CONTRACT_PRICE",
             "priceProtect": False, "origType": typ, "priceMatch": "NONE", "selfTradePreventionMode": "EXPIRE_MAKER",
             "goodTillDate": 0, "time": self.now, "updateTime": self.now}
        self.orders[str(self._oid)] = o
        if typ == "MARKET":
            self._fill(o, qty_exec, px)
            if qty_exec < qty - 1e-12:
                o["status"] = "EXPIRED"
        elif typ == "LIMIT":
            marketable = (side == "BUY" and float(q["price"]) >= a) or (side == "SELL" and float(q["price"]) <= b)
            if marketable:
                self._fill(o, qty_exec, px)
        return self._order_json(o, create=True)

    def _find_order(self, q: dict[str, str], missing: int) -> dict[str, Any]:
        if "orderId" in q:
            o = self.orders.get(q["orderId"])
        else:
            o = next((x for x in self.orders.values() if x["clientOrderId"] == q.get("origClientOrderId")), None)
        if o is None or o["symbol"] != q.get("symbol"):
            raise ApiError(missing, "Order does not exist." if missing == -2013 else "Unknown order sent.")
        return o

    def get_order(self, q: dict[str, str]) -> Any:
        return self._order_json(self._find_order(q, -2013))

    def cancel_order(self, q: dict[str, str]) -> Any:
        o = self._find_order(q, -2011)
        if o["status"] not in ("NEW", "PARTIALLY_FILLED"):
            raise ApiError(-2011, "Unknown order sent.")
        o["status"] = "CANCELED"
        o["updateTime"] = self.now
        return self._order_json(o, create=True)

    def open_orders(self, q: dict[str, str]) -> Any:
        return [self._order_json(o) for o in self.orders.values()
                if q.get("symbol") in (None, o["symbol"]) and o["status"] in ("NEW", "PARTIALLY_FILLED")]

    def all_orders(self, q: dict[str, str]) -> Any:
        return [self._order_json(o) for o in self.orders.values() if o["symbol"] == q.get("symbol")]

    def fill_limit(self, order_id: str, qty: float | None = None) -> None:
        o = self.orders[order_id]
        left = float(o["origQty"]) - float(o["executedQty"])
        self._fill(o, left if qty is None else qty, float(o["price"]))

    def user_trades(self, q: dict[str, str]) -> Any:
        start = int(q.get("startTime", 0))
        end = int(q.get("endTime", 10 ** 15))
        return [t for t in self.trades if t["symbol"] == q["symbol"] and start <= t["time"] <= end]

    # ------------------------------------------------------------------ algo (conditional) orders
    def new_algo(self, q: dict[str, str]) -> Any:
        if q.get("algoType") != "CONDITIONAL":
            raise ApiError(-1102, "Mandatory parameter 'algotype' was not sent, was empty/null, or malformed.")
        sym, side, typ = q["symbol"], q["side"], q["type"]
        trig = float(q["triggerPrice"])
        b, a = self.price[sym]
        last = (a + b) / 2
        would = ((typ.startswith("STOP") and ((side == "SELL" and last <= trig) or (side == "BUY" and last >= trig)))
                 or (typ.startswith("TAKE_PROFIT") and ((side == "SELL" and last >= trig) or (side == "BUY" and last <= trig))))
        if would:
            raise ApiError(-2021, "Order would immediately trigger.")
        self._aid += 1
        al = {"algoId": self._aid, "clientAlgoId": q.get("clientAlgoId", "auto"), "algoType": "CONDITIONAL", "orderType": typ,
              "symbol": sym, "side": side, "positionSide": "BOTH", "timeInForce": "GTC", "quantity": q.get("quantity", "0"),
              "algoStatus": "NEW", "actualOrderId": "", "actualPrice": "0.00000", "triggerPrice": q["triggerPrice"],
              "price": "0.00", "icebergQuantity": None, "tpOrderType": "", "selfTradePreventionMode": "EXPIRE_MAKER",
              "workingType": q.get("workingType", "CONTRACT_PRICE"), "priceMatch": "NONE",
              "closePosition": q.get("closePosition") == "true", "priceProtect": q.get("priceProtect") == "true",
              "reduceOnly": q.get("reduceOnly") == "true", "createTime": self.now, "updateTime": self.now,
              "triggerTime": 0, "goodTillDate": 0}
        self.algos[str(self._aid)] = al
        return {k: v for k, v in al.items() if k not in ("actualOrderId", "actualPrice", "tpOrderType")}

    def _find_algo(self, q: dict[str, str], missing: int) -> dict[str, Any]:
        if "algoId" in q:
            al = self.algos.get(q["algoId"])
        else:
            al = next((x for x in self.algos.values() if x["clientAlgoId"] == q.get("clientAlgoId")), None)
        if al is None:
            raise ApiError(missing, "Order does not exist." if missing == -2013 else "Unknown order sent.")
        return al

    def get_algo(self, q: dict[str, str]) -> Any:
        al = self._find_algo(q, -2013)
        if self.completed_algo_query_missing and al["algoStatus"] in ("FINISHED", "TRIGGERED"):
            raise ApiError(-2013, "Order does not exist.")
        return dict(al)

    def cancel_algo(self, q: dict[str, str]) -> Any:
        al = self._find_algo(q, -2011)
        if al["algoStatus"] != "NEW":
            raise ApiError(-2011, "Unknown order sent.")
        al["algoStatus"] = "CANCELED"
        al["updateTime"] = self.now
        return {"algoId": al["algoId"], "clientAlgoId": al["clientAlgoId"], "code": "200", "msg": "success"}

    def all_algos(self, q: dict[str, str]) -> Any:
        if not self.algo_history_works:
            raise ApiError(-2013, "Order does not exist.")
        start = int(q.get("algoId", 0))
        rows = sorted((a for a in self.algos.values() if a["symbol"] == q.get("symbol") and int(a["algoId"]) >= start),
                      key=lambda a: int(a["algoId"]))
        return [dict(a) for a in rows[: int(q.get("limit", 500))]]

    def open_algos(self, q: dict[str, str]) -> Any:
        return [dict(a) for a in self.algos.values() if a["algoStatus"] == "NEW" and q.get("symbol") in (None, a["symbol"])]

    def trigger(self, algo_id: str, *, price: float | None = None, final: str = "FINISHED",
                fill_fraction: float = 1.0) -> dict[str, Any]:
        """SIMULATED trigger: the algo order sends a reduce-only MARKET child to the matching engine."""
        al = self.algos[algo_id]
        assert al["algoStatus"] == "NEW"
        sym, side = al["symbol"], al["side"]
        px = price if price is not None else float(al["triggerPrice"])
        p = self._p(sym)
        closing = (p["amt"] > 0 and side == "SELL") or (p["amt"] < 0 and side == "BUY")
        al["triggerTime"] = self.now
        al["updateTime"] = self.now
        if not closing:                       # reduce-only with nothing to reduce: no child order
            al["algoStatus"] = "EXPIRED"
            return al
        qty = min(float(al["quantity"]), abs(p["amt"])) * fill_fraction
        self._oid += 1
        child = {"orderId": self._oid, "symbol": sym, "status": "NEW", "clientOrderId": al["clientAlgoId"],
                 "price": "0.00", "avgPrice": "0.00", "origQty": al["quantity"], "executedQty": "0.000",
                 "cumQuote": "0.00000", "timeInForce": "GTC", "type": "MARKET", "reduceOnly": True, "closePosition": False,
                 "side": side, "positionSide": "BOTH", "stopPrice": al["triggerPrice"], "workingType": "CONTRACT_PRICE",
                 "priceProtect": False, "origType": al["orderType"], "priceMatch": "NONE",
                 "selfTradePreventionMode": "EXPIRE_MAKER", "goodTillDate": 0, "time": self.now, "updateTime": self.now}
        self.orders[str(self._oid)] = child
        self._fill(child, qty, px)
        if float(child["executedQty"]) < float(child["origQty"]) - 1e-12:
            child["status"] = "EXPIRED"
        al["algoStatus"] = final
        al["actualOrderId"] = str(self._oid)
        al["actualPrice"] = child["avgPrice"]
        return al


async def _no_sleep(_: float) -> None:
    return None


def make_binance(wallet: float = 46.90, **cfg: Any) -> tuple[CcxtBroker, FakeBinance]:
    """CcxtBroker on the REAL ccxt binanceusdm class with the fake transport (cross, 125x, one-way by default)."""
    base: dict[str, Any] = dict(type="ccxt", name="binance", exchange="binanceusdm", api_key="k", secret="s",
                                risk_usd=10, margin_mode="cross", leverage=125, max_leverage=125)
    base.update(cfg)
    broker = CcxtBroker(CcxtBrokerConfig(**base), sleep=_no_sleep)
    broker.set_risk_config(RiskConfig())
    broker.exchange = broker._build_exchange()
    broker.exchange.enableRateLimit = False
    fake = FakeBinance(broker.exchange, wallet=wallet)
    broker.exchange.fetch = fake.fetch                 # only the HTTP layer is replaced
    broker._now_ms = lambda: fake.now
    return broker, fake
