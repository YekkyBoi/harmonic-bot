"""Sample trades for the trade card tests (tests/test_tradecard.py): deterministic candles around a trade, the
trade's records as the bot keeps them, and the card data built from them."""

from __future__ import annotations

import math
import random
from datetime import UTC, datetime, timedelta
from typing import Any

from copytrader.karne import tradecard as tc
from copytrader.models import BrokerPosition, EntryType, PositionStatus, Side, TakeProfit, TradePlan, TradeRecord, \
    TradeStatus

MON = datetime(2026, 11, 16, 0, 0, tzinfo=UTC)          # a Monday (UK = UTC in November)


def walk(start: datetime, n: int, price: float, seed: int, tf_min: int = 15, vol: float = 0.0012,
         path: dict[int, float] | None = None) -> list[tuple[float, float, float, float, float]]:
    """``n`` candles from ``start``: a seeded random walk around ``price``; ``path`` pins the close of candle k (the
    walk is pulled there linearly from the previous pin)."""
    rnd = random.Random(seed)
    pins = sorted((path or {}).items())
    out = []
    close = price
    for k in range(n):
        target = None
        for (k0, p0), (k1, p1) in zip([(0, price), *pins], pins):
            if k0 <= k <= k1:
                target = p0 + (p1 - p0) * (k - k0) / max(1, k1 - k0)
                break
        o = close
        drift = (target - o) if target is not None else 0.0
        c = o + drift + rnd.gauss(0, price * vol) * (0.35 if target is not None else 1.0)
        h = max(o, c) + abs(rnd.gauss(0, price * vol * 0.6))
        low = min(o, c) - abs(rnd.gauss(0, price * vol * 0.6))
        t = (start + timedelta(minutes=tf_min * k)).timestamp()
        out.append((t, o, h, low, c))
        close = c
    return out


def plan(asset: str, side: Side, entry: float, stop: float, tps: list[float], *,
         entry_type: EntryType = EntryType.MARKET, quote: str | None = None, tf: int | None = None,
         risk_r: float = 1.0) -> TradePlan:
    return TradePlan(plan_id="p1", base_asset=asset, quote_asset=quote, side=side, entry_type=entry_type,
                     entry_price=entry, stop_loss=stop, take_profits=[TakeProfit(price=p, close_fraction=1.0 / len(tps))
                                                                      for p in tps],
                     risk_r=risk_r, risk_pct=2.0 * risk_r, source_chat_id=-100, source_message_ids=[1],
                     created_at=MON, chart_timeframe_min=tf)


def trade(no: int, p: TradePlan, status: TradeStatus, created: datetime, stop_now: float | None = None) -> TradeRecord:
    stop = stop_now if stop_now is not None else p.stop_loss
    return TradeRecord(id=no, plan=p, status=status, current_stop_loss=stop,
                       current_take_profits=list(p.take_profits), created_at=created, updated_at=created)


def nas_short_result() -> dict[str, Any]:
    """The approved sample's trade: NAS100 short on fx2 (BEM, US100.cash), limit 27300 filled 07:55, stop to entry
    09:04, closed by the group at 27055.10 09:23 (+1.70R); the price reaches the target later."""
    start = MON + timedelta(hours=17)                     # 16.11 17:00 ... the entry 17.11 07:45 bar
    bars = walk(start, 140, 27420.0, seed=7, path={40: 27350.0, 59: 27300.0, 61: 27330.0, 65: 27180.0,
                                                   65 + 1: 27120.0, 65 + 2: 27060.0, 70: 27150.0, 80: 27250.0,
                                                   95: 27350.0, 100: 27100.0, 104: 26880.0, 115: 26950.0},
                vol=0.0009)
    p = plan("NAS100", Side.SHORT, 27300.0, 27444.06, [27010.25], entry_type=EntryType.LIMIT, quote="USD", tf=15)
    created = datetime(2026, 11, 17, 5, 51, tzinfo=UTC)
    t = trade(7, p, TradeStatus.CLOSED, created, stop_now=27300.0)
    pos = BrokerPosition(broker="fx2", broker_symbol="US100.cash", side=Side.SHORT, status=PositionStatus.CLOSED,
                         quantity=1.0, filled_quantity=1.0, avg_entry_price=27300.0, stop_loss=27300.0,
                         take_profits=[TakeProfit(price=27010.25)], realized_pnl=170.0, risk_amount=100.0,
                         data={"price_tick": 0.01, "initial_risk": {"loss": 100.0, "quantity": 1.0},
                               "legs": [{"ticket": 501, "state": "closed", "filled": 1.0, "entry": 27300.0,
                                         "reason": "manual", "realized": 170.0}]})
    rec = tc.Records(placed=created, filled=datetime(2026, 11, 17, 7, 55, tzinfo=UTC),
                     closed=datetime(2026, 11, 17, 9, 23, tzinfo=UTC), exit_price=27055.10, reason="close",
                     moves=[(datetime(2026, 11, 17, 9, 4, tzinfo=UTC), "Stop girişe çekildi")], close_by="group")
    return {"trade": t, "pos": pos, "rec": rec, "bars": bars, "account": "fx2"}


def btc_long_result() -> dict[str, Any]:
    """BTC long on binance at market 91,000 (stop 90,400, TP 92,200 hit 4 h later: +2R), 15 minutes."""
    start = MON + timedelta(hours=1)
    bars = walk(start, 130, 90600.0, seed=3, path={58: 91000.0, 62: 90780.0, 70: 91400.0, 80: 91900.0,
                                                    74 + 1: 91650.0, 75 + 1: 91700.0, 75 + 2: 91900.0,
                                                    74 + 9: 92250.0, 100: 92600.0, 120: 92300.0}, vol=0.0011)
    p = plan("BTC", Side.LONG, 91000.0, 90400.0, [91800.0, 92200.0], tf=None)
    created = start + timedelta(minutes=15 * 58 + 3)
    t = trade(12, p, TradeStatus.CLOSED, created)
    pos = BrokerPosition(broker="binance", broker_symbol="BTC/USDT:USDT", side=Side.LONG,
                         status=PositionStatus.CLOSED, quantity=0.1, filled_quantity=0.1, avg_entry_price=91000.0,
                         stop_loss=91000.0, realized_pnl=118.0, risk_amount=60.0,
                         data={"initial_risk": {"loss": 60.0, "quantity": 0.1}})
    filled = created + timedelta(seconds=20)
    rec = tc.Records(placed=created, filled=filled, closed=start + timedelta(minutes=15 * 83 + 7),
                     exit_price=92000.0, reason="tp",
                     moves=[(start + timedelta(minutes=15 * 70 + 2), "Stop girişe çekildi")])
    return {"trade": t, "pos": pos, "rec": rec, "bars": bars, "account": "binance"}


def eth_long_entry() -> dict[str, Any]:
    """ETH long on breakout (hand-traded), filled at market 3,100.50 eleven candles ago and still open."""
    start = MON + timedelta(hours=3)
    bars = walk(start, 70, 3080.0, seed=11, path={58: 3100.5, 64: 3112.0, 69: 3121.0}, vol=0.0016)
    p = plan("ETH", Side.LONG, 3100.5, 3072.0, [3186.0], tf=15)
    created = start + timedelta(minutes=15 * 58 + 1)
    t = trade(14, p, TradeStatus.OPEN, created)
    pos = BrokerPosition(broker="breakout", broker_symbol="ETH/USDT:USDT", side=Side.LONG, status=PositionStatus.OPEN,
                         quantity=2.0, filled_quantity=2.0, remaining_quantity=2.0, avg_entry_price=3100.5,
                         stop_loss=3072.0, take_profits=[TakeProfit(price=3186.0)],
                         data={"simulated": True, "opened_at": int(created.timestamp() * 1000) + 5000,
                               "symbol_info": {"price_tick": 0.01}})
    rec = tc.Records(placed=created, filled=created + timedelta(seconds=5))
    return {"trade": t, "pos": pos, "rec": rec, "bars": bars, "account": "breakout", "manual": True}


def data_of(sample: dict[str, Any], tf: int = 15) -> dict[str, Any]:
    return tc.card_data(sample["trade"], sample["pos"], sample["account"], sample["rec"], sample["bars"], tf,
                        manual=bool(sample.get("manual")))


def finite(value: float) -> bool:
    return isinstance(value, float) and math.isfinite(value)


def nas_short_stopped() -> dict[str, Any]:
    """The NAS100 short of the sample, stopped out instead: the price rises to the original stop (27,444.06) and the
    stop fills at 27,446.00 (-1.02R)."""
    sample = nas_short_result()
    start = datetime.fromtimestamp(sample["bars"][0][0], tz=UTC)
    sample["bars"] = walk(start, 140, 27420.0, seed=7, path={40: 27350.0, 59: 27300.0, 62: 27360.0, 66: 27470.0,
                                                             72: 27380.0, 90: 27250.0, 110: 27150.0}, vol=0.0009)
    pos = sample["pos"].model_copy(deep=True)
    pos.realized_pnl, pos.stop_loss = -102.0, 27444.06
    pos.data["legs"][0]["reason"] = "sl"
    sample["pos"] = pos
    sample["trade"] = sample["trade"].model_copy(update={"current_stop_loss": 27444.06})
    sample["rec"] = tc.Records(placed=sample["rec"].placed, filled=sample["rec"].filled,
                               closed=datetime(2026, 11, 17, 9, 2, tzinfo=UTC), exit_price=27446.0, reason="stop")
    return sample


def btc_long_closed_at_a_loss() -> dict[str, Any]:
    """The BTC long of the sample, closed early by the owner (/pozkapat) at 90,700 (-0.5R)."""
    sample = btc_long_result()
    start = datetime.fromtimestamp(sample["bars"][0][0], tz=UTC)
    sample["bars"] = walk(start, 130, 90600.0, seed=3, path={58: 91000.0, 64: 90820.0, 68: 90700.0, 80: 90500.0,
                                                              95: 91400.0, 110: 92300.0}, vol=0.0011)
    pos = sample["pos"].model_copy(deep=True)
    pos.realized_pnl = -30.0
    sample["pos"] = pos
    sample["rec"] = tc.Records(placed=sample["rec"].placed, filled=sample["rec"].filled,
                               closed=start + timedelta(minutes=15 * 68 + 4), exit_price=90700.0, reason="close",
                               close_by="owner")
    return sample


def sol_short_pending() -> dict[str, Any]:
    """A SOL short limit at 150 on binance, waiting for the price (placed 8 candles ago)."""
    start = MON + timedelta(hours=5)
    bars = walk(start, 66, 146.0, seed=13, path={58: 147.2, 65: 148.4}, vol=0.002)
    p = plan("SOL", Side.SHORT, 150.0, 155.0, [140.0], entry_type=EntryType.LIMIT, tf=15)
    created = start + timedelta(minutes=15 * 58 + 2)
    t = trade(15, p, TradeStatus.PENDING, created)
    pos = BrokerPosition(broker="binance", broker_symbol="SOL/USDT:USDT", side=Side.SHORT,
                         status=PositionStatus.PENDING, quantity=10.0, stop_loss=155.0,
                         take_profits=[TakeProfit(price=140.0)],
                         data={"entry_created_ms": int(created.timestamp() * 1000)})
    rec = tc.Records(placed=created + timedelta(seconds=2))
    return {"trade": t, "pos": pos, "rec": rec, "bars": bars, "account": "binance"}
