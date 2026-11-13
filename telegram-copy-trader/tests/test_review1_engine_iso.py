"""Review 1 (lane engine_iso): the friend's OPTIONAL broker (tickmill, MetaApi) must never block, delay or crash
anything of the owner's brokers (binance, breakout, fx).

The production replica of tests/test_preflight_scenarios (REAL Engine, Monitor, CcxtBroker, ManualBroker,
Mt5Broker, MetaApiBroker on the fake venues), loaded with the server's config of today (PROD_NOW_YAML below:
notify_on_execution, drift "follow" on kripto, fx daily loss with open risk, max_open_trades 25 ...). The
tickmill connection can be told to leave its requests unanswered (a half-dead MetaApi link) until the test
releases them; then they are answered normally. Real wall clock, the shipped timing constants.

  I1  open flow: an optional leg is waited for OPTIONAL_LEG_GRACE_SEC only, then goes on as a late leg
  I2  management flow + the sync before it: the same for management; the monitor never holds a trade's lock
      while an optional broker's position sync is waiting for an answer
  I3  a binance resting limit that fills while its open flow still waits gets its stop at the next fast check
  I4  Engine.start() does not wait for an optional broker's connect
  I5  owner actions that would trade are refused (and stay pending) while the engine is starting
"""

from __future__ import annotations

import asyncio
import inspect
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

import test_preflight_scenarios as tps
from test_preflight_scenarios import CHAT, FX, KRIPTO, TITLES, TRADER, Prod, btc, fx_pair, set_eurusd, signal

from copytrader.brokers.ccxt_broker import CcxtBroker
from copytrader.brokers.manual_broker import ManualBroker
from copytrader.brokers.metaapi_broker import MetaApiBroker
from copytrader.brokers.mt5_broker import MT5, Mt5Broker
from copytrader.config import AppConfig, load_config
from copytrader.engine import orchestrator as orch
from copytrader.engine.orchestrator import Engine
from copytrader.engine.prices import PriceOracle
from copytrader.models import (
    ActionType,
    EntryType,
    MarketType,
    ParsedAction,
    ParseResult,
    PositionStatus,
    Severity,
    Side,
    StoredMessage,
    TradeStatus,
)
from copytrader.storage.db import Database
from copytrader.telegram import formatting as fmt

from test_mt5_broker import SERVER_TIME

# The owner's config.yaml as it is on the server now (secrets as ${VAR}; tps.DUMMY_ENV gives dummy values).
PROD_NOW_YAML = """\
dry_run: false
data_dir: data
timezone: Europe/Istanbul
telegram:
  api_id: ${TG_API_ID}
  api_hash: ${TG_API_HASH}
  source_chats: [-1003782294463]
  topics:
    - name: kripto
      title_contains: ["Kripto İşlemleri"]
      brokers: [binance, breakout, tickmill]
      kind: crypto
      explicit_market_drift: follow
    - name: fx
      title_contains: ["Fx İşlemleri"]
      brokers: [fx, tickmill]
      kind: fx
notifier:
  bot_token: ${TG_BOT_TOKEN}
  owner_user_id: ${TG_OWNER_ID}
  notify_on_execution: true
  notify_on_ignored: true
llm:
  api_key: ${ANTHROPIC_API_KEY}
brokers:
  - name: binance
    type: ccxt
    exchange: binanceusdm
    api_key: ${BINANCE_API_KEY}
    secret: ${BINANCE_API_SECRET}
    risk_usd: 10
    margin_mode: cross
    leverage: 125
    max_leverage: 125
    daily_loss_limit_pct: 0
    no_hedge: true
  - name: breakout
    type: manual
    risk_usd: 50
    allowed_assets: [BTC, ETH, SOL, XRP, DOGE, ADA, AVAX, LINK, BNB, LTC, SUI, TON, DOT, NEAR, APT, ARB, OP, PEPE, WIF, HYPE]
  - name: fx
    type: mt5
    login: ${MT5_LOGIN}
    password: ${MT5_PASSWORD}
    server: ${MT5_SERVER}
    risk_usd: 100
    no_hedge: true
    daily_loss_limit_pct: 3
    daily_loss_include_open_risk: true
  - name: tickmill
    type: metaapi
    enabled: true
    optional: true
    token: ${METAAPI_TOKEN:-}
    account_id: ${METAAPI_ACCOUNT_ID:-}
    risk_usd: 45
    reference_exchange: binanceusdm
    offset_mode: additive
    max_offset_pct: 1.0
    entry_trigger: reference_touch
    virtual_trigger_max_hours: 48
    symbol_map:
      BTC: BTCUSD
      ETH: ETHUSD
    allowed_assets: [EUR, GBP, USD, AUD, NZD, BTC, ETH, XAU, XAG]
    no_hedge: true
    daily_loss_limit_pct: 0
risk:
  max_open_trades: 25
  allow_min_qty_round_up: true
"""


def load_now_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AppConfig:
    for name, value in tps.DUMMY_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "config.yaml"
    path.write_text(PROD_NOW_YAML, encoding="utf-8")
    cfg = load_config(path)
    cfg.data_dir = str(tmp_path / "data")
    cfg.monitor_interval_sec = 3600.0
    return cfg


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Mt5Broker, "_now_epoch", staticmethod(lambda: SERVER_TIME - 7200.0))
    monkeypatch.setattr(Mt5Broker, "tick_wait_sec", 0.0)
    monkeypatch.setattr(fmt, "RISK_PER_R_NOTE", fmt.RISK_PER_R_NOTE)


# --------------------------------------------------------------------------- #
# Time line and fakes
# --------------------------------------------------------------------------- #
class Timeline:
    def __init__(self) -> None:
        self.t0 = time.monotonic()
        self.events: list[tuple[float, str, str]] = []

    def now(self) -> float:
        return time.monotonic() - self.t0

    def add(self, kind: str, text: str) -> float:
        t = self.now()
        self.events.append((t, kind, text))
        return t

    def first(self, kind: str, needle: str = "", after: float = -1.0) -> float | None:
        for t, k, text in self.events:
            if k == kind and needle in text and t > after:
                return t
        return None

    def dump(self) -> str:
        return "\n".join(f"{t:8.2f}s  {k:<14} {text}" for t, k, text in self.events)


_TRADE_CALLS = ("create_", "modify_position", "modify_order", "close_position", "cancel_order")
_NEVER_HANG = frozenset({"connect", "close", "wait_synchronized"})


class HangingTickmill(tps.Tickmill):
    """The friend's MetaApi RPC connection. ``hang_reads`` / ``hang_trades``: those requests are not answered
    until :meth:`release` (then they are answered normally, as a link that recovers would)."""

    def __init__(self) -> None:
        super().__init__()
        self.hang_reads = False
        self.hang_trades = False
        self.gate = asyncio.Event()
        self.timeline: Timeline | None = None
        self.opens_in_flight = 0
        self.max_opens_in_flight = 0
        self.hung: list[str] = []
        for name, _fn in inspect.getmembers(type(self), inspect.iscoroutinefunction):
            if not name.startswith("_") and name not in _NEVER_HANG:
                setattr(self, name, self._wrapped(name, getattr(self, name)))

    def release(self) -> None:
        self.hang_reads = self.hang_trades = False
        self.gate.set()

    def _wrapped(self, name: str, bound: Any) -> Any:
        trade = name.startswith(_TRADE_CALLS)

        async def call(*args: Any, **kwargs: Any) -> Any:
            hang = (self.hang_trades and trade) or (self.hang_reads and not trade)
            opening = name.startswith("create_")
            if opening:
                self.opens_in_flight += 1
                self.max_opens_in_flight = max(self.max_opens_in_flight, self.opens_in_flight)
            try:
                if hang:
                    self.hung.append(name)
                    if self.timeline is not None:
                        self.timeline.add("tickmill", f"{name}{args[:2]} -> no answer")
                    await self.gate.wait()
                if self.timeline is not None and trade:
                    self.timeline.add("tickmill", f"{name}{args[:2]}")
                return await bound(*args, **kwargs)
            finally:
                if opening:
                    self.opens_in_flight -= 1

        return call


class RegistryParser:
    """Reads each stored message as the action registered for its id (batches of the two topics may be parsed in
    any order)."""

    def __init__(self) -> None:
        self.actions: dict[int, ParsedAction] = {}

    async def parse(self, messages: list[StoredMessage], context: Any) -> ParseResult:
        return ParseResult(actions=[self.actions[m.message_id] for m in messages if m.message_id in self.actions],
                           model="fake")


async def make_replica(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, start: bool = True,
                       timeline: Timeline | None = None, interval: float | None = None) -> Prod:
    """Prod.create of the scenario tests, on the server's config of today and a HangingTickmill. ``interval``:
    the monitor's interval (production: 10 s; the replica's default 3600 s leaves only the 2 s fast checks)."""
    p = Prod()
    p.cfg = load_now_config(tmp_path, monkeypatch)
    if interval is not None:
        p.cfg.monitor_interval_sec = interval
    p.parser = RegistryParser()                                   # type: ignore[assignment]
    by = {b.name: b for b in p.cfg.brokers}
    p.terminal = tps.FxTerminal()
    p.tickmill = HangingTickmill()

    async def no_sleep(_seconds: float) -> None:
        return None

    binance = CcxtBroker(by["binance"], exchange=p.binance, sleep=no_sleep)
    breakout = ManualBroker(by["breakout"], p.cfg.data_dir, price_source=p.binance)
    fx = Mt5Broker(by["fx"], mt5_module=p.terminal)
    fx.retry_delay_sec = 0
    tickmill = MetaApiBroker(by["tickmill"], connection=p.tickmill)
    tickmill.retry_delay_sec = 0
    p.brokers = {"binance": binance, "breakout": breakout, "fx": fx, "tickmill": tickmill}
    p.db = Database(":memory:")
    await p.db.open()
    p.ref = tps.Reference(p.binance)
    holder: list[Engine] = []
    oracle = PriceOracle(lambda: holder[0].brokers, p.cfg.price_source_exchange, cache_ttl_sec=0.0,
                         public_exchange=p.binance)
    p.engine = Engine(p.cfg, p.db, p.parser, list(p.brokers.values()), p.notifier,  # type: ignore[arg-type]
                      price_oracle=oracle, reference_prices=p.ref)
    holder.append(p.engine)
    if timeline is not None:
        instrument(p, timeline)
    if start:
        await p.engine.start(monitor=False)
        assert p.engine.failed_brokers == {}
        p.notifier.notifications.clear()
        p.ref.calls.clear()
    return p


def instrument(p: Prod, tl: Timeline) -> None:
    """Stamp the owner's orders (binance, fx), every notice and tickmill's requests on the time line."""
    p.tickmill.timeline = tl                                      # type: ignore[attr-defined]
    original = p.binance.create_order

    async def create_order(symbol: str, type: str, side: str, amount: float, price: float | None = None,
                           params: dict | None = None) -> Any:
        params = dict(params or {})
        if params.get("stopLossPrice"):
            kind = "stop"
        elif params.get("takeProfitPrice"):
            kind = "tp"
        elif params.get("reduceOnly"):
            kind = "close"
        else:
            kind = "entry"
        tl.add("binance", f"{kind} {type} {side} {amount} {symbol}")
        return await original(symbol, type, side, amount, price, params)

    p.binance.create_order = create_order                         # type: ignore[method-assign]

    def fx_hook(request: dict[str, Any]) -> None:
        if request.get("action") == MT5.TRADE_ACTION_DEAL and "position" not in request:
            tl.add("fx", f"entry {request.get('symbol')} vol={request.get('volume')} sl={request.get('sl')}")
        elif request.get("action") == MT5.TRADE_ACTION_DEAL:
            tl.add("fx", f"close {request.get('symbol')} vol={request.get('volume')}")
        elif request.get("action") == MT5.TRADE_ACTION_SLTP:
            tl.add("fx", f"sltp {request.get('symbol')} sl={request.get('sl')}")
        else:
            tl.add("fx", f"request {request.get('action')} {request.get('symbol')}")
        return None

    p.terminal.hooks.append(fx_hook)
    original_notify = p.notifier.notify

    async def notify(text: str, severity: Severity = Severity.INFO) -> None:
        tl.add("notice", text.replace("\n", " | ")[:300])
        await original_notify(text, severity)

    p.notifier.notify = notify                                    # type: ignore[method-assign]


async def post(p: Prod, action: ParsedAction, topic: int, text: str, *, reply_to: int | None = None,
               tl: Timeline | None = None, label: str = "") -> int:
    """The trader posts ``text`` in ``topic``; the parser reads it as ``action``. Returns when the batch is done."""
    p._mid += 1
    mid = p._mid
    p.parser.actions[mid] = action.model_copy(update={"used_message_ids": [mid]})   # type: ignore[attr-defined]
    if tl is not None:
        tl.add("post", f"{label or mid}: {text}")
    await p.db.save_message(StoredMessage(
        chat_id=CHAT, message_id=mid, sender_id=TRADER, sender_name="Hoca", date=datetime.now(UTC), text=text,
        reply_to_message_id=reply_to if reply_to is not None else topic, topic_id=topic, topic_title=TITLES[topic]))
    await p.engine.on_messages_ready(CHAT, TRADER, topic)
    if tl is not None:
        tl.add("batch-done", f"{label or mid}")
    return mid


def eth_market() -> ParsedAction:
    return signal("ETHUSDT.P", "ETH", "USDT", MarketType.PERP, Side.LONG, 2232.0, 2200.0, 2300.0,
                  entry_type=EntryType.MARKET)


def btc_market() -> ParsedAction:
    return btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0)


def eur_short() -> ParsedAction:
    return fx_pair("EURUSD", Side.SHORT, 1.10872, 1.11005, 1.10654)


async def settle(p: Prod, seconds: float = 10.0) -> None:
    """Wait until no order flow (late legs included) is running any more."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and any(not t.done() for t in p.engine._critical):
        await asyncio.sleep(0.05)


# =========================================================================== #
# I1: open flow
# =========================================================================== #
@pytest.mark.parametrize("hang", ["trade_requests", "reads"])
async def test_i1_hanging_tickmill_never_delays_the_owners_orders(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                                  hang: str) -> None:
    """S1 kripto BTC market, S2 fx EURUSD short 1.5 s later, S3 kripto ETH market 1.5 s after that, while tickmill
    leaves its requests unanswered. Before the fix S2's fx order went out ~30 s and S3's binance order ~57 s after
    their signals (the open lock was held for tickmill's 30 s leg deadline, then 27 s of busy wait)."""
    tl = Timeline()
    p = await make_replica(tmp_path, monkeypatch, timeline=tl)
    try:
        set_eurusd(p, 1.10872, 1.10874)
        tm: HangingTickmill = p.tickmill                          # type: ignore[assignment]
        if hang == "reads":
            tm.hang_reads = True
        else:
            tm.hang_trades = True
        tl.t0 = time.monotonic()
        s1 = asyncio.ensure_future(post(p, btc_market(), KRIPTO, "btc 1R long piyasadan", tl=tl, label="S1"))
        await asyncio.sleep(1.5)
        s2 = asyncio.ensure_future(post(p, eur_short(), FX, "Eu short", tl=tl, label="S2"))
        await asyncio.sleep(1.5)
        s3 = asyncio.ensure_future(post(p, eth_market(), KRIPTO, "eth 1R long piyasadan", tl=tl, label="S3"))
        await asyncio.wait({s1, s2, s3}, timeout=20)
        assert all(t.done() for t in (s1, s2, s3)), tl.dump()
        flows_done = tl.now()
        tl.add("test", "tickmill answers again")
        tm.release()
        await settle(p, 40.0)
        texts = p.texts()
    finally:
        p.tickmill.release()                                      # type: ignore[attr-defined]
        await p.close()
    print("\n" + tl.dump())
    post1, post2, post3 = (tl.first("post", f"S{i}:") for i in (1, 2, 3))
    btc_entry = tl.first("binance", "entry market buy", after=-1)
    fx_entry = tl.first("fx", "entry EURUSD")
    eth_entry = tl.first("binance", "ETH/USDT:USDT")
    assert None not in (post1, post2, post3, btc_entry, fx_entry, eth_entry), tl.dump()
    assert btc_entry - post1 < 1.0, tl.dump()                    # type: ignore[operator]
    assert fx_entry - post2 < 3.5, f"fx order {fx_entry - post2:.1f}s after its signal\n{tl.dump()}"   # type: ignore[operator]
    # S3 waits for nothing of tickmill: its own tickmill leg is behind S1's unanswered order (late at once)
    assert eth_entry - post3 < 1.5, f"binance ETH order {eth_entry - post3:.1f}s after its signal\n{tl.dump()}"   # type: ignore[operator]
    assert flows_done < 6.0, tl.dump()
    # tickmill never had two opens in flight, and its late results were reported once it answered
    assert tm.max_opens_in_flight <= 1
    late = [t for t in texts if "tickmill geç yanıt verdi" in t]
    assert any("#1 BTC LONG" in t for t in late), texts
    waiting = [t for t in texts if "tickmill: yanıt bekleniyor; sonuç ayrıca bildirilecek" in t]
    assert any("#1" in t for t in waiting), texts
    # the owner's reports came at once, not after tickmill's answer
    first_report = tl.first("notice", "#1 BTC LONG açıldı")
    assert first_report is not None and first_report - post1 < 4.5, tl.dump()   # type: ignore[operator]


async def test_i1_healthy_tickmill_is_in_the_single_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tl = Timeline()
    p = await make_replica(tmp_path, monkeypatch, timeline=tl)
    try:
        await post(p, btc_market(), KRIPTO, "btc 1R long piyasadan", tl=tl, label="S1")
        texts = p.texts()
        (trade,) = await p.trades()
        positions = await p.positions(trade.id)
    finally:
        p.tickmill.release()                                      # type: ignore[attr-defined]
        await p.close()
    assert set(positions) == {"binance", "breakout", "tickmill"}
    assert trade.status is TradeStatus.OPEN
    reports = [t for t in texts if "#1 BTC LONG açıldı" in t]
    assert len(reports) == 1 and reports[0].startswith("✅") and "tickmill: " in reports[0], texts
    assert not any("geç yanıt" in t or "yanıt bekleniyor" in t for t in texts), texts


async def test_i1_route_of_only_optional_brokers_keeps_the_leg_deadline(tmp_path: Path,
                                                                        monkeypatch: pytest.MonkeyPatch) -> None:
    """A route whose brokers are all optional: its open waits LEG_DEADLINE_SEC for them, as before."""
    tasks: dict[str, Any] = {}
    seen: dict[str, float] = {}

    async def slow() -> str:
        await asyncio.sleep(0.6)
        return "x"

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(orch, "OPTIONAL_LEG_GRACE_SEC", 0.2)
    tasks["tickmill"] = asyncio.ensure_future(slow())
    started = loop.time()
    await Engine._wait_legs(tasks, {"tickmill"}, started, required_sec=orch.LEG_DEADLINE_SEC,
                            all_optional_sec=orch.LEG_DEADLINE_SEC)
    seen["all_optional"] = loop.time() - started
    assert tasks["tickmill"].done() and seen["all_optional"] >= 0.55
    # with a required leg next to it, the optional one is given the grace only
    tasks = {"binance": asyncio.ensure_future(asyncio.sleep(0.01)), "tickmill": asyncio.ensure_future(slow())}
    started = loop.time()
    await Engine._wait_legs(tasks, {"tickmill"}, started, required_sec=orch.LEG_DEADLINE_SEC,
                            all_optional_sec=orch.LEG_DEADLINE_SEC)
    took = loop.time() - started
    assert tasks["binance"].done() and not tasks["tickmill"].done() and 0.15 <= took < 0.45
    await tasks["tickmill"]


# =========================================================================== #
# I2: management
# =========================================================================== #
def manage(kind: ActionType, trade_id: int, base: str = "BTC", **kw: Any) -> ParsedAction:
    data: dict[str, Any] = dict(action=kind, base_asset=base, target_trade_id=trade_id, confidence=0.95,
                                summary_tr=kw.pop("summary", kind.value))
    data.update(kw)
    return ParsedAction(**data)


async def until(pred: Any, seconds: float, what: str, tl: Timeline | None = None) -> None:
    deadline = time.monotonic() + seconds
    while not pred():
        assert time.monotonic() < deadline, f"not within {seconds:g}s: {what}\n{tl.dump() if tl else ''}"
        await asyncio.sleep(0.02)


@pytest.mark.parametrize("monitor_pass", [True, False], ids=["monitor_pass_hangs_on_tickmill", "no_monitor_pass"])
async def test_i2_trader_closes_while_tickmill_hangs_binance_closes_at_once_and_the_next_signal_is_not_held(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, monitor_pass: bool) -> None:
    """The BTC market long is open on binance, breakout and tickmill; the friend's MetaApi stops answering (reads and
    trade requests). The trader replies "kapattık" and posts "eth 1R long piyasadan" one second later.
    Before the fix (production timeouts): the binance close went out 30 s (60 s with a monitor pass hanging on
    tickmill) after "kapattık" and the ETH entry 60-90 s after it."""
    tl = Timeline()
    p = await make_replica(tmp_path, monkeypatch, timeline=tl, interval=10.0)
    tm: HangingTickmill = p.tickmill                              # type: ignore[assignment]
    try:
        signal_id = await post(p, btc_market(), KRIPTO, "btc 1R long piyasadan", tl=tl, label="S1")
        (trade,) = await p.trades()
        assert set(await p.positions(trade.id)) == {"binance", "breakout", "tickmill"}
        tm.hang_reads = tm.hang_trades = True
        if monitor_pass:
            hung = len(tm.hung)
            p.engine._start_monitor()                             # its first pass syncs every position at once
            await until(lambda: len(tm.hung) > hung, 5.0, "the monitor's tickmill sync hangs", tl)
            await asyncio.sleep(0.3)
        tl.t0 = time.monotonic()
        close = asyncio.ensure_future(post(p, manage(ActionType.CLOSE, trade.id, close_fraction=1.0), KRIPTO,
                                           "kapattık", reply_to=signal_id, tl=tl, label="CLOSE"))
        await asyncio.sleep(1.0)
        eth = asyncio.ensure_future(post(p, eth_market(), KRIPTO, "eth 1R long piyasadan", tl=tl, label="ETH"))
        await until(lambda: close.done() and eth.done(), 15.0, "both batches", tl)
        t_close = tl.first("binance", "close market sell")
        t_eth = tl.first("binance", "entry market buy 0.291 ETH")
        assert t_close is not None and t_close < 3.0, f"binance close at {t_close}\n{tl.dump()}"
        assert t_eth is not None and t_eth - 1.0 < 3.0, f"binance ETH entry at {t_eth}\n{tl.dump()}"
        report = [t for t in p.texts() if "#1 BTC LONG" in t and "yanıt bekleniyor" in t]
        assert report and "binance: tamamı kapatıldı" in report[0], p.texts()
        # tickmill answers again: its close goes out and is reported, the trade is closed
        tl.add("test", "tickmill answers again")
        tm.release()
        await until(lambda: any("— tickmill geç yanıt verdi" in t and "tamamı kapatıldı" in t for t in p.texts()),
                    40.0, "tickmill's late close report", tl)
        await settle(p, 10.0)
        closed = next(t for t in await p.trades() if t.id == trade.id)
        positions = await p.positions(trade.id)
    finally:
        p.tickmill.release()                                      # type: ignore[attr-defined]
        await p.close()
    print("\n" + tl.dump())
    assert closed.status is TradeStatus.CLOSED, closed
    assert {b: pos.status for b, pos in positions.items()} == {
        "binance": PositionStatus.CLOSED, "breakout": PositionStatus.CLOSED, "tickmill": PositionStatus.CLOSED}


async def test_i2_fx_stop_to_entry_while_tickmill_hangs_reaches_the_prop_account_at_once(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The EURUSD short is open on fx and tickmill; tickmill stops answering; the trader writes "1R kâr var entry
    stop attık". Before the fix the fx stop move went out 30 s later and the fx topic was held for 90 s."""
    tl = Timeline()
    p = await make_replica(tmp_path, monkeypatch, timeline=tl)
    tm: HangingTickmill = p.tickmill                              # type: ignore[assignment]
    try:
        set_eurusd(p, 1.10872, 1.10874)
        signal_id = await post(p, eur_short(), FX, "Eu short", tl=tl, label="S1")
        (trade,) = await p.trades()
        assert set(await p.positions(trade.id)) == {"fx", "tickmill"}
        set_eurusd(p, 1.10740, 1.10742)                           # 1R in profit: the stop may go to the entry
        tm.hang_reads = tm.hang_trades = True
        tl.t0 = time.monotonic()
        started = tl.now()
        await asyncio.wait_for(post(p, manage(ActionType.MOVE_SL, trade.id, "EUR", move_sl_to_entry=True), FX,
                                    "1R kâr var entry stop attık", reply_to=signal_id, tl=tl, label="BE"), 10.0)
        took = tl.now() - started
        t_sltp = tl.first("fx", "sltp EURUSD")
        assert t_sltp is not None and t_sltp < 3.0, tl.dump()
        assert took < 4.0, f"the fx topic was held {took:.1f}s\n{tl.dump()}"
        tm.release()
        await until(lambda: any("— tickmill geç yanıt verdi" in t and "stop" in t for t in p.texts()), 40.0,
                    "tickmill's late stop report", tl)
        await settle(p, 10.0)
        after = next(t for t in await p.trades() if t.id == trade.id)
        positions = await p.positions(trade.id)
    finally:
        p.tickmill.release()                                      # type: ignore[attr-defined]
        await p.close()
    print("\n" + tl.dump())
    assert after.current_stop_loss == pytest.approx(1.10872)
    assert positions["fx"].stop_loss == pytest.approx(positions["fx"].avg_entry_price)
    assert positions["tickmill"].stop_loss == pytest.approx(positions["tickmill"].avg_entry_price)


async def test_i2_healthy_tickmill_management_is_one_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tl = Timeline()
    p = await make_replica(tmp_path, monkeypatch, timeline=tl, interval=10.0)
    try:
        p.engine._start_monitor()
        signal_id = await post(p, btc_market(), KRIPTO, "btc 1R long piyasadan", tl=tl, label="S1")
        (trade,) = await p.trades()
        await asyncio.sleep(0.3)
        p.notifier.notifications.clear()
        await post(p, manage(ActionType.CLOSE, trade.id, close_fraction=1.0), KRIPTO, "kapattık", reply_to=signal_id,
                   tl=tl, label="CLOSE")
        texts = p.texts()
        after = next(t for t in await p.trades() if t.id == trade.id)
    finally:
        p.tickmill.release()                                      # type: ignore[attr-defined]
        await p.close()
    reports = [t for t in texts if "#1 BTC LONG" in t and "uygulandı" in t]
    assert len(reports) == 1 and reports[0].startswith("✅"), texts
    assert "tickmill: tamamı kapatıldı" in reports[0] and "binance: tamamı kapatıldı" in reports[0]
    assert not any("geç yanıt" in t or "yanıt bekleniyor" in t for t in texts), texts
    assert after.status is TradeStatus.CLOSED


# =========================================================================== #
# I3: a resting binance limit that fills while its open flow still waits
# =========================================================================== #
@pytest.mark.parametrize("slow", ["tickmill_reads_hang", "breakout_leg_takes_8s"])
async def test_i3_binance_limit_filling_while_the_flow_waits_gets_its_stop_at_the_next_fast_check(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, slow: str) -> None:
    """A kripto BTC LIMIT next to the market: binance's limit rests (its stop is placed when the fast check sees
    the fill), breakout gets its instruction, tickmill registers a virtual trigger. One leg is slow; the limit
    fills 1 s after it was placed. Before the fix the open flow kept the trade lock and the fast check skipped the
    trade: no stop for ~31 s (tickmill hanging) / until the slow leg answered."""
    tl = Timeline()
    p = await make_replica(tmp_path, monkeypatch, timeline=tl)
    tm: HangingTickmill = p.tickmill                              # type: ignore[assignment]
    try:
        if slow == "tickmill_reads_hang":
            tm.hang_reads = True
        else:
            breakout = p.brokers["breakout"]
            original = breakout.open_position

            async def slow_open(request: Any) -> Any:
                await asyncio.sleep(8.0)
                return await original(request)

            breakout.open_position = slow_open
        p.engine._start_monitor()
        tl.t0 = time.monotonic()
        sig = asyncio.ensure_future(post(p, btc(EntryType.LIMIT, 73990.0, 73300.0, 75500.0), KRIPTO,
                                         "btc 1R long limit 73990", tl=tl, label="S1"))
        await until(lambda: tl.first("binance", "entry limit") is not None, 5.0, "the binance limit", tl)
        await asyncio.sleep(1.0)
        oid = p.binance.created()[0]["id"]
        p.binance.set_price("BTC", 73989.0, 73991.0)
        p.binance.fill(oid)
        filled = tl.add("test", "the binance limit fills")
        await until(lambda: tl.first("binance", "stop ") is not None, 15.0, "the stop at binance", tl)
        stop_at = tl.first("binance", "stop ")
        assert stop_at is not None and stop_at - filled < 3.0, f"no stop for {stop_at - filled:.1f}s\n{tl.dump()}"   # type: ignore[operator]
        await until(lambda: sig.done(), 20.0, "the open flow", tl)
        tm.release()
        await settle(p, 10.0)
        (trade,) = await p.trades()
        positions = await p.positions(trade.id)
        texts = p.texts()
    finally:
        p.tickmill.release()                                      # type: ignore[attr-defined]
        await p.close()
    print("\n" + tl.dump())
    assert positions["binance"].status is PositionStatus.OPEN and positions["binance"].stop_loss == pytest.approx(73300.0)
    assert trade.status is TradeStatus.OPEN
    (report,) = [t for t in texts if "#1 BTC LONG" in t and ("açıldı" in t or "yerleştirildi" in t)]
    assert "→ şimdi açık" in report, report                       # the owner is told the limit has filled meanwhile


# =========================================================================== #
# I4: start-up does not wait for the optional broker's connect
# =========================================================================== #
class HeldListener:
    """Stand-in for the Telegram listener with its start-up contract (hold_batches / release_batches, as in
    review/iletisim/test_exp6): the trader's message is stored 1 s after the start, its batch fires after the
    debounce but never before release_batches()."""

    stamps: dict[str, float] = {}
    t0 = 0.0

    def __init__(self, config: Any, db: Any, data_dir: Any, on_ready: Any, on_edit: Any) -> None:
        self.db, self.on_ready = db, on_ready
        self.ready = asyncio.Event()
        self.ready.set()
        self.stopped = asyncio.Event()
        self.tasks: list[asyncio.Task[None]] = []
        self.debounce = config.debounce_sec

    def set_notifier(self, notifier: Any) -> None:
        pass

    def hold_batches(self) -> None:
        self.ready.clear()

    def release_batches(self) -> None:
        HeldListener.stamps.setdefault("released", time.monotonic() - HeldListener.t0)
        self.ready.set()

    async def start(self) -> None:
        self.tasks.append(asyncio.ensure_future(self._trader_posts()))

    async def _trader_posts(self) -> None:
        await asyncio.sleep(1.0)
        HeldListener.stamps["signal_written"] = time.monotonic() - HeldListener.t0
        await self.db.save_message(StoredMessage(
            chat_id=CHAT, message_id=9001, sender_id=TRADER, sender_name="Hoca", date=datetime.now(UTC),
            text="btc 1R long piyasadan, SL 73300, TP 75500", reply_to_message_id=KRIPTO, topic_id=KRIPTO,
            topic_title=TITLES[KRIPTO]))
        await asyncio.sleep(self.debounce)
        await self.ready.wait()                       # the real listener: _fire_later -> await self._ready.wait()
        HeldListener.stamps["batch"] = time.monotonic() - HeldListener.t0
        await self.on_ready(CHAT, TRADER, KRIPTO)

    async def run_until_disconnected(self) -> None:
        await self.stopped.wait()

    async def stop(self) -> None:
        self.stopped.set()
        for t in self.tasks:
            t.cancel()


async def test_i4_restart_with_a_slow_tickmill_connect_does_not_hold_the_owners_signal(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The real copytrader.app.serve() on the replica; tickmill's MetaApi connect takes 6 s (production allows
    180 s). A kripto BTC market signal is written 1 s after the start. Before the fix the batch was held until
    tickmill's connect had ended (review/iletisim/test_exp6: binance order 24 s after the signal)."""
    from copytrader import app as appmod

    p = await make_replica(tmp_path, monkeypatch, start=False)
    p.cfg.telegram.debounce_sec = 0.5
    await p.db.close()
    tl = Timeline()
    instrument(p, tl)
    original_info = p.tickmill.get_account_information
    slow = {"left": 1}

    async def slow_connect_info(*a: Any, **k: Any) -> Any:      # MetaApi's connect chain takes its time
        if slow["left"]:
            slow["left"] -= 1
            await asyncio.sleep(6.0)
        return await original_info(*a, **k)

    p.tickmill.get_account_information = slow_connect_info      # type: ignore[method-assign]
    p.parser.actions[9001] = btc_market().model_copy(update={"used_message_ids": [9001]})   # type: ignore[attr-defined]
    db = Database(":memory:")
    application = appmod.build_app(p.cfg, db=db, parser=p.parser, notifier=p.notifier,  # type: ignore[arg-type]
                                   broker_factory=lambda cfg: p.brokers[cfg.name], listener_factory=HeldListener)
    stop = asyncio.Event()
    HeldListener.stamps = {}
    HeldListener.t0 = tl.t0 = time.monotonic()
    serving = asyncio.ensure_future(appmod.serve(application, stop))
    try:
        await until(lambda: tl.first("binance", "entry market buy") is not None, 15.0, "the binance order", tl)
        order = tl.first("binance", "entry market buy")
        written = HeldListener.stamps["signal_written"]
        assert order is not None and order - written < p.cfg.telegram.debounce_sec + 1.0, \
            f"binance order {order - written:.1f}s after the signal\n{tl.dump()}"   # type: ignore[operator]
        await until(lambda: any("tickmill</b> bağlandı (bot başladıktan sonra)" in t for t in p.texts()), 15.0,
                    "the notice that tickmill is connected", tl)
        connected = [b.name for b in application.engine.brokers]
        trades = sorted(await db.recent_trades(datetime(2000, 1, 1, tzinfo=UTC)), key=lambda t: t.id)
        positions = {b.broker for b in await db.broker_positions(trades[0].id)}
        texts = p.texts()
    finally:
        stop.set()
        await asyncio.wait({serving}, timeout=30)
    print("\n" + tl.dump())
    assert connected == ["binance", "breakout", "fx", "tickmill"]
    assert positions == {"binance", "breakout"}                  # tickmill was not connected yet: skipped
    assert any("tickmill bağlı değil, bu işlem atlandı" in t for t in texts), texts
    (startup,) = [t for t in texts if "Kopya-trade motoru başladı" in t]
    assert "Brokerlar: binance, breakout, fx" in startup and "⏳ Hâlâ bağlanıyor: tickmill" in startup, startup
    assert sum("tickmill</b> bağlandı" in t for t in texts) == 1, texts


async def test_i4_optional_broker_that_connects_at_once_is_in_the_single_startup_message(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = await make_replica(tmp_path, monkeypatch, start=False)
    try:
        await p.engine.start(monitor=False)
        texts = p.texts()
        connected = [b.name for b in p.engine.brokers]
    finally:
        await p.close()
    assert connected == ["binance", "breakout", "fx", "tickmill"]
    (startup,) = [t for t in texts if "Kopya-trade motoru başladı" in t]
    assert "Brokerlar: binance, breakout, fx, tickmill" in startup and "bağlanıyor" not in startup.lower()
    assert not any("bağlandı" in t for t in texts), texts


# =========================================================================== #
# I5: owner actions while the engine is starting
# =========================================================================== #
STARTING = "⏳ Bot başlıyor; birkaç saniye sonra tekrar deneyin."


def test_i5_starting_text_is_plain_turkish() -> None:
    assert orch.STARTING_TEXT == STARTING


async def test_i5_approval_and_other_owner_actions_wait_until_the_owners_brokers_are_connected(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A question was pending before a restart; the owner taps "Onayla" while the restarted engine is still
    connecting binance. Before the fix the approval ran at once on the brokers that were not connected yet and
    the question was used up (review/acilis/test_h, review/iletisim/test_exp5)."""
    p = await make_replica(tmp_path, monkeypatch)
    await post(p, btc_market().model_copy(update={"confidence": 0.3}), KRIPTO, "btc long belki")
    (conf,) = await p.db.pending_confirmations()
    await p.engine.stop(grace_sec=1.0)                            # the bot stops; the question stays pending
    # the restarted bot: new adapters on the same venues and database; binance's connect takes 2 s
    by = {b.name: b for b in p.cfg.brokers}

    async def no_sleep(_seconds: float) -> None:
        return None

    binance = CcxtBroker(by["binance"], exchange=p.binance, sleep=no_sleep)
    original_connect = binance.connect

    async def slow_connect() -> None:
        await asyncio.sleep(2.0)
        await original_connect()

    binance.connect = slow_connect                                # type: ignore[method-assign]
    fx = Mt5Broker(by["fx"], mt5_module=p.terminal)
    fx.retry_delay_sec = 0
    tickmill = MetaApiBroker(by["tickmill"], connection=p.tickmill)
    tickmill.retry_delay_sec = 0
    adapters = [binance, ManualBroker(by["breakout"], p.cfg.data_dir, price_source=p.binance), fx, tickmill]
    holder: list[Engine] = []
    oracle = PriceOracle(lambda: holder[0].brokers, p.cfg.price_source_exchange, cache_ttl_sec=0.0,
                         public_exchange=p.binance)
    engine = Engine(p.cfg, p.db, p.parser, adapters, p.notifier, price_oracle=oracle,  # type: ignore[arg-type]
                    reference_prices=p.ref)
    holder.append(engine)
    p.engine = engine
    try:
        starting = asyncio.ensure_future(engine.start(monitor=False))
        await asyncio.sleep(0.3)
        tapped = await engine.handle_confirmation(conf.id, True)
        still = await p.db.get_confirmation(conf.id)
        assert tapped == STARTING, (tapped, still.status if still else None, await p.trades())
        assert await engine.request_correction(conf.id) == STARTING
        assert await engine.handle_correction(conf.id, "btc long 74000 stop 73300") == STARTING
        assert await engine.close_all() == STARTING
        still = await p.db.get_confirmation(conf.id)
        assert still is not None and still.status.value == "pending" and not still.awaiting_correction
        assert p.binance.entry_orders() == [] and await p.trades() == []
        await starting
        result = await engine.handle_confirmation(conf.id, True)  # once started: approved and opened normally
        approved = await p.db.get_confirmation(conf.id)
        (trade,) = await p.trades()
        positions = await p.positions(trade.id)
    finally:
        await p.close()
    assert "#1 BTC LONG açıldı" in result, result
    assert approved is not None and approved.status.value == "approved"
    assert {b: pos.status for b, pos in positions.items()} == {
        "binance": PositionStatus.OPEN, "breakout": PositionStatus.OPEN, "tickmill": PositionStatus.OPEN}


async def test_i3_the_flow_waits_for_a_running_fast_check_at_most_its_bound(tmp_path: Path,
                                                                           monkeypatch: pytest.MonkeyPatch) -> None:
    """unlend(): a fast check that does not end (the exchange hangs) is cancelled after the bound, so that nothing
    writes the trade's positions without its lock once the open flow goes on."""
    p = await make_replica(tmp_path, monkeypatch)
    try:
        monitor = p.engine.monitor
        monitor.lend(1)
        hanging = asyncio.ensure_future(asyncio.Event().wait())
        monitor._protecting[1] = hanging
        started = time.monotonic()
        ran = await monitor.unlend(1, timeout=0.2)
        took = time.monotonic() - started
    finally:
        await p.close()
    assert 0.15 <= took < 2.0 and hanging.cancelled() and ran is False
    assert 1 not in monitor._lent
