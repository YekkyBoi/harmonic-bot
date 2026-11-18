"""size_from_signal_entry (the friend's tickmill): a market order is sized like the trader sizes it - on the signal's own
entry-to-stop distance - never below the size from the live entry (never under 1R at the stop), and never losing more
than 1R + lot_rounding_max_over (10) at the stop from the live entry.

The live case: EURUSD LONG, signal entry 1.09706, stop 1.096 (10.6 pips; the trader: 0.94 lot per 100 USD -> 0.47 per
50). The bot filled at 1.09713 (11.3 pips): 50 / 113 = 0.4425 -> 0.44 (0.45 with lot_rounding "up"). Wanted: 0.47.
On the production replica (real Engine, MT5 adapter for fx, MetaApi for tickmill)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import test_preflight_scenarios as tps
from copytrader.config import AppConfig, PaperBrokerConfig
from copytrader.models import EntryType, Side, SizingResult
from copytrader.engine.orchestrator import max_rounded_risk
from test_preflight_scenarios import FX, Prod, _isolate, fx_pair  # noqa: F401  (_isolate: autouse)

ENTRY, STOP, TP = 1.09706, 1.09600, 1.09900


def _tickmill(cfg: AppConfig, *, on: bool = True) -> None:
    t = next(b for b in cfg.brokers if b.name == "tickmill")
    t.risk_usd = 50.0
    t.lot_rounding = "up"                     # type: ignore[assignment]
    t.fees_in_risk = False
    t.size_from_signal_entry = on
    cfg.notifier.notify_on_execution = True   # (the live config: every open is reported)


async def _env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, on: bool = True) -> Prod:
    return await Prod.create(tmp_path, monkeypatch, edit=lambda cfg: _tickmill(cfg, on=on))


async def _market_long(p: Prod, *, ask: float, entry: float | None = ENTRY) -> None:
    bid = round(ask - 0.00002, 5)
    p.terminal.set_price("EURUSD", bid, ask)
    p.tickmill.set_price("EURUSD", bid, ask)
    await p.say(fx_pair("EURUSD", Side.LONG, entry, STOP, TP, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")


def _tickmill_lots(p: Prod) -> float:
    (order,) = p.tickmill_orders()
    assert order[0] == "market_buy"
    return float(order[2])


async def test_the_live_case_opens_the_traders_lot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = await _env(tmp_path, monkeypatch)
    try:
        await _market_long(p, ask=1.09713)
        assert _tickmill_lots(p) == 0.47                                   # (today: 0.45)
        pos = (await p.positions(1))["tickmill"]
        assert pos.risk_amount == 50.0
        line = next(t for t in p.texts() if "tickmill: 0.47" in t)
        assert "lot sinyal girişine göre (0.47); stopta kayıp ~53.11" in line
    finally:
        await p.close()


async def test_without_the_setting_the_live_entry_sizes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = await _env(tmp_path, monkeypatch, on=False)
    try:
        await _market_long(p, ask=1.09713)
        assert _tickmill_lots(p) == 0.45                                   # 50 / 113 = 0.4425, rounded up
        assert not any("sinyal girişine göre" in t for t in p.texts())
    finally:
        await p.close()


async def test_a_fill_far_worse_is_capped_at_1r_plus_the_overshoot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = await _env(tmp_path, monkeypatch)
    try:
        await _market_long(p, ask=1.09728)                                 # 12.8 pips: 0.47 would lose 60.16
        lots = _tickmill_lots(p)
        assert lots == 0.46 and lots * 128.0 <= 60.0                       # the largest step within 50 + 10
        assert any("lot sinyal girişine göre 0.47 olurdu, 1R + 10 sınırıyla 0.46; stopta kayıp ~58.88" in t
                   for t in p.texts())
    finally:
        await p.close()


async def test_a_fill_better_than_the_signal_keeps_the_live_size(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = await _env(tmp_path, monkeypatch)
    try:
        await _market_long(p, ask=1.09700)                                 # 10 pips: 0.47 would lose 47 (< 1R)
        lots = _tickmill_lots(p)
        assert lots == 0.50 and lots * 100.0 >= 50.0                       # never below 1R
        assert not any("sinyal girişine göre" in t for t in p.texts())
    finally:
        await p.close()


async def test_a_market_signal_without_an_entry_price_sizes_as_today(tmp_path: Path,
                                                                    monkeypatch: pytest.MonkeyPatch) -> None:
    p = await _env(tmp_path, monkeypatch)
    try:
        await _market_long(p, ask=1.09713, entry=None)
        assert _tickmill_lots(p) == 0.45
        assert not any("sinyal girişine göre" in t for t in p.texts())
    finally:
        await p.close()


async def test_the_owners_fx_account_is_unchanged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    on = await _env(tmp_path / "a", monkeypatch)
    try:
        await _market_long(on, ask=1.09713)
        fx_on = on.fx_orders()
    finally:
        await on.close()
    off = await _env(tmp_path / "b", monkeypatch, on=False)
    try:
        await _market_long(off, ask=1.09713)
        assert off.fx_orders() == fx_on                                     # (only tickmill has the setting)
    finally:
        await off.close()


def test_config_default_off_and_the_open_risk_bound() -> None:
    assert PaperBrokerConfig(type="paper", name="p").size_from_signal_entry is False
    from types import SimpleNamespace
    b = SimpleNamespace(config=SimpleNamespace(lot_rounding="down", size_from_signal_entry=True,
                                               lot_rounding_max_over=10.0))
    assert max_rounded_risk(b, 50.0) == 60.0                                # type: ignore[arg-type]


async def test_a_refused_signal_entry_size_keeps_the_live_one() -> None:
    """The adapter refuses the larger size (margin...): the live entry's size stands."""
    from types import SimpleNamespace

    from copytrader.brokers.base import BrokerError
    from copytrader.engine.orchestrator import Engine

    calls: list[float] = []

    class Broker:
        name = "tickmill"
        config = SimpleNamespace(lot_rounding_max_over=10.0)

        async def size_position(self, sym: Any, side: Any, entry: float, stop: float, amount: float) -> SizingResult:
            calls.append(amount)
            if len(calls) == 2:
                raise BrokerError("Yetersiz teminat")
            loss = abs(entry - stop) * 100000
            q = round(amount / loss, 2)
            return SizingResult(quantity=q, risk_amount=amount, est_loss_at_sl=q * loss, est_fees_at_sl=0.0,
                                notional=0.0, leverage=None, margin_required=None, warnings=[])

    live = SizingResult(quantity=0.45, risk_amount=50.0, est_loss_at_sl=0.45 * 113, est_fees_at_sl=0.0,
                        notional=0.0, leverage=None, margin_required=None, warnings=[])
    sym = SimpleNamespace(qty_step=0.01)
    logged: list[tuple[str, dict[str, Any]]] = []

    async def log(kind: str, payload: dict[str, Any]) -> None:
        logged.append((kind, payload))

    size, note = await Engine._signal_entry_size(SimpleNamespace(_log=log), Broker(), sym, Side.LONG, live, ENTRY,
                                                 1.09713, STOP, 50.0)  # type: ignore[arg-type]
    assert size is live and note is None
    assert logged and logged[-1][0] == "signal_entry_size" and logged[-1][1]["used"] is False


@pytest.mark.parametrize("rounding", ["up", "down", "nearest"])
async def test_a_per_lot_loss_that_moves_a_little_between_calls_still_gives_the_signal_lot(rounding: str) -> None:
    """The adapter reads its conversion rate again on every call: a per-lot loss a little different on the last call
    must still land on the signal's lot, whatever the broker's own rounding."""
    from types import SimpleNamespace

    from copytrader.engine.orchestrator import Engine
    from copytrader.risk.sizing import compute_lot_size

    drift = [1.0, 1.0, 1.003]                       # (live, signal, the last call: +0.3 %)

    class Broker:
        name = "tickmill"
        config = SimpleNamespace(lot_rounding=rounding, lot_rounding_max_over=10.0)

        async def size_position(self, sym: Any, side: Any, entry: float, stop: float, amount: float) -> SizingResult:
            per_lot = abs(entry - stop) * 100000 * drift.pop(0)
            q = compute_lot_size(risk_amount=amount, loss_per_lot=per_lot, lot_step=0.01, min_lot=0.01,
                                 rounding=rounding, max_over=10.0)
            return SizingResult(quantity=q, risk_amount=amount, est_loss_at_sl=q * per_lot, est_fees_at_sl=0.0,
                                notional=0.0, leverage=None, margin_required=None, warnings=[])

    async def log(kind: str, payload: dict[str, Any]) -> None:
        return None

    broker = Broker()
    live = await broker.size_position(None, Side.LONG, 1.09713, STOP, 50.0)
    size, note = await Engine._signal_entry_size(SimpleNamespace(_log=log), broker, SimpleNamespace(qty_step=0.01),
                                                 Side.LONG, live, ENTRY, 1.09713, STOP, 50.0)  # type: ignore[arg-type]
    assert size.quantity == 0.47 and note is not None and "(0.47)" in note


async def test_a_waiting_entry_fired_by_fxs_fill_is_sized_from_the_signal_entry(tmp_path: Path,
                                                                               monkeypatch: pytest.MonkeyPatch) -> None:
    """fx-route limit, tickmill's entry waits for fx's fill (reference_broker): at the fire, the signal's entry
    shifted by the offset (as the stop) gives the trader's distance; the live tickmill price only bounds it."""
    import test_reference_broker as trb

    def edit(cfg: AppConfig) -> None:
        trb._reference(cfg)
        _tickmill(cfg)

    p, _friend = await trb._make(tmp_path, monkeypatch, reference=False, edit=edit)
    try:
        await trb._eur_limit(p)                       # EURUSD LONG limit 1.1610, stop 1.1590 (20 pips)
        # fx mid 1.16100, tickmill mid 1.16120 (+0.0002): signal levels 1.1612 / 1.1592; tickmill's ask 1.16130
        await trb._fill_fx(p, fx=(1.16095, 1.16105), tickmill=(1.16110, 1.16130))
        (order,) = p.tickmill_orders()
        assert order[0] == "market_buy" and order[2] == 0.25       # 50 / 200 (today: 50 / 210 -> 0.24)
        assert any("lot sinyal girişine göre (0.25); stopta kayıp ~52.50" in t for t in p.texts())
    finally:
        await p.close()
