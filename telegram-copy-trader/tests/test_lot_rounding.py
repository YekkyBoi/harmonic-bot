"""lot_rounding: per broker, "down" (default: never more than the target risk) or "nearest" (the friend's tickmill:
0.125 -> 0.13), never more than LOT_ROUNDING_MAX_OVERSHOOT x the target at the stop."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from copytrader.config import Mt5BridgeBrokerConfig, Mt5BrokerConfig
from copytrader.engine.orchestrator import rounding_headroom
from copytrader.risk import guards
from copytrader.risk.sizing import (LOT_ROUNDING_MAX_OVERSHOOT, SizingError, compute_linear_size, compute_lot_size,
                                    round_quantity)
from copytrader.models import BrokerPosition, PositionStatus, Side

from test_mt5_broker import _server_clock, FakeMT5, connected, make_cfg, make_symbol  # noqa: F401 (autouse fixture)


@pytest.mark.parametrize(("raw", "mode", "want"), [
    (0.125, "nearest", 0.13), (0.1249, "nearest", 0.12), (0.125, "down", 0.12), (0.129, "down", 0.12),
    (0.1125, "nearest", 0.11), (0.1175, "nearest", 0.12), (0.12, "nearest", 0.12), (0.125, "bogus", 0.12),
])
def test_round_quantity(raw: float, mode: str, want: float) -> None:
    assert round_quantity(raw, 0.01, mode, risk_amount=raw * 10, loss_per_unit=10.0) == want


def test_nearest_falls_back_to_down_above_the_overshoot_cap() -> None:
    # step 0.1: 0.86 -> 0.9 is 4.7 % over the target (allowed); 0.051 -> 0.1 would be 96 % over: rounded down
    assert round_quantity(0.86, 0.1, "nearest", risk_amount=86.0, loss_per_unit=100.0) == 0.9
    assert round_quantity(0.051, 0.1, "nearest", risk_amount=5.1, loss_per_unit=100.0) == 0.0
    cap = LOT_ROUNDING_MAX_OVERSHOOT
    assert cap == 1.15
    # exactly at the cap is allowed, a hair above is not
    assert round_quantity(1.0 / cap * 0.1 + 1e-12, 0.1, "nearest", risk_amount=1.0, loss_per_unit=1.0 / (0.1 / cap)) == 0.1
    assert round_quantity(0.0869, 0.1, "nearest", risk_amount=0.0869, loss_per_unit=1.0) == 0.0  # 0.1/0.0869 = 1.151
    assert round_quantity(0.125, 0.01, "nearest") == 0.12          # without the figures: down


def test_both_sizing_functions_round_the_same_way() -> None:
    lots = compute_lot_size(risk_amount=50.0, loss_per_lot=400.0, lot_step=0.01, min_lot=0.01, rounding="nearest")
    assert lots == 0.13                                             # 50 / 400 = 0.125
    assert compute_lot_size(risk_amount=50.0, loss_per_lot=400.0, lot_step=0.01, min_lot=0.01) == 0.12
    linear = compute_linear_size(risk_amount=50.0, entry=100.0, stop=96.0, qty_step=0.01, min_qty=0.01,
                                 contract_size=100.0, rounding="nearest")
    assert linear.quantity == 0.13 and linear.est_loss_at_sl == pytest.approx(52.0)
    with pytest.raises(SizingError):                                # below the minimum lot: the old rules stand
        compute_lot_size(risk_amount=1.0, loss_per_lot=400.0, lot_step=0.01, min_lot=0.01, rounding="nearest")


def test_defaults_are_down_for_every_broker_type() -> None:
    assert Mt5BrokerConfig(type="mt5", name="fx", login=1, password="p", server="s").lot_rounding == "down"
    assert Mt5BridgeBrokerConfig(type="mt5_bridge", name="t", terminal_path=r"C:\T\terminal64.exe",
                                 broker_company="Tickmill").lot_rounding == "down"


async def test_metatrader_adapter_uses_the_brokers_rounding_and_says_so() -> None:
    fake = FakeMT5()
    fake.symbols["BTCUSD"] = make_symbol("BTCUSD", trade_contract_size=1.0)
    nearest = await connected(fake, make_cfg(lot_rounding="nearest"))
    down = await connected(FakeMT5(), make_cfg())
    try:
        sym = await nearest.resolve_symbol("ETH", "USDT")
        # loss per lot 400 (entry 2232.95, stop 1832.95): 50 USD -> 0.125 lots
        n = await nearest.size_position(sym, Side.LONG, 2232.95, 1832.95, 50.0)
        assert n.quantity == 0.13 and n.est_loss_at_sl == pytest.approx(52.0)
        assert n.warnings == ["Lot en yakın adıma yukarı yuvarlandı (0.125 -> 0.13); stopta kayıp ~52.00 (hedef 50.00)."]
        d = await down.size_position(sym, Side.LONG, 2232.95, 1832.95, 50.0)
        assert d.quantity == 0.12 and d.warnings == []              # the owner's accounts: as before
        # 6.5 USD -> 0.01625 lots: the nearest step 0.02 would lose 8.00 = 23 % over the target: down, 0.01
        small = await nearest.size_position(sym, Side.LONG, 2232.95, 1832.95, 6.5)
        assert small.quantity == 0.01 and small.warnings == []
        # 7 USD -> 0.0175: 0.02 loses 8.00 = 14.3 % over: still within the cap, rounded up
        assert (await nearest.size_position(sym, Side.LONG, 2232.95, 1832.95, 7.0)).quantity == 0.02
    finally:
        await nearest.close()
        await down.close()


def test_open_risk_counts_what_a_rounded_up_size_really_loses() -> None:
    broker = SimpleNamespace(config=SimpleNamespace(lot_rounding="nearest"))
    assert rounding_headroom(broker) == LOT_ROUNDING_MAX_OVERSHOOT                         # type: ignore[arg-type]
    assert rounding_headroom(SimpleNamespace(config=SimpleNamespace())) == 1.0          # type: ignore[arg-type]
    assert rounding_headroom(SimpleNamespace(config=SimpleNamespace(lot_rounding="down"))) == 1.0  # type: ignore[arg-type]
    # the engine stores the real loss of a size above its target in risk_basis["loss"]: the open-risk sum uses it
    pos = BrokerPosition(broker="t", broker_symbol="ETHUSD", side=Side.LONG, status=PositionStatus.OPEN, quantity=0.13,
                         filled_quantity=0.13, remaining_quantity=0.13, stop_loss=1832.95, avg_entry_price=2232.95,
                         risk_amount=50.0, data={"risk_basis": {"entry": 2232.95, "stop": 1832.95, "quantity": 0.13,
                                                                "loss": 52.0}})
    assert guards.position_open_risk(pos) == pytest.approx(52.0)


def test_fx_open_risk_uses_the_rounded_down_quantity_of_its_own_size() -> None:
    pos = BrokerPosition(broker="fx", broker_symbol="EURUSD", side=Side.LONG, status=PositionStatus.OPEN, quantity=0.12,
                         filled_quantity=0.12, remaining_quantity=0.12, stop_loss=1.08, avg_entry_price=1.09,
                         risk_amount=100.0, data={"risk_basis": {"entry": 1.09, "stop": 1.08, "quantity": 0.12}})
    assert guards.position_open_risk(pos) == pytest.approx(100.0)  # target counted (never less than the size loses)



# --------------------------------------------------------------------------- #
# the engine: a broker that may round UP counts the larger loss in the daily open-risk check
# --------------------------------------------------------------------------- #
from test_preflight2_engine import FX, _restore_formatting_global, db, env, fx_pair  # noqa: E402,F401 (fixtures)
from copytrader.models import Severity  # noqa: E402


async def test_daily_open_risk_check_counts_the_rounding_headroom_of_a_nearest_broker(env: Any) -> None:
    env.fx.config.daily_loss_include_open_risk = True    # 3% of 10 000 = 300 USD, 1R = 100 USD
    env.fx.config.lot_rounding = "nearest"               # (fx stays on "down" in production: this is the rule only)
    await env.say(fx_pair("EURUSD"), FX, "EURUSD LONG 1R")          # 0 +   0 + 115
    await env.say(fx_pair("GBPUSD"), FX, "GBPUSD LONG 1R")          # 0 + 100 + 115
    assert len(env.fx.opened) == 2
    env.notifier.notifications.clear()
    await env.say(fx_pair("USDJPY"), FX, "USDJPY LONG 1R")          # 0 + 200 + 115 = 315 > 300 (with "down": 300)
    assert len(env.fx.opened) == 2
    (told,) = [t for t in env.texts(Severity.WARNING) if "günlük zarar" in t]
    assert "bu işlemin riski 115.00 = 315.00 USD" in told


async def test_daily_open_risk_check_of_a_down_broker_is_unchanged(env: Any) -> None:
    env.fx.config.daily_loss_include_open_risk = True
    for pair in ("EURUSD", "GBPUSD", "USDJPY"):                      # 0 + 200 + 100 = 300: exactly the limit, opens
        await env.say(fx_pair(pair), FX, f"{pair} LONG 1R")
    assert len(env.fx.opened) == 3
