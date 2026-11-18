"""lot_rounding: "up" (never below the target loss at the stop, at most lot_rounding_max_over above it) and the
per-broker fees_in_risk (the friend's tickmill: 1R = 50 USD on the price loss at the stop, commission on top)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from copytrader.config import AppConfig, Mt5BridgeBrokerConfig, PaperBrokerConfig, RiskConfig, fees_in_risk
from copytrader.engine.orchestrator import max_rounded_risk
from copytrader.models import Side
from copytrader.risk.sizing import compute_linear_size, compute_lot_size, round_quantity, up_refused_warning
from copytrader.selftest import _risk_text

from test_mt5_broker import FakeMT5, _server_clock, connected, make_cfg, make_symbol  # noqa: F401 (autouse fixture)

# 1 lot loses 106.40 between these (contract size 1): the signal's "100 dolar için 0.94 lot" -> 50 USD = 0.47
ENTRY, STOP = 2232.95, 2126.55


def test_round_up_reaches_the_target() -> None:
    raw = 50.0 / 106.4                                       # 0.46992...
    assert round_quantity(raw, 0.01, "up", risk_amount=50.0, loss_per_unit=106.4) == 0.47
    assert round_quantity(raw, 0.01, "down", risk_amount=50.0, loss_per_unit=106.4) == 0.46
    assert round_quantity(raw, 0.01, "nearest", risk_amount=50.0, loss_per_unit=106.4) == 0.47


def test_exact_multiples_stay_exact() -> None:
    assert round_quantity(0.1 + 0.2, 0.1, "up", risk_amount=30.0, loss_per_unit=100.0) == 0.3
    assert round_quantity(0.47000000000000003, 0.01, "up", risk_amount=47.0, loss_per_unit=100.0) == 0.47
    assert round_quantity(0.94, 0.01, "up", risk_amount=94.0, loss_per_unit=100.0) == 0.94


def test_round_up_limited_by_max_over_then_down() -> None:
    # step 0.1, 1 lot loses 100: 0.41 -> 0.5 loses 50 = 9 above the target 41
    assert round_quantity(0.41, 0.1, "up", risk_amount=41.0, loss_per_unit=100.0, max_over=10.0) == 0.5
    assert round_quantity(0.41, 0.1, "up", risk_amount=41.0, loss_per_unit=100.0, max_over=9.0) == 0.5   # on the edge
    assert round_quantity(0.41, 0.1, "up", risk_amount=41.0, loss_per_unit=100.0, max_over=8.99) == 0.4
    assert round_quantity(0.41, 0.1, "up", risk_amount=41.0, loss_per_unit=100.0) == 0.5                 # default 10
    assert round_quantity(0.41, 0.1, "up") == 0.4                                    # without the figures: down
    text = up_refused_warning(0.41, 0.4, 0.1, "up", 41.0, 100.0, 5.0)
    assert text is not None and "aşağı yuvarlandı" in text and "0.4 -> 0.5" in text
    assert up_refused_warning(0.41, 0.5, 0.1, "up", 41.0, 100.0, 10.0) is None
    assert up_refused_warning(0.41, 0.4, 0.1, "down", 41.0, 100.0, 5.0) is None


def test_sizing_functions_round_up() -> None:
    assert compute_lot_size(risk_amount=50.0, loss_per_lot=106.4, lot_step=0.01, min_lot=0.01, rounding="up") == 0.47
    linear = compute_linear_size(risk_amount=41.0, entry=100.0, stop=99.0, qty_step=0.1, min_qty=0.1,
                                 contract_size=100.0, rounding="up", max_over=5.0, include_fees=False)
    assert linear.quantity == 0.4 and any("aşağı yuvarlandı" in w for w in linear.warnings)
    linear = compute_linear_size(risk_amount=41.0, entry=100.0, stop=99.0, qty_step=0.1, min_qty=0.1,
                                 contract_size=100.0, rounding="up", include_fees=False)
    assert linear.quantity == 0.5 and linear.warnings == []


def test_config_accepts_and_validates() -> None:
    cfg = Mt5BridgeBrokerConfig(type="mt5_bridge", name="tickmill", terminal_path=r"C:\T\terminal64.exe",
                                broker_company="Tickmill", lot_rounding="up", lot_rounding_max_over=10,
                                fees_in_risk=False)
    assert cfg.lot_rounding == "up" and cfg.lot_rounding_max_over == 10.0 and cfg.fees_in_risk is False
    default = PaperBrokerConfig(type="paper", name="p")
    assert default.lot_rounding == "down" and default.fees_in_risk is None and default.lot_rounding_max_over == 10.0
    with pytest.raises(ValidationError):
        PaperBrokerConfig(type="paper", name="p", lot_rounding_max_over=-1)
    with pytest.raises(ValidationError):
        PaperBrokerConfig(type="paper", name="p", lot_rounding="sideways")
    with pytest.raises(ValidationError):
        PaperBrokerConfig(type="paper", name="p", fees_in_risk="maybe")
    assert fees_in_risk(default, RiskConfig(include_fees_in_risk=True)) is True
    assert fees_in_risk(default, RiskConfig(include_fees_in_risk=False)) is False
    assert fees_in_risk(cfg, RiskConfig(include_fees_in_risk=True)) is False


def test_selftest_names_the_settings() -> None:
    app = AppConfig.model_validate({
        "telegram": {"api_id": 1, "api_hash": "h", "source_chats": [1]},
        "notifier": {"bot_token": "1:a", "owner_user_id": 2},
        "brokers": [{"type": "paper", "name": "fx"},
                    {"type": "paper", "name": "tickmill", "risk_usd": 50, "lot_rounding": "up",
                     "lot_rounding_max_over": 10, "fees_in_risk": False}],
    })
    text = _risk_text(app)
    assert "tickmill 50 USD (lot yukarı yuvarlanır, stopta hedefin en çok 10 üstü; komisyon riske katılmaz)" in text
    assert "fx bakiyenin" in text and "fx bakiyenin %2 kadarı (" not in text


def test_open_risk_bound_of_an_up_broker() -> None:
    up = SimpleNamespace(config=SimpleNamespace(lot_rounding="up", lot_rounding_max_over=10.0))
    assert max_rounded_risk(up, 50.0) == 60.0                                          # type: ignore[arg-type]
    assert max_rounded_risk(SimpleNamespace(config=SimpleNamespace()), 50.0) == 50.0   # type: ignore[arg-type]
    near = SimpleNamespace(config=SimpleNamespace(lot_rounding="nearest"))
    assert max_rounded_risk(near, 100.0) == pytest.approx(115.0)                       # type: ignore[arg-type]


async def test_metatrader_commission_and_rounding() -> None:
    """The case of the report: 1 lot loses 106.40 at the stop, commission 7 per lot, 1R = 50."""
    async def size(**cfg: object) -> object:
        fake = FakeMT5()
        broker = await connected(fake, make_cfg(commission_per_lot=7.0, **cfg), include_fees_in_risk=True)
        try:
            sym = await broker.resolve_symbol("ETH", "USDT")
            return await broker.size_position(sym, Side.LONG, ENTRY, STOP, 50.0)
        finally:
            await broker.close()

    today = await size()                                       # 50 / (106.4 + 7) = 0.4409 -> 0.44
    assert today.quantity == 0.44                              # type: ignore[attr-defined]
    no_fee = await size(fees_in_risk=False)                    # 50 / 106.4 = 0.4699 -> 0.46 (down)
    assert no_fee.quantity == 0.46                             # type: ignore[attr-defined]
    wanted = await size(fees_in_risk=False, lot_rounding="up")  # -> 0.47, loss 50.01: never below 50
    assert wanted.quantity == 0.47 and wanted.warnings == []   # type: ignore[attr-defined]
    assert wanted.est_loss_at_sl == pytest.approx(0.47 * 106.4) and wanted.est_fees_at_sl == 0.0  # type: ignore[attr-defined]
    strict = await size(fees_in_risk=False, lot_rounding="up", lot_rounding_max_over=0)
    assert strict.quantity == 0.46                             # type: ignore[attr-defined]
    assert any("aşağı yuvarlandı" in w for w in strict.warnings)  # type: ignore[attr-defined]


def test_make_symbol_is_the_one_lot_loss_used_above() -> None:
    assert make_symbol("ETHUSD").trade_contract_size == 1.0
    assert abs(ENTRY - STOP) == pytest.approx(106.4)
