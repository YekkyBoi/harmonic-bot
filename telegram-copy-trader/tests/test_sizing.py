import math

import pytest

from copytrader.brokers.base import BrokerError
from copytrader.risk.sizing import (
    SizingError,
    ceil_to_step,
    choose_leverage,
    compute_linear_size,
    compute_lot_size,
    floor_to_step,
    risk_amount,
    round_to_tick,
)

ETH_ENTRY, ETH_STOP = 2232.18, 2205.11


# --------------------------------------------------------------------------- #
# risk_amount
# --------------------------------------------------------------------------- #
def test_risk_amount_one_r_is_two_percent():
    assert risk_amount(10_000, 2.0) == pytest.approx(200.0)


def test_risk_amount_half_r_and_multiplier():
    assert risk_amount(10_000, 1.0) == pytest.approx(100.0)
    assert risk_amount(10_000, 2.0, multiplier=0.5) == pytest.approx(100.0)


# --------------------------------------------------------------------------- #
# floor_to_step / ceil_to_step / round_to_tick
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("value", "step", "expected"),
    [
        (0.1 + 0.2, 0.1, 0.3),            # 0.30000000000000004 must not become 0.2 or 0.30000000000000004
        (0.3, 0.1, 0.3),
        (0.7, 0.1, 0.7),
        (1.23456, 0.001, 1.234),
        (1.2349999, 0.001, 1.234),
        (0.0029999999999999996, 0.001, 0.003),  # float noise from a division
        (3 * 0.001, 0.001, 0.003),
        (6.7267, 0.001, 6.726),
        (0.00012345, 1e-6, 0.000123),
        (12345.678, 10, 12340.0),
        (7.389, 0.01, 7.38),
        (0.0009, 0.001, 0.0),
        (5.0, 0, 5.0),
        (5.0, -1, 5.0),
    ],
)
def test_floor_to_step(value, step, expected):
    assert floor_to_step(value, step) == expected


def test_floor_never_rounds_up_beyond_noise():
    assert floor_to_step(1.0019, 0.001) == 1.001
    assert floor_to_step(0.99999, 0.001) == 0.999


@pytest.mark.parametrize(
    ("value", "step", "expected"),
    [(0.0011, 0.001, 0.002), (0.002, 0.001, 0.002), (0.1 + 0.2, 0.1, 0.3), (1.0, 0, 1.0)],
)
def test_ceil_to_step(value, step, expected):
    assert ceil_to_step(value, step) == expected


@pytest.mark.parametrize(
    ("price", "tick", "expected"),
    [
        (2232.184, 0.01, 2232.18),
        (2232.185, 0.01, 2232.19),
        (2232.18, 0.05, 2232.2),
        (2232.12, 0.05, 2232.1),
        (0.1 + 0.2, 0.1, 0.3),
        (67251.3, 0.5, 67251.5),
        (1.0, 0, 1.0),
        (1.0, -0.01, 1.0),
    ],
)
def test_round_to_tick(price, tick, expected):
    assert round_to_tick(price, tick) == expected


# --------------------------------------------------------------------------- #
# compute_linear_size
# --------------------------------------------------------------------------- #
def eth(**kw):
    args = dict(
        risk_amount=200.0, entry=ETH_ENTRY, stop=ETH_STOP, qty_step=0.001, min_qty=0.001, fee_rate=0.0006
    )
    args.update(kw)
    return compute_linear_size(**args)


def test_linear_size_eth_example_with_fees():
    r = eth()
    loss_per_unit = (ETH_ENTRY - ETH_STOP) + 0.0006 * (ETH_ENTRY + ETH_STOP)
    assert r.quantity == 6.726
    assert r.quantity == floor_to_step(200.0 / loss_per_unit, 0.001)
    assert r.risk_amount == 200.0
    assert r.est_loss_at_sl == pytest.approx(6.726 * loss_per_unit)
    assert r.est_loss_at_sl <= 200.0
    assert r.notional == pytest.approx(6.726 * ETH_ENTRY)
    assert r.leverage is None and r.margin_required is None
    assert r.warnings == []


def test_linear_size_without_fees():
    r = eth(include_fees=False)
    assert r.quantity == 7.388
    assert r.est_loss_at_sl == pytest.approx(7.388 * (ETH_ENTRY - ETH_STOP))


def test_fee_rate_ignored_when_fees_excluded():
    assert eth(include_fees=False, fee_rate=0.01).quantity == eth(include_fees=False, fee_rate=0.0).quantity


def test_fees_reduce_quantity():
    assert eth(fee_rate=0.001).quantity < eth(fee_rate=0.0).quantity


def test_linear_size_short_is_symmetric():
    r = eth(entry=ETH_STOP, stop=ETH_ENTRY)
    assert r.quantity == 6.726
    assert r.notional == pytest.approx(6.726 * ETH_STOP)


def test_linear_size_contract_size():
    # 1 contract = 0.1 ETH -> about 10x more contracts, whole contracts only
    r = eth(contract_size=0.1, qty_step=1, min_qty=1)
    assert r.quantity == 67.0
    assert r.notional == pytest.approx(67 * 0.1 * ETH_ENTRY)
    assert r.est_loss_at_sl <= 200.0


def test_linear_size_max_qty_cap_warns():
    r = eth(max_qty=5.0)
    assert r.quantity == 5.0
    assert r.warnings and "azami" in r.warnings[0]
    assert r.est_loss_at_sl < 200.0


def test_below_min_qty_raises_turkish_message():
    with pytest.raises(SizingError) as err:
        compute_linear_size(risk_amount=0.34, entry=67250, stop=68100, qty_step=0.0001, min_qty=0.001)
    text = str(err.value)
    assert "(0.0004)" in text and "(0.001)" in text and "minimum emir miktarının" in text


def test_below_min_qty_round_up_allowed():
    r = compute_linear_size(
        risk_amount=0.5, entry=67250, stop=68100, qty_step=0.0001, min_qty=0.001, allow_min_qty_round_up=True
    )
    assert r.quantity == 0.001
    assert r.est_loss_at_sl == pytest.approx(0.85)   # larger than the 0.5 target, shown honestly
    assert r.warnings and "yükseltildi" in r.warnings[0]


def test_round_up_refused_above_twice_the_risk():
    with pytest.raises(SizingError, match="2 katından"):
        compute_linear_size(
            risk_amount=0.34, entry=67250, stop=68100, qty_step=0.0001, min_qty=0.001, allow_min_qty_round_up=True
        )


def test_below_min_notional_raises():
    with pytest.raises(SizingError, match="minimum emir tutarının"):
        compute_linear_size(risk_amount=1.0, entry=67250, stop=68100, qty_step=0.001, min_qty=0.001, min_notional=100)


def test_below_min_notional_round_up():
    r = compute_linear_size(
        risk_amount=1.0, entry=67250, stop=68100, qty_step=0.001, min_qty=0.001, min_notional=100,
        allow_min_qty_round_up=True,
    )
    assert r.quantity == 0.002          # ceil(100 / 67250, 0.001)
    assert r.notional >= 100
    assert r.est_loss_at_sl == pytest.approx(1.7)


def test_min_notional_exactly_met_is_ok():
    # 0.1 * 1000 = 100 -> exactly the minimum
    r = compute_linear_size(risk_amount=10.0, entry=1000, stop=900, qty_step=0.1, min_qty=0.1, min_notional=100)
    assert r.quantity == 0.1


def test_round_up_beyond_max_qty_raises():
    with pytest.raises(SizingError):
        compute_linear_size(
            risk_amount=0.5, entry=67250, stop=68100, qty_step=0.0001, min_qty=0.001, max_qty=0.0005,
            allow_min_qty_round_up=True,
        )


def test_zero_quantity_with_no_min_qty_raises():
    with pytest.raises(SizingError):
        compute_linear_size(risk_amount=0.01, entry=67250, stop=68100, qty_step=0.001, min_qty=0)


def test_entry_equal_stop_raises():
    with pytest.raises(SizingError):
        eth(stop=ETH_ENTRY)


@pytest.mark.parametrize(
    "override",
    [
        {"risk_amount": 0.0},
        {"risk_amount": -5.0},
        {"entry": 0.0},
        {"stop": -1.0},
        {"contract_size": 0.0},
        {"qty_step": 0.0},
        {"risk_amount": math.nan},
        {"entry": math.inf},
        {"min_qty": -1.0},
        {"max_qty": 0.0},
        {"fee_rate": -0.001},
    ],
)
def test_invalid_inputs_raise(override):
    with pytest.raises(SizingError):
        eth(**override)


def test_sizing_error_is_broker_error():
    assert issubclass(SizingError, BrokerError)


# --------------------------------------------------------------------------- #
# compute_lot_size
# --------------------------------------------------------------------------- #
def test_lot_size_basic():
    assert compute_lot_size(risk_amount=200, loss_per_lot=270.7, lot_step=0.01, min_lot=0.01) == 0.73


def test_lot_size_float_noise():
    # 0.3 lots exactly: 30 / 100
    assert compute_lot_size(risk_amount=30, loss_per_lot=100, lot_step=0.01, min_lot=0.01) == 0.3


def test_lot_size_max_cap():
    assert compute_lot_size(risk_amount=200, loss_per_lot=270.7, lot_step=0.01, min_lot=0.01, max_lot=0.5) == 0.5


def test_lot_size_below_min_raises():
    with pytest.raises(SizingError, match="minimum lotunun"):
        compute_lot_size(risk_amount=2, loss_per_lot=270.7, lot_step=0.01, min_lot=0.01)


def test_lot_size_round_up():
    assert compute_lot_size(
        risk_amount=2, loss_per_lot=270.7, lot_step=0.01, min_lot=0.01, allow_min_qty_round_up=True
    ) == 0.01


def test_lot_size_round_up_refused_above_twice_the_risk():
    with pytest.raises(SizingError):
        compute_lot_size(risk_amount=1, loss_per_lot=270.7, lot_step=0.01, min_lot=0.01, allow_min_qty_round_up=True)


def test_lot_size_invalid():
    with pytest.raises(SizingError):
        compute_lot_size(risk_amount=200, loss_per_lot=0, lot_step=0.01, min_lot=0.01)


# --------------------------------------------------------------------------- #
# choose_leverage
# --------------------------------------------------------------------------- #
ETH_STOP_FRAC = (ETH_ENTRY - ETH_STOP) / ETH_ENTRY  # ~1.21%


def test_leverage_preferred_when_margin_fits():
    lev, margin = choose_leverage(notional=15_000, free_margin=10_000, stop_distance_frac=ETH_STOP_FRAC,
                                  preferred=10, max_leverage=50)
    assert (lev, margin) == (10, 1500.0)
    assert isinstance(lev, int)


def test_leverage_lowered_to_liquidation_cap():
    # floor(1 / (0.05 * 1.5 + 0.005)) = floor(12.5) = 12
    lev, margin = choose_leverage(notional=1000, free_margin=10_000, stop_distance_frac=0.05,
                                  preferred=20, max_leverage=50)
    assert lev == 12
    assert margin == pytest.approx(1000 / 12)


def test_liquidation_cap_is_decimal_exact():
    # 1 / (0.05 * 1.5 + 0.025) = 10 exactly; float math could give 9.999...
    lev, _ = choose_leverage(notional=100, free_margin=10_000, stop_distance_frac=0.05, preferred=20,
                             max_leverage=50, maintenance_margin_rate=0.025)
    assert lev == 10


def test_liquidation_stays_beyond_stop():
    for frac in (0.002, 0.0121, 0.03, 0.1, 0.3):
        lev, _ = choose_leverage(notional=1000, free_margin=1e9, stop_distance_frac=frac, preferred=125,
                                 max_leverage=125)
        # approx. distance to liquidation = 1/lev - mmr, must exceed the stop distance by the buffer
        assert 1 / lev - 0.005 >= frac * 1.5 - 1e-12


def test_leverage_raised_when_margin_requires():
    # allowed margin = 1000 * 90% = 900 -> ceil(15000 / 900) = 17
    lev, margin = choose_leverage(notional=15_000, free_margin=1000, stop_distance_frac=ETH_STOP_FRAC,
                                  preferred=10, max_leverage=50)
    assert lev == 17
    assert margin == pytest.approx(15_000 / 17)
    assert margin <= 900


def test_leverage_exact_fit():
    lev, margin = choose_leverage(notional=9000, free_margin=1000, stop_distance_frac=0.01, preferred=5,
                                  max_leverage=50)
    assert (lev, margin) == (10, 900.0)


def test_leverage_margin_usage_pct():
    lev, _ = choose_leverage(notional=9000, free_margin=1000, stop_distance_frac=0.01, preferred=5,
                             max_leverage=50, max_margin_usage_pct=50)
    assert lev == 18


def test_leverage_not_enough_margin_raises():
    with pytest.raises(SizingError, match="Yetersiz teminat"):
        choose_leverage(notional=15_000, free_margin=100, stop_distance_frac=ETH_STOP_FRAC, preferred=10,
                        max_leverage=50)


def test_leverage_venue_max_caps():
    with pytest.raises(SizingError):
        # needs ceil(15000/450) = 34x, venue allows 20x
        choose_leverage(notional=15_000, free_margin=500, stop_distance_frac=ETH_STOP_FRAC, preferred=10,
                        max_leverage=50, venue_max_leverage=20.0)
    lev, _ = choose_leverage(notional=1000, free_margin=10_000, stop_distance_frac=0.001, preferred=50,
                             max_leverage=50, venue_max_leverage=25.5)
    assert lev == 25


def test_leverage_config_max_caps():
    lev, _ = choose_leverage(notional=1000, free_margin=10_000, stop_distance_frac=0.001, preferred=40,
                             max_leverage=20)
    assert lev == 20
    with pytest.raises(SizingError):
        choose_leverage(notional=15_000, free_margin=500, stop_distance_frac=0.001, preferred=10, max_leverage=20)


def test_leverage_at_least_one():
    lev, margin = choose_leverage(notional=100, free_margin=1000, stop_distance_frac=0.9, preferred=20,
                                  max_leverage=50)
    assert (lev, margin) == (1, 100.0)
    lev, _ = choose_leverage(notional=100, free_margin=1000, stop_distance_frac=0.01, preferred=0, max_leverage=50)
    assert lev == 1


@pytest.mark.parametrize("free", [0.0, -50.0])
def test_leverage_no_free_margin_raises(free):
    with pytest.raises(SizingError):
        choose_leverage(notional=100, free_margin=free, stop_distance_frac=0.01, preferred=10, max_leverage=50)


@pytest.mark.parametrize("kw", [{"notional": 0.0}, {"stop_distance_frac": 0.0}, {"stop_distance_frac": math.nan}])
def test_leverage_invalid_inputs(kw):
    args = dict(notional=100.0, free_margin=1000.0, stop_distance_frac=0.01, preferred=10, max_leverage=50)
    args.update(kw)
    with pytest.raises(SizingError):
        choose_leverage(**args)
