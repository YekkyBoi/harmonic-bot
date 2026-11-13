"""Live 2026-11-13: an offset-shifted tickmill stop was shown as 'stop 1970.9599999999998' (1972 - 1.04)."""
from __future__ import annotations

import pytest

from copytrader.telegram.formatting import fmt_price


@pytest.mark.parametrize("value, shown", [
    (1972 - 1.04, "1970.96"),
    (0.1 + 0.2, "0.3"),
    (2310.6, "2310.6"),
    (1.2e-05, "0.000012"),
    (0.00000123456789, "0.00000123456789"),
    (73450.12345678, "73450.12345678"),
    (1.16205, "1.16205"),
    (1972.0, "1972"),
    (100000.0, "100000"),
    (-0.0, "0"),
    (0.0, "0"),
    (None, "—"),
])
def test_fmt_price_drops_float_noise_and_keeps_real_digits(value, shown) -> None:
    assert fmt_price(value) == shown


def test_any_price_of_up_to_15_digits_is_shown_unchanged() -> None:
    for text in ("1970.96", "0.000012345", "65432.1", "1.08765", "123456789012.345", "0.1"):
        assert fmt_price(float(text)) == text
