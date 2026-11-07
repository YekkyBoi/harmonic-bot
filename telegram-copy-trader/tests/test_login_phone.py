"""Phone numbers typed at login are turned into Telegram's international format."""

import pytest

from copytrader.telegram.listener import normalize_phone


@pytest.mark.parametrize("typed, expected", [
    ("+4407765643200", "+447765643200"),      # UK number typed with its national 0
    ("+44 07765 643200", "+447765643200"),
    ("0044 7765 643200", "+447765643200"),
    ("+447765643200", "+447765643200"),       # already correct
    ("+90 0555 111 2233", "+905551112233"),
    ("+905551112233", "+905551112233"),
    ("+39 06 1234 5678", "+390612345678"),    # Italy keeps its 0
])
def test_normalize_phone(typed: str, expected: str) -> None:
    assert normalize_phone(typed) == expected
