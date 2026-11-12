"""The MetaTrader5 package's calling convention for order_send.

Production failure (first live FX signal): every order to the native MT5 broker ended with
``MT5 emir yanıtı alınamadı (Unnamed arguments not allowed (kod -2))``. The adapter called
``mt5.order_send(request, **{})``. For a C function declared METH_VARARGS | METH_KEYWORDS, CPython passes an
EMPTY dict (not NULL) for ``**{}``, and the package's order_send / order_check refuse a positional request
together with any keyword dict: they set last_error (-2, "Unnamed arguments not allowed") and return None
before anything is sent to the terminal (package 5.0.6231, order_send entry at 0x18000f480).

A pure Python fake cannot tell ``f(x)`` from ``f(x, **{})``, which is why the existing tests never saw this.
Here ``order_send`` of the fake terminal is wrapped in a REAL builtin function object (built with ctypes) that
applies the package's rule, so the difference is visible exactly as it is on the server.
"""

from __future__ import annotations

import ctypes
from typing import Any

import pytest

from copytrader.models import PositionStatus

from test_mt5_broker import FakeMT5, connected, make_plan, make_req  # noqa: E402  (tests dir is on sys.path)

METH_VARARGS, METH_KEYWORDS = 0x0001, 0x0002
_PROTO = ctypes.PYFUNCTYPE(ctypes.py_object, ctypes.c_void_p, ctypes.py_object, ctypes.c_void_p)


class _PyMethodDef(ctypes.Structure):
    _fields_ = [("ml_name", ctypes.c_char_p), ("ml_meth", ctypes.c_void_p), ("ml_flags", ctypes.c_int),
                ("ml_doc", ctypes.c_char_p)]


_new_cfunction = ctypes.pythonapi.PyCFunction_NewEx
_new_cfunction.restype = ctypes.py_object
_new_cfunction.argtypes = [ctypes.POINTER(_PyMethodDef), ctypes.py_object, ctypes.py_object]


class StrictMT5(FakeMT5):
    """FakeMT5 whose ``order_send`` is a real builtin with the package's argument rule."""

    def __init__(self) -> None:
        super().__init__()
        self.kwargs_seen: list[str] = []        # "NULL" / "dict(n)" per order_send call
        self.callback_error: BaseException | None = None
        python_order_send = super().order_send

        def impl(_self: Any, args: tuple[Any, ...], kwargs_ptr: Any) -> Any:
            try:
                if kwargs_ptr:                  # a keyword dict was passed (even an empty one)
                    kwargs = ctypes.cast(kwargs_ptr, ctypes.py_object).value
                    self.kwargs_seen.append(f"dict({len(kwargs)})")
                    if len(args) != 0:
                        self.last = (-2, "Unnamed arguments not allowed")
                        return None
                    request = kwargs
                else:
                    self.kwargs_seen.append("NULL")
                    if len(args) != 1:
                        self.last = (-2, "Invalid arguments")
                        return None
                    request = args[0]
                return python_order_send(request)
            except BaseException as exc:  # noqa: BLE001 - must never escape a ctypes callback
                self.callback_error = exc
                return None

        self._callback = _PROTO(impl)           # keep the callback and the method table alive
        self._method_def = _PyMethodDef(b"order_send", ctypes.cast(self._callback, ctypes.c_void_p),
                                        METH_VARARGS | METH_KEYWORDS, b"strict fake")
        self.order_send = _new_cfunction(ctypes.byref(self._method_def), None, None)  # type: ignore[method-assign]


@pytest.fixture
def strict() -> StrictMT5:
    return StrictMT5()


def test_the_strict_fake_behaves_like_the_package(strict: StrictMT5) -> None:
    """The rule being emulated: positional request + keyword dict (even empty) -> None, last_error -2."""
    request = {"action": 1, "symbol": "ETHUSD", "volume": 0.1, "type": 0, "price": 2240.0, "sl": 0.0, "tp": 0.0,
               "deviation": 20, "magic": 1, "comment": "x", "type_time": 0, "type_filling": 0}
    empty: dict[str, Any] = {}
    assert strict.order_send(request, **empty) is None
    assert strict.last_error() == (-2, "Unnamed arguments not allowed")
    assert strict.kwargs_seen == ["dict(0)"]
    assert strict.order_send(request) is not None            # the documented form
    assert strict.kwargs_seen == ["dict(0)", "NULL"]
    assert strict.callback_error is None


async def test_market_order_reaches_order_send_without_a_keyword_dict(strict: StrictMT5) -> None:
    """The exact production path: open a market position on the native adapter."""
    broker = await connected(strict)
    try:
        sym = await broker.resolve_symbol("ETH")
        pos = await broker.open_position(make_req(sym, 0.10, make_plan()))
    finally:
        await broker.close()
    assert strict.callback_error is None
    assert pos.status is PositionStatus.OPEN and pos.error is None, pos.error
    assert strict.kwargs_seen and set(strict.kwargs_seen) == {"NULL"}       # never an (empty) keyword dict
    assert len(strict.positions) >= 1


async def test_stop_move_and_close_also_go_through(strict: StrictMT5) -> None:
    """Every later instruction (move the stop, close) uses order_send too."""
    broker = await connected(strict)
    try:
        sym = await broker.resolve_symbol("ETH")
        pos = await broker.open_position(make_req(sym, 0.10, make_plan()))
        assert pos.status is PositionStatus.OPEN and pos.error is None, pos.error
        opened = len(strict.kwargs_seen)
        pos = await broker.update_stop_loss(pos, 2220.0)
        assert pos.error is None and pos.stop_loss == pytest.approx(2220.0)
        moved = len(strict.kwargs_seen)
        pos = await broker.close_position(pos, 1.0)
    finally:
        await broker.close()
    assert strict.callback_error is None
    assert pos.status is PositionStatus.CLOSED, pos.error
    assert opened >= 1 and moved > opened and len(strict.kwargs_seen) > moved   # each step sent orders
    assert set(strict.kwargs_seen) == {"NULL"}
    assert not strict.positions
