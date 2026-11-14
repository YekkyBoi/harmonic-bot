"""A fake MetaTrader5 module for the mt5_bridge HELPER PROCESS (tests only).

The helper imports it through mt5_helper.FAKE_ENV ("mt5_fake_terminal:make"). It is test_mt5_broker.FakeMT5 (the
fake every native-adapter test uses) whose account state - positions, orders, history, deals, ticket counter - lives
in a file (TERMINAL_STATE_ENV), so that it survives the helper being killed and started again (one "terminal", many
helper processes), and that the test itself can read it. A control file (TERMINAL_CONTROL_ENV, JSON) makes the helper
misbehave on purpose, once per entry:

  {"crash_at": "order_send"}           os._exit(3) when that call starts (nothing done)
  {"crash_after": "order_send"}        os._exit(3) right after that call did its work (the answer is lost)
  {"freeze_at": "symbol_info_tick"}    SIGSTOP of the whole helper process at that call (a real hang)
  {"slow": {"account_info": 3.0}}      that call sleeps this long first (in the worker thread)
"""

from __future__ import annotations

import json
import os
import pickle
import signal
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from test_mt5_broker import FakeMT5, make_symbol

TERMINAL_STATE_ENV = "FAKE_TERMINAL_STATE"
TERMINAL_CONTROL_ENV = "FAKE_TERMINAL_CONTROL"
TERMINAL_FOLDER_ENV = "FAKE_TERMINAL_FOLDER"
TERMINAL_COMPANY_ENV = "FAKE_TERMINAL_COMPANY"
_KEPT = ("positions", "orders", "history_orders", "deals", "_next")
BTC_BID, BTC_ASK = 95_000.0, 95_010.0
FX = {"EURUSD": (1.16195, 1.16205), "GBPUSD": (1.31495, 1.31505)}


def server_now() -> int:
    return int(time.time()) + 7200          # a trade server on UTC+2 (the adapter's staleness rule)


class PersistentFake(FakeMT5):
    def __init__(self, state: str, control: str, folder: str, company: str) -> None:
        super().__init__()
        self.state_path = Path(state)
        self.control_path = Path(control)
        self.terminal.path = folder
        self.terminal.company = company
        self.account.login = 7007
        self.account.server = f"{company.split()[0]}-Demo"
        self.account.company = company
        now = server_now()
        self.symbols["BTCUSD"] = make_symbol("BTCUSD", trade_tick_size=0.01, point=0.01)
        self.ticks["BTCUSD"] = SimpleNamespace(bid=BTC_BID, ask=BTC_ASK, last=0.0, time=now)
        self.ticks["ETHUSD"].time = now
        for pair, (bid, ask) in FX.items():           # (the prices of test_preflight2_engine.FX_QUOTES)
            self.symbols[pair] = make_symbol(pair, trade_tick_size=0.00001, point=0.00001, digits=5,
                                             trade_contract_size=100_000.0, currency_profit=pair[3:],
                                             currency_margin=pair[:3])
            self.ticks[pair] = SimpleNamespace(bid=bid, ask=ask, last=0.0, time=now)
        self._load()

    # ---- state file ----------------------------------------------------------
    def _load(self) -> None:
        if self.state_path.is_file():
            data = pickle.loads(self.state_path.read_bytes())
            for key in _KEPT:
                setattr(self, key, data[key])

    def _save(self) -> None:
        tmp = self.state_path.with_name(self.state_path.name + ".tmp")
        tmp.write_bytes(pickle.dumps({key: getattr(self, key) for key in _KEPT}))
        os.replace(tmp, self.state_path)

    # ---- misbehaviour ----------------------------------------------------------
    def _control(self) -> dict[str, Any]:
        try:
            return json.loads(self.control_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    def _consume(self, key: str) -> None:
        data = self._control()
        data.pop(key, None)
        self.control_path.write_text(json.dumps(data), encoding="utf-8")

    def _before(self, name: str) -> None:
        ctl = self._control()
        slow = (ctl.get("slow") or {}).get(name)
        if slow:
            time.sleep(float(slow))
        if ctl.get("crash_at") == name:
            self._consume("crash_at")
            os._exit(3)
        if ctl.get("freeze_at") == name:
            self._consume("freeze_at")
            os.kill(os.getpid(), signal.SIGSTOP)

    def _after(self, name: str) -> None:
        if self._control().get("crash_after") == name:
            self._consume("crash_after")
            os._exit(3)

    # ---- the package -------------------------------------------------------------
    def initialize(self, *args: Any, **kwargs: Any) -> bool:
        self._before("initialize")
        self._load()
        return super().initialize(*args, **kwargs)

    def account_info(self) -> Any:
        self._before("account_info")
        return super().account_info()

    def symbol_info_tick(self, name: str) -> Any:
        self._before("symbol_info_tick")
        tick = self.ticks.get(name)
        if tick is not None:
            tick.time = server_now()
        return super().symbol_info_tick(name)

    def positions_get(self, ticket: int | None = None, symbol: str | None = None) -> Any:
        self._before("positions_get")
        self._load()
        return super().positions_get(ticket=ticket, symbol=symbol)

    def orders_get(self, ticket: int | None = None, symbol: str | None = None) -> Any:
        self._load()
        return super().orders_get(ticket=ticket, symbol=symbol)

    def order_send(self, request: dict[str, Any]) -> Any:
        self._before("order_send")
        self._load()
        res = super().order_send(request)
        self._save()
        self._after("order_send")
        return res


def make() -> PersistentFake:
    return PersistentFake(os.environ[TERMINAL_STATE_ENV], os.environ[TERMINAL_CONTROL_ENV],
                          os.environ.get(TERMINAL_FOLDER_ENV, r"C:\Program Files\Tickmill MT5 Terminal"),
                          os.environ.get(TERMINAL_COMPANY_ENV, "Tickmill Ltd"))
