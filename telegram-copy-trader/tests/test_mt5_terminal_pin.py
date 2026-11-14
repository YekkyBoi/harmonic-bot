"""The owner's fx account never moves to another MetaTrader terminal (the friend's Tickmill terminal on the same
computer).

fx (type "mt5") calls MetaTrader5.initialize() without a path: with a second terminal installed, the package could
attach to that one - and log it in to the owner's account. The fake package here is a computer with two terminals:
``initialize(path)`` attaches to the terminal at that path, ``initialize()`` without one to ``default`` (whichever the
package would pick), and a log-in (login=...) switches the terminal it reached to that account, as the real one does.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from copytrader.brokers.base import BrokerError
from copytrader.brokers.mt5_broker import Mt5Broker, TerminalGuard, build_guard, read_pins
from copytrader.config import Mt5BrokerConfig, RiskConfig, terminal_key
from copytrader.models import SymbolInfo

from test_mt5_broker import _server_clock, FakeMT5, make_plan, make_req  # noqa: F401 (autouse fixture)


def make_cfg(**kw: Any) -> Mt5BrokerConfig:
    return Mt5BrokerConfig(**{"type": "mt5", "name": "fx", "login": 5001, "password": "pw", "server": "Demo-Server", **kw})

FP = r"C:\Program Files\FundingPips MT5 Terminal"
TM = r"C:\Program Files\Tickmill MT5 Terminal"
OTHER = r"D:\MT5\MetaTrader 5"


class TwoTerminals(FakeMT5):
    def __init__(self) -> None:
        super().__init__()
        self.terms: dict[str, dict[str, Any]] = {
            FP: {"company": "FundingPips Ltd", "login": 5001, "server": "Demo-Server"},
            TM: {"company": "Tickmill Ltd", "login": 7007, "server": "Tickmill-Live"},
            OTHER: {"company": "MetaQuotes Ltd", "login": 0, "server": ""},
        }
        self.default = FP
        self.current: str | None = None
        self.logins: list[tuple[str, int]] = []         # (terminal, login) of every log-in
        self.server_after_login: str | None = None

    def initialize(self, *args: Any, **kwargs: Any) -> bool:
        self._log("initialize", *args, **kwargs)
        self.init_count += 1
        path = args[0] if args else kwargs.get("path")
        folder = path.rsplit("\\", 1)[0] if path else self.default
        if folder not in self.terms:
            self.last = (-6, "Terminal: Authorization failed")
            return False
        self.current = folder
        if "login" in kwargs:
            self.logins.append((folder, int(kwargs["login"])))
            self.terms[folder].update(login=int(kwargs["login"]),
                                      server=self.server_after_login or kwargs.get("server"))
        return True

    def shutdown(self) -> None:
        super().shutdown()
        self.current = None

    def terminal_info(self) -> Any:
        self._log("terminal_info")
        t = self.terms[self.current] if self.current else {}
        return SimpleNamespace(**{**vars(self.terminal), "path": self.current or "",
                                  **({"company": t["company"]} if t else {})})

    def account_info(self) -> Any:
        if self._log("account_info") or self.current is None:
            return None
        t = self.terms[self.current]
        return SimpleNamespace(**{**vars(self.account), "login": t["login"], "server": t["server"],
                                  "company": t["company"]})


def tickmill_guard(pins: Path | None = None) -> TerminalGuard:
    """What the registry gives fx in the owner's config: the tickmill block (MetaApi or mt5_bridge) and the pins."""
    tick = SimpleNamespace(name="tickmill", type="mt5_bridge", terminal_path=TM + r"\terminal64.exe",
                           broker_company="Tickmill")
    return build_guard("fx", [tick], str(pins) if pins else None, ["fx", "Demo-Server", ""])


async def fx(fake: TwoTerminals, pins: Path, guard: TerminalGuard | None = None, **cfg: Any) -> Mt5Broker:
    b = Mt5Broker(make_cfg(**cfg), mt5_module=fake, pin_dir=str(pins), guard=guard)
    b.retry_delay_sec = 0
    b.set_risk_config(RiskConfig(include_fees_in_risk=False))
    await b.connect()
    return b


def inits(fake: TwoTerminals) -> list[tuple[tuple[Any, ...], dict[str, Any]]]:
    return [(a, k) for n, a, k in fake.calls if n == "initialize"]


async def test_first_connect_pins_the_terminal_and_every_later_one_uses_its_path(tmp_path: Path) -> None:
    fake = TwoTerminals()
    b = await fx(fake, tmp_path)                                     # only FundingPips is known yet
    await b.close()
    pin = read_pins(str(tmp_path))["fx"]
    assert pin["folder"] == FP and pin["company"] == "FundingPips Ltd" and pin["server"] == "Demo-Server"
    assert pin["login"] == 5001 and pin["path"] == FP + r"\terminal64.exe"
    # Tickmill is installed now and the package would pick it: fx still logs in at its own terminal, by path
    fake.default = TM
    fake.calls.clear()
    for _ in range(3):                                               # restarts of the bot
        b = await fx(fake, tmp_path, tickmill_guard(tmp_path))
        await b.close()
    assert [args for args, kw in inits(fake) if "login" in kw] == [(FP + r"\terminal64.exe",)] * 3
    assert fake.terms[TM]["login"] == 7007 and all(t == FP for t, _ in fake.logins)   # Tickmill never touched


async def test_without_a_pin_fx_looks_first_and_never_logs_the_tickmill_terminal_in(tmp_path: Path) -> None:
    fake = TwoTerminals()
    fake.default = TM                                                # the package picks the friend's terminal
    with pytest.raises(BrokerError) as err:
        await fx(fake, tmp_path, tickmill_guard())
    text = str(err.value)
    assert "'tickmill' hesabının terminaline bağlandı" in text and TM in text
    assert "Güvenlik için bağlantı kesildi; bu hesapta işlem yapılmayacak." in text
    assert fake.logins == [] and fake.terms[TM]["login"] == 7007       # it only looked: no log-in, no order
    assert all("login" not in kw for _, kw in inits(fake)) and fake.sent == []
    assert ("shutdown", (), {}) in fake.calls and read_pins(str(tmp_path)) == {}


async def test_without_a_pin_fx_finds_its_own_terminal_and_logs_in_by_its_path(tmp_path: Path) -> None:
    fake = TwoTerminals()
    b = await fx(fake, tmp_path, tickmill_guard())
    await b.close()
    first, second = inits(fake)[:2]
    assert first == ((), {"timeout": 10_000})                        # attach only: nothing changes in that terminal
    assert second[0] == (FP + r"\terminal64.exe",) and second[1]["login"] == 5001
    assert read_pins(str(tmp_path))["fx"]["folder"] == FP


async def test_a_pinned_path_that_now_holds_another_company_is_refused(tmp_path: Path) -> None:
    fake = TwoTerminals()
    await (await fx(fake, tmp_path)).close()
    fake.terms[FP]["company"] = "Tickmill Ltd"                       # someone installed Tickmill over that folder
    with pytest.raises(BrokerError) as err:
        await fx(fake, tmp_path, tickmill_guard(tmp_path))
    text = str(err.value)
    assert "bu hesabın daha önce kullandığı terminal yerine başka bir terminale bağlandı" in text
    assert "şirket: FundingPips Ltd" in text and "mt5_terminal_fx.json dosyasını silin" in text
    assert fake.sent == []


async def test_a_pinned_server_that_changed_is_refused(tmp_path: Path) -> None:
    fake = TwoTerminals()
    await (await fx(fake, tmp_path)).close()                         # pinned: Demo-Server
    fake.server_after_login = "Other-Server"                         # the terminal is now on another trade server
    with pytest.raises(BrokerError, match=r"daha önce bağlandığı sunucudan \(Demo-Server\) farklı bir sunucuya bağlı "
                                          r"\(Other-Server\)"):
        await fx(fake, tmp_path)
    assert fake.sent == []
    # the owner changed the server in config.yaml on purpose (a new account of the same firm): accepted, re-pinned
    b = await fx(fake, tmp_path, server="Other-Server")
    await b.close()
    assert read_pins(str(tmp_path))["fx"]["server"] == "Other-Server"


async def test_an_explicit_terminal_path_always_wins(tmp_path: Path) -> None:
    fake = TwoTerminals()
    await (await fx(fake, tmp_path)).close()                         # pinned to FundingPips
    fake.terms[OTHER].update(company="FundingPips Ltd")
    fake.calls.clear()
    b = await fx(fake, tmp_path, tickmill_guard(tmp_path), terminal_path=OTHER + r"\terminal64.exe")
    await b.close()
    assert [args for args, kw in inits(fake) if "login" in kw] == [(OTHER + r"\terminal64.exe",)]


async def test_an_explicit_terminal_path_of_the_tickmill_terminal_is_refused(tmp_path: Path) -> None:
    fake = TwoTerminals()
    with pytest.raises(BrokerError, match="'tickmill' hesabının terminaline"):
        await fx(fake, tmp_path, tickmill_guard(), terminal_path=TM + r"\terminal64.exe")


async def test_a_relogin_after_a_lost_terminal_link_uses_the_pin_and_refuses_another_terminal(tmp_path: Path) -> None:
    fake = TwoTerminals()
    b = await fx(fake, tmp_path, tickmill_guard())
    try:
        fake.default = TM
        fake.fail_once["account_info"] = (-10004, "No IPC connection")
        assert await b.get_free_margin() == 9_000.0                  # re-logged in, by the pinned path
        assert inits(fake)[-1][0] == (FP + r"\terminal64.exe",) and fake.terms[TM]["login"] == 7007
        fake.terms[FP]["company"] = "Tickmill Ltd"                   # the terminal at that path is not fx's any more
        fake.fail_once["account_info"] = (-10004, "No IPC connection")
        with pytest.raises(BrokerError):
            await b.get_free_margin()
        assert b._attach_problem is not None and "daha önce kullandığı terminal" in b._attach_problem
        # from now on nothing reaches the terminal: every call (and the heartbeat's check) says why
        with pytest.raises(BrokerError, match="daha önce kullandığı terminal"):
            await b.resolve_symbol("ETH", "USDT")
        assert "daha önce kullandığı terminal" in (await b.health_problem() or "") and fake.sent == []
    finally:
        await b.close()


async def test_fx_alone_on_the_computer_connects_exactly_as_before(tmp_path: Path) -> None:
    fake = TwoTerminals()
    b = Mt5Broker(make_cfg(), mt5_module=fake)             # no pin folder, no other MetaTrader account
    await b.connect()
    await b.close()
    ((args, kw),) = inits(fake)
    assert args == () and kw["login"] == 5001 and [c[0] for c in fake.calls[:2]] == ["initialize", "account_info"]


def test_the_registry_gives_fx_its_pin_folder_and_the_tickmill_markers(tmp_path: Path) -> None:
    from copytrader.brokers.registry import create_broker
    from copytrader.config import AppConfig

    data = {
        "data_dir": str(tmp_path), "telegram": {"api_id": 1, "api_hash": "h", "source_chats": [-1]},
        "notifier": {"bot_token": "1:x", "owner_user_id": 1},
        "brokers": [
            {"name": "fx", "type": "mt5", "login": 5001, "password": "pw", "server": "FundingPips-SIM"},
            {"name": "tickmill", "type": "mt5_bridge", "optional": True, "broker_company": "Tickmill",
             "terminal_path": TM + r"\terminal64.exe"},
        ],
    }
    config = AppConfig.model_validate(data)
    fx_broker = create_broker(config.brokers[0], dry_run=False, app_config=config)
    tick = create_broker(config.brokers[1], dry_run=False, app_config=config)
    assert isinstance(fx_broker, Mt5Broker) and fx_broker._pin_dir == str(tmp_path)
    assert fx_broker._guard.markers == {"tickmill": "tickmill"}
    assert fx_broker._guard.folders == {terminal_key(TM + r"\terminal64.exe"): "tickmill"}
    assert type(tick).__name__ == "Mt5BridgeBroker" and tick.kind == "mt5_bridge"
    # one terminal for both accounts is refused already when the config is loaded
    data["brokers"][0]["terminal_path"] = TM.lower() + r"\terminal64.exe"
    with pytest.raises(ValueError, match="her hesap kendi MetaTrader terminalini kullanmalı"):
        AppConfig.model_validate(data)


async def test_no_order_is_ever_sent_through_a_refused_terminal(tmp_path: Path) -> None:
    fake = TwoTerminals()
    fake.default = TM
    b = Mt5Broker(make_cfg(), mt5_module=fake, pin_dir=str(tmp_path), guard=tickmill_guard())
    with pytest.raises(BrokerError):
        await b.connect()
    sym = SymbolInfo(broker_symbol="ETHUSD", base_asset="ETH", quote_asset="USD", qty_step=0.01, min_qty=0.01, price_tick=0.01)
    with pytest.raises(BrokerError, match="hesabının terminaline bağlandı"):
        await b.open_position(make_req(sym, 0.1, make_plan()))
    assert fake.sent == []


async def test_when_the_package_reaches_tickmill_fx_logs_in_at_the_only_other_installed_terminal(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from copytrader.brokers import mt5_broker

    monkeypatch.setattr(mt5_broker, "installed_terminals", lambda roots=None: [FP, TM])
    fake = TwoTerminals()
    fake.default = TM
    b = await fx(fake, tmp_path, tickmill_guard())
    await b.close()
    assert fake.logins == [(FP, 5001)] and fake.terms[TM]["login"] == 7007
    assert read_pins(str(tmp_path))["fx"]["folder"] == FP


async def test_no_log_in_without_a_path_while_a_tickmill_terminal_is_installed(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from copytrader.brokers import mt5_broker

    fake = TwoTerminals()
    original = fake.initialize

    def initialize(*args: Any, **kwargs: Any) -> bool:
        if not args and "login" not in kwargs:          # the attach-only look: no terminal is logged in
            fake._log("initialize", *args, **kwargs)
            fake.last = (-6, "Terminal: Authorization failed")
            return False
        return original(*args, **kwargs)

    fake.initialize = initialize                          # type: ignore[method-assign]
    monkeypatch.setattr(mt5_broker, "installed_terminals", lambda roots=None: [TM])
    with pytest.raises(BrokerError, match="bu hesabın MetaTrader terminali belirlenemedi"):
        await fx(fake, tmp_path, tickmill_guard())
    assert fake.logins == []
    # nothing installed in Program Files, but tickmill's terminal is configured: still refused (review round 2)
    monkeypatch.setattr(mt5_broker, "installed_terminals", lambda roots=None: [])
    with pytest.raises(BrokerError, match="bu hesabın MetaTrader terminali belirlenemedi"):
        await fx(fake, tmp_path, tickmill_guard())
    assert fake.logins == []
    # no other account's terminal known at all (today's computer, tickmill on MetaApi): as before
    b = await fx(fake, tmp_path, metaapi_guard())
    await b.close()
    assert fake.logins == [(FP, 5001)]


def test_installed_terminals_are_found_one_level_deep(tmp_path: Path) -> None:
    from copytrader.brokers.mt5_broker import installed_terminals

    for folder in ("FundingPips MT5 Terminal", "Tickmill/MT5", "Nothing here"):
        (tmp_path / folder).mkdir(parents=True)
    (tmp_path / "FundingPips MT5 Terminal" / "terminal64.exe").write_bytes(b"MZ")
    (tmp_path / "Tickmill" / "MT5" / "terminal64.exe").write_bytes(b"MZ")
    assert installed_terminals([tmp_path, tmp_path / "missing"]) == [
        str(tmp_path / "FundingPips MT5 Terminal"), str(tmp_path / "Tickmill" / "MT5")]


# --------------------------------------------------------------------------- #
# review fixes: credentials only to a terminal that passed the check WITHOUT them; clear text for a moved pin
# --------------------------------------------------------------------------- #
X = r"C:\Program Files\MetaTrader 5"           # the friend's Tickmill terminal in a folder without "Tickmill"


def friend_in_x(fake: TwoTerminals) -> None:
    fake.terms[X] = fake.terms.pop(TM)          # company "Tickmill Ltd", logged in to the friend's 7007


def metaapi_guard() -> TerminalGuard:
    """tickmill still on MetaApi (no folder named Tickmill was found): only its name is known."""
    tick = SimpleNamespace(name="tickmill", type="metaapi", terminal_path=None, broker_company=None)
    return build_guard("fx", [tick], None, ["fx", "Demo-Server", ""])


async def test_the_terminal_the_look_refused_is_never_logged_in_to(tmp_path: Path,
                                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    from copytrader.brokers import mt5_broker

    monkeypatch.setattr(mt5_broker, "installed_terminals", lambda roots=None: [X])
    fake = TwoTerminals()
    friend_in_x(fake)
    fake.default = X
    with pytest.raises(BrokerError, match="'tickmill' hesabının terminaline bağlandı"):
        await fx(fake, tmp_path, metaapi_guard())
    assert fake.logins == [] and fake.terms[X]["login"] == 7007          # the owner's account never went there


async def test_a_scanned_candidate_is_judged_without_credentials_before_the_log_in(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from copytrader.brokers import mt5_broker

    monkeypatch.setattr(mt5_broker, "installed_terminals", lambda roots=None: [X])
    fake = TwoTerminals()
    friend_in_x(fake)
    original = fake.initialize

    def initialize(*args: Any, **kwargs: Any) -> bool:
        if not args and "login" not in kwargs:      # the first look: no terminal is running and logged in
            fake._log("initialize", *args, **kwargs)
            fake.last = (-6, "Terminal: Authorization failed")
            return False
        return original(*args, **kwargs)

    fake.initialize = initialize                      # type: ignore[method-assign]
    with pytest.raises(BrokerError, match="bu hesabın MetaTrader terminali belirlenemedi"):
        await fx(fake, tmp_path, metaapi_guard())
    looks = [(a, k) for a, k in inits(fake) if a == (X + r"\terminal64.exe",)]
    assert looks and all("login" not in k for _, k in looks)          # it was looked at, never logged in to
    assert fake.logins == [] and fake.terms[X]["login"] == 7007
    # the owner's own terminal as the one candidate: looked at first, then the log-in by its path
    monkeypatch.setattr(mt5_broker, "installed_terminals", lambda roots=None: [FP])
    fake.calls.clear()
    b = await fx(fake, tmp_path, metaapi_guard())
    await b.close()
    calls = inits(fake)
    look = calls.index(((FP + r"\terminal64.exe",), {"timeout": mt5_broker.LOOK_TIMEOUT_MS}))
    login = next(i for i, (a, k) in enumerate(calls) if "login" in k)
    assert look < login and fake.logins == [(FP, 5001)]


async def test_a_moved_pinned_terminal_names_the_pin_and_the_way_out(tmp_path: Path) -> None:
    fake = TwoTerminals()
    await (await fx(fake, tmp_path)).close()
    fake.terms[r"D:\FundingPips MT5"] = fake.terms.pop(FP)               # moved: nothing at the pinned path any more
    with pytest.raises(BrokerError) as err:
        await fx(fake, tmp_path)
    text = str(err.value)
    assert f"Bu hesap daha önce {FP}\\terminal64.exe terminaline bağlanmıştı ve bot yalnızca o terminale bağlanır" in text
    assert "config.yaml'da bu hesaba yeni terminal_path yazın ya da" in text and "mt5_terminal_fx.json dosyasını" in text


async def test_the_look_has_its_own_short_limit_and_the_connect_limit_grows_with_it(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from copytrader.brokers import mt5_broker

    seen: list[float] = []
    real = Mt5Broker._run

    async def run(self: Mt5Broker, fn: Any, *, timeout: float | None = None, what: str = "call", **kw: Any) -> Any:
        if what == "initialize":
            seen.append(float(timeout or 0.0))
        return await real(self, fn, timeout=timeout, what=what, **kw)

    monkeypatch.setattr(Mt5Broker, "_run", run)
    fake = TwoTerminals()
    b = await fx(fake, tmp_path, tickmill_guard())                       # no pin yet: it looks first
    await b.close()
    assert inits(fake)[0] == ((), {"timeout": mt5_broker.LOOK_TIMEOUT_MS}) and mt5_broker.LOOK_TIMEOUT_MS == 10_000
    assert seen == [mt5_broker.INIT_TIMEOUT_SEC + 10.0 * (1 + mt5_broker.MAX_TERMINAL_LOOKS)]
    b = await fx(fake, tmp_path, tickmill_guard())                       # pinned now: one log-in by path
    await b.close()
    assert seen[-1] == mt5_broker.INIT_TIMEOUT_SEC


async def test_no_pathless_log_in_while_another_accounts_terminal_is_configured(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Safety review round 2 (test_K): tickmill on mt5_bridge with its terminal outside Program Files, fx without a
    pin, the look fails and the scan finds nothing: fx must refuse, never log in without a path (it could land on the
    friend's terminal)."""
    from copytrader.brokers import mt5_broker

    y = r"D:\Tickmill"
    monkeypatch.setattr(mt5_broker, "installed_terminals", lambda roots=None: [])
    fake = TwoTerminals()
    fake.terms[y] = fake.terms.pop(TM)
    fake.default = y
    original = fake.initialize
    calls = {"n": 0}

    def initialize(*args: Any, **kwargs: Any) -> bool:
        calls["n"] += 1
        if calls["n"] == 1:                              # the 10 s look times out (the terminal is still starting)
            fake._log("initialize", *args, **kwargs)
            fake.last = (-10005, "IPC timeout")
            return False
        return original(*args, **kwargs)

    fake.initialize = initialize                          # type: ignore[method-assign]
    tick = SimpleNamespace(name="tickmill", type="mt5_bridge", terminal_path=y + r"\terminal64.exe",
                           broker_company="Tickmill")
    guard = build_guard("fx", [tick], None, ["fx", "Demo-Server", ""])
    with pytest.raises(BrokerError, match="bu hesabın MetaTrader terminali belirlenemedi"):
        await fx(fake, tmp_path, guard)
    assert fake.logins == [] and fake.terms[y]["login"] == 7007
    # nothing of another account known (tickmill on MetaApi, today's setup): the log-in without a path as before
    fake.terms[FP]["login"] = 5001
    fake.default = FP
    calls["n"] = 0
    b = await fx(fake, tmp_path, metaapi_guard())
    await b.close()
    assert fake.logins == [(FP, 5001)]
