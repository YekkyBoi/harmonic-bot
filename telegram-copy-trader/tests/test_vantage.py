"""The owner's own Vantage MetaTrader 5 account ("vantage"), added by migrate step 12.

Owner's decisions: GBP account, 1R = 20 GBP; the kripto and fx topics; no limit order - a LIMIT signal opens at
market once VANTAGE'S OWN price touches the entry (``entry_trigger: own_touch``: the reference_broker watch on the
account itself, zero offset, no chase, stop / first target passed -> cancelled); market signals at once; no
question (confirm_open: false); no daily loss brake; optional (the bot runs without it) but the OWNER's account
(friend_account: false: his karne and cards, never the friend's).

  config      own_touch only on MetaTrader accounts, never with reference_exchange / reference_broker; risk_currency
  migrate     block + both routes only with the three VANTAGE_* variables and the terminal found; idempotent;
              comments kept (text edit), CRLF, flow and block lists, the rewrite fallback; never a value in a note
  engine      on the production replica (tests/test_preflight_scenarios.py) migrated as the update leaves it, with
              fx2 (tests/test_own_accounts_no_confirm.py) and vantage on a fake GBP terminal ("EURUSD+" symbols)
  privacy     nothing the friend's chat gets names vantage; his karne / cards never hold it
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from copytrader import app as app_mod
from copytrader import migrate
from copytrader.brokers import mt5_broker
from copytrader.brokers.mt5_broker import MT5, Mt5Broker
from copytrader.config import AppConfig, is_friend_account, load_config
from copytrader.engine.monitor import is_virtual_pending
from copytrader.engine.orchestrator import own_touch_entry, waiting_entry_text
from copytrader.models import EntryType, PositionStatus, Severity, Side
from copytrader.telegram import notifier as notifier_mod

import test_own_accounts_no_confirm as own
import test_preflight_scenarios as tps
from fakes import FakeNotifier
from test_mt5_broker import make_symbol
from test_preflight_scenarios import (  # noqa: F401  (_isolate: autouse)
    DUMMY_ENV,
    FX,
    KRIPTO,
    MT_SYMBOLS,
    USD_PER,
    FxTerminal,
    Prod,
    _isolate,
    btc,
    fx_pair,
)

VANTAGE_EXE = r"C:\Program Files\Vantage International MT5\terminal64.exe"
VANTAGE_ENV = {"VANTAGE_LOGIN": "8001", "VANTAGE_PASSWORD": "vantage-secret-pw",
               "VANTAGE_SERVER": "VantageInternational-Live 3"}
GBP_USD = USD_PER["GBP"]                      # 1.315: 1 GBP in USD (the fake terminals' rate)
T0 = tps.SERVER_TIME


# =================================================================================================================
# config
# =================================================================================================================
def _app(**vantage: Any) -> AppConfig:
    block = dict(name="vantage", type="mt5_bridge", terminal_path=VANTAGE_EXE, broker_company="Vantage", **vantage)
    return AppConfig.model_validate({
        "telegram": {"api_id": 1, "api_hash": "x", "source_chats": [1]},
        "notifier": {"bot_token": "t", "owner_user_id": 1},
        "brokers": [block, {"name": "binance", "type": "ccxt", "exchange": "binanceusdm"}],
    })


def test_config_own_touch_on_a_metatrader_account() -> None:
    cfg = _app(entry_trigger="own_touch", risk_usd=20, risk_currency="gbp")
    vantage = cfg.brokers[0]
    assert vantage.entry_trigger == "own_touch" and vantage.risk_currency == "GBP"
    assert _app().brokers[0].risk_currency is None and _app().brokers[0].entry_trigger == "broker_limit"


@pytest.mark.parametrize("extra", [{"reference_exchange": "binanceusdm"}, {"reference_broker": "binance"}])
def test_config_own_touch_is_never_combined_with_another_reference(extra: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="own_touch hesabın kendi fiyatını izler"):
        _app(entry_trigger="own_touch", **extra)


def test_config_own_touch_needs_a_metatrader_account() -> None:
    with pytest.raises(ValueError, match="yalnızca MetaTrader hesaplarında"):
        AppConfig.model_validate({
            "telegram": {"api_id": 1, "api_hash": "x", "source_chats": [1]},
            "notifier": {"bot_token": "t", "owner_user_id": 1},
            "brokers": [{"name": "binance", "type": "ccxt", "exchange": "binanceusdm", "entry_trigger": "own_touch"}],
        })


def test_config_risk_currency_must_be_a_currency_code() -> None:
    with pytest.raises(ValueError, match="risk_currency"):
        _app(risk_currency="pound sterling")


# =================================================================================================================
# migrate step 12
# =================================================================================================================
def _server_yaml(*, crlf: bool = False, block_routes: bool = False) -> str:
    """LIVE_YAML + fx2 + challenge2, as the server's config is before this update."""
    text = tps.LIVE_YAML.replace("  - name: tickmill\n", own.FX2_BLOCK + "  - name: tickmill\n", 1)
    text = text.replace("notifier:\n", own.CHALLENGE2_ROUTE + "notifier:\n", 1)
    text = text.replace("brokers:\n  - name: binance\n",
                        "brokers:\n  # the owner's comment above his first account\n  - name: binance\n", 1)
    if block_routes:
        text = text.replace("      brokers: [fx, tickmill]\n", "      brokers:\n        - fx\n        - tickmill\n", 1)
    return text.replace("\n", "\r\n") if crlf else text


def _write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, text: str) -> Path:
    for name, value in DUMMY_ENV.items():
        monkeypatch.setenv(name, value)
    for name in VANTAGE_ENV:
        monkeypatch.delenv(name, raising=False)
    folder = tmp_path / "guncelleme"
    folder.mkdir(exist_ok=True)
    path = folder / "config.yaml"
    path.write_bytes(text.encode("utf-8"))
    monkeypatch.chdir(folder)
    return path


def _migrate(path: Path, env: dict[str, str] | None, *, notes: list[str] | None = None,
             vantage: tuple[str | None, str | None] | None = (VANTAGE_EXE, None)) -> list[str]:
    return migrate.migrate_config(path, notes=notes, terminal=(None, None), env={} if env is None else env,
                                  vantage=vantage)


def _vantage_lines(lines: list[str]) -> list[str]:
    return [ln for ln in lines if "vantage" in ln.lower()]


def _no_secret(texts: list[str]) -> None:
    for text in texts:
        for value in VANTAGE_ENV.values():
            assert value not in text, text


def test_migrate_without_the_env_lines_adds_nothing_and_says_so(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _write(tmp_path, monkeypatch, _server_yaml())
    notes: list[str] = []
    lines = _migrate(path, None, notes=notes)
    assert _vantage_lines(lines) == []
    assert "Vantage eklenmedi: .env'de VANTAGE_LOGIN/PASSWORD/SERVER yok; adımlar: docs/VANTAGE_EKLE.md" in notes
    assert "vantage" not in path.read_text(encoding="utf-8")
    # one of the three missing: named, still nothing added
    notes = []
    lines = _migrate(path, {"VANTAGE_LOGIN": "8001", "VANTAGE_PASSWORD": "x"}, notes=notes)
    assert lines == [] and any("(eksik: VANTAGE_SERVER)" in n for n in notes)
    assert "vantage" not in path.read_text(encoding="utf-8")


@pytest.mark.parametrize("env, why", [
    ({**VANTAGE_ENV, "VANTAGE_LOGIN": "80 01x"}, "VANTAGE_LOGIN bir hesap numarası değil"),
    ({**VANTAGE_ENV, "VANTAGE_SERVER": "SomeOtherBroker-Live"}, "VANTAGE_SERVER adında 'Vantage' geçmiyor"),
])
def test_migrate_refuses_unusable_env_values_without_showing_them(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                                  env: dict[str, str], why: str) -> None:
    path = _write(tmp_path, monkeypatch, _server_yaml())
    notes: list[str] = []
    _migrate(path, env, notes=notes)
    assert "vantage" not in path.read_text(encoding="utf-8")
    assert any(why in n for n in notes)
    for n in notes:
        assert env["VANTAGE_LOGIN"] not in n and env["VANTAGE_SERVER"] not in n and env["VANTAGE_PASSWORD"] not in n


def test_migrate_without_the_terminal_adds_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _write(tmp_path, monkeypatch, _server_yaml())
    notes: list[str] = []
    lines = _migrate(path, VANTAGE_ENV, notes=notes, vantage=migrate.vantage_terminal({}, roots=[tmp_path / "pf"]))
    assert _vantage_lines(lines) == [] and "vantage" not in path.read_text(encoding="utf-8")
    (note,) = [n for n in notes if n.startswith("Vantage eklenmedi")]
    assert "Vantage MT5 terminali bulunamadı" in note and "--vantage-terminal" in note


def test_migrate_never_takes_another_accounts_terminal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _write(tmp_path, monkeypatch, _server_yaml())
    notes: list[str] = []
    lines = _migrate(path, VANTAGE_ENV, notes=notes, vantage=(own.BEM_EXE, None))
    assert _vantage_lines(lines) == [] and any("'fx2' hesabının terminali" in n for n in notes)


def test_the_vantage_terminal_is_found_in_program_files(tmp_path: Path) -> None:
    pf = tmp_path / "Program Files"
    (pf / "Vantage International MT5").mkdir(parents=True)
    (pf / "Vantage International MT5" / "terminal64.exe").write_bytes(b"")
    (pf / "Tickmill MT5 Terminal").mkdir()
    (pf / "Tickmill MT5 Terminal" / "terminal64.exe").write_bytes(b"")
    assert migrate.vantage_terminal({}, roots=[pf]) == (str(pf / "Vantage International MT5" / "terminal64.exe"), None)
    assert migrate.find_tickmill_terminals([pf]) == [pf / "Tickmill MT5 Terminal" / "terminal64.exe"]
    # two Vantage terminals: none is guessed
    (pf / "Vantage MT5 2").mkdir()
    (pf / "Vantage MT5 2" / "terminal64.exe").write_bytes(b"")
    path, why = migrate.vantage_terminal({}, roots=[pf])
    assert path is None and "birden fazla Vantage MT5 terminali" in str(why)
    # the override wins
    exe = pf / "Vantage MT5 2" / "terminal64.exe"
    assert migrate.vantage_terminal({migrate.VANTAGE_TERMINAL_ENV: str(exe.parent)}, roots=[pf]) == (str(exe), None)


@pytest.mark.parametrize("crlf", [False, True])
@pytest.mark.parametrize("block_routes", [False, True])
def test_migrate_adds_the_block_and_both_routes_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, crlf: bool,
                                                     block_routes: bool) -> None:
    path = _write(tmp_path, monkeypatch, _server_yaml(crlf=crlf, block_routes=block_routes))
    notes: list[str] = []
    lines = _migrate(path, VANTAGE_ENV, notes=notes)
    head, kripto, fx = _vantage_lines(lines)
    assert head.startswith("vantage: Vantage MT5 hesabı eklendi") and "1R = 20 GBP" in head
    assert kripto == "kripto konusu: sinyaller vantage hesabında da açılacak (brokers: [binance, breakout, tickmill, " \
                     "vantage])"
    assert fx == "fx konusu: sinyaller vantage hesabında da açılacak (brokers: [fx, tickmill, vantage])"
    assert not any("korunamadı" in n for n in notes)                       # a text edit: comments kept
    _no_secret([*lines, *notes])
    raw = path.read_bytes().decode("utf-8")
    assert "# the owner's comment above his first account" in raw
    assert ("\r\n" in raw) is crlf and (raw.count("\n") == raw.count("\r\n")) is crlf
    for value in VANTAGE_ENV.values():
        assert value not in raw                                            # only the placeholders are written
    data = yaml.safe_load(raw)
    block = data["brokers"][-1]
    assert block == {
        "name": "vantage", "type": "mt5_bridge", "enabled": True, "optional": True, "friend_account": False,
        "terminal_path": VANTAGE_EXE, "broker_company": "Vantage", "login": "${VANTAGE_LOGIN:-}",
        "password": "${VANTAGE_PASSWORD:-}", "server": "${VANTAGE_SERVER:-}", "risk_usd": 20, "risk_currency": "GBP",
        "daily_loss_limit_pct": 0, "confirm_open": False, "entry_trigger": "own_touch"}
    routes = {t["name"]: t["brokers"] for t in data["telegram"]["topics"]}
    assert routes == {"kripto": ["binance", "breakout", "tickmill", "vantage"], "fx": ["fx", "tickmill", "vantage"],
                      "challenge2": ["fx2"]}
    # the bot loads it: the owner's account, optional, sized in GBP, no brake, no question, its own price
    for name, value in VANTAGE_ENV.items():
        monkeypatch.setenv(name, value)
    cfg = load_config(path)
    v = next(b for b in cfg.brokers if b.name == "vantage")
    assert (v.login, v.server, v.password) == (8001, VANTAGE_ENV["VANTAGE_SERVER"], VANTAGE_ENV["VANTAGE_PASSWORD"])
    assert v.optional and not is_friend_account(v) and v.confirm_open is False and v.daily_loss_limit_pct == 0
    assert v.lot_rounding == "down" and v.size_from_signal_entry is False and v.fees_in_risk is None
    # a second run changes nothing and writes no backup
    backups = sorted(p.name for p in path.parent.iterdir() if "yedek" in p.name)
    before = path.read_bytes()
    notes = []
    assert _migrate(path, VANTAGE_ENV, notes=notes) == [] and notes == []
    assert path.read_bytes() == before
    assert sorted(p.name for p in path.parent.iterdir() if "yedek" in p.name) == backups


@pytest.mark.parametrize("with_env_file", [True, False])
def test_the_servers_migrate_reads_the_three_lines_from_the_env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                                     with_env_file: bool) -> None:
    """As on the server: ``python -m copytrader migrate`` (run_cli) with no environment given; the config loader
    loads the .env beside config.yaml."""
    path = _write(tmp_path, monkeypatch, _server_yaml())
    if with_env_file:
        (path.parent / ".env").write_text("".join(f"{k}={v}\n" for k, v in VANTAGE_ENV.items()), encoding="utf-8")
    monkeypatch.setattr(migrate, "vantage_terminal", lambda env=None, roots=None: (VANTAGE_EXE, None))
    import os
    for name in VANTAGE_ENV:            # (load_config writes the .env values into os.environ: undone after the test)
        monkeypatch.setenv(name, "")
        monkeypatch.delenv(name)
    out: list[str] = []
    assert migrate.run_cli(str(path), out.append) == 0
    text = "\n".join(out)
    assert ("vantage: Vantage MT5 hesabı eklendi" in text) is with_env_file
    assert ("Vantage eklenmedi: .env'de VANTAGE_LOGIN/PASSWORD/SERVER yok" in text) is not with_env_file
    assert ("name: vantage" in path.read_text(encoding="utf-8")) is with_env_file
    _no_secret(out)
    for name in VANTAGE_ENV:
        os.environ.pop(name, None)


def test_the_migrated_config_still_loads_when_the_env_lines_are_removed(tmp_path: Path,
                                                                        monkeypatch: pytest.MonkeyPatch) -> None:
    """``${VAR:-}`` placeholders: without the three lines the bot still starts; vantage then attaches to its terminal's
    own log-in and is still refused on anything but a Vantage account (broker_company)."""
    path = _write(tmp_path, monkeypatch, _server_yaml())
    _migrate(path, VANTAGE_ENV)
    cfg = load_config(path)                                 # (VANTAGE_* are not set in this process)
    v = next(b for b in cfg.brokers if b.name == "vantage")
    assert v.login is None and v.password is None and v.server is None and v.broker_company == "Vantage"
    assert Mt5Broker(v)._attach_only
    # a log-in only with all three parts; any one missing: attached like an account without credentials
    assert not Mt5Broker(v.model_copy(update={"login": 8001, "password": "x", "server": "Vantage-Live"}))._attach_only
    assert Mt5Broker(v.model_copy(update={"login": 8001, "password": "x"}))._attach_only
    assert Mt5Broker(v.model_copy(update={"password": "x", "server": "Vantage-Live"}))._attach_only


def test_a_route_listing_every_account_is_left_alone(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _write(tmp_path, monkeypatch, _server_yaml().replace("      brokers: [fx, tickmill]\n", "", 1))
    notes: list[str] = []
    lines = _migrate(path, VANTAGE_ENV, notes=notes)
    assert [ln.split(":")[0] for ln in _vantage_lines(lines)] == ["vantage", "kripto konusu"]
    assert any("'fx' konusunun broker listesi boş" in n for n in notes)


def test_the_rewrite_fallback_adds_the_same(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _write(tmp_path, monkeypatch, _server_yaml())

    def unsupported(*_a: Any, **_k: Any) -> str:
        raise migrate._TextEditUnsupported("test")

    monkeypatch.setattr(migrate, "_edit_text", unsupported)
    notes: list[str] = []
    lines = _migrate(path, VANTAGE_ENV, notes=notes)
    assert len(_vantage_lines(lines)) == 3 and any("korunamadı" in n for n in notes)
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert data["brokers"][-1]["entry_trigger"] == "own_touch" and "${VANTAGE_LOGIN:-}" in path.read_text("utf-8")


def test_a_block_this_version_could_not_load_is_never_written(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _write(tmp_path, monkeypatch, _server_yaml())
    monkeypatch.setattr(migrate, "_vantage_block", lambda p: {"name": "vantage", "type": "mt5_bridge",
                                                              "terminal_path": p, "no_such_key": True})
    notes: list[str] = []
    lines = _migrate(path, VANTAGE_ENV, notes=notes)
    assert _vantage_lines(lines) == [] and len(lines) > 0           # the update's other steps still went through
    assert any("Vantage eklenmedi: yeni ayarlar bu sürümle doğrulanamadı" in n for n in notes)
    assert "vantage" not in path.read_text(encoding="utf-8")


def test_the_append_text_edit_keeps_an_unusual_layout_out() -> None:
    text = "brokers:\n- name: a\n  type: paper\n"
    ch = migrate.Change((), "brokers", {"name": "b", "type": "paper"}, "", lambda c: True, op="append")
    assert migrate._edit_text(text, [ch]) == text + "- name: b\n  type: paper\n"
    flow = "brokers: [{name: a, type: paper}]\n"
    with pytest.raises(migrate._TextEditUnsupported):
        migrate._edit_text(flow, [ch])


# =================================================================================================================
# the engine: the production replica as the update leaves it, with vantage on a GBP terminal
# =================================================================================================================
VANTAGE_SYMBOLS = {"EURUSD+": "EURUSD", "GBPUSD+": "GBPUSD", "XAUUSD+": "XAUUSD", "BTCUSD": "BTCUSD",
                   "ETHUSD": "ETHUSD"}


class VantageTerminal(FxTerminal):
    """The Vantage MetaTrader 5 module: a GBP account (5,000 GBP), forex with a "+" suffix, profit in GBP."""

    def __init__(self) -> None:
        super().__init__(names=())
        self.terminal.path = VANTAGE_EXE.rsplit("\\", 1)[0]
        self.terminal.company = "Vantage International Group Limited"
        self.account.login = int(VANTAGE_ENV["VANTAGE_LOGIN"])
        self.account.company, self.account.server = "Vantage International Group Limited", VANTAGE_ENV["VANTAGE_SERVER"]
        self.account.currency = "GBP"
        self.account.equity = self.account.balance = self.account.margin_free = 5_000.0
        for name, plain in VANTAGE_SYMBOLS.items():
            base, quote, digits, contract, bid, ask = MT_SYMBOLS[plain]
            point = 10.0 ** -digits
            self.symbols[name] = make_symbol(
                name, digits=digits, point=point, trade_tick_size=point, trade_contract_size=contract,
                filling_mode=MT5.SYMBOL_FILLING_IOC, currency_profit=quote, currency_base=base, currency_margin=base,
                volume_max=100.0)
            self.set_price(name, bid, ask)
        self.set_price("BTCUSD", 73949.0, 73951.0)

    def order_calc_profit(self, action: int, symbol: str, volume: float, price_open: float, price_close: float) -> float:
        return super().order_calc_profit(action, symbol, volume, price_open, price_close) / GBP_USD   # USD -> GBP

    def tick(self, name: str, bid: float, ask: float, at: float) -> None:
        """The latest tick at server time ``at`` (seconds)."""
        self.ticks[name] = SimpleNamespace(bid=bid, ask=ask, last=0.0, time=int(at), time_msc=int(at * 1000))

    def history(self, ticks: list[tuple[float, float, float]]) -> list[tuple[Any, ...]]:
        """The tick history (server time, bid, ask) copy_ticks_from answers; returns the calls made to it."""
        calls: list[tuple[Any, ...]] = []
        rows = [{"time": int(t), "time_msc": int(t * 1000), "bid": b, "ask": a} for t, b, a in ticks]

        def copy_ticks_from(symbol: str, date_from: int, count: int, flags: int) -> list[dict[str, Any]]:
            calls.append((symbol, date_from, count, flags))
            return [r for r in rows if r["time"] >= date_from][:count]

        self.copy_ticks_from = copy_ticks_from                         # type: ignore[attr-defined]
        return calls


class FriendNotes:
    """What the friend's chat gets (the notifier's friend_note)."""

    def __init__(self, notifier: FakeNotifier) -> None:
        self.texts: list[str] = []
        notifier.friend_note = self.note          # type: ignore[attr-defined]

    async def note(self, text: str) -> None:
        self.texts.append(text)


async def vantage_world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, edit: Any = None) -> Prod:
    text = _server_yaml()
    path = _write(tmp_path, monkeypatch, text)
    _migrate(path, VANTAGE_ENV)
    for name, value in VANTAGE_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(tps, "LIVE_YAML", path.read_text(encoding="utf-8"))
    monkeypatch.setitem(tps.TITLES, own.CHALLENGE2, "Kasım Ayı Challenge İşlemleri")
    monkeypatch.setattr(mt5_broker, "_ACTIVE_LOGINS", {})
    bem, vt = own.bem_terminal(), VantageTerminal()
    vt.history([])                          # a real terminal answers copy_ticks_from (no ticks between the looks)

    def more(_p: Prod, by: dict[str, Any]) -> dict[str, Any]:
        out = {}
        for name, term in (("fx2", bem), ("vantage", vt)):
            broker = Mt5Broker(by[name], mt5_module=term)
            broker.retry_delay_sec = 0
            out[name] = broker
        return out

    p = await Prod.create(tmp_path, monkeypatch, replica="live", more=more, edit=edit)
    p.fx2, p.vantage = bem, vt                                     # type: ignore[attr-defined]
    p.friend = FriendNotes(p.notifier)                             # type: ignore[attr-defined]
    return p


@pytest.fixture
async def world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    p = await vantage_world(tmp_path, monkeypatch)
    yield p
    await p.close()


def vorders(p: Prod) -> list[tuple[Any, ...]]:
    return own.orders(p.vantage)                                   # type: ignore[attr-defined]


def pendings(term: FxTerminal) -> list[dict[str, Any]]:
    return term.sent_actions(MT5.TRADE_ACTION_PENDING)


async def settle(p: Prod) -> None:
    for _ in range(5):
        await asyncio.sleep(0)
    pending = [t for t in list(p.engine._tasks) if not t.done()]
    if pending:
        await asyncio.wait(pending, timeout=2.0)


def friend_clean(p: Prod) -> None:
    for text in p.friend.texts:                                    # type: ignore[attr-defined]
        assert "vantage" not in text.lower(), text


async def test_vantage_connects_as_the_owners_account_and_its_1r_reads_20_gbp(world) -> None:
    p = world
    assert "vantage" in p.cfg.telegram.topics[0].brokers and "vantage" in p.cfg.telegram.topics[1].brokers
    assert p.engine._currency["vantage"] == "GBP"
    assert "vantage 20 GBP" in p.engine._risk_per_r_text() and "fx 100 USD" in p.engine._risk_per_r_text()
    assert "vantage" not in p.engine.friend_account_names()
    assert not any("para birimi" in t for t in p.texts())          # GBP named, GBP reported: no warning


async def test_vantage_logs_in_with_the_env_credentials_at_its_own_terminal(world) -> None:
    p = world
    _name, args, kwargs = next(c for c in p.vantage.calls if c[0] == "initialize" and c[2].get("password"))
    assert args == (VANTAGE_EXE,)
    assert (kwargs["login"], kwargs["password"], kwargs["server"]) == (
        8001, VANTAGE_ENV["VANTAGE_PASSWORD"], VANTAGE_ENV["VANTAGE_SERVER"])
    for text in p.texts():
        assert VANTAGE_ENV["VANTAGE_PASSWORD"] not in text


async def test_a_suffixed_symbol_is_taken_when_the_bare_name_cannot_be_traded(world) -> None:
    p = world
    bare = make_symbol("EURUSD", trade_mode=MT5.SYMBOL_TRADE_MODE_DISABLED, currency_profit="USD",
                       currency_base="EUR")
    p.vantage.symbols["EURUSD"] = bare
    sym = await p.brokers["vantage"].resolve_symbol("EUR", "USD", tps.MarketType.FOREX, "EURUSD")
    assert sym is not None and sym.broker_symbol == "EURUSD+"
    gold = await p.brokers["vantage"].resolve_symbol("XAU", "USD", tps.MarketType.CFD, "XAUUSD")
    assert gold is not None and gold.broker_symbol == "XAUUSD+"


async def test_a_wrong_currency_label_is_told_once_at_connect(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def usd_label(cfg: AppConfig) -> None:
        next(b for b in cfg.brokers if b.name == "vantage").risk_currency = None
    notifier_texts: list[str] = []
    p = await vantage_world(tmp_path, monkeypatch, edit=usd_label)
    try:
        await p.engine._check_risk_currency(p.brokers["vantage"])
        notifier_texts = [t for t in p.texts(Severity.WARNING) if "para birimi" in t]
        (text,) = notifier_texts
        assert "vantage: hesabın para birimi GBP, config.yaml'da 1R 20 USD yazıyor" in text
        assert "stopta kayıp 20 GBP olur" in text and "risk_currency: GBP" in text
    finally:
        await p.close()


async def test_fx_market_opens_on_vantage_at_once_sized_for_20_gbp(world) -> None:
    p = world
    await p.say(fx_pair("EURUSD", Side.LONG, 1.1620, 1.1600, 1.1660), FX, "EUR LONG")
    # ask 1.16205, stop 1.1600: 205 USD = 155.89 GBP per lot -> 20 GBP = 0.128 lot -> 0.12 (20 USD would be 0.09)
    assert vorders(p) == [("BUY", "EURUSD+", 0.12, 1.16205, 1.16, 1.166)]
    assert p.asked() == []
    pos = (await p.positions(1))["vantage"]
    assert pos.status is PositionStatus.OPEN and pos.risk_amount == pytest.approx(20.0)
    friend_clean(p)


async def test_kripto_market_opens_on_vantage_at_once_with_the_signals_levels(world) -> None:
    p = world
    await p.say(btc(EntryType.MARKET, 74000.0, 73000.0, 75000.0), KRIPTO, "BTC LONG")
    ((kind, symbol, lots, price, sl, tp),) = vorders(p)
    assert (kind, symbol, price, sl, tp) == ("BUY", "BTCUSD", 73951.0, 73000.0, 75000.0)   # never shifted
    # 951 USD per lot (1 BTC) = 723.2 GBP -> 20 GBP = 0.0276 -> 0.02 (in USD it would be 0.021 -> 0.02 as well)
    assert lots == 0.02


async def test_fx_limit_never_a_limit_on_vantage_its_own_price_touch_opens_it_at_market(world) -> None:
    p = world
    entry, stop, tp = 1.1610, 1.1590, 1.1650
    p.vantage.tick("EURUSD+", 1.16195, 1.16205, T0)
    await p.say(fx_pair("EURUSD", Side.LONG, entry, stop, tp, entry_type=EntryType.LIMIT), FX, "EUR LONG Limit Emir")
    assert pendings(p.vantage) == [] and vorders(p) == []          # no limit order at Vantage
    assert p.fx_orders() == [("BUY_LIMIT", "EURUSD", 0.5, entry, stop, tp)]            # fx: its limit, as before
    pos = (await p.positions(1))["vantage"]
    assert is_virtual_pending(pos) and own_touch_entry(pos) and pos.data["reference_broker"] == "vantage"
    assert pos.broker_symbol == "EURUSD+" and pos.data["trigger_price"] == entry
    assert waiting_entry_text(pos, owner=True) == "giriş 1.161 · vantage fiyatı seviyeye gelince"
    (note,) = [ln for t in p.texts() for ln in t.splitlines() if "vantage: sanal tetik kuruldu" in ln]
    assert note.endswith("vantage: sanal tetik kuruldu (limit emir konmadı): vantage fiyatı 1.161 seviyesine gelince "
                         "piyasa emriyle açılacak (en çok 24 saat beklenir)")
    await p.engine.monitor.run_virtual_once()                      # its first look starts the watch
    assert vorders(p) == []
    # Vantage's own ask reaches the entry (fx and tickmill do not move): MARKET at Vantage, levels as signalled
    p.vantage.tick("EURUSD+", 1.16090, 1.16098, T0 + 2)
    await p.engine.monitor.run_virtual_once()
    # ask 1.16098, stop 1.1590: 198 USD = 150.57 GBP per lot -> 0.1328 -> 0.13 lot
    assert vorders(p) == [("BUY", "EURUSD+", 0.13, 1.16098, stop, tp)]
    assert p.tickmill_orders() == [] and len(p.fx_orders()) == 1       # tickmill still waits for fx's fill
    pos = (await p.positions(1))["vantage"]
    assert pos.status is PositionStatus.OPEN and not is_virtual_pending(pos)
    for _ in range(3):
        await p.engine.monitor.run_virtual_once()
    assert len(vorders(p)) == 1                                    # once
    await settle(p)
    friend_clean(p)


async def test_kripto_limit_waits_for_vantages_own_btc_price_not_binance(world) -> None:
    p = world
    p.vantage.tick("BTCUSD", 73949.0, 73951.0, T0)
    await p.say(btc(EntryType.LIMIT, 73500.0, 73000.0, 75000.0), KRIPTO, "BTC LONG limit")
    assert pendings(p.vantage) == [] and vorders(p) == []
    vpos = (await p.positions(1))["vantage"]
    assert own_touch_entry(vpos) and "reference_exchange" not in vpos.data
    await p.engine.monitor.run_virtual_once()
    p.binance.set_price("BTC", 73400.0, 73401.0)                   # Binance below the entry: not Vantage's trigger
    await p.engine.monitor.run_virtual_once()
    assert vorders(p) == []
    p.vantage.tick("BTCUSD", 73490.0, 73495.0, T0 + 2)             # Vantage's own ask <= 73500
    await p.engine.monitor.run_virtual_once()
    ((kind, symbol, _lots, price, sl, tp),) = vorders(p)
    assert (kind, symbol, price, sl, tp) == ("BUY", "BTCUSD", 73495.0, 73000.0, 75000.0)


async def test_a_touch_the_price_ran_away_from_is_not_chased(world) -> None:
    p = world
    p.vantage.tick("EURUSD+", 1.16195, 1.16205, T0)
    await p.say(fx_pair("EURUSD", Side.LONG, 1.1610, 1.1590, 1.1650, entry_type=EntryType.LIMIT), FX, "EUR Limit")
    await p.engine.monitor.run_virtual_once()
    p.vantage.history([(T0 + 0.5, 1.16090, 1.16098)])             # a wick to the entry between two looks ...
    p.vantage.tick("EURUSD+", 1.16245, 1.16255, T0 + 1.5)          # ... and 15 pips away now
    await p.engine.monitor.run_virtual_once()
    assert vorders(p) == []
    pos = (await p.positions(1))["vantage"]
    assert is_virtual_pending(pos) and pos.data["virtual_missed"]
    assert any("tetik seviyesi görüldü ama fiyat uzaklaştı; vantage'da açılmadı" in t for t in p.texts())
    p.vantage.tick("EURUSD+", 1.16090, 1.16100, T0 + 3.0)          # back at the level: opens
    await p.engine.monitor.run_virtual_once()
    assert [o[:2] for o in vorders(p)] == [("BUY", "EURUSD+")]


@pytest.mark.parametrize("wick, why", [((1.15880, 1.15890), "vantage fiyatı (1.1588) stop seviyesini (1.159) geçti"),
                                       ((1.16505, 1.16515), "vantage fiyatı (1.16505) ilk hedefi (1.165) geçti")])
async def test_stop_or_first_target_before_the_touch_cancels_it(world, wick: tuple[float, float], why: str) -> None:
    p = world
    p.vantage.tick("EURUSD+", 1.16195, 1.16205, T0)
    await p.say(fx_pair("EURUSD", Side.LONG, 1.1610, 1.1590, 1.1650, entry_type=EntryType.LIMIT), FX, "EUR Limit")
    await p.engine.monitor.run_virtual_once()
    p.vantage.history([(T0 + 0.5, *wick)])
    p.vantage.tick("EURUSD+", 1.16300, 1.16310, T0 + 1.5)
    await p.engine.monitor.run_virtual_once()
    pos = (await p.positions(1))["vantage"]
    assert pos.status is PositionStatus.CANCELLED and vorders(p) == []
    (note,) = [t for t in p.texts() if "sanal tetik iptal edildi" in t and "vantage" in t]
    assert f"sanal tetik iptal edildi: {why}; giriş tetiklenmeden fiyat uzaklaştı, işlem AÇILMADI" in note


async def test_the_trade_ending_on_the_other_accounts_cancels_the_waiting_entry(world) -> None:
    """The inherited rule: the trade's limits elsewhere ended without a fill (fx and tickmill): Vantage's waiting entry
    is cancelled too, never opened later on its own."""
    p = world
    p.vantage.tick("EURUSD+", 1.16195, 1.16205, T0)
    await p.say(fx_pair("EURUSD", Side.LONG, 1.1610, 1.1590, 1.1650, entry_type=EntryType.LIMIT), FX, "EUR Limit")
    trade_id = 1
    for name, pos in (await p.positions(trade_id)).items():
        if name == "fx":
            await p.db.save_broker_position(trade_id, pos.model_copy(update={"status": PositionStatus.CANCELLED}))
    tm = (await p.positions(trade_id))["tickmill"]
    await p.db.save_broker_position(trade_id, tm.model_copy(update={"status": PositionStatus.CANCELLED}))
    await p.engine.monitor.run_virtual_once()
    assert (await p.positions(trade_id))["vantage"].status is PositionStatus.CANCELLED and vorders(p) == []


async def test_an_unsure_signal_opens_on_vantage_without_the_owners_answer(world) -> None:
    p = world
    await p.say(own.unsure(btc(EntryType.MARKET, 74000.0, 73000.0, 75000.0)), KRIPTO, "BTC LONG")
    assert p.asked()                                               # breakout is still asked ...
    assert [o[:2] for o in vorders(p)] == [("BUY", "BTCUSD")]      # ... vantage opened at once
    (report,) = p.reports()
    assert "• vantage: 0.02 BTCUSD (açık), risk 20.00 GBP, giriş ~73951\n" in report
    assert "⚡ binance, tickmill ve vantage için onay beklenmedi (confirm_open: false); diğer hesaplar için onay " \
           "soruldu" in report
    assert "(1R: binance 10 USD, breakout 50 USD, fx 100 USD, fx2 1000 USD, tickmill 50 USD, vantage 20 GBP)" in report


async def test_nothing_the_friend_gets_names_vantage(world) -> None:
    p = world
    await p.say(fx_pair("EURUSD", Side.LONG, 1.1620, 1.1600, 1.1660), FX, "EUR LONG")
    await p.say(fx_pair("GBPUSD", Side.LONG, 1.3140, 1.3120, 1.3180, entry_type=EntryType.LIMIT), FX, "GBP Limit")
    await settle(p)
    assert p.friend.texts                                          # (the friend does get his tickmill's notices)
    friend_clean(p)


# =================================================================================================================
# karne, cards, help
# =================================================================================================================
def test_vantage_is_on_the_owners_karne_and_cards_never_the_friends(tmp_path: Path,
                                                                    monkeypatch: pytest.MonkeyPatch) -> None:
    path = _write(tmp_path, monkeypatch, _server_yaml())
    _migrate(path, VANTAGE_ENV)
    cfg = load_config(path)
    assert "vantage" in app_mod.real_account_names(cfg, friend=False)
    assert "vantage" not in app_mod.real_account_names(cfg, friend=True)
    assert "vantage" in app_mod.card_account_names(cfg)
    assert app_mod.friend_card_account_names(cfg) == ["tickmill"]
    service = app_mod.build_tradecard(cfg, None, object(), type("E", (), {"brokers": []})(), None, str(tmp_path))
    assert service.command_account("vantagekart") == "vantage"
    assert service.command_account("vantagekart", friend=True) is None
    assert service._sources["vantage"] == ("mt5", None)
    assert "/vantagekart" in service.commands_text() and "vantage" not in service.commands_text(friend=True)
    assert "/vantagekart" in notifier_mod.HELP_TEXT


# =================================================================================================================
# one account's leg that never held the trade never ends another's waiting entry
# =================================================================================================================
def fx_watchable(p: Prod, ticks: list[tuple[float, float, float]] | None = None) -> None:
    """fx's terminal as tickmill watches it (its tick history: (server time, bid, ask))."""
    rows = [{"time": int(t), "time_msc": int(t * 1000), "bid": b, "ask": a} for t, b, a in ticks or []]
    p.terminal.copy_ticks_from = (                                  # type: ignore[attr-defined]
        lambda symbol, date_from, count, flags: [r for r in rows if r["time"] >= date_from][:count])


def fx_tick(p: Prod, bid: float, ask: float, at: float) -> None:
    p.terminal.ticks["EURUSD"] = SimpleNamespace(bid=bid, ask=ask, last=0.0, time=int(at), time_msc=int(at * 1000))


async def eur_limit_with_fx_refused(p: Prod) -> None:
    """An EUR long limit on the fx route whose fx order was refused (FAILED, nothing filled): tickmill then watches
    fx's price, vantage its own."""
    p.vantage.tick("EURUSD+", 1.16195, 1.16205, T0)
    fx_tick(p, 1.16195, 1.16205, T0)
    fx_watchable(p)
    await p.say(fx_pair("EURUSD", Side.LONG, 1.1610, 1.1590, 1.1650, entry_type=EntryType.LIMIT), FX, "EUR Limit")
    fx = (await p.positions(1))["fx"]
    await p.db.save_broker_position(1, fx.model_copy(update={"status": PositionStatus.FAILED}))


async def waiting(p: Prod) -> dict[str, bool]:
    return {n: is_virtual_pending(pos) for n, pos in (await p.positions(1)).items() if n in ("tickmill", "vantage")}


async def test_fx_refused_vantage_and_tickmill_both_keep_waiting_on_their_own_prices(world) -> None:
    p = world
    await eur_limit_with_fx_refused(p)
    for _ in range(3):
        await p.engine.monitor.run_virtual_once()
    assert await waiting(p) == {"tickmill": True, "vantage": True}
    assert not any("girişleri dolmadan sona erdi" in t for t in p.texts())
    p.vantage.tick("EURUSD+", 1.16090, 1.16098, T0 + 2)              # Vantage touches: only vantage opens
    await p.engine.monitor.run_virtual_once()
    assert [o[:2] for o in vorders(p)] == [("BUY", "EURUSD+")] and p.tickmill_orders() == []
    assert (await waiting(p))["tickmill"] is True
    fx_tick(p, 1.16090, 1.16098, T0 + 4)                             # fx's price touches: tickmill opens
    p.tickmill.set_price("EURUSD", 1.16105, 1.16115)
    await p.engine.monitor.run_virtual_once()
    assert [o[0] for o in p.tickmill_orders()] == ["market_buy"]


async def test_vantage_cancelled_by_its_own_stop_rule_never_cancels_tickmill(world) -> None:
    p = world
    await eur_limit_with_fx_refused(p)
    await p.engine.monitor.run_virtual_once()
    p.vantage.history([(T0 + 0.5, 1.15880, 1.15890)])                # through vantage's stop (fx untouched)
    p.vantage.tick("EURUSD+", 1.16300, 1.16310, T0 + 1.5)
    for _ in range(3):
        await p.engine.monitor.run_virtual_once()
    positions = await p.positions(1)
    assert positions["vantage"].status is PositionStatus.CANCELLED
    assert is_virtual_pending(positions["tickmill"])


async def test_tickmill_cancelled_by_its_own_stop_rule_never_cancels_vantage(world) -> None:
    p = world
    await eur_limit_with_fx_refused(p)
    await p.engine.monitor.run_virtual_once()
    fx_watchable(p, [(T0 + 0.5, 1.15880, 1.15890)])                   # through the stop on fx's price
    fx_tick(p, 1.16300, 1.16310, T0 + 1.5)
    for _ in range(3):
        await p.engine.monitor.run_virtual_once()
    positions = await p.positions(1)
    assert positions["tickmill"].status is PositionStatus.CANCELLED
    assert is_virtual_pending(positions["vantage"]) and vorders(p) == []


async def test_fx_skipping_the_signal_leaves_both_waiting(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """fx's own rule leaves the signal out (no row is saved for it): nothing has ended."""
    def fx_without_eur(cfg: AppConfig) -> None:
        next(b for b in cfg.brokers if b.name == "fx").allowed_assets = ["XAU"]
    p = await vantage_world(tmp_path, monkeypatch, edit=fx_without_eur)
    try:
        p.vantage.tick("EURUSD+", 1.16195, 1.16205, T0)
        fx_tick(p, 1.16195, 1.16205, T0)
        fx_watchable(p)
        await p.say(fx_pair("EURUSD", Side.LONG, 1.1610, 1.1590, 1.1650, entry_type=EntryType.LIMIT), FX, "EUR")
        assert "fx" not in await p.positions(1) and p.fx_orders() == []
        for _ in range(3):
            await p.engine.monitor.run_virtual_once()
        assert await waiting(p) == {"tickmill": True, "vantage": True}
    finally:
        await p.close()
