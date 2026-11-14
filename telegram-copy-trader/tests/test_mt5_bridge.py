"""Broker type "mt5_bridge": the friend's Tickmill MetaTrader 5 terminal driven from a HELPER PROCESS.

Every test here starts the real helper (``python -m copytrader.brokers.mt5_helper``) as a real subprocess, with
mt5_fake_terminal.py in place of the MetaTrader5 package: one fake "terminal" whose account lives in a file, so that
it outlives a helper that crashed, hung or was killed. What is checked: trading goes through the native adapter in
the helper; a crash, a hang, a kill or a lost answer never blocks the caller beyond its limit, is told to the owner
truthfully and is followed by an automatic restart; an order whose answer was lost is never sent twice; nothing is
left running after the bot ends.
"""

from __future__ import annotations

import asyncio
import json
import os
import pickle
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from copytrader.brokers import mt5_bridge, mt5_helper
from copytrader.brokers.base import BrokerError, BrokerUnavailableError, OrderOutcomeUnknownError, outcome_unknown
from copytrader.brokers.mt5_bridge import Mt5BridgeBroker
from copytrader.brokers.mt5_broker import TerminalGuard
from copytrader.config import Mt5BridgeBrokerConfig, RiskConfig, terminal_key
from copytrader.engine.health import BrokerHeartbeat
from copytrader.models import PositionStatus, Severity, Side

from mt5_fake_terminal import (TERMINAL_COMPANY_ENV, TERMINAL_CONTROL_ENV, TERMINAL_FOLDER_ENV,
                               TERMINAL_STATE_ENV)
from test_mt5_broker import THREE_TPS, make_plan, make_req

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals drive the crash / hang cases")

TESTS = Path(__file__).resolve().parent
FOLDER = r"C:\Program Files\Tickmill MT5 Terminal"
EXE = FOLDER + "\\terminal64.exe"
GUARD = 25.0            # no single await here may take longer: a hang of the bridge fails the test, not the run


class Notes:
    def __init__(self) -> None:
        self.sent: list[tuple[str, Severity]] = []

    async def notify(self, text: str, severity: Severity = Severity.INFO) -> None:
        self.sent.append((text, severity))

    def texts(self) -> list[str]:
        return [t for t, _ in self.sent]


class Terminal:
    """The fake terminal's files and its misbehaviour switches."""

    def __init__(self, folder: Path) -> None:
        self.state = folder / "terminal.pkl"
        self.control = folder / "control.json"
        self.pins = folder / "data"
        self.pins.mkdir()

    def set(self, **entries: Any) -> None:
        data = self.read_control()
        data.update(entries)
        self.control.write_text(json.dumps(data), encoding="utf-8")

    def read_control(self) -> dict[str, Any]:
        try:
            return json.loads(self.control.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    def account(self) -> dict[str, Any]:
        return pickle.loads(self.state.read_bytes()) if self.state.is_file() else {"positions": {}, "orders": {}}

    def positions(self) -> dict[int, Any]:
        return self.account()["positions"]


@pytest.fixture
def term(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Terminal:
    t = Terminal(tmp_path)
    monkeypatch.setenv(mt5_helper.FAKE_ENV, "mt5_fake_terminal:make")
    monkeypatch.setenv(mt5_helper.FAKE_PATH_ENV, str(TESTS))
    monkeypatch.setenv(TERMINAL_STATE_ENV, str(t.state))
    monkeypatch.setenv(TERMINAL_CONTROL_ENV, str(t.control))
    monkeypatch.setenv(TERMINAL_FOLDER_ENV, FOLDER)
    monkeypatch.setenv(TERMINAL_COMPANY_ENV, "Tickmill Ltd")
    # watch and restart quickly (the production values are in mt5_bridge.py)
    monkeypatch.setattr(mt5_bridge, "PING_SEC", 0.1)
    monkeypatch.setattr(mt5_bridge, "HANG_SEC", 1.5)
    monkeypatch.setattr(mt5_bridge, "RESTART_BACKOFF_SEC", (0.1, 0.3, 0.5))
    monkeypatch.setattr(mt5_bridge, "ADOPT_WAIT_SEC", 15.0)
    return t


def cfg(**kw: Any) -> Mt5BridgeBrokerConfig:
    base: dict[str, Any] = dict(type="mt5_bridge", name="tickmill", optional=True, terminal_path=EXE,
                                broker_company="Tickmill", risk_usd=50)
    base.update(kw)
    return Mt5BridgeBrokerConfig(**base)


async def bridge(term: Terminal, notes: Notes | None = None, guard: TerminalGuard | None = None,
                 **kw: Any) -> Mt5BridgeBroker:
    b = Mt5BridgeBroker(cfg(**kw), pin_dir=str(term.pins), guard=guard)
    b.set_notifier(notes or Notes())
    b.set_risk_config(RiskConfig(include_fees_in_risk=False))
    await asyncio.wait_for(b.connect(), GUARD)
    return b


def alive(pid: int | None) -> bool:
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    try:                                    # a zombie (ended, not yet reaped) does not count
        with open(f"/proc/{pid}/stat", encoding="utf-8") as fh:
            return fh.read().split(")")[-1].split()[0] != "Z"
    except OSError:
        return True


async def until(check: Any, limit: float = GUARD) -> None:
    end = time.monotonic() + limit
    while not check():
        if time.monotonic() > end:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.05)


def pid_of(b: Mt5BridgeBroker) -> int:
    assert b._helper is not None and b._helper.pid is not None
    return b._helper.pid


async def eth(b: Mt5BridgeBroker) -> Any:
    sym = await b.resolve_symbol("ETH", "USDT", None, None)
    assert sym is not None and sym.broker_symbol == "ETHUSD"
    return sym


# --------------------------------------------------------------------------- #
# the normal path
# --------------------------------------------------------------------------- #
async def test_trades_through_the_native_adapter_in_the_helper_and_ends_it(term: Terminal) -> None:
    b = await bridge(term)
    pid = pid_of(b)
    assert pid != os.getpid() and alive(pid)
    assert await b.get_equity() == 10_000.0 and await b.account_currency() == "USD"
    assert await b.health_problem() is None
    sym = await eth(b)
    size = await b.size_position(sym, Side.LONG, 2232.95, 2205.11, 50.0)
    assert size.quantity == 1.79                     # 50 / 27.84 = 1.796: lot_rounding "down" (the default)
    pos = await b.open_position(make_req(sym, 0.3, make_plan(tps=THREE_TPS), tag="ctaa11bb22cc"))
    assert pos.status is PositionStatus.OPEN and [leg["tag"] for leg in pos.data["legs"]] == [
        "ctaa11bb22cc-1", "ctaa11bb22cc-2", "ctaa11bb22cc-3"]
    on_venue = term.positions()
    assert sorted(p.comment for p in on_venue.values()) == ["ctaa11bb22cc-1", "ctaa11bb22cc-2", "ctaa11bb22cc-3"]
    pos = await b.update_stop_loss(pos, 2220.0)
    assert {p.sl for p in term.positions().values()} == {2220.0}
    pos = await b.close_position(pos, 1 / 3)                             # nearest target's leg first
    assert len(term.positions()) == 2
    pos = await b.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(0.2)
    pos = await b.close_position(pos, 1.0)
    assert pos.status is PositionStatus.CLOSED and term.positions() == {}
    # the same tag again: found on the account, never sent twice (rule 4, the native adapter's own check)
    t0 = time.monotonic()
    await asyncio.wait_for(b.close(), GUARD)
    assert time.monotonic() - t0 < 5.0
    await until(lambda: not alive(pid), 5.0)


async def test_rounds_to_the_nearest_step_when_the_config_says_so(term: Terminal) -> None:
    b = await bridge(term, lot_rounding="nearest")
    try:
        sym = await eth(b)
        size = await b.size_position(sym, Side.LONG, 2232.95, 2205.11, 50.0)   # 50 / 27.84 = 1.796 lots
        assert size.quantity == 1.8 and size.est_loss_at_sl == pytest.approx(50.112)
        assert any("en yakın adıma yukarı yuvarlandı" in w for w in size.warnings)
    finally:
        await b.close()


async def test_startup_refuses_a_terminal_whose_account_is_not_tickmill(term: Terminal,
                                                                       monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(TERMINAL_COMPANY_ENV, "FundingPips Ltd")
    b = Mt5BridgeBroker(cfg(), pin_dir=str(term.pins))
    with pytest.raises(BrokerError) as err:
        await asyncio.wait_for(b.connect(), GUARD)
    assert "bir Tickmill hesabı görünmüyor" in str(err.value) and "bağlanılmadı" in str(err.value)
    assert b._helper is None
    await b.close()


async def test_startup_refuses_the_owners_fx_terminal(term: Terminal) -> None:
    # fx's pin (written by the bot process when fx connected) names this very folder: never attached to
    (term.pins / "mt5_terminal_fx.json").write_text(json.dumps(
        {"broker": "fx", "folder": FOLDER, "company": "FundingPips Ltd", "server": "FP-1", "login": 5001}))
    b = Mt5BridgeBroker(cfg(), pin_dir=str(term.pins))
    with pytest.raises(BrokerError, match="'fx' hesabının terminaline bağlandı"):
        await asyncio.wait_for(b.connect(), GUARD)
    await b.close()
    # ... and a terminal at another folder than terminal_path is refused too
    (term.pins / "mt5_terminal_fx.json").unlink()
    b = Mt5BridgeBroker(cfg(terminal_path=r"C:\Program Files\Tickmill Other\terminal64.exe"), pin_dir=str(term.pins))
    with pytest.raises(BrokerError, match="config.yaml'daki terminal yerine başka bir terminale"):
        await asyncio.wait_for(b.connect(), GUARD)
    await b.close()


async def test_the_friends_login_is_pinned_and_another_account_is_refused_later(term: Terminal) -> None:
    b = await bridge(term)
    await b.close()
    pin = json.loads((term.pins / "mt5_terminal_tickmill.json").read_text(encoding="utf-8"))
    assert pin["login"] == 7007 and terminal_key(pin["folder"]) == terminal_key(EXE)
    pin["login"] = 9999                                                 # as if the terminal now shows another account
    (term.pins / "mt5_terminal_tickmill.json").write_text(json.dumps(pin), encoding="utf-8")
    b = Mt5BridgeBroker(cfg(), pin_dir=str(term.pins))
    with pytest.raises(BrokerError, match=r"farklı bir hesaba bağlı \(7007, beklenen 9999\)"):
        await asyncio.wait_for(b.connect(), GUARD)
    await b.close()


# --------------------------------------------------------------------------- #
# crash
# --------------------------------------------------------------------------- #
async def test_crash_refuses_at_once_restarts_and_tells_the_owner_how_long_it_lasted(term: Terminal) -> None:
    notes = Notes()
    b = await bridge(term, notes)
    try:
        sym = await eth(b)
        first = pid_of(b)
        term.set(crash_at="symbol_info_tick")
        t0 = time.monotonic()
        with pytest.raises(BrokerUnavailableError, match="MT5 bağlantısı koptu"):
            await asyncio.wait_for(b.get_quote(sym), GUARD)
        assert time.monotonic() - t0 < 3.0
        # while it is restarting: refused at once, nothing is sent (the engine may catch the signal up later)
        if not b._up:
            t1 = time.monotonic()
            with pytest.raises(BrokerUnavailableError, match="yeniden kuruluyor"):
                await b.get_equity()
            assert time.monotonic() - t1 < 0.1
            assert "yeniden kuruluyor" in (await b.health_problem() or "")
        await until(lambda: b._up)
        assert pid_of(b) != first and not alive(first)
        quote = await b.get_quote(sym)
        assert quote.bid > 0
        await until(lambda: len(notes.sent) >= 2)
        down, back = notes.texts()[:2]
        assert down == ("🔌 <b>tickmill</b>: MT5 bağlantısı koptu (yardımcı süreç kapandı (çıkış kodu 3)). Bot "
                        "bağlantıyı kendisi yeniden kuruyor; bu sırada bu hesapta yeni işlem açılmaz, diğer hesaplar "
                        "etkilenmez. Bağlantı gelince ayrıca haber verilir.")
        assert back.startswith("✅ <b>tickmill</b>: MT5 bağlantısı yeniden kuruldu, kesinti yaklaşık ")
        assert " sn sürdü. Kesinti sırasında gelen yeni sinyaller bu hesapta o an açılmadı (atlanan olduysa ayrıca " \
               "bildirilir). Diğer hesaplar etkilenmedi; yeni sinyaller bu hesapta yine açılır." in back
        assert b.outages == 1
    finally:
        await b.close()


async def test_a_restart_that_keeps_failing_is_told_once_and_retried(term: Terminal,
                                                                     monkeypatch: pytest.MonkeyPatch) -> None:
    notes = Notes()
    b = await bridge(term, notes)
    try:
        monkeypatch.setenv(TERMINAL_COMPANY_ENV, "Other Broker")       # every restart now meets a wrong terminal
        os.kill(pid_of(b), signal.SIGKILL)
        await until(lambda: len(notes.sent) >= 2)
        await asyncio.sleep(1.5)                                       # several failed attempts
        texts = notes.texts()
        assert sum("yeniden kurulamadı" in t for t in texts) == 1 and not any(t.startswith("✅") for t in texts)
        assert "Tickmill hesabı görünmüyor" in texts[1] and "(bu uyarı tekrarlanmaz)" in texts[1]
        assert "Bot denemeyi sürdürüyor (en seyrek 0.5 saniyede bir)." in texts[1]
        monkeypatch.setenv(TERMINAL_COMPANY_ENV, "Tickmill Ltd")
        await until(lambda: b._up)
        await until(lambda: any(t.startswith("✅") for t in notes.texts()))
    finally:
        await b.close()


# --------------------------------------------------------------------------- #
# lost reply
# --------------------------------------------------------------------------- #
async def test_open_whose_answer_was_lost_is_found_by_its_tag_and_never_sent_twice(term: Terminal) -> None:
    notes = Notes()
    b = await bridge(term, notes)
    try:
        sym = await eth(b)
        term.set(crash_after="order_send")          # the order is executed, then the helper dies before answering
        pos = await asyncio.wait_for(b.open_position(make_req(sym, 0.1, make_plan(), tag="ctlost000001")), GUARD)
        assert pos.status is PositionStatus.OPEN
        assert [leg["tag"] for leg in pos.data["legs"]] == ["ctlost000001"]
        assert any("etiketiyle hesapta bulundu" in w for w in pos.data["warnings"])
        assert len(term.positions()) == 1                              # exactly one: nothing was sent again
        ticket = pos.data["legs"][0]["ticket"]
        pos = await b.sync_position(pos)                               # tracked like any other position
        assert pos.status is PositionStatus.OPEN and pos.data["legs"][0]["ticket"] == ticket
        await until(lambda: any(t.startswith("✅") for t in notes.texts()))
        back = next(t for t in notes.texts() if t.startswith("✅"))
        assert "Kesinti sırasında bu hesaba gönderilmiş bir emir yanıt bekliyordu; o emir gerçekleşmiş olabilir" in back
    finally:
        await b.close()


async def test_open_lost_before_it_reached_the_terminal_is_unknown_never_failed(term: Terminal) -> None:
    b = await bridge(term)
    try:
        sym = await eth(b)
        term.set(crash_at="order_send")             # the helper dies as it is about to send: nothing executed
        with pytest.raises(OrderOutcomeUnknownError) as err:
            await asyncio.wait_for(b.open_position(make_req(sym, 0.1, make_plan(), tag="ctlost000002")), GUARD)
        assert outcome_unknown(err.value) and "durumu belirsiz:" in str(err.value)
        assert "'ctlost000002' etiketli emir ya da pozisyon şu an görünmüyor" in str(err.value)
        assert "ikinci kez gönderilmedi" in str(err.value)
        assert term.positions() == {}
    finally:
        await b.close()


async def test_close_whose_answer_was_lost_is_outcome_unknown(term: Terminal) -> None:
    b = await bridge(term)
    try:
        sym = await eth(b)
        pos = await b.open_position(make_req(sym, 0.1, make_plan(), tag="ctlost000003"))
        term.set(crash_after="order_send")
        with pytest.raises(OrderOutcomeUnknownError) as err:
            await asyncio.wait_for(b.close_position(pos, 1.0), GUARD)
        assert outcome_unknown(err.value) and "gerçekleşmiş olabilir" in str(err.value)
        await until(lambda: b._up)
        pos = await b.sync_position(pos)                               # the next read shows what really happened
        assert pos.status is PositionStatus.CLOSED and term.positions() == {}
    finally:
        await b.close()


async def test_a_call_over_its_limit_fails_in_time_and_a_write_is_unknown(term: Terminal,
                                                                         monkeypatch: pytest.MonkeyPatch) -> None:
    b = await bridge(term)
    try:
        sym = await eth(b)
        pos = await b.open_position(make_req(sym, 0.1, make_plan(), tag="ctslow000001"))
        monkeypatch.setattr(mt5_bridge, "READ_TIMEOUT_SEC", 0.5)
        term.set(slow={"account_info": 2.0})
        t0 = time.monotonic()
        with pytest.raises(BrokerError, match="0.5 sn içinde yanıt vermedi"):
            await b.get_equity()
        assert time.monotonic() - t0 < 1.5
        monkeypatch.setattr(mt5_bridge, "WRITE_TIMEOUT_SEC", 0.5)
        term.set(slow={"order_send": 2.0})
        with pytest.raises(OrderOutcomeUnknownError) as err:
            await b.close_position(pos, 1.0)
        assert outcome_unknown(err.value)
        term.set(slow={})
        await asyncio.sleep(2.5)                   # the slow helper answered late: only logged, and it is still up
        assert b._up and await b.get_equity() == 10_000.0
    finally:
        await b.close()


# --------------------------------------------------------------------------- #
# hang
# --------------------------------------------------------------------------- #
async def test_a_hung_helper_is_killed_within_its_limit_and_started_again(term: Terminal) -> None:
    notes = Notes()
    b = await bridge(term, notes)
    try:
        frozen = pid_of(b)
        term.set(freeze_at="account_info")         # SIGSTOP: the whole helper stops (no pong, no answer)
        t0 = time.monotonic()
        with pytest.raises(BrokerUnavailableError):
            await asyncio.wait_for(b.get_equity(), GUARD)
        assert time.monotonic() - t0 < mt5_bridge.HANG_SEC + 2.0
        assert "yanıt vermiyor" in notes.texts()[0]
        await until(lambda: b._up)
        await until(lambda: not alive(frozen), 5.0)
        assert await b.get_equity() == 10_000.0
    finally:
        await b.close()


async def test_close_is_bounded_even_when_the_helper_is_frozen(term: Terminal, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mt5_bridge, "CLOSE_RPC_SEC", 0.5)
    monkeypatch.setattr(mt5_bridge, "CLOSE_EXIT_SEC", 0.5)
    b = await bridge(term)
    pid = pid_of(b)
    os.kill(pid, signal.SIGSTOP)
    t0 = time.monotonic()
    await asyncio.wait_for(b.close(), GUARD)
    assert time.monotonic() - t0 < 0.5 + 0.5 + mt5_bridge.CLOSE_KILL_SEC + 1.0
    await until(lambda: not alive(pid), 5.0)


# --------------------------------------------------------------------------- #
# restart and positions
# --------------------------------------------------------------------------- #
async def test_after_a_kill_and_restart_open_positions_are_still_managed(term: Terminal) -> None:
    b = await bridge(term)
    try:
        sym = await eth(b)
        pos = await b.open_position(make_req(sym, 0.2, make_plan(), tag="ctkill000001"))
        os.kill(pid_of(b), signal.SIGKILL)
        await until(lambda: not b._up)
        await until(lambda: b._up)
        pos = await b.sync_position(pos)
        assert pos.status is PositionStatus.OPEN
        pos = await b.update_stop_loss(pos, 2215.0)
        assert [p.sl for p in term.positions().values()] == [2215.0]
        pos = await b.close_position(pos, 1.0)
        assert pos.status is PositionStatus.CLOSED
    finally:
        await b.close()


METAAPI_COMMENTS = ["CT_9f00aa11bbL{n}_0|copytrader", "copytrader|CT_9f00aa11bbL{n}_0", "CT_9f00aa11bbL{n}_0"]


@pytest.mark.parametrize("comment", METAAPI_COMMENTS)
async def test_positions_the_metaapi_adapter_opened_are_managed_after_the_switch(term: Terminal, comment: str) -> None:
    """A friend's position opened through MetaApi while the bot ran on type "metaapi" (legs with MetaApi's string
    tickets and the internal tag) is still on the same Tickmill account after the switch: the bridge syncs, moves and
    closes it, and finds it by its tag (MetaApi keeps the clientId in the comment) instead of opening it again."""
    from test_metaapi_to_native import metaapi_position, seed_terminal

    pos, req = await metaapi_position()                 # opened by the REAL MetaApi adapter (its test fake)
    seed_terminal(term.state, pos, comment)             # ... and as the native terminal shows the same account
    stored = type(pos).model_validate_json(pos.model_dump_json())       # through the database, as after a restart
    b = await bridge(term)
    try:
        synced = await b.sync_position(stored)
        assert synced.status is PositionStatus.OPEN and synced.remaining_quantity == pytest.approx(0.3)
        moved = await b.update_stop_loss(synced, 2210.0)
        assert {p.sl for p in term.positions().values()} == {2210.0}
        again = await b.open_position(req)                              # the same signal once more: not re-sent
        assert len(term.positions()) == 3 and "zaten var; tekrar gönderilmedi" in (again.error or "")
        done = await b.close_position(moved, 1.0)
        assert done.status is PositionStatus.CLOSED and term.positions() == {}
    finally:
        await b.close()


# --------------------------------------------------------------------------- #
# orphans, process start
# --------------------------------------------------------------------------- #
PARENT = textwrap.dedent("""
    import asyncio, sys
    sys.path.insert(0, sys.argv[1])
    from copytrader.brokers.mt5_bridge import Mt5BridgeBroker
    from copytrader.config import Mt5BridgeBrokerConfig
    async def main():
        b = Mt5BridgeBroker(Mt5BridgeBrokerConfig(type="mt5_bridge", name="tickmill", terminal_path=sys.argv[2],
                                                  broker_company="Tickmill"), pin_dir=sys.argv[3])
        await b.connect()
        print("HELPER", b._helper.pid, flush=True)
        await asyncio.sleep(3600)
    asyncio.run(main())
""")


def test_no_helper_outlives_a_bot_that_was_killed(term: Terminal, tmp_path: Path) -> None:
    script = tmp_path / "bot.py"
    script.write_text(PARENT, encoding="utf-8")
    bot = subprocess.Popen([sys.executable, str(script), str(TESTS.parent), EXE, str(term.pins)],
                           stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    try:
        line = ""
        end = time.monotonic() + GUARD
        while not line.startswith("HELPER") and time.monotonic() < end:
            line = bot.stdout.readline() if bot.stdout else ""
        helper = int(line.split()[1])
        assert alive(helper)
        bot.kill()                                  # no clean-up of any kind in the bot
        bot.wait(10)
        end = time.monotonic() + 5.0
        while alive(helper) and time.monotonic() < end:
            time.sleep(0.05)
        assert not alive(helper)
    finally:
        if bot.poll() is None:
            bot.kill()


def test_the_helper_is_the_bots_own_python_and_package() -> None:
    argv, env, cwd = mt5_bridge.helper_command()
    assert argv == [sys.executable, "-m", "copytrader.brokers.mt5_helper"]
    assert Path(cwd) == TESTS.parent and env["PYTHONPATH"].split(os.pathsep)[0] == str(TESTS.parent)


def test_on_windows_the_helper_starts_without_a_window_and_in_the_kill_on_close_job(
        monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    class FakePopen:
        def __init__(self, argv: list[str], **kw: Any) -> None:
            seen.update(kw, argv=argv)
            self.pid, self.stdin, self.stdout, self.stderr = 4242, None, None, None

        def poll(self) -> None:
            return None

    jobs: list[Any] = []
    monkeypatch.setattr(mt5_bridge.sys, "platform", "win32")
    monkeypatch.setattr(mt5_bridge.subprocess, "Popen", FakePopen)
    monkeypatch.setattr(mt5_bridge, "_put_in_job", jobs.append)
    monkeypatch.setattr(mt5_bridge.threading, "Thread", lambda **kw: SimpleNamespace(start=lambda: None))
    helper = mt5_bridge._Helper("tickmill", asyncio.new_event_loop(), lambda h, why: None)
    helper.start({"version": 1})
    assert seen["creationflags"] == mt5_bridge.CREATE_NO_WINDOW == 0x08000000
    assert seen["close_fds"] is True and jobs and jobs[0].pid == 4242
    helper.loop.close()


# --------------------------------------------------------------------------- #
# the engine's heartbeat during an outage the adapter announced itself
# --------------------------------------------------------------------------- #
async def test_heartbeat_sends_no_second_alert_for_an_outage_the_bridge_announced() -> None:
    notes = Notes()
    broker = SimpleNamespace(name="tickmill", kind="mt5_bridge", config=SimpleNamespace(type="mt5_bridge"),
                             link_outage_announced=True)

    async def health_problem() -> str:
        return "tickmill: MT5 bağlantısı şu an yeniden kuruluyor"

    broker.health_problem = health_problem
    hb = BrokerHeartbeat(lambda: [broker], notes)  # type: ignore[arg-type,list-item]
    for _ in range(5):
        await hb.check_once()
    assert notes.sent == [] and "tickmill" in hb.problems()
    broker.link_outage_announced = False           # the terminal itself has a problem now: the heartbeat tells
    for _ in range(3):
        await hb.check_once()
    assert len(notes.sent) == 1 and notes.texts()[0].startswith("🔌 <b>tickmill</b> hesabında sorun var")


def test_error_rebuild_keeps_the_kinds_the_engine_tells_apart() -> None:
    from copytrader.brokers.mt5_broker import OrderReplyLostError, TerminalTimeoutError
    from copytrader.risk.sizing import SizingError

    def back(exc: BaseException) -> BaseException:
        return mt5_bridge.error_from_wire(mt5_helper.error_to_wire(exc))

    lost = back(OrderReplyLostError("Emir durumu belirsiz: x"))
    assert isinstance(lost, OrderReplyLostError) and outcome_unknown(lost)
    t = back(TerminalTimeoutError("MetaTrader terminali 30 sn içinde yanıt vermedi", started=True))
    assert isinstance(t, TerminalTimeoutError) and t.started is True and "yanıt vermedi" in str(t)
    assert isinstance(back(BrokerUnavailableError("x")), BrokerUnavailableError)
    assert isinstance(back(SizingError("lot")), SizingError)
    assert type(back(ValueError("boom"))) is BrokerError



# --------------------------------------------------------------------------- #
# review fixes
# --------------------------------------------------------------------------- #
class _SlowAfterWrite:
    """The helper's stdin, whose write of an open request returns only a while AFTER the bytes reached the helper:
    the writer thread being descheduled right after its write (GIL / OS scheduling)."""

    def __init__(self, real: Any) -> None:
        self.real = real

    def write(self, data: bytes) -> int:
        n = self.real.write(data)
        self.real.flush()
        if b"open_position" in data:
            time.sleep(1.0)
        return n

    def __getattr__(self, name: str) -> Any:
        return getattr(self.real, name)


async def test_an_order_the_helper_received_is_never_reported_as_not_sent(term: Terminal,
                                                                          monkeypatch: pytest.MonkeyPatch) -> None:
    real_popen = subprocess.Popen

    def popen(*args: Any, **kw: Any) -> Any:
        proc = real_popen(*args, **kw)
        proc.stdin = _SlowAfterWrite(proc.stdin)          # type: ignore[assignment]
        return proc

    monkeypatch.setattr(mt5_bridge.subprocess, "Popen", popen)
    monkeypatch.setattr(mt5_bridge, "ADOPT_WAIT_SEC", 0.0)              # look at once: shows the classification
    b = await bridge(term)
    try:
        sym = await eth(b)
        term.set(crash_after="order_send")         # executed, then the helper ends while the writer still "writes"
        with pytest.raises(OrderOutcomeUnknownError) as err:
            await asyncio.wait_for(b.open_position(make_req(sym, 0.1, make_plan(), tag="ctrace00001")), GUARD)
        assert outcome_unknown(err.value) and not isinstance(err.value, BrokerUnavailableError)
        assert len(term.positions()) == 1           # it IS there: "unknown", never "not sent" (no catch-up re-open)
    finally:
        await b.close()


async def test_a_partly_found_multi_leg_open_reports_only_what_the_account_holds(term: Terminal) -> None:
    b = await bridge(term)
    try:
        sym = await eth(b)
        term.set(crash_after="order_send")          # the first of three legs executed, then the helper ended
        pos = await asyncio.wait_for(
            b.open_position(make_req(sym, 0.3, make_plan(tps=THREE_TPS), tag="ctpart00001")), GUARD)
        assert len(term.positions()) == 1
        assert pos.quantity == pytest.approx(0.1) and pos.filled_quantity == pytest.approx(0.1)
        assert [t.price for t in pos.take_profits] == [2260.0]
        assert pos.data["legs"][0]["tp_indexes"] == [0]
        (missing,) = [w for w in pos.data["warnings"] if "yalnızca" in w]
        assert missing == ("Bu emrin 3 bacağından yalnızca 1 tanesi hesapta bulundu (0.1 lot); ctpart00001-2, "
                           "ctpart00001-3 etiketli bacak(lar) açılmamış olabilir ya da durumu belirsiz, tekrar "
                           "gönderilmedi: MT5'te kontrol edin.")
        assert pos.error == missing
    finally:
        await b.close()


async def test_a_crash_loop_is_one_outage_for_the_owner(term: Terminal) -> None:
    notes = Notes()
    b = await bridge(term, notes)
    try:
        for _ in range(5):
            await until(lambda: b._up)
            os.kill(pid_of(b), signal.SIGKILL)
            await until(lambda: not b._up)
        await until(lambda: b._up)
        await asyncio.sleep(0.5)
        texts = notes.texts()
        assert sum("koptu" in t for t in texts) == 1 and sum(t.startswith("✅") for t in texts) == 1, texts
        assert b.link_outage_announced is False and b._quiet_drops == 4
    finally:
        await b.close()


async def test_an_unsendable_request_fails_alone_and_the_writer_lives_on(term: Terminal) -> None:
    import threading

    b = await bridge(term)
    try:
        sym = await eth(b)
        t0 = time.monotonic()
        with pytest.raises(BrokerError, match="gönderilemedi") as err:
            await asyncio.wait_for(b._rpc("get_quote", threading.Lock()), GUARD)
        assert not isinstance(err.value, (BrokerUnavailableError, OrderOutcomeUnknownError))
        assert time.monotonic() - t0 < 2.0
        assert (await b.get_quote(sym)).bid > 0 and b._up                  # the same helper, still answering
    finally:
        await b.close()


@pytest.mark.parametrize("what", ["open", "close"])
async def test_a_late_success_after_the_time_limit_is_told_to_the_owner(term: Terminal,
                                                                       monkeypatch: pytest.MonkeyPatch,
                                                                       what: str) -> None:
    notes = Notes()
    b = await bridge(term, notes)
    try:
        sym = await eth(b)
        pos = None if what == "open" else await b.open_position(make_req(sym, 0.1, make_plan(), tag="ctlate00001"))
        monkeypatch.setattr(mt5_bridge, "OPEN_TIMEOUT_SEC", 0.5)
        monkeypatch.setattr(mt5_bridge, "WRITE_TIMEOUT_SEC", 0.5)
        term.set(slow={"order_send": 1.5})
        with pytest.raises(OrderOutcomeUnknownError):
            if what == "open":
                await b.open_position(make_req(sym, 0.1, make_plan(), tag="ctlate00002"))
            else:
                await b.close_position(pos, 1.0)              # type: ignore[arg-type]
        await until(lambda: any("gecikmiş bir yanıt" in t for t in notes.texts()), 10.0)
        (late,) = [t for t in notes.texts() if "gecikmiş bir yanıt" in t]
        if what == "open":
            assert late == ("⚠️ <b>tickmill</b>: zaman sınırı aşıldıktan sonra MT5'ten gecikmiş bir yanıt geldi: "
                            "'ctlate00002' etiketli emir AÇILDI (0.1 lot ETHUSD, durum: open). Bot bu pozisyonu "
                            "kaydetmedi ve takip ETMİYOR: MT5'te kontrol edin, kendiniz yönetin ya da kapatın.")
        else:
            assert late == ("⚠️ <b>tickmill</b>: zaman sınırı aşıldıktan sonra MT5'ten gecikmiş bir yanıt geldi: "
                            "ETHUSD için kapatma isteği GERÇEKLEŞTİ. Bot bunu pozisyonun bir sonraki kontrolünde "
                            "hesaptan okur; MT5'te kontrol edin.")
    finally:
        term.set(slow={})
        await b.close()


# --------------------------------------------------------------------------- #
# review round 2
# --------------------------------------------------------------------------- #
async def test_a_long_outage_soon_after_the_last_one_is_still_told(term: Terminal,
                                                                  monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mt5_bridge, "FLAP_QUIET_SEC", 1.0)
    notes = Notes()
    b = await bridge(term, notes)
    try:
        os.kill(pid_of(b), signal.SIGKILL)                              # outage 1: told (down + back)
        await until(lambda: sum(t.startswith("✅") for t in notes.texts()) == 1)
        term.set(slow={"initialize": 2.0})                              # outage 2, soon after: lasts > FLAP_QUIET_SEC
        os.kill(pid_of(b), signal.SIGKILL)
        await until(lambda: not b._up)
        assert b.link_outage_announced is False         # nobody told yet: the heartbeat's own alert stays armed
        await until(lambda: b._up)
        await until(lambda: sum(t.startswith("✅") for t in notes.texts()) == 2)
        back = [t for t in notes.texts() if t.startswith("✅")][-1]
        assert "MT5 bağlantısı yeniden kuruldu, kesinti yaklaşık " in back and " sn sürdü." in back
        assert sum("koptu" in t for t in notes.texts()) == 1             # (the second drop had no down notice)
        assert "kısa kopma daha" not in back            # this outage is the one told here: not counted a second time
    finally:
        term.set(slow={})
        await b.close()


@pytest.mark.parametrize(("status", "error", "want"), [
    (PositionStatus.FAILED, "Stop konulamadı; pozisyon kapatıldı.",
     "⚠️ <b>tickmill</b>: zaman sınırı aşıldıktan sonra MT5'ten gecikmiş bir yanıt geldi: 'ctlate00009' etiketli emir "
     "AÇILMADI (durum: failed; <code>Stop konulamadı; pozisyon kapatıldı.</code>). Bu hesapta bu işlem için pozisyon "
     "yok; yine de MT5'te kontrol edin."),
    (PositionStatus.CANCELLED, None,
     "⚠️ <b>tickmill</b>: zaman sınırı aşıldıktan sonra MT5'ten gecikmiş bir yanıt geldi: 'ctlate00009' etiketli emir "
     "AÇILMADI (durum: cancelled; <code>neden bildirilmedi</code>). Bu hesapta bu işlem için pozisyon yok; yine de "
     "MT5'te kontrol edin."),
    (PositionStatus.CLOSED, "Stop konulamadı; pozisyon kapatıldı.",
     "⚠️ <b>tickmill</b>: zaman sınırı aşıldıktan sonra MT5'ten gecikmiş bir yanıt geldi: 'ctlate00009' etiketli emir "
     "açıldı ama pozisyon hemen KAPANDI (durum: closed; <code>Stop konulamadı; pozisyon kapatıldı.</code>). Bu "
     "hesapta bu işlemden açık pozisyon kalmamış olmalı; yine de MT5'te kontrol edin."),
    (PositionStatus.PENDING, None,
     "⚠️ <b>tickmill</b>: zaman sınırı aşıldıktan sonra MT5'ten gecikmiş bir yanıt geldi: 'ctlate00009' etiketli "
     "bekleyen emir ETHUSD için YERLEŞTİRİLDİ (0.1 lot). Bot bu emri kaydetmedi ve takip ETMİYOR: dolarsa pozisyonu da "
     "izlemez. MT5'te kontrol edin, gerekirse emri iptal edin."),
    (PositionStatus.OPEN, None,
     "⚠️ <b>tickmill</b>: zaman sınırı aşıldıktan sonra MT5'ten gecikmiş bir yanıt geldi: 'ctlate00009' etiketli emir "
     "AÇILDI (0.1 lot ETHUSD, durum: open). Bot bu pozisyonu kaydetmedi ve takip ETMİYOR: MT5'te kontrol edin, "
     "kendiniz yönetin ya da kapatın."),
])
async def test_a_late_open_answer_is_worded_by_what_really_happened(status: PositionStatus, error: str | None,
                                                                    want: str) -> None:
    from copytrader.models import BrokerPosition, SymbolInfo

    notes = Notes()
    b = Mt5BridgeBroker(cfg())
    b.set_notifier(notes)
    sym = SymbolInfo(broker_symbol="ETHUSD", base_asset="ETH", quote_asset="USD", qty_step=0.01, min_qty=0.01,
                     price_tick=0.01)
    req = make_req(sym, 0.1, make_plan(), tag="ctlate00009")
    filled = 0.1 if status is PositionStatus.OPEN else 0.0
    value = BrokerPosition(broker="tickmill", broker_symbol="ETHUSD", side=Side.LONG, status=status, quantity=0.1,
                           filled_quantity=filled, remaining_quantity=filled, stop_loss=2205.11, error=error)
    pending = mt5_bridge._Pending(asyncio.get_running_loop().create_future(), "open_position", (req,))
    b._late_answer(pending, value)
    await until(lambda: bool(notes.sent), 2.0)
    assert notes.texts() == [want]
