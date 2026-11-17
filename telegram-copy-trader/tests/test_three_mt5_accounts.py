"""A third MetaTrader 5 account (the owner's BemFunding prop account, named "fx2") next to fx (native, in the bot
process) and the friend's tickmill (type mt5_bridge): a second mt5_bridge account of another company, in a terminal
of its own.

  config      ``friend_account`` says whose account it is (not written: optional = the friend's, as before); two
              mt5_bridge accounts can never name the same terminal.
  catch-up    the question for a signal the owner's own optional account missed is answered by the OWNER, never by
              the friend (/arkadas); the friend still answers it for tickmill.
  two bridges two helper processes at the same time, each bound to its own terminal: an order lands only on its own
              account, and neither one ever attaches to the other's terminal.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from mt5_fake_terminal import (
    ACCOUNT_COMPANY_ENV,
    ACCOUNT_SERVER_ENV,
    TERMINAL_COMPANY_ENV,
    TERMINAL_CONTROL_ENV,
    TERMINAL_FOLDER_ENV,
    TERMINAL_STATE_ENV,
)
from pydantic import ValidationError
from test_friend_mirror import (  # noqa: F401  (_ask_mode: autouse fixture)
    FRIEND,
    OWNER,
    World,
    _ask_mode,
    buttons,
)
from test_mt5_bridge import GUARD, Notes, Terminal, alive, eth, pid_of, until
from test_mt5_broker import THREE_TPS, make_plan, make_req
from test_notifier import CbEvent

from copytrader.brokers import mt5_bridge
from copytrader.brokers.mt5_bridge import Mt5BridgeBroker, helper_command
from copytrader.brokers.mt5_broker import build_guard, write_pin
from copytrader.config import (
    AppConfig,
    Mt5BridgeBrokerConfig,
    RiskConfig,
)
from copytrader.models import PositionStatus, Side

TM_FOLDER = r"C:\Program Files\Tickmill MT5 Terminal"
BEM_FOLDER = r"C:\Program Files\BEM Funding MT5 Terminal"


def _app(brokers: list[dict[str, Any]]) -> AppConfig:
    return AppConfig.model_validate({
        "telegram": {"api_id": 1, "api_hash": "h", "source_chats": [-1001]},
        "notifier": {"bot_token": "1:a", "owner_user_id": 1},
        "brokers": brokers,
    })


FX = {"name": "fx", "type": "mt5", "login": 5001, "password": "x", "server": "FP-1", "risk_usd": 100}
TICKMILL = {"name": "tickmill", "type": "mt5_bridge", "optional": True, "terminal_path": TM_FOLDER + r"\terminal64.exe",
            "broker_company": "Tickmill", "risk_usd": 50}
BEM = {"name": "fx2", "type": "mt5_bridge", "optional": True, "friend_account": False,
       "terminal_path": BEM_FOLDER + r"\terminal64.exe", "broker_company": "BEM", "risk_usd": 100}


# --------------------------------------------------------------------------- #
# config
# --------------------------------------------------------------------------- #
def test_three_metatrader_accounts_are_one_valid_config() -> None:
    c = _app([FX, TICKMILL, BEM])
    by = {b.name: b for b in c.brokers}
    assert by["tickmill"].friend_account is None and by["fx2"].friend_account is False
    assert by["fx"].friend_account is None


def test_two_bridge_accounts_never_share_a_terminal() -> None:
    same = dict(BEM, terminal_path=TM_FOLDER.lower() + r"\TERMINAL64.EXE")       # same file, other spelling
    with pytest.raises(ValidationError, match="'tickmill' hesabının terminaliyle aynı"):
        _app([FX, TICKMILL, same])
    off = dict(same, enabled=False)                                             # a disabled block is never started
    assert len(_app([FX, TICKMILL, off]).brokers) == 3


def test_friend_account_is_a_yes_no_setting() -> None:
    with pytest.raises(ValidationError):
        _app([FX, dict(BEM, friend_account="maybe")])


# --------------------------------------------------------------------------- #
# who answers the catch-up question
# --------------------------------------------------------------------------- #
class OwnAccountWorld(World):
    """As tests/test_friend_mirror.py, but the optional account is the OWNER's (friend_account: false)."""

    async def up(self, *, tickmill_down: bool = False) -> OwnAccountWorld:
        await super().up(tickmill_down=tickmill_down)
        self.tm.config.friend_account = False
        return self


async def test_the_owner_answers_the_question_of_his_own_optional_account(tmp_path: Path) -> None:
    world = await OwnAccountWorld(tmp_path).up()
    try:
        await world.owner_says(f"/arkadas {FRIEND}")
        world.client.sent.clear()
        conf = await world.ask_catch_up()
        assert (conf.catch_up or {}).get("decider") is None
        (question,) = [m for m in world.client.to(OWNER) if m.buttons]               # the owner's buttons
        assert buttons(question)[0] == ("✅ Evet", f"ok:{conf.id}".encode())
        assert not any(m.buttons for m in world.client.to(FRIEND))                     # the friend only reads along
        assert not any("arkadaşınız) cevaplayacak" in m.text for m in world.client.sent)
        # a press from the friend's chat is refused; the owner's opens it
        await world.n.handle_callback(CbEvent(f"ok:{conf.id}".encode(), sender_id=FRIEND, message_id=question.id))
        await world.settle()
        assert world.tm.opened == []
        await world.n.handle_callback(CbEvent(f"ok:{conf.id}".encode(), sender_id=OWNER, message_id=question.id))
        await world.settle()
        assert len(world.tm.opened) == 1
    finally:
        await world.down()


async def test_the_friend_still_answers_for_tickmill_when_nothing_is_written(tmp_path: Path) -> None:
    world = await World(tmp_path).up()
    try:
        await world.owner_says(f"/arkadas {FRIEND}")
        conf = await world.ask_catch_up()
        assert (conf.catch_up or {}).get("decider") == FRIEND
    finally:
        await world.down()


async def test_the_friend_answers_when_friend_account_is_written_true(tmp_path: Path) -> None:
    world = await World(tmp_path).up()
    try:
        world.tm.config.friend_account = True
        await world.owner_says(f"/arkadas {FRIEND}")
        conf = await world.ask_catch_up()
        assert (conf.catch_up or {}).get("decider") == FRIEND
    finally:
        await world.down()


# --------------------------------------------------------------------------- #
# two bridge helpers at the same time
# --------------------------------------------------------------------------- #
def _command(term: Terminal, folder: str, company: str, account: tuple[str, str] | None = None):
    """The helper's start command bound to ONE fake terminal (state file, folder, company; ``account``: the
    (company, server) of the account logged in to it when that is not the terminal's own company)."""
    def command() -> tuple[list[str], dict[str, str], str]:
        argv, env, cwd = helper_command()
        env.update({TERMINAL_STATE_ENV: str(term.state), TERMINAL_CONTROL_ENV: str(term.control),
                    TERMINAL_FOLDER_ENV: folder, TERMINAL_COMPANY_ENV: company})
        if account is not None:
            env.update({ACCOUNT_COMPANY_ENV: account[0], ACCOUNT_SERVER_ENV: account[1]})
        return argv, env, cwd
    return command


@pytest.fixture
def two(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Terminal, Terminal, Path]:
    from copytrader.brokers import mt5_helper
    monkeypatch.setenv(mt5_helper.FAKE_ENV, "mt5_fake_terminal:make")
    monkeypatch.setenv(mt5_helper.FAKE_PATH_ENV, str(Path(__file__).resolve().parent))
    monkeypatch.setattr(mt5_bridge, "PING_SEC", 0.1)
    monkeypatch.setattr(mt5_bridge, "HANG_SEC", 1.5)
    monkeypatch.setattr(mt5_bridge, "RESTART_BACKOFF_SEC", (0.1, 0.3, 0.5))
    (tmp_path / "tm").mkdir()
    (tmp_path / "bem").mkdir()
    pins = tmp_path / "pins"
    pins.mkdir()
    return Terminal(tmp_path / "tm"), Terminal(tmp_path / "bem"), pins


def _bridge(cfg: dict[str, Any], others: list[dict[str, Any]], pins: Path, term: Terminal, folder: str,
            company: str, account: tuple[str, str] | None = None) -> Mt5BridgeBroker:
    c = Mt5BridgeBrokerConfig(**{k: v for k, v in cfg.items()})
    other_cfgs = [Mt5BridgeBrokerConfig(**o) if o["type"] == "mt5_bridge" else _app([o]).brokers[0] for o in others]
    guard = build_guard(c.name, other_cfgs, str(pins), [c.name, c.broker_company or ""])
    b = Mt5BridgeBroker(c, pin_dir=str(pins), guard=guard, command=_command(term, folder, company, account))
    b.set_notifier(Notes())
    b.set_risk_config(RiskConfig(include_fees_in_risk=False))
    return b


async def test_two_bridges_run_together_and_each_order_lands_on_its_own_account(two) -> None:
    tm_term, bem_term, pins = two
    tm = _bridge(TICKMILL, [FX, BEM], pins, tm_term, TM_FOLDER, "Tickmill Ltd")
    bem = _bridge(BEM, [FX, TICKMILL], pins, bem_term, BEM_FOLDER, "BEM Funding Ltd")
    try:
        await asyncio.wait_for(asyncio.gather(tm.connect(), bem.connect()), GUARD)
        assert pid_of(tm) != pid_of(bem) and alive(pid_of(tm)) and alive(pid_of(bem))
        pinned = {p.name: json.loads(p.read_text(encoding="utf-8")) for p in pins.glob("mt5_terminal_*.json")}
        assert pinned["mt5_terminal_tickmill.json"]["folder"] == TM_FOLDER
        assert pinned["mt5_terminal_fx2.json"]["folder"] == BEM_FOLDER

        s_tm, s_bem = await asyncio.gather(eth(tm), eth(bem))
        p_tm, p_bem = await asyncio.wait_for(asyncio.gather(
            tm.open_position(make_req(s_tm, 0.3, make_plan(tps=THREE_TPS), tag="cttm00000001")),
            bem.open_position(make_req(s_bem, 0.6, make_plan(tps=THREE_TPS), tag="ctbe00000001"))), GUARD)
        assert p_tm.status is PositionStatus.OPEN and p_bem.status is PositionStatus.OPEN
        assert {p.comment.split("-")[0] for p in tm_term.positions().values()} == {"cttm00000001"}
        assert {p.comment.split("-")[0] for p in bem_term.positions().values()} == {"ctbe00000001"}
        assert sum(p.volume for p in bem_term.positions().values()) == pytest.approx(0.6)

        # a stop move and a close on one account never touch the other
        p_bem = await bem.update_stop_loss(p_bem, 2220.0)
        assert {p.sl for p in bem_term.positions().values()} == {2220.0}
        assert 2220.0 not in {p.sl for p in tm_term.positions().values()}
        p_tm = await tm.close_position(p_tm, 1.0)
        assert p_tm.status is PositionStatus.CLOSED and tm_term.positions() == {}
        assert len(bem_term.positions()) == 3
        p_bem = await bem.close_position(p_bem, 1.0)
        assert bem_term.positions() == {}
        size = await bem.size_position(s_bem, Side.LONG, 2232.95, 2205.11, 100.0)
        assert size.quantity > 0
    finally:
        pids = [pid for pid in (getattr(tm._helper, "pid", None), getattr(bem._helper, "pid", None)) if pid]
        await asyncio.wait_for(asyncio.gather(tm.close(), bem.close()), GUARD)
        for pid in pids:
            await until(lambda pid=pid: not alive(pid), 5.0)


async def test_the_new_account_never_attaches_to_tickmills_terminal(two) -> None:
    tm_term, _bem_term, pins = two
    tm = _bridge(TICKMILL, [FX, BEM], pins, tm_term, TM_FOLDER, "Tickmill Ltd")
    await asyncio.wait_for(tm.connect(), GUARD)                 # tickmill pinned to its folder
    await tm.close()
    # fx2's terminal_path written wrong: it points at the Tickmill terminal
    wrong = dict(BEM, terminal_path=TM_FOLDER + r"\terminal64.exe")
    bem = _bridge(wrong, [FX, TICKMILL], pins, tm_term, TM_FOLDER, "Tickmill Ltd")
    with pytest.raises(Exception) as err:
        await asyncio.wait_for(bem.connect(), GUARD)
    assert "'tickmill' hesabının terminaline bağlandı" in str(err.value)
    assert tm_term.positions() == {}
    await bem.close()


async def test_the_new_account_refuses_its_terminal_logged_in_to_another_company(two) -> None:
    _tm_term, bem_term, pins = two
    bem = _bridge(BEM, [FX, TICKMILL], pins, bem_term, BEM_FOLDER, "FundingPips Ltd")
    with pytest.raises(Exception, match="bir BEM hesabı görünmüyor"):
        await asyncio.wait_for(bem.connect(), GUARD)
    await bem.close()


# --------------------------------------------------------------------------- #
# terminals installed from metatrader5.com: the same company for every account
# --------------------------------------------------------------------------- #
# On the owner's computer fx (FundingPips) and fx2 (BEM) both run in a terminal downloaded from metatrader5.com:
# terminal_info().company is "MetaQuotes Ltd." for both, only the ACCOUNT says whose it is (FundingPips Corp on
# FundingPips-SIM1, BEM Ltd. on BEM-Server). fx2 names its own terminal (terminal_path): that terminal's folder,
# not its maker, tells it apart from fx's.
FP_FOLDER = r"C:\Program Files\MetaTrader 5"
GENERIC = "MetaQuotes Ltd."
BEM_ACCOUNT = ("BEM Ltd.", "BEM-Server")


def _pin_fx(pins: Path) -> None:
    write_pin(str(pins), "fx", {"folder": FP_FOLDER, "path": FP_FOLDER + r"\terminal64.exe", "company": GENERIC,
                                "server": "FundingPips-SIM1", "login": 5001})


async def test_fx2_connects_to_its_metaquotes_terminal_next_to_fx_of_the_same_maker(two) -> None:
    _tm_term, bem_term, pins = two
    _pin_fx(pins)
    bem = _bridge(BEM, [FX, TICKMILL], pins, bem_term, BEM_FOLDER, GENERIC, BEM_ACCOUNT)
    try:
        await asyncio.wait_for(bem.connect(), GUARD)
        pinned = json.loads((pins / "mt5_terminal_fx2.json").read_text(encoding="utf-8"))
        assert (pinned["folder"], pinned["company"], pinned["server"]) == (BEM_FOLDER, GENERIC, "BEM-Server")
        s = await eth(bem)
        p = await asyncio.wait_for(
            bem.open_position(make_req(s, 0.6, make_plan(tps=THREE_TPS), tag="ctbe00000002")), GUARD)
        assert p.status is PositionStatus.OPEN
        assert {x.comment.split("-")[0] for x in bem_term.positions().values()} == {"ctbe00000002"}
        # fx's pin is untouched
        fx_pin = json.loads((pins / "mt5_terminal_fx.json").read_text(encoding="utf-8"))
        assert (fx_pin["folder"], fx_pin["company"]) == (FP_FOLDER, GENERIC)
    finally:
        await bem.close()


async def test_fx2_pointed_at_fxs_pinned_terminal_is_still_refused(two) -> None:
    _tm_term, bem_term, pins = two
    _pin_fx(pins)
    wrong = dict(BEM, terminal_path=FP_FOLDER + r"\terminal64.exe")
    bem = _bridge(wrong, [FX, TICKMILL], pins, bem_term, FP_FOLDER, GENERIC, ("FundingPips Corp", "FundingPips-SIM1"))
    with pytest.raises(Exception) as err:
        await asyncio.wait_for(bem.connect(), GUARD)
    assert "'fx' hesabının terminaline bağlandı" in str(err.value)
    assert bem_term.positions() == {}
    assert not (pins / "mt5_terminal_fx2.json").exists()
    await bem.close()


async def test_fx2s_metaquotes_terminal_logged_in_to_another_broker_is_still_refused(two) -> None:
    _tm_term, bem_term, pins = two
    _pin_fx(pins)
    bem = _bridge(BEM, [FX, TICKMILL], pins, bem_term, BEM_FOLDER, GENERIC, ("FundingPips Corp", "FundingPips-SIM1"))
    with pytest.raises(Exception, match="bir BEM hesabı görünmüyor"):
        await asyncio.wait_for(bem.connect(), GUARD)
    assert not (pins / "mt5_terminal_fx2.json").exists()
    await bem.close()


def test_the_report_card_accounts_of_each_person() -> None:
    from copytrader.app import real_account_names
    c = _app([FX, TICKMILL, BEM, {"name": "breakout", "type": "manual", "risk_usd": 50}])
    c.dry_run = False
    assert real_account_names(c, friend=False) == ["fx", "fx2"]           # (a manual account is on no card)
    assert real_account_names(c, friend=True) == ["tickmill"]
    assert real_account_names(c) == ["fx", "tickmill", "fx2"]
