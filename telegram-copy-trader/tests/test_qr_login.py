"""QR login: terminal rendering keeps the code readable, and the login loop refreshes / handles 2FA."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
import segno

from copytrader.config import TelegramConfig
from copytrader.telegram import listener

URL = "tg://login?token=AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSYnKCkqKywtLi8w"


def test_render_qr_text_round_trips_the_matrix() -> None:
    border = 2
    text = listener.render_qr_text(URL, border=border)
    matrix = segno.make(URL, error="l").matrix
    size = len(matrix)
    total = size + 2 * border
    lines = text.split("\n")
    assert len(lines) == (total + 1) // 2 and all(len(line) == total for line in lines)
    top = {"█": True, "▀": True, "▄": False, " ": False}
    bottom = {"█": True, "▀": False, "▄": True, " ": False}
    for r in range(total):
        for c in range(total):
            ch = lines[r // 2][c]
            is_light = (top if r % 2 == 0 else bottom)[ch]
            rr, cc = r - border, c - border
            expected_light = not matrix[rr][cc] if 0 <= rr < size and 0 <= cc < size else True
            assert is_light == expected_light, (r, c)


class FakeQR:
    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes = outcomes
        self.url = URL
        self.expires = datetime.now(timezone.utc) + timedelta(seconds=30)
        self.recreated = 0
        self.timeouts: list[float | None] = []
        self._resp = None
        self.resp_after_recreate = None

    async def wait(self, timeout=None):
        self.timeouts.append(timeout)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def recreate(self):
        self.recreated += 1
        self._resp = self.resp_after_recreate


class FakeClient:
    def __init__(self, qr: FakeQR) -> None:
        self.qr = qr
        self.signed_password = None
        self.passwords_tried: list[str] = []
        self.wrong_passwords: set[str] = set()
        self.qr_login_error: BaseException | None = None
        self.logged_in_user = None
        self.session = SimpleNamespace()

    async def connect(self):
        pass

    async def disconnect(self):
        pass

    async def is_user_authorized(self):
        return False

    async def qr_login(self):
        if self.qr_login_error is not None:
            raise self.qr_login_error
        return self.qr

    async def sign_in(self, password=None):
        from telethon import errors

        self.passwords_tried.append(password)
        if password in self.wrong_passwords:
            raise errors.PasswordHashInvalidError(request=None)
        self.signed_password = password

    async def _on_login(self, user):
        self.logged_in_user = user

    async def get_me(self):
        return SimpleNamespace(bot=False, id=1, first_name="Owner", last_name=None, title=None, username=None)


@pytest.fixture
def cfg(tmp_path) -> TelegramConfig:
    return TelegramConfig(api_id=1, api_hash="h", source_chats=[-1], session_path=str(tmp_path / "telegram_user"))


def _patch(monkeypatch, client: FakeClient) -> None:
    monkeypatch.setattr(listener, "make_user_client", lambda config, prefer_file=False: client)
    monkeypatch.setattr(listener, "_clear_screen", lambda: None)
    monkeypatch.setattr(listener.StringSession, "save", staticmethod(lambda session: "SESSION"))


async def test_qr_login_refreshes_expired_code_then_succeeds(monkeypatch, cfg, capsys) -> None:
    qr = FakeQR([asyncio.TimeoutError(), SimpleNamespace(id=1)])
    client = FakeClient(qr)
    _patch(monkeypatch, client)
    assert await listener.interactive_qr_login(cfg) == "SESSION"
    assert qr.recreated == 1
    out = capsys.readouterr().out
    assert "Masaüstü Cihaz Bağla" in out and "BAŞKA bir cihazda" in out


async def test_qr_login_asks_2fa_password(monkeypatch, cfg) -> None:
    from telethon import errors

    qr = FakeQR([errors.SessionPasswordNeededError(request=None)])
    client = FakeClient(qr)
    _patch(monkeypatch, client)

    async def fake_ask(prompt: str, secret: bool = False) -> str:
        assert secret
        return "pw123"

    monkeypatch.setattr(listener, "_ask", fake_ask)
    assert await listener.interactive_qr_login(cfg) == "SESSION"
    assert client.signed_password == "pw123"


async def test_qr_wait_timeout_ignores_vps_clock_skew(monkeypatch, cfg) -> None:
    """A VPS clock running ahead of Telegram must not make wait() time out instantly and spin."""
    qr = FakeQR([asyncio.TimeoutError(), SimpleNamespace(id=1)])
    qr.expires = datetime.now(timezone.utc) - timedelta(seconds=60)    # "already expired" by the local clock
    client = FakeClient(qr)
    _patch(monkeypatch, client)
    await listener.interactive_qr_login(cfg)
    assert qr.timeouts and all(t is not None and 10.0 <= t <= 30.0 for t in qr.timeouts)


async def test_scan_accepted_during_recreate_completes_login(monkeypatch, cfg) -> None:
    from telethon.tl import types

    qr = FakeQR([asyncio.TimeoutError()])     # second wait() is never reached
    user = types.User(id=42)
    qr.resp_after_recreate = types.auth.LoginTokenSuccess(authorization=types.auth.Authorization(user=user))
    client = FakeClient(qr)
    _patch(monkeypatch, client)
    assert await listener.interactive_qr_login(cfg) == "SESSION"
    assert client.logged_in_user is user


async def test_wrong_2fa_password_is_asked_again(monkeypatch, cfg, capsys) -> None:
    from telethon import errors

    qr = FakeQR([errors.SessionPasswordNeededError(request=None)])
    client = FakeClient(qr)
    client.wrong_passwords = {"typo"}
    _patch(monkeypatch, client)
    answers = iter(["typo", "right"])

    async def fake_ask(prompt: str, secret: bool = False) -> str:
        return next(answers)

    monkeypatch.setattr(listener, "_ask", fake_ask)
    assert await listener.interactive_qr_login(cfg) == "SESSION"
    assert client.passwords_tried == ["typo", "right"] and client.signed_password == "right"
    assert "Şifre yanlış" in capsys.readouterr().out


async def test_rerun_after_unfinished_2fa_goes_straight_to_password(monkeypatch, cfg) -> None:
    from telethon import errors

    client = FakeClient(FakeQR([]))
    client.qr_login_error = errors.SessionPasswordNeededError(request=None)
    _patch(monkeypatch, client)

    async def fake_ask(prompt: str, secret: bool = False) -> str:
        return "pw"

    monkeypatch.setattr(listener, "_ask", fake_ask)
    assert await listener.interactive_qr_login(cfg) == "SESSION"
    assert client.signed_password == "pw"


def test_cli_login_uses_qr_by_default_and_code_with_flag(monkeypatch, tmp_path) -> None:
    from copytrader import __main__ as cli

    used: list[str] = []

    async def fake_qr(tg):
        used.append("qr")
        return "S"

    async def fake_code(tg):
        used.append("code")
        return "S"

    monkeypatch.setattr(listener, "interactive_qr_login", fake_qr)
    monkeypatch.setattr(listener, "interactive_login", fake_code)
    monkeypatch.setattr(cli, "_telegram_config", lambda path: SimpleNamespace(session_path="data/x"))
    assert cli.main(["login"]) == 0
    assert cli.main(["login", "--code"]) == 0
    assert used == ["qr", "code"]
