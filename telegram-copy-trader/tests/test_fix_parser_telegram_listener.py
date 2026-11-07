"""Regression tests for audited listener defects F13 (edit during download) and F18 (bad session string)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from telethon.crypto import AuthKey
from telethon.sessions import StringSession

from copytrader.config import TelegramConfig
from copytrader.telegram.listener import TelegramAuthError, make_user_client
from test_listener import CHAT, TRADER, FakeMessage, make_listener, settle


# --------------------------------------------------------------------------- #
# F13: an edit that arrives while the photo is still downloading
# --------------------------------------------------------------------------- #
async def test_f13_edit_during_download_stores_the_edited_version_before_on_ready(tmp_path):
    listener, db, rec = make_listener(tmp_path, debounce_sec=0.05)
    photo = SimpleNamespace(id=5)
    listener._spawn(listener.handle_message(
        FakeMessage(id=700, message="eth long 2232.18 stop 2205.11", photo=photo, download_delay=0.3)))
    await asyncio.sleep(0.05)                                   # download in progress, nothing stored yet
    edit = FakeMessage(id=700, message="eth long 2232.18 stop 2215", photo=photo, download_delay=0.3)
    await listener.handle_edit(edit)
    await settle(0.6)
    stored = db.messages[(CHAT, 700)]
    assert stored.text == "eth long 2232.18 stop 2215" and stored.edited
    assert stored.media_path is not None and edit.downloads == 0     # same photo: not downloaded again
    assert rec.ready == [(CHAT, TRADER)]
    assert rec.snapshots == [[700]]                             # engine called once, after the edited save
    assert rec.edits == []                                      # nothing was built from the old text


async def test_f13_newest_of_several_edits_wins(tmp_path):
    listener, db, rec = make_listener(tmp_path, debounce_sec=0.05)
    t0 = datetime.now(timezone.utc)
    listener._spawn(listener.handle_message(
        FakeMessage(id=701, message="v1", photo=SimpleNamespace(id=5), download_delay=0.3)))
    await asyncio.sleep(0.05)
    v3 = FakeMessage(id=701, message="v3", photo=SimpleNamespace(id=5))
    v3.edit_date = t0 + timedelta(seconds=3)
    v2 = FakeMessage(id=701, message="v2", photo=SimpleNamespace(id=5))
    v2.edit_date = t0 + timedelta(seconds=2)
    await listener.handle_edit(v3)
    await listener.handle_edit(v2)                              # delivered late: older than v3
    await settle(0.6)
    assert db.messages[(CHAT, 701)].text == "v3"


async def test_f13_edit_with_new_photo_during_download_downloads_the_new_one(tmp_path):
    listener, db, rec = make_listener(tmp_path, debounce_sec=0.05)
    listener._spawn(listener.handle_message(
        FakeMessage(id=702, message="chart", photo=SimpleNamespace(id=1), download_delay=0.2)))
    await asyncio.sleep(0.05)
    edit = FakeMessage(id=702, message="chart", photo=SimpleNamespace(id=2))
    await listener.handle_edit(edit)
    await settle(0.5)
    assert edit.downloads == 1 and db.messages[(CHAT, 702)].edited


async def test_f13_edit_while_row_is_being_written_is_applied_as_normal_edit(tmp_path):
    listener, db, rec = make_listener(tmp_path, debounce_sec=0.05)
    gate = asyncio.Event()
    real_save = db.save_message

    async def slow_save(msg):
        await real_save(msg)
        await gate.wait()                                       # row written, save call not returned yet

    db.save_message = slow_save
    task = listener._spawn(listener.handle_message(FakeMessage(id=703, message="eth long stop 2205")))
    await asyncio.sleep(0.05)
    await listener.handle_edit(FakeMessage(id=703, message="eth long stop 2215"))
    db.save_message = real_save
    gate.set()
    await task
    await settle()
    assert db.messages[(CHAT, 703)].text == "eth long stop 2215" and db.messages[(CHAT, 703)].edited
    assert [m.text for m in rec.edits] == ["eth long stop 2215"]   # engine told about the edit


async def test_f13_reaction_only_edit_during_download_is_not_marked_edited(tmp_path):
    listener, db, rec = make_listener(tmp_path, debounce_sec=0.05)
    listener._spawn(listener.handle_message(
        FakeMessage(id=704, message="same", photo=SimpleNamespace(id=5), download_delay=0.2)))
    await asyncio.sleep(0.05)
    await listener.handle_edit(FakeMessage(id=704, message="same", photo=SimpleNamespace(id=5)))
    await settle(0.5)
    assert db.messages[(CHAT, 704)].edited is False


# --------------------------------------------------------------------------- #
# F18: truncated / corrupted TG_SESSION_STRING
# --------------------------------------------------------------------------- #
def _session_string() -> str:
    s = StringSession()
    s.set_dc(2, "149.154.167.51", 443)
    s.auth_key = AuthKey(b"\x01" * 256)
    return s.save()


def _cfg(session: str) -> TelegramConfig:
    return TelegramConfig(api_id=1, api_hash="h", source_chats=[CHAT], session_string=session)


def test_f18_every_truncation_is_a_telegram_auth_error():
    full = _session_string()
    make_user_client(_cfg(full))                                 # the full string works
    for n in range(2, len(full)):
        with pytest.raises(TelegramAuthError) as ei:
            make_user_client(_cfg(full[:n]))
        assert "TG_SESSION_STRING" in str(ei.value) and "login" in str(ei.value)


@pytest.mark.parametrize("bad", ["x", "1@@@@", "1" + "A" * 351, "1" + "é" * 40])
def test_f18_garbage_is_a_telegram_auth_error(bad: str):
    with pytest.raises(TelegramAuthError):
        make_user_client(_cfg(bad))
