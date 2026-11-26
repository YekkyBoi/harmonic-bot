"""telegram.debounce_complete_sec: one message with picture + text (no album) is handed over after the short wait;
anything else keeps debounce_sec."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from tests.test_listener import CHAT, TRADER, FakeMessage, make_listener


def photo(i: int, text: str = "BTC SHORT 1R", **kw) -> FakeMessage:
    return FakeMessage(id=i, message=text, photo=SimpleNamespace(id=i), **kw)


async def test_picture_with_text_fires_after_the_short_wait(tmp_path):
    listener, _db, rec = make_listener(tmp_path, debounce_sec=1.0, debounce_complete_sec=0.05)
    await listener.handle_message(photo(10))
    await asyncio.sleep(0.3)
    assert rec.ready == [(CHAT, TRADER)]
    assert rec.snapshots == [[10]]


async def test_off_by_default(tmp_path):
    listener, _db, rec = make_listener(tmp_path, debounce_sec=0.6)
    await listener.handle_message(photo(10))
    await asyncio.sleep(0.3)
    assert rec.ready == []
    await asyncio.sleep(0.6)
    assert rec.ready == [(CHAT, TRADER)]


async def test_text_only_keeps_the_normal_wait(tmp_path):
    listener, _db, rec = make_listener(tmp_path, debounce_sec=0.6, debounce_complete_sec=0.05)
    await listener.handle_message(FakeMessage(id=11, message="BTC SHORT 1R"))
    await asyncio.sleep(0.3)
    assert rec.ready == []
    await asyncio.sleep(0.6)
    assert rec.ready == [(CHAT, TRADER)]


async def test_picture_without_text_keeps_the_normal_wait(tmp_path):
    listener, _db, rec = make_listener(tmp_path, debounce_sec=0.6, debounce_complete_sec=0.05)
    await listener.handle_message(photo(12, text=""))
    await asyncio.sleep(0.3)
    assert rec.ready == []
    await asyncio.sleep(0.6)
    assert rec.ready == [(CHAT, TRADER)]


async def test_album_keeps_the_normal_wait(tmp_path):
    listener, _db, rec = make_listener(tmp_path, debounce_sec=0.6, debounce_complete_sec=0.05)
    await listener.handle_message(photo(13, grouped_id=77))
    await asyncio.sleep(0.3)
    assert rec.ready == []
    await asyncio.sleep(0.6)
    assert rec.ready == [(CHAT, TRADER)]


async def test_text_before_the_picture_keeps_the_batch_on_the_normal_wait(tmp_path):
    listener, _db, rec = make_listener(tmp_path, debounce_sec=0.6, debounce_complete_sec=0.05)
    await listener.handle_message(FakeMessage(id=20, message="Limit emir"))
    await listener.handle_message(photo(21))
    await asyncio.sleep(0.3)
    assert rec.ready == []
    await asyncio.sleep(0.7)
    assert rec.snapshots == [[20, 21]]


async def test_slow_second_message_still_joins_the_batch(tmp_path):
    listener, _db, rec = make_listener(tmp_path, debounce_sec=0.6, debounce_complete_sec=0.05)
    slow = FakeMessage(id=31, photo=SimpleNamespace(id=1), download_delay=0.3)
    listener._spawn(listener.handle_message(photo(30)))
    listener._spawn(listener.handle_message(slow))
    await asyncio.sleep(1.3)
    assert rec.snapshots == [[30, 31]]


async def test_next_batch_starts_fresh(tmp_path):
    listener, _db, rec = make_listener(tmp_path, debounce_sec=0.6, debounce_complete_sec=0.05)
    await listener.handle_message(FakeMessage(id=40, message="selam"))
    await asyncio.sleep(0.9)
    await listener.handle_message(photo(41))
    await asyncio.sleep(0.3)
    assert len(rec.ready) == 2               # the second batch did not wait the full debounce_sec
