"""The watchman's low-disk warning: below 5 GB free on the data folder's drive one message, a reminder at most every
30 minutes while it stays low, nothing once there is room again; the bot-down messages are unaffected."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from test_preflight3_tools import Clock, Outbox, b, bot_folder, state  # noqa: F401  (b: fixture)

GB = 1024 ** 3


def test_disk_decision_first_reminder_and_back(b: Any) -> None:
    an = 1_800_000_000.0
    msg, st = b.disk_karar({}, 20 * GB, an)
    assert msg is None and st.get("disk_ilk") is None
    msg, st = b.disk_karar(st, 3.2 * GB, an)
    assert msg == b.DISK_UYARI.format(bos="3,2 GB") and "3,2 GB boş" in msg
    for k in range(1, 6):                                                  # 25 minutes: silence
        msg, st = b.disk_karar(st, 3 * GB, an + k * 300)
        assert msg is None
    msg, st = b.disk_karar(st, 2.5 * GB, an + 1800)
    assert msg is not None and "hâlâ az yer var: 2,5 GB" in msg and "30 dakika" in msg
    msg, st = b.disk_karar(st, 30 * GB, an + 2100)                         # room again: no message
    assert msg is None and st["disk_ilk"] is None
    msg, st = b.disk_karar(st, 4 * GB, an + 2400)                          # low again 5 min after a message
    assert msg is None
    msg, st = b.disk_karar(st, 4 * GB, an + 1800 + 1800)                   # 30 min after the last: a new first one
    assert msg == b.DISK_UYARI.format(bos="4,0 GB")
    assert b.gb_metni(4.97 * GB) == "4,9 GB"                               # never "5,0 GB" below the 5 GB limit
    msg, st2 = b.disk_karar(st, None, an + 9999)                           # unreadable: nothing changes
    assert msg is None and st2 == st


def test_disk_warning_on_whole_passes_next_to_the_bot_messages(b: Any, tmp_path: Path) -> None:
    bot = bot_folder(tmp_path)
    clock, box = Clock(), Outbox()
    free = {"now": 20 * GB}
    seen = {"now": b.CALISIYOR}

    def run_pass() -> str:
        clock.minutes(5)
        return b.bak(bot, simdi=clock, gonder=box, durum_sor=lambda data: seen["now"],
                     disk_sor=lambda data: free["now"])

    assert run_pass() == b.CALISIYOR and box.sent == []
    free["now"] = 4 * GB
    run_pass()
    assert box.sent == [b.DISK_UYARI.format(bos="4,0 GB")]
    seen["now"] = b.KAPALI                                                 # the bot stops too: both are said
    assert run_pass() == "gonderildi" and box.sent[-1] == b.ILK_UYARI and len(box.sent) == 2
    for _ in range(4):
        run_pass()
    assert len(box.sent) == 2
    run_pass()                                                             # 30 min after the disk warning
    assert len(box.sent) == 3 and "hâlâ az yer var" in box.sent[-1]
    run_pass()                                                             # 30 min after the bot warning
    assert len(box.sent) == 4 and "hâlâ çalışmıyor" in box.sent[-1]
    free["now"] = 25 * GB
    seen["now"] = b.CALISIYOR
    assert run_pass() == "gonderildi" and box.sent[-1] == b.GERI_GELDI
    assert state(bot)["disk_ilk"] is None


def test_a_failed_disk_message_is_tried_again(b: Any, tmp_path: Path) -> None:
    bot = bot_folder(tmp_path)
    clock, box = Clock(), Outbox(fail=1)

    def run_pass() -> None:
        clock.minutes(5)
        b.bak(bot, simdi=clock, gonder=box, durum_sor=lambda data: b.CALISIYOR, disk_sor=lambda data: 1 * GB)

    run_pass()
    assert box.sent == [] and state(bot).get("disk_ilk") is None
    run_pass()
    assert box.sent == [b.DISK_UYARI.format(bos="1,0 GB")]
    run_pass()
    assert len(box.sent) == 1
