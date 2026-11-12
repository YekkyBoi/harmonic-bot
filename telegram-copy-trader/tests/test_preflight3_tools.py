"""Owner tooling, second round (no network, no real Telegram, no real Windows):

U1  bekci.py      the watchman: one Telegram message when the bot is NOT running, reminders, "it is back";
                  guncelle.py registers it as a Windows scheduled task and keeps it silent during an update
U2  guncelle.py   the MetaApi token an older version wrote into copytrader.log* is masked in place
U3  selftest      the adapter's own account report (real ccxt code, HTTP faked) and the Telegram group's
                  posting rights (fake client; nothing is sent)
U4  docs/GUNCELLEME.md   what the owner is told about all of this

What only a real Windows server can prove (schtasks itself, the SYSTEM account, the certificate store) is not
proven here: these tests cover the logic around it with injected fakes.
"""

from __future__ import annotations

import ast
import asyncio
import http.server
import importlib.util
import inspect
import json
import os
import subprocess
import sys
import threading
import time
import urllib.parse
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from telethon import utils
from telethon.tl import functions, types

import copytrader.__main__ as cli
from copytrader import selftest
from copytrader.brokers.dry_run import DryRunBroker
from copytrader.config import AppConfig, load_config

from binance_rest_fake import make_binance
from fakes import FakePriceOracle
from test_preflight2_tools import Venue, paper_config

REPO = Path(__file__).resolve().parents[1]
POSIX_PROC = os.name == "posix" and Path("/proc/self/stat").is_file()

TOKEN = "123456789:" + "AbC-_" * 7                     # the shape of a bot token; not a real one
OWNER = 4242
ENV = {"TG_API_ID": "123456", "TG_API_HASH": "hash-SECRET-1", "TG_BOT_TOKEN": TOKEN, "TG_OWNER_ID": str(OWNER),
       "ANTHROPIC_API_KEY": "sk-ant-SECRET-" + "x" * 30}
# The parts of the owner's production config.yaml that matter here (placeholders as in production).
PROD_YAML = """\
# canlı ayarlar
dry_run: false
data_dir: data
timezone: Europe/Istanbul
telegram:
  api_id: ${TG_API_ID}
  api_hash: ${TG_API_HASH}
  source_chats: [-1003782294463]
  topics:
    - name: kripto
      title_contains: ["Kripto İşlemleri"]
      brokers: [sanal]
      kind: crypto
    - name: fx
      title_contains: ["Fx İşlemleri"]
      brokers: [sanal]
      kind: fx
notifier:
  bot_token: ${TG_BOT_TOKEN}
  owner_user_id: ${TG_OWNER_ID}
llm:
  api_key: ${ANTHROPIC_API_KEY}
brokers:
  - name: sanal
    type: paper
"""
NEW_MAIN = 'LOCK_FILE = "bot.lock"\nprint("YENI SURUM")\n'
OLD_MAIN = "print('ESKI SURUM')\n"


@pytest.fixture(autouse=True)
def _keep_environment():
    """load_config() copies .env values into os.environ and the watchman reads os.environ: start every test
    without the bot's names and undo all changes afterwards."""
    saved = dict(os.environ)
    for name in ENV:
        os.environ.pop(name, None)
    yield
    os.environ.clear()
    os.environ.update(saved)


def load(name: str) -> Any:
    """A top-level script of the package root as a module (no bytecode written next to it)."""
    saved, sys.dont_write_bytecode = sys.dont_write_bytecode, True
    try:
        spec = importlib.util.spec_from_file_location(f"{name}_under_test3", REPO / f"{name}.py")
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = saved
    return module


@pytest.fixture(scope="module")
def b() -> Any:
    return load("bekci")


@pytest.fixture(scope="module")
def g() -> Any:
    module = load("guncelle")
    module.BEKLEME = 0.0
    return module


def bot_folder(folder: Path, config: str = PROD_YAML, env: dict[str, str] | None = None, main: str = NEW_MAIN) -> Path:
    """A bot folder as the watchman finds it: config.yaml, .env, the installed package, the data folder."""
    bot = folder / "telegram-copy-trader"
    (bot / "copytrader").mkdir(parents=True)
    (bot / "copytrader" / "__init__.py").write_text("", encoding="utf-8")
    (bot / "copytrader" / "__main__.py").write_text(main, encoding="utf-8")
    (bot / "config.yaml").write_text(config, encoding="utf-8")
    (bot / ".env").write_text("".join(f"{k}={v}\n" for k, v in (ENV if env is None else env).items()), encoding="utf-8")
    (bot / "data" / "logs").mkdir(parents=True)
    return bot


class Clock:
    def __init__(self, now: float = 1_800_000_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def minutes(self, n: float) -> None:
        self.now += n * 60.0


class Outbox:
    """Stands in for the Telegram request: keeps what would be sent; ``fail``: the next n sends fail."""

    def __init__(self, fail: int = 0) -> None:
        self.sent: list[str] = []
        self.calls: list[tuple[str, int]] = []
        self.fail = fail

    def __call__(self, token: str, chat: int, text: str) -> None:
        self.calls.append((token, chat))
        if self.fail:
            self.fail -= 1
            raise OSError(f"bağlantı yok https://api.telegram.org/bot{token}/sendMessage")
        self.sent.append(text)


def state(bot: Path) -> dict[str, Any]:
    return json.loads((bot / "data" / "bekci.json").read_text(encoding="utf-8"))


# =========================================================================== #
# U1  bekci.py: settings
# =========================================================================== #
def test_watchman_reads_the_same_token_and_owner_as_the_bot(b: Any, tmp_path: Path) -> None:
    bot = bot_folder(tmp_path)
    assert b.bildirim_ayarlari(bot) == (TOKEN, OWNER)
    assert b.veri_klasoru(bot) == bot / "data"
    cfg = load_config(bot / "config.yaml")                                 # the bot's own loader on the same folder
    assert (cfg.notifier.bot_token, cfg.notifier.owner_user_id) == (TOKEN, OWNER)


@pytest.mark.parametrize("notifier", [
    "notifier:\n  bot_token: ${TG_BOT_TOKEN}    # @BotFather -> /newbot ; owner_user_id: 1 yazmayın\n"
    "  owner_user_id: ${TG_OWNER_ID} # kendi kullanıcı ID'niz\n",
    f"notifier:\n    owner_user_id: {OWNER}\n    confirm_timeout_sec: 600\n    bot_token: \"{TOKEN}\"\n",
    f"notifier:   # bildirim botu\n  bot_token: '{TOKEN}'\n\n  # owner_user_id: 1\n  owner_user_id: {OWNER}   # ben\n",
    "notifier: {bot_token: ${TG_BOT_TOKEN}, owner_user_id: ${TG_OWNER_ID}, notify_on_execution: true}\n",
    "notifier:\n  bot_token: ${BASKA_AD:-" + TOKEN + "}\n  owner_user_id: ${BASKA_KIMLIK:-" + str(OWNER) + "}\n",
    "bildirim: yok\n",                                                     # unreadable section: the names of .env.example
], ids=["placeholders+comments", "literal-quoted", "single-quotes", "inline-section", "defaults", "env-names"])
def test_watchman_reads_other_ways_of_writing_the_notifier_section(b: Any, tmp_path: Path, notifier: str) -> None:
    text = ("﻿# yorum\r\ndata_dir: 'veri'   # yorum\r\ntelegram:\r\n  bot_token: baska-bir-sey\r\n"
            + notifier.replace("\n", "\r\n") + "llm:\r\n  owner_user_id: 7\r\n")
    bot = bot_folder(tmp_path, config=text)
    assert b.bildirim_ayarlari(bot) == (TOKEN, OWNER)
    assert b.veri_klasoru(bot) == bot / "veri"


def test_watchman_settings_env_rules_and_refusals(b: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    other = "987654321:" + "Z" * 35
    bot = bot_folder(tmp_path, env={"TG_OWNER_ID": "'77'"})
    (bot / ".env").write_text(f"# yorum\nexport TG_BOT_TOKEN=\"{TOKEN}\"\nTG_OWNER_ID='77'\nBOZUK SATIR\n", encoding="utf-8")
    assert b.bildirim_ayarlari(bot) == (TOKEN, 77)
    monkeypatch.setenv("TG_BOT_TOKEN", other)                              # the process environment wins, as in the bot
    assert b.bildirim_ayarlari(bot) == (other, 77)
    monkeypatch.setenv("TG_BOT_TOKEN", "")                                 # ... unless it is empty
    assert b.bildirim_ayarlari(bot) == (TOKEN, 77)

    for env, expected in (({"TG_BOT_TOKEN": "token-degil", "TG_OWNER_ID": "5"}, (None, 5)),
                          ({"TG_BOT_TOKEN": TOKEN, "TG_OWNER_ID": "ben"}, (TOKEN, None)),
                          ({"TG_BOT_TOKEN": TOKEN, "TG_OWNER_ID": "-5"}, (TOKEN, None)),
                          ({}, (None, None))):
        (bot / ".env").write_text("".join(f"{k}={v}\n" for k, v in env.items()), encoding="utf-8")
        assert b.bildirim_ayarlari(bot) == expected
    (bot / ".env").unlink()
    (bot / "config.yaml").unlink()
    assert b.bildirim_ayarlari(bot) == (None, None) and b.veri_klasoru(bot) == bot / "data"


# =========================================================================== #
# U1  bekci.py: what to say (pure)
# =========================================================================== #
def test_watchman_says_it_once_reminds_every_30_minutes_and_reports_the_return(b: Any) -> None:
    t0 = 1_800_000_000.0
    said: list[tuple[float, str]] = []
    st: dict[str, Any] = {}
    # minute -> what the pass sees
    seen = {0: b.CALISIYOR, 5: b.KAPALI, 10: b.KAPALI, 15: b.BILINMIYOR, 20: b.KAPALI, 25: b.KAPALI, 30: b.KAPALI,
            35: b.KAPALI, 40: b.GUNCELLEME, 45: b.ESKI_SURUM, 50: b.KAPALI, 55: b.KAPALI, 60: b.KAPALI, 65: b.KAPALI,
            70: b.CALISIYOR, 75: b.CALISIYOR, 80: b.KAPALI, 85: b.CALISIYOR}
    for minute, what in seen.items():
        message, st = b.karar(st, what, t0 + minute * 60)
        if message:
            said.append((minute, message))
    assert said == [
        (5, "⚠️ Bot çalışmıyor (sunucu yeniden başlamış ya da pencere kapanmış olabilir). Sunucuya bağlanıp botu başlatın."),
        (35, "⚠️ Bot hâlâ çalışmıyor (ilk uyarıdan bu yana 30 dakika geçti). Sunucuya bağlanıp botu başlatın."),
        (65, "⚠️ Bot hâlâ çalışmıyor (ilk uyarıdan bu yana 60 dakika geçti). Sunucuya bağlanıp botu başlatın."),
        (70, "✅ Bot tekrar çalışıyor."),
        (80, "⚠️ Bot çalışmıyor (sunucu yeniden başlamış ya da pencere kapanmış olabilir). Sunucuya bağlanıp botu başlatın."),
        (85, "✅ Bot tekrar çalışıyor."),
    ]
    assert st["ilk_uyari"] is None and st["son_uyari"] is None and st["uyari_sayisi"] == 0 and st["son_durum"] == b.CALISIYOR


def test_watchman_never_more_than_one_message_per_30_minutes_whatever_the_state_holds(b: Any) -> None:
    t0 = 1_800_000_000.0
    # a whole day down, one pass a minute: the first message and then exactly one per 30 minutes
    st: dict[str, Any] = {}
    times = []
    for minute in range(24 * 60):
        message, st = b.karar(st, b.KAPALI, t0 + minute * 60)
        if message:
            times.append(minute)
    assert times == list(range(0, 24 * 60, 30)) and st["uyari_sayisi"] == 48
    assert "23 saat 30 dakika" in b.karar(dict(st, son_uyari=t0), b.KAPALI, t0 + 23.5 * 3600)[0]

    # the clock was set back two hours: no message now, the next one 30 minutes from the new time
    message, back = b.karar({"ilk_uyari": t0, "son_uyari": t0, "uyari_sayisi": 1}, b.KAPALI, t0 - 7200)
    assert message is None and back["son_uyari"] == t0 - 7200
    assert b.karar(back, b.KAPALI, t0 - 7200 + 29 * 60)[0] is None
    assert "1 dakika" in b.karar(back, b.KAPALI, t0 - 7200 + 30 * 60)[0]   # never a negative time in the text

    # damaged state values never raise and never mean "say it at every pass"
    for broken in ({"ilk_uyari": "dün", "son_uyari": None}, {"ilk_uyari": True, "son_uyari": []}, {"uyari_sayisi": "x"}):
        message, st = b.karar(broken, b.KAPALI, t0)
        assert message == b.ILK_UYARI and b.karar(st, b.KAPALI, t0 + 60)[0] is None
    message, st = b.karar({"ilk_uyari": t0, "son_uyari": t0, "uyari_sayisi": "bozuk"}, b.KAPALI, t0 + 1800)
    assert message is not None and st["uyari_sayisi"] == 2
    assert b.karar({}, b.CALISIYOR, t0)[0] is None and b.karar({}, b.BILINMIYOR, t0) == (None, {"son_durum": b.BILINMIYOR})
    assert [b.sure_metni(s) for s in (0, 59, 60, 89 * 60, 90 * 60, 125 * 60, 47 * 3600, 48 * 3600, 76 * 3600)] == [
        "1 dakika", "1 dakika", "1 dakika", "89 dakika", "1 saat 30 dakika", "2 saat 5 dakika", "47 saat", "2 gün",
        "3 gün 4 saat"]


# =========================================================================== #
# U1  bekci.py: the lock file
# =========================================================================== #
@pytest.fixture
def sleeper():
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(300)"])
    yield proc
    proc.kill()
    proc.wait()


def write_lock(data: Path, **fields: Any) -> None:
    data.mkdir(parents=True, exist_ok=True)
    (data / "bot.lock").write_text(json.dumps(fields), encoding="utf-8")


def test_watchman_lock_rules_with_injected_process_states(b: Any, tmp_path: Path) -> None:
    data = tmp_path / "data"
    alive = {7: (True, "t7"), 8: (False, None), 9: (None, None), 10: (True, None)}
    look = lambda pid: alive.get(pid, (False, None))      # noqa: E731
    never, always = (lambda started: False), (lambda started: True)

    assert b.bot_durumu(data, look, never) == b.KAPALI                     # no folder, no file
    data.mkdir()
    assert b.bot_durumu(data, look, never) == b.KAPALI
    for text in ("", "{", "[]", '{"pid": "x"}', '{"token": "t7"}', "\xff\xfe"):
        (data / "bot.lock").write_bytes(text.encode("latin-1"))
        assert b.bot_durumu(data, look, never) == b.KAPALI, text           # empty / damaged: nobody holds it
    write_lock(data, pid=7, token="t7", started="2026-11-12T08:00:00+00:00")
    assert b.bot_durumu(data, look, never) == b.CALISIYOR
    # Windows (the creation time is a point in time): same pid AND same creation time is the bot itself. A clock
    # that was corrected while the bot runs (then "from before the last system start" would say yes) must not
    # turn a running bot into an alarm. Linux counts the creation time from the system start: the rule stays.
    assert b.bot_durumu(data, look, always, mutlak=True) == b.CALISIYOR
    assert b.bot_durumu(data, look, always, mutlak=False) == b.KAPALI
    assert b.bot_durumu(data, look, always) == (b.CALISIYOR if os.name == "nt" else b.KAPALI)
    write_lock(data, pid=7, token="baska", started="x")
    assert b.bot_durumu(data, look, never) == b.KAPALI                     # the pid belongs to another program now
    write_lock(data, pid=8, token="t8")
    assert b.bot_durumu(data, look, never) == b.KAPALI                     # the process is gone
    write_lock(data, pid=9, token="t9")
    assert b.bot_durumu(data, look, never) == b.BILINMIYOR                 # cannot be told: nothing is said
    write_lock(data, pid=10, token="t10")                                 # alive, creation time not readable
    assert b.bot_durumu(data, look, never) == b.CALISIYOR and b.bot_durumu(data, look, always) == b.KAPALI
    write_lock(data, pid=7)                                               # a lock without a creation time
    assert b.bot_durumu(data, look, never) == b.CALISIYOR and b.bot_durumu(data, look, always) == b.KAPALI
    write_lock(data, pid=os.getpid(), token="x")
    assert b.bot_durumu(data, lambda pid: (True, "x"), never) == b.KAPALI  # our own pid: its writer cannot be alive
    write_lock(data, pid=0)
    assert b.bot_durumu(data, lambda pid: (True, None), never) == b.CALISIYOR and b.bot_durumu(data) == b.KAPALI
    (data / "bot.lock").unlink()
    (data / "bot.lock").mkdir()                                            # exists but cannot be read
    assert b.bot_durumu(data, look, never) == b.BILINMIYOR


@pytest.mark.skipif(not POSIX_PROC, reason="needs /proc")
def test_watchman_sees_a_real_bot_lock_and_its_end(b: Any, tmp_path: Path, sleeper: Any) -> None:
    """A lock written by the bot's own code in another process is 'running'; the same file after that process was
    killed (a closed window: nothing removes the file) is 'not running'. The bot's own check agrees."""
    data = tmp_path / "data"
    code = ("import sys, time; sys.path.insert(0, sys.argv[1]); import copytrader.__main__ as cli; "
            "cli.acquire_lock(sys.argv[2]); print('kilit', flush=True); time.sleep(300)")
    proc = subprocess.Popen([sys.executable, "-c", code, str(REPO), str(data)], stdout=subprocess.PIPE, text=True,
                            env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))
    try:
        assert proc.stdout is not None and proc.stdout.readline().strip() == "kilit"
        assert b.bot_durumu(data) == b.CALISIYOR and cli.lock_holder(data) is not None
        assert b.surec_durumu(proc.pid) == cli._process_state(proc.pid)
    finally:
        proc.kill()
        proc.wait()
    assert (data / "bot.lock").is_file()                                   # left behind
    assert b.bot_durumu(data) == b.KAPALI and cli.lock_holder(data) is None

    # the pid is alive again but it is another process (after a restart of the server): not the bot.
    # (Another creation time is written by hand: Linux counts it in 10 ms steps, two processes of this test
    # can share one.)
    info = json.loads((data / "bot.lock").read_text(encoding="utf-8"))
    write_lock(data, pid=sleeper.pid, token=str(int(b.surec_durumu(sleeper.pid)[1]) + 12345), started=info["started"])
    assert b.bot_durumu(data) == b.KAPALI and cli.lock_holder(data) is None
    write_lock(data, pid=sleeper.pid, token=b.surec_durumu(sleeper.pid)[1], started=info["started"])
    assert b.bot_durumu(data) == b.CALISIYOR and cli.lock_holder(data) is not None
    assert b.surec_durumu(0) == (False, None) and b.surec_durumu(-5) == (False, None)
    uptime = b.calisma_suresi()
    assert uptime is not None and uptime > 0
    assert b.acilistan_once_yazilmis("2001-01-01T00:00:00+00:00") is True
    assert b.acilistan_once_yazilmis("2999-01-01T00:00:00+00:00") is False
    assert b.acilistan_once_yazilmis("2001-01-01T00:00:00") is False and b.acilistan_once_yazilmis(None) is False


def test_watchman_recognises_an_update_in_progress_and_an_old_version(b: Any, g: Any, tmp_path: Path) -> None:
    bot = bot_folder(tmp_path)
    data = bot / "data"
    now = time.time()
    assert b.guncelleme_suruyor(data, now) is False
    g.guncelleme_kilidi_yaz(data)                                          # what the updater writes
    assert b.guncelleme_suruyor(data, now) is True and b.guncelleme_suruyor(data, now + 29 * 60) is True
    assert b.guncelleme_suruyor(data, now + 31 * 60) is False              # a forgotten lock stops counting
    g.guncelleme_kilidi_kaldir(data, bot_basliyor=True)                    # the bot is being started
    assert (data / "guncelleme.lock").is_file()
    assert b.guncelleme_suruyor(data, time.time() + 60) is True            # ... its own lock is not there at once
    assert b.guncelleme_suruyor(data, time.time() + g.BASLANGIC_PAYI + 5) is False
    g.guncelleme_kilidi_kaldir(data)
    assert not (data / "guncelleme.lock").exists() and b.guncelleme_suruyor(data, now) is False
    g.guncelleme_kilidi_kaldir(data)                                       # nothing there: no error
    g.guncelleme_kilidi_kaldir(data, bot_basliyor=True)
    g.guncelleme_kilidi_yaz(tmp_path / "olmayan-klasor")                   # no data folder: nothing is created
    assert not (tmp_path / "olmayan-klasor").exists()
    assert (g.GUNCELLEME_KILIDI, g.KILIT_OMRU, g.KILIT_DOSYASI, g.BEKCI_DURUMU, g.BEKCI_DENEME) == (
        b.GUNCELLEME_KILIDI, b.GUNCELLEME_SURESI, b.KILIT_DOSYASI, b.DURUM_DOSYASI, b.DENEME_ISTEGI)

    assert b.kilit_yazan_surum(bot) is True and g.kilit_yazan_surum(bot) is True
    assert b.kilit_yazan_surum(REPO) is True                               # this source tree's bot writes the lock
    (bot / "copytrader" / "__main__.py").write_text(OLD_MAIN, encoding="utf-8")
    assert b.kilit_yazan_surum(bot) is False and g.kilit_yazan_surum(bot) is False
    (bot / "copytrader" / "__main__.py").unlink()
    assert b.kilit_yazan_surum(bot) is False


def test_a_bot_that_runs_without_its_lock_file_tells_the_owner_what_the_watchman_will_think(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    import copytrader.app as app_mod

    async def fake_run(config_path: Any = "config.yaml") -> None:
        return None

    def no_lock(data_dir: Any) -> Path:
        raise PermissionError(13, "erişim engellendi")

    monkeypatch.setattr(app_mod, "run", fake_run)
    monkeypatch.setattr(cli, "acquire_lock", no_lock)
    assert cli.main(["run", "--config", str(paper_config(tmp_path))]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "bot yine de başlatılıyor. Bekçi kuruluysa botu kapalı sanıp \"Bot çalışmıyor\" uyarısı gönderebilir." in out


# =========================================================================== #
# U1  bekci.py: whole passes on a bot folder
# =========================================================================== #
def test_watchman_passes_over_an_outage(b: Any, tmp_path: Path) -> None:
    bot = bot_folder(tmp_path)
    clock, box = Clock(), Outbox()
    seen = {"now": b.CALISIYOR}

    def run_pass() -> str:
        return b.bak(bot, simdi=clock, gonder=box, durum_sor=lambda data: seen["now"])

    assert run_pass() == b.CALISIYOR and box.sent == []
    assert state(bot)["son_durum"] == b.CALISIYOR and state(bot)["son_bakis"] == clock.now
    seen["now"] = b.KAPALI
    clock.minutes(5)
    assert run_pass() == "gonderildi" and box.sent == [b.ILK_UYARI] and box.calls == [(TOKEN, OWNER)]
    for _ in range(5):                                                     # 25 more minutes down: silence
        clock.minutes(5)
        assert run_pass() == b.KAPALI
    assert len(box.sent) == 1
    clock.minutes(5)
    assert run_pass() == "gonderildi" and "ilk uyarıdan bu yana 30 dakika geçti" in box.sent[-1]
    seen["now"] = b.BILINMIYOR
    clock.minutes(60)
    assert run_pass() == b.BILINMIYOR and len(box.sent) == 2               # cannot tell: nothing is said
    seen["now"] = b.CALISIYOR
    clock.minutes(5)
    assert run_pass() == "gonderildi" and box.sent[-1] == "✅ Bot tekrar çalışıyor."
    clock.minutes(5)
    assert run_pass() == b.CALISIYOR and len(box.sent) == 3
    saved = (bot / "data" / "bekci.json").read_text(encoding="utf-8")
    assert TOKEN not in saved and str(OWNER) not in saved                  # the state file holds no secret
    assert not (bot / "data" / "bekci.json.tmp").exists()


def test_watchman_a_failed_send_is_tried_again_and_never_doubles(b: Any, tmp_path: Path) -> None:
    bot = bot_folder(tmp_path)
    clock, box = Clock(), Outbox(fail=2)

    def run_pass(what: str) -> str:
        clock.minutes(5)
        return b.bak(bot, simdi=clock, gonder=box, durum_sor=lambda data: what)

    assert run_pass(b.KAPALI) == "gonderilemedi" and run_pass(b.KAPALI) == "gonderilemedi" and box.sent == []
    error = state(bot)["son_hata"]
    assert "bağlantı yok" in error and TOKEN not in error and "***" in error      # the reason, without the token
    assert state(bot).get("ilk_uyari") is None
    assert run_pass(b.KAPALI) == "gonderildi" and box.sent == [b.ILK_UYARI]       # third pass: the network is back
    assert state(bot)["son_hata"] is None
    assert run_pass(b.KAPALI) == b.KAPALI and len(box.sent) == 1

    box.fail = 1                                                           # the "it is back" message fails once
    assert run_pass(b.CALISIYOR) == "gonderilemedi" and state(bot)["ilk_uyari"] is not None
    assert run_pass(b.CALISIYOR) == "gonderildi" and box.sent[-1] == b.GERI_GELDI
    assert run_pass(b.CALISIYOR) == b.CALISIYOR and len(box.sent) == 2

    assert run_pass(b.KAPALI) == "gonderildi"                              # a new outage
    box.fail = 1                                                           # its reminder fails: it comes 5 minutes later
    clock.minutes(30)
    assert run_pass(b.KAPALI) == "gonderilemedi" and run_pass(b.KAPALI) == "gonderildi"
    assert "hâlâ çalışmıyor" in box.sent[-1] and run_pass(b.KAPALI) == b.KAPALI and len(box.sent) == 4


def test_watchman_that_cannot_remember_stays_silent(b: Any, tmp_path: Path) -> None:
    """If bekci.json cannot be written the pass would not know five minutes later that it already spoke:
    it says nothing instead of the same message at every pass."""
    bot = bot_folder(tmp_path)
    (bot / "data" / "bekci.json").mkdir()                                  # cannot be replaced by a file
    clock, box = Clock(), Outbox()
    for _ in range(4):
        clock.minutes(5)
        assert b.bak(bot, simdi=clock, gonder=box, durum_sor=lambda data: b.KAPALI) == "durum-yazilamadi"
    assert box.sent == [] and box.calls == [] and not (bot / "data" / "bekci.json.tmp").exists()

    gone = bot_folder(tmp_path / "veri-yok")
    (gone / "data" / "logs").rmdir()
    (gone / "data").rmdir()                                                # no data folder at all
    assert b.bak(gone, simdi=clock, gonder=box, durum_sor=b.bot_durumu) == "durum-yazilamadi" and box.sent == []
    assert not (gone / "data").exists()                                    # ... and none is created


def test_watchman_is_silent_during_an_update_and_for_a_version_without_a_lock(b: Any, g: Any, tmp_path: Path) -> None:
    bot = bot_folder(tmp_path)
    clock, box = Clock(time.time()), Outbox()

    def run_pass() -> str:
        return b.bak(bot, simdi=clock, gonder=box)                         # the real look at the lock file: no bot

    g.guncelleme_kilidi_yaz(bot / "data")
    for _ in range(5):                                                     # 25 minutes of update: silence
        assert run_pass() == b.GUNCELLEME
        clock.minutes(5)
    assert box.sent == []
    clock.minutes(10)                                                      # the lock is 35 minutes old: forgotten
    assert run_pass() == "gonderildi" and box.sent == [b.ILK_UYARI]

    old = bot_folder(tmp_path / "eski", main=OLD_MAIN)                     # after --geri-al to a version without bot.lock
    box2 = Outbox()
    for _ in range(3):
        clock.minutes(30)
        assert b.bak(old, simdi=clock, gonder=box2) == b.ESKI_SURUM
    assert box2.sent == [] and state(old)["son_durum"] == b.ESKI_SURUM

    # an outage that was reported, then an update: no reminder during it, "it is back" after it
    g.guncelleme_kilidi_yaz(bot / "data")
    os.utime(bot / "data" / "guncelleme.lock", (clock.now, clock.now))
    clock.minutes(20)
    assert run_pass() == b.GUNCELLEME and len(box.sent) == 1
    g.guncelleme_kilidi_kaldir(bot / "data")
    assert b.bak(bot, simdi=clock, gonder=box, durum_sor=lambda data: b.CALISIYOR) == "gonderildi"
    assert box.sent[-1] == b.GERI_GELDI


def test_watchman_without_token_or_owner_sends_nothing_and_says_why(b: Any, tmp_path: Path) -> None:
    bot = bot_folder(tmp_path, env={"TG_OWNER_ID": str(OWNER)})            # no bot token anywhere
    clock, box = Clock(), Outbox()
    for _ in range(3):
        clock.minutes(5)
        assert b.bak(bot, simdi=clock, gonder=box, durum_sor=lambda data: b.KAPALI) == "gonderilemedi"
    assert box.calls == [] and state(bot)["son_hata"] == b.AYAR_YOK and state(bot).get("ilk_uyari") is None
    assert b.mesaj_gonder(bot, "x", box) == b.AYAR_YOK


def test_watchman_answers_the_updaters_test_request_once(b: Any, g: Any, tmp_path: Path) -> None:
    bot = bot_folder(tmp_path)
    data = bot / "data"
    clock, box = Clock(time.time()), Outbox()
    g.guncelleme_kilidi_yaz(data)                                          # asked for in the middle of an update
    (data / "bekci.dene").write_text("istek-1", encoding="ascii")
    assert b.bak(bot, simdi=clock, gonder=box) == b.GUNCELLEME
    assert box.sent == [b.DENEME_METNI] and not (data / "bekci.dene").exists()
    assert state(bot)["deneme"] == {"istek": "istek-1", "sonuc": "tamam", "zaman": clock.now}
    clock.minutes(5)
    assert b.bak(bot, simdi=clock, gonder=box) == b.GUNCELLEME and len(box.sent) == 1      # not again

    box.fail = 1                                                           # a test message that cannot be sent
    (data / "bekci.dene").write_text("istek-2", encoding="ascii")
    b.bak(bot, simdi=clock, gonder=box)
    result = state(bot)["deneme"]
    assert result["istek"] == "istek-2" and "bağlantı yok" in result["sonuc"] and TOKEN not in result["sonuc"]
    assert not (data / "bekci.dene").exists() and len(box.sent) == 1       # one try per request, never a loop

    # together with a real alert in the same pass: both go out, both are remembered
    g.guncelleme_kilidi_kaldir(data)
    (data / "bekci.dene").write_text("istek-3", encoding="ascii")
    assert b.bak(bot, simdi=clock, gonder=box, durum_sor=lambda d: b.KAPALI) == "gonderildi"
    assert box.sent[-2:] == [b.DENEME_METNI, b.ILK_UYARI] and state(bot)["deneme"]["istek"] == "istek-3"
    (data / "bekci.dene").mkdir()                                          # a request that cannot be read or removed
    clock.minutes(5)
    assert b.bak(bot, simdi=clock, gonder=box, durum_sor=lambda d: b.KAPALI) == b.KAPALI and len(box.sent) == 3
    (data / "bekci.dene").rmdir()


def test_watchman_test_also_checks_that_it_can_look_at_processes(b: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A watchman that cannot tell whether a process lives would stay silent for ever (it never guesses):
    the test the updater asks for says so instead of reporting success."""
    bot = bot_folder(tmp_path)
    box = Outbox()
    assert b.deneme_yap(bot, box) is None and box.sent == [b.DENEME_METNI]
    monkeypatch.setattr(b, "surec_durumu", lambda pid: (None, None))
    (bot / "data" / "bekci.dene").write_text("istek-9", encoding="ascii")
    b.bak(bot, gonder=box, durum_sor=lambda data: b.CALISIYOR)
    assert state(bot)["deneme"]["sonuc"] == b.SUREC_OKUNAMIYOR and len(box.sent) == 1     # no "all is well" message


# =========================================================================== #
# U1  bekci.py: the Telegram request and the script itself
# =========================================================================== #
class _BotApi(http.server.BaseHTTPRequestHandler):
    seen: list[tuple[str, str, dict[str, list[str]]]] = []
    answer: tuple[int, dict[str, Any]] = (200, {"ok": True, "result": {"message_id": 1}})

    def do_POST(self) -> None:      # noqa: N802
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0)).decode("utf-8")
        type(self).seen.append((self.command, self.path, urllib.parse.parse_qs(body)))
        code, payload = type(self).answer
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *args: Any) -> None:
        return None


@pytest.fixture
def bot_api(b: Any, monkeypatch: pytest.MonkeyPatch):
    """A local stand-in for api.telegram.org (plain HTTP on 127.0.0.1); the module is pointed at it."""
    _BotApi.seen = []
    _BotApi.answer = (200, {"ok": True, "result": {"message_id": 1}})
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _BotApi)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    monkeypatch.setattr(b, "TELEGRAM_API", f"http://127.0.0.1:{server.server_address[1]}")
    yield _BotApi
    server.shutdown()
    server.server_close()


def test_watchman_telegram_request_is_a_sendmessage_post_and_errors_never_show_the_token(
        b: Any, bot_api: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    b.telegram_gonder(TOKEN, OWNER, b.ILK_UYARI)
    assert bot_api.seen == [("POST", f"/bot{TOKEN}/sendMessage", {"chat_id": [str(OWNER)], "text": [b.ILK_UYARI]})]

    bot = bot_folder(tmp_path)                                             # the whole way: folder -> settings -> request
    assert b.mesaj_gonder(bot, b.GERI_GELDI) is None
    assert bot_api.seen[-1][2] == {"chat_id": [str(OWNER)], "text": ["✅ Bot tekrar çalışıyor."]}

    bot_api.answer = (403, {"ok": False, "error_code": 403, "description": "Forbidden: bot was blocked by the user"})
    with pytest.raises(RuntimeError) as refused:
        b.telegram_gonder(TOKEN, OWNER, "x")
    assert "HTTP 403" in str(refused.value) and "bot was blocked by the user" in str(refused.value)
    assert refused.value.__cause__ is None and refused.value.__suppress_context__      # no chained error carrying the URL
    reason = b.mesaj_gonder(bot, "x")
    assert reason is not None and "HTTP 403" in reason and TOKEN not in reason

    bot_api.answer = (200, {"ok": False, "description": "bir şey"})
    with pytest.raises(RuntimeError, match="anlaşılamadı"):
        b.telegram_gonder(TOKEN, OWNER, "x")
    bot_api.answer = (502, {"bozuk": 1})
    assert "HTTP 502" in (b.mesaj_gonder(bot, "x") or "")

    monkeypatch.setattr(b, "TELEGRAM_API", "http://127.0.0.1:9")           # nothing listens there
    monkeypatch.setattr(b, "ZAMAN_ASIMI", 3.0)
    reason = b.mesaj_gonder(bot, "x")
    assert reason is not None and reason.startswith("Telegram'a ulaşılamadı") and TOKEN not in reason
    assert b.gizle(f"https://api.telegram.org/bot{TOKEN}/sendMessage hata", None) == "https://api.telegram.org/bot***/sendMessage hata"
    assert b.TELEGRAM_API == "http://127.0.0.1:9" and load("bekci").TELEGRAM_API == "https://api.telegram.org"


def test_watchman_script_needs_only_the_standard_library_and_never_imports_the_bot(b: Any) -> None:
    tree = ast.parse((REPO / "bekci.py").read_text(encoding="utf-8"))
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0
            modules.add((node.module or "").split(".")[0])
    assert "copytrader" not in modules
    assert modules - set(sys.stdlib_module_names) == {"certifi"}           # optional (inside try): extra certificates only
    assert isinstance(b._ssl_baglami().verify_mode, int) and b._ssl_baglami().check_hostname is True


def run_script(bot: Path, *args: str, bare: bool = True) -> subprocess.CompletedProcess[str]:
    """bekci.py copied into ``bot`` and run as the scheduled task runs it (another working directory).
    ``bare``: without site-packages and environment (-S -E): the standard library alone."""
    (bot / "bekci.py").write_bytes((REPO / "bekci.py").read_bytes())
    flags = ["-S", "-E", "-B"] if bare else ["-B"]
    env = {k: v for k, v in os.environ.items() if k not in ENV}
    return subprocess.run([sys.executable, *flags, str(bot / "bekci.py"), *args], cwd=str(bot.parent), env=env,
                          capture_output=True, text=True, encoding="utf-8", timeout=120)


def test_watchman_script_ends_quietly_whatever_it_finds(b: Any, tmp_path: Path) -> None:
    """Real runs of the file. None of these folders has a usable token, so no request is ever made."""
    empty = tmp_path / "bos"
    empty.mkdir()
    result = run_script(empty)                                             # not a bot folder at all
    assert (result.returncode, result.stdout, result.stderr) == (0, "", "")
    assert sorted(p.name for p in empty.iterdir()) == ["bekci.py"]         # nothing created

    bot = bot_folder(tmp_path, env={"TG_OWNER_ID": str(OWNER)})            # bot down, no token: cannot speak
    result = run_script(bot)
    assert (result.returncode, result.stdout, result.stderr) == (0, "", "")
    assert state(bot)["son_durum"] == b.KAPALI and state(bot)["son_hata"] == b.AYAR_YOK

    (bot / "config.yaml").unlink()
    (bot / "config.yaml").mkdir()                                          # a folder where the file should be
    (bot / "data" / "bot.lock").write_text("{bozuk", encoding="utf-8")
    result = run_script(bot)
    assert (result.returncode, result.stdout, result.stderr) == (0, "", "")

    result = run_script(bot, "--dene")                                     # the manual test says why it cannot send
    assert result.returncode == 1 and result.stderr == ""
    assert result.stdout.strip() == f"Deneme başarısız: {b.AYAR_YOK}"


LOCK_HOLDER = ("import sys, time; sys.path.insert(0, sys.argv[1]); import copytrader.__main__ as cli; "
               "cli.acquire_lock(sys.argv[2]); print('kilit', flush=True); time.sleep(300)")
# the scheduled task's command with one difference: the Bot API address is the local stand-in
PASS_DRIVER = ("import importlib.util, sys; spec = importlib.util.spec_from_file_location('bekci', sys.argv[1]); "
               "m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m); m.TELEGRAM_API = sys.argv[2]; "
               "sys.exit(m.main([]))")


@pytest.mark.skipif(not POSIX_PROC, reason="needs /proc")
def test_watchman_as_real_processes_over_a_whole_outage(b: Any, bot_api: Any, tmp_path: Path) -> None:
    """Nothing injected: a process that holds the bot's lock (written by the bot's own code), the watchman as its
    own process at every 'tick', a local HTTP server in the place of Telegram, the state file on disk."""
    bot = bot_folder(tmp_path)
    (bot / "bekci.py").write_bytes((REPO / "bekci.py").read_bytes())
    env = {k: v for k, v in os.environ.items() if k not in ENV}
    env.update(NO_PROXY="127.0.0.1,localhost", no_proxy="127.0.0.1,localhost", PYTHONDONTWRITEBYTECODE="1")

    def start_bot() -> subprocess.Popen[str]:
        proc = subprocess.Popen([sys.executable, "-c", LOCK_HOLDER, str(REPO), str(bot / "data")],
                                stdout=subprocess.PIPE, text=True, env=env)
        assert proc.stdout is not None and proc.stdout.readline().strip() == "kilit"
        return proc

    def tick() -> list[str]:
        """One run of the scheduled task; the texts Telegram has received so far."""
        result = subprocess.run([sys.executable, "-c", PASS_DRIVER, str(bot / "bekci.py"), b.TELEGRAM_API],
                                cwd=str(tmp_path), env=env, capture_output=True, text=True, timeout=120)
        assert (result.returncode, result.stdout, result.stderr) == (0, "", "")
        assert all(path == f"/bot{TOKEN}/sendMessage" and body["chat_id"] == [str(OWNER)] for _m, path, body in bot_api.seen)
        return [body["text"][0] for _m, _path, body in bot_api.seen]

    running = start_bot()
    try:
        assert tick() == [] and tick() == []                               # the bot runs: silence
        running.kill()                                                     # the window is closed: the lock file stays
        running.wait()
        assert (bot / "data" / "bot.lock").is_file()
        assert tick() == [b.ILK_UYARI] and tick() == [b.ILK_UYARI]         # said once
        saved = state(bot)
        saved["son_uyari"] -= 31 * 60                                      # ... 31 minutes later
        saved["ilk_uyari"] -= 31 * 60
        (bot / "data" / "bekci.json").write_text(json.dumps(saved), encoding="utf-8")
        texts = tick()
        assert len(texts) == 2 and texts[1].startswith("⚠️ Bot hâlâ çalışmıyor (ilk uyarıdan bu yana 31 dakika geçti)")
        assert len(tick()) == 2
        running = start_bot()                                              # the owner starts the bot again
        assert tick()[2:] == ["✅ Bot tekrar çalışıyor."] and len(tick()) == 3
    finally:
        running.kill()
        running.wait()
    assert TOKEN not in (bot / "data" / "bekci.json").read_text(encoding="utf-8")


def test_watchman_main_swallows_unexpected_errors(b: Any, monkeypatch: pytest.MonkeyPatch,
                                                  capsys: pytest.CaptureFixture[str]) -> None:
    def broken(bot: Path, **kw: Any) -> str:
        raise RuntimeError(f"beklenmeyen hata {TOKEN}")

    monkeypatch.setattr(b, "bak", broken)
    assert b.main([]) == 0
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == ""


# =========================================================================== #
# U1  guncelle.py: the scheduled task
# =========================================================================== #
SERVER_BOT = r"C:\Users\Administrator\Downloads\harmonic-bot-claude-telegram-crypto-automation-o6xo7l\telegram-copy-trader"


def windows_argv(line: str) -> list[str]:
    """A command line split by the documented rules of the Microsoft C runtime (what schtasks.exe sees):
    blanks separate arguments outside double quotes; 2n backslashes before a quote give n backslashes and the
    quote delimits; 2n+1 give n backslashes and a literal quote; other backslashes are literal."""
    args: list[str] = []
    current, quoted, started, i = "", False, False, 0
    while i < len(line):
        ch = line[i]
        if ch == "\\":
            n = 0
            while i < len(line) and line[i] == "\\":
                n, i = n + 1, i + 1
            if i < len(line) and line[i] == '"':
                current += "\\" * (n // 2)
                if n % 2:
                    current += '"'
                    i += 1
            else:
                current += "\\" * n
            started = True
        elif ch == '"':
            quoted, started, i = not quoted, True, i + 1
        elif ch in " \t" and not quoted:
            if started:
                args.append(current)
            current, started, i = "", False, i + 1
        else:
            current, started, i = current + ch, True, i + 1
    if started:
        args.append(current)
    return args


def test_updater_builds_the_scheduled_task_command(g: Any) -> None:
    python, script = SERVER_BOT + r"\venv\Scripts\python.exe", SERVER_BOT + r"\bekci.py"
    command = g.bekci_komutu(python, script)
    assert Path(command[0]).name in ("schtasks", "schtasks.exe")
    assert command[1:] == ["/Create", "/TN", "CopytraderBekci", "/SC", "MINUTE", "/MO", "5", "/RU", "SYSTEM",
                           "/RL", "HIGHEST", "/F", "/TR", f"{python} {script}"]
    assert len(command[-1]) == 248 <= g.GOREV_TR_SINIRI == 261             # the server's folder fits (no quotes needed)
    assert windows_argv(subprocess.list2cmdline(command)) == command       # what Windows hands to schtasks
    assert windows_argv(command[-1]) == [python, script]                   # ... and the task's own line: program, script

    python = r"C:\Program Files\Python312\python.exe"                      # blanks: each path in its own quotes
    script = r"C:\Users\Ali Veli\Downloads\harmonic-bot (1)\telegram-copy-trader\bekci.py"
    command = g.bekci_komutu(python, script)
    assert command[-1] == f'"{python}" "{script}"'
    line = subprocess.list2cmdline(command)
    assert f'/TR "\\"{python}\\" \\"{script}\\""' in line                  # the form Microsoft documents for /TR
    assert windows_argv(line) == command and windows_argv(command[-1]) == [python, script]
    mixed = g.bekci_komutu(r"C:\Program Files\Python312\python.exe", r"C:\bot\bekci.py")
    assert mixed[-1] == r'"C:\Program Files\Python312\python.exe" C:\bot\bekci.py'
    assert windows_argv(subprocess.list2cmdline(mixed)) == mixed
    assert g.varsayilan_gorev("nt") is subprocess.run and g.varsayilan_gorev("posix") is None
    assert g.varsayilan_gorev() is (subprocess.run if os.name == "nt" else None)


class Result:
    def __init__(self, returncode: int = 0, stdout: bytes = b"", stderr: bytes = b"") -> None:
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


CREATED = Result(0, b'SUCCESS: The scheduled task "CopytraderBekci" has successfully been created.\r\n')


class Schtasks:
    """Stands in for schtasks.exe. /Create answers ``create``; /Run answers ``run`` and (``passes``) does what
    Windows would do: one pass of the installed bekci.py over the bot folder, sending through ``box``."""

    def __init__(self, bot: Path, *, create: Any = CREATED, run: Any = None, box: Outbox | None = None,
                 passes: bool = True) -> None:
        self.bot, self.create, self.run, self.passes = bot, create, run or Result(0), passes
        self.box = box if box is not None else Outbox()
        self.calls: list[list[str]] = []
        self.options: list[dict[str, Any]] = []
        self.seen: list[str] = []

    def __call__(self, komut: list[str], **options: Any) -> Any:
        self.calls.append(list(komut))
        self.options.append(options)
        answer = self.create if "/Create" in komut else self.run
        if isinstance(answer, BaseException):
            raise answer
        if "/Run" in komut and self.passes:
            installed = load_from(self.bot / "bekci.py")
            self.seen.append(installed.bak(self.bot, gonder=self.box))
        return answer


def load_from(path: Path) -> Any:
    saved, sys.dont_write_bytecode = sys.dont_write_bytecode, True
    try:
        spec = importlib.util.spec_from_file_location("bekci_installed", path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = saved
    return module


def new_tree(folder: Path) -> Path:
    """'The new version' the updater installs: the real guncelle.py and bekci.py around a stub package."""
    tree = folder / "yeni-surum" / "telegram-copy-trader"
    (tree / "copytrader").mkdir(parents=True)
    for name, text in (("__init__.py", "__version__ = 'yeni'\n"), ("__main__.py", NEW_MAIN),
                       ("migrate.py", "# yeni\n"), ("selftest.py", "# yeni\n")):
        (tree / "copytrader" / name).write_text(text, encoding="utf-8")
    (tree / "requirements.txt").write_text("telethon>=1.0\n", encoding="utf-8")
    for name in ("guncelle.py", "bekci.py"):
        (tree / name).write_bytes((REPO / name).read_bytes())
    return tree


def old_bot(folder: Path) -> Path:
    """The server's bot folder before the update: a version without bot.lock, bekci.py or guncelle.py."""
    bot = bot_folder(folder, main=OLD_MAIN)
    (bot / "requirements.txt").write_text("telethon>=1.0\n", encoding="utf-8")
    (bot / "data" / "logs" / "copytrader.log").write_text("kayıt\n", encoding="utf-8")
    return bot


class Runner:
    """Stands in for ``python -m copytrader ...`` / pip. ``hook(key)`` runs at every command; ``codes``: exit
    code per command (default 0)."""

    def __init__(self, hook: Any = None, codes: dict[str, int] | None = None) -> None:
        self.hook, self.codes, self.calls = hook, codes or {}, []

    def __call__(self, komut: list[str], klasor: Path, sure: float, satir: Any) -> int:
        key = " ".join(komut[1:])
        self.calls.append(key)
        if self.hook is not None:
            self.hook(key)
        if key == "-m copytrader check":
            satir("Sorun yok.")
        return self.codes.get(key, 0)


def update(g: Any, bot: Path, tree: Path, *options: str, runner: Runner | None = None, gorev: Any = None,
           start: Any = None, processes: Any = lambda: []) -> tuple[int, Runner, list[str]]:
    runner = runner or Runner()
    started: list[str] = []

    def baslat(hedef: Path, python: str) -> int:
        started.append(python)
        if start is not None:
            start()
        return 130                                   # the bot ran until Ctrl+C

    code = g.main(["--hedef", str(bot), *options], calistir=runner, surecler=processes, baslat=baslat,
                  girdi=lambda prompt: "", yeni_agac=tree, gorev=gorev)
    return code, runner, started


def test_update_registers_the_watchman_and_has_it_send_a_test_message(
        g: Any, b: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bot, tree = old_bot(tmp_path), new_tree(tmp_path)
    schtasks = Schtasks(bot)
    code, runner, started = update(g, bot, tree, gorev=schtasks)
    out = capsys.readouterr().out
    assert code == 130 and started == [sys.executable]                     # the update went through, the bot was started
    assert (bot / "bekci.py").read_bytes() == (REPO / "bekci.py").read_bytes()
    # exactly two schtasks calls: register (replacing an older registration), then run it once now
    assert schtasks.calls == [g.bekci_komutu(sys.executable, str(bot / "bekci.py")),
                              [schtasks.calls[1][0], "/Run", "/TN", "CopytraderBekci"]]
    assert all(o.get("capture_output") is True and o.get("timeout") for o in schtasks.options)
    # the pass Windows started saw "an update is running" (no alarm about the stopped bot) and sent the test message
    assert schtasks.seen == [b.GUNCELLEME] and schtasks.box.sent == [b.DENEME_METNI]
    assert schtasks.box.calls == [(TOKEN, OWNER)] and not (bot / "data" / "bekci.dene").exists()
    line = next(ln for ln in out.splitlines() if "Bekçi" in ln)
    assert line == ("   Bekçi kuruldu: bot çalışmıyorsa birkaç dakika içinde Telegram'dan haber verir. Deneme mesajı "
                    "gönderildi; Telegram'ınıza bakın.")
    assert out.count("Bekçi") == 1 and TOKEN not in out
    assert out.index("8. Sağlık kontrolü") < out.index("Bekçi kuruldu") < out.index("9. Bot başlatılıyor (Yeni sürüm)")
    assert line in next(bot.glob("guncelleme_*.log")).read_text(encoding="utf-8")
    steps = [ln for ln in out.splitlines() if ln[:3] in {f"{n}. " for n in range(1, 10)}]
    assert [s[0] for s in steps] == list("123456789") and all(s.endswith(": TAMAM") for s in steps), out


@pytest.mark.parametrize("case", ["refused", "no-schtasks", "no-script", "path-too-long", "crash"])
def test_update_goes_on_when_the_watchman_cannot_be_registered(
        g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, case: str) -> None:
    bot, tree = old_bot(tmp_path), new_tree(tmp_path)
    schtasks = Schtasks(bot)
    expected = ""
    if case == "refused":
        schtasks.create = Result(1, b"", "ERROR: Access is denied.\r\n".encode("ascii"))
        expected = "ERROR: Access is denied."
    elif case == "no-schtasks":
        schtasks.create = FileNotFoundError(2, "schtasks bulunamadı")
        expected = "schtasks çalıştırılamadı ("
    elif case == "no-script":
        (tree / "bekci.py").unlink()
        expected = "bekci.py bot klasöründe yok"
    elif case == "path-too-long":
        monkeypatch.setattr(g, "GOREV_TR_SINIRI", 20)
        expected = "bot klasörünün yolu çok uzun (görev satırı "
    else:
        monkeypatch.setattr(g, "bekci_kur", lambda *a, **k: 1 / 0)         # a bug in the registration itself
        expected = "division by zero"
    code, runner, started = update(g, bot, tree, gorev=schtasks)
    out = capsys.readouterr().out
    (line,) = [ln for ln in out.splitlines() if "Bekçi" in ln]
    assert line.startswith("   Bekçi kurulamadı (isteğe bağlı): ") and expected in line
    # the update itself is untouched by it: new version installed and kept, checked, started
    assert code == 130 and started == [sys.executable] and "GÜNCELLEME YAPILMADI" not in out
    assert "9. Bot başlatılıyor (Yeni sürüm)" in out and (bot / "copytrader" / "migrate.py").is_file()
    assert runner.calls[-1] == "-m copytrader selftest"
    assert len(schtasks.calls) == (1 if case in ("refused", "no-schtasks") else 0)      # never a /Run after a failure
    assert not (bot / "data" / "bekci.dene").exists() and not (bot / "data" / "guncelleme.lock").exists()


def test_watchman_registration_reasons(g: Any, tmp_path: Path) -> None:
    bot = bot_folder(tmp_path)
    (bot / "bekci.py").write_text("# bekçi\n", encoding="utf-8")
    denied = Result(1, b"", "HATA: Eri\x9fim engellendi.\r\n".encode("latin-1"))
    reason = g.bekci_kur(bot, "python.exe", lambda komut, **kw: denied, yonetici=lambda: False)
    assert reason is not None and reason.startswith("bu pencere yönetici yetkisiyle açılmamış")
    assert "Windows'un yanıtı: HATA: Eri" in reason
    reason = g.bekci_kur(bot, "python.exe", lambda komut, **kw: denied, yonetici=lambda: True)
    assert reason is not None and reason.startswith("HATA: Eri")                       # an administrator: Windows' own words
    assert g.bekci_kur(bot, "python.exe", lambda komut, **kw: Result(1), yonetici=lambda: None) == "schtasks çıkış kodu 1"
    assert g.bekci_kur(bot, "python.exe", lambda komut, **kw: CREATED) is None
    assert g._cikti(Result(1, "çıktı\r\nikinci  satır".encode("utf-8"), b"")) == "çıktı ikinci satır"
    assert g._cikti(Result(1, b"", b"")) == "" and g.yonetici_mi() in (None, True, False)
    # the task runs the bot's own Python when the folder has one (also when the update was started with another)
    seen: list[list[str]] = []
    assert g.bekci_kur(bot, "C:\\baska\\python.exe", lambda komut, **kw: seen.append(komut) or CREATED) is None
    assert seen[-1][-1] == f"C:\\baska\\python.exe {bot / 'bekci.py'}"
    (bot / "venv" / "Scripts").mkdir(parents=True)
    (bot / "venv" / "Scripts" / "python.exe").write_bytes(b"MZ")
    assert g.bekci_kur(bot, "C:\\baska\\python.exe", lambda komut, **kw: seen.append(komut) or CREATED) is None
    assert seen[-1][-1] == f"{bot / 'venv' / 'Scripts' / 'python.exe'} {bot / 'bekci.py'}"


def test_update_says_so_when_the_watchman_is_registered_but_cannot_be_trusted(
        g: Any, b: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(g, "BEKCI_BEKLEME", 1.0)
    # 1. it runs but Telegram cannot be reached from there
    bot, tree = old_bot(tmp_path / "a"), new_tree(tmp_path / "a")
    schtasks = Schtasks(bot, box=Outbox(fail=1))
    code, _, started = update(g, bot, tree, gorev=schtasks)
    out = capsys.readouterr().out
    (line,) = [ln for ln in out.splitlines() if "Bekçi" in ln]
    assert line.startswith("   Bekçi kuruldu, ama denemesi başarısız oldu: bağlantı yok") and "Bu haliyle haber veremez" in line
    assert TOKEN not in out and code == 130 and len(started) == 1

    # 2. Windows does not start it (or it cannot start): no answer; the request waits for the next scheduled pass
    bot, tree = old_bot(tmp_path / "b"), new_tree(tmp_path / "b")
    schtasks = Schtasks(bot, passes=False)
    code, _, started = update(g, bot, tree, gorev=schtasks)
    out = capsys.readouterr().out
    (line,) = [ln for ln in out.splitlines() if "Bekçi" in ln]
    assert line.startswith("   Bekçi kuruldu, ama denemesi doğrulanamadı (1 saniye içinde yanıt gelmedi).")
    assert '"Bekçi kuruldu" mesajı gelmezse bekçi çalışmıyordur' in line and code == 130 and len(started) == 1
    assert (bot / "data" / "bekci.dene").is_file()
    box = Outbox()
    assert b.bak(bot, gonder=box, durum_sor=lambda data: b.CALISIYOR) == b.CALISIYOR and box.sent == [b.DENEME_METNI]

    # 3. the run request itself is refused
    bot, tree = old_bot(tmp_path / "c"), new_tree(tmp_path / "c")
    schtasks = Schtasks(bot, run=Result(1, b"", b"ERROR: The system cannot find the file specified.\r\n"))
    update(g, bot, tree, gorev=schtasks)
    (line,) = [ln for ln in capsys.readouterr().out.splitlines() if "Bekçi" in ln]
    assert "denemesi doğrulanamadı (ERROR: The system cannot find the file specified.)" in line


def test_update_leaves_the_watchman_alone_when_asked_elsewhere_and_after_a_rollback(
        g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bot, tree = old_bot(tmp_path / "a"), new_tree(tmp_path / "a")
    schtasks = Schtasks(bot)
    code, _, started = update(g, bot, tree, "--bekci-yok", gorev=schtasks)
    out = capsys.readouterr().out
    assert code == 130 and schtasks.calls == [] and "   Bekçi kurulmadı (--bekci-yok)." in out.splitlines()

    bot, tree = old_bot(tmp_path / "b"), new_tree(tmp_path / "b")          # not Windows: nothing is done or said
    code, _, started = update(g, bot, tree, gorev=g.varsayilan_gorev("posix"))
    assert code == 130 and "Bekçi" not in capsys.readouterr().out

    bot, tree = old_bot(tmp_path / "c"), new_tree(tmp_path / "c")          # the update failed: old version restored
    schtasks = Schtasks(bot)
    code, runner, started = update(g, bot, tree, runner=Runner(codes={"-m copytrader migrate": 2}), gorev=schtasks)
    out = capsys.readouterr().out
    assert "ESKİ SÜRÜM GERİ YÜKLENDİ" in out and "9. Bot başlatılıyor (ESKİ sürüm)" in out and len(started) == 1
    assert schtasks.calls == [] and "Bekçi" not in out

    bot, tree = old_bot(tmp_path / "d"), new_tree(tmp_path / "d")          # Ctrl+C while registering: no half state
    schtasks = Schtasks(bot, create=KeyboardInterrupt())
    code, _, started = update(g, bot, tree, gorev=schtasks)
    out = capsys.readouterr().out
    assert code == 130 and started == [] and "Durduruldu (Ctrl+C). Yeni sürüm kurulu; bot başlatılmadı." in out
    assert (bot / "copytrader" / "migrate.py").is_file() and not (bot / "data" / "guncelleme.lock").exists()


# =========================================================================== #
# U1  guncelle.py: the watchman is silent while an update runs
# =========================================================================== #
def test_update_keeps_the_watchman_silent_from_the_stop_check_until_the_bot_is_back(
        g: Any, b: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bot, tree = old_bot(tmp_path), new_tree(tmp_path)
    (bot / "copytrader" / "__main__.py").write_text(NEW_MAIN, encoding="utf-8")        # a version the watchman watches
    data, box = bot / "data", Outbox()
    seen: list[tuple[str, str]] = []

    def watch(where: str) -> None:
        """What a scheduled pass would do at this moment (the real look at the lock files; the bot is down)."""
        seen.append((where, b.bak(bot, gonder=box)))

    at_start: list[Any] = []

    def start() -> None:
        lock = data / "guncelleme.lock"
        at_start.extend([lock.is_file(), b.guncelleme_suruyor(data, time.time() + 60),
                         b.guncelleme_suruyor(data, time.time() + g.BASLANGIC_PAYI + 5)])
        watch("bot starting")

    code, runner, started = update(g, bot, tree, runner=Runner(hook=watch), start=start)
    assert code == 130 and len(started) == 1
    assert [where for where, _ in seen] == ["-m copytrader check", "-m copytrader migrate", "-m copytrader check",
                                           "-m copytrader selftest", "bot starting"]
    assert {what for _, what in seen} == {b.GUNCELLEME} and box.sent == []            # never an alarm during the update
    # while the bot starts the lock still counts (the bot's own lock file is not there at once), 2 minutes later not
    assert at_start == [True, True, False]
    assert not (data / "guncelleme.lock").exists()                         # the bot has ended: the watchman reports it
    assert b.bak(bot, gonder=box) == "gonderildi" and box.sent == [b.ILK_UYARI]


def test_update_lock_is_gone_whenever_the_update_ends_without_starting_the_bot(
        g: Any, b: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def lock(bot: Path) -> Path:
        return bot / "data" / "guncelleme.lock"

    during: list[bool] = []
    bot, tree = old_bot(tmp_path / "a"), new_tree(tmp_path / "a")          # --baslatma
    code, _, started = update(g, bot, tree, "--baslatma", runner=Runner(hook=lambda key: during.append(lock(bot).is_file())))
    assert code == 0 and started == [] and during == [True] * 4 and not lock(bot).exists()

    bot, tree = old_bot(tmp_path / "b"), new_tree(tmp_path / "b")          # the bot is running: nothing is written at all
    running = [(4321, "python.exe", r"C:\bot\venv\Scripts\python.exe -m copytrader run")]
    assert update(g, bot, tree, processes=lambda: running)[0] == 2 and not lock(bot).exists()

    def interrupt_at(n: int) -> Any:
        calls: list[str] = []

        def hook(key: str) -> None:
            calls.append(key)
            if len(calls) == n:
                raise KeyboardInterrupt

        return hook

    for n, text in ((1, "hiçbir şey değiştirilmedi"), (3, "ESKİ SÜRÜM GERİ YÜKLENDİ; bot başlatılmadı"),
                    (4, "Yeni sürüm kurulu; bot başlatılmadı")):
        bot, tree = old_bot(tmp_path / f"c{n}"), new_tree(tmp_path / f"c{n}")
        capsys.readouterr()
        code, _, started = update(g, bot, tree, runner=Runner(hook=interrupt_at(n)))
        assert code == 130 and started == [] and text in capsys.readouterr().out
        assert not lock(bot).exists(), n

    # the way back (--geri-al): silent during it, the same grace when the old version is started, gone afterwards
    bot, tree = old_bot(tmp_path / "d"), new_tree(tmp_path / "d")
    assert update(g, bot, tree, "--baslatma", "--kontrol-yok")[0] == 0
    during.clear()
    at_start: list[Any] = []
    capsys.readouterr()
    code, runner, started = update(
        g, bot, tree, "--geri-al", runner=Runner(hook=lambda key: during.append(lock(bot).is_file())),
        start=lambda: at_start.extend([lock(bot).is_file(), b.guncelleme_suruyor(bot / "data", time.time() + 60),
                                       b.guncelleme_suruyor(bot / "data", time.time() + g.BASLANGIC_PAYI + 5)]))
    out = capsys.readouterr().out
    assert code == 130 and len(started) == 1 and during == [True] and at_start == [True, True, False]
    assert not lock(bot).exists()
    # the restored version writes no bot.lock: the owner is told that the watchman cannot watch it
    assert "Not: bu eski sürüm, çalışıp çalışmadığını bekçiye bildirmez" in out
    assert update(g, bot, tree, "--geri-al", "--baslatma")[0] == 2 and not lock(bot).exists()      # nothing older: refused
    empty = tmp_path / "bos"
    empty.mkdir()
    assert update(g, empty, tree, "--geri-al")[0] == 2 and list(empty.iterdir()) == []


# =========================================================================== #
# U2  guncelle.py: tokens in old log files
# =========================================================================== #
JWT = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiJhYmMxMjM0NTY3ODkwIiwiYWNjZXNzUnVsZXMiOltdfQ.c2lnbmF0dXJlLXNpZ25hdHVyZS1zaWduYXR1cmU"
LOG_SECRETS = [JWT, "duz-SECRET-token", "sorgu-SECRET-1", "baslik-SECRET-9"]
URL = "https://mt-client-api-v1.london.agiliumtrade.ai"
DIRTY_LINES = [
    f"2026-11-10 08:00:01,123 INFO    engineio.client: Attempting polling connection to {URL}/ws/?auth-token={JWT}"
    "&clientId=0.6548&protocol=3&transport=polling&EIO=4",
    f"2026-11-10 08:00:01,987 INFO    engineio.client: Attempting WebSocket upgrade to wss://x.agiliumtrade.ai/ws/"
    "?auth-token=duz-SECRET-token&clientId=0.6548&protocol=3&transport=websocket&EIO=4",
    '2026-11-10 08:00:02,000 INFO    httpx: HTTP Request: GET https://ornek.test/api?x=1&token=sorgu-SECRET-1 "HTTP/1.1 200 OK"',
    "2026-11-10 08:00:03,000 DEBUG   x: headers={'auth-token': 'baslik-SECRET-9', 'Client-Id': '0.6548'}",
    f"2026-11-10 08:00:04,000 WARNING x: token reddedildi: {JWT}",
]
CLEAN_LINES = [
    "2026-11-10 07:52:11,004 INFO    copytrader.engine.orchestrator: Trade #4 EUR SHORT: açılıyor (fx, tickmill)",
    "2026-11-10 07:52:12,100 ERROR   copytrader.brokers.mt5_broker: MT5 emir yanıtı alınamadı (kod -2)",
    "2026-11-10 07:52:13,000 INFO    x: token sayısı 5, auth-token= ",                 # the words alone are not a secret
    "2026-11-10 07:52:14,000 INFO    x: auth-token=***&clientId=0.1 (zaten gizli)",
]


def write_logs(bot: Path) -> dict[str, bytes]:
    logs = bot / "data" / "logs"
    files = {
        "copytrader.log": "\n".join([CLEAN_LINES[0], DIRTY_LINES[0], CLEAN_LINES[1], DIRTY_LINES[1], ""]).encode("utf-8"),
        "copytrader.log.1": ("\r\n".join([DIRTY_LINES[2], CLEAN_LINES[2], DIRTY_LINES[3]]) + "\r\n").encode("utf-8")
        + b"bozuk bayt \xff\xfe satiri\r\n" + (DIRTY_LINES[4] + " (son satir, satir sonu yok)").encode("utf-8"),
        "copytrader.log.5": ("\n".join(CLEAN_LINES) + "\n").encode("utf-8"),              # nothing to hide in it
    }
    for name, content in files.items():
        (logs / name).write_bytes(content)
        os.utime(logs / name, (1_700_000_000, 1_700_000_000))
    (logs / "baska.log").write_bytes(DIRTY_LINES[0].encode("utf-8"))                      # not the bot's log: not touched
    return files


def test_update_masks_the_metaapi_token_in_the_old_log_files(g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bot, tree = old_bot(tmp_path), new_tree(tmp_path)
    before = write_logs(bot)
    logs = bot / "data" / "logs"
    untouched = (logs / "copytrader.log.5").stat()
    code, _, started = update(g, bot, tree, "--kontrol-yok")
    out = capsys.readouterr().out
    assert code == 130 and len(started) == 1
    assert "   Eski kayıt dosyalarında (copytrader.log) 5 satırdaki hesap anahtarı (token) gizlendi." in out.splitlines()
    assert out.index("3. Eski sürüm yedeklendi") < out.index("Eski kayıt dosyalarında") < out.index("4. Yeni sürüm kuruldu")
    after = {name: (logs / name).read_bytes() for name in before}
    for name, content in after.items():
        assert not any(secret.encode() in content for secret in LOG_SECRETS), name
        # every other byte is as it was: same lines, same line ends, same order
        old_lines, new_lines = before[name].splitlines(keepends=True), content.splitlines(keepends=True)
        assert len(old_lines) == len(new_lines)
        for old, new in zip(old_lines, new_lines, strict=True):
            assert new == old or (b"***" in new and any(s.encode() in old for s in LOG_SECRETS))
    text = after["copytrader.log"].decode("utf-8").splitlines()
    assert text[0] == CLEAN_LINES[0] and text[2] == CLEAN_LINES[1]
    assert text[1].endswith(f"connection to {URL}/ws/?auth-token=***&clientId=0.6548&protocol=3&transport=polling&EIO=4")
    assert text[3].endswith("/ws/?auth-token=***&clientId=0.6548&protocol=3&transport=websocket&EIO=4")
    older = after["copytrader.log.1"]
    assert b"api?x=1&token=*** \"HTTP/1.1 200 OK\"\r\n" in older and b"bozuk bayt \xff\xfe satiri\r\n" in older
    assert b"headers={'auth-token': '***', 'Client-Id': '0.6548'}\r\n" in older
    assert older.endswith(b"token reddedildi: *** (son satir, satir sonu yok)")
    assert after["copytrader.log.5"] == before["copytrader.log.5"]
    same = (logs / "copytrader.log.5").stat()
    assert (same.st_ino, same.st_mtime) == (untouched.st_ino, untouched.st_mtime)          # not even rewritten
    assert (logs / "copytrader.log").stat().st_mtime == 1_700_000_000                      # a masked file keeps its date
    assert (logs / "baska.log").read_bytes() == DIRTY_LINES[0].encode("utf-8")
    assert sorted(p.name for p in logs.iterdir()) == ["baska.log", "copytrader.log", "copytrader.log.1", "copytrader.log.5"]
    assert not any(secret in out for secret in LOG_SECRETS)
    assert not any(secret in next(bot.glob("guncelleme_*.log")).read_text(encoding="utf-8") for secret in LOG_SECRETS)

    time.sleep(1.1)                                                        # (backup folders are named by the second)
    code, _, _ = update(g, bot, tree, "--kontrol-yok", "--baslatma")       # again: nothing left to hide, nothing changes
    out = capsys.readouterr().out
    assert code == 0 and "   Eski kayıt dosyalarında (copytrader.log) gizlenecek hesap anahtarı bulunmadı." in out.splitlines()
    assert {name: (logs / name).read_bytes() for name in before} == after
    assert g.gunlukleri_temizle(bot / "data") == (3, 0, 0, None)
    assert g.gunlukleri_temizle(tmp_path / "yok") == (0, 0, 0, None)


def test_update_never_fails_because_a_log_file_cannot_be_rewritten(
        g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    bot, tree = old_bot(tmp_path), new_tree(tmp_path)
    before = write_logs(bot)
    logs = bot / "data" / "logs"
    real = os.replace
    monkeypatch.setattr(g, "_tekrarla", lambda islem, kez=6, bekle=0.5: islem())      # no waiting in the test

    def replace(src: Any, dst: Any) -> None:
        if str(dst).endswith("copytrader.log"):
            raise PermissionError(13, "dosya başka bir program tarafından kullanılıyor")
        real(src, dst)

    monkeypatch.setattr(g.os, "replace", replace)
    code, _, started = update(g, bot, tree, "--kontrol-yok")
    out = capsys.readouterr().out
    assert code == 130 and len(started) == 1 and "GÜNCELLEME YAPILMADI" not in out     # the update went through
    assert "   Eski kayıt dosyalarında (copytrader.log) 3 satırdaki hesap anahtarı (token) gizlendi." in out.splitlines()
    (line,) = [ln for ln in out.splitlines() if "temizlenemedi" in ln]
    assert line.startswith("   Eski kayıt dosyaları temizlenemedi (isteğe bağlı): copytrader.log: ")
    assert (logs / "copytrader.log").read_bytes() == before["copytrader.log"]          # whole, as it was
    assert b"SECRET" not in (logs / "copytrader.log.1").read_bytes()                   # the others were still done
    assert not list(logs.glob("*.gizle"))                                              # no temporary file left

    monkeypatch.setattr(g.os, "replace", real)
    monkeypatch.setattr(g, "gunlukleri_temizle", lambda veri: (_ for _ in ()).throw(KeyboardInterrupt()))
    bot2, tree2 = old_bot(tmp_path / "ctrl-c"), new_tree(tmp_path / "ctrl-c")
    code, _, started = update(g, bot2, tree2, "--kontrol-yok")                         # Ctrl+C in the middle of it
    assert code == 130 and started == [] and "kurulu sürüm değişmedi" in capsys.readouterr().out
    assert (bot2 / "copytrader" / "__main__.py").read_text(encoding="utf-8") == OLD_MAIN


# =========================================================================== #
# U3a  selftest: the adapter's own account report (the REAL ccxt adapter, HTTP faked)
# =========================================================================== #
async def test_selftest_prints_the_real_binance_adapters_report_and_only_reads(tmp_path: Path) -> None:
    broker, fake = make_binance()                    # real CcxtBroker on the real ccxt class; fake transport
    fake.local_clock_ahead_ms = 50.0
    fake.pos["BTCUSDT"] = {"amt": -0.002, "entry": 74000.0}               # a position somebody opened by hand
    cfg = AppConfig.model_validate({
        "dry_run": False, "data_dir": str(tmp_path),
        "telegram": {"api_id": 1, "api_hash": "h", "session_string": "x", "source_chats": [-1001], "topics": [
            {"name": "kripto", "title_contains": ["Kripto İşlemleri"], "brokers": ["binance"], "kind": "crypto"}]},
        "notifier": {"bot_token": TOKEN, "owner_user_id": OWNER},
        "brokers": [broker.config.model_dump()],
    })
    lines: list[str] = []
    wrapped = DryRunBroker(broker, str(tmp_path))    # what the CLI's factory hands to the selftest
    code = await selftest.run_selftest(cfg, [wrapped], [], assets="BTC,DOGE", out=lines.append)
    text = "\n".join(lines)
    assert code == 0, text
    # the adapter was asked about the coins this account is routed (through getattr: it has the method)
    for expected in (
        "          TAMAM Pozisyon modu: tek yönlü (ayarla uyumlu).",
        "          TAMAM Çoklu varlık modu kapalı.",
        "          TAMAM BTC: BTC/USDT:USDT sözleşmesi; en küçük emir 0.001 BTC ve 50 USDT; borsanın kaldıraç tablosunda "
        "en yüksek 125x (50000 USDT büyüklüğe kadar).",
        "          [UYARI] Hesapta 1 açık pozisyon var (botun açtıkları dahil; tek yönlü modda aynı coindeki bot "
        "işlemleriyle birleşir):",
        "          [UYARI]    BTC/USDT:USDT SHORT 0.002, giriş 74000, cross",
        "          TAMAM Hesapta bekleyen emir yok (normal ve koşullu).",
    ):
        assert expected in lines, expected
    assert any(ln.strip().startswith("TAMAM DOGE: DOGE/USDT:USDT sözleşmesi") and "en yüksek 75x" in ln for ln in lines)
    # a DİKKAT line and its indented detail are ONE numbered warning, not a problem
    assert ("  1. binance: Hesapta 1 açık pozisyon var (botun açtıkları dahil; tek yönlü modda aynı coindeki bot "
            "işlemleriyle birleşir): BTC/USDT:USDT SHORT 0.002, giriş 74000, cross") in lines
    assert "SONUÇ: SORUN YOK." in text and text.count("[UYARI]") == 2
    # the whole selftest sent nothing but GET requests: no order, no leverage or margin-mode change
    assert fake.log and {method for method, _path, _params in fake.log} == {"GET"}
    assert {"/fapi/v1/positionSide/dual", "/fapi/v1/leverageBracket", "/fapi/v3/positionRisk", "/fapi/v1/openOrders",
            "/fapi/v1/openAlgoOrders"} <= {path for _method, path, _params in fake.log}
    assert fake.lev == {} and fake.mtype == {} and fake.orders == {} and fake.algos == {}


async def test_selftest_report_lines_of_adapters_without_or_with_a_broken_report() -> None:
    async def run(lines: Any) -> tuple[list[str] | None, list[str]]:
        report = selftest._Report(lambda text: None)
        broker = SimpleNamespace(name="x", config=SimpleNamespace(name="x", optional=False))
        if lines is not None:
            async def own(bases: list[str]) -> list[str]:
                if isinstance(lines, BaseException):
                    raise lines
                return lines
            broker.preflight_report = own
        return await selftest._report_lines(broker, ["BTC"], report), report.warnings

    assert await run(None) == (None, [])                                  # an adapter without the method: nothing
    assert await run(RuntimeError("borsa yanıt vermedi")) == (["hesap raporu alınamadı: borsa yanıt vermedi"], [])
    out, warnings = await run(["DİKKAT   baştaki ayrıntı", "TAMAM bir", "DİKKAT iki:", "DİKKAT   a", "DİKKAT   b",
                               "DİKKAT üç", "DİKKAT   c"])
    assert out == ["[UYARI]    baştaki ayrıntı", "TAMAM bir", "[UYARI] iki:", "[UYARI]    a", "[UYARI]    b",
                   "[UYARI] üç", "[UYARI]    c"]
    assert warnings == ["x: baştaki ayrıntı", "x: iki: a; b", "x: üç; c"]


# =========================================================================== #
# U3b  selftest: can ordinary members post where the bot copies from? (fake Telegram client)
# =========================================================================== #
def tl(cls: Any, **fields: Any) -> Any:
    """A Telethon object of the installed version with every field we do not care about left empty."""
    values = {name: (None if p.default is inspect.Parameter.empty else p.default)
              for name, p in inspect.signature(cls.__init__).parameters.items() if name != "self"}
    values.update(fields)
    return cls(**values)


def group(*, forum: bool = True, **banned: bool) -> Any:
    return tl(types.Channel, id=3782294463, title="KLAN VIP", megagroup=True, forum=forum,
              default_banned_rights=tl(types.ChatBannedRights, **banned))


def topic(topic_id: int, title: str, closed: bool = False) -> Any:
    return tl(types.ForumTopic, id=topic_id, title=title, closed=closed or None)


class FakeTelegram:
    """Stands in for the Telethon user client. Whatever the selftest calls is logged; anything that is not one
    of the reads below (send_message, forward_messages, send_read_acknowledge, start, ...) fails the test."""

    def __init__(self, entity: Any, topics: list[Any] | None = None, *, authorized: bool = True,
                 total: int | None = None, unknown_first: bool = False, hang: str = "", fail: str = "") -> None:
        self.entity, self.topics, self.authorized, self.total = entity, list(topics or []), authorized, total
        self.unknown_first, self.hang, self.fail = unknown_first, hang, fail
        self.log: list[Any] = []

    async def _step(self, name: str) -> None:
        self.log.append(name)
        if self.hang == name:
            await asyncio.sleep(3600)
        if self.fail == name:
            raise ConnectionError("Connection to Telegram failed 5 time(s)")

    async def connect(self) -> None:
        await self._step("connect")

    async def is_user_authorized(self) -> bool:
        await self._step("is_user_authorized")
        return self.authorized

    async def get_entity(self, target: Any) -> Any:
        await self._step("get_entity")
        self.log.append(target)
        if self.unknown_first and "get_dialogs" not in self.log:
            raise ValueError("Cannot find any entity corresponding to the given id")
        return self.entity

    async def get_dialogs(self) -> list[Any]:
        await self._step("get_dialogs")
        return []

    async def __call__(self, request: Any) -> Any:
        await self._step("request")
        self.log.append(request)
        deleted = tl(types.ForumTopicDeleted, id=999)
        return SimpleNamespace(topics=[*self.topics, deleted],
                               count=self.total if self.total is not None else len(self.topics) + 1)

    async def disconnect(self) -> None:
        self.log.append("disconnect")

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"the selftest used client.{name}: only the listed reads are allowed")


def tg_config(**telegram: Any) -> AppConfig:
    base: dict[str, Any] = {"api_id": 1, "api_hash": "h", "session_string": "1oturum", "source_chats": [-1003782294463],
                            "topics": [{"name": "kripto", "title_contains": ["Kripto İşlemleri"], "kind": "crypto"},
                                       {"name": "fx", "title_contains": ["Fx İşlemleri"], "kind": "fx"}]}
    base.update(telegram)
    return AppConfig.model_validate({"telegram": base, "notifier": {"bot_token": TOKEN, "owner_user_id": OWNER}})


async def telegram_report(cfg: AppConfig, client: Any) -> tuple[list[str], list[str], list[str], list[Any]]:
    lines: list[str] = []
    report = selftest._Report(lines.append)
    handed: list[Any] = []

    def factory(telegram_config: Any) -> Any:
        handed.append(telegram_config)
        if isinstance(client, BaseException):
            raise client
        return client

    await selftest._telegram_report(cfg, report, factory)
    return lines, report.warnings, report.problems, handed


async def test_selftest_says_when_ordinary_members_can_post_in_a_followed_topic() -> None:
    """The owner's case as far as it is known: the Fx topic is closed, the crypto topic is not known."""
    cfg = tg_config()
    client = FakeTelegram(group(), [topic(5, "Kasım Ayı Kripto İşlemleri"), topic(7, "Kasım Ayı FX İşlemleri", closed=True),
                                    topic(9, "Genel Sohbet"), topic(11, "Ekim Ayı Kripto İşlemleri", closed=True)])
    lines, warnings, problems, handed = await telegram_report(cfg, client)
    assert handed == [cfg.telegram]                                        # the listener's own way of connecting
    assert lines == [
        "",
        "TELEGRAM GRUBU  (yalnızca okunur: hiçbir mesaj gönderilmez, hiçbir şey okundu sayılmaz)",
        '  Grup "KLAN VIP": grup ayarında üyeler mesaj yazabiliyor',
        '    konu "Kasım Ayı Kripto İşlemleri" (kripto): açık konu; üyeler yazabilir',
        '    konu "Kasım Ayı FX İşlemleri" (fx): kapalı konu; üyeler yazamaz',
        '    konu "Ekim Ayı Kripto İşlemleri" (kripto): kapalı konu; üyeler yazamaz',
        "  DİKKAT: üyeler de yazabiliyor; allowed_sender_ids boş olduğu için yazan herkes kopyalanır",
    ]
    assert problems == [] and len(warnings) == 1                           # a warning, never a problem
    assert warnings[0].startswith('Telegram: "Kasım Ayı Kripto İşlemleri": üyeler de yazabiliyor; allowed_sender_ids boş')
    assert "telegram.allowed_sender_ids" in warnings[0]
    # exactly these calls, in this order; the one request is the read-only topic list the listener also makes
    assert [x for x in client.log if isinstance(x, str)] == ["connect", "is_user_authorized", "get_entity", "request",
                                                             "disconnect"]
    assert -1003782294463 in client.log
    (request,) = [x for x in client.log if not isinstance(x, (str, int))]
    assert isinstance(request, functions.messages.GetForumTopicsRequest)
    assert (request.peer, request.limit, request.offset_id, request.offset_topic) == (client.entity, 100, 0, 0)
    # ... and it is a request the installed Telethon can put on the wire for such a group (the listener sends the same)
    peer = utils.get_input_peer(tl(types.Channel, id=3782294463, title="KLAN VIP", megagroup=True, forum=True,
                                   access_hash=123456789))
    wire = bytes(functions.messages.GetForumTopicsRequest(peer=peer, offset_date=None, offset_id=0, offset_topic=0, limit=100))
    assert isinstance(peer, types.InputPeerChannel) and len(wire) > 20


async def test_selftest_says_tamam_when_only_admins_can_post() -> None:
    both_closed = [topic(5, "Kasım Ayı Kripto İşlemleri", closed=True), topic(7, "Fx İşlemleri", closed=True)]
    lines, warnings, problems, _ = await telegram_report(tg_config(), FakeTelegram(group(), both_closed))
    assert lines[-1] == "  TAMAM: bu konulara sadece yöneticiler yazabiliyor" and warnings == [] and problems == []

    open_topics = [topic(5, "Kripto İşlemleri"), topic(7, "Fx İşlemleri")]
    # the group itself does not let members write (text and pictures off, or everything off)
    for banned in ({"send_messages": True}, {"send_plain": True, "send_photos": True, "send_docs": True},
                   {"send_plain": True, "send_media": True}):
        lines, warnings, _, _ = await telegram_report(tg_config(), FakeTelegram(group(**banned), open_topics))
        assert lines[2] == '  Grup "KLAN VIP": grup ayarında üyelerin mesaj yazması kapalı (yalnızca yöneticiler yazabilir)'
        assert lines[3] == '    konu "Kripto İşlemleri" (kripto): açık konu, ama grupta üyelerin yazması kapalı'
        assert lines[-1] == "  TAMAM: bu konulara sadece yöneticiler yazabiliyor" and warnings == [], banned
    # text is off but pictures are allowed: a screenshot alone is a signal for the bot
    lines, warnings, _, _ = await telegram_report(tg_config(), FakeTelegram(group(send_plain=True), open_topics))
    assert lines[-1].startswith("  DİKKAT: üyeler de yazabiliyor") and len(warnings) == 1

    # members can write, but the config already limits the senders
    lines, warnings, _, _ = await telegram_report(tg_config(allowed_sender_ids=[111, 222]), FakeTelegram(group(), open_topics))
    assert lines[-1] == ('  TAMAM: üyeler de yazabiliyor ("Kripto İşlemleri", "Fx İşlemleri"), ama allowed_sender_ids '
                         "ayarlı: yalnızca listedeki 2 gönderenin mesajları kopyalanır") and warnings == []

    # a group without topics in the config: the group's own setting decides
    plain = tg_config(topics=[])
    lines, warnings, _, _ = await telegram_report(plain, FakeTelegram(group(forum=False, send_messages=True)))
    assert lines[-1] == "  TAMAM: bu gruba sadece yöneticiler yazabiliyor" and warnings == []
    client = FakeTelegram(group(forum=False))
    lines, warnings, _, _ = await telegram_report(plain, client)
    assert lines[-1].startswith("  DİKKAT: üyeler de yazabiliyor") and 'grup "KLAN VIP"' in warnings[0]
    assert "request" not in client.log                                    # no topic list is asked for
    channel = tl(types.Channel, id=1, title="Kanal", broadcast=True)      # a channel: only admins post
    lines, warnings, _, _ = await telegram_report(plain, FakeTelegram(channel))
    assert lines[-1] == "  TAMAM: bu gruba sadece yöneticiler yazabiliyor"
    assert selftest.members_can_post(SimpleNamespace()) is None and selftest.members_can_post(group()) is True


async def test_selftest_telegram_section_never_breaks_the_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(selftest, "TELEGRAM_TIMEOUT_SEC", 0.2)
    open_topics = [topic(5, "Kripto İşlemleri"), topic(7, "Fx İşlemleri", closed=True)]

    def unread(lines: list[str], warnings: list[str], reason: str) -> None:
        assert lines[2].startswith("  [UYARI] grup ayarları okunamadı (") and reason in lines[2], lines
        assert lines[2].endswith("üyelerin yazıp yazamadığı kontrol edilmedi") and len(lines) == 3
        assert warnings == ["Telegram: " + lines[2].replace("  [UYARI] ", "")]

    # no session at all: nothing is opened (and no empty session file is created)
    monkeypatch.chdir(tmp_path)
    cfg = tg_config(session_string=None)
    lines, warnings, problems, handed = await telegram_report(cfg, FakeTelegram(group(), open_topics))
    unread(lines, warnings, "Telegram kullanıcı oturumu yok")
    assert handed == [] and problems == [] and list(tmp_path.iterdir()) == []
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "telegram_user.session").write_bytes(b"oturum")  # the session file of the listener
    lines, warnings, _, handed = await telegram_report(cfg, FakeTelegram(group(), open_topics))
    assert handed == [cfg.telegram] and lines[-1].startswith("  DİKKAT: üyeler de yazabiliyor")

    cases = [
        (FakeTelegram(group(), open_topics, authorized=False), "Telegram oturumu geçersiz", True),
        (FakeTelegram(group(), open_topics, hang="connect"), "0 sn içinde yanıt gelmedi", True),
        (FakeTelegram(group(), open_topics, fail="connect"), "Connection to Telegram failed", True),
        (FakeTelegram(group(), open_topics, hang="request"), "0 sn içinde yanıt gelmedi", True),
        (FakeTelegram(group(), open_topics, fail="get_entity"), "Connection to Telegram failed", True),
        (RuntimeError("TG_SESSION_STRING geçersiz"), "TG_SESSION_STRING geçersiz", False),
    ]
    for client, reason, disconnects in cases:
        lines, warnings, problems, _ = await telegram_report(tg_config(), client)
        assert problems == [] and warnings and reason in warnings[0], (reason, lines)
        assert lines[-1].endswith("üyelerin yazıp yazamadığı kontrol edilmedi")
        if disconnects:
            assert client.log[-1] == "disconnect"                          # the session is always closed again

    # the session does not know the group yet: the dialogs are loaded once (as the listener does), then it is found
    client = FakeTelegram(group(), open_topics, unknown_first=True)
    lines, warnings, _, _ = await telegram_report(tg_config(), client)
    assert [x for x in client.log if isinstance(x, str)] == ["connect", "is_user_authorized", "get_entity", "get_dialogs",
                                                             "get_entity", "request", "disconnect"]
    assert lines[-1].startswith("  DİKKAT: üyeler de yazabiliyor")

    # a route without a topic, a group whose rights cannot be read, topics set on a group that has none
    lines, warnings, _, _ = await telegram_report(tg_config(), FakeTelegram(group(), [topic(7, "Fx İşlemleri", closed=True)]))
    assert "    [UYARI] 'kripto' rotasına uyan bir konu grupta bulunamadı (aranan: Kripto İşlemleri)" in lines
    assert lines[-1] == "  TAMAM: bu konulara sadece yöneticiler yazabiliyor" and len(warnings) == 1
    lines, warnings, _, _ = await telegram_report(tg_config(), FakeTelegram(group(), [topic(7, "Fx İşlemleri")], total=250))
    assert "    [UYARI] 'kripto' rotasına uyan bir konu ilk 100 konu arasında bulunamadı (aranan: Kripto İşlemleri)" in lines
    unknown = tl(types.Channel, id=1, title="KLAN VIP", megagroup=True, forum=True)       # no default rights returned
    lines, warnings, _, _ = await telegram_report(tg_config(), FakeTelegram(unknown, open_topics))
    assert lines[2] == '  Grup "KLAN VIP": grup ayarında üyelerin yazma izni okunamadı'
    assert lines[3] == '    konu "Kripto İşlemleri" (kripto): açık konu; üyelerin yazma izni okunamadı'
    assert 'grup ayarları okunamadı ("Kripto İşlemleri" için üyelerin yazma izni okunamadı)' in lines[-1]
    lines, warnings, _, _ = await telegram_report(tg_config(), FakeTelegram(group(forum=False), open_topics))
    assert "konulara ayrılmış bir grup değil; bot bu gruptan hiçbir mesajı kopyalamaz" in lines[-1] and len(warnings) == 1


def test_selftest_command_reads_telegram_only_while_the_bot_is_stopped(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], sleeper: Any) -> None:
    import yaml

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.safe_dump({
        "dry_run": True, "data_dir": str(tmp_path / "data"),
        "telegram": {"api_id": 1, "api_hash": "h", "session_string": "1oturum", "source_chats": [-1003782294463],
                     "topics": [{"name": "kripto", "title_contains": ["Kripto İşlemleri"], "brokers": ["sanal"],
                                 "kind": "crypto"}]},
        "notifier": {"bot_token": TOKEN, "owner_user_id": OWNER},
        "llm": {"api_key": "sk-ant-" + "x" * 40},
        "brokers": [{"name": "sanal", "type": "paper"}],
    }, allow_unicode=True), encoding="utf-8")
    venue = Venue(load_config(cfg_path).brokers[0], equity=1000.0)
    clients: list[FakeTelegram] = []

    def make_client(telegram_config: Any) -> FakeTelegram:
        clients.append(FakeTelegram(group(), [topic(5, "Kasım Ayı Kripto İşlemleri")]))
        return clients[-1]

    monkeypatch.setattr(cli, "_make_brokers", lambda config: ([venue], []))
    monkeypatch.setattr(cli, "_make_oracle", lambda config, brokers: FakePriceOracle({"ETH": 3500.0}))
    monkeypatch.setattr(cli, "_make_user_client", make_client)
    assert cli.main(["selftest", "--config", str(cfg_path), "--varlik", "ETH"]) == cli.EXIT_OK       # a warning only
    out = capsys.readouterr().out
    assert "TELEGRAM GRUBU" in out and '    konu "Kasım Ayı Kripto İşlemleri" (kripto): açık konu; üyeler yazabilir' in out
    assert "  DİKKAT: üyeler de yazabiliyor; allowed_sender_ids boş olduğu için yazan herkes kopyalanır" in out
    assert "UYARILAR (işlem açmayı engellemez)" in out and "SONUÇ: SORUN YOK" in out
    assert out.index("KONU \"kripto\"") < out.index("TELEGRAM GRUBU") < out.index("UYARILAR") < out.index("SONUÇ")
    assert len(clients) == 1 and clients[0].log[-1] == "disconnect" and venue.writes == []

    # while the bot runs (a live process holds bot.lock) the command is refused before anything is opened
    (tmp_path / "data").mkdir(exist_ok=True)
    (tmp_path / "data" / "bot.lock").write_text(json.dumps({"pid": sleeper.pid, "started": "x", "token": None}),
                                                encoding="utf-8")
    assert cli.main(["selftest", "--config", str(cfg_path), "--varlik", "ETH"]) == cli.EXIT_PROBLEM
    assert "Bot şu anda çalışıyor" in capsys.readouterr().out and len(clients) == 1
    # the real factory is the listener's (no connection is made by building it; a bad session string says so)
    from copytrader.telegram.listener import TelegramAuthError

    monkeypatch.undo()
    with pytest.raises(TelegramAuthError):
        cli._make_user_client(load_config(cfg_path).telegram)


# =========================================================================== #
# U1  guncelle.py --bekci-kur: the watchman alone (a window opened as administrator, a moved folder)
# =========================================================================== #
def test_the_watchman_can_be_registered_alone_without_an_update(g: Any, b: Any, tmp_path: Path,
                                                                capsys: pytest.CaptureFixture[str]) -> None:
    def files(folder: Path) -> dict[str, bytes]:
        return {p.relative_to(folder).as_posix(): p.read_bytes() for p in sorted(folder.rglob("*"))
                if p.is_file() and "/data/" not in p.as_posix()}

    def register(bot: Path, gorev: Any) -> tuple[int, str]:
        code = g.main(["--hedef", str(bot), "--bekci-kur"], calistir=Runner(), surecler=lambda: [], gorev=gorev,
                      baslat=lambda h, p: pytest.fail("the bot must not be started"), yeni_agac=bot)
        return code, capsys.readouterr().out

    bot = bot_folder(tmp_path / "a")
    (bot / "bekci.py").write_bytes((REPO / "bekci.py").read_bytes())
    (bot / "guncelle.py").write_bytes((REPO / "guncelle.py").read_bytes())
    before = files(bot)
    schtasks = Schtasks(bot)
    code, out = register(bot, schtasks)
    assert code == 0 and out.splitlines()[0].startswith("=== BEKÇİ KURULUMU ===")
    assert "   Bekçi kuruldu: bot çalışmıyorsa birkaç dakika içinde Telegram'dan haber verir." in out
    assert [c[1] for c in schtasks.calls] == ["/Create", "/Run"] and files(bot) == before      # nothing else changed
    assert not (bot / "yedek").exists() and not list(bot.glob("guncelleme_*.log"))
    # the bot of this folder is not running and no update is: the very first pass says so, after the test message
    assert schtasks.box.sent == [b.DENEME_METNI, b.ILK_UYARI] and "Not: bu eski sürüm" not in out

    old = bot_folder(tmp_path / "b", main=OLD_MAIN)                        # a version that writes no bot.lock
    (old / "bekci.py").write_bytes((REPO / "bekci.py").read_bytes())
    schtasks = Schtasks(old)
    code, out = register(old, schtasks)
    assert code == 0 and schtasks.box.sent == [b.DENEME_METNI] and schtasks.seen == [b.ESKI_SURUM]
    assert "Not: bu eski sürüm, çalışıp çalışmadığını bekçiye bildirmez" in out

    code, out = register(bot, Schtasks(bot, create=Result(1, b"", b"ERROR: Access is denied.\r\n")))
    assert code == 1 and "   Bekçi kurulamadı (isteğe bağlı): ERROR: Access is denied." in out
    code, out = register(bot, None)                                        # not Windows
    assert code == 2 and "Bekçi yalnızca Windows'ta kurulur" in out
    empty = tmp_path / "bos"
    empty.mkdir()
    code, out = register(empty, Schtasks(empty))
    assert code == 2 and "burası botun klasörü değil" in out and list(empty.iterdir()) == []


# =========================================================================== #
# U4  docs/GUNCELLEME.md says what the code does
# =========================================================================== #
def test_the_guide_tells_the_owner_about_the_watchman_the_live_test_and_the_way_back(g: Any, b: Any) -> None:
    doc = (REPO / "docs" / "GUNCELLEME.md").read_text(encoding="utf-8")
    # the watchman: its messages, how often, and the one line that removes it
    assert "## Bekçi: bot çalışmıyorsa haber verir" in doc
    assert b.ILK_UYARI.split(" (")[0] in doc and b.GERI_GELDI in doc and "Bekçi kuruldu ve çalışıyor" in b.DENEME_METNI
    assert "`Bekçi kuruldu ve çalışıyor`" in doc
    assert "30 dakikada bir" in doc and b.HATIRLATMA_ARASI == 30 * 60
    assert "5 dakikada bir" in doc and g.bekci_komutu("p", "s")[5:8] == ["MINUTE", "/MO", "5"]
    assert f"\nschtasks /Delete /TN {g.GOREV_ADI} /F\n" in doc and g.GOREV_ADI == "CopytraderBekci"
    assert "`Bekçi kurulamadı`" in doc and "Yönetici olarak çalıştır" in doc
    # every updater / watchman option the guide names exists
    assert "`--bekci-yok`" in doc and g._secenekler(["--bekci-yok"]).bekci_yok is True
    assert "guncelle.py --bekci-kur`" in doc and g._secenekler(["--bekci-kur"]).bekci_kur is True
    assert "\n.\\venv\\Scripts\\python.exe guncelle.py --geri-al\n" in doc and g._secenekler(["--geri-al"]).geri_al is True
    assert "bekci.py --dene`" in doc and '"--dene" in secenek' in (REPO / "bekci.py").read_text(encoding="utf-8")
    # the way back names the folder the backups are in
    assert "`yedek\\<tarih-saat>\\`" in doc and "## Eski sürüme dönmek" in doc
    # the old log files
    assert "copytrader.log" in doc and "`***`" in doc and "yeni bir token" in doc
    # the live test: a real order, and on the prop account with --bekle because the firm's rule is not known
    section = doc[doc.index("**Gerçek** küçük bir deneme işlemi"):]
    assert "en küçük gerçek işlemi" in section and "Bu gerçek bir emirdir" in section
    assert "kuralın ne olduğunu bilmiyoruz" in section and "`--bekle`" in section
    commands = [ln for ln in doc.splitlines() if ln.startswith(".\\venv\\Scripts\\python.exe -m copytrader ")]
    assert ".\\venv\\Scripts\\python.exe -m copytrader canli-test --broker fx --sembol EURUSD --bekle 120" in commands
    parser = cli.build_parser()
    for line in commands:                                                  # every documented bot command parses
        args = parser.parse_args(line.split(" -m copytrader ", 1)[1].split())
        assert args.command in ("selftest", "canli-test", "run")
    fx = parser.parse_args("canli-test --broker fx --sembol EURUSD --bekle 120".split())
    assert (fx.broker, fx.sembol, fx.bekle) == ("fx", "EURUSD", 120.0)
    # the steps table still has its nine steps, and the unnumbered watchman row
    rows = [ln for ln in doc.splitlines() if ln.startswith("| ") and ln[2:3].isdigit() and ln[3:5] == " |"]
    assert [r[2] for r in rows] == list("123456789") and "| – | Bekçi kurulur" in doc
