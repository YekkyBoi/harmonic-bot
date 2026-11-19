"""tasi.py: moving the bot to a new server (paketle on the old one, kontrol, kur on the new one).

The outside world is replaced as in the guncelle.py tests: the long commands (venv, pip, check, selftest) are
recorded instead of run, the process list is empty, the bot start writes a lock file. What reads or edits
config.yaml runs for real: a subprocess of this test's Python in the bot folder (PyYAML, copytrader.migrate).
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

REPO = Path(__file__).resolve().parents[1]

ENV_SECRETS = {
    "TG_API_ID": "12345", "TG_API_HASH": "a1b2c3d4e5f6GIZLIhash", "TG_BOT_TOKEN": "123456:GIZLI-bot-token-value",
    "TG_OWNER_ID": "42", "ANTHROPIC_API_KEY": "sk-ant-GIZLI-anahtar-0001", "BINANCE_API_KEY": "GIZLI-binance-key",
    "BINANCE_API_SECRET": "GIZLI-binance-secret", "MT5_LOGIN": "88123457", "MT5_PASSWORD": "GIZLI-mt5-pass",
    "MT5_SERVER": "FundingPips-Live",
}
LITERAL_PASSWORD = "LiteralPw-9931x"       # written into config.yaml itself (fx2)
LITERAL_LOGIN = "77001234"
PIN_LOGIN = 66012345

CONFIG = """\
dry_run: false
data_dir: data
telegram:
  api_id: ${{TG_API_ID}}
  api_hash: ${{TG_API_HASH}}
  source_chats: [-1003782294463]
notifier:
  bot_token: ${{TG_BOT_TOKEN}}
  owner_user_id: ${{TG_OWNER_ID}}   # sahibin Telegram ID'si
llm:
  api_key: ${{ANTHROPIC_API_KEY}}
brokers:
  - name: binance
    type: ccxt
    exchange: binanceusdm
    api_key: ${{BINANCE_API_KEY}}
    secret: ${{BINANCE_API_SECRET}}
    risk_usd: 10
  - name: fx
    type: mt5
    login: ${{MT5_LOGIN}}
    password: ${{MT5_PASSWORD}}
    server: ${{MT5_SERVER}}
    risk_usd: 100
  - name: fx2
    type: mt5_bridge
    optional: true
    terminal_path: {fx2}
    broker_company: BEM
    login: {login}
    password: {password}
    risk_usd: 1000
  - name: tickmill
    type: mt5_bridge
    optional: true
    terminal_path: {tickmill}     # arkadaşın terminali
    broker_company: Tickmill
    risk_usd: 50
"""


def load_tasi() -> ModuleType:
    spec = importlib.util.spec_from_file_location("tasi_under_test", REPO / "tasi.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def tasi() -> ModuleType:
    return load_tasi()


def terminal(root: Path, folder: str) -> Path:
    exe = root / folder / "terminal64.exe"
    exe.parent.mkdir(parents=True, exist_ok=True)
    exe.write_bytes(b"MZ terminal")
    return exe


def write_lock(data: Path) -> None:
    """bot.lock of a LIVE process (this test's parent), as the bot writes it."""
    data.mkdir(parents=True, exist_ok=True)
    (data / "bot.lock").write_text(json.dumps({
        "pid": os.getppid(), "started": datetime.now(timezone.utc).isoformat(timespec="seconds")}), encoding="utf-8")


class World:
    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.tmp = tmp_path
        self.old_pf = tmp_path / "eski_program_files"
        self.new_pf = tmp_path / "yeni_program_files"
        self.new_pf.mkdir()
        for name in ("ProgramW6432", "ProgramFiles(x86)"):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("ProgramFiles", str(self.new_pf))
        for name in ENV_SECRETS:
            monkeypatch.delenv(name, raising=False)
        self.bot = tmp_path / "eski sunucu" / "telegram-copy-trader"
        self.bot.mkdir(parents=True)
        shutil.copytree(REPO / "copytrader", self.bot / "copytrader", ignore=shutil.ignore_patterns("__pycache__"))
        for name in ("guncelle.py", "bekci.py", "tasi.py", "requirements.txt", "README.md"):
            shutil.copy2(REPO / name, self.bot / name)
        (self.bot / "docs").mkdir()
        (self.bot / "docs" / "TASIMA.md").write_text("# Taşıma\n", encoding="utf-8")
        self.fx_exe = terminal(self.old_pf, "FundingPips MT5")
        self.fx2_exe = terminal(self.old_pf, "BEM MT5")
        self.tickmill_exe = terminal(self.old_pf, "Tickmill MT5 Terminal")
        self.config_text = CONFIG.format(fx2=self.fx2_exe, tickmill=self.tickmill_exe, login=LITERAL_LOGIN,
                                         password=LITERAL_PASSWORD)
        (self.bot / "config.yaml").write_text(self.config_text, encoding="utf-8")
        self.env_text = "".join(f"{k}={v}\n" for k, v in ENV_SECRETS.items())
        (self.bot / ".env").write_text(self.env_text, encoding="utf-8")
        data = self.bot / "data"
        (data / "logs").mkdir(parents=True)
        (data / "copytrader.db").write_bytes(b"SQLite format 3\x00" + os.urandom(4096))
        (data / "telegram_user.session").write_bytes(b"session" + os.urandom(256))
        (data / "mt5_terminal_fx.json").write_text(json.dumps({
            "broker": "fx", "folder": str(self.fx_exe.parent), "path": str(self.fx_exe),
            "company": "MetaQuotes Ltd.", "server": "FundingPips-Live", "login": PIN_LOGIN}), encoding="utf-8")
        (data / "logs" / "copytrader.log").write_text("bugün\n", encoding="utf-8")
        old_log = data / "logs" / "copytrader.log.3"
        old_log.write_text("eski\n", encoding="utf-8")
        ten_days = time.time() - 10 * 86400
        os.utime(old_log, (ten_days, ten_days))
        (data / "stop.request").write_text("1", encoding="utf-8")
        (data / "bekci.json").write_text("{}", encoding="utf-8")
        for folder in ("venv", "yedek/20261101-101010", "karne_paketleri/PIL", "copytrader/__pycache__"):
            (self.bot / folder).mkdir(parents=True, exist_ok=True)
            (self.bot / folder / "x.bin").write_bytes(b"x")
        (self.bot / "copytrader" / "__pycache__" / "app.cpython-312.pyc").write_bytes(b"pyc")
        self.commands: list[list[str]] = []
        self.started: list[Path] = []
        self.answers: list[str] = []

    # ---- the outside world -------------------------------------------------------------------------------
    def yardimci(self, komut: list[str], klasor: Path, sure: float) -> tuple[int, str]:
        if komut[1:4] == ["-m", "pip", "freeze"]:
            return 0, "MetaTrader5==5.0.45\nTelethon==1.41.2\ncryptg==0.6.1\n"
        return self.tasi.yardimci_calistir(komut, klasor, sure)

    def calistir(self, komut: list[str], klasor: Path, sure: float, satir: Any) -> int:
        self.commands.append([str(k) for k in komut])
        satir("ok")
        return 0

    def baslat(self, hedef: Path, python: str) -> None:
        self.started.append(hedef)
        write_lock(hedef / "data")

    def girdi(self, soru: str) -> str:
        return self.answers.pop(0) if self.answers else ""

    def run(self, *argv: str, **kw: Any) -> int:
        args: dict[str, Any] = dict(calistir=self.calistir, surecler=lambda: [], girdi=self.girdi,
                                    yardimci=self.yardimci, gorev=None, ek_paket=None, baslat=self.baslat,
                                    python_bul=lambda _h: sys.executable, baslama_bekleme=5.0)
        args.update(kw)
        return self.tasi.main(list(argv), **args)

    def paketle(self, *extra: str) -> Path:
        assert self.run("paketle", "--hedef", str(self.bot), *extra) == 0
        zips = sorted(self.bot.glob("copytrader-tasima-*.zip"))
        assert len(zips) == 1
        return zips[0]

    def move_terminals(self) -> None:
        """The old server's terminals are not on the new one; the same three are installed elsewhere."""
        shutil.rmtree(self.old_pf, ignore_errors=True)
        self.new_fx = terminal(self.new_pf, "FundingPips MT5")
        self.new_fx2 = terminal(self.new_pf, "BEM MT5")
        self.new_tickmill = terminal(self.new_pf, "Tickmill MT5 Terminal")


@pytest.fixture()
def world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tasi: ModuleType) -> World:
    w = World(tmp_path, monkeypatch)
    w.tasi = tasi
    return w


# --------------------------------------------------------------------------- #
# paketle
# --------------------------------------------------------------------------- #
def test_paketle_refuses_while_the_bot_runs_and_says_how_to_stop_it(world: World, capsys) -> None:
    write_lock(world.bot / "data")
    assert world.run("paketle", "--hedef", str(world.bot)) == world.tasi.CIKIS_REDDEDILDI
    out = capsys.readouterr().out
    assert "bot çalışıyor" in out and "-m copytrader stop" in out and "zorla" in out
    assert not list(world.bot.glob("copytrader-tasima-*"))
    assert (world.bot / "data" / "bot.lock").is_file()          # never touched


def test_paketle_refuses_when_the_process_list_shows_a_bot(world: World, capsys) -> None:
    bot = [(4242, "python.exe", r"C:\bot\venv\Scripts\python.exe -m copytrader run")]
    assert world.run("paketle", "--hedef", str(world.bot), surecler=lambda: bot) == world.tasi.CIKIS_REDDEDILDI
    assert "4242" in capsys.readouterr().out
    assert not list(world.bot.glob("copytrader-tasima-*"))


def test_the_package_holds_the_bot_without_venv_caches_backups_old_logs_and_runtime_marks(world: World) -> None:
    zip_path = world.paketle()
    with zipfile.ZipFile(zip_path) as z:
        names = set(z.namelist())
    for wanted in ("config.yaml", ".env", "data/copytrader.db", "data/telegram_user.session",
                   "data/mt5_terminal_fx.json", "data/logs/copytrader.log", "docs/TASIMA.md", "tasi.py",
                   "guncelle.py", "bekci.py", "copytrader/__main__.py", "tasima_manifest.json"):
        assert wanted in names, wanted
    assert not [n for n in names if n.startswith(("venv/", "yedek/", "karne_paketleri/"))]
    assert not [n for n in names if "__pycache__" in n or n.endswith(".pyc")]
    assert "data/logs/copytrader.log.3" not in names                  # older than 7 days
    assert not {"data/stop.request", "data/bekci.json", "data/bot.lock"} & names
    assert not [n for n in names if n.startswith("copytrader-tasima-") or n.startswith("tasima_paketle_")]


def test_yedekli_takes_the_backups_along(world: World) -> None:
    with zipfile.ZipFile(world.paketle("--yedekli")) as z:
        assert "yedek/20261101-101010/x.bin" in z.namelist()


def test_the_manifest_lists_every_file_with_its_hash_and_the_terminals_without_logins(world: World) -> None:
    zip_path = world.paketle()
    with zipfile.ZipFile(zip_path) as z:
        manifest = json.loads(z.read("tasima_manifest.json"))
        members = {n for n in z.namelist() if n != "tasima_manifest.json"}
        import hashlib
        for entry in manifest["dosyalar"]:
            assert hashlib.sha256(z.read(entry["yol"])).hexdigest() == entry["sha256"]
    assert {e["yol"] for e in manifest["dosyalar"]} == members
    assert manifest["kaynak_klasor"] == str(world.bot)
    assert manifest["telegram_oturumu"] == {"yol": "data/telegram_user.session", "var": True}
    terminals = {t["ad"]: t for t in manifest["mt5_terminalleri"]}
    assert set(terminals) == {"fx", "fx2", "tickmill"}
    assert terminals["fx"]["terminal_path"] == str(world.fx_exe) and terminals["fx"]["kaynak"] == "pin"
    assert terminals["fx"]["server"] == "FundingPips-Live"
    assert terminals["tickmill"]["terminal_path"] == str(world.tickmill_exe)
    assert terminals["tickmill"]["broker_company"] == "Tickmill" and terminals["fx2"]["broker_company"] == "BEM"
    assert manifest["bekci"]["gorev_adi"] == "CopytraderBekci" and manifest["bekci"]["kullanici"] == "SYSTEM"
    assert "MetaTrader5==5.0.45" in manifest["pip_paketleri"]
    text = json.dumps(manifest)
    assert LITERAL_LOGIN not in text and LITERAL_PASSWORD not in text and str(PIN_LOGIN) not in text
    assert "login" not in text and "password" not in text


# --------------------------------------------------------------------------- #
# kontrol
# --------------------------------------------------------------------------- #
def rebuild(zip_path: Path, out: Path, change: dict[str, bytes], extra: dict[str, bytes] | None = None) -> Path:
    with zipfile.ZipFile(zip_path) as src, zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as dst:
        for info in src.infolist():
            dst.writestr(info.filename, change.get(info.filename, src.read(info.filename)))
        for name, data in (extra or {}).items():
            dst.writestr(name, data)
    return out


def test_kontrol_accepts_an_untouched_package(world: World, capsys) -> None:
    zip_path = world.paketle()
    capsys.readouterr()
    assert world.run("kontrol", str(zip_path)) == 0
    out = capsys.readouterr().out
    assert "SONUÇ: TAMAM" in out and "tickmill (Tickmill)" in out and "fx2 (BEM)" in out


def test_kontrol_finds_a_changed_file_an_added_file_and_a_damaged_zip(world: World, capsys) -> None:
    zip_path = world.paketle()
    changed = rebuild(zip_path, world.tmp / "degisik.zip", {"config.yaml": b"dry_run: true\n"},
                      {"copytrader/ek.py": b"print(1)\n", "../disari.txt": b"x"})
    capsys.readouterr()
    assert world.run("kontrol", str(changed)) == world.tasi.CIKIS_SORUN
    out = capsys.readouterr().out
    assert "içeriği değişmiş: config.yaml" in out
    assert "listede olmayan dosya: copytrader/ek.py" in out and "güvensiz dosya adı: ../disari.txt" in out
    half = world.tmp / "yarim.zip"
    half.write_bytes(zip_path.read_bytes()[: zip_path.stat().st_size // 2])
    assert world.run("kontrol", str(half)) == world.tasi.CIKIS_SORUN
    assert "SORUN" in capsys.readouterr().out


def test_kur_refuses_a_changed_package_and_extracts_nothing(world: World) -> None:
    changed = rebuild(world.paketle(), world.tmp / "degisik.zip", {".env": b"X=1\n"})
    target = world.tmp / "yeni" / "copytrader"
    assert world.run("kur", str(changed), "--hedef", str(target)) == world.tasi.CIKIS_REDDEDILDI
    assert not target.exists() and not world.commands


# --------------------------------------------------------------------------- #
# kur
# --------------------------------------------------------------------------- #
def test_kur_with_the_terminals_in_place_installs_checks_and_starts(world: World, capsys) -> None:
    zip_path = world.paketle()
    # same paths on the new server (the "old" Program Files stay): nothing to change
    target = world.tmp / "yeni" / "copytrader"
    assert world.run("kur", str(zip_path), "--hedef", str(target)) == 0
    out = capsys.readouterr().out
    for step in range(1, 10):
        assert f"\n{step}. " in out
    assert "11. Bot başlatıldı ve çalışıyor" in out and "=== ÖZET ===" in out and "SORUN" not in out
    assert (target / "config.yaml").read_text(encoding="utf-8") == world.config_text
    assert (target / ".env").read_text(encoding="utf-8") == world.env_text
    assert (target / "data" / "telegram_user.session").read_bytes() == (
        world.bot / "data" / "telegram_user.session").read_bytes()
    assert world.started == [target]
    joined = [" ".join(c) for c in world.commands]
    assert any("-m venv --clear" in c for c in joined)
    assert any("-m pip install --disable-pip-version-check -r requirements.txt" in c for c in joined)
    assert any(c.endswith("pip install --disable-pip-version-check MetaTrader5==5.0.45") for c in joined)
    assert any(c.endswith("-m copytrader check") for c in joined)
    assert any(c.endswith("-m copytrader selftest") for c in joined)
    assert any("import copytrader.__main__, copytrader.app, MetaTrader5" in c for c in joined)
    assert not (target / "data" / "guncelleme.lock").exists() or (
        time.time() - (target / "data" / "guncelleme.lock").stat().st_mtime > 1500)
    assert list(target.glob("tasima_*.log"))


def test_kur_stops_with_the_list_of_terminals_to_install(world: World, capsys) -> None:
    zip_path = world.paketle()
    shutil.rmtree(world.old_pf)                       # nothing installed on the new server yet
    target = world.tmp / "yeni" / "copytrader"
    assert world.run("kur", str(zip_path), "--hedef", str(target)) == world.tasi.CIKIS_SORUN
    out = capsys.readouterr().out
    assert "7. MetaTrader 5 terminalleri: SORUN" in out
    assert "fx (FundingPips): MetaTrader 5 terminali bulunamadı" in out
    assert "fx2 (BEM): MetaTrader 5 terminali bulunamadı" in out
    assert "tickmill (Tickmill): MetaTrader 5 terminali bulunamadı" in out
    assert "kurun, açın ve hesabınızla giriş yapın" in out
    assert not world.started
    assert not any(c[-1] in ("check", "selftest", "run") for c in world.commands)


def test_kur_offers_the_terminal_found_elsewhere_and_changes_only_that_key(world: World, capsys) -> None:
    zip_path = world.paketle()
    world.move_terminals()
    target = world.tmp / "yeni" / "copytrader"
    world.answers = ["EVET", "EVET", "EVET"]
    assert world.run("kur", str(zip_path), "--hedef", str(target)) == 0
    out = capsys.readouterr().out
    assert out.count("olarak değiştirildi") == 3
    new_text = (target / "config.yaml").read_text(encoding="utf-8")
    backups = sorted(target.glob("config.yaml.yedek-*"))
    assert len(backups) == 2                                          # one per changed key
    assert backups[0].read_text(encoding="utf-8") == world.config_text
    old_lines, new_lines = world.config_text.splitlines(), new_text.splitlines()
    assert len(old_lines) == len(new_lines)
    changed = [(a, b) for a, b in zip(old_lines, new_lines) if a != b]
    assert len(changed) == 2 and all(b.startswith("    terminal_path: ") for _a, b in changed)
    assert json.dumps(str(world.new_fx2)) in new_text and json.dumps(str(world.new_tickmill)) in new_text
    assert "# arkadaşın terminali" in new_text                          # the comment of that line stays
    pin = json.loads((target / "data" / "mt5_terminal_fx.json").read_text(encoding="utf-8"))
    assert pin["path"] == str(world.new_fx) and pin["folder"] == str(world.new_fx.parent)
    assert pin["company"] == "MetaQuotes Ltd." and pin["server"] == "FundingPips-Live" and pin["login"] == PIN_LOGIN
    assert len(list((target / "data").glob("mt5_terminal_fx.json.yedek-*"))) == 1
    assert world.started == [target]


def test_a_terminal_found_elsewhere_without_a_yes_changes_nothing(world: World, capsys) -> None:
    zip_path = world.paketle()
    world.move_terminals()
    target = world.tmp / "yeni" / "copytrader"
    assert world.run("kur", str(zip_path), "--hedef", str(target)) == world.tasi.CIKIS_SORUN
    assert (target / "config.yaml").read_text(encoding="utf-8") == world.config_text
    assert not list(target.glob("config.yaml.yedek-*")) and not world.started
    assert "onay verilmedi" in capsys.readouterr().out


def test_running_kur_again_goes_on_without_extracting_again(world: World, capsys) -> None:
    zip_path = world.paketle()
    shutil.rmtree(world.old_pf)
    target = world.tmp / "yeni" / "copytrader"
    assert world.run("kur", str(zip_path), "--hedef", str(target)) == world.tasi.CIKIS_SORUN
    (target / "data" / "yeni_veri.txt").write_text("korunmalı", encoding="utf-8")
    world.move_terminals()
    capsys.readouterr()
    assert world.run("kur", str(zip_path), "--hedef", str(target), "--evet") == 0
    out = capsys.readouterr().out
    assert "daha önce açılmış" in out
    assert (target / "data" / "yeni_veri.txt").read_text(encoding="utf-8") == "korunmalı"


def test_kur_never_overwrites_another_bot_folder_unless_asked_and_then_moves_it_aside(world: World, capsys) -> None:
    zip_path = world.paketle()
    target = world.tmp / "yeni" / "copytrader"
    (target / "data").mkdir(parents=True)
    (target / "data" / "copytrader.db").write_bytes(b"baska")
    assert world.run("kur", str(zip_path), "--hedef", str(target)) == world.tasi.CIKIS_REDDEDILDI
    assert "--uzerine-yaz" in capsys.readouterr().out
    assert (target / "data" / "copytrader.db").read_bytes() == b"baska"
    assert world.run("kur", str(zip_path), "--hedef", str(target), "--uzerine-yaz") == 0
    aside = list(target.parent.glob("copytrader.eski-*"))
    assert len(aside) == 1 and (aside[0] / "data" / "copytrader.db").read_bytes() == b"baska"
    assert (target / "data" / "copytrader.db").read_bytes() == (world.bot / "data" / "copytrader.db").read_bytes()


def test_a_failing_check_does_not_start_the_bot(world: World, capsys) -> None:
    zip_path = world.paketle()
    target = world.tmp / "yeni" / "copytrader"

    def calistir(komut: list[str], klasor: Path, sure: float, satir: Any) -> int:
        world.commands.append(komut)
        return 1 if komut[-1] == "check" else 0

    assert world.run("kur", str(zip_path), "--hedef", str(target), calistir=calistir) == world.tasi.CIKIS_SORUN
    assert "8. Ayarlar ve hesap bağlantıları kontrol edildi: SORUN" in capsys.readouterr().out
    assert not world.started and not (target / "data" / "guncelleme.lock").exists()


def test_a_failing_pip_stops_before_anything_else(world: World, capsys) -> None:
    zip_path = world.paketle()
    target = world.tmp / "yeni" / "copytrader"

    def calistir(komut: list[str], klasor: Path, sure: float, satir: Any) -> int:
        world.commands.append(komut)
        return 1 if "requirements.txt" in komut else 0

    assert world.run("kur", str(zip_path), "--hedef", str(target), calistir=calistir) == world.tasi.CIKIS_SORUN
    assert "5. Gerekli paketler kuruldu: SORUN" in capsys.readouterr().out
    assert not world.started


def test_baslatma_and_a_failing_watchman_registration(world: World, capsys) -> None:
    zip_path = world.paketle()
    target = world.tmp / "yeni" / "copytrader"

    class Done:
        returncode = 1
        stdout = b""
        stderr = b"ERROR: Access is denied."

    def gorev(komut: list[str], **_kw: Any) -> Done:
        return Done()

    assert world.run("kur", str(zip_path), "--hedef", str(target), "--baslatma", gorev=gorev) == 0
    out = capsys.readouterr().out
    assert "Bekçi kurulamadı" in out and "10. Bekçi: SORUN" in out and "11. Bot başlatılmadı (--baslatma)" in out
    assert not world.started


def test_no_secret_is_ever_printed_logged_or_put_in_the_manifest(world: World, capsys) -> None:
    zip_path = world.paketle()
    world.move_terminals()
    target = world.tmp / "yeni" / "copytrader"
    assert world.run("kur", str(zip_path), "--hedef", str(target), "--evet") == 0
    assert world.run("kontrol", str(zip_path)) == 0
    out = capsys.readouterr().out
    logs = "".join(p.read_text(encoding="utf-8") for p in [*world.bot.glob("tasima_*.log"),
                                                           *target.glob("tasima_*.log")])
    with zipfile.ZipFile(zip_path) as z:
        manifest = z.read("tasima_manifest.json").decode("utf-8")
    secrets = [v for k, v in ENV_SECRETS.items() if k not in ("TG_API_ID", "TG_OWNER_ID", "MT5_SERVER")]
    secrets += [LITERAL_PASSWORD, LITERAL_LOGIN, str(PIN_LOGIN), world.env_text.strip()]
    for secret in secrets:
        assert secret not in out, secret
        assert secret not in logs, secret
        assert secret not in manifest, secret


def test_tasi_is_standard_library_only() -> None:
    import ast

    tree = ast.parse((REPO / "tasi.py").read_text(encoding="utf-8"))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.add(node.module.split(".")[0])
    assert names <= set(sys.stdlib_module_names) | {"__future__"}, names - set(sys.stdlib_module_names)


def test_unsafe_names_are_recognised(tasi: ModuleType) -> None:
    for bad in ("../x", "/etc/x", "C:/x", "a\\b", "a//b", "a/../b", ""):
        assert tasi.guvensiz_ad(bad), bad
    for good in ("config.yaml", "data/logs/copytrader.log", ".env"):
        assert not tasi.guvensiz_ad(good)


def test_set_broker_value_refuses_a_placeholder_and_a_missing_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from copytrader.migrate import MigrationError, set_broker_value

    for name, value in ENV_SECRETS.items():
        monkeypatch.setenv(name, value)
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "config.yaml"
    text = CONFIG.format(fx2="/x/BEM/terminal64.exe", tickmill="${TICKMILL_EXE:-/y/terminal64.exe}",
                         login=LITERAL_LOGIN, password=LITERAL_PASSWORD)
    path.write_text(text, encoding="utf-8")
    with pytest.raises(MigrationError, match="ortam değişkeninden"):
        set_broker_value(path, "tickmill", "terminal_path", "/z/terminal64.exe")
    with pytest.raises(MigrationError, match="satırı yok"):
        set_broker_value(path, "fx", "terminal_path", "/z/terminal64.exe")
    with pytest.raises(MigrationError, match="adlı hesap"):
        set_broker_value(path, "yok", "terminal_path", "/z/terminal64.exe")
    assert path.read_text(encoding="utf-8") == text and not list(tmp_path.glob("config.yaml.yedek-*"))
    backup = set_broker_value(path, "fx2", "terminal_path", r"C:\Program Files\BEM MT5\terminal64.exe")
    assert backup.read_text(encoding="utf-8") == text
    assert path.read_text(encoding="utf-8") == text.replace(
        "terminal_path: /x/BEM/terminal64.exe", 'terminal_path: "C:\\\\Program Files\\\\BEM MT5\\\\terminal64.exe"')


def test_the_summary_lists_every_missing_terminal(world: World, capsys) -> None:
    zip_path = world.paketle()
    shutil.rmtree(world.old_pf)
    target = world.tmp / "yeni" / "copytrader"
    assert world.run("kur", str(zip_path), "--hedef", str(target)) == world.tasi.CIKIS_SORUN
    summary = capsys.readouterr().out.split("=== ÖZET ===")[-1]
    for name in ("fx (FundingPips)", "fx2 (BEM)", "tickmill (Tickmill)"):
        assert name in summary


def test_a_summary_also_follows_an_early_refusal(world: World, capsys) -> None:
    write_lock(world.bot / "data")
    world.run("paketle", "--hedef", str(world.bot))
    out = capsys.readouterr().out
    assert "=== ÖZET ===" in out and "2. Bot kapalı mı diye bakıldı: SORUN" in out.split("=== ÖZET ===")[-1]


def test_an_optional_account_that_did_not_connect_is_named_and_the_bot_still_starts(world: World, capsys) -> None:
    zip_path = world.paketle()
    target = world.tmp / "yeni" / "copytrader"

    def calistir(komut: list[str], klasor: Path, sure: float, satir: Any) -> int:
        world.commands.append(komut)
        if komut[-1] == "check":
            satir("✅ fx (mt5): hesap değeri 1,000.00 USD, serbest 1,000.00 USD")
            satir("⚠️  tickmill: bağlanılamadı: terminal kapalı (isteğe bağlı hesap: bot bu hesap olmadan çalışır, "
                  "bu hesabın sinyalleri atlanır)")
            satir("Sorun yok (isteğe bağlı tickmill bağlı değil).")
        return 0

    assert world.run("kur", str(zip_path), "--hedef", str(target), calistir=calistir) == 0
    out = capsys.readouterr().out
    assert "8. Ayarlar ve hesap bağlantıları kontrol edildi: SORUN" in out
    assert "Bağlanamayan isteğe bağlı hesap: tickmill." in out.split("=== ÖZET ===")[-1]
    assert world.started == [target]


def test_a_windows_config_keeps_its_line_endings_byte_for_byte(world: World) -> None:
    crlf = world.config_text.replace("\n", "\r\n").encode("utf-8")
    (world.bot / "config.yaml").write_bytes(crlf)
    zip_path = world.paketle()
    world.move_terminals()
    target = world.tmp / "yeni" / "copytrader"
    assert world.run("kur", str(zip_path), "--hedef", str(target), "--evet") == 0
    new = (target / "config.yaml").read_bytes()
    backups = sorted(target.glob("config.yaml.yedek-*"))
    assert backups[0].read_bytes() == crlf
    expected = crlf
    for old, exe in ((world.fx2_exe, world.new_fx2), (world.tickmill_exe, world.new_tickmill)):
        expected = expected.replace(f"terminal_path: {old}".encode(), f"terminal_path: {json.dumps(str(exe))}".encode())
    assert new == expected and b"\r\n" in new and new.count(b"\n") == new.count(b"\r\n")
