"""Command line: ``python -m copytrader <komut> [--config config.yaml]``.

login  - first Telegram login (prints the session string)
chats  - list chats to find the source group id
run    - run the bot
check  - check config + broker connections (never trades)
parse  - show how a message would be understood (never trades)
"""

from __future__ import annotations

import argparse
import asyncio
import html
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .brokers.base import Broker
from .config import AppConfig, TelegramConfig, load_config
from .engine.prices import PriceOracle
from .models import ParseContext, Severity, StoredMessage, TradePlan

EXIT_OK, EXIT_PROBLEM, EXIT_CONFIG, EXIT_INTERRUPTED = 0, 1, 2, 130
_TAG = re.compile(r"<[^>]+>")
_BOT_TOKEN = re.compile(r"^\d{5,}:[A-Za-z0-9_-]{30,}$")


def html_to_text(text: str) -> str:
    """Telegram HTML -> terminal text."""
    return html.unescape(_TAG.sub("", text))


class PrintNotifier:
    """Notifier for CLI commands: prints to the terminal, never asks anything."""

    async def start(self, engine: Any) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def ask_confirmation(self, conf: Any) -> int | None:
        return None

    async def close_confirmation(self, conf: Any, result_text: str) -> None:
        return None

    async def notify(self, text: str, severity: Severity = Severity.INFO) -> None:
        label = {Severity.INFO: "BİLGİ", Severity.WARNING: "UYARI", Severity.ERROR: "HATA"}[severity]
        print(f"[{label}] {html_to_text(text)}")


# --------------------------------------------------------------------------- #
# Factories (patched in tests)
# --------------------------------------------------------------------------- #
def _make_parser(config: AppConfig) -> Any:
    from .parsing.llm_parser import ClaudeSignalParser

    return ClaudeSignalParser(config.llm)


def _make_oracle(config: AppConfig, brokers: Any) -> Any:
    return PriceOracle(brokers, config.price_source_exchange)


def _make_brokers(config: AppConfig) -> tuple[list[Broker], list[str]]:
    """Brokers for read-only commands: always dry-run wrapped, so no order can ever be sent."""
    from .brokers.base import BrokerError
    from .brokers.registry import create_broker

    out: list[Broker] = []
    errors: list[str] = []
    for cfg in config.enabled_brokers:
        try:
            out.append(create_broker(cfg, dry_run=True, app_config=config))
        except BrokerError as exc:
            errors.append(f"{cfg.name}: {exc}")
    return out, errors


# --------------------------------------------------------------------------- #
# Config helpers
# --------------------------------------------------------------------------- #
def _load_telegram_only(path: str) -> TelegramConfig:
    """Only the ``telegram`` section (login/chats work before the rest is filled in)."""
    import yaml

    from . import config as config_mod

    p = Path(path)
    config_mod._load_dotenv(p.parent / ".env")
    config_mod._load_dotenv(Path.cwd() / ".env")
    raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    return TelegramConfig.model_validate(config_mod._expand_env(raw.get("telegram") or {}))


def _telegram_config(path: str) -> TelegramConfig:
    try:
        return load_config(path).telegram
    except FileNotFoundError:
        raise
    except Exception:
        return _load_telegram_only(path)


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #
async def cmd_login(args: argparse.Namespace) -> int:
    from .telegram.listener import interactive_login

    tg = _telegram_config(args.config)
    session = await interactive_login(tg)
    print()
    print("✅ Giriş başarılı. Oturum dosyası kaydedildi:", f"{tg.session_path}.session")
    print()
    print("⚠️  UYARI: Aşağıdaki oturum metni Telegram hesabınıza TAM erişim verir.")
    print("    Kimseyle paylaşmayın, ekran görüntüsü almayın. Sunucu/Docker için yalnızca")
    print("    .env dosyasında TG_SESSION_STRING olarak saklayın.")
    print()
    print(session)
    return EXIT_OK


async def cmd_chats(args: argparse.Namespace) -> int:
    from .telegram.listener import list_dialogs

    tg = _telegram_config(args.config)
    rows = await list_dialogs(tg, limit=args.limit)
    print(f"{'SOHBET ID':>16}  {'TÜR':<10}  AD")
    print("-" * 60)
    for chat_id, title, kind in rows:
        print(f"{chat_id:>16}  {kind:<10}  {title}")
    print()
    print("Sinyal grubunun ID'sini config.yaml içinde telegram.source_chats listesine yazın.")
    return EXIT_OK


async def cmd_run(args: argparse.Namespace) -> int:
    from .app import run

    await run(args.config)
    return EXIT_OK


async def cmd_check(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    problems = 0
    print(f"Yapılandırma: {args.config}")
    print("Mod:", "DRY-RUN (gerçek emir gönderilmez)" if config.dry_run else "CANLI — GERÇEK EMİRLER GÖNDERİLİR")
    print(f"Risk: 1R = bakiyenin %{config.risk.risk_per_r_pct:g}'si · kaynak sohbet: {config.telegram.source_chats}")

    api_key = config.llm.api_key or os.environ.get("ANTHROPIC_API_KEY")
    print("Anthropic API anahtarı:", "ayarlı" if api_key and len(api_key) > 20 else "YOK / geçersiz görünüyor")
    problems += not (api_key and len(api_key) > 20)
    token_ok = bool(_BOT_TOKEN.match(config.notifier.bot_token.strip()))
    print("Bot token:", "ayarlı" if token_ok else "YOK / geçersiz görünüyor (@BotFather'dan alın)")
    problems += not token_ok
    owner_ok = config.notifier.owner_user_id > 0
    print("Sahip kullanıcı ID:", config.notifier.owner_user_id if owner_ok else "geçersiz")
    problems += not owner_ok
    session_ok = bool(config.telegram.session_string) or Path(f"{config.telegram.session_path}.session").is_file()
    print("Telegram kullanıcı oturumu:", "var" if session_ok else "YOK ('python -m copytrader login' çalıştırın)")
    problems += not session_ok

    brokers, errors = _make_brokers(config)
    for err in errors:
        print(f"❌ {err}")
        problems += 1
    if not brokers and not errors:
        print("⚠️  Etkin broker yok.")
        problems += 1
    for broker in brokers:
        try:
            broker.set_risk_config(config.risk)
            await asyncio.wait_for(broker.connect(), 120)
            equity = await broker.get_equity()
            free = await broker.get_free_margin()
            ccy = await broker.account_currency()
            print(f"✅ {broker.name} ({broker.kind}): hesap değeri {equity:,.2f} {ccy}, serbest {free:,.2f} {ccy}")
        except Exception as exc:
            print(f"❌ {broker.name}: bağlanılamadı: {exc}")
            problems += 1
        finally:
            try:
                await broker.close()
            except Exception:
                pass
    print()
    print("Sorun yok." if problems == 0 else f"{problems} sorun bulundu.")
    return EXIT_OK if problems == 0 else EXIT_PROBLEM


async def cmd_parse(args: argparse.Namespace) -> int:
    from .engine.orchestrator import Engine
    from .storage.db import Database

    config = load_config(args.config)
    now = datetime.now(timezone.utc)
    msgs: list[StoredMessage] = []
    for i, image in enumerate(args.image or [], start=1):
        if not Path(image).is_file():
            print(f"Görsel bulunamadı: {image}")
            return EXIT_CONFIG
        msgs.append(StoredMessage(chat_id=0, message_id=i, date=now, media_path=str(image), media_type="photo"))
    if args.text or not msgs:
        msgs.append(StoredMessage(chat_id=0, message_id=len(msgs) + 1, date=now, text=args.text or ""))

    db_path = Path(config.data_dir) / "copytrader.db"
    db = Database(db_path if db_path.is_file() else ":memory:")   # real DB only to see open trades
    await db.open()
    parser = _make_parser(config)
    brokers, errors = _make_brokers(config)
    for err in errors:
        print(f"[HATA] {err}")
    oracle = _make_oracle(config, lambda: engine.brokers)   # late-bound: brokers connected below
    engine = Engine(config, db, parser, brokers, PrintNotifier(), price_oracle=oracle)
    try:
        await engine.connect_brokers()       # read-only: no confirmations / trades touched
        ctx = ParseContext(now=now, open_trades=[t.summary() for t in await db.active_trades()])
        print("⏳ Okunuyor…")
        result = await parser.parse(msgs, ctx)
        print(html_to_text(await engine.describe_actions(result.actions, chat_id=0)))
        for action in result.actions:
            ev = await engine.evaluate(action, 0)
            if ev is not None and isinstance(ev.plan, TradePlan):
                print()
                print("📐 Broker bazında boyut önizlemesi (emir gönderilmez):")
                for line in await engine.preview_open(ev.plan, raw_symbol=action.symbol):
                    print("  •", line)
        if action_reasoning := [a.reasoning_brief for a in result.actions if a.reasoning_brief]:
            print()
            print("🔎 Okunan:", " | ".join(action_reasoning))
    finally:
        await engine.stop()
        close = getattr(oracle, "close", None)
        if close is not None:
            await close()
        aclose = getattr(parser, "aclose", None)
        if aclose is not None:
            await aclose()
        await db.close()
    return EXIT_OK


# --------------------------------------------------------------------------- #
# Argument parsing
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", default="config.yaml", help="yapılandırma dosyası (varsayılan: config.yaml)")

    p = argparse.ArgumentParser(
        prog="copytrader",
        description="Telegram grubundaki işlemleri kripto borsalarınıza ve MT4/MT5 brokerlarınıza kopyalar.",
    )
    sub = p.add_subparsers(dest="command", required=True, metavar="KOMUT")
    sub.add_parser("login", parents=[common], help="Telegram kullanıcı hesabınızla ilk girişi yapar")
    chats = sub.add_parser("chats", parents=[common], help="sohbetleri listeler (kaynak grubun ID'sini bulmak için)")
    chats.add_argument("--limit", type=int, default=200, help="en fazla kaç sohbet listelensin (varsayılan: 200)")
    sub.add_parser("run", parents=[common], help="botu çalıştırır")
    sub.add_parser("check", parents=[common], help="ayarları ve broker bağlantılarını kontrol eder (işlem açmaz)")
    parse = sub.add_parser("parse", parents=[common], help="bir mesajın nasıl anlaşılacağını gösterir (işlem açmaz)")
    parse.add_argument("--text", default="", help='mesaj metni, ör. "eth 1R long"')
    parse.add_argument("--image", action="append", metavar="DOSYA", help="ekran görüntüsü (birden fazla verilebilir)")
    return p


_COMMANDS = {"login": cmd_login, "chats": cmd_chats, "run": cmd_run, "check": cmd_check, "parse": cmd_parse}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command != "run":   # run sets up console + file logging itself
        logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    try:
        return asyncio.run(_COMMANDS[args.command](args))
    except KeyboardInterrupt:
        print("\nDurduruldu.")
        return EXIT_INTERRUPTED
    except FileNotFoundError as exc:
        print(f"Dosya bulunamadı: {exc.filename or exc}. (Örnek: config.example.yaml dosyasını config.yaml "
              "olarak kopyalayıp doldurun.)")
        return EXIT_CONFIG
    except Exception as exc:
        from pydantic import ValidationError

        if isinstance(exc, ValidationError):
            print(f"Yapılandırma hatalı ({args.config}):\n{exc}")
            return EXIT_CONFIG
        if type(exc).__name__ == "TelegramAuthError" or (isinstance(exc, ValueError) and "environment" in str(exc)):
            print(f"Hata: {exc}")
            return EXIT_CONFIG
        logging.getLogger(__name__).exception("Command %s failed", args.command)
        print(f"Hata: {exc}")
        return EXIT_PROBLEM


if __name__ == "__main__":
    sys.exit(main())
