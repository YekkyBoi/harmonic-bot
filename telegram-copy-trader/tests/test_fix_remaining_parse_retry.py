"""Regression tests: transient parse retries reload the exact failed messages (F5), the failure counter never
leaks into the next signal (N3), ids an incomplete reading keeps are never consumed by another action (F11),
and a parser setup error at startup exits with the config-error code (F16)."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import copytrader.__main__ as cli
from copytrader.app import build_app, make_parser, parser_deadline_sec
from copytrader.config import load_config
from copytrader.engine.orchestrator import Engine
from copytrader.errors import ConfigError, ParserError
from copytrader.models import ActionType, ConfirmationStatus, Severity, Side, StoredMessage
from copytrader.storage.db import Database

from fakes import FakeBroker, FakeNotifier, FakeParser, FakePriceOracle
from test_app_cli import clean_logging, write_config  # noqa: F401  (fixture)
from test_engine import CHAT, SENDER, Env, action, add_msg, db, env, eth_open, make_config, result, wait_for  # noqa: F401


def transient() -> ParserError:
    return ParserError("Yapay zekâ servisi hata döndürdü (HTTP 529); sinyal okunamadı.", transient=True)


# --------------------------------------------------------------------------- #
# An engine on a virtual clock (the model being down for minutes)
# --------------------------------------------------------------------------- #
@dataclass
class Clocked:
    engine: Engine
    db: Database
    parser: FakeParser
    broker: FakeBroker
    notifier: FakeNotifier
    now: list[datetime]

    def advance(self, seconds: float) -> None:
        self.now[0] += timedelta(seconds=seconds)

    async def msg(self, mid: int, text: str) -> None:
        await self.db.save_message(StoredMessage(chat_id=CHAT, message_id=mid, sender_id=SENDER, sender_name="T",
                                                 date=self.now[0], text=text))

    def warnings(self) -> list[str]:
        return self.notifier.texts(Severity.WARNING)

    async def settle(self) -> None:
        for _ in range(60):
            await asyncio.sleep(0.01)


@pytest.fixture
async def clocked(db: Database) -> AsyncIterator[Clocked]:
    now = [datetime.now(timezone.utc)]
    parser, broker, notifier = FakeParser(), FakeBroker("binance"), FakeNotifier()
    engine = Engine(make_config(), db, parser, [broker], notifier, price_oracle=FakePriceOracle({"ETH": 2232.85}),
                    clock=lambda: now[0])
    engine.config.telegram.context_window_sec = 300
    engine.retry_delays = (0.0, 0.0, 0.0)
    await engine.start(monitor=False)
    notifier.notifications.clear()
    env = Clocked(engine, db, parser, broker, notifier, now)
    yield env
    await engine.stop()


def jump_before_retry(env: Clocked, seconds: float) -> None:
    """Each failed parse + engine delay takes ``seconds`` of (virtual) time."""
    original = env.engine._retry_later

    async def later(key, delay, _orig=original):  # type: ignore[no-untyped-def]
        env.advance(seconds)
        await _orig(key, 0.0)

    env.engine._retry_later = later  # type: ignore[method-assign]


# --------------------------------------------------------------------------- #
# F5: the retry reloads the exact failed messages
# --------------------------------------------------------------------------- #
async def test_retry_reloads_the_failed_message_after_the_window_moved_past_it(clocked: Clocked) -> None:
    jump_before_retry(clocked, 400)                      # longer than context_window_sec (300)
    clocked.parser.push(transient(), result(eth_open(used_message_ids=[501])))
    await clocked.msg(501, "eth long 2232.18 stop 2205.11 tp 2310.6 1R")
    await clocked.engine.on_messages_ready(CHAT, SENDER)
    await clocked.settle()
    assert [[m.message_id for m in msgs] for msgs, _ in clocked.parser.calls] == [[501], [501]]
    assert len(clocked.broker.opened) == 1
    assert (await clocked.db.get_message(CHAT, 501)).consumed  # type: ignore[union-attr]
    assert clocked.engine._parse_failures == {} and clocked.engine._failed_ids == {}
    assert len(clocked.warnings()) == 1 and "tekrar denenecek" in clocked.warnings()[0]


async def test_exhausted_retries_outside_the_window_give_the_final_notice(clocked: Clocked) -> None:
    jump_before_retry(clocked, 150)                      # 4 attempts span 450 s: the last is outside the window
    clocked.parser.push(transient())
    await clocked.msg(700, "ETH long giriş 2232 stop 2205 tp 2310 1R")
    await clocked.engine.on_messages_ready(CHAT, SENDER)
    await clocked.settle()
    assert len(clocked.parser.calls) == 4
    assert all([m.message_id for m in msgs] == [700] for msgs, _ in clocked.parser.calls)
    final = clocked.warnings()[-1]
    assert "4 denemede okunamadı" in final and "otomatik açılmayacak" in final and "ETH long giriş" in final
    assert (await clocked.db.get_message(CHAT, 700)).consumed  # type: ignore[union-attr]
    assert clocked.engine._parse_failures == {} and clocked.engine._failed_ids == {}
    assert clocked.engine._retry_tasks == {} and clocked.broker.opened == []


async def test_failed_messages_that_are_gone_get_the_final_notice(clocked: Clocked,
                                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    jump_before_retry(clocked, 400)
    clocked.parser.push(transient())
    await clocked.msg(701, "BTC long giriş 67250 stop 66800 tp 68000 1R")
    original = clocked.db.get_message

    async def deleted(chat_id: int, message_id: int):  # type: ignore[no-untyped-def]
        return None if message_id == 701 and clocked.parser.calls else await original(chat_id, message_id)

    monkeypatch.setattr(clocked.db, "get_message", deleted)
    await clocked.engine.on_messages_ready(CHAT, SENDER)
    await clocked.settle()
    assert len(clocked.parser.calls) == 1
    final = clocked.warnings()[-1]
    assert "artık bulunamıyor" in final and "701" in final and "otomatik açılmayacak" in final
    assert clocked.engine._parse_failures == {} and clocked.engine._failed_ids == {}


# --------------------------------------------------------------------------- #
# N3: the counter never leaks between signals
# --------------------------------------------------------------------------- #
async def test_next_signal_gets_its_own_retries_after_a_final_failure(clocked: Clocked) -> None:
    jump_before_retry(clocked, 150)
    clocked.parser.push(transient(), transient(), transient(), transient())
    await clocked.msg(700, "ETH long giriş 2232 stop 2205 tp 2310 1R")
    await clocked.engine.on_messages_ready(CHAT, SENDER)
    await clocked.settle()
    assert clocked.engine._parse_failures == {}
    clocked.notifier.notifications.clear()
    # the API is flaky once more, then healthy: the new signal is retried, not given up after one attempt
    clocked.parser.push(transient(), result(eth_open(used_message_ids=[701])))
    await clocked.msg(701, "eth long 2232.18 stop 2205.11 tp 2310.6 1R")
    await clocked.engine.on_messages_ready(CHAT, SENDER)
    await clocked.settle()
    assert len(clocked.broker.opened) == 1
    assert [w.splitlines()[0] for w in clocked.warnings()] == [
        "Sinyal okunamadı (geçici hata: Yapay zekâ servisi hata döndürdü (HTTP 529); sinyal okunamadı.); "
        "tekrar denenecek."]
    assert clocked.engine._parse_failures == {}


async def test_counter_is_reset_when_the_failed_messages_were_handled_elsewhere(clocked: Clocked) -> None:
    clocked.engine.retry_delays = (0.2, 0.2, 0.2)
    clocked.parser.push(transient())
    await clocked.msg(700, "ETH long giriş 2232 stop 2205 tp 2310 1R")
    await clocked.engine.on_messages_ready(CHAT, SENDER)
    assert clocked.engine._parse_failures == {(CHAT, SENDER): 1}
    await clocked.db.mark_consumed(CHAT, [700])          # e.g. an incomplete watch consumed and asked about it
    await clocked.engine.on_messages_ready(CHAT, SENDER)  # nothing left: early return
    assert clocked.engine._parse_failures == {} and clocked.engine._failed_ids == {}
    assert clocked.engine._retry_tasks == {}              # the pending retry was cancelled
    assert len(clocked.parser.calls) == 1


async def test_counter_is_reset_on_success(clocked: Clocked) -> None:
    clocked.engine.retry_delays = (5.0, 5.0, 5.0)
    clocked.parser.push(transient(), result(eth_open(used_message_ids=[501, 502])))
    await clocked.msg(501, "eth long 2232.18 stop 2205.11 tp 2310.6")
    await clocked.engine.on_messages_ready(CHAT, SENDER)
    assert clocked.engine._parse_failures == {(CHAT, SENDER): 1}
    await clocked.msg(502, "1R")                          # the next message arrives before the retry
    await clocked.engine.on_messages_ready(CHAT, SENDER)
    assert [m.message_id for m in clocked.parser.calls[-1][0]] == [501, 502]
    assert len(clocked.broker.opened) == 1
    assert clocked.engine._parse_failures == {} and clocked.engine._retry_tasks == {}


async def test_a_message_joining_a_failing_batch_keeps_its_own_budget(clocked: Clocked) -> None:
    clocked.engine.retry_delays = (0.05,)
    clocked.parser.push(transient(), transient(), result(eth_open(used_message_ids=[502])))
    await clocked.msg(501, "BTC long giriş 67250 stop 66800 tp 68000 1R")
    await clocked.engine.on_messages_ready(CHAT, SENDER)     # 501: attempt 1
    await clocked.msg(502, "eth long 2232.18 stop 2205.11 tp 2310.6 1R")
    await clocked.engine.on_messages_ready(CHAT, SENDER)     # [501, 502]: 501 exhausted, 502 attempt 1
    await wait_for(lambda: len(clocked.parser.calls) == 3)
    await clocked.settle()
    assert [m.message_id for m in clocked.parser.calls[-1][0]] == [502]
    warnings = clocked.warnings()
    final = [w for w in warnings if "denemede okunamadı" in w]
    assert len(final) == 1 and "2 denemede" in final[0] and "BTC long" in final[0] and "eth long" not in final[0]
    assert any("tekrar denenecek" in w and "eth long" in w for w in warnings)
    assert len(clocked.broker.opened) == 1 and clocked.engine._parse_failures == {}


# --------------------------------------------------------------------------- #
# F11: an incomplete reading's ids are never consumed by another action of the same result
# --------------------------------------------------------------------------- #
def close_half(trade_id: int, ids: list[int]):  # type: ignore[no-untyped-def]
    return action(ActionType.CLOSE, base_asset="ETH", side=Side.LONG, target_trade_id=trade_id, close_fraction=0.5,
                  used_message_ids=ids, summary_tr="ETH yarısı kapatılacak")


def btc_incomplete(ids: list[int]):  # type: ignore[no-untyped-def]
    return action(ActionType.INCOMPLETE, symbol="BTCUSDT.P", base_asset="BTC", side=Side.LONG, used_message_ids=ids,
                  confidence=0.85, summary_tr="BTC LONG hazırlanıyor")


async def test_incomplete_ids_stay_unconsumed_and_the_owner_is_asked(env: Env) -> None:
    env.cfg.telegram.incomplete_timeout_sec = 0.2
    env.parser.push(result(eth_open()))
    await env.signal()
    trade = (await env.db.active_trades())[0]
    env.parser.push(result(close_half(trade.id, [601, 602]), btc_incomplete([602])))
    await add_msg(env.db, 601, "eth yarısını kapattım")
    await add_msg(env.db, 602, media="/x.jpg")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert (await env.db.get_message(CHAT, 601)).consumed  # type: ignore[union-attr]
    assert not (await env.db.get_message(CHAT, 602)).consumed  # type: ignore[union-attr]
    assert all(len(b.calls_of("close_position")) == 1 for b in env.brokers)
    await wait_for(lambda: any(c.kind == "incomplete" for c in env.notifier.confirmations))
    assert [c.action.summary_tr for c in env.notifier.confirmations if c.kind == "incomplete"] == [
        "BTC LONG hazırlanıyor"]


async def test_a_re_read_of_a_kept_message_never_applies_the_close_twice(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()
    trade = (await env.db.active_trades())[0]
    env.parser.push(result(close_half(trade.id, [601, 602]), btc_incomplete([602])))
    await add_msg(env.db, 601, "eth yarısını kapattım")
    await add_msg(env.db, 602, media="/x.jpg")
    await env.engine.on_messages_ready(CHAT, SENDER)
    # the next batch contains 602 again; the model reads the same close from it
    env.parser.push(result(close_half(trade.id, [602])))
    await add_msg(env.db, 603, "btc 1R")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert all(len(b.calls_of("close_position")) == 1 for b in env.brokers)
    conf = env.notifier.confirmations[-1]
    assert "message_already_applied" in [i.code for i in conf.issues]
    text = next(i.message_tr for i in conf.issues if i.code == "message_already_applied")
    assert "zaten uygulandı" in text and "%50'si kapatıldı" in text
    assert conf.status is ConfirmationStatus.PENDING


async def test_a_different_instruction_referring_to_an_applied_message_stays_automatic(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()
    trade = (await env.db.active_trades())[0]
    env.parser.push(result(close_half(trade.id, [601])))
    await add_msg(env.db, 601, "eth yarısını kapattım")
    await env.engine.on_messages_ready(CHAT, SENDER)
    n = len(env.notifier.confirmations)
    env.parser.push(result(action(ActionType.CLOSE, base_asset="ETH", side=Side.LONG, target_trade_id=trade.id,
                                  close_fraction=1.0, used_message_ids=[610, 601])))
    await add_msg(env.db, 610, "kalanı da kapattım", reply_to=601)
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert len(env.notifier.confirmations) == n
    assert all(len(b.calls_of("close_position")) == 2 for b in env.brokers)


# --------------------------------------------------------------------------- #
# F5 wiring: the parser gets a deadline well inside the context window
# --------------------------------------------------------------------------- #
def test_parser_deadline_is_a_third_of_the_window_at_most(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-" + "x" * 40)
    config = load_config(write_config(tmp_path))
    assert config.llm.timeout_sec == 120 and config.telegram.context_window_sec == 300
    assert parser_deadline_sec(config) == 100
    assert make_parser(config)._deadline_sec == 100
    assert cli._make_parser(config)._deadline_sec == 100
    config.telegram.context_window_sec = 600
    assert make_parser(config)._deadline_sec == 120         # timeout_sec is the upper bound
    assert build_app(config).parser._deadline_sec == 120
    config.telegram.context_window_sec = 60
    assert parser_deadline_sec(config) == 60                # never so short that normal readings time out
    config.llm.timeout_sec = 30
    assert parser_deadline_sec(config) == 30


# --------------------------------------------------------------------------- #
# F16: no API key at startup -> config-error exit code (no restart loop)
# --------------------------------------------------------------------------- #
@pytest.fixture
def no_credentials(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for k in [k for k in os.environ if k.startswith("ANTHROPIC_") and k != "ANTHROPIC_BASE_URL"]:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))


@pytest.mark.usefixtures("no_credentials", "clean_logging")
def test_run_without_api_key_exits_with_the_config_code(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = cli.main(["run", "--config", str(write_config(tmp_path))])
    out = capsys.readouterr().out
    assert code == cli.EXIT_CONFIG == 2
    assert "Yapılandırma hatası" in out and "API anahtarı bulunamadı" in out and "ANTHROPIC_API_KEY" in out


@pytest.mark.usefixtures("no_credentials")
def test_parse_without_api_key_exits_with_the_config_code(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = cli.main(["parse", "--text", "eth long", "--config", str(write_config(tmp_path))])
    assert code == cli.EXIT_CONFIG and "API anahtarı bulunamadı" in capsys.readouterr().out


@pytest.mark.usefixtures("no_credentials")
def test_build_app_raises_config_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as info:
        build_app(load_config(write_config(tmp_path)))
    assert "API anahtarı bulunamadı" in str(info.value) and isinstance(info.value.__cause__, ParserError)
