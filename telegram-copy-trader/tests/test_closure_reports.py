"""Closure reports: when the trader reports that ONE trade of his is over ("2R kapandı", "hedef geldi",
"stop oldu"), the copied trade is closed too, automatically, even if our own take profit is not hit.

Three layers, no network: the system prompt teaches the rule (text checks), the parser passes such a
reading through unchanged and shows the model the fields the rule relies on, and the engine applies it
without asking (the real incident replayed with a scripted parser)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from copytrader.config import AppConfig, LLMConfig, NotifierConfig, PaperBrokerConfig, TelegramConfig, TopicRoute
from copytrader.engine.orchestrator import Engine
from copytrader.models import (
    ActionType,
    EntryType,
    MarketType,
    ParseContext,
    ParsedAction,
    ParseResult,
    PositionStatus,
    Quote,
    Severity,
    Side,
    StoredMessage,
    TakeProfit,
    TradePlan,
    TradeRecord,
    TradeStatus,
)
from copytrader.parsing import prompts
from copytrader.parsing.llm_parser import ClaudeSignalParser, _WireResult
from copytrader.storage.db import Database
from copytrader.telegram import formatting as fmt

from fakes import FakeBroker, FakeNotifier, FakeParser, FakePriceOracle

SP = prompts.SYSTEM_PROMPT
CLOSURE_WORDING = ("2R kapandı", "kapandı", "kapattık", "TP geldi", "TP oldu", "hedef geldi", "hedefe ulaştı",
                   "kârla kapandı", "stop olduk", "stop oldu", "-1R stop", "closed", "hit TP", "stopped out")


def _line(start: str) -> str:
    """The one prompt line that starts with ``start``."""
    (line,) = [ln for ln in SP.splitlines() if ln.startswith(start)]
    return line


def _closure_rule() -> str:
    """The "Closure reports" bullet with its indented continuation lines."""
    lines = SP.splitlines()
    first = lines.index(_line("- Closure reports."))
    last = next(i for i in range(first + 1, len(lines)) if not lines[i].startswith("  "))
    return "\n".join(lines[first:last])


# --------------------------------------------------------------------------- #
# Prompt: the rule
# --------------------------------------------------------------------------- #
def test_prompt_has_one_closure_report_rule_in_the_group_wording_section() -> None:
    wording = SP.split("## Wording used in this group")[1].split("# Fields")[0]
    assert wording.count("- Closure reports.") == 1
    rule = _closure_rule()
    assert rule in wording
    assert "reports that ONE specific trade of his is over" in rule
    assert 'the copied trade is closed too: "close" with close_fraction 1.0' in rule
    for phrase in CLOSURE_WORDING:
        assert f'"{phrase}"' in rule, phrase
    assert "A copy must never outlive the trader's trade" in rule
    assert "close it even when our own take profit in OPEN TRADES has not been hit" in rule
    assert "our level can differ from the price where the trader really got out" in rule
    assert 'The result in the text ("2R", "-1R", "0.2R kar") says how the trade ended' in rule


def test_prompt_says_how_the_trade_is_identified() -> None:
    rule = _closure_rule()
    assert "Which trade is meant must be clear from ONE of these:" in rule
    reply, screenshot, asset = (next(ln for ln in rule.splitlines() if ln.startswith(f"  - {name}:"))
                                for name in ("Reply", "Screenshot", "Asset"))
    assert "replies to the trade's signal" in reply and "source_message_ids" in reply
    # A screenshot of the trade's position tool is as strong as a reply; the entry is what must match.
    assert "even when it is not a reply" in screenshot and "A matching screenshot is as strong as a reply" in screenshot
    assert "the tool's ENTRY label equals the trade's entry in OPEN TRADES within rounding" in screenshot
    assert "about 0.1% of the price" in screenshot
    # After "entry stop attık" the tool's stop label is the ORIGINAL stop, not the current stop_loss.
    assert "The tool's STOP label is the stop the trade was OPENED with" in screenshot
    assert "So asset, side and entry are enough" in screenshot
    assert "The tool's target label may differ from our take_profits too" in screenshot
    assert "names the asset, and exactly one OPEN TRADES entry has that asset" in asset
    # The general linking rules know the screenshot too, and the "one open trade" guess excludes it.
    linking = SP.split("## Linking management actions")[1].split("## Replied-to message")[0]
    assert "carries a position-tool screenshot of an open trade" in linking and "as surely as a reply does" in linking
    assert "If there is no reply, no such screenshot and no asset named, and exactly one trade is open" in linking


def test_prompt_asks_for_a_confident_reading_without_ambiguities() -> None:
    rule = _closure_rule()
    assert "set target_trade_id, give confidence about 0.9 or higher" in rule and "and NO ambiguity" in rule
    assert "never write it as an ambiguity and never lower the confidence for it" in rule
    # The three doubts the incident's reading raised are named as non-doubts.
    for not_a_doubt in ("our take profit is not hit yet",
                        "the tool's target or stop label differs from our levels",
                        '"it may only be a result report" (a result report about one identified trade IS the reason',
                        "the message is not a reply although the screenshot or the asset identifies the trade",
                        "whether the copy should be closed like the trader's or stay open"):
        assert not_a_doubt in rule, not_a_doubt
    ambiguities = _line("- ambiguities:")
    assert "Never put a question about the copying policy here" in ambiguities
    assert "never ask the owner to choose the policy" in SP.split("# Input")[0]


def test_prompt_keeps_the_cases_that_are_still_asked() -> None:
    rule = _closure_rule()
    assert "Ask the owner (confidence at most 0.8 plus one ambiguity) only when a real doubt remains:" in rule
    assert 'Nothing identifies the trade (no reply, no screenshot, no asset) and exactly one trade is open ' \
           '("kapattık" alone): close 1.0 of that trade.' in rule
    assert "Several open trades fit: target_trade_id null, and the candidates in the ambiguity." in rule
    # A report about a trade whose copy is already closed (or was never opened) is not a question either.
    assert 'OPEN TRADES lists no trade of the reported asset (or no trade at all)' in rule
    assert 'Return "none" without an ambiguity' in rule
    assert "(Exception: a closure report that is not a reply and has no open trade it could be about" in SP
    topics = SP.split("## Forum topics")[1].split("## Wording used in this group")[0]
    assert "or about a trade of this topic whose copy is already closed (or was never opened)" in topics


def test_prompt_keeps_the_cases_that_stay_none() -> None:
    lines = SP.splitlines()
    first = lines.index(_line("- NOT a closure report"))
    block = "\n".join(lines[first:next(i for i in range(first + 1, len(lines)) if not lines[i].startswith("  "))])
    assert block.startswith('- NOT a closure report -> "none", unless the same message also gives an instruction:')
    assert '"TP1 ✅", "TP1 geldi"' in block and "The copied take-profit orders handle it." in block
    assert "Results that cover several trades: a result list, a weekly or monthly summary" in block
    assert 'A bare result number with no reply, no position-tool screenshot and no asset: "2.8 r", "+3R".' in block
    assert '"-1R olarak yazılacak", "Grup işlemlerine -1 olarak yazılacak"' in block
    assert '"1R kâr var entry stop attık" -> "move_sl" with move_sl_to_entry true' in block
    # Explicit instructions for followers still win over the report in the same message.
    assert "An explicit instruction for those who are still in the trade wins over the closure report" in SP


def test_action_type_definitions_agree_with_the_rule() -> None:
    close, none = _line('- "close":'), _line('- "none":')
    assert "or reports that the trade is over" in close
    assert '"2R kapandı" / "hedef geldi" / "stop oldu" (1.0: the trader\'s trade is over' in close
    listed_as_none = none.split(' But "TP1 geldi, stopu girişe çekin"')[0]
    assert "stop oldu" not in listed_as_none and "hedef geldi" not in listed_as_none
    assert '"TP1 ✅ +2R"' in listed_as_none and "while later targets of the trade are still open" in listed_as_none
    assert 'a report that ONE trade is over ("2R kapandı", "hedef geldi", "stop oldu"' in none


def test_the_old_contradicting_wording_is_gone() -> None:
    for old in ('A result report on its own -> "none"',
                "Only an instruction in the same message makes it an action.",
                'results and hits ("TP1 ✅ +2R", "stop oldu", "+%15 kâr", "hedef geldi")',
                "A take profit or stop being hit needs no action",
                '"TP1 ✅ +2R" -> none, confidence about 0.95 (a result report',
                '"2.8 r" -> none (a result report)',
                "Without a reply and with exactly one open trade in the topic",
                "If no asset is named and exactly one trade is open, it is probably that one",
                "a management message of a topic always refers to one of them"):
        assert old not in SP, old


# --------------------------------------------------------------------------- #
# Prompt: the examples
# --------------------------------------------------------------------------- #
def test_prompt_has_the_incident_as_a_worked_example() -> None:
    examples = SP.split("# Examples")[1].split("## 3.")[1]
    (example,) = [ln for ln in examples.splitlines() if ln.startswith("- A closure report with a screenshot")]
    for part in ("OPEN TRADES has trade 23: asset BTC, side short, entry 74868.8, stop_loss 74868.8",
                 "take_profits [73095 (1.0, hit false)]", "source_message_ids [630, 631]",
                 'msg 640 (photo, NOT a reply) with the text "2R kapandı"',
                 '"75.575,0" on red', '"74.868,0" on blue', '"73.801,0" on black with "29:15"', '"73.448,2" on grey',
                 "-> close, close_fraction 1.0, target_trade_id 23, refers_to_message_id 630, base BTC, side short, "
                 "used_message_ids [640], confidence about 0.92, ambiguities []",
                 'summary "BTC SHORT kapatılacak (trader 2R ile kapattı)"',
                 "The screenshot identifies trade 23 although the message is not a reply",
                 "is the stop the trade was opened with, not the current stop_loss",
                 "differs from our take profit 73095, and that take profit is not hit",
                 "All of this is expected and none of it is an ambiguity"):
        assert part in example, part
    assert "2026-" not in examples                      # still no timestamps: the prompt is the cache prefix
    assert '"BTC SHORT kapatılacak (trader 2R ile kapattı)"' in _line("- summary_tr:")


def test_prompt_examples_cover_text_only_reports_and_the_none_cases() -> None:
    examples = SP.split("# Examples")[1]
    for part in ('"BTC hedef geldi" (not a reply) with exactly one open BTC trade (trade_id 5, its take profit not '
                 "hit yet) -> close, close_fraction 1.0, target_trade_id 5",
                 '"stop olduk" replying to the signal of open trade 8 -> close, close_fraction 1.0, target_trade_id 8',
                 '"TP1 ✅ +2R" about a trade whose later take profit (TP2) is still open -> none',
                 '"+3R" (no reply, no screenshot, no asset) -> none',
                 '"Bu hafta: BTC +2R, ETH -1R, SOL stop oldu" -> none',
                 '"2.8 r" (no reply, no screenshot, no asset) -> none',
                 '"btc tp geldi" -> close, close_fraction 1.0, target_trade_id 23',
                 '"kapandı" alone (no reply, no screenshot, no asset) while trade 23 is the only open trade of the '
                 "topic -> close, close_fraction 1.0, target_trade_id 23, confidence at most 0.8",
                 "The same msg 640 while OPEN TRADES lists no BTC trade",
                 '"EUR TP oldu" (not a reply) with exactly one open EUR trade in the topic -> close',
                 "Without a reply, a screenshot or an asset, and with exactly one open trade in the topic: close 1.0 "
                 "of it, confidence at most 0.8 plus an ambiguity"):
        assert part in examples, part


# --------------------------------------------------------------------------- #
# Parser: what the model sees for OPEN TRADES, and the reading passes through unchanged
# --------------------------------------------------------------------------- #
NOW = datetime(2026, 11, 10, 9, 30, tzinfo=timezone.utc)
CHAT = -1001111111111
CRYPTO = 10
TITLE = "💰 Kasım Ayı Kripto İşlemleri"
SIGNAL_IDS = [20101, 20102]


def incident_trade() -> TradeRecord:
    """Trade #1 as it stood when "2R kapandı" arrived: the stop already moved to the entry."""
    plan = TradePlan(
        plan_id="p1", base_asset="BTC", quote_asset="USDT", market_type=MarketType.PERP, side=Side.SHORT,
        entry_type=EntryType.LIMIT, entry_price=74868.8, stop_loss=75575.0,
        take_profits=[TakeProfit(price=73095.0)], risk_r=1.0, risk_pct=1.0, source_chat_id=CHAT,
        source_message_ids=SIGNAL_IDS, created_at=NOW,
    )
    return TradeRecord(id=1, plan=plan, status=TradeStatus.OPEN, current_stop_loss=74868.8,
                       current_take_profits=[TakeProfit(price=73095.0)], created_at=NOW, updated_at=NOW)


def closure_wire(**overrides: Any) -> dict[str, Any]:
    """The reading the prompt asks for (see the worked example), as the model would return it."""
    data: dict[str, Any] = {
        "action": "close", "symbol": "BTCUSDT.P", "base_asset": "BTC", "quote_asset": "USDT", "market_type": "perp",
        "side": "short", "entry_type": None, "entry_price": None, "stop_loss": None, "take_profits": [],
        "risk_r": None, "new_stop_loss": None, "move_sl_to_entry": False, "close_fraction": 1.0,
        "refers_to_message_id": 20101, "target_trade_id": 1, "used_message_ids": [20129],
        "evidence": "msg 20129 '2R kapandı' + grafik: BTC short pozisyon aracı, giriş 74.868,0 = #1 işleminin girişi.",
        "ambiguities": [], "confidence": 0.92, "summary_tr": "BTC SHORT kapatılacak (trader 2R ile kapattı)",
    }
    data.update(overrides)
    return data


class _Client:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.calls: list[dict[str, Any]] = []
        self._payload = payload
        self.beta = SimpleNamespace(messages=self)

    async def create(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        block = SimpleNamespace(type="text", text=json.dumps(self._payload))
        return SimpleNamespace(content=[block], stop_reason="end_turn", model="claude-opus-5-5")


def closure_message() -> StoredMessage:
    # Not a reply: in a forum every message "replies" to its topic's root message.
    return StoredMessage(chat_id=CHAT, message_id=20129, sender_id=900, sender_name="Kadir", date=NOW,
                         text="2R kapandı", reply_to_message_id=CRYPTO, topic_id=CRYPTO, topic_title=TITLE)


async def test_model_sees_the_current_stop_and_not_the_original_one() -> None:
    client = _Client({"actions": [closure_wire()]})
    parser = ClaudeSignalParser(LLMConfig(), client=client)
    context = ParseContext(now=NOW, open_trades=[incident_trade().summary(tp_hit=[False], open_fraction=1.0)],
                           topic_title=TITLE, topic_kind="crypto")
    await parser.parse([closure_message()], context)

    content = client.calls[0]["messages"][0]["content"]
    text = "\n".join(b["text"] for b in content if b["type"] == "text")
    assert "[msg_id=20129 | 2026-11-10T09:30:00Z | from=Kadir (id 900)]\n2R kapandı" in text   # no reply_to=
    (block,) = [b["text"] for b in content if b["text"].startswith(prompts.OPEN_TRADES_HEADER)]
    (shown,) = json.loads(block.split("\n", 1)[1])
    assert shown == {
        "trade_id": 1, "asset": "BTC", "quote": "USDT", "side": "short", "entry": 74868.8,
        "stop_loss": 74868.8,                                       # the CURRENT stop (moved to the entry)
        "take_profits": [{"price": 73095.0, "close_fraction": 1.0, "hit": False}],
        "open_fraction": 1.0, "status": "open", "source_message_ids": SIGNAL_IDS, "opened_at": NOW.isoformat(),
    }
    # The screenshot's red label (75575, the stop the trade was opened with) is not in the context: the
    # prompt therefore says so and matches on asset, side and entry. If the summary ever lists the
    # original stop, update that sentence of the prompt (and this test) with it.
    assert "75575" not in block
    described = _line("   - OPEN TRADES: trades this system")
    assert "stop_loss (the CURRENT stop" in described and "that original stop is not listed" in described
    for field in shown:
        assert field in described, field


async def test_the_closure_reading_passes_through_the_parser_unchanged() -> None:
    wire = closure_wire()
    _WireResult.model_validate({"actions": [wire]})
    parser = ClaudeSignalParser(LLMConfig(), client=_Client({"actions": [wire]}))
    context = ParseContext(now=NOW, open_trades=[incident_trade().summary()], topic_title=TITLE, topic_kind="crypto")
    (a,) = (await parser.parse([closure_message()], context)).actions

    assert a.action is ActionType.CLOSE and a.close_fraction == 1.0
    assert a.target_trade_id == 1 and a.refers_to_message_id == 20101 and a.used_message_ids == [20129]
    assert (a.base_asset, a.side) == ("BTC", Side.SHORT)
    assert a.confidence == 0.92 and a.ambiguities == []            # nothing added: the engine will not ask
    assert a.confidence >= LLMConfig().min_confidence
    assert a.summary_tr == "BTC SHORT kapatılacak (trader 2R ile kapattı)"


# --------------------------------------------------------------------------- #
# Engine: the incident replayed (scripted parser, fake broker, real in-memory database)
# --------------------------------------------------------------------------- #
KADIR = 900
ROUTE = TopicRoute(name="kripto", title_contains=["Kripto İşlemleri"], brokers=["binance"], kind="crypto")
OLD_AMBIGUITIES = [
    "'2R kapandı' bir sonuç bildirimi olabilir; trader'ın işlemi 73448.2 hedefinde kapanmış.",
    "Bizim #1 işlemimizin TP'si 73095 ve dolmamış görünüyor; trader gibi kapatılsın mı, yoksa açık mı kalsın?",
    "Mesaj bir yanıt değil; işlem #1'e asset eşleşmesiyle bağlandı.",
]


def result(*actions: ParsedAction) -> ParseResult:
    return ParseResult(actions=list(actions), model="fake")


def btc_signal() -> ParsedAction:
    return ParsedAction(
        action=ActionType.OPEN, symbol="BTCUSDT.P", base_asset="BTC", quote_asset="USDT",
        market_type=MarketType.PERP, side=Side.SHORT, entry_type=EntryType.LIMIT, entry_price=74868.8,
        stop_loss=75575.0, take_profits=[TakeProfit(price=73095.0)], risk_r=1.0, used_message_ids=SIGNAL_IDS,
        confidence=0.95, summary_tr="BTC SHORT, limit 74868.8, SL 75575, TP 73095, risk 1R",
    )


def closure(trade_id: int | None, **kw: Any) -> ParsedAction:
    data: dict[str, Any] = dict(
        action=ActionType.CLOSE, close_fraction=1.0, target_trade_id=trade_id, refers_to_message_id=20101,
        base_asset="BTC", side=Side.SHORT, used_message_ids=[20129], confidence=0.92,
        summary_tr="BTC SHORT kapatılacak (trader 2R ile kapattı)",
    )
    data.update(kw)
    return ParsedAction(**data)


class Env:
    def __init__(self, db: Database) -> None:
        cfg = AppConfig(
            telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[CHAT], topics=[ROUTE]),
            notifier=NotifierConfig(bot_token="123:abc", owner_user_id=42),
            brokers=[PaperBrokerConfig(type="paper", name="binance")],
        )
        cfg.monitor_interval_sec = 3600.0
        self.db, self.parser, self.notifier = db, FakeParser(), FakeNotifier()
        self.binance = FakeBroker("binance")
        self.oracle = FakePriceOracle()
        self.engine = Engine(cfg, db, self.parser, [self.binance], self.notifier, price_oracle=self.oracle)

    def price(self, last: float) -> None:
        self.binance.quote = Quote(bid=last - 0.1, ask=last + 0.1, last=last)
        self.oracle.prices["BTC"] = last

    async def msg(self, mid: int, text: str = "", *, reply_to: int | None = None, photo: bool = False) -> None:
        await self.db.save_message(StoredMessage(
            chat_id=CHAT, message_id=mid, sender_id=KADIR, sender_name="Kadir", date=datetime.now(timezone.utc),
            text=text, media_path="/nonexistent/chart.jpg" if photo else None, media_type="photo" if photo else None,
            reply_to_message_id=reply_to if reply_to is not None else CRYPTO, topic_id=CRYPTO, topic_title=TITLE,
        ))

    async def ready(self) -> None:
        await self.engine.on_messages_ready(CHAT, KADIR, CRYPTO)

    async def signal(self) -> int:
        """The limit signal, copied as a pending trade."""
        self.price(74500.0)                               # below the sell-limit entry
        self.parser.push(result(btc_signal()))
        await self.msg(20101, photo=True)
        await self.msg(20102, "BTC SHORT 1R - Limit Emir\nEntry: 74868,8\nStop: 75575\n100 dolar risk için 0,14 lot")
        await self.ready()
        trade = await self.db.find_trade_by_message(CHAT, 20102)
        assert trade is not None and trade.status is TradeStatus.PENDING
        return trade.id

    async def filled_with_stop_at_entry(self) -> int:
        """Signal -> the limit fills ("Aktif") -> the reply "1R kâr var entry stop attık" moves the stop."""
        trade_id = await self.signal()
        self.binance.sync_fn = lambda pos: pos if pos.status is not PositionStatus.PENDING else pos.model_copy(
            update={"status": PositionStatus.OPEN, "filled_quantity": pos.quantity,
                    "remaining_quantity": pos.quantity, "avg_entry_price": 74868.8})
        self.price(74161.0)                               # 1R in profit
        self.parser.push(result(ParsedAction(
            action=ActionType.MOVE_SL, move_sl_to_entry=True, target_trade_id=trade_id, refers_to_message_id=20102,
            base_asset="BTC", side=Side.SHORT, used_message_ids=[20110], confidence=0.95,
            summary_tr="BTC işleminde stop girişe çekilecek")))
        await self.msg(20110, "1R kâr var entry stop attık", reply_to=20102, photo=True)
        await self.ready()
        trade = await self.db.get_trade(trade_id)
        assert trade is not None and trade.status is TradeStatus.OPEN and trade.current_stop_loss == 74868.8
        assert self.notifier.confirmations == []
        return trade_id

    async def closure_report(self, action: ParsedAction) -> None:
        """Kadir's "2R kapandı" with the chart, NOT a reply; the market is at the incident's price."""
        self.price(73638.75)
        self.parser.push(result(action))
        await self.msg(20129, "2R kapandı", photo=True)
        await self.ready()


@pytest.fixture
async def env():
    db = Database(":memory:")
    await db.open()
    e = Env(db)
    await e.engine.start(monitor=False)
    e.notifier.notifications.clear()
    yield e
    await e.engine.stop()
    await db.close()


async def test_incident_closure_report_closes_the_copy_without_asking(env: Env) -> None:
    trade_id = await env.filled_with_stop_at_entry()
    await env.closure_report(closure(trade_id))

    # What the parser was given: a message that is not a reply, and the trade with its CURRENT stop.
    msgs, ctx = env.parser.calls[-1]
    assert [m.message_id for m in msgs] == [20129] and ctx.reply_to is None and ctx.topic_kind == "crypto"
    (shown,) = ctx.open_trades
    assert (shown["trade_id"], shown["entry"], shown["stop_loss"]) == (trade_id, 74868.8, 74868.8)
    assert shown["take_profits"] == [{"price": 73095.0, "close_fraction": 1.0, "hit": False}]
    assert shown["source_message_ids"] == SIGNAL_IDS and "75575" not in json.dumps(shown)

    # Our take profit (73095) is not hit at 73638.75, yet the copy ends with the trader's trade.
    assert env.notifier.confirmations == [] and env.notifier.texts(Severity.ERROR) == []
    ((_pos, fraction),) = env.binance.calls_of("close_position")
    assert fraction == 1.0
    trade = await env.db.get_trade(trade_id)
    assert trade is not None and trade.status is TradeStatus.CLOSED
    assert (await env.db.get_message(CHAT, 20129)).consumed          # type: ignore[union-attr]


async def test_the_old_unsure_reading_is_asked_as_a_plain_question(env: Env) -> None:
    """What happened in the incident (confidence 0.60 and three ambiguities): the owner is asked; the
    message now opens with the question itself and says what the buttons do."""
    trade_id = await env.filled_with_stop_at_entry()
    await env.closure_report(closure(trade_id, confidence=0.6, ambiguities=OLD_AMBIGUITIES))

    assert env.binance.calls_of("close_position") == []
    (conf,) = env.notifier.confirmations
    assert conf.kind == "manage" and conf.plan is not None and conf.plan.trade_id == trade_id  # type: ignore[union-attr]
    lines = fmt.format_confirmation(conf).split("\n")
    assert lines[0] == f"❓ <b>#{trade_id} BTC SHORT pozisyonunu KAPATAYIM MI?</b>"
    assert lines[1] == "✅ Onayla = uygula · ❌ İptal = dokunma" and lines[2] == ""
    assert lines[3] == fmt.HEADER_UNSURE


async def test_closure_report_for_a_copy_whose_limit_never_filled_cancels_the_entry(env: Env) -> None:
    """The prompt says the status does not matter: for a pending trade the system cancels the entry."""
    trade_id = await env.signal()
    await env.closure_report(closure(trade_id))

    assert env.notifier.confirmations == []
    assert len(env.binance.calls_of("cancel_entry")) == 1 and env.binance.calls_of("close_position") == []
    trade = await env.db.get_trade(trade_id)
    assert trade is not None and trade.status is TradeStatus.CANCELLED


async def test_closure_report_right_after_our_own_exit_is_ignored(env: Env) -> None:
    """The prompt says a report for a copy that already closed is ignored: here our take profit filled
    since the last monitor pass, so the position is synced first and nothing is closed twice or asked."""
    trade_id = await env.filled_with_stop_at_entry()
    env.binance.sync_fn = lambda pos: pos.model_copy(update={
        "status": PositionStatus.CLOSED, "remaining_quantity": 0.0, "realized_pnl": 250.0})
    await env.closure_report(closure(trade_id))

    assert env.binance.calls_of("close_position") == [] and env.notifier.confirmations == []
    trade = await env.db.get_trade(trade_id)
    assert trade is not None and trade.status is TradeStatus.CLOSED


async def test_unidentified_closure_with_one_open_trade_is_still_asked(env: Env) -> None:
    """"kapandı" alone (no reply, no screenshot, no asset): the prompt keeps confidence <= 0.8 plus an
    ambiguity, so the owner is asked before the only open trade is closed."""
    trade_id = await env.filled_with_stop_at_entry()
    env.price(73638.75)
    env.parser.push(result(closure(trade_id, used_message_ids=[20130], refers_to_message_id=None, confidence=0.8,
                                   ambiguities=[f"Hangi işlem olduğu yazılmamış; tek açık işlem #{trade_id} BTC seçildi."],
                                   summary_tr="BTC SHORT kapatılacak")))
    await env.msg(20130, "kapandı")
    await env.ready()

    assert env.binance.calls_of("close_position") == []
    (conf,) = env.notifier.confirmations
    assert fmt.format_confirmation(conf).startswith(f"❓ <b>#{trade_id} BTC SHORT pozisyonunu KAPATAYIM MI?</b>\n")
