"""Fast consensus read (llm.fast_consensus): two parallel fast, low-effort reads; identical and clean -> used at
once, anything else -> the normal (slow, careful) read. No network."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

from copytrader.config import LLMConfig
from copytrader.models import ActionType
from copytrader.parsing.llm_parser import ClaudeSignalParser
from tests.test_llm_parser import ctx, make_response, msg, wire_action

OPEN = dict(action="open", base_asset="ETH", symbol="ETHUSDT", side="long", entry_type="market",
            entry_price=1696.63, stop_loss=1667.04, take_profits=[{"price": 1755.8, "close_fraction": 1.0}],
            risk_r=1.0, confidence=0.95, summary_tr="ETH long")


def resp(**over: Any) -> SimpleNamespace:
    return make_response({"actions": [wire_action(**{**OPEN, **over})]})


class SeqMessages:
    """Fast calls (speed='fast') get ``fast`` responses in order; the normal call gets ``slow``."""

    def __init__(self, fast: list[Any], slow: Any) -> None:
        self.fast, self.slow = list(fast), slow
        self.calls: list[dict[str, Any]] = []

    async def create(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        r = self.fast.pop(0) if kwargs.get("speed") == "fast" else self.slow
        if isinstance(r, Exception):
            raise r
        return r


def parser(fast: list[Any], slow: Any, **cfg: Any) -> tuple[ClaudeSignalParser, SeqMessages]:
    m = SeqMessages(fast, slow)
    client = SimpleNamespace(messages=m, beta=SimpleNamespace(messages=m))
    return ClaudeSignalParser(LLMConfig(**{"fast_consensus": True, **cfg}), client=client), m


def fast_calls(m: SeqMessages) -> list[dict[str, Any]]:
    return [c for c in m.calls if c.get("speed") == "fast"]


async def test_off_by_default_single_normal_call() -> None:
    m = SeqMessages([], resp())
    client = SimpleNamespace(messages=m, beta=SimpleNamespace(messages=m))
    res = await ClaudeSignalParser(LLMConfig(), client=client).parse([msg(1, "ETH long")], ctx())
    assert len(m.calls) == 1 and "speed" not in m.calls[0]
    assert res.actions[0].action is ActionType.OPEN


async def test_two_identical_fast_reads_are_used_without_the_slow_read() -> None:
    p, m = parser([resp(), resp()], resp(stop_loss=1.0))
    res = await p.parse([msg(1, "ETH long")], ctx())
    assert len(m.calls) == 2 and len(fast_calls(m)) == 2
    for c in m.calls:
        assert c["output_config"]["effort"] == "low"
        assert c["model"] == "claude-opus-5-5"
    a = res.actions[0]
    assert (a.action, a.base_asset, a.stop_loss, a.entry_price) == (ActionType.OPEN, "ETH", 1667.04, 1696.63)


async def test_differing_fast_reads_fall_back_to_the_slow_read() -> None:
    p, m = parser([resp(), resp(stop_loss=1667.4)], resp(stop_loss=1667.04, summary_tr="slow"))
    res = await p.parse([msg(1, "ETH long")], ctx())
    assert len(fast_calls(m)) == 2 and len(m.calls) == 3
    slow = m.calls[-1]
    assert "speed" not in slow and slow["output_config"]["effort"] == "high"
    assert res.actions[0].summary_tr == "slow"


async def test_take_profit_difference_also_falls_back() -> None:
    p, m = parser([resp(), resp(take_profits=[])], resp(summary_tr="slow"))
    res = await p.parse([msg(1, "ETH long")], ctx())
    assert len(m.calls) == 3 and res.actions[0].summary_tr == "slow"


async def test_fast_error_falls_back_to_the_slow_read() -> None:
    p, m = parser([resp(), RuntimeError("429")], resp(summary_tr="slow"))
    res = await p.parse([msg(1, "ETH long")], ctx())
    assert len(m.calls) == 3 and res.actions[0].summary_tr == "slow"


async def test_agreeing_but_unsure_or_incomplete_reads_fall_back() -> None:
    for over in ({"ambiguities": ["stop etiketi okunamadı"]}, {"confidence": 0.5},
                 {"action": "incomplete", "stop_loss": None}):
        p, m = parser([resp(**over), resp(**over)], resp(summary_tr="slow"))
        res = await p.parse([msg(1, "ETH long")], ctx())
        assert len(m.calls) == 3, over
        assert res.actions[0].summary_tr == "slow"


async def test_agreeing_reads_with_no_trade_action_are_used() -> None:
    none = make_response({"actions": [wire_action()]})
    p, m = parser([none, none], resp(summary_tr="slow"))
    res = await p.parse([msg(1, "günaydın")], ctx())
    assert len(m.calls) == 2 and res.actions[0].action is ActionType.NONE


async def test_fast_reads_keep_the_rest_of_the_request() -> None:
    p, m = parser([resp(), resp()], resp())
    await p.parse([msg(1, "ETH long")], ctx())
    a, b = fast_calls(m)
    assert json.dumps(a["messages"], sort_keys=True) == json.dumps(b["messages"], sort_keys=True)
    assert a["output_config"]["format"]["type"] == "json_schema"
    assert a["fallbacks"] == "default"
