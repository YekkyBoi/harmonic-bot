"""fx2 (the owner's BEM challenge), 24.11: "pozlar genelde 1.8R'lık setuplar, lavuklar genelde 1.8R'da kapatıyor, sen
1.9R'da kapat, geri kalanı aynen eskisi gibi" - ``tp_at_r: 1.9``, ``tp_at_r_from: 1.5``.

    targets     R is the trader's, on his chart (signal entry 1.1000 to stop 1.0980): his targets at 1.5R or farther
                go to 1.9R (1.1038), each with its share; nearer ones stay; none given: 1.9R; his moves the same way
    his close   while the price is at +1.5R or more (his R) and fx2 has the 1.9R target: not followed (the target or the
                stop decides) and the trade is over for him; below it it closes as before; partial closes, stop moves
                and the owner's own closes as before

On the replica of tests/test_own_accounts_no_confirm.py (the server's config after the update, fx2 on the
"challenge2" topic, its MetaTrader adapter in process), signals and management read by the REAL parser.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from copytrader.brokers.mt5_broker import MT5
from copytrader.models import PositionStatus, TradeStatus

import test_own_accounts_no_confirm as oa
from test_llm_parser import wire_action
from test_preflight_scenarios import Prod, ScriptedModel, _isolate  # noqa: F401  (_isolate: autouse)

C2 = oa.CHALLENGE2
ENTRY, STOP, TP = 1.1000, 1.0980, 1.1036          # a 1.8R long (20 pips of risk, 36 of target)


class World:
    def __init__(self, p: Prod, model: ScriptedModel) -> None:
        self.p, self.model, self.mid = p, model, 9100

    @property
    def term(self) -> Any:
        return self.p.fx2                                             # type: ignore[attr-defined]

    def price(self, bid: float, ask: float) -> None:
        oa.eur_at(self.p, bid, ask)          # (every terminal: the bot's market price is read from fx)

    async def post(self, text: str, reply_to: int | None = None, **wire: Any) -> int:
        self.mid += 1
        self.model.queue.append([wire_action(used_message_ids=[self.mid], confidence=0.95, **wire)])
        await self.p._post(self.mid, C2, text, reply_to)
        return self.mid

    async def signal(self, tps: list[dict[str, float]] | None = None, entry_type: str = "market") -> int:
        return await self.post("EUR long 1R", action="open", base_asset="EUR", side="long", entry_type=entry_type,
                               entry_price=ENTRY, stop_loss=STOP,
                               take_profits=tps if tps is not None else [{"price": TP, "close_fraction": 1.0}],
                               risk_r=1.0, summary_tr="EUR LONG")

    async def close(self, sig: int, text: str = "1.8R kapattık") -> int:
        return await self.post(text, reply_to=sig, action="close", base_asset="EUR", side="long",
                               close_fraction=1.0, refers_to_message_id=sig, summary_tr="EUR LONG kapat")

    def deals(self) -> list[tuple[Any, ...]]:
        """Market deals sent to fx2: (kind, symbol, lots, price, sl, tp); a closing deal carries no levels."""
        kinds = {MT5.ORDER_TYPE_BUY: "BUY", MT5.ORDER_TYPE_SELL: "SELL"}
        return [(kinds[r["type"]], r["symbol"], r["volume"], r.get("price"), r.get("sl"), r.get("tp"))
                for r in self.term.sent if r["action"] == MT5.TRADE_ACTION_DEAL]

    def opened(self) -> list[Any]:
        return list(self.term.positions.values())


def _fx2_block(r: float | None = None, low: float | None = None, *, off: bool = False) -> str:
    """fx2's block before the update (step 14 writes tp_at_r 1.9 / tp_at_r_from 1.5); ``off``: written as null by
    hand (the update leaves a written key alone)."""
    if off:
        return oa.FX2_BLOCK + "    tp_at_r: null\n"
    extra = (f"    tp_at_r: {r}\n" if r else "") + (f"    tp_at_r_from: {low}\n" if low else "")
    return oa.FX2_BLOCK + extra


async def _make(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **kw: Any) -> World:
    monkeypatch.setattr(oa, "FX2_BLOCK", _fx2_block(**kw))
    model = ScriptedModel()
    p = await oa.own(tmp_path, monkeypatch, model=model)
    w = World(p, model)
    w.price(1.10000, 1.10010)                                     # fx2 buys at 1.10010
    return w


@pytest.fixture
async def w(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    world = await _make(tmp_path, monkeypatch)
    yield world
    await world.p.close()


async def _fx2(p: Prod, trade_id: int):
    return (await p.positions(trade_id))["fx2"]


# 1.9R on the trader's chart: 1.1000 + 1.9 x 0.0020
TARGET = 1.1038


# --------------------------------------------------------------------------------------------------------------------
async def test_a_18r_setup_gets_its_target_at_19r_from_fx2s_entry(w) -> None:
    await w.signal()
    (deal,) = w.deals()
    assert deal[:2] == ("BUY", "EURUSD") and deal[4] == pytest.approx(STOP)
    assert deal[5] == pytest.approx(TARGET, abs=1e-5)
    (trade,) = await w.p.trades()
    pos = await _fx2(w.p, trade.id)
    assert [tp.price for tp in pos.take_profits] == [pytest.approx(TARGET, abs=1e-5)]
    assert pos.data["fixed_tp"]["r"] == 1.9
    assert any("TP 1.9R: 1.1038" in t for t in w.p.texts())


async def test_his_close_at_18r_is_held_at_1r_it_closes(w) -> None:
    sig = await w.signal()
    w.price(1.10370, 1.10380)                                     # +1.85R (bid)
    await w.close(sig)
    assert len(w.deals()) == 1 and len(w.opened()) == 1          # nothing closed
    (trade,) = await w.p.trades()
    assert trade.status is TradeStatus.OPEN
    assert (await _fx2(w.p, trade.id)).status is PositionStatus.OPEN
    (note,) = [t for t in w.p.texts() if "KAPATILMADI" in t]
    assert "trader kapattı" in note and "1.9R hedefi (1.1038)" in note and note.startswith("⏸")
    assert (await _fx2(w.p, trade.id)).data["close_held"]["r"] == pytest.approx(1.85)
    # (the monitor never closes it on its own afterwards)
    for _ in range(2):
        await w.p.engine.monitor.run_once()
    assert len(w.opened()) == 1
    # the next close of his, at +1R: followed
    w.price(1.10220, 1.10230)                                     # +1.0R
    await w.close(sig, "1R'da kapattık")
    assert w.deals()[-1][0] == "SELL" and w.opened() == []


async def test_his_close_at_1r_closes_at_once(w) -> None:
    sig = await w.signal()
    w.price(1.10220, 1.10230)
    await w.close(sig, "1R'da kapattık")
    assert [d[0] for d in w.deals()] == ["BUY", "SELL"] and w.opened() == []
    (trade,) = await w.p.trades()
    assert trade.status is TradeStatus.CLOSED


async def test_break_even_is_followed_then_his_close_at_18r_is_held_with_the_stop_at_the_entry(w) -> None:
    sig = await w.signal()
    w.price(1.10300, 1.10310)
    await w.post("stop girişe", reply_to=sig, action="move_sl", base_asset="EUR", side="long", move_sl_to_entry=True,
                 refers_to_message_id=sig, summary_tr="EUR LONG BE")
    (trade,) = await w.p.trades()
    pos = await _fx2(w.p, trade.id)
    assert pos.stop_loss == pytest.approx(1.10010, abs=1e-5)
    assert [tp.price for tp in pos.take_profits] == [pytest.approx(TARGET, abs=1e-5)]     # target kept
    w.price(1.10370, 1.10380)                                     # +1.85R measured on the original R
    await w.close(sig)
    assert len(w.opened()) == 1
    assert any("KAPATILMADI" in t and "stop (1.1001)" in t for t in w.p.texts())


async def test_the_owners_own_close_always_closes(w) -> None:
    await w.signal()
    w.price(1.10370, 1.10380)
    (trade,) = await w.p.trades()
    await w.p.engine.close_on_account("fx2", [trade.id])           # /pozkapat fx2
    assert w.opened() == [] and w.deals()[-1][0] == "SELL"


async def test_a_partial_close_of_his_is_followed(w) -> None:
    sig = await w.signal()
    w.price(1.10370, 1.10380)
    await w.post("yarısını kapattım", reply_to=sig, action="close", base_asset="EUR", side="long",
                 close_fraction=0.5, refers_to_message_id=sig, summary_tr="EUR LONG yarısı")
    assert w.deals()[-1][0] == "SELL"


async def test_multi_target_near_one_stays_the_far_one_goes_to_19r(w) -> None:
    await w.signal([{"price": 1.1010, "close_fraction": 0.5}, {"price": TP, "close_fraction": 0.5}])
    tps = sorted(d[5] for d in w.deals())
    assert tps == [pytest.approx(1.1010, abs=1e-5), pytest.approx(TARGET, abs=1e-5)]


async def test_no_target_given_gets_19r(w) -> None:
    await w.signal([])
    (deal,) = w.deals()
    assert deal[5] == pytest.approx(TARGET, abs=1e-5)


async def test_his_target_moves_far_stay_at_19r_near_ones_are_followed(w) -> None:
    sig = await w.signal()
    await w.post("TP 1.1040", reply_to=sig, action="move_tp", base_asset="EUR", side="long",
                 take_profits=[{"price": 1.1040, "close_fraction": 1.0}], refers_to_message_id=sig, summary_tr="TP")
    assert oa.level_changes(w.term)[-1][3] == pytest.approx(TARGET, abs=1e-5)
    await w.post("TP 1.1015", reply_to=sig, action="move_tp", base_asset="EUR", side="long",
                 take_profits=[{"price": 1.1015, "close_fraction": 1.0}], refers_to_message_id=sig, summary_tr="TP")
    assert oa.level_changes(w.term)[-1][3] == pytest.approx(1.1015, abs=1e-5)


async def test_a_limit_entry_measures_from_its_limit(w) -> None:
    w.price(1.10100, 1.10110)                                     # above the entry: the limit rests
    await w.signal(entry_type="limit")
    (order,) = oa.orders(w.term)
    assert order[0] == "BUY_LIMIT" and order[5] == pytest.approx(1.1000 + 1.9 * 0.0020, abs=1e-5)


async def test_without_the_setting_everything_is_as_before(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = await _make(tmp_path, monkeypatch, off=True)
    try:
        sig = await world.signal()
        assert world.deals()[0][5] == pytest.approx(TP)
        world.price(1.10370, 1.10380)
        await world.close(sig)
        assert world.opened() == []
    finally:
        await world.p.close()


def test_the_from_line_needs_a_target_beyond_it() -> None:
    from copytrader.config import Mt5BridgeBrokerConfig
    base = {"name": "x", "type": "mt5_bridge", "terminal_path": r"C:\\t\\terminal64.exe", "risk_usd": 10,
            "broker_company": "BEM"}
    with pytest.raises(ValueError):
        Mt5BridgeBrokerConfig(**base, tp_at_r_from=1.5)
    with pytest.raises(ValueError):
        Mt5BridgeBrokerConfig(**base, tp_at_r=1.9, tp_at_r_from=2.0)
    assert Mt5BridgeBrokerConfig(**base, tp_at_r=1.9, tp_at_r_from=1.5).tp_at_r == 1.9


async def test_his_target_rewrite_with_another_split_keeps_19r_on_fx2s_legs(w) -> None:
    sig = await w.signal([{"price": 1.1010, "close_fraction": 0.34}, {"price": 1.1030, "close_fraction": 0.33},
                          {"price": TP, "close_fraction": 0.33}])
    assert sorted(d[5] for d in w.deals()) == [pytest.approx(1.1010), pytest.approx(TARGET), pytest.approx(TARGET)]
    await w.post("TP'ler 1.1010 / 1.1020 / 1.1036", reply_to=sig, action="move_tp", base_asset="EUR", side="long",
                 take_profits=[{"price": 1.1010, "close_fraction": 0.34}, {"price": 1.1020, "close_fraction": 0.33},
                               {"price": TP, "close_fraction": 0.33}], refers_to_message_id=sig, summary_tr="TP")
    (trade,) = await w.p.trades()
    pos = await _fx2(w.p, trade.id)
    assert sorted(tp.price for tp in pos.take_profits) == [pytest.approx(1.1010), pytest.approx(1.1020),
                                                           pytest.approx(TARGET)]
    legs = sorted(p.tp for p in w.opened())
    assert legs == [pytest.approx(1.1010), pytest.approx(1.1020), pytest.approx(TARGET)]
    assert not any("kısmen uygulandı" in t for t in w.p.texts())


async def test_after_a_hold_the_trade_is_over_for_the_trader(w) -> None:
    sig = await w.signal()
    w.price(1.10370, 1.10380)
    await w.close(sig)
    # his next signal, the other way: no question about an opposite trade; fx2 keeps its long (no_hedge)
    await w.post("EUR short 1R", action="open", base_asset="EUR", side="short", entry_type="market",
                 entry_price=1.1037, stop_loss=1.1057, take_profits=[{"price": 1.1001, "close_fraction": 1.0}],
                 risk_r=1.0, summary_tr="EUR SHORT")
    assert w.p.asked() == []
    assert len(w.opened()) == 1 and w.opened()[0].type == MT5.ORDER_TYPE_BUY
    assert any("hedging yasağı" in t for t in w.p.texts())


async def test_no_hold_when_no_target_of_his_was_moved(w) -> None:
    sig = await w.signal([{"price": 1.1028, "close_fraction": 1.0}])          # 1.4R: kept
    (deal,) = w.deals()
    assert deal[5] == pytest.approx(1.1028)
    w.price(1.10330, 1.10340)                                     # +1.65R, its target not (yet) filled here
    await w.close(sig)
    assert w.opened() == []


def test_the_update_sets_fx2_only_and_keeps_a_written_value(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from copytrader.config import load_config
    text = oa.server_config_after_update(tmp_path, monkeypatch)
    path = tmp_path / "guncelleme" / "config.yaml"
    cfg = load_config(path)
    by = {b.name: b for b in cfg.brokers}
    assert (by["fx2"].tp_at_r, by["fx2"].tp_at_r_from) == (1.9, 1.5)
    assert all((b.tp_at_r, b.tp_at_r_from) == (None, None) for n, b in by.items() if n != "fx2")
    from copytrader import migrate
    assert migrate.migrate_config(path, terminal=(None, None)) == []                    # idempotent
    hand = text.replace("    tp_at_r: 1.9\n", "    tp_at_r: 2.5\n").replace("    tp_at_r_from: 1.5\n", "")
    path.write_text(hand, encoding="utf-8")
    assert migrate.migrate_config(path, terminal=(None, None)) == []
    assert ({b.name: b for b in load_config(path).brokers}["fx2"].tp_at_r,
            {b.name: b for b in load_config(path).brokers}["fx2"].tp_at_r_from) == (2.5, None)
