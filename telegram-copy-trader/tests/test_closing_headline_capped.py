"""Review round 2: a size-capped entry (live price much closer to the stop than the signal's entry) must not give
the closing notice a headline: its real risk goes to zero near the stop and "realized / real risk" read "+197.8R" for
the trader's 2R. Real Engine and real CcxtBroker on the production replica."""
from __future__ import annotations

import pytest

from copytrader.models import EntryType, PositionStatus, TradeStatus

from test_preflight_scenarios import KRIPTO, Prod, _isolate, btc  # noqa: F401
from test_review1_engine_mgmt import finals, owner_texts, passes, prod  # noqa: F401

HEADLINES = ("🟢", "🔴", "😐")


@pytest.mark.parametrize("ask", [74000.0, 73817.0, 73803.0])
@pytest.mark.parametrize("which", ["stopLossPrice", "takeProfitPrice"])
async def test_a_capped_entry_close_to_its_stop_has_no_r_headline(prod: Prod, which: str, ask: float) -> None:
    """BTC long, entry 74000, stop 73800, target 74400 (the trader's 2R); Binance trades at ``ask`` at the signal."""
    win = which == "takeProfitPrice"
    prod.binance.set_price("BTC", ask - 2.0, ask)
    prod.tickmill.set_price("BTCUSD", ask - 52.0, ask - 50.0)
    await prod.say(btc(EntryType.MARKET, 74000.0, 73800.0, 74400.0), KRIPTO, "btc 1R long piyasadan")
    assert prod.asked() == []
    pos = await prod.positions(1)
    assert pos["binance"].status is PositionStatus.OPEN
    (order,) = prod.binance.trigger_orders(which)
    if win:
        prod.binance.set_price("BTC", 74400.0, 74402.0)
        prod.tickmill.set_price("BTCUSD", 74350.0, 74352.0)
    else:
        prod.binance.set_price("BTC", 73799.0, 73801.0)
        prod.tickmill.set_price("BTCUSD", 73749.0, 73751.0)
    prod.binance.fill(order["id"])
    for pid in list(prod.tickmill.positions):
        prod.tickmill.hit(pid, "DEAL_REASON_TP" if win else "DEAL_REASON_SL", 74350.0 if win else 73750.0)
    await passes(prod, 3)
    assert (await prod.trades())[0].status is TradeStatus.CLOSED
    pos = await prod.positions(1)
    (summary,) = finals(owner_texts(prod))
    first = summary.split("\n", 1)[0]
    if ask == 74000.0:                                  # at the signal's entry: not capped, the trader's R
        assert pos["binance"].data.get("initial_risk") is not None
        assert first.startswith("🟢 hooopp +2R" if win else "🔴 amk -1R"), summary
    else:                                               # capped: no stored risk, no headline, money line as ever
        assert pos["binance"].data.get("initial_risk") is None
        assert not first.startswith(HEADLINES) and "🏁" in first, summary
    assert "R koyduk" not in summary or "+2R" in summary  # never a wild figure
