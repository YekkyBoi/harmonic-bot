"""The first live FX signal (2026-11-12), replayed on the production replica with the real adapters on fakes.

Group text: "Eu short / 1R risk / 100 risk için 0.75 lot" with an EURUSD chart: entry 1.10872, stop 1.11005,
target 1.10654. On the server that day the native MT5 broker refused every order (order_send was called with
an empty keyword dict) and the MetaApi broker did not answer. The trader's own number is the yardstick here:
with 100 USD per R the fx account must be sent 0.75 lot.
"""

from __future__ import annotations

from copytrader.models import EntryType, Side, TradeStatus

from test_preflight_scenarios import FX, Prod, fx_pair, prod, set_eurusd  # noqa: F401  (prod: fixture)

ENTRY, STOP, TARGET = 1.10872, 1.11005, 1.10654


async def test_todays_eurusd_short_opens_on_both_accounts_with_the_traders_own_lot(prod: Prod) -> None:  # noqa: F811
    set_eurusd(prod, 1.10872, 1.10874)
    await prod.say(fx_pair("EURUSD", Side.SHORT, ENTRY, STOP, TARGET, entry_type=EntryType.MARKET), FX,
                   "Eu short\n1R risk\n100 risk için 0.75 lot")

    assert prod.asked() == []                                    # executed without a question, as on the day
    (trade,) = await prod.trades()
    assert trade.status is TradeStatus.OPEN
    # fx (100 USD per R): a market SELL of the trader's own 0.75 lot with the chart's stop and target
    assert prod.fx_orders() == [("SELL", "EURUSD", 0.75, 1.10872, STOP, TARGET)]
    # tickmill (45 USD per R): 45 / (13.3 pips x 10 USD) = 0.338 -> 0.33 lot, same levels, no price offset on fx
    assert prod.tickmill_orders() == [("market_sell", "EURUSD", 0.33, STOP, TARGET)]
    positions = await prod.positions(trade.id)
    assert {name: p.status.value for name, p in positions.items()} == {"fx": "open", "tickmill": "open"}
    assert all(p.stop_loss == STOP for p in positions.values())
    # loss at the stop stays within 1R on each account
    assert 0.75 * (STOP - 1.10872) * 100_000.0 <= 100.0 and 0.33 * (STOP - 1.10872) * 100_000.0 <= 45.0
