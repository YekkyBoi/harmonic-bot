"""Verifier findings on forum topics: batches never mix topics, and a plan without a topic is never
opened on every broker while topics are configured."""

from __future__ import annotations

from datetime import datetime, timezone

from copytrader.models import EntryType, MarketType, Severity, Side, TakeProfit, TradePlan

from test_topics_engine import CRYPTO, Env, _env, db, eth_short, result  # noqa: F401  (db fixture)


async def test_general_batch_never_takes_another_topics_messages(db) -> None:
    env: Env = await _env(db, topics=False)          # forum group, no topic routes configured
    try:
        await env.msg(610, topic=CRYPTO, photo=True)
        await env.msg(611, "Eth short\n1R risk", topic=CRYPTO)  # still waiting for its own batch
        env.parser.push(result())                                  # General message: nothing to do
        await env.msg(612, "günaydın, ETH grafiğine bakın", topic=None)
        await env.ready(None)
        msgs, _ = env.parser.calls[0]
        assert [m.message_id for m in msgs] == [612]
        for mid in (610, 611):
            stored = await env.db.get_message(-1001111111111, mid)
            assert stored is not None and not stored.consumed
    finally:
        await env.engine.stop()


async def test_plan_without_route_is_refused_when_topics_configured(db) -> None:
    env: Env = await _env(db, topics=True)
    try:
        plan = TradePlan(
            plan_id="noroute", base_asset="ETH", quote_asset="USDT", market_type=MarketType.PERP, side=Side.SHORT,
            entry_type=EntryType.MARKET, entry_price=2239.2, stop_loss=2250.6,
            take_profits=[TakeProfit(price=2205.11)], risk_r=1.0, risk_pct=2.0, source_chat_id=-1001111111111,
            created_at=datetime.now(timezone.utc),
        )
        text = await env.engine.execute_open(plan)
        assert "AÇILMADI" in text
        assert env.binance.opened == [] and env.mt5.opened == []
        assert any("AÇILMADI" in t for t in env.notifier.texts(Severity.ERROR))
        assert await env.db.active_trades() == []
    finally:
        await env.engine.stop()
