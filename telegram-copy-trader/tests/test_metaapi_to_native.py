"""Positions the MetaApi adapter opened stay managed when the same account is driven by the native terminal.

The friend's Tickmill account switches from type "metaapi" to "mt5_bridge" (the native adapter in a helper process)
while a position of his may be open. The positions here are opened by the REAL MetaApi adapter (against its test
fake, test_metaapi_broker.FakeConn): legs with MetaApi's tickets (strings of the MetaTrader tickets) and the internal
tags, orders labelled with a clientId "CT_<body>L<leg>_0". The native adapter (against test_mt5_broker.FakeMT5, with
the same tickets) must sync, move, close and recognise them:

* by TICKET - always, whatever the terminal shows in the comment (sync / stop / target / close);
* by TAG - from the clientId MetaApi keeps in the MetaTrader comment (mt5_broker.comment_tag), for the checks that
  look orders up by their tag (a signal sent again, a lost answer, an unconfirmed fill).
"""

from __future__ import annotations

import pickle
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from copytrader.brokers.mt5_broker import MT5, comment_tag
from copytrader.models import BrokerPosition, EntryType, OpenRequest, PositionStatus

from test_metaapi_broker import THREE_TPS, FakeConn
from test_metaapi_broker import connected as metaapi_connected
from test_metaapi_broker import make_plan as metaapi_plan
from test_metaapi_broker import make_req as metaapi_req
from test_mt5_broker import _server_clock, FakeMT5, connected  # noqa: F401  (_server_clock: autouse fixture)

TAG = "ct9f00aa11bb"
FORMATS = ["CT_9f00aa11bbL{n}_0|copytrader", "copytrader|CT_9f00aa11bbL{n}_0", "CT_9f00aa11bbL{n}_0",
           "CT_9f00aa11bbL{n}_0 copytrader", "CT_9f00aa11bbL{n}"]


async def metaapi_position(entry_type: EntryType = EntryType.MARKET) -> tuple[BrokerPosition, OpenRequest]:
    """A three-leg position (0.1 lot per leg) opened by the MetaApi adapter, and the request it was opened with."""
    conn = FakeConn()
    b = await metaapi_connected(conn)
    try:
        sym = await b.resolve_symbol("ETH")
        if entry_type is EntryType.LIMIT:
            plan = metaapi_plan(entry_type=EntryType.LIMIT, entry=2220.0, sl=2205.11, tps=THREE_TPS)
        else:
            plan = metaapi_plan(tps=THREE_TPS)
        req = metaapi_req(sym, 0.3, plan, tag=TAG)
        pos = await b.open_position(req)
    finally:
        await b.close()
    assert [leg["tag"] for leg in pos.data["legs"]] == [f"{TAG}-1", f"{TAG}-2", f"{TAG}-3"]
    assert all(isinstance(leg["ticket"], str) for leg in pos.data["legs"])          # MetaApi ids are strings
    return pos, req


def _position(leg: dict[str, Any], comment: str, magic: int = 770077) -> SimpleNamespace:
    ticket = int(leg["ticket"])
    return SimpleNamespace(ticket=ticket, identifier=ticket, symbol="ETHUSD", type=MT5.ORDER_TYPE_BUY,
                           volume=float(leg["filled"]), price_open=float(leg["entry"]), sl=float(leg["sl"]),
                           tp=float(leg["tp"] or 0.0), magic=magic, comment=comment, profit=0.0, swap=0.0)


def _deal(p: SimpleNamespace) -> SimpleNamespace:
    return SimpleNamespace(ticket=p.ticket + 500, order=p.ticket, time_msc=1, type=0, entry=MT5.DEAL_ENTRY_IN,
                           magic=p.magic, position_id=p.ticket, reason=3, volume=p.volume, price=p.price_open,
                           commission=0.0, swap=0.0, profit=0.0, fee=0.0, symbol=p.symbol, comment=p.comment)


def _order(leg: dict[str, Any], comment: str) -> SimpleNamespace:
    ticket = int(leg["order_ticket"])
    return SimpleNamespace(ticket=ticket, symbol="ETHUSD", type=MT5.ORDER_TYPE_BUY_LIMIT,
                           volume_initial=float(leg["volume"]), volume_current=float(leg["volume"]),
                           price_open=float(leg["entry"]), sl=float(leg["sl"]), tp=float(leg["tp"] or 0.0),
                           magic=770077, comment=comment, type_time=0, time_expiration=0, state=1, position_id=0)


def account_of(pos: BrokerPosition, fmt: str) -> dict[str, Any]:
    """The same account as the native terminal lists it: same tickets, MetaApi's label in the comment."""
    positions, orders, deals = {}, {}, []
    for n, leg in enumerate(pos.data["legs"], start=1):
        if leg["state"] == "pending":
            o = _order(leg, fmt.format(n=n))
            orders[o.ticket] = o
        else:
            p = _position(leg, fmt.format(n=n))
            positions[p.ticket] = p
            deals.append(_deal(p))
    top = max([*positions, *orders])
    return {"positions": positions, "orders": orders, "history_orders": {}, "deals": deals, "_next": top + 1000}


def seed_terminal(state: Path, pos: BrokerPosition, fmt: str) -> None:
    """Writes that account as the state of tests/mt5_fake_terminal.py (the helper process's fake terminal)."""
    state.write_bytes(pickle.dumps(account_of(pos, fmt)))


def seed_fake(fake: FakeMT5, pos: BrokerPosition, fmt: str) -> None:
    for key, value in account_of(pos, fmt).items():
        setattr(fake, key, value)


def stored(pos: BrokerPosition) -> BrokerPosition:
    """Through the database (JSON), as after the restart that switches the connection type."""
    return BrokerPosition.model_validate_json(pos.model_dump_json())


# --------------------------------------------------------------------------- #
# reading MetaApi's label back
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("comment", "tag"), [
    ("CT_9f00aa11bbL2_0|copytrader", "ct9f00aa11bb-2"),
    ("copytrader|CT_9f00aa11bbL2_0", "ct9f00aa11bb-2"),
    ("CT_9f00aa11bb_0", "ct9f00aa11bb"),
    ("CT_9f00aa11bbL12_0 copytrader", "ct9f00aa11bb-12"),
    ("CT_9f00aa11bbL3", "ct9f00aa11bb-3"),                  # the end cut off by the 31-character comment limit
    ("ct9f00aa11bb-3", "ct9f00aa11bb-3"),                   # the native adapter's own label
    ("ct9f00aa11bb", "ct9f00aa11bb"),
    ("copytrader", "copytrader"),                           # nothing to read back: unchanged, matches no tag
    ("XCT_9f00aa11bb_0", "XCT_9f00aa11bb_0"),               # inside another word: not a label of this bot
    ("", ""),
])
def test_comment_tag_reads_metaapis_client_id_back(comment: str, tag: str) -> None:
    assert comment_tag(comment) == tag


def test_the_metaapi_adapters_own_client_ids_read_back_to_its_tags() -> None:
    from copytrader.brokers.metaapi_broker import encode_client_id

    for tag in ("ct9f00aa11bb", "ct9f00aa11bb-1", "ct9f00aa11bb-3", "ctd7c454ab04-12"):
        cid = encode_client_id(tag)
        assert cid is not None
        for fmt in ("{cid}|copytrader", "copytrader|{cid}", "{cid}"):
            assert comment_tag(fmt.format(cid=cid)) == tag, (tag, fmt)


# --------------------------------------------------------------------------- #
# the native adapter manages what MetaApi opened
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("fmt", FORMATS)
async def test_native_adapter_syncs_moves_closes_and_recognises_metaapi_positions(fmt: str) -> None:
    pos, req = await metaapi_position()
    fake = FakeMT5()
    seed_fake(fake, pos, fmt)
    b = await connected(fake)
    try:
        synced = await b.sync_position(stored(pos))
        assert synced.status is PositionStatus.OPEN and synced.remaining_quantity == pytest.approx(0.3)
        assert [leg["state"] for leg in synced.data["legs"]] == ["open"] * 3
        moved = await b.update_stop_loss(synced, 2210.0)
        sltp = fake.sent_actions(MT5.TRADE_ACTION_SLTP)
        assert sorted(r["position"] for r in sltp) == sorted(int(leg["ticket"]) for leg in pos.data["legs"])
        assert {p.sl for p in fake.positions.values()} == {2210.0}
        # the same signal again (a repost, a retried request): recognised by its tag, nothing is sent
        before = len(fake.sent)
        again = await b.open_position(req)
        assert len(fake.sent) == before and "zaten var; tekrar gönderilmedi" in (again.error or "")
        assert sorted(leg["tag"] for leg in again.data["legs"]) == [f"{TAG}-1", f"{TAG}-2", f"{TAG}-3"]
        part = await b.close_position(moved, 1 / 3)                     # the nearest target's leg first
        assert len(fake.positions) == 2 and part.remaining_quantity == pytest.approx(0.2)
        done = await b.close_position(part, 1.0)
        assert done.status is PositionStatus.CLOSED and fake.positions == {}
    finally:
        await b.close()


async def test_a_comment_the_broker_overwrote_still_leaves_the_position_managed_by_ticket() -> None:
    pos, _req = await metaapi_position()
    fake = FakeMT5()
    seed_fake(fake, pos, "[tp]")                                        # nothing of the label is left
    b = await connected(fake)
    try:
        synced = await b.sync_position(stored(pos))
        assert synced.status is PositionStatus.OPEN and synced.remaining_quantity == pytest.approx(0.3)
        done = await b.close_position(await b.update_stop_loss(synced, 2212.0), 1.0)
        assert done.status is PositionStatus.CLOSED and fake.positions == {}
    finally:
        await b.close()


async def test_a_pending_metaapi_order_that_fills_after_the_switch_is_tracked() -> None:
    pos, _req = await metaapi_position(EntryType.LIMIT)
    assert [leg["state"] for leg in pos.data["legs"]] == ["pending"] * 3
    fake = FakeMT5()
    seed_fake(fake, pos, FORMATS[0])
    b = await connected(fake)
    try:
        synced = await b.sync_position(stored(pos))
        assert synced.status is PositionStatus.PENDING
        for leg in pos.data["legs"]:
            fake.fill(int(leg["order_ticket"]))
        filled = await b.sync_position(synced)
        assert filled.status is PositionStatus.OPEN and filled.remaining_quantity == pytest.approx(0.3)
        assert {str(leg["ticket"]) for leg in filled.data["legs"]} == {leg["order_ticket"] for leg in pos.data["legs"]}
    finally:
        await b.close()


async def test_another_programs_position_with_the_same_comment_is_never_taken() -> None:
    pos, req = await metaapi_position()
    fake = FakeMT5()
    seed_fake(fake, pos, FORMATS[0])
    for p in fake.positions.values():
        p.magic = 123                                    # not this bot's magic number
    b = await connected(fake)
    try:
        assert b._legs_from_snapshot(*(await b._snapshot("ETHUSD")), TAG) == []
    finally:
        await b.close()

