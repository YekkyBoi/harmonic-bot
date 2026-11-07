from datetime import datetime, timedelta, timezone

import pytest
from telethon.extensions import html as tl_html

from copytrader.models import (
    ActionType,
    EntryType,
    ManagementPlan,
    MarketType,
    ParsedAction,
    PendingConfirmation,
    Side,
    TakeProfit,
    TradePlan,
    TradeRecord,
    TradeStatus,
    ValidationIssue,
)
from copytrader.telegram import formatting as f

NOW = datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)


def make_plan(**kw) -> TradePlan:
    data = dict(
        plan_id="p1", base_asset="ETH", quote_asset="USDT", market_type=MarketType.PERP, side=Side.LONG,
        entry_type=EntryType.MARKET, entry_price=2232.18, stop_loss=2205.11,
        take_profits=[TakeProfit(price=2310.6, close_fraction=1.0)], risk_r=1.0, risk_pct=2.0,
        source_chat_id=-1001, source_message_ids=[10, 11], summary_tr="ETH 1R long", created_at=NOW,
    )
    data.update(kw)
    return TradePlan(**data)


def make_action(**kw) -> ParsedAction:
    data = dict(
        action=ActionType.OPEN, symbol="ETHUSDT.P", base_asset="ETH", quote_asset="USDT", side=Side.LONG,
        entry_price=2232.18, stop_loss=2205.11, take_profits=[TakeProfit(price=2310.6)], risk_r=1.0,
        confidence=0.72, used_message_ids=[10, 11],
    )
    data.update(kw)
    return ParsedAction(**data)


def make_conf(**kw) -> PendingConfirmation:
    data = dict(
        id="abc123", kind="open", action=make_action(), plan=make_plan(), issues=[], market_price=2232.85,
        source_chat_id=-1001, source_message_ids=[10, 11], created_at=NOW, expires_at=NOW + timedelta(seconds=600),
    )
    data.update(kw)
    return PendingConfirmation(**data)


def assert_valid_html(text: str) -> str:
    plain, _ = tl_html.parse(text)
    assert plain
    return plain


def test_format_plan_turkish_content():
    text = f.format_plan(make_plan())
    plain = assert_valid_html(text)
    assert "📈" in text and "LONG" in text
    assert "piyasa" in text
    assert "2232.18" in text and "2205.11" in text and "2310.6" in text
    assert "1R = bakiyenin %2'si" in plain
    assert "pozisyonun %100'ü" in plain


def test_format_plan_limit_short_and_tp_split():
    plan = make_plan(
        side=Side.SHORT, entry_type=EntryType.LIMIT, entry_price=2300.0, stop_loss=2350.0,
        take_profits=[TakeProfit(price=2200, close_fraction=0.5), TakeProfit(price=2100, close_fraction=0.5)],
        risk_r=0.5, risk_pct=1.0,
    )
    plain = assert_valid_html(f.format_plan(plan))
    assert "📉" in plain and "SHORT" in plain
    assert "limit @ 2300" in plain
    assert "TP1: 2200" in plain and "TP2: 2100" in plain
    assert plain.count("pozisyonun %50'si") == 2
    assert "0.5R = bakiyenin %1'i" in plain


def test_html_escaping_of_untrusted_text():
    action = make_action(
        summary_tr="<b>hack</b> & co", ambiguities=["<script>x</script>"], reasoning_brief="a<b",
        symbol="<ETH>", base_asset=None,
    )
    conf = make_conf(
        action=action, plan=None,
        issues=[ValidationIssue(code="x", message_tr="SL < giriş & yanlış", blocking=True)],
    )
    text = f.format_confirmation(conf)
    assert "<b>hack</b>" not in text and "&lt;b&gt;hack&lt;/b&gt; &amp; co" in text
    assert "<script>" not in text and "&lt;script&gt;" in text
    assert "SL &lt; giriş &amp; yanlış" in text
    assert "&lt;ETH&gt;" in text
    plain = assert_valid_html(text)
    assert "<b>hack</b> & co" in plain        # renders as the literal text


def test_confirmation_header_issues_and_expiry():
    conf = make_conf(
        issues=[
            ValidationIssue(code="missing_stop_loss", message_tr="Stop loss yok", blocking=True),
            ValidationIssue(code="far_entry", message_tr="Giriş piyasadan uzak", blocking=False),
        ],
        action=make_action(ambiguities=["2232.85 güncel fiyat olabilir"]),
    )
    text = f.format_confirmation(conf)
    plain = assert_valid_html(text)
    assert "🤔" in plain and "Emin olamadım — şunu anladım, doğru mu?" in plain
    assert "⛔ Stop loss yok" in plain
    assert "⚠️ Giriş piyasadan uzak" in plain
    assert "⛔ Giriş" not in plain
    assert "❔ 2232.85 güncel fiyat olabilir" in plain
    assert "Güncel fiyat: 2232.85" in plain
    assert "Güven: %72" in plain
    assert "#10" in plain and "#11" in plain
    assert "⏳ 10 dk içinde yanıt gelmezse işlem açılmaz." in plain


def test_confirmation_incomplete_and_without_expiry():
    conf = make_conf(
        kind="incomplete", plan=None, action=make_action(action=ActionType.INCOMPLETE, stop_loss=None),
        expires_at=NOW + timedelta(seconds=180),
    )
    plain = assert_valid_html(f.format_confirmation(conf))
    assert "🧩 Eksik sinyal — tamamlayabilir misin?" in plain
    assert "Stop: ❓" in plain
    assert "⏳ 3 dk" in plain
    assert "Düzelt" in plain
    closed = f.format_confirmation(conf, with_expiry=False)
    assert "⏳" not in closed


def test_confirmation_uses_action_when_no_plan():
    conf = make_conf(plan=None, action=make_action(stop_loss=None))
    plain = assert_valid_html(f.format_confirmation(conf))
    assert "Yeni işlem" in plain and "ETHUSDT.P" in plain


def test_format_management_move_sl_to_entry_with_trade():
    trade = TradeRecord(
        id=7, plan=make_plan(), status=TradeStatus.OPEN, current_stop_loss=2205.11,
        current_take_profits=[TakeProfit(price=2310.6)], created_at=NOW, updated_at=NOW,
    )
    mp = ManagementPlan(
        plan_id="m1", action=ActionType.MOVE_SL, trade_id=7, move_sl_to_entry=True,
        source_chat_id=-1001, source_message_ids=[20], summary_tr="SL girişe", created_at=NOW,
    )
    plain = assert_valid_html(f.format_management(mp, trade))
    assert "Stop taşı" in plain and "#7 ETH LONG" in plain
    assert "2205.11 → girişe (başabaş) 2232.18" in plain
    assert "açık" in plain


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        (ManagementPlan(plan_id="m", action=ActionType.CLOSE, trade_id=3, close_fraction=0.5,
                        source_chat_id=1, created_at=NOW), "Kapat: %50"),
        (ManagementPlan(plan_id="m", action=ActionType.CLOSE, trade_id=3, source_chat_id=1, created_at=NOW),
         "Kapat: tamamı"),
        (ManagementPlan(plan_id="m", action=ActionType.CANCEL, trade_id=3, source_chat_id=1, created_at=NOW),
         "iptal"),
        (ManagementPlan(plan_id="m", action=ActionType.MOVE_TP, trade_id=3, source_chat_id=1, created_at=NOW,
                        take_profits=[TakeProfit(price=2400)]), "TP1: 2400"),
    ],
)
def test_format_management_kinds(action, expected):
    plain = assert_valid_html(f.format_management(action, None))
    assert expected in plain
    assert "#3" in plain


def test_manage_confirmation_expiry_wording():
    mp = ManagementPlan(plan_id="m", action=ActionType.MOVE_SL, trade_id=3, new_stop_loss=2240.0,
                        source_chat_id=1, created_at=NOW)
    conf = make_conf(kind="manage", plan=mp, action=make_action(action=ActionType.MOVE_SL, new_stop_loss=2240.0))
    plain = assert_valid_html(f.format_confirmation(conf))
    assert "2240" in plain and "uygulanmaz" in plain


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        (make_action(action=ActionType.MOVE_SL, new_stop_loss=2240.5, target_trade_id=4), "yeni stop 2240.5"),
        (make_action(action=ActionType.MOVE_SL, move_sl_to_entry=True), "girişe (başabaş)"),
        (make_action(action=ActionType.CLOSE, close_fraction=0.25), "Kapat (ETH/USDT"),
        (make_action(action=ActionType.CANCEL), "İptal"),
        (make_action(action=ActionType.NONE), "İşlem talimatı değil"),
    ],
)
def test_format_action_kinds(action, expected):
    assert expected in assert_valid_html(f.format_action(action))


def test_number_helpers():
    assert f.fmt_price(2310.6) == "2310.6"
    assert f.fmt_price(65000.0) == "65000"
    assert f.fmt_price(0.000012345) == "0.000012345"
    assert f.fmt_price(None) == "—"
    assert f.fmt_pct(2, possessive=True) == "%2'si"
    assert f.fmt_pct(1, possessive=True) == "%1'i"
    assert f.fmt_pct(0.5, possessive=True) == "%0.5'i"
    assert f.fmt_pct(10, possessive=True) == "%10'u"
    assert f.fmt_pct(100, possessive=True) == "%100'ü"
    assert f.fmt_pct(50, possessive=True) == "%50'si"
    assert f.esc("<a & b>") == "&lt;a &amp; b&gt;"
