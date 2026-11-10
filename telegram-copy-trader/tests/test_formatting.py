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


# --------------------------------------------------------------------------- #
# The question on top of a confirmation (what the owner is asked, in one line)
# --------------------------------------------------------------------------- #
def manage_plan(action: ActionType, summary: str = "#1 BTC SHORT: değişiklik", **kw) -> ManagementPlan:
    return ManagementPlan(plan_id="m", action=action, trade_id=1, source_chat_id=1, created_at=NOW,
                          summary_tr=summary, **kw)


def manage_conf(plan: ManagementPlan, **kw) -> PendingConfirmation:
    data = dict(kind="manage", plan=plan, market_price=73638.75,
                action=make_action(action=plan.action, symbol=None, base_asset="BTC", side=Side.SHORT,
                                   entry_price=None, stop_loss=None, take_profits=[], risk_r=None, confidence=0.6))
    data.update(kw)
    return make_conf(**data)


@pytest.mark.parametrize(
    ("plan", "question"),
    [
        (manage_plan(ActionType.CLOSE, close_fraction=1.0), "#1 BTC SHORT pozisyonunu KAPATAYIM MI?"),
        (manage_plan(ActionType.CLOSE), "#1 BTC SHORT pozisyonunu KAPATAYIM MI?"),
        (manage_plan(ActionType.CLOSE, close_fraction=0.5), "#1 BTC SHORT pozisyonunun %50'sini KAPATAYIM MI?"),
        (manage_plan(ActionType.CLOSE, close_fraction=0.3), "#1 BTC SHORT pozisyonunun %30'unu KAPATAYIM MI?"),
        (manage_plan(ActionType.CLOSE, close_fraction=0.25), "#1 BTC SHORT pozisyonunun %25'ini KAPATAYIM MI?"),
        (manage_plan(ActionType.MOVE_SL, new_stop_loss=74868.8), "#1 BTC SHORT stopunu 74868.8 yapayım mı?"),
        (manage_plan(ActionType.MOVE_SL, new_stop_loss=74868.8, move_sl_to_entry=True),
         "#1 BTC SHORT stopunu girişe (74868.8) çekeyim mi?"),
        (manage_plan(ActionType.MOVE_SL, move_sl_to_entry=True), "#1 BTC SHORT stopunu girişe çekeyim mi?"),
        (manage_plan(ActionType.MOVE_TP, take_profits=[TakeProfit(price=73095)]),
         "#1 BTC SHORT TP'sini 73095 yapayım mı?"),
        (manage_plan(ActionType.MOVE_TP, take_profits=[TakeProfit(price=73448.2, close_fraction=0.5),
                                                       TakeProfit(price=73095, close_fraction=0.5)]),
         "#1 BTC SHORT TP'lerini 73448.2 / 73095 yapayım mı?"),
        (manage_plan(ActionType.CANCEL), "#1 BTC SHORT bekleyen emrini iptal edeyim mi?"),
    ],
)
def test_manage_confirmation_starts_with_the_question_and_the_button_hint(plan, question):
    text = f.format_confirmation(manage_conf(plan))
    lines = text.split("\n")
    assert lines[0] == f"❓ <b>{question}</b>"
    assert lines[1] == "✅ Onayla = uygula · ❌ İptal = dokunma" == f.BUTTON_HINT_MANAGE
    assert lines[2] == "" and lines[3] == f.HEADER_UNSURE            # then the details, as before
    assert f"🔗 İşlem: #{plan.trade_id}" in text and "uygulanmaz" in text
    assert assert_valid_html(text).startswith(f"❓ {question}\n")
    assert f.confirmation_question(manage_conf(plan)) == question


def test_incident_close_confirmation_reads_as_a_question():
    plan = manage_plan(ActionType.CLOSE, "#1 BTC SHORT: pozisyonun tamamı kapatılacak", close_fraction=1.0,
                       source_message_ids=[20129])
    conf = manage_conf(plan, source_message_ids=[20129], issues=[
        ValidationIssue(code="low_confidence", message_tr="Güven düşük (%60)."),
        ValidationIssue(code="ambiguity", message_tr="Mesaj bir yanıt değil; işlem #1'e asset eşleşmesiyle bağlandı."),
    ])
    plain = assert_valid_html(f.format_confirmation(conf))
    assert plain.split("\n")[:3] == ["❓ #1 BTC SHORT pozisyonunu KAPATAYIM MI?",
                                     "✅ Onayla = uygula · ❌ İptal = dokunma", ""]
    for kept in ("Emin olamadım — şunu anladım, doğru mu?", "🔒 Kapat: tamamı", "🔗 İşlem: #1",
                 "📝 #1 BTC SHORT: pozisyonun tamamı kapatılacak", "Güncel fiyat: 73638.75", "Güven: %60",
                 "⚠️ Güven düşük (%60).", "Kaynak mesaj: #20129", "⏳ 10 dk içinde yanıt gelmezse bu değişiklik"):
        assert kept in plain, kept


def test_cancel_of_a_filled_trade_asks_about_closing_the_open_position():
    conf = manage_conf(manage_plan(ActionType.CANCEL), issues=[
        ValidationIssue(code="cancel_filled_trade", message_tr="#1 BTC SHORT girişi zaten dolmuş")])
    assert f.format_confirmation(conf).split("\n")[0] == (
        "❓ <b>#1 BTC SHORT işlemini iptal edip AÇIK POZİSYONU KAPATAYIM MI?</b>")


def test_open_confirmation_starts_with_the_question_and_the_button_hint():
    conf = make_conf(plan=make_plan(base_asset="btc", side=Side.SHORT, entry_type=EntryType.LIMIT,
                                    entry_price=74868.8, stop_loss=75575.0,
                                    take_profits=[TakeProfit(price=73095.0)]))
    text = f.format_confirmation(conf)
    lines = text.split("\n")
    assert lines[0] == "❓ <b>BTC SHORT işlemini AÇAYIM MI?</b>"
    assert lines[1] == "✅ Onayla = aç · ❌ İptal = açma" == f.BUTTON_HINT_OPEN
    assert lines[2] == "" and lines[3] == f.HEADER_UNSURE
    assert "işlem açılmaz" in text
    assert f.format_confirmation(make_conf()).split("\n")[0] == "❓ <b>ETH LONG işlemini AÇAYIM MI?</b>"


def test_closed_confirmation_keeps_the_question_but_not_the_button_hint():
    conf = manage_conf(manage_plan(ActionType.CLOSE, close_fraction=1.0))
    closed = f.format_confirmation(conf, with_expiry=False)
    assert closed.split("\n")[:3] == ["❓ <b>#1 BTC SHORT pozisyonunu KAPATAYIM MI?</b>", "", f.HEADER_UNSURE]
    assert "Onayla" not in closed and "⏳" not in closed
    closed_open = f.format_confirmation(make_conf(), with_expiry=False)
    assert closed_open.startswith("❓ <b>ETH LONG işlemini AÇAYIM MI?</b>\n\n") and "Onayla" not in closed_open


def test_no_question_when_there_is_nothing_to_approve():
    # No plan (blocking issue): only Düzelt / İptal buttons exist, so no "Onayla = ..." line either.
    blocked = make_conf(plan=None, action=make_action(stop_loss=None))
    text = f.format_confirmation(blocked)
    assert text.startswith(f.HEADER_UNSURE) and "❓ <b>" not in text and "Onayla =" not in text
    assert f.confirmation_question(blocked) is None
    # A manage question without a plan asks which trade is meant (answered with Düzelt, no Onayla).
    blocked_manage = make_conf(kind="manage", plan=None, action=make_action(action=ActionType.CLOSE))
    text = f.format_confirmation(blocked_manage)
    assert text.startswith("❓ <b>Hangi işlemden bahsediliyor?") and "Onayla =" not in text
    assert f.HEADER_UNSURE in text
    incomplete = make_conf(kind="incomplete", plan=None, action=make_action(action=ActionType.INCOMPLETE))
    assert f.format_confirmation(incomplete).startswith(f.HEADER_INCOMPLETE)
    assert f.confirmation_question(incomplete) is None


def test_question_label_comes_from_the_plan_not_from_the_parsers_reading():
    # The parser read ETH, the plan targets trade #1 BTC SHORT: the question names the real trade.
    plan = manage_plan(ActionType.CLOSE, "#1 BTC SHORT: pozisyonun tamamı kapatılacak", close_fraction=1.0)
    conf = manage_conf(plan, action=make_action(action=ActionType.CLOSE, base_asset="ETH", side=Side.LONG))
    assert f.confirmation_question(conf) == "#1 BTC SHORT pozisyonunu KAPATAYIM MI?"
    # A summary without the trade's label (or with another trade's): only the id is certain.
    for summary in ("", "pozisyon kapatılacak", "#2 ETH LONG: pozisyonun tamamı kapatılacak", "#1 BTC SHORTY: x"):
        plain_plan = manage_plan(ActionType.CLOSE, summary, close_fraction=1.0)
        assert f.confirmation_question(manage_conf(plain_plan)) == "#1 pozisyonunu KAPATAYIM MI?", summary


def test_question_escapes_untrusted_asset_names():
    conf = manage_conf(manage_plan(ActionType.CLOSE, "#1 A&B<i> SHORT: x", close_fraction=1.0))
    text = f.format_confirmation(conf)
    assert text.split("\n")[0] == "❓ <b>#1 A&amp;B&lt;i&gt; SHORT pozisyonunu KAPATAYIM MI?</b>"
    assert assert_valid_html(text).startswith("❓ #1 A&B<i> SHORT pozisyonunu KAPATAYIM MI?")
    opened = f.format_confirmation(make_conf(plan=make_plan(base_asset="<eth>")))
    assert opened.split("\n")[0] == "❓ <b>&lt;ETH&gt; LONG işlemini AÇAYIM MI?</b>"
    assert_valid_html(opened)


def test_question_uses_the_label_the_validator_writes_into_the_plan():
    from copytrader.parsing import validator

    trade = TradeRecord(
        id=7, plan=make_plan(), status=TradeStatus.OPEN, current_stop_loss=2205.11,
        current_take_profits=[TakeProfit(price=2310.6)], created_at=NOW, updated_at=NOW,
    )

    def question(action: ParsedAction, market: float = 2250.0) -> str:
        plan, issues = validator.build_management_plan(
            action, trade=trade, candidates=[trade], market_price=market, source_chat_id=-1001, now=NOW)
        assert plan is not None, issues
        conf = make_conf(kind="manage", plan=plan, action=action, issues=issues)
        return f.format_confirmation(conf).split("\n")[0]

    close = ParsedAction(action=ActionType.CLOSE, close_fraction=1.0, target_trade_id=7, confidence=0.6)
    assert question(close) == "❓ <b>#7 ETH LONG pozisyonunu KAPATAYIM MI?</b>"
    to_entry = ParsedAction(action=ActionType.MOVE_SL, move_sl_to_entry=True, target_trade_id=7, confidence=0.6)
    assert question(to_entry) == "❓ <b>#7 ETH LONG stopunu girişe (2232.18) çekeyim mi?</b>"
    cancel = ParsedAction(action=ActionType.CANCEL, target_trade_id=7, confidence=0.6)
    assert question(cancel) == "❓ <b>#7 ETH LONG işlemini iptal edip AÇIK POZİSYONU KAPATAYIM MI?</b>"
