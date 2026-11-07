"""Regression tests for audited prefilter defect F7 (Turkish close / partial-close phrasings dropped)."""

from datetime import datetime, timezone

import pytest

from copytrader.models import StoredMessage
from copytrader.parsing.prefilter import looks_relevant


def msg(text: str) -> StoredMessage:
    return StoredMessage(chat_id=-100, message_id=1, date=datetime(2026, 1, 1, tzinfo=timezone.utc), text=text)


@pytest.mark.parametrize(
    "text",
    [
        # from the audit
        "yarısını realize ettim", "kalanı da realize ettim", "realize edin arkadaşlar", "ethi bozdum",
        "hepsini bozdum", "bozun", "solu da kestim", "yarıya indirdim", "kalanını tuttum",
        # task list, with suffixes and Turkish casing
        "çıktım", "ÇIKTIM", "çıkıyoruz", "kâr aldım", "KÂR ALDIM", "karı al", "kârı alın", "yarısını sat",
        "YARISINI SAT", "pozisyonu kapat", "POZİSYONU KAPAT", "stop oldu", "STOP OLDU", "iptal", "İPTAL",
        "İptal ettim", "i̇ptal",                       # i + combining dot above
        # more everyday management wording
        "kapadım", "kapayın", "sattım", "hepsini kapatın", "tamamını kapattım", "kâra geçtik",
        "maliyetine çektim", "vazgeçtik", "likit oldum", "tp2ye geldi", "btcyi kapattım", "solden çıktım",
        "zararına kestim", "yarıya indirin", "yarım kapattım", "poz kapattım", "i'm out", "exit now",
        "partial close", "booked profits",
    ],
)
def test_f7_close_and_partial_close_phrasings_reach_the_parser(text: str) -> None:
    assert looks_relevant(msg(text)) is True


@pytest.mark.parametrize(
    "text",
    [
        "yarın görüşürüz", "yarına kadar bekleyin", "tamamdır hocam", "tamam", "eyvallah", "hayırlı cumalar",
        "çok güzel analiz", "ellerine sağlık", "hepinize iyi geceler", "merhaba arkadaşlar", "slm arkadaşlar",
        "kolay gelsin", "geçmiş olsun",
    ],
)
def test_f7_plain_chatter_still_skips_the_parser(text: str) -> None:
    assert looks_relevant(msg(text)) is False
