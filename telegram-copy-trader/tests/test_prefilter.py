from datetime import datetime, timezone

import pytest

from copytrader.models import StoredMessage
from copytrader.parsing.prefilter import looks_relevant


def msg(text: str = "", **kw) -> StoredMessage:
    return StoredMessage(chat_id=-100, message_id=1, date=datetime(2026, 1, 1, tzinfo=timezone.utc), text=text, **kw)


@pytest.mark.parametrize(
    "text",
    [
        "eth 1R long",
        "BTC 67k'ya gelirse bakarız",
        "stopu girişe çekin",
        "SOL yarısını kapattım",
        "BTC short 0.5R giriş 67250 stop 68100 tp1 66000 tp2 64800",
        "XAUUSD buy limit 2318 sl 2309 tp 2340",
        "limit gelmedi iptal",
        "emri sildim",
        "kârı aldım çıktım",
        "TP1 ✅ +2R",
        "SL to BE",
        "breakeven",
        "0,5r",
        "x20 kaldıraç",
        "10x",
        "%30 kâr aldım",
        "$SOL",
        "#eth",
        "ETHUSDT.P",
        "1000pepeusdt",
        "eurusd",
        "altın alım",
        "hedef 2310",
        "2232.18",
        "Kapattım",
        "GİRİŞ 2232",
        "longladım",
    ],
)
def test_relevant(text):
    assert looks_relevant(msg(text)) is True


@pytest.mark.parametrize(
    "text",
    [
        "günaydın 🌞",
        "bugün piyasa çok sakin",
        "harika gün",
        "",
        "   ",
        "🚀🚀🚀",
        "👍",
        "selam millet",
        "iyi akşamlar",
        "teşekkürler hocam",
        "nasılsınız",
        "GÜNAYDIN",
        "it's a nice day",
    ],
)
def test_not_relevant(text):
    assert looks_relevant(msg(text)) is False


def test_image_only_is_relevant():
    assert looks_relevant(msg("", media_type="photo", media_path="/data/media/1.jpg")) is True


def test_image_with_chatter_is_relevant():
    assert looks_relevant(msg("günaydın", media_type="image_document", media_path="/x.png")) is True


def test_media_path_without_type_is_relevant():
    assert looks_relevant(msg("", media_path="/x.jpg")) is True
