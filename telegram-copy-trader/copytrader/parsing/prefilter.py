"""Cheap pre-filter so plain chatter does not cost an LLM call.

Deliberately generous: a false positive costs one LLM call, a false negative
can miss a trade.
"""

from __future__ import annotations

import re
import unicodedata

from ..models import StoredMessage
from .symbols import ASSET_ALIASES, FIAT_CURRENCIES, KNOWN_QUOTES

# Turkish letters -> ASCII, applied before lower(); keywords below are folded. Letters with a
# decomposition (İ, ş, ğ, ü, ö, ç, â...) are also handled by NFKD + dropping combining marks, so
# text typed with combining characters ("i̇ptal") folds the same way; ı/I have no decomposition.
_FOLD = str.maketrans({
    "ı": "i", "İ": "i", "ş": "s", "Ş": "s", "ğ": "g", "Ğ": "g", "ü": "u", "Ü": "u",
    "ö": "o", "Ö": "o", "ç": "c", "Ç": "c", "â": "a", "Â": "a", "î": "i", "Î": "i",
    "û": "u", "Û": "u",
})

# Whole-word keywords (folded, lower-case).
_KEYWORDS = frozenset({
    "buy", "sell", "al", "alin", "alim", "alis", "aldim", "aldik", "aliyorum", "alalim",
    "sat", "satin", "satis", "sattim", "sattik", "satiyorum", "satalim",
    "gir", "girin", "girdim", "girdik", "giriyorum", "girelim",
    "entry", "enter", "sl", "tp", "be", "rr", "target", "limit", "market", "piyasadan",
    "spot", "perp", "futures", "vadeli", "lot", "pip", "pips", "hold",
    "kar", "kari", "karda", "setup", "vazgectim",
    "cek", "cekin", "cektim", "cektik", "cekiyorum", "cekelim",
    "cik", "cikin", "ciktim", "ciktik", "cikiyorum", "cikalim", "cikis",
    "sil", "silin", "sildim", "sildik", "doldu", "dolmadi", "dolmazsa", "dolarsa",
    # Close / partial close / exit wording ("kes": cut; "poz": short for pozisyon).
    "aldi", "alindi", "alinir", "alabilirsiniz", "alabilirsin", "alacagim", "alicam", "alacaz",
    "kes", "kesin", "kesip", "kesiniz", "kesmek", "poz", "half", "out",
})
# Keyword stems (prefix match): "stopu", "kapattım", "girişe", "longladım"...
_STEMS = (
    "long", "short", "stop", "kapat", "iptal", "giris", "hedef", "target", "entry", "breakeven",
    "cancel", "close", "risk", "pozisyon", "position", "islem", "trade", "sinyal", "signal",
    "profit", "loss", "zarar", "kaldirac", "leverage", "ekle", "trail", "likid", "liquid",
    "emir", "emri",
    # Turkish close / partial-close / exit verbs with any suffix: "çıktım", "çıkıyoruz", "kapadım",
    # "sattık", "bozdum", "bozun", "kestim", "yarıya indirdim", "realize ettim", "kâra geçtim"...
    "cik", "kapa", "sat", "boz", "kest", "kesiyor", "kesel", "kesil", "indir", "realiz",
    "gir", "cek", "vazgec", "likit", "maliyet", "ortala",
    "kari", "karda", "karla", "karin",
    # Amounts typical of management messages: "yarısını", "yarıya", "kalanı", "hepsini", "tamamını".
    "yari", "kalan", "heps", "tamami",
    # English exits / partials, TP/SL with suffixes ("tp2ye", "tp'yi" is split at the apostrophe).
    "exit", "partial", "secure", "booked", "tp",
    # Signal-group status words: "Aktif" (limit filled), "Update geçiyoruz", "güncelleme", "manuel kapattık".
    "aktif", "update", "guncelle", "manuel",
)
# Words that start with a stem but are plain chatter.
_STEM_EXCEPTIONS = ("yarin",)          # "yarın" (tomorrow), "yarına"...

# Short management wording that names no instrument, level or order word. Stems that are trading talk wherever
# they stand (prefix match on folded words): "Patladı" / "Patladık" (stopped out), "Başabaş yaptık", "Sıfırladık"
# (stop to entry), "Tetiklendi" (the entry triggered), "Geçersiz" (setup invalid), "Kilitledik" (profit locked),
# "Azalttık" / "Küçülttük" (reduced), "Taşıdık" (stop / target moved), "filled", "Düzeltme" (a correction of the
# signal).
_STATUS_STEMS = (
    "patla", "basabas", "sifirla", "tetikle", "gecersiz", "kilitle", "azalt", "kucult",
    "tasidi", "tasiyor", "tasiyin", "tasiyal", "tasindi", "fill", "duzelt",
)
# Two-word forms: "Başa baş" (both words exact), "Tersine döndük" / "tersine dönüyoruz" (second word by prefix).
_STATUS_PAIRS = (("basa", "bas", True), ("tersine", "don", False))
# Everyday words that are a status report only when they are (nearly) the whole message: "Geldi", "Tamamdır",
# "Açtık", "Açıldı", "Yedik", "Bitti", "Tuttu", "Olmadı", "Devam", "Bekliyoruz", "Bırakın", "Dokunmayın", "Tut",
# "Yanlış oldu", "done", "hit". Inside a longer sentence ("kargo geldi mi", "tamamdır hocam") they stay chatter.
_LONE_STATUS_WORDS = frozenset({"tamamdir", "olmadi", "devam", "done", "hit", "pardon"})
_LONE_STATUS_STEMS = (
    "geldi", "gelmedi", "acti", "acil", "yedi", "bitti", "bitir", "tut", "bekl", "birak", "dokunma", "yanlis",
)
# Words that may stand next to a status word without making the message a sentence about something else.
_LONE_STATUS_FILLERS = frozenset({
    "oldu", "olduk", "yaptik", "yaptim", "ettik", "ettim", "arkadaslar", "beyler", "simdi", "yine", "tekrar",
    "artik", "sonunda", "nihayet", "hala", "de", "da", "ve",
})
_LONE_STATUS_MAX_WORDS = 4
# A message made of emoji only is a status report when it carries one of these (check marks, target, cross,
# stop sign): "✅", "🎯".
_STATUS_MARKS = frozenset("✅✔☑🎯❌✖🛑⛔")

# Common tickers / instrument names (folded, lower-case), beyond the alias table.
_TICKERS = frozenset({
    "btc", "eth", "sol", "xrp", "bnb", "ada", "doge", "dot", "avax", "link", "ltc", "bch", "trx",
    "ton", "matic", "pol", "atom", "near", "apt", "arb", "op", "sui", "sei", "inj", "tia",
    "pepe", "shib", "wif", "bonk", "floki", "fet", "rndr", "render", "ftm", "etc", "fil", "icp",
    "uni", "aave", "mkr", "ldo", "crv", "xlm", "xmr", "hbar", "algo", "vet", "eos", "kas", "ena",
    "jup", "wld", "ordi", "stx", "rune", "imx", "gala", "sand", "mana", "axs", "ape", "dydx",
    "gmx", "pendle", "ondo", "trump", "hype", "tao", "virtual", "bera",
    "xau", "xag", "usoil", "ukoil", "nas100", "us30", "us500", "spx", "dax", "ger40",
}) | {a.lower() for a in ASSET_ALIASES}

_FIAT_LOWER = frozenset(c.lower() for c in FIAT_CURRENCIES)
_PAIR_QUOTES = tuple(sorted({q.lower() for q in KNOWN_QUOTES} | _FIAT_LOWER, key=len, reverse=True))
# Any word ending in these is pair-like even with an unknown base ("wifusdt", "1000pepeusdt").
_USD_QUOTES = ("fdusd", "usdt", "usdc", "busd", "usd")
# All-caps words that are not tickers.
_CAPS_STOPWORDS = frozenset({
    "TAMAM", "SELAM", "EVET", "HAYIR", "ABD", "LOL", "OMG", "WOW", "YES", "THE", "AND", "FOR",
})

_WORD = re.compile(r"[a-z0-9]+")
_TICKER_MARK = re.compile(r"[$#][a-z][a-z0-9]{0,14}")
_R_MULTIPLE = re.compile(r"(?<![a-z0-9])\d+(?:[.,]\d+)?\s?r(?![a-z0-9])")
_LEVERAGE = re.compile(r"(?<![a-z0-9])(?:\d{1,3}\s?x|x\s?\d{1,3})(?![a-z0-9])")
_PRICE = re.compile(r"\d+[.,]\d+|\d{3,}|(?<![a-z0-9])\d+k(?![a-z0-9])")
_TP_SL = re.compile(r"(?:tp|sl)\d{0,2}")
_CAPS_WORD = re.compile(r"(?<!\w)[A-Z][A-Z0-9]{2,11}(?!\w)")


def _fold(text: str) -> str:
    text = unicodedata.normalize("NFKD", text.translate(_FOLD))
    return "".join(c for c in text if not unicodedata.combining(c)).lower()


# Turkish case/possessive suffixes written straight onto a ticker: "ethi", "btcyi", "solden", "ethde".
_TICKER_SUFFIXES = tuple(sorted({
    "i", "u", "e", "a", "yi", "yu", "ye", "ya", "de", "da", "te", "ta", "den", "dan", "ten", "tan",
    "in", "un", "nin", "nun", "le", "la", "yle", "yla", "ler", "lar",
}, key=len, reverse=True))


def _has_stem(word: str) -> bool:
    return word.startswith(_STEMS) and not word.startswith(_STEM_EXCEPTIONS)


def _is_ticker_like(word: str) -> bool:
    if word in _TICKERS:
        return True
    if any(word.endswith(sfx) and len(word) - len(sfx) >= 3 and word[: -len(sfx)] in _TICKERS
           for sfx in _TICKER_SUFFIXES):
        return True  # ethi, btcyi, solden
    if len(word) == 6 and word[:3] in _FIAT_LOWER and word[3:] in _FIAT_LOWER:
        return True  # eurusd
    if any(word.endswith(q) and word[: -len(q)] in _TICKERS for q in _PAIR_QUOTES):
        return True  # ethbtc, xaueur, soltry
    return any(word.endswith(q) and len(word) > len(q) + 1 for q in _USD_QUOTES)  # wifusdt


def _has_status_pair(words: list[str]) -> bool:
    return any(a == first and (b == second if exact else b.startswith(second))
               for a, b in zip(words, words[1:]) for first, second, exact in _STATUS_PAIRS)


def _is_lone_status(raw: str, words: list[str]) -> bool:
    """A short status report without any trading word: "Geldi", "Geldi ✅", "Tamamdır", "Yanlış oldu", or an
    emoji-only message with a check mark / target ("✅", "🎯"). Every word must be a status word or a filler
    ("oldu", "arkadaşlar"...), so the same words inside a sentence about something else do not count."""
    if not words:
        return any(c in _STATUS_MARKS for c in raw)
    if len(words) > _LONE_STATUS_MAX_WORDS:
        return False

    def status(word: str) -> bool:
        return word in _LONE_STATUS_WORDS or word.startswith(_LONE_STATUS_STEMS)

    return any(status(w) for w in words) and all(status(w) or w in _LONE_STATUS_FILLERS for w in words)


def _has_trading_content(text: str, words: list[str]) -> bool:
    """``text`` (folded) names a trading word, a level, an instrument or a management status word."""
    if "%" in text or _R_MULTIPLE.search(text) or _LEVERAGE.search(text):
        return True
    if _TICKER_MARK.search(text) or _PRICE.search(text):
        return True
    if "kara gec" in text:     # "kâra geçti" (in profit) -> may be followed by a stop move
        return True
    for word in words:
        if word in _KEYWORDS or _has_stem(word) or _TP_SL.fullmatch(word):
            return True
        if _is_ticker_like(word):
            return True
        if word.startswith(_STATUS_STEMS):
            return True
    return _has_status_pair(words)


def looks_relevant(msg: StoredMessage) -> bool:
    """True if the message might contain or modify a trade.

    Always True for images. For text: True if it contains any trading keyword
    (Turkish or English: long, short, buy, sell, al, sat, giriş, entry, stop,
    sl, tp, hedef, target, kapat, close, iptal, cancel, be, breakeven, R
    multiples like "1R"/"0.5r", "%", leverage like "10x"), a ticker-looking
    token (ETH, BTCUSDT, XAUUSD, $SOL, #eth), a price-looking number, a
    management status word ("Patladık", "Başa baş", "Tetiklendi", "Geçersiz",
    "filled") or when the whole message is a short status report ("Geldi",
    "Tamamdır", "Bitti ✅", a lone "✅" / "🎯"). False for short greetings /
    other emoji-only / empty messages and for chatter that merely contains an
    everyday status word ("tamamdır hocam", "kargo geldi mi"). Prefer false
    positives over false negatives.
    """
    if msg.media_path or msg.media_type:
        return True
    raw = msg.text or ""
    text = _fold(raw)
    if not text.strip():
        return False
    words = _WORD.findall(text)
    if _has_trading_content(text, words) or _is_lone_status(raw, words):
        return True
    return any(w not in _CAPS_STOPWORDS for w in _CAPS_WORD.findall(raw))


def is_bare_status(msg: StoredMessage) -> bool:
    """True for a text-only message that is relevant ONLY as a short status report: an everyday word or a
    mark with no trading word, level or instrument ("Geldi", "Tamamdır", "Bitti ✅", a lone "✅"). Such a
    message does not say what happened (entry filled? target? stop?), so nothing should be done on it without
    asking. False for "Stop", "TP geldi", "Başa baş", "Aktif" and for every message with a picture."""
    if msg.media_path or msg.media_type:
        return False
    raw = msg.text or ""
    text = _fold(raw)
    if not text.strip():
        return False
    words = _WORD.findall(text)
    return _is_lone_status(raw, words) and not _has_trading_content(text, words)
