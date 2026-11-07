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
)
# Words that start with a stem but are plain chatter.
_STEM_EXCEPTIONS = ("yarin",)          # "yarın" (tomorrow), "yarına"...

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


def looks_relevant(msg: StoredMessage) -> bool:
    """True if the message might contain or modify a trade.

    Always True for images. For text: True if it contains any trading keyword
    (Turkish or English: long, short, buy, sell, al, sat, giriş, entry, stop,
    sl, tp, hedef, target, kapat, close, iptal, cancel, be, breakeven, R
    multiples like "1R"/"0.5r", "%", leverage like "10x"), a ticker-looking
    token (ETH, BTCUSDT, XAUUSD, $SOL, #eth) or a price-looking number. False for
    short greetings / emoji-only / empty messages. Prefer false positives over
    false negatives.
    """
    if msg.media_path or msg.media_type:
        return True
    raw = msg.text or ""
    text = _fold(raw)
    if not text.strip():
        return False
    if "%" in text or _R_MULTIPLE.search(text) or _LEVERAGE.search(text):
        return True
    if _TICKER_MARK.search(text) or _PRICE.search(text):
        return True
    if "kara gec" in text:     # "kâra geçti" (in profit) -> may be followed by a stop move
        return True
    for word in _WORD.findall(text):
        if word in _KEYWORDS or _has_stem(word) or _TP_SL.fullmatch(word):
            return True
        if _is_ticker_like(word):
            return True
    return any(w not in _CAPS_STOPWORDS for w in _CAPS_WORD.findall(raw))
