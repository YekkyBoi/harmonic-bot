"""Symbol normalization: how a trader writes an instrument -> (base, quote, market type).

Pure string logic, no I/O. Brokers map the canonical base asset to their own
instrument names (see ``Broker.resolve_symbol``).
"""

from __future__ import annotations

import re

from ..models import MarketType

#: Common names -> canonical base asset. Upper-case, Turkish-folded keys (Ü->U, Ş->S...).
ASSET_ALIASES: dict[str, str] = {
    # metals
    "XAU": "XAU", "GOLD": "XAU", "ALTIN": "XAU", "ONSALTIN": "XAU",
    "XAG": "XAG", "SILVER": "XAG", "GUMUS": "XAG",
    "XPT": "XPT", "PLATINUM": "XPT", "PLATIN": "XPT",
    "XPD": "XPD", "PALLADIUM": "XPD", "PALADYUM": "XPD",
    # crypto full names
    "BITCOIN": "BTC", "XBT": "BTC",
    "ETHEREUM": "ETH", "ETHER": "ETH",
    "SOLANA": "SOL", "RIPPLE": "XRP", "CARDANO": "ADA", "DOGECOIN": "DOGE",
    "LITECOIN": "LTC", "POLKADOT": "DOT", "AVALANCHE": "AVAX", "CHAINLINK": "LINK",
    "TONCOIN": "TON",
    # indices
    "NAS100": "NAS100", "NASDAQ": "NAS100", "NASDAQ100": "NAS100", "US100": "NAS100",
    "USTEC": "NAS100", "USTECH": "NAS100", "NDX": "NAS100",
    "US30": "US30", "DOW": "US30", "DOWJONES": "US30", "DJI": "US30", "DJ30": "US30", "WS30": "US30",
    "US500": "US500", "SPX": "US500", "SP500": "US500", "SPX500": "US500", "S&P500": "US500",
    "GER40": "GER40", "DAX": "GER40", "DE40": "GER40", "GER30": "GER40", "DE30": "GER40",
    "UK100": "UK100", "FTSE": "UK100", "FTSE100": "UK100",
    "JP225": "JP225", "JPN225": "JP225", "NIKKEI": "JP225", "NIKKEI225": "JP225",
    # energy
    "USOIL": "USOIL", "OIL": "USOIL", "WTI": "USOIL", "XTI": "USOIL", "CRUDE": "USOIL",
    "CRUDEOIL": "USOIL", "PETROL": "USOIL",
    "UKOIL": "UKOIL", "BRENT": "UKOIL", "XBR": "UKOIL",
}

#: Quote currencies recognised as a symbol suffix, longest first.
KNOWN_QUOTES: tuple[str, ...] = ("FDUSD", "USDT", "USDC", "BUSD", "USD", "EUR", "TRY", "BTC", "ETH")

#: ISO currencies that make up forex pairs.
FIAT_CURRENCIES: frozenset[str] = frozenset({
    "USD", "EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD", "SEK", "NOK", "DKK", "PLN",
    "HUF", "CZK", "TRY", "ZAR", "MXN", "SGD", "HKD", "CNH", "CNY", "RUB", "ILS", "THB",
    "INR", "KRW", "BRL",
})

#: CFD assets (metals, indices, energy) -> currency they are priced in.
CFD_QUOTES: dict[str, str] = {
    "XAU": "USD", "XAG": "USD", "XPT": "USD", "XPD": "USD",
    "NAS100": "USD", "US30": "USD", "US500": "USD", "USOIL": "USD", "UKOIL": "USD",
    "GER40": "EUR", "UK100": "GBP", "JP225": "JPY",
}

_METALS = frozenset({"XAU", "XAG", "XPT", "XPD"})

# Assets whose ticker merely ends with a quote code: never split ("WBTC" is not W/BTC).
_NO_SPLIT = frozenset({
    "WBTC", "TBTC", "CBBTC", "WETH", "STETH", "WSTETH", "RETH", "CBETH", "BETH", "EETH",
    "WEETH", "TUSD", "PYUSD", "LUSD", "SUSD", "GUSD",
})

# Perpetual-futures suffixes (checked in order; longer variants first).
_PERP_SUFFIXES: tuple[str, ...] = (".PERP", "-PERP", "_PERP", "PERP", ".P", "-SWAP", "_SWAP")

_TR_FOLD = str.maketrans({
    "Ü": "U", "Ş": "S", "Ğ": "G", "Ç": "C", "Ö": "O", "İ": "I", "Â": "A", "Î": "I", "Û": "U",
})
_SEPARATORS = re.compile(r"[/\-_]")


def _clean(raw: str) -> str:
    """Upper-case and drop whitespace, '#' and '$'."""
    return re.sub(r"[\s#$]+", "", raw).upper()


def canonical_asset(name: str) -> str:
    """'gold' -> 'XAU', 'Bitcoin' -> 'BTC', 'eth' -> 'ETH'. Unknown names are upper-cased."""
    cleaned = _clean(name)
    return ASSET_ALIASES.get(cleaned.translate(_TR_FOLD), cleaned)


def _strip_perp_suffix(s: str) -> tuple[str, bool]:
    for suffix in _PERP_SUFFIXES:
        if s.endswith(suffix) and len(s) > len(suffix):
            return s[: -len(suffix)], True
    return s, False


def _split_pair(s: str) -> tuple[str, str | None]:
    """Split a cleaned, suffix-free symbol into (base, quote)."""
    parts = _SEPARATORS.split(s)
    if len(parts) >= 2 and all(parts[:2]):
        return parts[0], parts[1]  # explicit "BTC/USDT", "ETH-USDT", "NAS100_USD"
    s = "".join(parts)
    if s.translate(_TR_FOLD) in ASSET_ALIASES:
        return s, None
    if len(s) == 6 and s[3:] in FIAT_CURRENCIES and (s[:3] in FIAT_CURRENCIES or s[:3] in _METALS):
        return s[:3], s[3:]  # EURUSD, USDJPY, XAUUSD, XAGEUR
    if s in KNOWN_QUOTES or s in _NO_SPLIT:
        return s, None
    for quote in KNOWN_QUOTES:  # longest first
        if s.endswith(quote) and len(s) > len(quote):
            return s[: -len(quote)], quote
    return s, None


def normalize_symbol(raw: str) -> tuple[str | None, str | None, MarketType | None]:
    """Split a symbol as written in a signal into (base, quote, market_type).

    Examples:
      "ETHUSDT.P"      -> ("ETH", "USDT", PERP)     # TradingView perpetual notation
      "BINANCE:ETHUSDT.P" -> ("ETH", "USDT", PERP)
      "BTC/USDT:USDT"  -> ("BTC", "USDT", PERP)     # ccxt swap notation
      "BTCUSDT"        -> ("BTC", "USDT", None)
      "1000PEPEUSDT.P" -> ("1000PEPE", "USDT", PERP)
      "XAUUSD", "GOLD" -> ("XAU", "USD", CFD)
      "EURUSD"         -> ("EUR", "USD", FOREX)
      "ETH", "#eth"    -> ("ETH", None, None)
      ""               -> (None, None, None)
    """
    s = _clean(raw or "")
    if not s:
        return None, None, None

    perp = False
    settle: str | None = None
    # "BINANCE:ETHUSDT.P" (exchange prefix) vs "BTC/USDT:USDT" (ccxt settle currency).
    while ":" in s:
        left, _, right = s.partition(":")
        if not right:
            s = left
            break
        if not left:
            s = right
            continue
        if "/" in left:
            s, settle = left, right
            perp = "-" not in right  # "USDT-241227" = dated future, not a perp
            break
        s = right

    s, suffix_perp = _strip_perp_suffix(s)
    perp = perp or suffix_perp

    base, quote = _split_pair(s)
    base = canonical_asset(base)
    if quote is None and settle and "-" not in settle:
        quote = settle
    if not base:
        return None, None, None

    market_type: MarketType | None = None
    if perp:
        market_type = MarketType.PERP
    elif base in FIAT_CURRENCIES and quote in FIAT_CURRENCIES and base != quote:
        market_type = MarketType.FOREX
    elif base in CFD_QUOTES and (quote is None or quote in FIAT_CURRENCIES):
        quote = quote or CFD_QUOTES[base]
        market_type = MarketType.CFD
    return base, quote, market_type
