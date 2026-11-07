import pytest

from copytrader.models import MarketType
from copytrader.parsing.symbols import ASSET_ALIASES, canonical_asset, normalize_symbol

PERP, CFD, FOREX = MarketType.PERP, MarketType.CFD, MarketType.FOREX


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # docstring contract
        ("ETHUSDT.P", ("ETH", "USDT", PERP)),
        ("BINANCE:ETHUSDT.P", ("ETH", "USDT", PERP)),
        ("BTC/USDT:USDT", ("BTC", "USDT", PERP)),
        ("BTCUSDT", ("BTC", "USDT", None)),
        ("1000PEPEUSDT.P", ("1000PEPE", "USDT", PERP)),
        ("XAUUSD", ("XAU", "USD", CFD)),
        ("GOLD", ("XAU", "USD", CFD)),
        ("EURUSD", ("EUR", "USD", FOREX)),
        ("ETH", ("ETH", None, None)),
        ("#eth", ("ETH", None, None)),
        ("", (None, None, None)),
        ("   ", (None, None, None)),
        # prefixes / suffixes / separators
        ("BYBIT:BTCUSDT.P", ("BTC", "USDT", PERP)),
        ("OANDA:XAUUSD", ("XAU", "USD", CFD)),
        ("TVC:GOLD", ("XAU", "USD", CFD)),
        ("BTC-PERP", ("BTC", None, PERP)),
        ("ETHUSDT-PERP", ("ETH", "USDT", PERP)),
        ("ETHUSDT_PERP", ("ETH", "USDT", PERP)),
        ("ETHUSDTPERP", ("ETH", "USDT", PERP)),
        ("BTCUSD_PERP", ("BTC", "USD", PERP)),
        ("ETH-USDT-SWAP", ("ETH", "USDT", PERP)),
        ("BTC/USDT", ("BTC", "USDT", None)),
        ("eth / usdt", ("ETH", "USDT", None)),
        ("$SOL", ("SOL", None, None)),
        ("#SOLUSDT", ("SOL", "USDT", None)),
        ("btc/usd:btc", ("BTC", "USD", PERP)),          # ccxt inverse swap
        ("BTC/USDT:USDT-241227", ("BTC", "USDT", None)),  # dated future, not a perp
        # quotes: longest suffix first, never an empty base
        ("ETHFDUSD", ("ETH", "FDUSD", None)),
        ("BTCUSDC", ("BTC", "USDC", None)),
        ("BNBBUSD", ("BNB", "BUSD", None)),
        ("ETHBTC", ("ETH", "BTC", None)),
        ("SOLTRY", ("SOL", "TRY", None)),
        ("ETHEUR", ("ETH", "EUR", None)),
        ("BTC", ("BTC", None, None)),
        ("USDT", ("USDT", None, None)),
        ("BUSD", ("BUSD", None, None)),
        ("FDUSD", ("FDUSD", None, None)),
        ("WBTC", ("WBTC", None, None)),
        ("STETH", ("STETH", None, None)),
        ("PERP", ("PERP", None, None)),
        ("PERPUSDT", ("PERP", "USDT", None)),
        # forex
        ("USDJPY", ("USD", "JPY", FOREX)),
        ("GBPUSD", ("GBP", "USD", FOREX)),
        ("USDTRY", ("USD", "TRY", FOREX)),
        ("EUR/USD", ("EUR", "USD", FOREX)),
        # CFDs: metals, indices, oil
        ("XAGUSD", ("XAG", "USD", CFD)),
        ("XAUEUR", ("XAU", "EUR", CFD)),
        ("altın", ("XAU", "USD", CFD)),
        ("Gümüş", ("XAG", "USD", CFD)),
        ("SILVER", ("XAG", "USD", CFD)),
        ("NASDAQ", ("NAS100", "USD", CFD)),
        ("US100", ("NAS100", "USD", CFD)),
        ("USTEC", ("NAS100", "USD", CFD)),
        ("NAS100_USD", ("NAS100", "USD", CFD)),
        ("US30", ("US30", "USD", CFD)),
        ("DOW", ("US30", "USD", CFD)),
        ("SPX", ("US500", "USD", CFD)),
        ("SPX500USD", ("US500", "USD", CFD)),
        ("WTI", ("USOIL", "USD", CFD)),
        ("WTIUSD", ("USOIL", "USD", CFD)),
        ("DAX", ("GER40", "EUR", CFD)),
        # metal on a crypto exchange: not a CFD
        ("XAUUSDT", ("XAU", "USDT", None)),
        ("XAUUSDT.P", ("XAU", "USDT", PERP)),
        # crypto aliases
        ("BITCOIN", ("BTC", None, None)),
        ("XBTUSD", ("BTC", "USD", None)),
        ("ethereum", ("ETH", None, None)),
        # unknown dotted suffix is kept (BTC dominance is not BTC)
        ("BTC.D", ("BTC.D", None, None)),
    ],
)
def test_normalize_symbol(raw, expected):
    assert normalize_symbol(raw) == expected


def test_exchange_prefix_with_quote_like_symbol():
    # "BINANCE:BTC" must not be read as base BINANCE / settle BTC
    assert normalize_symbol("BINANCE:BTC") == ("BTC", None, None)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("gold", "XAU"),
        ("GOLD", "XAU"),
        ("altın", "XAU"),
        ("ALTIN", "XAU"),
        ("xau", "XAU"),
        ("silver", "XAG"),
        ("gümüş", "XAG"),
        ("GUMUS", "XAG"),
        ("Bitcoin", "BTC"),
        ("ethereum", "ETH"),
        ("eth", "ETH"),
        ("#sol", "SOL"),
        ("$sol", "SOL"),
        (" btc ", "BTC"),
        ("nasdaq", "NAS100"),
        ("NAS100", "NAS100"),
        ("us100", "NAS100"),
        ("ustec", "NAS100"),
        ("dow", "US30"),
        ("sp500", "US500"),
        ("us500", "US500"),
        ("oil", "USOIL"),
        ("wti", "USOIL"),
        ("1000pepe", "1000PEPE"),
        ("SomethingNew", "SOMETHINGNEW"),
    ],
)
def test_canonical_asset(name, expected):
    assert canonical_asset(name) == expected


def test_alias_table_is_canonical():
    # every alias target maps to itself, keys are upper-case
    for key, target in ASSET_ALIASES.items():
        assert key == key.upper()
        assert canonical_asset(target) == target
