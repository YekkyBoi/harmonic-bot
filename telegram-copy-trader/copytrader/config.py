"""Configuration: one YAML file + environment variables.

``${VAR}`` and ``${VAR:-default}`` inside the YAML are replaced with
environment variables (a ``.env`` file next to the config is loaded first),
so secrets never have to live in the YAML itself.
"""

from __future__ import annotations

import os
import re
import unicodedata
from pathlib import Path
from typing import Annotated, Any, Literal, Union
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid")


# --------------------------------------------------------------------------- #
# Telegram (user account = "userbot") and notification bot
# --------------------------------------------------------------------------- #
_TR_FOLD = str.maketrans({"İ": "i", "I": "ı", "Ş": "ş", "Ğ": "ğ", "Ü": "ü", "Ö": "ö", "Ç": "ç"})


def fold_tr(text: str) -> str:
    """Case-insensitive comparison key that respects Turkish letters ('İŞLEMLERİ' == 'işlemleri')."""
    return " ".join(text.translate(_TR_FOLD).casefold().split())


_ASCII_I = str.maketrans({"ı": "i", "İ": "i"})    # the two letters NFKD does not turn into a plain "i"


def fold_ascii(text: str) -> str:
    """Comparison key for text typed WITHOUT Turkish letters ('Islemleri' == 'İşlemleri' == 'ISLEMLERI'):
    compatibility-decomposed, accents and dots dropped, dotless / dotted i -> i, case-folded. Also equal for
    letters written as a base letter plus a combining mark."""
    plain = "".join(ch for ch in unicodedata.normalize("NFKD", text.translate(_ASCII_I))
                    if not unicodedata.combining(ch))
    return " ".join(plain.casefold().split())


class TopicRoute(_Base):
    """Which forum topics of the source group to follow, and where their trades go.

    Topic titles change every month ("Kasım Ayı Kripto İşlemleri" -> "Aralık Ayı Kripto İşlemleri"),
    so a topic is matched by text contained in its title, not by id."""

    name: str                                     # short label, e.g. "kripto" / "fx"
    title_contains: list[str] = Field(min_length=1)  # topic matches if its title contains ANY of these (Turkish-insensitive)
    brokers: list[str] = Field(default_factory=list)  # broker names to trade this topic on; empty = all enabled brokers
    kind: Literal["crypto", "fx", "any"] = "any"  # hint for reading symbols (fx: "EUR" means EURUSD)
    # Per-route overrides of the global risk.* values (percent of price). Unset: kind "fx" uses the FX defaults
    # below (a crypto-sized 0.5% is ~54 pips on EURUSD), other kinds use the global values.
    market_entry_tolerance_pct: float | None = Field(None, gt=0)  # auto: market only if price is this close to entry
    max_entry_distance_pct: float | None = Field(None, gt=0)      # entry farther than this from market -> ask / skip
    duplicate_tolerance_pct: float | None = Field(None, gt=0)     # repost check: entries this close are "the same"
    # Per-route overrides of risk.market_entry_tolerance_r / risk.explicit_market_drift. Unset: kind "fx" uses
    # FX_MARKET_ENTRY_TOLERANCE_R and "follow", other kinds use the global values.
    market_entry_tolerance_r: float | None = Field(None, gt=0)    # also market within this fraction of the entry-to-stop distance
    explicit_market_drift: Literal["ask", "follow"] | None = None  # explicit MARKET signal outside the tolerance: ask / follow


FX_MARKET_ENTRY_TOLERANCE_PCT = 0.03     # ~3 pips on EURUSD
FX_MARKET_ENTRY_TOLERANCE_R = 0.25       # ... or within a quarter of the signal's own entry-to-stop distance
FX_MAX_ENTRY_DISTANCE_PCT = 1.0
FX_DUPLICATE_TOLERANCE_PCT = 0.05
DEFAULT_DUPLICATE_TOLERANCE_PCT = 0.5


def route_thresholds(risk: "RiskConfig", route: "TopicRoute | None") -> tuple[float, float, float]:
    """(market_entry_tolerance_pct, max_entry_distance_pct, duplicate_tolerance_pct) effective for ``route``."""
    fx = route is not None and route.kind == "fx"
    def pick(own: float | None, fx_default: float, global_value: float) -> float:
        if route is not None and own is not None:
            return own
        return fx_default if fx else global_value
    return (
        pick(route.market_entry_tolerance_pct if route else None, FX_MARKET_ENTRY_TOLERANCE_PCT,
             risk.market_entry_tolerance_pct),
        pick(route.max_entry_distance_pct if route else None, FX_MAX_ENTRY_DISTANCE_PCT,
             risk.max_entry_distance_pct),
        pick(route.duplicate_tolerance_pct if route else None, FX_DUPLICATE_TOLERANCE_PCT,
             DEFAULT_DUPLICATE_TOLERANCE_PCT),
    )


def effective_risk(risk: "RiskConfig", route: "TopicRoute | None") -> "RiskConfig":
    """``risk`` with the route's entry settings applied (the global object when nothing differs).

    Percent thresholds: see ``route_thresholds``. ``market_entry_tolerance_r`` / ``explicit_market_drift``: the
    route's own value wins; otherwise kind "fx" uses FX_MARKET_ENTRY_TOLERANCE_R / "follow" and every other
    kind the global values."""
    tol, dist, _dup = route_thresholds(risk, route)
    fx = route is not None and route.kind == "fx"
    tol_r, drift = risk.market_entry_tolerance_r, risk.explicit_market_drift
    if route is not None and route.market_entry_tolerance_r is not None:
        tol_r = route.market_entry_tolerance_r
    elif fx:
        tol_r = FX_MARKET_ENTRY_TOLERANCE_R
    if route is not None and route.explicit_market_drift is not None:
        drift = route.explicit_market_drift
    elif fx:
        drift = "follow"
    update = {
        "market_entry_tolerance_pct": tol, "max_entry_distance_pct": dist,
        "ask_if_limit_marketable": risk.ask_if_limit_marketable and not fx,
        "market_entry_tolerance_r": tol_r, "explicit_market_drift": drift,
    }
    if all(getattr(risk, name) == value for name, value in update.items()):
        return risk
    return risk.model_copy(update=update)


def match_topic(routes: list[TopicRoute], title: str | None) -> TopicRoute | None:
    """First route whose title_contains matches ``title`` (None if no title or no match). A part matches when
    the title contains it on the Turkish key (:func:`fold_tr`) or on the ASCII key (:func:`fold_ascii`): a
    topic created from a keyboard without Turkish letters ("Aralik Ayi Kripto Islemleri") is still followed."""
    if not title:
        return None
    key, plain = fold_tr(title), fold_ascii(title)
    for route in routes:
        if any(fold_tr(part) in key or fold_ascii(part) in plain for part in route.title_contains if part.strip()):
            return route
    return None


class TelegramConfig(_Base):
    api_id: int                                   # from https://my.telegram.org
    api_hash: str
    session_path: str = "data/telegram_user"      # Telethon session file (without .session)
    session_string: str | None = None             # alternative: Telethon StringSession (for containers)
    source_chats: list[int | str]                 # group/channel ids (-100...) or @usernames / t.me links
    allowed_sender_ids: list[int] = Field(default_factory=list)  # empty = any sender in source chats
    debounce_sec: float = 4.0                     # wait this long after a sender's last message before parsing
    context_window_sec: int = 300                 # unconsumed messages this recent are given to the parser together
    incomplete_timeout_sec: int = 180             # INCOMPLETE signal not completed in time -> ask the owner
    max_context_messages: int = 12
    catchup_max_age_sec: int = 120                # on restart, messages missed while offline and younger than this are processed
    topics: list[TopicRoute] = Field(default_factory=list)  # forum groups: follow only these topics; empty = every message


class NotifierConfig(_Base):
    bot_token: str                                # from @BotFather; the bot DMs the owner
    owner_user_id: int                            # your own Telegram user id; only this user can control the bot
    confirm_timeout_sec: int = 600                # unanswered confirmation expires -> trade is NOT taken
    notify_on_execution: bool = False             # True: also report every executed trade (errors are always reported)
    notify_on_ignored: bool = False               # True: report messages that were understood as non-trading


# --------------------------------------------------------------------------- #
# LLM signal parser
# --------------------------------------------------------------------------- #
class LLMConfig(_Base):
    model: str = "claude-opus-5-5"
    effort: Literal["low", "medium", "high", "xhigh", "max"] = "high"
    max_tokens: int = 16000
    use_fallbacks: bool = True                    # server-side refusal fallback (beta)
    min_confidence: float = 0.85                  # below this -> ask the owner
    max_images: int = 4
    timeout_sec: float = 120.0
    prefilter: bool = True                        # skip the LLM for text-only chatter with no trading words/numbers
    api_key: str | None = None                    # default: ANTHROPIC_API_KEY env var


# --------------------------------------------------------------------------- #
# Risk / execution policy
# --------------------------------------------------------------------------- #
class RiskConfig(_Base):
    risk_per_r_pct: float = 2.0                   # 1R = 2% of equity lost if SL is hit
    default_r: float = 1.0                        # used when a signal does not state R
    ask_if_r_missing: bool = False                # True: ask instead of using default_r
    max_r_per_trade: float = 2.0                  # signals above this -> ask
    max_open_trades: int = 10
    daily_loss_limit_pct: float = 10.0            # per broker; stop opening new trades for the day after this realized loss
    entry_mode: Literal["auto", "market", "limit"] = "auto"
    market_entry_tolerance_pct: float = 0.5       # auto: |price-entry|/entry <= this -> market, else limit at entry
    # also enter at market when the live price is within this fraction of the entry-to-stop distance from the
    # entry (None = off)
    market_entry_tolerance_r: float | None = Field(None, gt=0)
    # explicit MARKET signal whose live price is outside the tolerance: "ask" the owner (old behaviour) or
    # "follow" = treat it like a signal without entry type
    explicit_market_drift: Literal["ask", "follow"] = "ask"
    max_entry_distance_pct: float = 10.0          # entry this far from market -> probably misread -> ask
    ask_if_limit_marketable: bool = True          # a limit entry already at/through the market: ask (False for fx routes: the broker opens it at market)
    pending_entry_expiry_min: int = 1440          # unfilled limit entries are cancelled after this
    include_fees_in_risk: bool = True             # size so that loss incl. estimated fees ~= 1R
    fee_rate: float = 0.0006                      # assumed taker fee per side when the broker cannot tell
    duplicate_window_sec: int = 900               # same asset+side+similar entry within this -> ask (likely a repost)
    multi_tp_split: Literal["equal", "last_only"] = "equal"
    use_equity: bool = True                       # True: equity incl. unrealized PnL; False: free balance
    allow_min_qty_round_up: bool = False          # if 1R is below the minimum order size: False = skip, True = use min size
    max_margin_usage_pct: float = 90.0            # never lock more than this % of free balance as margin
    # never size a position larger than this many times the size implied by the signal's own entry-to-stop
    # distance (applied by the engine and the MetaTrader adapters)
    max_size_multiple: float = Field(2.0, ge=1)
    # Plausible entry-to-stop distance in percent of the entry, per kind of instrument (crypto; currency pairs;
    # "cfd" = metals, indices, oil). A stop closer or farther than this was probably misread: the owner is asked
    # (parsing/validator.py: stop_too_tight / stop_too_wide). 0 switches that bound off.
    min_stop_distance_pct_crypto: float = Field(0.15, ge=0)
    max_stop_distance_pct_crypto: float = Field(25.0, ge=0)
    min_stop_distance_pct_fx: float = Field(0.03, ge=0)      # about 3.5 pips on EURUSD
    max_stop_distance_pct_fx: float = Field(3.0, ge=0)
    min_stop_distance_pct_cfd: float = Field(0.05, ge=0)
    max_stop_distance_pct_cfd: float = Field(8.0, ge=0)


# --------------------------------------------------------------------------- #
# Brokers
# --------------------------------------------------------------------------- #
class _BrokerBase(_Base):
    name: str
    enabled: bool = True
    # optional: a broker that is not essential (e.g. a friend's account). If it cannot connect (bad token, service
    # down...) the bot still starts, warns the owner once, skips its trades and retries in the background.
    optional: bool = False
    symbol_map: dict[str, str] = Field(default_factory=dict)   # "ETH" -> "ETHUSD" (base asset or raw symbol -> broker symbol)
    allowed_assets: list[str] = Field(default_factory=list)     # empty = all assets
    risk_multiplier: float = 1.0                  # scale risk on this broker only (1.0 = 2% per R)
    risk_usd: float | None = Field(None, gt=0)    # fixed money at risk for 1R on this broker (instead of a % of equity)
    no_hedge: bool = False                        # never open the opposite direction of an active position on the same instrument (broker symbol; prop firms / netting)
    daily_loss_limit_pct: float | None = Field(None, ge=0)  # replaces risk.daily_loss_limit_pct for this broker (0 = off)
    # True (prop accounts): the daily limit also counts what is still at risk. A new trade is refused when
    # today's realized loss + the loss of this broker's open and pending positions at their current stops + the
    # new trade's own risk would be more than the limit. False: only the realized loss counts (a day can then
    # end near twice the limit).
    daily_loss_include_open_risk: bool = False
    # Price-offset brokers (CFD price differs from the exchange the signal charts come from): None = off.
    reference_exchange: str | None = None         # ccxt id of the chart's exchange, e.g. "binanceusdm"
    offset_mode: Literal["additive", "ratio"] = "additive"  # shift levels by (broker - ref) or scale by broker / ref
    max_offset_pct: float = Field(1.0, gt=0)      # broker vs reference price differing more than this % -> skip the trade
    # LIMIT entries on an offset broker: "broker_limit" = resting limit order on the broker (default);
    # "reference_touch" = no broker order: the bot waits for the reference exchange to touch the chart level,
    # then measures the offset right then and opens a MARKET order with the shifted SL/TPs.
    entry_trigger: Literal["broker_limit", "reference_touch"] = "broker_limit"
    virtual_trigger_max_hours: float = Field(48.0, gt=0)   # a virtual entry not triggered within this is cancelled
    # How the sized quantity is rounded to the broker's lot / quantity step: "down" (never more than the target
    # risk; the default, every account of the owner) or "nearest" (0.125 -> 0.13), falling back to "down" when the
    # nearest step would lose more than LOT_ROUNDING_MAX_OVERSHOOT times the target at the stop.
    lot_rounding: Literal["down", "nearest"] = "down"

    @model_validator(mode="after")
    def _touch_needs_reference(self) -> "_BrokerBase":
        if self.entry_trigger == "reference_touch" and not self.reference_exchange:
            raise ValueError(f"broker '{self.name}': entry_trigger: reference_touch için reference_exchange gerekli")
        return self


class CcxtBrokerConfig(_BrokerBase):
    type: Literal["ccxt"]
    exchange: str                                 # ccxt id: binanceusdm, bybit, okx, bitget, kucoinfutures, gate, mexc...
    api_key: str | None = None
    secret: str | None = None
    password: str | None = None                   # OKX / Bitget / KuCoin passphrase
    sandbox: bool = False                         # testnet
    demo: bool = False                            # exchange "demo trading" (Binance: demo.binance.com keys, fake balance)
    market_type: Literal["swap"] = "swap"         # USDT-margined perpetuals (spot is not supported: SL/TP would lock the same coins)
    quote_currency: str = "USDT"
    margin_mode: Literal["isolated", "cross"] = "isolated"
    leverage: int = 10                            # preferred leverage (raised automatically only if margin requires it)
    max_leverage: int = 50
    position_mode: Literal["oneway", "hedge"] = "oneway"
    options: dict[str, Any] = Field(default_factory=dict)       # extra ccxt constructor options


    @model_validator(mode="after")
    def _demo_or_sandbox(self) -> "CcxtBrokerConfig":
        if self.demo and self.sandbox:
            raise ValueError(f"broker '{self.name}': demo ve sandbox aynı anda açık olamaz; birini seçin")
        return self


class Mt5BrokerConfig(_BrokerBase):
    type: Literal["mt5"]                          # native MetaTrader5 terminal (Windows only)
    login: int
    password: str
    server: str
    terminal_path: str | None = None
    magic: int = 770077
    deviation_points: int = 20
    filling: Literal["auto", "fok", "ioc", "return"] = "auto"
    commission_per_lot: float | None = None       # round-trip commission per 1 lot in account currency (risk buffer)


class MetaApiBrokerConfig(_BrokerBase):
    type: Literal["metaapi"]                      # MT4/MT5 via metaapi.cloud (works on Linux/Docker)
    token: str | None = None                      # required unless optional (an optional broker may start without it)
    account_id: str | None = None
    region: str | None = None
    magic: int = 770077
    slippage_points: int = 20
    commission_per_lot: float | None = None       # round-trip commission per 1 lot in account currency (risk buffer)
    # Settings of type "mt5_bridge", kept in the block so that switching between the two types is the one line
    # "type:". Not used by MetaApi.
    terminal_path: str | None = None
    broker_company: str | None = None
    deviation_points: int | None = None
    filling: Literal["auto", "fok", "ioc", "return"] | None = None

    @model_validator(mode="after")
    def _credentials(self) -> "MetaApiBrokerConfig":
        if not self.optional and not (self.token and self.account_id):
            raise ValueError(f"broker '{self.name}': token ve account_id gerekli")
        return self


class Mt5BridgeBrokerConfig(_BrokerBase):
    """A second MetaTrader 5 terminal on the same Windows computer (e.g. the friend's Tickmill account), driven by the
    same native adapter as type "mt5" but in a helper process of its own (copytrader/brokers/mt5_bridge.py): the
    MetaTrader5 package can talk to ONE terminal per process, and the bot process already uses it for type "mt5".

    The account is logged in once in that terminal's own window (the password is typed only there); the helper
    attaches to the logged-in terminal at ``terminal_path`` and refuses any other terminal, a terminal whose account
    server / company does not contain ``broker_company``, and (when given, or once seen) another login."""

    type: Literal["mt5_bridge"]
    terminal_path: str                            # full path of that terminal's terminal64.exe
    broker_company: str | None = None             # e.g. "Tickmill": must appear in the account's server or company
    login: int | None = None                      # optional: the account number the terminal must be logged in to
    server: str | None = None                     # optional: the exact trade server name
    password: str | None = None                   # normally empty: the terminal keeps its own log-in
    magic: int = 770077
    deviation_points: int = 20
    filling: Literal["auto", "fok", "ioc", "return"] = "auto"
    commission_per_lot: float | None = None       # round-trip commission per 1 lot in account currency (risk buffer)
    # Settings of type "metaapi", kept in the block so that switching back is the one line "type:". Not used here.
    token: str | None = None
    account_id: str | None = None
    region: str | None = None
    slippage_points: int | None = None

    @model_validator(mode="after")
    def _bridge_identity(self) -> "Mt5BridgeBrokerConfig":
        if not str(self.terminal_path or "").strip():
            raise ValueError(f"broker '{self.name}': terminal_path gerekli (terminal64.exe dosyasının tam yolu)")
        if not (self.broker_company or "").strip() and self.login is None:
            raise ValueError(f"broker '{self.name}': broker_company (örn. Tickmill) ya da login gerekli")
        return self


class PaperBrokerConfig(_BrokerBase):
    type: Literal["paper"]                        # simulated account, real prices from a public ccxt exchange
    starting_balance: float = 10_000.0
    price_source_exchange: str = "binanceusdm"
    quote_currency: str = "USDT"
    fee_rate: float = 0.0005


class ManualBrokerConfig(_BrokerBase):
    """No orders: the owner gets a Telegram message (entry / stop / TP / quantity) and trades by hand.

    Risk is a fixed amount per R (``risk_usd``), not a % of equity. Positions are tracked virtually
    with real prices so later "close" / "stop to entry" messages are forwarded too."""

    type: Literal["manual"]
    risk_usd: float | None = Field(50.0, gt=0)   # money at risk for 1R
    qty_decimals: int | None = None              # round the quantity down to this many decimals (None = exchange step)
    price_source_exchange: str = "binanceusdm"   # public prices for checks and virtual tracking
    quote_currency: str = "USDT"
    starting_balance: float = 1_000_000.0        # virtual, only for tracking
    fee_rate: float = 0.0


BrokerConfig = Annotated[
    Union[CcxtBrokerConfig, Mt5BrokerConfig, Mt5BridgeBrokerConfig, MetaApiBrokerConfig, PaperBrokerConfig,
          ManualBrokerConfig],
    Field(discriminator="type"),
]


def terminal_key(path: str | None) -> str:
    """Comparison key of a MetaTrader terminal: the FOLDER of its terminal64.exe (a path to the folder itself
    counts as that folder), case-folded, with Windows or POSIX separators and without trailing separators."""
    text = str(path or "").strip().strip('"').replace("/", "\\").rstrip("\\")
    if text.lower().endswith(".exe"):
        text = text.rsplit("\\", 1)[0] if "\\" in text else ""
    return text.rstrip("\\").casefold()


# --------------------------------------------------------------------------- #
# Root
# --------------------------------------------------------------------------- #
class AppConfig(_Base):
    dry_run: bool = True                          # True: everything runs, but NO real orders are sent
    data_dir: str = "data"
    log_level: str = "INFO"
    timezone: str = "Europe/Istanbul"
    monitor_interval_sec: float = 10.0
    price_source_exchange: str = "binanceusdm"    # public market data for validation when no broker can quote
    telegram: TelegramConfig
    notifier: NotifierConfig
    llm: LLMConfig = Field(default_factory=LLMConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    brokers: list[BrokerConfig] = Field(default_factory=list)

    @field_validator("timezone")
    @classmethod
    def _valid_timezone(cls, v: str) -> str:
        # A typo here would only surface at execution time (daily-loss guard) and fail every trade.
        name = (v or "").strip()
        try:
            if not name:
                raise ValueError(name)
            ZoneInfo(name)
        except (ZoneInfoNotFoundError, ValueError, TypeError, OSError):
            raise ValueError(
                f"geçersiz saat dilimi (timezone): {v!r}. IANA adı yazın, örneğin 'Europe/Istanbul' veya 'UTC'."
            ) from None
        return name

    @field_validator("brokers")
    @classmethod
    def _unique_names(cls, v: list[Any]) -> list[Any]:
        names = [b.name for b in v]
        if len(names) != len(set(names)):
            raise ValueError("broker names must be unique")
        return v

    @model_validator(mode="after")
    def _sanity(self) -> "AppConfig":
        if not 0 < self.risk.risk_per_r_pct <= 10:
            raise ValueError("risk.risk_per_r_pct must be in (0, 10]")
        names = {b.name for b in self.brokers}
        own_paths = {terminal_key(b.terminal_path): b.name for b in self.brokers
                     if b.type == "mt5" and b.enabled and getattr(b, "terminal_path", None)}
        for b in self.brokers:
            if b.type == "mt5_bridge" and b.enabled and terminal_key(b.terminal_path) in own_paths:
                raise ValueError(
                    f"broker '{b.name}': terminal_path, '{own_paths[terminal_key(b.terminal_path)]}' hesabının "
                    "terminaliyle aynı; her hesap kendi MetaTrader terminalini kullanmalı")
        for route in self.telegram.topics:
            unknown = [b for b in route.brokers if b not in names]
            if unknown:
                raise ValueError(
                    f"telegram.topics '{route.name}': bilinmeyen broker adı {unknown} (brokers listesindeki 'name' ile aynı olmalı)"
                )
        return self

    @property
    def enabled_brokers(self) -> list[Any]:
        return [b for b in self.brokers if b.enabled]


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _load_dotenv(path: Path) -> None:
    """Minimal .env loader (KEY=VALUE lines). Existing env vars win."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip().removeprefix("export ").strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _expand_env(node: Any) -> Any:
    if isinstance(node, dict):
        return {k: _expand_env(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_expand_env(v) for v in node]
    if isinstance(node, str):
        def repl(m: re.Match[str]) -> str:
            name, default = m.group(1), m.group(2)
            val = os.environ.get(name)
            if val is None or val == "":
                if default is not None:
                    return default
                raise ValueError(f"ortam değişkeni {name} ayarlanmamış (.env dosyasına ekleyin; config.yaml bu değişkeni kullanıyor)")
            return val

        expanded = _ENV_PATTERN.sub(repl, node)
        # Keep secrets verbatim (no YAML re-parsing: "0123", "#abc", "yes" stay strings);
        # pydantic converts "123" to int where a field is numeric. An empty
        # placeholder means "not set".
        if expanded == "" and _ENV_PATTERN.fullmatch(node):
            return None
        return expanded
    return node


def _beside(folder: Path, value: str) -> str:
    """``value`` (a path from the config) as it is meant: a relative path is relative to ``folder``, the
    folder of the config file. Returned exactly as written: absolute paths, "~" paths, and everything when
    the config file is given without a folder ("config.yaml": the folder is the current one anyway)."""
    if folder == Path("."):
        return value
    try:
        path = Path(value).expanduser()
    except RuntimeError:            # "~" without a home directory: left to its users, as before
        return value
    return value if path.is_absolute() else str(folder / path)


def load_config(path: str | os.PathLike[str] = "config.yaml") -> AppConfig:
    """Load and validate the config file. A relative ``data_dir`` / ``telegram.session_path`` is resolved
    against the folder of the config file, not against the folder the program was started from: the same
    database, Telegram session and logs are used wherever the bot is started (a scheduled task without a
    "Start in" folder, a shortcut). Started in the bot folder with the default ``config.yaml`` nothing changes
    ("data" stays "data")."""
    p = Path(path)
    _load_dotenv(p.parent / ".env")
    _load_dotenv(Path.cwd() / ".env")
    raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    config = AppConfig.model_validate(_expand_env(raw))
    config.data_dir = _beside(p.parent, config.data_dir)
    config.telegram.session_path = _beside(p.parent, config.telegram.session_path)
    return config
