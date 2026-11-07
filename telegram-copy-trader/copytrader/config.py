"""Configuration: one YAML file + environment variables.

``${VAR}`` and ``${VAR:-default}`` inside the YAML are replaced with
environment variables (a ``.env`` file next to the config is loaded first),
so secrets never have to live in the YAML itself.
"""

from __future__ import annotations

import os
import re
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
    max_entry_distance_pct: float = 10.0          # entry this far from market -> probably misread -> ask
    pending_entry_expiry_min: int = 1440          # unfilled limit entries are cancelled after this
    include_fees_in_risk: bool = True             # size so that loss incl. estimated fees ~= 1R
    fee_rate: float = 0.0006                      # assumed taker fee per side when the broker cannot tell
    duplicate_window_sec: int = 900               # same asset+side+similar entry within this -> ask (likely a repost)
    multi_tp_split: Literal["equal", "last_only"] = "equal"
    use_equity: bool = True                       # True: equity incl. unrealized PnL; False: free balance
    allow_min_qty_round_up: bool = False          # if 1R is below the minimum order size: False = skip, True = use min size
    max_margin_usage_pct: float = 90.0            # never lock more than this % of free balance as margin


# --------------------------------------------------------------------------- #
# Brokers
# --------------------------------------------------------------------------- #
class _BrokerBase(_Base):
    name: str
    enabled: bool = True
    symbol_map: dict[str, str] = Field(default_factory=dict)   # "ETH" -> "ETHUSD" (base asset or raw symbol -> broker symbol)
    allowed_assets: list[str] = Field(default_factory=list)     # empty = all assets
    risk_multiplier: float = 1.0                  # scale risk on this broker only (1.0 = 2% per R)


class CcxtBrokerConfig(_BrokerBase):
    type: Literal["ccxt"]
    exchange: str                                 # ccxt id: binanceusdm, bybit, okx, bitget, kucoinfutures, gate, mexc...
    api_key: str | None = None
    secret: str | None = None
    password: str | None = None                   # OKX / Bitget / KuCoin passphrase
    sandbox: bool = False                         # testnet
    market_type: Literal["swap"] = "swap"         # USDT-margined perpetuals (spot is not supported: SL/TP would lock the same coins)
    quote_currency: str = "USDT"
    margin_mode: Literal["isolated", "cross"] = "isolated"
    leverage: int = 10                            # preferred leverage (raised automatically only if margin requires it)
    max_leverage: int = 50
    position_mode: Literal["oneway", "hedge"] = "oneway"
    options: dict[str, Any] = Field(default_factory=dict)       # extra ccxt constructor options


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
    token: str
    account_id: str
    region: str | None = None
    magic: int = 770077
    slippage_points: int = 20
    commission_per_lot: float | None = None       # round-trip commission per 1 lot in account currency (risk buffer)


class PaperBrokerConfig(_BrokerBase):
    type: Literal["paper"]                        # simulated account, real prices from a public ccxt exchange
    starting_balance: float = 10_000.0
    price_source_exchange: str = "binanceusdm"
    quote_currency: str = "USDT"
    fee_rate: float = 0.0005


BrokerConfig = Annotated[
    Union[CcxtBrokerConfig, Mt5BrokerConfig, MetaApiBrokerConfig, PaperBrokerConfig],
    Field(discriminator="type"),
]


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
                raise ValueError(f"environment variable {name} is not set (referenced in config)")
            return val

        expanded = _ENV_PATTERN.sub(repl, node)
        # Keep secrets verbatim (no YAML re-parsing: "0123", "#abc", "yes" stay strings);
        # pydantic converts "123" to int where a field is numeric. An empty
        # placeholder means "not set".
        if expanded == "" and _ENV_PATTERN.fullmatch(node):
            return None
        return expanded
    return node


def load_config(path: str | os.PathLike[str] = "config.yaml") -> AppConfig:
    p = Path(path)
    _load_dotenv(p.parent / ".env")
    _load_dotenv(Path.cwd() / ".env")
    raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    return AppConfig.model_validate(_expand_env(raw))
