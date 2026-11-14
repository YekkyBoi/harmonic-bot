"""``python -m copytrader migrate``: bring an existing config.yaml up to date for this version.

Only the settings listed in :func:`plan_changes` are touched, each only when it is not already as wanted.

How the file is written
-----------------------
1. Text edit (the normal case): the file is edited as TEXT at the positions PyYAML reports for the nodes, so
   comments, key order, quoting, line endings and ``${VAR}`` placeholders stay byte for byte. An existing
   value is replaced in place, a missing key becomes a new line after the last line of its section.
2. Verification: the old and the new text are parsed again; the ONLY differences must be the planned keys
   (values, types and key order are compared) and every ``${...}`` placeholder must still be there.
3. Fallback: when the text edit is not possible (unusual layout) or does not verify, the file is rewritten
   through PyYAML instead (same values and key order, placeholders kept; comments and the original layout are
   lost) and verified the same way.
4. ``config.yaml.yedek-<timestamp>`` is written before the file is replaced. The new file must load through
   the app's real loader (with the variables of ``.env``) and show the wanted values there; otherwise the old
   content is put back and :class:`MigrationError` is raised.

Placeholders are never expanded and nothing is read from the environment into the file.
"""

from __future__ import annotations

import json
import os
import re
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from .config import AppConfig, NotifierConfig, RiskConfig, TopicRoute, load_config
from .errors import ConfigError

MAX_OPEN_TRADES = 25              # risk.max_open_trades is raised to this when it is lower
BINANCE_BROKER = "binance"        # broker names of the owner's config
FX_BROKER = "fx"
TICKMILL_BROKER = "tickmill"      # the friend's account
TICKMILL_COMPANY = "Tickmill"
# The friend's two requests the owner approved: 1R 45 -> 50 USD (only from exactly 45, and only together with the
# first lot_rounding of the block: a later manual 45 stays), and sizes rounded to the NEAREST lot step.
TICKMILL_OLD_RISK_USD, TICKMILL_NEW_RISK_USD = 45, 50
# Manual override of the Tickmill terminal search (also set by ``guncelle.py --tickmill-terminal``): the path of its
# terminal64.exe (or of its folder), or one of TERMINAL_OFF to stay on MetaApi.
TICKMILL_TERMINAL_ENV = "COPYTRADER_TICKMILL_TERMINAL"
TERMINAL_OFF = frozenset({"yok", "off", "metaapi", "kapali", "kapalı", "hayir", "hayır", "no"})
TERMINAL_EXE = "terminal64.exe"

_PLACEHOLDER = re.compile(r"\$\{[^}]*\}")
_PLAIN_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_YAML_WORDS = frozenset({"true", "false", "yes", "no", "on", "off", "null", "y", "n"})
_MISSING = object()


class MigrationError(ConfigError):
    """The config could not be migrated; the file on disk is the old one. ``str(err)`` is Turkish."""


class _TextEditUnsupported(Exception):
    """The text-level edit cannot be done reliably for this file (the PyYAML rewrite is used instead)."""


@dataclass
class Change:
    path: tuple[str | int, ...]            # the mapping that gets the key: keys / list indexes from the root
    key: str
    value: Any                             # bool | int | str
    text: str                              # Turkish line for the owner
    check: Callable[[AppConfig], bool]     # is the wanted value effective in the loaded config?


# --------------------------------------------------------------------------- #
# What to change
# --------------------------------------------------------------------------- #
def _is_placeholder(value: Any) -> bool:
    return isinstance(value, str) and "${" in value


def _knows(model: Any, key: str) -> bool:
    return key in (getattr(model, "model_fields", None) or {})


def _broker(config: AppConfig, name: str) -> Any:
    return next((b for b in config.brokers if b.name == name), None)


def _is_zero(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value == 0


# --------------------------------------------------------------------------- #
# The Tickmill MetaTrader 5 terminal on this computer
# --------------------------------------------------------------------------- #
def program_files_roots(env: Any = None) -> list[Path]:
    """The Program Files folders of this computer (64- and 32-bit), from the environment; on Windows also the
    usual places when the environment does not name them. Nothing elsewhere."""
    env = os.environ if env is None else env
    found: list[str] = [str(env[n]) for n in ("ProgramW6432", "ProgramFiles", "ProgramFiles(x86)") if env.get(n)]
    if os.name == "nt":
        found += [r"C:\Program Files", r"C:\Program Files (x86)"]
    out: list[Path] = []
    seen: set[str] = set()
    for item in found:
        key = os.path.normcase(os.path.normpath(item))
        if key not in seen:
            seen.add(key)
            out.append(Path(item))
    return out


def find_tickmill_terminals(roots: list[Path]) -> list[Path]:
    """Every terminal64.exe in a folder of ``roots`` whose name contains "Tickmill" (any case), directly in it or
    one folder below it ("Tickmill MT5 Terminal\\terminal64.exe", "Tickmill\\MT5\\terminal64.exe")."""
    out: list[Path] = []
    seen: set[str] = set()

    def add(exe: Path) -> None:
        key = os.path.normcase(str(exe))
        try:
            ok = exe.is_file()
        except OSError:
            ok = False
        if ok and key not in seen:
            seen.add(key)
            out.append(exe)

    for root in roots:
        try:
            subs = sorted(root.iterdir())
        except OSError:
            continue
        for sub in subs:
            try:
                if "tickmill" not in sub.name.casefold() or not sub.is_dir():
                    continue
                add(sub / TERMINAL_EXE)
                for inner in sorted(sub.iterdir()):
                    if inner.is_dir():
                        add(inner / TERMINAL_EXE)
            except OSError:
                continue
    return out


def tickmill_terminal(env: Any = None, roots: list[Path] | None = None) -> tuple[str | None, str | None]:
    """(path of the Tickmill terminal64.exe to switch to, None) or (None, one Turkish line why tickmill stays on
    MetaApi). The override (TICKMILL_TERMINAL_ENV) wins over the search."""
    env = os.environ if env is None else env
    stay = "tickmill MetaApi ile çalışmaya devam ediyor"
    how = (f"terminal başka bir yerdeyse güncellemeyi 'python guncelle.py --tickmill-terminal "
           f"\"<terminal64.exe yolu>\"' ile ya da {TICKMILL_TERMINAL_ENV} ortam değişkeniyle yapın")
    override = str(env.get(TICKMILL_TERMINAL_ENV) or "").strip().strip('"').strip()
    if override:
        if override.casefold() in TERMINAL_OFF:
            return None, f"{TICKMILL_BROKER}: MT5 terminaline geçiş {TICKMILL_TERMINAL_ENV}={override} ile kapatıldı; {stay}."
        exe = Path(override)
        if exe.is_dir():
            exe = exe / TERMINAL_EXE
        if exe.is_file() and exe.name.casefold() == TERMINAL_EXE:
            return str(exe), None
        return None, f"{TICKMILL_BROKER}: belirtilen Tickmill terminali bulunamadı ({override}); {stay}."
    found = find_tickmill_terminals(program_files_roots(env) if roots is None else roots)
    if len(found) == 1:
        return str(found[0]), None
    if not found:
        return None, (f"{TICKMILL_BROKER}: bu bilgisayarda Tickmill MT5 terminali bulunamadı (Program Files içinde adında "
                      f"Tickmill geçen klasörde {TERMINAL_EXE} yok); {stay}; {how}.")
    names = ", ".join(str(f) for f in found)
    return None, (f"{TICKMILL_BROKER}: birden fazla Tickmill MT5 terminali bulundu ({names}); hangisinin kullanılacağı "
                  f"bilinmediği için {stay}; {how}.")


def _other_accounts_terminal(path: str, config: AppConfig) -> str | None:
    """The name of the OTHER account whose terminal ``path`` is (its configured terminal_path, or the terminal it
    was last connected to: its pin in the data folder), else None."""
    from .brokers.mt5_broker import read_pins
    from .config import terminal_key

    key = terminal_key(path)
    for b in config.brokers:
        if b.name != TICKMILL_BROKER and getattr(b, "terminal_path", None) and terminal_key(b.terminal_path) == key:
            return b.name
    for name, rec in read_pins(str(config.data_dir)).items():
        if name != TICKMILL_BROKER and rec.get("folder") and terminal_key(str(rec["folder"])) == key:
            return name
    return None


def _same_number(value: Any, number: float) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and float(value) == float(number)


def plan_changes(raw: dict[str, Any], config: AppConfig, *,
                 terminal: tuple[str | None, str | None] | None = None) -> tuple[list[Change], list[str]]:
    """(changes, notes) for the raw YAML data ``raw`` whose loaded form is ``config``.

    A key the installed config model does not know, or one whose present value is a ``${VAR}`` placeholder,
    is skipped with a note (never written). ``terminal``: the result of :func:`tickmill_terminal` (looked for on
    this computer when None, and only when the tickmill block can switch)."""
    changes: list[Change] = []
    notes: list[str] = []

    def setting(path: tuple[str | int, ...], section: Any, model: Any, key: str, value: Any, text: str,
                check: Callable[[AppConfig], bool], label: str, *, wanted: Callable[[Any], bool]) -> None:
        if not _knows(model, key):
            notes.append(f"bu sürüm '{key}' ayarını tanımıyor; {label} için atlandı")
            return
        current = section.get(key, _MISSING) if isinstance(section, dict) else _MISSING
        if _is_placeholder(current):
            notes.append(f"{label}: '{key}' değeri bir ortam değişkeninden okunuyor; dokunulmadı")
            return
        if current is not _MISSING and wanted(current):
            return
        changes.append(Change(path, key, value, text, check))

    brokers = raw.get("brokers") if isinstance(raw.get("brokers"), list) else []

    def broker_index(name: str) -> int | None:
        return next((i for i, b in enumerate(brokers) if isinstance(b, dict) and str(b.get("name")) == name), None)

    # 1. Binance: no daily brake (the owner's decision), and no opposite trade on a coin that has an open one
    #    (the account is in one-way mode: the exchange would net the two; the engine skips the signal and says so).
    i = broker_index(BINANCE_BROKER)
    if i is None or _broker(config, BINANCE_BROKER) is None:
        notes.append(f"'{BINANCE_BROKER}' adlı broker yok; günlük zarar freni ve no_hedge ayarı atlandı")
    else:
        setting(("brokers", i), brokers[i], type(_broker(config, BINANCE_BROKER)), "daily_loss_limit_pct", 0,
                f"{BINANCE_BROKER}: günlük zarar freni kapatıldı (daily_loss_limit_pct: 0)",
                lambda c: _broker(c, BINANCE_BROKER).daily_loss_limit_pct == 0, BINANCE_BROKER, wanted=_is_zero)
        setting(("brokers", i), brokers[i], type(_broker(config, BINANCE_BROKER)), "no_hedge", True,
                f"{BINANCE_BROKER}: aynı coinde açık işlem varken ters yönlü sinyal atlanacak (no_hedge: true)",
                lambda c: getattr(_broker(c, BINANCE_BROKER), "no_hedge", None) is True,
                BINANCE_BROKER, wanted=lambda v: v is True)

    # 2. The owner wants every notice.
    notifier = raw.get("notifier")
    for key, text in (
        ("notify_on_execution", "bildirim: açılan ve kapanan her işlem bildirilecek (notify_on_execution: true)"),
        ("notify_on_ignored", "bildirim: işlem sayılmayan mesajlar da bildirilecek (notify_on_ignored: true)"),
    ):
        setting(("notifier",), notifier, NotifierConfig, key, True, text,
                lambda c, key=key: getattr(c.notifier, key) is True, "bildirim", wanted=lambda v: v is True)

    # 3. / 4. Risk section (created when the file has none).
    risk = raw.get("risk")
    if risk is not None and not isinstance(risk, dict):
        notes.append("'risk' bölümü beklenen biçimde değil; risk ayarları atlandı")
    else:
        present = config.risk.max_open_trades          # the effective value (the code default when not in the file)
        if present < MAX_OPEN_TRADES:
            setting(("risk",), risk, RiskConfig, "max_open_trades", MAX_OPEN_TRADES,
                    f"risk: aynı anda açık tutulabilecek işlem sayısı {present} → {MAX_OPEN_TRADES} "
                    f"(max_open_trades: {MAX_OPEN_TRADES})",
                    lambda c: c.risk.max_open_trades >= MAX_OPEN_TRADES, "risk", wanted=lambda v: False)
        setting(("risk",), risk, RiskConfig, "allow_min_qty_round_up", True,
                "risk: 1R en küçük emir miktarının altında kalırsa işlem en küçük miktarla açılacak "
                "(allow_min_qty_round_up: true)",
                lambda c: c.risk.allow_min_qty_round_up is True, "risk", wanted=lambda v: v is True)

    # 5. fx: the daily loss limit also counts the risk of the open trades.
    i = broker_index(FX_BROKER)
    if i is None or _broker(config, FX_BROKER) is None:
        notes.append(f"'{FX_BROKER}' adlı broker yok; daily_loss_include_open_risk atlandı")
    else:
        setting(("brokers", i), brokers[i], type(_broker(config, FX_BROKER)), "daily_loss_include_open_risk", True,
                f"{FX_BROKER}: günlük zarar sınırına açık işlemlerin riski de sayılacak "
                "(daily_loss_include_open_risk: true)",
                lambda c: getattr(_broker(c, FX_BROKER), "daily_loss_include_open_risk", None) is True,
                FX_BROKER, wanted=lambda v: v is True)

    # 6. Crypto topics: an explicit MARKET signal whose price has drifted is followed, not asked.
    telegram = raw.get("telegram")
    topics = telegram.get("topics") if isinstance(telegram, dict) else None
    for j, topic in enumerate(topics if isinstance(topics, list) else []):
        if not isinstance(topic, dict) or topic.get("kind") != "crypto":
            continue
        name = str(topic.get("name") or f"{j + 1}. konu")
        setting(("telegram", "topics", j), topic, TopicRoute, "explicit_market_drift", "follow",
                f"{name} konusu: 'market' yazan sinyalde fiyat girişten uzaklaşmışsa sormadan devam edilecek "
                "(explicit_market_drift: follow)",
                lambda c, j=j: c.telegram.topics[j].explicit_market_drift == "follow",
                f"{name} konusu", wanted=lambda v: v == "follow")

    # 7. tickmill (the friend's account): MetaApi -> this computer's Tickmill MetaTrader 5 terminal (type mt5_bridge),
    #    once: a block that already names a terminal_path (switched before, and maybe switched back by hand with the
    #    one line "type: metaapi") is never switched again. Every other key stays, the MetaApi ones included.
    i = broker_index(TICKMILL_BROKER)
    if i is not None and _broker(config, TICKMILL_BROKER) is not None:
        block, model = brokers[i], type(_broker(config, TICKMILL_BROKER))
        if block.get("type") == "metaapi" and "terminal_path" not in block:
            path, why = terminal if terminal is not None else tickmill_terminal()
            if path is not None:
                owner = _other_accounts_terminal(path, config)
                if owner is not None:
                    notes.append(f"{TICKMILL_BROKER}: {path} '{owner}' hesabının terminali; tickmill bu terminale "
                                 "çevrilmedi ve MetaApi ile çalışmaya devam ediyor. Tickmill'in kendi terminal64.exe "
                                 "yolunu verin.")
                    path = why = None
                elif "tickmill" not in path.casefold():
                    notes.append(f"{TICKMILL_BROKER}: dikkat, {path} klasörünün adında 'Tickmill' geçmiyor; bot bu "
                                 "terminale yalnızca içinde bir Tickmill hesabı açıksa bağlanır.")
            if path is None:
                if why:
                    notes.append(why)
            elif all(_knows(model, key) for key in ("type", "terminal_path", "broker_company")):
                text = (f"{TICKMILL_BROKER}: MetaApi yerine bu bilgisayardaki Tickmill MT5 terminali kullanılacak "
                        f"(type: mt5_bridge, terminal_path: {path}); MetaApi ayarları blokta duruyor, bu sürümde "
                        "MetaApi'ye geri dönmek için type: metaapi yazmanız yeterli (eski bir sürüme dönüş yalnızca "
                        "guncelle.py --geri-al ile)")
                changes.append(Change(("brokers", i), "type", "mt5_bridge", text,
                                      lambda c: _broker(c, TICKMILL_BROKER).type == "mt5_bridge"))
                changes.append(Change(("brokers", i), "terminal_path", path, "",
                                      lambda c, p=path: getattr(_broker(c, TICKMILL_BROKER), "terminal_path", None) == p))
                if "broker_company" not in block:
                    changes.append(Change(("brokers", i), "broker_company", TICKMILL_COMPANY, "",
                                          lambda c: getattr(_broker(c, TICKMILL_BROKER), "broker_company", None)
                                          == TICKMILL_COMPANY))
            else:
                notes.append(f"bu sürüm mt5_bridge ayarlarını tanımıyor; {TICKMILL_BROKER} MetaApi'de bırakıldı")
        # 8. The friend's requests: 1R 45 -> 50 USD and lots rounded to the nearest step. Both only together with the
        #    block's first lot_rounding: once it is there, whatever the owner writes later stays.
        if "lot_rounding" not in block:
            risk_now = block.get("risk_usd", _MISSING)
            if _same_number(risk_now, TICKMILL_OLD_RISK_USD) and _knows(model, "risk_usd"):
                changes.append(Change(
                    ("brokers", i), "risk_usd", TICKMILL_NEW_RISK_USD,
                    f"{TICKMILL_BROKER}: 1R riski {TICKMILL_OLD_RISK_USD} → {TICKMILL_NEW_RISK_USD} USD "
                    f"(risk_usd: {TICKMILL_NEW_RISK_USD})",
                    lambda c: getattr(_broker(c, TICKMILL_BROKER), "risk_usd", None) == TICKMILL_NEW_RISK_USD))
            setting(("brokers", i), block, model, "lot_rounding", "nearest",
                    f"{TICKMILL_BROKER}: lot en yakın adıma yuvarlanacak (0.125 → 0.13; stopta kayıp hedefi %15'ten "
                    "fazla aşacaksa aşağı yuvarlanır) (lot_rounding: nearest)",
                    lambda c: getattr(_broker(c, TICKMILL_BROKER), "lot_rounding", None) == "nearest",
                    TICKMILL_BROKER, wanted=lambda v: True)
    return changes, notes


# --------------------------------------------------------------------------- #
# Parsed-data side: the expected result and its comparison
# --------------------------------------------------------------------------- #
def _unshare(obj: Any) -> Any:
    """Deep copy that also separates YAML aliases (one object per place in the document)."""
    if isinstance(obj, dict):
        return {k: _unshare(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_unshare(v) for v in obj]
    return obj


def _apply_to_data(data: Any, changes: list[Change]) -> Any:
    """``data`` (parsed YAML) with ``changes`` applied: new keys go to the end of their mapping, a missing or
    empty section is created (a new one at the end of the file)."""
    out = _unshare(data)
    for ch in changes:
        node = out
        for depth, step in enumerate(ch.path):
            child = node.get(step) if isinstance(step, str) else node[step]
            if child is None and isinstance(step, str) and depth == len(ch.path) - 1:
                child = node[step] = {}
            node = child
        node[ch.key] = ch.value
    return out


def _freeze(obj: Any) -> Any:
    """Comparable form that keeps value types (0 is not False) and the order of mapping keys."""
    if isinstance(obj, dict):
        return ("map", tuple((_freeze(k), _freeze(v)) for k, v in obj.items()))
    if isinstance(obj, (list, tuple)):
        return ("seq", tuple(_freeze(v) for v in obj))
    return (type(obj).__name__, repr(obj))


def _verify(old_text: str, new_text: str, changes: list[Change]) -> str | None:
    """None when ``new_text`` is exactly ``old_text`` plus the planned changes, else what is wrong."""
    try:
        old, new = yaml.safe_load(old_text), yaml.safe_load(new_text)
    except yaml.YAMLError as exc:
        return f"yeni metin geçerli YAML değil ({str(exc).splitlines()[0]})"
    if _freeze(_apply_to_data(old, changes)) != _freeze(new):
        return "planlanan ayarlar dışında bir fark oluştu"
    if sorted(_PLACEHOLDER.findall(old_text)) != sorted(_PLACEHOLDER.findall(new_text)):
        return "${...} yer tutucuları değişti"
    return None


# --------------------------------------------------------------------------- #
# Text side
# --------------------------------------------------------------------------- #
def _scalar(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str) and _PLAIN_WORD.fullmatch(value) and value.lower() not in _YAML_WORDS:
        return value
    return json.dumps(value, ensure_ascii=False)


def _value_node(mapping: yaml.MappingNode, key: str | int) -> yaml.Node | None:
    for k, v in mapping.value:
        if isinstance(k, yaml.ScalarNode) and k.value == str(key):
            return v
    return None


def _node_at(root: yaml.Node, path: tuple[str | int, ...]) -> yaml.Node | None:
    """Node at ``path``; None when the LAST step is a missing key. Anything else unexpected is unsupported."""
    node: yaml.Node | None = root
    for depth, step in enumerate(path):
        if isinstance(step, str):
            if not isinstance(node, yaml.MappingNode):
                raise _TextEditUnsupported(f"{step}: eşleme bekleniyordu")
            node = _value_node(node, step)
            if node is None:
                if depth == len(path) - 1:
                    return None
                raise _TextEditUnsupported(f"{step}: bölüm yok")
        else:
            if not isinstance(node, yaml.SequenceNode) or step >= len(node.value):
                raise _TextEditUnsupported(f"{step}: liste bekleniyordu")
            node = node.value[step]
    return node


def _content_end(node: yaml.Node) -> int:
    """Index right after the last character that belongs to ``node``. (PyYAML's own end mark of a block
    collection is where the NEXT token starts, i.e. after blank lines and comments that follow it.)"""
    if isinstance(node, yaml.ScalarNode) or getattr(node, "flow_style", False):
        return int(node.end_mark.index)
    ends: list[int] = []
    for item in node.value:
        ends += [_content_end(part) for part in (item if isinstance(item, tuple) else (item,))]
    return max(ends) if ends else int(node.end_mark.index)


def _next_line_start(text: str, index: int, newline: str) -> tuple[int, str]:
    """(position of the first line start at or after ``index``, text to put before an insertion there)."""
    if index > 0 and text[index - 1] == "\n":      # a block scalar ends at the start of the following line
        return index, ""
    end = text.find("\n", index)
    if end < 0:
        return len(text), newline                  # the last line has no line break
    return end + 1, ""


def _child_indent(root: yaml.MappingNode) -> int:
    """Indentation the file uses for the keys of a top-level section (2 when it has no such section)."""
    for _key, value in root.value:
        if isinstance(value, yaml.MappingNode) and not value.flow_style and value.value:
            column = int(value.value[0][0].start_mark.column)
            if column > 0:
                return column
    return 2


def _edit_text(text: str, changes: list[Change]) -> str:
    """``text`` with ``changes`` applied as text edits (see the module docstring). Raises
    :class:`_TextEditUnsupported` for a layout it does not handle; the caller verifies the result."""
    try:
        root = yaml.compose(text, Loader=yaml.SafeLoader)
    except yaml.YAMLError as exc:
        raise _TextEditUnsupported(str(exc)) from exc
    if not isinstance(root, yaml.MappingNode) or root.flow_style:
        raise _TextEditUnsupported("dosyanın kökü blok biçiminde bir eşleme değil")
    newline = "\r\n" if "\r\n" in text else "\n"
    groups: dict[tuple[str | int, ...], list[Change]] = {}
    for ch in changes:
        groups.setdefault(ch.path, []).append(ch)

    edits: list[tuple[int, int, str]] = []        # (start, end, replacement) in the ORIGINAL text
    appended: list[str] = []                      # whole new top-level sections for the end of the file
    for path, group in groups.items():
        node = _node_at(root, path)
        if node is None:
            if len(path) != 1:
                raise _TextEditUnsupported(f"{path}: iç içe eksik bölüm")
            pad = " " * _child_indent(root)
            appended.append(f"{path[0]}:{newline}"
                            + "".join(f"{pad}{c.key}: {_scalar(c.value)}{newline}" for c in group))
            continue
        if not isinstance(node, yaml.MappingNode):
            raise _TextEditUnsupported(f"{path}: eşleme değil")       # e.g. "risk:" with nothing below it
        missing: list[Change] = []
        for ch in group:
            value = _value_node(node, ch.key)
            if value is None:
                missing.append(ch)
                continue
            if not isinstance(value, yaml.ScalarNode) or value.style in ("|", ">"):
                raise _TextEditUnsupported(f"{ch.key}: değer tek satırlık değil")
            start, end = int(value.start_mark.index), int(value.end_mark.index)
            # "key:" with nothing after it is an empty node right behind the colon
            edits.append((start, end, (" " if start == end else "") + _scalar(ch.value)))
        if not missing:
            continue
        if node.flow_style:
            close = int(node.end_mark.index) - 1
            if close < 0 or text[close] != "}":
                raise _TextEditUnsupported(f"{path}: satır içi eşlemenin sonu bulunamadı")
            body = ", ".join(f"{c.key}: {_scalar(c.value)}" for c in missing)
            edits.append((close, close, (", " if node.value else "") + body))
        else:
            if not node.value:
                raise _TextEditUnsupported(f"{path}: boş eşleme")
            pad = " " * int(node.value[0][0].start_mark.column)
            at, before = _next_line_start(text, _content_end(node), newline)
            edits.append((at, at, before + "".join(f"{pad}{c.key}: {_scalar(c.value)}{newline}" for c in missing)))

    out = text
    for start, end, replacement in sorted(edits, key=lambda e: (e[0], e[1]), reverse=True):
        out = out[:start] + replacement + out[end:]
    if appended:
        if out and not out.endswith("\n"):
            out += newline
        out += "".join(appended)
    return out


def _rewrite(text: str, data: Any, changes: list[Change], backup_name: str) -> str:
    """Fallback: the whole file through PyYAML (values, key order and placeholders kept; comments lost)."""
    body = yaml.safe_dump(_apply_to_data(data, changes), sort_keys=False, allow_unicode=True,
                          default_flow_style=None, width=4096)
    out = (f"# Bu dosya güncelleme sırasında yeniden yazıldı (açıklama satırları korunamadı).\n"
           f"# Açıklamalı eski hali: {backup_name}\n") + body
    if "\r\n" in text:
        out = out.replace("\n", "\r\n")
    return ("﻿" if text.startswith("﻿") else "") + out


# --------------------------------------------------------------------------- #
# Files
# --------------------------------------------------------------------------- #
def _short_error(exc: BaseException) -> str:
    """One line without values from the file (a pydantic error text would quote them, secrets included)."""
    errors = getattr(exc, "errors", None)
    if callable(errors):
        try:
            parts = [f"{'.'.join(str(x) for x in e.get('loc', ()))}: {e.get('msg', '')}" for e in errors()[:5]]
            return "; ".join(parts) or type(exc).__name__
        except Exception:      # noqa: BLE001 - never fail while describing a failure
            return type(exc).__name__
    return (str(exc).splitlines() or [type(exc).__name__])[0]


def _write_atomic(path: Path, content: bytes) -> None:
    """Replace ``path`` in one step (a crash or an error in between leaves the old file)."""
    tmp = path.with_name(path.name + ".tmp")
    try:
        with open(tmp, "wb") as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        try:
            shutil.copymode(path, tmp)
        except OSError:
            pass
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _backup_path(path: Path, now: datetime) -> Path:
    base = f"{path.name}.yedek-{now.strftime('%Y%m%d-%H%M%S')}"
    candidate, n = path.with_name(base), 1
    while candidate.exists():
        n += 1
        candidate = path.with_name(f"{base}-{n}")
    return candidate


def migrate_config(path: str | os.PathLike[str] = "config.yaml", *, notes: list[str] | None = None,
                   now: datetime | None = None, terminal: tuple[str | None, str | None] | None = None) -> list[str]:
    """Update ``path`` in place. Returns one Turkish line per change (empty: the file was already up to date
    and nothing was written). ``notes`` (optional list) receives remarks: skipped settings, the backup's name,
    lost comments. Raises :class:`MigrationError` when the file cannot be migrated; the file on disk is then
    the old one."""
    p = Path(path)
    raw_bytes = p.read_bytes()
    try:
        text = raw_bytes.decode("utf-8")
        data = yaml.safe_load(text)
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise MigrationError(f"{p.name} okunamadı ({_short_error(exc)}); hiçbir şey değiştirilmedi") from exc
    if not isinstance(data, dict):
        raise MigrationError(f"{p.name} beklenen biçimde değil; hiçbir şey değiştirilmedi")
    try:
        config = load_config(p)        # the app's own loader: also tells which keys this version knows
    except Exception as exc:
        raise MigrationError(
            f"{p.name} bu sürümle yüklenemiyor ({_short_error(exc)}); hiçbir şey değiştirilmedi") from exc

    changes, found = plan_changes(data, config, terminal=terminal)
    remarks = list(found)
    try:
        if not changes:
            return []
        backup = _backup_path(p, now or datetime.now())
        rewritten = False
        try:
            new_text = _edit_text(text, changes)
            problem = _verify(text, new_text, changes)
            if problem:
                raise _TextEditUnsupported(problem)
        except _TextEditUnsupported:
            rewritten = True
            new_text = _rewrite(text, data, changes, backup.name)
            problem = _verify(text, new_text, changes)
            if problem:
                raise MigrationError(f"{p.name} güvenle güncellenemedi ({problem}); hiçbir şey değiştirilmedi")

        try:
            backup.write_bytes(raw_bytes)
            _write_atomic(p, new_text.encode("utf-8"))
        except OSError as exc:
            raise MigrationError(f"{p.name} yazılamadı ({exc}); hiçbir şey değiştirilmedi") from exc
        try:
            loaded = load_config(p)
            failed = [c.key for c in changes if not c.check(loaded)]
            if failed:
                raise MigrationError(f"yeni değerler etkili olmadı: {', '.join(failed)}")
        except BaseException as exc:
            _write_atomic(p, raw_bytes)            # the old content, byte for byte
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise MigrationError(
                f"güncellenen {p.name} bu sürümle yüklenemedi ({_short_error(exc)}); eski hali geri konuldu"
            ) from exc
        remarks.append(f"eski dosyanın yedeği: {backup.name}")
        if rewritten:
            remarks.append("dosyadaki açıklama satırları korunamadı (dosya baştan yazıldı); açıklamalı eski "
                           f"hali {backup.name} dosyasında")
        return [c.text for c in changes if c.text]
    finally:
        if notes is not None:
            notes.extend(remarks)


def run_cli(path: str, out: Callable[[str], None] = print) -> int:
    """The ``migrate`` command: one line per change. Exit code 0 (also when nothing had to change), 2 on failure."""
    notes: list[str] = []
    try:
        lines = migrate_config(path, notes=notes)
    except MigrationError as exc:
        out(f"Ayar dosyası güncellenemedi: {exc}")
        return 2
    for line in lines:
        out(f"• {line}")
    for note in notes:
        out(f"  not: {note}")
    out(f"{len(lines)} ayar güncellendi." if lines else "Ayar dosyası zaten güncel; değişiklik yapılmadı.")
    return 0


__all__ = ["Change", "MigrationError", "migrate_config", "plan_changes", "run_cli"]
