"""Broker heartbeat.

Without it nothing notices a broker that stopped answering (terminal closed overnight, MetaApi disconnected,
API key revoked) until a signal fails on it. The monitor owns one :class:`BrokerHeartbeat` and runs it as a
task of its own inside ``Monitor.run``: it never blocks or delays the position sync.

Every ``HEARTBEAT_SEC`` each connected broker is asked whether it is well: through its own
``health_problem()`` coroutine when the adapter has one (it returns None, or a short Turkish text naming the
problem), else by reading the balance. An exception, a timeout and a ``CancelledError`` nobody requested all
count as a problem. After ``HEARTBEAT_ALERT_AFTER`` problems in a row the owner is told once, and once more when
the broker answers again. Message-only (manual) brokers and simulated ones (paper, dry-run) are left alone.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable, Sequence
from typing import Any

from ..brokers.base import Broker
from ..interfaces import Notifier
from ..models import Severity
from ..telegram.formatting import esc

logger = logging.getLogger(__name__)

HEARTBEAT_SEC = 60.0                  # every connected broker is checked this often
HEARTBEAT_ALERT_AFTER = 3             # problems in a row before the owner is told (once per outage)
HEARTBEAT_READ_TIMEOUT_SEC = 20.0     # the balance read used for adapters without health_problem()
HEARTBEAT_CHECK_TIMEOUT_SEC = 30.0    # upper bound of one broker's whole check
REASON_MAX_CHARS = 300

_SILENT = "silent"        # the broker did not answer (timeout, error, connection cut)
_REPORTED = "reported"    # the broker answered and named a problem itself (health_problem())


def stray_cancel() -> bool:
    """Call inside ``except asyncio.CancelledError``: True when nobody asked to cancel the running task.

    A broker SDK can raise CancelledError on its own: metaapi-cloud-sdk does while the account is disconnected
    (its calls await one shared future that an earlier caller's timeout cancelled). That is a failed call, not a
    cancellation: ``Task.cancelling()`` is 0 then, and above 0 for a real ``task.cancel()`` (shutdown, an
    enclosing timeout), which must always be re-raised."""
    task = asyncio.current_task()
    cancelling = getattr(task, "cancelling", None)
    return cancelling is not None and cancelling() == 0


def _short(text: object) -> str:
    """One line, no trailing full stop, bounded length (the alert adds its own punctuation)."""
    out = " ".join(str(text).split()).rstrip(". ")
    return out if len(out) <= REASON_MAX_CHARS else out[: REASON_MAX_CHARS - 1].rstrip() + "…"


class BrokerHeartbeat:
    def __init__(self, brokers: Callable[[], Sequence[Broker]], notifier: Notifier) -> None:
        """``brokers``: returns the brokers that are connected right now."""
        self._brokers = brokers
        self.notifier = notifier
        self._count: dict[str, int] = {}       # broker -> problems in a row
        self._reason: dict[str, str] = {}      # broker -> its latest problem
        self._alerted: dict[str, str] = {}     # broker -> kind of the alert the owner got for this outage
        # Called with the broker's name after every check it passed (the engine looks then whether that account
        # missed a signal while it was not connected). Must not block; whatever it raises is logged only.
        self.on_healthy: Callable[[str], Any] | None = None

    @staticmethod
    def watched(broker: Any) -> bool:
        """Real accounts only: not the message-only broker, not simulated ones (paper, dry-run wrapper)."""
        kind = str(getattr(broker, "kind", "") or "")
        cfg_type = str(getattr(getattr(broker, "config", None), "type", "") or "")
        if "manual" in (kind, cfg_type) or "paper" in (kind, cfg_type):
            return False
        return not kind.startswith("dry-run")

    def problems(self) -> dict[str, str]:
        """Brokers that did not pass their latest check: name -> reason (Turkish)."""
        return dict(self._reason)

    # ---- loop ----------------------------------------------------------------
    async def run(self) -> None:
        """Check every HEARTBEAT_SEC until this task is cancelled. Nothing else ends or escapes it."""
        while True:
            await asyncio.sleep(HEARTBEAT_SEC)
            try:
                await self.check_once()
            except asyncio.CancelledError:
                if not stray_cancel():
                    raise
                logger.error("Heartbeat round ended by a CancelledError nobody requested; next round as usual")
            except Exception:
                logger.exception("Heartbeat round failed")

    async def check_once(self) -> None:
        """One round: every watched broker at the same time, each bounded to HEARTBEAT_CHECK_TIMEOUT_SEC."""
        brokers = [b for b in self._brokers() if self.watched(b)]
        names = {b.name for b in brokers}
        for state in (self._count, self._reason, self._alerted):
            for name in [n for n in state if n not in names]:
                del state[name]                 # no longer connected: the engine reports that itself
        if not brokers:
            return
        results = await asyncio.gather(*(self._probe(b) for b in brokers), return_exceptions=True)
        for broker, result in zip(brokers, results, strict=True):
            if isinstance(result, BaseException):       # _probe handles its own errors; a backstop
                result = (_SILENT, _short(str(result) or type(result).__name__))
            try:
                await self._record(broker.name, result)
            except asyncio.CancelledError:
                if not stray_cancel():
                    raise
                logger.error("Heartbeat: telling the owner about %s was cut off; tried again next round", broker.name)
            except Exception:
                logger.exception("Heartbeat: could not tell the owner about %s; tried again next round", broker.name)

    # ---- one broker ------------------------------------------------------------
    async def _probe(self, broker: Broker) -> tuple[str, str] | None:
        """None when the broker is well, else (kind, Turkish reason). Never raises (a real cancellation excepted)."""
        try:
            return await asyncio.wait_for(self._ask(broker), HEARTBEAT_CHECK_TIMEOUT_SEC)
        except asyncio.TimeoutError as exc:
            return _SILENT, _short(str(exc) or f"{HEARTBEAT_CHECK_TIMEOUT_SEC:g} sn içinde yanıt gelmedi")
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            return _SILENT, "bağlantı kopuk (istek yarıda kaldı)"
        except Exception as exc:
            return _SILENT, _short(str(exc) or type(exc).__name__)

    async def _ask(self, broker: Broker) -> tuple[str, str] | None:
        check = getattr(broker, "health_problem", None)
        if callable(check):
            answer = check()
            if inspect.isawaitable(answer):
                answer = await answer
            text = _short(answer) if answer is not None else ""
            return (_REPORTED, text) if text else None
        try:
            await asyncio.wait_for(broker.get_equity(), HEARTBEAT_READ_TIMEOUT_SEC)
        except asyncio.TimeoutError as exc:
            return _SILENT, _short(str(exc) or f"{HEARTBEAT_READ_TIMEOUT_SEC:g} sn içinde yanıt gelmedi")
        return None

    def _healthy(self, name: str) -> None:
        hook = self.on_healthy
        if hook is None:
            return
        try:
            hook(name)
        except Exception:
            logger.exception("Heartbeat: the hook for a healthy %s failed", name)

    async def _record(self, name: str, problem: tuple[str, str] | None) -> None:
        if problem is None:
            self._healthy(name)
            failed = self._count.pop(name, 0)
            self._reason.pop(name, None)
            kind = self._alerted.get(name)
            if kind is None:
                if failed:
                    logger.info("Heartbeat: %s answers again after %d problem(s)", name, failed)
                return
            logger.info("Heartbeat: %s is well again", name)
            await self.notifier.notify(
                f"✅ <b>{esc(name)}</b> " + ("hesabındaki sorun giderildi." if kind == _REPORTED
                                            else "tekrar yanıt veriyor."),
                Severity.INFO,
            )
            del self._alerted[name]         # only after the owner was told (else: tried again next round)
            return
        kind, reason = problem
        count = self._count.get(name, 0) + 1
        self._count[name] = count
        self._reason[name] = reason
        logger.warning("Heartbeat: %s problem (%d in a row): %s", name, count, reason)
        if count < HEARTBEAT_ALERT_AFTER or name in self._alerted:
            return
        await self.notifier.notify(
            f"🔌 <b>{esc(name)}</b> " + ("hesabında sorun var" if kind == _REPORTED else "yanıt vermiyor")
            + f": {esc(reason)}. Yeni sinyaller bu hesapta açılamayabilir.",
            Severity.WARNING,
        )
        self._alerted[name] = kind          # only after the owner was told (else: tried again next round)


__all__ = [
    "BrokerHeartbeat", "stray_cancel", "HEARTBEAT_SEC", "HEARTBEAT_ALERT_AFTER", "HEARTBEAT_READ_TIMEOUT_SEC",
    "HEARTBEAT_CHECK_TIMEOUT_SEC",
]
