"""The drawing process: ``python -m copytrader.karne <data.json> <card.png> [--scale N] [--limit SECONDS]``.

Started by the bot (:mod:`copytrader.karne.render`) for ONE card and nothing else: it reads the JSON the bot wrote,
draws the card with Pillow and writes the PNG (to a temporary name first, renamed when it is whole: a process that is
killed never leaves half a picture under the final name). It talks to nothing: no network, no database, no Telegram.

Exit codes: 0 the picture is there; EXIT_NO_PILLOW Pillow is not installed (the bot sends the text card); EXIT_INPUT
the data could not be read; EXIT_FAILED drawing failed (the reason is on stderr); EXIT_TIMEOUT its own time limit ran
out (the bot kills it at its limit anyway; this one also ends a process whose bot is gone).
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import threading
import traceback

from . import OUT_SCALE, PRESET

EXIT_OK, EXIT_FAILED, EXIT_INPUT, EXIT_NO_PILLOW, EXIT_TIMEOUT = 0, 1, 2, 3, 4
DEFAULT_LIMIT_SEC = 120.0
NICE = 10                        # POSIX: lower CPU priority (Windows: the bot starts it below normal priority)


def _own_limit(seconds: float) -> None:
    """End this process after ``seconds`` whatever it is doing (a daemon timer; os._exit skips every cleanup)."""
    timer = threading.Timer(max(1.0, seconds), os._exit, args=(EXIT_TIMEOUT,))
    timer.daemon = True
    timer.start()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m copytrader.karne", description="Draw one report card.")
    parser.add_argument("data", help="the card's data (JSON, written by the bot)")
    parser.add_argument("out", help="the PNG file to write")
    parser.add_argument("--scale", type=int, default=OUT_SCALE, help="output width = 1080 px * scale")
    parser.add_argument("--limit", type=float, default=DEFAULT_LIMIT_SEC, help="seconds this process may run")
    args = parser.parse_args(argv)
    _own_limit(args.limit)
    with contextlib.suppress(Exception):          # (never a reason to fail: priority is a courtesy to the bot)
        if hasattr(os, "nice"):
            os.nice(NICE)
    try:
        with open(args.data, encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict) or args.scale not in (1, 2):
            raise ValueError("the data is not a card, or the scale is not 1 or 2")
    except Exception as exc:      # noqa: BLE001
        print(f"karne: cannot read the card data: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_INPUT
    try:
        from . import design
    except ImportError as exc:
        print(f"karne: Pillow is not installed ({exc})", file=sys.stderr)
        return EXIT_NO_PILLOW
    part = f"{args.out}.part.png"
    try:
        design.render(data, PRESET, part, out_scale=args.scale)
        os.replace(part, args.out)
    except Exception:             # noqa: BLE001
        traceback.print_exc()
        with contextlib.suppress(OSError):
            os.unlink(part)
        return EXIT_FAILED
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
