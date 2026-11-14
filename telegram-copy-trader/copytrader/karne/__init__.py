"""The owner's weekly report card ("karne"): every Sunday at 22:00 UK time the bot sends him one picture, as a file,
with the week's closed trades and their results in R (never a money amount); ``/karne`` sends the card of the week so
far and ``/karne hepsi`` the card of every trade so far.

Nothing here may hold up trading:

* the bot itself only reads the database and builds a small dictionary (:mod:`.data`);
* the picture is drawn by ANOTHER process (``python -m copytrader.karne``, :mod:`.__main__` + :mod:`.design`), started
  and watched by :mod:`.render`: time limit, killed on overrun, never waited for in a way that holds the event loop;
* the bot's own code never imports Pillow or the drawing module, and the bot's own process cannot: Pillow is
  optional and lives in a folder of its own next to the package (``karne_paketleri``; guncelle.py installs it there
  with ``pip install --target``, a failure is only a warning), which only the drawing process has on its path.
  Without it the card goes out as a plain text summary. (Were Pillow in the bot's Python environment, Telethon
  would import it and resize uploaded photos with it on the event loop: that use is switched off as a second line
  of defence, ``telegram.notifier.keep_pillow_out_of_telethon``.)
* the schedule, the stored cut-off and the rate limit of the command are in :mod:`.service`.

This module holds the few constants the parts share and imports nothing.
"""

from __future__ import annotations

# The ONE design the bot draws: the designer's final preset (design.FINAL_PRESET; tests keep the two equal).
PRESET = 25
TITLE = "BABÜR'S TRADE PERFORMANCE"
# Rows the design's table holds (design.MAX_ROWS; tests keep the two equal): with more trades it shows one less and
# counts the rest in a line ("… ve N işlem daha (toplama dahil)").
TABLE_ROWS = 20
# Output width = 1080 px * OUT_SCALE. 1 on purpose (see render.py: measured cost of 1x and 2x).
OUT_SCALE = 1
# What guncelle.py installs for the picture, into karne_paketleri (tests keep the two equal). 13.0.0 has win_amd64
# wheels for cp311-cp313.
PILLOW = "pillow==13.0.0"

__all__ = ["OUT_SCALE", "PILLOW", "PRESET", "TABLE_ROWS", "TITLE"]
