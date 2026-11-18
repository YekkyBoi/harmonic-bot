"""The owner's trade card, drawn (Pillow): the design the owner approved (plum background, glass panels, palette 26:
ice-white / blood-red candles, a soft red stop zone and a light grey target zone, rose-gold outlined hero numerals).

Imported ONLY by the drawing process (``python -m copytrader.karne``, see :mod:`.render`): never by the bot. The
data comes from :func:`copytrader.karne.tradecard.card_data` (JSON); nothing here reads anything else, and nothing
here decides a figure: it draws what it is given.

2560 x 1600 px (Telegram's largest photo side), drawn natively at that size (layout units x S = 1.6): no
resampling, crisp text. Fonts: only the ones the weekly card ships (fonts/). The asset's name is set in Cormorant
with lining figures ("NAS100", OpenType ``lnum``), which needs Pillow's raqm layout; a Pillow without raqm (e.g. a
wheel without libraqm) draws the digits of the name in Montserrat instead (:data:`LNUM`): no crash, same layout.
"""

from __future__ import annotations

import math
import os
from functools import lru_cache
from typing import Any

from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont

try:
    from PIL import features as _features

    LNUM = bool(_features.check("raqm"))      # lining figures need raqm (OpenType features)
except Exception:  # noqa: BLE001 - an odd Pillow: the fallback
    LNUM = False

FONTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fonts")
W, H = 1600, 1000                 # layout units
S = 1.6                           # drawn directly at 2560 x 1600
SIZE = (round(W * S), round(H * S))

BG = ["#0C0810", "#17101B", "#221625"]
GLOW = "#633352"
GOLD = "#EDBFAE"
GOLD2 = "#C88E7C"
DEEP = "#9C6656"
LIGHT = "#FCE7DE"
INK = "#F6F1EA"
MUTE = "#A99CA5"
UP = "#EDEFF2"                    # candles: ice white (up)
DOWN = "#D7263D"                  # candles: blood red (down)
STOP_Z = "#D7263D"                # setup: stop zone (palette 26)
TARGET_Z = "#D8D0D6"              # setup: target zone, light grey
ARROW_UP = "#34D399"              # buy arrow: green, pointing up, under the candle
ARROW_DN = "#F0405A"              # sell arrow: red, pointing down, over the candle
DOTS = {"stop": STOP_Z, "target": TARGET_Z}

SERIF = "CormorantGaramond_600SemiBold"
SERIF_I = "CormorantGaramond_600SemiBold_Italic"
M4, M5, M6 = "Montserrat_400Regular", "Montserrat_500Medium", "Montserrat_600SemiBold"
I5 = "Inter_500Medium"


def rgb(h: str) -> tuple[int, int, int]:
    h = h.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def rgba(h: str, a: float) -> tuple[int, int, int, int]:
    return (*rgb(h), int(round(255 * a)))


@lru_cache(maxsize=64)
def _font(name: str, px: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(os.path.join(FONTS_DIR, name + ".ttf"), px)


def font(name: str, size: float) -> ImageFont.FreeTypeFont:
    return _font(name, max(1, round(size * S)))


def lerp(a: tuple[int, ...], b: tuple[int, ...], t: float) -> tuple[int, ...]:
    return tuple(int(round(a[i] + (b[i] - a[i]) * t)) for i in range(len(a)))


def num(x: float, digits: int) -> str:
    s = f"{x:,.{max(0, digits)}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


class Canvas:
    def __init__(self) -> None:
        self.size = SIZE
        self._background()

    def p(self, v: float) -> int:
        return int(round(v * S))

    def layer(self) -> tuple[Image.Image, ImageDraw.ImageDraw]:
        lay = Image.new("RGBA", self.size, (0, 0, 0, 0))
        return lay, ImageDraw.Draw(lay)

    def put(self, lay: Image.Image, blur: float = 0.0) -> None:
        if blur:
            lay = lay.filter(ImageFilter.GaussianBlur(blur * S))
        self.img.alpha_composite(lay)

    def _background(self) -> None:
        g = Image.new("RGBA", (1, 512))
        stops = [(0.0, rgb(BG[0])), (0.55, rgb(BG[1])), (1.0, rgb(BG[2]))]
        for y in range(512):
            t = y / 511
            for (t0, c0), (t1, c1) in zip(stops, stops[1:]):
                if t0 <= t <= t1:
                    g.putpixel((0, y), lerp(c0, c1, (t - t0) / (t1 - t0)) + (255,))
                    break
        self.img = g.resize(self.size, Image.BICUBIC)
        lay, d = self.layer()
        d.ellipse((self.p(-260), self.p(-320), self.p(820), self.p(520)), fill=rgba(GLOW, 0.55))
        d.ellipse((self.p(1100), self.p(640), self.p(1900), self.p(1260)), fill=rgba(GLOW, 0.28))
        self.put(lay, blur=150)
        lay, d = self.layer()
        d.rectangle((0, 0, *self.size), outline=(0, 0, 0, 150), width=self.p(70))
        self.put(lay, blur=90)

    def glass(self, box: tuple[float, float, float, float], radius: float, a: float = 0.055) -> None:
        x0, y0, x1, y1 = box
        lay, d = self.layer()
        d.rounded_rectangle((self.p(x0 + 4), self.p(y0 + 14), self.p(x1 - 4), self.p(y1 + 18)), radius=self.p(radius),
                            fill=(0, 0, 0, 120))
        self.put(lay, blur=22)
        lay, d = self.layer()
        d.rounded_rectangle((self.p(x0), self.p(y0), self.p(x1), self.p(y1)), radius=self.p(radius),
                            fill=rgba("#FFFFFF", a), outline=rgba(LIGHT, 0.12), width=max(1, self.p(1)))
        d.line((self.p(x0 + radius), self.p(y0) + 1, self.p(x1 - radius), self.p(y0) + 1),
               fill=rgba("#FFFFFF", 0.10), width=1)
        self.put(lay)

    def text(self, xy: tuple[float, float], s: str, f: ImageFont.FreeTypeFont, fill: str, a: float = 1.0,
             anchor: str = "la", tracking: float = 0.0, features: list[str] | None = None) -> float:
        lay, d = self.layer()
        x, y = self.p(xy[0]), self.p(xy[1])
        if tracking:
            total = sum(d.textlength(ch, font=f) for ch in s) + self.p(tracking) * (len(s) - 1)
            x -= {"l": 0, "m": total / 2, "r": total}[anchor[0]]
            for ch in s:
                d.text((x, y), ch, font=f, fill=rgba(fill, a), anchor="l" + anchor[1])
                x += d.textlength(ch, font=f) + self.p(tracking)
            self.put(lay)
            return total / S
        extra = {"features": features} if features else {}
        d.text((x, y), s, font=f, fill=rgba(fill, a), anchor=anchor, **extra)
        self.put(lay)
        return d.textlength(s, font=f, **extra) / S

    def width(self, s: str, f: ImageFont.FreeTypeFont, tracking: float = 0.0) -> float:
        d = ImageDraw.Draw(self.img)
        if tracking:
            return (sum(d.textlength(ch, font=f) for ch in s) + self.p(tracking) * (len(s) - 1)) / S
        return d.textlength(s, font=f) / S

    def hero(self, xy: tuple[float, float], s: str, size: float, anchor: str = "rs") -> None:
        """Outlined numerals like the weekly card's: a rose-gold metallic outline, a faint fill, a soft glow."""
        f = font(M5, size)
        sw = max(3, self.p(3.0))
        pos = (self.p(xy[0]), self.p(xy[1]))
        full = Image.new("L", self.size, 0)
        ImageDraw.Draw(full).text(pos, s, font=f, fill=255, anchor=anchor, stroke_width=sw, stroke_fill=255)
        inner = Image.new("L", self.size, 0)
        ImageDraw.Draw(inner).text(pos, s, font=f, fill=255, anchor=anchor)
        ring = ImageChops.subtract(full, inner)
        box = full.getbbox()
        if box is None:
            return
        x0, y0, x1, y1 = box
        grad = Image.new("RGBA", (1, 256))
        stops = [(0, rgb(LIGHT)), (0.30, rgb(LIGHT)), (0.46, rgb(GOLD)), (0.56, rgb(DEEP)), (0.66, rgb(GOLD2)),
                 (1.0, rgb(GOLD))]
        for y in range(256):
            t = y / 255
            for (t0, c0), (t1, c1) in zip(stops, stops[1:]):
                if t0 <= t <= t1:
                    grad.putpixel((0, y), lerp(c0, c1, (t - t0) / max(1e-9, t1 - t0)) + (255,))
                    break
        metal = Image.new("RGBA", self.size, (0, 0, 0, 0))
        metal.paste(grad.resize((x1 - x0, y1 - y0), Image.BICUBIC), (x0, y0))
        g2 = Image.new("RGBA", self.size, (0, 0, 0, 0))
        g2.paste(Image.new("RGBA", self.size, rgba(GOLD, 1)), (0, 0), full.point(lambda v: int(v * 0.30)))
        self.put(g2, blur=10)
        fill = Image.new("RGBA", self.size, (0, 0, 0, 0))
        fill.paste(Image.new("RGBA", self.size, rgba(LIGHT, 1)), (0, 0), inner.point(lambda v: int(v * 0.10)))
        self.put(fill)
        out = Image.new("RGBA", self.size, (0, 0, 0, 0))
        out.paste(metal, (0, 0), ring)
        self.put(out)


def asset_name(cv: Canvas, xy: tuple[float, float], name: str, size: float, color: str, *,
               lnum: bool | None = None) -> float:
    """The asset's name in the serif with LINING figures ("NAS100"). With raqm: one run with the ``lnum`` feature.
    Without (``lnum`` False): letters in the serif, each run of digits in Montserrat Regular, sized so that its
    digits stand as tall as the serif's capitals, on the same baseline. Returns the width (layout units)."""
    lnum = LNUM if lnum is None else lnum
    serif = font(SERIF, size)
    if lnum or not any(ch.isdigit() for ch in name):
        return cv.text(xy, name, serif, color, anchor="ls", features=["lnum"] if lnum else None)
    cap = _ink_height(serif, "H")
    digit_font = font(M4, size)
    ratio = cap / max(1, _ink_height(digit_font, "0"))
    digit_font = font(M4, size * ratio * 0.98)
    x = xy[0]
    runs: list[tuple[str, bool]] = []
    for ch in name:
        is_digit = ch.isdigit()
        if runs and runs[-1][1] == is_digit:
            runs[-1] = (runs[-1][0] + ch, is_digit)
        else:
            runs.append((ch, is_digit))
    for k, (text, is_digit) in enumerate(runs):
        if is_digit and k > 0:
            x += 2
        x += cv.text((x, xy[1]), text, digit_font if is_digit else serif, color, anchor="ls")
        if is_digit:
            x += 2
    return x - xy[0]


def _ink_height(f: ImageFont.FreeTypeFont, ch: str) -> int:
    box = f.getbbox(ch, anchor="ls")
    return max(1, box[3] - box[1]) if box else 1


def _nice_step(span: float) -> float:
    """A round price step that gives about 8-10 grid lines over ``span``."""
    if not (span > 0 and math.isfinite(span)):
        return 1.0
    raw = span / 9.0
    base = 10 ** math.floor(math.log10(raw))
    for m in (1.0, 2.0, 2.5, 5.0, 10.0):
        if base * m >= raw * 0.999:
            return base * m
    return base * 10


def _step_digits(step: float) -> int:
    digits = 0
    while digits < 8 and abs(step * 10 ** digits - round(step * 10 ** digits)) > 1e-6 * max(1.0, step * 10 ** digits):
        digits += 1
    return digits


def render(T: dict[str, Any], out: str) -> None:
    """Draw the card ``T`` (tradecard.card_data) into the PNG ``out``."""
    result = T["kind"] == "result"
    long_ = bool(T["long"])
    bars = [list(b) for b in T["bars"]]
    slots = int(T["slots"])
    ke = int(T["k_entry"])
    kx = T.get("k_exit")
    fill, stop = float(T["fill"]), float(T["stop"])
    target = T.get("target")
    box_far = T.get("box_far")

    cv = Canvas()
    L, R = 56, W - 56

    # ---- header ----
    cv.text((L, 58), f"İŞLEM KARTI  ·  #{T['no']}", font(M6, 13), GOLD2, tracking=3.2, anchor="lm")
    x = L
    x += asset_name(cv, (x, 130), str(T["asset"]), 80, INK) + 14
    x += cv.text((x, 130), str(T["side"]), font(SERIF_I, 80), GOLD, anchor="ls") + 22
    pill = str(T["account"])
    pf = font(M6, 13)
    pw = cv.width(pill, pf, 1.6) + 28
    lay, d = cv.layer()
    d.rounded_rectangle((cv.p(x), cv.p(99), cv.p(x + pw), cv.p(125)), radius=cv.p(13), outline=rgba(GOLD, 0.6),
                        width=max(1, cv.p(1)), fill=rgba(GOLD, 0.08))
    cv.put(lay)
    cv.text((x + pw / 2, 112), pill, pf, GOLD, anchor="mm", tracking=1.6)
    cv.text((L, 162), str(T["subtitle"]), font(M4, 15.5), MUTE, anchor="lm")
    if result and T.get("hero"):
        cv.text((R, 58), "SONUÇ", font(M6, 13), GOLD2, anchor="rm", tracking=3.2)
        cv.hero((R, 152), str(T["hero"]), 92)

    # ---- chips ----
    chips = T["chips"]
    cy0, cy1, gap = 200, 286, 16
    cw = (R - L - gap * (len(chips) - 1)) / len(chips)
    for k, (lab, val, sub, dot) in enumerate(chips):
        x0 = L + k * (cw + gap)
        cv.glass((x0, cy0, x0 + cw, cy1), radius=18)
        lf = font(M6, 12)
        cv.text((x0 + 22, cy0 + 23), lab, lf, GOLD2, anchor="lm", tracking=2.6)
        if dot in DOTS:
            cx = x0 + 22 + cv.width(lab, lf, 2.6) + 10
            lay, d = cv.layer()
            d.ellipse((cv.p(cx - 3.5), cv.p(cy0 + 19.5), cv.p(cx + 3.5), cv.p(cy0 + 26.5)), fill=rgba(DOTS[dot], 1))
            cv.put(lay)
        check = val.endswith(" ✓")
        vw = cv.text((x0 + 22, cy0 + 56), val.removesuffix(" ✓"), font(M6, 24), INK, anchor="lm")
        if check:                                  # (the fonts have no check mark glyph: drawn)
            cx, cy = x0 + 22 + vw + 14, cy0 + 56
            lay, d = cv.layer()
            d.line([(cv.p(cx), cv.p(cy)), (cv.p(cx + 6), cv.p(cy + 6)), (cv.p(cx + 17), cv.p(cy - 7))],
                   fill=rgba(UP, 1), width=max(2, cv.p(3)), joint="curve")
            cv.put(lay)
        cv.text((x0 + cw - 20, cy0 + 57), sub, font(M5, 13), MUTE, anchor="rm")

    # ---- chart ----
    px0, py0, px1, py1 = L, 310, R, 886
    cv.glass((px0, py0, px1, py1), radius=28, a=0.045)
    gx0, gx1, gy0, gy1 = px0 + 26, px1 - 122, py0 + 34, py1 - 54
    levels = [fill, stop, *(float(v) for v in (target, box_far, T.get("exit")) if v is not None)]
    lo_raw = min(min(b[3] for b in bars), *levels)
    hi_raw = max(max(b[2] for b in bars), *levels)
    span = max(hi_raw - lo_raw, abs(hi_raw) * 1e-6, 1e-12)
    pad = span * 0.04
    lo, hi = lo_raw - pad, hi_raw + pad

    def X(k: float) -> float:
        return gx0 + (k + 0.5) * (gx1 - gx0) / slots

    def Y(v: float) -> float:
        return gy0 + (hi - v) / (hi - lo) * (gy1 - gy0)

    bw = (gx1 - gx0) / slots
    step = _nice_step(hi - lo)
    axis_digits = _step_digits(step)
    lay, d = cv.layer()
    v = math.floor(lo / step + 1) * step
    ticks = []
    while v < hi:
        d.line((cv.p(gx0), cv.p(Y(v)), cv.p(gx1), cv.p(Y(v))), fill=rgba("#FFFFFF", 0.045), width=1)
        ticks.append(v)
        v += step
    for k, _label in T.get("time_labels") or []:
        d.line((cv.p(X(k)), cv.p(gy0), cv.p(X(k)), cv.p(gy1)), fill=rgba("#FFFFFF", 0.035), width=1)
    cv.put(lay)
    for v in ticks:
        cv.text((gx1 + 24, Y(v)), num(v, axis_digits), font(I5, 13), MUTE, anchor="lm")
    for k, label in T.get("time_labels") or []:
        cv.text((X(k), gy1 + 28), str(label), font(I5, 13), MUTE, anchor="mm")

    end_k = int(T["box_end"])
    xe, xr = X(ke) - bw / 2, X(end_k) + bw / 2
    ye, ys = Y(fill), Y(stop)

    def flat(x0: float, x1: float, y_a: float, y_b: float, color: str, a: float) -> None:
        lay, d = cv.layer()
        top, bot = sorted((y_a, y_b))
        d.rectangle((cv.p(x0), cv.p(top), cv.p(x1), cv.p(bot)), fill=rgba(color, a))
        cv.put(lay)

    flat(xe, xr, ye, ys, STOP_Z, 0.20)
    if box_far is not None:
        yt = Y(float(box_far))
        flat(xe, xr, ye, yt, TARGET_Z, 0.09)
    lay, d = cv.layer()
    d.line((cv.p(xe), cv.p(ys), cv.p(xr), cv.p(ys)), fill=rgba(STOP_Z, 0.95), width=max(2, cv.p(1.6)))
    if box_far is not None:
        d.line((cv.p(xe), cv.p(yt), cv.p(xr), cv.p(yt)), fill=rgba(TARGET_Z, 0.85), width=max(2, cv.p(1.6)))
    cv.put(lay)
    if result and kx is not None and T.get("exit") is not None:
        k_end, p_end = int(kx), float(T["exit"])
        sgn = 1 if long_ else -1
        x_exit = X(k_end) + bw / 2
        yx = Y(p_end)
        flat(xe, x_exit, ye, yx, TARGET_Z if sgn * (p_end - fill) >= 0 else STOP_Z, 0.17)
        x_a, y_a, x_b, y_b = X(ke), ye, X(k_end), yx
        lay, d = cv.layer()
        length = ((x_b - x_a) ** 2 + (y_b - y_a) ** 2) ** 0.5
        t = 0.0
        while length > 0 and t < length:
            t1 = min(length, t + 9.0)
            d.line((cv.p(x_a + (x_b - x_a) * t / length), cv.p(y_a + (y_b - y_a) * t / length),
                    cv.p(x_a + (x_b - x_a) * t1 / length), cv.p(y_a + (y_b - y_a) * t1 / length)),
                   fill=rgba(INK, 0.85), width=max(2, cv.p(1.6)))
            t = t1 + 7.0
        cv.put(lay)
    lay, d = cv.layer()
    d.line((cv.p(xe), cv.p(ye), cv.p(xr), cv.p(ye)), fill=rgba(GOLD, 0.9), width=cv.p(5))
    cv.put(lay, blur=4)
    lay, d = cv.layer()
    d.line((cv.p(xe), cv.p(ye), cv.p(xr), cv.p(ye)), fill=rgba(GOLD, 1), width=max(2, cv.p(1.8)))
    cv.put(lay)

    # candles
    lay, d = cv.layer()
    body = max(2.0, bw * 0.64)
    for k, b in enumerate(bars):
        _t, o, h, low, c = b[:5]
        col = UP if c >= o else DOWN
        xk = X(k)
        d.line((cv.p(xk), cv.p(Y(h)), cv.p(xk), cv.p(Y(low))), fill=rgba(col, 0.95), width=max(1, cv.p(1.25)))
        top, bot = Y(max(o, c)), Y(min(o, c))
        bot = max(bot, top + 1.4)
        d.rounded_rectangle((cv.p(xk - body / 2), cv.p(top), cv.p(xk + body / 2), cv.p(bot)), radius=cv.p(1.4),
                            fill=rgba(col, 1))
    cv.put(lay)

    def arrow(k: int, down: bool, color: str) -> None:
        """A thin shaft + chevron arrow: above the candle pointing down, or below it pointing up."""
        if not 0 <= k < len(bars):
            return
        xa = X(k)
        lay, d = cv.layer()
        w = max(2, cv.p(2.4))
        if down:
            tip = Y(bars[k][2]) - 12
            tail = tip - 26
            d.line((cv.p(xa), cv.p(tail), cv.p(xa), cv.p(tip)), fill=rgba(color, 1), width=w)
            d.line([(cv.p(xa - 8), cv.p(tip - 8)), (cv.p(xa), cv.p(tip)), (cv.p(xa + 8), cv.p(tip - 8))],
                   fill=rgba(color, 1), width=w, joint="curve")
        else:
            tip = Y(bars[k][3]) + 12
            tail = tip + 26
            d.line((cv.p(xa), cv.p(tail), cv.p(xa), cv.p(tip)), fill=rgba(color, 1), width=w)
            d.line([(cv.p(xa - 8), cv.p(tip + 8)), (cv.p(xa), cv.p(tip)), (cv.p(xa + 8), cv.p(tip + 8))],
                   fill=rgba(color, 1), width=w, joint="curve")
        cv.put(lay)

    # entry: short = sell (red, down, above), long = buy (green, up, below); the exit the opposite way
    if T.get("arrow_entry", True):
        arrow(ke, not long_, ARROW_UP if long_ else ARROW_DN)
    if result and kx is not None:
        arrow(int(kx), long_, ARROW_DN if long_ else ARROW_UP)
    if not result and T.get("last_price") is not None:
        # the price now: a dotted line from the last candle across the room on the right
        last_c = float(T["last_price"])
        lay, d = cv.layer()
        xx = X(len(bars) - 1) + bw
        while xx < gx1:
            d.line((cv.p(xx), cv.p(Y(last_c)), cv.p(min(xx + 3, gx1)), cv.p(Y(last_c))), fill=rgba(INK, 0.35),
                   width=1)
            xx += 7
        cv.put(lay)

    # ---- timeline ----
    ty = 936
    footer = str(T.get("footer") or "")
    ff = font(M5, 13)
    limit = R - cv.width(footer, ff, 1.2) - 40
    steps = _fitting_steps(cv, [tuple(s) for s in T.get("timeline") or []], L, limit)
    x = L
    for k, (hm, what) in enumerate(steps):
        lay, d = cv.layer()
        d.ellipse((cv.p(x), cv.p(ty - 6), cv.p(x + 12), cv.p(ty + 6)), fill=rgba(GOLD, 1))
        cv.put(lay)
        x += 24
        if hm:
            x += cv.text((x, ty), hm, font(M6, 15), INK, anchor="lm") + 10
        x += cv.text((x, ty), what, font(M4, 15), MUTE, anchor="lm") + 22
        if k < len(steps) - 1:
            lay, d = cv.layer()
            d.line((cv.p(x), cv.p(ty), cv.p(x + 30), cv.p(ty)), fill=rgba(GOLD, 0.35), width=max(1, cv.p(1)))
            cv.put(lay)
            x += 52
    cv.text((R, ty), footer, ff, MUTE, anchor="rm", tracking=1.2)

    cv.img.convert("RGB").save(out, format="PNG", optimize=True)


def _steps_width(cv: Canvas, steps: list[tuple[str, str]]) -> float:
    total = 0.0
    for k, (hm, what) in enumerate(steps):
        total += 24 + (cv.width(hm, font(M6, 15)) + 10 if hm else 0) + cv.width(what, font(M4, 15)) + 22
        if k < len(steps) - 1:
            total += 52
    return total


def _fitting_steps(cv: Canvas, steps: list[tuple[str, str]], left: float, limit: float) -> list[tuple[str, str]]:
    """The timeline's events that fit before the footer: the first and the last always; events in between are
    left out from the second-to-last backwards (the oldest moves stay) until the line fits."""
    steps = list(steps)
    while len(steps) > 2 and left + _steps_width(cv, steps) > limit:
        del steps[-2]
    return steps


__all__ = ["LNUM", "SIZE", "asset_name", "render"]
