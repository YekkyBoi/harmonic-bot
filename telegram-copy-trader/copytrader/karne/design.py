# -*- coding: utf-8 -*-
"""
[bot] copytrader/karne/design.py: the designer's module (karne.py) as delivered, with the changes marked "[bot]" in
the code (all of them in the shared helpers and in the one layout the bot draws):

* a trade or an account whose R is not known (``r`` is None) is drawn with "—" and stays out of every total, rate
  and best / worst;
* a trade or an account whose R is an approximation (``approx`` true) is drawn with a leading "≈", and so is every
  figure that includes one (the total, the win rate, best / worst);
* a trade or an account figure that leaves out an account whose R is not known (``partial`` true) is drawn with a
  "*" behind it;
* a break-even trade (``outcome`` "be") is printed "0.0R" whatever its third decimal says;
* the lines under the table that say what the marks mean, the line that counts the rows the table does not show
  and best / worst can be GIVEN by the data (``footnotes``, ``more_text``, ``best`` / ``worst``): the bot gives
  them, so that the picture says the same as its caption and the text card; without them ``_footnotes`` counts;
* the total is summed with math.fsum (the same figure in whatever order the rows are given);
* the figures of the WHOLE period can be given (``aggregates``: wins, losses, be, unknown, approx, partial, total_r,
  win_rate, best, worst) together with the number of rows that are not in ``trades`` (``hidden``): the bot hands
  over only the rows that are drawn, however long the period is, and the card still shows the period's figures;
* the labels "HAFTALIK SONUÇ", "HAFTANIN İŞLEMLERİ" and "Bu hafta işlem yok" can be replaced by the data
  (``hero_label``, ``table_label``, ``empty_text``) for a card that is not about one week;
* the background chart is decoration made from a seeded random series: its price labels ("125.00" ...) are drawn
  for the designer's SAMPLE data only (``sample`` true); a real card (``sample`` false) has the faint price lines
  and no numbers on them, so that no made-up figure is ever on a card of real results;
* without accounts (a bot that only simulates has no real account) the "HESAPLAR" section is not drawn.

Data WITHOUT such entries is drawn exactly as by the designer's module: tests/test_karne_render.py compares the
sample card with the designer's reference render pixel for pixel.
The bot draws ONE design, ``copytrader.karne.PRESET`` (the designer's FINAL_PRESET), and ships only the fonts that
design loads (fonts/, with their OFL licence texts); the other designs of this module are not used and their fonts
are not shipped. Imported only by the render process (``python -m copytrader.karne``), never by the bot itself:
Pillow is optional and drawing takes seconds of CPU.

karne.py - haftalik "BABUR'S TRADE PERFORMANCE" karnesi (yalnizca Pillow).

Kullanim:
    from karne import render, sample_data
    render(sample_data(), 7, "karne.png")                 # galeri numarasi 1..24 (PRESETS)
    render(sample_data(), 7, "karne.png", out_scale=2)    # ayni tasarim, 2160 px
    # eski tasarimlar: "G1".."G4" (2. tur cam) | "A" | "B" | "C" (1. tur)

Bagimlilik: yalnizca Pillow (>= 9.2). Tarayici, matplotlib, numpy yok.
Fontlar bu dosyanin yanindaki fonts/ klasorunden yuklenir (hepsi SIL OFL 1.1).

Metin yerlesimi bilerek ImageFont.Layout.BASIC ile yapilir; boylece Pillow'un
libraqm ile derlenip derlenmedigine bakmadan Linux ve Windows'ta ayni cikti
uretilir. Bedeli: OpenType ozellikleri (kerning, tnum) kullanilmaz; tablo
rakamlari burada elle sabit genislikli hucrelere yerlestirilir.

data sozlugu (bkz. sample_data):
    title, subtitle, footer, sample (bool)
    trades:   [{date, pair, side, r, note, outcome?}]  outcome: win|loss|be
    accounts: [{name, r, usd?}]      (G1..G4 usd'yi hic gostermez)
    Kurallar:
        total_r  = islemlerin R toplami (trades varsa her zaman buradan hesaplanir)
        win_rate = kazanc / (kazanc + kayip); basabas islemler haric (verilirse o kullanilir)
        wins, losses, be, best, worst verilmezse trades'ten hesaplanir
    istege bagli bicim: r_decimals (1), decimal_sep (".")
"""
from __future__ import annotations

import datetime as _dt
import math
import os
import random
from functools import lru_cache

from PIL import Image, ImageChops, ImageDraw, ImageEnhance, ImageFilter, ImageFont

__all__ = ["render", "sample_data", "sample_negative", "summarize", "DESIGNS", "PRESETS"]

W = 1080                      # cikti genisligi (px)
MAX_ROWS = 20                 # tabloda gosterilen en fazla islem satiri
FONTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fonts")

SERIF_D = "PlayfairDisplay_600SemiBold"
SERIF_C = "CormorantGaramond_600SemiBold"
SERIF_CB = "CormorantGaramond_700Bold"
SERIF_CI = "CormorantGaramond_600SemiBold_Italic"
INTER = {300: "Inter_300Light", 400: "Inter_400Regular", 500: "Inter_500Medium",
         600: "Inter_600SemiBold", 700: "Inter_700Bold"}
MONT = {300: "Montserrat_300Light", 400: "Montserrat_400Regular", 500: "Montserrat_500Medium",
        600: "Montserrat_600SemiBold", 700: "Montserrat_700Bold"}

_DAYS_TR = ["Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz"]
_DIGITS = "0123456789"


# --------------------------------------------------------------------------
# kucuk yardimcilar
# --------------------------------------------------------------------------
def tr_upper(s: str) -> str:
    """Turkce kurallariyla buyuk harf (i -> İ, ı -> I)."""
    return s.replace("i", "İ").replace("ı", "I").upper()


def _rgb(c):
    if isinstance(c, str):
        c = c.lstrip("#")
        return tuple(int(c[i:i + 2], 16) for i in (0, 2, 4))
    return tuple(c[:3])


def _rgba(c, a=1.0):
    if c is None:
        return None
    r, g, b = _rgb(c)
    return (r, g, b, int(round(255 * a)))


def _to_date(v):
    if isinstance(v, _dt.datetime):
        return v.date()
    if isinstance(v, _dt.date):
        return v
    return _dt.date.fromisoformat(str(v)[:10])


def fmt_num(v: float, dec: int = 1, sep: str = ".") -> str:
    """Isaretli sayi: +6.4 / −1.0 / 0.0 (eksi icin U+2212)."""
    v = round(float(v), dec)
    body = f"{abs(v):.{dec}f}".replace(".", sep)
    if v > 0:
        return "+" + body
    if v < 0:
        return "−" + body
    return body


def fmt_r(v, dec=1, sep=".") -> str:
    return fmt_num(v, dec, sep) + "R"


def fmt_usd(v: float) -> str:
    v = float(v)
    a = abs(v)
    body = f"{a:,.0f}" if (a >= 100 or abs(a - round(a)) < 0.005) else f"{a:,.2f}"
    sign = "+" if v > 0.004 else ("−" if v < -0.004 else "")
    return f"{sign}${body}"


def fmt_pct(v: float) -> str:
    return f"%{int(math.floor(float(v) + 0.5))}"      # Turkce yazim: %63 (yarim yukari yuvarlanir)


def _outcome(t) -> str:
    if "r" in t and t["r"] is None:                 # [bot] R bilinmiyor: tabloda "—", toplamlara girmez
        return "unknown"
    oc = (t.get("outcome") or "").lower()
    if oc in ("win", "loss", "be"):
        return oc
    r = float(t.get("r", 0))
    return "win" if r > 0.049 else ("loss" if r < -0.049 else "be")


def summarize(data: dict) -> dict:
    """Eksik ozet alanlarini islemlerden hesaplar; girdiyi degistirmez."""
    d = dict(data)
    trades = []
    for t in d.get("trades", []):
        t = dict(t)
        t["outcome"] = _outcome(t)
        t["r"] = float(t.get("r", 0)) if t["outcome"] != "unknown" else None        # [bot]
        t["side"] = str(t.get("side", "")).upper()
        t["pair"] = str(t.get("pair", ""))
        t["note"] = str(t.get("note", "") or "")
        trades.append(t)
    d["trades"] = trades
    # [bot] R'si bilinmeyen islemler listelenir ama hicbir toplama / orana / en iyi-en kotuye girmez;
    # R'si yaklasik olanlar girer ve sayilir (onlari iceren her rakam "≈" ile cizilir)
    d["unknown"] = sum(t["outcome"] == "unknown" for t in trades)
    listed, trades = trades, [t for t in trades if t["outcome"] != "unknown"]
    d["approx"] = sum(bool(t.get("approx")) for t in trades)
    d["partial"] = sum(bool(t.get("partial")) for t in trades)
    n = len(trades)
    wins = sum(t["outcome"] == "win" for t in trades)
    losses = sum(t["outcome"] == "loss" for t in trades)
    be = n - wins - losses
    d.setdefault("wins", wins)
    d.setdefault("losses", losses)
    d.setdefault("be", be)
    d["n"] = d["wins"] + d["losses"] + d["be"] if "n_trades" not in d else d["n_trades"]
    # Toplam R = islemlerin R toplami (islem listesi varsa disaridan verilen deger yok sayilir).
    if listed or "total_r" not in d:                # [bot] (R'si bilinen islemlerin toplami; fsum: sira bagimsiz)
        d["total_r"] = math.fsum(t["r"] for t in trades)
    # Kazanma orani = kazanc / (kazanc + kayip); basabas islemler HARIC.
    d["decided"] = d["wins"] + d["losses"]
    d.setdefault("win_rate", (100.0 * d["wins"] / d["decided"]) if d["decided"] else None)
    d.setdefault("best", max(trades, key=lambda t: t["r"]) if trades else None)
    d.setdefault("worst", min(trades, key=lambda t: t["r"]) if trades else None)
    accounts = [dict(a) for a in d.get("accounts", [])]
    d["accounts"] = accounts
    if "total_usd" not in d:
        usd = [a.get("usd") for a in accounts if a.get("usd") is not None]
        d["total_usd"] = sum(usd) if usd else None
    d.setdefault("title", "BABÜR'S TRADE PERFORMANCE")
    d.setdefault("subtitle", "HAFTALIK KARNE")
    d.setdefault("footer", "Copy Trader · otomatik rapor")
    d.setdefault("r_decimals", 1)
    d.setdefault("decimal_sep", ".")
    d["title"] = d["title"].replace("'", "’")      # tipografik kesme
    if isinstance(d.get("aggregates"), dict):       # [bot] donemin TUM islemlerinin rakamlari (trades: cizilen satirlar)
        d.update(d["aggregates"])
        d["n"] = d["wins"] + d["losses"] + d["be"]
        d["decided"] = d["wins"] + d["losses"]
    return d


def sample_data(n: int = 9) -> dict:
    """Ornek veri. n == 9 icin maket verisi; diger n'ler icin test verisi."""
    base = [
        ("2026-11-09", "ETH", "LONG", 2.1, "TP2"),
        ("2026-11-09", "EURUSD", "SHORT", -1.0, "SL"),
        ("2026-11-10", "BTC", "LONG", 1.5, "TP1"),
        ("2026-11-11", "XAUUSD", "LONG", 3.0, "TP1 %50 → TP2"),      # kismi kar al ornegi
        ("2026-11-11", "SOL", "SHORT", -1.0, "SL"),
        ("2026-11-12", "GBPUSD", "LONG", 0.0, "BE"),
        ("2026-11-13", "BTC", "SHORT", 1.8, "TP2"),
        ("2026-11-13", "XAUUSD", "SHORT", -1.0, "SL"),
        ("2026-11-14", "ETH", "LONG", 0.8, "TP1"),
    ]
    if n == 9:
        rows = base
        accounts = [
            {"name": "binance", "r": 5.2},
            {"name": "fx", "r": -1.0},
            {"name": "tickmill", "r": 2.0},
        ]
    else:
        extra = [
            ("2026-11-10", "USDJPY", "SHORT", 1.2, "TP1"),
            ("2026-11-12", "AVAX", "LONG", -1.0, "SL"),
            ("2026-11-12", "NAS100", "LONG", 2.4, "TP2"),
            ("2026-11-13", "DOGE", "SHORT", 0.0, "BE"),
            ("2026-11-14", "XAGUSD", "LONG", -0.5, "Kısmi SL"),
            ("2026-11-15", "BNB", "LONG", 1.1, "TP1"),
        ]
        pool = base + extra
        rows = sorted((pool[i % len(pool)] for i in range(n)), key=lambda r: r[0])
        tot = sum(r[3] for r in rows)
        accounts = [
            {"name": "binance", "r": round(tot * 0.6, 1)},
            {"name": "fx", "r": round(tot * 0.1, 1)},
            {"name": "tickmill", "r": round(tot - round(tot * 0.6, 1) - round(tot * 0.1, 1), 1)},
        ]
    return {
        "title": "BABÜR'S TRADE PERFORMANCE",
        "subtitle": "HAFTALIK KARNE · 9–15 Kasım 2026",
        "footer": "Copy Trader · otomatik rapor",
        "sample": True,
        "accounts": accounts,
        "trades": [dict(date=a, pair=b, side=c, r=r, note=nt) for a, b, c, r, nt in rows],
    }


# --------------------------------------------------------------------------
# font / degrade yardimcilari
# --------------------------------------------------------------------------
@lru_cache(maxsize=256)
def _font(name: str, px: int) -> ImageFont.FreeTypeFont:
    path = os.path.join(FONTS_DIR, name + ".ttf")
    return ImageFont.truetype(path, px, layout_engine=ImageFont.Layout.BASIC)


def _lerp(a, b, t):
    return tuple(int(round(a[i] + (b[i] - a[i]) * t)) for i in range(3))


def _strip(stops, n=256) -> Image.Image:
    """stops: [(0..1, renk)] -> n x 1 RGB serit."""
    stops = [(p, _rgb(c)) for p, c in stops]
    im = Image.new("RGB", (n, 1))
    px = im.load()
    for i in range(n):
        t = i / (n - 1)
        for j in range(len(stops) - 1):
            p0, c0 = stops[j]
            p1, c1 = stops[j + 1]
            if t <= p1 or j == len(stops) - 2:
                u = 0.0 if p1 == p0 else min(1.0, max(0.0, (t - p0) / (p1 - p0)))
                px[i, 0] = _lerp(c0, c1, u)
                break
    return im


def _gradient(size, stops, shear=0.0, vertical=False) -> Image.Image:
    w, h = max(1, int(size[0])), max(1, int(size[1]))
    s = _strip(stops)
    if vertical:
        s = s.transpose(Image.Transpose.TRANSPOSE)
        return s.resize((w, h), Image.Resampling.BILINEAR)
    if not shear:
        return s.resize((w, h), Image.Resampling.BILINEAR)
    ext = int(abs(shear) * h) + 1
    big = s.resize((w + ext, h), Image.Resampling.BILINEAR)
    return big.transform((w, h), Image.Transform.AFFINE, (1, shear, 0, 0, 1, 0),
                         resample=Image.Resampling.BILINEAR)


# --------------------------------------------------------------------------
# tuval: mantiksal (1080 px) koordinat -> S kat buyuk cizim -> kucultme
# --------------------------------------------------------------------------
class Canvas:
    def __init__(self, w, h, bg, scale=2, dry=False):
        self.S, self.w, self.h, self.dry = scale, w, h, dry
        self.out = 1                      # cikti olcegi (render ayarlar); snap() icin
        self.img = Image.new("RGB", (w * scale, int(round(h * scale))), _rgb(bg))
        self.d = ImageDraw.Draw(self.img, "RGBA")

    # -- olcu ---------------------------------------------------------------
    def p(self, v):
        return int(round(v * self.S))

    def snap(self, v):
        """mantiksal degeri cikti piksel izgarasina oturtur (keskin dikdortgenler icin)"""
        return round(v * self.out) / float(self.out)

    def f(self, name, size):
        return _font(name, max(1, int(round(size * self.S))))

    def _adv(self, font, s, tr, tab):
        """karakter ilerlemeleri (cihaz px)"""
        if not tr and not tab:
            return None
        dig = max(font.getlength(c) for c in _DIGITS) if tab else 0
        sgn = max(font.getlength("+"), font.getlength("−")) if tab else 0
        out = []
        cut = max(s.rfind("."), s.rfind(",")) if tab == "dec" else -1
        for i, ch in enumerate(s):
            g = font.getlength(ch)
            if tab == "dec":
                # ondalik hane: yari sabit hucre (dar "1" icin bosluk/hiza dengesi)
                cell = g + 0.5 * (dig - g) if (ch in _DIGITS and i > cut >= 0) else g
            elif tab and ch in _DIGITS:
                cell = dig
            elif tab and ch in "+−":
                cell = sgn
            else:
                cell = g
            out.append((ch, g, cell))
        return out

    def _width(self, font, s, tr, tab):
        adv = self._adv(font, s, tr, tab)
        if adv is None:
            return font.getlength(s)
        return sum(c for _, _, c in adv) + tr * max(0, len(adv) - 1)

    def tw(self, s, name, size, tracking=0, tab=False):
        return self._width(self.f(name, size), s, tracking * self.S, tab) / self.S

    def fit(self, s, name, size, max_w, tracking=0, min_size=10, tab=False):
        """max_w'ye sigan en buyuk punto (size'dan kucultur)."""
        while size > min_size and self.tw(s, name, size, tracking, tab) > max_w:
            size -= 0.5
        return size

    def ellip(self, s, name, size, max_w, tracking=0):
        if self.tw(s, name, size, tracking) <= max_w:
            return s
        while len(s) > 1 and self.tw(s + "…", name, size, tracking) > max_w:
            s = s[:-1]
        return s.rstrip() + "…"

    # -- metin --------------------------------------------------------------
    def _run(self, draw, x, y, s, font, fill, tr, tab):
        adv = self._adv(font, s, tr, tab)
        if adv is None:
            draw.text((x, y), s, font=font, fill=fill, anchor="ls")
            return
        for ch, g, cell in adv:
            draw.text((x + (cell - g) / 2.0, y), ch, font=font, fill=fill, anchor="ls")
            x += cell + tr

    def text(self, x, y, s, name, size, fill, anchor="l", tracking=0, tab=False, alpha=1.0):
        """y = satir tabani (baseline). anchor: l | m | r. Genisligi dondurur."""
        font = self.f(name, size)
        tr = tracking * self.S
        w = self._width(font, s, tr, tab)
        x0 = x * self.S - (w if anchor == "r" else w / 2.0 if anchor == "m" else 0)
        self._run(self.d, x0, y * self.S, s, font, _rgba(fill, alpha), tr, tab)
        return w / self.S

    def halo(self, x, y, s, name, size, color, anchor="l", tracking=0, blur=12, alpha=0.7):
        """Metnin arkasina yumusak koyu hale: arka plan deseni yaziyla yarismasin."""
        if self.dry:
            return
        font = self.f(name, size)
        tr = tracking * self.S
        w = self._width(font, s, tr, False)
        x0 = x * self.S - (w if anchor == "r" else w / 2.0 if anchor == "m" else 0)
        asc, desc = font.getmetrics()
        pad = self.p(blur) * 3
        mask = Image.new("L", (int(w) + 2 * pad, asc + desc + 2 * pad), 0)
        self._run(ImageDraw.Draw(mask), pad, pad + asc, s, font, 255, tr, False)
        mask = mask.filter(ImageFilter.GaussianBlur(self.p(blur)))
        mask = mask.point(lambda v: int(min(255, v * 2.6) * alpha))
        ox, oy = int(round(x0)) - pad, int(round(y * self.S)) - asc - pad
        self.img.paste(_rgb(color), (ox, oy, ox + mask.size[0], oy + mask.size[1]), mask)

    def foil(self, x, y, s, name, size, stops, anchor="l", tracking=0, tab=False, shear=0.4,
             vertical=False):
        """Degrade (varak) dolgulu metin."""
        font = self.f(name, size)
        tr = tracking * self.S
        w = self._width(font, s, tr, tab)
        x0 = x * self.S - (w if anchor == "r" else w / 2.0 if anchor == "m" else 0)
        asc, desc = font.getmetrics()
        pad = int(size * self.S * 0.35) + 4
        mask = Image.new("L", (int(w) + 2 * pad, asc + desc + 2 * pad), 0)
        self._run(ImageDraw.Draw(mask), pad, pad + asc, s, font, 255, tr, tab)
        g = _gradient(mask.size, stops, shear=shear, vertical=vertical)
        self.img.paste(g, (int(round(x0)) - pad, int(round(y * self.S)) - asc - pad), mask)
        return w / self.S

    # -- sekiller -----------------------------------------------------------
    def line(self, x1, y1, x2, y2, fill, width=1.0, alpha=1.0):
        self.d.line([(self.p(x1), self.p(y1)), (self.p(x2), self.p(y2))],
                    fill=_rgba(fill, alpha), width=max(1, self.p(width)))

    def hline(self, x1, x2, y, fill, width=1.0, alpha=1.0):
        wpx = max(1, self.p(width))
        y0 = self.p(y) - wpx // 2
        self.d.rectangle([self.p(x1), y0, self.p(x2) - 1, y0 + wpx - 1], fill=_rgba(fill, alpha))

    def vline(self, x, y1, y2, fill, width=1.0, alpha=1.0):
        wpx = max(1, self.p(width))
        x0 = self.p(x) - wpx // 2
        self.d.rectangle([x0, self.p(y1), x0 + wpx - 1, self.p(y2) - 1], fill=_rgba(fill, alpha))

    def dots(self, x1, x2, y, fill, step=7.0, r=0.9, alpha=1.0):
        """noktali kilavuz cizgisi (saga hizali baslar)"""
        x = x2
        while x >= x1:
            self.d.ellipse([self.p(x - r), self.p(y - r), self.p(x + r), self.p(y + r)],
                           fill=_rgba(fill, alpha))
            x -= step

    def rect(self, box, fill=None, outline=None, width=1.0, radius=0, fa=1.0, oa=1.0):
        b = [self.p(v) for v in box]
        b[2] -= 1
        b[3] -= 1
        kw = dict(fill=_rgba(fill, fa), outline=_rgba(outline, oa),
                  width=max(1, self.p(width)) if outline else 0)
        if radius:
            self.d.rounded_rectangle(b, radius=self.p(radius), **kw)
        else:
            self.d.rectangle(b, **kw)

    def poly(self, pts, fill=None, outline=None, width=1.0, alpha=1.0):
        pp = [(v[0] * self.S, v[1] * self.S) for v in pts]
        if fill:
            self.d.polygon(pp, fill=_rgba(fill, alpha))
        if outline:
            self.d.line(pp + [pp[0], pp[1]], fill=_rgba(outline, alpha),
                        width=max(1, self.p(width)), joint="curve")

    def diamond(self, cx, cy, r, fill=None, outline=None, width=1.0, alpha=1.0):
        self.poly([(cx, cy - r), (cx + r, cy), (cx, cy + r), (cx - r, cy)], fill, outline, width, alpha)

    def circle(self, cx, cy, r, fill=None, outline=None, width=1.0, alpha=1.0):
        self.d.ellipse([self.p(cx - r), self.p(cy - r), self.p(cx + r), self.p(cy + r)],
                       fill=_rgba(fill, alpha), outline=_rgba(outline, alpha),
                       width=max(1, self.p(width)) if outline else 0)

    def square(self, cx, cy, r, fill=None, outline=None, width=1.0, alpha=1.0):
        self.rect((cx - r, cy - r, cx + r, cy + r), fill=fill, outline=outline, width=width,
                  fa=alpha, oa=alpha)

    def glow(self, cx, cy, rx, ry, color, strength=1.0):
        """yumusak radyal isik"""
        # radial_gradient kenar ortasinda 181, kosede 255 verir; 181'de sifira inecek sekilde olcekle
        m = Image.radial_gradient("L").point(lambda v: int(max(0.0, (181 - v) / 181.0) ** 1.6 * 255 * min(1.0, strength)))
        m = m.resize((max(2, self.p(2 * rx)), max(2, self.p(2 * ry))), Image.Resampling.BILINEAR)
        self.img.paste(Image.new("RGB", m.size, _rgb(color)), (self.p(cx - rx), self.p(cy - ry)), m)

    def fill_gradient(self, box, stops, shear=0.0, vertical=False):
        x1, y1, x2, y2 = [self.p(v) for v in box]
        self.img.paste(_gradient((x2 - x1, y2 - y1), stops, shear, vertical), (x1, y1))

    def ring(self, cx, cy, r, width, frac, track, color, track_alpha=1.0):
        """yuzde halkasi (4x ornekleme ile puruzsuz)"""
        k = 4
        pad = width
        size = int((2 * r + 2 * pad) * k)
        lay = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        dr = ImageDraw.Draw(lay)
        c = size / 2.0
        rr = r * k
        wd = int(width * k)
        box = [c - rr, c - rr, c + rr, c + rr]
        dr.ellipse(box, outline=_rgba(track, track_alpha), width=wd)
        frac = max(0.0, min(1.0, frac))
        if frac > 0:
            import math
            dr.arc(box, -90, -90 + 360 * frac, fill=_rgba(color), width=wd)
            rm = rr - wd / 2.0
            for ang in (-90, -90 + 360 * frac):
                ex = c + rm * math.cos(math.radians(ang))
                ey = c + rm * math.sin(math.radians(ang))
                dr.ellipse([ex - wd / 2.0, ey - wd / 2.0, ex + wd / 2.0, ey + wd / 2.0], fill=_rgba(color))
        out = int(round((2 * r + 2 * pad) * self.S))
        lay = lay.resize((out, out), Image.Resampling.LANCZOS)
        self.img.paste(lay.convert("RGB"), (self.p(cx - r - pad), self.p(cy - r - pad)), lay.getchannel("A"))

    def stroke_text(self, x, y, s, name, size, stops, stroke=2.0, fill_a=0.16, anchor="l"):
        """Kontur (dis cizgi) rakam: ici hafif dolgulu, cizgisi degrade."""
        font = self.f(name, size)
        w = font.getlength(s)
        x0 = x * self.S - (w if anchor == "r" else w / 2.0 if anchor == "m" else 0)
        asc, desc = font.getmetrics()
        sw = max(1, self.p(stroke))
        pad = int(size * self.S * 0.35) + 4
        mask = Image.new("L", (int(w) + 2 * pad, asc + desc + 2 * pad), 0)
        ImageDraw.Draw(mask).text((pad, pad + asc), s, font=font, fill=int(255 * fill_a), anchor="ls",
                                  stroke_width=sw, stroke_fill=255)
        g = _gradient(mask.size, stops, vertical=True)
        self.img.paste(g, (int(round(x0)) - pad, int(round(y * self.S)) - asc - pad), mask)
        return w / self.S

    def beam(self, color, strength=0.10, pos=0.42, width=0.16, shear=0.9):
        """capraz yumusak isik huzmesi (sag ustten sol alta)"""
        if self.dry:
            return
        a = int(255 * strength)
        st = [(0, (0,) * 3), (max(0.0, pos - width), (0,) * 3), (pos, (a,) * 3), (min(1.0, pos + width), (0,) * 3),
              (1, (0,) * 3)]
        m = _gradient(self.img.size, st, shear=-shear * self.img.size[0] / float(self.img.size[1]) * 0.6).convert("L")
        self.img.paste(_rgb(color), (0, 0) + self.img.size, m)

    def vignette(self, strength=0.35):
        """kenarlari hafifce karartir"""
        if self.dry:
            return
        m = Image.radial_gradient("L").point(lambda v: int(max(0.0, (v - 105) / 150.0) ** 1.5 * 255 * strength))
        m = m.resize(self.img.size, Image.Resampling.BILINEAR)
        self.img.paste((0, 0, 0), (0, 0) + self.img.size, m)

    def grain(self, amount=3, seed=7):
        """tohumlu (her calismada ayni) ince film greni; yalnizca parlaklik kanalinda"""
        if self.dry or not amount:
            return
        rng = random.Random(seed)
        n = 384
        tile = Image.frombytes("L", (n, n), bytes(bytearray(rng.getrandbits(8) for _ in range(n * n))))
        u = max(1, int(round(self.S / float(self.out))))
        if u > 1:
            tile = tile.resize((n * u, n * u), Image.Resampling.NEAREST)
        full = Image.new("L", self.img.size)
        for yy in range(0, self.img.size[1], tile.size[1]):
            for xx in range(0, self.img.size[0], tile.size[0]):
                full.paste(tile, (xx, yy))
        pos = full.point(lambda v: int(max(0, v - 128) / 127.0 * amount))
        neg = full.point(lambda v: int(max(0, 128 - v) / 128.0 * amount))
        rgb = lambda m: Image.merge("RGB", (m, m, m))
        self.img.paste(ImageChops.subtract(ImageChops.add(self.img, rgb(pos)), rgb(neg)))

    def ring_ticks(self, cx, cy, r, frac, color, n=44, length=13, width=3.2, off="#FFFFFF", off_a=0.14):
        """cizgi cizgi (kadran) yuzde halkasi"""
        if self.dry:
            return
        k = 4
        pad = 6
        size = int((2 * r + 2 * pad) * k)
        c = size / 2.0
        on = Image.new("L", (size, size), 0)
        of = Image.new("L", (size, size), 0)
        frac = max(0.0, min(1.0, frac))
        for i in range(n):
            ang = math.radians(-90 + 360.0 * (i + 0.5) / n)
            d = ImageDraw.Draw(on if (i + 0.5) / n <= frac + 1e-9 else of)
            r0, r1 = (r - length) * k, r * k
            d.line([(c + r0 * math.cos(ang), c + r0 * math.sin(ang)), (c + r1 * math.cos(ang), c + r1 * math.sin(ang))],
                   fill=255, width=int(width * k))
        out = int(round((2 * r + 2 * pad) * self.S))
        pos = (self.p(cx - r - pad), self.p(cy - r - pad))
        bx = pos + (pos[0] + out, pos[1] + out)
        rs = lambda im: im.resize((out, out), Image.Resampling.LANCZOS)
        self.img.paste(_rgb(off), bx, rs(of).point(lambda v: int(v * off_a)))
        self.img.paste(_rgb(color), bx, rs(on))

    def ring2(self, cx, cy, r, width, frac, c0, c1, track="#FFFFFF", track_a=0.10, track_w=None, glow_a=0.30):
        """Renk gecisli yuzde halkasi: ince iz + kalin yay + yumusak isilti."""
        if self.dry:
            return
        k = 4
        pad = width * 2.5
        size = int((2 * r + 2 * pad) * k)
        c = size / 2.0
        wd = int(width * k)
        tw = int((track_w or width * 0.45) * k)
        rc = r * k - wd / 2.0                              # yay merkez yaricapi
        box = [c - r * k, c - r * k, c + r * k, c + r * k]
        tmask = Image.new("L", (size, size), 0)
        ImageDraw.Draw(tmask).ellipse([c - rc - tw / 2.0, c - rc - tw / 2.0, c + rc + tw / 2.0, c + rc + tw / 2.0],
                                      outline=255, width=tw)
        amask = Image.new("L", (size, size), 0)
        col = Image.new("RGB", (size, size), _rgb(c0))
        frac = max(0.0, min(1.0, frac))
        if frac > 0:
            da, dc = ImageDraw.Draw(amask), ImageDraw.Draw(col)
            a_end = -90 + 360 * frac
            da.arc(box, -90, a_end, fill=255, width=wd)
            steps = max(2, int(160 * frac))
            for i in range(steps):
                dc.arc(box, -90 + 360 * frac * i / steps, -90 + 360 * frac * (i + 1) / steps + 1.2,
                       fill=_lerp(_rgb(c0), _rgb(c1), (i + 0.5) / steps), width=wd)
            for ang, cc in ((-90, c0), (a_end, c1)):
                ex = c + rc * math.cos(math.radians(ang))
                ey = c + rc * math.sin(math.radians(ang))
                eb = [ex - wd / 2.0, ey - wd / 2.0, ex + wd / 2.0, ey + wd / 2.0]
                da.ellipse(eb, fill=255)
                dc.ellipse(eb, fill=_rgb(cc))
        out = int(round((2 * r + 2 * pad) * self.S))
        pos = (self.p(cx - r - pad), self.p(cy - r - pad))
        rs = lambda im: im.resize((out, out), Image.Resampling.LANCZOS)
        self.img.paste(_rgb(track), pos + (pos[0] + out, pos[1] + out), rs(tmask).point(lambda v: int(v * track_a)))
        if frac > 0:
            glow = rs(amask.filter(ImageFilter.GaussianBlur(width * k * 0.9))).point(lambda v: int(v * glow_a))
            self.img.paste(_rgb(c1), pos + (pos[0] + out, pos[1] + out), glow)
            self.img.paste(rs(col), pos, rs(amask))

    def tag(self, x, y, s, name, size, color, anchor="r", tracking=2.5, pad=(12, 7), fill=None,
            alpha=1.0, radius=3):
        """cerceveli kucuk etiket; y = kutunun ustu. Kutuyu dondurur."""
        tw = self.tw(s, name, size, tracking)
        bw, bh = tw + 2 * pad[0], size * 1.0 + 2 * pad[1]
        x1 = x - bw if anchor == "r" else (x - bw / 2.0 if anchor == "m" else x)
        self.rect((x1, y, x1 + bw, y + bh), fill=fill, outline=color, width=1, radius=radius, oa=alpha)
        self.text(x1 + pad[0], y + pad[1] + size * 0.86, s, name, size, color, tracking=tracking, alpha=alpha)
        return (x1, y, x1 + bw, y + bh)

    def glass(self, box, radius, tint, tint_a=0.4, blur=16, rim="#FFFFFF", rim_a=0.6, shadow_a=0.38,
              glow_a=0.09, light_a=0.10, soft=False, sheen_a=0.0, bottom_a=0.0, bright=1.04):
        """Buzlu cam kart: arka plani bulaniklastirir, renklendirir; golge, ic isik ve
        sol-ust kenarda parlak cizgi ekler."""
        if self.dry:
            return
        x1, y1, x2, y2 = [self.p(v) for v in box]
        w, h = x2 - x1, y2 - y1
        r = self.p(radius)
        rr = [0, 0, w - 1, h - 1]
        # 1) arka plan (golgeden once alinir), kenar tasmasi icin payli kesilir
        bp = self.p(blur) * 2
        back = self.img.crop((x1 - bp, y1 - bp, x2 + bp, y2 + bp))
        back = back.filter(ImageFilter.GaussianBlur(self.p(blur))).crop((bp, bp, bp + w, bp + h))
        back = ImageEnhance.Brightness(ImageEnhance.Color(back).enhance(1.25)).enhance(bright)
        # 2) yumusak golge
        # soft=True: genis ortam golgesi + dar temas golgesi (daha yumusak, daha derin)
        layers = ((84, 26, 32, shadow_a * 0.82), (30, 6, 8, shadow_a * 0.55)) if soft else ((48, 16, 20, shadow_a),)
        for pad_, off_, blur_, a_ in layers:
            pad, off = self.p(pad_), self.p(off_)
            sm = Image.new("L", (w + 2 * pad, h + 2 * pad), 0)
            ImageDraw.Draw(sm).rounded_rectangle([pad, pad + off, pad + w - 1, pad + h - 1 + off], r,
                                                 fill=int(255 * a_))
            sm = sm.filter(ImageFilter.GaussianBlur(self.p(blur_)))
            self.img.paste((0, 0, 0), (x1 - pad, y1 - pad, x1 - pad + sm.size[0], y1 - pad + sm.size[1]), sm)
        # 3) renk tonu
        card = Image.blend(back, Image.new("RGB", (w, h), _rgb(tint)), tint_a)
        white = Image.new("RGB", (w, h), _rgb(rim))
        # 4) ustten gelen yumusak isik
        lg = _gradient((w, h), [(0, (255, 255, 255)), (0.55, (0, 0, 0)), (1, (0, 0, 0))], vertical=True).convert("L")
        card.paste(white, (0, 0), lg.point(lambda v: int(v * light_a)))
        # 4b) capraz parlaklik gecisi (sol ustten gelen isik)
        if sheen_a:
            a1 = int(255 * sheen_a)
            sh = _gradient((w, h), [(0, (a1,) * 3), (0.22, (int(a1 * 0.55),) * 3), (0.5, (0,) * 3), (1, (0,) * 3)],
                           shear=float(w) / max(1, h) * 0.9).convert("L")
            card.paste(white, (0, 0), sh)
        # 4c) alt kenarda ince koyuluk (cam kalinligi)
        if bottom_a:
            bm = Image.new("L", (w, h), 0)
            ImageDraw.Draw(bm).rounded_rectangle(rr, r, outline=255, width=max(2, self.p(2.5)))
            bm = bm.filter(ImageFilter.GaussianBlur(self.p(1.6)))
            bg_ = _gradient((w, h), [(0, (0,) * 3), (max(0.0, 1 - self.p(radius) * 1.5 / max(1, h)), (0,) * 3),
                                     (1, (255,) * 3)], vertical=True).convert("L")
            card.paste((0, 0, 0), (0, 0, w, h), ImageChops.multiply(bm, bg_).point(lambda v: int(v * bottom_a)))
        # 5) ic kenar parlamasi
        ig = Image.new("L", (w, h), 0)
        ImageDraw.Draw(ig).rounded_rectangle(rr, r, outline=255, width=max(2, self.p(9)))
        ig = ig.filter(ImageFilter.GaussianBlur(self.p(8)))
        card.paste(white, (0, 0), ig.point(lambda v: int(v * glow_a)))
        # 5b) ust kenarin hemen altinda ince parlama (cam kalinligi hissi)
        tg = Image.new("L", (w, h), 0)
        ImageDraw.Draw(tg).rounded_rectangle([self.p(1.5), self.p(1.5), w - 1 - self.p(1.5), h - 1 - self.p(1.5)],
                                             max(1, r - self.p(1.5)), outline=255, width=max(2, self.p(3)))
        tg = tg.filter(ImageFilter.GaussianBlur(self.p(2.2)))
        vg = _gradient((w, h), [(0, (255,) * 3), (min(0.9, self.p(radius) * 1.2 / max(1, h)), (0,) * 3), (1, (0,) * 3)],
                       vertical=True).convert("L")
        card.paste(white, (0, 0), ImageChops.multiply(tg, vg).point(lambda v: int(v * 0.26)))
        # 6) kenar cizgisi: sol-ustte parlak, ortada sonuk, sag-altta hafif
        rm = Image.new("L", (w, h), 0)
        ImageDraw.Draw(rm).rounded_rectangle(rr, r, outline=255, width=max(1, self.p(1.3)))
        a0 = int(255 * rim_a)
        dg = _gradient((w, h), [(0, (a0,) * 3), (0.30, (int(a0 * 0.42),) * 3), (0.62, (int(a0 * 0.14),) * 3),
                                (1, (int(a0 * 0.50),) * 3)], shear=float(w) / max(1, h) * 0.55).convert("L")
        card.paste(white, (0, 0), ImageChops.multiply(rm, dg))
        # 7) yuvarlak maske ile yerine koy
        m = Image.new("L", (w, h), 0)
        ImageDraw.Draw(m).rounded_rectangle(rr, r, fill=255)
        self.img.paste(card, (x1, y1), m)

    def finish(self, out_scale: int = 1) -> Image.Image:
        size = (self.w * out_scale, int(round(self.h * out_scale)))
        if self.img.size == size:
            return self.img
        return self.img.resize(size, Image.Resampling.LANCZOS)


# --------------------------------------------------------------------------
# ortak parcalar
# --------------------------------------------------------------------------
def _row_h(n: int, base: float) -> float:
    """satir yuksekligi: cok satirda sikisir"""
    if n <= 10:
        return base
    if n <= 15:
        return base - 8
    return base - 14


def _visible(trades):
    """(gosterilen satirlar, gizlenen adet)"""
    if len(trades) <= MAX_ROWS:
        return trades, 0
    return trades[:MAX_ROWS - 1], len(trades) - (MAX_ROWS - 1)


def _col(oc, T):
    return {"win": T["win"], "loss": T["loss"]}.get(oc, T["be"])


def _footnotes(d):
    """[bot] tablonun altindaki aciklama satirlari: yaklasik ("≈") ve bilinmeyen ("—") R'li islem sayilari"""
    if d.get("footnotes") is not None:              # (bot: resim, aciklama ve metin karne ayni cumleleri kullanir)
        return [str(note) for note in d["footnotes"]]
    out = []
    if d.get("approx"):
        out.append(f'≈ yaklaşık: {d["approx"]} işlemde gerçek risk kayıtlı değil; R, hesabın planlanan riskine '
                   'göre hesaplandı')
    if d.get("partial"):
        out.append(f'* {d["partial"]} işlemde R’si bilinmeyen hesap, işlemin ortalamasına ve kendi toplamına '
                   'girmedi')
    if d.get("unknown"):
        out.append(f'— R’si bilinmeyen {d["unknown"]} işlem toplama ve kazanma oranına girmedi')
    return out


def _marker(cv, kind, oc, cx, cy, r, T):
    """renkten bagimsiz isaret: dolu = kazanc, ici bos = kayip, cizgi = basabas"""
    if oc == "unknown":                             # [bot] isaret yok
        return
    c = _col(oc, T)
    if oc == "be":
        cv.hline(cx - r, cx + r, cy, c, 2)
        return
    shape = {"diamond": cv.diamond, "circle": cv.circle, "square": cv.square}[kind]
    if oc == "win":
        shape(cx, cy, r, fill=c)
    else:
        shape(cx, cy, r, outline=c, width=1.6)


def _date_parts(t):
    try:
        dt = _to_date(t["date"])
        return dt.strftime("%d.%m"), _DAYS_TR[dt.weekday()]
    except Exception:
        return str(t.get("date", "")), ""


def _trade_desc(t):
    return f'{t["pair"]} {t["side"]}'.strip() if t else ""


def _sign_col(v, T):
    return T["win"] if v > 0.049 else (T["loss"] if v < -0.049 else T["be"])


# --------------------------------------------------------------------------
# TASARIM A - siyah, altin varak, ortali (Playfair Display + Inter)
# --------------------------------------------------------------------------
TA = dict(
    bg="#09090A", ink="#F1EBDD", mute="#918B7D", gold="#C9A961", gold2="#B79C63",
    win="#7BD3A1", loss="#EC7B72", be="#B4AE9F",
    foil=[(0, "#A8843F"), (0.22, "#F6E7B4"), (0.48, "#C49B48"), (0.74, "#F9EDC6"), (1, "#B08A42")],
    foil_big=[(0, "#FBF0CB"), (0.42, "#E2C272"), (0.58, "#B58E3E"), (1, "#EBD496")],
)


def _design_a(cv: Canvas, d: dict, H):
    T = TA
    M = 84
    L, R, cx = M, W - M, W / 2.0
    CW = R - L
    dec, sep = d["r_decimals"], d["decimal_sep"]
    if H:
        cv.glow(cx, 120, 760, 560, "#2B2212", 0.95)
        cv.glow(cx, H - 40, 760, 420, "#1B160C", 0.8)
        cv.rect((24, 24, W - 24, H - 24), outline=T["gold"], width=1.5, oa=0.9)
        cv.rect((33, 33, W - 33, H - 33), outline=T["gold"], width=0.5, oa=0.45)
        for (qx, qy) in ((24, 24), (W - 24, 24), (24, H - 24), (W - 24, H - 24)):
            cv.diamond(qx, qy, 7, fill=T["bg"])
            cv.diamond(qx, qy, 4.5, fill=T["gold"])
    if d.get("sample"):
        cv.tag(W - 56, 54, "ÖRNEK VERİ", INTER[600], 12, T["gold2"], alpha=0.9)

    # --- baslik
    oy = 118
    cv.hline(cx - 170, cx - 26, oy, T["gold"], 1, 0.7)
    cv.hline(cx + 26, cx + 170, oy, T["gold"], 1, 0.7)
    cv.diamond(cx, oy, 7, fill=T["gold"])
    cv.diamond(cx - 26, oy, 2.5, fill=T["gold"])
    cv.diamond(cx + 26, oy, 2.5, fill=T["gold"])
    ts = cv.fit(d["title"], SERIF_D, 50, CW, tracking=5)
    cv.foil(cx, 196, d["title"], SERIF_D, ts, T["foil"], anchor="m", tracking=5)
    cv.text(cx, 242, tr_upper(d["subtitle"]), INTER[500], 19, T["gold2"], "m", tracking=4.2)

    # --- ana rakam
    cv.text(cx, 316, "HAFTALIK SONUÇ", INTER[600], 16, T["mute"], "m", tracking=5)
    hero = fmt_r(d["total_r"], dec, sep)
    hs = cv.fit(hero, INTER[300], 164, CW - 80)
    cv.foil(cx, 316 + 40 + hs * 0.74, hero, INTER[300], hs, T["foil_big"], anchor="m", shear=0.0, vertical=True)
    y = 316 + 40 + 164 * 0.74 + 50
    if d.get("total_usd") is not None:
        cv.text(cx, y, f'{fmt_usd(d["total_usd"])}  ·  {len(d["accounts"])} hesap toplamı', INTER[500], 23,
                T["ink"], "m", alpha=0.82)
        y += 46
    else:
        y += 6

    # --- ozet satiri
    cv.hline(L, R, y, T["gold"], 1, 0.38)
    cols = [200, 284, 218, 218]
    best, worst = d["best"], d["worst"]
    stats = [
        ("KAZANMA ORANI", fmt_pct(d["win_rate"]) if d["win_rate"] is not None else "—",
         f'{d["wins"]} / {d["decided"]} · başabaş hariç', T["ink"]),
        ("İŞLEM", str(d["n"]), f'{d["wins"]} kazanç · {d["losses"]} kayıp · {d["be"]} başabaş', T["ink"]),
        ("EN İYİ", fmt_r(best["r"], dec, sep) if best else "—", _trade_desc(best),
         _sign_col(best["r"], T) if best else T["ink"]),
        ("EN KÖTÜ", fmt_r(worst["r"], dec, sep) if worst else "—", _trade_desc(worst),
         _sign_col(worst["r"], T) if worst else T["ink"]),
    ]
    x = L
    for i, (lab, val, sub, colr) in enumerate(stats):
        mx = x + cols[i] / 2.0
        cv.text(mx, y + 43, lab, INTER[600], 14, T["mute"], "m", tracking=3.4)
        vs = cv.fit(val, INTER[600], 40, cols[i] - 28)
        cv.text(mx, y + 94, val, INTER[600], vs, colr, "m")
        ss = cv.fit(sub, INTER[400], 16, cols[i] - 16, min_size=11)
        cv.text(mx, y + 125, cv.ellip(sub, INTER[400], ss, cols[i] - 16), INTER[400], ss, T["mute"], "m")
        x += cols[i]
        if i < 3:
            cv.vline(x, y + 28, y + 130, T["gold"], 1, 0.28)
    y += 156
    cv.hline(L, R, y, T["gold"], 1, 0.38)

    # --- hesaplar
    y = _a_section(cv, cx, y + 50, "HESAPLAR", T)
    acc = d["accounts"]
    if acc:
        cw = CW / float(len(acc))
        for i, a in enumerate(acc):
            mx = L + cw * (i + 0.5)
            nm = tr_upper(str(a.get("name", "")))
            cv.text(mx, y + 34, cv.ellip(nm, INTER[600], 15, cw - 30, 3.6), INTER[600], 15, T["gold"], "m",
                    tracking=3.6)
            rv = fmt_r(a.get("r", 0), dec, sep)
            cv.text(mx, y + 84, rv, INTER[600], cv.fit(rv, INTER[600], 38, cw - 30), _sign_col(a.get("r", 0), T), "m")
            if a.get("usd") is not None:
                cv.text(mx, y + 116, fmt_usd(a["usd"]), INTER[400], 19, T["ink"], "m", alpha=0.72)
            if i:
                cv.vline(L + cw * i, y + 16, y + 120, T["gold"], 1, 0.28)
        y += 142
    cv.hline(L, R, y, T["gold"], 1, 0.38)

    # --- islemler
    y = _a_section(cv, cx, y + 50, "HAFTANIN İŞLEMLERİ", T)
    y = _table(cv, d, y + 4, L, R, T, base_h=52, font=INTER, marker="diamond", style="a")

    # --- alt bilgi
    y += 54
    cv.diamond(cx, y - 26, 3, fill=T["gold"], alpha=0.8)
    foot = d["footer"] + ("  ·  ÖRNEK VERİ" if d.get("sample") else "")
    cv.text(cx, y + 8, foot, INTER[400], 15, T["mute"], "m", tracking=1.6)
    return y + 8 + 62


def _a_section(cv, cx, y, label, T):
    w = cv.tw(label, INTER[600], 15, 5)
    cv.text(cx, y, label, INTER[600], 15, T["gold"], "m", tracking=5)
    cv.hline(cx - w / 2 - 70, cx - w / 2 - 18, y - 5, T["gold"], 1, 0.55)
    cv.hline(cx + w / 2 + 18, cx + w / 2 + 70, y - 5, T["gold"], 1, 0.55)
    return y + 14


# --------------------------------------------------------------------------
# ortak islem tablosu
# --------------------------------------------------------------------------
def _pill(cv, kind, side, xs, cy, k, font, T, fs):
    """LONG/SHORT gosterimi. kind: tri | outline | solid | text"""
    is_long = side == "LONG"
    known = side in ("LONG", "SHORT")
    psz, ph = fs(13), fs(27)
    tri = 3.7 * (0.7 + 0.3 * k)

    def triangle(tx, colr, a=0.9):
        if not known:
            return
        if is_long:
            cv.poly([(tx - tri, cy + tri * 0.75), (tx + tri, cy + tri * 0.75), (tx, cy - tri * 0.95)], fill=colr, alpha=a)
        else:
            cv.poly([(tx - tri, cy - tri * 0.75), (tx + tri, cy - tri * 0.75), (tx, cy + tri * 0.95)], fill=colr, alpha=a)

    if kind == "text":
        colr = T["gold"] if is_long else T["ink"]
        triangle(xs + tri, colr, 0.95)
        cv.text(xs + tri * 2 + 9, cy + fs(15) * 0.36, side, font[600], fs(15), colr, tracking=2.6,
                alpha=1.0 if is_long else 0.85)
        return
    wide = max(cv.tw(v, font[600], psz, 2.0) for v in ("LONG", "SHORT", side))
    if kind == "outline":
        pwid = wide + 28
        bx = (xs, cy - ph / 2, xs + pwid, cy + ph / 2)
        if is_long:
            cv.rect(bx, outline=T["gold"], width=1, radius=ph / 2, oa=0.78)
        else:
            cv.rect(bx, fill="#FFFFFF", fa=0.10, radius=ph / 2)
        cv.text(xs + 14, cy + psz * 0.36, side, font[600], psz, T["gold"] if is_long else T["ink"], tracking=2.0,
                alpha=0.95)
        return
    pwid = wide + 44
    bx = (xs, cy - ph / 2, xs + pwid, cy + ph / 2)
    if kind == "solid":
        if is_long:
            cv.rect(bx, fill=T["gold"], fa=0.92, radius=ph / 2)
            dark = T["grad"][0][1] if "grad" in T else "#111111"
            triangle(xs + 15, dark, 0.9)
            cv.text(xs + 27, cy + psz * 0.36, side, font[700], psz, dark, tracking=2.0)
        else:
            cv.rect(bx, outline=T["ink"], width=1, radius=ph / 2, oa=0.45)
            triangle(xs + 15, T["ink"], 0.85)
            cv.text(xs + 27, cy + psz * 0.36, side, font[600], psz, T["ink"], tracking=2.0, alpha=0.92)
        return
    # tri (varsayilan)
    colr = T["gold"] if is_long else T["ink"]
    if is_long:
        cv.rect(bx, fill=T["gold"], fa=0.07, outline=T["gold"], width=1, radius=ph / 2, oa=0.70)
    else:
        cv.rect(bx, fill="#FFFFFF", fa=0.085, outline="#FFFFFF", width=1, radius=ph / 2, oa=0.16)
    triangle(xs + 15, colr)
    cv.text(xs + 27, cy + psz * 0.36, side, font[600], psz, colr, tracking=2.0, alpha=0.95)


def _table(cv, d, y, L, R, T, base_h, font, marker, style, pad=0, cols=(0.275, 0.515, 0.70), pill="tri"):
    """Islem tablosu; alt kenarin y'sini dondurur.
    style: a (ince altin cizgiler) | b (kart ici, zebra, rozet) | c (defter)."""
    dec, sep = d["r_decimals"], d["decimal_sep"]
    rows, hidden = _visible(d["trades"])
    hidden = int(d.get("hidden") or hidden)         # [bot] trades'te olmayan (yalnizca sayilan) satirlar
    n = len(rows)
    notes = _footnotes(d)                           # [bot]
    rh = _row_h(n + (1 if hidden else 0) + len(notes), base_h)
    k = rh / float(base_h)
    fs = lambda v: max(v * 0.86, v * (0.55 + 0.45 * k))          # punto sikisma
    x0, x1 = L + pad, R - pad
    cwid = x1 - x0
    # sutunlar (oransal)
    xm = x0 + 8                           # isaret
    xd = x0 + 34                          # tarih
    xp = x0 + cwid * cols[0]              # parite
    xs = x0 + cwid * cols[1]              # yon
    xn = x0 + cwid * cols[2]              # not
    xr = x1 - (4 if style not in ("b", "g") else 0)  # sonuc (saga hizali)
    head = T.get("head", T["mute"])
    hy = y + 30
    hsz = 13.5
    for lab, hx, an in (("TARİH", xd, "l"), ("PARİTE", xp, "l"), ("YÖN", xs, "l"), ("NOT", xn, "l"),
                        ("SONUÇ", xr, "r")):
        cv.text(hx, hy, lab, font[600], hsz, head, an, tracking=3.0)
    y = hy + 16
    if style == "c":
        cv.hline(x0, x1, y, T["rule"], 1.5, 1.0)
    else:
        cv.hline(x0, x1, y, T["gold"], 1, 0.45 if style == "a" else 0.30)
    if n == 0:
        cv.text((x0 + x1) / 2, y + rh * 0.62, d.get("empty_text", "Bu hafta işlem yok"), font[400], 20, T["mute"],
                "m")                                # [bot] empty_text
        y += rh
    star = (cv.tw("*", font[600], fs(25)) + 2) if d.get("partial") else 0      # [bot] "*" icin yer (varsa)
    r_w = cv.tw(("≈" if d.get("approx") else "") + "−00.0R", font[600], fs(25), tab=True) + 14 + star   # sonuc
    #                                                 sutunu genisligi ([bot] yaklasik / eksik isaretleriyle)
    for i, t in enumerate(rows):
        top = y + i * rh
        by = top + rh * 0.5 + fs(22) * 0.36                      # baseline
        if style in ("b", "g") and i % 2 == 1:
            cv.rect((x0 - pad + 10, top, x1 + pad - 10, top + rh), fill="#FFFFFF", fa=0.035, radius=10)
        oc = t["outcome"]
        _marker(cv, marker, oc, xm, top + rh * 0.5, 6.5 * (0.7 + 0.3 * k), T)
        ds, dn = _date_parts(t)
        cv.text(xd, by, ds, font[500], fs(21), T["ink"], alpha=0.86)
        if dn:
            cv.text(xd + cv.tw("00.00", font[500], fs(21)) + 11, by, dn, font[400], fs(17), T["mute"])
        # parite: sigmazsa kucult, yine sigmazsa kes
        pw = xs - xp - 18
        ps = cv.fit(t["pair"], font[600], fs(24), pw, min_size=fs(24) * 0.72)
        cv.text(xp, by, cv.ellip(t["pair"], font[600], ps, pw), font[600], ps, T["ink"])
        side = t["side"]
        if style == "g" and side:
            _pill(cv, pill, side, xs, top + rh / 2.0, k, font, T, fs)
        elif style == "b" and side:
            sw = cv.tw(side, font[600], fs(13.5), 2.2)
            ph = fs(28)
            bx = (xs, top + rh / 2 - ph / 2, xs + sw + 26, top + rh / 2 + ph / 2)
            if side == "LONG":
                cv.rect(bx, outline=T["gold"], width=1, radius=ph / 2, oa=0.75)
                cv.text(xs + 13, by - fs(22) * 0.36 + fs(13.5) * 0.36, side, font[600], fs(13.5), T["gold"],
                        tracking=2.2)
            else:
                cv.rect(bx, fill="#FFFFFF", fa=0.10, radius=ph / 2)
                cv.text(xs + 13, by - fs(22) * 0.36 + fs(13.5) * 0.36, side, font[600], fs(13.5), T["ink"],
                        tracking=2.2, alpha=0.9)
        elif side:
            cv.text(xs, by, side, font[500], fs(17), T["ink"], tracking=2.6, alpha=0.80)
        nw = xr - r_w - xn - 10
        if t["note"]:
            cv.text(xn, by, cv.ellip(t["note"], font[400], fs(19), nw), font[400], fs(19), T["mute"])
        if oc == "unknown":                         # [bot] R bilinmiyor
            cv.text(xr - star, by, "—", font[600], fs(25), T["mute"], "r")
        else:                                       # [bot] yaklasik R: basinda "≈"; basabas: 0.0R; eksik hesap: "*"
            shown = 0.0 if oc == "be" else t["r"]
            cv.text(xr - star, by, ("≈" if t.get("approx") else "") + fmt_r(shown, dec, sep), font[600], fs(25),
                    _col(oc, T), "r", tab="dec")
            if t.get("partial"):
                cv.text(xr, by, "*", font[600], fs(25), T["mute"], "r")
        if i < n - 1 or hidden or style == "c":
            if style == "a":
                cv.hline(x0, x1, top + rh, T["gold"], 0.5, 0.30)
            elif style == "c":
                cv.hline(x0, x1, top + rh, T["hair"], 1, 1.0)
    y += n * rh
    if hidden:
        more = d.get("more_text") or f"… ve {hidden} işlem daha (toplama dahil)"      # [bot] more_text
        cv.text(xd, y + rh * 0.5 + 7, cv.ellip(more, font[400], fs(18), x1 - xd), font[400], fs(18), T["mute"])
        y += rh
    for note in notes:                              # [bot] kac islemin R'si yaklasik / bilinmiyor
        cv.text(xd, y + rh * 0.5 + 7, cv.ellip(note, font[400], fs(18), x1 - xd), font[400], fs(18), T["mute"])
        y += rh
    return y


# --------------------------------------------------------------------------
# TASARIM B - gece mavisi / zumrut, sampanya (Cormorant Garamond + Montserrat)
# --------------------------------------------------------------------------
TB = dict(
    bg="#0A1724", ink="#F3EEE2", mute="#93A7A8", gold="#DCC79A", gold2="#CDB98A",
    win="#6FDDB0", loss="#F48C86", be="#B7C2C2",
    grad=[(0, "#0B1830"), (0.55, "#0A2230"), (1, "#0A3029")],
    foil=[(0, "#F7ECD2"), (0.5, "#D7BF8C"), (1, "#F3E4C0")],
)


def _b_card(cv, box, T, radius=26):
    cv.rect(box, fill="#FFFFFF", fa=0.055, outline=T["gold"], oa=0.22, width=1, radius=radius)


def _design_b(cv: Canvas, d: dict, H):
    T = TB
    M = 60
    L, R = M, W - M
    CW = R - L
    dec, sep = d["r_decimals"], d["decimal_sep"]
    if H:
        cv.fill_gradient((0, 0, W, H), T["grad"], shear=0.9)
        cv.glow(W - 80, 60, 720, 620, "#12504A", 0.55)
        cv.glow(60, H - 60, 700, 600, "#0E2C50", 0.45)

    # --- baslik
    cv.hline(L, L + 56, 82, T["gold"], 3, 1.0)
    if d.get("sample"):
        cv.tag(R, 66, "ÖRNEK VERİ", MONT[600], 12, T["gold"], alpha=0.9, radius=14, pad=(14, 8))
    title = d["title"]
    parts = title.split(" ", 1)
    if len(parts) == 2:
        cv.text(L, 152, parts[0], SERIF_CI, cv.fit(parts[0], SERIF_CI, 54, CW), T["gold"])
        s2 = cv.fit(parts[1], SERIF_C, 80, CW, tracking=1)
        cv.text(L - 2, 152 + 16 + s2 * 0.72, parts[1], SERIF_C, s2, T["ink"], tracking=1)
        y = 152 + 16 + s2 * 0.72
    else:
        s2 = cv.fit(title, SERIF_C, 88, CW)
        cv.text(L, 190, title, SERIF_C, s2, T["ink"])
        y = 190
    cv.text(L, y + 46, tr_upper(d["subtitle"]), MONT[500], 18, T["mute"], tracking=3.4)
    y += 46 + 34

    # --- ana kart
    hh = 262
    _b_card(cv, (L, y, R, y + hh), T, 30)
    cv.text(L + 40, y + 56, "HAFTALIK SONUÇ", MONT[600], 15, T["gold"], tracking=4)
    hero = fmt_r(d["total_r"], dec, sep)
    ring_r = 84
    hs = cv.fit(hero, MONT[500], 124, CW - 80 - 2 * ring_r - 70)
    cv.foil(L + 34, y + 76 + 124 * 0.73 + 4, hero, MONT[500], hs, T["foil"], shear=0.5)
    if d.get("total_usd") is not None:
        cv.text(L + 40, y + hh - 36, f'{fmt_usd(d["total_usd"])}  ·  {len(d["accounts"])} hesap toplamı',
                MONT[500], 20, T["ink"], alpha=0.8)
    rx, ry = R - 48 - ring_r, y + hh / 2.0
    wr = d["win_rate"]
    cv.ring(rx, ry, ring_r, 12, (wr or 0) / 100.0, "#FFFFFF", T["gold"], track_alpha=0.10)
    cv.text(rx, ry + 8, fmt_pct(wr) if wr is not None else "—", MONT[600], 40, T["ink"], "m")
    cv.text(rx, ry + 34, "KAZANMA", MONT[600], 11.5, T["mute"], "m", tracking=2.6)
    if d.get("total_usd") is None and d["decided"]:
        cv.text(L + 40, y + hh - 36, f'Kazanma oranı başabaş hariç: {d["wins"]} / {d["decided"]}', MONT[500], 17,
                T["mute"])
    y += hh + 18

    # --- uc kutu
    gap = 18
    tw_ = (CW - 2 * gap) / 3.0
    th = 132
    best, worst = d["best"], d["worst"]
    tiles = [
        ("İŞLEM", str(d["n"]), f'{d["wins"]} kazanç · {d["losses"]} kayıp · {d["be"]} başabaş', T["ink"]),
        ("EN İYİ", fmt_r(best["r"], dec, sep) if best else "—", _trade_desc(best),
         _sign_col(best["r"], T) if best else T["ink"]),
        ("EN KÖTÜ", fmt_r(worst["r"], dec, sep) if worst else "—", _trade_desc(worst),
         _sign_col(worst["r"], T) if worst else T["ink"]),
    ]
    for i, (lab, val, sub, colr) in enumerate(tiles):
        x = L + i * (tw_ + gap)
        _b_card(cv, (x, y, x + tw_, y + th), T, 22)
        cv.text(x + 24, y + 36, lab, MONT[600], 13, T["mute"], tracking=3.2)
        cv.text(x + 24, y + 82, val, MONT[600], cv.fit(val, MONT[600], 36, tw_ - 48), colr)
        ss = cv.fit(sub, MONT[400], 15, tw_ - 44, min_size=11)
        cv.text(x + 24, y + 110, cv.ellip(sub, MONT[400], ss, tw_ - 44), MONT[400], ss, T["mute"])
    y += th + 44

    # --- hesaplar
    cv.text(L, y, "HESAPLAR", MONT[600], 15, T["gold"], tracking=4)
    y += 18
    acc = d["accounts"]
    if acc:
        aw = (CW - (len(acc) - 1) * gap) / float(len(acc))
        cells = []
        for a in acc:
            rv = fmt_r(a.get("r", 0), dec, sep)
            rs = cv.fit(rv, MONT[600], 32, aw - 48)
            us = fmt_usd(a["usd"]) if a.get("usd") is not None else ""
            room = aw - 48 - cv.tw(rv, MONT[600], rs) - 16
            cells.append((a, rv, rs, us, room))
        # USD, R'nin yanina sigmiyorsa butun kutularda alt satira iner
        stack = any(us and cv.tw(us, MONT[500], 18) > room for _, _, _, us, room in cells)
        ah = 144 if stack else 118
        for i, (a, rv, rs, us, room) in enumerate(cells):
            x = L + i * (aw + gap)
            _b_card(cv, (x, y, x + aw, y + ah), T, 22)
            nm = tr_upper(str(a.get("name", "")))
            cv.text(x + 24, y + 36, cv.ellip(nm, MONT[600], 13, aw - 48, 3.2), MONT[600], 13, T["mute"],
                    tracking=3.2)
            cv.text(x + 24, y + 86, rv, MONT[600], rs, _sign_col(a.get("r", 0), T))
            if us and stack:
                cv.text(x + 24, y + 118, us, MONT[500], cv.fit(us, MONT[500], 18, aw - 48, min_size=11), T["ink"],
                        alpha=0.75)
            elif us:
                cv.text(x + aw - 24, y + 86, us, MONT[500], 18, T["ink"], "r", alpha=0.75)
        y += ah
    y += 44

    # --- islemler
    cv.text(L, y, "HAFTANIN İŞLEMLERİ", MONT[600], 15, T["gold"], tracking=4)
    y += 18
    rows, hidden = _visible(d["trades"])
    nrow = max(1, len(rows)) + (1 if hidden else 0)
    rh = _row_h(nrow, 54)
    card_h = 46 + nrow * rh + 14
    _b_card(cv, (L, y, R, y + card_h), T, 26)
    _table(cv, d, y, L, R, T, base_h=54, font=MONT, marker="circle", style="b", pad=26)
    y += card_h

    # --- alt bilgi
    foot = d["footer"] + ("  ·  ÖRNEK VERİ" if d.get("sample") else "")
    cv.text(L, y + 54, foot, MONT[500], 14, T["mute"], tracking=1.4)
    cv.hline(R - 56, R, y + 49, T["gold"], 3, 1.0)
    return y + 54 + 50


# --------------------------------------------------------------------------
# TASARIM C - fildisi "ozel banka ekstresi" (Cormorant Garamond + Inter)
# --------------------------------------------------------------------------
TC = dict(
    bg="#F6F1E4", ink="#1B2A24", mute="#6F6A5C", gold="#A8873A", gold2="#D8C489",
    green="#143B2D", win="#1B6B48", loss="#A8392C", be="#6F6A5C",
    rule="#143B2D", hair="#DDD3BD", head="#143B2D",
)


def _design_c(cv: Canvas, d: dict, H):
    T = TC
    M = 72
    L, R = M, W - M
    CW = R - L
    dec, sep = d["r_decimals"], d["decimal_sep"]
    band = 236
    if H:
        cv.glow(W / 2, H * 0.55, 900, H * 0.6, "#FBF8EF", 0.9)
        cv.fill_gradient((0, 0, W, band), [(0, "#0F3226"), (1, "#184636")], shear=0.6)
        cv.hline(0, W, band + 2, T["gold"], 4, 1.0)
        cv.hline(0, W, band + 10, T["gold"], 1, 0.8)
        cv.rect((20, 20, W - 20, band - 16), outline=T["gold2"], width=1, oa=0.35)
    # --- antet
    title = d["title"]
    ts = cv.fit(title, SERIF_C, 60, CW - 20, tracking=2.5)
    cv.text(W / 2, 122, title, SERIF_C, ts, "#F7F1E1", "m", tracking=2.5)
    cv.hline(W / 2 - 60, W / 2 + 60, 148, T["gold2"], 1, 0.9)
    cv.diamond(W / 2, 148, 4, fill=T["gold2"])
    cv.text(W / 2, 190, tr_upper(d["subtitle"]), INTER[500], 17, T["gold2"], "m", tracking=4)

    # --- ust bilgi satiri
    y = band + 58
    cv.text(L, y, "HAFTALIK HESAP ÖZETİ", INTER[600], 13.5, T["green"], tracking=3.2)
    if d.get("sample"):
        cv.tag(R, y - 22, "ÖRNEK VERİ", INTER[600], 12, T["loss"], alpha=0.9, radius=2)

    # --- ozet: sol buyuk rakam, sag defter satirlari
    y += 30
    cv.hline(L, R, y, T["rule"], 1.5)
    split = L + CW * 0.44
    cv.text(L, y + 50, "Haftalık net sonuç", SERIF_CI, 27, T["mute"])
    hero = fmt_r(d["total_r"], dec, sep)
    hs = cv.fit(hero, INTER[500], 112, split - L - 30)
    cv.text(L - 4, y + 70 + 112 * 0.74 + 4, hero, INTER[500], hs, _sign_col(d["total_r"], T) if abs(
        d["total_r"]) > 0.049 else T["ink"])
    if d.get("total_usd") is not None:
        cv.text(L, y + 208, f'{fmt_usd(d["total_usd"])}  ·  {len(d["accounts"])} hesap toplamı', INTER[500], 19,
                T["ink"], alpha=0.85)
    cv.vline(split, y + 26, y + 214, T["hair"], 1)
    best, worst = d["best"], d["worst"]
    lines = [
        ("Kazanma oranı (başabaş hariç)", fmt_pct(d["win_rate"]) if d["win_rate"] is not None else "—", T["ink"], ""),
        ("İşlem sayısı", str(d["n"]), T["ink"], ""),
        ("Kazanç / Kayıp / Başabaş", f'{d["wins"]} / {d["losses"]} / {d["be"]}', T["ink"], ""),
        ("En iyi işlem", fmt_r(best["r"], dec, sep) if best else "—",
         _sign_col(best["r"], T) if best else T["ink"], _trade_desc(best) if best else ""),
        ("En kötü işlem", fmt_r(worst["r"], dec, sep) if worst else "—",
         _sign_col(worst["r"], T) if worst else T["ink"], _trade_desc(worst) if worst else ""),
    ]
    lx = split + 34
    ly = y + 52
    for lab, val, colr, extra in lines:
        w1 = cv.text(lx, ly, lab, INTER[400], 18.5, T["mute"])
        w2 = cv.text(R, ly, val, INTER[600], 20, colr, "r")
        xe = R - w2 - 12
        if extra:
            ex = cv.ellip(extra, INTER[400], 14.5, xe - (lx + w1 + 30))
            xe -= cv.text(xe, ly, ex, INTER[400], 14.5, T["mute"], "r") + 10
        cv.dots(lx + w1 + 10, xe, ly - 4, T["mute"], alpha=0.55)
        ly += 38.5
    y += 240
    cv.hline(L, R, y, T["hair"], 1)

    # --- hesap ozeti
    y += 58
    cv.text(L, y, "Hesaplar", SERIF_CB, 32, T["green"])
    cv.hline(L + cv.tw("Hesaplar", SERIF_CB, 32) + 18, R, y - 9, T["gold"], 1, 0.9)
    y += 16
    xr_r, xr_u = L + CW * 0.66, R - 4
    cv.text(L + 8, y + 28, "HESAP", INTER[600], 13.5, T["head"], tracking=3)
    cv.text(xr_r, y + 28, "SONUÇ (R)", INTER[600], 13.5, T["head"], "r", tracking=3)
    if any(a.get("usd") is not None for a in d["accounts"]):
        cv.text(xr_u, y + 28, "SONUÇ (USD)", INTER[600], 13.5, T["head"], "r", tracking=3)
    y += 44
    cv.hline(L, R, y, T["rule"], 1.5)
    arh = 48
    for a in d["accounts"]:
        by = y + arh * 0.5 + 8
        nm = str(a.get("name", ""))
        cv.text(L + 8, by, cv.ellip(nm, INTER[500], 21, CW * 0.36), INTER[500], 21, T["ink"])
        cv.text(xr_r, by, fmt_r(a.get("r", 0), dec, sep), INTER[600], 22, _sign_col(a.get("r", 0), T), "r", tab="dec")
        if a.get("usd") is not None:
            cv.text(xr_u, by, fmt_usd(a["usd"]), INTER[500], 21, T["ink"], "r", alpha=0.9)
        y += arh
        cv.hline(L, R, y, T["hair"], 1)
    if d["accounts"]:
        cv.hline(L, R, y, T["rule"], 1)
        by = y + arh * 0.5 + 8
        cv.text(L + 8, by, "Toplam", INTER[700], 21, T["ink"])
        cv.text(xr_r, by, fmt_r(d["total_r"], dec, sep), INTER[700], 22, _sign_col(d["total_r"], T), "r", tab="dec")
        if d.get("total_usd") is not None:
            cv.text(xr_u, by, fmt_usd(d["total_usd"]), INTER[700], 21, T["ink"], "r")
        y += arh
        cv.hline(L, R, y, T["rule"], 1)
        cv.hline(L, R, y + 4, T["rule"], 1)

    # --- islem dokumu
    y += 64
    cv.text(L, y, "İşlem dökümü", SERIF_CB, 32, T["green"])
    cv.hline(L + cv.tw("İşlem dökümü", SERIF_CB, 32) + 18, R, y - 9, T["gold"], 1, 0.9)
    y = _table(cv, d, y + 14, L, R, T, base_h=50, font=INTER, marker="square", style="c")

    # --- alt bilgi
    y += 60
    cv.hline(W / 2 - 40, W / 2 + 40, y - 24, T["gold"], 1, 0.9)
    foot = d["footer"] + ("  ·  ÖRNEK VERİ" if d.get("sample") else "")
    cv.text(W / 2, y + 8, foot, INTER[400], 14.5, T["mute"], "m", tracking=1.4)
    return y + 8 + 50


# --------------------------------------------------------------------------
# TASARIM G1..G4 - "liquid glass": B yerlesimi, buzlu cam kartlar, cizim grafik
# arka plani, dort renk paleti. Para birimi gosterilmez; sonuclar yalnizca R.
# --------------------------------------------------------------------------
GLASS = {
    "G1": dict(
        name="Obsidyen & Rose Gold",
        grad=[(0, "#0B090C"), (0.5, "#141014"), (1, "#1E1518")],
        glow1="#74423A", glow2="#33222F", glow3="#4A2B28",
        gold="#EBBBA6", foil=[(0, "#F9DDCF"), (0.5, "#DDA088"), (1, "#F5D0BD")],
        ink="#F7F1EC", mute="#CDC2BF", win="#7FE6BB", loss="#FF7D8C", be="#D2C9C6",
        tint="#2B2226", tint_a=0.46, rim="#FFF1EA", chart_dn="#D8C8C2",
    ),
    "G2": dict(
        name="Patlıcan & Şampanya",
        grad=[(0, "#11071A"), (0.5, "#1D0D2B"), (1, "#2B133A")],
        glow1="#6A3382", glow2="#43123A", glow3="#4E2A66",
        gold="#EDDAAC", foil=[(0, "#FAF0D0"), (0.5, "#DCC48C"), (1, "#F5E6BC")],
        ink="#F7F1E8", mute="#D0C5D9", win="#84EAC2", loss="#FF8FA1", be="#D6CDDE",
        tint="#33203F", tint_a=0.46, rim="#FFF6E6", chart_dn="#CDBBDD",
    ),
    "G3": dict(
        name="Bordo & Antik Altın",
        grad=[(0, "#130506"), (0.5, "#25090D"), (1, "#3A0F16")],
        glow1="#8A2329", glow2="#45180A", glow3="#6A1C22",
        gold="#DDBB74", foil=[(0, "#F4E0A6"), (0.5, "#C69C4E"), (1, "#EED594")],
        ink="#F9F0E4", mute="#DBC6C0", win="#93EEC0", loss="#FF9DA4", be="#DDCFC8",
        tint="#3A151B", tint_a=0.48, rim="#FFF0DC", chart_dn="#E0C2B6",
    ),
    "G4": dict(
        name="Petrol & Bakır",
        grad=[(0, "#041317"), (0.5, "#082127"), (1, "#0D3036")],
        glow1="#8A5230", glow2="#0F4B54", glow3="#5C3A26",
        gold="#EDAA7C", foil=[(0, "#F9D3AE"), (0.5, "#D88A5C"), (1, "#F4C197")],
        ink="#F3F2EC", mute="#C0D2D3", win="#98F2D0", loss="#FF8A9F", be="#CEDADA",
        tint="#11383E", tint_a=0.46, rim="#FFF0E2", chart_dn="#BFD6D8",
    ),
}
CHART_SEED = 2046          # arka plan grafigi bu tohumdan uretilir (her calismada ayni)


def _spline(pts, sub=8):
    """Catmull-Rom ile yumusatilmis nokta dizisi."""
    out = []
    n = len(pts)
    for i in range(n - 1):
        p0, p1, p2, p3 = pts[max(0, i - 1)], pts[i], pts[i + 1], pts[min(n - 1, i + 2)]
        for j in range(sub):
            t = j / float(sub)
            t2, t3 = t * t, t * t * t
            out.append(tuple(
                0.5 * ((2 * p1[k]) + (-p0[k] + p2[k]) * t + (2 * p0[k] - 5 * p1[k] + 4 * p2[k] - p3[k]) * t2 +
                       (-p0[k] + 3 * p1[k] - 3 * p2[k] + p3[k]) * t3) for k in (0, 1)))
    out.append(pts[-1])
    return out


def _chart(cv: Canvas, H, P, seed=CHART_SEED):
    """Arka plana dusuk kontrastli mum grafigi + yumusak ortalama cizgisi + izgara cizer.
    Yalnizca random.Random(seed).random() kullanir: cikti her calismada aynidir."""
    k = 2                                   # grafik 2x cizilir, tuvale olceklenir
    w, h = W * k, int(round(H * k))
    rng = random.Random(seed)
    top, bot = 112.0, 900.0                 # grafik bandi (mantiksal px); satir sayisindan bagimsiz
    n = 30
    pitch = (W + 44.0) / n
    cs, c = [], 0.0
    for _ in range(n):
        o = c
        c = o + 0.46 + 2.2 * (rng.random() + rng.random() + rng.random() - 1.5)
        hi = max(o, c) + 0.15 + 0.9 * rng.random()
        lo = min(o, c) - 0.15 - 0.9 * rng.random()
        cs.append((o, hi, lo, c))
    vmin, vmax = min(x[2] for x in cs), max(x[1] for x in cs)
    Y = lambda v: (bot - (v - vmin) / (vmax - vmin) * (bot - top)) * k
    X = lambda i: (-14 + pitch * (i + 0.5)) * k

    def paste(mask, color, alpha):
        m = mask.resize(cv.img.size, Image.Resampling.BILINEAR).point(lambda v: int(v * alpha))
        cv.img.paste(_rgb(color), (0, 0) + cv.img.size, m)

    # solma maskeleri: grafik asagi dogru ve sola dogru soner
    vfade = _gradient((w, h), [(0, (255,) * 3), ((bot - 70) / H, (255,) * 3), (min(1.0, (bot + 150) / H), (0,) * 3),
                               (1, (0,) * 3)], vertical=True).convert("L")
    hfade = _gradient((w, h), [(0, (150,) * 3), (0.5, (235,) * 3), (1, (255,) * 3)]).convert("L")
    fade = ImageChops.multiply(vfade, hfade)
    focus = _gradient((w, h), [(0, (40,) * 3), (0.30, (140,) * 3), (0.65, (255,) * 3), (1, (255,) * 3)]).convert("L")

    # izgara
    grid = Image.new("L", (w, h), 0)
    gd = ImageDraw.Draw(grid)
    gy = 64.0
    while gy < H:
        gd.line([(0, gy * k), (w, gy * k)], fill=255, width=2)
        gy += 97.0
    gx = X(1)
    while gx < w:
        gd.line([(gx, 0), (gx, h)], fill=255, width=2)
        gx += pitch * 4 * k
    gfade = _gradient((w, h), [(0, (255,) * 3), (0.5, (190,) * 3), (1, (90,) * 3)], vertical=True).convert("L")
    paste(ImageChops.multiply(grid, gfade), "#FFFFFF", 0.05)

    # hareketli ortalama cizgisi + altinda solan alan
    closes = [x[3] for x in cs]
    ma = []
    for i in range(n):
        seg = closes[max(0, i - 3):i + 2]
        ma.append(sum(seg) / len(seg) - 0.9)
    pts = _spline([(X(-1), Y(ma[0]))] + [(X(i), Y(ma[i])) for i in range(n)] + [(X(n), Y(ma[-1] + 0.4))])
    above = Image.new("L", (w, h), 0)
    ImageDraw.Draw(above).polygon([(pts[0][0], 0)] + pts + [(pts[-1][0], 0)], fill=255)
    below = ImageChops.invert(above)
    area = ImageChops.multiply(above.filter(ImageFilter.GaussianBlur(70 * k)), below)
    paste(ImageChops.multiply(area, fade), P["gold"], 0.36)
    line = Image.new("L", (w, h), 0)
    ImageDraw.Draw(line).line(pts, fill=255, width=int(2.4 * k), joint="curve")
    line = Image.composite(line.filter(ImageFilter.GaussianBlur(0.7 * k)), line.filter(ImageFilter.GaussianBlur(5 * k)),
                           focus)
    paste(ImageChops.multiply(line, fade), P["gold"], 0.62)

    # mumlar: yukselen = vurgu rengi dolu, dusen = soluk
    up = Image.new("L", (w, h), 0)
    dn = Image.new("L", (w, h), 0)
    bw = pitch * 0.48 * k
    for i, (o, hi, lo, cl) in enumerate(cs):
        dr = ImageDraw.Draw(up if cl >= o else dn)
        x = X(i)
        dr.line([(x, Y(hi)), (x, Y(lo))], fill=255, width=int(1.5 * k))
        y1, y2 = sorted((Y(o), Y(cl)))
        dr.rounded_rectangle([x - bw / 2, y1, x + bw / 2, max(y2, y1 + 3 * k)], radius=int(2 * k), fill=255)
    for mask, colr, a in ((up, P["gold"], 0.38), (dn, P["chart_dn"], 0.22)):
        m = Image.composite(mask.filter(ImageFilter.GaussianBlur(0.6 * k)), mask.filter(ImageFilter.GaussianBlur(6 * k)),
                            focus)
        paste(ImageChops.multiply(m, fade), colr, a)


def _g_card(cv, box, P, radius):
    cv.glass(box, radius, P["tint"], tint_a=P["tint_a"], rim=P["rim"], blur=P.get("blur", 9))


def _design_glass(cv: Canvas, d: dict, H, P):
    T = P
    M = 56
    L, R = M, W - M
    CW = R - L
    dec, sep = d["r_decimals"], d["decimal_sep"]
    if H:
        cv.fill_gradient((0, 0, W, H), P["grad"], shear=0.7)
        cv.glow(W - 150, 190, 780, 660, P["glow1"], 0.62)
        cv.glow(40, H * 0.60, 720, 760, P["glow2"], 0.55)
        cv.glow(W - 40, H - 260, 640, 640, P["glow3"], 0.42)
        _chart(cv, H, P)

    # --- baslik
    cv.hline(L, L + 56, 80, T["gold"], 3, 1.0)
    if d.get("sample"):
        cv.tag(R, 62, "ÖRNEK VERİ", MONT[600], 12, T["gold"], alpha=0.95, radius=14, pad=(14, 8))
    title = d["title"]
    parts = title.split(" ", 1)
    if len(parts) == 2:
        cv.text(L, 150, parts[0], SERIF_CI, cv.fit(parts[0], SERIF_CI, 54, CW), T["gold"])
        s2 = cv.fit(parts[1], SERIF_C, 80, CW, tracking=1)
        y = 150 + 16 + s2 * 0.72
        cv.halo(L - 2, y, parts[1], SERIF_C, s2, P["grad"][0][1], tracking=1, blur=14, alpha=0.62)
        cv.text(L - 2, y, parts[1], SERIF_C, s2, T["ink"], tracking=1)
    else:
        y = 190
        cv.text(L, y, title, SERIF_C, cv.fit(title, SERIF_C, 80, CW), T["ink"])
    cv.text(L, y + 46, tr_upper(d["subtitle"]), MONT[500], 18, T["ink"], tracking=3.4, alpha=0.78)
    y += 46 + 36

    # --- ana kart
    hh = 272
    _g_card(cv, (L, y, R, y + hh), P, 40)
    cv.text(L + 44, y + 60, "HAFTALIK SONUÇ", MONT[600], 15, T["gold"], tracking=4)
    hero = fmt_r(d["total_r"], dec, sep)
    ring_r = 80
    hs = cv.fit(hero, MONT[500], 124, CW - 88 - 2 * ring_r - 84)
    cv.foil(L + 38, y + 80 + 124 * 0.73 + 4, hero, MONT[500], hs, T["foil"], shear=0.5)
    cv.text(L + 44, y + hh - 38, f'{d["n"]} işlemin R toplamı', MONT[500], 17, T["mute"])
    rx, ry = R - 56 - ring_r, y + 122
    wr = d["win_rate"]
    cv.ring(rx, ry, ring_r, 12, (wr or 0) / 100.0, "#FFFFFF", T["gold"], track_alpha=0.13)
    cv.text(rx, ry + 9, fmt_pct(wr) if wr is not None else "—", MONT[600], 40, T["ink"], "m")
    cv.text(rx, ry + 34, "KAZANMA", MONT[600], 11.5, T["mute"], "m", tracking=2.6)
    cap = f'{d["wins"]} / {d["decided"]}  ·  başabaş hariç' if d["decided"] else "başabaş hariç"
    cv.text(rx, y + hh - 32, cap, MONT[500], 14.5, T["mute"], "m")
    y += hh + 20

    # --- uc kutu
    gap = 20
    tw_ = (CW - 2 * gap) / 3.0
    th = 134
    best, worst = d["best"], d["worst"]
    tiles = [
        ("İŞLEM", str(d["n"]), f'{d["wins"]} kazanç · {d["losses"]} kayıp · {d["be"]} başabaş', T["ink"]),
        ("EN İYİ", fmt_r(best["r"], dec, sep) if best else "—", _trade_desc(best),
         _sign_col(best["r"], T) if best else T["ink"]),
        ("EN KÖTÜ", fmt_r(worst["r"], dec, sep) if worst else "—", _trade_desc(worst),
         _sign_col(worst["r"], T) if worst else T["ink"]),
    ]
    for i, (lab, val, sub, colr) in enumerate(tiles):
        x = L + i * (tw_ + gap)
        _g_card(cv, (x, y, x + tw_, y + th), P, 30)
        cv.text(x + 26, y + 38, lab, MONT[600], 13, T["mute"], tracking=3.2)
        cv.text(x + 26, y + 84, val, MONT[600], cv.fit(val, MONT[600], 36, tw_ - 52), colr)
        ss = cv.fit(sub, MONT[500], 15, tw_ - 48, min_size=11)
        cv.text(x + 26, y + 112, cv.ellip(sub, MONT[500], ss, tw_ - 48), MONT[500], ss, T["mute"])
    y += th + 46

    # --- hesaplar (yalnizca R)
    cv.halo(L, y, "HESAPLAR", MONT[600], 15, P["grad"][0][1], tracking=4, blur=9, alpha=0.7)
    cv.text(L, y, "HESAPLAR", MONT[600], 15, T["gold"], tracking=4)
    y += 20
    acc = d["accounts"]
    if acc:
        aw = (CW - (len(acc) - 1) * gap) / float(len(acc))
        ah = 108
        for i, a in enumerate(acc):
            x = L + i * (aw + gap)
            _g_card(cv, (x, y, x + aw, y + ah), P, 30)
            nm = tr_upper(str(a.get("name", "")))
            cv.text(x + 26, y + 38, cv.ellip(nm, MONT[600], 13, aw - 52, 3.2), MONT[600], 13, T["mute"],
                    tracking=3.2)
            rv = fmt_r(a.get("r", 0), dec, sep)
            cv.text(x + 26, y + 84, rv, MONT[600], cv.fit(rv, MONT[600], 34, aw - 52), _sign_col(a.get("r", 0), T))
        y += ah
    y += 46

    # --- islemler
    cv.halo(L, y, "HAFTANIN İŞLEMLERİ", MONT[600], 15, P["grad"][0][1], tracking=4, blur=9, alpha=0.7)
    cv.text(L, y, "HAFTANIN İŞLEMLERİ", MONT[600], 15, T["gold"], tracking=4)
    y += 20
    rows, hidden = _visible(d["trades"])
    nrow = max(1, len(rows)) + (1 if hidden else 0)
    rh = _row_h(nrow, 54)
    card_h = 46 + nrow * rh + 16
    _g_card(cv, (L, y, R, y + card_h), P, 34)
    _table(cv, d, y, L, R, T, base_h=54, font=MONT, marker="circle", style="b", pad=28, cols=(0.245, 0.465, 0.632))
    y += card_h

    # --- alt bilgi
    foot = d["footer"] + ("  ·  ÖRNEK VERİ" if d.get("sample") else "")
    cv.text(L, y + 56, foot, MONT[500], 14, T["mute"], tracking=1.4)
    cv.hline(R - 56, R, y + 51, T["gold"], 3, 1.0)
    return y + 56 + 50


def _glass_fn(key):
    return lambda cv, d, H: _design_glass(cv, d, H, GLASS[key])


# --------------------------------------------------------------------------
# GALERI (P01..P24) - "liquid glass" B yerlesimi, gercekci mum grafigi arka plani.
# Her numara PRESETS icinde adli bir on ayardir: render(data, 7, "x.png") ile ayni
# cikti yeniden uretilir (out_scale=2 -> 2160 px).
# --------------------------------------------------------------------------
def _mix(a, b, t):
    return "#%02X%02X%02X" % _lerp(_rgb(a), _rgb(b), t)


def _mkpal(name, bg, glow, accent, deep, light, ink="#F6F1EA", win="#7FE6BB", loss="#FF8A99", tint=None,
           glow2=None):
    """Birkac temel renkten tam palet turetir."""
    b0, b1, b2 = bg
    lo = _mix(light, accent, 0.35)
    return dict(
        name=name, grad=[(0, b0), (0.5, b1), (1, b2)],
        glow1=glow, glow2=glow2 or _mix(glow, b0, 0.45), glow3=_mix(glow, b1, 0.35),
        gold=accent, deep=deep, light=light,
        # dikey metalik gecis: duraklar Montserrat buyuk harf bandina gore ayarli
        metal=[(0, light), (0.31, light), (0.45, accent), (0.535, deep), (0.60, _mix(accent, deep, 0.4)),
               (0.70, lo), (1, lo)],
        ink=ink, mute=_mix(ink, b2, 0.20), be=_mix(ink, b2, 0.14), win=win, loss=loss,
        tint=tint or _mix(b2, "#FFFFFF", 0.07), rim=_mix(light, "#FFFFFF", 0.55),
        c_up=_mix(accent, b1, 0.08), c_dn=_mix(_mix(ink, accent, 0.35), b1, 0.56),
    )


PALETTES = {
    # --- obsidyen & rose gold ailesi
    "rose": _mkpal("Obsidyen & Rose Gold", ("#0B090C", "#141014", "#1E1518"), "#74423A",
                   "#EBBBA6", "#C68B74", "#FCE6DB", loss="#FF8394"),
    "rose_black": _mkpal("Saf Siyah & Rose Gold", ("#040405", "#0A090B", "#121013"), "#55302B",
                         "#EBBBA6", "#C68B74", "#FCE6DB", loss="#FF8394"),
    "rose_plum": _mkpal("Obsidyen-Mürdüm & Rose Gold", ("#0C0810", "#17101B", "#221625"), "#633352",
                        "#EDBFAE", "#C88E7C", "#FCE7DE", loss="#FF8394"),
    "rose_cocoa": _mkpal("Kakao Obsidyen & Rose Gold", ("#0F0A09", "#1B1311", "#291C18"), "#8C5341",
                         "#E9B49C", "#C2876C", "#FBE0D2", loss="#FF8394"),
    # --- diger paletler
    "platinum": _mkpal("Siyah & Platin", ("#06070A", "#0F1114", "#181B20"), "#3C4654",
                       "#D8DDE5", "#9AA3B0", "#F7F9FC", ink="#F2F4F7"),
    "bronze": _mkpal("Espresso & Bronz", ("#0F0906", "#1B110B", "#2A1A10"), "#7A4A22",
                     "#D6A56C", "#A97941", "#F3D6AB", loss="#FF8E9C"),
    "champagne": _mkpal("Kömür & Şampanya", ("#0E0E0E", "#171716", "#22211F"), "#5A5138",
                        "#E9D9B2", "#BFA871", "#FBF2D8"),
    "blush": _mkpal("Gece Moru & Pudra", ("#110812", "#1E0F20", "#2C162C"), "#6E2F5E",
                    "#F2C6CE", "#CF94A2", "#FDE8EC", loss="#FF9482"),
    "forest": _mkpal("Koyu Orman & Pirinç", ("#060F0A", "#0C1A12", "#13271B"), "#5E4E22",
                     "#DAB86C", "#AC8840", "#F5E2AC", win="#98F0C6", loss="#FF8A93", glow2="#123524"),
    "pearl": _mkpal("Mürekkep & İnci", ("#07080D", "#0F111A", "#191C29"), "#3B4466",
                    "#ECE6DA", "#B9B1A2", "#FFFFFF", ink="#F4F2EE"),
    "burgundy": _mkpal("Bordo & Pembe Altın", ("#12050A", "#210A12", "#34101C"), "#84243A",
                       "#EBC1A2", "#C69272", "#FCE6D2", loss="#FF96A0", win="#8EECC0"),
    "copper": _mkpal("Grafit & Bakır", ("#0C0D0F", "#15171A", "#202327"), "#7A4A30",
                     "#E59D70", "#BD7347", "#F9CCA8", loss="#FF8BA3"),
    "emerald": _mkpal("Oniks & Zümrüt Cam", ("#050807", "#0A100D", "#101814"), "#12503C",
                      "#EADFC6", "#BFAF86", "#FCF6E6", win="#A6F6D0", loss="#FF8A93", tint="#0F3A2C",
                      glow2="#0C3A2B"),
    "aubergine": _mkpal("Patlıcan & Şampanya", ("#10071A", "#1C0D2A", "#2A1338"), "#6A3382",
                        "#EDDAAC", "#C6AC70", "#FBF1D2", loss="#FF8FA1"),
    "noir_gold": _mkpal("Siyah & Altın", ("#050505", "#0C0B09", "#15130E"), "#6B5320",
                        "#E4C57E", "#B48F42", "#FAE9B8"),
    "violet": _mkpal("Duman Moru & Gümüş", ("#0B0A12", "#14131F", "#1F1D2D"), "#4A4378",
                     "#D8D2EA", "#A29BBE", "#F8F6FF", ink="#F3F1F8"),
}

# cam turleri
GLASS_STYLES = {
    "std": dict(blur=9, tint_a=0.46, rim_a=0.62, sheen_a=0.05, soft=True, bottom_a=0.18),
    "frost": dict(blur=15, tint_a=0.45, rim_a=0.74, sheen_a=0.08, soft=True, bottom_a=0.20, bright=1.0,
                  light_a=0.11, glow_a=0.07),
    "clear": dict(blur=5, tint_a=0.36, rim_a=0.78, sheen_a=0.06, soft=True, bottom_a=0.22),
    "smoke": dict(blur=11, tint_a=0.62, rim_a=0.46, sheen_a=0.03, soft=True, bottom_a=0.10, tint_dark=True),
    "tint": dict(blur=10, tint_a=0.60, rim_a=0.66, sheen_a=0.06, soft=True, bottom_a=0.16, tint_accent=True,
                 bright=1.0),
}

# grafik yerlesimleri (olculer mantiksal px; strip = baslik ile ana kart arasinda acikta kalan serit)
CHART_MODES = {
    # ppu = fiyat birimi basina en fazla piksel (mum oranlari gercekci kalsin diye sinirlanir)
    "top": dict(strip=178, height=250, pitch=9, alpha=0.95, fade="top", ppu=12),         # ustte serit
    "full": dict(strip=156, height=900, pitch=10, alpha=0.88, fade="full", ppu=16),      # sayfa boyu, asagi soner
    "band": dict(strip=290, height=316, pitch=8, alpha=1.0, fade="band", ppu=14),        # acik genis bant
    "closeup": dict(strip=176, height=700, pitch=18, alpha=0.78, fade="full", ppu=14),   # yakin plan iri mumlar
    "pano": dict(strip=132, height=196, pitch=5, alpha=0.95, fade="top", ppu=6),         # panorama, cok ince mum
    "diag": dict(strip=150, height=860, pitch=10, alpha=0.90, fade="diag", ppu=16),      # capraz isikla
}

_D = dict(pal="rose", chart="top", glass="std", hero="metal", hero_w=500, hero_layout="std", ring="gradient",
          pill="tri", radius=34, gap=20, grain=0, vignette=0.0, beam=0.0, hollow=False, last=False, labels=True,
          be_text=True,       # be_text=False: "başabaş" yazilari gizlenir (kural ve tablo satiri degismez)
          footer=True,        # footer=False: alt bilgi satiri ve cizgisi cizilmez
          ring_cap=True)      # ring_cap=False: halkanin altindaki "5 / 8" yazisi cizilmez, halka dikeyde ortalanir


def _preset(desc, **kw):
    p = dict(_D)
    p.update(kw)
    p["desc"] = desc
    return p


PRESETS = {
    # ---- obsidyen & rose gold ailesi
    1: _preset("Rose gold · üstte grafik şeridi · standart cam · metalik rakam"),
    2: _preset("Rose gold · sayfa boyu grafik · kalın buzlu cam", chart="full", glass="frost", radius=38),
    3: _preset("Rose gold · açık grafik bandı · birleşik büyük ana kart", chart="band", hero_layout="merged",
               last=True, radius=36),
    4: _preset("Saf siyah · panorama ince mumlar · berrak cam · ince rakam, ince halka", pal="rose_black",
               chart="pano", glass="clear", hero="ink", hero_w=300, ring="thin", pill="text", radius=26,
               vignette=0.30),
    5: _preset("Rose gold · yakın plan iri mumlar · buzlu cam · kontur rakam · kadran halka", chart="closeup",
               glass="frost", hero="outline", ring="ticks", pill="outline", radius=40),
    6: _preset("Mürdüm tonlu · çapraz ışık · dolu rozetler", pal="rose_plum", chart="diag", beam=0.09,
               pill="solid", radius=32),
    7: _preset("Kakao tonlu · füme cam · düz rose rakam · film greni", pal="rose_cocoa", glass="smoke",
               hero="solid", hero_w=600, ring="classic", radius=24, grain=3),
    8: _preset("Saf siyah · sayfa boyu grafik · içi boş düşüş mumları · vinyet", pal="rose_black", chart="full",
               hollow=True, ring="thin", radius=30, vignette=0.35),
    9: _preset("Mürdüm tonlu · açık grafik bandı · buzlu cam · kadran halka", pal="rose_plum", chart="band",
               glass="frost", ring="ticks", pill="text", radius=36, last=True),
    10: _preset("Rose gold · panorama · rose tonlu cam · çok yuvarlak kartlar", chart="pano", glass="tint",
                pill="outline", radius=44, gap=24),
    11: _preset("Kakao tonlu · yakın plan mumlar · berrak cam · çapraz ışık", pal="rose_cocoa", chart="closeup",
                glass="clear", beam=0.08, pill="solid", radius=28),
    12: _preset("Saf siyah · üst şerit · buzlu cam · kontur rakam · birleşik ana kart", pal="rose_black",
                glass="frost", hero="outline", hero_layout="merged", radius=34),
    # ---- diger paletler
    13: _preset("Siyah & platin · sayfa boyu grafik · buzlu cam · ince halka", pal="platinum", chart="full",
                glass="frost", ring="thin", radius=30),
    14: _preset("Espresso & bronz · üst şerit · dolu rozetler · film greni", pal="bronze", pill="solid",
                radius=36, grain=3),
    15: _preset("Kömür & şampanya · açık grafik bandı · birleşik ana kart", pal="champagne", chart="band",
                hero_layout="merged", last=True, radius=34),
    16: _preset("Gece moru & pudra · çapraz ışık · buzlu cam", pal="blush", chart="diag", glass="frost",
                beam=0.08, pill="outline", radius=40),
    17: _preset("Koyu orman & pirinç · üst şerit · füme cam", pal="forest", glass="smoke", ring="classic",
                radius=28),
    18: _preset("Mürekkep & inci · panorama · berrak cam · ince inci rakam", pal="pearl", chart="pano",
                glass="clear", hero="ink", hero_w=300, ring="thin", pill="text", radius=24),
    19: _preset("Bordo & pembe altın · sayfa boyu grafik", pal="burgundy", chart="full", radius=34),
    20: _preset("Grafit & bakır · yakın plan mumlar · buzlu cam · kadran halka", pal="copper", chart="closeup",
                glass="frost", ring="ticks", pill="solid", radius=32),
    21: _preset("Oniks & zümrüt cam · üst şerit · şampanya vurgu", pal="emerald", glass="tint", radius=36),
    22: _preset("Patlıcan & şampanya · açık grafik bandı", pal="aubergine", chart="band", pill="outline",
                radius=34, last=True),
    23: _preset("Siyah & altın · panorama · füme cam · vinyet", pal="noir_gold", chart="pano", glass="smoke",
                ring="thin", radius=26, vignette=0.30),
    24: _preset("Duman moru & gümüş · sayfa boyu grafik · buzlu cam · kontur rakam", pal="violet", chart="full",
                glass="frost", hero="outline", ring="ticks", pill="text", radius=38),
    # ---- secilen son tasarim ("final"): 24'un her seyi + 06'nin renk paleti; "başabaş" yazilari yok
    25: _preset("FINAL · 24'ün düzeni (buzlu cam, kontur rakam, kadran halka) + 06'nın mürdüm & rose gold paleti",
                pal="rose_plum", chart="full", glass="frost", hero="outline", ring="ticks", pill="text", radius=38,
                be_text=False, footer=False, ring_cap=False, name="final"),
}
FINAL_PRESET = 25


def _ohlc(n, direction, seed, warm=30):
    """Gercekci OHLC dizisi: oynaklik kumelenmesi (GARCH benzeri), itki / yatay / geri cekilme
    rejimleri, kalin kuyruklar, dojiler, ara sira bosluk ve uzun fitiller. Yalnizca
    random.Random(seed).random() ve toplama/carpma kullanir: her platformda ayni sonuc."""
    rng = random.Random(seed * 10 + direction + 1)
    nrm = lambda: sum(rng.random() for _ in range(12)) - 6.0
    N = n + warm
    drift, vmul = [], []
    kind = "cons"
    while len(drift) < N:
        r = rng.random()
        if kind == "imp":
            kind = "cons" if r < 0.55 else "pull"
        elif kind == "cons":
            kind = "imp" if r < 0.68 else "pull"
        else:
            kind = "imp" if r < 0.62 else "cons"
        if kind == "imp":
            dur, dd, vm = 5 + int(rng.random() * 9), 0.42, 1.30
        elif kind == "cons":
            dur, dd, vm = 7 + int(rng.random() * 13), 0.0, 0.50
        else:
            dur, dd, vm = 3 + int(rng.random() * 7), -0.55, 1.10
        if direction == 0:
            dd *= 0.55 if rng.random() < 0.5 else -0.55
        else:
            dd *= direction
        drift += [dd] * dur
        vmul += [vm] * dur
    s2, shock, rets, sig, doji = 1.0, 0.0, [], [], []
    for i in range(N):
        s2 = min(5.0, 0.06 + 0.22 * shock * shock + 0.74 * s2)
        s = math.sqrt(s2) * vmul[i]
        z = nrm()
        u = rng.random()
        dj = False
        if u < 0.05:
            z *= 2.2                         # kalin kuyruk: ara sira buyuk mum
        elif u < 0.15:
            z *= 0.07                        # doji
            dj = True
        shock = s * z * 1.30
        rets.append(drift[i] * (0.0 if dj else 1.0) + shock)
        sig.append(s)
        doji.append(dj)
    vis = rets[warm:]
    target = direction * 2.2 * math.sqrt(n)
    adj = (target - sum(vis)) / float(n)
    out, c = [], 100.0
    for i in range(N):
        o = c + (0.5 * sig[i] * nrm() if rng.random() < 0.06 else 0.0)        # bosluk
        c = c + rets[i] + adj
        cap = 2.4 + 0.8 * abs(c - o)          # fitil ust siniri: ara sira uzun, ama asiri degil
        wu = min(cap, sig[i] * 0.55 * abs(nrm()) * (2.6 if rng.random() < 0.06 else 1.0) * (1.6 if doji[i] else 1.0))
        wl = min(cap, sig[i] * 0.55 * abs(nrm()) * (2.6 if rng.random() < 0.06 else 1.0) * (1.6 if doji[i] else 1.0))
        out.append((o, max(o, c) + wu, min(o, c) - wl, c))
    return out[warm:]


def _chart_real(cv: Canvas, P, box, direction, hero_top, mode, hollow=False, last=False, labels=True,
                seed=CHART_SEED):
    """Arka plan mum grafigi: ince fitil, slotun ~%65'i govde, soluk yatay fiyat cizgileri ve
    sag kenarda kucuk fiyat etiketleri. Hareketli ortalama ve hacim yok. Kartlarin disinda keskin."""
    if cv.dry:
        return
    x1, y1, x2, y2 = box
    out = cv.out
    u = max(1, int(round(cv.S / float(out))))          # cikti pikseli basina cihaz pikseli
    gutter = 50 if labels else 8
    pitch = int(mode["pitch"])                          # 1x cikti pikseli
    body = max(1, int(round(pitch * 0.66)))
    if body % 2 == 0:
        body -= 1                                       # fitil (1 px) tam ortada kalsin
    n = int((x2 - x1 - gutter) // pitch)
    cs = _ohlc(n, direction, seed)
    lo, hi = min(c[2] for c in cs), max(c[1] for c in cs)
    py1, py2 = y1 + 10.0, y2 - 12.0
    ppu = min((py2 - py1) / (hi - lo), float(mode.get("ppu", 1e9)))
    Y = lambda p: py1 + (hi - p) * ppu                   # ustten hizali
    xr = x2 - gutter                                    # mumlarin sag siniri
    X = lambda i: xr - pitch * (n - i) + pitch // 2     # mum merkezi (tam piksel)
    fk = mode["fade"]
    span = float(y2 - y1)

    def fade(x, y):
        if fk == "top":
            v = 1.0 if y < hero_top + 24 else max(0.0, 1.0 - (y - hero_top - 24) / max(1.0, y2 - hero_top - 24))
            return v * (0.50 + 0.50 * min(1.0, x / 460.0))
        if fk == "full":
            t = (y - y1) / span
            v = 1.0 if t < 0.40 else (1.0 - (t - 0.40) / 0.40 * 0.56 if t < 0.80 else max(0.0, 0.44 * (1 - (t - 0.80) / 0.20)))
            return v * (0.60 + 0.40 * min(1.0, x / 420.0))
        if fk == "band":
            return 0.30 + 0.70 * min(1.0, x / 150.0)
        d = x / float(W) - (y - y1) / span              # diag: sag ustte belirgin
        return max(0.0, min(1.0, (d + 0.62) / 0.85))

    A = mode["alpha"]
    dev = lambda v: int(round(v * out)) * u             # mantiksal -> cikti pikseline oturmus cihaz px

    # yatay fiyat cizgileri + etiketler
    raw = min(hi - lo, (py2 - py1) / ppu) / 5.0
    step = next((s for s in (0.5, 1, 2, 2.5, 5, 10, 20, 25, 50) if s >= raw * 0.8), 50)
    lvl = math.ceil(lo / step) * step
    while lvl < hi:
        yy = Y(lvl)
        if py1 + 6 < yy < py2 - 2:
            f = fade(W * 0.75, yy)
            cv.d.rectangle([dev(x1), dev(yy), dev(xr + 8) - 1, dev(yy) + u - 1], fill=_rgba("#FFFFFF", 0.075 * A * f))
            if labels and f > 0.05:
                cv.text(x2 - 2, yy + 3.6, f"{lvl:.2f}", INTER[500], 10, P["mute"], "r", alpha=min(1.0, 0.62 * A * f))
        lvl += step

    # mumlar
    for i, (o, h_, l_, c) in enumerate(cs):
        up = c >= o
        xc = X(i)
        yb1, yb2 = sorted((Y(o), Y(c)))
        f = fade(xc, (Y(h_) + Y(l_)) / 2.0)
        if f <= 0.02:
            continue
        col = P["c_up"] if up else P["c_dn"]
        a = (0.86 if up else 0.80) * A * f
        px = int(round(xc * out)) * u
        bh = (body * out // 2) * u if out > 1 else (body // 2) * u
        wk = max(1, out) * u
        wx = px - (wk // 2 if out > 1 else 0)
        t1, t2 = dev(Y(h_)), dev(Y(l_))
        b1, b2 = dev(yb1), max(dev(yb2), dev(yb1) + u)
        bx1, bx2 = px - bh, px + bh + (u if out == 1 else 0)
        if out > 1:
            wx, bx1, bx2 = px - wk // 2, px - (body * out // 2) * u, px + (body * out // 2) * u
        cv.d.rectangle([wx, t1, wx + wk - 1, max(t1, b1 - 1)], fill=_rgba(col, a))          # ust fitil
        cv.d.rectangle([wx, b2, wx + wk - 1, max(b2, t2)], fill=_rgba(col, a))              # alt fitil
        if hollow and not up and (b2 - b1) > 3 * u and body >= 5:
            cv.d.rectangle([bx1, b1, bx2 - 1, b2 - 1], outline=_rgba(col, a), width=u * max(1, out))
        else:
            cv.d.rectangle([bx1, b1, bx2 - 1, b2 - 1], fill=_rgba(col, a))

    # son fiyat: kesikli cizgi + kucuk etiket
    if last:
        yy = Y(cs[-1][3])
        f = fade(W * 0.9, yy)
        xx = x1
        while xx < xr + 6:
            cv.d.rectangle([dev(xx), dev(yy), dev(min(xx + 5, xr + 6)) - 1, dev(yy) + u - 1],
                           fill=_rgba(P["gold"], 0.55 * A * f * (0.35 + 0.65 * min(1.0, xx / 300.0))))
            xx += 10
        if labels:
            cv.rect((xr + 8, yy - 8.5, x2, yy + 8.5), fill=P["gold"], fa=0.92 * f, radius=3)
            cv.text(x2 - 4, yy + 3.6, f"{cs[-1][3]:.2f}", INTER[700], 10, P["grad"][0][1], "r")


def _direction(total_r) -> int:
    """grafik yonu haftanin sonucunu izler: + yukari, - asagi, sifira yakin yatay"""
    return 1 if total_r > 0.25 else (-1 if total_r < -0.25 else 0)


def _design_lux(cv: Canvas, d: dict, H, pr):
    P = PALETTES[pr["pal"]]
    T = P
    M = 56
    L, R = M, W - M
    CW = R - L
    dec, sep = d["r_decimals"], d["decimal_sep"]
    rad, gap = pr["radius"], pr["gap"]
    direction = _direction(d["total_r"])
    G = dict(GLASS_STYLES[pr["glass"]])
    tint = P["tint"]
    if G.pop("tint_accent", False):
        tint = _mix(P["tint"], P["deep"], 0.08) if pr["pal"] != "emerald" else P["tint"]
    if G.pop("tint_dark", False):
        tint = _mix(P["grad"][1][1], P["grad"][2][1], 0.5)
    bg0 = P["grad"][0][1]

    def card(box, r):
        cv.glass(box, r, tint, rim=P["rim"], **G)

    # --- baslik olculeri (grafik bunun altinda baslar: baslik bolgesi temiz kalir)
    title = d["title"]
    parts = title.split(" ", 1)
    if len(parts) == 2:
        s1 = cv.fit(parts[0], SERIF_CI, 54, CW)
        s2 = cv.fit(parts[1], SERIF_C, 80, CW, tracking=1)
        y_t = 150 + 16 + s2 * 0.72
    else:
        s2 = cv.fit(title, SERIF_C, 80, CW)
        y_t = 190
    y_sub = y_t + 46
    mode = CHART_MODES[pr["chart"]]
    chart_top = y_sub + 28
    hero_top = chart_top + mode["strip"]
    chart_bot = chart_top + mode["height"]

    # --- zemin
    if H:
        cv.fill_gradient((0, 0, W, H), P["grad"], shear=0.7)
        cv.glow(W - 140, chart_top + 40, 800, 640, P["glow1"], 0.60)
        cv.glow(30, H * 0.62, 720, 760, P["glow2"], 0.55)
        cv.glow(W - 30, H - 280, 660, 660, P["glow3"], 0.45)
        if pr["beam"]:
            cv.beam(P["light"], pr["beam"])
        _chart_real(cv, P, (0, chart_top, W - 6, chart_bot), direction, hero_top, mode, hollow=pr["hollow"],
                    last=pr["last"], labels=pr["labels"] and bool(d.get("sample")))     # [bot] gercek karta rakam yok
        if pr["vignette"]:
            cv.vignette(pr["vignette"])
        if pr["grain"]:
            cv.grain(pr["grain"])

    # --- baslik
    cv.hline(L, L + 56, 80, T["gold"], 3, 1.0)
    if d.get("sample"):
        cv.tag(R, 62, "ÖRNEK VERİ", MONT[600], 12, T["gold"], alpha=0.95, radius=14, pad=(14, 8))
    if len(parts) == 2:
        cv.text(L, 150, parts[0], SERIF_CI, s1, T["gold"])
        cv.text(L - 2, y_t, parts[1], SERIF_C, s2, T["ink"], tracking=1)
    else:
        cv.text(L, y_t, title, SERIF_C, s2, T["ink"])
    cv.text(L, y_sub, tr_upper(d["subtitle"]), MONT[500], 18, T["ink"], tracking=3.4, alpha=0.80)

    def section(y, label):
        """bolum etiketi; arkasinda grafik varsa kucuk cam hap icinde"""
        if y < chart_bot + 6:
            wl = cv.tw(label, MONT[600], 13.5, 3.6)
            cv.glass((L, y - 23, L + wl + 34, y + 11), 17, tint, rim=P["rim"], **dict(G, soft=False, shadow_a=0.25))
            cv.text(L + 17, y, label, MONT[600], 13.5, T["gold"], tracking=3.6)
        else:
            cv.text(L, y, label, MONT[600], 15, T["gold"], tracking=4)
        return y + 22

    neg = d["total_r"] < -0.049
    metal = P["metal"] if not neg else [(p_, _mix(c_, "#B9B4B8", 0.62)) for p_, c_ in P["metal"]]
    apx = "≈" if d.get("approx") else ""            # [bot] yaklasik R iceren her toplam "≈" ile baslar
    hero = apx + fmt_r(d["total_r"], dec, sep)
    wr = d["win_rate"]

    def hero_number(x, base, size, max_w):
        hf = MONT[pr["hero_w"]]
        hs = cv.fit(hero, hf, size, max_w)
        kind = pr["hero"]
        if kind == "outline":
            cv.halo(x, base + 4, hero, MONT[400], hs, "#000000", blur=8, alpha=0.36)
            cv.stroke_text(x, base, hero, MONT[400], hs, metal, stroke=max(1.8, hs / 50.0), fill_a=0.30)
        elif kind == "solid":
            cv.halo(x, base + 3, hero, hf, hs, "#000000", blur=7, alpha=0.40)
            cv.text(x, base, hero, hf, hs, T["gold"] if not neg else _mix(T["gold"], "#B9B4B8", 0.62))
        elif kind == "ink":
            cv.text(x, base, hero, hf, hs, T["ink"])
        else:
            cv.halo(x, base + 4, hero, hf, hs, "#000000", blur=8, alpha=0.42)
            cv.foil(x, base, hero, hf, hs, metal, shear=0.0, vertical=True)

    def ring(rx, ry, rr_):
        frac = (wr or 0) / 100.0
        kind = pr["ring"]
        if kind == "classic":
            cv.ring(rx, ry, rr_, 12, frac, "#FFFFFF", T["gold"], track_alpha=0.13)
        elif kind == "thin":
            cv.ring2(rx, ry, rr_, 5, frac, P["deep"], P["light"], track_a=0.16, track_w=1.5, glow_a=0.22)
        elif kind == "ticks":
            cv.ring_ticks(rx, ry, rr_, frac, T["gold"])
        else:
            cv.ring2(rx, ry, rr_, 11, frac, P["deep"], P["light"], track_a=0.11, track_w=4, glow_a=0.26)
        inner = 44 if kind == "ticks" else 40
        pct = (apx + fmt_pct(wr)) if wr is not None else "—"                 # [bot]
        psz = inner - 4 if kind == "ticks" else inner
        cv.text(rx, ry + 9, pct, MONT[600], cv.fit(pct, MONT[600], psz, 2 * rr_ - 44) if apx else psz, T["ink"], "m")
        cv.text(rx, ry + 33, "KAZANMA", MONT[600], 11, T["mute"], "m", tracking=2.6)

    if pr.get("be_text", True):
        cap = f'{d["wins"]} / {d["decided"]}  ·  başabaş hariç' if d["decided"] else "başabaş hariç"
        counts = f'{d["wins"]} kazanç · {d["losses"]} kayıp · {d["be"]} başabaş'
    else:
        cap = f'{d["wins"]} / {d["decided"]}' if d["decided"] else ""
        counts = f'{d["wins"]} kazanç · {d["losses"]} kayıp' 
    best, worst = d["best"], d["worst"]
    stats = [
        ("İŞLEM", str(d["n"]), counts, T["ink"]),
        ("EN İYİ", (("≈" if best.get("approx") else "") + fmt_r(best["r"], dec, sep)) if best else "—",   # [bot]
         _trade_desc(best),
         _sign_col(best["r"], T) if best else T["ink"]),
        ("EN KÖTÜ", (("≈" if worst.get("approx") else "") + fmt_r(worst["r"], dec, sep)) if worst else "—",   # [bot]
         _trade_desc(worst),
         _sign_col(worst["r"], T) if worst else T["ink"]),
    ]
    y = hero_top
    if pr["hero_layout"] == "merged":
        # --- tek buyuk kart: rakam + halka + alt satirda uc ozet
        top_h, low_h = 284, 128
        card((L, y, R, y + top_h + low_h), rad + 4)
        cv.text(L + 44, y + 60, d.get("hero_label", "HAFTALIK SONUÇ"), MONT[600], 15, T["gold"],
                tracking=4)                         # [bot] hero_label
        rr_ = 86
        hero_number(L + 38, y + 82 + 140 * 0.73 + 4, 140, CW - 88 - 2 * rr_ - 80)
        cv.text(L + 44, y + top_h - 36, f'{d["n"]} işlemin R toplamı', MONT[500], 17, T["mute"])
        rx, ry = R - 58 - rr_, y + (128 if pr.get("ring_cap", True) else top_h / 2.0)
        ring(rx, ry, rr_)
        if pr.get("ring_cap", True):
            cv.text(rx, y + top_h - 30, cap, MONT[500], 14.5, T["mute"], "m")
        cv.hline(L + 36, R - 36, y + top_h, "#FFFFFF", 1, 0.12)
        cw_ = (CW - 72) / 3.0
        for i, (lab, val, sub, colr) in enumerate(stats):
            x = L + 36 + i * cw_ + (0 if i == 0 else 26)
            if i:
                cv.vline(L + 36 + i * cw_, y + top_h + 26, y + top_h + low_h - 24, "#FFFFFF", 1, 0.12)
            cv.text(x, y + top_h + 38, lab, MONT[600], 12.5, T["mute"], tracking=3.2)
            cv.text(x, y + top_h + 78, val, MONT[600], cv.fit(val, MONT[600], 31, cw_ - 40), colr)
            ss = cv.fit(sub, MONT[500], 14, cw_ - 34, min_size=10.5)
            cv.text(x, y + top_h + 102, cv.ellip(sub, MONT[500], ss, cw_ - 34), MONT[500], ss, T["mute"])
        y += top_h + low_h + 46
    else:
        hh = 272
        card((L, y, R, y + hh), rad + 4)
        cv.text(L + 44, y + 60, d.get("hero_label", "HAFTALIK SONUÇ"), MONT[600], 15, T["gold"],
                tracking=4)                         # [bot] hero_label
        rr_ = 80
        hero_number(L + 38, y + 80 + 124 * 0.73 + 4, 124, CW - 88 - 2 * rr_ - 84)
        cv.text(L + 44, y + hh - 38, f'{d["n"]} işlemin R toplamı', MONT[500], 17, T["mute"])
        rx, ry = R - 56 - rr_, y + (122 if pr.get("ring_cap", True) else hh / 2.0)
        ring(rx, ry, rr_)
        if pr.get("ring_cap", True):
            cv.text(rx, y + hh - 32, cap, MONT[500], 14.5, T["mute"], "m")
        y += hh + gap
        tw_ = (CW - 2 * gap) / 3.0
        th = 134
        for i, (lab, val, sub, colr) in enumerate(stats):
            x = L + i * (tw_ + gap)
            card((x, y, x + tw_, y + th), rad - 4)
            cv.text(x + 26, y + 38, lab, MONT[600], 13, T["mute"], tracking=3.2)
            cv.text(x + 26, y + 84, val, MONT[600], cv.fit(val, MONT[600], 36, tw_ - 52), colr)
            ss = cv.fit(sub, MONT[500], 15, tw_ - 48, min_size=11)
            cv.text(x + 26, y + 112, cv.ellip(sub, MONT[500], ss, tw_ - 48), MONT[500], ss, T["mute"])
        y += th + 46

    # --- hesaplar (yalnizca R)
    acc = d["accounts"]
    if acc:                                         # [bot] gercek hesap yoksa (deneme modu) bolum de cizilmez
        y = section(y, "HESAPLAR")
        aw = (CW - (len(acc) - 1) * gap) / float(len(acc))
        ah = 108
        for i, a in enumerate(acc):
            x = L + i * (aw + gap)
            card((x, y, x + aw, y + ah), rad - 4)
            nm = tr_upper(str(a.get("name", "")))
            cv.text(x + 26, y + 38, cv.ellip(nm, MONT[600], 13, aw - 52, 3.2), MONT[600], 13, T["mute"],
                    tracking=3.2)
            if "r" in a and a["r"] is None:         # [bot] bu hesabin R'si bilinmiyor
                cv.text(x + 26, y + 84, "—", MONT[600], 34, T["mute"])
                continue
            rv = (("≈" if a.get("approx") else "") + fmt_r(a.get("r", 0), dec, sep)     # [bot] yaklasik: "≈",
                  + ("*" if a.get("partial") else ""))                                  # eksik islem: "*"
            cv.text(x + 26, y + 84, rv, MONT[600], cv.fit(rv, MONT[600], 34, aw - 52), _sign_col(a.get("r", 0), T))
        y += ah
        y += 46                                     # [bot] (bolum yoksa altindaki bosluk da yok)

    # --- islemler
    y = section(y, d.get("table_label", "HAFTANIN İŞLEMLERİ"))      # [bot] table_label
    rows, hidden = _visible(d["trades"])
    hidden = int(d.get("hidden") or hidden)         # [bot] trades'te olmayan (yalnizca sayilan) satirlar
    nrow = max(1, len(rows)) + (1 if hidden else 0) + len(_footnotes(d))      # [bot] aciklama satirlari
    rh = _row_h(nrow, 54)
    card_h = 46 + nrow * rh + 16
    card((L, y, R, y + card_h), rad)
    _table(cv, d, y, L, R, T, base_h=54, font=MONT, marker="circle", style="g", pad=28,
           cols=(0.245, 0.465, 0.632), pill=pr["pill"])
    y += card_h

    # --- alt bilgi
    if not pr.get("footer", True):
        return y + M                      # alt bilgi yok: tablodan sonra kenar boslugu kadar pay
    foot = d["footer"] + ("  ·  ÖRNEK VERİ" if d.get("sample") else "")
    cv.text(L, y + 56, foot, MONT[500], 14, T["mute"], tracking=1.4)
    cv.hline(R - 56, R, y + 51, T["gold"], 3, 1.0)
    return y + 56 + 50


def _lux_fn(num):
    return lambda cv, d, H: _design_lux(cv, d, H, PRESETS[num])


def sample_negative() -> dict:
    """Eksi kapanan ornek hafta (toplam -2.4R)."""
    rows = [
        ("2026-11-09", "ETH", "LONG", 0.6, "TP1"),
        ("2026-11-09", "EURUSD", "SHORT", -1.0, "SL"),
        ("2026-11-10", "BTC", "LONG", -1.0, "SL"),
        ("2026-11-11", "XAUUSD", "LONG", 0.8, "TP1 %50 → BE"),
        ("2026-11-11", "SOL", "SHORT", -1.0, "SL"),
        ("2026-11-12", "GBPUSD", "LONG", 0.0, "BE"),
        ("2026-11-13", "BTC", "SHORT", -1.0, "SL"),
        ("2026-11-13", "XAUUSD", "SHORT", -1.0, "SL"),
        ("2026-11-14", "AVAX", "LONG", 1.2, "TP1"),
    ]
    d = sample_data()
    d["trades"] = [dict(date=a, pair=b, side=c, r=r, note=nt) for a, b, c, r, nt in rows]
    d["accounts"] = [{"name": "binance", "r": -1.2}, {"name": "fx", "r": -1.0}, {"name": "tickmill", "r": -0.2}]
    return d


# --------------------------------------------------------------------------
# genel giris
# --------------------------------------------------------------------------
# anahtar -> (cizim fonksiyonu, zemin rengi, varsayilan cikti olcegi)
DESIGNS = {
    "A": (_design_a, TA["bg"], 1),
    "B": (_design_b, TB["bg"], 1),
    "C": (_design_c, TC["bg"], 1),
}
for _k in GLASS:
    DESIGNS[_k] = (_glass_fn(_k), GLASS[_k]["grad"][0][1], 2)
for _n in PRESETS:
    DESIGNS["P%02d" % _n] = (_lux_fn(_n), PALETTES[PRESETS[_n]["pal"]]["grad"][0][1], 1)


def _design_key(design) -> str:
    if isinstance(design, int) or str(design).strip().isdigit():      # galeri numarasi: 7 / "07"
        k = "P%02d" % int(design)
        if k in DESIGNS:
            return k
    if str(design).strip().lower() == "final":                        # secilen son tasarim
        return "P%02d" % FINAL_PRESET
    s = str(design).strip().upper()
    for junk in ("DESIGN", "GLASS", "_", "-", " ", ".PNG"):
        s = s.replace(junk, "")
    if s in DESIGNS:
        return s
    if "G" + s in DESIGNS:           # "glass_1" -> "G1"
        return "G" + s
    raise ValueError(f"bilinmeyen tasarim: {design!r} ({', '.join(DESIGNS)})")


def render(data: dict, design, out_path: str, scale: int = 2, out_scale: int = None):
    """Karneyi cizer ve out_path'e kaydeder (PNG; uzanti .jpg ise JPEG).

    design:    "final" (= 25) | galeri numarasi 1..25 (PRESETS) | "G1".."G4" | "A" | "B" | "C"
    scale:     kenar yumusatma icin ic buyutme katsayisi
    out_scale: cikti genisligi = 1080 * out_scale (galeri varsayilani 1; 2 -> 2160 px)
    (genislik, yukseklik) dondurur."""
    fn, bg, default_out = DESIGNS[_design_key(design)]
    out_scale = default_out if out_scale is None else int(out_scale)
    d = summarize(data)
    # 1. gecis: yalnizca yuksekligi olc
    h = int(round(fn(Canvas(W, 64, bg, 1, dry=True), d, None)))
    # 2. gecis: gercek cizim
    cv = Canvas(W, h, bg, scale * out_scale)
    cv.out = out_scale
    fn(cv, d, h)
    img = cv.finish(out_scale)
    if out_path.lower().endswith((".jpg", ".jpeg")):
        img.save(out_path, quality=93, subsampling=0, optimize=True)
    else:
        img.save(out_path, optimize=True)
    return img.size


if __name__ == "__main__":
    import sys
    here = os.path.dirname(os.path.abspath(__file__))
    # python karne.py 7          -> preset_07.png (1080 px)
    # python karne.py 7 2x       -> preset_07@2x.png (2160 px)
    # python karne.py G1 A       -> glass_1.png, design_A.png
    args = [a for a in sys.argv[1:] if a.lower() != "2x"]
    big = len(args) != len(sys.argv[1:])
    for a in args or ["1"]:
        k = _design_key(a)
        name = f"glass_{k[1:]}" if k in GLASS else (f"preset_{k[1:]}" if k.startswith("P") else f"design_{k}")
        out = os.path.join(here, name + ("@2x" if big else "") + ".png")
        print(out, render(sample_data(), k, out, out_scale=2 if big else None))
