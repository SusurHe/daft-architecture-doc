#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Daft 文档插图生成器：用统一的设计语言批量生成 SVG。

设计语言:
  - 画布浅色底 (--bg-soft)，卡片白底 + 细边框 + 柔和投影
  - 强调色: violet / pink / cyan / amber / emerald / slate
  - 中英混排，字体走系统 sans-serif（HTML 内联后继承页面字体）
"""
from __future__ import annotations

import math
from pathlib import Path

OUT = Path(__file__).resolve().parent / "diagrams"
OUT.mkdir(exist_ok=True)

FONT = ('-apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", '
        '"Hiragino Sans GB", "Microsoft YaHei", "Noto Sans CJK SC", sans-serif')
MONO = '"JetBrains Mono", "SFMono-Regular", Consolas, "Liberation Mono", monospace'

C = {
    "ink": "#0f172a",
    "ink2": "#1e293b",
    "muted": "#64748b",
    "faint": "#94a3b8",
    "line": "#e2e8f0",
    "line2": "#cbd5e1",
    "bg": "#f8fafc",
    "white": "#ffffff",
    "violet": "#7c3aed",
    "violet_bg": "#f5f3ff",
    "violet_line": "#ddd6fe",
    "pink": "#db2777",
    "pink_bg": "#fdf2f8",
    "pink_line": "#fbcfe8",
    "cyan": "#0e7490",
    "cyan_bg": "#ecfeff",
    "cyan_line": "#a5f3fc",
    "amber": "#b45309",
    "amber_bg": "#fffbeb",
    "amber_line": "#fde68a",
    "green": "#047857",
    "green_bg": "#ecfdf5",
    "green_line": "#a7f3d0",
    "slate_bg": "#f1f5f9",
    "slate_line": "#e2e8f0",
}
ACCENTS = {
    "violet": ("violet", "violet_bg", "violet_line"),
    "pink": ("pink", "pink_bg", "pink_line"),
    "cyan": ("cyan", "cyan_bg", "cyan_line"),
    "amber": ("amber", "amber_bg", "amber_line"),
    "green": ("green", "green_bg", "green_line"),
    "slate": ("muted", "slate_bg", "slate_line"),
}


def esc(s: str) -> str:
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def tw(text: str, size: float) -> float:
    """粗略估算文本宽度（CJK 按 1.0em，ASCII 按 0.55em）。"""
    w = 0.0
    for ch in text:
        w += size * (1.0 if ord(ch) > 0x2E80 else 0.552)
    return w


class Svg:
    def __init__(self, w: int, h: int, title: str = "", bg: bool = True):
        self.w, self.h = w, h
        self.parts: list[str] = []
        self.title = title
        self.bg = bg
        self._markers: set[str] = set()
        self._defs: list[str] = []

    # ---------- 基础 ----------
    def raw(self, s: str) -> None:
        self.parts.append(s)

    def rect(self, x, y, w, h, fill="none", stroke="none", rx=0, sw=1.5, dash=None, opacity=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        o = f' opacity="{opacity}"' if opacity else ""
        self.parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" rx="{rx}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="{sw}"{d}{o}/>'
        )

    def text(self, x, y, s, size=14, fill=None, weight="400", anchor="start",
             font=None, opacity=None, style=""):
        fill = fill or C["ink"]
        fam = font or FONT
        o = f' opacity="{opacity}"' if opacity else ""
        st = f' {style}' if style else ""
        self.parts.append(
            f'<text x="{x:.1f}" y="{y:.1f}" font-family=\'{fam}\' font-size="{size}" '
            f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}"{o}{st}>{esc(s)}</text>'
        )

    def line(self, x1, y1, x2, y2, stroke=None, sw=1.5, dash=None, marker=None, opacity=None):
        stroke = stroke or C["line2"]
        d = f' stroke-dasharray="{dash}"' if dash else ""
        m = f' marker-end="url(#{marker})"' if marker else ""
        o = f' opacity="{opacity}"' if opacity else ""
        self.parts.append(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
            f'stroke="{stroke}" stroke-width="{sw}"{d}{m}{o}/>'
        )

    def path(self, d, fill="none", stroke=None, sw=1.5, dash=None, marker=None, opacity=None):
        s = f' stroke="{stroke}"' if stroke else ""
        da = f' stroke-dasharray="{dash}"' if dash else ""
        m = f' marker-end="url(#{marker})"' if marker else ""
        o = f' opacity="{opacity}"' if opacity else ""
        self.parts.append(f'<path d="{d}" fill="{fill}"{s} stroke-width="{sw}"{da}{m}{o}/>')

    def circle(self, cx, cy, r, fill="none", stroke="none", sw=1.5):
        self.parts.append(
            f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{r}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"/>'
        )

    def pill(self, x, y, label, accent="violet", size=12, pad=9, h=22, mono=False):
        key = ACCENTS.get(accent, ACCENTS["violet"])
        fg, bg, ln = C[key[0]], C[key[1]], C[key[2]]
        w = tw(label, size) + pad * 2
        self.rect(x, y, w, h, fill=bg, stroke=ln, rx=h / 2, sw=1.2)
        self.text(x + w / 2, y + h / 2 + size * 0.36, label, size=size, fill=fg,
                  weight="600", anchor="middle", font=MONO if mono else None)
        return w

    # ---------- 复合组件 ----------
    def card(self, x, y, w, h, title, lines=None, accent="violet", badge=None,
             icon=None, title_size=15, line_size=12.5, dash=None, fill=None, tag=None,
             shadow=True, title_top=False):
        key = ACCENTS.get(accent, ACCENTS["violet"])
        fg, bg, ln = C[key[0]], C[key[1]], C[key[2]]
        fill = fill or C["white"]
        if shadow:
            self.rect(x + 1.5, y + 2.5, w, h, fill="#0f172a", rx=13, opacity="0.05")
        self.rect(x, y, w, h, fill=fill, stroke=ln, rx=13, sw=1.5, dash=dash)
        self.rect(x, y, 5, h, fill=fg, rx=2.5, opacity="0.85")
        cx = x + 18
        ty = y + (26 if (lines or title_top) else h / 2 + title_size * 0.36)
        icon_w = 0.0
        if icon:
            self.text(cx, ty, icon, size=title_size + 2, fill=fg, weight="600")
            icon_w = tw(icon, title_size + 2) + 8
            cx += icon_w
        tag_w = 0.0
        if tag:
            tag_w = tw(tag, 11.5) + 16
            self.pill(x + w - tag_w - 12, y + 10, tag, accent=accent, size=11.5, h=20)
        avail = w - 18 - 12 - icon_w - (tag_w + 10 if tag else 0)
        tsize = title_size
        while tsize > 10.5 and tw(title, tsize) > avail:
            tsize -= 0.5
        self.text(cx, ty, title, size=tsize, fill=C["ink"], weight="660")
        if lines:
            ly = ty + 21
            avail_l = w - 30
            for ln_ in lines:
                lsize = line_size
                while lsize > 9.5 and tw(ln_, lsize) > avail_l:
                    lsize -= 0.5
                self.text(x + 18, ly, ln_, size=lsize, fill=C["muted"])
                ly += line_size + 7.5
        if badge:
            self.pill(x + w - tw(badge, 11) - 28, y + h - 28, badge, accent=accent, size=11, h=19)

    def tile(self, x, y, w, h, title, sub=None, accent="green", size=12.6):
        """小方块（用于流程/能力标签），不画投影。"""
        key = ACCENTS.get(accent, ACCENTS["green"])
        fg, bg, ln = C[key[0]], C[key[1]], C[key[2]]
        self.rect(x, y, w, h, fill=bg, stroke=ln, rx=10, sw=1.3)
        ts = size
        while ts > 9.5 and tw(title, ts) > w - 12:
            ts -= 0.5
        self.text(x + w / 2, y + h / 2 + (0 if not sub else -7) + ts * 0.36, title,
                  size=ts, fill=fg, weight="650", anchor="middle")
        if sub:
            ss = 11.2
            while ss > 8.5 and tw(sub, ss) > w - 10:
                ss -= 0.4
            self.text(x + w / 2, y + h / 2 + 16, sub, size=ss, fill=C["muted"], anchor="middle")

    def arrow(self, x1, y1, x2, y2, color=None, label=None, dash=None, sw=1.8,
              label_bg=True, label_size=11.5, curve=None):
        color = color or C["line2"]
        marker = self._marker(color)
        if curve == "down":
            mid = (y1 + y2) / 2
            d = f"M {x1} {y1} L {x1} {mid} L {x2} {mid} L {x2} {y2}"
            self.path(d, stroke=color, sw=sw, dash=dash, marker=marker)
        elif curve == "right":
            mid = (x1 + x2) / 2
            d = f"M {x1} {y1} L {mid} {y1} L {mid} {y2} L {x2} {y2}"
            self.path(d, stroke=color, sw=sw, dash=dash, marker=marker)
        else:
            self.line(x1, y1, x2, y2, stroke=color, sw=sw, dash=dash, marker=marker)
        if label:
            lx, ly = (x1 + x2) / 2, (y1 + y2) / 2
            if label_bg:
                lw = tw(label, label_size) + 12
                self.rect(lx - lw / 2, ly - label_size * 0.95, lw, label_size + 8,
                          fill=C["white"], rx=6, opacity="0.92")
            self.text(lx, ly + label_size * 0.34, label, size=label_size,
                      fill=C["muted"], anchor="middle", weight="500")

    def band(self, x, y, w, h, label, accent="slate", sub=None, fill=None):
        key = ACCENTS.get(accent, ACCENTS["slate"])
        fg, bg, ln = C[key[0]], C[key[1]], C[key[2]]
        self.rect(x, y, w, h, fill=fill or bg, stroke=ln, rx=16, sw=1.4, dash="6 5")
        self.text(x + 16, y + 22, label, size=13.5, fill=fg, weight="700")
        if sub:
            self.text(x + 16 + tw(label, 13.5) + 10, y + 22, sub, size=12, fill=C["faint"])

    def note(self, x, y, w, text, accent="amber", size=12.5, pad=12, h=None):
        key = ACCENTS.get(accent, ACCENTS["amber"])
        fg, bg, ln = C[key[0]], C[key[1]], C[key[2]]
        lines: list[str] = []
        for para in text.split("\n"):
            cur = ""
            for ch in para:
                if tw(cur + ch, size) > w - pad * 2:
                    lines.append(cur)
                    cur = ch
                else:
                    cur += ch
            lines.append(cur)
        lh = size + 6
        h = h or (pad * 1.6 + len(lines) * lh)
        self.rect(x, y, w, h, fill=bg, stroke=ln, rx=10, sw=1.2)
        self.rect(x, y, 4, h, fill=fg, rx=2, opacity="0.8")
        ty = y + pad + size * 0.9
        for line_ in lines:
            self.text(x + pad, ty, line_, size=size, fill=C["ink2"])
            ty += lh
        return h

    def title_block(self, x, y, main, sub=None, size=19):
        self.text(x, y, main, size=size, fill=C["ink"], weight="700")
        if sub:
            self.text(x, y + 21, sub, size=12.5, fill=C["muted"])

    # ---------- 输出 ----------
    def _marker(self, color: str) -> str:
        mid = "arw" + color.replace("#", "")
        if mid not in self._markers:
            self._markers.add(mid)
            self._defs.append(
                f'<marker id="{mid}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" '
                f'markerHeight="7" orient="auto-start-reverse">'
                f'<path d="M 0 0 L 10 5 L 0 10 z" fill="{color}"/></marker>'
            )
        return mid

    def render(self) -> str:
        head = (
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" '
            f'width="{self.w}" height="{self.h}" role="img" '
            f'aria-label="{esc(self.title)}">'
        )
        defs = "<defs>" + "".join(self._defs) + "</defs>" if self._defs else ""
        bg = (f'<rect width="{self.w}" height="{self.h}" fill="{C["bg"]}" rx="14"/>'
              if self.bg else "")
        return head + defs + bg + "".join(self.parts) + "</svg>"


def write(name: str, svg: Svg) -> None:
    # render() 会收集 marker defs，需先调用
    out = svg.render()
    (OUT / f"{name}.svg").write_text(out, encoding="utf-8")
    print(f"  -> diagrams/{name}.svg  ({len(out) / 1024:.1f} KB)")
