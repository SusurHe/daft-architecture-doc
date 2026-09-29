#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
构建 Daft 架构剖析文档：
  docs 源文件 (Markdown)  ->  自包含 HTML（内联 SVG 图 + 侧边目录 + 代码高亮）

用法:
    python build_doc.py

约定:
    <!-- diagram:ID caption="图注" -->
    ```mermaid
    ...
    ```
    上面的 mermaid 代码块会被替换为 diagrams/ID.svg 的内容（figure + figcaption）。
    找不到 SVG 时保留 mermaid 源码块，并在控制台告警。
"""
from __future__ import annotations

import html
import re
import sys
from pathlib import Path

import markdown
from pygments.formatters import HtmlFormatter

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "Daft架构深度剖析.md"
OUT = ROOT / "Daft架构深度剖析.html"
DIAGRAMS = ROOT / "diagrams"

DIAGRAM_RE = re.compile(
    r'<!--\s*diagram:(?P<id>[A-Za-z0-9_\-]+)(?:\s+caption="(?P<caption>[^"]*)")?\s*-->\s*'
    r'(?:```mermaid\n.*?\n```)?',
    re.DOTALL,
)


def inject_diagrams(text: str) -> tuple[str, list[str]]:
    missing: list[str] = []

    def repl(m: re.Match[str]) -> str:
        did = m.group("id")
        caption = m.group("caption") or ""
        svg_path = DIAGRAMS / f"{did}.svg"
        if not svg_path.exists():
            missing.append(did)
            return m.group(0)
        svg = svg_path.read_text(encoding="utf-8")
        svg = re.sub(r"<\?xml.*?\?>", "", svg, flags=re.DOTALL)
        cap = f'<figcaption><span class="fig-no">图</span>{html.escape(caption)}</figcaption>' if caption else ""
        return f'<figure class="diagram" id="fig-{did}">{svg}{cap}</figure>'

    return DIAGRAM_RE.sub(repl, text), missing


def build_toc(tokens: list[dict], level: int = 0) -> str:
    if not tokens:
        return ""
    items = []
    for t in tokens:
        children = build_toc(t.get("children", []), level + 1)
        items.append(
            f'<li class="lvl{level}"><a href="#{t["id"]}">{html.escape(t["name"])}</a>{children}</li>'
        )
    return f'<ul class="toc-lvl{level}">' + "".join(items) + "</ul>"


TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
:root {{
  --ink: #0f172a;
  --ink-2: #1e293b;
  --muted: #64748b;
  --line: #e2e8f0;
  --bg: #ffffff;
  --bg-soft: #f8fafc;
  --violet: #7c3aed;
  --violet-soft: #f3e8ff;
  --pink: #db2777;
  --cyan: #0e7490;
  --amber: #b45309;
  --green: #047857;
  --code-bg: #f6f8fa;
  --radius: 14px;
}}
* {{ box-sizing: border-box; }}
html {{ scroll-behavior: smooth; scroll-padding-top: 24px; }}
body {{
  margin: 0;
  color: var(--ink);
  background: var(--bg);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC",
    "Hiragino Sans GB", "Microsoft YaHei", "Source Han Sans SC", "Noto Sans CJK SC", sans-serif;
  font-size: 16.5px;
  line-height: 1.85;
  -webkit-font-smoothing: antialiased;
}}
.layout {{ display: flex; align-items: flex-start; max-width: 1560px; margin: 0 auto; }}
nav.side {{
  position: sticky; top: 0; flex: 0 0 320px; height: 100vh; overflow-y: auto;
  padding: 28px 18px 60px 26px; border-right: 1px solid var(--line);
  background: linear-gradient(180deg, #fbfbfe 0%, #ffffff 65%);
  font-size: 13.5px; line-height: 1.55;
}}
nav.side .brand {{ display: flex; align-items: center; gap: 10px; margin-bottom: 6px; }}
nav.side .brand svg {{ flex: 0 0 auto; }}
nav.side .brand b {{ font-size: 16px; letter-spacing: .2px; }}
nav.side .sub {{ color: var(--muted); font-size: 12px; margin: 0 0 18px 42px; }}
nav.side ul {{ list-style: none; margin: 0; padding-left: 0; }}
nav.side li.lvl0 > a {{ font-weight: 650; color: var(--ink); }}
nav.side li.lvl1 > a {{ color: var(--ink-2); }}
nav.side li.lvl2 > a, nav.side li.lvl3 > a {{ color: var(--muted); }}
nav.side li.lvl1, nav.side li.lvl2, nav.side li.lvl3 {{ padding-left: 12px; border-left: 1px solid var(--line); }}
nav.side a {{ display: block; padding: 3px 8px; border-radius: 8px; text-decoration: none; }}
nav.side a:hover {{ background: var(--violet-soft); color: var(--violet); }}
main {{ flex: 1 1 auto; min-width: 0; padding: 40px 64px 120px; max-width: 1080px; }}
h1 {{ font-size: 2.15rem; line-height: 1.3; margin: 0 0 .4em; letter-spacing: -.4px; }}
h2 {{
  font-size: 1.5rem; margin: 3.2em 0 1em; padding-bottom: .35em;
  border-bottom: 2px solid var(--line); letter-spacing: -.2px;
}}
h2::before {{
  content: ""; display: inline-block; width: 9px; height: 9px; border-radius: 3px;
  background: linear-gradient(135deg, var(--violet), var(--pink));
  margin-right: 12px; transform: translateY(-2px);
}}
h3 {{ font-size: 1.17rem; margin: 2.2em 0 .7em; color: var(--ink-2); }}
h4 {{ font-size: 1.02rem; margin: 1.7em 0 .5em; color: var(--ink-2); }}
p {{ margin: .75em 0; }}
a {{ color: var(--violet); text-decoration: none; border-bottom: 1px solid rgba(124,58,237,.28); }}
a:hover {{ border-bottom-color: var(--violet); }}
strong {{ color: var(--ink); font-weight: 660; }}
code {{
  font-family: "JetBrains Mono", "SFMono-Regular", Consolas, "Liberation Mono", monospace;
  font-size: .855em; background: var(--code-bg); padding: .16em .42em;
  border-radius: 6px; border: 1px solid #e6e9ef; color: #b91c66;
}}
pre {{ background: var(--code-bg); border: 1px solid var(--line); border-radius: var(--radius);
  padding: 14px 16px; overflow-x: auto; line-height: 1.62; font-size: 13.4px; }}
pre code {{ background: none; border: none; padding: 0; color: var(--ink); font-size: 13.4px; }}
.codehilite {{ background: var(--code-bg); border: 1px solid var(--line); border-radius: var(--radius);
  padding: 12px 16px; overflow-x: auto; margin: 1em 0; }}
.codehilite pre {{ border: none; padding: 0; margin: 0; background: none; }}
blockquote {{
  margin: 1.1em 0; padding: .7em 1.1em; border-left: 4px solid var(--violet);
  background: linear-gradient(90deg, #faf5ff, #ffffff); border-radius: 0 10px 10px 0; color: var(--ink-2);
}}
blockquote p {{ margin: .3em 0; }}
table {{ border-collapse: collapse; width: 100%; margin: 1.2em 0; font-size: 14.6px; display: block; overflow-x: auto; }}
th, td {{ border: 1px solid var(--line); padding: 8px 12px; text-align: left; vertical-align: top; }}
th {{ background: var(--bg-soft); font-weight: 660; }}
tr:nth-child(even) td {{ background: #fcfdff; }}
hr {{ border: none; border-top: 1px solid var(--line); margin: 3em 0; }}
figure.diagram {{
  margin: 2em 0; padding: 18px 18px 6px; background: var(--bg-soft);
  border: 1px solid var(--line); border-radius: 18px; overflow-x: auto;
}}
figure.diagram svg {{ display: block; width: 100%; height: auto; min-width: 640px; }}
figure.diagram figcaption {{
  text-align: center; color: var(--muted); font-size: 13.6px; padding: 10px 0 6px;
}}
.fig-no {{
  display: inline-block; background: var(--violet); color: #fff; font-size: 11.5px;
  border-radius: 5px; padding: 1px 6px; margin-right: 8px; transform: translateY(-1px);
}}
.lead {{ font-size: 1.06rem; color: var(--ink-2); }}
.meta-cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(215px, 1fr)); gap: 14px; margin: 26px 0 34px; }}
.meta-cards .card {{
  border: 1px solid var(--line); border-radius: var(--radius); padding: 14px 16px; background: #fff;
  box-shadow: 0 1px 2px rgba(15,23,42,.04);
}}
.meta-cards .card b {{ display: block; font-size: 12px; color: var(--muted); font-weight: 600;
  text-transform: uppercase; letter-spacing: .06em; margin-bottom: 4px; }}
.meta-cards .card span {{ font-size: 15px; font-weight: 600; }}
.toc-top {{ border: 1px solid var(--line); border-radius: var(--radius); padding: 6px 22px 14px; background: var(--bg-soft); }}
footer.doc {{ margin-top: 70px; padding-top: 22px; border-top: 1px solid var(--line); color: var(--muted); font-size: 13.6px; }}
@media (max-width: 1180px) {{
  nav.side {{ display: none; }}
  main {{ padding: 28px 22px 80px; }}
}}
@media print {{
  nav.side {{ display: none; }}
  main {{ padding: 0; max-width: none; }}
  figure.diagram {{ break-inside: avoid; }}
  h2 {{ break-after: avoid; }}
}}
{pygments_css}
</style>
</head>
<body>
<div class="layout">
<nav class="side">
  <div class="brand">
    <svg width="30" height="30" viewBox="0 0 32 32" fill="none"><defs><linearGradient id="lg" x1="0" y1="0" x2="32" y2="32"><stop stop-color="#7c3aed"/><stop offset="1" stop-color="#db2777"/></linearGradient></defs><rect x="2" y="2" width="28" height="28" rx="8" fill="url(#lg)"/><path d="M10 10h6.5a6 6 0 0 1 0 12H10V10Z" stroke="#fff" stroke-width="2.1" fill="none"/><path d="M13.6 14h2.6a2 2 0 0 1 0 4h-2.6v-4Z" fill="#fff"/></svg>
    <b>Daft 架构剖析</b>
  </div>
  <p class="sub">{subtitle}</p>
  {toc}
</nav>
<main>
{body}
<footer class="doc">
  文档基于 Daft <code>main</code> 分支（commit <code>dadd8a0</code>，2026-09-25，对应 v0.7.25+）源码与官方文档整理。
  图表为本文件内联 SVG，可离线阅读、可打印。
</footer>
</main>
</div>
</body>
</html>
"""


def main() -> int:
    if not SRC.exists():
        print(f"[x] 找不到源文件: {SRC}", file=sys.stderr)
        return 1

    raw = SRC.read_text(encoding="utf-8")
    raw, missing = inject_diagrams(raw)

    md = markdown.Markdown(
        extensions=[
            "extra",
            "tables",
            "fenced_code",
            "codehilite",
            "toc",
            "attr_list",
            "admonition",
            "sane_lists",
            "md_in_html",
        ],
        extension_configs={
            "codehilite": {"guess_lang": False, "css_class": "codehilite", "linenums": False},
            "toc": {"permalink": False, "toc_depth": "1-3"},
        },
    )
    body = md.convert(raw)
    toc_html = build_toc(md.toc_tokens)
    pygments_css = HtmlFormatter(style="friendly").get_style_defs(".codehilite")

    title = "Daft 架构与实现原理深度剖析"
    out = TEMPLATE.format(
        title=title,
        subtitle="多模态数据引擎 · 源码级架构剖析",
        toc=f'<div class="toc-top">{toc_html}</div>',
        body=body,
        pygments_css=pygments_css,
    )
    OUT.write_text(out, encoding="utf-8")
    if missing:
        print(f"[!] 缺少 SVG 的图: {', '.join(sorted(set(missing)))}")
    print(f"[ok] 已生成 {OUT}  ({len(out) / 1024:.0f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
