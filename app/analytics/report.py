"""
report.py — a printable trading report.

Browser Ctrl+P → Save as PDF. No PDF library on purpose: weasyprint needs
cairo/pango system libraries, which is a poor trade on the ARM box, and
reportlab means hand-laying every table. The browser is already a competent
PDF engine, and an HTML report stays readable on screen without downloading
anything first.

Shares _lifecycle_rows() with the workbook export, so the report and the
spreadsheet can never disagree about what happened.
"""

import html as _html
from collections import defaultdict
from datetime import datetime, timezone

from app.core.db import SessionLocal
from app.core.models import Position
from app.analytics.trade_logger import (
    _NOT_TAKEN_COLS,
    _ROUNDTRIP_COLS,
    _et,
    _lifecycle_rows,
    _round_trips,
)

# Kept out of the f-string below so the braces need no doubling.
_CSS = """
  @page { size: A4 landscape; margin: 12mm 10mm 14mm; }
  * { box-sizing: border-box; }
  body { font:12px/1.45 -apple-system,'Segoe UI',system-ui,sans-serif;
         color:#111; background:#fff; margin:0; padding:18px; }
  h1 { font-size:19px; margin:0 0 2px; }
  h2 { font-size:13px; margin:20px 0 7px; padding-bottom:4px;
       border-bottom:1px solid #ccc; }
  .sub { color:#666; font-size:11px; margin-bottom:14px; }
  .tiles { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:6px; }
  .tile { border:1px solid #ddd; border-radius:5px; padding:8px 13px; min-width:118px; }
  .tl { font-size:10px; color:#666; text-transform:uppercase; letter-spacing:.4px; }
  .tv { font-size:19px; font-weight:700; margin-top:2px; }
  table { width:100%; border-collapse:collapse; margin-top:4px; }
  th { text-align:left; font-size:10px; text-transform:uppercase; letter-spacing:.4px;
       color:#555; border-bottom:1px solid #bbb; padding:5px 6px; white-space:nowrap; }
  td { padding:4px 6px; border-bottom:1px solid #eee; font-size:11px; }
  .num { text-align:right; font-variant-numeric:tabular-nums; white-space:nowrap; }
  .mono { font-family:ui-monospace,Menlo,Consolas,monospace; font-size:10px; }
  .b { font-weight:600; }
  .dim { color:#666; }
  .pos { color:#15803d; }
  .neg { color:#b91c1c; }
  .note { font-size:10px; color:#666; margin-top:6px; }
  .btn { border:1px solid #bbb; background:#fff; border-radius:5px; padding:5px 13px;
         font-size:12px; cursor:pointer; }
  /* Print: repeat headers across pages, never split a row, and start the long
     trade table on a fresh page so it cannot begin two lines before a break.
     Colours are already print-safe — this report is light-only by design, so
     it does not inherit the dashboard's dark theme into a printer. */
  @media print {
    body { padding:0; }
    thead { display:table-header-group; }
    tr, .tile { break-inside:avoid; }
    h2 { break-after:avoid; }
    .page { break-before:page; }
    .noprint { display:none; }
  }
"""


def _e(v):
    return _html.escape("" if v is None else str(v))


def _money(v):
    if v in (None, ""):
        return "—"
    return ("+" if v >= 0 else "−") + "$" + format(abs(v), ",.0f")


def _cls(v):
    if v in (None, ""):
        return ""
    return " pos" if v >= 0 else " neg"


def _td(v, c=""):
    return '<td class="' + c + '">' + _e(v) + "</td>"


def _tdm(v):
    return '<td class="num' + _cls(v) + '">' + _money(v) + "</td>"


def _num(v, fmt="{:.2f}"):
    return _td(fmt.format(v) if isinstance(v, (int, float)) else "—", "num")


def _table(headers, body_rows):
    if not body_rows:
        return ""
    head = "".join("<th>" + _e(h) + "</th>" for h in headers)
    body = "".join("<tr>" + "".join(r) + "</tr>" for r in body_rows)
    return "<table><thead><tr>" + head + "</tr></thead><tbody>" + body + "</tbody></table>"


def render_report_html(days: int = 30) -> str:
    """Build the whole report as one self-contained HTML string."""
    db = SessionLocal()
    try:
        rows, not_taken = _lifecycle_rows(db, days)
        open_pos = (db.query(Position)
                    .filter(Position.status.in_(["OPEN", "PARTIAL"]))
                    .order_by(Position.open_time.desc()).all())
        open_rows = [(
            p.symbol, p.osi_symbol, p.author or "", p.remaining, p.avg_price,
            p.current_price,
            ((p.current_price / p.avg_price - 1) * 100)
            if (p.avg_price and p.current_price) else None,
            _et(p.open_time),
        ) for p in open_pos]
    finally:
        db.close()

    # _lifecycle_rows is one row per ORDER, so a round trip appears twice and
    # the position's P&L repeats on both. _round_trips matches the BUY fills
    # against the SELL fills first-in-first-out, so each row below is one
    # closed lot: the entry alert (who called it, at what price, how fast the
    # bot reacted, what it paid vs. the call) joined to the exit that actually
    # sold those contracts, with money computed off that pair alone.
    C = {n: i for i, n in enumerate(_ROUNDTRIP_COLS)}
    trips = _round_trips(rows)
    closed = [t for t in trips if t[C["Match"]] == "FIFO"]
    open_lots = sum(1 for t in trips if t[C["Match"]] == "OPEN")
    orphan_sells = sum(1 for t in trips if t[C["Match"]] == "SELL W/O BUY")

    def pnl_of(r):
        v = r[C["P&L $"]]
        return v if isinstance(v, (int, float)) else None

    booked = [p for p in (pnl_of(r) for r in closed) if p is not None]
    net = sum(booked)
    wins = sum(1 for p in booked if p > 0)
    wr = round(100.0 * wins / len(booked), 1) if booked else 0.0

    by_a = defaultdict(lambda: [0, 0, 0.0])   # trades, wins, pnl
    for r in closed:
        p = pnl_of(r)
        if p is None:
            continue
        slot = by_a[r[C["Analyst"]] or "—"]
        slot[0] += 1
        slot[1] += 1 if p > 0 else 0
        slot[2] += p

    reasons = defaultdict(int)
    why_i = _NOT_TAKEN_COLS.index("Why Not Taken")
    st_i = _NOT_TAKEN_COLS.index("Status")
    for nt in not_taken:
        why = (nt[why_i] or nt[st_i] or "—")
        reasons[str(why).split(":")[0].strip()[:70] or "—"] += 1

    analyst_rows = [
        [_td(a, "b"), _td(v[0], "num"), _td(str(v[1]) + "/" + str(v[0] - v[1]), "num"),
         _td((str(round(100 * v[1] / v[0])) + "%") if v[0] else "—", "num"), _tdm(v[2])]
        for a, v in sorted(by_a.items(), key=lambda kv: -kv[1][2])
    ]

    trade_rows = []
    for r in closed:
        pct = r[C["P&L %"]]
        pct_cell = ('<td class="num' + _cls(pct if isinstance(pct, (int, float)) else None)
                    + '">' + (str(pct) + "%" if pct not in ("", None) else "—") + "</td>")
        slip = r[C["Slip vs Alert %"]]
        slip_cell = ('<td class="num' + _cls(-slip if isinstance(slip, (int, float)) else None)
                     + '">' + (format(slip, "+.1f") + "%" if isinstance(slip, (int, float)) else "—")
                     + "</td>")
        react = r[C["Reaction (s)"]]
        trade_rows.append([
            _td(r[C["Alert Time (ET)"]] or r[C["Buy Time (ET)"]] or "—", "dim"),
            _td(r[C["Analyst"]] or "—"),
            _td(r[C["Symbol"]], "b"), _td(r[C["OSI"]], "mono dim"),
            _td(r[C["Lane"]] or "—", "dim"),
            _num(r[C["Alert Price"]]),
            _td(format(react, ".1f") + "s" if isinstance(react, (int, float)) else "—", "num dim"),
            _td(r[C["Qty"]], "num"),
            _num(r[C["Buy Fill"]]), slip_cell,
            _td(r[C["Exit Via"]] or "—", "dim"),
            _num(r[C["Sell Fill"]]),
            _td(r[C["Sell Time (ET)"]], "dim"),
            pct_cell, _tdm(pnl_of(r)),
        ])

    open_tbl = []
    for (sym, osi, au, rem, ap, cp, pc, ot) in open_rows:
        open_tbl.append([
            _td(sym, "b"), _td(osi, "mono dim"), _td(au), _td(rem, "num"),
            _num(ap), _num(cp),
            '<td class="num' + _cls(pc) + '">'
            + (format(pc, "+.1f") + "%" if pc is not None else "—") + "</td>",
            _td(ot, "dim"),
        ])

    reason_rows = [[_td(k), _td(v, "num")]
                   for k, v in sorted(reasons.items(), key=lambda kv: -kv[1])[:12]]

    # The full list, not just the census — which alerts were passed on is the
    # thing the operator reviews (with manual_halt live, most alerts land
    # here). Newest first; _lifecycle_rows already sorted it that way.
    N = _NOT_TAKEN_COLS.index
    nt_rows = [[
        _td(nt[N("Alert Time (ET)")], "dim"), _td(nt[N("Analyst")] or "—"),
        _td(nt[N("Lane")] or "—", "dim"), _td(nt[N("Action")] or "—"),
        _td(nt[N("OSI")] or (nt[N("Symbol")] or "—"), "mono dim"),
        _num(nt[N("Alert Price")]),
        _td(nt[N("Status")] or "—", "dim"),
        _td(str(nt[N("Why Not Taken")] or "—")[:110]),
    ] for nt in not_taken]

    tiles = [
        ("Net realised", _money(net), _cls(net)),
        ("Round trips", len(closed), ""),
        ("Win rate", str(wr) + "%", ""),
        ("Open now", len(open_rows), ""),
        ("Alerts not taken", len(not_taken), ""),
    ]
    tile_html = "".join(
        '<div class="tile"><div class="tl">' + _e(l) + '</div><div class="tv'
        + c + '">' + _e(v) + "</div></div>" for l, v, c in tiles)

    gen = _et(datetime.now(timezone.utc))
    open_block = (_table(["Symbol", "Contract", "Analyst", "Qty", "Entry", "Now",
                          "Unreal.", "Opened (ET)"], open_tbl)
                  or '<div class="note">None open.</div>')
    reason_block = (_table(["Reason", "Count"], reason_rows)
                    or '<div class="note">None.</div>')
    trade_block = (_table(["Alert (ET)", "Analyst", "Symbol", "Contract", "Lane",
                           "Alert $", "React", "Qty", "Buy $", "Slip", "Exit via",
                           "Sell $", "Sold (ET)", "Return", "P&L"], trade_rows)
                   or '<div class="note">No closed trades in this period.</div>')

    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        "<title>TradeRelay report — " + _e(gen) + "</title><style>" + _CSS
        + "</style></head><body>"
        '<div class="noprint" style="float:right">'
        '<button class="btn" onclick="window.print()">Print / Save as PDF</button></div>'
        "<h1>TradeRelay — trading report</h1>"
        '<div class="sub">Last ' + _e(days) + " days · generated " + _e(gen)
        + " ET</div>"
        '<div class="tiles">' + tile_html + "</div>"
        "<h2>P&amp;L by analyst</h2>"
        + (_table(["Analyst", "Trades", "W/L", "Win rate", "Net P&L"], analyst_rows)
           or '<div class="note">No closed trades in this period.</div>')
        + "<h2>Open positions (" + str(len(open_rows)) + ")</h2>" + open_block
        + "<h2>Alerts not taken (" + str(len(not_taken)) + ")</h2>" + reason_block
        + '<div class="page"></div>'
        + "<h2>Round trips — buy &rarr; sell, FIFO matched (" + str(len(closed))
        + ")</h2>" + trade_block
        + '<div class="note">One row per <b>closed lot</b>: each BUY fill is matched '
          "against the SELL fills that closed it, first-in-first-out, per contract and "
          "analyst. A position scaled out in three exits is three rows whose Qty sums "
          "back to the entry, each with its own P&amp;L. Columns read end to end: when "
          "the analyst called it and at what price, how long the bot took to act "
          "(React), what it paid (Buy $, Slip vs the alert), and what sold it (Exit "
          "via: DISCORD = analyst exit, MANUAL = dashboard button, SL/PT = automated "
          "rule). " + str(open_lots) + " buy lot(s) are still open or were sold outside "
          "this window, and " + str(orphan_sells) + " sell(s) had no matching buy in it; "
          "neither is counted above. Blank alert columns are orders placed before the "
          "alert link existed (2026-08-19) with no timestamp match. P&amp;L is computed "
          "from the matched fills — not positions.realized_pnl, which is NULL or 0 on "
          "most rows.</div>"
        + '<div class="page"></div>'
        + "<h2>Alerts not taken — full list (" + str(len(not_taken)) + ")</h2>"
        + (_table(["Time (ET)", "Analyst", "Lane", "Action", "Contract",
                   "Alert $", "Status", "Reason"], nt_rows)
           or '<div class="note">None.</div>')
        + "</body></html>"
    )
