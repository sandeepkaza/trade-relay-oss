"""
reports.py — Extended Discord analytics reports, posted by the daily scheduler
right after the P&L summary / pattern review / infra report (16:05 ET weekdays).

Four reports, all best-effort (a failure in one never blocks the others or the
scheduler), all posted to daily_summary_channel_id via trade_logger:

  post_daily_breakdown()    — today's per-analyst / per-symbol / best-hour leaders
  post_execution_quality()  — today's fill latency, entry slippage, blocked/skipped
  post_analyst_scorecard()  — rolling 30d per-analyst leaderboard
  post_weekly_digest()      — Friday only: the week's P&L, leaderboard, extremes

Gating:
  [trading] extended_reports_enabled  (default true) → breakdown+exec+scorecard
  [trading] weekly_digest_enabled     (default true) → Friday digest
"""

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app.core.db import SessionLocal
from app.core.models import Alert, Order, Position
from app.core.config_manager import cfg

log = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")


# ── helpers ────────────────────────────────────────────────────────────────

def _today_start_utc() -> datetime:
    now_et = datetime.now(_ET).replace(hour=0, minute=0, second=0, microsecond=0)
    return now_et.astimezone(timezone.utc)


def _days_ago_utc(days: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days)


def _pos_pnl(pos) -> float:
    """Realized $ for a closed position (same idiom analytics.py uses)."""
    try:
        return float(pos.realized_pnl if pos.realized_pnl is not None else pos.pnl_dollar())
    except Exception:
        try:
            if pos.avg_price and pos.close_price is not None:
                from app.core.models import fill_pnl_dollar
                return fill_pnl_dollar(
                    pos.close_price, pos.avg_price, pos.total_contracts or 0,
                    bool(getattr(pos, "is_credit", False)),
                )
        except Exception:
            pass
        return 0.0


def _closed_since(db, start_utc: datetime):
    return db.query(Position).filter(
        Position.status == "CLOSED", Position.close_time != None,  # noqa: E711
        Position.close_time >= start_utc,
    ).all()


async def _post(embed: dict) -> None:
    import app.analytics.trade_logger as tl
    await tl._send_to_channel(embed=embed, channel_id=tl._daily_summary_channel_id())


def _fmt_money(v: float) -> str:
    return f"${v:+,.0f}"


def _leaderboard(by_key: dict, top: int = 5) -> tuple[list, list]:
    """Return (top winners, top losers) as [(key, stats)] sorted by total_pnl."""
    items = sorted(by_key.items(), key=lambda kv: kv[1]["pnl"], reverse=True)
    winners = [x for x in items if x[1]["pnl"] > 0][:top]
    losers = [x for x in items if x[1]["pnl"] < 0][-top:][::-1]
    return winners, losers


# ── 1. Daily breakdown (today's leaders) ─────────────────────────────────────

def _compute_daily_breakdown() -> dict:
    start = _today_start_utc()
    db = SessionLocal()
    try:
        closed = _closed_since(db, start)
        by_analyst: dict[str, dict] = defaultdict(lambda: {"pnl": 0.0, "n": 0})
        by_symbol: dict[str, dict] = defaultdict(lambda: {"pnl": 0.0, "n": 0})
        by_hour: dict[int, float] = defaultdict(float)
        for p in closed:
            pnl = _pos_pnl(p)
            a = (getattr(p, "author", None) or "unknown")[:20]
            s = (p.symbol or "?")[:12]
            by_analyst[a]["pnl"] += pnl; by_analyst[a]["n"] += 1
            by_symbol[s]["pnl"] += pnl; by_symbol[s]["n"] += 1
            ct = p.close_time
            if ct is not None:
                if ct.tzinfo is None:
                    ct = ct.replace(tzinfo=timezone.utc)
                by_hour[ct.astimezone(_ET).hour] += pnl
    finally:
        db.close()
    return {"analyst": dict(by_analyst), "symbol": dict(by_symbol),
            "hour": dict(by_hour), "n": len(closed)}


def _breakdown_embed(d: dict) -> dict:
    def _lines(by_key):
        w, l = _leaderboard(by_key, top=3)
        out = []
        for k, st in w:
            out.append(f"🟢 {k:<18} {_fmt_money(st['pnl'])} ({st['n']})")
        for k, st in l:
            out.append(f"🔴 {k:<18} {_fmt_money(st['pnl'])} ({st['n']})")
        return "\n".join(out) or "—"

    hour = d["hour"]
    best_h = max(hour.items(), key=lambda kv: kv[1], default=(None, 0.0))
    worst_h = min(hour.items(), key=lambda kv: kv[1], default=(None, 0.0))
    hour_txt = "—"
    if best_h[0] is not None:
        hour_txt = (f"best {best_h[0]:02d}:00 ET {_fmt_money(best_h[1])}  •  "
                    f"worst {worst_h[0]:02d}:00 ET {_fmt_money(worst_h[1])}")

    return {
        "title": f"📈 DAILY BREAKDOWN — {datetime.now(_ET).strftime('%Y-%m-%d')}",
        "description": f"{d['n']} positions closed today.",
        "color": 0x2ECC71,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fields": [
            {"name": "By analyst", "value": _lines(d["analyst"]), "inline": False},
            {"name": "By symbol", "value": _lines(d["symbol"]), "inline": False},
            {"name": "By hour", "value": hour_txt, "inline": False},
        ],
        "footer": {"text": "Daily breakdown — realized P&L by analyst / symbol / hour"},
    }


async def post_daily_breakdown() -> None:
    if not cfg.getboolean("trading", "extended_reports_enabled", fallback=True):
        return
    try:
        d = _compute_daily_breakdown()
        if d["n"] == 0:
            return  # nothing closed today — skip the noise
        await _post(_breakdown_embed(d))
        log.info("[reports] daily breakdown posted")
    except Exception as exc:
        log.exception("[reports] daily breakdown failed: %s", exc)


# ── 2. Execution quality ─────────────────────────────────────────────────────

def _compute_execution_quality() -> dict:
    start = _today_start_utc()
    db = SessionLocal()
    try:
        fills = db.query(Order).filter(
            Order.status == "FILLED", Order.filled_at != None,  # noqa: E711
            Order.filled_at >= start, Order.placed_at != None,  # noqa: E711
        ).all()
        # First BUY alert per OSI today → slippage reference.
        buy_alerts = db.query(Alert).filter(
            Alert.timestamp >= start, Alert.action == "BUY",
        ).all()
        skipped = db.query(Alert).filter(
            Alert.timestamp >= start, Alert.status.in_(["SKIPPED", "ERROR", "REJECTED"]),
        ).all()
    finally:
        db.close()

    lats = []
    for o in fills:
        try:
            ms = (o.filled_at - o.placed_at).total_seconds() * 1000.0
            if ms >= 0:
                lats.append(ms)
        except Exception:
            pass
    lats.sort()
    n = len(lats)
    avg = sum(lats) / n if n else 0.0
    p95 = lats[min(n - 1, int(round(0.95 * (n - 1))))] if n else 0.0

    ref: dict[str, float] = {}
    for a in buy_alerts:
        if a.osi_symbol and a.osi_symbol not in ref and a.alert_price:
            ref[a.osi_symbol] = float(a.alert_price)
    slips = []
    for o in fills:
        if o.side == "BUY" and o.fill_price and ref.get(o.osi_symbol):
            base = ref[o.osi_symbol]
            if base > 0:
                slips.append((float(o.fill_price) - base) / base * 100.0)
    avg_slip = sum(slips) / len(slips) if slips else 0.0

    reasons: dict[str, int] = defaultdict(int)
    for a in skipped:
        head = (a.error_text or a.status or "?").split(":", 1)[0].strip()[:24]
        reasons[head] += 1
    top_reasons = sorted(reasons.items(), key=lambda kv: kv[1], reverse=True)[:5]

    return {"fills": n, "avg_ms": avg, "p95_ms": p95, "avg_slip": avg_slip,
            "n_slip": len(slips), "skipped": len(skipped), "reasons": top_reasons}


def _exec_embed(d: dict) -> dict:
    reasons_txt = "\n".join(f"• {r}: {c}" for r, c in d["reasons"]) or "—"
    return {
        "title": f"⚙️ EXECUTION QUALITY — {datetime.now(_ET).strftime('%Y-%m-%d')}",
        "color": 0xE67E22,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fields": [
            {"name": "Fills", "value": str(d["fills"]), "inline": True},
            {"name": "Fill latency avg", "value": f"{d['avg_ms']/1000:.1f}s", "inline": True},
            {"name": "Fill latency p95", "value": f"{d['p95_ms']/1000:.1f}s", "inline": True},
            {"name": "Entry slippage avg", "value": f"{d['avg_slip']:+.1f}% (n={d['n_slip']})", "inline": True},
            {"name": "Blocked / skipped", "value": str(d["skipped"]), "inline": True},
            {"name": "Top skip reasons", "value": reasons_txt, "inline": False},
        ],
        "footer": {"text": "Execution quality — latency, entry slippage, guardrail skips"},
    }


async def post_execution_quality() -> None:
    if not cfg.getboolean("trading", "extended_reports_enabled", fallback=True):
        return
    try:
        d = _compute_execution_quality()
        if d["fills"] == 0 and d["skipped"] == 0:
            return
        await _post(_exec_embed(d))
        log.info("[reports] execution quality posted")
    except Exception as exc:
        log.exception("[reports] execution quality failed: %s", exc)


# ── 3. Analyst scorecard (rolling window) ────────────────────────────────────

def _compute_scorecard(days: int) -> dict:
    db = SessionLocal()
    try:
        closed = _closed_since(db, _days_ago_utc(days))
        by: dict[str, dict] = defaultdict(lambda: {"pnl": 0.0, "n": 0, "w": 0, "best": None})
        for p in closed:
            a = (getattr(p, "author", None) or "unknown")[:20]
            pnl = _pos_pnl(p)
            st = by[a]
            st["pnl"] += pnl; st["n"] += 1
            if pnl > 0:
                st["w"] += 1
            if st["best"] is None or pnl > st["best"][1]:
                st["best"] = ((p.symbol or "?")[:8], pnl)
    finally:
        db.close()
    return {"by": dict(by), "days": days, "n": sum(v["n"] for v in by.values())}


def _scorecard_embed(d: dict) -> dict:
    items = sorted(d["by"].items(), key=lambda kv: kv[1]["pnl"], reverse=True)
    lines = ["```",
             f"{'Analyst':<18}{'Net$':>9}{'Trades':>7}{'Win%':>6}",
             "─" * 40]
    for a, st in items[:15]:
        wr = (st["w"] / st["n"] * 100) if st["n"] else 0
        lines.append(f"{a:<18}{st['pnl']:>+9.0f}{st['n']:>7}{wr:>5.0f}%")
    lines.append("```")
    return {
        "title": f"🏅 ANALYST SCORECARD — last {d['days']}d",
        "description": "\n".join(lines) if d["n"] else "No closed trades in window.",
        "color": 0x9B59B6,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": f"Per-analyst realized P&L • {d['n']} trades / {d['days']}d"},
    }


async def post_analyst_scorecard(days: int = 30) -> None:
    if not cfg.getboolean("trading", "extended_reports_enabled", fallback=True):
        return
    try:
        d = _compute_scorecard(days)
        if d["n"] == 0:
            return
        await _post(_scorecard_embed(d))
        log.info("[reports] analyst scorecard posted")
    except Exception as exc:
        log.exception("[reports] analyst scorecard failed: %s", exc)


# ── 4. Weekly digest (Friday) ────────────────────────────────────────────────

def _compute_weekly() -> dict:
    db = SessionLocal()
    try:
        closed = _closed_since(db, _days_ago_utc(7))
        by_analyst: dict[str, dict] = defaultdict(lambda: {"pnl": 0.0, "n": 0})
        by_symbol: dict[str, dict] = defaultdict(lambda: {"pnl": 0.0, "n": 0})
        total = 0.0; wins = 0; losses = 0
        best = None; worst = None
        for p in closed:
            pnl = _pos_pnl(p)
            total += pnl
            if pnl >= 0:
                wins += 1
            else:
                losses += 1
            a = (getattr(p, "author", None) or "unknown")[:20]
            s = (p.symbol or "?")[:12]
            by_analyst[a]["pnl"] += pnl; by_analyst[a]["n"] += 1
            by_symbol[s]["pnl"] += pnl; by_symbol[s]["n"] += 1
            if best is None or pnl > best[1]:
                best = (f"{s}", pnl)
            if worst is None or pnl < worst[1]:
                worst = (f"{s}", pnl)
    finally:
        db.close()
    n = wins + losses
    return {"total": total, "wins": wins, "losses": losses,
            "win_rate": (wins / n * 100) if n else 0.0, "n": n,
            "analyst": dict(by_analyst), "symbol": dict(by_symbol),
            "best": best, "worst": worst}


def _weekly_embed(d: dict) -> dict:
    def _top(by_key):
        w, l = _leaderboard(by_key, top=3)
        rows = [f"🟢 {k}: {_fmt_money(st['pnl'])} ({st['n']})" for k, st in w]
        rows += [f"🔴 {k}: {_fmt_money(st['pnl'])} ({st['n']})" for k, st in l]
        return "\n".join(rows) or "—"

    pnl_emoji = "💰" if d["total"] >= 0 else "📉"
    fields = [
        {"name": "Week P&L", "value": f"{pnl_emoji} {_fmt_money(d['total'])}", "inline": True},
        {"name": "Trades", "value": str(d["n"]), "inline": True},
        {"name": "Win rate", "value": f"{d['win_rate']:.0f}% ({d['wins']}W/{d['losses']}L)", "inline": True},
        {"name": "Analyst leaderboard", "value": _top(d["analyst"]), "inline": False},
        {"name": "Symbol leaderboard", "value": _top(d["symbol"]), "inline": False},
    ]
    if d["best"]:
        fields.append({"name": "Biggest win", "value": f"{d['best'][0]} {_fmt_money(d['best'][1])}", "inline": True})
    if d["worst"]:
        fields.append({"name": "Biggest loss", "value": f"{d['worst'][0]} {_fmt_money(d['worst'][1])}", "inline": True})
    return {
        "title": f"🗓️ WEEKLY DIGEST — week ending {datetime.now(_ET).strftime('%Y-%m-%d')}",
        "color": 0x1ABC9C,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fields": fields,
        "footer": {"text": "Weekly digest — trailing 7 days, realized"},
    }


async def post_weekly_digest() -> None:
    if not cfg.getboolean("trading", "weekly_digest_enabled", fallback=True):
        return
    try:
        d = _compute_weekly()
        if d["n"] == 0:
            return
        await _post(_weekly_embed(d))
        log.info("[reports] weekly digest posted")
    except Exception as exc:
        log.exception("[reports] weekly digest failed: %s", exc)


# ── CLI: manual trigger (posts all applicable reports) ───────────────────────
if __name__ == "__main__":
    import asyncio as _asyncio
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    async def _all():
        await post_daily_breakdown()
        await post_execution_quality()
        await post_analyst_scorecard()
        await post_weekly_digest()

    _asyncio.run(_all())
