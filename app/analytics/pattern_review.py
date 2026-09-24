"""
pattern_review.py — Daily bot-vs-analyst leak analysis.

Runs at the same daily-scheduler tick as the P&L summary (16:00 ET).
For each OSI traded today, it compares what the analyst ALERTED vs what
the bot EXECUTED, surfacing:
  - SKIPPED alerts (bot ignored a signal that worked)
  - ERROR alerts (broker / parser failures that lost trades)
  - SL whipsaws (bot got stopped on a winner)
  - PT-too-early caps (bot banked at PT1 while analyst ran to +200%)

Posts a Discord embed to daily_summary_channel_id (with trade_log fallback).
Optional Claude narrative if ai_parser.api_key is set.
"""

import logging
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app.core.db import SessionLocal
from app.core.models import Alert, Order
from app.core.config_manager import cfg

log = logging.getLogger(__name__)


def _today_window_utc() -> datetime:
    """Start of today in ET, expressed in UTC."""
    et = ZoneInfo("America/New_York")
    today_et = datetime.now(et).replace(hour=0, minute=0, second=0, microsecond=0)
    return today_et.astimezone(timezone.utc)


def _compute_review() -> dict:
    """Pull alerts + orders + positions and build the per-OSI scoreboard."""
    today_utc = _today_window_utc()

    db = SessionLocal()
    try:
        alerts = db.query(Alert).filter(Alert.timestamp >= today_utc).all()
        orders = db.query(Order).filter(
            Order.placed_at >= today_utc, Order.status == "FILLED"
        ).all()
    finally:
        db.close()

    # Group alerts by OSI
    by_osi: dict[str, dict] = defaultdict(lambda: {
        "buy_alerts": [], "sell_alerts": [], "author": "", "channel": "",
    })
    for a in alerts:
        if not a.osi_symbol:
            continue
        slot = by_osi[a.osi_symbol]
        slot["author"] = a.author or slot["author"]
        slot["channel"] = a.channel_name or slot["channel"]
        if a.action == "BUY":
            slot["buy_alerts"].append(a)
        elif a.action == "SELL":
            slot["sell_alerts"].append(a)

    # Group fills by OSI/side
    fills: dict[str, dict] = defaultdict(lambda: {
        "buy_qty": 0, "buy_cost": 0.0, "sell_qty": 0, "sell_proceeds": 0.0,
    })
    for o in orders:
        f = fills[o.osi_symbol]
        if o.side == "BUY":
            f["buy_qty"] += o.filled_qty or 0
            f["buy_cost"] += (o.filled_qty or 0) * (o.fill_price or 0) * 100
        else:
            f["sell_qty"] += o.filled_qty or 0
            f["sell_proceeds"] += (o.filled_qty or 0) * (o.fill_price or 0) * 100

    # Build per-OSI rows
    rows = []
    bot_total = 0.0
    analyst_avg_total = 0.0
    skipped_count = 0
    error_count = 0

    for osi, slot in by_osi.items():
        if not slot["buy_alerts"]:
            # SELL-only alerts (no BUY to compare): ignore in scoreboard
            continue
        first_buy = slot["buy_alerts"][0]
        buy_alert_px = float(first_buy.alert_price or 0)
        sells = [(float(s.alert_price or 0), s.fraction) for s in slot["sell_alerts"]
                 if s.alert_price]
        analyst_avg_sell = (sum(p for p, _ in sells) / len(sells)) if sells else 0.0
        analyst_pct = (
            (analyst_avg_sell - buy_alert_px) / buy_alert_px * 100
            if buy_alert_px and analyst_avg_sell else 0.0
        )

        f = fills.get(osi)
        if f and f["buy_qty"] > 0:
            cost_per = f["buy_cost"] / f["buy_qty"]
            bot_realized = (
                f["sell_proceeds"] - cost_per * f["sell_qty"]
                if f["sell_qty"] > 0 else 0.0
            )
            bot_status = "TRADED"
        else:
            bot_realized = 0.0
            if first_buy.status == "ERROR":
                bot_status = "ERROR"
                error_count += 1
            elif first_buy.status == "SKIPPED":
                bot_status = "SKIPPED"
                skipped_count += 1
            else:
                bot_status = first_buy.status or "—"

        bot_total += bot_realized
        if analyst_pct:
            analyst_avg_total += analyst_pct

        rows.append({
            "osi": osi,
            "author": slot["author"][:24],
            "buy_alert_px": buy_alert_px,
            "analyst_avg_sell": analyst_avg_sell,
            "analyst_pct": analyst_pct,
            "bot_realized": bot_realized,
            "bot_status": bot_status,
        })

    # Sort by impact (analyst pct desc when bot didn't capture)
    rows.sort(key=lambda r: (
        0 if r["bot_status"] in ("ERROR", "SKIPPED") else 1,
        -r["analyst_pct"],
    ))

    return {
        "rows": rows,
        "bot_total": bot_total,
        "skipped": skipped_count,
        "errored": error_count,
        "alerts_total": len(alerts),
        "fills_total": len(orders),
    }


def _format_embed(review: dict) -> dict:
    """Build a Discord embed from the computed review."""
    rows = review["rows"]
    if not rows:
        desc = "No BUY alerts processed today."
    else:
        # Discord embed description max ~4096 chars; field max 1024.
        # Use a code block for the table.
        lines = [
            "```",
            f"{'OSI':<22} {'Author':<14} {'Alert%':>8} {'Bot$':>8}  Status",
            "─" * 70,
        ]
        for r in rows[:20]:  # cap to 20 rows
            osi = r["osi"][:22]
            author = r["author"][:14]
            apct = f"{r['analyst_pct']:+.0f}%" if r["analyst_pct"] else "  —"
            bot = f"{r['bot_realized']:+.0f}" if r["bot_realized"] else "  0"
            lines.append(f"{osi:<22} {author:<14} {apct:>8} {bot:>8}  {r['bot_status']}")
        lines.append("```")
        desc = "\n".join(lines)

    n = len(rows)
    embed = {
        "title": f"\U0001f4ca DAILY PATTERN REVIEW — {datetime.now(ZoneInfo('America/New_York')).strftime('%Y-%m-%d')}",
        "description": desc,
        "color": 0x3498DB,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fields": [
            {"name": "Alerts processed", "value": str(review["alerts_total"]), "inline": True},
            {"name": "Tickers traded", "value": str(n), "inline": True},
            {"name": "Bot realized $", "value": f"${review['bot_total']:+.0f}", "inline": True},
            {"name": "Skipped alerts", "value": str(review["skipped"]), "inline": True},
            {"name": "Error alerts", "value": str(review["errored"]), "inline": True},
            {"name": "Fills", "value": str(review["fills_total"]), "inline": True},
        ],
        "footer": {"text": "Pattern review — bot $ vs analyst %"},
    }
    return embed


async def _ai_narrative(review: dict) -> str:
    """Optional: ask Claude for a one-paragraph leak commentary. Returns ""
    if API key not set or call fails."""
    api_key = cfg.get("ai_parser", "api_key", fallback="").strip()
    if not api_key:
        return ""
    if not review["rows"]:
        return ""
    import aiohttp

    # Build a compact prompt
    lines = []
    for r in review["rows"][:15]:
        lines.append(
            f"{r['osi']} ({r['author']}): analyst {r['analyst_pct']:+.0f}%, "
            f"bot ${r['bot_realized']:+.0f} ({r['bot_status']})"
        )
    body = "\n".join(lines)
    prompt = (
        "You are reviewing a discretionary options-trading bot's results vs the "
        "analysts whose alerts it follows. Each row shows: OSI symbol, analyst "
        "name, the % move the analyst captured (BUY-alert price → average SELL-"
        "alert price), and the bot's realized $ on the same ticker.\n\n"
        f"{body}\n\n"
        "In <=120 words, identify the single biggest leak today and one concrete "
        "tunable that would have closed it (PT/SL/size/skip-reason). Be terse "
        "and concrete; no preamble."
    )

    model = cfg.get("ai_parser", "model", fallback="claude-haiku-4-5-20251001")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": model,
                    "max_tokens": 250,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=aiohttp.ClientTimeout(total=12),
            ) as resp:
                if resp.status != 200:
                    log.warning("[pattern_review] AI call %s: %s", resp.status, (await resp.text())[:200])
                    return ""
                data = await resp.json()
                return data.get("content", [{}])[0].get("text", "").strip()
    except Exception as exc:
        log.warning("[pattern_review] AI call failed: %s", exc)
        return ""


async def run_and_post():
    """Compute today's pattern review and post it to Discord."""
    if not cfg.getboolean("trading", "pattern_review_enabled", fallback=True):
        log.info("[pattern_review] disabled via config")
        return
    try:
        review = _compute_review()
    except Exception as exc:
        log.exception("[pattern_review] compute failed: %s", exc)
        return

    embed = _format_embed(review)

    # AI narrative as a second embed (optional)
    narrative = ""
    try:
        narrative = await _ai_narrative(review)
    except Exception as exc:
        log.warning("[pattern_review] narrative failed: %s", exc)

    embeds = [embed]
    if narrative:
        embeds.append({
            "title": "Leak commentary (AI)",
            "description": narrative,
            "color": 0x95A5A6,
        })

    # Post via trade_logger using the daily summary channel
    import app.analytics.trade_logger as tl
    for e in embeds:
        await tl._send_to_channel(embed=e, channel_id=tl._daily_summary_channel_id())
    log.info("[pattern_review] posted (%d embeds)", len(embeds))


# ── CLI: manual trigger ──────────────────────────────────────────────────
# Usage:
#   docker compose exec trader python -m pattern_review
# Posts the same Discord embed as the scheduled run. Useful for ad-hoc
# review or for testing changes without waiting for 16:00 ET.
if __name__ == "__main__":
    import asyncio as _asyncio
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    _asyncio.run(run_and_post())
