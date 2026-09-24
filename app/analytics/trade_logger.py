"""
trade_logger.py - Posts trade events to #trade-logs Discord channel
and provides CSV export functionality.

Logs:
  - New alert received (BTO/STC)
  - Order placed (with price/qty)
  - Order filled (with fill price)
  - Order cancelled/rejected
  - Auto-exit triggered (PT1/PT2/PT3/SL)
  - Daily P&L summary
"""

import asyncio
import configparser
import csv
import io
import logging
from datetime import datetime, timezone, timedelta

import aiohttp

from app.core.db import SessionLocal
from app.core.models import Alert, Order, Position
from app.core.config_manager import cfg  # hot-reloadable singleton

log = logging.getLogger(__name__)

# Static fallback config for values not in cfg singleton yet
_static_cfg = configparser.ConfigParser(inline_comment_prefixes=(";", "#"))
_static_cfg.read("config.ini", encoding="utf-8-sig")

# NOTE: channel_id and token read LIVE per-send from cfg so dashboard changes take effect
def _trade_log_channel_id() -> str:
    return cfg.get("discord", "trade_log_channel_id", fallback="").strip()


def _daily_summary_channel_id() -> str:
    """Channel id for the post-market daily summary. Falls back to the
    standard trade-log channel if not set, so existing deployments keep
    working unchanged."""
    cid = cfg.get("discord", "daily_summary_channel_id", fallback="").strip()
    return cid or _trade_log_channel_id()

def _bot_token() -> str:
    # Token is sensitive — read from env (.env) first, fall back to static
    # config.ini. Mirrors the pattern in forwarder.py / discord_listener.py.
    # 2026-05-06 incident: config.ini.discord.token was empty on the VM
    # (secrets live in .env), so trade_logger silently dropped every post,
    # including the daily summary. Operator saw "Daily summary posted" log
    # line but Discord received nothing.
    import os
    return (
        os.getenv("DISCORD_TOKEN")
        or _static_cfg.get("discord", "token", fallback="")
    ).strip()

def _logger_enabled() -> bool:
    try:
        return cfg.getboolean("trading", "trade_logger_enabled", fallback=True)
    except Exception:
        return _static_cfg.getboolean("trading", "trade_logger_enabled", fallback=True)

API = "https://discord.com/api/v10"

# Throttle "missing creds" warning to once per process (set in _send_to_channel).
_missing_creds_warned: bool = False


# ── Persistent HTTP session ──────────────────────────────────────────────────
# Reuse one aiohttp.ClientSession for all trade-log Discord posts.
# This eliminates per-message TCP handshakes (~50-100ms each).
# The session is created lazily on first use and auto-replaced if it closes.
_http_session: aiohttp.ClientSession | None = None


def _get_http_session() -> aiohttp.ClientSession:
    """Return the shared HTTP session, creating it if needed."""
    global _http_session
    if _http_session is None or _http_session.closed:
        connector = aiohttp.TCPConnector(
            limit=4,               # max 4 concurrent Discord API connections
            ttl_dns_cache=300,     # cache Discord DNS for 5 min
            enable_cleanup_closed=True,
        )
        _http_session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=8),
        )
    return _http_session


async def close_http_session():
    """Cleanly close the shared HTTP session on shutdown."""
    global _http_session
    if _http_session and not _http_session.closed:
        await _http_session.close()
        _http_session = None


# ── Discord message sender ───────────────────────────────────────────────────

async def _send_to_channel(content: str = "", embed: dict = None, channel_id: str | None = None):
    """Send a message to a Discord channel using the bot token. Defaults to
    the #trade-logs channel; pass an explicit channel_id to route a single
    message somewhere else (e.g. the post-market daily summary channel)."""
    import time
    from app.monitors.api_monitor import monitor

    if not _logger_enabled():
        return
    if channel_id is None:
        channel_id = _trade_log_channel_id()
    token = _bot_token()
    if not channel_id or not token:
        # Used to silently drop. Now log loudly so a missing token doesn't
        # disappear notifications without a trace. Throttled to once per
        # process to avoid spamming logs on every event.
        global _missing_creds_warned
        if not _missing_creds_warned:
            # Logs only presence flags ("set"/"MISSING", "yes"/"MISSING") — never
            # the token value itself. False positive for credential disclosure.
            log.error(  # nosemgrep: python-logger-credential-disclosure
                "[trade_logger] Skipping Discord post — channel_id=%s token_set=%s. "
                "Set DISCORD_TOKEN in .env or [discord] token in config.ini.",
                "set" if channel_id else "MISSING",
                "yes" if token else "MISSING",
            )
            _missing_creds_warned = True
        return

    headers = {
        "Authorization": f"Bot {token}",
        "Content-Type": "application/json",
    }
    payload = {}
    if content:
        payload["content"] = content[:2000]
    if embed:
        payload["embeds"] = [embed]

    if not payload:
        return

    start = time.perf_counter()
    status_code = 200
    error_msg = None

    try:
        session = _get_http_session()
        async with session.post(
            f"{API}/channels/{channel_id}/messages",
            headers=headers,
            json=payload,
        ) as resp:
            status_code = resp.status
            if resp.status == 429:
                # Rate limited — wait and retry once
                retry_after = float((await resp.json()).get("retry_after", 1.0))
                log.warning("Trade log rate-limited; retrying in %.1fs", retry_after)
                await asyncio.sleep(retry_after)
                start_retry = time.perf_counter()
                async with session.post(
                    f"{API}/channels/{channel_id}/messages",
                    headers=headers,
                    json=payload,
                ) as resp2:
                    retry_elapsed = (time.perf_counter() - start_retry) * 1000
                    monitor.record_call("discord", "/channels/{id}/messages", resp2.status, retry_elapsed)
                    if resp2.status not in (200, 201):
                        log.error("Trade log post failed after retry (%d)", resp2.status)
            elif resp.status not in (200, 201):
                text = await resp.text()
                log.error("Trade log post failed (%d): %s", resp.status, text[:200])
    except aiohttp.ClientConnectorError:
        # Force-recreate session on next call (connection was dropped)
        global _http_session
        _http_session = None
        status_code = 0
        error_msg = "Connection lost"
        log.error("Trade log connection lost; session reset")
    except Exception as exc:
        status_code = 500
        error_msg = str(exc)
        log.error("Trade log send error: %s", exc)
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        monitor.record_call("discord", "/channels/{id}/messages", status_code, elapsed_ms, error_msg)


# ── Formatters ────────────────────────────────────────────────────────────────

def _timestamp():
    return datetime.now(timezone.utc).strftime("%H:%M:%S UTC")


def _color_for_action(action: str) -> int:
    """Discord embed color as integer."""
    colors = {
        "BUY": 0x00C853,     # green
        "SELL": 0xFF1744,    # red
        "FILLED": 0x2196F3,  # blue
        "CANCELLED": 0xFF9800,  # orange
        "ERROR": 0xF44336,   # dark red
        "PT1": 0x4CAF50,     # green
        "PT2": 0x8BC34A,     # light green
        "PT3": 0xCDDC39,     # lime
        "SL": 0xF44336,      # red
        "INFO": 0x607D8B,    # grey
        "BLOCKED": 0xFF6F00,  # deep orange
        "SKIPPED": 0x9E9E9E,  # grey
    }
    return colors.get(action, 0x607D8B)



# ── Public logging functions ──────────────────────────────────────────────────

async def log_alert_received(
    action: str, symbol: str, osi: str, price: float,
    author: str, channel: str, size_tag: str = "",
    fraction: float = 1.0, is_historical: bool = False,
):
    """Log when a new alert is parsed from Discord."""
    if is_historical:
        return  # Don't log backfilled alerts

    action_emoji = "\U0001f7e2" if action == "BUY" else "\U0001f534"  # 🟢 / 🔴
    size_info = f" [{size_tag}]" if size_tag else ""
    frac_info = f" {fraction:.0%}" if fraction < 1.0 else ""

    embed = {
        "title": f"{action_emoji} ALERT — {action} {symbol}",
        "description": (
            f"**OSI:** `{osi}`\n"
            f"**Price:** ${price:.2f}{size_info}{frac_info}\n"
            f"**Author:** {author}\n"
            f"**Channel:** {channel}"
        ),
        "color": _color_for_action(action),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Alert Received"},
    }
    await _send_to_channel(embed=embed)


async def log_order_placed(
    side: str, osi: str, qty: int, limit_price: float,
    trigger: str = "DISCORD", order_id: int = None,
):
    """Log when an order is submitted to Public.com."""
    embed = {
        "title": f"\U0001f4e4 ORDER PLACED — {side}",  # 📤
        "description": (
            f"**Symbol:** `{osi}`\n"
            f"**Side:** {side} x{qty}\n"
            f"**Limit:** ${limit_price:.2f}\n"
            f"**Trigger:** {trigger}\n"
            f"**Order #:** {order_id or 'N/A'}"
        ),
        "color": _color_for_action(side),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Order Submitted"},
    }
    await _send_to_channel(embed=embed)


async def log_order_filled(
    side: str, osi: str, qty: int, fill_price: float,
    trigger: str = "DISCORD", order_id: int = None,
    pnl: float = None, pnl_pct: float = None,
):
    """Log when an order is filled."""
    pnl_text = ""
    if pnl is not None:
        pnl_emoji = "\U0001f4b0" if pnl >= 0 else "\U0001f4c9"  # 💰 / 📉
        pnl_text = f"\n**P&L:** {pnl_emoji} ${pnl:+.2f} ({pnl_pct:+.1f}%)" if pnl_pct else f"\n**P&L:** {pnl_emoji} ${pnl:+.2f}"

    embed = {
        "title": f"\u2705 ORDER FILLED — {side}",  # ✅
        "description": (
            f"**Symbol:** `{osi}`\n"
            f"**Side:** {side} x{qty}\n"
            f"**Fill Price:** ${fill_price:.2f}\n"
            f"**Trigger:** {trigger}{pnl_text}"
        ),
        "color": 0x2196F3,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Order Filled"},
    }
    await _send_to_channel(embed=embed)


async def log_order_cancelled(
    osi: str, side: str, reason: str = "", order_id: int = None,
):
    """Log when an order is cancelled or rejected."""
    embed = {
        "title": f"\u274c ORDER CANCELLED — {side}",  # ❌
        "description": (
            f"**Symbol:** `{osi}`\n"
            f"**Reason:** {reason or 'User cancelled'}\n"
            f"**Order #:** {order_id or 'N/A'}"
        ),
        "color": _color_for_action("CANCELLED"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Order Cancelled"},
    }
    await _send_to_channel(embed=embed)


async def log_order_rejected(
    osi: str, side: str, qty: int, reason: str, order_id: int | None = None,
):
    """Post a broker-side rejection to Discord. Triggered from the order
    poller when Public.com returns REJECTED with a reject_reason. Distinct
    from log_order_cancelled because re-peg watcher auto-retries within 1s."""
    embed = {
        "title": f"\U0001f6a8 ORDER REJECTED — {side}",
        "description": (
            f"**Symbol:** `{osi}`\n"
            f"**Side / Qty:** {side} x{qty}\n"
            f"**Broker reason:** {reason}\n"
            f"**Order #:** {order_id or 'N/A'}\n"
            f"*Re-peg watcher will retry with fresh quote.*"
        ),
        "color": _color_for_action("BLOCKED"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Order Rejected by Broker"},
    }
    await _send_to_channel(embed=embed)


async def log_health_alarm(component: str, message: str):
    """Post a Discord alarm when a bot component goes silent / DEGRADED."""
    embed = {
        "title": f"\U0001f6a8 HEALTH ALARM — {component}",
        "description": message,
        "color": _color_for_action("BLOCKED"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Health Monitor"},
    }
    # Route to the dedicated daily-summary / alerts channel so all critical
    # notifications land in one place. Falls back to trade-log channel.
    await _send_to_channel(embed=embed, channel_id=_daily_summary_channel_id())


# Severity colors: critical=red, warning=orange, info=blue.
_SEV_COLORS = {"critical": 0xE74C3C, "warning": 0xE67E22, "info": 0x3498DB}


async def log_critical(title: str, message: str, severity: str = "critical", footer: str | None = None):
    """Post a critical-class alert to the daily-summary / alerts channel.

    Use for events the operator must see in real time:
      - broker upstream errors retried / exhausted
      - external fills auto-recovered
      - suspicious config edits
      - guardrail tripped that blocks live orders

    severity ∈ {critical, warning, info} — colors the embed accordingly.
    Falls back to the trade-log channel if daily_summary_channel_id is unset.
    """
    color = _SEV_COLORS.get(severity, _SEV_COLORS["critical"])
    icon = {"critical": "\U0001f6a8", "warning": "⚠️", "info": "ℹ️"}.get(severity, "\U0001f6a8")
    embed = {
        "title": f"{icon} {title}",
        "description": message,
        "color": color,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": footer or f"Critical alert ({severity})"},
    }
    await _send_to_channel(embed=embed, channel_id=_daily_summary_channel_id())


async def log_reconcile_alert(osi: str, message: str):
    """Manual-review-required alert from the reconciler (e.g. avg-price big drift)."""
    embed = {
        "title": f"⚠️ RECONCILE ALERT — {osi}",
        "description": message,
        "color": _color_for_action("BLOCKED"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Reconciler — Manual Review"},
    }
    await _send_to_channel(embed=embed)


async def log_slippage_alert(osi: str, alert_price: float, fill_price: float, slippage_pct: float):
    """Alert when a BUY filled significantly above the analyst's alert price."""
    embed = {
        "title": f"\U0001f4b8 HIGH SLIPPAGE — {osi}",
        "description": (
            f"**Alert price:** ${alert_price:.2f}\n"
            f"**Fill price:** ${fill_price:.2f}\n"
            f"**Slippage:** {slippage_pct:+.1f}% over alert\n"
            f"Investigate buy_slippage tuning or wide spread at entry."
        ),
        "color": _color_for_action("BLOCKED"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Slippage Alert"},
    }
    await _send_to_channel(embed=embed)


async def log_alert_blocked(
    action: str,
    osi: str,
    reason: str,
    author: str = "",
    block_type: str = "BLOCKED",
):
    """
    Post a Discord embed when an alert is BLOCKED before an order is placed.

    block_type can be:
      - "BLOCKED"  → guardrail rule violation (e.g. market closed, max cost)
      - "SKIPPED"  → scorer confidence below minimum
      - "KELLY"    → Kelly Criterion says no edge
      - "DUPLICATE"→ same alert already processed
    """
    # Map long guardrail reason strings to a short rule name + detail
    rule = block_type
    detail = reason

    # Parse structured reason strings from guardrails
    if ":" in reason:
        parts = reason.split(":", 1)
        rule_part = parts[0].strip()   # e.g. "MAX COST", "BLOCKED", "DUPLICATE"
        detail = parts[1].strip()      # rest of the message
        # Use the specific rule name directly
        if rule_part in ("MAX COST", "MAX POSITIONS", "MAX TRADES", "MAX LOSS",
                         "HALTED", "DUPLICATE", "CROSS-CHANNEL DUPLICATE", "COOLDOWN"):
            rule = rule_part
        elif rule_part == "BLOCKED":
            # "BLOCKED: Market closed ..." — extract sub-rule
            if "Market closed" in detail:
                rule = "MARKET CLOSED"
            elif "max" in detail.lower():
                rule = "LIMIT EXCEEDED"

    emoji = {
        "BLOCKED":               "\U0001f6ab",  # 🚫
        "MARKET CLOSED":         "\U0001f55b",  # 🕛
        "MAX COST":              "\U0001f4b8",  # 💸
        "MAX POSITIONS":         "\U0001f4ca",  # 📊
        "MAX TRADES":            "\U0001f4cb",  # 📋
        "MAX LOSS":              "\U0001f6d1",  # 🛑
        "HALTED":                "\U0001f6d1",  # 🛑
        "DUPLICATE":             "\U0001f501",  # 🔁
        "CROSS-CHANNEL DUPLICATE": "\U0001f501",
        "COOLDOWN":              "\u23f3",       # ⏳
        "SKIPPED":               "\U0001f4c9",  # 📉
        "KELLY":                 "\U0001f9ee",  # 🧮
    }.get(rule, "\U0001f6ab")

    title = f"{emoji} {rule} — {action} {osi or '(no symbol)'}"

    lines = [f"**Rule:** {rule}", f"**Detail:** {detail}"]
    if author:
        lines.append(f"**Author:** {author}")

    embed = {
        "title": title,
        "description": "\n".join(lines),
        "color": _color_for_action(block_type),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": f"Alert {block_type.title()}"},
    }
    await _send_to_channel(embed=embed)


async def log_auto_exit(
    rule: str, osi: str, symbol: str, strike: int, option_type: str,
    qty: int, price: float, pnl_pct: float,
    avg_price: float = None,
):
    """Log when an auto-exit rule triggers (PT1/PT2/PT3/SL)."""
    rule_descriptions = {
        "PT1": "Profit Target 1 — Taking partial profits",
        "PT2": "Profit Target 2 — Taking more profits",
        "PT3": "Profit Target 3 — Closing remaining",
        "TPT": "Trailing Profit Target — Locking in gains",
        "SL": "Stop Loss — Cutting losses",
        "TRAILING_SL": "Trailing Stop Loss — Protecting gains",
        "MANUAL": "Manual Exit",
    }

    from app.core.models import is_credit_combo_osi
    credit = is_credit_combo_osi(osi)
    if avg_price:
        pnl_per_contract = ((avg_price - price) if credit else (price - avg_price)) * 100
    else:
        pnl_per_contract = 0
    total_pnl = pnl_per_contract * qty

    emoji = "\U0001f6a8" if rule in ("SL", "TRAILING_SL") else "\U0001f3af"  # 🚨 / 🎯

    # Build description safely (no nested f-string conditionals)
    desc_lines = [
        f"**Rule:** {rule_descriptions.get(rule, rule)}",
        f"**Symbol:** `{osi}`",
        f"**Contracts:** {qty}",
        f"**Exit Price:** ${price:.2f}",
    ]
    if avg_price:
        desc_lines.append(f"**Avg Entry:** ${avg_price:.2f}")
    desc_lines.append(f"**P&L %:** {pnl_pct:+.1f}%")
    desc_lines.append(f"**P&L $:** ${total_pnl:+.2f}")

    embed = {
        "title": f"{emoji} AUTO-EXIT — {rule} on {symbol} {strike}{option_type}",
        "description": "\n".join(desc_lines),
        "color": _color_for_action(rule),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": f"Auto-Exit: {rule}"},
    }
    await _send_to_channel(embed=embed)


async def _generate_ai_summary(stats: dict) -> str:
    """Generate AI-powered insights for the daily summary using Claude."""
    try:
        import aiohttp

        if not cfg.getboolean("trading", "daily_summary_ai_enabled", fallback=True):
            return ""

        api_key = cfg.get("ai_parser", "api_key", fallback="")
        if not api_key:
            return ""

        model = cfg.get("ai_parser", "model", fallback="claude-haiku-4-5-20251001")

        # Build prompt with trading data
        prompt = f"""As a trading analyst, review today's performance and provide 2-3 brief insights:

Today's Stats:
- Realized P&L: ${stats['total_realized']:+.2f}
- Win Rate: {stats['win_rate']}
- Trades: {stats['buy_orders']} buys, {stats['sell_orders']} sells
- Positions Closed: {stats['closed_count']}
- Open Positions: {stats['open_count']}
- Wins: {stats['wins']}, Losses: {stats['losses']}

Provide:
1. Pattern observation (if any)
2. Risk assessment
3. One actionable tip for tomorrow

Keep it under 150 words, professional but conversational."""

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
                    "max_tokens": 300,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("content", [{}])[0].get("text", "")
                return ""
    except Exception as e:
        log.warning("AI summary generation failed: %s", e)
        return ""



async def log_daily_summary():
    """Generate and post a daily P&L summary to #trade-logs."""
    # Compute all stats first, close DB, THEN do network calls. Holding a
    # SessionLocal across the AI summary (10s) and Discord post blocks the
    # SQLite WAL writers (reconciler / position_monitor commits) for the
    # whole network round-trip duration.
    db = SessionLocal()
    try:
        # "Today" = the ET trading day, not UTC midnight (which starts 19:00/
        # 20:00 ET the prior evening and mislabels the window).
        from zoneinfo import ZoneInfo
        _et = ZoneInfo("America/New_York")
        _now_et = datetime.now(_et)
        today = _now_et.date()
        today_start = _now_et.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)

        orders = db.query(Order).filter(
            Order.placed_at >= today_start,
            Order.status == "FILLED",
        ).all()
        open_positions = db.query(Position).filter(
            Position.status.in_(["OPEN", "PARTIAL"])
        ).all()
        closed_today = db.query(Position).filter(
            Position.close_time >= today_start,
            Position.status == "CLOSED",
        ).all()

        # Calculate P&L
        total_realized = 0.0
        total_unrealized = 0.0
        wins = 0
        losses = 0

        for pos in closed_today:
            if not pos.avg_price:
                continue
            pnl = pos.pnl_dollar()
            total_realized += pnl
            if pnl >= 0:
                wins += 1
            else:
                losses += 1

        for pos in open_positions:
            if pos.avg_price and pos.current_price is not None:
                total_unrealized += pos.pnl_dollar()

        buy_orders = sum(1 for o in orders if o.side == "BUY")
        sell_orders = sum(1 for o in orders if o.side == "SELL")
        win_rate = f"{wins/(wins+losses)*100:.0f}%" if (wins + losses) > 0 else "N/A"
        orders_count = len(orders)
        closed_count = len(closed_today)
        open_count = len(open_positions)

        # Execution quality: submit→fill latency p95 + worst fill-vs-limit
        # slippage across today's fills.
        _lats = sorted(
            (o.filled_at - o.placed_at).total_seconds() * 1000.0
            for o in orders if o.filled_at and o.placed_at and o.filled_at >= o.placed_at
        )
        fill_p95_ms = _lats[min(len(_lats) - 1, int(len(_lats) * 0.95))] if _lats else None
        worst_slip = None  # (signed $, osi) — positive = paid over limit (BUY) / sold under (SELL)
        for o in orders:
            if o.fill_price and o.limit_price:
                adverse = (o.fill_price - o.limit_price) if o.side == "BUY" else (o.limit_price - o.fill_price)
                if worst_slip is None or adverse > worst_slip[0]:
                    worst_slip = (adverse, o.osi_symbol)

        # Guardrail blocks: today's SKIPPED/ERROR alerts grouped by rule
        # (error_text convention is "RULE: detail").
        blocked_rows = db.query(Alert).filter(
            Alert.timestamp >= today_start,
            Alert.status.in_(["SKIPPED", "ERROR"]),
        ).all()
        block_counts: dict = {}
        for a in blocked_rows:
            rule = (a.error_text or "unknown").split(":", 1)[0].strip()[:40]
            block_counts[rule] = block_counts.get(rule, 0) + 1
    finally:
        db.close()

    # Snapshot complete; safe to do network I/O without holding the DB.
    stats = {
        "total_realized": total_realized,
        "win_rate": win_rate,
        "buy_orders": buy_orders,
        "sell_orders": sell_orders,
        "closed_count": closed_count,
        "open_count": open_count,
        "wins": wins,
        "losses": losses,
    }
    ai_insights = await _generate_ai_summary(stats)

    pnl_emoji = "\U0001f4b0" if total_realized >= 0 else "\U0001f4c9"
    robot_emoji = "\U0001f916" if ai_insights else ""

    description = (
        f"**Orders Filled:** {orders_count} ({buy_orders} buys, {sell_orders} sells)\n"
        f"**Positions Closed:** {closed_count}\n"
        f"**Open Positions:** {open_count}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"**Realized P&L:** {pnl_emoji} ${total_realized:+.2f}\n"
        f"**Unrealized P&L:** ${total_unrealized:+.2f}\n"
        f"**Win Rate:** {win_rate} ({wins}W / {losses}L)\n"
    )

    # Execution quality + guardrail digest
    if fill_p95_ms is not None:
        description += (
            f"**Fill latency p95:** {fill_p95_ms/1000:.1f}s"
            if fill_p95_ms >= 1000 else f"**Fill latency p95:** {fill_p95_ms:.0f}ms"
        )
        if worst_slip and abs(worst_slip[0]) >= 0.01:
            description += f" · **Worst slippage:** ${worst_slip[0]:+.2f} ({worst_slip[1]})"
        description += "\n"
    if block_counts:
        top = sorted(block_counts.items(), key=lambda kv: -kv[1])[:5]
        description += "**Blocked:** " + ", ".join(f"{n}× {r}" for r, n in top) + "\n"

    if ai_insights:
        description += f"\n{robot_emoji} **AI Insights:**\n{ai_insights}"

    # API usage section — Vision (chart OCR) + Vertex AI (Gemini parser/advisor).
    # Counters reset at midnight ET; this is today's consumption since last reset.
    try:
        import app.core.usage_tracker as _ut
        usage = _ut.snapshot()
        v_calls = usage["vision"]["calls"]
        v_err = usage["vision"]["errors"]
        g = usage["vertex"]
        if v_calls or g["calls"]:
            description += "\n━━━━━━━━━━━━━━━━━━━━━\n**API Usage Today:**"
            if v_calls:
                description += (
                    f"\n• Vision OCR: {v_calls} calls"
                    f"{f' ({v_err} errors)' if v_err else ''}"
                )
            if g["calls"]:
                in_k = g["input_tokens"] / 1000.0
                out_k = g["output_tokens"] / 1000.0
                description += (
                    f"\n• Vertex AI: {g['calls']} calls"
                    f"{f' ({g['errors']} errors)' if g['errors'] else ''}"
                    f", {in_k:,.1f}k in / {out_k:,.1f}k out tokens"
                )
                # Per-model breakdown if multiple models were used.
                bm = g.get("by_model", {})
                if len(bm) > 1:
                    for model, mv in sorted(bm.items()):
                        description += (
                            f"\n   ◦ {model}: {mv['calls']}× "
                            f"({mv['input_tokens']/1000.0:,.1f}k/{mv['output_tokens']/1000.0:,.1f}k)"
                        )
    except Exception as _u_exc:
        log.warning("Daily summary usage section failed: %s", _u_exc)

    embed = {
        "title": f"\U0001f4ca DAILY SUMMARY — {today.strftime('%A, %B %d')}",
        "description": description,
        "color": 0x00C853 if total_realized >= 0 else 0xFF1744,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Daily Summary | Auto-generated" + (" | AI-powered" if ai_insights else "")},
    }
    await _send_to_channel(embed=embed, channel_id=_daily_summary_channel_id())


# ── CSV Export ────────────────────────────────────────────────────────────────

def export_trades_csv(days: int = 30) -> str:
    """Export trade history as CSV string."""
    db = SessionLocal()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        orders = db.query(Order).filter(
            Order.placed_at >= cutoff,
        ).order_by(Order.placed_at.desc()).all()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Date", "Time (UTC)", "Analyst", "Lane", "Side", "Symbol", "OSI",
            "Qty", "Filled Qty", "Limit Price", "Fill Price", "Slip vs Limit %",
            "Cost Basis", "Status", "Trigger", "Resting", "Error",
        ])

        for o in orders:
            created = o.placed_at or datetime.now(timezone.utc)
            # The ticker used to be osi_symbol[:6], which is only right for
            # six-character roots — it produced "MU2608" and "SPXW26".
            try:
                from app.risk.guardrails import extract_root_symbol
                root = extract_root_symbol(o.osi_symbol or "") or ""
            except Exception:
                root = ""

            slip = ""
            if o.limit_price and o.fill_price:
                slip = f"{(o.fill_price / o.limit_price - 1) * 100:+.2f}%"

            writer.writerow([
                created.strftime("%Y-%m-%d"),
                created.strftime("%H:%M:%S"),
                o.author or "",
                o.strategy_tag or "",
                o.side,
                root,
                o.osi_symbol,
                o.quantity,
                o.filled_qty if o.filled_qty is not None else "",
                f"{o.limit_price:.2f}" if o.limit_price else "",
                f"{o.fill_price:.2f}" if o.fill_price else "",
                slip,
                f"{o.cost_basis:.2f}" if o.cost_basis is not None else "",
                o.status,
                o.trigger,
                "yes" if o.is_resting else "",
                o.error_text or "",
            ])

        return output.getvalue()
    finally:
        db.close()


def export_positions_csv() -> str:
    """Export all positions (open and closed) as CSV.

    Carries the columns the dashboard already knows but the old 13-column
    export dropped on the floor: who the analyst was, which lane the trade
    ran in (SMALL_ACCT / WHALE / RE-ENTRY), the resolved profile whose knobs
    actually governed it, expiry, the high/low water marks and the exit
    triggers. Without author and strategy_tag the file cannot answer "how is
    this analyst doing" or "how do the small-account trades compare", which
    is most of what anyone opens it for.

    Two notes on the P&L columns:

    - "P&L $" is RECONSTRUCTED from avg_price/exit/contracts, not read from
      positions.realized_pnl. That column is NULL or 0 on the overwhelming
      majority of rows (297 of 309 over the 90 days to 2026-08-19), so
      trusting it understates the book badly. The stored value ships beside
      it as "Realized P&L (db)" so the two can be compared rather than
      silently conflated.
    - It uses `remaining` for open rows and `total_contracts` for closed
      ones, because a partially-closed position has not realised the whole
      lot.
    """
    db = SessionLocal()
    try:
        positions = db.query(Position).order_by(Position.open_time.desc()).all()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Open Date", "Close Date", "Analyst", "Lane", "Profile",
            "Symbol", "OSI", "Expiry", "Strike", "Type",
            "Contracts", "Remaining", "Avg Entry", "Exit Price",
            "P&L $", "P&L %", "Realized P&L (db)",
            "Peak", "MFE %", "Trough", "MAE %",
            "Exits Fired", "Spread", "Status",
        ])

        for p in positions:
            closed = p.status == "CLOSED"
            exit_px = p.close_price if (closed and p.close_price is not None) else p.current_price
            qty = (p.total_contracts or 0) if closed else (p.remaining or 0)

            pnl = pnl_pct = 0.0
            if p.avg_price and exit_px is not None:
                from app.core.models import fill_pnl_dollar, is_credit_combo_osi
                credit = bool(getattr(p, "is_credit", False)) or is_credit_combo_osi(
                    getattr(p, "osi_symbol", "") or ""
                )
                pnl = fill_pnl_dollar(exit_px, p.avg_price, qty, credit)
                if credit:
                    risk = (getattr(p, "spread_width", 0) or 0) - p.avg_price
                    pnl_pct = ((p.avg_price - exit_px) / risk) * 100 if risk > 0 else 0.0
                else:
                    pnl_pct = ((exit_px / p.avg_price) - 1) * 100

            def _pct(v):
                if not p.avg_price or v is None:
                    return ""
                return f"{((v / p.avg_price) - 1) * 100:+.1f}%"

            fired = ",".join(n for n, hit in (
                ("PT1", p.pt1_triggered), ("PT2", p.pt2_triggered),
                ("PT3", p.pt3_triggered), ("SL", p.sl_triggered),
                ("SL_PARTIAL", p.sl_partial_triggered), ("TPT_ARMED", p.tpt_armed),
            ) if hit)

            try:
                from app.core.profile_resolver import _sections_for
                profile = next((s.removeprefix("profile:") for s in _sections_for(p)), "trading")
            except Exception:
                profile = ""

            writer.writerow([
                p.open_time.strftime("%Y-%m-%d %H:%M") if p.open_time else "",
                p.close_time.strftime("%Y-%m-%d %H:%M") if p.close_time else "",
                p.author or "",
                p.strategy_tag or "",
                profile,
                p.symbol,
                p.osi_symbol,
                p.expiry or "",
                p.strike,
                p.option_type,
                p.total_contracts,
                p.remaining,
                f"{p.avg_price:.2f}" if p.avg_price else "",
                f"{exit_px:.2f}" if exit_px is not None else "",
                f"{pnl:+.2f}",
                f"{pnl_pct:+.1f}%",
                f"{p.realized_pnl:+.2f}" if p.realized_pnl is not None else "",
                f"{p.highest_price:.2f}" if p.highest_price else "",
                _pct(p.highest_price),
                f"{p.lowest_price:.2f}" if p.lowest_price else "",
                _pct(p.lowest_price),
                fired,
                f"{p.spread_width:g}-wide credit" if p.is_credit and p.spread_width else ("credit" if p.is_credit else ""),
                p.status,
            ])

        return output.getvalue()
    finally:
        db.close()


def export_alerts_csv(days: int = 30) -> str:
    """Export alert history as CSV."""
    db = SessionLocal()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        alerts = db.query(Alert).filter(
            Alert.timestamp >= cutoff,
        ).order_by(Alert.timestamp.desc()).all()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Date", "Time (UTC)", "Author", "Action", "Symbol", "OSI",
            "Strike", "Type", "Price", "Size", "Fraction",
            "Channel", "Status", "Error", "Raw Text",
        ])

        for a in alerts:
            ts = a.timestamp or datetime.now(timezone.utc)
            writer.writerow([
                ts.strftime("%Y-%m-%d"),
                ts.strftime("%H:%M:%S"),
                a.author,
                a.action,
                a.symbol,
                a.osi_symbol,
                a.strike,
                a.option_type,
                f"{a.alert_price:.2f}" if a.alert_price else "",
                a.size_tag,
                a.fraction,
                a.channel_name,
                a.status,
                a.error_text or "",
                (a.raw_text or "")[:200],
            ])

        return output.getvalue()
    finally:
        db.close()


# ── One export, related ───────────────────────────────────────────────────────

_LIFECYCLE_COLS = [
    # what the analyst said
    "Alert ID", "Alert Time (ET)", "Analyst", "Channel", "Lane", "Alert Text",
    "Alert Price", "Stated Qty", "Alert Status",
    # what the bot did about it
    "Order ID", "Order Time (ET)", "Side", "Trigger", "Qty", "Filled Qty",
    "Limit", "Fill", "Slip vs Alert %", "Slip vs Limit %", "Reaction (s)",
    "Order Status", "Resting", "Order Error",
    # what it turned into
    "Position ID", "Symbol", "OSI", "Expiry", "Strike", "Type", "Profile",
    "Position Status", "Entry", "Exit", "Contracts", "Remaining",
    "P&L $", "P&L %", "Peak", "MFE %", "Trough", "MAE %", "Exits Fired",
    "Opened (ET)", "Closed (ET)", "Hold (min)", "Spread",
]

_NOT_TAKEN_COLS = [
    "Alert Time (ET)", "Analyst", "Channel", "Lane", "Action", "OSI",
    "Symbol", "Strike", "Type", "Alert Price", "Stated Qty",
    "Status", "Why Not Taken", "Alert Text",
]


def _et(dt):
    """UTC-naive DB datetime → 'YYYY-MM-DD HH:MM:SS' Eastern, per repo convention."""
    if not dt:
        return ""
    from zoneinfo import ZoneInfo
    d = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return d.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")


def _lifecycle_rows(db, days: int):
    """Join alert → order → position into one row per order.

    Orders carry alert_id from 2026-08-19 on. Older rows predate the column,
    so they fall back to matching the nearest same-symbol, same-side alert
    within a couple of minutes — good enough for history, exact from here.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    orders = (db.query(Order).filter(Order.placed_at >= cutoff)
              .order_by(Order.placed_at.desc()).all())
    alerts = db.query(Alert).filter(Alert.timestamp >= cutoff).all()
    positions = {p.osi_symbol: p for p in db.query(Position).all()}

    by_id = {a.id: a for a in alerts}
    fallback = {}
    for a in alerts:
        fallback.setdefault((a.osi_symbol, a.action), []).append(a)

    def _alert_for(o):
        if getattr(o, "alert_id", None) and o.alert_id in by_id:
            return by_id[o.alert_id]
        best, best_gap = None, 121.0
        for a in fallback.get((o.osi_symbol, o.side), ()):
            if not a.timestamp or not o.placed_at:
                continue
            gap = abs((o.placed_at - a.timestamp).total_seconds())
            if gap < best_gap:
                best, best_gap = a, gap
        return best

    used, rows = set(), []
    for o in orders:
        a = _alert_for(o)
        if a is not None:
            used.add(a.id)
        p = positions.get(o.osi_symbol)

        slip_alert = slip_lim = react = ""
        if a is not None and a.alert_price and o.fill_price:
            slip_alert = round((o.fill_price / a.alert_price - 1) * 100, 2)
        if o.limit_price and o.fill_price:
            slip_lim = round((o.fill_price / o.limit_price - 1) * 100, 2)
        if a is not None and a.timestamp and o.placed_at:
            react = round((o.placed_at - a.timestamp).total_seconds(), 2)

        pnl = pnl_pct = ""
        entry = exit_px = None
        if p is not None:
            closed = p.status == "CLOSED"
            exit_px = p.close_price if (closed and p.close_price is not None) else p.current_price
            qty = (p.total_contracts or 0) if closed else (p.remaining or 0)
            entry = p.avg_price
            if entry and exit_px is not None:
                from app.core.models import fill_pnl_dollar, is_credit_combo_osi
                credit = bool(getattr(p, "is_credit", False)) or is_credit_combo_osi(
                    getattr(p, "osi_symbol", "") or ""
                )
                pnl = round(fill_pnl_dollar(exit_px, entry, qty, credit), 2)
                if credit:
                    risk = (getattr(p, "spread_width", 0) or 0) - entry
                    pnl_pct = round(((entry - exit_px) / risk) * 100, 1) if risk > 0 else 0.0
                else:
                    pnl_pct = round((exit_px / entry - 1) * 100, 1)

        def _wm(v):
            return round((v / entry - 1) * 100, 1) if (entry and v) else ""

        hold = ""
        if p is not None and p.open_time and p.close_time:
            hold = round((p.close_time - p.open_time).total_seconds() / 60, 1)

        fired = ",".join(n for n, hit in (
            ("PT1", getattr(p, "pt1_triggered", False)), ("PT2", getattr(p, "pt2_triggered", False)),
            ("PT3", getattr(p, "pt3_triggered", False)), ("SL", getattr(p, "sl_triggered", False)),
        ) if hit) if p is not None else ""

        profile = ""
        if p is not None:
            try:
                from app.core.profile_resolver import _sections_for
                profile = next((s.removeprefix("profile:") for s in _sections_for(p)), "trading")
            except Exception:
                profile = ""

        rows.append([
            a.id if a is not None else "", _et(a.timestamp) if a is not None else "",
            (a.author if a is not None else None) or o.author or "",
            (a.channel_name if a is not None else "") or "",
            o.strategy_tag or (a.strategy_tag if a is not None else "") or "",
            ((a.raw_text or "").replace("\n", " ")[:300]) if a is not None else "",
            a.alert_price if a is not None else "", a.qty if a is not None else "",
            a.status if a is not None else "",

            o.id, _et(o.placed_at), o.side, o.trigger, o.quantity,
            o.filled_qty if o.filled_qty is not None else "",
            o.limit_price or "", o.fill_price or "", slip_alert, slip_lim, react,
            o.status, "yes" if o.is_resting else "", (o.error_text or "")[:200],

            p.id if p is not None else "",
            (p.symbol if p is not None else "") or "",
            o.osi_symbol,
            (p.expiry if p is not None else "") or "",
            p.strike if p is not None else "", (p.option_type if p is not None else "") or "",
            profile, (p.status if p is not None else "") or "",
            entry or "", exit_px if exit_px is not None else "",
            p.total_contracts if p is not None else "", p.remaining if p is not None else "",
            pnl, pnl_pct,
            (p.highest_price if p is not None else "") or "", _wm(getattr(p, "highest_price", None)),
            (p.lowest_price if p is not None else "") or "", _wm(getattr(p, "lowest_price", None)),
            fired,
            _et(p.open_time) if p is not None else "", _et(p.close_time) if p is not None else "",
            hold,
            (f"{p.spread_width:g}-wide credit" if p is not None and p.is_credit and p.spread_width
             else ("credit" if p is not None and p.is_credit else "")),
        ])

    not_taken = []
    for a in alerts:
        if a.id in used:
            continue
        not_taken.append([
            _et(a.timestamp), a.author or "", a.channel_name or "", a.strategy_tag or "",
            a.action or "", a.osi_symbol or "", a.symbol or "", a.strike, a.option_type or "",
            a.alert_price, a.qty, a.status or "",
            (a.error_text or "").replace("\n", " ")[:300],
            (a.raw_text or "").replace("\n", " ")[:300],
        ])
    not_taken.sort(key=lambda r: r[0], reverse=True)
    return rows, not_taken


_ROUNDTRIP_COLS = [
    "Buy Order ID", "Buy Time (ET)", "Analyst", "Channel", "Lane",
    "Alert Time (ET)", "Alert Price", "Reaction (s)",
    "Symbol", "OSI", "Expiry", "Strike", "Type",
    "Qty", "Buy Fill", "Slip vs Alert %",
    "Sell Order ID", "Sell Time (ET)", "Exit Via", "Sell Fill",
    "Hold (min)", "P&L $", "P&L %", "Match", "Position ID", "Alert Text",
]


def _rt_row(b, s, qty, C):
    """One matched (or half-matched) lot as a Round Trips row."""
    ref = b if b is not None else s
    bp = b[C["Fill"]] if b is not None else None
    sp = s[C["Fill"]] if s is not None else None
    pnl = pnl_pct = ""
    if bp and sp:
        from app.core.models import is_credit_combo_osi
        osi = ref[C["OSI"]] if "OSI" in C else ""
        if is_credit_combo_osi(osi):
            pnl = round((bp - sp) * qty * 100, 2)
            pnl_pct = round((bp - sp) / bp * 100, 1)
        else:
            pnl = round((sp - bp) * qty * 100, 2)
            pnl_pct = round((sp / bp - 1) * 100, 1)

    hold = ""
    if b is not None and s is not None:
        try:
            fmt = "%Y-%m-%d %H:%M:%S"
            hold = round((datetime.strptime(s[C["Order Time (ET)"]], fmt)
                          - datetime.strptime(b[C["Order Time (ET)"]], fmt)
                          ).total_seconds() / 60, 1)
        except Exception:
            hold = ""

    match = ("FIFO" if (b is not None and s is not None)
             else ("OPEN" if s is None else "SELL W/O BUY"))

    return [
        b[C["Order ID"]] if b is not None else "",
        b[C["Order Time (ET)"]] if b is not None else "",
        ref[C["Analyst"]] or "", ref[C["Channel"]] or "", ref[C["Lane"]] or "",
        b[C["Alert Time (ET)"]] if b is not None else "",
        b[C["Alert Price"]] if b is not None else "",
        b[C["Reaction (s)"]] if b is not None else "",
        ref[C["Symbol"]] or "", ref[C["OSI"]],
        ref[C["Expiry"]] or "", ref[C["Strike"]], ref[C["Type"]] or "",
        qty, bp if bp else "", b[C["Slip vs Alert %"]] if b is not None else "",
        s[C["Order ID"]] if s is not None else "",
        s[C["Order Time (ET)"]] if s is not None else "",
        s[C["Trigger"]] if s is not None else "",
        sp if sp else "",
        hold, pnl, pnl_pct, match,
        ref[C["Position ID"]],
        (b[C["Alert Text"]] if b is not None else ref[C["Alert Text"]]) or "",
    ]


def _round_trips(rows):
    """FIFO-match filled BUY lots against filled SELLs → one row per round trip.

    The Lifecycle sheet is one row per ORDER, so a partially-scaled position
    is spread across several rows with no lot-level link between them and the
    P&L column repeats the whole position's number on each. This walks the
    fills oldest-first and consumes the oldest open BUY lot on each SELL, so
    every row is a real closed lot with its own basis, its own exit and its
    own money. Three partial exits off one 10-lot entry are three rows of
    3/3/4 contracts, not one row counted three times.

    Matching key is (contract, analyst) — positions are author-scoped, so a
    cross-analyst SELL must not consume someone else's lot. SELLs whose author
    is blank (MANUAL / SL / pre-backfill rows) fall back to any open lot on
    the same contract.
    """
    C = {n: i for i, n in enumerate(_LIFECYCLE_COLS)}

    def _fq(r):
        q = r[C["Filled Qty"]]
        if isinstance(q, (int, float)) and q > 0:
            return int(q)
        q = r[C["Qty"]]
        if r[C["Order Status"]] == "FILLED" and isinstance(q, (int, float)) and q > 0:
            return int(q)
        return 0

    fills = [r for r in rows
             if isinstance(r[C["Fill"]], (int, float)) and r[C["Fill"]] > 0 and _fq(r) > 0]
    fills.sort(key=lambda r: (str(r[C["Order Time (ET)"]]), r[C["Order ID"]] or 0))

    lots, out = {}, []          # (osi, analyst) -> [[buy_row, remaining], ...]
    for r in fills:
        osi, who, qty = r[C["OSI"]], r[C["Analyst"]] or "", _fq(r)
        if r[C["Side"]] == "BUY":
            lots.setdefault((osi, who), []).append([r, qty])
            continue
        while qty > 0:
            q = lots.get((osi, who))
            if not q and not who:
                # Authorless exit (MANUAL / SL / pre-backfill): it closed
                # *something*, so take any open lot on the contract.
                # ponytail: first queue found, not the globally oldest lot.
                q = next((v for (o, _), v in lots.items() if o == osi and v), None)
            if not q:
                out.append(_rt_row(None, r, qty, C))   # entry predates the window
                break
            lot = q[0]
            take = min(qty, lot[1])
            out.append(_rt_row(lot[0], r, take, C))
            lot[1] -= take
            qty -= take
            if lot[1] <= 0:
                q.pop(0)

    for q in lots.values():                             # never sold in-window
        out.extend(_rt_row(row, None, rem, C) for row, rem in q if rem > 0)

    si, bi = _ROUNDTRIP_COLS.index("Sell Time (ET)"), _ROUNDTRIP_COLS.index("Buy Time (ET)")
    out.sort(key=lambda r: str(r[si] or r[bi]), reverse=True)
    return out


def export_all_xlsx(days: int = 30) -> bytes:
    """One workbook, four related sheets. Replaces the three separate exports.

    Sheet "Round Trips" is the money view: BUY fills matched to SELL fills
    first-in-first-out, one row per closed lot, so each row has its own basis,
    its own exit and its own P&L instead of repeating a position total.

    Sheet "Lifecycle" is the audit view: one row per order with its alert and its
    position joined on, so a single row reads Discord message -> what the bot
    placed -> what it filled at -> what the position did. Previously those
    lived in three files with no key between them, so answering "what did this
    alert actually make" meant matching timestamps by hand across two CSVs.

    Sheet 2 "Not Taken" is every alert of the period that produced no order --
    blocked, skipped, errored, or ignored -- with the reason. That is the set
    the Lifecycle sheet cannot show, because there is nothing to join to.

    Sheet 3 "Summary" is a small header block so the file is self-describing
    once it is off the dashboard and in someone's downloads folder.
    """
    from openpyxl import Workbook
    from openpyxl.styles import Font
    from openpyxl.utils import get_column_letter

    db = SessionLocal()
    try:
        rows, not_taken = _lifecycle_rows(db, days)
    finally:
        db.close()

    wb = Workbook()

    def _sheet(title, cols, data, widths=None):
        ws = wb.create_sheet(title)
        ws.append(cols)
        for c in ws[1]:
            c.font = Font(bold=True)
        for r in data:
            ws.append(r)
        ws.freeze_panes = "A2"
        if data:
            ws.auto_filter.ref = f"A1:{get_column_letter(len(cols))}{len(data) + 1}"
        for i, name in enumerate(cols, start=1):
            w = (widths or {}).get(name, min(max(11, len(name) + 2), 34))
            ws.column_dimensions[get_column_letter(i)].width = w
        return ws

    trips = _round_trips(rows)

    wb.remove(wb.active)
    _sheet("Round Trips", _ROUNDTRIP_COLS, trips,
           {"Alert Text": 60, "OSI": 24})
    _sheet("Lifecycle", _LIFECYCLE_COLS, rows,
           {"Alert Text": 60, "Order Error": 30, "OSI": 24})
    _sheet("Not Taken", _NOT_TAKEN_COLS, not_taken,
           {"Alert Text": 60, "Why Not Taken": 60, "OSI": 24})

    filled = [r for r in rows if r[_LIFECYCLE_COLS.index("Order Status")] == "FILLED"]
    pnls = [r[_LIFECYCLE_COLS.index("P&L $")] for r in rows
            if isinstance(r[_LIFECYCLE_COLS.index("P&L $")], (int, float))]
    M = _ROUNDTRIP_COLS.index("Match")
    matched = [t for t in trips if t[M] == "FIFO"]
    fifo_pnl = [t[_ROUNDTRIP_COLS.index("P&L $")] for t in matched
                if isinstance(t[_ROUNDTRIP_COLS.index("P&L $")], (int, float))]
    ws = wb.create_sheet("Summary", 0)
    for k, v in (
        ("Generated (ET)", _et(datetime.now(timezone.utc))),
        ("Period", f"last {days} days"),
        ("Orders", len(rows)),
        ("Filled", len(filled)),
        ("Alerts not taken", len(not_taken)),
        ("Round trips (FIFO matched)", len(matched)),
        ("Net P&L (FIFO matched lots)", round(sum(fifo_pnl), 2) if fifo_pnl else 0),
        ("Buy lots still open", sum(1 for t in trips if t[M] == "OPEN")),
        ("Sells with no buy in window", sum(1 for t in trips if t[M] == "SELL W/O BUY")),
        ("Net P&L of joined positions", round(sum(set(pnls)), 2) if pnls else 0),
        ("", ""),
        ("Note", "Round Trips is the sheet to read: BUY fills are matched to SELL fills "
                 "first-in-first-out per (contract, analyst), so each row is one closed lot "
                 "with its own basis, exit and P&L. Partial exits off one entry become "
                 "several rows whose Qty sums back to the entry."),
        ("Note", "Match=OPEN is a buy lot with no sell yet (or sold outside the window); "
                 "Match='SELL W/O BUY' is an exit whose entry predates the window. Neither "
                 "counts toward the FIFO net."),
        ("Note", "P&L is reconstructed from fills and contracts, not positions.realized_pnl, "
                 "which is NULL or 0 on most rows."),
        ("Note", "Lifecycle is one row per ORDER. A round trip appears twice (the BUY and the "
                 "SELL) and both carry the same Position ID and the same position P&L."),
        ("Note", "Orders placed before 2026-08-19 have no stored alert link; their alert "
                 "columns come from a nearest-timestamp match and may be blank."),
    ):
        ws.append([k, v])
    ws["A1"].font = Font(bold=True)
    ws.column_dimensions["A"].width = 30
    ws.column_dimensions["B"].width = 100

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
