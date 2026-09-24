"""
discord_slash.py - Discord slash command definitions for the trading bot.

Registers two slash commands accessible from any Discord server the bot is in:
  /status   → Shows bot health, guardrail state, and open positions count
  /positions → Shows all currently open positions with live P&L
  /stats     → Shows today's realized P&L and trade count

Usage:
  Called from discord_listener.py after client.setup_hook() to register
  and sync the command tree with Discord.

Requirements:
  - Bot must have the `applications.commands` scope in your Discord server
  - Re-run with SYNC_COMMANDS=true or call sync_commands() once after deploy
"""

import logging
import os
from datetime import datetime, timezone

import discord
from discord import app_commands

from app.core.db import SessionLocal
from app.core.models import Position, Order
import app.risk.guardrails as guardrails

log = logging.getLogger(__name__)


def _allowed_user_ids() -> set[int]:
    """Comma-separated DISCORD_OWNER_IDS env var. If unset, ALL users in the
    guild can run /status, /positions, /stats — which leaks live P&L, strikes,
    qty, daily loss caps to anyone in the server. Set this env var to lock
    commands to the account owner."""
    raw = os.getenv("DISCORD_OWNER_IDS", "").strip()
    if not raw:
        return set()
    out = set()
    for tok in raw.split(","):
        tok = tok.strip()
        if tok.isdigit():
            out.add(int(tok))
    return out


async def _authorize(interaction: discord.Interaction) -> bool:
    """Return True if the caller may use slash commands. If DISCORD_OWNER_IDS
    is set, only those user IDs pass; otherwise everyone passes (backwards
    compatible — the open-mode warning is logged once at startup)."""
    allowed = _allowed_user_ids()
    if not allowed:
        return True
    if interaction.user and interaction.user.id in allowed:
        return True
    try:
        await interaction.response.send_message("Forbidden.", ephemeral=True)
    except Exception:
        pass
    log.warning(
        "[SLASH] denied %s (id=%s) — not in DISCORD_OWNER_IDS allowlist",
        getattr(interaction.user, "name", "?"),
        getattr(interaction.user, "id", "?"),
    )
    return False


def register_slash_commands(client: discord.Client) -> app_commands.CommandTree:
    """
    Build and return an app_commands.CommandTree with all slash commands.
    Call sync_commands(tree, client) once after on_ready to push to Discord.
    """
    tree = app_commands.CommandTree(client)

    # ── /status ──────────────────────────────────────────────────────────────

    @tree.command(name="status", description="Show bot health, guardrail state, and open positions")
    async def slash_status(interaction: discord.Interaction):
        """Return a concise bot status embed."""
        if not await _authorize(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        try:
            status = guardrails.get_guardrail_status()
            db = SessionLocal()
            try:
                # Trading day = ET, not UTC. After 8 PM ET, UTC midnight has
                # already rolled and a UTC-based count would show 0.
                from zoneinfo import ZoneInfo as _ZI
                _et = _ZI("America/New_York")
                _et_midnight = datetime.now(_et).replace(hour=0, minute=0, second=0, microsecond=0)
                today_start = _et_midnight.astimezone(timezone.utc)
                filled_today = db.query(Order).filter(
                    Order.status == "FILLED",
                    Order.placed_at >= today_start,
                ).count()
                pending_count = db.query(Order).filter(Order.status == "PENDING").count()
            finally:
                db.close()

            market_emoji = "🟢" if status["is_market_open"] else "🔴"
            halt_emoji   = "🚨" if status["trading_halted"] else "✅"

            embed = discord.Embed(
                title="🤖 Trading Bot Status",
                color=discord.Color.red() if status["trading_halted"] else discord.Color.green(),
                timestamp=datetime.now(timezone.utc),
            )
            embed.add_field(
                name="Market",
                value=f"{market_emoji} {'OPEN' if status['is_market_open'] else 'CLOSED'}",
                inline=True,
            )
            embed.add_field(
                name="Trading",
                value=f"{halt_emoji} {'HALTED' if status['trading_halted'] else 'ACTIVE'}",
                inline=True,
            )
            embed.add_field(
                name="Open Positions",
                value=f"{status['open_positions']} / {status['max_open_positions']}",
                inline=True,
            )
            embed.add_field(
                name="Today's Trades",
                value=f"{status['today_trades']} / {status['max_daily_trades']} (filled: {filled_today})",
                inline=True,
            )
            embed.add_field(
                name="Daily P&L",
                value=f"${status['daily_pnl']:+.2f}  (limit: -${status['max_daily_loss']:.0f})",
                inline=True,
            )
            embed.add_field(
                name="Pending Orders",
                value=str(pending_count),
                inline=True,
            )
            embed.set_footer(text="discord-trader bot")
            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as exc:
            log.error("Slash /status error: %s", exc, exc_info=exc)
            await interaction.followup.send(
                "❌ Error fetching status (see server logs).", ephemeral=True,
            )

    # ── /positions ────────────────────────────────────────────────────────────

    @tree.command(name="positions", description="Show all currently open positions with live P&L")
    async def slash_positions(interaction: discord.Interaction):
        """Return an embed listing all open positions."""
        if not await _authorize(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        try:
            db = SessionLocal()
            try:
                open_positions = db.query(Position).filter(
                    Position.status.in_(["OPEN", "PARTIAL"]),
                    Position.remaining > 0,
                ).order_by(Position.open_time.desc()).all()
            finally:
                db.close()

            if not open_positions:
                await interaction.followup.send("📭 No open positions.", ephemeral=True)
                return

            embed = discord.Embed(
                title=f"📊 Open Positions ({len(open_positions)})",
                color=discord.Color.blurple(),
                timestamp=datetime.now(timezone.utc),
            )

            for pos in open_positions[:20]:   # Discord embed field limit = 25
                pnl_pct = pos.pnl_pct()
                pnl_dollar = pos.pnl_dollar()
                pnl_emoji = "🟢" if pnl_pct >= 0 else "🔴"
                entry_date = pos.open_time.strftime("%m/%d %H:%M") if pos.open_time else "?"

                embed.add_field(
                    name=f"{pnl_emoji} {pos.symbol} {pos.strike}{pos.option_type}",
                    value=(
                        f"Qty: {pos.remaining}/{pos.total_contracts} | "
                        f"Entry: ${pos.avg_price:.2f} | "
                        f"P&L: **{pnl_pct:+.1f}%** (${pnl_dollar:+.0f})\n"
                        f"Status: {pos.status} | Opened: {entry_date}"
                    ),
                    inline=False,
                )

            if len(open_positions) > 20:
                embed.set_footer(text=f"Showing 20 of {len(open_positions)} positions")
            else:
                embed.set_footer(text="discord-trader bot")

            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as exc:
            log.error("Slash /positions error: %s", exc, exc_info=exc)
            await interaction.followup.send(
                "❌ Error fetching positions (see server logs).", ephemeral=True,
            )

    # ── /stats ────────────────────────────────────────────────────────────────

    @tree.command(name="stats", description="Show today's realized P&L and trade summary")
    async def slash_stats(interaction: discord.Interaction):
        """Return today's trading summary."""
        if not await _authorize(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        try:
            db = SessionLocal()
            try:
                # ET midnight; see /status.
                from zoneinfo import ZoneInfo as _ZI
                _et = _ZI("America/New_York")
                _et_midnight = datetime.now(_et).replace(hour=0, minute=0, second=0, microsecond=0)
                today_start = _et_midnight.astimezone(timezone.utc)

                closed_today = db.query(Position).filter(
                    Position.status == "CLOSED",
                    Position.close_time >= today_start,
                ).all()

                wins = losses = 0
                realized_pnl = 0.0
                for pos in closed_today:
                    if not pos.avg_price:
                        continue
                    pnl = pos.pnl_dollar()
                    realized_pnl += pnl
                    if pnl > 0:
                        wins += 1
                    else:
                        losses += 1

                filled_today = db.query(Order).filter(
                    Order.status == "FILLED",
                    Order.placed_at >= today_start,
                ).count()

                open_count = db.query(Position).filter(
                    Position.status.in_(["OPEN", "PARTIAL"])
                ).count()
            finally:
                db.close()

            total_closed = wins + losses
            win_rate = (wins / total_closed * 100) if total_closed > 0 else 0
            pnl_emoji = "🟢" if realized_pnl >= 0 else "🔴"

            embed = discord.Embed(
                title=f"{pnl_emoji} Today's Trading Summary",
                color=discord.Color.green() if realized_pnl >= 0 else discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
            embed.add_field(name="Realized P&L", value=f"**${realized_pnl:+.2f}**", inline=True)
            embed.add_field(name="Trades Closed", value=f"{total_closed} ({wins}W / {losses}L)", inline=True)
            embed.add_field(name="Win Rate", value=f"{win_rate:.0f}%", inline=True)
            embed.add_field(name="Orders Filled", value=str(filled_today), inline=True)
            embed.add_field(name="Still Open", value=str(open_count), inline=True)
            embed.set_footer(text="discord-trader bot")

            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as exc:
            log.error("Slash /stats error: %s", exc, exc_info=exc)
            await interaction.followup.send(
                "❌ Error fetching stats (see server logs).", ephemeral=True,
            )

    return tree


async def sync_commands(tree: app_commands.CommandTree, client: discord.Client):
    """
    Sync slash commands to Discord (call once after on_ready).
    Uses global sync — commands available in all guilds within ~1 hour.
    For instant availability in a specific server, pass guild=discord.Object(id=GUILD_ID).
    """
    try:
        synced = await tree.sync()
        log.info("Slash commands synced: %d commands registered globally", len(synced))
        for cmd in synced:
            log.info("  /%s — %s", cmd.name, cmd.description)
    except Exception as exc:
        log.error("Failed to sync slash commands: %s", exc)
