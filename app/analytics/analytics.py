"""
analytics.py - Strategy performance analytics and reporting.

Provides:
- P&L tracking by strategy tag
- Win rate analysis
- Performance by time of day, symbol, size
- Strategy comparison dashboard data
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from app.core.db import SessionLocal
from app.core.models import Position
from app.core.config_manager import cfg

log = logging.getLogger(__name__)


def _ANALYTICS_ENABLED() -> bool:
    return cfg.getboolean("trading", "analytics_enabled", fallback=False)


def get_strategy_performance(days: int = 30) -> dict:
    """
    Get P&L performance breakdown by strategy tag.
    
    Returns:
        Dict with strategy stats including win rate, avg P&L, etc.
    """
    if not _ANALYTICS_ENABLED():
        return {"enabled": False, "message": "Analytics disabled"}
    
    db = SessionLocal()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        
        # Get all closed positions with strategy tags
        positions = db.query(Position).filter(
            Position.status == "CLOSED",
            Position.close_time >= cutoff,
            Position.strategy_tag.isnot(None)
        ).all()
        
        # Aggregate by strategy
        stats: dict[str, dict] = {}
        for pos in positions:
            tag = pos.strategy_tag or "untagged"
            
            if tag not in stats:
                stats[tag] = {
                    "count": 0,
                    "wins": 0,
                    "losses": 0,
                    "total_pnl": 0.0,
                    "avg_pnl": 0.0,
                    "max_gain": 0.0,
                    "max_loss": 0.0,
                }
            
            pnl = pos.realized_pnl or pos.pnl_dollar()
            stats[tag]["count"] += 1
            stats[tag]["total_pnl"] += pnl
            
            if pnl > 0:
                stats[tag]["wins"] += 1
                stats[tag]["max_gain"] = max(stats[tag]["max_gain"], pnl)
            else:
                stats[tag]["losses"] += 1
                stats[tag]["max_loss"] = min(stats[tag]["max_loss"], pnl)
        
        # Calculate averages and win rates
        for tag in stats:
            count = stats[tag]["count"]
            if count > 0:
                stats[tag]["avg_pnl"] = stats[tag]["total_pnl"] / count
                stats[tag]["win_rate"] = (stats[tag]["wins"] / count) * 100
        
        return {
            "enabled": True,
            "period_days": days,
            "total_positions": len(positions),
            "strategies": stats,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        
    finally:
        db.close()


def get_symbol_performance(days: int = 30) -> dict:
    """Get P&L performance breakdown by symbol."""
    if not _ANALYTICS_ENABLED():
        return {"enabled": False}
    
    db = SessionLocal()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        
        positions = db.query(Position).filter(
            Position.status == "CLOSED",
            Position.close_time >= cutoff
        ).all()
        
        stats: dict[str, dict] = {}
        for pos in positions:
            sym = pos.symbol or "unknown"
            
            if sym not in stats:
                stats[sym] = {"count": 0, "wins": 0, "total_pnl": 0.0}
            
            pnl = pos.realized_pnl or pos.pnl_dollar()
            stats[sym]["count"] += 1
            stats[sym]["total_pnl"] += pnl
            if pnl > 0:
                stats[sym]["wins"] += 1
        
        # Sort by total P&L
        sorted_stats = dict(sorted(
            stats.items(),
            key=lambda x: x[1]["total_pnl"],
            reverse=True
        ))
        
        return {
            "enabled": True,
            "period_days": days,
            "symbols": sorted_stats,
        }
        
    finally:
        db.close()


def get_hourly_performance(days: int = 30) -> dict:
    """Get P&L performance breakdown by hour of day (ET)."""
    if not _ANALYTICS_ENABLED():
        return {"enabled": False}
    
    db = SessionLocal()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        
        positions = db.query(Position).filter(
            Position.status == "CLOSED",
            Position.close_time >= cutoff
        ).all()
        
        # Hour buckets (9-16 = market hours)
        hourly: dict[int, dict] = {h: {"count": 0, "total_pnl": 0.0} for h in range(9, 17)}
        
        from zoneinfo import ZoneInfo
        _ET = ZoneInfo("America/New_York")
        for pos in positions:
            if not pos.close_time:
                continue
            # Convert UTC to ET — DST-aware so EST/EDT transitions don't
            # shift trades into the wrong hourly bucket.
            close_utc = pos.close_time
            if close_utc.tzinfo is None:
                close_utc = close_utc.replace(tzinfo=timezone.utc)
            et_hour = close_utc.astimezone(_ET).hour
            if 9 <= et_hour <= 16:
                pnl = pos.realized_pnl or pos.pnl_dollar()
                hourly[et_hour]["count"] += 1
                hourly[et_hour]["total_pnl"] += pnl
        
        return {
            "enabled": True,
            "period_days": days,
            "hourly": hourly,
        }
        
    finally:
        db.close()


def assign_strategy_tag(alert_text: str, author: str = "") -> Optional[str]:
    """
    Auto-assign strategy tag based on alert text patterns.
    Returns None if no pattern matches.
    """
    text_lower = (alert_text or "").lower()

    # Pattern matching for common strategies. Word-boundary match required —
    # short keywords like "er" otherwise hit "alert", "pattern", "performance"
    # and tag everything as earnings.
    patterns = {
        "momentum": ["momentum", "breakout", "above", "resistance", "break"],
        "reversal": ["reversal", "support", "bounce", "oversold", "dip"],
        "earnings": ["earnings", "er", "earnings play", "post-earnings"],
        "gap": ["gap", "gap fill", "gap up", "gap down"],
        "technical": ["rsi", "macd", "ema", "sma", "technical"],
        "news": ["news", "catalyst", "announcement", "fda", "approval"],
    }

    import re
    for strategy, keywords in patterns.items():
        for kw in keywords:
            if re.search(rf"\b{re.escape(kw)}\b", text_lower):
                return strategy
    
    # Default based on author if no pattern matches
    if author:
        return f"author_{author.lower().replace(' ', '_')}"
    
    return None


def get_analytics_summary() -> dict:
    """Get high-level analytics summary for dashboard."""
    if not _ANALYTICS_ENABLED():
        return {"enabled": False}
    
    strategy_perf = get_strategy_performance(days=30)

    # Calculate overall stats
    total_pnl = 0.0
    total_trades = 0
    total_wins = 0
    
    for stats in strategy_perf.get("strategies", {}).values():
        total_pnl += stats.get("total_pnl", 0)
        total_trades += stats.get("count", 0)
        total_wins += stats.get("wins", 0)
    
    win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0
    
    return {
        "enabled": True,
        "total_pnl_30d": round(total_pnl, 2),
        "total_trades_30d": total_trades,
        "win_rate_30d": round(win_rate, 1),
        "top_strategy": max(
            strategy_perf.get("strategies", {}).items(),
            key=lambda x: x[1].get("total_pnl", 0),
            default=("none", {})
        )[0],
        "strategies": list(strategy_perf.get("strategies", {}).keys()),
    }
