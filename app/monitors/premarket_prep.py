"""
premarket_prep.py - Pre-market preparation and warm-up.

Runs before market open to:
1. Pre-fetch options chains for watched symbols
2. Pre-calculate position sizes for expected alerts
3. Alert on high-impact news for held positions
4. Warm up API connections

This reduces latency when the first real alerts come in at market open.
"""

import asyncio
import logging
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

from app.core.config_manager import cfg

_ET_TZ = ZoneInfo("America/New_York")

log = logging.getLogger(__name__)


def _PREMARKET_ENABLED() -> bool:
    return cfg.getboolean("trading", "premarket_prep_enabled", fallback=False)


def _PREMARKET_START_TIME() -> time:
    """Time to start pre-market preparation (ET)."""
    hour = cfg.getint("trading", "premarket_start_hour", fallback=9)
    minute = cfg.getint("trading", "premarket_start_minute", fallback=10)
    return time(hour, minute)


def _PREMARKET_SYMBOLS() -> list[str]:
    """Symbols to pre-fetch chains for."""
    raw = cfg.get("trading", "premarket_symbols", fallback="SPX,SPY,QQQ,IWM")
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


def _get_et_time(dt: datetime) -> time:
    """Convert UTC datetime to ET time, DST-aware."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    et_dt = dt.astimezone(_ET_TZ)
    return time(et_dt.hour, et_dt.minute)


def should_run_premarket() -> bool:
    """Check if it's time to run pre-market preparation."""
    if not _PREMARKET_ENABLED():
        return False
    
    now = datetime.now(timezone.utc)
    et_time = _get_et_time(now)
    target = _PREMARKET_START_TIME()
    
    # Run if within 1 minute window of target time
    target_minutes = target.hour * 60 + target.minute
    current_minutes = et_time.hour * 60 + et_time.minute
    
    return abs(current_minutes - target_minutes) <= 1


async def warmup_api_connections():
    """Warm up the active broker's API connection."""
    try:
        from app.execution.broker_router import fetch_account_balance
        balance = await fetch_account_balance()
        log.info("[PREMARKET] API connection warmed up, balance: %s", balance.get("total_equity", "unknown"))
        return True
    except Exception as e:
        log.warning("[PREMARKET] API warm-up failed: %s", e)
        return False


async def check_held_positions_news():
    """Check for news that might affect held positions."""
    try:
        from app.core.db import SessionLocal
        from app.core.models import Position
        
        db = SessionLocal()
        try:
            held = db.query(Position).filter(
                Position.status.in_(["OPEN", "PARTIAL"])
            ).all()
            
            if held:
                symbols = [p.symbol for p in held if p.symbol]
                log.info("[PREMARKET] %d positions held overnight: %s", len(held), ", ".join(set(symbols)))
                # TODO: Integrate with news API to check for earnings, upgrades, etc.
                # This is a placeholder for future integration
            
            return len(held)
        finally:
            db.close()
    except Exception as e:
        log.debug("[PREMARKET] Failed to check held positions: %s", e)
        return 0


async def calculate_premarket_position_sizes():
    """Pre-calculate position sizes for common scenarios.

    Returns a static-tag → fixed-contract map plus a Kelly cap derived from
    current account equity. The fixed contract counts come from the size_tag
    convention used elsewhere in the bot; Kelly enforces a per-trade max
    independently. Returns {} on any failure (caller must tolerate empty).
    """
    try:
        from app.risk.kelly_sizer import calculate_kelly_contracts
        from app.execution.broker_router import fetch_account_balance

        balance = await fetch_account_balance()
        equity = balance.get("total_equity", 0) or 0

        # Indicative price used only to derive a Kelly cap — real entries
        # recompute Kelly with the actual fill price at order time.
        kelly_cap_indicative = calculate_kelly_contracts(
            price=1.50, account_balance=equity, author=""
        ).get("max_contracts", 0)

        sizes = {}
        for tag, contracts, max_cost_pct in (
            ("XS", 2, 0.05), ("SMALL", 4, 0.10), ("MEDIUM", 8, 0.20),
            ("LARGE", 12, 0.30), ("FULL", 16, 0.40), ("XL", 20, 0.50),
        ):
            sizes[tag] = {
                "contracts": contracts,
                "max_cost": equity * max_cost_pct,
                "kelly_cap": kelly_cap_indicative,
            }

        log.info("[PREMARKET] Pre-calculated position sizes for %d tags (kelly cap=%s)",
                 len(sizes), kelly_cap_indicative)
        return sizes
    except Exception as e:
        log.warning("[PREMARKET] Failed to calculate sizes: %s", e)
        return {}


async def run_premarket_preparation():
    """Run all pre-market preparation tasks."""
    if not _PREMARKET_ENABLED():
        return
    
    if not should_run_premarket():
        log.debug("[PREMARKET] Not within run window, skipping")
        return
    
    log.info("[PREMARKET] Starting pre-market preparation at %s ET", _PREMARKET_START_TIME().strftime("%H:%M"))
    
    # Run all prep tasks concurrently
    results = await asyncio.gather(
        warmup_api_connections(),
        check_held_positions_news(),
        calculate_premarket_position_sizes(),
        return_exceptions=True
    )
    
    # Log results
    api_ok = results[0] if not isinstance(results[0], Exception) else False
    held_count = results[1] if not isinstance(results[1], Exception) else 0
    sizes = results[2] if not isinstance(results[2], Exception) else {}
    
    log.info("[PREMARKET] Complete - API: %s, Held positions: %d, Sizes cached: %d",
             "OK" if api_ok else "FAIL", held_count, len(sizes))


async def premarket_scheduler():
    """Background task to run pre-market prep at scheduled time."""
    if not _PREMARKET_ENABLED():
        log.info("[PREMARKET] Pre-market preparation disabled")
        return
    
    log.info("[PREMARKET] Scheduler started, will run at %s ET", _PREMARKET_START_TIME().strftime("%H:%M"))
    
    # Anchor "today" on ET, not UTC. The trading day is ET; using UTC date
    # would trigger or skip based on whether UTC midnight has rolled, which
    # diverges from market hours.
    from zoneinfo import ZoneInfo as _ZI
    _et = _ZI("America/New_York")

    last_run_date = None
    while True:
        try:
            today = datetime.now(_et).date()

            # Only run once per day (per ET calendar day)
            if today != last_run_date and should_run_premarket():
                await run_premarket_preparation()
                last_run_date = today

            await asyncio.sleep(30)
        except Exception as e:
            log.error("[PREMARKET] Scheduler error: %s", e)
            await asyncio.sleep(60)


def get_premarket_status() -> dict:
    """Get pre-market preparation status for dashboard."""
    return {
        "enabled": _PREMARKET_ENABLED(),
        "start_time": _PREMARKET_START_TIME().strftime("%H:%M"),
        "target_symbols": _PREMARKET_SYMBOLS(),
        "next_run": "9:10 AM ET" if _PREMARKET_ENABLED() else "disabled",
    }
