"""
position_monitor.py - Polls Public.com for current option prices every N
seconds and fires auto-exit orders when thresholds are hit. All thresholds
are tunable via [trading] section or per-author [profile:*] override.

Exit cascade (defaults; portal can override per-author):
  PT1     bank pt1_sell of remaining when pnl ≥ pt1_pct
  PT2     bank pt2_sell of remaining when pnl ≥ pt2_pct
  PT3     bank pt3_sell of remaining when pnl ≥ pt3_pct, ARM TPT for runner
  TPT     trailing profit exit — runner sells when price drops
          tpt_trail_pct% from peak (lets winners run past PT3)
  SL      sell all remaining when pnl ≤ sl_pct
  TRAIL_SL  armed once peak ≥ trailing_sl_arm_pct, sells if price drops
            trailing_sl_pct% from peak (replaces fixed SL once armed)
  TIME    sell at configured ET clock time (e.g. 15:45 ET) for 0DTE risk

Floor ratchets (stack with each other; tighten-only):
  breakeven_lock  peak ≥ pt1_pct → SL floor moves to breakeven_lock_floor_pct
  pt2_floor_lock  peak ≥ pt2_pct → SL floor moves to pt1_pct
  pt3_floor_lock  peak ≥ pt3_pct → SL floor moves to pt2_pct

Defensive gates (refuse to fire on unreliable quotes):
  sl_market_hours_only  refuse SL outside RTH (thin pre/post-market quotes)
  wide_spread_skip_sl   defer SL when ask-bid spread > N% of mid
  sl_post_fill_grace    refuse SL within N seconds of position open
                        (anti-whipsaw on entry-tick dip)
"""

import logging
import asyncio
import json
import time
import threading
import re
from datetime import datetime, date, timezone, time as dttime
from decimal import Decimal
from zoneinfo import ZoneInfo

from app.core.db import SessionLocal
from app.core.models import Order, Position
from app.execution.broker_router import get_executor
from app.core.log_config import exit_logger, price_logger
from app.core.config_manager import cfg  # hot-reloadable config singleton
from app.core.profile_resolver import xd  # derived exit defaults (one copy)
import app.analytics.trade_logger as trade_logger
import app.execution.exit_engine_v2 as exit_engine_v2  # HYBRID_V2 engine — only fires when exit_engine_mode=HYBRID_V2

log = logging.getLogger(__name__)

# ── Recent auto-exit tracking (prevents duplicate SELL when alert comes after auto-exit) ───────
# Key: osi_symbol, Value: (exit_type, timestamp, qty)
_recent_auto_exits: dict[str, tuple[str, float, int]] = {}
_recent_auto_exits_lock = threading.Lock()
_RECENT_EXIT_WINDOW_SECONDS = 60  # 1 minute — only race-window dedup (executor checks <15s)

def _clean_stale_auto_exits():
    """Remove stale entries from _recent_auto_exits. Must hold lock when calling."""
    now = time.time()
    cutoff = now - _RECENT_EXIT_WINDOW_SECONDS
    stale = [k for k, v in _recent_auto_exits.items() if v[1] < cutoff]
    for k in stale:
        del _recent_auto_exits[k]

def record_auto_exit(osi_symbol: str, exit_type: str, qty: int):
    """Record that an auto-exit fired for this symbol."""
    with _recent_auto_exits_lock:
        _recent_auto_exits[osi_symbol] = (exit_type, time.time(), qty)
        _clean_stale_auto_exits()

def get_recent_auto_exit(osi_symbol: str) -> tuple[str, float, int] | None:
    """Check if there was a recent auto-exit for this symbol. Returns (exit_type, timestamp, qty) or None."""
    with _recent_auto_exits_lock:
        _clean_stale_auto_exits()
        return _recent_auto_exits.get(osi_symbol)

# ── Live config accessors ─────────────────────────────────────────────────────────────────────────
# These read from the in-memory singleton updated on /api/config POST.

def _MONITOR_ENABLED():     return cfg.getboolean("trading", "position_monitor_enabled", fallback=True)
def _POLL_INTERVAL():       return cfg.getfloat("trading", "poll_interval_seconds", fallback=10.0)
def _BALANCE_BROADCAST_INTERVAL(): return cfg.getfloat("trading", "balance_broadcast_interval_seconds", fallback=5.0)
def _override(pos, attr):
    """Per-position override, or None to fall through to the profile stack.

    The row's sliders write these. NULL means "follow the default and keep
    following it", so a position nobody touched still tracks a later config
    change — only an explicitly set value detaches.
    """
    return getattr(pos, attr, None) if pos is not None else None


def _AUTO_EXIT_ENABLED(pos=None):
    """Master auto-exit switch. Position override → profile → [trading].

    A position whose profile sets `auto_exit_enabled` (e.g. [profile:whale])
    overrides the global value; a per-position override beats both, so one
    trade can be armed or disarmed without touching the book.
    Called with no arg → global only (back-compat for tests/non-position use)."""
    _global = cfg.getboolean("trading", "auto_exit_enabled", fallback=True)
    if pos is None:
        return _global
    ov = _override(pos, "auto_exit_override")
    if ov is not None:
        return bool(ov)
    return _pcfg_bool(pos, "auto_exit_enabled", _global)
def _PT1_ENABLED():         return cfg.getboolean("trading", "pt1_enabled", fallback=xd("pt1_enabled"))
def _PT1_PCT():             return cfg.getfloat("trading", "pt1_pct", fallback=xd("pt1_pct"))
def _PT1_SELL():            return cfg.getfloat("trading", "pt1_sell", fallback=xd("pt1_sell"))
def _PT2_ENABLED():         return cfg.getboolean("trading", "pt2_enabled", fallback=xd("pt2_enabled"))
def _PT2_PCT():             return cfg.getfloat("trading", "pt2_pct", fallback=xd("pt2_pct"))
def _PT2_SELL():            return cfg.getfloat("trading", "pt2_sell", fallback=xd("pt2_sell"))
def _PT3_ENABLED():         return cfg.getboolean("trading", "pt3_enabled", fallback=xd("pt3_enabled"))
def _PT3_PCT():             return cfg.getfloat("trading", "pt3_pct", fallback=xd("pt3_pct"))
def _SL_ENABLED():          return cfg.getboolean("trading", "sl_enabled", fallback=xd("sl_enabled"))
def _SL_PCT():              return cfg.getfloat("trading", "sl_pct", fallback=xd("sl_pct"))
def _TRAILING_SL_ENABLED(): return cfg.getboolean("trading", "trailing_sl_enabled", fallback=xd("trailing_sl_enabled"))
def _TRAILING_SL_PCT():     return cfg.getfloat("trading", "trailing_sl_pct", fallback=xd("trailing_sl_pct"))
def _TRAILING_SL_ARM_PCT(): return cfg.getfloat("trading", "trailing_sl_arm_pct", fallback=xd("trailing_sl_arm_pct"))
def _TPT_ENABLED():         return cfg.getboolean("trading", "tpt_enabled", fallback=xd("tpt_enabled"))
def _PT3_SELL():            return cfg.getfloat("trading", "pt3_sell", fallback=xd("pt3_sell"))
def _TPT_ARM_PCT():         return cfg.getfloat("trading", "tpt_arm_pct", fallback=xd("tpt_arm_pct"))
def _TPT_TRAIL_PCT():       return cfg.getfloat("trading", "tpt_trail_pct", fallback=xd("tpt_trail_pct"))
def _SL_STAGE_ENABLED():    return cfg.getboolean("trading", "sl_stage_enabled", fallback=False)
def _SL_STAGE_FIRST():      return cfg.getfloat("trading", "sl_stage_first_fraction", fallback=0.50)
def _MANUAL_SELL_PRIORITY_S(): return cfg.getint("trading", "manual_sell_priority_seconds", fallback=0)
def _SL_MARKET_HOURS_ONLY(): return cfg.getboolean("trading", "sl_market_hours_only", fallback=True)
def _MARKET_OPEN_HOUR():    return cfg.getint("guardrails", "market_open_hour", fallback=9)
def _MARKET_OPEN_MINUTE():  return cfg.getint("guardrails", "market_open_minute", fallback=30)
def _MARKET_CLOSE_HOUR():   return cfg.getint("guardrails", "market_close_hour", fallback=16)
def _MARKET_CLOSE_MINUTE(): return cfg.getint("guardrails", "market_close_minute", fallback=0)
def _WIDE_SPREAD_PCT():     return cfg.getfloat("trading", "wide_spread_skip_sl_pct", fallback=20.0)
def _SL_POST_FILL_GRACE_S(): return cfg.getfloat("trading", "sl_post_fill_grace_seconds", fallback=30.0)
def _MAX_TICK_JUMP():       return cfg.getfloat("trading", "max_tick_jump_multiple", fallback=20.0)
def _AUTO_EXIT_REPEG_ENABLED(): return cfg.getboolean("trading", "auto_exit_repeg_enabled", fallback=False)
# Measure the stop against the bid rather than the mid. Every quote in this
# module is (bid+ask)/2, but a SELL fills at the bid — so on a $2.00 contract
# with a $0.50 spread a stop that reads "-20%" realises about -32%. OFF keeps
# the legacy mid comparison. Profit targets stay on the mid either way: a PT
# measured on the bid would fire late on exactly the contracts that are hardest
# to sell.
def _SL_USE_BID_ENABLED(): return cfg.getboolean("trading", "sl_use_bid_enabled", fallback=False)
# How many consecutive wide-spread ticks the SL may be deferred before it fires
# anyway. 0 = legacy: defer forever, which removes the stop during exactly the
# volatility that widened the spread. Needs the re-peg ladder to be any use.
def _WIDE_SPREAD_MAX_DEFER_TICKS(): return cfg.getint("trading", "wide_spread_max_defer_ticks", fallback=0)
# Consecutive ticks with no quote for an OPEN position before alarming. A
# contract that goes dark has no stop and no target and says nothing about it.
# Market-hours gated, so overnight quote gaps stay quiet. 0 = off.
def _QUOTE_GAP_ALARM_TICKS(): return cfg.getint("trading", "quote_gap_alarm_ticks", fallback=120)

# Stop-class exits that must actually fill. A profit target deliberately does
# NOT chase: an unfilled PT just means the profit wasn't taken, and the re-peg
# ladder ends at a decisively marketable bid − 20%, which is the wrong price to
# dump a winner at.
_REPEG_EXIT_TRIGGERS = frozenset({"SL", "SL_PARTIAL", "TRAILING_SL", "TPT", "TIME_EXIT"})
# Which position flag each stop-class trigger latches, for the re-arm below.
_TRIGGER_FLAG = {"SL": "sl_triggered", "TRAILING_SL": "sl_triggered",
                 "SL_PARTIAL": "sl_partial_triggered"}
_ORDER_DEAD = ("CANCELLED", "REJECTED", "EXPIRED", "ERROR")

# Per-position counters for the two guards above. Keyed by osi_symbol; both
# reset the moment the condition clears, so nothing accumulates across a
# position's life.
_wide_spread_defers: dict[str, int] = {}
_quote_gaps: dict[str, int] = {}


def _exit_price(current_price: float, spread_pct: float) -> float:
    """What a SELL would actually get, given a mid and the spread around it.

    spread_pct is (ask - bid) / mid x 100, so bid = mid x (1 - spread_pct/200).
    Clamped at 90% of mid: a spread wide enough to imply a lower bid than that
    is a broken quote, not a tradable market, and the wide-spread guard is the
    right thing to handle it — not a stop trigger computed off garbage.
    """
    if spread_pct <= 0:
        return current_price
    return current_price * max(0.10, 1.0 - (spread_pct / 200.0))
def _PRICE_TRACK_ENABLED(): return cfg.getboolean("trading", "price_track_enabled", fallback=True)
def _PRICE_TRACK_INTERVAL(): return cfg.getfloat("trading", "price_track_interval_seconds", fallback=30.0)

# position_id → monotonic seconds of last sample. Bounded by open-position
# count, and _sample_price drops entries as positions close.
_last_sample: dict[int, float] = {}


def _tick_is_sane(pos, current_price: float, prev_price: float | None) -> bool:
    """False when a quote is too absurd to act on.

    A single bad print used to latch into highest_price forever, because the
    high-water update is a bare max() with no bound. Observed on relaybot-vm
    over 90 days:
        LITE260702C00900000  entry $2.85 -> peak $4938.87  (closed $9.70)
        VRT260702C00320000   entry $3.20 -> peak $1544.87  (closed $3.62)
        LITE260702C00950000  entry $4.65 -> peak  $535.56  (closed $5.00)
    A corrupt peak is worse than a corrupt tick: the tick is replaced on the
    next poll, the peak never is. It poisons MFE analysis and would make a
    trailing stop compute its level off a high that never existed.

    Callers drop the whole tick rather than just its effect on the water
    marks — the same number also drives pnl_pct, the PT/SL comparisons and
    the dashboard. Skipping costs one poll interval; acting on it can fire an
    exit at a price that never traded.

    The bound is deliberately loose, and measured against the HIGHER of entry
    and the last accepted tick so a position that has legitimately run up is
    not re-clipped to its entry. Real options do multiply: the largest genuine
    MFE in 90 days was +471% (5.7x), so 20x has wide headroom while still
    catching the 115x-1732x corruption above.

    Symmetric on the downside, because lowest_price latches exactly the same
    way and is the input to any future MAE study. The low bound uses the
    LOWER of entry and last tick, so a position already down 90% is not
    re-clipped to its entry, and it only rejects a print that is below
    1/20th of that — an option genuinely going to zero still prints
    pennies, not fractions of a cent.
    """
    hi_ref = max(pos.avg_price or 0.0, prev_price or 0.0)
    if hi_ref <= 0:
        return True
    mult = _MAX_TICK_JUMP()
    if mult <= 0:
        return True                      # guard disabled

    if getattr(pos, "is_credit", False):
        # A credit spread's winning mark falls toward zero. The long-premium
        # low bound (1/20th of entry) would reject a $2.05 → $0.05 collapse
        # that IS the max-profit outcome. Cap the high side at 1.5× width
        # (or 5× credit if width is missing) so a garbage print still dies.
        width = float(getattr(pos, "spread_width", 0) or 0)
        cap = width * 1.5 if width > 0 else (pos.avg_price or 0.0) * 5.0
        if cap > 0 and current_price > cap:
            log.warning(
                "[BAD_TICK] %s ignored credit-spread quote $%.2f — over cap $%.2f "
                "(width $%.2f, entry $%.2f). Position state left unchanged.",
                getattr(pos, "osi_symbol", "?"), current_price, cap, width,
                pos.avg_price or 0.0,
            )
            return False
        if current_price < 0:
            return False
        return True

    if current_price > hi_ref * mult:
        log.warning(
            "[BAD_TICK] %s ignored quote $%.2f — over %.0fx reference $%.2f "
            "(entry $%.2f, prev $%.2f). Position state left unchanged.",
            getattr(pos, "osi_symbol", "?"), current_price, mult, hi_ref,
            pos.avg_price or 0.0, prev_price or 0.0,
        )
        return False

    lo_candidates = [x for x in (pos.avg_price or 0.0, prev_price or 0.0) if x and x > 0]
    lo_ref = min(lo_candidates) if lo_candidates else 0.0
    if lo_ref > 0 and current_price < lo_ref / mult:
        log.warning(
            "[BAD_TICK] %s ignored quote $%.4f — under 1/%.0f of reference $%.2f "
            "(entry $%.2f, prev $%.2f). Position state left unchanged.",
            getattr(pos, "osi_symbol", "?"), current_price, mult, lo_ref,
            pos.avg_price or 0.0, prev_price or 0.0,
        )
        return False

    return True


def _sample_price(pos, current_price: float) -> None:
    """Append one JSON line per position per price_track_interval_seconds.

    Broker-agnostic on purpose — every broker's monitor tick lands here, so
    Public / IBKR / Tradier / Lime all record identically without touching
    any executor. ON by default; kill with [trading] price_track_enabled=false.

    Writes to logs/prices.jsonl, NOT the database: at 30s a single open
    position is ~780 rows/day, and that write volume belongs nowhere near the
    SQLite the guardrails and order queue share. The file rolls daily and is
    never pruned — see log_config for the disk budget.
    """
    if not _PRICE_TRACK_ENABLED() or not current_price or current_price <= 0:
        return
    pid = getattr(pos, "id", None)
    if pid is None:
        return
    now = time.monotonic()
    last = _last_sample.get(pid)
    interval = _PRICE_TRACK_INTERVAL()
    if last is not None and (now - last) < interval:
        return
    _last_sample[pid] = now
    # Drop bookkeeping for positions that closed, so a long-running process
    # doesn't accumulate one float per position traded, forever.
    if len(_last_sample) > 512:
        for dead in [k for k, v in _last_sample.items() if now - v > 86400]:
            _last_sample.pop(dead, None)
    entry = pos.avg_price or 0.0
    try:
        price_logger.info(json.dumps({
            "ts": datetime.now(ZoneInfo("America/New_York")).isoformat(timespec="seconds"),
            "pid": pid,
            "osi": pos.osi_symbol,
            "author": pos.author or "",
            "entry": round(entry, 4),
            "px": round(float(current_price), 4),
            "pnl_pct": (
                round(pos.pnl_pct(), 2) if callable(getattr(pos, "pnl_pct", None))
                else (round((current_price / entry - 1) * 100, 2) if entry else None)
            ),
            "high": round(pos.highest_price or 0.0, 4),
            "low": round(pos.lowest_price or 0.0, 4),
            "qty": pos.remaining,
            "spread_pct": round(float(getattr(pos, "_last_spread_pct", 0.0) or 0.0), 2),
        }, separators=(",", ":")))
    except Exception:
        # Observability must never take down the money path.
        log.debug("[PRICE_TRACK] sample failed for %s", getattr(pos, "osi_symbol", "?"),
                  exc_info=True)


def _is_within_market_hours() -> bool:
    """Return True if current ET time is within RTH (Mon-Fri 09:30-16:00).
    Used to gate SL fires so pre-market thin-quote spikes don't whipsaw stops.
    """
    now_et = datetime.now(ZoneInfo("America/New_York"))
    if now_et.weekday() >= 5:  # 5=Sat, 6=Sun
        return False
    open_min  = _MARKET_OPEN_HOUR()  * 60 + _MARKET_OPEN_MINUTE()
    close_min = _MARKET_CLOSE_HOUR() * 60 + _MARKET_CLOSE_MINUTE()
    cur_min   = now_et.hour * 60 + now_et.minute
    return open_min <= cur_min < close_min


# ── Time-Based Exit config ───────────────────────────────────────────────────
def _TIME_EXIT_ENABLED():   return cfg.getboolean("trading", "time_exit_enabled", fallback=False)
def _TIME_EXIT_HOUR():      return cfg.getint("trading", "time_exit_hour", fallback=15)
def _TIME_EXIT_MINUTE():    return cfg.getint("trading", "time_exit_minute", fallback=45)
def _TIME_EXIT_SYMBOLS(): return cfg.get("trading", "time_exit_symbols", fallback="").strip().upper()
def _TIME_EXIT_DAYS():      return cfg.get("trading", "time_exit_days", fallback="0,1,2,3,4").strip()  # 0=Monday


# ── Per-author / per-symbol profile resolution ──────────────────────────────
# Moved to app/core/profile_resolver.py so the entry path (public_executor)
# can use the same lookup. Re-exported under the old underscore names to
# keep this file's internal call sites unchanged.

from app.core.profile_resolver import (  # noqa: F401  (re-exported under old underscore names; tests + callers read them off this module)
    pcfg_float as _pcfg_float,
    pcfg_bool as _pcfg_bool,
    pcfg_get as _pcfg_get,
    _PROFILE_TOKENS,
    _INDEX_PROFILE_PREFIXES,
    _profile_section_for,
    _index_profile_section_for,
    _multi_day_profile_section_for,
    _MULTI_DAY_PROFILE_ENABLED,
    _MULTI_DAY_DAYS_THRESHOLD,
)
from app.core.profile_resolver import whale_resting_tp_enabled as _WHALE_RESTING_TP_ENABLED


async def start_position_monitor(ws_manager):
    """Background loop: polls prices and fires auto-exits."""

    if not _MONITOR_ENABLED():
        log.info("Position monitor disabled via config (position_monitor_enabled=false)")
        return

    executor = get_executor(ws_manager)

    # Heartbeat must be called *inside* the loop. The previous wrapper
    # called heartbeat once after start_position_monitor returned — but
    # this is an infinite loop, so the heartbeat never fired and the
    # health monitor showed the component as forever-healthy even after
    # the loop died.
    try:
        from app.monitors.health_monitor import heartbeat as _heartbeat
    except Exception:
        async def _heartbeat(_): return

    # Balance broadcast is decoupled from the price-poll cadence: at sub-second
    # poll intervals the account-balance endpoint would be hit 2x/sec, which is
    # wasteful and risks rate-limiting. Track last broadcast time and only
    # refresh balance every balance_broadcast_interval_seconds.
    _last_balance_at = 0.0

    while True:
        try:
            await _check_all_positions(ws_manager, executor)
            now = time.monotonic()
            if (now - _last_balance_at) >= _BALANCE_BROADCAST_INTERVAL():
                await _broadcast_balance(ws_manager)
                _last_balance_at = now
            await _heartbeat("position_monitor")
        except Exception as e:
            log.error("Position monitor error: %s", e)

        await _wait_next_monitor_tick()


async def _wait_next_monitor_tick():
    """Lime: wait for the next account-feed frame. Everyone else: sleep."""
    try:
        from app.execution.broker_router import active_broker_name
        if active_broker_name() == "lime":
            from app.execution import lime_sdk_bridge as _lime
            if _lime.feed_ok():
                # Lime only sends a frame when something changes, so a quiet
                # contract went up to 30s without a tick and the dashboard badged
                # the mark ⛔ DEAD at 10s (AAOI 2026-09-23). On timeout the next
                # fetch_prices finds the feed price >2s old and quotes over REST.
                # 30 = previous behavior.
                await _lime.wait_book_event(
                    timeout=cfg.getfloat("lime", "feed_wait_max_seconds", fallback=2.0))
                return
    except Exception:
        pass
    await asyncio.sleep(_POLL_INTERVAL())


async def _broadcast_balance(ws_manager):
    """Push the current account balance to all connected clients."""
    try:
        from app.execution.broker_router import fetch_account_balance, active_broker_name
        balance = None
        if active_broker_name() == "lime":
            from app.execution import lime_sdk_bridge as _lime
            balance = _lime.latest_balance()
        if not balance:
            balance = await fetch_account_balance()
        await ws_manager.broadcast({"type": "balance_update", "data": balance})
    except Exception as e:
        log.error("Balance broadcast error: %s", e)


async def _check_all_positions(ws_manager, executor):
    db = SessionLocal()
    try:
        open_positions = db.query(Position).filter(Position.status.in_(["OPEN", "PARTIAL"])).all()
        if not open_positions:
            return

        # Fetch current prices in bulk from the active broker. Combo keys
        # (`short|long`) are not a tradable OSI — expand to legs, quote those,
        # then stitch a net mark so the dashboard P&L for a credit spread
        # actually moves.
        from app.execution.broker_router import fetch_prices
        from app.execution.spread_executor import is_combo, combo_legs, combo_mark
        osi_symbols: list[str] = []
        seen: set[str] = set()
        combos: list[str] = []
        for p in open_positions:
            osi = p.osi_symbol or ""
            if is_combo(osi):
                combos.append(osi)
                for leg in combo_legs(osi):
                    if leg and leg not in seen:
                        seen.add(leg)
                        osi_symbols.append(leg)
            elif osi and osi not in seen:
                seen.add(osi)
                osi_symbols.append(osi)
        prices = await fetch_prices(osi_symbols)   # dict: {osi: float}
        for combo in combos:
            mark = combo_mark(prices, combo)
            if mark is not None:
                prices[combo] = mark
        # Freshness heartbeat for the last_price_poll_age_seconds metric — a
        # stalled monitor otherwise freezes P&L and auto-exits silently.
        try:
            from app.core import metrics as _metrics
            _metrics.mark_price_poll()
        except Exception:
            pass

        # Per-position state to replay after batch commit
        _pos_events: list[tuple] = []  # [(pos, triggered, pnl_pct, prev_price), ...]

        for pos in open_positions:
            # Expiry auto-close runs BEFORE the quote check — expired symbols
            # return no price, so anything gated behind a live quote (the skip
            # below) would never fire and the position would linger forever.
            if _close_if_expired(db, pos):
                continue

            current_price = prices.get(pos.osi_symbol)
            if current_price is None:
                # A contract that stops quoting is a contract with no stop and
                # no target, and until now it said nothing at all — the
                # last_price_poll_age metric only catches a whole-loop stall,
                # not one symbol going dark. Market-hours gated so overnight
                # gaps (every position, every night) stay quiet.
                _gap_cap = _QUOTE_GAP_ALARM_TICKS()
                if _gap_cap > 0 and _is_within_market_hours():
                    _gaps = _quote_gaps.get(pos.osi_symbol, 0) + 1
                    _quote_gaps[pos.osi_symbol] = _gaps
                    if _gaps == _gap_cap:      # == not >=: alarm once per outage
                        log.error("[QUOTE_GAP] %s — no quote for %d consecutive ticks; "
                                  "position is unprotected", pos.osi_symbol, _gaps)
                        exit_logger.error(
                            "QUOTE_GAP | %s | no quote for %d ticks | remaining=%d | "
                            "no stop or target can fire",
                            pos.osi_symbol, _gaps, pos.remaining or 0,
                        )
                        asyncio.create_task(trade_logger.log_critical(
                            title=f"NO QUOTE — {pos.osi_symbol}",
                            message=(
                                f"The broker has returned no price for `{pos.osi_symbol}` "
                                f"for **{_gaps} consecutive polls** during market hours. "
                                f"{pos.remaining or 0} contracts are open and **no stop or "
                                f"profit target can fire** while this lasts."
                            ),
                            severity="warning",
                            footer="position monitor — quote gap",
                        ))
                continue
            _quote_gaps.pop(pos.osi_symbol, None)

            # Per-position try/except: a single position's exit failure
            # (broker error, qty race after a same-tick exit, malformed
            # state) must not break the whole tick — every other position
            # would skip its checks for that cycle.
            try:
                result = await _evaluate_one_position(
                    db, pos, current_price, executor, ws_manager,
                )
            except Exception as exc:
                log.exception(
                    "Position eval failed for %s (id=%s): %s — other positions continue",
                    pos.osi_symbol, pos.id, exc,
                )
                try:
                    db.rollback()
                except Exception:
                    pass
                continue
            if result is None:
                continue
            triggered, pnl_pct, prev_price = result
            _pos_events.append((pos, triggered, pnl_pct, prev_price))

        # Final commit catches non-trigger mutations (highest_price /
        # current_price updates on positions that didn't fire any exit).
        # Per-trigger blocks already commit individually so order/flag state
        # is durable as soon as the broker confirms.
        db.commit()

        # ── Broadcast position updates and emit exit events (after durable commit)
        for pos, pos_triggered, pos_pnl_pct, pos_prev_price in _pos_events:
            current_price = prices.get(pos.osi_symbol)
            if current_price is None:
                continue
            price_moved_pct = abs((current_price - (pos_prev_price or current_price)) / (pos_prev_price or current_price) * 100)
            # Always send a lightweight price_update so dashboard unrealized P&L stays live.
            # Full position_update still throttled to price move >=0.5% or exit to limit payload size.
            _dol = getattr(pos, "pnl_dollar", None)
            pnl_dollar = _dol() if callable(_dol) else (
                (current_price - pos.avg_price) * (pos.remaining or 0) * 100
                if pos.avg_price else 0.0
            )
            await ws_manager.broadcast({"type": "price_update", "data": {
                "id": pos.id,
                "osiSymbol": pos.osi_symbol,
                "currentPrice": current_price,
                "highestPrice": pos.highest_price,
                "pnlPct": round(pos_pnl_pct, 2),
                "pnlDollar": round(pnl_dollar, 2),
            }})
            if price_moved_pct >= 0.5 or pos_triggered:
                await ws_manager.broadcast({"type": "position_update", "data": pos.to_dict()})

            for rule, fired_qty in pos_triggered:
                msg = f"{rule} triggered on {pos.symbol} {pos.strike}{pos.option_type} — P&L {pos_pnl_pct:+.1f}%"
                severity = "danger" if rule in ("SL", "TRAILING_SL") else "success"
                await ws_manager.broadcast({"type": "auto_exit", "data": {
                    "message": msg, "severity": severity,
                    "osi": pos.osi_symbol, "rule": rule, "pnl_pct": round(pos_pnl_pct, 2),
                }})
                asyncio.create_task(trade_logger.log_auto_exit(
                    rule=rule, osi=pos.osi_symbol, symbol=pos.symbol,
                    strike=pos.strike, option_type=pos.option_type,
                    qty=fired_qty, price=current_price, pnl_pct=pos_pnl_pct,
                    avg_price=pos.avg_price,
                ))

    finally:
        db.close()


# OSI: ROOT + YYMMDD + C/P + strike×1000 (8 digits). e.g. TSLA260618C00432500
_EXP_OSI_RE = re.compile(r"^[A-Z]+(\d{2})(\d{2})(\d{2})[CP]\d{8}$")


def _expiry_date(pos):
    """Expiry as a date. Prefer the expiry column; fall back to parsing the
    osi_symbol — legacy/reconciled rows can have a blank expiry column even
    though the osi encodes the date, and those would never auto-close."""
    if pos.expiry:
        try:
            return datetime.strptime(str(pos.expiry)[:10], "%Y-%m-%d").date()
        except Exception:
            pass
    # Combo rows store `short|long` (or `body|lower|upper`) and leave expiry
    # NULL. The date is still in each leg — read the first one.
    osi = (pos.osi_symbol or "").split("|", 1)[0]
    m = _EXP_OSI_RE.match(osi)
    if m:
        try:
            yy, mm, dd = (int(g) for g in m.groups())
            return date(2000 + yy, mm, dd)
        except Exception:
            pass
    return None


# OSI keys already warned about as unsettleable expired credit spreads, so the
# per-tick monitor loop says it once instead of every pass.
_EXPIRED_WARNED: set[str] = set()


def _close_if_expired(db, pos) -> bool:
    """Mark an expired option CLOSED locally. Returns True if it acted.

    Must run independent of quotes — at/after expiry the broker rejects the
    symbol and returns no price, so any close logic gated behind a live quote
    never executes. Fires after RTH on the expiry day itself (>=16:00 ET) or
    any time on a later day. close_time is stamped to the expiry session close
    (16:00 ET of the expiry day), NOT "now" — daily P&L buckets by close_time
    in ET, so a next-day sweep would otherwise attribute the trade to the
    wrong trading day.
    """
    if pos.status == "CLOSED":
        return False
    exp = _expiry_date(pos)
    if exp is None:
        return False
    if getattr(pos, "is_credit", False):
        # A credit spread's expiry outcome is not observable from here. This
        # function books close_price = current_price, which for a combo row
        # never updates (fetch_prices cannot quote a "short|long" key), so the
        # spread would close at its entry credit — exactly $0 P&L whether it
        # expired worthless (+full credit) or through both strikes (-max loss).
        # Guessing either way writes a number that never happened, so leave it
        # OPEN and let the operator reconcile against the SPX settlement print.
        # ponytail: manual reconcile; automate once a settlement-value source
        # exists to compare against the strikes.
        now_et = datetime.now(ZoneInfo("America/New_York"))
        if (exp < now_et.date()) or (exp == now_et.date() and now_et.hour >= 16):
            # Once per row per process. This branch returns False every tick, so
            # an unguarded warning here wrote 19,672 identical lines in the three
            # hours after the 2026-09-15 close and rotated bot.log six times,
            # taking the day's real history with it.
            if pos.osi_symbol not in _EXPIRED_WARNED:
                _EXPIRED_WARNED.add(pos.osi_symbol)
                log.warning(
                    "[EXPIRED] %s is a credit spread that expired %s — NOT auto-closed. "
                    "Settle it by hand against the SPX print; P&L is unknowable here.",
                    pos.osi_symbol, exp,
                )
        return False
    now_et = datetime.now(ZoneInfo("America/New_York"))
    expired = (exp < now_et.date()) or (exp == now_et.date() and now_et.hour >= 16)
    if not expired:
        return False
    log.warning(
        "[EXPIRED] %s expired %s (now=%s ET) — marking CLOSED locally",
        pos.osi_symbol, exp, now_et,
    )
    closed_qty = pos.remaining or pos.total_contracts or 0
    pos.status = "CLOSED"
    pos.remaining = 0
    close_et = datetime.combine(exp, dttime(16, 0), ZoneInfo("America/New_York"))
    pos.close_time = close_et.astimezone(timezone.utc)
    if not pos.close_price:
        pos.close_price = pos.current_price or 0.0

    # Write the realizing leg. Without this the position goes CLOSED with no
    # SELL order, and every consumer that defines a trade as "a FILLED SELL"
    # — /api/calendar, the stats tab, analyst attribution, the EOD summary —
    # is structurally blind to it. Last week that hid a real -$1,309 expiry
    # (SPXW260731P07420000, 13.11 -> 0.02) and made the week read -130.92
    # instead of -1,439.92. The reconciler already logged these as [PHANTOM]
    # ("no broker SELL in last 90d") but only warned.
    #
    # trigger=EXPIRED distinguishes it from a real broker fill, and
    # public_order_id stays NULL because no order was ever sent — an expiry is
    # the absence of a sale, not a sale at zero.
    if closed_qty > 0 and not db.query(Order).filter(
        Order.osi_symbol == pos.osi_symbol,
        Order.side == "SELL",
        Order.status == "FILLED",
        Order.filled_at.isnot(None),
    ).first():
        db.add(Order(
            osi_symbol=pos.osi_symbol,
            side="SELL",
            quantity=closed_qty,
            filled_qty=closed_qty,
            fill_price=pos.close_price or 0.0,
            cost_basis=pos.avg_price,
            limit_price=pos.close_price or 0.0,
            status="FILLED",
            trigger="EXPIRED",
            filled_at=pos.close_time,
            author=getattr(pos, "author", None),
        ))
        log.warning(
            "[EXPIRED] %s — wrote realizing SELL x%d @ $%.2f (basis $%.2f, P&L $%+.2f)",
            pos.osi_symbol, closed_qty, pos.close_price or 0.0, pos.avg_price or 0.0,
            ((pos.close_price or 0.0) - (pos.avg_price or 0.0)) * closed_qty * 100,
        )
    db.commit()
    return True


async def _evaluate_one_position(db, pos, current_price, executor, ws_manager):
    """Evaluate exit rules for a single position. Returns (triggered, pnl_pct,
    prev_price) on success, or None if the position should be skipped (e.g.
    no avg_price). Raises on broker / state errors so the caller can skip
    this position without breaking the rest of the tick."""
    prev_price = pos.current_price or current_price
    if not pos.avg_price or pos.avg_price <= 0:
        pos.current_price = current_price
        return None

    # Absurd-tick guard. A single bad print used to be latched into
    # highest_price forever, because the high-water update is a bare max()
    # with no bound. Observed on relaybot-vm over 90 days:
    #   LITE260702C00900000  entry $2.85 -> peak $4938.87  (closed $9.70)
    #   VRT260702C00320000   entry $3.20 -> peak $1544.87  (closed $3.62)
    #   LITE260702C00950000  entry $4.65 -> peak  $535.56  (closed $5.00)
    # A corrupt peak is worse than a corrupt tick: the tick is replaced 0.5s
    # later, the peak never is. It poisons MFE analysis and would make a
    # trailing stop compute its level off a fictional high.
    #
    # Reject the whole tick rather than just its effect on the water marks —
    # the same bad number also drives pnl_pct, the PT/SL comparisons and the
    # dashboard. Skipping costs one poll interval; acting on it can fire an
    # exit at a price that never existed.
    #
    # Bound is deliberately loose: real options do multiply. The largest
    # genuine MFE in 90 days was +471% (5.7x), so the 20x default has wide
    # headroom while still catching the 115x-1732x corruption above.
    if not _tick_is_sane(pos, current_price, prev_price):
        return None

    pos.current_price = current_price
    # Position.pnl_pct() inverts for is_credit (and measures vs risk, not vs
    # the credit). Dummy test objects without the method keep the long formula.
    _pnl_fn = getattr(pos, "pnl_pct", None)
    pnl_pct = _pnl_fn() if callable(_pnl_fn) else ((current_price / pos.avg_price) - 1) * 100

    # Re-arm a stop whose order died unfilled. Runs before any trigger is
    # evaluated, so a re-armed stop can fire on this very tick. Cheap: the
    # guard returns immediately unless a stop flag is actually set.
    if _AUTO_EXIT_REPEG_ENABLED():
        _rearm_dead_stop(db, pos)

    # Stash latest spread % on the in-memory pos object so the SL gate can
    # read it without re-fetching quotes. Not persisted to DB — recomputed
    # every tick from public_sdk_bridge._LAST_SPREAD_PCT.
    try:
        from app.execution.broker_router import get_last_spread_pct
        pos._last_spread_pct = get_last_spread_pct(pos.osi_symbol)
    except Exception:
        pos._last_spread_pct = 0.0

    # Update high-water mark for legacy compatibility (used by time exit)
    pos.highest_price = max(pos.highest_price or pos.avg_price, current_price)
    # Low-water mark — the counterpart nothing recorded until now. Without it
    # MAE is unknowable after the fact, which is what blocked the 2026-08-18
    # stop-width analysis: 90 days of closed trades and no way to tell how
    # many winners dipped through -20% before they ran.
    pos.lowest_price = min(pos.lowest_price or pos.avg_price, current_price)

    _sample_price(pos, current_price)

    triggered = []

    # Expired-option auto-close moved to _close_if_expired(), called from the
    # main loop BEFORE the quote-None skip — expired symbols return no price,
    # so a check here (behind a live quote) would never run for the very
    # positions it targets. See _close_if_expired for close_time attribution.

    # Manual sell priority: if a DISCORD SELL just filled, skip auto-PT AND
    # auto-SL this tick so the analyst's ladder isn't pre-empted by PT racing
    # or SL firing on top of a manual exit-in-flight (double-sell).
    _manual_priority_s = _MANUAL_SELL_PRIORITY_S()
    _skip_auto_pt = False
    _skip_auto_sl_manual = False
    if _manual_priority_s > 0 and pos.last_manual_sell_at:
        _last = pos.last_manual_sell_at
        if _last.tzinfo is None:
            _last = _last.replace(tzinfo=timezone.utc)
        _age = (datetime.now(timezone.utc) - _last).total_seconds()
        if _age < _manual_priority_s:
            _skip_auto_pt = True
            _skip_auto_sl_manual = True
            log.debug("[AUTO_EXIT] Skipped on %s — manual SELL %.1fs ago (PT+SL gated)", pos.osi_symbol, _age)

    # Post-fill SL grace: refuse SL fires for the first N seconds after a
    # position opens. Anti-whipsaw guard for entry-tick dips that shake out
    # before the trade can establish (2026-05-06 NVDA: bought $1.25, SL'd
    # at $0.97 in 2 minutes, then ran +67% — the SL killed a winner).
    # PTs still fire (an immediate +X% is a real win, take it).
    _post_fill_grace_active = False
    if pos.open_time:
        _ot = pos.open_time
        if _ot.tzinfo is None:
            _ot = _ot.replace(tzinfo=timezone.utc)
        _age_s = (datetime.now(timezone.utc) - _ot).total_seconds()
        _grace_s = _SL_POST_FILL_GRACE_S()
        if _grace_s > 0 and _age_s < _grace_s:
            _post_fill_grace_active = True
            log.debug(
                "[SL_GRACE] %s within post-fill grace (%.0fs of %.0fs) — SL deferred",
                pos.osi_symbol, _age_s, _grace_s,
            )

    # Per-position override: pts_disabled skips ALL profit targets (PT1/PT2/
    # PT3/TPT) for this position. SL still fires. Lets a runner ride past PT3
    # without auto-trimming when user explicitly opts in per-position.
    # Per-author profile may default this on (e.g. Sarang's swing trades).
    _pts_off = bool(getattr(pos, "pts_disabled", False)) or _pcfg_bool(pos, "pts_disabled_default", False)

    # Per-author profile reads (fall back to [trading] when no author match).
    p_pt1_en  = _pcfg_bool(pos, "pt1_enabled", xd("pt1_enabled"))
    # Slider override beats the profile stack for these two — they are the only
    # knobs the position row exposes, and the whole point is that one trade can
    # carry its own numbers without touching the book.
    p_pt1_pct = _override(pos, "pt_pct_override")
    if p_pt1_pct is None:
        p_pt1_pct = _pcfg_float(pos, "pt1_pct", xd("pt1_pct"))
    # Same slider override rule as the target itself: an explicit fraction on
    # the position wins, NULL keeps tracking the config default.
    p_pt1_sl  = _override(pos, "pt_sell_override")
    if p_pt1_sl is None:
        p_pt1_sl = _pcfg_float(pos, "pt1_sell", xd("pt1_sell"))
    p_pt2_en  = _pcfg_bool(pos, "pt2_enabled", xd("pt2_enabled"))
    p_pt2_pct = _pcfg_float(pos, "pt2_pct", xd("pt2_pct"))
    p_pt2_sl  = _pcfg_float(pos, "pt2_sell", xd("pt2_sell"))
    p_pt3_en  = _pcfg_bool(pos, "pt3_enabled", xd("pt3_enabled"))
    p_pt3_pct = _pcfg_float(pos, "pt3_pct", xd("pt3_pct"))
    p_pt3_sl  = _pcfg_float(pos, "pt3_sell", xd("pt3_sell"))
    p_sl_en   = _pcfg_bool(pos, "sl_enabled", xd("sl_enabled"))
    p_sl_pct  = _override(pos, "sl_pct_override")
    if p_sl_pct is None:
        p_sl_pct = _pcfg_float(pos, "sl_pct", xd("sl_pct"))
    p_tsl_en  = _pcfg_bool(pos, "trailing_sl_enabled", xd("trailing_sl_enabled"))
    p_tsl_pct = _pcfg_float(pos, "trailing_sl_pct", xd("trailing_sl_pct"))
    p_tsl_arm = _pcfg_float(pos, "trailing_sl_arm_pct", xd("trailing_sl_arm_pct"))
    p_tpt_en  = _pcfg_bool(pos, "tpt_enabled", xd("tpt_enabled"))
    p_tpt_trl = _pcfg_float(pos, "tpt_trail_pct", xd("tpt_trail_pct"))
    p_max_loss_dollars = _pcfg_float(pos, "max_loss_per_position_dollars", 0.0)
    # Per-position override: breakeven_lock_disabled flag (UI toggle)
    # short-circuits the lock entirely for this position regardless of cfg.
    _be_off = bool(getattr(pos, "breakeven_lock_disabled", False))
    p_be_en      = (not _be_off) and _pcfg_bool(pos, "breakeven_lock_enabled", xd("breakeven_lock_enabled"))
    p_be_arm_pct = _pcfg_float(pos, "breakeven_lock_arm_pct", xd("breakeven_lock_arm_pct"))
    p_be_floor   = _pcfg_float(pos, "breakeven_lock_floor_pct", xd("breakeven_lock_floor_pct"))

    # Anti-whipsaw trailing-SL arm gate. Trailing SL only arms once the peak
    # price has actually run far enough above entry; otherwise a small post-PT1
    # pullback (e.g. AAPL 2026-05-01: peak 1.55 → trail target 1.085 → tagged
    # at 1.42, then ran to 2.66) triggers a near-breakeven exit. With arm gate,
    # trailing SL stays disarmed until peak >= entry × (1 + arm_pct/100); fixed
    # SL keeps protecting downside in the meantime.
    _peak = pos.highest_price or pos.avg_price or 0
    _arm_threshold = (pos.avg_price or 0) * (1 + p_tsl_arm / 100.0)
    _trail_armed = (
        p_tsl_en and pos.pt1_triggered
        and pos.avg_price and _peak >= _arm_threshold
    )

    # Breakeven lock — once peak ≥ entry × (1 + arm%/100), raise SL floor
    # to floor% (default -1%). Tighten-only: max(p_sl_pct, floor) so wider
    # stops like Sarang's -50% become -1% after the position has shown +20%.
    # Prevents winner-into-loser. Multiplicative compare avoids float edge
    # at exactly the arm boundary (mirrors trailing-SL arm gate above).
    # highest_price is reset at PT3, but pnl is already +100% by then so
    # lock stays armed naturally.
    _be_arm_threshold = (pos.avg_price or 0) * (1 + p_be_arm_pct / 100.0)
    _be_locked = p_be_en and pos.avg_price and _peak >= _be_arm_threshold
    if _be_locked:
        p_sl_pct = max(p_sl_pct, p_be_floor)

    # Step-trail at PT2 / PT3 — once the peak crosses each profit-take
    # threshold, ratchet the SL floor up to the previous tier so we don't
    # give back banked-level gains. Stacks with breakeven_lock; tighten-only.
    # 2026-05-06 incident: TSLA crossed +60-70% then faded back below +50%,
    # gave back the PT2-level profit because base SL was still at -20%.
    p_pt2_lock_en = _pcfg_bool(pos, "pt2_floor_lock_enabled", xd("pt2_floor_lock_enabled"))
    p_pt2_floor   = _pcfg_float(pos, "pt2_floor_lock_pct", p_pt1_pct)  # default = pt1_pct
    _pt2_lock_arm = (pos.avg_price or 0) * (1 + p_pt2_pct / 100.0)
    if p_pt2_lock_en and pos.avg_price and _peak >= _pt2_lock_arm:
        p_sl_pct = max(p_sl_pct, p_pt2_floor)

    p_pt3_lock_en = _pcfg_bool(pos, "pt3_floor_lock_enabled", xd("pt3_floor_lock_enabled"))
    p_pt3_floor   = _pcfg_float(pos, "pt3_floor_lock_pct", p_pt2_pct)  # default = pt2_pct
    _pt3_lock_arm = (pos.avg_price or 0) * (1 + p_pt3_pct / 100.0)
    if p_pt3_lock_en and pos.avg_price and _peak >= _pt3_lock_arm:
        p_sl_pct = max(p_sl_pct, p_pt3_floor)

    # Whale resting-TP gate (WHALE-ONLY): when a broker-resting WHALE_TP limit
    # is live, the broker owns the +15% exit — skip the poll-based pt1 so we
    # never double-sell. If that order ever fails/cancels, pt1 resumes as the
    # backstop. No effect on any non-whale position or when the flag is off.
    _whale_tp_active = False
    if _WHALE_RESTING_TP_ENABLED() and (getattr(pos, "strategy_tag", "") or "") == "WHALE":
        _whale_tp_active = db.query(Order.id).filter(
            Order.osi_symbol == pos.osi_symbol,
            Order.side == "SELL",
            Order.status == "PENDING",
            Order.trigger == "WHALE_TP",
        ).first() is not None

    # Set outside the auto-exit branch — the post-partial TPT block further
    # down runs in pure-relay mode (auto_exit_enabled=false) and reads it.
    _tpt_fired = False

    if _AUTO_EXIT_ENABLED(pos) and exit_engine_v2.is_active():
        # ── HYBRID_V2 engine — replaces PT/SL ladder below when active ────
        # State machine + time-confirmed exits. Same _fire_exit plumbing.
        v2_triggers = await _run_hybrid_v2(
            executor, db, pos, current_price, ws_manager,
            spread_pct=float(getattr(pos, "_last_spread_pct", 0.0) or 0.0),
            skip_pt=_skip_auto_pt, skip_sl=_skip_auto_sl_manual,
            grace_active=_post_fill_grace_active,
        )
        triggered.extend(v2_triggers)

    elif _AUTO_EXIT_ENABLED(pos):
        # PT1 / PT2 / PT3 cascade — independent `if` blocks (NOT elif) so a
        # gap-up tick that crosses multiple thresholds in one poll fires all
        # applicable exits this tick, not just PT1. Without this, +120% gap
        # only sells PT1's 50% and waits 5-10s for PT2/PT3 next tick.
        #
        # Each block sets the `triggered` flag AFTER the broker call returns
        # successfully — if `_fire_exit` raises, the flag stays False so the
        # exit retries next tick. (Flag-before-broker would lock the position
        # into a stuck state on broker failure.)
        if not _pts_off and not _skip_auto_pt and not _whale_tp_active and p_pt1_en and not pos.pt1_triggered and pnl_pct >= p_pt1_pct and pos.remaining > 0:
            qty = max(1, int(pos.remaining * p_pt1_sl))
            try:
                _order = await _fire_exit(executor, db, pos, qty, current_price, "PT1", ws_manager)
            except Exception:
                log.exception("[PT1] Broker call failed for %s — will retry next tick", pos.osi_symbol)
            else:
                if _order is not None:
                    pos.pt1_triggered = True
                    db.commit()
                    record_auto_exit(pos.osi_symbol, "PT1", qty)
                    triggered.append(("PT1", qty))

        if not _pts_off and not _skip_auto_pt and p_pt2_en and pos.pt1_triggered and not pos.pt2_triggered and pnl_pct >= p_pt2_pct and pos.remaining > 0:
            qty = max(1, int(pos.remaining * p_pt2_sl))
            try:
                _order = await _fire_exit(executor, db, pos, qty, current_price, "PT2", ws_manager)
            except Exception:
                log.exception("[PT2] Broker call failed for %s — will retry next tick", pos.osi_symbol)
            else:
                if _order is not None:
                    pos.pt2_triggered = True
                    db.commit()
                    record_auto_exit(pos.osi_symbol, "PT2", qty)
                    triggered.append(("PT2", qty))

        if not _pts_off and not _skip_auto_pt and p_pt3_en and pos.pt2_triggered and not pos.pt3_triggered and pnl_pct >= p_pt3_pct and pos.remaining > 0:
            tpt_on = p_tpt_en; pt3s = p_pt3_sl
            if tpt_on and pt3s < 1.0:
                # Use floor (int), not max(1, ...). When remaining=1 and
                # pt3s=0.5, max(1, int(0.5))=1 sells the last contract
                # — defeats the whole point of TPT. With floor: qty=0,
                # skip the partial sell, let TPT trail carry the runner.
                qty = int(pos.remaining * pt3s) if pt3s > 0 else 0
                _placed = True  # whether we should flip flags this tick
                if qty > 0:
                    try:
                        _order = await _fire_exit(executor, db, pos, qty, current_price, "PT3", ws_manager)
                    except Exception:
                        log.exception("[PT3] Broker call failed for %s — will retry next tick", pos.osi_symbol)
                        _placed = False
                    else:
                        # Idempotency-skip (existing PENDING SELL) → don't flip
                        # flags so this tick's PT3 retries next poll.
                        if _order is None:
                            _placed = False
                if _placed:
                    pos.pt3_triggered = True
                    pos.tpt_armed = True
                    pos.highest_price = current_price  # reset peak so TPT trails from PT3 arm
                    db.commit()
                    if qty > 0:
                        record_auto_exit(pos.osi_symbol, "PT3", qty)
                        triggered.append(("PT3", qty))
                        log.info("[TPT] Armed on %s at $%.2f (pnl %.1f%%, sold %d). Remaining: %d",
                                 pos.osi_symbol, current_price, pnl_pct, qty, pos.remaining)
                    else:
                        log.info("[TPT] Armed on %s at $%.2f (pnl %.1f%%, no partial — runner=%d)",
                                 pos.osi_symbol, current_price, pnl_pct, pos.remaining)
            else:
                qty = pos.remaining
                try:
                    _order = await _fire_exit(executor, db, pos, qty, current_price, "PT3", ws_manager)
                except Exception:
                    log.exception("[PT3] Broker call failed for %s — will retry next tick", pos.osi_symbol)
                else:
                    if _order is not None:
                        pos.pt3_triggered = True
                        db.commit()
                        record_auto_exit(pos.osi_symbol, "PT3", qty)
                        triggered.append(("PT3", qty))

        # TPT trailing exit — armed after PT3, exits on reversal from peak.
        # No flag flip needed — tpt_armed stays True so the next tick retries
        # if this fire is skipped or rejected.
        if not _pts_off and p_tpt_en and pos.tpt_armed and pos.remaining > 0:
            tpt_exit_level = pos.highest_price * (1 - p_tpt_trl / 100)
            if current_price <= tpt_exit_level:
                qty = pos.remaining
                exit_pnl = ((current_price / pos.avg_price) - 1) * 100 if pos.avg_price else 0
                log.info("[TPT] Triggered on %s — peak $%.2f, exit $%.2f (%.1f%% from peak), P&L %.1f%%",
                         pos.osi_symbol, pos.highest_price, current_price,
                         (1 - current_price / pos.highest_price) * 100, exit_pnl)
                try:
                    _order = await _fire_exit(executor, db, pos, qty, current_price, "TPT", ws_manager)
                except Exception:
                    log.exception("[TPT] Broker call failed for %s — will retry next tick", pos.osi_symbol)
                else:
                    if _order is not None:
                        record_auto_exit(pos.osi_symbol, "TPT", qty)
                        triggered.append(("TPT", qty))
                        _tpt_fired = True

        # SL gate: pre/post-market quotes are thin and wide. Stop fires on
        # those produce false exits on real positions (e.g. 2026-05-05
        # TSLA260508C00400000 SL'd pre-market at 03:21 ET on a reconciler-
        # bumped avg_price). Honour sl_market_hours_only to refuse SL fires
        # outside RTH. PT cascade still runs — PTs only fire on real
        # gains, no whipsaw risk on stale quotes.
        _sl_hours_ok = (not _SL_MARKET_HOURS_ONLY()) or _is_within_market_hours()

        # Wide-spread guard: thin quotes (e.g. SPX 0DTE close, pre-market)
        # have ask-bid > 20% of mid. SL fires on those use the mid as
        # current_price and exit at the mid, but the actual fill lands at
        # the bid — a structural ~10% loss on top of the SL trigger. Defer
        # SL to next tick when spread is wide.
        _spread_pct = float(getattr(pos, "_last_spread_pct", 0.0) or 0.0)
        _spread_ok = _spread_pct < _WIDE_SPREAD_PCT()
        # Deferring forever is its own failure: wide spreads and fast moves are
        # the same event, so an unbounded defer deletes the stop during the only
        # session where it matters. After wide_spread_max_defer_ticks the exit
        # goes anyway and the re-peg ladder is what makes it fill.
        if not _spread_ok:
            _defers = _wide_spread_defers.get(pos.osi_symbol, 0) + 1
            _wide_spread_defers[pos.osi_symbol] = _defers
            _cap = _WIDE_SPREAD_MAX_DEFER_TICKS()
            if _cap > 0 and _defers >= _cap:
                _spread_ok = True
                log.warning(
                    "[SL_DEFER] %s — spread %.1f%% still over %.1f%% after %d ticks; "
                    "firing the stop anyway",
                    pos.osi_symbol, _spread_pct, _WIDE_SPREAD_PCT(), _defers,
                )
                exit_logger.warning(
                    "SL_DEFER_EXPIRED | %s | spread=%.1f%% | deferred %d ticks | forcing exit",
                    pos.osi_symbol, _spread_pct, _defers,
                )
            else:
                log.info("[SL_DEFER] %s — spread %.1f%% > %.1f%% threshold; SL skipped this tick (%d)",
                         pos.osi_symbol, _spread_pct, _WIDE_SPREAD_PCT(), _defers)
        else:
            _wide_spread_defers.pop(pos.osi_symbol, None)

        # What the stop is measured against. Everything else on this position —
        # PTs, the locks, the dashboard — stays on the mid.
        _sl_price = _exit_price(current_price, _spread_pct) if _SL_USE_BID_ENABLED() else current_price
        _sl_pnl_pct = ((_sl_price / pos.avg_price) - 1) * 100 if pos.avg_price else 0.0

        # Trailing stop loss — only fires when ARMED (peak crossed arm
        # threshold). When disarmed, fixed SL still protects below.
        # Manual-priority and post-fill-grace gates apply here too.
        if (_sl_hours_ok and _spread_ok and _trail_armed and not pos.sl_triggered
            and not pos.tpt_armed and pos.remaining > 0 and not _tpt_fired
            and not _skip_auto_sl_manual and not _post_fill_grace_active):
            trailing_sl_level = pos.highest_price * (1 - p_tsl_pct / 100)
            if _sl_price <= trailing_sl_level:
                qty = pos.remaining
                try:
                    _order = await _fire_exit(executor, db, pos, qty, current_price, "TRAILING_SL", ws_manager)
                except Exception:
                    log.exception("[TRAILING_SL] Broker call failed for %s — will retry next tick", pos.osi_symbol)
                else:
                    if _order is not None:
                        pos.sl_triggered = True
                        db.commit()
                        record_auto_exit(pos.osi_symbol, "TRAILING_SL", qty)
                        triggered.append(("TRAILING_SL", qty))

        # Fixed stop loss — fires when trailing SL is NOT armed. Trailing
        # being enabled in config but un-armed (peak < arm threshold) still
        # falls through to fixed SL so we never run naked between PT1 and the
        # arm threshold.
        # Two trip conditions:
        #   1) pnl_pct <= sl_pct (percent stop)
        #   2) max_loss_per_position_dollars exceeded (absolute $ stop —
        #      defends against wide-%SL profiles like Sarang's -50% on a
        #      large notional position)
        # Both trip conditions read _sl_price, which is the mid unless
        # sl_use_bid_enabled — then it is what a SELL would actually receive.
        _pos_loss_dollars = (pos.avg_price - _sl_price) * (pos.remaining or 0) * 100 if pos.avg_price else 0.0
        _abs_loss_breach = (p_max_loss_dollars > 0 and _pos_loss_dollars >= p_max_loss_dollars)
        _pct_breach = _sl_pnl_pct <= p_sl_pct
        # Skip fixed-SL when TPT already fired this tick OR position is in
        # TPT mode post-PT3 — TPT trail is now the active exit; firing fixed
        # SL on top emits a phantom event and risks stranding sl_triggered=True
        # if the TPT fire is later rejected by the broker.
        if (_sl_hours_ok and _spread_ok and p_sl_en and not pos.sl_triggered
            and not pos.tpt_armed and not _tpt_fired
            and (_pct_breach or _abs_loss_breach) and pos.remaining > 0
            and not _skip_auto_sl_manual and not _post_fill_grace_active):
            if not _trail_armed:
                staged = _SL_STAGE_ENABLED()
                if staged and not pos.sl_partial_triggered and pos.remaining > 1:
                    qty = max(1, int(pos.remaining * _SL_STAGE_FIRST()))
                    try:
                        _order = await _fire_exit(executor, db, pos, qty, current_price, "SL_PARTIAL", ws_manager)
                    except Exception:
                        log.exception("[SL_PARTIAL] Broker call failed for %s — will retry next tick", pos.osi_symbol)
                    else:
                        if _order is not None:
                            pos.sl_partial_triggered = True
                            db.commit()
                            record_auto_exit(pos.osi_symbol, "SL_PARTIAL", qty)
                            triggered.append(("SL_PARTIAL", qty))
                            log.info("[SL_PARTIAL] Staged SL on %s — sold %d, %d remaining (will exit rest next tick if still under %.0f%%)",
                                     pos.osi_symbol, qty, pos.remaining, p_sl_pct)
                else:
                    qty = pos.remaining
                    try:
                        _order = await _fire_exit(executor, db, pos, qty, current_price, "SL", ws_manager)
                    except Exception:
                        log.exception("[SL] Broker call failed for %s — will retry next tick", pos.osi_symbol)
                    else:
                        if _order is not None:
                            pos.sl_triggered = True
                            db.commit()
                            record_auto_exit(pos.osi_symbol, "SL", qty)
                            triggered.append(("SL", qty))

    # ── Post-partial TPT (Option A) — runs even when auto_exit_enabled=false ─
    # Mirrors the TPT trail block above but is gated only on the profile
    # knob tpt_remainder_only, so [profile:spx_index] gets a trailing
    # exit on the analyst's leftover contracts without enabling the full
    # PT/SL/auto-exit ladder. Skipped when the legacy TPT block already
    # fired this tick (avoid double-exit) or when tpt_armed is false
    # (analyst hasn't taken a profitable partial yet → still pure relay).
    #
    # Deliberately OUTSIDE the _AUTO_EXIT_ENABLED gate — it lived inside it
    # until 2026-08-19, which made the whole feature dead in production
    # (auto_exit_enabled=false): public_executor armed tpt_armed on the
    # analyst's partial, and nothing ever fired the trail.
    _tpt_remainder_only = _pcfg_bool(pos, "tpt_remainder_only", False)
    if (
        not _tpt_fired
        and _tpt_remainder_only
        and p_tpt_en
        and pos.tpt_armed
        and pos.remaining > 0
        and not _pts_off
    ):
        tpt_exit_level = (pos.highest_price or pos.avg_price or 0) * (1 - p_tpt_trl / 100)
        if current_price <= tpt_exit_level and current_price > 0:
            qty = pos.remaining
            exit_pnl = ((current_price / pos.avg_price) - 1) * 100 if pos.avg_price else 0
            log.info(
                "[TPT_REMAINDER] Triggered on %s — peak $%.2f, exit $%.2f (%.1f%% off peak), P&L %+.1f%%, qty=%d",
                pos.osi_symbol, pos.highest_price or 0, current_price,
                (1 - current_price / (pos.highest_price or current_price)) * 100, exit_pnl, qty,
            )
            try:
                _order = await _fire_exit(executor, db, pos, qty, current_price, "TPT", ws_manager)
            except Exception:
                log.exception("[TPT_REMAINDER] Broker call failed for %s — will retry next tick", pos.osi_symbol)
            else:
                if _order is not None:
                    record_auto_exit(pos.osi_symbol, "TPT", qty)
                    triggered.append(("TPT", qty))
                    _tpt_fired = True

    # ── Per-position trailing stop (dashboard opt-in) ──────────────────────
    # Deliberately OUTSIDE the _AUTO_EXIT_ENABLED gate above: this is the one
    # automated exit the operator arms by hand, on one specific position,
    # after entry. Pure relay stays the default — nothing here fires unless
    # the button was pressed on this row (trail_enabled defaults False).
    #
    # Also deliberately NOT gated on pos.pt1_triggered, unlike the config
    # trail at _trail_armed above: pressing the button IS the arm signal, so
    # requiring a PT1 auto-trim first would defeat the point.
    #
    # Thresholds come from the same profile-aware knobs as the config trail
    # (trailing_sl_arm_pct / trailing_sl_pct) so per-author profiles apply.
    if (bool(getattr(pos, "trail_enabled", False)) and pos.remaining > 0
            and not pos.sl_triggered and pos.avg_price and current_price > 0):
        _mt_peak = pos.highest_price or pos.avg_price or 0
        _mt_armed = _mt_peak >= pos.avg_price * (1 + p_tsl_arm / 100.0)
        _mt_level = _mt_peak * (1 - p_tsl_pct / 100.0)
        _mt_hours_ok = (not _SL_MARKET_HOURS_ONLY()) or _is_within_market_hours()
        _mt_spread = float(getattr(pos, "_last_spread_pct", 0.0) or 0.0)
        # Don't stack on an exit this tick already produced (TPT/SL/PT3 path
        # when auto_exit is on) — that would double-sell the remainder.
        _mt_already = any(t[0] in ("SL", "SL_PARTIAL", "TPT", "TRAILING_SL", "PT3")
                          for t in triggered)
        if (_mt_armed and current_price <= _mt_level and _mt_hours_ok
                and _mt_spread < _WIDE_SPREAD_PCT() and not _mt_already
                and not _skip_auto_sl_manual and not _post_fill_grace_active):
            qty = pos.remaining
            log.info("[TRAIL_MANUAL] %s — peak $%.2f, exit $%.2f (%.1f%% off peak), "
                     "P&L %+.1f%%, qty=%d (armed by hand)",
                     pos.osi_symbol, _mt_peak, current_price,
                     (1 - current_price / _mt_peak) * 100, pnl_pct, qty)
            try:
                _order = await _fire_exit(executor, db, pos, qty, current_price,
                                          "TRAILING_SL", ws_manager)
            except Exception:
                log.exception("[TRAIL_MANUAL] Broker call failed for %s — will retry next tick",
                              pos.osi_symbol)
            else:
                if _order is not None:
                    pos.sl_triggered = True
                    db.commit()
                    record_auto_exit(pos.osi_symbol, "TRAILING_SL", qty)
                    triggered.append(("TRAILING_SL", qty))

    # ── HYBRID_V2 shadow eval ──────────────────────────────────────────────
    # When [exit_engine_v2] shadow_eval_enabled = true AND mode is still
    # PARALLEL, log what v2 would have done this tick. Lets us compare
    # before flipping the master switch.
    try:
        if not exit_engine_v2.is_active() and exit_engine_v2.shadow_eval_enabled():
            exit_engine_v2.shadow_log(pos, current_price, triggered)
    except Exception:
        log.exception("[V2_SHADOW] failed for %s — continuing", pos.osi_symbol)

    # ── Time-Based Exit (close before market close) ───────────────
    # Gated under _AUTO_EXIT_ENABLED() so the master kill-switch silences
    # ALL automated closes (not just PT/SL/TPT) consistently.
    if _AUTO_EXIT_ENABLED(pos) and _TIME_EXIT_ENABLED() and pos.remaining > 0:
        from app.risk.guardrails import extract_root_symbol
        # DST-aware ET conversion (handles EST/EDT automatically)
        now_et = datetime.now(ZoneInfo("America/New_York"))
        current_day = now_et.weekday()  # 0=Monday
        current_hour = now_et.hour
        current_minute = now_et.minute

        # Parse allowed days
        allowed_days = [int(d.strip()) for d in _TIME_EXIT_DAYS().split(",") if d.strip().isdigit()]
        if current_day in allowed_days:
            # Check if it's time to exit
            exit_hour = _TIME_EXIT_HOUR()
            exit_minute = _TIME_EXIT_MINUTE()
            if current_hour > exit_hour or (current_hour == exit_hour and current_minute >= exit_minute):
                # Check if symbol matches filter (if specified)
                target_symbols = _TIME_EXIT_SYMBOLS()
                root = extract_root_symbol(pos.osi_symbol)
                should_exit = True
                if target_symbols:
                    # Only exit if symbol is in the list
                    should_exit = root and root in target_symbols.split(",")
                if should_exit:
                    qty = pos.remaining
                    log.info("[TIME_EXIT] Firing exit for %s at %d:%02d ET (remaining=%d)",
                             pos.osi_symbol, current_hour, current_minute, qty)
                    try:
                        _order = await _fire_exit(executor, db, pos, qty, current_price, "TIME_EXIT", ws_manager)
                    except Exception:
                        log.exception("[TIME_EXIT] Broker call failed for %s — will retry next tick", pos.osi_symbol)
                    else:
                        if _order is not None:
                            record_auto_exit(pos.osi_symbol, "TIME_EXIT", qty)
                            triggered.append(("TIME_EXIT", qty))

    if pos.remaining <= 0 and pos.status != "CLOSED":
        pos.status = "CLOSED"
        pos.close_time = datetime.now(timezone.utc)
    elif pos.remaining > 0 and pos.remaining < pos.total_contracts:
        pos.status = "PARTIAL"

    return triggered, pnl_pct, prev_price


# Per-position consecutive "exceeds amount" rejection counter. Cleared on
# any successful fire OR on next reconcile pass. Threshold below triggers
# reconcile_once + Discord alarm. 2026-05-06 incident: pos #1 saw 26
# consecutive SL rejections and bot kept retrying without reconciling.
_exceeds_count: dict[int, int] = {}
_EXCEEDS_RECONCILE_THRESHOLD = 2


def _rearm_dead_stop(db, pos) -> None:
    """Clear a stop flag whose order died without filling.

    `sl_triggered` is set the moment `_fire_exit` returns — that is on SUBMIT,
    not on fill. Nothing ever clears it except a fresh BUY, so an SL limit that
    the broker cancels (NBBO reject, max_pending_minutes timeout) leaves the
    position open with its stop permanently disarmed and no line saying so.

    Re-arm only when it is provably safe: the latest stop order for this
    contract is terminal, filled nothing, and no SELL is pending. Under those
    three conditions there is no order that could still take contracts, so
    letting the stop fire again cannot oversell.

    Deliberately does NOT re-arm the PT flags. A PT that never filled is
    forgone profit, not carried risk, and re-arming a partial rung has an
    oversell path this guard does not cover.
    """
    live = {f for t, f in _TRIGGER_FLAG.items()
            if getattr(pos, f, False)} - {None}
    if not live:
        return
    if db.query(Order.id).filter(
        Order.osi_symbol == pos.osi_symbol,
        Order.side == "SELL",
        Order.status == "PENDING",
    ).first():
        return                       # something is still working — leave it alone

    for trigger, flag in _TRIGGER_FLAG.items():
        if not getattr(pos, flag, False):
            continue
        last = (db.query(Order)
                .filter(Order.osi_symbol == pos.osi_symbol,
                        Order.side == "SELL",
                        Order.trigger == trigger)
                .order_by(Order.id.desc()).first())
        if last is None or last.status not in _ORDER_DEAD or (last.filled_qty or 0) > 0:
            continue
        setattr(pos, flag, False)
        log.warning("[REARM] %s %s order #%d died %s with no fill — %s re-armed",
                    pos.osi_symbol, trigger, last.id, last.status, flag)
        exit_logger.warning(
            "REARM | %s | %s order #%d %s, filled=0 | %s cleared — stop live again",
            pos.osi_symbol, trigger, last.id, last.status, flag,
        )


async def _fire_exit(executor, db, pos, qty: int, current_price: float, trigger: str, ws_manager):
    """Place a SELL order and record it in the DB.

    Returns the placed Order, or None if the SELL was skipped (existing
    PENDING SELL idempotency, position already CLOSED, OR broker
    "exceeds amount" auto-suppress). Callers MUST check the return value
    before flipping `*_triggered` flags — otherwise a skipped fire
    permanently locks out the trigger even though no order was placed.
    """
    limit_px = executor._compute_limit(Decimal(str(round(current_price, 2))), "SELL")
    log.info("[%s] Firing exit: %s  x%s @ $%.2f", trigger, pos.osi_symbol, qty, current_price)
    pnl_pct = ((current_price / pos.avg_price) - 1) * 100 if pos.avg_price else 0
    exit_logger.info(
        "TRIGGER | %s | %s | qty=%d | entry=$%.2f → current=$%.2f | pnl=%.1f%% | highest=$%.2f | remaining=%d→%d",
        trigger, pos.osi_symbol, qty, pos.avg_price or 0, current_price,
        pnl_pct, pos.highest_price or 0, pos.remaining, max(0, pos.remaining - qty),
    )
    try:
        result = await executor._place_sell(db, pos, qty, limit_px, trigger)
    except Exception as exc:
        msg = str(exc).lower()
        if "exceeds the amount you have available" in msg or "exceeds the amount" in msg:
            n = _exceeds_count.get(pos.id, 0) + 1
            _exceeds_count[pos.id] = n
            log.warning(
                "[%s] %s broker rejected (consec %d): exceeds-amount → broker has fewer "
                "contracts than DB. Treating as skipped fire (no flag flip).",
                trigger, pos.osi_symbol, n,
            )
            if n >= _EXCEEDS_RECONCILE_THRESHOLD:
                # Trigger reconcile to write-down stale total_contracts and
                # mark the position CLOSED if broker has 0. Throttled by
                # threshold (only fires once per N consecutive rejections).
                try:
                    from app.monitors.reconciler import reconcile_once
                    import asyncio as _asyncio
                    _asyncio.create_task(reconcile_once(ws_manager))
                except Exception as _r:
                    log.error("[%s] reconcile trigger failed: %s", trigger, _r)
                try:
                    import app.analytics.trade_logger as _tl
                    import asyncio as _asyncio
                    _asyncio.create_task(_tl.log_critical(
                        title=f"AUTO-EXIT EXCEEDS AMOUNT — {pos.osi_symbol}",
                        message=(
                            f"Broker rejected `{trigger}` for pos #{pos.id} "
                            f"({pos.osi_symbol}) **{n} times** with 'exceeds amount'. "
                            f"DB shows remaining={pos.remaining}, total_contracts="
                            f"{pos.total_contracts}, but broker has fewer. "
                            f"Reconcile triggered to write-down qty and close if needed."
                        ),
                        severity="warning",
                        footer="Auto-exit exceeds-amount → reconcile",
                    ))
                except Exception:
                    pass
            return None  # signal "skipped" so caller doesn't flip trigger flag
        # Other errors bubble up unchanged
        raise
    # Successful fire → reset counter for this position
    _exceeds_count.pop(pos.id, None)
    # ── Record SL-class exits in the cooldown table so future BUYs on the
    # same root are time-banned. Profitable-trail and PT exits don't trigger
    # cooldown (handled inside record_sl_exit). Failure to record must never
    # block the broker fill — log and move on.
    if trigger in ("SL", "SL_PARTIAL", "SOFT_STOP", "SOFT_STOP_PARTIAL",
                   "PANIC", "TRAILING_SL", "TRAIL_HIT", "TIME_EXIT"):
        try:
            from app.risk.guardrails import record_sl_exit
            record_sl_exit(pos.osi_symbol, trigger)
        except Exception:
            log.exception("[%s] record_sl_exit failed for %s — continuing", trigger, pos.osi_symbol)

    # ── Chase the fill on stop-class exits ────────────────────────────────
    # Without this the limit goes out once at mark × (1 - sell_slippage) and is
    # never re-pegged: on a fast move it is stale within seconds and the stop
    # silently doesn't happen. The DISCORD lane has had this watcher since the
    # 2026-05-05 SPX 7275C incident; auto exits never got one. Same ladder —
    # bid − 1 tick every close_all_repeg_seconds, then a decisively marketable
    # final attempt — carrying this exit's own trigger so the replacement keeps
    # both its log tag and its priority.
    if result is not None and _AUTO_EXIT_REPEG_ENABLED() and trigger in _REPEG_EXIT_TRIGGERS:
        try:
            executor._track_task(executor._repeg_close_all_sell(
                order_db_id=result.id, position_id=pos.id,
                initial_qty=qty, trigger=trigger,
            ))
        except Exception:
            log.exception("[%s] re-peg watcher failed to start for %s — order stands unchased",
                          trigger, pos.osi_symbol)
    return result


async def _run_hybrid_v2(executor, db, pos, current_price, ws_manager,
                         *, spread_pct: float, skip_pt: bool, skip_sl: bool,
                         grace_active: bool) -> list[tuple[str, int]]:
    """Bridge between legacy `_fire_exit` plumbing and the v2 engine.

    Calls `exit_engine_v2.evaluate(pos, current_price)` to get the list of
    (rule, qty) triggers, then fires each via the same `_fire_exit` used
    by PARALLEL — preserving:
      • broker idempotency (existing PENDING SELL skip)
      • exceeds-amount auto-suppress and reconcile trigger
      • DB persistence + WebSocket broadcasts upstream

    Honors the same gates as PARALLEL:
      • manual-priority skip (_skip_auto_pt / _skip_auto_sl_manual)
      • post-fill grace period (only soft-stop deferred; PT/PANIC/EOD still fire)
      • spread guard (skips SOFT_STOP when spread > max_spread_pct_normal)
      • market-hours gate inherited from legacy _SL_MARKET_HOURS_ONLY
    """
    fired: list[tuple[str, int]] = []
    if pos.remaining <= 0:
        return fired

    # Mirror legacy SL hours/spread gates so HYBRID_V2 doesn't fire SL during
    # thin pre-market / wide-spread windows. Panic floor and EOD theta still
    # fire (catastrophic protection trumps spread).
    sl_hours_ok = (not _SL_MARKET_HOURS_ONLY()) or _is_within_market_hours()

    triggers = exit_engine_v2.evaluate(pos, current_price)
    for rule, qty in triggers:
        if qty <= 0 or pos.remaining <= 0:
            continue

        # Per-rule gating
        if rule in ("PT1", "PT2", "PT3"):
            if skip_pt:
                continue
            # Skip if already triggered (defense; engine usually filters)
            if rule == "PT1" and getattr(pos, "pt1_triggered", False): continue
            if rule == "PT2" and getattr(pos, "pt2_triggered", False): continue
            if rule == "PT3" and getattr(pos, "pt3_triggered", False): continue
        elif rule in ("SOFT_STOP", "SOFT_STOP_PARTIAL", "TRAIL_HIT"):
            if skip_sl or grace_active or not sl_hours_ok:
                continue
            if getattr(pos, "sl_triggered", False):
                continue
            # Wide-spread defer for soft stops
            if spread_pct >= cfg.getfloat("exit_engine_v2", "max_spread_pct_normal", fallback=15.0):
                log.info("[V2_DEFER] %s spread %.1f%% — soft stop skipped this tick",
                         pos.osi_symbol, spread_pct)
                continue
        elif rule == "PANIC":
            # Panic floor — only honor extreme spread cap
            if spread_pct >= cfg.getfloat("exit_engine_v2", "max_spread_pct_panic", fallback=30.0):
                log.warning("[V2_DEFER] %s spread %.1f%% > panic_max — PANIC deferred",
                            pos.osi_symbol, spread_pct)
                continue
        # EOD_THETA always fires when triggered.

        try:
            qty = min(qty, pos.remaining)
            order = await _fire_exit(executor, db, pos, qty, current_price, rule, ws_manager)
        except Exception:
            log.exception("[%s/v2] Broker call failed for %s — will retry next tick", rule, pos.osi_symbol)
            continue
        if order is None:
            continue

        # Flip flags so subsequent ticks don't re-fire the same rung.
        if rule == "PT1": pos.pt1_triggered = True
        elif rule == "PT2": pos.pt2_triggered = True
        elif rule == "PT3":
            pos.pt3_triggered = True
            pos.tpt_armed = True
            pos.highest_price = current_price
        elif rule in ("SOFT_STOP", "PANIC", "TRAIL_HIT", "EOD_THETA",
                      "ANALYST_GREEN", "ANALYST_AI_CUT", "ANALYST_AI_REDUCE"):
            pos.sl_triggered = True
        elif rule == "SOFT_STOP_PARTIAL":
            pos.sl_partial_triggered = True

        db.commit()
        record_auto_exit(pos.osi_symbol, rule, qty)
        fired.append((rule, qty))

    return fired
