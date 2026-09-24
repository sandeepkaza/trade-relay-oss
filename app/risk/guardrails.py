"""
guardrails.py - Safety checks that run BEFORE any order is placed.

Prevents:
  1. Duplicate alerts (same OSI+action within 60s)
  2. Trading outside market hours (9:30 AM - 4:00 PM ET)
  3. Exceeding max daily loss
  4. Too many open positions
  5. Too many trades per day
  6. Per-trade max cost (prevents 1 trade from blowing the account)
  7. Rapid-fire orders (cooldown between trades)
  8. Cross-channel duplicates (same OSI+action+price from different channels)

All limits are configurable in config.ini under [guardrails].
"""

import hashlib
import logging
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional
from zoneinfo import ZoneInfo

from app.core.db import SessionLocal
from app.core.models import Alert, Order, Position
from app.core.config_manager import cfg  # hot-reloadable config singleton

log = logging.getLogger(__name__)

# ── Live config accessors (read on every check — no cached module-level vars) ─
# These properties read from the in-memory singleton which is updated by
# config_manager.cfg.reload() whenever /api/config POST is called.

def _DEDUP_WINDOW():
    return cfg.getint("guardrails", "dedup_window_seconds", fallback=60)

def _MARKET_HOURS_ONLY():
    return cfg.getboolean("guardrails", "market_hours_only", fallback=True)

def _MARKET_OPEN_HOUR():
    return cfg.getint("guardrails", "market_open_hour", fallback=9)

def _MARKET_OPEN_MIN():
    return cfg.getint("guardrails", "market_open_minute", fallback=30)

def _MARKET_CLOSE_HOUR():
    return cfg.getint("guardrails", "market_close_hour", fallback=16)

def _MARKET_CLOSE_MIN():
    return cfg.getint("guardrails", "market_close_minute", fallback=0)

def _MAX_DAILY_LOSS():
    return cfg.getfloat("guardrails", "max_daily_loss", fallback=200.0)

def _MAX_OPEN_POSITIONS():
    return cfg.getint("guardrails", "max_open_positions", fallback=5)

def _MAX_DAILY_TRADES():
    return cfg.getint("guardrails", "max_daily_trades", fallback=20)

def _MAX_TRADE_COST():
    return cfg.getfloat("guardrails", "max_trade_cost", fallback=10000.0)

def _TRADE_COOLDOWN():
    return cfg.getint("guardrails", "trade_cooldown_seconds", fallback=10)

def _CROSS_CHANNEL_DEDUP_WINDOW():
    return cfg.getint("guardrails", "cross_channel_dedup_seconds", fallback=120)

def _SCALE_IN_ENABLED():
    return cfg.getboolean("guardrails", "scale_in_enabled", fallback=True)

def _GUARDRAILS_ENABLED():
    return cfg.getboolean("guardrails", "guardrails_enabled", fallback=True)

def _SCALE_IN_MIN_PROFIT_PCT():
    return cfg.getfloat("guardrails", "scale_in_min_profit_pct", fallback=20.0)

def _BLOCKED_SYMBOLS():
    """Return set of blocked symbols that should not be traded."""
    raw = cfg.get("guardrails", "blocked_symbols", fallback="")
    if not raw:
        return set()
    # Support comma-separated or newline-separated, case-insensitive
    symbols = set()
    for sep in [",", "\n", ";"]:
        if sep in raw:
            for s in raw.split(sep):
                s = s.strip().upper()
                if s:
                    symbols.add(s)
            break
    else:
        # Single symbol, no separator
        s = raw.strip().upper()
        if s:
            symbols.add(s)
    return symbols


def _SYMBOL_MAX_CONTRACTS() -> dict[str, int]:
    """
    Return dict of symbol -> max contracts allowed.
    Format in config: SPX:1,SPY:2,TSLA:5 (symbol:count pairs)
    """
    raw = cfg.get("guardrails", "symbol_max_contracts", fallback="")
    if not raw:
        return {}
    result = {}
    # Split by comma or newline
    for sep in [",", "\n", ";"]:
        if sep in raw:
            items = raw.split(sep)
            break
    else:
        items = [raw]
    for item in items:
        item = item.strip()
        if not item:
            continue
        # Parse "SPX:1" or "SPX=1" format
        if ":" in item:
            symbol, count_str = item.split(":", 1)
        elif "=" in item:
            symbol, count_str = item.split("=", 1)
        else:
            # Just a symbol name - default to 1
            symbol = item
            count_str = "1"
        symbol = symbol.strip().upper()
        try:
            count = int(count_str.strip())
            if count > 0 and symbol:
                result[symbol] = count
        except ValueError:
            continue
    return result


# Common index symbols (options on indexes, not ETFs)
_INDEX_SYMBOLS: frozenset[str] = frozenset({
    "SPX",   # S&P 500 Index
    "RUT",   # Russell 2000 Index
    "NDX",   # Nasdaq-100 Index
    "DJX",   # Dow Jones Industrial Average
    "VIX",   # Volatility Index
    "XSP",   # Mini-SPX (S&P 500)
    "XEO",   # S&P 100 Index (European exercise)
    "OEX",   # S&P 100 Index (American exercise)
    "MRUT",  # Micro Russell 2000
    "MNX",   # Mini Nasdaq-100
    "QQQ",   # Nasdaq-100 ETF (commonly treated as index)
})


# OSI weekly/EOM variants share the underlying — collapse so caps,
# correlation, and index detection all key on the underlying root.
_INDEX_WEEKLY_MAP: dict[str, str] = {
    "SPXW": "SPX",   # SPX weekly + EOM
    "NDXP": "NDX",   # NDX weekly
    "RUTW": "RUT",   # RUT weekly
}


def extract_root_symbol(osi_symbol: str) -> str | None:
    """Extract root symbol from OSI format (e.g., 'SPX' from 'SPX250425C06010000').
    Returns uppercase root symbol or None if extraction fails.

    Index weekly variants (SPXW/NDXP/RUTW) are collapsed to their underlying
    (SPX/NDX/RUT) so symbol_max_contracts and index_max_contracts caps fire
    regardless of which OSI prefix the broker routes through."""
    if not osi_symbol:
        return None
    # Strip ALL whitespace — OSI feeds pad root to 6 chars (e.g. "SPX   250425C06010000")
    clean = ''.join(osi_symbol.split())
    # OSI format: <root><YYMMDD><C/P><strike> where root is 1-6 chars
    # Find where the date part starts (2 digits for year)
    raw_root = None
    for i in range(min(6, len(clean))):
        if clean[i:i+2].isdigit():
            raw_root = clean[:i].upper()
            break
    if raw_root is None:
        raw_root = clean.upper()
    return _INDEX_WEEKLY_MAP.get(raw_root, raw_root)


def _is_index_symbol(osi_symbol: str) -> bool:
    """Check if the OSI symbol is an index (not an ETF/stock)."""
    root_symbol = extract_root_symbol(osi_symbol)
    if not root_symbol:
        return False
    return root_symbol in _INDEX_SYMBOLS


def _INDEX_MAX_CONTRACTS() -> int | None:
    """
    Return max contracts allowed ONLY for index symbols (SPX, QQQ, etc).
    Returns None if not configured (no limit).
    """
    raw = cfg.get("guardrails", "index_max_contracts", fallback="").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None

# ── In-memory state ──────────────────────────────────────────────────────────

# Track recent alerts: key = (osi_symbol, action), value = timestamp
_recent_alerts: dict[tuple[str, str], float] = {}

# Track recent content hashes: key = content_hash, value = timestamp
_recent_content_hashes: dict[str, float] = {}

# Track last trade time per symbol
_last_trade_time: dict[str, float] = {}

def _clean_stale_hashes(cutoff: float):
    """Remove stale entries from _recent_content_hashes."""
    stale = [k for k, v in _recent_content_hashes.items() if v < cutoff]
    for k in stale:
        del _recent_content_hashes[k]


# ── Correlation Guardrail ────────────────────────────────────────────────────
# Define correlated asset pairs (if you have one, you can't add the other)
_CORRELATED_PAIRS: dict[str, str] = {
    "SPX": "SPY",   # S&P 500 Index ↔ S&P 500 ETF
    "SPY": "SPX",
    "NDX": "QQQ",   # Nasdaq-100 Index ↔ Nasdaq-100 ETF
    "QQQ": "NDX",
    "RUT": "IWM",   # Russell 2000 Index ↔ Russell 2000 ETF
    "IWM": "RUT",
}


def _ANALYST_WHITELIST() -> set[str]:
    raw = cfg.get("trading", "analyst_whitelist", fallback="") or ""
    return {t.strip().lower() for t in raw.split(",") if t.strip()}


def _ANALYST_BLACKLIST() -> set[str]:
    raw = cfg.get("trading", "analyst_blacklist", fallback="") or ""
    return {t.strip().lower() for t in raw.split(",") if t.strip()}


def _ANALYST_BLACKLIST_BLOCKS_EXITS() -> bool:
    # When true (default), a blacklisted (untrusted) author is blocked from
    # SELL / close_all too — not just BUY. 2026-06-11: BUY-blacklisted 'TC'
    # posted "everything selling now"; the AI parser emitted a blank-symbol
    # close_all and the executor flattened the whole book because the blacklist
    # was gated to BUY. Set false to revert to BUY-only blacklist.
    return cfg.getboolean("trading", "analyst_blacklist_blocks_exits", fallback=True)


def _BLACKLIST_ALLOWS_OWNER_EXIT() -> bool:
    # Set false to restore the pre-2026-09-17 behaviour: a blacklisted author's
    # SELL is dropped even for a position they themselves hold.
    return cfg.getboolean("trading", "blacklist_allows_owner_exit", fallback=True)


def blacklisted_sell_may_exit(action: str, author: str, osi_symbol: str) -> bool:
    """True when a blacklisted author's SELL must be let through anyway.

    The blacklist blocks exits on purpose (2026-06-11), but it assumes a
    blacklisted author never has a position to exit. The dashboard's manual
    resubmit breaks that assumption: an operator override opens a position
    STAMPED with the blacklisted author, and from then on their own exits are
    dropped at ingest while `position_owner_scope=author` refuses everyone
    else's — the position becomes unexitable by any automated path and only
    the dashboard's SELL button can close it.

    2026-09-17 SPCX 9/18 160C: resubmitted for 'spacemonkey' at 10:16 ET, his
    own "ALL OUT" dropped at 10:23, closed by hand at 10:24 for -30.2%.

    Narrow on purpose: only a SELL, only one that NAMES a contract, and only
    when that author actually holds it. A blank-symbol close_all has no
    osi_symbol, so the nuclear button stays default-deny and the incident this
    blacklist was built for cannot come back.
    """
    if action != "SELL" or not osi_symbol or not _BLACKLIST_ALLOWS_OWNER_EXIT():
        return False
    db = SessionLocal()
    try:
        rows = db.query(Position).filter(
            Position.osi_symbol == osi_symbol,
            Position.status.in_(["OPEN", "PARTIAL"]),
            Position.remaining > 0,
        ).all()
    except Exception:
        log.exception("[BLACKLIST] owner-exit lookup failed for %s", osi_symbol)
        return False
    finally:
        db.close()
    # Same ownership rule as the executor's _pos_owned_by: an unowned row or an
    # un-attributed alert matches, so nothing gets stranded by missing author.
    a = (author or "").strip().lower()
    return any(not (p.author or "").strip() or not a or (p.author or "").strip().lower() == a
               for p in rows)


def _CLOSE_ALL_AUTHOR_WHITELIST() -> set[str]:
    # Allowlist for the nuclear button: a blank-symbol close_all exits EVERY
    # open position. When non-empty, ONLY authors matching a token may fire it
    # — default-deny for everyone else, regardless of blacklist. Empty (default)
    # = no extra gate beyond the blacklist above. Set to your own handle so a
    # rogue/garbled alert from any analyst can never flatten the book.
    raw = cfg.get("trading", "close_all_author_whitelist", fallback="") or ""
    return {t.strip().lower() for t in raw.split(",") if t.strip()}


def _SYMBOL_ANALYST_WHITELIST() -> dict[str, set[str]]:
    """Per-root-symbol analyst whitelist.

    Format in [trading].symbol_analyst_whitelist:
      SPX:spacemonkey:sarang, NDX:foo:bar

    Each comma-separated entry is `ROOT:tok1:tok2:...`. When the alert's root
    matches a configured entry, the author MUST contain one of the listed
    tokens (case-insensitive substring) or the BUY is rejected.
    """
    raw = cfg.get("trading", "symbol_analyst_whitelist", fallback="") or ""
    result: dict[str, set[str]] = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry or ":" not in entry:
            continue
        parts = [p.strip() for p in entry.split(":") if p.strip()]
        if len(parts) < 2:
            continue
        root = parts[0].upper()
        toks = {p.lower() for p in parts[1:]}
        if root and toks:
            result[root] = result.get(root, set()) | toks
    return result


def _ROLLUP_BLOCK_ENABLED() -> bool:
    return cfg.getboolean("trading", "rollup_block_enabled", fallback=False)


def _ROLLUP_WHITELIST() -> set[str]:
    raw = cfg.get("trading", "rollup_whitelist", fallback="") or ""
    return {t.strip().lower() for t in raw.split(",") if t.strip()}


def check_rollup_block(author: str, size_tag: str) -> tuple[bool, str]:
    """Block BUYs tagged [ROLL UP] / [ROLL DOWN] / LOTTO unless author is whitelisted.

    Rolls are follow-on adjustments to an existing position the bot didn't
    enter — mirroring them blind chases stale strikes. LOTTO is a low-conviction
    flyer the analyst themselves flags as throwaway; it's grouped under the same
    toggle so both are gated together. When the toggle is on, only authors
    matching a `rollup_whitelist` token (substring, case-insensitive) pass.
    """
    if not _ROLLUP_BLOCK_ENABLED():
        return True, ""
    tag = (size_tag or "").upper()
    if tag not in ("ROLL UP", "ROLL DOWN", "LOTTO"):
        return True, ""
    wl = _ROLLUP_WHITELIST()
    a = (author or "").lower()
    if wl and any(tok in a for tok in wl):
        return True, ""
    return False, (f"ROLLUP_BLOCK: skipped a {tag} alert from {author} — roll-ups/lottos are follow-on "
                   f"adjustments to positions we never entered, so mirroring them blind chases stale strikes. "
                   f"Allow an analyst via rollup_whitelist (now: {sorted(wl) or 'empty'}).")


def check_analyst_filter(author: str, osi_symbol: str = "") -> tuple[bool, str]:
    """Apply analyst whitelist / blacklist tokens.

    Returns (allowed, reason). Whitelist wins when both populated:
      - whitelist non-empty: author must contain at least one whitelist token
      - whitelist empty + blacklist non-empty: author must contain none
      - both empty: allow all

    Per-symbol whitelist (symbol_analyst_whitelist) is an additional gate
    applied on top of the global rules: when the alert's root has a
    configured entry, author must match one of those tokens.

    Match is case-insensitive substring on the author string.
    """
    a = (author or "").lower()
    wl = _ANALYST_WHITELIST()
    bl = _ANALYST_BLACKLIST()

    # Global whitelist / blacklist
    if wl:
        if not any(tok in a for tok in wl):
            return False, (f"ANALYST_WHITELIST: '{author}' is not one of your allowed analysts "
                   f"({', '.join(sorted(wl))}) — BUY not taken. Edit analyst_whitelist to change who can trade.")
    elif bl:
        if any(tok in a for tok in bl):
            return False, (f"ANALYST_BLACKLIST: '{author}' is on your blocked list — BUY not taken. "
                   f"Remove them from analyst_blacklist to trade their alerts again.")

    # Per-symbol whitelist gate
    sym_wl = _SYMBOL_ANALYST_WHITELIST()
    if sym_wl and osi_symbol:
        root = extract_root_symbol(osi_symbol)
        if root and root in sym_wl:
            allowed = sym_wl[root]
            if not any(tok in a for tok in allowed):
                return False, (f"SYMBOL_ANALYST_WHITELIST: you only take {root} trades from "
                               f"{', '.join(sorted(allowed))} — '{author}' isn't on that list, so this BUY was skipped.")

    return True, ""


# ── SL cooldown table (HYBRID_V2 only when sl_cooldown_minutes > 0) ──────────
# Per-underlying time ban after an SL/SL_PARTIAL exit. Persisted in-memory;
# cleared on bot restart (acceptable — restart is a manual action and resets
# all state). Keyed by extract_root_symbol(osi_symbol).
_sl_cooldown: dict[str, tuple[float, str]] = {}     # root -> (expires_epoch, last_reason)
_sl_count_today: dict[tuple[str, str], int] = {}   # (root, et_date) -> count


def _SL_COOLDOWN_MIN() -> int:
    return cfg.getint("trading", "sl_cooldown_minutes", fallback=0)


def _SL_COOLDOWN_PANIC_MIN() -> int:
    return cfg.getint("trading", "sl_cooldown_after_panic_minutes", fallback=0)


def _SL_MAX_PER_DAY() -> int:
    return cfg.getint("trading", "sl_max_per_underlying_per_day", fallback=0)


def record_sl_exit(osi_symbol: str, reason: str) -> None:
    """Called by exit pipeline whenever an SL-class rule fires (SL,
    SL_PARTIAL, SOFT_STOP, SOFT_STOP_PARTIAL, PANIC, TRAIL_HIT after a
    losing trade). Sets the cooldown timestamp for the root symbol and
    increments the daily counter."""
    root = extract_root_symbol(osi_symbol)
    if not root:
        return
    now = time.time()
    if reason == "PANIC":
        mins = _SL_COOLDOWN_PANIC_MIN()
    elif reason in ("TRAIL_HIT", "ANALYST_GREEN", "ANALYST_AI_REDUCE"):
        # Profitable / neutral exits — no cooldown
        return
    else:
        mins = _SL_COOLDOWN_MIN()
    if mins > 0:
        _sl_cooldown[root] = (now + mins * 60, reason)
        log.info("[SL_COOLDOWN] %s blocked %d min after %s", root, mins, reason)

    today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    key = (root, today)
    _sl_count_today[key] = _sl_count_today.get(key, 0) + 1


def check_sl_cooldown(osi_symbol: str) -> tuple[bool, str]:
    """Returns (allowed, reason). Blocks BUY if root has active SL cooldown
    OR has hit max_sl_per_underlying_per_day."""
    root = extract_root_symbol(osi_symbol)
    if not root:
        return True, ""
    # Active cooldown?
    entry = _sl_cooldown.get(root)
    if entry:
        expires, reason = entry
        now = time.time()
        if now < expires:
            mins_left = int((expires - now) / 60) + 1
            return False, (f"SL_COOLDOWN: not re-entering {root} for another {mins_left}m — a stop-loss just fired on it "
                   f"({reason}). Cooldown prevents revenge-entering the same falling name.")
        else:
            _sl_cooldown.pop(root, None)
    # Max-per-day?
    max_day = _SL_MAX_PER_DAY()
    if max_day > 0:
        today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
        n = _sl_count_today.get((root, today), 0)
        if n >= max_day:
            return False, (f"SL_DAY_CAP: {root} stopped out {n}x today (your daily max is {max_day}) — "
                   f"no more entries on this name until tomorrow.")
    return True, ""


def _CORRELATION_GUARD_ENABLED() -> bool:
    return cfg.getboolean("guardrails", "correlation_guard_enabled", fallback=False)


def _BLOCK_BUYS_ON_STALE_MARKS() -> bool:
    return cfg.getboolean("guardrails", "block_buys_on_stale_marks", fallback=True)


def _STALE_MARK_GRACE_SECONDS() -> int:
    return cfg.getint("guardrails", "stale_mark_grace_seconds", fallback=120)


def _STALE_MARK_HAIRCUT_PCT() -> float:
    """Conservative valuation for an un-marked open position in the daily-loss
    cap. 0 (default) = legacy behavior (value at $0 unrealized). When >0, an
    un-marked position is valued at avg_price × (1 − haircut/100) so a bleeding
    blind position pushes the cap toward HALT instead of hiding (GH #11 residual).
    Portal-tunable; pair with sl_pct (e.g. 50 ≈ assume it's at the stop)."""
    return cfg.getfloat("guardrails", "stale_mark_haircut_pct", fallback=0.0)


def _check_stale_marks() -> tuple[bool, str]:
    """Block NEW BUYs while any open position has never received a price mark
    past the grace window (GH #11).

    The daily-loss cap values an un-marked position as $0 unrealized
    (_calculate_daily_pnl), so a position we've never priced is invisible to the
    halt — we could keep opening risk while blind to existing exposure. A fresh
    open with no mark yet is normal, so only positions older than the grace count
    (a never-priced position past the grace means the price feed is broken, not
    just warming up). SELLs are never blocked — closing blind exposure is safe.
    """
    if not _BLOCK_BUYS_ON_STALE_MARKS():
        return False, ""
    grace = _STALE_MARK_GRACE_SECONDS()
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=grace)
    db = SessionLocal()
    try:
        stale = db.query(Position).filter(
            Position.status.in_(["OPEN", "PARTIAL"]),
            Position.remaining > 0,
            Position.avg_price.isnot(None),
            (Position.current_price.is_(None)) | (Position.current_price == 0),
            Position.open_time < cutoff,
        ).all()
        if stale:
            syms = ", ".join(sorted({p.osi_symbol for p in stale})[:5])
            return True, (
                f"STALE_MARKS: {len(stale)} open position(s) have no price mark "
                f">{grace}s after open ({syms}) — refusing new BUYs while blind to "
                f"existing exposure (daily-loss cap can't value them). Check the price feed."
            )
        return False, ""
    finally:
        db.close()


def _REENTRY_GUARD_ENABLED() -> bool:
    return cfg.getboolean("trading", "reentry_guard_enabled", fallback=True)


def _REENTRY_MAX_PRICE_MULT() -> float:
    return cfg.getfloat("trading", "reentry_max_price_multiplier", fallback=1.5)


def _REENTRY_PROBE_ONLY_INDEX() -> bool:
    return cfg.getboolean("trading", "reentry_probe_only_for_index", fallback=True)


def check_reentry_guard(osi_symbol: str, alert_price: Optional[Decimal]) -> tuple[str, str, dict]:
    """
    Check if BUY is a same-strike same-day re-entry and decide what to do.

    A position counts as a prior CLOSED-today entry if any Position with the
    same osi_symbol opened and closed today (ET). Looks up the original entry
    price from that closed Position's avg_price.

    Returns ("ok"|"reject"|"probe"|"flag", reason, ctx) where ctx may carry:
      - first_entry_price: float (original avg_price of the closed position)
      - is_index: bool

    Decisions:
      - ok      : not a re-entry, or guard disabled — proceed normally
      - reject  : alert_price > first_entry × multiplier — block as too-late chase
      - probe   : index re-entry under multiplier and probe_only_for_index=true —
                  caller should clamp qty to 1
      - flag    : valid re-entry — caller should tag Position.strategy_tag="RE-ENTRY"
    """
    if not _REENTRY_GUARD_ENABLED() or not osi_symbol:
        return "ok", "", {}

    today_et = datetime.now(ZoneInfo("America/New_York")).date()
    today_start = datetime.combine(today_et, datetime.min.time()).replace(tzinfo=ZoneInfo("America/New_York")).astimezone(timezone.utc)

    db = SessionLocal()
    try:
        prior = db.query(Position).filter(
            Position.osi_symbol == osi_symbol,
            Position.status == "CLOSED",
            Position.close_time.isnot(None),
            Position.close_time >= today_start,
        ).order_by(Position.close_time.desc()).first()
        if not prior:
            return "ok", "", {}
        first_entry_px = float(prior.avg_price or 0)
    finally:
        db.close()

    if first_entry_px <= 0 or alert_price is None:
        # Can't compare without prices; tag as re-entry but allow.
        return "flag", f"RE-ENTRY: same-day re-buy on {osi_symbol} (no price compare)", {"first_entry_price": first_entry_px}

    try:
        new_px = float(alert_price)
    except Exception:
        return "flag", f"RE-ENTRY: same-day re-buy on {osi_symbol}", {"first_entry_price": first_entry_px}

    mult = _REENTRY_MAX_PRICE_MULT()
    is_index = _is_index_symbol(osi_symbol)

    if new_px > first_entry_px * mult:
        return ("reject",
                f"RE-ENTRY BLOCKED: you already traded {osi_symbol} today at ${first_entry_px:.2f} and this new alert "
                f"is ${new_px:.2f} — more than {mult:.2f}x the original entry (${first_entry_px * mult:.2f} cutoff). "
                f"Re-buying that much higher is a late chase, so it was skipped.",
                {"first_entry_price": first_entry_px, "is_index": is_index})

    if is_index and _REENTRY_PROBE_ONLY_INDEX():
        return ("probe",
                f"RE-ENTRY PROBE: {osi_symbol} index re-entry — clamp qty=1 (alert ${new_px:.2f} vs original ${first_entry_px:.2f})",
                {"first_entry_price": first_entry_px, "is_index": True})

    return ("flag",
            f"RE-ENTRY: {osi_symbol} same-day re-buy (alert ${new_px:.2f} vs original ${first_entry_px:.2f}) — tighter exits applied",
            {"first_entry_price": first_entry_px, "is_index": is_index})


def _check_correlation_conflict(osi_symbol: str) -> tuple[bool, str]:
    """Check if adding this symbol would conflict with existing correlated position."""
    if not _CORRELATION_GUARD_ENABLED():
        return False, ""
    
    root = extract_root_symbol(osi_symbol)
    if not root:
        return False, ""
    
    # Check if this symbol has a correlated partner
    partner = _CORRELATED_PAIRS.get(root)
    if not partner:
        return False, ""
    
    # Check if we have an open position in the correlated asset
    db = SessionLocal()
    try:
        from app.core.models import Position
        # Look for any open position where the osi_symbol starts with the partner root
        existing = db.query(Position).filter(
            Position.osi_symbol.ilike(f"{partner}%"),
            Position.status.in_(["OPEN", "PARTIAL"])
        ).first()
        
        if existing:
            return True, (f"CORRELATION CONFLICT: you already hold {partner} (position #{existing.id}) and {root} "
                  f"tracks the same underlying market — holding both doubles the same bet, so this BUY was skipped.")
        
        return False, ""
    finally:
        db.close()


# Daily kill switch — set True if max daily loss is exceeded.
# Backed by the system_state table so a restart/redeploy can't resume trading
# on a day that already blew the max-daily-loss cap (GH #10). The module globals
# are a write-through cache; the DB row is authoritative.
_trading_halted = False
_halt_date: Optional[str] = None
_halt_loaded = False
_HALT_KEY = "trading_halt"   # system_state value = ET date the halt was set


def _ensure_halt_loaded():
    """Lazy-load the persisted halt into the in-memory cache once per process.
    Restart-safe: if the DB says we halted today, we stay halted."""
    global _trading_halted, _halt_date, _halt_loaded
    if _halt_loaded:
        return
    _halt_loaded = True
    try:
        from app.core.models import get_system_state
        db = SessionLocal()
        try:
            persisted = get_system_state(db, _HALT_KEY)
        finally:
            db.close()
        if persisted:
            today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
            if persisted == today:
                _trading_halted = True
                _halt_date = persisted
                log.warning("[GUARDRAILS] Restored persisted halt for %s — trading paused", persisted)
            else:
                # Stale halt from a previous day — clear it.
                _persist_halt(None)
    except Exception as e:
        # Never let a state-store hiccup open the gate or crash the check.
        log.error("[GUARDRAILS] Halt load failed (%s) — assuming not halted", e)


def _halt_persisted_today() -> bool:
    """Dashboard helper: is a halt persisted in system_state for today (ET)?"""
    try:
        from app.core.models import get_system_state
        db = SessionLocal()
        try:
            v = get_system_state(db, _HALT_KEY)
        finally:
            db.close()
        return bool(v) and v == datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        return False


def _order_queue_depth() -> int:
    """Dashboard helper: current serialized-execution queue backlog."""
    try:
        from app.execution.order_queue import queue_depth
        return queue_depth()
    except Exception:
        return 0


def _persist_halt(date_or_none: Optional[str]):
    """Write-through: set the halt date in system_state, or clear it when None."""
    try:
        from app.core.models import set_system_state, delete_system_state
        db = SessionLocal()
        try:
            if date_or_none:
                set_system_state(db, _HALT_KEY, date_or_none)
            else:
                delete_system_state(db, _HALT_KEY)
        finally:
            db.close()
    except Exception as e:
        log.error("[GUARDRAILS] Halt persist failed (%s) — in-memory state still applies", e)


def reload_config():
    """Re-read config.ini into the shared AppConfig singleton (no restart needed)."""
    cfg.reload()
    log.info("[GUARDRAILS] Config reloaded via config_manager")


# ── Public API ───────────────────────────────────────────────────────────────

def compute_content_hash(osi_symbol: str, action: str, price: Optional[Decimal] = None) -> str:
    """
    Generate a content hash for cross-channel dedup.
    Same OSI + action + normalized price = same hash, regardless of source channel.
    Price is normalized to 2 decimal places to handle minor formatting differences.
    """
    # Use Decimal quantize, never float — float(Decimal("3.005")) → "3.00"
    # while float(Decimal("3.015")) → "3.02" due to IEEE-754 rounding,
    # producing different hashes for near-identical alerts and bypassing
    # cross-channel dedup. Decimal.quantize uses banker's rounding
    # consistently regardless of input representation.
    if price is not None:
        try:
            price_str = str(Decimal(price).quantize(Decimal("0.01")))
        except Exception:
            price_str = f"{float(price):.2f}"
    else:
        price_str = "none"
    raw = f"{osi_symbol}|{action}|{price_str}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def check_guardrails(
    action: str,
    osi_symbol: str,
    price: Optional[Decimal],
    qty: int,
    alert_author: str = "",
    content_hash: str = "",
    alert_db_id: Optional[int] = None,
    size_tag: str = "",
) -> tuple[bool, str]:
    """Check all guardrails before placing an order. Thin wrapper that records
    the pass/block metric in ONE place around the full implementation."""
    allowed, reason = _check_guardrails_impl(
        action, osi_symbol, price, qty,
        alert_author=alert_author, content_hash=content_hash,
        alert_db_id=alert_db_id, size_tag=size_tag,
    )
    try:
        from app.core import metrics
        if allowed:
            metrics.inc("guardrail_pass_total")
        else:
            metrics.inc("guardrail_block_total",
                        reason=metrics.guardrail_reason_category(reason))
    except Exception:
        pass
    return allowed, reason


def audit_order_decision(*, action, osi_symbol, author, content_hash,
                         allowed, reason="", qty=None, price=None) -> None:
    """Append-only provenance record of an order decision (best-effort,
    flag-gated). Called from the executor AFTER check_guardrails so the audit
    write never sits on the latency-critical guardrail path."""
    if not cfg.getboolean("trading", "audit_log_enabled", fallback=True):
        return
    try:
        from app.core.models import record_order_decision
        db = SessionLocal()
        try:
            record_order_decision(
                db, action=action, osi_symbol=osi_symbol, author=author,
                content_hash=content_hash,
                verdict="ALLOWED" if allowed else "BLOCKED",
                reason="" if allowed else reason, qty=qty, price=price,
            )
        finally:
            db.close()
    except Exception:
        pass


def _check_guardrails_impl(
    action: str,
    osi_symbol: str,
    price: Optional[Decimal],
    qty: int,
    alert_author: str = "",
    content_hash: str = "",
    alert_db_id: Optional[int] = None,
    size_tag: str = "",
) -> tuple[bool, str]:
    """
    Check all guardrails before placing an order.

    Returns:
        (True, "") if order is allowed
        (False, "reason") if order is blocked
    """
    global _trading_halted, _halt_date

    # Restore any persisted daily halt before the first decision (GH #10).
    _ensure_halt_loaded()

    # ── 0-. Operator manual halt (BUY-block, SELL-allow) ──────────────────
    # A true "stop trading" switch, distinct from dry_run. dry_run PAPER-
    # SIMULATES: it still parses alerts and writes synthetic FILLED rows to
    # positions/orders at made-up prices (2026-06-29 incident: operator flipped
    # dry_run to "pause for the day" and got 9 phantom FILLED positions that
    # never hit the broker → 404, corrupting P&L by ~$877). manual_halt instead
    # blocks new entries HERE at the guardrail — alert marked SKIPPED, zero
    # order/position rows, no simulation. SELLs fall through so open positions
    # stay exitable (never block exits — see analyst_sell_must_fill). Checked
    # BEFORE the master guardrails toggle so an operator halt holds even when
    # guardrails are disabled. One-flip revert: [trading] manual_halt=false
    # (portal hot-reload, no restart).
    if action == "BUY" and cfg.getboolean("trading", "manual_halt", fallback=False):
        return False, ("MANUAL HALT: you switched trading off (Services → Trade execution) — new BUYs are paused, "
                   "SELLs still work so open positions stay exitable. Flip manual_halt off to resume.")

    # ── 0. Master guardrails toggle ───────────────────────────────────────
    if not _GUARDRAILS_ENABLED():
        return True, ""  # Guardrails disabled - allow all orders

    # ── 0b. Blocked symbols check ───────────────────────────────────────────
    blocked = _BLOCKED_SYMBOLS()
    if osi_symbol and blocked:
        root_symbol = extract_root_symbol(osi_symbol)
        if root_symbol and root_symbol in blocked:
            return False, (f"BLOCKED SYMBOL: {root_symbol} is on your do-not-trade list (blocked_symbols) — alert ignored.")

    # ── 0c. Analyst whitelist / blacklist ───────────────────────────────────
    # BUY: full whitelist + blacklist gate (entry authorization).
    # SELL: blacklist ONLY — a blacklisted (untrusted) author must not drive
    #   exits either. Whitelist stays BUY-only so a trusted-but-unlisted
    #   analyst's exit still fills (see [[feedback_analyst_sell_must_fill]]).
    #   2026-06-11 incident: BUY-blacklisted 'TC' posted "everything selling
    #   now" → AI parser emitted blank-symbol close_all → executor flattened
    #   the whole book; the blacklist never ran because it was gated to BUY.
    if action == "BUY":
        ok, reason = check_analyst_filter(alert_author, osi_symbol)
        if not ok:
            return False, reason
    elif action == "SELL" and _ANALYST_BLACKLIST_BLOCKS_EXITS():
        bl = _ANALYST_BLACKLIST()
        if bl and any(tok in (alert_author or "").lower() for tok in bl):
            # ...unless it's their own open position. See blacklisted_sell_may_exit.
            if blacklisted_sell_may_exit(action, alert_author, osi_symbol):
                log.warning("[BLACKLIST] %s is blacklisted but holds %s — letting their exit through",
                            alert_author, osi_symbol)
            else:
                return False, (f"ANALYST_BLACKLIST: '{alert_author}' is on your blocked list — their SELL/close-all was ignored "
                       f"so an untrusted alert can never flatten your book (2026-06-11 lesson).")

    # ── 0c1. Blank-symbol close_all guard (the nuclear button) ──────────────
    # A SELL with no osi_symbol is always a blank-symbol close_all: it exits
    # EVERY open position. When close_all_author_whitelist is set, only listed
    # authors may fire it — default-deny for everyone. Empty list = the
    # blacklist above is the only gate.
    if action == "SELL" and not osi_symbol:
        cw = _CLOSE_ALL_AUTHOR_WHITELIST()
        if cw and not any(tok in (alert_author or "").lower() for tok in cw):
            return False, (f"CLOSE_ALL_NOT_AUTHORIZED: '{alert_author}' not in "
                           f"close_all_author_whitelist")

    # ── 0c2. Rollup block (BUY only) — block [ROLL UP]/[ROLL DOWN] except whitelist
    if action == "BUY":
        ok, reason = check_rollup_block(alert_author, size_tag)
        if not ok:
            return False, reason

    # ── 0d. SL cooldown / per-day ban (BUY only) ────────────────────────────
    if action == "BUY" and osi_symbol:
        ok, reason = check_sl_cooldown(osi_symbol)
        if not ok:
            return False, reason

    # ── 0e. Correlation guardrail ───────────────────────────────────────────
    if action == "BUY" and osi_symbol:
        conflict, reason = _check_correlation_conflict(osi_symbol)
        if conflict:
            return False, reason

    # ── 0f. Stale-mark guard (BUY only) — don't open new risk while blind ────
    if action == "BUY":
        blocked, reason = _check_stale_marks()
        if blocked:
            return False, reason

    # Reset halt on a new ET trading day. Using UTC date here would reset
    # the halt at 8 PM ET (UTC midnight during EDT) — practically harmless
    # because the market is closed, but inconsistent with the rest of the
    # bot which reasons in ET (premarket_prep, _is_market_open, etc.). Use
    # ET so the boundary is well-defined regardless of DST drift.
    today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    if _halt_date and _halt_date != today:
        _trading_halted = False
        _halt_date = None
        _persist_halt(None)
        log.info("[GUARDRAILS] New day — trading resumed")

    # ── 1. Trading halted? ────────────────────────────────────────────────
    # Re-evaluate against current limit so hot-reloaded config (raised limit
    # or disabled guardrails) clears the sticky halt without a bot restart.
    if _trading_halted:
        try:
            from app.core.db import SessionLocal as _SL
            _db = _SL()
            try:
                _current_pnl = _calculate_daily_pnl(_db)
            finally:
                _db.close()
            _max_loss = _MAX_DAILY_LOSS()
            if _current_pnl > -_max_loss:
                _trading_halted = False
                _halt_date = None
                _persist_halt(None)
                log.info(
                    "[GUARDRAILS] Halt cleared — daily P&L $%+.2f within new limit -$%.0f",
                    _current_pnl, _max_loss,
                )
            else:
                return False, (f"HALTED: down ${abs(_current_pnl):.2f} today, past your ${_max_loss:.0f} daily-loss limit — "
                   f"no more BUYs until tomorrow. SELLs still work.")
        except Exception as e:
            log.error("[GUARDRAILS] Halt re-check failed: %s — keeping halt active", e)
            return False, (f"HALTED: your ${_MAX_DAILY_LOSS():.0f} daily-loss limit was hit — "
                   f"no more BUYs until tomorrow. SELLs still work.")

    # ── 1b. Broker reconcile required for BUYs ───────────────────────────
    # Prevents trading on stale local data if the startup reconcile failed
    # (network, auth, broker outage). SELLs still allowed — closing an
    # existing position never depends on broker-as-truth handshake.
    if action == "BUY":
        try:
            from app.monitors.reconciler import has_reconciled
            if not has_reconciled() and cfg.getboolean(
                "trading", "require_reconcile_before_buy", fallback=True
            ) and not cfg.getboolean("trading", "dry_run", fallback=False):
                return False, ("BLOCKED: Broker reconciliation has not completed yet "
                               "(startup reconcile may have failed — check errors.log). "
                               "Local DB may be stale; refusing BUYs until broker state confirmed.")
        except Exception:
            # Reconciler import failure — fail open so tests/imports don't break.
            pass

    # ── 2. Market hours check ─────────────────────────────────────────────
    if _MARKET_HOURS_ONLY() and action == "BUY":
        blocked, reason = _check_market_hours()
        if blocked:
            return False, reason

    # ── 3. Duplicate alert detection ──────────────────────────────────────
    # Exception: if scale-in is enabled and the same OSI already has a
    # profitable open position, allow the add and SKIP the duplicate check.
    # We must confirm via the DB here — relying only on _recent_alerts allows
    # a recently rejected BUY to act as a phantom "position exists" signal
    # and bypass the dedup window for a real duplicate.
    scale_in = False
    if osi_symbol and action == "BUY" and _SCALE_IN_ENABLED():
        # Fast in-memory hint first, then confirm against the DB.
        if (osi_symbol, action) in _recent_alerts:
            confirmed, _ = _check_scale_in(osi_symbol)
            scale_in = confirmed

    # SELL alerts targeting an open position bypass dedup — analyst ladders
    # (e.g. 1/3 → 1/3 → 1/6 within 60s) are intentional, not duplicates.
    sell_ladder_bypass = False
    if action == "SELL" and osi_symbol:
        try:
            from app.core.db import SessionLocal as _SL
            from app.core.models import Position as _Pos
            _s = _SL()
            try:
                _open = _s.query(_Pos).filter(
                    _Pos.osi_symbol == osi_symbol,
                    _Pos.status.in_(("OPEN", "PARTIAL")),
                    _Pos.remaining > 0,
                ).first()
                sell_ladder_bypass = _open is not None
            finally:
                _s.close()
        except Exception:
            sell_ladder_bypass = False

    if not scale_in and not sell_ladder_bypass and osi_symbol:
        blocked, reason = _check_duplicate(osi_symbol, action)
        if blocked:
            return False, reason

    # ── 3b. Cross-channel duplicate detection (content hash) ─────────────
    # Fast in-memory check only — DB check is deferred into _check_buy_guards
    if content_hash:
        ccdw = _CROSS_CHANNEL_DEDUP_WINDOW()
        now_ts = time.time()
        cutoff = now_ts - ccdw
        _clean_stale_hashes(cutoff)
        if content_hash in _recent_content_hashes:
            elapsed = int(now_ts - _recent_content_hashes[content_hash])
            return False, (f"CROSS-CHANNEL DUPLICATE: this exact {action} on {osi_symbol} already came in from another "
                   f"channel {elapsed}s ago — skipping the copy so you don't double-enter.")

    # ── 4. Cooldown check (BUY only — never block exits) ────────────────
    if osi_symbol and action == "BUY":
        blocked, reason = _check_cooldown(osi_symbol)
        if blocked:
            return False, reason

    # Only apply remaining checks to BUY orders (opening positions)
    if action == "BUY":
        # ── 5-8: Combined DB check (single session for ALL DB work) ─────
        # This single session handles: scale-in check, cross-channel DB dedup,
        # max positions, max daily trades, max daily loss.
        blocked, reason, scale_in_confirmed = _check_buy_guards(
            osi_symbol, price, qty,
            content_hash=content_hash, alert_db_id=alert_db_id,
            check_scale_in=_SCALE_IN_ENABLED() and (osi_symbol, action) in _recent_alerts,
        )
        if scale_in_confirmed:
            log.info("[SCALE-IN] %s confirmed via DB check", osi_symbol)
        if blocked:
            if "MAX LOSS" in reason:
                _trading_halted = True
                _halt_date = today
                _persist_halt(today)   # survive restart (GH #10)
                try:
                    from app.core import metrics
                    metrics.inc("halt_set_total")
                    metrics.set_gauge("trading_halted", 1)
                except Exception:
                    pass
            return False, reason

    # Record this alert
    _record_alert(osi_symbol, action, content_hash)

    return True, ""


def is_scale_in(osi_symbol: str) -> bool:
    """Convenience: return True if this OSI would qualify as a scale-in BUY."""
    if not _SCALE_IN_ENABLED() or not osi_symbol:
        return False
    ok, _ = _check_scale_in(osi_symbol)
    return ok


def get_guardrail_status() -> dict:
    """Return current guardrail state for the dashboard."""
    db = SessionLocal()
    try:
        today = datetime.now(timezone.utc).date()
        today_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=timezone.utc)

        open_positions = db.query(Position).filter(
            Position.status.in_(["OPEN", "PARTIAL"]),
            Position.remaining > 0,
        ).count()

        today_trades = db.query(Order).filter(
            Order.placed_at >= today_start,
            Order.side == "BUY",
            Order.status.in_(["PENDING", "FILLED"]),
        ).count()

        daily_pnl = _calculate_daily_pnl(db)

        try:
            from app.core import metrics
            metrics.set_gauge("trading_halted", 1 if _trading_halted else 0)
            metrics.set_gauge("open_positions", open_positions)
            metrics.set_gauge("order_queue_depth", _order_queue_depth())
        except Exception:
            pass

        return {
            "guardrails_enabled": _GUARDRAILS_ENABLED(),
            "trading_halted": _trading_halted,
            "market_hours_only": _MARKET_HOURS_ONLY(),
            "is_market_open": _is_market_open(),
            "open_positions": open_positions,
            "max_open_positions": _MAX_OPEN_POSITIONS(),
            "today_trades": today_trades,
            "max_daily_trades": _MAX_DAILY_TRADES(),
            "daily_pnl": round(daily_pnl, 2),
            "max_daily_loss": _MAX_DAILY_LOSS(),
            "max_trade_cost": _MAX_TRADE_COST(),
            "dedup_window": _DEDUP_WINDOW(),
            "cooldown": _TRADE_COOLDOWN(),
            "cross_channel_dedup_window": _CROSS_CHANNEL_DEDUP_WINDOW(),
            "tracked_content_hashes": len(_recent_content_hashes),
            "blocked_symbols": list(_BLOCKED_SYMBOLS()),
            # New-subsystem visibility (serialized execution + persisted halt).
            "halt_persisted": _halt_persisted_today(),
            "serialized_execution": cfg.getboolean(
                "trading", "serialized_order_execution", fallback=True),
            "order_queue_depth": _order_queue_depth(),
        }
    finally:
        db.close()


# ── Internal checks ──────────────────────────────────────────────────────────

# US equity market full-close holidays (NYSE/Nasdaq). Maintain this list
# annually. Early-close (1 PM ET) days are not in here — callers still trade,
# just with a shorter window.
_US_MARKET_HOLIDAYS: frozenset[str] = frozenset({
    # 2025
    "2025-01-01",  # New Year's Day
    "2025-01-09",  # Day of Mourning for President Carter
    "2025-01-20",  # MLK Day
    "2025-02-17",  # Presidents' Day
    "2025-04-18",  # Good Friday
    "2025-05-26",  # Memorial Day
    "2025-06-19",  # Juneteenth
    "2025-07-04",  # Independence Day
    "2025-09-01",  # Labor Day
    "2025-11-27",  # Thanksgiving
    "2025-12-25",  # Christmas
    # 2026
    "2026-01-01",  # New Year's Day
    "2026-01-19",  # MLK Day
    "2026-02-16",  # Presidents' Day
    "2026-04-03",  # Good Friday
    "2026-05-25",  # Memorial Day
    "2026-06-19",  # Juneteenth
    "2026-07-03",  # Independence Day (observed — July 4 is Saturday)
    "2026-09-07",  # Labor Day
    "2026-11-26",  # Thanksgiving
    "2026-12-25",  # Christmas
    # 2027
    "2027-01-01",  # New Year's Day
    "2027-01-18",  # MLK Day
    "2027-02-15",  # Presidents' Day
    "2027-03-26",  # Good Friday
    "2027-05-31",  # Memorial Day
    "2027-06-18",  # Juneteenth (observed — June 19 is Saturday)
    "2027-07-05",  # Independence Day (observed — July 4 is Sunday)
    "2027-09-06",  # Labor Day
    "2027-11-25",  # Thanksgiving
    "2027-12-24",  # Christmas (observed — Dec 25 is Saturday)
})


def _is_market_holiday(et: datetime) -> bool:
    return et.strftime("%Y-%m-%d") in _US_MARKET_HOLIDAYS


def _is_market_open() -> bool:
    """Check if US stock market is currently open (handles EST/EDT automatically)."""
    et = datetime.now(ZoneInfo("America/New_York"))
    if et.weekday() >= 5:  # Saturday or Sunday
        return False
    if _is_market_holiday(et):
        return False
    market_open = et.replace(hour=_MARKET_OPEN_HOUR(), minute=_MARKET_OPEN_MIN(), second=0, microsecond=0)
    market_close = et.replace(hour=_MARKET_CLOSE_HOUR(), minute=_MARKET_CLOSE_MIN(), second=0, microsecond=0)
    return market_open <= et <= market_close


def _check_market_hours() -> tuple[bool, str]:
    """Block BUY orders outside market hours."""
    moh = _MARKET_OPEN_HOUR(); mom = _MARKET_OPEN_MIN()
    mch = _MARKET_CLOSE_HOUR(); mcm = _MARKET_CLOSE_MIN()
    if not _is_market_open():
        et = datetime.now(ZoneInfo("America/New_York"))
        if _is_market_holiday(et):
            return True, (f"MARKET CLOSED: {et.strftime('%A %Y-%m-%d')} is a US market holiday — BUYs only go out on trading days.")
        return True, (f"MARKET CLOSED: it's {et.strftime('%H:%M ET %A')} — BUYs only go out {moh}:{mom:02d}–{mch}:{mcm:02d} ET Mon–Fri. "
                  f"Alert logged but not traded.")
    return False, ""


def _check_duplicate(osi_symbol: str, action: str) -> tuple[bool, str]:
    """Block duplicate alerts within the dedup window."""
    now = time.time()
    key = (osi_symbol, action)
    dw = _DEDUP_WINDOW()

    # Clean old entries
    cutoff = now - dw
    expired = [k for k, v in _recent_alerts.items() if v < cutoff]
    for k in expired:
        del _recent_alerts[k]

    if key in _recent_alerts:
        elapsed = int(now - _recent_alerts[key])
        return True, (f"DUPLICATE: same {action} on {osi_symbol} was already handled {elapsed}s ago — "
                  f"repeats inside {dw}s are ignored so one alert can't fire twice.")

    return False, ""


def _check_cross_channel_duplicate(content_hash: str, osi_symbol: str, action: str, alert_db_id: Optional[int] = None) -> tuple[bool, str]:
    """Block alerts that match a recently processed content hash from any channel.
    This catches the same alert coming through #pro-alerts AND #all-alerts."""
    now = time.time()
    ccdw = _CROSS_CHANNEL_DEDUP_WINDOW()

    # Clean old entries from in-memory cache
    cutoff = now - ccdw
    _clean_stale_hashes(cutoff)

    # Check in-memory cache first (fast path)
    if content_hash in _recent_content_hashes:
        elapsed = int(now - _recent_content_hashes[content_hash])
        return True, (f"CROSS-CHANNEL DUPLICATE: this exact {action} on {osi_symbol} already came in from another "
                  f"channel {elapsed}s ago — skipping the copy so you don't double-enter.")

    # Also check the database for recent alerts with same content_hash
    # (covers restarts where in-memory cache is empty).
    # Exclude the current alert's own row (alert_db_id) so it doesn't
    # block itself — the alert is saved to DB before guardrails run.
    db = SessionLocal()
    try:
        from app.core.models import Alert
        dedup_cutoff = datetime.now(timezone.utc) - timedelta(seconds=ccdw)
        query = db.query(Alert).filter(
            Alert.content_hash == content_hash,
            Alert.timestamp >= dedup_cutoff,
            Alert.status.notin_(["HISTORICAL"]),
        )
        if alert_db_id is not None:
            query = query.filter(Alert.id != alert_db_id)
        existing = query.first()
        if existing:
            return True, (f"CROSS-CHANNEL DUPLICATE: identical {action} on {osi_symbol} was already taken from "
                  f"#{existing.channel_name} — skipping this copy so you don't double-enter.")
    finally:
        db.close()

    return False, ""


def _check_scale_in(osi_symbol: str) -> tuple[bool, str]:
    """
    Check if a BUY alert qualifies as a scale-in (adding to a profitable winner).

    Returns (True, reason) if the alert should bypass the dedup window because:
      - scale_in_enabled = true
      - An OPEN or PARTIAL position already exists for this OSI
      - The position is profitable (pnl_pct >= scale_in_min_profit_pct)
    """
    min_pct = _SCALE_IN_MIN_PROFIT_PCT()
    db = SessionLocal()
    try:
        pos = db.query(Position).filter(
            Position.osi_symbol == osi_symbol,
            Position.status.in_(["OPEN", "PARTIAL"]),
            Position.remaining > 0,
        ).first()
        if not pos:
            return False, ""
        if not pos.avg_price or not pos.current_price:
            return False, ""
        pnl_pct = ((pos.current_price / pos.avg_price) - 1) * 100
        if pnl_pct >= min_pct:
            return True, (
                f"SCALE-IN: {osi_symbol} already +{pnl_pct:.1f}% "
                f"(min required: +{min_pct:.0f}%) — adding to winner"
            )
        return False, (f"SCALE-IN GATE: you already hold {osi_symbol} and it's only {pnl_pct:+.1f}% — "
                   f"adding is allowed only once a position proves itself (+{min_pct:.0f}%).")
    finally:
        db.close()


def _check_cooldown(osi_symbol: str) -> tuple[bool, str]:
    """Enforce cooldown between trades on the same symbol."""
    now = time.time()
    cooldown = _TRADE_COOLDOWN()
    if osi_symbol in _last_trade_time:
        elapsed = now - _last_trade_time[osi_symbol]
        if elapsed < cooldown:
            remaining = int(cooldown - elapsed)
            return True, (f"COOLDOWN: {osi_symbol} traded moments ago — waiting {remaining}s between trades on the "
                  f"same contract so rapid-fire alerts can't stack entries.")
    return False, ""


def _check_buy_guards(
    osi_symbol: str,
    price: Optional[Decimal],
    qty: int,
    content_hash: str = "",
    alert_db_id: Optional[int] = None,
    check_scale_in: bool = False,
) -> tuple[bool, str, bool]:
    """
    Combined BUY-only guardrails using a SINGLE DB session for ultra-low latency.

    Handles in one round-trip:
      1. Scale-in position check (if check_scale_in=True)
      2. Cross-channel dedup DB fallback (if content_hash given)
      3. Max open positions
      4. Max daily trades
      5. Max daily loss

    Returns: (blocked: bool, reason: str, scale_in_confirmed: bool)
    """
    # Max cost is pure math — check first, no DB needed
    max_cost = _MAX_TRADE_COST()
    if price is not None:
        cost = float(price) * qty * 100
        if cost > max_cost:
            return True, (f"MAX COST: this trade needs ${cost:.0f} (${price} x {qty} contracts x 100) but your per-trade "
                  f"budget is ${max_cost:.0f} — skipped. Raise max_trade_cost or lower sizing to take it."), False

    db = SessionLocal()
    try:
        scale_in_confirmed = False

        # ── 1. Scale-in check (deferred from fast path above) ────────────
        if check_scale_in:
            min_pct = _SCALE_IN_MIN_PROFIT_PCT()
            pos = db.query(Position).filter(
                Position.osi_symbol == osi_symbol,
                Position.status.in_(["OPEN", "PARTIAL"]),
                Position.remaining > 0,
            ).first()
            if pos and pos.avg_price and pos.current_price:
                pnl_pct = ((pos.current_price / pos.avg_price) - 1) * 100
                if pnl_pct >= min_pct:
                    scale_in_confirmed = True

        # ── 2. Cross-channel dedup DB fallback ────────────────────────────
        # Only runs on cache miss (in-memory hit was already handled above)
        if content_hash:
            ccdw = _CROSS_CHANNEL_DEDUP_WINDOW()
            dedup_cutoff = datetime.now(timezone.utc) - timedelta(seconds=ccdw)
            query = db.query(Alert).filter(
                Alert.content_hash == content_hash,
                Alert.timestamp >= dedup_cutoff,
                Alert.status.notin_(["HISTORICAL"]),
            )
            if alert_db_id is not None:
                query = query.filter(Alert.id != alert_db_id)
            existing = query.first()
            if existing:
                return True, (f"CROSS-CHANNEL DUPLICATE: identical alert on {osi_symbol} was already taken from "
                  f"#{existing.channel_name} — skipping this copy so you don't double-enter."), False

        # ── 3. Max open positions ─────────────────────────────────────────
        if not scale_in_confirmed:
            max_pos = _MAX_OPEN_POSITIONS()
            open_count = db.query(Position).filter(
                Position.status.in_(["OPEN", "PARTIAL"]),
                Position.remaining > 0,
            ).count()
            if open_count >= max_pos:
                return True, (f"MAX POSITIONS: {open_count} positions already open, your cap is {max_pos} — "
                  f"close something or raise max_open_positions to take new entries."), False

        # ── 4. Max daily trades ───────────────────────────────────────────
        max_trades = _MAX_DAILY_TRADES()
        today = datetime.now(timezone.utc).date()
        today_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=timezone.utc)
        trade_count = db.query(Order).filter(
            Order.placed_at >= today_start,
            Order.side == "BUY",
            Order.status.in_(["PENDING", "FILLED"]),
        ).count()
        if trade_count >= max_trades:
            return True, (f"MAX TRADES: {trade_count} BUYs already placed today, your daily cap is {max_trades} — "
                  f"no more entries today."), False

        # ── 5. Max daily loss ─────────────────────────────────────────────
        max_loss = _MAX_DAILY_LOSS()
        daily_pnl = _calculate_daily_pnl(db)
        if daily_pnl <= -max_loss:
            return True, (f"MAX LOSS: down ${abs(daily_pnl):.2f} today, past your ${max_loss:.0f} daily-loss limit — "
                  f"BUYs halted until tomorrow. SELLs still work."), False

        return False, "", scale_in_confirmed
    finally:
        db.close()



def _calculate_daily_pnl(db) -> float:
    """Calculate realized + unrealized P&L for today."""
    from app.core.models import realized_pnl_since
    today = datetime.now(timezone.utc).date()
    today_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=timezone.utc)

    # Realized from per-leg SELL fills today (multi-leg-safe).
    total = realized_pnl_since(db, today_start)

    # Unrealized from open/partial positions at current mark.
    # When current_price is missing (price feed hiccup, fresh open before
    # first poll), treat unrealized P&L as 0 (assume flat) rather than
    # silently skipping the position — a $$$-bleeding open position with
    # a stale mark must NOT hide from the daily-loss cap. Skipping was the
    # old behavior; logging here makes the gap visible in cap calc.
    open_positions = db.query(Position).filter(
        Position.status.in_(["OPEN", "PARTIAL"]),
    ).all()
    _stale_mark_count = 0
    haircut = _STALE_MARK_HAIRCUT_PCT()
    for pos in open_positions:
        if not pos.avg_price:
            continue
        if pos.current_price:
            total += (pos.current_price - pos.avg_price) * pos.remaining * 100
        else:
            _stale_mark_count += 1
            if haircut > 0:
                # Conservative: assume the blind position is down `haircut`% from
                # entry so it can't hide from the cap. Always a loss (<=0).
                assumed_px = pos.avg_price * (1.0 - haircut / 100.0)
                total += (assumed_px - pos.avg_price) * (pos.remaining or 0) * 100
    if _stale_mark_count:
        _val = f"a -{haircut:.0f}% haircut" if haircut > 0 else "$0"
        log.warning("[CAP] %d open position(s) have no current_price — unrealized P&L for those valued at %s; "
                    "set guardrails.stale_mark_haircut_pct to tighten the daily-loss cap when blind",
                    _stale_mark_count, _val)

    return total


def _record_alert(osi_symbol: str, action: str, content_hash: str = ""):
    """Record that this alert was processed."""
    now = time.time()
    if osi_symbol:
        _recent_alerts[(osi_symbol, action)] = now
        _last_trade_time[osi_symbol] = now
    if content_hash:
        _recent_content_hashes[content_hash] = now

    # Prune _last_trade_time entries older than 24h to prevent unbounded growth
    cutoff_24h = now - 86400
    stale_times = [k for k, v in _last_trade_time.items() if v < cutoff_24h]
    for k in stale_times:
        del _last_trade_time[k]
