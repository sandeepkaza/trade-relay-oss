"""
exit_engine_v2.py — HYBRID_V2 exit engine for short-dated options.

Active only when config.ini [trading] exit_engine_mode = HYBRID_V2. The
legacy PARALLEL engine in position_monitor.py runs unchanged when this is
not selected. Toggle via portal hot-reload — no redeploy.

Design (see [exit_engine_v2] section in config.ini for thresholds):

    Premium-% as standalone SL is removed. Premium drives only the panic
    floor (default -60% with 3-tick confirmation). Soft stops require
    sustained breach (default 60 seconds). PT1/PT2/PT3 ladder is kept;
    trailing engages after PT2.

    When underlying_feed_enabled = true (Phase 2), invalidation uses
    underlying price ± k·ATR with time confirmation. When false, engine
    runs in "premium-confirmed" mode using pnl_pct only — still much less
    noisy than fixed-% SL because of the time-confirmation gate.

State machine (per Position, lives in this module's _STATE dict keyed by
pos.id):

    ENTERED  → ARMED (after PT1)  → TRAILING (after PT2/PT3)  → CLOSED
                  ↓                       ↓
              soft-stop                trail-stop
                  ↓                       ↓
                CLOSED                  CLOSED

    Any state can transition to CLOSED via:
      - panic floor (premium ≤ panic_premium_pct, confirmed)
      - EOD theta cliff (≤ eod_close_minutes_to_expiry remaining)
      - analyst close_all override (P/L tier-gated)

The engine is driven by `evaluate(pos, current_price, ws_manager)` which
returns a list of (rule, qty) tuples for any exits to fire this tick.
position_monitor.py wraps this with the existing `_fire_exit` plumbing so
broker calls, DB updates, and Discord broadcasting are unchanged.

Cooldown table is in guardrails.py (sl_cooldown_minutes), persisted in
the same SQLite DB as Positions. Re-entry blocking is handled there, not
here — this module is concerned only with exits.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from app.core.config_manager import cfg

log = logging.getLogger(__name__)


# ── State per position (in-memory; cleared on bot restart, which is fine —
# rebuilds from pos flags pt1_triggered/pt2_triggered/pt3_triggered).
@dataclass
class _PosState:
    state: str = "ENTERED"            # ENTERED | ARMED | TRAILING | CLOSED
    panic_below_count: int = 0
    soft_below_since: Optional[float] = None  # epoch seconds
    trail_below_since: Optional[float] = None
    peak_pnl_pct: float = 0.0


_STATE: dict[int, _PosState] = {}


def _state_for(pos) -> _PosState:
    st = _STATE.get(pos.id)
    if st is None:
        # Rebuild from pos flags so a bot restart mid-trade resumes correctly.
        st = _PosState()
        if getattr(pos, "pt2_triggered", False) or getattr(pos, "pt3_triggered", False) or getattr(pos, "tpt_armed", False):
            st.state = "TRAILING"
        elif getattr(pos, "pt1_triggered", False):
            st.state = "ARMED"
        _STATE[pos.id] = st
    return st


def reset_state(pos_id: int) -> None:
    _STATE.pop(pos_id, None)


# ── Config accessors (read live so portal changes apply immediately) ────────
def _F(key, fb): return cfg.getfloat("exit_engine_v2", key, fallback=fb)
def _I(key, fb): return cfg.getint("exit_engine_v2", key, fallback=fb)
def _B(key, fb): return cfg.getboolean("exit_engine_v2", key, fallback=fb)


def is_active() -> bool:
    """True when HYBRID_V2 is the selected mode."""
    mode = (cfg.get("trading", "exit_engine_mode", fallback="PARALLEL") or "PARALLEL").upper().strip()
    return mode == "HYBRID_V2"


def shadow_eval_enabled() -> bool:
    return _B("shadow_eval_enabled", False)


# ── Decision objects ────────────────────────────────────────────────────────
@dataclass
class ExitDecision:
    rule: str           # PANIC, SOFT_STOP, TRAIL_HIT, PT1, PT2, PT3, TPT,
                        # EOD_THETA, ANALYST_GREEN, ANALYST_AI_CUT
    qty: int
    fraction: float
    note: str = ""


# ── Public API ──────────────────────────────────────────────────────────────
def evaluate(pos, current_price: float, *, now: Optional[float] = None) -> list[tuple[str, int]]:
    """Run the v2 state machine for one position.

    Returns a list of (rule_name, qty) tuples. Caller fires each via the
    existing `_fire_exit` plumbing (preserving broker idempotency, exceeds-
    amount handling, manual-priority gates, post-fill grace).

    Note: this method does not call the broker or update the DB. Callers
    must mark pos flags (pt1_triggered, etc.) on successful broker fill,
    same as the legacy path. State transitions in _STATE happen here so
    the engine knows when ARMED/TRAILING are reached.
    """
    if pos is None or not pos.avg_price or pos.avg_price <= 0 or pos.remaining <= 0:
        return []

    now = now or time.time()
    pnl_pct = ((current_price / pos.avg_price) - 1.0) * 100.0
    st = _state_for(pos)
    st.peak_pnl_pct = max(st.peak_pnl_pct, pnl_pct)

    triggers: list[tuple[str, int]] = []

    # ── 1. Panic floor — premium-only, fires before anything else ───────────
    panic_pct = _F("panic_premium_pct", -60.0)
    panic_conf = _I("panic_confirm_ticks", 3)
    if pnl_pct <= panic_pct:
        st.panic_below_count += 1
        if st.panic_below_count >= panic_conf:
            triggers.append(("PANIC", pos.remaining))
            st.state = "CLOSED"
            return triggers
    else:
        st.panic_below_count = 0

    # ── 2. EOD theta cliff (0DTE protection) ────────────────────────────────
    mte = _minutes_to_expiry(pos)
    eod_min = _I("eod_close_minutes_to_expiry", 30)
    if mte is not None and mte <= eod_min and st.state != "TRAILING":
        triggers.append(("EOD_THETA", pos.remaining))
        st.state = "CLOSED"
        return triggers

    # ── 3. Profit targets — same %s as legacy, fire-and-flag via caller ─────
    pt1_pct = _F("pt1_pct", 15.0); pt1_sell = _F("pt1_sell", 0.25)
    pt2_pct = _F("pt2_pct", 25.0); pt2_sell = _F("pt2_sell", 0.40)
    pt3_pct = _F("pt3_pct", 40.0); pt3_sell = _F("pt3_sell", 0.50)

    if not getattr(pos, "pt1_triggered", False) and pnl_pct >= pt1_pct:
        qty = max(1, int(pos.remaining * pt1_sell))
        triggers.append(("PT1", qty))
        # Note: state transition deferred to caller's success path.

    if getattr(pos, "pt1_triggered", False) and not getattr(pos, "pt2_triggered", False) and pnl_pct >= pt2_pct:
        qty = max(1, int(pos.remaining * pt2_sell))
        triggers.append(("PT2", qty))

    if getattr(pos, "pt2_triggered", False) and not getattr(pos, "pt3_triggered", False) and pnl_pct >= pt3_pct:
        # PT3 sells pt3_sell of remaining (default 0.50), arms TRAILING for runner.
        qty = max(1, int(pos.remaining * pt3_sell))
        triggers.append(("PT3", qty))

    # ── 4. Trailing exit — only after PT2 (or PT3) ──────────────────────────
    if _B("trail_enabled", True) and getattr(pos, "pt2_triggered", False):
        give = _F("trail_giveback_pct", 25.0)
        confirm_s = _I("trail_confirm_seconds", 60)
        # Premium-surrogate trail: peak_pnl - giveback
        trail_floor_pnl = st.peak_pnl_pct - give
        if pnl_pct <= trail_floor_pnl:
            if st.trail_below_since is None:
                st.trail_below_since = now
            elif (now - st.trail_below_since) >= confirm_s:
                triggers.append(("TRAIL_HIT", pos.remaining))
                st.state = "CLOSED"
                return triggers
        else:
            st.trail_below_since = None

    # ── 5. Soft stop — time-confirmed premium drawdown ──────────────────────
    soft_pct = _F("soft_stop_premium_pct", -25.0)
    soft_conf_s = _I("soft_stop_confirm_seconds", 60)
    soft_first = _F("soft_stop_first_fraction", 0.50)
    in_soft_zone = pnl_pct <= soft_pct and not getattr(pos, "sl_triggered", False)
    spread_pct = float(getattr(pos, "_last_spread_pct", 0.0) or 0.0)
    spread_ok = spread_pct < _F("max_spread_pct_normal", 15.0)
    if in_soft_zone and spread_ok:
        if st.soft_below_since is None:
            st.soft_below_since = now
        elif (now - st.soft_below_since) >= soft_conf_s:
            if not getattr(pos, "sl_partial_triggered", False) and pos.remaining > 1:
                qty = max(1, int(pos.remaining * soft_first))
                triggers.append(("SOFT_STOP_PARTIAL", qty))
            else:
                triggers.append(("SOFT_STOP", pos.remaining))
                st.state = "CLOSED"
    else:
        st.soft_below_since = None

    return triggers


def evaluate_close_all(pos, current_price: float, ai_check_fn=None) -> Optional[tuple[str, int]]:
    """Apply analyst close_all P/L tier gate.

    Returns (rule, qty) if the close should fire, None to ignore. Caller
    invokes this when a Discord close_all (DISCORD reason) signal is
    routed to a position. Pure function — no side effects beyond optional
    AI call.

    `ai_check_fn` is a callable returning {"action": "CUT"|"REDUCE"|"HOLD",
    "fraction": 0..1} or None on timeout. If None, the band is treated as
    HOLD by default.
    """
    if not pos or not pos.avg_price or pos.avg_price <= 0:
        return None
    pnl_pct = ((current_price / pos.avg_price) - 1.0) * 100.0
    green = _F("analyst_close_all_green_threshold", 0.0)
    red = _F("analyst_close_all_red_threshold", -10.0)

    if pnl_pct >= green:
        return ("ANALYST_GREEN", pos.remaining)

    if pnl_pct >= red:
        if not _B("ai_exit_check_enabled", True) or ai_check_fn is None:
            log.info("[V2] close_all band [%s, %s] pnl=%.1f%% — no AI check, holding", red, green, pnl_pct)
            return None
        try:
            verdict = ai_check_fn(pos, current_price, pnl_pct)
        except Exception as e:
            log.warning("[V2] AI exit check raised: %s — defaulting HOLD", e)
            verdict = None
        if not verdict:
            return None
        action = (verdict.get("action") or "").upper()
        if action == "CUT":
            return ("ANALYST_AI_CUT", pos.remaining)
        if action == "REDUCE":
            frac = float(verdict.get("fraction") or 0.5)
            qty = max(1, int(pos.remaining * frac))
            return ("ANALYST_AI_REDUCE", qty)
        return None

    # pnl < red — let panic floor / soft stop manage. Don't realize loss
    # by mirroring an analyst capitulation.
    log.info("[V2] close_all ignored (pnl=%.1f%% < %.1f%%) — soft stop will manage", pnl_pct, red)
    return None


# ── Helpers ─────────────────────────────────────────────────────────────────
def _minutes_to_expiry(pos) -> Optional[int]:
    exp = getattr(pos, "expiry", None)
    if not exp:
        return None
    try:
        d = datetime.strptime(str(exp)[:10], "%Y-%m-%d")
    except Exception:
        return None
    # Treat expiry as 16:00 ET on that calendar day.
    from zoneinfo import ZoneInfo
    eod_et = d.replace(hour=16, minute=0, tzinfo=ZoneInfo("America/New_York"))
    delta = eod_et.astimezone(timezone.utc) - datetime.now(timezone.utc)
    return max(0, int(delta.total_seconds() // 60))


def shadow_log(pos, current_price: float, parallel_actions: list[tuple[str, int]]) -> None:
    """Emit a single line comparing what v2 would have done vs the PARALLEL
    engine's actual decision this tick. Active only when [exit_engine_v2]
    shadow_eval_enabled = true AND mode is still PARALLEL — gives confidence
    before the user flips the switch."""
    if not shadow_eval_enabled():
        return
    try:
        v2_actions = evaluate(pos, current_price)
    except Exception as e:
        log.debug("[V2_SHADOW] eval raised: %s", e)
        return
    if v2_actions == parallel_actions:
        return
    log.info(
        "[V2_SHADOW] %s parallel=%s v2=%s pnl=%.1f%%",
        pos.osi_symbol,
        [(r, q) for r, q in parallel_actions] or "[]",
        [(r, q) for r, q in v2_actions] or "[]",
        ((current_price / pos.avg_price) - 1.0) * 100.0 if pos.avg_price else 0.0,
    )
