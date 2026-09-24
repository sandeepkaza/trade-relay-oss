"""
public_executor.py - Places and tracks orders on Public.com.
"""

import asyncio
import logging
import math
import os
import re
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo
from types import SimpleNamespace
from typing import Any

# Analyst-typed "sized to 0" / "size to 0" / "sizing to 0" sentinel. Catches
# the common variants ("size 0", "sized 0", with optional colon/dash) without
# tripping on incidental phrases like "$0.50" or "10 contracts at 0".
_SIZED_TO_ZERO_RE = re.compile(
    r"\bsiz(?:e|ed|ing)\s*(?:to|=|:|-)?\s*0\b",
    re.IGNORECASE,
)

from sqlalchemy import or_

from app.core.db import SessionLocal
from app.core.models import Alert, Order, Position
import app.analytics.trade_logger as trade_logger
import app.risk.guardrails as guardrails
import app.risk.ai_scorer as ai_scorer
import app.risk.kelly_sizer as kelly_sizer
from app.core.log_config import guardrail_logger, order_logger, scorer_logger, PipelineTimer
from app.core.config_manager import cfg  # hot-reloadable config singleton
from app.core.profile_resolver import (
    pcfg_float as _pcfg_float,
    pcfg_int as _pcfg_int,
    pcfg_bool as _pcfg_bool,
    pcfg_get as _pcfg_get,
    entry_overrides_enabled as _entry_overrides_enabled,
    whale_resting_tp_enabled as _whale_resting_tp_enabled,
)

log = logging.getLogger(__name__)


def _metric_order_placed(side: str) -> None:
    """Best-effort counter — metrics must never break the order path."""
    try:
        from app.core import metrics
        metrics.inc("orders_placed_total", side=side)
    except Exception:
        pass


# Startup-only API credentials (not editable from dashboard)
import configparser as _cp
_boot = _cp.ConfigParser(inline_comment_prefixes=(";", "#"))
_boot.read("config.ini", encoding="utf-8-sig")
API_KEY     = os.getenv("PUBLIC_API_KEY",        _boot.get("public", "api_key",        fallback=""))
ACCOUNT_NUM = os.getenv("PUBLIC_ACCOUNT_NUMBER", _boot.get("public", "account_number", fallback=""))
del _boot, _cp  # free memory; credentials are now captured in module-level vars


# ── Live config accessors (read from cfg on every call) ────────────────────
# Changing these from the dashboard Settings tab takes effect immediately.

def _DRY_RUN():             return cfg.getboolean("trading", "dry_run",                  fallback=False)
def _BUY_SLIPPAGE():        return Decimal(cfg.get("trading", "buy_slippage",             fallback="0.05"))
def _SELL_SLIPPAGE():       return Decimal(cfg.get("trading", "sell_slippage",            fallback="0.05"))
def _MAX_PENDING_MINUTES(): return cfg.getint("trading",    "max_pending_minutes",        fallback=30)
# Seconds cancel_order waits for a Public.com order to reach a terminal state
# after the cancel request, so a fill that RACES the cancel is caught before a
# SELL re-peg places a replacement (double-sell prevention, parity with IBKR).
# Shared config key with the IBKR broker. 0 = legacy fire-and-forget.
def _CANCEL_CONFIRM_TIMEOUT(): return cfg.getfloat("trading", "cancel_confirm_timeout_seconds", fallback=8.0)  # 3s < real cancel latency (2026-07-09)
# Safety gate for the re-peg lanes: only place a replacement order once the
# prior order's cancel is CONFIRMED terminal (not filled). When True, an
# unconfirmed cancel (await timeout / broker reconnect that dropped the cancel)
# makes the re-peg hold off instead of re-placing — prevents the double-sell /
# double-buy when a fill or a still-live order races the cancel (SPXW 7550C
# 2026-07-09 IBKR: order 344 filled after PendingCancel + IB reconnect while the
# re-peg had already placed 345). False = legacy fire-and-forget re-place.
def _REPEG_CONFIRMED_CANCEL_ONLY(): return cfg.getboolean("trading", "repeg_confirmed_cancel_only", fallback=True)
def _FILL_POLL_INTERVAL():  return cfg.getint("trading",    "fill_poll_interval_seconds", fallback=3)
def _MAX_ENTRY_SLIPPAGE_PCT(): return cfg.getfloat("trading", "max_entry_slippage_pct",    fallback=5.0)
def _SLIPPAGE_RETRY_MAX(): return cfg.getint("trading", "slippage_retry_max_attempts", fallback=1)
def _SLIPPAGE_RETRY_WINDOW_S(): return cfg.getint("trading", "slippage_retry_window_seconds", fallback=300)

# Per-OSI slippage attempt tracker — counts how many SLIPPAGE_BLOCK events the
# same osi_symbol has had inside a rolling window. Once the count reaches
# `slippage_retry_max_attempts`, further BUY alerts on that OSI are hard-skipped
# regardless of live price. Stops the "block-block-block-fill at the worst
# price" pattern (APP $8.00 alert → 4 blocks → $8.46 fill → -36% loss).
_slippage_attempts: dict[str, list[float]] = {}


def _slippage_attempt_count(osi_symbol: str) -> int:
    import time as _t
    now = _t.time()
    win = _SLIPPAGE_RETRY_WINDOW_S()
    arr = _slippage_attempts.get(osi_symbol, [])
    arr = [t for t in arr if (now - t) <= win]
    _slippage_attempts[osi_symbol] = arr
    return len(arr)


def _record_slippage_attempt(osi_symbol: str) -> None:
    import time as _t
    arr = _slippage_attempts.setdefault(osi_symbol, [])
    arr.append(_t.time())
# Reject-signal registry: maps in-flight order_db_id → asyncio.Event a re-peg
# watcher is sleeping on. When the order polling loop detects a broker reject
# (REJECTED/CANCELLED in <1s), it sets the event to wake the watcher
# immediately instead of waiting out the full partial_repeg_seconds nap.
# Closes a ~30s window where a stranded SELL would otherwise sit idle.
_reject_signals: dict[int, "asyncio.Event"] = {}

def _signal_reject(order_db_id: int) -> None:
    ev = _reject_signals.get(order_db_id)
    if ev is not None:
        ev.set()

def _SLOW_SUBMIT_MS(): return cfg.getfloat("trading", "slow_submit_warn_ms", fallback=2000.0)
def _CLOSE_ALL_REPEG_SECONDS(): return cfg.getfloat("trading", "close_all_repeg_seconds", fallback=5.0)
def _CLOSE_ALL_REPEG_MAX_ATTEMPTS(): return cfg.getint("trading", "close_all_repeg_max_attempts", fallback=3)
def _CLOSE_ALL_FINAL_SLIPPAGE_PCT(): return cfg.getfloat("trading", "close_all_final_slippage_pct", fallback=20.0)
# Auto-exit re-peg. OFF by default: this is the one-knob revert to the legacy
# behaviour where a poll-fired exit is placed once and never chased. See
# docs/EXIT_ENGINE_HARDENING.md for what it fixes and why it matters only once
# auto_exit_enabled is true.
def _AUTO_EXIT_REPEG_ENABLED(): return cfg.getboolean("trading", "auto_exit_repeg_enabled", fallback=False)
def _AUTO_EXIT_STALE_SECONDS(): return cfg.getfloat("trading", "auto_exit_stale_seconds", fallback=10.0)
def _CLOSE_ALL_SCOPE():
    # "author" (default): a blank-symbol close_all ("sell everything") closes
    #   ONLY the positions the SAME analyst opened — never another analyst's
    #   book. "all": legacy book-wide flatten. 2026-06-11: a blank close_all
    #   from one analyst liquidated the entire book (3 unrelated positions).
    return (cfg.get("trading", "close_all_scope", fallback="author") or "author").strip().lower()
def _POSITION_OWNER_SCOPE():
    # "author" (default): a Position belongs to the analyst who opened it, and
    #   ONLY that analyst's alerts may act on it — a named-contract SELL from a
    #   different analyst is a no-op, and a BUY from a different analyst is
    #   blocked instead of merging into the opener's row.
    # "osi": legacy — the contract is the identity, any analyst's SELL on that
    #   OSI exits whoever holds it. 2026-08-05: Sniper's "ALL OUT MU 950C" (he
    #   was flat — his own entry never filled) closed Fluid's MU 950C at -82%
    #   / -$350.99 because Position.osi_symbol is UNIQUE and author was only a
    #   label. One-knob revert: position_owner_scope=osi.
    return (cfg.get("trading", "position_owner_scope", fallback="author") or "author").strip().lower()


# ponytail: one owner per row, enforced in code instead of the schema. The real
# model is a (osi_symbol, author) primary key so two analysts can hold the same
# contract independently — that needs the UNIQUE dropped on Position.osi_symbol,
# a migration, and ~83 filter_by(osi_symbol=...) call sites across 16 files.
# Upgrade when blocking the second analyst's entry actually costs a trade worth
# more than the migration.
def _owner_scoped() -> bool:
    return _POSITION_OWNER_SCOPE() == "author"


def _pos_owned_by(pos, author: str) -> bool:
    """True when `author` is allowed to act on `pos`.

    Unowned rows (author NULL — legacy pre-backfill positions, whale-lane
    entries) match ANYONE: they have no claimant, and refusing them would make
    them unsellable. A blank alert author likewise matches anything, so an
    un-attributed exit can never strand an open position.
    """
    own = (getattr(pos, "author", "") or "").strip()
    if not own or not author:
        return True
    return own.lower() == author.strip().lower()


def _scope_to_owner(rows: list, alert) -> list:
    """Drop positions this alert's analyst does not own.

    Position.osi_symbol is UNIQUE, so the contract — not the analyst — is the
    row identity. Without this filter ANY analyst's SELL on an OSI exits
    whoever happens to hold it. 2026-08-05: Sniper posted "ALL OUT MU 950C 8/5
    0.9" while flat (his own entry was slippage-blocked and its cooldown retry
    abandoned because Fluid's position on that OSI counted as "already open")
    and closed Fluid's MU 950C — entry $4.27, exit $0.76, -$350.99.
    Set position_owner_scope=osi to restore the legacy behavior.
    """
    if not rows or not _owner_scoped():
        return rows
    author = (getattr(alert, "_alert_author", "") or "").strip()
    kept, dropped = [], []
    for p in rows:
        (kept if _pos_owned_by(p, author) else dropped).append(p)
    # Read by _sell_from_alert: distinguishes "nobody holds this" from
    # "someone else holds this", which need different handling.
    alert._owner_scope_dropped = [(p.osi_symbol, (p.author or "").strip()) for p in dropped]
    for p in dropped:
        log.warning(
            "[OWNER_SCOPE] %s SELL from %r skipped — position is owned by %r "
            "(remaining=%d). Their exit does not close someone else's book.",
            p.osi_symbol, author, (p.author or "").strip(), p.remaining,
        )
        order_logger.warning(
            "SELL SKIPPED | %s | trigger_author=%s | position_author=%s | "
            "cross-analyst exit blocked by position_owner_scope=author",
            p.osi_symbol, author, (p.author or "").strip(),
        )
        # _match_open_positions is sync and reachable off-loop (tests, reconcile
        # helpers) — same guard as _mark_alert_status.
        try:
            asyncio.get_running_loop().create_task(trade_logger.log_alert_blocked(
                action="SELL", osi=p.osi_symbol,
                reason=(f"{author} called an exit on a position opened by "
                        f"{(p.author or '').strip()} — not their contract to close."),
                author=author, block_type="CROSS_ANALYST_SELL",
            ))
        except RuntimeError as e:
            log.debug("Cannot log cross-analyst SELL block: no event loop (%s)", e)
    return kept


def _CLOSE_ALL_REQUIRES_SYMBOL():
    # When true (default), a SELL parsed as close_all but with NO symbol/contract
    # is never executed — it is dropped SKIPPED. 365d of Discord history showed
    # blank-symbol close_all is ~entirely parser noise (chitchat "all time high",
    # "call it a day", market commentary "they are selling everything") or a real
    # exit whose ticker the parser failed to extract — never a genuine "flatten my
    # entire book" instruction. Set false to restore the scope-gated execution.
    return cfg.getboolean("trading", "close_all_requires_symbol", fallback=True)

# Profile-aware variants. Pass a Position (or any obj with osi_symbol/author/
# expiry/strategy_tag) so SPX/index 0DTE can run a tighter re-peg cadence
# than the global default. Fall back to the [trading] knob when no profile
# override exists.
def _pos_close_all_repeg_seconds(pos):
    if not pos or not _entry_overrides_enabled():
        return _CLOSE_ALL_REPEG_SECONDS()
    return _pcfg_float(pos, "close_all_repeg_seconds", _CLOSE_ALL_REPEG_SECONDS())

def _pos_close_all_repeg_max_attempts(pos):
    if not pos or not _entry_overrides_enabled():
        return _CLOSE_ALL_REPEG_MAX_ATTEMPTS()
    return _pcfg_int(pos, "close_all_repeg_max_attempts", _CLOSE_ALL_REPEG_MAX_ATTEMPTS())
# Newer DISCORD SELL must be allowed to displace a stale DISCORD pending.
# Without this, a partial SELL @ stale limit can block a follow-up ALL OUT
# from the same analyst (2026-05-08 SPX 7380P: partial $4.60 sat 17min,
# blocked ALL OUT $2.55 → manual cleanup at $2.79 = -13%, ~$120 swing).
def _DISCORD_DISPLACES_DISCORD(): return cfg.getboolean("trading", "discord_displaces_discord", fallback=True)
# First-sell-closes-all: when true, the analyst's FIRST SELL on a position
# (even a "1/4" partial) exits the ENTIRE remaining position instead of the
# signalled fraction. Since the first sell empties the book, later portions are
# no-ops — so effectively any analyst SELL becomes a full exit. Also overrides
# the post-PT2 runner guard (analyst exit = flat, always). Default OFF; profile-
# aware so it can be scoped per analyst. One-flip revert via portal.
def _FIRST_SELL_CLOSES_ALL(): return cfg.getboolean("trading", "first_sell_closes_all", fallback=False)
def _pos_first_sell_closes_all(pos):
    if not pos or not _entry_overrides_enabled():
        return _FIRST_SELL_CLOSES_ALL()
    return _pcfg_bool(pos, "first_sell_closes_all", _FIRST_SELL_CLOSES_ALL())
# Partial DISCORD SELL re-peg: chase the bid every N seconds so a 1/2-out
# limit doesn't sit while price collapses through it. No final widen
# (partials are not forced exits).
def _PARTIAL_REPEG_SECONDS(): return cfg.getfloat("trading", "partial_repeg_seconds", fallback=30.0)
def _PARTIAL_REPEG_MAX_ATTEMPTS(): return cfg.getint("trading", "partial_repeg_max_attempts", fallback=4)
# On the final partial re-peg attempt, widen to a marketable limit so the
# analyst's exit fills instead of the lane giving up and stranding the position
# (SPXW 7480C 2026-07-08). Set false to restore the old give-up behaviour.
def _PARTIAL_REPEG_FINAL_MARKETABLE(): return cfg.getboolean("trading", "partial_repeg_final_marketable", fallback=True)
def _PARTIAL_REPEG_FINAL_SLIPPAGE_PCT(): return cfg.getfloat("trading", "partial_repeg_final_slippage_pct", fallback=20.0)


def _snap_down_to_tick(px: Decimal) -> Decimal:
    """Floor a SELL limit to the option's min price variation ($0.05 below $3,
    $0.10 at/above $3) so IBKR doesn't reject it as non-conforming (Warning 110
    — cost SPXW 7480C a wasted re-peg attempt 2026-07-08). Floor (not nearest)
    keeps the limit marketable for a SELL. Never returns below one tick."""
    inc = Decimal("0.05") if px < Decimal("3.00") else Decimal("0.10")
    # Decimal floor-division — float math.floor(4.60/0.10) rounds to 45 (=4.50)
    # because 4.60/0.10 is 45.9999… in binary float; (px // inc) is exact.
    snapped = ((px // inc) * inc).quantize(Decimal("0.01"))
    return max(snapped, inc)

def _pos_partial_repeg_seconds(pos):
    if not pos or not _entry_overrides_enabled():
        return _PARTIAL_REPEG_SECONDS()
    return _pcfg_float(pos, "partial_repeg_seconds", _PARTIAL_REPEG_SECONDS())

def _pos_partial_repeg_max_attempts(pos):
    if not pos or not _entry_overrides_enabled():
        return _PARTIAL_REPEG_MAX_ATTEMPTS()
    return _pcfg_int(pos, "partial_repeg_max_attempts", _PARTIAL_REPEG_MAX_ATTEMPTS())
# Slippage cooldown retry: when a BUY is blocked because live > alert ×
# (1 + max_entry_slippage_pct/100), poll the quote every poll_seconds for
# retry_seconds. First poll where live drops back under the cap, place
# the BUY at the cooled-down price. Set retry_seconds=0 to disable
# (legacy behaviour: block is terminal until next alert).
def _SLIPPAGE_COOLDOWN_RETRY_SECONDS(): return cfg.getint("trading", "slippage_cooldown_retry_seconds", fallback=180)
def _SLIPPAGE_COOLDOWN_POLL_SECONDS(): return cfg.getfloat("trading", "slippage_cooldown_poll_seconds", fallback=3.0)


# ── Per-alert entry knobs (resolved through [profile:*] cascade) ────────────
# When entry_profile_overrides_enabled=false in [trading], these all fall
# through to the [trading] section — full one-knob rollback to legacy.
#
# Knobs in scope:
#   buy_slippage                    — % cushion above base for BUY limit
#   max_entry_slippage_pct          — hard-reject if live > alert × (1+pct)
#   max_pending_minutes             — auto-cancel unfilled BUY after N min
#   entry_base_price_mode           — live_or_alert (default) | max_live_alert
#                                     | alert_first (start at analyst price,
#                                     re-peg up only if not filled)
#   buy_repeg_enabled               — walk BUY limit up if unfilled
#   buy_repeg_seconds               — repeg cadence
#   buy_repeg_max_attempts          — cap on repeg attempts
#   buy_repeg_cap_pct               — never bid above alert × (1 + cap_pct/100)
#   exit_base_price_mode            — live (default) | alert_first (start SELL
#                                     at analyst price, re-peg down only if
#                                     not filled)

def _alert_buy_slippage(alert) -> Decimal:
    if not _entry_overrides_enabled():
        return _BUY_SLIPPAGE()
    return Decimal(str(_pcfg_get(alert, "buy_slippage", str(_BUY_SLIPPAGE())) or "0.01"))

def _alert_max_entry_slippage_pct(alert) -> float:
    if not _entry_overrides_enabled():
        return _MAX_ENTRY_SLIPPAGE_PCT()
    return _pcfg_float(alert, "max_entry_slippage_pct", _MAX_ENTRY_SLIPPAGE_PCT())

def _alert_max_pending_minutes(alert) -> int:
    if not _entry_overrides_enabled():
        return _MAX_PENDING_MINUTES()
    return _pcfg_int(alert, "max_pending_minutes", _MAX_PENDING_MINUTES())

def _alert_entry_base_price_mode(alert) -> str:
    if not _entry_overrides_enabled():
        return "live_or_alert"
    v = _pcfg_get(alert, "entry_base_price_mode", "live_or_alert") or "live_or_alert"
    return str(v).strip().lower()

def _honors_message_qty(alert) -> bool:
    """True when this alert's stated contract count must beat ignore_message_qty.

    Sniper restarted his $5k small-account challenge on 2026-07-30 and now posts
    the exact size on those trades ("BOUGHT CRM 180C 7/31 1.63 ‼️ - 2 on small
    account"). We want to mirror HIS count on those, while every other alert —
    his main-account trades and every other analyst — keeps sizing from
    size_default. Needs BOTH:
      • the alert carries the small-account marker (parser.small_account), and
      • the author matches [trading] message_qty_authors (comma list, substring).
    Empty author list (the default) = feature off, one-knob rollback.
    """
    if not getattr(alert, "small_account", False) or not getattr(alert, "qty", None):
        return False
    toks = [t.strip().lower()
            for t in (cfg.get("trading", "message_qty_authors", fallback="") or "").split(",")
            if t.strip()]
    if not toks:
        return False
    author = (getattr(alert, "_alert_author", "") or "").lower()
    return any(t in author for t in toks)


def _detached_alert_stub(alert_row):
    """Snapshot an Alert row into a plain object for a background fill task.

    Passing the ORM row across an asyncio.create_task boundary detaches it, and
    any lazy attribute access in _finalize_buy_fill then raises
    DetachedInstanceError — hence a snapshot. The hazard is that this is a
    hand-listed field set, so anything the fill path reads and this forgets
    silently degrades instead of failing: first Position.author (cooldown-retry
    entries landed authorless, so their SELL rows showed no analyst), then the
    lane tag (2026-08-12 CRWV 120C opened untagged and its exit row carried no
    SMALL_ACCT badge, because Order.strategy_tag copies pos.strategy_tag).
    Anything _finalize_buy_fill or _stamp_position_author reads belongs here.
    """
    from types import SimpleNamespace
    tag = getattr(alert_row, "strategy_tag", None)
    return SimpleNamespace(
        osi_symbol=alert_row.osi_symbol,
        symbol=alert_row.symbol,
        expiry=alert_row.expiry,
        strike=alert_row.strike,
        option_type=alert_row.option_type,
        _reentry_tag=getattr(alert_row, "_reentry_tag", None),
        _alert_author=(alert_row.author or "").strip() or None,
        strategy_tag=tag,
        small_account=(tag == "SMALL_ACCT"),
    )


def _small_account_only_blocks(alert) -> bool:
    """True when this instance is dedicated to the small-account challenge and
    the alert is a BUY without the ‼ marker. SELLs never block — his exit posts
    usually drop the marker, and blocking one would strand an open position."""
    return (getattr(alert, "action", "") == "BUY"
            and cfg.getboolean("trading", "small_account_only", fallback=False)
            and not getattr(alert, "small_account", False))


def _spreads_only_blocks(alert) -> bool:
    """True when this instance trades vertical credit spreads and nothing else.

    Spreads never reach this executor — SpreadExecutor handles them on its own
    path — so this only has to refuse single-leg BUYs.

    An author whitelist cannot express this rule. The spread analyst also posts
    ordinary single-leg alerts in four other rooms that forward here (486 of
    them across the 2yr dumps), so whitelisting him to let his spreads through
    would quietly start mirroring those too. small_account_only happens to block
    them today only because he never uses the other analyst's ‼ marker — real
    protection, but incidental, and it disappears the moment that unrelated flag
    is flipped. This states the intent directly instead.

    SELLs fall through, as with every other entry gate here: an instance that
    stops taking entries must still be able to close what it already holds.
    """
    return (getattr(alert, "action", "") == "BUY"
            and cfg.getboolean("trading", "spreads_only", fallback=False))


def _alert_buy_repeg_enabled(alert) -> bool:
    if not _entry_overrides_enabled():
        return False
    return _pcfg_bool(alert, "buy_repeg_enabled", False)

def _alert_buy_repeg_seconds(alert) -> float:
    return float(_pcfg_get(alert, "buy_repeg_seconds", "15") or "15")

def _alert_buy_repeg_max_attempts(alert) -> int:
    return _pcfg_int(alert, "buy_repeg_max_attempts", 4)

def _alert_buy_repeg_cap_pct(alert) -> float:
    return _pcfg_float(alert, "buy_repeg_cap_pct", 25.0)

def _alert_exit_base_price_mode(alert) -> str:
    """SELL-side counterpart to entry_base_price_mode. Default 'live' keeps
    legacy behavior (compute_limit derives from base × (1 - sell_slippage)).
    'alert_first' starts at the analyst-typed SELL price exactly, then lets
    the partial/close_all re-peg watcher walk DOWN toward live bid."""
    if not _entry_overrides_enabled():
        return "live"
    v = _pcfg_get(alert, "exit_base_price_mode", "live") or "live"
    return str(v).strip().lower()

# Pre-submit guard: clamp SELL limit so it sits within max_limit_band_pct of
# the latest cached mark. Public.com rejects orders whose limit is too far
# from NBBO (e.g. "Cancelled due to too aggressive pricing" when SELL limit
# is ~10%+ above last). Default 8% leaves room for legitimate analyst-driven
# SELL limits above market but blocks fat-finger / stale-price submissions.
# Override per profile (e.g. spx_index could tighten to 5%).
def _max_limit_band_pct(obj) -> float:
    return _pcfg_float(obj, "max_limit_band_pct", 8.0)

def _clamp_sell_limit_to_band(pos, limit_px: Decimal) -> tuple[Decimal, bool]:
    """If pos.current_price is fresh enough, clamp SELL limit_px to
    live × (1 + band/100). Returns (clamped_px, was_clamped). When
    current_price is unavailable, returns original (best-effort —
    re-peg watcher catches the resulting reject as fallback)."""
    live = getattr(pos, "current_price", None)
    if not live or live <= 0:
        return limit_px, False
    band = _max_limit_band_pct(pos) / 100.0
    if band <= 0:
        return limit_px, False
    live_dec = Decimal(str(round(float(live), 2)))
    ceiling = live_dec * (Decimal("1") + Decimal(str(band)))
    if limit_px <= ceiling:
        return limit_px, False
    inc = Decimal("0.05") if ceiling < Decimal("3.00") else Decimal("0.10")
    clamped = (Decimal(math.floor(float(ceiling) / float(inc))) * inc).quantize(Decimal("0.01"))
    clamped = max(clamped, inc)
    return clamped, True

def _osi_for_broker(osi: str) -> str:
    """Public.com routes SPX weeklies/dailies under root 'SPXW'; only 3rd-Friday
    monthlies use plain 'SPX'. Alerts come in as SPX regardless, so translate here.
    Safe no-op for already-SPXW or non-SPX symbols."""
    if not osi or not osi.startswith("SPX") or osi.startswith("SPXW"):
        return osi
    try:
        yy, mm, dd = int(osi[3:5]), int(osi[5:7]), int(osi[7:9])
        d = date(2000 + yy, mm, dd)
    except ValueError:
        return osi
    is_third_friday = (d.weekday() == 4 and 15 <= d.day <= 21)
    return osi if is_third_friday else "SPXW" + osi[3:]


def _size_contracts() -> dict:
    return {
        "XS":     cfg.getint("trading", "size_xs",      fallback=1),
        "SMALL":  cfg.getint("trading", "size_small",   fallback=1),
        "MEDIUM": cfg.getint("trading", "size_medium",  fallback=2),
        "LARGE":  cfg.getint("trading", "size_large",   fallback=3),
        "FULL":   cfg.getint("trading", "size_full",    fallback=4),
        "XL":     cfg.getint("trading", "size_xl",      fallback=5),
        "": cfg.getint("trading", "size_default",       fallback=1),
    }


class PublicExecutor:
    def __init__(self, ws_manager):
        self.ws_manager = ws_manager
        self._client: Any | None = None
        self._sdk: dict[str, Any] | None = None
        # Strong refs to background fill-poll tasks. asyncio holds only a WEAK
        # ref to a create_task result, so an un-referenced poll task can be
        # garbage-collected mid-await and silently stop booking the fill.
        self._poll_tasks: "set[asyncio.Task]" = set()

    def _track_task(self, coro) -> "asyncio.Task":
        """create_task that won't be GC'd mid-run and surfaces exceptions.
        Used for background fill-poll / OCO-watcher tasks."""
        task = asyncio.create_task(coro)
        self._poll_tasks.add(task)

        def _done(t: "asyncio.Task") -> None:
            self._poll_tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                log.error("Fill-poll task failed: %s", t.exception(), exc_info=t.exception())

        task.add_done_callback(_done)
        return task

    async def _get_client(self):
        if self._client is None:
            sdk = self._load_sdk()
            self._client = sdk["AsyncPublicApiClient"](
                auth_config=sdk["ApiKeyAuthConfig"](api_secret_key=API_KEY),
                config=sdk["AsyncPublicApiClientConfiguration"](default_account_number=ACCOUNT_NUM),
            )
        return self._client

    def _load_sdk(self) -> dict[str, Any]:
        if self._sdk is None:
            from public_api_sdk import (
                ApiKeyAuthConfig,
                AsyncPublicApiClient,
                AsyncPublicApiClientConfiguration,
                InstrumentType,
                OpenCloseIndicator,
                OrderExpirationRequest,
                OrderInstrument,
                OrderRequest,
                OrderSide,
                OrderType,
                TimeInForce,
            )
            from public_api_sdk.exceptions import APIError, NotFoundError

            self._sdk = {
                "ApiKeyAuthConfig": ApiKeyAuthConfig,
                "AsyncPublicApiClient": AsyncPublicApiClient,
                "AsyncPublicApiClientConfiguration": AsyncPublicApiClientConfiguration,
                "InstrumentType": InstrumentType,
                "OpenCloseIndicator": OpenCloseIndicator,
                "OrderExpirationRequest": OrderExpirationRequest,
                "OrderInstrument": OrderInstrument,
                "OrderRequest": OrderRequest,
                "OrderSide": OrderSide,
                "OrderType": OrderType,
                "TimeInForce": TimeInForce,
                "APIError": APIError,
                "NotFoundError": NotFoundError,
            }
        return self._sdk

    async def execute(self, alert, alert_db_id: int, alert_author: str = "", content_hash: str = "", _pipeline_timer: PipelineTimer = None):
        # Normalize SPX → SPXW (or no-op) once at entry so DB rows, dedup,
        # position lookups, and broker calls all share the same OSI key.
        # Prevents phantom SPX rows that reconciler later has to clean up.
        if alert.osi_symbol:
            new_osi = _osi_for_broker(alert.osi_symbol)
            if new_osi != alert.osi_symbol:
                alert.osi_symbol = new_osi
                # Keep alert.symbol in sync so symbol-only SELL matches the right rows.
                if alert.symbol == "SPX":
                    alert.symbol = "SPXW"
        # Stamp the opening analyst onto the alert so it reaches the position
        # at entry (Position.author) and the SELL matcher. _alert_author was
        # read downstream but NEVER written → Position.author stayed NULL for
        # every row, silently disabling per-author exit profiles and making
        # author-scoped exits impossible. 2026-06-11 incident root enabler.
        alert._alert_author = alert_author
        timer = _pipeline_timer or PipelineTimer(osi_symbol=alert.osi_symbol, action=alert.action, author=alert_author)

        # Challenge lane. [trading] small_account_only=true dedicates THIS
        # instance to the analyst's small-account (‼) challenge: every other
        # BUY is ignored. SELLs always fall through — his exit posts usually
        # drop the marker ("SOLD 1/2"), and blocking one would strand an open
        # position. Default false = feature off, one-knob rollback.
        if _spreads_only_blocks(alert):
            reason = (f"SPREADS_ONLY: spreads_only=true — this instance trades vertical credit "
                      f"spreads only, so this single-leg BUY on {alert.osi_symbol} was not taken.")
            guardrail_logger.info("BLOCKED | BUY %s | reason=%s | author=%s",
                                  alert.osi_symbol, reason, alert_author)
            db = SessionLocal()
            try:
                self._mark_alert_status(db, alert_db_id, "SKIPPED", reason)
                db.commit()
            finally:
                db.close()
            timer.finish("SPREADS_ONLY")
            return

        if _small_account_only_blocks(alert):
            reason = (f"NOT_SMALL_ACCOUNT: small_account_only=true — this instance mirrors only "
                      f"small-account (‼) entries, and this BUY on {alert.osi_symbol} carries no marker.")
            guardrail_logger.info("BLOCKED | BUY %s | reason=%s | author=%s",
                                  alert.osi_symbol, reason, alert_author)
            db = SessionLocal()
            try:
                self._mark_alert_status(db, alert_db_id, "SKIPPED", reason)
                db.commit()
            finally:
                db.close()
            timer.finish("NOT_SMALL_ACCOUNT")
            return

        # ── Guardrails check ──────────────────────────────────────────────
        timer.mark("guardrails_start")
        _honor_qty = _honors_message_qty(alert)
        qty = self._contracts_for_size(alert.size_tag, getattr(alert, 'qty', None), alert.osi_symbol,
                                       honor_qty=_honor_qty)
        if getattr(alert, 'qty', None) and (
            _honor_qty or not cfg.getboolean("trading", "ignore_message_qty", fallback=False)
        ):
            log.info("[QTY] Using explicit contract count from message: %d%s",
                     alert.qty, " (small-account author)" if _honor_qty else "")

        # Profile-gated analyst "sized to 0" block. Some analysts post BUY
        # alerts where they explicitly tag the trade as "sized to 0" / "size 0"
        # — meaning they're flagging the idea but risking nothing themselves.
        # When the profile opts in (block_analyst_sized_to_zero=true), refuse
        # to mirror those trades.
        if _entry_overrides_enabled() and _pcfg_bool(alert, "block_analyst_sized_to_zero", False):
            _raw = (getattr(alert, "raw_text", "") or "")
            if _SIZED_TO_ZERO_RE.search(_raw):
                reason = (f"ANALYST_SIZED_TO_ZERO: the analyst themselves sized this {alert.action} on {alert.osi_symbol} "
                          f"at zero contracts (a watch-only mention) — nothing to mirror, so nothing was traded.")
                guardrail_logger.warning("BLOCKED | %s %s | qty=%d | reason=%s | author=%s",
                                         alert.action, alert.osi_symbol, qty, reason, alert_author)
                log.warning("[SIZED_TO_ZERO] %s | text=%r", reason, _raw[:200])
                db = SessionLocal()
                try:
                    self._mark_alert_status(db, alert_db_id, "SKIPPED", reason)
                    db.commit()
                finally:
                    db.close()
                asyncio.create_task(trade_logger.log_alert_blocked(
                    action=alert.action, osi=alert.osi_symbol, reason=reason,
                    author=alert_author, block_type="SIZED_TO_ZERO",
                ))
                timer.finish("SIZED_TO_ZERO")
                return

        # Same-strike same-day re-entry guard (BUY only). Detects late-chase
        # re-buys after a closed winner — e.g. SPX 7275C 2026-05-05: cycle 1
        # closed +$228, cycle 2 re-bought $2.10 (3× original $0.70) and lost
        # $305. Either reject the chase, clamp index qty to a probe, or tag
        # the new position for tighter exits.
        reentry_tag = None
        if alert.action == "BUY":
            decision, rg_reason, rg_ctx = guardrails.check_reentry_guard(
                alert.osi_symbol, alert.price,
            )
            if decision == "reject":
                guardrail_logger.warning("BLOCKED | BUY %s | qty=%d | reason=%s | author=%s",
                                         alert.osi_symbol, qty, rg_reason, alert_author)
                log.warning("[REENTRY] %s", rg_reason)
                db = SessionLocal()
                try:
                    self._mark_alert_status(db, alert_db_id, "SKIPPED", rg_reason)
                    db.commit()
                finally:
                    db.close()
                asyncio.create_task(trade_logger.log_alert_blocked(
                    action="BUY", osi=alert.osi_symbol, reason=rg_reason,
                    author=alert_author, block_type="REENTRY_BLOCK",
                ))
                timer.finish("REENTRY_BLOCK")
                return
            elif decision == "probe":
                if qty > 1:
                    log.warning("[REENTRY] %s — qty %d → 1", rg_reason, qty)
                    qty = 1
                reentry_tag = "RE-ENTRY"
            elif decision == "flag":
                log.info("[REENTRY] %s", rg_reason)
                reentry_tag = "RE-ENTRY"

        # ── Whale-tracker lane caps ───────────────────────────────────────
        # Whale alerts (strategy_tag stamped upstream by discord_listener when
        # whale_lane_enabled) get their own hard limits, independent of the
        # global [guardrails] knobs: max contracts (default 1) and max trade
        # cost (default $500). One contract that still breaks the cost cap is
        # blocked outright — keeps premium ≤ cost/100 per the user's rule.
        if alert.action == "BUY" and getattr(alert, "strategy_tag", "") == "WHALE":
            w_max = _pcfg_int(alert, "whale_max_contracts", 1)
            if qty > w_max:
                log.info("[WHALE] qty %d → %d (whale_max_contracts)", qty, w_max)
                qty = w_max
            w_cost = _pcfg_float(alert, "max_trade_cost", 500.0)
            if alert.price is not None and float(alert.price) * qty * 100 > w_cost:
                _c = float(alert.price) * qty * 100
                reason = (f"WHALE_MAX_COST: this whale entry needs ${_c:.0f} but the whale lane's budget cap is ${w_cost:.0f} "
                          f"— skipped. Whale trades are deliberately kept small "
                          f"(${alert.price} x {qty} x 100)")
                guardrail_logger.warning("BLOCKED | BUY %s | qty=%d | reason=%s | author=%s",
                                         alert.osi_symbol, qty, reason, alert_author)
                log.warning("[WHALE] %s", reason)
                db = SessionLocal()
                try:
                    self._mark_alert_status(db, alert_db_id, "SKIPPED", reason)
                    db.commit()
                finally:
                    db.close()
                asyncio.create_task(trade_logger.log_alert_blocked(
                    action="BUY", osi=alert.osi_symbol, reason=reason,
                    author=alert_author, block_type="WHALE_MAX_COST",
                ))
                timer.finish("WHALE_MAX_COST")
                return

        # Run the guardrail (synchronous SQLite work — open-count, daily-trade
        # count, daily-P&L) off the event loop so a slow query / WAL checkpoint
        # can't freeze ingest, the position monitor, and websocket broadcasts.
        # Safe because execution is serialized through the single order worker
        # (order_queue / [trading] serialized_order_execution) — only one
        # guardrail runs at a time, so the in-memory dedup dicts aren't raced
        # across threads. (With the flag off, the legacy concurrent path applies
        # the same last-write-wins dedup as before — no worse than pre-serialize.)
        allowed, reason = await asyncio.to_thread(
            guardrails.check_guardrails,
            action=alert.action,
            osi_symbol=alert.osi_symbol,
            price=alert.price,
            qty=qty,
            alert_author=alert_author,
            content_hash=content_hash,
            alert_db_id=alert_db_id,
            size_tag=alert.size_tag,
        )
        timer.mark("guardrails_done")

        # Append-only provenance record of the decision (off the guardrail's
        # latency path; best-effort, flag-gated). Offloaded so the DB write
        # can't stall the loop.
        try:
            await asyncio.to_thread(
                guardrails.audit_order_decision,
                action=alert.action, osi_symbol=alert.osi_symbol,
                author=alert_author, content_hash=content_hash,
                allowed=allowed, reason=reason, qty=qty,
                price=alert.price,
            )
        except Exception:
            pass

        if not allowed:
            guardrail_logger.warning("BLOCKED | %s %s | qty=%d | reason=%s | author=%s", alert.action, alert.osi_symbol, qty, reason, alert_author)
            log.warning("[GUARDRAIL] BLOCKED %s %s: %s", alert.action, alert.osi_symbol, reason)
            db = SessionLocal()
            try:
                self._mark_alert_status(db, alert_db_id, "SKIPPED", f"Guardrail: {reason}")
                db.commit()
            finally:
                db.close()
            asyncio.create_task(trade_logger.log_order_cancelled(
                osi=alert.osi_symbol, side=alert.action,
                reason=f"Guardrail: {reason}",
            ))
            # ← NEW: post clear rejection reason to #trade-logs
            asyncio.create_task(trade_logger.log_alert_blocked(
                action=alert.action,
                osi=alert.osi_symbol,
                reason=reason,
                author=alert_author,
                block_type="BLOCKED",
            ))
            timer.finish(f"BLOCKED:{reason[:40]}")
            return
        guardrail_logger.info("PASSED | %s %s | qty=%d | author=%s", alert.action, alert.osi_symbol, qty, alert_author)

        # ── AI Confidence Scorer (BUY only) ───────────────────────────────
        # Skip scorer for index symbols (SPX/NDX/RUT/etc). Index 0DTE behavior
        # is dictated by the underlying, not the analyst — TC's 29% win rate
        # downgraded the SPX 7180P qty 4→1 on 2026-05-04, killing the run-up.
        # Index sizing is already capped by index_max_contracts, so per-author
        # win-rate downgrades do more harm than good here.
        from app.risk.guardrails import _is_index_symbol as _is_idx
        if alert.action == "BUY" and ai_scorer._SCORER_ENABLED() and not _is_idx(alert.osi_symbol):
            timer.mark("scorer_start")
            score_result = ai_scorer.score_alert(
                action=alert.action,
                author=alert_author,
                symbol=alert.symbol,
                osi_symbol=alert.osi_symbol,
                price=float(alert.price) if alert.price else None,
                size_tag=alert.size_tag,
            )
            score = score_result["score"]
            adjusted_qty = ai_scorer.adjust_quantity(qty, score, alert.size_tag)
            timer.mark("scorer_done")

            scorer_logger.info(
                "SCORE | %s | score=%d | qty %d→%d | author=%s | breakdown=%s | reason=%s",
                alert.osi_symbol, score, qty, adjusted_qty, alert_author,
                score_result["breakdown"], score_result["reason"],
            )

            await self.ws_manager.broadcast({
                "type": "score_update",
                "data": {
                    "osi": alert.osi_symbol,
                    "symbol": alert.symbol,
                    "author": alert_author,
                    "score": score,
                    "original_qty": qty,
                    "adjusted_qty": adjusted_qty,
                    "breakdown": score_result["breakdown"],
                    "reason": score_result["reason"],
                },
            })

            if adjusted_qty <= 0:
                scorer_logger.warning("SKIP | %s | score=%d below min | reason=%s", alert.osi_symbol, score, score_result["reason"])
                log.warning("[SCORER] SKIPPED %s: score=%d (%s)", alert.osi_symbol, score, score_result["reason"])
                db = SessionLocal()
                try:
                    self._mark_alert_status(db, alert_db_id, "SKIPPED", f"Low confidence: {score_result['reason']}")
                    db.commit()
                finally:
                    db.close()
                asyncio.create_task(trade_logger.log_order_cancelled(
                    osi=alert.osi_symbol, side="BUY",
                    reason=f"Scorer: {score_result['reason']}",
                ))
                # ← NEW: clear rejection reason to Discord
                asyncio.create_task(trade_logger.log_alert_blocked(
                    action="BUY",
                    osi=alert.osi_symbol,
                    reason=f"Score {score}/100 — {score_result['reason']}",
                    author=alert_author,
                    block_type="SKIPPED",
                ))
                timer.finish(f"SCORER_SKIP:score={score}")
                return

            # Override quantity with scored amount
            alert._scored_qty = adjusted_qty
            log.info("[SCORER] %s score=%d, qty %d→%d (%s)", alert.osi_symbol, score, qty, adjusted_qty, score_result["reason"])

        # ── Kelly Criterion position sizing cap (BUY only) ───────────────────
        if alert.action == "BUY" and kelly_sizer._KELLY_ENABLED() and alert.price:
            timer.mark("kelly_start")
            try:
                from app.execution.broker_router import fetch_account_balance
                balance = await fetch_account_balance()
                account_value = balance.get("total_value", 0) or balance.get("buying_power", 0)

                current_qty = getattr(alert, "_scored_qty", None) or self._contracts_for_size(
                    alert.size_tag, getattr(alert, "qty", None), alert.osi_symbol,
                    honor_qty=_honors_message_qty(alert))
                capped_qty, kelly_result = kelly_sizer.apply_kelly_cap(
                    qty=current_qty,
                    price=float(alert.price),
                    account_balance=account_value,
                    author=alert_author,
                )
                timer.mark("kelly_done")

                scorer_logger.info(
                    "KELLY | %s | kelly_f=%.4f | half=%.4f | qty %d→%d | win_rate=%.1f%% | payoff=%.2f | trades=%d | reason=%s",
                    alert.osi_symbol, kelly_result["kelly_fraction"], kelly_result["half_kelly_fraction"],
                    current_qty, capped_qty, kelly_result["win_rate"] * 100,
                    kelly_result["payoff_ratio"], kelly_result["trades_analyzed"], kelly_result["reason"],
                )

                await self.ws_manager.broadcast({
                    "type": "kelly_update",
                    "data": {
                        "osi": alert.osi_symbol,
                        "symbol": alert.symbol,
                        "original_qty": current_qty,
                        "kelly_qty": capped_qty,
                        "kelly_fraction": kelly_result["kelly_fraction"],
                        "half_kelly": kelly_result["half_kelly_fraction"],
                        "max_contracts": kelly_result["max_contracts"],
                        "win_rate": kelly_result["win_rate"],
                        "payoff_ratio": kelly_result["payoff_ratio"],
                        "trades_analyzed": kelly_result["trades_analyzed"],
                        "reason": kelly_result["reason"],
                    },
                })

                if capped_qty <= 0 and kelly_result["kelly_fraction"] < 0:
                    scorer_logger.warning("KELLY BLOCK | %s | no edge | reason=%s", alert.osi_symbol, kelly_result["reason"])
                    log.warning("[KELLY] BLOCKED %s: no edge (%s)", alert.osi_symbol, kelly_result["reason"])
                    db = SessionLocal()
                    try:
                        self._mark_alert_status(db, alert_db_id, "SKIPPED", f"Kelly: {kelly_result['reason']}")
                        db.commit()
                    finally:
                        db.close()
                    asyncio.create_task(trade_logger.log_order_cancelled(
                        osi=alert.osi_symbol, side="BUY",
                        reason=f"Kelly: {kelly_result['reason']}",
                    ))
                    # ← NEW: clear rejection reason to Discord
                    asyncio.create_task(trade_logger.log_alert_blocked(
                        action="BUY",
                        osi=alert.osi_symbol,
                        reason=kelly_result["reason"],
                        author=alert_author,
                        block_type="KELLY",
                    ))
                    timer.finish("KELLY_BLOCK:no_edge")
                    return

                if capped_qty != current_qty:
                    alert._scored_qty = capped_qty
                    log.info("[KELLY] %s qty %d→%d (%s)", alert.osi_symbol, current_qty, capped_qty, kelly_result["reason"])
            except Exception as exc:
                log.error("[KELLY] Error computing Kelly for %s: %s", alert.osi_symbol, exc)
                timer.mark("kelly_error")

        # ── Place order ──────────────────────────────────────────────────
        # Stash re-entry tag so _buy / _finalize_buy_fill can stamp it on the
        # Position row (read by position_monitor for tighter exits).
        if reentry_tag:
            alert._reentry_tag = reentry_tag
            # Probe-clamp may have lowered qty below scorer/kelly result; the
            # smallest of the three wins (probe is the strictest cap).
            existing_scored = getattr(alert, "_scored_qty", None)
            if existing_scored is None or qty < existing_scored:
                alert._scored_qty = qty

        timer.mark("order_start")
        if alert.action == "BUY":
            await self._buy(alert, alert_db_id, timer)
        else:
            await self._sell_from_alert(alert, alert_db_id, timer)

    async def execute_override(self, alert, alert_db_id: int, alert_author: str = ""):
        """
        Force-execute a BUY alert, bypassing ALL risk filters:
          - Guardrails (market hours, max cost, duplicates, daily limits)
          - AI Confidence Scorer (score threshold)
          - Kelly Criterion position sizing

        This is called from the dashboard's 'Force Submit (Override)' / 'Resubmit' button.
        The original qty from size_tag is used; no adjustments are made.

        A clear OVERRIDE audit log entry is written before placement so there's a paper
        trail. The alert status flows: PENDING → FILLED | ERROR (same as normal execute).
        """
        # Same SPX→SPXW normalization as execute() so DB/broker keys align.
        if alert.osi_symbol:
            new_osi = _osi_for_broker(alert.osi_symbol)
            if new_osi != alert.osi_symbol:
                alert.osi_symbol = new_osi
                if alert.symbol == "SPX":
                    alert.symbol = "SPXW"

        # Attribute the override entry identically to a normal execute():
        # Position.author (entry stamp), Order.author, the SELL matcher and
        # author-scoped close_all all read alert._alert_author. Without this an
        # overridden BUY lands "unowned" (NULL author) → invisible to that
        # analyst's blank close_all and to per-author attribution. execute()
        # sets the same field at the top of its body.
        alert._alert_author = alert_author

        from app.core.log_config import PipelineTimer as _PT
        timer = _PT(osi_symbol=alert.osi_symbol, action=alert.action, author=alert_author)

        qty = self._contracts_for_size(alert.size_tag, getattr(alert, 'qty', None), alert.osi_symbol,
                                       honor_qty=_honors_message_qty(alert))

        # ── Audit log — always write an override record ───────────────────
        guardrail_logger.warning(
            "OVERRIDE | %s %s | qty=%d | author=%s | ALL RISK FILTERS BYPASSED (manual resubmit)",
            alert.action, alert.osi_symbol, qty, alert_author,
        )
        log.warning(
            "[OVERRIDE] Bypassing guardrails/scorer/kelly for %s %s x%d (author=%s)",
            alert.action, alert.osi_symbol, qty, alert_author,
        )

        timer.mark("override_start")
        try:
            await self._buy(alert, alert_db_id, timer)
        except Exception as exc:
            db = SessionLocal()
            try:
                self._mark_alert_status(db, alert_db_id, "ERROR", f"Override resubmit failed: {exc}")
                db.commit()
            finally:
                db.close()
            log.error("[OVERRIDE] BUY failed for %s: %s", alert.osi_symbol, exc)

    async def _buy(self, alert, alert_db_id: int, timer: PipelineTimer = None):

        db = SessionLocal()
        db_order = None
        try:
            if alert.price is None:
                self._mark_alert_status(db, alert_db_id, "ERROR", "BUY alert missing entry price")
                if timer:
                    timer.finish("ERROR:missing_price")
                return

            qty = getattr(alert, "_scored_qty", None) or self._contracts_for_size(
                alert.size_tag, getattr(alert, "qty", None), alert.osi_symbol,
                honor_qty=_honors_message_qty(alert))

            # Cross-analyst entry block. Position.osi_symbol is UNIQUE, so a
            # second analyst's BUY on a contract someone else already holds
            # MERGES into the opener's row (_finalize_buy_fill) — their
            # contracts land under the wrong name and every later exit is
            # ambiguous between the two. Owner-scoped exits only hold if a row
            # never has two owners, so refuse the entry instead of merging.
            # Pre-order on purpose: the merge happens at fill time, when the
            # money is already spent. Applies to the override path too — this
            # is a data-integrity invariant, not a risk filter.
            if _owner_scoped() and alert.osi_symbol:
                _held = db.query(Position).filter(
                    Position.osi_symbol == alert.osi_symbol,
                    Position.status.in_(["OPEN", "PARTIAL"]),
                    Position.remaining > 0,
                ).first()
                _me = (getattr(alert, "_alert_author", "") or "").strip()
                if _held is not None and not _pos_owned_by(_held, _me):
                    reason = (
                        f"CROSS_ANALYST_POSITION: {(_held.author or '').strip()} already holds "
                        f"{alert.osi_symbol} ({_held.remaining} contract(s)). One position row "
                        f"can only have one owner, so this entry would merge into theirs and make "
                        f"both analysts' exits ambiguous. Skipped (position_owner_scope=author)."
                    )
                    log.warning("[BUY] %s", reason)
                    guardrail_logger.warning(
                        "BLOCKED | BUY %s | qty=%d | reason=%s | author=%s",
                        alert.osi_symbol, qty, reason, _me,
                    )
                    self._mark_alert_status(db, alert_db_id, "SKIPPED", reason)
                    db.commit()
                    asyncio.create_task(trade_logger.log_alert_blocked(
                        action="BUY", osi=alert.osi_symbol, reason=reason,
                        author=_me, block_type="CROSS_ANALYST_POSITION",
                    ))
                    if timer:
                        timer.finish("CROSS_ANALYST_POSITION")
                    return

            # Slippage-attempt cap: hard skip if this OSI already had enough
            # SLIPPAGE_BLOCK events in the recent window. Stops "block, block,
            # block, fill at the worst price" pattern. Window + max are
            # portal-tunable; default 1 attempt / 300s.
            _slip_max = _SLIPPAGE_RETRY_MAX()
            if _slip_max > 0:
                _attempts = _slippage_attempt_count(alert.osi_symbol)
                if _attempts >= _slip_max:
                    reason = (
                        f"SLIPPAGE_RETRY_CAP: duplicate alert skipped — a BUY for "
                        f"{alert.osi_symbol} was already price-blocked "
                        f"{_attempts}x in the last {_SLIPPAGE_RETRY_WINDOW_S()}s and its "
                        f"auto-retry is still watching for the price to cool. Ignoring "
                        f"this repeat so we don't chase or double-enter."
                    )
                    log.warning("[BUY] %s", reason)
                    self._mark_alert_status(db, alert_db_id, "SKIPPED", reason)
                    db.commit()
                    asyncio.create_task(trade_logger.log_alert_blocked(
                        action="BUY", osi=alert.osi_symbol, reason=reason,
                        author=getattr(alert, "_alert_author", "") or "",
                        block_type="SLIPPAGE_RETRY_CAP",
                    ))
                    if timer:
                        timer.finish("SLIPPAGE_RETRY_CAP")
                    return

            # Quote-aware base price: alert price can be stale (especially
            # 0DTE), causing broker to reject "limit too far from quote".
            # Prefer live quote, but cap chase at +8% over alert to avoid
            # paying +25% slippage on fast-moving alerts (real-world bug
            # 2026-04-30: META 600P alert $3.45 → fill $4.30, +24.6%, halved
            # the trade's gain).
            alert_dec = Decimal(str(alert.price))
            base_price = alert_dec
            CHASE_CAP = Decimal("1.08")
            # Hard-reject gate is independent of CHASE_CAP. Live > alert ×
            # (1 + max_entry_slippage_pct/100) → refuse the trade outright.
            # Today (2026-05-05) blocks SPX 7275C re-entry that filled $2.21
            # vs alert $2.10 (+5.2%) at the literal top of the move.
            slip_cap_pct = _alert_max_entry_slippage_pct(alert)
            base_price_mode = _alert_entry_base_price_mode(alert)
            slip_cap = Decimal("1") + (Decimal(str(slip_cap_pct)) / Decimal("100"))

            # alert_first mode: SKIP the live-quote chase entirely. Initial
            # limit = analyst price with zero slippage cushion. Re-peg walks
            # up toward live if alert price doesn't fill. Lets analyst-chosen
            # entries (e.g. SPX 7420P $5.00) try the named level first instead
            # of immediately paying the spread.
            alert_first_mode = (base_price_mode == "alert_first")
            # Sanity envelope: if live is more than 2× alert, the quote is
            # almost certainly stale or simulated (DRY_RUN simulator can drift
            # far). Don't block on those — let CHASE_CAP handle it. Real
            # late-chase scenarios (today's SPX +5.2%) sit well below 2×.
            slip_sanity_max = alert_dec * Decimal("2.0")
            if alert_first_mode:
                # alert_first mode: skip live-quote chase + slippage gate.
                # Initial bid = analyst price exactly; re-peg walks up from
                # there if not filled within buy_repeg_seconds.
                log.info(
                    "[BUY] alert_first: starting at analyst price $%s "
                    "(re-peg will walk up if unfilled)",
                    alert_dec,
                )
            else:
                try:
                    from app.execution.broker_router import fetch_prices
                    quotes = await fetch_prices([alert.osi_symbol])
                    live_px = quotes.get(alert.osi_symbol) if quotes else None
                    if live_px and live_px > 0 and not _DRY_RUN():
                        live_dec = Decimal(str(round(float(live_px), 2)))
                        if alert_dec * slip_cap < live_dec <= slip_sanity_max:
                            slip_pct_actual = (live_dec - alert_dec) / alert_dec * Decimal("100")
                            _retry_s = _SLIPPAGE_COOLDOWN_RETRY_SECONDS()
                            reason = (
                                f"ENTRY SLIPPAGE BLOCKED: price ran away before we could enter — "
                                f"live ${live_dec} is +{slip_pct_actual:.1f}% over the alert's ${alert_dec} "
                                f"(your cap: {slip_cap_pct:.1f}%). "
                                + (f"Auto-retry will watch {_retry_s}s and enter if it cools back under the cap."
                                   if _retry_s > 0 else "Auto-retry disabled — entry skipped.")
                            )
                            log.warning("[BUY] %s", reason)
                            _record_slippage_attempt(alert.osi_symbol)
                            self._mark_alert_status(db, alert_db_id, "SKIPPED", reason)
                            db.commit()
                            asyncio.create_task(trade_logger.log_alert_blocked(
                                action="BUY", osi=alert.osi_symbol, reason=reason,
                                author=getattr(alert, "_alert_author", "") or "",
                                block_type="SLIPPAGE_BLOCK",
                            ))
                            # Cooldown retry: if live cools back under the cap
                            # within retry window, place the BUY at the cooled
                            # price. Captures primitives (alert object will be
                            # detached after this method returns).
                            if _SLIPPAGE_COOLDOWN_RETRY_SECONDS() > 0:
                                asyncio.create_task(self._retry_buy_on_slippage_cooldown(
                                    osi=alert.osi_symbol,
                                    alert_price=float(alert.price),
                                    alert_db_id=alert_db_id,
                                    qty=qty,
                                    slip_cap_pct=float(slip_cap_pct),
                                ))
                            if timer:
                                timer.finish("SLIPPAGE_BLOCK")
                            return
                    if live_px and live_px > 0:
                        live_dec = Decimal(str(round(float(live_px), 2)))
                        capped = min(live_dec, alert_dec * CHASE_CAP)
                        if base_price_mode == "max_live_alert":
                            # SPX-style profile: never bid below the alert price.
                            # When live is below alert (analyst late and price
                            # dipped), the alert level is what the analyst saw
                            # at signal time; bidding below it means we miss
                            # the entry if price reverses upward. Cap chase
                            # at CHASE_CAP × alert just like the default mode.
                            base_price = max(capped, alert_dec)
                            if live_dec < alert_dec:
                                log.info(
                                    "[BUY] max_live_alert: live $%s below alert $%s — using alert as base",
                                    live_dec, alert_dec,
                                )
                            elif live_dec > alert_dec * CHASE_CAP:
                                log.warning(
                                    "[BUY] max_live_alert: live $%s exceeds alert $%s × %s — capping at $%s",
                                    live_dec, alert_dec, CHASE_CAP, capped,
                                )
                        else:
                            if live_dec > alert_dec * CHASE_CAP:
                                log.warning(
                                    "[BUY] Live quote $%s exceeds alert $%s × %s — capping base at $%s",
                                    live_dec, alert_dec, CHASE_CAP, capped,
                                )
                            elif abs(live_dec - alert_dec) / alert_dec > Decimal("0.10"):
                                log.info(
                                    "[BUY] Live quote $%s differs from alert $%s — using live as base",
                                    live_dec, alert_dec,
                                )
                            base_price = capped
                except Exception as e:
                    log.warning("[BUY] Live quote fetch failed for %s: %s — using alert price", alert.osi_symbol, e)

            # alert_first mode skips the slippage cushion on the FIRST bid; the
            # re-peg watcher reapplies buy_slippage on each retry attempt.
            _initial_slippage = Decimal("0") if alert_first_mode else _alert_buy_slippage(alert)
            limit_px = self._compute_limit(base_price, "BUY", slippage_override=_initial_slippage)
            oid = str(uuid.uuid4())

            db_order = Order(
                public_order_id=oid,
                osi_symbol=alert.osi_symbol,
                side="BUY",
                quantity=qty,
                limit_price=float(limit_px),
                status="PENDING",
                trigger="DISCORD",
                alert_id=alert_db_id,
                author=(getattr(alert, "_alert_author", "") or "").strip() or None,
                strategy_tag=(getattr(alert, "strategy_tag", None)
                              or ("SMALL_ACCT" if getattr(alert, "small_account", False) else None)),
            )
            db.add(db_order)
            db.commit()
            db.refresh(db_order)
            if timer:
                timer.mark("order_created")
            # Fire-and-forget: broker submit must never queue behind dashboard
            # WS I/O (a slow client costs up to the 2s send timeout).
            asyncio.create_task(self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()}))
            asyncio.create_task(trade_logger.log_order_placed(
                side="BUY", osi=alert.osi_symbol, qty=qty,
                limit_price=float(limit_px), trigger="DISCORD", order_id=db_order.id,
            ))

            order_logger.info(
                "PLACED | BUY %s x%d @ $%s (limit) | alert_price=$%s | order_id=%d | oid=%s",
                alert.osi_symbol, qty, limit_px, alert.price, db_order.id, oid,
            )

            if _DRY_RUN():
                log.info("[DRY RUN] BUY %s x%s @ $%s", alert.osi_symbol, qty, limit_px)
                fill_px = float(alert.price)
                await self._finalize_buy_fill(db, db_order, alert, alert_db_id, fill_px, qty)
                if timer:
                    timer.mark("dry_run_filled")
                    timer.finish("FILLED_DRY_RUN")
            else:
                await self._place_on_public(oid, alert.osi_symbol, "BUY", qty, limit_px)
                self._mark_alert_status(db, alert_db_id, "PENDING", linked_order_id=db_order.id)
                db.commit()
                if timer:
                    timer.mark("submitted_to_public")
                    timer.finish("PENDING_ON_PUBLIC")
                log.info("BUY submitted (PENDING): %s x%s @ $%s", alert.osi_symbol, qty, limit_px)
                _metric_order_placed("BUY")
                order_logger.info("SUBMITTED | BUY %s | oid=%s | polling for fill...", alert.osi_symbol, oid)
                _pending_min = _alert_max_pending_minutes(alert)
                self._track_task(self._poll_order_fill(
                    order_db_id=db_order.id,
                    public_oid=oid,
                    alert=alert,
                    alert_db_id=alert_db_id,
                    side="BUY",
                    qty=qty,
                    max_pending_minutes_override=_pending_min,
                ))
                # Per-profile BUY re-peg: walk the limit up if the market
                # runs above our bid. SPX 0DTE moves through stale bids in
                # seconds; default mode leaves this off so non-index BUYs
                # behave exactly as before.
                if _alert_buy_repeg_enabled(alert):
                    asyncio.create_task(self._repeg_buy(
                        order_db_id=db_order.id,
                        alert_osi=alert.osi_symbol,
                        alert_price=float(alert.price),
                        alert_db_id=alert_db_id,
                        qty=qty,
                        repeg_seconds=_alert_buy_repeg_seconds(alert),
                        max_attempts=_alert_buy_repeg_max_attempts(alert),
                        cap_pct=_alert_buy_repeg_cap_pct(alert),
                        buy_slippage=_alert_buy_slippage(alert),
                        author=(getattr(alert, "_alert_author", "") or "").strip() or None,
                        strategy_tag=db_order.strategy_tag,
                    ))

        except Exception as exc:
            self._handle_order_failure(db, db_order, alert_db_id, exc, alert.osi_symbol, "BUY")
            if timer:
                timer.finish(f"ERROR:{exc}")
        finally:
            db.close()

    async def _retry_buy_on_slippage_cooldown(
        self,
        osi: str,
        alert_price: float,
        alert_db_id: int,
        qty: int,
        slip_cap_pct: float,
    ):
        """
        Watch the live quote after a SLIPPAGE_BLOCK. Polls every
        slippage_cooldown_poll_seconds for slippage_cooldown_retry_seconds.
        First poll where live <= alert × (1 + slip_cap_pct/100), place a
        BUY at the cooled-down price.

        Bails on:
          - Position already opened for this OSI (manual entry, etc.)
          - Alert row gone or no longer in SKIPPED state
          - Window expires with no cooldown observed
          - Quote fetch returns nothing for the entire window

        2026-05-08 user request: cooldown spike-and-recover is the most
        common cause of an analyst BUY missing — alert price prints on a
        wick high that retraces within seconds. Polling for ~3 minutes
        catches that without chasing dead setups.
        """
        from app.execution.broker_router import fetch_prices
        retry_s = _SLIPPAGE_COOLDOWN_RETRY_SECONDS()
        poll_s = _SLIPPAGE_COOLDOWN_POLL_SECONDS()
        if retry_s <= 0 or poll_s <= 0:
            return
        alert_dec = Decimal(str(alert_price))
        cap = alert_dec * (Decimal("1") + Decimal(str(slip_cap_pct)) / Decimal("100"))
        deadline = time.time() + retry_s
        log.info(
            "[BUY_COOLDOWN] %s polling every %ss for %ss — retry when live <= $%s (alert=$%s, cap=+%.1f%%)",
            osi, poll_s, retry_s, cap.quantize(Decimal("0.01")), alert_dec, slip_cap_pct,
        )
        while time.time() < deadline:
            await asyncio.sleep(poll_s)

            # Bail if a position got opened by some other path.
            db = SessionLocal()
            try:
                existing_pos = db.query(Position).filter(
                    Position.osi_symbol == osi,
                    Position.status.in_(["OPEN", "PARTIAL"]),
                ).first()
                if existing_pos:
                    log.info("[BUY_COOLDOWN] %s position already open — abandoning retry", osi)
                    return
                # Confirm alert row still in SKIPPED state. If a duplicate
                # alert came in and ran a fresh BUY path, we should not
                # double-place.
                alert_row = db.query(Alert).filter(Alert.id == alert_db_id).first()
                if not alert_row:
                    log.info("[BUY_COOLDOWN] %s alert #%d gone — abandoning retry", osi, alert_db_id)
                    return
                if alert_row.status != "SKIPPED":
                    log.info(
                        "[BUY_COOLDOWN] %s alert #%d status=%s (no longer SKIPPED) — abandoning retry",
                        osi, alert_db_id, alert_row.status,
                    )
                    return
            finally:
                db.close()

            try:
                quotes = await fetch_prices([osi])
                live_px = quotes.get(osi) if quotes else None
            except Exception as e:
                log.warning("[BUY_COOLDOWN] %s quote fetch failed: %s", osi, e)
                continue
            if not live_px or live_px <= 0:
                continue
            live_dec = Decimal(str(round(float(live_px), 2)))
            if live_dec > cap:
                continue

            # Quote cooled — place the BUY at the live price.
            db = SessionLocal()
            db_order = None
            try:
                # Re-check inside transaction to avoid races.
                existing_pos = db.query(Position).filter(
                    Position.osi_symbol == osi,
                    Position.status.in_(["OPEN", "PARTIAL"]),
                ).first()
                if existing_pos:
                    return
                alert_row = db.query(Alert).filter(Alert.id == alert_db_id).first()
                if not alert_row or alert_row.status != "SKIPPED":
                    return
                # Flip alert back from SKIPPED → PENDING so _poll_order_fill
                # → _finalize_buy_fill can mark it FILLED.
                alert_row.status = "PENDING"
                alert_row.error_text = None
                limit_px = self._compute_limit(live_dec, "BUY")
                oid = str(uuid.uuid4())
                db_order = Order(
                    public_order_id=oid,
                    osi_symbol=osi,
                    side="BUY",
                    quantity=qty,
                    limit_price=float(limit_px),
                    status="PENDING",
                    trigger="DISCORD",
                    alert_id=getattr(alert_row, "id", None),
                    author=(getattr(alert_row, "author", "") or "").strip() or None,
                    strategy_tag=getattr(alert_row, "strategy_tag", None),
                )
                db.add(db_order)
                db.commit()
                db.refresh(db_order)
                db.refresh(alert_row)
                await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
                order_logger.info(
                    "PLACED | BUY %s x%d @ $%s (slippage cooldown retry, live=$%s) | alert=$%s | order_id=%d | oid=%s",
                    osi, qty, limit_px, live_dec, alert_dec, db_order.id, oid,
                )
                alert_for_task = _detached_alert_stub(alert_row)
            except Exception as exc:
                log.error("[BUY_COOLDOWN] %s DB/order setup failed: %s", osi, exc)
                return
            finally:
                db.close()

            try:
                await self._place_on_public(oid, osi, "BUY", qty, limit_px)
            except Exception as exc:
                log.error("[BUY_COOLDOWN] %s _place_on_public failed: %s", osi, exc)
                db = SessionLocal()
                try:
                    self._handle_order_failure(db, db_order, alert_db_id, exc, osi, "BUY")
                finally:
                    db.close()
                return

            log.info(
                "[BUY_COOLDOWN] %s submitted x%s @ $%s (alert was $%s, live $%s, %.0fs after block)",
                osi, qty, limit_px, alert_dec, live_dec, retry_s - (deadline - time.time()),
            )
            self._track_task(self._poll_order_fill(
                order_db_id=db_order.id,
                public_oid=oid,
                alert=alert_for_task,
                alert_db_id=alert_db_id,
                side="BUY",
                qty=qty,
            ))
            return

        log.info("[BUY_COOLDOWN] %s window expired (%ss) — no cooldown observed", osi, retry_s)

    async def _finalize_buy_fill(self, db, db_order, alert, alert_db_id, fill_px, qty):
        """Update DB after a BUY order is confirmed fully filled."""
        # Handle case where order is marked FILLED but 0 contracts actually filled
        if qty <= 0:
            log.warning("BUY order %s marked FILLED but qty=0 - marking as CANCELLED", db_order.public_order_id)
            db_order.status = "CANCELLED"
            db_order.error_text = "Order reported as FILLED but 0 contracts filled"
            self._mark_alert_status(db, alert_db_id, "CANCELLED", db_order.error_text)
            db.commit()
            await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
            return

        # Calculate newly filled quantity (avoid double-counting if already recorded partial fills)
        previously_filled = db_order.filled_qty or 0
        newly_filled = max(0, qty - previously_filled)

        db_order.fill_price = fill_px
        db_order.filled_qty = qty          # record total filled contracts
        db_order.status = "FILLED"
        db_order.filled_at = datetime.now(timezone.utc)

        pos = db.query(Position).filter_by(osi_symbol=alert.osi_symbol).first()
        if pos and pos.status in ("OPEN", "PARTIAL"):
            # Cross-analyst merge. Position.osi_symbol is UNIQUE, so two
            # analysts holding the SAME contract share one row: the second
            # buyer's contracts land under the first opener's name, a
            # fraction SELL ("SOLD 1/2") applies to the combined lot with no
            # way to say whose half, and an author-scoped close_all from the
            # opener flattens both books. 8 of 306 traded contracts have had
            # more than one analyst on them. Splitting the row needs a
            # (osi_symbol, author) key and a migration; until then, make the
            # merge loud instead of silent so it can be reconciled by hand.
            _new_author = (getattr(alert, "_alert_author", "") or "").strip()
            _own_author = (pos.author or "").strip()
            if newly_filled > 0 and _new_author and _own_author and _new_author != _own_author:
                log.warning(
                    "[POSITION_MERGE] %s: BUY from %r is merging into %r's open "
                    "position (%d + %d contracts). Exits for this row are now "
                    "ambiguous between the two analysts.",
                    alert.osi_symbol, _new_author, _own_author, pos.remaining, newly_filled,
                )
                asyncio.create_task(trade_logger.log_alert_blocked(
                    action="BUY", osi=alert.osi_symbol,
                    reason=(f"Merged into {_own_author}'s existing position — this row now holds "
                            f"contracts from two analysts and their exits will collide."),
                    author=_new_author, block_type="POSITION_MERGE",
                ))
            # Only add the newly filled contracts, not the total
            if newly_filled > 0:
                total = pos.remaining + newly_filled
                pos.avg_price = (pos.avg_price * pos.remaining + fill_px * newly_filled) / total
                pos.total_contracts += newly_filled
                pos.remaining += newly_filled
                pos.current_price = fill_px
                pos.highest_price = max(pos.highest_price or pos.avg_price, fill_px)
                pos.status = "OPEN"
        elif pos and pos.status == "CLOSED":
            # User re-bought a previously held contract. Position.osi_symbol
            # is UNIQUE, so we must REOPEN the existing CLOSED row in place
            # rather than INSERT a new one (which would raise IntegrityError
            # and lose the trade). Reset all flags / peak / realized so the
            # new cycle starts clean — prior cycle's state must not bleed
            # into the new one (e.g. stale highest_price would arm TPT at
            # the wrong level on the very first tick).
            pos.total_contracts = qty
            pos.remaining = qty
            pos.avg_price = fill_px
            pos.current_price = fill_px
            pos.highest_price = fill_px
            pos.close_price = None
            pos.close_time = None
            pos.status = "OPEN"
            pos.pt1_triggered = False
            pos.pt2_triggered = False
            pos.pt3_triggered = False
            pos.sl_triggered = False
            pos.tpt_armed = False
            if hasattr(pos, "realized_pnl"):
                pos.realized_pnl = None
            pos.open_time = datetime.now(timezone.utc)
            # Ownership belongs to whoever opened THIS cycle. Both stamps below
            # are write-once ("only if unset"), so a stale value would survive
            # the reopen: NVDA 215C 8/5 2026-08-04 reopened by Bdorts kept
            # author='Sniper Alerts', which would let Sniper's blank close_all
            # sweep Bdorts' position and resolve the wrong exit profile.
            pos.author = None
            pos.strategy_tag = None
            log.info("Reopened CLOSED position for %s (qty=%d @ $%s)", alert.osi_symbol, qty, fill_px)
        else:
            # No existing position - create with total qty (nothing was double-counted yet)
            pos = Position(
                osi_symbol=alert.osi_symbol,
                symbol=alert.symbol,
                expiry=str(alert.expiry),
                strike=alert.strike,
                option_type=alert.option_type,
                total_contracts=qty,
                remaining=qty,
                avg_price=fill_px,
                current_price=fill_px,
                highest_price=fill_px,
                status="OPEN",
            )
            db.add(pos)

        # Stamp the opening analyst (new/reopen sets it; scale-in preserves
        # the first opener). Enables author-scoped exits + per-author profiles.
        self._stamp_position_author(pos, alert)

        # Stamp re-entry tag so position_monitor reads [profile:reentry]
        # for tighter SL/PT on this cycle. Set on whichever branch above
        # produced the pos (new, reopened, or scaled-in).
        if pos and getattr(alert, "_reentry_tag", None):
            pos.strategy_tag = alert._reentry_tag
            log.info("[REENTRY] Tagged %s position with strategy_tag=%s", pos.osi_symbol, pos.strategy_tag)

        # Stamp whale tag so position_monitor reads [profile:whale] (auto-exit
        # +15% take-profit). Whale lane is entry-only — no analyst SELL ever
        # comes, so the position must exit on its own profile rules.
        if pos and getattr(alert, "strategy_tag", "") == "WHALE" and pos.strategy_tag != "RE-ENTRY":
            pos.strategy_tag = "WHALE"
            log.info("[WHALE] Tagged %s position with strategy_tag=WHALE", pos.osi_symbol)

        self._mark_alert_status(db, alert_db_id, "FILLED", linked_order_id=db_order.id)
        db.commit()
        db.refresh(pos)

        await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
        await self.ws_manager.broadcast({"type": "position_update", "data": pos.to_dict()})

        # Whale resting take-profit (WHALE-ONLY, flag-gated). Rest a GTD limit
        # SELL at +pt1_pct so the position exits at the broker without polling.
        # No-op for every non-whale position and when the flag is off.
        if pos and (getattr(pos, "strategy_tag", "") or "") == "WHALE":
            await self._maybe_place_whale_tp(db, pos)

        if newly_filled > 0 and previously_filled > 0:
            log.info("BUY filled (final): %s +%d new contracts (total filled=%d) @ $%s", alert.osi_symbol, newly_filled, qty, fill_px)
        else:
            log.info("BUY filled: %s x%s @ $%s", alert.osi_symbol, qty, fill_px)
        order_logger.info(
            "FILLED | BUY %s total=%d (newly=%d, prev=%d) @ $%.2f | avg_entry=$%.2f | pos_total=%d | pos_id=%d",
            alert.osi_symbol, qty, newly_filled, previously_filled, fill_px, pos.avg_price, pos.total_contracts, pos.id,
        )
        asyncio.create_task(trade_logger.log_order_filled(
            side="BUY", osi=alert.osi_symbol, qty=qty,
            fill_price=fill_px, trigger="DISCORD", order_id=db_order.id,
        ))

        # Slippage alert: when bot pays N% above analyst's alert price.
        # 2026-05-05: paid up to +10% over alert (TSLA 415C $1.09 → $1.20),
        # +9% (AAPL 282.5C $1.92 → $2.09). Compounds losses on losers.
        try:
            alert_px = float(getattr(alert, "price", 0) or 0)
            cap_pct = cfg.getfloat("trading", "slippage_alert_pct", fallback=3.0)
            if alert_px > 0 and fill_px > 0:
                slippage_pct = ((fill_px - alert_px) / alert_px) * 100.0
                if slippage_pct > cap_pct:
                    log.warning(
                        "[SLIPPAGE] BUY %s filled $%.2f vs alert $%.2f (+%.1f%% over)",
                        alert.osi_symbol, fill_px, alert_px, slippage_pct,
                    )
                    asyncio.create_task(trade_logger.log_slippage_alert(
                        osi=alert.osi_symbol, alert_price=alert_px,
                        fill_price=fill_px, slippage_pct=slippage_pct,
                    ))
        except Exception:
            pass

    async def _finalize_partial_buy_fill(self, db, db_order, alert, alert_db_id, fill_px: float, filled_so_far: int):
        """
        Update DB state when a BUY order has PARTIALLY_FILLED.

        - Records the running filled_qty on the order (so we can detect new fills next poll)
        - Updates the position avg_price and contracts with the newly filled quantity only
        - Broadcasts a position_update so the dashboard shows the live partial position
        - Does NOT mark the order FILLED (keeps it PENDING so polling continues)
        """
        prev_filled = db_order.filled_qty or 0
        newly_filled = filled_so_far - prev_filled
        if newly_filled <= 0:
            return  # no new contracts since last poll

        db_order.fill_price = fill_px
        db_order.filled_qty = filled_so_far
        db_order.status = "PARTIALLY_FILLED"
        # Don't set filled_at yet — the order isn't done

        pos = db.query(Position).filter_by(osi_symbol=alert.osi_symbol).first()
        if pos and pos.status in ("OPEN", "PARTIAL"):
            # Weighted avg with newly_filled contracts
            total = pos.remaining + newly_filled
            pos.avg_price = (pos.avg_price * pos.remaining + fill_px * newly_filled) / total
            pos.total_contracts += newly_filled
            pos.remaining += newly_filled
            pos.current_price = fill_px
            pos.highest_price = max(pos.highest_price or pos.avg_price, fill_px)
        elif pos and pos.status == "CLOSED":
            # Re-buy of a previously closed contract — REOPEN, don't INSERT.
            # Same UNIQUE-constraint rationale as in _finalize_buy_fill.
            pos.total_contracts = newly_filled
            pos.remaining = newly_filled
            pos.avg_price = fill_px
            pos.current_price = fill_px
            pos.highest_price = fill_px
            pos.close_price = None
            pos.close_time = None
            pos.status = "PARTIAL"
            pos.pt1_triggered = False
            pos.pt2_triggered = False
            pos.pt3_triggered = False
            pos.sl_triggered = False
            pos.tpt_armed = False
            if hasattr(pos, "realized_pnl"):
                pos.realized_pnl = None
            pos.open_time = datetime.now(timezone.utc)
        else:
            pos = Position(
                osi_symbol=alert.osi_symbol,
                symbol=alert.symbol,
                expiry=str(alert.expiry),
                strike=alert.strike,
                option_type=alert.option_type,
                total_contracts=newly_filled,
                remaining=newly_filled,
                avg_price=fill_px,
                current_price=fill_px,
                highest_price=fill_px,
                status="PARTIAL",
            )
            db.add(pos)

        self._stamp_position_author(pos, alert)

        db.commit()
        db.refresh(pos)

        await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
        await self.ws_manager.broadcast({"type": "position_update", "data": pos.to_dict()})
        log.info(
            "PARTIAL FILL: %s +%d contracts (total filled=%d/%d) @ $%.2f",
            alert.osi_symbol, newly_filled, filled_so_far, db_order.quantity, fill_px,
        )
        order_logger.info(
            "PARTIAL | BUY %s +%d contracts (total=%d/%d) @ $%.2f | avg_entry=$%.2f",
            alert.osi_symbol, newly_filled, filled_so_far, db_order.quantity, fill_px, pos.avg_price,
        )


    async def _sell_from_alert(self, alert, alert_db_id: int, timer: PipelineTimer = None):
        # ── Check for recent auto-exit (prevents duplicate sell when alert comes after auto-exit) ─
        from app.monitors.position_monitor import get_recent_auto_exit
        recent_exit = get_recent_auto_exit(alert.osi_symbol) if alert.osi_symbol else None
        if recent_exit:
            exit_type, exit_time, qty = recent_exit
            elapsed = int(time.time() - exit_time)
            log.info("[SELL] Alert for %s blocked: auto-exit %s fired %ds ago (qty=%d)",
                     alert.osi_symbol, exit_type, elapsed, qty)
            db = SessionLocal()
            try:
                self._mark_alert_status(db, alert_db_id, "IGNORED", f"Auto-exit {exit_type} already fired {elapsed}s ago")
                db.commit()
            finally:
                db.close()
            if timer:
                timer.finish(f"IGNORED:auto_exit_{exit_type}_recent")
            return

        # ── Blank-symbol close_all gate ───────────────────────────────────
        # A SELL parsed as close_all but with no symbol/contract named. Per 365d
        # of Discord history this is ~entirely parser noise (chitchat / market
        # commentary) or a real exit whose ticker the parser dropped — never a
        # genuine "flatten everything" order. Drop it SKIPPED so it can never
        # sweep the book (even author-scoped). Set close_all_requires_symbol=false
        # to revert to the scope-gated execution path.
        if alert.close_all and not (alert.osi_symbol or alert.symbol) and _CLOSE_ALL_REQUIRES_SYMBOL():
            db = SessionLocal()
            try:
                self._mark_alert_status(
                    db, alert_db_id, "SKIPPED",
                    "CLOSE_ALL_NO_SYMBOL: sell-all with no contract named — dropped "
                    "(close_all_requires_symbol=true)",
                )
                db.commit()
            finally:
                db.close()
            order_logger.warning(
                "SELL DROPPED | blank-symbol close_all, no contract named | author=%s | requires_symbol gate",
                getattr(alert, "_alert_author", "") or "",
            )
            if timer:
                timer.finish("SKIPPED:close_all_no_symbol")
            return

        db = SessionLocal()
        any_filled = False
        try:
            alert._owner_scope_dropped = []
            positions = self._match_open_positions(db, alert)
            if not positions and alert._owner_scope_dropped:
                # Every match belongs to a different analyst. This is NOT "we
                # don't hold it" — someone does, just not this caller. Leave
                # their position AND their pending BUYs alone; cancelling those
                # would be the same cross-analyst leak one step earlier.
                owners = ", ".join(sorted({a for _, a in alert._owner_scope_dropped if a}))
                reason = (
                    f"CROSS_ANALYST_SELL: this contract is held by {owners or 'another analyst'}, "
                    f"not by the analyst who called this exit. Their SELL does not close "
                    f"someone else's position (position_owner_scope=author)."
                )
                self._mark_alert_status(db, alert_db_id, "IGNORED", reason)
                db.commit()
                order_logger.info(
                    "SELL IGNORED | %s | cross-analyst exit | trigger_author=%s | owners=%s",
                    alert.osi_symbol or alert.symbol,
                    (getattr(alert, "_alert_author", "") or "").strip(), owners,
                )
                if timer:
                    timer.finish("IGNORED:cross_analyst")
                return
            if not positions:
                # No open position — but a BUY may still be PENDING on the
                # broker (limit too tight, price ran). Analyst already
                # SOLD, so we should NOT enter late. Cancel any matching
                # pending BUY so we don't fill into a falling/spiked book.
                # Real-world: MS260522C00200000 2026-05-14 — BUY @ $1.70
                # limit never filled (alert $1.61, stock ran to $1.90),
                # then SELL alert hit and was IGNORED while pending BUY
                # sat live for 30 minutes.
                cancelled_buys = await self._cancel_pending_buys_for_alert(db, alert)
                reason = (
                    f"No matching open position — you don't hold this contract, so the SELL is a no-op. "
                    f"Also cancelled {cancelled_buys} still-pending BUY on it so it can't fill into a falling market."
                    if cancelled_buys
                    else "No matching open position — you don't hold this contract (never filled, already exited, or entry was blocked), so the SELL is a no-op."
                )
                self._mark_alert_status(db, alert_db_id, "IGNORED", reason)
                db.commit()  # ← FIX: commit so status isn’t left as PENDING
                order_logger.info(
                    "SELL IGNORED | %s | no matching open position | close_all=%s | pending_buys_cancelled=%d",
                    alert.osi_symbol or alert.symbol, alert.close_all, cancelled_buys,
                )
                if timer:
                    timer.finish(
                        f"IGNORED:no_position_buys_cancelled={cancelled_buys}"
                        if cancelled_buys else "IGNORED:no_position"
                    )
                return

            for pos in positions:
                # first_sell_closes_all: treat the analyst's (first) SELL as a
                # FULL exit — dump all remaining, ignore alert.fraction. Later
                # portions become no-ops since the book is already empty.
                _force_full = _pos_first_sell_closes_all(pos)
                effective_close_all = bool(alert.close_all) or _force_full
                qty = pos.remaining if effective_close_all else max(1, int(pos.remaining * float(alert.fraction)))
                qty = min(qty, pos.remaining)
                if qty <= 0:
                    continue
                if _force_full and not alert.close_all:
                    log.info(
                        "[SELL] first_sell_closes_all: analyst partial (fraction=%s) on %s → FULL exit x%d",
                        alert.fraction, pos.osi_symbol, qty,
                    )

                # Post-PT2 runner protection: once PT1+PT2 have already taken
                # profit, let the trailing stop (TPT) carry the runner. Ignore
                # non-close_all DISCORD SELL alerts so a partial-exit signal
                # from the analyst doesn't kill upside (real-world bug
                # 2026-04-30: QCOM 175C had PT1+PT2 hit, then a DISCORD SELL
                # closed the last contract at $3.10 — minutes later the same
                # analyst called SELL @ $5.00 and $6.15, but position was
                # already gone). Full exits (close_all OR first_sell_closes_all)
                # still pass — an analyst exit under that flag closes the runner.
                if pos.pt2_triggered and not effective_close_all:
                    log.info(
                        "[SELL] Ignoring non-close_all DISCORD SELL on %s — PT2 already hit, runner protected by TPT",
                        pos.osi_symbol,
                    )
                    continue

                # For blank-symbol close_all, alert.price comes from text that
                # doesn't reference this specific contract — never trust it.
                # Always use the position's own price as the limit base.
                trust_alert_price = not (alert.close_all and not alert.osi_symbol and not alert.symbol)
                base_price = self._resolve_sell_price(
                    alert.price if trust_alert_price else None, pos
                )
                # alert_first SELL: place the FIRST ask at the analyst's typed
                # price with zero slippage haircut. Re-peg walks DOWN toward
                # live bid if unfilled. Only honored when the analyst price
                # is the actual basis (not synthesized from pos.current_price).
                _exit_mode = _alert_exit_base_price_mode(alert)
                if _exit_mode == "alert_first" and trust_alert_price and alert.price is not None:
                    limit_px = self._compute_limit(
                        base_price, "SELL", slippage_override=Decimal("0"),
                    )
                    log.info(
                        "[SELL] alert_first: starting at analyst price $%s on %s (re-peg will walk down if unfilled)",
                        base_price, pos.osi_symbol,
                    )
                else:
                    limit_px = self._compute_limit(base_price, "SELL")
                order_logger.info(
                    "PLACED | SELL %s x%d @ $%s (limit) | remaining=%d | fraction=%s close_all=%s",
                    pos.osi_symbol, qty, limit_px, pos.remaining, alert.fraction, alert.close_all,
                )
                result = await self._place_sell(db, pos, qty, limit_px, "DISCORD", alert_id=alert_db_id)
                if result is not None:  # None = position already closed (race condition)
                    any_filled = True
                    # close_all market-out watcher: if analyst signalled a full
                    # exit, the position MUST get out — don't sit on a stale
                    # limit while price collapses through it. Spawn a re-peg
                    # task that cancels + re-places at fresh bid every N
                    # seconds, then on final attempt uses a wide-slippage
                    # limit (effectively marketable).
                    # Route to the FORCED (marketable) lane when this SELL exits
                    # the WHOLE remaining position — even a fractional alert that
                    # rounds up to all contracts (e.g. "sell 1/2" on a 1-lot). A
                    # full exit must fill, not sit on a limit and strand (SPXW
                    # 7480C 2026-07-08: partial lane gave up, position stranded
                    # ~14min, exited -30%). True partials keep the gentler lane.
                    if effective_close_all or qty >= pos.remaining:
                        asyncio.create_task(self._repeg_close_all_sell(
                            order_db_id=result.id,
                            position_id=pos.id,
                            initial_qty=qty,
                        ))
                    else:
                        # Partial DISCORD SELL re-peg: chase the bid every N
                        # seconds. NO final wide-slippage attempt — partials
                        # are not forced exits. Stops when filled, when a
                        # newer DISCORD/MANUAL/AUTO order takes over the row,
                        # or when max_attempts is reached.
                        asyncio.create_task(self._repeg_partial_sell(
                            order_db_id=result.id,
                            position_id=pos.id,
                            initial_qty=qty,
                        ))

            if any_filled:
                self._mark_alert_status(db, alert_db_id, "FILLED")
                if timer:
                    timer.mark("sell_filled")
                    timer.finish("FILLED")
            else:
                self._mark_alert_status(db, alert_db_id, "IGNORED", "Matched position had no remaining contracts")
                if timer:
                    timer.finish("IGNORED:no_remaining")
            db.commit()
        except Exception as exc:
            self._mark_alert_status(db, alert_db_id, "ERROR", str(exc))
            db.commit()
            log.error("SELL failed %s: %s", alert.osi_symbol or alert.symbol, exc)
            if timer:
                timer.finish(f"ERROR:{exc}")
        finally:
            db.close()

    async def _place_sell(self, db, pos: Position, qty: int, limit_px: Decimal, trigger: str, expiration=None, skip_band_clamp: bool = False,
                          force_plain: bool = False, alert_id: int | None = None):
        # Re-check remaining BEFORE placing — auto-exit may have already fired
        db.refresh(pos)
        if pos.remaining <= 0 or pos.status == "CLOSED":
            log.warning("[SELL] Skipped: %s already closed (remaining=%d status=%s)",
                        pos.osi_symbol, pos.remaining, pos.status)
            return None
        qty = min(qty, max(0, pos.remaining))
        if qty <= 0:
            raise ValueError(f"No contracts remaining for {pos.osi_symbol}")

        # IDEMPOTENCY: Check for existing pending order for this position.
        # Priority order (highest wins, cancels lower):
        #   AUTO (PT*/SL/TPT/TRAILING_SL)  — risk-managed close
        #   MANUAL                         — explicit operator override
        #   DISCORD                        — analyst alert
        # AUTO trumps MANUAL+DISCORD. MANUAL trumps DISCORD (operator's last
        # word beats the analyst's stale limit). 2026-05-05 SPX 7275C: a
        # DISCORD SELL @ $2.10 sat unfilled while price collapsed to $1.29.
        # User's manual SELL @ 14:14:11 was SKIPPED because of this — costing
        # 60s of bleed. Now MANUAL cancels DISCORD to flush.
        AUTO_TRIGGERS = {"PT1", "PT2", "PT3", "SL", "TRAILING_SL", "TPT", "SL_PARTIAL", "TIME_EXIT"}
        existing_pending = db.query(Order).filter(
            Order.osi_symbol == pos.osi_symbol,
            Order.side == "SELL",
            Order.status == "PENDING",
        ).first()
        if existing_pending:
            stale_trigger = existing_pending.trigger or ""
            stale_is_auto = stale_trigger in AUTO_TRIGGERS
            stale_is_manual = stale_trigger == "MANUAL"
            stale_is_discord = stale_trigger == "DISCORD"
            new_is_auto = trigger in AUTO_TRIGGERS
            new_is_manual = trigger == "MANUAL"
            new_is_discord = trigger == "DISCORD"
            # Newer DISCORD displaces stale DISCORD only if it does NOT
            # shrink the pending exit. Example: stale SELL x2 (1/2 of 4) is
            # pending, a follow-up SELL x1 (1/4) arrives — we must NOT
            # cancel the x2 and replace with x1 (would leave 3 contracts
            # unprotected). But ALL OUT (x4) or larger fraction (x3) MUST
            # displace the stale x2.
            stale_qty = existing_pending.quantity or 0
            new_displaces_discord = (
                new_is_discord
                and stale_is_discord
                and _DISCORD_DISPLACES_DISCORD()
                and qty >= stale_qty
            )
            # Stale AUTO displacement (flag-gated, OFF by default).
            # A poll-fired exit outranks everything and nothing outranks it
            # back: no newer AUTO, no MANUAL, no analyst ALL OUT. So an SL
            # limit that goes stale in a fast market strands the position for
            # up to max_pending_minutes with its stop already marked fired and
            # every other exit route answering "skipped: existing pending".
            # Age-gated rather than unconditional: a same-tick PT1→PT2 cascade
            # must still stack, not have the second rung cancel the first.
            # qty guard so a displacement never shrinks the exit and leaves
            # contracts less protected than they were.
            stale_auto_expired = False
            if _AUTO_EXIT_REPEG_ENABLED() and stale_is_auto and qty >= stale_qty:
                _placed = existing_pending.placed_at
                if _placed is not None:
                    if _placed.tzinfo is None:          # SQLite drops tzinfo; stored as UTC
                        _placed = _placed.replace(tzinfo=timezone.utc)
                    _age_s = (datetime.now(timezone.utc) - _placed).total_seconds()
                    stale_auto_expired = _age_s >= _AUTO_EXIT_STALE_SECONDS()
                    if stale_auto_expired:
                        log.warning(
                            "[SELL] Stale AUTO %s order #%d unfilled for %.0fs on %s — "
                            "letting %s displace it",
                            stale_trigger, existing_pending.id, _age_s, pos.osi_symbol, trigger,
                        )

            # AUTO cancels MANUAL or DISCORD;
            # MANUAL cancels DISCORD;
            # newer DISCORD cancels stale DISCORD when its qty ≥ stale qty
            #   (flag-gated; needed so ALL OUT / larger fraction is not
            #   blocked by an unfilled partial limit).
            # anything cancels an AUTO that has gone stale (flag-gated).
            should_cancel = (
                (new_is_auto and not stale_is_auto)
                or (new_is_manual and not stale_is_auto and not stale_is_manual)
                or new_displaces_discord
                or stale_auto_expired
            )
            if should_cancel:
                log.warning(
                    "[SELL] Cancelling stale %s pending order #%d to make room for %s on %s",
                    stale_trigger, existing_pending.id, trigger, pos.osi_symbol,
                )
                # Same 3-state confirmed-cancel contract as the re-peg lanes
                # (SPXW 7550C 2026-07-09): never place the replacement while the
                # stale order might still fill. One-knob revert via
                # [trading] repeg_confirmed_cancel_only=false.
                _confirm = _REPEG_CONFIRMED_CANCEL_ONLY()
                try:
                    cancel_res = await self.cancel_order(
                        existing_pending.id, require_confirmed_terminal=_confirm)
                except Exception as cancel_exc:
                    if _confirm:
                        log.error(
                            "[SELL] Cancel of stale pending #%d raised: %s — NOT placing "
                            "%s replacement (stale order may still be live and fill)",
                            existing_pending.id, cancel_exc, trigger,
                        )
                        return None
                    log.error(
                        "[SELL] Failed to cancel stale pending #%d: %s — proceeding anyway (legacy mode)",
                        existing_pending.id, cancel_exc,
                    )
                    cancel_res = {"status": "ok"}
                _cstatus = cancel_res.get("status") if isinstance(cancel_res, dict) else None
                if _cstatus == "filled_during_cancel":
                    # Stale SELL filled while we were cancelling — position is
                    # already reduced (or will be once its poll books the fill).
                    # A replacement would oversell.
                    log.warning(
                        "[SELL] Stale pending #%d FILLED during cancel — skipping %s replacement on %s",
                        existing_pending.id, trigger, pos.osi_symbol,
                    )
                    return None
                if _confirm and _cstatus == "cancel_unconfirmed":
                    # Cancel not confirmed terminal (await timeout / gateway
                    # reconnect). Stale order stays LIVE and will fill once —
                    # placing another SELL now is the double-sell.
                    log.warning(
                        "[SELL] Cancel of stale pending #%d UNCONFIRMED — holding off %s replacement on %s",
                        existing_pending.id, trigger, pos.osi_symbol,
                    )
                    return None
                # Refresh from DB after cancel — re-query in case state changed.
                db.refresh(pos)
                if pos.remaining <= 0 or pos.status == "CLOSED":
                    return None
                qty = min(qty, pos.remaining)
                if qty <= 0:
                    return None
            else:
                log.warning("[SELL] Skipped: existing pending SELL order for %s (order_id=%d, trigger=%s, new_trigger=%s)",
                            pos.osi_symbol, existing_pending.id, stale_trigger, trigger)
                return None

        # Band clamp: SELL limits more than max_limit_band_pct above the live
        # mark get rejected by Public.com ("too aggressive pricing"). Clamp
        # pre-submit to avoid the round-trip + 30s re-peg gap.
        # Skipped for the whale resting take-profit (WHALE_TP): it is meant to
        # rest well above the mark (+15%), so clamping it to the band would
        # defeat the purpose.
        clamped_px, was_clamped = (limit_px, False) if skip_band_clamp else _clamp_sell_limit_to_band(pos, limit_px)
        if was_clamped:
            log.warning(
                "[SELL] %s band-clamped: %s → %s (live=%s, band=%.1f%%) trigger=%s",
                pos.osi_symbol, limit_px, clamped_px, pos.current_price,
                _max_limit_band_pct(pos), trigger,
            )
            order_logger.warning(
                "BAND CLAMP | SELL %s | requested=$%s → submitted=$%s | live=$%s | band=%.1f%% | trigger=%s",
                pos.osi_symbol, limit_px, clamped_px, pos.current_price,
                _max_limit_band_pct(pos), trigger,
            )
            limit_px = clamped_px

        oid = str(uuid.uuid4())
        db_order = Order(
            public_order_id=oid,
            osi_symbol=pos.osi_symbol,
            side="SELL",
            quantity=qty,
            limit_price=float(limit_px),
            status="PENDING",
            trigger=trigger,
            alert_id=alert_id,   # the exit alert, so a round trip maps end to end
            author=(pos.author or "").strip() or None,  # opener, so BUY+SELL rows share the analyst
            strategy_tag=getattr(pos, "strategy_tag", None),  # lane tag rides the whole round trip
        )
        db.add(db_order)
        db.commit()
        # Every SELL lane logs PLACED, not just the alert path. A MANUAL exit
        # used to appear in orders.log only once it FILLED, so the 2026-08-05
        # NDXP260805C29700000 exit (row at 14:31:35, broker POST at 14:31:47)
        # had no trace of where those 12.6s went.
        _submit_t0 = time.monotonic()
        # _sell_from_alert already logs PLACED for the DISCORD lane (with
        # fraction/close_all); this covers every other one.
        if trigger != "DISCORD":
            order_logger.info(
                "PLACED | SELL %s x%d @ $%s (limit) | remaining=%d | trigger=%s | order_id=%d | oid=%s",
                pos.osi_symbol, qty, limit_px, pos.remaining, trigger, db_order.id, oid,
            )
        asyncio.create_task(trade_logger.log_order_placed(
            side="SELL", osi=pos.osi_symbol, qty=qty,
            limit_price=float(limit_px), trigger=trigger, order_id=db_order.id,
        ))
        db.refresh(db_order)
        # Fire-and-forget: the exit submit must never queue behind dashboard
        # WS I/O (a slow client costs up to the 2s send timeout).
        asyncio.create_task(self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()}))

        try:
            if _DRY_RUN():
                log.info("[DRY RUN] SELL %s x%s @ $%s [%s]", pos.osi_symbol, qty, limit_px, trigger)
                fill_px = float(limit_px)
                await self._finalize_sell_fill(db, db_order, pos, fill_px, qty, trigger)
            else:
                await self._place_on_public(oid, pos.osi_symbol, "SELL", qty, limit_px,
                                            expiration=expiration, _force_plain=force_plain)
                _submit_ms = (time.monotonic() - _submit_t0) * 1000
                log.info("SELL submitted (PENDING) [%s]: %s x%s @ $%s (row→broker %.0fms)",
                         trigger, pos.osi_symbol, qty, limit_px, _submit_ms)
                # An exit that takes seconds to even reach the broker is a real
                # execution risk on a moving position — surface it instead of
                # leaving it to be reconstructed from httpx timestamps later.
                if _submit_ms >= _SLOW_SUBMIT_MS():
                    order_logger.warning(
                        "SLOW SUBMIT | SELL %s | row→broker %.0fms | trigger=%s | order_id=%d",
                        pos.osi_symbol, _submit_ms, trigger, db_order.id,
                    )
                _metric_order_placed("SELL")
                self._track_task(self._poll_order_fill(
                    order_db_id=db_order.id,
                    public_oid=oid,
                    alert=None,
                    alert_db_id=None,
                    side="SELL",
                    qty=qty,
                    position_id=pos.id,
                ))
            return db_order

        except Exception as exc:
            db_order.status = "REJECTED"
            db_order.error_text = str(exc)
            db.commit()
            await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
            raise

    def _gtd_expiration_for(self, pos: Position):
        """Build a GTD OrderExpirationRequest expiring at 16:00 ET on the
        option's expiry day (capped to <90d per SDK limit). Returns None to
        signal 'use DAY' when expiry is missing, already past, or same-day
        (0DTE — DAY is correct there)."""
        sdk = self._load_sdk()
        try:
            d = datetime.strptime(str(pos.expiry)[:10], "%Y-%m-%d")
            exp = datetime(d.year, d.month, d.day, 16, 0, tzinfo=ZoneInfo("America/New_York"))
            now = datetime.now(timezone.utc)
            if exp <= now:
                return None  # 0DTE / past → DAY order
            cap = now + timedelta(days=89)
            if exp > cap:
                exp = cap
            return sdk["OrderExpirationRequest"](
                time_in_force=sdk["TimeInForce"].GTD, expiration_time=exp,
            )
        except Exception:
            return None

    async def _maybe_place_whale_tp(self, db, pos: Position):
        """WHALE-ONLY resting take-profit. On a whale BUY fill, rest a GTD limit
        SELL at avg x (1 + pt1_pct/100) so the position auto-exits at the broker
        without polling. No-op unless whale_resting_tp_enabled AND the position
        is tagged WHALE. Failure is non-fatal — the poll-based +15% exit in
        position_monitor remains the backstop (pt1 only skips while a live
        WHALE_TP rests)."""
        if not _whale_resting_tp_enabled():
            return
        if (getattr(pos, "strategy_tag", "") or "") != "WHALE":
            return
        if pos.remaining <= 0 or not pos.avg_price:
            return
        # Don't double-place if one already rests.
        existing = db.query(Order).filter(
            Order.osi_symbol == pos.osi_symbol,
            Order.side == "SELL",
            Order.status == "PENDING",
            Order.trigger == "WHALE_TP",
        ).first()
        if existing:
            return
        pct = _pcfg_float(pos, "pt1_pct", 15.0)
        raw = Decimal(str(pos.avg_price)) * (Decimal(1) + Decimal(str(pct)) / Decimal(100))
        limit_px = raw.quantize(Decimal("0.01"))
        expiration = self._gtd_expiration_for(pos)
        tif = "GTD" if expiration is not None else "DAY"
        log.info("[WHALE_TP] Placing resting SELL %s x%d @ $%s (entry $%s, +%.0f%%, %s)",
                 pos.osi_symbol, pos.remaining, limit_px, pos.avg_price, pct, tif)
        try:
            await self._place_sell(
                db, pos, pos.remaining, limit_px, "WHALE_TP",
                expiration=expiration, skip_band_clamp=True,
            )
        except Exception as e:
            log.error("[WHALE_TP] place failed for %s: %s — poll exit (pt1) will back it up",
                      pos.osi_symbol, e)

    async def _repeg_close_all_sell(self, order_db_id: int, position_id: int, initial_qty: int,
                                    trigger: str = "DISCORD"):
        """
        Re-peg watcher for close_all SELL orders.

        Wakes every close_all_repeg_seconds. If the SELL is still PENDING:
          - Cancel it
          - Fetch fresh bid (live quote)
          - Place new SELL at bid - 1 tick
          - On the final attempt, widen slippage to close_all_final_slippage_pct
            (default 20%) so the new limit is well below market = marketable.

        Stops on FILLED, CANCELLED-by-other-path, or position CLOSED.
        2026-05-05 SPX 7275C: $2.10 limit on "ALL OUT $2.25" alert never
        filled while price collapsed to ~$1.30. This watcher would have
        re-pegged at the live bid within 5s and exited around $1.80–$2.00.
        """
        from app.execution.broker_router import fetch_prices
        repeg_seconds = _CLOSE_ALL_REPEG_SECONDS()
        max_attempts = _CLOSE_ALL_REPEG_MAX_ATTEMPTS()
        final_slip_pct = _CLOSE_ALL_FINAL_SLIPPAGE_PCT()
        if repeg_seconds <= 0 or max_attempts <= 0:
            return

        current_order_id = order_db_id
        attempt = 0
        # Eager-wake event (see _repeg_partial_sell).
        reject_event = asyncio.Event()
        _reject_signals[current_order_id] = reject_event
        try:
            while attempt < max_attempts:
                try:
                    await asyncio.wait_for(reject_event.wait(), timeout=repeg_seconds)
                    reject_event.clear()
                except asyncio.TimeoutError:
                    pass
                attempt += 1

                db = SessionLocal()
                try:
                    db_order = db.query(Order).filter(Order.id == current_order_id).first()
                    if not db_order:
                        return
                    # FILLED → done. PARTIALLY_FILLED handled inline by qty
                    # delta against pos.remaining. CANCELLED/REJECTED →
                    # broker rejected (e.g. NBBO violation on a stale-price
                    # limit). Fall through and re-peg with a fresh quote so
                    # the position doesn't strand open.
                    # 2026-05-14 CRCL 5/15 135C: SELL @ $1.75 cancelled by
                    # IBKR ("Limit price too far outside of NBBO") because
                    # alert price was stale vs spiking market. Old code
                    # exited on status != PENDING and left 4 contracts open.
                    if db_order.status == "FILLED":
                        return
                    pos = db.query(Position).filter(Position.id == position_id).first()
                    if not pos or pos.remaining <= 0 or pos.status == "CLOSED":
                        return
                    osi = pos.osi_symbol
                    remaining_qty = min(initial_qty, pos.remaining)
                finally:
                    db.close()

                if remaining_qty <= 0:
                    return

                # Fetch fresh quote for re-peg base.
                try:
                    quotes = await fetch_prices([osi])
                    live_px = quotes.get(osi) if quotes else None
                except Exception as e:
                    log.warning("[REPEG] %s quote fetch failed: %s — skipping this attempt", osi, e)
                    continue
                if not live_px or live_px <= 0:
                    log.warning("[REPEG] %s no live quote — skipping attempt %d", osi, attempt)
                    continue

                # Cancel the stale pending order. Skip if already terminal
                # (broker-cancel path): no live order to cancel, we just
                # need to place a fresh SELL.
                db = SessionLocal()
                try:
                    _stale = db.query(Order).filter(Order.id == current_order_id).first()
                    stale_status = _stale.status if _stale else None
                finally:
                    db.close()
                if stale_status == "PENDING":
                    _confirm = _REPEG_CONFIRMED_CANCEL_ONLY()
                    try:
                        cancel_res = await self.cancel_order(
                            current_order_id, require_confirmed_terminal=_confirm)
                    except Exception as e:
                        log.warning("[REPEG] %s cancel of #%d failed: %s — skipping this attempt", osi, current_order_id, e)
                        continue
                    _cstatus = cancel_res.get("status") if isinstance(cancel_res, dict) else None
                    # A fill raced the cancel — position already reduced; a
                    # replacement SELL would oversell (UNH 430C double-sell
                    # 2026-07-08). Stop the re-peg lane.
                    if _cstatus == "filled_during_cancel":
                        log.warning("[REPEG] %s prior SELL #%d FILLED during cancel — not re-placing",
                                    osi, current_order_id)
                        return
                    # Cancel not confirmed terminal (await timeout / gateway
                    # reconnect). The order is still live and could fill — do NOT
                    # place a replacement (would oversell, SPXW 7550C 2026-07-09).
                    # Hold off and retry next cycle; the prior order stays live so
                    # the exit still fills once.
                    if _confirm and _cstatus == "cancel_unconfirmed":
                        log.warning("[REPEG] %s cancel of #%d UNCONFIRMED — holding off re-place this cycle",
                                    osi, current_order_id)
                        continue
                else:
                    log.info("[REPEG] %s prior order #%d already %s — placing fresh SELL",
                             osi, current_order_id, stale_status)

                # Re-fetch position state post-cancel.
                db = SessionLocal()
                try:
                    pos = db.query(Position).filter(Position.id == position_id).first()
                    if not pos or pos.remaining <= 0 or pos.status == "CLOSED":
                        return
                    qty_now = min(initial_qty, pos.remaining)
                finally:
                    db.close()
                if qty_now <= 0:
                    return

                # Compute new limit. Final attempt: widen slippage to bid × (1 - 20%)
                # so the limit is decisively marketable. Earlier attempts: bid - 1
                # tick (small discount, lets limit cross the spread).
                live_dec = Decimal(str(round(float(live_px), 2)))
                if attempt >= max_attempts:
                    slip = Decimal(str(final_slip_pct)) / Decimal("100")
                    raw = live_dec * (Decimal("1") - slip)
                    inc = Decimal("0.05") if raw < Decimal("3.00") else Decimal("0.10")
                    limit_px = (Decimal(math.floor(float(raw) / float(inc))) * inc).quantize(Decimal("0.01"))
                    limit_px = max(limit_px, inc)
                    log.warning("[REPEG] %s FINAL attempt — wide-slippage limit $%s (bid=$%s, slip=%.0f%%)",
                                osi, limit_px, live_dec, float(final_slip_pct))
                else:
                    limit_px = _snap_down_to_tick(live_dec - (Decimal("0.05") if live_dec < Decimal("3.00") else Decimal("0.10")))
                    log.info("[REPEG] %s attempt %d/%d — re-peg @ $%s (bid=$%s, prev order=#%d)",
                             osi, attempt, max_attempts, limit_px, live_dec, current_order_id)

                # Place a new SELL via _place_sell so idempotency, polling, and
                # finalize-fill flow run as normal. The DISCORD default lets an
                # AUTO exit preempt this lane if SL/PT fires concurrently; an
                # auto exit chasing its own fill passes its own trigger through
                # instead, so exits.log keeps the [SL]/[TPT] tag and the
                # replacement keeps the priority of the exit it replaces.
                db = SessionLocal()
                try:
                    pos = db.query(Position).filter(Position.id == position_id).first()
                    if not pos or pos.remaining <= 0 or pos.status == "CLOSED":
                        return
                    try:
                        new_order = await self._place_sell(db, pos, qty_now, limit_px, trigger,
                                                          force_plain=True)
                    except Exception as e:
                        log.error("[REPEG] %s _place_sell failed on attempt %d: %s", osi, attempt, e)
                        return
                finally:
                    db.close()
                if new_order is None:
                    return
                _reject_signals.pop(current_order_id, None)
                current_order_id = new_order.id
                _reject_signals[current_order_id] = reject_event
        finally:
            _reject_signals.pop(current_order_id, None)

    async def _repeg_partial_sell(self, order_db_id: int, position_id: int, initial_qty: int):
        """
        Re-peg watcher for partial DISCORD SELL orders (close_all=False).

        Wakes every partial_repeg_seconds. While the SELL is still PENDING:
          - Cancel it
          - Fetch fresh bid (live quote)
          - Place new SELL at bid - 1 tick (same qty, same trigger=DISCORD)

        Stops on FILLED, CANCELLED-by-other-path, position CLOSED, or
        max_attempts. Unlike close_all, NEVER widens to a marketable limit:
        partials are not forced exits — if the bid keeps collapsing past
        attempt N, leave the limit where it is and let later AUTO/MANUAL
        decide. 2026-05-08 SPX 7380P: partial $4.60 sat 17min unfilled
        while price decayed through it; this watcher would have re-pegged
        every 30s and either filled near $4.50 or stepped down with bid.
        """
        from app.execution.broker_router import fetch_prices
        repeg_seconds = _PARTIAL_REPEG_SECONDS()
        max_attempts = _PARTIAL_REPEG_MAX_ATTEMPTS()
        final_marketable = _PARTIAL_REPEG_FINAL_MARKETABLE()
        final_slip_pct = _PARTIAL_REPEG_FINAL_SLIPPAGE_PCT()
        if repeg_seconds <= 0 or max_attempts <= 0:
            return

        current_order_id = order_db_id
        attempt = 0
        # Eager-wake event: poller calls _signal_reject() on broker
        # REJECT/CANCEL so this watcher reacts in <1s instead of napping
        # the full repeg_seconds (30s default).
        reject_event = asyncio.Event()
        _reject_signals[current_order_id] = reject_event
        try:
            while attempt < max_attempts:
                try:
                    await asyncio.wait_for(reject_event.wait(), timeout=repeg_seconds)
                    reject_event.clear()
                except asyncio.TimeoutError:
                    pass
                attempt += 1

                db = SessionLocal()
                try:
                    db_order = db.query(Order).filter(Order.id == current_order_id).first()
                    if not db_order:
                        return
                    # FILLED → done. CANCELLED/REJECTED → broker bounced the limit
                    # (e.g. "Cancelled due to too aggressive pricing" when limit is
                    # outside Public.com's NBBO band). Fall through and re-peg with
                    # a fresh quote so the position doesn't strand open.
                    if db_order.status == "FILLED":
                        return
                    # Newer order took the slot — bail.
                    if (db_order.trigger or "") != "DISCORD":
                        return
                    stale_status = db_order.status
                    pos = db.query(Position).filter(Position.id == position_id).first()
                    if not pos or pos.remaining <= 0 or pos.status == "CLOSED":
                        return
                    osi = pos.osi_symbol
                    remaining_qty = min(initial_qty, pos.remaining)
                finally:
                    db.close()

                if remaining_qty <= 0:
                    return

                try:
                    quotes = await fetch_prices([osi])
                    live_px = quotes.get(osi) if quotes else None
                except Exception as e:
                    log.warning("[PARTIAL_REPEG] %s quote fetch failed: %s — skipping attempt %d", osi, e, attempt)
                    continue
                if not live_px or live_px <= 0:
                    log.warning("[PARTIAL_REPEG] %s no live quote — skipping attempt %d", osi, attempt)
                    continue

                # Only cancel if still live on broker. Already-terminal orders
                # (CANCELLED/REJECTED by broker) have nothing to cancel — placing
                # a fresh SELL is the only path forward.
                if stale_status == "PENDING":
                    _confirm = _REPEG_CONFIRMED_CANCEL_ONLY()
                    try:
                        cancel_res = await self.cancel_order(
                            current_order_id, require_confirmed_terminal=_confirm)
                    except Exception as e:
                        log.warning("[PARTIAL_REPEG] %s cancel of #%d failed: %s — skipping attempt %d",
                                    osi, current_order_id, e, attempt)
                        continue
                    _cstatus = cancel_res.get("status") if isinstance(cancel_res, dict) else None
                    # A fill raced the cancel — the position is already reduced;
                    # placing a replacement SELL would oversell (UNH 430C
                    # double-sell 2026-07-08). Stop the re-peg lane.
                    if _cstatus == "filled_during_cancel":
                        log.warning("[PARTIAL_REPEG] %s prior SELL #%d FILLED during cancel — not re-placing",
                                    osi, current_order_id)
                        return
                    # Cancel unconfirmed — order still live, could fill. Hold off
                    # re-placing this cycle (never oversell); retry next cycle.
                    if _confirm and _cstatus == "cancel_unconfirmed":
                        log.warning("[PARTIAL_REPEG] %s cancel of #%d UNCONFIRMED — holding off re-place this cycle",
                                    osi, current_order_id)
                        continue
                else:
                    log.info("[PARTIAL_REPEG] %s prior order #%d already %s — placing fresh SELL",
                             osi, current_order_id, stale_status)

                db = SessionLocal()
                try:
                    pos = db.query(Position).filter(Position.id == position_id).first()
                    if not pos or pos.remaining <= 0 or pos.status == "CLOSED":
                        return
                    qty_now = min(initial_qty, pos.remaining)
                finally:
                    db.close()
                if qty_now <= 0:
                    return

                live_dec = Decimal(str(round(float(live_px), 2)))
                if attempt >= max_attempts and final_marketable:
                    # Final attempt: widen to a marketable limit so the analyst's
                    # exit actually FILLS instead of the lane giving up and
                    # stranding the position (SPXW 7480C 2026-07-08).
                    slip = Decimal(str(final_slip_pct)) / Decimal("100")
                    limit_px = _snap_down_to_tick(live_dec * (Decimal("1") - slip))
                    log.warning("[PARTIAL_REPEG] %s FINAL attempt — marketable limit $%s (bid=$%s, slip=%.0f%%)",
                                osi, limit_px, live_dec, float(final_slip_pct))
                else:
                    inc = Decimal("0.05") if live_dec < Decimal("3.00") else Decimal("0.10")
                    limit_px = _snap_down_to_tick(live_dec - inc)
                    log.info("[PARTIAL_REPEG] %s attempt %d/%d — re-peg @ $%s (bid=$%s, prev order=#%d)",
                             osi, attempt, max_attempts, limit_px, live_dec, current_order_id)

                db = SessionLocal()
                try:
                    pos = db.query(Position).filter(Position.id == position_id).first()
                    if not pos or pos.remaining <= 0 or pos.status == "CLOSED":
                        return
                    try:
                        new_order = await self._place_sell(db, pos, qty_now, limit_px, "DISCORD",
                                                          force_plain=True)
                    except Exception as e:
                        log.error("[PARTIAL_REPEG] %s _place_sell failed on attempt %d: %s", osi, attempt, e)
                        return
                finally:
                    db.close()
                if new_order is None:
                    return
                # Re-key event to the new order id so the next reject wakes us.
                _reject_signals.pop(current_order_id, None)
                current_order_id = new_order.id
                _reject_signals[current_order_id] = reject_event
        finally:
            _reject_signals.pop(current_order_id, None)

    async def _repeg_buy(self, order_db_id: int, alert_osi: str, alert_price: float,
                        alert_db_id: int, qty: int, repeg_seconds: float,
                        max_attempts: int, cap_pct: float, buy_slippage: Decimal,
                        author: str | None = None, strategy_tag: str | None = None):
        """
        BUY re-peg watcher. Walks the limit up every repeg_seconds while the
        order is still PENDING and the market is trading above our bid.

        Stops on: FILL / non-PENDING status / max_attempts reached / cap reached
                  (cap = alert_price × (1 + cap_pct/100)).

        Mirrors _repeg_partial_sell but in the opposite direction: SELL chases
        bid down, BUY chases ask up. Both are gated by config knobs so default
        behavior is unchanged.

        2026-05-18 SPX 7410C: alert $3.65, bot bid $2.10, 30min timeout, never
        filled while price ran to +200%. This watcher would have re-pegged
        toward live every 15s up to alert × 1.25 cap, very likely filling
        inside the first minute.
        """
        from app.execution.public_sdk_bridge import fetch_prices
        if repeg_seconds <= 0 or max_attempts <= 0:
            return

        alert_dec = Decimal(str(alert_price))
        hard_cap = (alert_dec * (Decimal("1") + Decimal(str(cap_pct)) / Decimal("100"))).quantize(Decimal("0.01"))
        current_order_id = order_db_id
        attempt = 0
        # Eager-wake event (see _repeg_partial_sell).
        reject_event = asyncio.Event()
        _reject_signals[current_order_id] = reject_event
        try:
            while attempt < max_attempts:
                try:
                    await asyncio.wait_for(reject_event.wait(), timeout=repeg_seconds)
                    reject_event.clear()
                except asyncio.TimeoutError:
                    pass
                attempt += 1

                # Re-read current state — order may have filled or been cancelled.
                db = SessionLocal()
                try:
                    db_order = db.query(Order).filter(Order.id == current_order_id).first()
                    if not db_order:
                        return
                    # FILLED → done. CANCELLED/REJECTED → broker bounced the limit
                    # (e.g. "Cancelled due to too aggressive pricing" when bid is
                    # outside Public.com's NBBO band). Fall through and re-peg with
                    # a fresh quote so the BUY doesn't strand on a dead order.
                    if db_order.status == "FILLED":
                        return
                    if (db_order.trigger or "") != "DISCORD":
                        return
                    stale_status = db_order.status
                    cur_limit = Decimal(str(db_order.limit_price or 0))
                finally:
                    db.close()

                try:
                    quotes = await fetch_prices([alert_osi])
                    live_px = quotes.get(alert_osi) if quotes else None
                except Exception as e:
                    log.warning("[BUY_REPEG] %s quote fetch failed: %s — skipping attempt %d", alert_osi, e, attempt)
                    continue
                if not live_px or live_px <= 0:
                    log.warning("[BUY_REPEG] %s no live quote — skipping attempt %d", alert_osi, attempt)
                    continue

                live_dec = Decimal(str(round(float(live_px), 2)))
                # PENDING path: re-peg only when market moved above our standing bid.
                # Broker-terminal path: always re-peg — prior order is dead, so we
                # must place fresh regardless of whether market has moved.
                if stale_status == "PENDING" and live_dec <= cur_limit:
                    log.debug("[BUY_REPEG] %s live $%s ≤ limit $%s — no re-peg needed", alert_osi, live_dec, cur_limit)
                    continue

                # New limit = live × (1 + buy_slippage), capped at alert × (1+cap_pct).
                raw = live_dec * (Decimal("1") + buy_slippage)
                inc = Decimal("0.05") if raw < Decimal("3.00") else Decimal("0.10")
                new_limit = (Decimal(math.ceil(float(raw) / float(inc))) * inc).quantize(Decimal("0.01"))
                if new_limit > hard_cap:
                    log.warning(
                        "[BUY_REPEG] %s would re-peg to $%s but cap is $%s (alert $%s × %s%%) — stopping",
                        alert_osi, new_limit, hard_cap, alert_dec, cap_pct,
                    )
                    return
                if stale_status == "PENDING" and new_limit <= cur_limit:
                    continue

                # Only cancel if still live on broker. Already-terminal orders
                # (CANCELLED/REJECTED by broker) have nothing to cancel.
                if stale_status == "PENDING":
                    _confirm = _REPEG_CONFIRMED_CANCEL_ONLY()
                    try:
                        cancel_res = await self.cancel_order(
                            current_order_id, require_confirmed_terminal=_confirm)
                    except Exception as e:
                        log.warning("[BUY_REPEG] %s cancel #%d failed: %s — skipping attempt %d", alert_osi, current_order_id, e, attempt)
                        continue
                    _cstatus = cancel_res.get("status") if isinstance(cancel_res, dict) else None
                    # A fill raced the cancel — the entry is already open. A
                    # replacement BUY would DOUBLE the long. Stop the re-peg lane.
                    if _cstatus == "filled_during_cancel":
                        log.warning("[BUY_REPEG] %s prior BUY #%d FILLED during cancel — entry open, not re-placing",
                                    alert_osi, current_order_id)
                        return
                    # Cancel unconfirmed — order still live, could fill. Hold off
                    # re-placing this cycle (never double-buy); retry next cycle.
                    if _confirm and _cstatus == "cancel_unconfirmed":
                        log.warning("[BUY_REPEG] %s cancel of #%d UNCONFIRMED — holding off re-place this cycle",
                                    alert_osi, current_order_id)
                        continue
                else:
                    log.info("[BUY_REPEG] %s prior order #%d already %s — placing fresh BUY @ $%s",
                             alert_osi, current_order_id, stale_status, new_limit)

                new_oid = str(uuid.uuid4())
                db = SessionLocal()
                new_db_order_id = None
                try:
                    new_db_order = Order(
                        public_order_id=new_oid,
                        osi_symbol=alert_osi,
                        side="BUY",
                        quantity=qty,
                        limit_price=float(new_limit),
                        status="PENDING",
                        trigger="DISCORD",
                        author=author,
                        strategy_tag=strategy_tag,
                    )
                    db.add(new_db_order)
                    db.commit()
                    db.refresh(new_db_order)
                    new_db_order_id = new_db_order.id
                    await self.ws_manager.broadcast({"type": "order_update", "data": new_db_order.to_dict()})
                    if alert_db_id:
                        self._mark_alert_status(db, alert_db_id, "PENDING", linked_order_id=new_db_order.id)
                        db.commit()
                finally:
                    db.close()

                try:
                    await self._place_on_public(new_oid, alert_osi, "BUY", qty, new_limit)
                except Exception as e:
                    log.error("[BUY_REPEG] %s place_on_public failed: %s — aborting", alert_osi, e)
                    return

                order_logger.info(
                    "REPEG | BUY %s attempt %d/%d | $%s → $%s (live $%s, cap $%s) | new order_id=%d oid=%s",
                    alert_osi, attempt, max_attempts, cur_limit, new_limit, live_dec, hard_cap,
                    new_db_order_id, new_oid,
                )

                # Start a fresh poll on the new order id (legacy poll on old id
                # will terminate cleanly because status is no longer PENDING).
                # Stub carries osi/price so the SPX profile resolves correctly
                # for the timeout override (otherwise the fresh poll would
                # default to global max_pending_minutes=30 instead of SPX=5).
                # author: for profile resolution AND Position/Order attribution
                # on fill (was dropped here → authorless re-pegged BUYs).
                _alert_stub = SimpleNamespace(
                    osi_symbol=alert_osi, price=alert_price,
                    author=author, _alert_author=author,
                    expiry=None,
                    # Carry the lane through the re-peg, don't blank it:
                    # _finalize_buy_fill stamps Position.strategy_tag="WHALE"
                    # off this field, so a hardcoded None left a re-pegged whale
                    # entry outside [profile:whale] — and the whale lane is
                    # entry-only, so nothing else would ever tag it.
                    strategy_tag=strategy_tag,
                    # so _stamp_position_author tags the position on fill
                    small_account=(strategy_tag == "SMALL_ACCT"),
                )
                _new_pending_min = _alert_max_pending_minutes(_alert_stub)
                self._track_task(self._poll_order_fill(
                    order_db_id=new_db_order_id,
                    public_oid=new_oid,
                    alert=_alert_stub,
                    alert_db_id=alert_db_id,
                    side="BUY",
                    qty=qty,
                    max_pending_minutes_override=_new_pending_min,
                ))
                _reject_signals.pop(current_order_id, None)
                current_order_id = new_db_order_id
                _reject_signals[current_order_id] = reject_event
        finally:
            _reject_signals.pop(current_order_id, None)

    async def _finalize_sell_fill(self, db, db_order, pos, fill_px, qty, trigger):
        """Update DB after a SELL order is confirmed filled."""
        # Calculate newly filled quantity (avoid double-counting if already recorded partial fills)
        previously_filled = db_order.filled_qty or 0
        newly_filled = max(0, qty - previously_filled)

        db_order.fill_price = fill_px
        db_order.filled_qty = qty          # record total filled contracts
        db_order.status = "FILLED"
        db_order.filled_at = datetime.now(timezone.utc)
        # Snapshot current pos.avg_price BEFORE any further mutation. Anchors
        # realized P&L for this sell to the basis at fill time, even if later
        # BUYs (or reconciler reopens) overwrite pos.avg_price.
        if db_order.cost_basis is None:
            db_order.cost_basis = pos.avg_price

        pos.current_price = fill_px
        # Only subtract newly filled contracts. If broker re-reports the same
        # fill count (newly_filled==0), we already counted those contracts on
        # the previous poll — do not re-subtract or we will double-decrement.
        pos.remaining -= newly_filled
        if pos.remaining <= 0:
            pos.remaining = 0
            pos.status = "CLOSED"
            pos.close_time = datetime.now(timezone.utc)
            pos.close_price = fill_px   # record actual exit price for P&L
        else:
            pos.status = "PARTIAL"

        db.commit()
        db.refresh(pos)

        await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
        await self.ws_manager.broadcast({"type": "position_update", "data": pos.to_dict()})
        sell_qty_for_pnl = newly_filled if newly_filled > 0 else qty
        if newly_filled > 0 and previously_filled > 0:
            log.info("SELL filled (final) [%s]: %s +%d new contracts (total sold=%d) @ $%s", trigger, pos.osi_symbol, newly_filled, qty, fill_px)
        else:
            log.info("SELL filled [%s]: %s x%s @ $%s", trigger, pos.osi_symbol, qty, fill_px)
        pnl = (fill_px - pos.avg_price) * sell_qty_for_pnl * 100 if pos.avg_price else None
        pnl_pct = ((fill_px / pos.avg_price) - 1) * 100 if pos.avg_price else None
        order_logger.info(
            "FILLED | SELL %s total=%d (newly=%d, prev=%d) @ $%.2f [%s] | entry=$%.2f | pnl=$%s pnl_pct=%s | remaining=%d status=%s",
            pos.osi_symbol, qty, newly_filled, previously_filled, fill_px, trigger,
            pos.avg_price or 0, f"{pnl:+.2f}" if pnl else "N/A",
            f"{pnl_pct:+.1f}%" if pnl_pct else "N/A", pos.remaining, pos.status,
        )
        asyncio.create_task(trade_logger.log_order_filled(
            side="SELL", osi=pos.osi_symbol, qty=qty,
            fill_price=fill_px, trigger=trigger, order_id=db_order.id,
            pnl=pnl, pnl_pct=pnl_pct,
        ))

        # Post-partial TPT arm: when an analyst (DISCORD) SELL fills profitably
        # and the position has any remaining contracts, ARM trailing-take-profit
        # on the leftover. Anchors trail at fill price so the next leg can ride
        # further. Only active for profiles that opt in via tpt_remainder_only
        # (e.g. [profile:spx_index]) so default pure-relay behavior is unchanged
        # for non-index trades.
        if (
            trigger == "DISCORD"
            and pos.remaining > 0
            and newly_filled > 0
            and pnl_pct is not None
            and _entry_overrides_enabled()
            and _pcfg_bool(pos, "tpt_enabled", False)
            and _pcfg_bool(pos, "tpt_remainder_only", False)
        ):
            tpt_arm_pct = _pcfg_float(pos, "tpt_arm_pct", 100.0)
            if pnl_pct >= tpt_arm_pct and not pos.tpt_armed:
                pos.tpt_armed = True
                # Anchor peak at this fill; subsequent ticks (and broker
                # day_high in position_monitor) will bump it higher.
                pos.highest_price = max(pos.highest_price or 0.0, fill_px)
                db.commit()
                log.info(
                    "[TPT] Armed post-partial on %s after DISCORD SELL @ $%.2f (pnl %+.1f%% ≥ arm %.0f%%, remainder=%d)",
                    pos.osi_symbol, fill_px, pnl_pct, tpt_arm_pct, pos.remaining,
                )

    async def _cancel_external_blocking_orders(self, broker_symbol: str) -> int:
        """Cancel any PENDING orders on Public.com for `broker_symbol` that
        weren't placed by this bot (i.e. orders the operator left open from
        the mobile app). Returns the count of orders cancelled.

        Only safe to call for index symbols where the operator has explicitly
        delegated exit authority to the bot — see _place_on_public retry path.
        Real-world bug 2026-05-04: operator placed a manual SELL on SPXW 7200P
        from mobile app; bot's SL_PARTIAL retried 30+ times and was rejected
        each time with 'must first cancel an invalid order'. Position decayed
        from +36% to -6% while the bot spun.
        """
        client = await self._get_client()
        portfolio = await client.get_portfolio()
        cancelled = 0
        db = SessionLocal()
        try:
            our_oids = {
                o.public_order_id for o in db.query(Order).filter(
                    Order.public_order_id.isnot(None)
                ).all() if o.public_order_id
            }
        finally:
            db.close()

        for order in (portfolio.orders or []):
            try:
                inst = getattr(order, "instrument", None)
                sym = inst.symbol if inst else ""
                if sym != broker_symbol:
                    continue
                status_val = str(getattr(order.status, "value", order.status) or "").upper()
                if status_val not in ("PENDING", "OPEN", "QUEUED", "PARTIALLY_FILLED", "ACCEPTED"):
                    continue
                broker_oid = str(order.order_id)
                if broker_oid in our_oids:
                    # One of our own orders — leave alone; the normal cancel
                    # path (cancel_order by db id) handles bot-bot conflicts.
                    continue
                log.warning(
                    "[EXTERNAL_CANCEL] Cancelling external pending order %s on %s "
                    "(blocking bot exit; placed outside this bot)",
                    broker_oid, broker_symbol,
                )
                await client.cancel_order(broker_oid)
                cancelled += 1
                # Notify operator via Discord trade-logs so they know their
                # mobile-app order got cancelled by the bot.
                asyncio.create_task(trade_logger.log_order_cancelled(
                    osi=broker_symbol, side="EXTERNAL",
                    reason=f"Bot cancelled external order {broker_oid} blocking auto-exit on index symbol",
                ))
            except Exception as exc:
                log.error("[EXTERNAL_CANCEL] Failed to cancel %s: %s",
                          getattr(order, "order_id", "?"), exc)
        return cancelled

    async def _place_on_public(self, oid: str, osi_symbol: str, side: str, quantity: int, limit_price: Decimal, expiration=None,
                               *, _force_plain: bool = False):
        """Place an order on Public.com. Returns immediately after the API accepts it.

        For index symbols (SPX/NDX/RUT/etc), if the broker rejects with
        'must first cancel an invalid order', try once to clear the blocking
        order placed outside this bot, then retry. See
        _cancel_external_blocking_orders for the rationale.

        `expiration` (OrderExpirationRequest) overrides the default DAY TIF —
        used by the whale resting take-profit to rest GTD until option expiry.
        """
        sdk = self._load_sdk()
        client = await self._get_client()
        open_close = sdk["OpenCloseIndicator"].OPEN if side == "BUY" else sdk["OpenCloseIndicator"].CLOSE
        broker_symbol = _osi_for_broker(osi_symbol)
        if broker_symbol != osi_symbol:
            log.info("Symbol translated for broker: %s → %s", osi_symbol, broker_symbol)

        exp_req = expiration or sdk["OrderExpirationRequest"](time_in_force=sdk["TimeInForce"].DAY)

        def _build_req():
            return sdk["OrderRequest"](
                order_id=oid,
                instrument=sdk["OrderInstrument"](symbol=broker_symbol, type=sdk["InstrumentType"].OPTION),
                order_side=sdk["OrderSide"].BUY if side == "BUY" else sdk["OrderSide"].SELL,
                order_type=sdk["OrderType"].LIMIT,
                expiration=exp_req,
                quantity=quantity,
                limit_price=limit_price,
                open_close_indicator=open_close,
            )

        # Public.com sometimes returns 400 with HTML body when their broker
        # upstream times out (504 Gateway Time-out wrapped as 400 by the
        # gateway). The SDK surfaces this as "API Error 400: Invalid upstream
        # response: <html>...". Real validation 400s have JSON bodies. Retry
        # transient upstream errors with backoff; let real 400s bubble up.
        # 2026-05-06 incident: SPX 7345C BUY missed entirely on a single 504,
        # cost the trade a +100% winner.
        async def _place_with_upstream_retry():
            attempts = 3
            for i in range(attempts):
                try:
                    await client.place_order(_build_req())
                    return
                except Exception as exc:
                    m = str(exc).lower()
                    transient = (
                        "invalid upstream" in m
                        or "504 gateway" in m
                        or "502 bad gateway" in m
                        or "503 service" in m
                    )
                    if not transient:
                        raise
                    if i >= attempts - 1:
                        # Retries exhausted — Discord alarm so operator sees it.
                        try:
                            import app.analytics.trade_logger as _tl
                            await _tl.log_critical(
                                title=f"BROKER UPSTREAM EXHAUSTED — {side} {broker_symbol}",
                                message=(
                                    f"Public.com upstream timed out **{attempts} times** for "
                                    f"`{side} {broker_symbol} x{quantity} @ ${limit_price}` "
                                    f"(oid={oid}).\n\n"
                                    f"**Trade was NOT placed.** Last error:\n```{str(exc)[:300]}```"
                                ),
                                severity="critical",
                                footer="Public.com 504 retry exhausted",
                            )
                        except Exception:
                            pass
                        raise
                    backoff = 2.0 * (i + 1)
                    log.warning(
                        "[PLACE] Public.com upstream transient (attempt %d/%d) — "
                        "retrying in %.1fs: %s",
                        i + 1, attempts, backoff, str(exc)[:160],
                    )
                    # Fire-and-forget warning on FIRST transient detection so
                    # operator knows the bot is in retry mode. Don't fire on
                    # every attempt — just the first.
                    if i == 0:
                        try:
                            import app.analytics.trade_logger as _tl
                            asyncio.create_task(_tl.log_critical(
                                title=f"BROKER UPSTREAM RETRY — {side} {broker_symbol}",
                                message=(
                                    f"Public.com returned a transient error on `{side} "
                                    f"{broker_symbol} x{quantity}`. Bot is retrying "
                                    f"(up to {attempts} attempts, backoff {backoff:.0f}s)."
                                ),
                                severity="warning",
                                footer="Public.com upstream transient — retrying",
                            ))
                        except Exception:
                            pass
                    await asyncio.sleep(backoff)

        try:
            await _place_with_upstream_retry()
        except Exception as exc:
            msg = str(exc)
            from app.risk.guardrails import _is_index_symbol as _is_idx
            should_recover = (
                "must first cancel" in msg.lower()
                and _is_idx(osi_symbol)
            )
            if not should_recover:
                raise
            log.warning(
                "[PLACE] Index %s rejected with stale blocking order — attempting external cancel + retry: %s",
                broker_symbol, msg,
            )
            n = await self._cancel_external_blocking_orders(broker_symbol)
            log.info("[PLACE] Cancelled %d external blocking order(s) on %s; retrying place_order", n, broker_symbol)
            if n == 0:
                # Nothing to cancel — error came from somewhere else; surface it.
                raise
            # Brief settle window so broker reflects the cancel before retry.
            await asyncio.sleep(1.0)
            await _place_with_upstream_retry()

        log.info("Order placed on Public.com: %s %s x%s @ $%s (oid=%s)", side, broker_symbol, quantity, limit_price, oid)

    async def _poll_order_fill(self, order_db_id: int, public_oid: str, alert, alert_db_id, side: str, qty: int, position_id: int | None = None, max_pending_minutes_override: int | None = None):
        """
        Background task: poll Public.com for order fill status.
        Checks every 3 seconds. Runs until filled, cancelled, rejected, expired,
        or until max_pending_minutes is exceeded (auto-cancels).
        """
        client = await self._get_client()
        NotFoundError = self._load_sdk()["NotFoundError"]
        # PARTIALLY_FILLED is NOT terminal — keep polling until FILLED or another terminal state.
        # REPLACED is terminal (the order was swapped via cancel-and-replace; the new order has its own poll loop).
        TERMINAL = {"FILLED", "CANCELLED", "QUEUED_CANCELLED", "REJECTED", "EXPIRED", "REPLACED"}
        poll_interval = _FILL_POLL_INTERVAL()
        effective_max_min = max_pending_minutes_override if max_pending_minutes_override is not None else _MAX_PENDING_MINUTES()
        timeout_seconds = effective_max_min * 60 if effective_max_min > 0 else 0
        elapsed = 0
        # Order placement is async on the broker; get_order can 404 for the first
        # few seconds after place_order returns. Swallow early NotFoundError silently.
        not_found_grace_seconds = 15

        log.info("Started fill monitor for order %s (db_id=%s, timeout=%sm)", public_oid, order_db_id, effective_max_min or '∞')
        order_logger.info("POLL START | order_id=%s oid=%s | side=%s qty=%d | timeout=%sm", order_db_id, public_oid, side, qty, effective_max_min or '∞')

        # Track consecutive non-NotFound errors so a persistent SDK auth or
        # network failure breaks the poll loop (instead of spinning silently
        # for max_pending_minutes with the user blind to the problem).
        consecutive_errors = 0
        MAX_CONSECUTIVE_ERRORS = 10

        while True:
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

            # ── Timeout: auto-cancel stale pending orders ─────────────
            if timeout_seconds > 0 and elapsed >= timeout_seconds:
                max_min = effective_max_min
                log.warning("Order %s timed out after %d minutes — auto-cancelling", public_oid, max_min)
                db = SessionLocal()
                try:
                    # Cancel on Public.com
                    try:
                        await client.cancel_order(public_oid)
                        log.info("Auto-cancelled order %s on Public.com", public_oid)
                    except Exception as cancel_exc:
                        log.warning("Auto-cancel API call failed (order may already be terminal): %s", cancel_exc)

                    db_order = db.query(Order).filter(Order.id == order_db_id).first()
                    if db_order and db_order.status == "PENDING":
                        db_order.status = "CANCELLED"
                        db_order.error_text = f"Auto-cancelled: no fill after {effective_max_min} minutes"
                        if alert_db_id:
                            self._mark_alert_status(db, alert_db_id, "CANCELLED", db_order.error_text)
                        db.commit()
                        await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
                        asyncio.create_task(trade_logger.log_order_cancelled(
                            osi=db_order.osi_symbol, side=side,
                            reason=db_order.error_text, order_id=db_order.id,
                        ))
                finally:
                    db.close()
                return

            try:
                order_resp = await client.get_order(public_oid)
                status_str = str(order_resp.status.value) if hasattr(order_resp.status, 'value') else str(order_resp.status)

                if status_str == "UNKNOWN":
                    log.warning(
                        "Order %s returned UNKNOWN status — SDK may need updating. Continuing to poll.",
                        public_oid,
                    )

                if status_str == "FILLED":
                    fill_px = float(order_resp.average_price) if order_resp.average_price else None
                    # Get actual filled quantity from broker response, or fall back to what we already tracked
                    filled_qty_from_broker = int(order_resp.filled_quantity) if hasattr(order_resp, 'filled_quantity') and order_resp.filled_quantity else 0
                    db = SessionLocal()
                    try:
                        db_order = db.query(Order).filter(Order.id == order_db_id).first()
                        if not db_order:
                            return
                        # Use broker's filled quantity, or what we already recorded from partial fills.
                        # Last resort: use the order's original quantity — broker says FILLED so
                        # contracts moved; falling back to 0 marks the order CANCELLED while
                        # the user actually holds an untracked position.
                        filled_qty_actual = filled_qty_from_broker if filled_qty_from_broker > 0 else (db_order.filled_qty or 0)
                        if filled_qty_actual <= 0:
                            log.error(
                                "Order %s marked FILLED but no filled quantity reported! "
                                "Falling back to ordered qty=%d (broker says contracts moved).",
                                public_oid, qty,
                            )
                            filled_qty_actual = qty
                        log.info("Order %s FILLED @ $%s (qty=%d/%d, broker_reported=%d)", public_oid, fill_px, filled_qty_actual, qty, filled_qty_from_broker)
                        order_logger.info("FILLED | order_id=%s | filled=%d/%d | broker_qty=%d | price=%s", order_db_id, filled_qty_actual, qty, filled_qty_from_broker, fill_px)

                        if side == "BUY" and alert:
                            await self._finalize_buy_fill(db, db_order, alert, alert_db_id, fill_px, filled_qty_actual)
                        elif side == "SELL" and position_id:
                            pos = db.query(Position).filter(Position.id == position_id).first()
                            if pos:
                                trigger = db_order.trigger or "DISCORD"
                                await self._finalize_sell_fill(db, db_order, pos, fill_px, filled_qty_actual, trigger)
                        else:
                            # Fallback: just mark order as filled
                            db_order.fill_price = fill_px
                            db_order.filled_qty = filled_qty_actual
                            db_order.status = "FILLED"
                            db_order.filled_at = datetime.now(timezone.utc)
                            if alert_db_id:
                                self._mark_alert_status(db, alert_db_id, "FILLED")
                            db.commit()
                            await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
                    finally:
                        db.close()
                    return

                elif status_str == "PARTIALLY_FILLED":
                    # Some contracts filled — update position with what we have,
                    # keep polling until fully filled or terminal state.
                    filled_so_far = int(order_resp.filled_quantity) if hasattr(order_resp, 'filled_quantity') and order_resp.filled_quantity else 0
                    fill_px_partial = float(order_resp.average_price) if order_resp.average_price else None
                    if filled_so_far > 0 and fill_px_partial and side == "BUY" and alert:
                        db = SessionLocal()
                        try:
                            db_order = db.query(Order).filter(Order.id == order_db_id).first()
                            if db_order and db_order.filled_qty != filled_so_far:
                                # Only update if the filled count actually changed
                                await self._finalize_partial_buy_fill(
                                    db, db_order, alert, alert_db_id, fill_px_partial, filled_so_far
                                )
                        finally:
                            db.close()
                    log.info(
                        "Order %s PARTIALLY_FILLED: %d/%d contracts @ $%s — still polling",
                        public_oid, filled_so_far, qty, fill_px_partial,
                    )
                    # Don't return — keep polling

                elif status_str in TERMINAL:
                    reject_reason = getattr(order_resp, "reject_reason", None)
                    if reject_reason:
                        log.warning("Order %s terminal status: %s | reason: %s", public_oid, status_str, reject_reason)
                        order_logger.warning(
                            "%s | order_id=%s oid=%s | side=%s qty=%s | reason=%s",
                            status_str, order_db_id, public_oid, side, qty, reject_reason,
                        )
                    else:
                        log.warning("Order %s terminal status: %s", public_oid, status_str)
                        order_logger.warning(
                            "%s | order_id=%s oid=%s | side=%s qty=%s",
                            status_str, order_db_id, public_oid, side, qty,
                        )
                    # Wake any re-peg watcher sleeping on this order so it
                    # re-pegs in <1s instead of waiting out the full nap.
                    # Safe no-op when no watcher is registered (e.g. close_all
                    # final attempt, BUYs without buy_repeg_enabled).
                    _signal_reject(order_db_id)
                    db = SessionLocal()
                    osi_for_alert = None
                    try:
                        db_order = db.query(Order).filter(Order.id == order_db_id).first()
                        if db_order:
                            db_order.status = status_str
                            db_order.error_text = (
                                f"Order {status_str} on Public.com: {reject_reason}"
                                if reject_reason
                                else f"Order {status_str} on Public.com"
                            )
                            osi_for_alert = db_order.osi_symbol
                            if alert_db_id:
                                self._mark_alert_status(db, alert_db_id, status_str, reject_reason)
                            db.commit()
                            await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
                    finally:
                        db.close()
                    # Discord: post broker-side rejection so user sees it
                    # within seconds. Only for actual rejects with reasons —
                    # plain CANCELLED (user/AUTO-cancel) already logged
                    # elsewhere; don't double-notify.
                    if reject_reason and osi_for_alert:
                        asyncio.create_task(trade_logger.log_order_rejected(
                            osi=osi_for_alert, side=side, qty=qty,
                            reason=reject_reason, order_id=order_db_id,
                        ))
                    return

                # Still pending (NEW, PARTIALLY_FILLED, etc.) — keep polling

            except NotFoundError as exc:
                # SDK docs: orders aren't immediately indexed after async placement.
                # Stay quiet for the first few seconds, then escalate to a warning.
                if elapsed <= not_found_grace_seconds:
                    log.debug("Order %s not yet indexed (elapsed=%ds): %s", public_oid, elapsed, exc)
                else:
                    log.warning("Order %s not found on broker after %ds: %s", public_oid, elapsed, exc)
                # NotFound is an expected transient state right after placement,
                # not a real error — don't count toward consecutive_errors.
            except Exception as exc:
                consecutive_errors += 1
                log.error("Fill poll error for order %s (%d/%d): %s",
                          public_oid, consecutive_errors, MAX_CONSECUTIVE_ERRORS, exc)
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    log.error(
                        "Order %s: %d consecutive poll errors — marking ERROR and breaking out. "
                        "Reconciler will attempt recovery on next pass.",
                        public_oid, consecutive_errors,
                    )
                    db = SessionLocal()
                    try:
                        db_order = db.query(Order).filter(Order.id == order_db_id).first()
                        if db_order and db_order.status == "PENDING":
                            db_order.status = "ERROR"
                            db_order.error_text = f"Poll failed after {consecutive_errors} consecutive errors: {exc}"
                            if alert_db_id:
                                self._mark_alert_status(db, alert_db_id, "ERROR", db_order.error_text)
                            db.commit()
                            await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
                    finally:
                        db.close()
                    return
                continue
            else:
                consecutive_errors = 0  # reset on successful poll

    async def _await_public_order_terminal(self, public_oid: str, timeout: float) -> str:
        """Poll Public.com until the order is terminal or `timeout` elapses.
        Returns:
          "FILLED"      — a fill raced the cancel (fully/partially filled).
          "CANCELLED"   — confirmed terminal non-fill (cancelled/rejected/expired).
          "UNCONFIRMED" — no terminal state confirmed within the window (the
                          order may still be live and could yet fill).
        Public has no get_order(oid) — statuses come from get_portfolio().orders."""
        if timeout <= 0:
            return "UNCONFIRMED"
        _WORKING = {"NEW", "PENDING", "OPEN", "QUEUED", "ACCEPTED",
                    "PENDING_CANCEL", "PENDING_REPLACE", "UNKNOWN"}
        loop = asyncio.get_event_loop()
        end = loop.time() + timeout
        while loop.time() < end:
            try:
                client = await self._get_client()
                portfolio = await client.get_portfolio()
                for order in (portfolio.orders or []):
                    if str(getattr(order, "order_id", "")) != str(public_oid):
                        continue
                    status = str(getattr(order.status, "value", order.status) or "").upper()
                    filled_qty = float(getattr(order, "filled_quantity", 0) or 0)
                    if status == "FILLED" or status == "PARTIALLY_FILLED" or filled_qty > 0:
                        return "FILLED"
                    if status not in _WORKING:   # CANCELLED/QUEUED_CANCELLED/REJECTED/EXPIRED/REPLACED
                        return "CANCELLED"
                    break  # found, still working — wait and re-poll
            except Exception as e:
                log.warning("Public cancel terminal-confirm poll error for %s: %s", public_oid, e)
            await asyncio.sleep(0.4)
        return "UNCONFIRMED"  # timed out without a confirmed terminal state

    async def cancel_order(self, order_db_id: int, *,
                           require_confirmed_terminal: bool = False) -> dict:
        """Cancel a pending order on Public.com and update local DB.

        After the cancel request we AWAIT a terminal state (up to
        cancel_confirm_timeout_seconds) and return one of:
          {"status": "filled_during_cancel"} — a fill raced the cancel; we do
              NOT mark it CANCELLED (the position is already reduced). Callers
              must skip any re-place (prevents the UNH 430C double-sell).
          {"status": "cancel_unconfirmed"}   — only when require_confirmed_terminal
              is set AND we could not confirm the order went terminal. We leave
              the order live (do NOT stamp CANCELLED) so its own poll can still
              book a fill, and signal the caller to hold off re-placing.
          {"status": "ok"}                    — confirmed cancelled (or manual
              cancel with require_confirmed_terminal=False).

        `require_confirmed_terminal` is set by the re-peg lanes so an unconfirmed
        cancel never greenlights a replacement order (double-sell/double-buy).
        Manual/API cancels use the default False → always mark CANCELLED, never
        leave a ghost PENDING.
        """
        db = SessionLocal()
        try:
            db_order = db.query(Order).filter(Order.id == order_db_id).first()
            if not db_order:
                raise ValueError(f"Order {order_db_id} not found in database")
            if db_order.status not in ("PENDING", "PARTIALLY_FILLED"):
                raise ValueError(f"Order #{order_db_id} is already {db_order.status} — nothing to cancel")

            public_cancel_note = ""
            if not _DRY_RUN() and db_order.public_order_id:
                try:
                    client = await self._get_client()
                    await client.cancel_order(db_order.public_order_id)
                    log.info("Cancelled order %s on Public.com — awaiting terminal", db_order.public_order_id)
                    # A fill can race the cancel. Wait for terminal; if it filled,
                    # leave the fill to _poll_order_fill and tell the caller not to
                    # re-place (would oversell).
                    outcome = await self._await_public_order_terminal(
                        db_order.public_order_id, _CANCEL_CONFIRM_TIMEOUT()
                    )
                    if outcome == "FILLED":
                        log.warning("Public order %s FILLED during cancel — not cancelling, "
                                    "signalling caller to skip re-place", db_order.public_order_id)
                        return {
                            "status": "filled_during_cancel",
                            "order_id": db_order.id,
                            "osi": db_order.osi_symbol,
                        }
                    if outcome == "UNCONFIRMED" and require_confirmed_terminal:
                        # Could not prove the order is dead. Leave it live (do NOT
                        # stamp CANCELLED) and tell the re-peg to hold off — it may
                        # still fill; re-placing now could oversell.
                        log.warning("Public order %s cancel UNCONFIRMED — leaving live, "
                                    "signalling caller to hold off re-place", db_order.public_order_id)
                        return {
                            "status": "cancel_unconfirmed",
                            "order_id": db_order.id,
                            "osi": db_order.osi_symbol,
                        }
                except Exception as pub_exc:
                    # Public.com may return 404 if the option expired, was already filled,
                    # or was rejected server-side. Still mark locally as CANCELLED.
                    public_cancel_note = f" [Public.com: {pub_exc}]"
                    log.warning(
                        "Public.com cancel returned error for %s — marking cancelled locally anyway: %s",
                        db_order.public_order_id, pub_exc,
                    )

            # A local poll (_finalize_sell_fill) may have booked the fill while
            # we awaited terminal — _await_public_order_terminal only catches
            # BROKER-reported fills, so a local fill slips past that guard.
            # Re-read and never clobber a FILLED order to CANCELLED. Real-world:
            # HOOD260710C00120000 2026-07-09 order 986 filled @$1.05 then got
            # mislabelled CANCELLED, so the dashboard showed the exit as a
            # cancelled sell with no exit price.
            db.refresh(db_order)
            if db_order.status in ("FILLED", "PARTIALLY_FILLED") or (db_order.filled_qty or 0) > 0:
                log.warning(
                    "Order #%d filled during cancel (local poll) — not marking CANCELLED",
                    db_order.id,
                )
                return {
                    "status": "filled_during_cancel",
                    "order_id": db_order.id,
                    "osi": db_order.osi_symbol,
                }

            db_order.status = "CANCELLED"
            db_order.error_text = f"Cancelled by user{public_cancel_note}"
            asyncio.create_task(trade_logger.log_order_cancelled(
                osi=db_order.osi_symbol, side=db_order.side,
                reason=f"Cancelled by user{public_cancel_note}", order_id=db_order.id,
            ))
            db.commit()
            db.refresh(db_order)

            await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
            return {
                "status": "ok",
                "order_id": db_order.id,
                "osi": db_order.osi_symbol,
                "public_note": public_cancel_note or None,
            }
        finally:
            db.close()


    def _contracts_for_size(self, size_tag: str, explicit_qty: int | None = None, osi_symbol: str | None = None,
                            honor_qty: bool = False) -> int:
        """Return contract count. explicit_qty (from alert.qty) wins over size_tag,
        UNLESS [trading] ignore_message_qty=true — then the analyst's stated count
        is discarded and sizing always comes from size tags / size_default.
        honor_qty=True (see _honors_message_qty) exempts one alert from that,
        so a small-account author's stated count is mirrored while every other
        alert keeps size_default. Per-symbol max contracts limit is applied last
        if configured. Index-specific limit (index_max_contracts) only applies to
        index symbols like SPX, NDX, etc."""
        if explicit_qty and not honor_qty and cfg.getboolean("trading", "ignore_message_qty", fallback=False):
            log.info("[QTY] ignore_message_qty=true — dropping analyst count %d, sizing from tag %r / size_default",
                     explicit_qty, size_tag or "")
            explicit_qty = None
        if explicit_qty is not None and explicit_qty > 0:
            qty = explicit_qty
        else:
            normalized = (size_tag or "").strip().upper()
            sizes = _size_contracts()
            qty = max(1, sizes.get(normalized, sizes[""]))

        if not osi_symbol:
            return qty

        from app.risk.guardrails import _SYMBOL_MAX_CONTRACTS, _INDEX_MAX_CONTRACTS, _is_index_symbol, extract_root_symbol

        # Extract root symbol from OSI using centralized helper
        root_symbol = extract_root_symbol(osi_symbol)
        if not root_symbol:
            return qty

        # Check 1: Specific symbol limit (applies to all symbols)
        symbol_limits = _SYMBOL_MAX_CONTRACTS()
        if symbol_limits:
            max_for_symbol = symbol_limits.get(root_symbol)
            if max_for_symbol is not None and qty > max_for_symbol:
                log.info("[SYMBOL_LIMIT] %s: qty %d capped to %d (symbol_max_contracts)",
                         root_symbol, qty, max_for_symbol)
                return max_for_symbol

        # Check 2: Index-specific limit (only applies to index symbols like SPX, NDX, etc)
        index_max = _INDEX_MAX_CONTRACTS()
        if index_max is not None and _is_index_symbol(osi_symbol):
            if qty > index_max:
                log.info("[INDEX_LIMIT] %s is an index: qty %d capped to %d (index_max_contracts)",
                         root_symbol, qty, index_max)
                return index_max

        # Check 3: VIX-based volatility sizing
        try:
            from app.risk.vix_sizer import apply_vix_sizing
            vix_result = apply_vix_sizing(qty, root_symbol or osi_symbol)
            if vix_result.adjusted_qty != qty:
                log.info("[VIX_SIZER] %s: qty %d → %d (%s)",
                         root_symbol or osi_symbol, qty, vix_result.adjusted_qty, vix_result.reason)
            return vix_result.adjusted_qty
        except Exception as e:
            log.debug("VIX sizing failed, using original qty: %s", e)
            return qty

    def _resolve_sell_price(self, alert_price: Decimal | None, pos: Position) -> Decimal:
        if alert_price is not None:
            return alert_price
        if pos.current_price and pos.current_price > 0:
            return Decimal(str(round(pos.current_price, 2)))
        if pos.avg_price and pos.avg_price > 0:
            return Decimal(str(round(pos.avg_price, 2)))
        return Decimal("0.05")

    def _compute_limit(self, base_price: Decimal, side: str, slippage_override: Decimal | None = None) -> Decimal:
        if slippage_override is not None:
            slippage = slippage_override
        else:
            slippage = _BUY_SLIPPAGE() if side == "BUY" else _SELL_SLIPPAGE()
        multiplier = Decimal("1") + slippage if side == "BUY" else Decimal("1") - slippage
        raw = base_price * multiplier

        # Options price increments: $0.05 below $3.00, $0.10 at $3.00 and above
        increment = Decimal("0.05") if raw < Decimal("3.00") else Decimal("0.10")

        if side == "BUY":
            # Round UP to next valid increment (more aggressive, fills faster)
            limit_px = (Decimal(math.ceil(float(raw) / float(increment))) * increment).quantize(Decimal("0.01"))
        else:
            # Round DOWN to next valid increment
            limit_px = (Decimal(math.floor(float(raw) / float(increment))) * increment).quantize(Decimal("0.01"))

        return max(limit_px, increment)

    def _stamp_position_author(self, pos, alert) -> None:
        """Record the opening analyst on the position the first time it's seen.

        Position.author was never written before — NULL on every row — which
        silently broke per-author exit profiles and made author-scoped exits
        impossible. Preserve the first opener on scale-ins (don't overwrite):
        a shared UNIQUE-OSI position belongs to whoever opened it first.
        """
        if pos is None:
            return
        author = (getattr(alert, "_alert_author", "") or "").strip() or None
        if author and not pos.author:
            pos.author = author
        # Small-account-challenge tag (‼ / "small account" alerts). Display-only:
        # no [profile:small_acct] section exists, so profile resolution ignores
        # it. Never clobbers WHALE/RE-ENTRY (those stamps run later and win).
        if getattr(alert, "small_account", False) and not pos.strategy_tag:
            pos.strategy_tag = "SMALL_ACCT"

    def _match_open_positions(self, db, alert) -> list[Position]:
        query = db.query(Position).filter(
            Position.status.in_(["OPEN", "PARTIAL"]),
            Position.remaining > 0,
            # Vertical credit spreads are keyed "short|long" and are closed by
            # SpreadExecutor as one multileg order. A single-leg SELL must never
            # match one. It otherwise can: spread rows store expiry=None, so the
            # expiry filter below is skipped for an undated exit, and symbol +
            # strike + option_type alone will match the SHORT leg — sending a
            # one-legged SELL against a combo osi_symbol, which resolves to a
            # garbage OCC string at the broker and would half-close a spread.
            # Filtered here rather than at the call sites so the osi, symbol and
            # close_all paths are all covered by one guard.
            or_(Position.is_credit.is_(None), Position.is_credit.is_(False)),
        )

        if alert.close_all and not alert.symbol:
            matches = query.order_by(Position.open_time.asc()).all()
            scope = _CLOSE_ALL_SCOPE()
            author = (getattr(alert, "_alert_author", "") or "").strip()

            # Author-scoped (default): "sell everything" closes ONLY the
            # positions THIS analyst opened — never another analyst's book.
            # No attribution on the alert → close NOTHING (safe default); a
            # legacy/NULL-author position is "unowned" and never swept by a
            # scoped close_all. Set close_all_scope=all to restore the legacy
            # book-wide flatten. 2026-06-11: a single analyst's blank close_all
            # liquidated 3 unrelated positions (~$571) under the old behavior.
            if scope == "author":
                if not author:
                    log.warning(
                        "[SELL] Blank-symbol close_all with no author + "
                        "scope=author — closing 0 positions "
                        "(set trading.close_all_scope=all to override)",
                    )
                    return []
                al = author.lower()
                scoped = [p for p in matches if (p.author or "").strip().lower() == al]
                log.warning(
                    "[SELL] Blank-symbol close_all (scope=author, author=%s) — "
                    "closing %d of %d open position(s): %s",
                    author, len(scoped), len(matches),
                    [p.osi_symbol for p in scoped],
                )
                return scoped

            # scope == "all": legacy book-wide flatten. Destructive — log loud.
            log.warning(
                "[SELL] Blank-symbol close_all (scope=all) firing — closing %d "
                "position(s): %s | author=%s",
                len(matches), [p.osi_symbol for p in matches], author or None,
            )
            return matches

        if alert.osi_symbol:
            rows = query.filter(Position.osi_symbol == alert.osi_symbol).all()
        elif not alert.symbol:
            return []
        else:
            query = query.filter(Position.symbol == alert.symbol)
            if alert.expiry:
                query = query.filter(Position.expiry == str(alert.expiry))
            if alert.strike is not None:
                query = query.filter(Position.strike == alert.strike)
            if alert.option_type:
                query = query.filter(Position.option_type == alert.option_type)
            rows = query.order_by(Position.open_time.asc()).all()

        return _scope_to_owner(rows, alert)

    async def _cancel_pending_buys_for_alert(self, db, alert) -> int:
        """Cancel PENDING BUY orders that match this SELL alert.

        Called when a SELL alert finds no open Position. Analyst already
        exited; entering late after a missed BUY is a bad fill. Cancel
        any matching pending BUY to keep capital free and prevent the
        bot from owning a position the analyst no longer holds.

        Returns the number of orders cancelled.
        """
        query = db.query(Order).filter(
            Order.side == "BUY",
            Order.status == "PENDING",
        )

        if alert.osi_symbol:
            query = query.filter(Order.osi_symbol == alert.osi_symbol)
        elif alert.close_all and not alert.symbol:
            # Blank-symbol close_all cancels ALL pending BUYs.
            pass
        elif alert.symbol:
            # Symbol-only (no osi): cancel pending BUYs whose osi starts
            # with the symbol. Cheap prefix match; expiry/strike/right
            # parsing would be more precise but options OSI always leads
            # with the underlying symbol.
            query = query.filter(Order.osi_symbol.like(f"{alert.symbol}%"))
        else:
            return 0

        pending = query.all()
        # Same ownership rule as the position matcher: one analyst's exit must
        # not cancel another analyst's in-flight entry. Order.author is stamped
        # at placement; NULL-author orders stay cancellable (unowned).
        if pending and _owner_scoped():
            _author = (getattr(alert, "_alert_author", "") or "").strip()
            _mine = [o for o in pending if _pos_owned_by(o, _author)]
            if len(_mine) != len(pending):
                log.warning(
                    "[OWNER_SCOPE] %s: kept %d of %d pending BUY(s) — the rest belong "
                    "to another analyst and are not this exit's to cancel.",
                    alert.osi_symbol or alert.symbol, len(_mine), len(pending),
                )
            pending = _mine
        if not pending:
            return 0

        cancelled = 0
        for o in pending:
            try:
                await self.cancel_order(o.id)
                cancelled += 1
                order_logger.info(
                    "BUY CANCELLED (sell-on-pending) | %s | order_id=%d oid=%s | analyst exited before BUY filled",
                    o.osi_symbol, o.id, o.public_order_id,
                )
            except Exception as exc:
                log.error(
                    "[SELL] Failed to cancel pending BUY #%d (%s): %s",
                    o.id, o.osi_symbol, exc,
                )
        return cancelled

    def _mark_alert_status(
        self,
        db,
        alert_db_id: int | None,
        status: str,
        error_text: str | None = None,
        linked_order_id: int | None = None,
    ):
        """Update alert status in DB and broadcast an alert_update WS event."""
        if not alert_db_id:
            return
        db_alert = db.query(Alert).filter(Alert.id == alert_db_id).first()
        if not db_alert:
            return
        db_alert.status = status
        if error_text is not None:
            db_alert.error_text = error_text
        # Broadcast the updated alert to all dashboard clients immediately
        # This keeps the feed row in sync without a page refresh.
        payload = db_alert.to_dict()
        if linked_order_id is not None:
            payload["linkedOrderId"] = linked_order_id
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.ws_manager.broadcast({"type": "alert_update", "data": payload}))
        except RuntimeError as e:
            log.debug("Cannot broadcast alert update: no event loop (%s)", e)

    def _handle_order_failure(self, db, db_order: Order | None, alert_db_id: int | None, exc: Exception, osi_symbol: str, side: str):
        if db_order is not None:
            db_order.status = "REJECTED"
            db_order.error_text = str(exc)
        self._mark_alert_status(db, alert_db_id, "ERROR", str(exc))
        db.commit()
        if db_order is not None:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()}))
            except RuntimeError as e:
                log.debug("Cannot broadcast order update: no event loop (%s)", e)
        log.error("%s failed %s: %s", side, osi_symbol, exc)
