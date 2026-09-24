"""
ibkr_broker.py — IBKR executor as a thin subclass of PublicExecutor.

ARCHITECTURE
────────────
We inherit ALL of PublicExecutor's logic (guardrails, sizing, AI scoring,
re-peg loops, partial-fill recovery, slippage cooldown, DB writes, fill
finalization) and override only the four methods that talk to the broker:

    _place_on_public          → places limit order via ib_async
    _poll_order_fill          → polls ib_async Trade.orderStatus
    cancel_order              → cancels via ib_async (full DB update logic
                                copied because parent inlines client call)
    _cancel_external_blocking_orders → IBKR-side external open-order cleanup

Plus:
    execute                   → safety gate (orders_enabled config flag),
                                then delegates to parent
    _get_client / _load_sdk   → no-op stubs (would only be hit by an
                                un-overridden path; logged loudly so we
                                can spot missing overrides fast)

WHY SUBCLASS
────────────
PublicExecutor is ~2000 lines of trading-flow logic that is
broker-AGNOSTIC except at the four seams above. Duplicating all of it
in a standalone class would (a) double maintenance burden and (b) drift
out of sync the first time a fix lands on the Public side. Inheritance
keeps the two vendors locked to the same playbook.

SAFETY MODEL
────────────
Two-step opt-in to fire real IBKR orders:
    1. cfg[trading].broker = ibkr
    2. cfg[ibkr].orders_enabled = true

With step 2 OFF, execute() short-circuits before parent runs. Quotes,
account reads, and position-monitor remain functional (read-only path
is always safe).

OSI → IBKR contract translation lives in ibkr_sdk_bridge._osi_to_ib_option
(re-used here so SPX/SPXW exchange routing stays consistent).
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from app.core.db import SessionLocal
from app.core.models import Alert, Order, Position
from app.core.config_manager import cfg
from app.core.log_config import order_logger
import app.analytics.trade_logger as trade_logger

from app.execution.public_executor import (
    PublicExecutor,
)
try:
    # Eager-wake hook for the SELL re-peg watcher. Present on current base;
    # guard the import so an older deployed public_executor.py (without it)
    # can't break IBKRExecutor's module load — the wake just degrades to a
    # no-op until the base is updated.
    from app.execution.public_executor import _signal_reject
except ImportError:  # pragma: no cover
    def _signal_reject(order_db_id: int) -> None:
        pass
from app.execution.ibkr_sdk_bridge import (
    _get_ib,
    _osi_to_ib_option,
    IBKR_ACCOUNT,
    register_on_connect,
    get_cached_contract,
)
from app.core.profile_resolver import (
    pcfg_float as _pcfg_float,
    whale_resting_tp_enabled as _whale_resting_tp_enabled,
)

log = logging.getLogger(__name__)


def _ORDERS_ENABLED() -> bool:
    """Master safety gate. Must be true for IBKRExecutor to place any order."""
    return cfg.getboolean("ibkr", "orders_enabled", fallback=False)


def _WHALE_OCA_SL_PCT(pos) -> float:
    """Downside-stop % for the IBKR whale OCA (negative, e.g. -50). 0 / >=0
    disables the stop leg → degrades to a single resting take-profit. Profile-
    tunable under [profile:whale] (portal hot-reload), default -50. WHALE-ONLY;
    never read on the analyst lane (gated by strategy_tag == WHALE upstream)."""
    return _pcfg_float(pos, "whale_oca_sl_pct", -50.0)


def _PREWARM_QUOTE_ENABLED() -> bool:
    """When true, an entry signal's option ticker is subscribed the instant its
    OSI is known (before guardrails/scorer) so fetch_prices finds it warm and
    IBKR has price context for the order — killing the cold-quote/price-validation
    latency. IBKR-only; default off. Hot-reloadable."""
    return cfg.getboolean("ibkr", "prewarm_quote_enabled", fallback=False)


def _EVENT_DRIVEN_FILLS() -> bool:
    """When true, the fill-poll loop waits on the ib_async Trade.statusEvent
    (wakes within ms of a status change) instead of a fixed sleep — cuts up to
    `fill_poll_interval` of latency off every fill reaction (re-peg, OCA, SELL
    displacement). Worst case identical to polling (timeout falls back to the
    interval). Default off → legacy fixed-poll. [ibkr], one-knob rollback."""
    return cfg.getboolean("ibkr", "event_driven_fills", fallback=False)


def _WHALE_OCA_STOP_TRIGGER_METHOD() -> int:
    """IBKR Order.triggerMethod for the whale OCA stop leg. Default 8 (mid-
    point) — fires on fair-value decline, robust to a single stray low print
    that would false-trigger the default. Alternatives: 1 double-bid/ask,
    2 last-trade, 7 last-or-bid/ask, 0 IBKR default. [ibkr], portal-tunable."""
    return cfg.getint("ibkr", "whale_oca_stop_trigger_method", fallback=8)


def _ADAPTIVE_ENTRY_ENABLED() -> bool:
    """When true, IBKR BUY entries route via IBKR's Adaptive algo (auto-paces
    bid↔ask toward the limit cap, seeking price improvement) instead of a plain
    DAY limit. BUY-only; exits + whale OCA never use it. Default off → today's
    fixed-limit behavior. One-knob hot-reload rollback."""
    return cfg.getboolean("ibkr", "adaptive_entry_enabled", fallback=False)


def _ADAPTIVE_EXIT_ENABLED() -> bool:
    """When true, the FIRST placement of a SELL also routes via Adaptive.

    Separate from the BUY flag and default off, because an exit has a
    different failure mode from an entry: a BUY that doesn't fill costs an
    opportunity, an analyst SELL that doesn't fill costs the position. IBKR
    documents the price-improvement setting as trading against fill speed
    ("price variance"), and an unfilled analyst exit is what took a +50%
    SPXW trade to -13% on 2026-05-08.

    The re-peg lanes pass force_plain=True, so any replacement SELL is a
    plain aggressive limit. Adaptive therefore only ever gets the first
    buy_repeg_seconds window before the normal must-fill machinery takes
    over — and the plain re-place is also what keeps the cancel/fill
    double-sell guards (2026-07-09) behaving as they were tested.

    Never applies to the whale OCA bracket, which is placed elsewhere.
    """
    return cfg.getboolean("ibkr", "adaptive_exit_enabled", fallback=False)


_ADAPTIVE_PRIORITIES = {"Patient", "Normal", "Urgent"}


def _ADAPTIVE_PRIORITY() -> str:
    """Adaptive urgency: Patient (best price, slow) | Normal | Urgent (fast,
    least improvement). Default Urgent — 0DTE fills matter more than a penny."""
    v = (cfg.get("ibkr", "adaptive_priority", fallback="Urgent") or "Urgent").strip().capitalize()
    return v if v in _ADAPTIVE_PRIORITIES else "Urgent"


# IBKR surfaces an algo-not-supported reject in the Trade log; match it so the
# entry can fall back to a plain limit (e.g. some index-option venues reject
# Adaptive). Substring match — exact error code varies by venue/build.
# `algorithm` (not `algorithmic`) so 442 "Specified algorithm is invalid" —
# what SPXW returns for Adaptive — is caught; it also still covers "algorithmic".
_ADAPTIVE_REJECT_RE = re.compile(
    r"adaptive|algo.*not\s*support|unsupport.*algo|not\s*support.*algo|algorithm",
    re.I,
)

# 442 = "Specified algorithm is invalid". Code-match as well as text-match, so a
# reworded message in a future TWS build still routes to the plain-limit fallback.
_ADAPTIVE_REJECT_CODES = {442}


# IBKR rejects Adaptive outright on index options — error 442, "Specified
# algorithm is invalid" (live SPXW case 2026-08-20, two entries stranded).
# The 442 fallback below recovers those, but only after a wasted reject
# round-trip on EVERY entry, which on a 0DTE fill is exactly the latency that
# matters. So don't send the algo at all for roots known to refuse it.
#
# Seeded with the index roots; extract_root_symbol collapses the weekly
# variants (SPXW->SPX, NDXP->NDX, RUTW->RUT) so those are covered too. ETF
# roots (SPY/QQQ/IWM) are deliberately ABSENT — Adaptive works on those and
# they keep the price improvement. Any other root that turns out to reject is
# added at runtime by the fallback, so this list never needs maintaining.
_ADAPTIVE_UNSUPPORTED_ROOTS: set[str] = {
    "SPX", "NDX", "RUT", "VIX", "XSP", "DJX", "OEX", "XEO", "MRUT", "MNX",
}


def _adaptive_unsupported(osi_symbol: str) -> bool:
    """True if IBKR is known to reject the Adaptive algo for this OSI's root."""
    from app.risk.guardrails import extract_root_symbol  # lazy: import cycle
    root = extract_root_symbol(osi_symbol or "")
    return bool(root) and root in _ADAPTIVE_UNSUPPORTED_ROOTS


def _mark_adaptive_unsupported(osi_symbol: str) -> str | None:
    """Record that IBKR rejected Adaptive for this OSI's root so subsequent
    orders on it skip the algo. Returns the root if newly learned, else None.

    Process-lifetime memory only: a restart re-learns at the cost of one
    reject per root, which the fallback already handles cleanly. Not persisted
    — a stale "unsupported" survives nothing worse than losing price
    improvement, but a stale one written to disk would be invisible forever.
    """
    from app.risk.guardrails import extract_root_symbol
    root = extract_root_symbol(osi_symbol or "")
    if root and root not in _ADAPTIVE_UNSUPPORTED_ROOTS:
        _ADAPTIVE_UNSUPPORTED_ROOTS.add(root)
        return root
    return None


def _is_adaptive_reject(trade) -> bool:
    """True if `trade` was rejected for an unsupported/invalid algo."""
    for entry in (getattr(trade, "log", None) or []):
        code = getattr(entry, "errorCode", 0)
        if not code:
            continue
        if code in _ADAPTIVE_REJECT_CODES:
            return True
        if _ADAPTIVE_REJECT_RE.search(getattr(entry, "message", "") or ""):
            return True
    return False


def _DRY_RUN() -> bool:
    return cfg.getboolean("trading", "dry_run", fallback=False)


def _FILL_POLL_INTERVAL() -> int:
    return cfg.getint("trading", "fill_poll_interval_seconds", fallback=3)


def _MAX_PENDING_MINUTES() -> int:
    return cfg.getint("trading", "max_pending_minutes", fallback=30)


def _CANCEL_CONFIRM_TIMEOUT() -> float:
    # How long cancel_order waits for an order to reach a terminal state after
    # firing ib.cancelOrder(), so a fill that RACES the cancel is recorded (by
    # the per-order watcher) before a SELL re-peg places a replacement. Without
    # this the replacement and the still-live "cancelled" order both fill — the
    # UNH 430C double-sell (2026-07-08). 0 = legacy fire-and-forget.
    # 8s default: 3s was shorter than real IBKR cancel latency (~6s), so the
    # await timed out UNCONFIRMED mid-cancel (SPXW 7550C double-sell 2026-07-09).
    return cfg.getfloat("trading", "cancel_confirm_timeout_seconds", fallback=8.0)


def _MAX_PRICE_CAP_RETRIES() -> int:
    """How many times to auto-re-place a BUY/SELL after IBKR rejects it with
    Error 202 (limit too aggressive vs market). 0 disables the behavior."""
    return cfg.getint("ibkr", "price_cap_retries", fallback=2)


# IBKR Error 202 reject text:
#   "We cannot accept an order at a limit price at or more aggressive than 7.4.
#    Please submit your order using a limit price that is closer to the current
#    market price of 6.9."
# The first number is the cap our limit must stay strictly inside of.
_PRICE_CAP_RE = re.compile(r"more aggressive than\s*\$?([0-9]+(?:\.[0-9]+)?)", re.I)


def _option_tick(price: float) -> float:
    """Option price increment: $0.05 below $3.00, $0.10 at/above (matches
    PublicExecutor._compute_limit)."""
    return 0.05 if price < 3.00 else 0.10


def _capped_limit(side: str, cap: float) -> float:
    """Limit one valid tick *inside* IBKR's price cap.

    Error 202 rejects a price "at or more aggressive than" `cap`, so `cap`
    itself is not acceptable — step one increment to the less-aggressive side:
        BUY  → strictly below cap
        SELL → strictly above cap
    """
    inc = _option_tick(cap)
    if side == "BUY":
        n = math.floor((cap - 1e-6) / inc)
    else:
        n = math.ceil((cap + 1e-6) / inc)
    return max(round(n * inc, 2), inc)


def _detect_price_cap(trade) -> float | None:
    """If `trade` was cancelled by an IBKR Error 202 price-conformance reject,
    return the cap price IBKR quoted; else None. ib_async appends the error
    (errorCode + message) to trade.log when the reqId matches the order."""
    for entry in (getattr(trade, "log", None) or []):
        if getattr(entry, "errorCode", 0) == 202:
            m = _PRICE_CAP_RE.search(getattr(entry, "message", "") or "")
            if m:
                return float(m.group(1))
    return None


def _reject_reason(trade) -> str | None:
    """Most recent IBKR error message on the Trade (e.g. the 202 cap text or a
    margin/contract reject). Mirrors PublicExecutor surfacing reject_reason so
    the DB/dashboard shows WHY an order died instead of a bare status."""
    for entry in reversed(getattr(trade, "log", None) or []):
        msg = getattr(entry, "message", "") or ""
        if getattr(entry, "errorCode", 0) and msg:
            return msg
    return None


# ── Status mapping: ib_async → Public.com vocabulary ─────────────────────
# PublicExecutor's polling code expects status strings like FILLED,
# CANCELLED, PARTIALLY_FILLED, etc. ib_async uses different names.
# Translate at the boundary so parent code stays unmodified.
_IB_STATUS_MAP = {
    "PendingSubmit":  "PENDING",
    "PendingCancel":  "PENDING",
    "PreSubmitted":   "PENDING",
    "Submitted":      "PENDING",       # may become PARTIALLY_FILLED below
    "ApiPending":     "PENDING",
    "Filled":         "FILLED",
    "Cancelled":      "CANCELLED",
    "ApiCancelled":   "CANCELLED",
    "Inactive":       "CANCELLED",     # rejected / unworkable
    "Rejected":       "REJECTED",      # not a standard ib_async value, but handled if surfaced
}


def _translate_status(trade) -> str:
    """Map ib_async Trade.orderStatus.status into Public-vocabulary status."""
    raw = trade.orderStatus.status if trade and trade.orderStatus else ""
    mapped = _IB_STATUS_MAP.get(raw, "PENDING")
    # ib_async signals a partial via filled > 0 while still Submitted.
    if mapped == "PENDING":
        filled = float(trade.orderStatus.filled or 0)
        remaining = float(trade.orderStatus.remaining or 0)
        if filled > 0 and remaining > 0:
            return "PARTIALLY_FILLED"
    return mapped


# ─────────────────────────────────────────────────────────────────────────
# Adapter: wrap an ib_async Trade so it quacks like a Public.com order_resp.
# PublicExecutor._poll_order_fill reads order_resp.status.value /
# .average_price / .filled_quantity — we mimic that shape.
# ─────────────────────────────────────────────────────────────────────────

class _StatusShim:
    __slots__ = ("value",)
    def __init__(self, value: str):
        self.value = value


class _OrderRespShim:
    """Quacks like the SDK's Order response for PublicExecutor's polling code."""
    __slots__ = ("status", "average_price", "filled_quantity")

    def __init__(self, trade):
        self.status = _StatusShim(_translate_status(trade))
        self.average_price = float(trade.orderStatus.avgFillPrice or 0) or None
        self.filled_quantity = int(trade.orderStatus.filled or 0)


# ─────────────────────────────────────────────────────────────────────────
# IBKRExecutor
# ─────────────────────────────────────────────────────────────────────────

class IBKRExecutor(PublicExecutor):
    """IBKR-flavoured executor. Inherits PublicExecutor's full pipeline,
    overrides only the broker-IO seams."""

    name: str = "ibkr"

    def __init__(self, ws_manager):
        super().__init__(ws_manager)
        # broker_oid (permId or our oid string) → ib_async Trade handle.
        # _poll_order_fill reads this to avoid round-tripping IBKR for status.
        self._trades: dict[str, Any] = {}
        # oids of BUY entries placed with the Adaptive algo — read by the poll
        # loop to decide whether a CANCELLED was an algo-reject worth a
        # plain-limit fallback. Discarded on terminal/fill.
        self._adaptive_entry_oids: set[str] = set()
        # Serialize resume passes; fires on every IBKR (re)connect.
        self._resume_lock = asyncio.Lock()
        # oids for which we issued a cancel that did NOT confirm terminal (await
        # timed out / gateway reconnected mid-cancel). resume_pending_orders
        # re-issues the cancel on the next connect so a dropped cancel can't leave
        # the order live to fill after the re-peg moved on (SPXW 7550C 2026-07-09).
        self._pending_cancels: dict[str, float] = {}
        register_on_connect(self.resume_pending_orders)
        # oid → asyncio.Event set by the Trade.statusEvent so the poll loop can
        # react to a fill/cancel within ms (event-driven fills, flag-gated).
        self._order_wakes: dict[str, asyncio.Event] = {}

    # ── Safety gate ──────────────────────────────────────────────────
    async def execute(self, alert: Alert, alert_db_id: int, alert_author: str = "",
                      content_hash: str = "", _pipeline_timer=None):
        if not _ORDERS_ENABLED():
            log.warning(
                "[IBKR] orders_enabled=false — refusing to %s %s. "
                "Toggle [ibkr] orders_enabled in config.ini (or portal) to enable.",
                alert.action, alert.osi_symbol,
            )
            order_logger.warning(
                "IBKR_DISABLED | %s %s | author=%s",
                alert.action, alert.osi_symbol, alert_author,
            )
            return None
        # NOTE: quote pre-warm moved UPSTREAM to the ingest path
        # (discord_listener parse_complete → broker_router.prewarm_symbol). Firing
        # it here, at execute() start, was too late — fetch_prices runs only ~7ms
        # later (after guardrails) while a cold subscribe needs ~200ms, so it
        # raced its own fetch and never got ahead. Warming on-mention gives the
        # whole pre-order window (and warms re-mentions / SELL-then-rebuy / whale
        # spotted→decision flows). _PREWARM_QUOTE_ENABLED still gates it there.
        return await super().execute(alert, alert_db_id, alert_author, content_hash, _pipeline_timer)

    # ── SDK no-ops (the four broker seams below should bypass these) ─
    async def _get_client(self):
        """ib_async has no equivalent 'client' object; _get_ib() returns the IB instance.
        Surface a hard error if a path we forgot to override calls this."""
        raise NotImplementedError(
            "IBKRExecutor._get_client called — a Public-SDK code path was not "
            "overridden for IBKR. Check the four broker seams in ibkr_broker.py."
        )

    def _load_sdk(self):
        """Return shims so any inherited code that calls self._load_sdk()['NotFoundError']
        gets a usable exception class to except on."""
        class _NotFound(Exception):
            pass
        return {"NotFoundError": _NotFound}

    def _attach_wake(self, oid: str, trade) -> None:
        """Bind the Trade's status/fill events to a per-oid Event so the poll
        loop wakes within ms of a change. No-op when event_driven_fills is off
        or for dry/eventless trades."""
        if not _EVENT_DRIVEN_FILLS():
            return
        ev = self._order_wakes.get(oid)
        if ev is None:
            ev = asyncio.Event()
            self._order_wakes[oid] = ev

        def _wake(*_a, _ev=ev):
            _ev.set()
        try:
            trade.statusEvent += _wake   # type: ignore[attr-defined]
            trade.filledEvent += _wake   # type: ignore[attr-defined]
        except Exception:
            pass

    # ── Seam 1: place order ──────────────────────────────────────────
    async def _place_on_public(self, oid: str, osi_symbol: str, side: str,
                               quantity: int, limit_price, expiration=None,
                               *, _force_plain: bool = False):
        """Drop-in replacement for PublicExecutor._place_on_public.
        Places a DAY limit order via ib_async; stashes the Trade handle
        under our oid so _poll_order_fill can find it.

        `expiration` accepted for signature parity with the parent (used by the
        public.com whale resting-TP/GTD path, which is off for the IBKR lane) —
        IBKR orders here are DAY; GTD wiring is a TODO if ever needed."""
        from ib_async import LimitOrder  # imported lazily so importing this module
                                          # doesn't require ib_async at load time

        if _DRY_RUN():
            log.info("[IBKR DRY] %s %sx %s @ %s (oid=%s)", side, quantity, osi_symbol, limit_price, oid)
            # Mint a fake Trade-like for the dry-run path so polling can complete.
            self._trades[oid] = _DryTrade(side, quantity, float(limit_price))
            return

        ib = await _get_ib()
        # Reuse the already-qualified contract from the live quote cache (the
        # entry path just fetched a quote for this OSI) — skips a redundant
        # qualify round-trip on the hot path. Falls back to qualify on a miss.
        contract = get_cached_contract(osi_symbol)
        if contract is None:
            contract = _osi_to_ib_option(osi_symbol)
            qualified = await ib.qualifyContractsAsync(contract)
            contract = qualified[0] if qualified else None
        if contract is None or not getattr(contract, "conId", 0):
            # IBKR couldn't resolve the contract (ambiguous, expired, no security
            # def). Surface a clean error instead of crashing inside placeOrder
            # with AttributeError on a NoneType.
            raise RuntimeError(
                f"IBKR could not qualify contract for {osi_symbol} — "
                f"check tradingClass/exchange or contract may not exist"
            )
        order = LimitOrder(
            action=side,
            totalQuantity=quantity,
            lmtPrice=float(limit_price),
            tif="DAY",
            account=IBKR_ACCOUNT or None,
            orderRef=oid,        # human-readable tag stored on IBKR side
            transmit=True,
        )
        # Adaptive algo, flag-gated per side. Walks the limit from the passive
        # side toward the cap seeking price improvement. Any _force_plain
        # placement stays a plain limit: that covers both the algo-reject
        # fallback and every SELL re-peg replacement, so the must-fill exit
        # path is never left waiting on an algo. tif must stay DAY (Adaptive
        # requirement). On reject, _poll_order_fill re-places plain.
        _adaptive_ok = (_ADAPTIVE_ENTRY_ENABLED() if side == "BUY"
                        else _ADAPTIVE_EXIT_ENABLED())
        if _adaptive_ok and not _force_plain and _adaptive_unsupported(osi_symbol):
            _adaptive_ok = False
            log.debug("[IBKR] %s oid=%s plain limit — IBKR rejects Adaptive for %s",
                      side, oid, osi_symbol)
        if not _force_plain and _adaptive_ok:
            from ib_async import TagValue  # lazy, same as LimitOrder
            order.algoStrategy = "Adaptive"
            order.algoParams = [TagValue("adaptivePriority", _ADAPTIVE_PRIORITY())]
            self._adaptive_entry_oids.add(oid)
            log.info("[IBKR] %s oid=%s via Adaptive algo (priority=%s, cap=%s)",
                     side, oid, _ADAPTIVE_PRIORITY(), limit_price)
        trade = ib.placeOrder(contract, order)
        # Stash under our internal oid so subsequent poll can locate it.
        # Also stash under permId once IBKR assigns one (event-driven later).
        self._trades[oid] = trade
        self._attach_wake(oid, trade)

        # ib_async's openOrderEvent will populate permId asynchronously; once
        # it's known, dual-key the trade so external lookups by permId work.
        def _on_status(_t=trade):
            pid = getattr(_t.order, "permId", None)
            if pid and str(pid) not in self._trades:
                self._trades[str(pid)] = _t
        try:
            trade.statusEvent += _on_status  # type: ignore[attr-defined]
        except Exception:
            pass

        log.info("[IBKR] %s placed oid=%s %sx %s @ %s (orderId=%s)",
                 side, oid, quantity, osi_symbol, limit_price,
                 trade.order.orderId or "?")

    @staticmethod
    def _log_fill_economics(trade, public_oid: str, order_db_id: int) -> None:
        """Log real commission, realized PnL, and maker/taker (lastLiquidity)
        per execution once a fill lands. Visibility only — no DB write, no
        schema change. ib_async populates Trade.fills[].commissionReport
        shortly after the fill; missing fields are logged as-is."""
        try:
            for f in (getattr(trade, "fills", None) or []):
                cr = getattr(f, "commissionReport", None)
                ex = getattr(f, "execution", None)
                comm = getattr(cr, "commission", None) if cr else None
                rpnl = getattr(cr, "realizedPNL", None) if cr else None
                liq = getattr(ex, "lastLiquidity", None) if ex else None  # 1=add,2=remove
                px = getattr(ex, "price", None) if ex else None
                shr = getattr(ex, "shares", None) if ex else None
                order_logger.info(
                    "FILL_ECON | order_id=%s oid=%s | px=%s qty=%s comm=%s realizedPNL=%s liq=%s",
                    order_db_id, public_oid, px, shr, comm, rpnl, liq,
                )
        except Exception as e:
            log.debug("[IBKR] fill-econ log failed for %s: %s", public_oid, e)

    # ── Seam 2: poll for fill ────────────────────────────────────────
    async def _poll_order_fill(self, order_db_id: int, public_oid: str, alert,
                               alert_db_id, side: str, qty: int,
                               position_id: int | None = None,
                               max_pending_minutes_override: int | None = None):
        """Re-implementation of PublicExecutor._poll_order_fill against ib_async.

        Mirrors the parent's logic (FILLED finalization, PARTIALLY_FILLED
        intermediate updates, terminal-state handling, timeout cancel,
        consecutive-error breakout) but reads state from a local Trade
        object instead of round-tripping the broker.
        """
        TERMINAL = {"FILLED", "CANCELLED", "QUEUED_CANCELLED", "REJECTED", "EXPIRED", "REPLACED"}
        poll = _FILL_POLL_INTERVAL()
        effective_max_min = (max_pending_minutes_override
                             if max_pending_minutes_override is not None
                             else _MAX_PENDING_MINUTES())
        timeout_s = effective_max_min * 60 if effective_max_min > 0 else 0
        elapsed = 0
        not_found_grace = 15
        consecutive_errors = 0
        MAX_CONSECUTIVE_ERRORS = 10
        price_cap_retries_left = _MAX_PRICE_CAP_RETRIES()

        log.info("[IBKR] poll start oid=%s (db=%s, timeout=%sm)",
                 public_oid, order_db_id, effective_max_min or "∞")
        order_logger.info("POLL START | order_id=%s oid=%s | side=%s qty=%d | timeout=%sm",
                          order_db_id, public_oid, side, qty,
                          effective_max_min or "∞")

        while True:
            # Event-driven: wake within ms of a status/fill change, falling back
            # to the poll interval as a ceiling. Legacy fixed-sleep when off.
            # elapsed tracks REAL time waited (not +poll) so early wakes don't
            # inflate it and trip the timeout early.
            _t0 = time.monotonic()
            ev = self._order_wakes.get(public_oid) if _EVENT_DRIVEN_FILLS() else None
            if ev is not None:
                try:
                    await asyncio.wait_for(ev.wait(), timeout=poll)
                except asyncio.TimeoutError:
                    pass
                ev.clear()
            else:
                await asyncio.sleep(poll)
            elapsed += time.monotonic() - _t0

            # ── Timeout: auto-cancel stale pending ─────────────
            if timeout_s > 0 and elapsed >= timeout_s:
                max_min = effective_max_min
                log.warning("[IBKR] order %s timed out after %dm — auto-cancelling",
                            public_oid, max_min)
                db = SessionLocal()
                try:
                    trade = self._trades.get(public_oid)
                    if trade and not _DRY_RUN():
                        try:
                            ib = await _get_ib()
                            ib.cancelOrder(trade.order)
                        except Exception as ex:
                            log.warning("[IBKR] timeout cancel failed (may already be terminal): %s", ex)
                    db_order = db.query(Order).filter(Order.id == order_db_id).first()
                    if db_order and db_order.status == "PENDING":
                        db_order.status = "CANCELLED"
                        db_order.error_text = f"Auto-cancelled: no fill after {max_min} minutes"
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
                self._trades.pop(public_oid, None)
                return

            # ── Read current state ─────────────────────────────
            trade = self._trades.get(public_oid)
            if trade is None:
                # Self-heal: an IBKR reconnect (or a container restart) drops the
                # in-memory trade map while the order is still live at IBKR.
                # Recover the Trade from the live session by matching
                # orderRef == our oid, so the fill still books instead of the
                # order orphaning and getting auto-cancelled as a phantom reject.
                try:
                    ib = await _get_ib()
                    for _t in ib.trades():
                        if (getattr(_t.order, "orderRef", "") or "") == public_oid:
                            self._trades[public_oid] = _t
                            trade = _t
                            log.info("[IBKR] order %s recovered from live session after reconnect", public_oid)
                            break
                except Exception as ex:
                    log.debug("[IBKR] recover lookup failed for %s: %s", public_oid, ex)
            if trade is None:
                if elapsed <= not_found_grace:
                    log.debug("[IBKR] order %s not yet tracked (elapsed=%ds)", public_oid, elapsed)
                else:
                    log.warning("[IBKR] order %s missing from trade map after %ds", public_oid, elapsed)
                continue

            try:
                order_resp = _OrderRespShim(trade)
                status_str = order_resp.status.value

                if status_str == "FILLED":
                    fill_px = order_resp.average_price
                    filled_qty_from_broker = order_resp.filled_quantity
                    db = SessionLocal()
                    try:
                        db_order = db.query(Order).filter(Order.id == order_db_id).first()
                        if not db_order:
                            return
                        filled_qty_actual = (
                            filled_qty_from_broker if filled_qty_from_broker > 0
                            else (db_order.filled_qty or 0)
                        )
                        if filled_qty_actual <= 0:
                            log.error(
                                "[IBKR] order %s FILLED but no qty reported! Falling back to ordered qty=%d",
                                public_oid, qty,
                            )
                            filled_qty_actual = qty
                        log.info("[IBKR] order %s FILLED @ $%s (qty=%d/%d, broker=%d)",
                                 public_oid, fill_px, filled_qty_actual, qty, filled_qty_from_broker)
                        order_logger.info("FILLED | order_id=%s | filled=%d/%d | broker_qty=%d | price=%s",
                                          order_db_id, filled_qty_actual, qty, filled_qty_from_broker, fill_px)

                        if side == "BUY" and alert:
                            await self._finalize_buy_fill(db, db_order, alert, alert_db_id, fill_px, filled_qty_actual)
                        elif side == "SELL" and position_id:
                            pos = db.query(Position).filter(Position.id == position_id).first()
                            if pos:
                                trigger = db_order.trigger or "DISCORD"
                                await self._finalize_sell_fill(db, db_order, pos, fill_px, filled_qty_actual, trigger)
                        else:
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
                    self._log_fill_economics(trade, public_oid, order_db_id)
                    self._trades.pop(public_oid, None)
                    self._adaptive_entry_oids.discard(public_oid)
                    self._order_wakes.pop(public_oid, None)
                    return

                elif status_str == "PARTIALLY_FILLED":
                    filled_so_far = order_resp.filled_quantity
                    fill_px_partial = order_resp.average_price
                    if filled_so_far > 0 and fill_px_partial and side == "BUY" and alert:
                        db = SessionLocal()
                        try:
                            db_order = db.query(Order).filter(Order.id == order_db_id).first()
                            if db_order and db_order.filled_qty != filled_so_far:
                                await self._finalize_partial_buy_fill(
                                    db, db_order, alert, alert_db_id, fill_px_partial, filled_so_far,
                                )
                        finally:
                            db.close()
                    log.info("[IBKR] order %s PARTIALLY_FILLED: %d/%d @ $%s — still polling",
                             public_oid, filled_so_far, qty, fill_px_partial)

                elif status_str in TERMINAL:
                    # ── Adaptive algo reject → re-place once as a plain limit ──
                    # Some venues (notably index options) reject the Adaptive
                    # algo. Fall back to a plain DAY limit at the same cap so
                    # the entry still fires instead of stranding.
                    if (status_str == "CANCELLED"
                            and public_oid in self._adaptive_entry_oids
                            and _is_adaptive_reject(trade)):
                        self._adaptive_entry_oids.discard(public_oid)
                        _osi = _lim = None
                        db = SessionLocal()
                        try:
                            _o = db.query(Order).filter(Order.id == order_db_id).first()
                            if _o:
                                _osi, _lim = _o.osi_symbol, _o.limit_price
                        finally:
                            db.close()
                        if _osi and _lim:
                            _learned = _mark_adaptive_unsupported(_osi)
                            if _learned:
                                log.warning("[IBKR] Adaptive unsupported for root %s — "
                                            "skipping the algo for it from now on", _learned)
                            log.warning("[IBKR] order %s Adaptive rejected — re-placing plain DAY limit @ %s",
                                        public_oid, _lim)
                            order_logger.info("ADAPTIVE_FALLBACK | order_id=%s oid=%s | plain limit @ %s",
                                              order_db_id, public_oid, _lim)
                            self._trades.pop(public_oid, None)
                            try:
                                await self._place_on_public(public_oid, _osi, side, qty, _lim, _force_plain=True)
                                elapsed = 0  # restart timeout window for the re-placed order
                                continue
                            except Exception as ex:
                                log.error("[IBKR] adaptive fallback re-place failed for %s: %s — giving up",
                                          public_oid, ex)
                                # fall through to terminal handling below

                    # ── Auto-retry on IBKR price-conformance reject (Error 202) ──
                    # IBKR hard-cancels a limit that is too aggressive vs market
                    # before our re-peg watcher can act. Re-place once at a limit
                    # one tick inside IBKR's quoted cap so the order can rest/fill.
                    if status_str == "CANCELLED" and price_cap_retries_left > 0:
                        cap = _detect_price_cap(trade)
                        if cap is not None:
                            new_limit = _capped_limit(side, cap)
                            price_cap_retries_left -= 1
                            log.warning(
                                "[IBKR] order %s rejected by price cap (202) cap=%.2f — "
                                "re-placing at %.2f (%d retries left)",
                                public_oid, cap, new_limit, price_cap_retries_left)
                            order_logger.info(
                                "PRICE_CAP_RETRY | order_id=%s oid=%s | cap=%.2f new_limit=%.2f | retries_left=%d",
                                order_db_id, public_oid, cap, new_limit, price_cap_retries_left)
                            osi = None
                            db = SessionLocal()
                            try:
                                db_order = db.query(Order).filter(Order.id == order_db_id).first()
                                if db_order:
                                    osi = db_order.osi_symbol
                                    db_order.error_text = (
                                        f"IBKR price cap {cap} — re-placed @ {new_limit}")
                                    db.commit()
                            finally:
                                db.close()
                            if osi:
                                self._trades.pop(public_oid, None)
                                try:
                                    await self._place_on_public(public_oid, osi, side, qty, new_limit)
                                    elapsed = 0  # restart the timeout window for the new order
                                    continue
                                except Exception as ex:
                                    log.error("[IBKR] price-cap re-place failed for %s: %s — giving up",
                                              public_oid, ex)
                                    # fall through to terminal handling below

                    reason = _reject_reason(trade)
                    if reason:
                        log.warning("[IBKR] order %s terminal status: %s | reason: %s",
                                    public_oid, status_str, reason)
                    else:
                        log.warning("[IBKR] order %s terminal status: %s", public_oid, status_str)
                    # Wake any SELL re-peg watcher napping on this order so it
                    # re-pegs in <1s instead of waiting out the full nap — parity
                    # with PublicExecutor's terminal path. Critical for the
                    # "analyst SELL must fill" re-peg lane. No-op if no watcher.
                    _signal_reject(order_db_id)
                    db = SessionLocal()
                    try:
                        db_order = db.query(Order).filter(Order.id == order_db_id).first()
                        if db_order:
                            db_order.status = status_str
                            db_order.error_text = (
                                f"Order {status_str} on IBKR: {reason}"
                                if reason else f"Order {status_str} on IBKR"
                            )
                            if alert_db_id:
                                self._mark_alert_status(db, alert_db_id, status_str)
                            db.commit()
                            await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
                    finally:
                        db.close()
                    self._trades.pop(public_oid, None)
                    self._adaptive_entry_oids.discard(public_oid)
                    self._order_wakes.pop(public_oid, None)
                    return

                # else: still PENDING, keep polling

            except Exception as exc:
                consecutive_errors += 1
                log.error("[IBKR] poll error for %s (%d/%d): %s",
                          public_oid, consecutive_errors, MAX_CONSECUTIVE_ERRORS, exc)
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    log.error("[IBKR] order %s: %d consecutive errors — marking ERROR",
                              public_oid, consecutive_errors)
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
                    self._trades.pop(public_oid, None)
                    self._adaptive_entry_oids.discard(public_oid)
                    self._order_wakes.pop(public_oid, None)
                    return
                continue
            else:
                consecutive_errors = 0

    # ── Seam 3: cancel a single order ────────────────────────────────
    async def _await_trade_terminal(self, trade, timeout: float) -> str:
        """Poll an ib_async Trade until it reaches a terminal state or `timeout`
        seconds elapse. Returns:
          "FILLED"      — ended FILLED (a fill raced the cancel).
          "CANCELLED"   — confirmed terminal non-fill (Cancelled/ApiCancelled/Inactive).
          "UNCONFIRMED" — still working (e.g. PendingCancel) when the window closed;
                          the order may yet fill.
        Sleeping yields to the event loop so the per-order watcher can record the
        fill/cancel meanwhile."""
        def _classify(status: str, done: bool) -> str | None:
            if status == "Filled":
                return "FILLED"
            if done:
                return "FILLED" if status == "Filled" else "CANCELLED"
            return None
        if timeout <= 0:
            status = getattr(getattr(trade, "orderStatus", None), "status", "") or ""
            return "FILLED" if status == "Filled" else "UNCONFIRMED"
        loop = asyncio.get_event_loop()
        end = loop.time() + timeout
        while loop.time() < end:
            status = getattr(getattr(trade, "orderStatus", None), "status", "") or ""
            try:
                done = trade.isDone()
            except Exception:
                done = status in ("Cancelled", "ApiCancelled", "Inactive")
            verdict = _classify(status, done)
            if verdict:
                return verdict
            await asyncio.sleep(0.1)
        status = getattr(getattr(trade, "orderStatus", None), "status", "") or ""
        return "FILLED" if status == "Filled" else "UNCONFIRMED"

    async def cancel_order(self, order_db_id: int, *,
                           require_confirmed_terminal: bool = False) -> dict:
        """IBKR cancel. Mirrors PublicExecutor.cancel_order's contract and 3-way
        return (filled_during_cancel | cancel_unconfirmed | ok).

        After firing the cancel we AWAIT a terminal state (up to
        cancel_confirm_timeout_seconds):
          - FILLED during cancel → return filled_during_cancel, don't mark
            CANCELLED (position already reduced; prevents the UNH 430C double-sell).
          - UNCONFIRMED (still PendingCancel, or gateway reconnected mid-cancel)
            with require_confirmed_terminal → journal the oid for re-issue on the
            next connect and return cancel_unconfirmed WITHOUT stamping CANCELLED,
            so the re-peg holds off (SPXW 7550C 2026-07-09: order filled after
            PendingCancel + reconnect while the re-peg had already re-placed).
          - otherwise → confirmed cancelled, mark CANCELLED, return ok."""
        db = SessionLocal()
        try:
            db_order = db.query(Order).filter(Order.id == order_db_id).first()
            if not db_order:
                raise ValueError(f"Order {order_db_id} not found in database")
            if db_order.status not in ("PENDING", "PARTIALLY_FILLED"):
                raise ValueError(f"Order #{order_db_id} is already {db_order.status} — nothing to cancel")

            oid = db_order.public_order_id
            note = ""
            if not _DRY_RUN() and oid:
                trade = self._trades.get(oid)
                try:
                    if trade is not None:
                        ib = await _get_ib()
                        # Journal BEFORE the await so a reconnect that fires during
                        # the wait still finds the in-flight cancel to re-issue.
                        self._pending_cancels[oid] = asyncio.get_event_loop().time()
                        ib.cancelOrder(trade.order)
                        log.info("[IBKR] cancel requested oid=%s — awaiting terminal", oid)
                        outcome = await self._await_trade_terminal(trade, _CANCEL_CONFIRM_TIMEOUT())
                        if outcome == "FILLED":
                            self._pending_cancels.pop(oid, None)
                            log.warning("[IBKR] oid=%s FILLED during cancel — not cancelling, "
                                        "signalling caller to skip re-place", oid)
                            return {
                                "status": "filled_during_cancel",
                                "order_id": db_order.id,
                                "osi": db_order.osi_symbol,
                            }
                        if outcome == "UNCONFIRMED" and require_confirmed_terminal:
                            # Cancel not confirmed terminal. Leave the order live and
                            # journalled; resume_pending_orders re-issues on reconnect.
                            # Do NOT stamp CANCELLED, do NOT drop the Trade handle.
                            log.warning("[IBKR] oid=%s cancel UNCONFIRMED — leaving live + journalled, "
                                        "signalling caller to hold off re-place", oid)
                            return {
                                "status": "cancel_unconfirmed",
                                "order_id": db_order.id,
                                "osi": db_order.osi_symbol,
                            }
                        # Confirmed terminal (or unconfirmed without require) → fall
                        # through and mark CANCELLED.
                        self._pending_cancels.pop(oid, None)
                    else:
                        # No live Trade handle — likely a resumed-pending order
                        # from before a restart. Cancel by orderId via the IB connection.
                        log.warning("[IBKR] no Trade handle for oid=%s — cannot precisely cancel "
                                    "(post-restart resumed order). Marking CANCELLED locally.", oid)
                except Exception as ex:
                    self._pending_cancels.pop(oid, None)
                    note = f" [IBKR: {ex}]"
                    log.warning("[IBKR] cancel returned error for %s — marking cancelled locally: %s",
                                oid, ex)

            # A per-order watcher may have booked the fill while we awaited
            # terminal. Re-read and never clobber a FILLED order to CANCELLED.
            db.refresh(db_order)
            if db_order.status in ("FILLED", "PARTIALLY_FILLED") or (db_order.filled_qty or 0) > 0:
                if oid:
                    self._pending_cancels.pop(oid, None)
                log.warning("[IBKR] order #%d filled during cancel (local) — not marking CANCELLED",
                            db_order.id)
                return {
                    "status": "filled_during_cancel",
                    "order_id": db_order.id,
                    "osi": db_order.osi_symbol,
                }

            db_order.status = "CANCELLED"
            db_order.error_text = f"Cancelled by user{note}"
            asyncio.create_task(trade_logger.log_order_cancelled(
                osi=db_order.osi_symbol, side=db_order.side,
                reason=f"Cancelled by user{note}", order_id=db_order.id,
            ))
            db.commit()
            db.refresh(db_order)
            await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
            if oid:
                self._trades.pop(oid, None)
                self._pending_cancels.pop(oid, None)
            return {
                "status": "ok",
                "order_id": db_order.id,
                "osi": db_order.osi_symbol,
                "public_note": note or None,
            }
        finally:
            db.close()

    # ── Seam 4: external blocking-order cleanup ──────────────────────
    async def _cancel_external_blocking_orders(self, broker_symbol: str) -> int:
        """IBKR analogue of PublicExecutor._cancel_external_blocking_orders.

        Cancels open orders on IBKR for `broker_symbol` that this bot did
        not place (no matching orderRef in our DB). Same use case as the
        Public version: operator left a manual order open from a different
        client and it's blocking an auto-exit.
        """
        if _DRY_RUN():
            return 0

        ib = await _get_ib()
        cancelled = 0

        # Build the set of orderRefs we own.
        db = SessionLocal()
        try:
            our_refs = {
                o.public_order_id for o in db.query(Order).filter(
                    Order.public_order_id.isnot(None)
                ).all() if o.public_order_id
            }
        finally:
            db.close()

        for trade in ib.openTrades():
            try:
                c = trade.contract
                # Match by IBKR symbol — SPX/SPXW both map to IBKR symbol 'SPX'.
                # broker_symbol arrives as the OSI root ('SPX' / 'SPXW' / 'AAPL').
                ib_symbol_for_root = "SPX" if broker_symbol in ("SPX", "SPXW") else broker_symbol
                if (c.symbol or "") != ib_symbol_for_root:
                    continue
                # Skip our own orders (orderRef matches our DB oid).
                ref = getattr(trade.order, "orderRef", "") or ""
                if ref in our_refs:
                    continue
                log.warning("[IBKR EXTERNAL_CANCEL] cancelling external open order on %s (orderId=%s ref=%s)",
                            broker_symbol, trade.order.orderId, ref or "(none)")
                ib.cancelOrder(trade.order)
                cancelled += 1
                asyncio.create_task(trade_logger.log_order_cancelled(
                    osi=broker_symbol, side="EXTERNAL",
                    reason=f"Bot cancelled external IBKR order {trade.order.orderId} blocking auto-exit",
                ))
            except Exception as exc:
                log.error("[IBKR EXTERNAL_CANCEL] failed for orderId=%s: %s",
                          getattr(trade.order, "orderId", "?"), exc)
        return cancelled

    # ── Whale resting exit: native IBKR OCA bracket (TP limit + SL stop) ──
    async def _maybe_place_whale_tp(self, db, pos: Position):
        """IBKR override of PublicExecutor._maybe_place_whale_tp.

        WHALE-ONLY. On a whale BUY fill, rest a broker-side OCA bracket so the
        position exits at IBKR even if the bot/VM is down:
            leg A  SELL LIMIT @ avg × (1 + pt1_pct/100)   (take-profit)
            leg B  SELL STOP  @ avg × (1 + sl_pct/100)    (stop-loss, sl_pct<0)
        Both share an OCA group (ocaType=1): whichever fills, IBKR auto-cancels
        the other. The TP leg is recorded with trigger='WHALE_TP' so the EXISTING
        position_monitor gate (pm.py:454) suppresses the poll-based pt1 — no
        monitor/public-executor change needed. The SL leg is 'WHALE_SL'.

        Gated by whale_resting_tp_enabled (same master flag as the public lane).
        Stop leg disabled when whale_oca_sl_pct >= 0 → degrades to TP-only.
        Failure is non-fatal: the poll-based +15% exit remains the backstop
        (pt1 only skips while a live WHALE_TP rests). public_executor untouched.
        """
        if not _whale_resting_tp_enabled():
            return
        if (getattr(pos, "strategy_tag", "") or "") != "WHALE":
            return
        if pos.remaining <= 0 or not pos.avg_price:
            return
        # Dry-run: defer to the parent's DAY/simulated path (instant dry fill);
        # don't touch the live IBKR session.
        if _DRY_RUN():
            return await super()._maybe_place_whale_tp(db, pos)
        # Don't double-place if a resting TP already exists for this OSI.
        existing = db.query(Order).filter(
            Order.osi_symbol == pos.osi_symbol,
            Order.side == "SELL",
            Order.status == "PENDING",
            Order.trigger == "WHALE_TP",
        ).first()
        if existing:
            return

        qty = pos.remaining
        avg = Decimal(str(pos.avg_price))
        tp_pct = Decimal(str(_pcfg_float(pos, "pt1_pct", 15.0)))
        tp_px = float((avg * (Decimal(1) + tp_pct / Decimal(100))).quantize(Decimal("0.01")))
        sl_pct = _WHALE_OCA_SL_PCT(pos)
        sl_px = (
            float((avg * (Decimal(1) + Decimal(str(sl_pct)) / Decimal(100))).quantize(Decimal("0.01")))
            if sl_pct < 0 else None
        )

        try:
            await self._place_whale_oca(db, pos, qty, tp_px, sl_px)
        except Exception as e:
            log.error("[WHALE_OCA] place failed for %s: %s — poll exit (pt1) will back it up",
                      pos.osi_symbol, e)

    async def _place_whale_oca(self, db, pos: Position, qty: int,
                               tp_px: float, sl_px: float | None):
        """Place the OCA bracket on IBKR + record both legs in the DB, then
        attach a SELL fill-watcher to each. Whichever leg fills books via
        _finalize_sell_fill; the OCA-cancelled sibling books as CANCELLED."""
        from ib_async import LimitOrder, StopOrder  # lazy — keep module import light

        ib = await _get_ib()
        contract = get_cached_contract(pos.osi_symbol)
        if contract is None:
            contract = _osi_to_ib_option(pos.osi_symbol)
            qualified = await ib.qualifyContractsAsync(contract)
            contract = qualified[0] if qualified else None
        if contract is None or not getattr(contract, "conId", 0):
            raise RuntimeError(
                f"IBKR could not qualify contract for {pos.osi_symbol} (whale OCA)")

        oca_group = f"WHALEOCA-{pos.id}-{uuid.uuid4().hex[:8]}"

        def _mk_row(px: float, trigger: str) -> tuple[Order, str]:
            oid = str(uuid.uuid4())
            row = Order(
                public_order_id=oid,
                osi_symbol=pos.osi_symbol,
                side="SELL",
                quantity=qty,
                limit_price=px,
                status="PENDING",
                trigger=trigger,
            )
            db.add(row)
            return row, oid

        legs: list[tuple[Order, str, Any]] = []

        tp_row, tp_oid = _mk_row(tp_px, "WHALE_TP")
        tp_order = LimitOrder(
            action="SELL", totalQuantity=qty, lmtPrice=tp_px, tif="GTC",
            account=IBKR_ACCOUNT or None, orderRef=tp_oid, transmit=True,
        )
        tp_order.ocaGroup = oca_group
        tp_order.ocaType = 1  # cancel all remaining on fill, with block
        legs.append((tp_row, tp_oid, tp_order))

        if sl_px is not None:
            sl_row, sl_oid = _mk_row(sl_px, "WHALE_SL")
            sl_order = StopOrder(
                action="SELL", totalQuantity=qty, stopPrice=sl_px, tif="GTC",
                account=IBKR_ACCOUNT or None, orderRef=sl_oid, transmit=True,
            )
            sl_order.ocaGroup = oca_group
            sl_order.ocaType = 1
            # Trigger on mid-point (default) so a single stray low print can't
            # false-trip the protective stop. Tunable via [ibkr].
            sl_order.triggerMethod = _WHALE_OCA_STOP_TRIGGER_METHOD()
            legs.append((sl_row, sl_oid, sl_order))

        db.commit()
        for row, _oid, _o in legs:
            db.refresh(row)

        log.info("[WHALE_OCA] %s x%d — TP %s%s (oca=%s)",
                 pos.osi_symbol, qty, tp_px,
                 f" / SL {sl_px}" if sl_px is not None else " (TP-only)", oca_group)
        order_logger.info("WHALE_OCA | %s x%d | tp=%s sl=%s | oca=%s",
                          pos.osi_symbol, qty, tp_px, sl_px, oca_group)

        for row, oid, order in legs:
            trade = ib.placeOrder(contract, order)
            self._trades[oid] = trade
            self._attach_wake(oid, trade)
            await self.ws_manager.broadcast({"type": "order_update", "data": row.to_dict()})
            asyncio.create_task(trade_logger.log_order_placed(
                side="SELL", osi=pos.osi_symbol, qty=qty,
                limit_price=row.limit_price, trigger=row.trigger, order_id=row.id,
            ))
            self._track_task(self._poll_order_fill(
                order_db_id=row.id,
                public_oid=oid,
                alert=None,
                alert_db_id=None,
                side="SELL",
                qty=qty,
                position_id=pos.id,
                # Resting bracket must NOT auto-cancel — it rests until filled
                # or OCA-cancelled by its sibling. 0 disables the poll timeout.
                max_pending_minutes_override=0,
            ))

    # ── Crash/restart recovery: IBKR has no reconciler (Public-only) ─────
    async def resume_pending_orders(self):
        """Fired on every IBKR (re)connect (via register_on_connect). For each
        DB-PENDING IBKR order with no live fill-watcher, either re-attach a
        watcher (order still working at the broker) or book the outcome that
        happened while we were disconnected (filled → finalize; gone → cancel).

        Closes the orphan gap for broker-resting orders (esp. the whale OCA):
        without this, an OCA leg that fills during a container restart would
        leave the position OPEN and PENDING forever. IBKR-only; the Public lane
        has its own reconciler and never reaches this code.
        """
        if self._resume_lock.locked():
            return
        async with self._resume_lock:
            if _DRY_RUN():
                return
            log.info("[IBKR RESUME] connect hook fired — scanning pending IBKR orders")
            try:
                ib = await _get_ib()
            except Exception as ex:
                log.warning("[IBKR RESUME] no IB connection: %s", ex)
                return

            db = SessionLocal()
            try:
                rows = [
                    (o.id, o.public_order_id, o.side, o.quantity, o.osi_symbol, o.trigger)
                    for o in db.query(Order).filter(
                        Order.status.in_(["PENDING", "PARTIALLY_FILLED"]),
                        Order.public_order_id.isnot(None),
                    ).all()
                ]
            finally:
                db.close()
            if not rows:
                return

            live: dict[str, Any] = {}
            try:
                for t in ib.openTrades():
                    ref = getattr(t.order, "orderRef", "") or ""
                    if ref:
                        live[ref] = t
            except Exception as ex:
                log.warning("[IBKR RESUME] openTrades() failed: %s", ex)

            fills_by_ref: dict[str, list] = {}
            try:
                for f in ib.fills():
                    ref = getattr(getattr(f, "execution", None), "orderRef", "") or ""
                    if ref:
                        fills_by_ref.setdefault(ref, []).append(f)
            except Exception:
                pass

            # Re-issue any in-flight cancels that never confirmed terminal (the
            # cancel request was dropped/deferred across this reconnect). Runs
            # BEFORE the reattach loop, which skips oids still in self._trades —
            # and an unconfirmed cancel intentionally keeps its Trade handle.
            # Without this, a dropped cancel leaves the order live to fill after
            # the re-peg moved on (SPXW 7550C 2026-07-09 double-sell).
            reissued = 0
            for c_oid in list(self._pending_cancels.keys()):
                t = live.get(c_oid)
                if t is not None:
                    try:
                        ib.cancelOrder(t.order)
                        reissued += 1
                        log.warning("[IBKR RESUME] re-issuing dropped cancel for oid=%s", c_oid)
                    except Exception as ex:
                        log.warning("[IBKR RESUME] re-issue cancel oid=%s failed: %s", c_oid, ex)
                else:
                    # No longer live at the broker → it filled (its watcher books
                    # it) or is already gone. Clear the journal.
                    self._pending_cancels.pop(c_oid, None)
            if reissued:
                order_logger.info("IBKR_RESUME | re-issued %d dropped cancel(s)", reissued)

            reattached = booked = cancelled = 0
            for oid_db, oid, side, qty, osi, trigger in rows:
                if oid in self._trades:
                    continue  # a live watcher is already running (poll self-heals)
                try:
                    t = live.get(oid)
                    if t is not None:
                        # Still working at the broker → re-attach a watcher.
                        self._trades[oid] = t
                        self._attach_wake(oid, t)
                        pos_id = self._pos_id_for_osi(osi) if side == "SELL" else None
                        self._track_task(self._poll_order_fill(
                            order_db_id=oid_db, public_oid=oid, alert=None,
                            alert_db_id=None, side=side, qty=qty,
                            position_id=pos_id, max_pending_minutes_override=0,
                        ))
                        reattached += 1
                        continue
                    # Gone from the broker → completed while we were down.
                    ff = fills_by_ref.get(oid)
                    if ff:
                        ex_last = getattr(ff[-1], "execution", None)
                        fill_px = float(getattr(ex_last, "avgPrice", 0)
                                        or getattr(ex_last, "price", 0) or 0)
                        fqty = int(sum(float(getattr(getattr(f, "execution", None), "shares", 0) or 0)
                                       for f in ff)) or qty
                        await self._book_resumed_fill(oid_db, side, osi, fill_px, fqty, trigger)
                        booked += 1
                    else:
                        self._mark_order_cancelled(
                            oid_db, "Resumed: order absent from IBKR (cancelled while disconnected)")
                        cancelled += 1
                except Exception as ex:
                    log.error("[IBKR RESUME] order oid=%s resume failed: %s", oid, ex)

            log.info("[IBKR RESUME] pending=%d reattached=%d booked=%d cancelled=%d",
                     len(rows), reattached, booked, cancelled)
            order_logger.info("IBKR_RESUME | pending=%d reattached=%d booked=%d cancelled=%d",
                              len(rows), reattached, booked, cancelled)

    def _pos_id_for_osi(self, osi: str) -> int | None:
        db = SessionLocal()
        try:
            pos = db.query(Position).filter(
                Position.osi_symbol == osi,
                Position.status.in_(["OPEN", "PARTIAL"]),
            ).first()
            return pos.id if pos else None
        finally:
            db.close()

    async def _book_resumed_fill(self, order_db_id: int, side: str, osi: str,
                                 fill_px: float, fqty: int, trigger: str | None):
        """Book a fill that completed while disconnected. SELL → finalize against
        the matching position (closes/reduces it). BUY → mark the order FILLED
        and warn (can't rebuild the Position without the original alert)."""
        db = SessionLocal()
        try:
            db_order = db.query(Order).filter(Order.id == order_db_id).first()
            if not db_order or db_order.status not in ("PENDING", "PARTIALLY_FILLED"):
                return
            if side == "SELL":
                pos = db.query(Position).filter(
                    Position.osi_symbol == osi,
                    Position.status.in_(["OPEN", "PARTIAL"]),
                ).first()
                if pos:
                    await self._finalize_sell_fill(
                        db, db_order, pos, fill_px, fqty, trigger or "DISCORD")
                    log.info("[IBKR RESUME] booked SELL fill oid_db=%s %s @ %s x%d (%s)",
                             order_db_id, osi, fill_px, fqty, trigger)
                    return
                # No open position to reduce — just close the order out.
                db_order.status = "FILLED"
                db_order.fill_price = fill_px
                db_order.filled_qty = fqty
                db_order.filled_at = datetime.now(timezone.utc)
                db.commit()
                await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
            else:
                db_order.status = "FILLED"
                db_order.fill_price = fill_px
                db_order.filled_qty = fqty
                db_order.filled_at = datetime.now(timezone.utc)
                db.commit()
                await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
                log.warning("[IBKR RESUME] BUY oid_db=%s %s filled @ %s while down — order booked "
                            "but Position not rebuilt (no alert); verify manually",
                            order_db_id, osi, fill_px)
        finally:
            db.close()

    def _mark_order_cancelled(self, order_db_id: int, reason: str):
        db = SessionLocal()
        try:
            db_order = db.query(Order).filter(Order.id == order_db_id).first()
            if db_order and db_order.status in ("PENDING", "PARTIALLY_FILLED"):
                db_order.status = "CANCELLED"
                db_order.error_text = reason
                db.commit()
                asyncio.create_task(self.ws_manager.broadcast(
                    {"type": "order_update", "data": db_order.to_dict()}))
        finally:
            db.close()


# ─────────────────────────────────────────────────────────────────────────
# Dry-run Trade stub — exposes the minimal attributes _poll_order_fill reads.
# ─────────────────────────────────────────────────────────────────────────

class _DryOrder:
    __slots__ = ("orderId", "permId", "orderRef")
    def __init__(self, ref: str):
        self.orderId = 0
        self.permId = 0
        self.orderRef = ref


class _DryStatus:
    __slots__ = ("status", "filled", "remaining", "avgFillPrice")
    def __init__(self, qty: int, px: float):
        self.status = "Filled"
        self.filled = qty
        self.remaining = 0
        self.avgFillPrice = px


class _DryTrade:
    """Mimics an ib_async Trade in dry-run mode — instantly 'filled'."""
    def __init__(self, side: str, qty: int, px: float):
        self.order = _DryOrder(ref="dry")
        self.orderStatus = _DryStatus(qty, px)
        self.contract = None
