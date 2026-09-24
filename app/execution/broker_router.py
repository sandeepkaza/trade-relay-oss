"""
broker_router.py — Vendor-agnostic factory layer.

All call sites that touch a broker should import from HERE, not from
public_executor / public_sdk_bridge / ibkr_* directly. The active vendor
is selected at runtime by cfg[trading].broker (hot-reloadable):

    [trading]
    broker = public   ; default — Public.com via official SDK
    broker = ibkr     ; flip live to switch to IBKR (ib_async + IB Gateway)
    broker = tradier  ; Tradier REST (the only broker wired for spreads)
    broker = lime     ; Lime Trader REST — no broker-side stops, see lime_broker

ROLLBACK
────────
Edit one line in config.ini (broker = public), config_manager hot-reloads,
next call to get_executor() returns PublicExecutor again. No restart.

PHASE-1 STATUS
──────────────
Call sites have NOT been migrated yet — they still import PublicExecutor
directly. This module is the landing zone for the migration in Phase 2.
Until then, the bot runs unchanged on Public.com regardless of the
broker config setting.
"""

from __future__ import annotations

import logging
from typing import Any

from app.core.config_manager import cfg

log = logging.getLogger(__name__)


def _active_broker() -> str:
    name = (cfg.get("trading", "broker", fallback="public") or "public").strip().lower()
    if name not in ("public", "ibkr", "tradier", "lime"):
        log.warning("[broker_router] unknown broker=%r — falling back to public", name)
        return "public"
    return name


# ─────────────────────────────────────────────────────────────────────────
# Executor factory
# ─────────────────────────────────────────────────────────────────────────

def get_executor(ws_manager) -> Any:
    """Return a Broker-compatible executor for the currently active vendor.

    Cached per (vendor, ws_manager id) so callers can hold a reference
    safely. Flipping cfg[trading].broker invalidates the cache.
    """
    active = _active_broker()
    cache_key = (active, id(ws_manager))
    cached = _executor_cache.get(cache_key)
    if cached is not None:
        return cached

    # Invalidate any stale executors for a different broker on the same ws.
    for k in list(_executor_cache.keys()):
        if k[1] == id(ws_manager) and k[0] != active:
            log.info("[broker_router] broker swap %s → %s; dropping cached executor",
                     k[0], active)
            _executor_cache.pop(k, None)

    if active == "ibkr":
        from app.execution.ibkr_broker import IBKRExecutor
        ex = IBKRExecutor(ws_manager)
    elif active == "tradier":
        from app.execution.tradier_broker import TradierExecutor
        ex = TradierExecutor(ws_manager)
    elif active == "lime":
        from app.execution.lime_broker import LimeExecutor
        ex = LimeExecutor(ws_manager)
    else:
        from app.execution.public_executor import PublicExecutor
        ex = PublicExecutor(ws_manager)

    _executor_cache[cache_key] = ex
    log.info("[broker_router] created %s executor", active)
    return ex


_executor_cache: dict[tuple[str, int], Any] = {}


def get_spread_executor(ws_manager) -> Any:
    """Return the vertical-credit-spread executor.

    Not vendor-dispatched like get_executor: SpreadExecutor places through
    Tradier and refuses to run under any other broker rather than legging a
    spread in one order at a time.

    lime_sdk_bridge.place_multileg_order exists and takes ratio legs natively
    (a 1-2-1 fly is literally legs 1, 2, 1), so the transport for a Lime spread
    lane is already here — but SpreadExecutor's guardrails, sizing and close
    path are still written against Tradier's payload vocabulary, so pointing it
    at Lime is a deliberate follow-up rather than a config flip.
    """
    cached = _spread_executor_cache.get(id(ws_manager))
    if cached is None:
        from app.execution.spread_executor import SpreadExecutor
        cached = SpreadExecutor(ws_manager)
        _spread_executor_cache[id(ws_manager)] = cached
        log.info("[broker_router] created spread executor")
    return cached


_spread_executor_cache: dict[int, Any] = {}


# ─────────────────────────────────────────────────────────────────────────
# Quote / account dispatch — same shape as public_sdk_bridge functions.
# ─────────────────────────────────────────────────────────────────────────

def _bridge():
    """Return the SDK-bridge module for the active vendor (same fn surface)."""
    active = _active_broker()
    if active == "ibkr":
        from app.execution import ibkr_sdk_bridge as b
    elif active == "tradier":
        from app.execution import tradier_sdk_bridge as b
    elif active == "lime":
        from app.execution import lime_sdk_bridge as b
    else:
        from app.execution import public_sdk_bridge as b
    return b


async def fetch_prices(osi_symbols: list[str]) -> dict[str, float]:
    return await _bridge().fetch_prices(osi_symbols)


async def fetch_index_level(symbol: str = "VIX") -> float | None:
    """Index level (VIX) from the active vendor, or None if it has no source.

    Only public and ibkr implement this; tradier/lime bridges have no such
    function, and the sizer treats None as "unknown" rather than an error.
    """
    fn = getattr(_bridge(), "fetch_index_level", None)
    if fn is None:
        return None
    return await fn(symbol)


def prewarm_symbol(osi: str) -> None:
    """Fire-and-forget: establish the streaming quote subscription for a contract
    the instant it's seen in chat (on-mention warmer), so a later/repeated order
    on the SAME contract finds it warm instead of paying the cold first-tick wait
    (~IBKR_QUOTE_WAIT_MS + price-validation) inside the entry's hot path.

    The streaming ticker cache IS the watchlist (LRU-capped). No-op unless the
    active broker is IBKR and [ibkr] prewarm_quote_enabled is on. Best-effort:
    never raises (the caller is the ingest path)."""
    if not osi:
        return
    active = _active_broker()
    try:
        import asyncio
        if active == "ibkr":
            if not cfg.getboolean("ibkr", "prewarm_quote_enabled", fallback=False):
                return
            from app.execution import ibkr_sdk_bridge as b
            asyncio.create_task(b.prewarm_quote(osi))
        elif active == "tradier":
            if not cfg.getboolean("tradier", "stream_quotes_enabled", fallback=False):
                return  # streaming off → REST snapshot, nothing to warm
            from app.execution import tradier_sdk_bridge as b
            asyncio.create_task(b.prewarm_quote(osi))
        # public: REST snapshot — no streaming sub to warm
    except RuntimeError:
        pass  # no running loop (e.g. called outside the async app) — skip
    except Exception:
        log.debug("prewarm_symbol skipped for %s", osi, exc_info=True)


async def fetch_positions() -> list[dict]:
    """Normalized open positions from the active broker.

    Returns [] when the active bridge has not implemented the seam yet, so a
    caller can tell "no positions" from "not supported" via
    supports_positions().
    """
    fn = getattr(_bridge(), "fetch_positions", None)
    if fn is None:
        return []
    return await fn()


def supports_positions() -> bool:
    """True when the active broker can report positions for reconciliation."""
    return getattr(_bridge(), "fetch_positions", None) is not None


async def fetch_orders() -> list[dict]:
    return await _bridge().fetch_orders()


async def fetch_account_data() -> dict:
    return await _bridge().fetch_account_data()


async def fetch_account_balance() -> dict:
    return await _bridge().fetch_account_balance()


def get_last_spread_pct(osi_symbol: str) -> float:
    return _bridge().get_last_spread_pct(osi_symbol)


def active_broker_name() -> str:
    """Public accessor for the current broker (for dashboards / logs)."""
    return _active_broker()


def is_connected() -> bool:
    """Best-effort connectivity for the active broker (for the broker_connected
    metric). IBKR holds a persistent gateway session — report its real state.
    REST vendors (public/tradier) hold no session; treat as connected (their
    degradation shows up in broker_api_errors_total instead). Never raises."""
    try:
        if _active_broker() == "ibkr":
            from app.execution import ibkr_sdk_bridge as b
            return bool(b.is_connected())
    except Exception:
        return False
    return True
