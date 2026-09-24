"""
tradier_stream.py — Tradier WebSocket market-data streaming.

Maintains a live quote cache so fetch_prices can serve warm streaming quotes
(killing the REST-snapshot round-trip on the hot order path) and so
broker_router.prewarm_symbol can warm a contract the instant it's seen in chat.

Mirrors the role of ibkr_sdk_bridge's streaming ticker cache:
    _STREAM_MID[osi]        -> last mid price (float)
    _LAST_SPREAD_PCT[osi]   -> last bid/ask spread % (shared with the REST bridge
                               via tradier_sdk_bridge so the SL gate is unchanged)

PROTOCOL (https://docs.tradier.com/docs/streaming-data)
    1. POST {prod_base}/markets/events/session  → {"stream":{"sessionid": "..."}}
       (session is short-lived: ~5 min to connect; PRODUCTION token only —
        sandbox does NOT support streaming)
    2. wss://ws.tradier.com/v1/markets/events
    3. send {"symbols":[occ...], "filter":["quote"], "sessionid":"...", "linebreak":true}
    4. receive {"type":"quote","symbol":occ,"bid":..,"ask":..,...}

SAFETY: gated by [tradier] stream_quotes_enabled (default off). Best-effort —
never raises into a caller. When the stream is down/disabled, fetch_prices
transparently falls back to REST snapshots.

UNTESTED against the live Tradier stream (no prod creds in this env). Flag-off
by default; verify in a paper/prod session before relying on warm quotes.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

import aiohttp

from app.core.config_manager import cfg

log = logging.getLogger(__name__)

_WS_URL = "wss://ws.tradier.com/v1/markets/events"
# Streaming is production-only; the session POST always hits the prod host
# regardless of the [tradier] sandbox flag (sandbox returns no stream session).
_SESSION_URL = "https://api.tradier.com/v1/markets/events/session"

# Shared quote state (keyed by OSI, same convention as the REST bridge).
_STREAM_MID: dict[str, float] = {}
_STREAM_TS: dict[str, float] = {}        # osi -> monotonic time of last update
# OCC symbol currently subscribed -> originating OSI (events echo the OCC).
_SUBS: dict[str, str] = {}

_STREAM_TASK: asyncio.Task | None = None
_WANT_RESUBSCRIBE = asyncio.Event()
_LOCK = asyncio.Lock()

# Consider a streamed quote stale after this many seconds (fall back to REST).
_FRESH_SECS = 5.0


def _enabled() -> bool:
    return cfg.getboolean("tradier", "stream_quotes_enabled", fallback=False)


def stream_mid(osi: str) -> float | None:
    """Fresh streamed mid for `osi`, or None if absent/stale/disabled."""
    if not _enabled():
        return None
    ts = _STREAM_TS.get(osi)
    if ts is None or (time.monotonic() - ts) > _FRESH_SECS:
        return None
    return _STREAM_MID.get(osi)


async def ensure_subscribed(osi: str, occ: str) -> None:
    """Add `occ` to the live subscription and make sure the stream is running.
    Best-effort; no-op when streaming is disabled."""
    if not _enabled() or not occ:
        return
    if _SUBS.get(occ) != osi:
        _SUBS[occ] = osi
        _WANT_RESUBSCRIBE.set()
    await _ensure_task()


async def _ensure_task() -> None:
    global _STREAM_TASK
    async with _LOCK:
        if _STREAM_TASK is None or _STREAM_TASK.done():
            _STREAM_TASK = asyncio.create_task(_run())


async def _create_session() -> str | None:
    """POST for a streaming sessionid (prod token). Returns None on failure."""
    from app.execution.tradier_sdk_bridge import TRADIER_TOKEN
    if not TRADIER_TOKEN:
        log.warning("[tradier stream] no token — cannot create session")
        return None
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                _SESSION_URL,
                headers={"Authorization": f"Bearer {TRADIER_TOKEN}", "Accept": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                resp.raise_for_status()
                body = await resp.json()
                return ((body or {}).get("stream") or {}).get("sessionid")
    except Exception as e:
        log.warning("[tradier stream] session create failed: %s", e)
        return None


async def _run() -> None:
    """Connect, subscribe, and pump quote events into the cache. Reconnects
    with backoff on drop. Exits when streaming is disabled."""
    backoff = 1.0
    while _enabled():
        sessionid = await _create_session()
        if not sessionid:
            await asyncio.sleep(min(backoff, 30))
            backoff = min(backoff * 2, 30)
            continue
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(_WS_URL, heartbeat=20) as ws:
                    backoff = 1.0
                    await _send_sub(ws, sessionid)
                    while _enabled():
                        # Re-subscribe promptly when a new symbol is added.
                        if _WANT_RESUBSCRIBE.is_set():
                            _WANT_RESUBSCRIBE.clear()
                            await _send_sub(ws, sessionid)
                        try:
                            msg = await asyncio.wait_for(ws.receive(), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            _on_event(msg.data)
        except Exception as e:
            log.warning("[tradier stream] ws error: %s — reconnecting", e)
        await asyncio.sleep(min(backoff, 30))
        backoff = min(backoff * 2, 30)
    log.info("[tradier stream] disabled — stream loop exiting")


async def _send_sub(ws, sessionid: str) -> None:
    if not _SUBS:
        return
    payload = {
        "symbols": list(_SUBS.keys()),
        "filter": ["quote"],
        "sessionid": sessionid,
        "linebreak": True,
    }
    await ws.send_str(json.dumps(payload))


def _on_event(raw: str) -> None:
    """Parse one quote event and update the cache. linebreak=true can deliver
    multiple newline-separated JSON objects per frame."""
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except Exception:
            continue
        if ev.get("type") != "quote":
            continue
        occ = ev.get("symbol") or ""
        osi = _SUBS.get(occ)
        if not osi:
            continue
        try:
            bid = float(ev.get("bid") or 0)
            ask = float(ev.get("ask") or 0)
        except (TypeError, ValueError):
            continue
        if bid > 0 and ask > 0:
            mid = (bid + ask) / 2
            _STREAM_MID[osi] = round(mid, 2)
            _STREAM_TS[osi] = time.monotonic()
            # Share spread % with the REST bridge's dict (SL gate reads it).
            from app.execution.tradier_sdk_bridge import _LAST_SPREAD_PCT
            _LAST_SPREAD_PCT[osi] = ((ask - bid) / mid) * 100.0 if mid > 0 else 0.0
