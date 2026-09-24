"""
ibkr_sdk_bridge.py — Thin async wrapper around ib_async (IBKR TWS API).

Mirrors the surface of public_sdk_bridge.py so broker_router.py can
dispatch transparently:
    fetch_prices(osi_symbols)        -> dict[osi, mid_price]
    fetch_orders()                   -> list[dict]
    fetch_account_data()             -> dict
    fetch_account_balance()          -> dict
    get_last_spread_pct(osi_symbol)  -> float

Connection: IB Gateway running on host:port (config.ini [ibkr]).
Library: `ib_async` (pip install ib_async). Drop-in replacement for the
abandoned ib_insync — same API surface, active maintenance.

Symbol translation:
    Public.com uses OSI strings ("SPXW260116C07380000"). IBKR uses
    Option(symbol, lastTradeDateOrContractMonth, strike, right, exchange).
    _osi_to_ib_option() handles the conversion.

NOTE: This module deliberately re-uses _LAST_SPREAD_PCT semantics from
public_sdk_bridge — same dict shape so position_monitor SL gate works
unchanged when broker is flipped to ibkr.
"""

from __future__ import annotations

import asyncio
import configparser
import logging
import os
import time
from typing import Any

from app.core.config_manager import cfg

log = logging.getLogger(__name__)

# ── Startup-only IBKR credentials ─────────────────────────────────────────
_boot = configparser.ConfigParser(inline_comment_prefixes=(";", "#"))
_boot.read("config.ini", encoding="utf-8-sig")

IBKR_HOST       = os.getenv("IBKR_HOST",       _boot.get("ibkr", "host",       fallback="127.0.0.1"))
IBKR_PORT       = int(os.getenv("IBKR_PORT",   _boot.get("ibkr", "port",       fallback="4002")))
IBKR_CLIENT_ID  = int(os.getenv("IBKR_CLIENT_ID", _boot.get("ibkr", "client_id", fallback="7")))
IBKR_ACCOUNT    = os.getenv("IBKR_ACCOUNT",    _boot.get("ibkr", "account",    fallback=""))
# Market-data type: 1=Live (needs OPRA/CBOE subs), 2=Frozen, 3=Delayed (free,
# 15-min lag), 4=Delayed-Frozen. We set on every connect so an account without
# live subs still gets delayed quotes instead of empty tickers + Error 354.
IBKR_MARKET_DATA_TYPE = int(os.getenv(
    "IBKR_MARKET_DATA_TYPE",
    _boot.get("ibkr", "market_data_type", fallback="3"),
))
# Max ms to wait for a ticker's bid/ask/last to populate after first subscribe.
# After a tick has arrived once, cached streaming ticker stays fresh — no wait.
IBKR_QUOTE_WAIT_MS = int(os.getenv(
    "IBKR_QUOTE_WAIT_MS",
    _boot.get("ibkr", "quote_wait_ms", fallback="500"),
))
# LRU cap for the streaming ticker cache. Each subscription = 1 market-data
# line on the IBKR side; default acct cap is ~100 lines. Stay well under.
IBKR_TICKER_CACHE_MAX = int(os.getenv(
    "IBKR_TICKER_CACHE_MAX",
    _boot.get("ibkr", "ticker_cache_max", fallback="80"),
))
del _boot


def _DRY_RUN() -> bool:
    return cfg.getboolean("trading", "dry_run", fallback=True)


# Last seen bid-ask spread % per OSI symbol. Same shape as public_sdk_bridge
# so position_monitor's wide-spread SL gate works identically.
_LAST_SPREAD_PCT: dict[str, float] = {}


def get_last_spread_pct(osi_symbol: str) -> float:
    """Return last observed bid-ask spread % for symbol, or 0 if unseen."""
    return _LAST_SPREAD_PCT.get(osi_symbol, 0.0)


# ── IB client singleton ───────────────────────────────────────────────────
_ib: Any | None = None
_ib_lock = asyncio.Lock()
# True once the very first connect of this process succeeds. Used to suppress
# a spurious "RECOVERED" Discord message on cold start when `connectedEvent`
# fires for the initial handshake.
_first_connect_done: bool = False

# Streaming ticker cache. Key = OSI symbol (input domain), value = (qualified
# contract, live Ticker). The Ticker stays subscribed; subsequent fetch_prices
# calls just read its current bid/ask/last. OrderedDict preserves LRU order.
from collections import OrderedDict
_TICKER_CACHE: "OrderedDict[str, tuple[Any, Any]]" = OrderedDict()

# Coroutine callbacks fired after every successful IBKR (re)connect. IBKRExecutor
# registers resume_pending_orders() here so resting/pending orders re-attach
# their fill-watchers after a process restart or reconnect (IBKR has no
# reconciler). This module stays ignorant of the executor — no import — to
# avoid a cycle. IBKR-only; never invoked on the Public lane.
_on_connect_callbacks: list = []


async def _run_on_connect_cb(cb) -> None:
    try:
        await cb()
    except Exception as ex:
        log.error("on-connect callback %s failed: %s", getattr(cb, "__qualname__", cb), ex)


def _schedule_on_connect_cb(cb) -> None:
    try:
        asyncio.get_event_loop().create_task(_run_on_connect_cb(cb))
    except Exception as ex:
        log.debug("on-connect schedule failed: %s", ex)


def register_on_connect(cb) -> None:
    """Register a coroutine fn to run after each IBKR (re)connect. If already
    connected when registered (executor created after the first connect), fire
    once immediately so the current session isn't missed."""
    if cb not in _on_connect_callbacks:
        _on_connect_callbacks.append(cb)
    if _ib is not None and _ib.isConnected():
        _schedule_on_connect_cb(cb)


def _fire_on_connect() -> None:
    for cb in list(_on_connect_callbacks):
        _schedule_on_connect_cb(cb)


def is_connected() -> bool:
    """Read-only view of the gateway session for the broker_connected metric.
    Does NOT trigger a (re)connect — just inspects the existing singleton."""
    try:
        return _ib is not None and _ib.isConnected()
    except Exception:
        return False


async def _get_ib():
    """Return a connected ib_async.IB() singleton. Auto-reconnects if dropped."""
    global _ib
    async with _ib_lock:
        if _ib is not None and _ib.isConnected():
            return _ib
        try:
            from ib_async import IB  # type: ignore
        except ImportError as e:
            raise RuntimeError(
                "ib_async not installed. Run: pip install ib_async"
            ) from e

        # Drop stale ticker cache from any prior (now-dead) connection — those
        # Ticker objects reference a defunct wrapper and will never tick.
        _TICKER_CACHE.clear()

        ib = IB()
        # Bind the error handler BEFORE connecting. IBKR pushes market-data farm
        # status (2104/2106/2103/…) during the connection handshake, so a handler
        # bound after connectAsync returns never sees them and farm state sits at
        # "unknown" for the whole session. Handshake-time connectivity errors
        # were being dropped the same way.
        try:
            ib.errorEvent += _on_ib_error                  # type: ignore[operator]
        except Exception as ex:
            log.warning("Could not bind IB error handler: %s", ex)
        await ib.connectAsync(
            host=IBKR_HOST,
            port=IBKR_PORT,
            clientId=IBKR_CLIENT_ID,
            timeout=10,
        )
        try:
            ib.reqMarketDataType(IBKR_MARKET_DATA_TYPE)
            log.info("IBKR market data type set to %d (1=live,2=frozen,3=delayed,4=delayed-frozen)",
                     IBKR_MARKET_DATA_TYPE)
        except Exception as ex:
            log.warning("reqMarketDataType(%d) failed: %s", IBKR_MARKET_DATA_TYPE, ex)
        _wire_disconnect_alarms(ib)
        _ib = ib
        global _first_connect_done
        if not _first_connect_done:
            _first_connect_done = True
        else:
            # We got here only because a prior session dropped → we just reconnected.
            asyncio.create_task(_post_ibkr_alarm(
                "recovered",
                f"IBKR reconnected to {IBKR_HOST}:{IBKR_PORT} clientId={IBKR_CLIENT_ID}",
            ))
        log.info("IBKR connected: %s:%s clientId=%s", IBKR_HOST, IBKR_PORT, IBKR_CLIENT_ID)
        # Fire post-connect hooks (e.g. IBKRExecutor.resume_pending_orders) so
        # resting orders re-attach their watchers after a (re)connect.
        _fire_on_connect()
        return _ib


# ── Sub-second disconnect alarm wiring ────────────────────────────────────
# ib_async fires `disconnectedEvent` the moment the TCP socket to IB Gateway
# closes — typically within a few ms of the gateway dying, daily IBC restart,
# socat relay crash, or the gateway kicking us off for a colliding clientId.
# We hook that event and push directly to the existing Discord health-alarm
# channel (same one the cron-based alarms use). The cron `ibgw.sh` probe is
# still useful as a coarser-grained safety net (every 2 min) that also fires
# when the trader process itself is down and these in-process handlers can't.

def _wire_disconnect_alarms(ib: Any) -> None:
    """Bind disconnectedEvent on a freshly-connected IB().

    errorEvent is bound earlier, before connectAsync (see _get_ib) — the farm
    status arrives mid-handshake. A disconnect can only matter after a
    connection succeeded, so that one stays here.
    """
    try:
        ib.disconnectedEvent += _on_ib_disconnect          # type: ignore[operator]
    except Exception as ex:
        log.warning("Could not bind IB disconnect handler: %s", ex)


# ── Reconnect watchdog ─────────────────────────────────────────────────────
# Without this, a dropped socket is only healed lazily: _get_ib() reconnects on
# the NEXT caller, so the first alert after a gateway restart pays the whole
# connect cost (connectAsync + IBKR streaming positions/orders/executions, 20s+)
# on the money path, and until that alert arrives the bot sits disconnected with
# orders blocked. The watchdog pulls that cost off the critical path: it retries
# in the background so the session is already live when the next alert lands.
#
# Revert knob: [ibkr] reconnect_watchdog_enabled = false restores the old
# lazy-only behavior exactly.
_reconnect_task: Any | None = None


def _reconnect_enabled() -> bool:
    return cfg.getboolean("ibkr", "reconnect_watchdog_enabled", fallback=True)


def _reconnect_max_delay() -> float:
    return cfg.getfloat("ibkr", "reconnect_max_delay_seconds", fallback=30.0)


def _reconnect_initial_delay() -> float:
    """First retry waits this long. Not zero: the socket usually drops because
    the gateway is going down, and an instant retry just burns an attempt."""
    return cfg.getfloat("ibkr", "reconnect_initial_delay_seconds", fallback=2.0)


async def _reconnect_watchdog() -> None:
    """Retry the gateway session in the background until it comes back.

    Deliberately unbounded in attempts: an IB Gateway restart needs a human 2FA
    tap and can take minutes, and giving up would leave the bot dead exactly
    when the analyst's next alert arrives. Bounded in RATE instead — backoff to
    reconnect_max_delay_seconds — so a gateway that is down for an hour costs
    ~120 connect attempts, not 3600.
    """
    global _reconnect_task
    delay = _reconnect_initial_delay()
    try:
        while True:
            await asyncio.sleep(delay)
            if not _reconnect_enabled():
                log.info("[IBKR] reconnect watchdog disabled via config — stopping")
                return
            if _ib is not None and _ib.isConnected():
                return                      # something else already reconnected
            try:
                await _get_ib()             # posts the "recovered" alarm itself
                log.info("[IBKR] reconnect watchdog restored the session")
                return
            except Exception as ex:
                log.warning("[IBKR] reconnect attempt failed (retry in %.0fs): %s", delay, ex)
            delay = min(delay * 2, _reconnect_max_delay())
    finally:
        _reconnect_task = None


def _start_reconnect_watchdog() -> None:
    global _reconnect_task
    if not _reconnect_enabled():
        return
    if _reconnect_task is not None and not _reconnect_task.done():
        return                              # one watchdog is enough
    try:
        _reconnect_task = asyncio.get_event_loop().create_task(_reconnect_watchdog())
    except RuntimeError:
        log.warning("[IBKR] reconnect watchdog not started: no event loop")


def _on_ib_disconnect() -> None:
    """ib_async fires this when the socket to IB Gateway closes."""
    log.error("IBKR socket disconnected from %s:%s", IBKR_HOST, IBKR_PORT)
    _start_reconnect_watchdog()
    # Farm state is per-session. Keeping the old "ok" across a dropped socket
    # would leave the panel green off a reading from a session that no longer
    # exists; the next connect re-reports it within seconds.
    _set_md_state("unknown", "socket disconnected")
    try:
        asyncio.create_task(_post_ibkr_alarm(
            "disconnected",
            f"IBKR socket dropped — host={IBKR_HOST}:{IBKR_PORT} clientId={IBKR_CLIENT_ID}. "
            + ("Reconnect watchdog is retrying in the background; orders blocked until it succeeds."
               if _reconnect_enabled() else
               "Bot will auto-reconnect on next request; orders blocked until then."),
        ))
    except RuntimeError:
        # No running loop (shutdown). Best-effort log only — Discord post lost.
        log.warning("IBKR disconnect alarm not posted: no event loop")


# IB API error codes that mean "the session is going away" — fire alarm.
# 1100 = Connectivity between IB and TWS has been lost
# 1101 = Connectivity restored - data lost
# 1102 = Connectivity restored - data maintained
# 1300 = TWS socket port has been reset and this connection is being dropped
# 2110 = Connectivity between TWS and server is broken
_CONN_LOSS_CODES = {1100, 1300, 2110}
_CONN_RESTORE_CODES = {1101, 1102}

# ── Market-data farm status ─────────────────────────────────────────────────
# "Socket open" and "quotes arriving" are different failures. The API port can
# accept a connection while the market-data farm behind it is down, and the
# 2026-08-06 outage showed the unit can be active with nothing flowing.
#
# IBKR pushes these codes unprompted on connect and on every farm state change,
# so they report data health even while the book is flat. A tick-arrival
# heartbeat cannot: the position monitor only polls prices when something is
# open (position_monitor.py returns early on an empty book), so it goes quiet
# overnight and would read as "data dead" when nothing is wrong.
#
# 2103 market data farm connection is broken
# 2104 market data farm connection is OK
# 2105 HMDS (historical) data farm connection is broken
# 2106 HMDS data farm connection is OK
# 2108 market data farm connection is inactive but should be available on demand
# 2119 market data farm is connecting
_MD_OK_CODES      = {2104, 2106, 2108}
_MD_BROKEN_CODES  = {2103, 2105}
_MD_PENDING_CODES = {2119}

# Per-session state. "unknown" until IBKR says something — never assume OK and
# never assume broken, because a red light nobody can explain is as useless as
# a green one that lies.
_md_state: str = "unknown"          # unknown | ok | broken | connecting
_md_ts: float | None = None         # when the state last changed
_md_detail: str = ""                # IBKR's own wording for the last change


def market_data_status() -> dict:
    """Last known market-data farm state, as reported by IBKR itself.

    state is one of unknown/ok/broken/connecting; age_seconds is how long ago
    that state was reported (None when nothing has been reported this session).
    """
    return {
        "state": _md_state,
        "detail": _md_detail,
        "age_seconds": (time.time() - _md_ts) if _md_ts else None,
        "market_data_type": IBKR_MARKET_DATA_TYPE,
    }


def _set_md_state(state: str, detail: str) -> None:
    global _md_state, _md_ts, _md_detail
    if state != _md_state:
        log.info("IBKR market-data farm: %s -> %s (%s)", _md_state, state, detail)
    _md_state, _md_ts, _md_detail = state, time.time(), detail


def _on_ib_error(reqId: int, errorCode: int, errorString: str, contract: Any = None) -> None:
    """ib_async errorEvent — fires for both API errors and connectivity events."""
    if errorCode in _CONN_LOSS_CODES:
        log.error("IBKR connectivity error %d: %s", errorCode, errorString)
        try:
            asyncio.create_task(_post_ibkr_alarm(
                "connectivity-loss",
                f"IBKR error {errorCode}: {errorString} (host={IBKR_HOST}:{IBKR_PORT})",
            ))
        except RuntimeError:
            pass
    elif errorCode in _CONN_RESTORE_CODES:
        try:
            asyncio.create_task(_post_ibkr_alarm(
                "recovered",
                f"IBKR error {errorCode} (recovery): {errorString}",
            ))
        except RuntimeError:
            pass
    elif errorCode in _MD_OK_CODES:
        _set_md_state("ok", errorString)
    elif errorCode in _MD_BROKEN_CODES:
        _set_md_state("broken", errorString)
    elif errorCode in _MD_PENDING_CODES:
        _set_md_state("connecting", errorString)


async def _post_ibkr_alarm(kind: str, message: str) -> None:
    """Forward an IBKR connectivity event to the Discord alarm channel.

    Uses trade_logger.log_health_alarm which routes to the daily-summary /
    alerts channel — same place the heartbeat-stale alarms land.
    """
    try:
        import app.analytics.trade_logger as _tl
        await _tl.log_health_alarm(
            component=f"ibkr-{kind}",
            message=message,
        )
    except Exception as ex:
        log.warning("Could not post IBKR %s alarm to Discord: %s", kind, ex)


# ── OSI → IBKR contract translator ────────────────────────────────────────

def _osi_to_ib_option(osi: str):
    """
    Convert OSI symbol to ib_async Option contract.

    OSI layout (21 chars): ROOT(6, padded) + YY(2) + MM(2) + DD(2) + C/P(1) + STRIKE(8, *1000)
    Example: 'SPXW  260116C07380000' → root=SPXW expiry=2026-01-16 strike=7380.0 right=C

    Exchange selection:
        SPX / SPXW → 'CBOE'
        Everything else → 'SMART' (IBKR smart routing)
    """
    try:
        from ib_async import Option  # type: ignore
    except ImportError as e:
        raise RuntimeError(
            "ib_async not installed. Run: pip install ib_async"
        ) from e

    # OSI may be padded or unpadded; normalize.
    s = osi.strip()
    if len(s) < 15:
        raise ValueError(f"OSI too short: {osi!r}")

    # Root is left-justified, padded to 6 chars in canonical OSI. Real-world
    # alerts often arrive unpadded — find the digit boundary.
    i = 0
    while i < len(s) and not s[i].isdigit():
        i += 1
    root = s[:i].rstrip()
    if not root:
        raise ValueError(f"OSI missing root: {osi!r}")

    rest = s[i:]
    if len(rest) < 15:
        raise ValueError(f"OSI suffix wrong length: {osi!r}")

    yy = int(rest[0:2])
    mm = int(rest[2:4])
    dd = int(rest[4:6])
    right = rest[6]
    strike_int = int(rest[7:15])
    strike = strike_int / 1000.0
    expiry = f"20{yy:02d}{mm:02d}{dd:02d}"

    if right not in ("C", "P"):
        raise ValueError(f"OSI bad right: {osi!r}")

    exchange = "CBOE" if root in ("SPX", "SPXW") else "SMART"
    # SPX index options trade as 'SPX' on IBKR even if alerts use 'SPXW' root
    # (SPXW is just OCC's weekly-suffix convention).
    ib_symbol = "SPX" if root in ("SPX", "SPXW") else root
    # tradingClass: SPX index options route to tradingClass=SPXW for every
    # expiry except the legacy AM-settled 3rd-Friday monthly. Alerts arrive
    # with either 'SPX' or 'SPXW' as root regardless of which contract was
    # actually meant; analysts treat the names interchangeably. Modern volume
    # is virtually all SPXW (PM-settled weeklies & EOM). Forcing SPXW avoids
    # the failure where root='SPX' + Friday-weekly expiry yields zero matches
    # at qualifyContractsAsync time (today's SPX260515C06450000 bug).
    if root in ("SPX", "SPXW"):
        trading_class = "SPXW"
    else:
        trading_class = ""

    opt = Option(
        symbol=ib_symbol,
        lastTradeDateOrContractMonth=expiry,
        strike=strike,
        right=right,
        exchange=exchange,
        currency="USD",
    )
    if trading_class:
        opt.tradingClass = trading_class
    return opt


# ── Quotes ────────────────────────────────────────────────────────────────

def _is_valid(x) -> bool:
    """ib_async marks missing fields as NaN (not None). Treat both as missing."""
    if x is None:
        return False
    try:
        xf = float(x)
    except (TypeError, ValueError):
        return False
    if xf <= 0:
        return False
    # NaN != NaN — cheapest valid-number check.
    return xf == xf


def _ticker_has_quote(t) -> bool:
    """True once at least one of bid/ask/last (or delayed equivalents) populated."""
    if _is_valid(getattr(t, "bid", None)) and _is_valid(getattr(t, "ask", None)):
        return True
    if _is_valid(getattr(t, "last", None)):
        return True
    if _is_valid(getattr(t, "delayedBid", None)) and _is_valid(getattr(t, "delayedAsk", None)):
        return True
    if _is_valid(getattr(t, "delayedLast", None)):
        return True
    return False


def _read_quote(t) -> tuple[float, float]:
    """Return (mid_price, spread_pct). Prefer live bid/ask, fall back to
    delayed bid/ask, then last, then delayedLast. Returns (0, 0) if no quote."""
    bid = float(t.bid) if _is_valid(getattr(t, "bid", None)) else 0.0
    ask = float(t.ask) if _is_valid(getattr(t, "ask", None)) else 0.0
    if not (bid > 0 and ask > 0):
        dbid = getattr(t, "delayedBid", None)
        dask = getattr(t, "delayedAsk", None)
        if _is_valid(dbid) and _is_valid(dask):
            bid, ask = float(dbid), float(dask)
    if bid > 0 and ask > 0:
        mid = (bid + ask) / 2.0
        spread = ((ask - bid) / mid) * 100.0 if mid > 0 else 0.0
        return round(mid, 2), spread
    for fld in ("last", "delayedLast"):
        v = getattr(t, fld, None)
        if _is_valid(v):
            return round(float(v), 2), 100.0
    return 0.0, 0.0


async def _wait_for_quotes(tickers, timeout_ms: int) -> None:
    """Poll-await until every ticker has a usable quote, or timeout.
    Uses tight asyncio.sleep so ib_async can dispatch incoming tick events."""
    if not tickers:
        return
    deadline = time.monotonic() + (timeout_ms / 1000.0)
    while time.monotonic() < deadline:
        if all(_ticker_has_quote(t) for t in tickers):
            return
        await asyncio.sleep(0.02)


def _evict_lru(ib, max_size: int) -> None:
    """Trim cache to max_size by cancelling oldest streaming subscriptions."""
    while len(_TICKER_CACHE) > max_size:
        _osi, (contract, _ticker) = _TICKER_CACHE.popitem(last=False)
        try:
            ib.cancelMktData(contract)
        except Exception:
            pass


def get_cached_contract(osi: str):
    """Return the already-qualified contract for `osi` if it's in the live
    ticker cache, else None. Order placement fetches a quote first (which
    qualifies + caches the contract), so this lets _place_on_public skip a
    redundant qualifyContractsAsync round-trip (~50-200ms) on the hot path.
    Same conId IBKR would return — safe to reuse."""
    entry = _TICKER_CACHE.get(osi)
    if entry is not None and getattr(entry[0], "conId", 0):
        return entry[0]
    return None


# ── Index-quote session (delayed) ──────────────────────────────────────────
# Separate from the trading singleton on purpose — see fetch_index_level. Its
# own clientId, its own market-data type, and deliberately NO disconnect alarm:
# losing the VIX feed is a sizing degradation, not a trading outage, and paging
# on it would train the on-call to ignore the alarm that does mean an outage.
_vix_ib: Any | None = None
_vix_lock = asyncio.Lock()


def _VIX_CLIENT_ID() -> int:
    """Must differ from IBKR_CLIENT_ID and from every probe script's id — IBKR
    kicks the older session off when a clientId is reused."""
    return cfg.getint("ibkr", "vix_client_id", fallback=19)


def _VIX_MD_TYPE() -> int:
    """Market-data type for the index session: 1=live, 3=delayed.

    Defaults to live because the account now carries Cboe Streaming Market
    Indexes (subscribed 2026-08-20; before that VIX returned Error 354 and the
    session had to run delayed). Falls back to delayed automatically when live
    yields nothing — see fetch_index_level — so cancelling the subscription
    later degrades instead of silently reinstating the 0.50x haircut.
    """
    return cfg.getint("ibkr", "vix_market_data_type", fallback=1)


async def _get_vix_ib():
    """Connected IB() for index quotes, or None if the gateway refuses.

    Never raises: the caller is a background refresher whose failure mode must
    be "VIX unknown", not a crashed loop.
    """
    global _vix_ib
    async with _vix_lock:
        if _vix_ib is not None and _vix_ib.isConnected():
            return _vix_ib
        try:
            from ib_async import IB  # type: ignore
            ib = IB()
            await ib.connectAsync(
                host=IBKR_HOST, port=IBKR_PORT,
                clientId=_VIX_CLIENT_ID(), timeout=10,
            )
            ib.reqMarketDataType(_VIX_MD_TYPE())
            _vix_ib = ib
            log.info("IBKR index session connected (clientId=%s, market_data_type=%s)",
                     _VIX_CLIENT_ID(), _VIX_MD_TYPE())
            return _vix_ib
        except Exception as ex:
            log.debug("IBKR index session connect failed: %s", ex)
            return None


def _index_level_from(ticker) -> float | None:
    """Level from an index ticker, or None if it carries no usable number.

    An index quotes as last/close with no bid/ask, and IBKR marks "nothing
    here" as bid/ask = -1 with last/close = nan, so a naive float() read
    produces -1.0 and a sizer that believes VIX is negative.
    """
    level, _spread = _read_quote(ticker)
    if level > 0:
        return level
    close = getattr(ticker, "close", None)
    if _is_valid(close) and float(close) > 0:
        return round(float(close), 2)
    return None


async def fetch_index_level(symbol: str = "VIX") -> float | None:
    """Fetch an index level (VIX) from IBKR. Returns None on any failure.

    Same contract as public_sdk_bridge.fetch_index_level so broker_router can
    dispatch either vendor. Exists because vix_sizing_enabled had no data source
    on IBKR: _vix_refresher only ever fed the cache from Public, so on the IBKR
    box the sizer fell through to vix_unavailable_scale and quietly cut EVERY
    entry to 0.50x — a permanent haircut dressed up as a volatility response.

    An index quotes differently from an option: there is usually no bid/ask, so
    the level arrives as `last` (or `close` out of hours, which is the right
    answer for a sizer — yesterday's VIX close beats no VIX at all). _read_quote
    already walks live → delayed → last in that order.

    snapshot=True on purpose: this runs once a minute and must not consume one
    of the account's ~100 streaming market-data lines, which the option ticker
    cache is already sized against (IBKR_TICKER_CACHE_MAX).

    Runs on its OWN IB session (see _get_vix_ib). Probed live on ibkr-oci
    2026-08-20: this account has no CBOE index subscription, so VIX under
    market-data type 1 returns Error 354 and nothing else —

        Error 354 ... Requested market data is not subscribed ...
        Delayed market data is available. VIX CBOE Volatility Index

    — while type 3 returns close=14.89. reqMarketDataType is per-CONNECTION,
    not per-contract, so serving VIX from the trading session would mean either
    no VIX or delayed option quotes on the order path. A second session isolates
    the setting. Delayed VIX is fine for a sizer: the thresholds are buckets
    (low/high/extreme), not ticks.
    """
    if _DRY_RUN():
        return None
    try:
        from ib_async import Index  # type: ignore
        ib = await _get_vix_ib()
        if ib is None:
            return None
        # VIX is a CBOE index. Qualify first: an unqualified Index() silently
        # returns an empty ticker rather than an error.
        qualified = await ib.qualifyContractsAsync(Index(symbol, "CBOE"))
        contract = qualified[0] if qualified else None
        if contract is None or not getattr(contract, "conId", 0):
            log.debug("fetch_index_level(%s): IBKR could not qualify the contract", symbol)
            return None
        ticker = ib.reqMktData(contract, "", True, False)
        # Indices tick slower than options and this is off the hot path, so give
        # it more room than IBKR_QUOTE_WAIT_MS (tuned for order-entry latency).
        await _wait_for_quotes([ticker], max(IBKR_QUOTE_WAIT_MS, 2000))
        level = _index_level_from(ticker)
        if level is not None:
            return level

        # Live returned nothing. That is either the wrong hour (indices are not
        # disseminated overnight — probed 03:22 ET: bid/ask -1, last/close nan)
        # or a lapsed subscription. Delayed can still carry the prior close,
        # which beats "unknown" because unknown costs every entry 0.50x.
        if _VIX_MD_TYPE() == 1:
            ib.reqMarketDataType(3)
            try:
                ticker = ib.reqMktData(contract, "", True, False)
                await _wait_for_quotes([ticker], max(IBKR_QUOTE_WAIT_MS, 2000))
                level = _index_level_from(ticker)
                if level is not None:
                    log.debug("fetch_index_level(%s): live empty, used delayed %.2f",
                              symbol, level)
                    return level
            finally:
                ib.reqMarketDataType(_VIX_MD_TYPE())   # leave the session as found
        return None
    except Exception as e:
        log.debug("fetch_index_level(%s) failed: %s", symbol, e)
        return None


async def fetch_prices(osi_symbols: list[str]) -> dict[str, float]:
    """Return dict osi → mid-price. Empty dict on failure.

    Uses a persistent streaming subscription per contract (cached in
    _TICKER_CACHE). First call for a symbol qualifies + reqMktData(snapshot=
    False) and waits up to IBKR_QUOTE_WAIT_MS for the first tick. Subsequent
    calls read the live ticker directly — no re-subscribe, no sleep.

    Mid = (bid+ask)/2 when both present. Falls back to delayedBid/Ask, then
    last/delayedLast. Delayed fields are populated when reqMarketDataType(3)
    is active or the account lacks live subs.

    In dry_run, simulates with random walk (same shape as public bridge).
    """
    from app.monitors.api_monitor import monitor

    if not osi_symbols:
        return {}

    if _DRY_RUN():
        return _simulate_prices(osi_symbols)

    start = time.perf_counter()
    status_code = 200
    error_msg = None

    try:
        ib = await _get_ib()
        prices: dict[str, float] = {}

        # Partition into already-cached (warm) vs needs-subscribe (cold).
        warm: list[str] = []
        cold_osi: list[str] = []
        cold_contracts: list[Any] = []
        for osi in osi_symbols:
            entry = _TICKER_CACHE.get(osi)
            if entry is not None:
                # Touch for LRU.
                _TICKER_CACHE.move_to_end(osi)
                warm.append(osi)
                continue
            try:
                cold_contracts.append(_osi_to_ib_option(osi))
                cold_osi.append(osi)
            except Exception as e:
                log.warning("ibkr osi parse failed %s: %s", osi, e)

        new_tickers = []
        if cold_contracts:
            # qualifyContractsAsync returns None for contracts IBKR couldn't
            # resolve (ambiguous, no security def, expired). Skip those — do
            # NOT abort the whole batch over one bad OSI.
            qualified = await ib.qualifyContractsAsync(*cold_contracts)
            for osi, contract in zip(cold_osi, qualified, strict=False):
                if contract is None or not getattr(contract, "conId", 0):
                    log.warning("ibkr qualify failed (skipped): %s", osi)
                    continue
                # snapshot=False → streaming subscription; persists until cancelled
                ticker = ib.reqMktData(contract, "", False, False)
                _TICKER_CACHE[osi] = (contract, ticker)
                new_tickers.append(ticker)
            _evict_lru(ib, IBKR_TICKER_CACHE_MAX)

        # Only wait for ticks on newly-subscribed contracts; warm cache is
        # already streaming and has whatever the most recent tick was.
        if new_tickers:
            await _wait_for_quotes(new_tickers, IBKR_QUOTE_WAIT_MS)

        for osi in osi_symbols:
            entry = _TICKER_CACHE.get(osi)
            if entry is None:
                continue
            _contract, ticker = entry
            mid, spread = _read_quote(ticker)
            if mid > 0:
                prices[osi] = mid
                _LAST_SPREAD_PCT[osi] = spread

        return prices

    except Exception as e:
        status_code = 500
        error_msg = str(e)
        log.error("ibkr fetch_prices error: %s", e)
        return {}
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        monitor.record_call("ibkr", "reqMktData", status_code, elapsed_ms, error_msg)


async def prewarm_quote(osi: str) -> None:
    """Fire-and-forget: establish the streaming market-data subscription for a
    contract WITHOUT waiting for the first tick.

    Called the instant an entry signal's OSI is known (before guardrails/scorer)
    so the subscription is live — and IBKR already has price context for the
    order — by the time fetch_prices reads it. Eliminates the cold-subscribe
    IBKR_QUOTE_WAIT_MS wait and the IBKR order price-validation lag a cold
    contract pays (the ~seconds penalty seen in exec_latency_probe).

    Best-effort: any failure is swallowed (fetch_prices' normal cold path still
    works). No-op in dry_run or when already cached/warming.
    """
    if _DRY_RUN() or not osi:
        return
    try:
        if osi in _TICKER_CACHE:
            _TICKER_CACHE.move_to_end(osi)            # already warm/warming
            return
        ib = await _get_ib()
        try:
            contract = _osi_to_ib_option(osi)
        except Exception as e:
            log.warning("prewarm osi parse failed %s: %s", osi, e)
            return
        qualified = await ib.qualifyContractsAsync(contract)
        c = qualified[0] if qualified else None
        if c is None or not getattr(c, "conId", 0):
            log.warning("prewarm qualify failed (skipped): %s", osi)
            return
        # Another task may have subscribed during the await above — re-check.
        if osi in _TICKER_CACHE:
            _TICKER_CACHE.move_to_end(osi)
            return
        # snapshot=False → streaming subscription; persists until cancelled.
        ticker = ib.reqMktData(c, "", False, False)
        _TICKER_CACHE[osi] = (c, ticker)
        _evict_lru(ib, IBKR_TICKER_CACHE_MAX)
        log.info("[IBKR] prewarm subscribed %s", osi)
    except Exception as e:
        log.warning("prewarm_quote error %s: %s", osi, e)


# ── Read-only quote+greeks (decision-support, no trading) ─────────────────

def _field_with_delayed(t, name: str):
    """Read a ticker numeric field, falling back to its delayed* twin."""
    v = getattr(t, name, None)
    if _is_valid(v):
        return round(float(v), 2)
    dv = getattr(t, "delayed" + name.capitalize(), None)
    if _is_valid(dv):
        return round(float(dv), 2)
    return None


async def fetch_quote_detail(osi: str, timeout_ms: int | None = None) -> dict[str, Any]:
    """One-shot read-only quote + greeks for a single option OSI.

    Returns a dict with whatever the feed supplied: bid/ask/mid/last,
    spread_pct, and greeks delta/gamma/theta/vega/iv plus und_price (the
    underlying spot from the option model). Missing fields are omitted.

    Reuses the shared IB connection (cannot open a second clientId) but does
    NOT touch _TICKER_CACHE — it subscribes, waits, reads, and cancels, so it
    never evicts a live trading subscription. Never raises on a data miss —
    returns {} so callers degrade gracefully.

    Greeks/IV require an options-data entitlement. On a delayed feed
    (market_data_type=3) modelGreeks may be absent; the price fields still
    populate. Switch to live (type 1) for reliable greeks.
    """
    if not osi:
        return {}
    if _DRY_RUN():
        sim = _simulate_prices([osi]).get(osi, 0.0)
        return {"osi": osi, "mid": sim, "bid": sim, "ask": sim,
                "spread_pct": 0.0, "_simulated": True}

    wait_ms = timeout_ms if timeout_ms is not None else max(IBKR_QUOTE_WAIT_MS, 2000)
    try:
        ib = await _get_ib()
    except Exception as e:
        log.warning("[QUOTE_DETAIL] IB connect failed for %s: %s", osi, e)
        return {}
    try:
        contract = _osi_to_ib_option(osi)
    except Exception as e:
        log.warning("[QUOTE_DETAIL] OSI parse failed %s: %s", osi, e)
        return {}

    ticker = None
    try:
        qualified = await ib.qualifyContractsAsync(contract)
        if not qualified or not getattr(qualified[0], "conId", 0):
            log.warning("[QUOTE_DETAIL] qualify failed: %s", osi)
            return {}
        contract = qualified[0]
        ticker = ib.reqMktData(contract, "", False, False)

        # Wait for a usable quote AND (ideally) model greeks.
        deadline = time.monotonic() + (wait_ms / 1000.0)
        while time.monotonic() < deadline:
            if _ticker_has_quote(ticker) and getattr(ticker, "modelGreeks", None) is not None:
                break
            await asyncio.sleep(0.05)

        out: dict[str, Any] = {"osi": osi}
        mid, spread = _read_quote(ticker)
        if mid > 0:
            out["mid"] = mid
            out["spread_pct"] = round(spread, 2)
        for fld in ("bid", "ask", "last"):
            v = _field_with_delayed(ticker, fld)
            if v is not None:
                out[fld] = v

        mg = getattr(ticker, "modelGreeks", None)
        if mg is not None:
            for src, dst in (("delta", "delta"), ("gamma", "gamma"),
                             ("theta", "theta"), ("vega", "vega"),
                             ("impliedVol", "iv"), ("undPrice", "und_price")):
                v = getattr(mg, src, None)
                if v is not None and v == v:  # filter NaN
                    out[dst] = round(float(v), 4)
        return out
    except Exception as e:
        log.warning("[QUOTE_DETAIL] fetch failed %s: %s", osi, e)
        return {}
    finally:
        if ticker is not None:
            try:
                ib.cancelMktData(contract)
            except Exception:
                pass


# ── Orders ────────────────────────────────────────────────────────────────

async def fetch_orders() -> list[dict]:
    """Return open + recent orders in the same shape as public_sdk_bridge.fetch_orders."""
    try:
        ib = await _get_ib()
        trades = ib.trades()  # all trades this session (open + filled)
        out: list[dict] = []
        for t in trades:
            o = t.order
            s = t.orderStatus
            c = t.contract
            # Build OSI back from IB contract for symbol consistency.
            sym = getattr(c, "localSymbol", None) or getattr(c, "symbol", "") or ""
            out.append({
                "id":              str(o.orderId or o.permId or ""),
                "symbol":          sym,
                "side":            "BUY" if o.action == "BUY" else "SELL",
                "status":          str(s.status or ""),
                "quantity":        float(o.totalQuantity or 0),
                "filled_quantity": float(s.filled or 0),
                "avg_fill_price":  float(s.avgFillPrice or 0),
                "created_at":      None,  # ib_async doesn't expose order create timestamp directly
            })
        return out
    except Exception as e:
        log.error("ibkr fetch_orders error: %s", e)
        return []


# ── Account ──────────────────────────────────────────────────────────────

async def fetch_account_data() -> dict:
    """Return account snapshot (cash, BP, positions, orders) in public_sdk_bridge shape."""
    try:
        ib = await _get_ib()
        acct = IBKR_ACCOUNT or (ib.managedAccounts()[0] if ib.managedAccounts() else "")

        # accountSummaryAsync returns list[AccountValue]
        vals = await ib.accountSummaryAsync(acct)
        summary = {v.tag: v.value for v in vals if v.account == acct or not acct}

        cash_val = float(summary.get("TotalCashValue") or 0)
        bp = float(summary.get("BuyingPower") or 0)
        opt_bp = float(summary.get("OptionMarketValue") or summary.get("BuyingPower") or 0)
        total_val = float(summary.get("NetLiquidation") or 0)

        positions = []
        for p in ib.positions(acct) if acct else ib.positions():
            try:
                # avgCost is PER CONTRACT on options (multiplier already
                # applied): a $1.20 option reports 119.5673. Every other
                # bridge publishes per share, so divide here or this field
                # reads 100x high for IBKR alone.
                _mult = float(getattr(p.contract, "multiplier", "") or 1) or 1.0
                positions.append({
                    "symbol":        getattr(p.contract, "localSymbol", None) or p.contract.symbol,
                    "quantity":      float(p.position or 0),
                    "avg_cost":      round(float(p.avgCost or 0) / _mult, 4),
                    "current_price": 0.0,  # IB doesn't bundle last price with position; fetch separately if needed
                })
            except Exception as e:
                log.warning("ibkr position parse: %s", e)

        orders = await fetch_orders()

        return {
            "cash": cash_val,
            "buying_power": bp,
            "options_buying_power": opt_bp,
            "total_value": total_val,
            "today_pnl": None,  # same convention as Public bridge
            "positions_count": len(positions),
            "orders": orders[:50],
        }
    except Exception as e:
        log.error("ibkr fetch_account_data error: %s", e)
        return _simulate_account_data()


async def fetch_account_balance() -> dict:
    """Cash / BP / total_value subset of fetch_account_data."""
    try:
        ib = await _get_ib()
        acct = IBKR_ACCOUNT or (ib.managedAccounts()[0] if ib.managedAccounts() else "")
        vals = await ib.accountSummaryAsync(acct)
        summary = {v.tag: v.value for v in vals if v.account == acct or not acct}
        return {
            "cash": float(summary.get("TotalCashValue") or 0),
            "buying_power": float(summary.get("BuyingPower") or 0),
            "options_buying_power": float(summary.get("OptionMarketValue") or summary.get("BuyingPower") or 0),
            "total_value": float(summary.get("NetLiquidation") or 0),
        }
    except Exception as e:
        log.error("ibkr fetch_account_balance error: %s", e)
        return _simulate_balance()


# ── Simulation (dry_run / no creds) ──────────────────────────────────────

import random

_sim_cache: dict[str, float] = {}
_sim_balance = {"cash": 10000.00, "buying_power": 10000.00, "total_value": 10000.00}


def _simulate_prices(osi_symbols: list[str]) -> dict[str, float]:
    result = {}
    for osi in osi_symbols:
        if osi not in _sim_cache:
            try:
                strike = int(osi[-8:]) / 1000
                _sim_cache[osi] = round(max(0.05, strike * 0.0025), 2)
            except (ValueError, IndexError):
                _sim_cache[osi] = 2.00
        delta = random.gauss(0.001, 0.025)
        _sim_cache[osi] = round(max(0.05, _sim_cache[osi] * (1 + delta)), 2)
        result[osi] = _sim_cache[osi]
    return result


def _simulate_balance() -> dict:
    drift = random.gauss(0, 15)
    _sim_balance["total_value"] = round(max(100, _sim_balance["total_value"] + drift), 2)
    pos_value = sum(_sim_cache.values()) * 100 if _sim_cache else 0
    _sim_balance["cash"] = round(max(0, _sim_balance["total_value"] - pos_value), 2)
    _sim_balance["buying_power"] = _sim_balance["cash"]
    return dict(_sim_balance)


def _simulate_account_data() -> dict:
    return {
        "cash": 10000.0,
        "buying_power": 10000.0,
        "options_buying_power": 10000.0,
        "total_value": 10000.0,
        "today_pnl": 0.0,
        "positions_count": 0,
        "orders": [],
    }


async def fetch_positions() -> list[dict]:
    """Normalized open positions for the reconciler.

    Shape: [{"osi_symbol", "qty", "avg_cost", "current_price", "sec_type"}].
    `avg_cost` is PER SHARE, matching positions.avg_price.

    IBKR reports Position.avgCost for an option as the per-CONTRACT cost,
    i.e. already multiplied by the contract multiplier: a $1.20 option comes
    back as 119.5673, not 1.1957. Public, Tradier and Lime all report per
    share, so the division has to happen here or the reconciler would
    overwrite every option's entry price with 100x its real value.
    """
    if _DRY_RUN():
        return []
    ib = await _get_ib()
    acct = IBKR_ACCOUNT or None
    out: list[dict] = []
    for p in (ib.positions(acct) if acct else ib.positions()):
        try:
            c = p.contract
            qty = float(p.position or 0)
            if qty == 0:
                continue
            mult = float(getattr(c, "multiplier", "") or 1) or 1.0
            avg = float(p.avgCost or 0) / mult
            out.append({
                "osi_symbol":    (getattr(c, "localSymbol", "") or c.symbol or "").replace(" ", ""),
                "qty":           qty,
                "avg_cost":      round(avg, 4),
                "current_price": 0.0,   # IB does not bundle a mark with positions
                "sec_type":      getattr(c, "secType", ""),
            })
        except Exception as e:
            log.warning("ibkr position parse: %s", e)
    return out
