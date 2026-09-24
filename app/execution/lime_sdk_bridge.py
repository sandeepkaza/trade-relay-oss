"""
lime_sdk_bridge.py — Thin async wrapper around the Lime Trader REST API
(https://docs.lime.co/trader/).

Mirrors the surface of public_sdk_bridge.py / tradier_sdk_bridge.py so
broker_router.py can dispatch transparently:
    fetch_prices(osi_symbols)        -> dict[osi, mid_price]
    fetch_orders()                   -> list[dict]
    fetch_account_data()             -> dict
    fetch_account_balance()          -> dict
    get_last_spread_pct(osi_symbol)  -> float

Plus the order-IO helpers lime_broker.LimeExecutor calls:
    place_option_order(...)          -> broker order id (str)
    place_multileg_order(...)        -> broker order id (str)
    validate_order(...)              -> (is_valid, message)
    get_order(order_id)              -> dict
    cancel_order(order_id)           -> bool
    list_account_orders()            -> list[dict]
    net_fill_price(order)            -> float | None

WHY RAW REST AND NOT lime-trader-sdk
────────────────────────────────────
Lime publishes an official Python SDK. We don't use it, for two concrete
reasons rather than taste:

  * It pins `attrs>=22.2,<23` and `cattrs` likewise. This app runs attrs 26.x.
    Installing the SDK downgrades attrs by four major versions underneath a
    process that trades real money.
  * Its streaming feed is a `threading.Thread` running websocket-client's
    `run_forever()` — even when obtained from `AsyncLimeClient` — and its
    subscribe path blocks on `time.sleep(1)`. Neither belongs on this app's
    single event loop without a bridge.

Lime's own description of the API is "just plain JSON and POST", and httpx is
already a dependency at the exact version the SDK asks for, so the wrapper
below costs less than the conflict would.

SYMBOL TRANSLATION
──────────────────
The app builds compact OSI ("SPXW260813P07800000", 19 chars). Every symbol
Lime accepts or prints is the OCC form padded to a six-character root field
("SPXW  260813P07800000", 21 chars). Same standard, different spelling of it —
and an unpadded symbol is a rejected order, so `_to_lime`/`_from_lime` are
load-bearing rather than cosmetic.

WHAT LIME CANNOT DO
───────────────────
`order_type` is market|limit ONLY. There is no stop, no stop-limit, and no
OCO/bracket primitive anywhere in the API. The whale OCO bracket that
tradier_broker places natively has no equivalent here — see lime_broker.

There is also no demo BASE URL. Lime issues demo accounts against the same
https://api.lime.co that serves real ones, so the host is never a safety
boundary; `account_number` is the only thing separating paper from real money.
That is why `orders_enabled` defaults false and dry_run is honoured here.
"""

from __future__ import annotations

import asyncio
import configparser
import json
import logging
import os
import socket
import time

import httpx

from app.core.config_manager import cfg  # hot-reloadable singleton
# Dry-run simulation is broker-agnostic — reuse it rather than keeping a third copy.
from app.execution.public_sdk_bridge import (_simulate_account_data, _simulate_balance,
                                             _simulate_prices)

log = logging.getLogger(__name__)

# ── Startup-only Lime credentials (secrets — not dashboard-editable) ───────
_boot = configparser.ConfigParser(inline_comment_prefixes=(";", "#"))
_boot.read("config.ini", encoding="utf-8-sig")

LIME_USERNAME       = os.getenv("LIME_USERNAME",       _boot.get("lime", "username",       fallback=""))
LIME_PASSWORD       = os.getenv("LIME_PASSWORD",       _boot.get("lime", "password",       fallback=""))
LIME_CLIENT_ID      = os.getenv("LIME_CLIENT_ID",      _boot.get("lime", "client_id",      fallback=""))
LIME_CLIENT_SECRET  = os.getenv("LIME_CLIENT_SECRET",  _boot.get("lime", "client_secret",  fallback=""))
LIME_ACCOUNT_NUMBER = os.getenv("LIME_ACCOUNT_NUMBER", _boot.get("lime", "account_number", fallback=""))
del _boot

_BASE = "https://api.lime.co"
_AUTH = "https://auth.lime.co"
# POST /marketdata/quotes rejects a body longer than this (probed 2026-09-09:
# 15 ok, 20 → "Maximum number of symbols exceeded").
_QUOTE_BATCH = 15


def _base_url() -> str:
    return (cfg.get("lime", "base_url", fallback=_BASE) or _BASE).rstrip("/")


def _auth_url() -> str:
    return (cfg.get("lime", "auth_url", fallback=_AUTH) or _AUTH).rstrip("/")


def _DRY_RUN() -> bool:
    return cfg.getboolean("trading", "dry_run", fallback=True)


def _creds_ok() -> bool:
    return all((LIME_USERNAME, LIME_PASSWORD, LIME_CLIENT_ID,
                LIME_CLIENT_SECRET, LIME_ACCOUNT_NUMBER))


def _account() -> str:
    return LIME_ACCOUNT_NUMBER


# ── Auth: OAuth2 password grant, token cached until it nearly expires ──────
# Lime's token is short-lived and every call carries it. Refreshing per request
# would add a round-trip to the entry hot path; refreshing never would fail
# mid-session. The two-minute margin mirrors the official SDK's own rule.

_token: str | None = None
_token_expiry: float = 0.0
_token_lock: asyncio.Lock | None = None


def _lock() -> asyncio.Lock:
    # Built lazily: module import can happen off-loop, and asyncio.Lock binds to
    # the running loop in older semantics.
    global _token_lock
    if _token_lock is None:
        _token_lock = asyncio.Lock()
    return _token_lock


async def _get_token(force: bool = False) -> str:
    """Return a live bearer token, fetching one when the cached copy is inside
    two minutes of expiry. Serialized so a burst of concurrent calls triggers
    one token request, not one per caller."""
    global _token, _token_expiry
    if not force and _token and time.time() < _token_expiry - 120:
        return _token
    async with _lock():
        if not force and _token and time.time() < _token_expiry - 120:
            return _token  # another waiter refreshed while we queued
        resp = await _raw().post(
            f"{_auth_url()}/connect/token",
            data={
                "grant_type": "password",
                "username": LIME_USERNAME,
                "password": LIME_PASSWORD,
                "client_id": LIME_CLIENT_ID,
                "client_secret": LIME_CLIENT_SECRET,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Lime auth failed ({resp.status_code}): {resp.text[:300]}")
        body = resp.json()
        _token = body.get("access_token") or ""
        if not _token:
            raise RuntimeError(f"Lime auth returned no access_token: {body}")
        _token_expiry = time.time() + float(body.get("expires_in") or 3600)
        log.info("[lime] token acquired, expires in %ss", body.get("expires_in"))
        return _token


_raw_client: httpx.AsyncClient | None = None
_keepalive_task: asyncio.Task | None = None
_warm_task: asyncio.Task | None = None
_feed_task: asyncio.Task | None = None
_opra_task: asyncio.Task | None = None
_feed_ok: bool = False
_feed_waiters: dict[str, asyncio.Event] = {}
_feed_snapshots: dict[str, dict] = {}
_feed_seq: dict[str, int] = {}
_feed_prices: dict[str, tuple[float, float]] = {}  # osi-or-lime-sym → (px, monotonic)
_feed_balance: dict | None = None
_book_waiters: list[asyncio.Event] = []


def _http2_ok() -> bool:
    try:
        import h2  # noqa: F401
        return True
    except ImportError:
        return False


def _raw() -> httpx.AsyncClient:
    """One keep-alive TLS session to Lime. TCP_NODELAY so a 200-byte place
    isn't delayed by Nagle waiting to piggyback on the next packet."""
    global _raw_client
    if _raw_client is None:
        limits = httpx.Limits(
            max_keepalive_connections=8,
            max_connections=16,
            keepalive_expiry=120.0,
        )
        transport = httpx.AsyncHTTPTransport(
            http2=_http2_ok(),
            retries=0,
            limits=limits,
            socket_options=[(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)],
        )
        _raw_client = httpx.AsyncClient(
            transport=transport,
            timeout=httpx.Timeout(2.5, connect=0.8, pool=0.2),
        )
    return _raw_client


async def _token_keepalive() -> None:
    """Refresh the bearer off the order path, ~3 minutes before expiry."""
    while True:
        try:
            wait = max(30.0, _token_expiry - time.time() - 180.0) if _token_expiry else 30.0
            await asyncio.sleep(wait)
            if _creds_ok() and not _DRY_RUN():
                await _get_token(force=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.warning("[lime] token keepalive failed", exc_info=True)
            await asyncio.sleep(15)


async def _conn_warm() -> None:
    """Touch the API every 10s so the keep-alive socket is still up when an
    alert hits. Idle NAT/LB drops are otherwise paid on the first place."""
    while True:
        try:
            await asyncio.sleep(10)
            if _creds_ok() and not _DRY_RUN():
                await _req("GET", "/marketdata/schedule")
        except asyncio.CancelledError:
            raise
        except Exception:
            log.debug("[lime] conn warm failed", exc_info=True)


async def warm() -> None:
    """Pay TLS + OAuth before the first alert, then open the account feed.
    Safe to call more than once."""
    global _keepalive_task, _feed_task, _warm_task, _opra_task
    if _DRY_RUN() or not _creds_ok():
        return
    await _get_token()
    try:
        await _req("GET", "/accounts")
    except Exception:
        log.warning("[lime] warm /accounts failed", exc_info=True)
    if _keepalive_task is None or _keepalive_task.done():
        _keepalive_task = asyncio.create_task(_token_keepalive(), name="lime-token-keepalive")
    if _warm_task is None or _warm_task.done():
        _warm_task = asyncio.create_task(_conn_warm(), name="lime-conn-warm")
    if _feed_task is None or _feed_task.done():
        _feed_task = asyncio.create_task(_feed_loop(), name="lime-account-feed")
    if _opra_task is None or _opra_task.done():
        _opra_task = asyncio.create_task(_opra_probe(), name="lime-opra-probe")


def feed_ok() -> bool:
    return _feed_ok


def latest_order_snapshot(order_id: str) -> dict | None:
    return _feed_snapshots.get(order_id)


def order_seq(order_id: str) -> int:
    return _feed_seq.get(order_id, 0)


def latest_balance() -> dict | None:
    return _feed_balance


def _poke_book() -> None:
    for ev in list(_book_waiters):
        ev.set()


def _wake_order(order_id: str, snapshot: dict | None = None) -> None:
    if snapshot is not None:
        _feed_snapshots[order_id] = snapshot
        _feed_seq[order_id] = _feed_seq.get(order_id, 0) + 1
    ev = _feed_waiters.get(order_id)
    if ev is not None:
        ev.set()
    _poke_book()


def _handle_feed_message(raw: str) -> None:
    """Dispatch a Lime account-feed frame onto waiters. No REST."""
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("[lime] feed non-json frame: %r", raw[:200])
        return
    kind = msg.get("t")
    data = msg.get("data")
    if kind == "o":
        for o in data or []:
            if not isinstance(o, dict):
                continue
            oid = str(o.get("client_id") or o.get("id") or o.get("order_id") or "")
            if oid:
                _wake_order(oid, o)
    elif kind == "p":
        now = time.time()
        for block in data or []:
            pos_list = block.get("positions") if isinstance(block, dict) else None
            if pos_list is None and isinstance(block, dict) and "symbol" in block:
                pos_list = [block]
            for pos in pos_list or []:
                if not isinstance(pos, dict):
                    continue
                sym = pos.get("symbol") or ""
                px = float(pos.get("current_price") or 0)
                if not sym or px <= 0:
                    continue
                _feed_prices[sym] = (px, now)
                try:
                    _feed_prices[_from_lime(sym)] = (px, now)
                except Exception:
                    pass
        _poke_book()
    elif kind == "b":
        acct = None
        if isinstance(data, list) and data:
            acct = next((a for a in data if a.get("account_number") == _account()), data[0])
        elif isinstance(data, dict):
            acct = data
        if acct:
            cash = float(acct.get("cash") or 0)
            bp = float(acct.get("margin_buying_power") or acct.get("non_margin_buying_power") or cash)
            global _feed_balance
            _feed_balance = {
                "cash": cash,
                "buying_power": bp,
                "options_buying_power": bp,
                "total_value": float(acct.get("account_value_total") or 0),
            }
        _poke_book()
    elif kind == "e":
        log.warning("[lime] feed error %s", msg)


async def wait_order_update(order_id: str, timeout: float, after_seq: int = -1) -> dict | None:
    """Block until a *new* feed frame for this order, or `timeout`.

    If a snapshot newer than `after_seq` is already here, return it immediately
    (fill that raced the waiter). Does not REST.
    """
    if not order_id:
        await asyncio.sleep(timeout)
        return None
    if _feed_seq.get(order_id, 0) > after_seq:
        return _feed_snapshots.get(order_id)
    ev = _feed_waiters.setdefault(order_id, asyncio.Event())
    ev.clear()
    if _feed_seq.get(order_id, 0) > after_seq:
        return _feed_snapshots.get(order_id)
    try:
        await asyncio.wait_for(ev.wait(), timeout)
    except asyncio.TimeoutError:
        return _feed_snapshots.get(order_id) if _feed_seq.get(order_id, 0) > after_seq else None
    return _feed_snapshots.get(order_id)


async def wait_book_event(timeout: float) -> None:
    """Block until any order/position/balance feed frame, or `timeout`."""
    ev = asyncio.Event()
    _book_waiters.append(ev)
    try:
        await asyncio.wait_for(ev.wait(), timeout)
    except asyncio.TimeoutError:
        pass
    finally:
        try:
            _book_waiters.remove(ev)
        except ValueError:
            pass


async def _feed_loop() -> None:
    """Stay on wss://api.lime.co/accounts for the life of the process.

    Official SDK runs this in a blocking thread with time.sleep(1) on
    subscribe. We use the already-pinned `websockets` package on the app
    loop instead. Reconnects on drop; REST watchdog in lime_broker covers
    the gap.
    """
    global _feed_ok
    import websockets

    url = _base_url().replace("https://", "wss://", 1).rstrip("/") + "/accounts"
    backoff = 1.0
    while True:
        try:
            token = await _get_token()
            async with websockets.connect(
                url,
                additional_headers={"Authorization": f"Bearer {token}"},
                compression=None,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=5,
            ) as ws:
                acct = _account()
                # Orders carry client_id + status + executed_qty — that is the
                # fill event. Trades do not carry order id and would wake every
                # waiter into a REST GET.
                await ws.send(json.dumps({"action": "subscribeOrders", "account": acct}))
                await ws.send(json.dumps({"action": "subscribePositions", "account": acct}))
                await ws.send(json.dumps({"action": "subscribeBalance", "account": acct}))
                _feed_ok = True
                backoff = 1.0
                log.info("[lime] account feed up (orders+positions+balance) acct=%s", acct)
                async for raw in ws:
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", "replace")
                    _handle_feed_message(raw)
        except asyncio.CancelledError:
            _feed_ok = False
            raise
        except Exception:
            _feed_ok = False
            log.warning("[lime] account feed dropped; retry in %.0fs", backoff, exc_info=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 15.0)


async def _req(method: str, path: str, **kw) -> httpx.Response:
    """Authenticated request against the API base. Retries ONCE on a 401 with a
    forced token refresh: a token can expire between our expiry check and the
    server's, and re-auth is cheaper than failing an exit."""
    url = f"{_base_url()}{path}"
    headers = dict(kw.pop("headers", {}))
    headers.setdefault("Accept", "application/json")
    for attempt in (1, 2):
        headers["Authorization"] = f"Bearer {await _get_token(force=attempt == 2)}"
        resp = await _raw().request(method, url, headers=headers, **kw)
        if resp.status_code != 401 or attempt == 2:
            return resp
        log.warning("[lime] 401 on %s — refreshing token and retrying once", path)
    return resp  # unreachable; keeps type checkers quiet


# Last seen ask-bid spread % per OSI symbol. Same contract as public_sdk_bridge:
# position_monitor reads this to gate SL fires when quotes are wide.
_LAST_SPREAD_PCT: dict[str, float] = {}


def get_last_spread_pct(osi_symbol: str) -> float:
    return _LAST_SPREAD_PCT.get(osi_symbol, 0.0)


# ── Symbol translation ─────────────────────────────────────────────────────

def _osi_parts(osi: str) -> tuple[str, str]:
    """Split an OSI string into (underlying, lime_symbol).

    OSI layout: ROOT + YYMMDD + C/P + STRIKE(8, *1000). Root may arrive padded
    or unpadded; Lime always wants it padded to six. The underlying for a
    multileg order's root `symbol` field: SPX/SPXW → 'SPX' (index weeklies
    carry the SPXW root but trade off the SPX underlying — same rule as
    tradier_sdk_bridge and ibkr_sdk_bridge).

    The split is taken from the END, not by scanning forward to the first
    digit: the tail is always exactly 15 characters (6 date + 1 right + 8
    strike), while a root may itself contain digits. Lime's own docs use
    `CZOO1 230818C00003000`, which a scan-forward split would cut after CZOO
    and turn into a different contract.
    """
    s = osi.strip()
    if len(s) < 16:
        raise ValueError(f"bad OSI: {osi!r}")
    root, tail = s[:-15].rstrip(), s[-15:]
    if not root or len(root) > 6 or not tail[:6].isdigit() or tail[6] not in "CP":
        raise ValueError(f"bad OSI: {osi!r}")
    lime = f"{root:<6}{tail}"
    underlying = "SPX" if root in ("SPX", "SPXW") else root
    return underlying, lime


def _to_lime(osi: str) -> str:
    return _osi_parts(osi)[1]


def _from_lime(symbol: str) -> str:
    """Lime's padded OCC → the compact OSI the rest of the app keys on."""
    return (symbol or "").replace(" ", "")


# ── Quotes ─────────────────────────────────────────────────────────────────

def _note_opra(status_code: int) -> None:
    """lime_opra_ok gauge: 0 on 403 (API token not activated for OPRA this
    session — docs.lime.co/trader/market-data/get-current-quote), 1 on 2xx.
    Other errors leave it alone; BrokerApiErrorSpike covers those."""
    from app.core import metrics
    if status_code == 403:
        metrics.set_gauge("lime_opra_ok", 0)
    elif 200 <= status_code < 300:
        metrics.set_gauge("lime_opra_ok", 1)


async def _opra_probe() -> None:
    """Quote one near-the-money SPY option every 60s, 09:00–16:00 ET weekdays,
    so a missing OPRA activation pages (via lime_opra_ok) before the first
    alert finds it. The lane otherwise quotes nothing while flat."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    while True:
        try:
            await asyncio.sleep(60)
            now = datetime.now(ZoneInfo("America/New_York"))
            if now.weekday() >= 5 or not (9 <= now.hour < 16) or _DRY_RUN() or not _creds_ok():
                continue
            resp = await _req("GET", "/marketdata/quote", params={"symbol": "SPY"})
            _note_opra(resp.status_code)
            last = float((resp.json() or {}).get("last") or 0) if resp.status_code == 200 else 0
            if last > 0:
                # SPY lists a 0DTE every weekday at $1 strikes.
                await fetch_prices([f"SPY{now:%y%m%d}C{round(last) * 1000:08d}"])
        except asyncio.CancelledError:
            raise
        except Exception:
            log.debug("[lime] opra probe failed", exc_info=True)


async def fetch_prices(osi_symbols: list[str]) -> dict[str, float]:
    """Mid-price per OSI option symbol. Dry-run / missing-creds → simulated.

    Lime has no market-data websocket, so unlike the Tradier path there is no
    warm streaming tier to serve first and nothing for prewarm_symbol to warm.
    Posts are chunked at _QUOTE_BATCH — a single POST of 20 symbols 400s.
    """
    from app.monitors.api_monitor import monitor

    if not osi_symbols:
        return {}
    if _DRY_RUN() or not _creds_ok():
        return _simulate_prices(osi_symbols)

    lime_to_osi: dict[str, str] = {}
    for osi in osi_symbols:
        try:
            lime_to_osi[_to_lime(osi)] = osi
        except ValueError:
            log.warning("fetch_prices: unparseable OSI %r — skipped", osi)
    if not lime_to_osi:
        return {}

    now = time.time()
    prices: dict[str, float] = {}
    stale: list[str] = []
    for lime_sym, osi in lime_to_osi.items():
        hit = _feed_prices.get(osi) or _feed_prices.get(lime_sym)
        if hit and now - hit[1] < 2.0:
            prices[osi] = hit[0]
        else:
            stale.append(lime_sym)
    if not stale:
        return prices

    # Lime rejects a quotes POST above 15 symbols ("Maximum number of symbols
    # exceeded"). Chunk rather than drop the tail — the monitor quotes every
    # open position in one call.
    symbols = stale
    start = time.perf_counter()
    status_code = 200
    error_msg = None
    try:
        for i in range(0, len(symbols), _QUOTE_BATCH):
            batch = symbols[i:i + _QUOTE_BATCH]
            resp = await _req("POST", "/marketdata/quotes", json=batch)
            status_code = resp.status_code
            _note_opra(status_code)
            resp.raise_for_status()
            for q in (resp.json() or []):
                osi = lime_to_osi.get(q.get("symbol") or "")
                if not osi:
                    # Lime echoes its own padding; fall back to a normalized match.
                    osi = lime_to_osi.get(_from_lime(q.get("symbol") or ""))
                if not osi:
                    continue
                bid = float(q.get("bid") or 0)
                ask = float(q.get("ask") or 0)
                last = float(q.get("last") or 0)
                if bid > 0 and ask > 0:
                    mid = (bid + ask) / 2
                    prices[osi] = round(mid, 2)
                    _LAST_SPREAD_PCT[osi] = ((ask - bid) / mid) * 100.0 if mid > 0 else 0.0
                elif last > 0:
                    prices[osi] = round(last, 2)
                    _LAST_SPREAD_PCT[osi] = 100.0  # single-sided → flag wide so SL gate defers
                if osi in prices:
                    # 2s reuse: LimeExecutor's quote gate and the BUY pricing
                    # that follows it ms later cost one REST call, not two.
                    _feed_prices[osi] = (prices[osi], time.time())
        return prices
    except Exception as e:
        status_code = status_code if status_code >= 400 else 500
        error_msg = str(e)
        log.error("lime fetch_prices error: %s", e)
        return prices
    finally:
        monitor.record_call("lime", "get_quotes", status_code,
                            (time.perf_counter() - start) * 1000, error_msg)


# ── Order IO ───────────────────────────────────────────────────────────────

# Lime status string → Public-vocabulary status the executor/poll expects.
_STATUS_MAP = {
    "new":              "PENDING",
    "pending_new":      "PENDING",
    "pending_cancel":   "PENDING",
    "partially_filled": "PARTIALLY_FILLED",
    "filled":           "FILLED",
    "canceled":         "CANCELLED",
    "cancelled":        "CANCELLED",
    "replaced":         "CANCELLED",   # the replacement carries a new id
    "rejected":         "REJECTED",
    "suspended":        "REJECTED",
    "done_for_day":     "EXPIRED",
}


def translate_status(raw: str) -> str:
    return _STATUS_MAP.get((raw or "").strip().lower(), "PENDING")


def _build_single_payload(osi: str, side: str, quantity: int, limit_price: float,
                          *, order_ref: str = "", duration: str = "day") -> dict:
    """JSON body for a single-leg option limit order.

    Lime infers open vs close from the current position ("The system will
    determine the proper side according to the current position"), so unlike
    Tradier there is no buy_to_open/sell_to_close vocabulary — just buy/sell.
    """
    payload = {
        "account_number": _account(),
        "symbol": _to_lime(osi),
        "quantity": int(quantity),
        "price": round(float(limit_price), 2),
        "order_type": "limit",
        "side": "buy" if side.upper() == "BUY" else "sell",
        "time_in_force": duration,
        "exchange": "auto",
    }
    if order_ref:
        # client_order_id is Lime's duplicate-submission guard: max 32 alphanumeric.
        payload["client_order_id"] = "".join(c for c in order_ref if c.isalnum())[:32]
    return payload


def _build_multileg_payload(legs: list[tuple[str, str, int]], net_price: float,
                            *, is_credit: bool, order_ref: str = "",
                            duration: str = "day") -> dict:
    """JSON body for a multileg order.

    Lime's shape differs from Tradier's in three ways that all matter:

      * The root `symbol` is the UNDERLYING, not a leg, and root `quantity` is
        the number of spreads.
      * Leg `quantity` is a RATIO, not an absolute count — "the total leg
        quantity will be calculated as the product of the leg ratio quantity by
        the order quantity". A 1-2-1 butterfly is literally legs 1, 2, 1.
      * Direction rides on the root `side` rather than a credit/debit order
        type: "use buy, if net debit or even. Use sell, if net credit."

    Validating here rather than letting Lime reject: a 400 on a three-leg order
    doesn't say which leg was wrong, and a silently mangled leg is a real
    position at the wrong strike.
    """
    if not 2 <= len(legs) <= 4:
        raise ValueError(f"multileg takes 2-4 legs, got {len(legs)}")
    if net_price < 0:
        # Lime wants the magnitude; the root side carries the direction.
        raise ValueError(f"net price must be non-negative, got {net_price}")

    out_legs, underlyings, seen = [], set(), set()
    for i, (osi, side, ratio) in enumerate(legs):
        side = side.lower()
        if not side.startswith(("buy", "sell")):
            raise ValueError(f"leg {i}: bad side {side!r}")
        if int(ratio) < 1:
            raise ValueError(f"leg {i}: ratio must be >= 1, got {ratio}")
        underlying, lime_sym = _osi_parts(osi)
        if lime_sym in seen:
            raise ValueError(f"leg {i}: duplicate option symbol {lime_sym}")
        seen.add(lime_sym)
        underlyings.add(underlying)
        # spread_legs emits app vocabulary (buy_to_open / sell_to_close); Lime
        # takes the bare direction and works out open vs close itself.
        out_legs.append({"symbol": lime_sym, "quantity": int(ratio),
                         "side": "buy" if side.startswith("buy") else "sell"})
    if len(underlyings) != 1:
        # One root `symbol` covers the whole order, so mixed underlyings would
        # silently trade under whichever root happened to be picked.
        raise ValueError(f"legs span multiple underlyings: {sorted(underlyings)}")

    payload = {
        "account_number": _account(),
        "symbol": underlyings.pop(),
        "quantity": 1,
        "price": round(float(net_price), 2),
        "order_type": "limit",
        "side": "sell" if is_credit else "buy",
        "time_in_force": duration,
        "exchange": "auto",
        "legs": out_legs,
    }
    if order_ref:
        payload["client_order_id"] = "".join(c for c in order_ref if c.isalnum())[:32]
    return payload


async def validate_order(payload: dict) -> tuple[bool, str]:
    """Run Lime's own validation without sending anything to market.

    Lime documents this as "the order is not sent to market" while applying the
    same logic as a real place. It is the closest thing to Tradier's preview —
    but note it returns only {is_valid, validation_message}: no cost, no
    commission, and no margin figure. The full-width margin surprise that
    preview caught on Tradier would NOT be visible here.
    """
    resp = await _req("POST", "/orders/validate", json=payload)
    if resp.status_code >= 400:
        return False, f"HTTP {resp.status_code}: {resp.text[:300]}"
    body = resp.json() or {}
    return bool(body.get("is_valid")), str(body.get("validation_message") or "")


def _VALIDATE_FIRST() -> bool:
    return cfg.getboolean("lime", "validate_before_order", fallback=False)


async def _place(payload: dict, label: str) -> str:
    """POST an order and return Lime's order id. Raises on rejection so the
    caller surfaces the failure rather than assuming a position exists."""
    from app.monitors.api_monitor import monitor

    if _VALIDATE_FIRST():
        ok, msg = await validate_order(payload)
        if not ok:
            raise RuntimeError(f"Lime validation rejected {label}: {msg}")
        log.info("[LIME VALIDATE] %s ok", label)

    start = time.perf_counter()
    status_code = 200
    error_msg = None
    try:
        resp = await _req("POST", "/orders/place", json=payload)
        status_code = resp.status_code
        if status_code >= 400:
            # raise_for_status() kept only the status line, so a 400 reached the
            # dashboard with Lime's reason thrown away (SPXW 7750C 2026-09-23).
            raise RuntimeError(f"Lime rejected {label}: HTTP {status_code}: {resp.text[:300]}")
        body = resp.json() if resp.content else {}
        if not body.get("success"):
            raise RuntimeError(f"Lime rejected {label}: {body}")
        oid = body.get("data")
        if not oid:
            raise RuntimeError(f"Lime returned no order id for {label}: {body}")
        log.info("[LIME] placed %s id=%s", label, oid)
        return str(oid)
    except Exception as e:
        status_code = status_code if status_code >= 400 else 500
        error_msg = str(e)
        raise
    finally:
        monitor.record_call("lime", "place_order", status_code,
                            (time.perf_counter() - start) * 1000, error_msg)


async def place_option_order(osi: str, side: str, quantity: int, limit_price: float,
                             *, order_ref: str = "", duration: str = "day") -> str:
    """Place a single-leg option limit order. `side` is app vocabulary
    ('BUY'/'SELL'). Returns the Lime order id."""
    payload = _build_single_payload(osi, side, quantity, limit_price,
                                    order_ref=order_ref, duration=duration)
    return await _place(payload, f"{side} {quantity}x {osi} @ {limit_price}")


async def place_multileg_order(legs: list[tuple[str, str, int]], net_price: float,
                               *, is_credit: bool, quantity: int = 1,
                               order_ref: str = "", duration: str = "day") -> str:
    """Place a multileg spread order. `legs` is [(osi, side, ratio), ...];
    `is_credit` is True when opening RECEIVES cash (SpreadAlert.is_credit_structure).
    `quantity` is the number of spreads — leg counts are ratios and Lime
    multiplies them by it."""
    payload = _build_multileg_payload(legs, net_price, is_credit=is_credit,
                                      order_ref=order_ref, duration=duration)
    payload["quantity"] = int(quantity)
    return await _place(payload, f"{'credit' if is_credit else 'debit'} "
                                 f"{quantity}x {len(legs)}-leg @ {net_price}")


def net_fill_price(order: dict) -> float | None:
    """Net price actually paid/received, as a positive magnitude.

    Simpler than the Tradier equivalent by construction: Lime treats a multileg
    as ONE order carrying ONE net price, so there is no per-leg recombination
    to get the sign of. Direction lives in the order's side, and the caller
    already knows it from the structure.

    Returns None while nothing has executed — a partial net is not a price, and
    treating one as final would book a fictitious basis.

    NOTE: `executed_price` appears in Lime's documented streaming order event
    but is absent from the REST OrderDetails model in their own SDK. We read it
    when present and fall back to the limit price, which is exact for a fill at
    the limit and wrong for price improvement. Confirming which field the REST
    payload actually carries is an open probe question.
    """
    if float(order.get("executed_quantity") or 0) <= 0:
        return None
    px = order.get("executed_price")
    if px is None:
        px = order.get("average_price") or order.get("avg_fill_price")
    if px is None:
        px = order.get("price")  # limit price — exact unless we got improvement
    try:
        return round(abs(float(px)), 4)
    except (TypeError, ValueError):
        return None


async def get_order(order_id: str) -> dict:
    """Fetch one order's live state. Returns {} on error (caller treats a
    missing read as 'keep polling')."""
    if not order_id:
        return {}
    try:
        resp = await _req("GET", f"/orders/{order_id}")
        resp.raise_for_status()
        return resp.json() or {}
    except Exception as e:
        log.warning("lime get_order(%s) failed: %s", order_id, e)
        return {}


async def cancel_order(order_id: str) -> bool:
    if not order_id:
        return False
    try:
        resp = await _req("POST", f"/orders/{order_id}/cancel", json={})
        resp.raise_for_status()
        return bool((resp.json() or {}).get("success", True))
    except Exception as e:
        log.warning("lime cancel_order(%s) failed: %s", order_id, e)
        return False


async def list_account_orders() -> list[dict]:
    """Active (working) orders. Lime's endpoint returns only live orders — it is
    not a day's history, which is why the reconciler cannot use it to reclaim
    already-filled orphans the way the Public path does."""
    try:
        resp = await _req("GET", f"/accounts/{_account()}/activeorders")
        resp.raise_for_status()
        return resp.json() or []
    except Exception as e:
        log.warning("lime list_account_orders failed: %s", e)
        return []


async def fetch_orders() -> list[dict]:
    """Normalized order list for the dashboard (mirrors public_sdk_bridge shape)."""
    if _DRY_RUN() or not _creds_ok():
        return []
    out = []
    for o in await list_account_orders():
        out.append({
            "id":       str(o.get("client_id") or ""),
            "symbol":   _from_lime(o.get("symbol") or ""),
            "side":     (o.get("order_side") or "").upper(),
            "quantity": float(o.get("quantity") or 0),
            "filled":   float(o.get("executed_quantity") or 0),
            "status":   translate_status(o.get("order_status") or ""),
            "price":    float(o.get("price") or 0),
        })
    return out


async def fetch_account_balance() -> dict:
    """Read-only balance. Always hits the API when creds exist (safe regardless
    of dry_run); simulated only when unconfigured."""
    from app.monitors.api_monitor import monitor

    if not _creds_ok():
        return _simulate_balance()
    if _feed_balance:
        return dict(_feed_balance)
    start = time.perf_counter()
    status_code = 200
    error_msg = None
    try:
        resp = await _req("GET", "/accounts")
        status_code = resp.status_code
        resp.raise_for_status()
        accounts = resp.json() or []
        acct = next((a for a in accounts
                     if a.get("account_number") == _account()), None) or (accounts[0] if accounts else {})
        cash = float(acct.get("cash") or 0)
        return {
            "cash": cash,
            "buying_power": float(acct.get("margin_buying_power")
                                  or acct.get("non_margin_buying_power") or cash),
            # Lime reports no options-specific buying power; the margin figure is
            # the closest honest number, and cash for a non-margin account.
            "options_buying_power": float(acct.get("margin_buying_power")
                                          or acct.get("non_margin_buying_power") or cash),
            "total_value": float(acct.get("account_value_total") or 0),
        }
    except Exception as e:
        status_code = status_code if status_code >= 400 else 500
        error_msg = str(e)
        log.error("lime fetch_account_balance error: %s", e)
        return _simulate_balance()
    finally:
        monitor.record_call("lime", "get_balances", status_code,
                            (time.perf_counter() - start) * 1000, error_msg)


async def fetch_account_data() -> dict:
    """Balance + positions + working orders (mirrors public_sdk_bridge shape)."""
    if not _creds_ok():
        return _simulate_account_data()
    try:
        bal = await fetch_account_balance()
        positions = []
        try:
            resp = await _req("GET", f"/accounts/{_account()}/positions")
            resp.raise_for_status()
            for p in (resp.json() or []):
                qty = float(p.get("quantity") or 0)
                positions.append({
                    "symbol":        _from_lime(p.get("symbol") or ""),
                    "quantity":      qty,
                    "avg_cost":      float(p.get("average_open_price") or 0),
                    "current_price": float(p.get("current_price") or 0),
                })
        except Exception as e:
            log.warning("lime positions fetch failed: %s", e)
        orders = await fetch_orders()
        return {
            "cash": bal.get("cash", 0),
            "buying_power": bal.get("buying_power", 0),
            "options_buying_power": bal.get("options_buying_power", 0),
            "total_value": bal.get("total_value", 0),
            # No reliable cost basis in the orders payload → defer realized P&L
            # to the local DB (same rationale as public_sdk_bridge).
            "today_pnl": None,
            "positions_count": len(positions),
            "orders": orders[:50],
        }
    except Exception as e:
        log.error("lime fetch_account_data error: %s", e)
        return _simulate_account_data()


async def fetch_positions() -> list[dict]:
    """Normalized open positions for the reconciler. avg_cost is per share
    (Lime's average_open_price is already per share)."""
    if _DRY_RUN() or not _creds_ok():
        return []
    out: list[dict] = []
    try:
        resp = await _req("GET", f"/accounts/{_account()}/positions")
        resp.raise_for_status()
        for p in (resp.json() or []):
            qty = float(p.get("quantity") or 0)
            if qty == 0:
                continue
            out.append({
                "osi_symbol":    _from_lime(p.get("symbol") or ""),
                "qty":           qty,
                "avg_cost":      float(p.get("average_open_price") or 0),
                "current_price": float(p.get("current_price") or 0),
                "sec_type":      "OPT",
            })
    except Exception as e:
        log.warning("lime fetch_positions failed: %s", e)
    return out
