"""
tradier_sdk_bridge.py — Thin async wrapper around the Tradier Brokerage
REST API (https://docs.tradier.com/docs/trading).

Mirrors the surface of public_sdk_bridge.py / ibkr_sdk_bridge.py so
broker_router.py can dispatch transparently:
    fetch_prices(osi_symbols)        -> dict[osi, mid_price]
    fetch_orders()                   -> list[dict]
    fetch_account_data()             -> dict
    fetch_account_balance()          -> dict
    get_last_spread_pct(osi_symbol)  -> float

Plus order-IO helpers the executor (tradier_broker.py) calls:
    place_option_order(...)          -> broker order id (str)
    get_order(order_id)              -> dict
    cancel_order(order_id)           -> bool
    list_account_orders()            -> list[dict]

Transport: Tradier is pure REST (no SDK). httpx async client, Bearer token.

Symbol translation:
    The app uses OSI strings ("SPXW260116C07380000"). Tradier option orders
    need BOTH the underlying root (`symbol`) and the full OCC option symbol
    (`option_symbol`). Quotes take the OCC symbol directly. _osi_parts()
    splits an OSI into (underlying, occ). SPX/SPXW index options map to
    underlying 'SPX' (mirrors the ibkr_sdk_bridge SPX/SPXW rule).

NOTE: re-uses _LAST_SPREAD_PCT semantics from public_sdk_bridge — same dict
shape so the position_monitor SL gate works unchanged when broker=tradier.
"""

from __future__ import annotations

import configparser
import logging
import os
import time
from typing import Any

import httpx

from app.core.config_manager import cfg  # hot-reloadable singleton

log = logging.getLogger(__name__)

# ── Startup-only Tradier credentials (secrets — not dashboard-editable) ─────
_boot = configparser.ConfigParser(inline_comment_prefixes=(";", "#"))
_boot.read("config.ini", encoding="utf-8-sig")

TRADIER_TOKEN      = os.getenv("TRADIER_TOKEN",      _boot.get("tradier", "access_token", fallback=""))
TRADIER_ACCOUNT_ID = os.getenv("TRADIER_ACCOUNT_ID", _boot.get("tradier", "account_id",   fallback=""))
del _boot

_PROD_BASE    = "https://api.tradier.com/v1"
_SANDBOX_BASE = "https://sandbox.tradier.com/v1"


def _base_url() -> str:
    """Sandbox vs production, read live so a config flip takes effect without
    restart. Default sandbox (safe) until explicitly set false.

    An empty `sandbox =` raises rather than falling back — configparser applies
    `fallback` only when the option is MISSING, not when the value fails to
    convert. Left unhandled that ValueError escapes into every quote, balance
    and order call. Resolving it to sandbox fails closed: a production token
    gets clean 401s instead of the bot silently trading live off a config typo.
    """
    try:
        sandbox = cfg.getboolean("tradier", "sandbox", fallback=True)
    except ValueError:
        log.warning("[tradier] sandbox is set to an unparseable value — "
                    "treating as sandbox=true. Set it to true or false explicitly.")
        sandbox = True
    return _SANDBOX_BASE if sandbox else _PROD_BASE


def _DRY_RUN() -> bool:
    return cfg.getboolean("trading", "dry_run", fallback=True)


def _creds_ok() -> bool:
    return bool(TRADIER_TOKEN) and bool(TRADIER_ACCOUNT_ID)


_client: httpx.AsyncClient | None = None


def _http() -> httpx.AsyncClient:
    """Lazily build a shared async client. Bearer auth + JSON accept; Tradier
    POSTs are form-encoded (set per-request)."""
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            headers={
                "Authorization": f"Bearer {TRADIER_TOKEN}",
                "Accept": "application/json",
            },
            timeout=httpx.Timeout(10.0, connect=5.0),
        )
    return _client


# Last seen ask-bid spread % per OSI symbol. Same contract as public_sdk_bridge:
# position_monitor reads this to gate SL fires when quotes are wide.
_LAST_SPREAD_PCT: dict[str, float] = {}


def get_last_spread_pct(osi_symbol: str) -> float:
    return _LAST_SPREAD_PCT.get(osi_symbol, 0.0)


async def prewarm_quote(osi: str) -> None:
    """Subscribe `osi` to the streaming quote feed so a later order finds it
    warm. Best-effort; no-op when streaming is disabled. Called from
    broker_router.prewarm_symbol on the ingest path."""
    try:
        from app.execution import tradier_stream
        await tradier_stream.ensure_subscribed(osi, _osi_parts(osi)[1])
    except Exception:
        log.debug("tradier prewarm_quote skipped for %s", osi, exc_info=True)


# ── Symbol translation ─────────────────────────────────────────────────────

def _osi_parts(osi: str) -> tuple[str, str]:
    """Split an OSI/OCC string into (underlying, occ_symbol).

    OSI layout: ROOT + YYMMDD + C/P + STRIKE(8, *1000). Root may arrive
    space-padded or unpadded; Tradier wants the unpadded OCC symbol. The
    underlying ticker for the order's `symbol` param: SPX/SPXW → 'SPX'
    (index weeklies carry the SPXW root but trade off the SPX underlying),
    everything else → its own root.
    """
    s = osi.strip()
    i = 0
    while i < len(s) and not s[i].isdigit():
        i += 1
    root = s[:i].rstrip()
    if not root or len(s) - i < 15:
        raise ValueError(f"bad OSI: {osi!r}")
    occ = root + s[i:]  # unpadded OCC symbol Tradier expects
    underlying = "SPX" if root in ("SPX", "SPXW") else root
    return underlying, occ


# ── Quotes ─────────────────────────────────────────────────────────────────

def _as_list(node: Any) -> list:
    """Tradier collapses single-element arrays to a bare object. Normalize.
    Also treats the literal string 'null' (Tradier's empty-collection marker)
    as empty."""
    if node is None or node == "null":
        return []
    return node if isinstance(node, list) else [node]


def _obj(node: Any) -> dict:
    """Tradier returns the literal string 'null' for empty collections
    (positions/orders/quotes). Normalize any non-dict to {} so .get() is safe."""
    return node if isinstance(node, dict) else {}


def _body(resp) -> dict:
    """Parse a Tradier response body without letting a JSON error mask the HTTP
    error underneath it.

    Tradier answers some 4xx with an empty or HTML body. Calling resp.json()
    first (as every order-path call used to) raised
    "Expecting value: line 1 column 1 (char 0)" and the real status never
    surfaced — 2026-08-19: a malformed spread symbol 400'd and the operator saw
    a JSON decode error instead of Tradier's rejection.
    """
    try:
        return resp.json() or {}
    except ValueError:
        text = (resp.text or "").strip()
        detail = f"non-JSON body: {text[:200]!r}" if text else "empty body"
        raise RuntimeError(f"Tradier HTTP {resp.status_code} with {detail}") from None


async def fetch_prices(osi_symbols: list[str]) -> dict[str, float]:
    """Mid-price per OSI option symbol. Dry-run / missing-creds → simulated."""
    from app.monitors.api_monitor import monitor

    if not osi_symbols:
        return {}
    if _DRY_RUN() or not _creds_ok():
        return _simulate_prices(osi_symbols)

    # Serve warm streaming quotes first (no REST round-trip); only the symbols
    # without a fresh streamed mid fall through to the REST snapshot below.
    from app.execution import tradier_stream
    prices: dict[str, float] = {}
    rest_needed: list[str] = []
    for osi in osi_symbols:
        mid = tradier_stream.stream_mid(osi)
        if mid is not None:
            prices[osi] = mid
        else:
            rest_needed.append(osi)
    if not rest_needed:
        return prices

    # Map OCC (what Tradier echoes back) → original OSI key the caller passed.
    occ_to_osi: dict[str, str] = {}
    for osi in rest_needed:
        try:
            occ_to_osi[_osi_parts(osi)[1]] = osi
        except ValueError:
            log.warning("fetch_prices: unparseable OSI %r — skipped", osi)

    if not occ_to_osi:
        return prices

    start = time.perf_counter()
    status_code = 200
    error_msg = None
    try:
        resp = await _http().get(
            f"{_base_url()}/markets/quotes",
            params={"symbols": ",".join(occ_to_osi.keys()), "greeks": "false"},
        )
        status_code = resp.status_code
        resp.raise_for_status()
        quotes = _as_list(_obj(resp.json().get("quotes")).get("quote"))

        for q in quotes:  # merge REST results into any warm streamed prices
            occ = q.get("symbol") or ""
            osi = occ_to_osi.get(occ)
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
        return prices
    except Exception as e:
        status_code = status_code if status_code >= 400 else 500
        error_msg = str(e)
        log.error("tradier fetch_prices error: %s", e)
        return prices  # return whatever streamed quotes we already had
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        monitor.record_call("tradier", "get_quotes", status_code, elapsed_ms, error_msg)


# ── Order IO (called by tradier_broker.TradierExecutor) ────────────────────

# Tradier status string → Public-vocabulary status the executor/poll expects.
_STATUS_MAP = {
    "pending":          "PENDING",
    "open":             "PENDING",
    "pending_cancel":   "PENDING",
    "partially_filled": "PARTIALLY_FILLED",
    "filled":           "FILLED",
    "canceled":         "CANCELLED",
    "cancelled":        "CANCELLED",
    "expired":          "EXPIRED",
    "rejected":         "REJECTED",
    "error":            "REJECTED",
}


def translate_status(raw: str) -> str:
    return _STATUS_MAP.get((raw or "").strip().lower(), "PENDING")


def _build_option_payload(osi: str, side: str, quantity: int, limit_price: float,
                          *, order_ref: str = "", duration: str = "day") -> dict:
    """Form params for a single-leg option limit order (shared by preview+place).
    BUY = open a long option; SELL = close it. The bot only ever goes long
    options (BUY entry → SELL exit), so to_open/to_close map cleanly."""
    underlying, occ = _osi_parts(osi)
    tside = "buy_to_open" if side.upper() == "BUY" else "sell_to_close"
    data = {
        "class": "option",
        "symbol": underlying,
        "option_symbol": occ,
        "side": tside,
        "quantity": str(int(quantity)),
        "type": "limit",
        "duration": duration,
        "price": f"{float(limit_price):.2f}",
    }
    if order_ref:
        data["tag"] = order_ref[:255]  # Tradier client tag (alnum/-, ≤255)
    return data


async def preview_option_order(osi: str, side: str, quantity: int, limit_price: float,
                               *, order_ref: str = "", duration: str = "day") -> dict:
    """Validate an option order WITHOUT executing (preview=true). Runs Tradier's
    buying-power/param checks and returns estimated cost + commission. On an
    invalid order Tradier replies HTTP 400 with a descriptive body → raised as
    RuntimeError. Returns the `order` preview dict (status='ok', result, cost,
    commission, margin_change, ...) when valid."""
    data = _build_option_payload(osi, side, quantity, limit_price,
                                 order_ref=order_ref, duration=duration)
    data["preview"] = "true"
    resp = await _http().post(
        f"{_base_url()}/accounts/{TRADIER_ACCOUNT_ID}/orders", data=data,
    )
    body = _body(resp)
    if resp.status_code >= 400:
        # Tradier returns {"errors":{"error":[...]}} on a rejected preview.
        raise RuntimeError(
            f"Tradier preview rejected ({resp.status_code}): {_err_text(body)}")
    order = (body or {}).get("order") or {}
    if str(order.get("status", "")).lower() != "ok":
        raise RuntimeError(f"Tradier preview not ok: {_err_text(body)}")
    return order


def _PREVIEW_ENABLED() -> bool:
    return cfg.getboolean("tradier", "preview_before_order", fallback=False)


async def place_option_order(osi: str, side: str, quantity: int, limit_price: float,
                             *, order_ref: str = "", duration: str = "day") -> str:
    """POST an option limit order. `side` is app vocabulary ('BUY'/'SELL');
    mapped to Tradier open/close semantics. Returns the Tradier order id (str).
    Raises on a non-OK response so the executor can surface the failure.

    When [tradier] preview_before_order is on, the order is validated via
    preview=true first (buying-power / param check); a rejected preview raises
    before any live order is sent."""
    from app.monitors.api_monitor import monitor

    if _PREVIEW_ENABLED():
        prev = await preview_option_order(osi, side, quantity, limit_price,
                                          order_ref=order_ref, duration=duration)
        log.info("[TRADIER PREVIEW] %s %sx %s @ %s — cost=%s commission=%s",
                 side, quantity, osi, limit_price,
                 prev.get("order_cost") or prev.get("cost"), prev.get("commission"))

    data = _build_option_payload(osi, side, quantity, limit_price,
                                 order_ref=order_ref, duration=duration)

    start = time.perf_counter()
    status_code = 200
    error_msg = None
    try:
        resp = await _http().post(
            f"{_base_url()}/accounts/{TRADIER_ACCOUNT_ID}/orders", data=data,
        )
        status_code = resp.status_code
        body = _body(resp)
        if resp.status_code >= 400:
            # Surface Tradier's own rejection text — raise_for_status alone
            # reports the status and drops the reason.
            errs = (body.get("errors") or {}).get("error")
            raise RuntimeError(f"Tradier rejected order ({resp.status_code}): {errs or body}")
        order = body.get("order") or {}
        if str(order.get("status", "")).lower() not in ("ok", "open", "pending"):
            raise RuntimeError(f"Tradier rejected order: {body}")
        oid = order.get("id")
        if oid is None:
            raise RuntimeError(f"Tradier returned no order id: {body}")
        return str(oid)
    except Exception as e:
        status_code = status_code if status_code >= 400 else 500
        error_msg = str(e)
        raise
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        monitor.record_call("tradier", "place_order", status_code, elapsed_ms, error_msg)


async def place_oco_bracket(osi: str, quantity: int, tp_px: float, sl_px: float,
                            *, tag: str = "") -> str:
    """Place a native OCO resting bracket for an already-held option position:
        leg[0] sell_to_close LIMIT @ tp_px   (take-profit)
        leg[1] sell_to_close STOP  @ sl_px   (stop-loss)
    When one fills, Tradier auto-cancels the other. Returns the parent OCO
    order id. duration=gtc so the bracket rests until filled/cancelled.

    Tradier OCO rule: both option legs must share option_symbol and differ in
    type (limit vs stop) — satisfied here. (See Advanced Orders docs.)"""
    from app.monitors.api_monitor import monitor

    underlying, occ = _osi_parts(osi)
    q = str(int(quantity))
    data = {
        "class": "oco",
        "duration": "gtc",
        "symbol": underlying,
        # leg 0 — take-profit limit
        "option_symbol[0]": occ, "side[0]": "sell_to_close",
        "quantity[0]": q, "type[0]": "limit", "price[0]": f"{float(tp_px):.2f}",
        # leg 1 — stop-loss stop
        "option_symbol[1]": occ, "side[1]": "sell_to_close",
        "quantity[1]": q, "type[1]": "stop", "stop[1]": f"{float(sl_px):.2f}",
    }
    if tag:
        data["tag"] = tag[:255]

    start = time.perf_counter()
    status_code = 200
    error_msg = None
    try:
        resp = await _http().post(
            f"{_base_url()}/accounts/{TRADIER_ACCOUNT_ID}/orders", data=data,
        )
        status_code = resp.status_code
        body = resp.json()
        resp.raise_for_status()
        order = body.get("order") or {}
        if str(order.get("status", "")).lower() not in ("ok", "open", "pending"):
            raise RuntimeError(f"Tradier rejected OCO: {body}")
        oid = order.get("id")
        if oid is None:
            raise RuntimeError(f"Tradier returned no OCO order id: {body}")
        return str(oid)
    except Exception as e:
        status_code = status_code if status_code >= 400 else 500
        error_msg = str(e)
        raise
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        monitor.record_call("tradier", "place_oco", status_code, elapsed_ms, error_msg)


# ── Multileg (verticals / spreads) ─────────────────────────────────────────
#
# Tradier takes a spread as ONE order with indexed legs and a single net price
# (docs.tradier.com/docs/trading): class=multileg, type=debit|credit|even, and
# option_symbol[n]/side[n]/quantity[n] per leg. Filling both legs as one order
# is the point — legging in separately leaves a naked short if the second fill
# never lands, which on a 5-wide SPX spread is an unbounded position instead of
# a $500 one.
#
# Unlike the single-leg path, `side` is passed in Tradier's own vocabulary
# rather than the app's BUY/SELL. Credit spreads need sell_to_open, which
# BUY/SELL cannot express, and inventing an app-level word for it would only
# add a translation layer with one caller.

_MULTILEG_SIDES = frozenset({
    "buy_to_open", "sell_to_open", "buy_to_close", "sell_to_close",
})
_MULTILEG_TYPES = frozenset({"debit", "credit", "even", "market"})


def _err_text(body) -> str:
    """Tradier's message out of a rejection body, or the body if it has none.

    A rejected preview can arrive as HTTP 200 with {"errors": {"error": ...}}
    and no "order" key. Raising the raw dict put `{'errors': {'error': 'Buy
    order is for more shares...` into Order.error_text, which the dashboard
    then truncated mid-repr at 80 chars — the operator saw punctuation where
    the broker's reason should be.
    """
    errs = ((body or {}).get("errors") or {}).get("error") if isinstance(body, dict) else None
    if isinstance(errs, (list, tuple)):
        errs = "; ".join(str(e) for e in errs)
    return str(errs).strip() if errs else str(body)


def _build_multileg_payload(legs: list[tuple[str, str, int]], net_price: float,
                            *, order_type: str, order_ref: str = "",
                            duration: str = "day") -> dict:
    """Form params for a multileg order (shared by preview + place).

    Validates before building rather than letting Tradier reject: a 400 on a
    two-leg order is ambiguous about which leg was wrong, and a silently
    mangled leg is a real position at the wrong strike.
    """
    if not 2 <= len(legs) <= 4:
        raise ValueError(f"multileg takes 2-4 legs, got {len(legs)}")
    order_type = order_type.lower()
    if order_type not in _MULTILEG_TYPES:
        raise ValueError(f"bad multileg type {order_type!r}")
    if net_price < 0:
        # Tradier wants the magnitude; `type` carries the direction.
        raise ValueError(f"net price must be non-negative, got {net_price}")

    data = {
        "class": "multileg",
        "type": order_type,
        "duration": duration,
        "price": f"{float(net_price):.2f}",
    }
    underlyings, seen = set(), set()
    for i, (osi, side, qty) in enumerate(legs):
        side = side.lower()
        if side not in _MULTILEG_SIDES:
            raise ValueError(f"leg {i}: bad side {side!r}")
        if int(qty) < 1:
            raise ValueError(f"leg {i}: quantity must be >= 1, got {qty}")
        underlying, occ = _osi_parts(osi)
        if occ in seen:
            raise ValueError(f"leg {i}: duplicate option symbol {occ}")
        seen.add(occ)
        underlyings.add(underlying)
        data[f"option_symbol[{i}]"] = occ
        data[f"side[{i}]"] = side
        data[f"quantity[{i}]"] = str(int(qty))
    if len(underlyings) != 1:
        # One `symbol` param covers the whole order, so mixed underlyings would
        # silently trade under whichever root happened to be picked.
        raise ValueError(f"legs span multiple underlyings: {sorted(underlyings)}")
    data["symbol"] = underlyings.pop()
    if order_ref:
        data["tag"] = order_ref[:255]
    return data


async def preview_multileg_order(legs: list[tuple[str, str, int]], net_price: float,
                                 *, order_type: str, order_ref: str = "",
                                 duration: str = "day") -> dict:
    """Validate a multileg order without executing it. Returns Tradier's preview
    dict (cost, commission, margin_change) or raises RuntimeError on rejection.

    Worth more here than on the single-leg path: a credit spread's cost isn't
    the premium, it's the margin Tradier reserves, and preview is the only way
    to see that number before the order is live.
    """
    data = _build_multileg_payload(legs, net_price, order_type=order_type,
                                   order_ref=order_ref, duration=duration)
    data["preview"] = "true"
    resp = await _http().post(
        f"{_base_url()}/accounts/{TRADIER_ACCOUNT_ID}/orders", data=data,
    )
    body = _body(resp)
    if resp.status_code >= 400:
        raise RuntimeError(
            f"Tradier multileg preview rejected ({resp.status_code}): {_err_text(body)}")
    order = (body or {}).get("order") or {}
    if str(order.get("status", "")).lower() != "ok":
        raise RuntimeError(f"Tradier multileg preview not ok: {_err_text(body)}")
    return order


async def place_multileg_order(legs: list[tuple[str, str, int]], net_price: float,
                               *, order_type: str, order_ref: str = "",
                               duration: str = "day") -> str:
    """POST a multileg spread order. `legs` is [(osi, tradier_side, qty), ...];
    `order_type` is 'credit' when the net price is received and 'debit' when
    it's paid. Returns the Tradier order id. Raises on rejection so the caller
    surfaces the failure rather than assuming a position exists.

    When [tradier] preview_before_order is on, the order is validated via
    preview first and a rejected preview raises before anything goes live.
    """
    from app.monitors.api_monitor import monitor

    # Preview ALWAYS on multileg, not just when preview_before_order is set.
    # Verified against the live account 2026-08-10: a 5-wide SPX credit spread
    # reserves the FULL WIDTH as margin — margin_change was 500.0 for 1x and
    # 1500.0 for 3x — not the (width - credit) that the position actually
    # risks. Nothing else on this path can see that number before the order is
    # live, and a spread that clears our risk cap can still exceed the account's
    # option buying power. Preview is Tradier's own buying-power check, so it
    # runs unconditionally and a rejection raises before anything goes out.
    prev = await preview_multileg_order(legs, net_price, order_type=order_type,
                                        order_ref=order_ref, duration=duration)
    log.info("[TRADIER PREVIEW] multileg %s %s @ %s — cost=%s commission=%s margin=%s",
             order_type, [f"{s} {q}x {o}" for o, s, q in legs], net_price,
             prev.get("order_cost") or prev.get("cost"), prev.get("commission"),
             prev.get("margin_change"))

    data = _build_multileg_payload(legs, net_price, order_type=order_type,
                                   order_ref=order_ref, duration=duration)

    start = time.perf_counter()
    status_code = 200
    error_msg = None
    try:
        resp = await _http().post(
            f"{_base_url()}/accounts/{TRADIER_ACCOUNT_ID}/orders", data=data,
        )
        status_code = resp.status_code
        body = _body(resp)
        if resp.status_code >= 400:
            errs = (body.get("errors") or {}).get("error")
            raise RuntimeError(f"Tradier rejected multileg ({resp.status_code}): {errs or body}")
        order = body.get("order") or {}
        if str(order.get("status", "")).lower() not in ("ok", "open", "pending"):
            raise RuntimeError(f"Tradier rejected multileg: {body}")
        oid = order.get("id")
        if oid is None:
            raise RuntimeError(f"Tradier returned no multileg order id: {body}")
        log.info("[TRADIER MULTILEG] placed %s id=%s %s @ %.2f",
                 order_type, oid, [f"{s} {q}x {o}" for o, s, q in legs], net_price)
        return str(oid)
    except Exception as e:
        status_code = status_code if status_code >= 400 else 500
        error_msg = str(e)
        raise
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        monitor.record_call("tradier", "place_multileg", status_code, elapsed_ms, error_msg)


def net_fill_price(order: dict) -> float | None:
    """Net price actually paid/received across a filled multileg order.

    Tradier reports `avg_fill_price` per leg, never as a net, so P&L has to
    recombine them: legs bought cost money, legs sold bring it in, and the
    sign of the result is what says whether the spread went on for a credit.
    Returns None while any leg is still unfilled — a partial net is not a
    price, and treating one as final would book a fictitious basis.
    """
    legs = _as_list(order.get("leg"))
    if not legs:
        return None
    net = 0.0
    for leg in legs:
        px = leg.get("avg_fill_price")
        filled = float(leg.get("exec_quantity") or 0)
        if px is None or filled <= 0:
            return None
        sign = -1.0 if str(leg.get("side", "")).startswith("buy") else 1.0
        net += sign * float(px) * filled
    # Per-spread net = total net / how many spreads went on. The smallest leg
    # is that count for every structure placed here: 1:1 for a vertical, and
    # 1-2-1 for a butterfly, where the wings — not the doubled body — are what
    # one spread is worth. Dividing by the MEAN leg size instead would understate
    # a fly's net by the ratio of its body to its wings.
    spreads = min(float(leg.get("exec_quantity") or 0) for leg in legs)
    return round(net / spreads, 4) if spreads else None


async def get_order(order_id: str) -> dict:
    """Fetch one order's live state. Returns {} on error (caller treats a
    missing read as 'keep polling')."""
    if not order_id:
        return {}
    try:
        resp = await _http().get(
            f"{_base_url()}/accounts/{TRADIER_ACCOUNT_ID}/orders/{order_id}",
        )
        resp.raise_for_status()
        return (resp.json().get("order") or {})
    except Exception as e:
        log.debug("tradier get_order(%s) error: %s", order_id, e)
        return {}


async def cancel_order(order_id: str) -> bool:
    """DELETE an order. True if Tradier accepted the cancel."""
    if not order_id:
        return False
    try:
        resp = await _http().delete(
            f"{_base_url()}/accounts/{TRADIER_ACCOUNT_ID}/orders/{order_id}",
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        log.warning("tradier cancel_order(%s) error: %s", order_id, e)
        return False


async def list_account_orders() -> list[dict]:
    """Raw Tradier order objects for the account (used by external-cancel)."""
    try:
        resp = await _http().get(
            f"{_base_url()}/accounts/{TRADIER_ACCOUNT_ID}/orders",
        )
        resp.raise_for_status()
        return _as_list(_obj(resp.json().get("orders")).get("order"))
    except Exception as e:
        log.warning("tradier list_account_orders error: %s", e)
        return []


async def fetch_orders() -> list[dict]:
    """Dashboard-shaped order list (mirrors public_sdk_bridge.fetch_orders)."""
    if not _creds_ok():
        return []
    out: list[dict] = []
    for o in await list_account_orders():
        out.append({
            "id":              str(o.get("id", "")),
            "symbol":          o.get("option_symbol") or o.get("symbol") or "",
            "side":            str(o.get("side") or ""),
            "status":          str(o.get("status") or ""),
            "quantity":        float(o.get("quantity") or 0),
            "filled_quantity": float(o.get("exec_quantity") or 0),
            "avg_fill_price":  float(o.get("avg_fill_price") or 0),
            "created_at":      o.get("create_date"),
        })
    return out


# ── Account ────────────────────────────────────────────────────────────────

async def fetch_account_balance() -> dict:
    """Read-only balance. Always hits the API when creds exist (safe regardless
    of dry_run); simulated only when unconfigured."""
    from app.monitors.api_monitor import monitor

    if not _creds_ok():
        return _simulate_balance()
    start = time.perf_counter()
    status_code = 200
    error_msg = None
    try:
        resp = await _http().get(
            f"{_base_url()}/accounts/{TRADIER_ACCOUNT_ID}/balances",
        )
        status_code = resp.status_code
        resp.raise_for_status()
        b = resp.json().get("balances") or {}
        # option_buying_power is nested per account type: margin{}/pdt{} carry it;
        # cash accounts expose cash{}.cash_available instead (no option_buying_power
        # key). total_cash/total_equity are top-level. Probe all, fall back to cash.
        margin = b.get("margin") or {}
        pdt = b.get("pdt") or {}
        cash_obj = b.get("cash") or {}
        cash = float(cash_obj.get("cash_available") or b.get("total_cash") or 0)
        opt_bp = float(margin.get("option_buying_power")
                       or pdt.get("option_buying_power")
                       or cash_obj.get("cash_available")
                       or b.get("total_cash") or 0)
        return {
            "cash": cash,
            "buying_power": float(b.get("total_cash") or cash or 0),
            "options_buying_power": opt_bp,
            "total_value": float(b.get("total_equity") or 0),
        }
    except Exception as e:
        status_code = status_code if status_code >= 400 else 500
        error_msg = str(e)
        log.error("tradier fetch_account_balance error: %s", e)
        return _simulate_balance()
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        monitor.record_call("tradier", "get_balances", status_code, elapsed_ms, error_msg)


async def fetch_account_data() -> dict:
    """Balance + positions + recent orders (mirrors public_sdk_bridge shape)."""
    if not _creds_ok():
        return _simulate_account_data()
    try:
        bal = await fetch_account_balance()
        positions = []
        try:
            resp = await _http().get(
                f"{_base_url()}/accounts/{TRADIER_ACCOUNT_ID}/positions",
            )
            resp.raise_for_status()
            for p in _as_list(_obj(resp.json().get("positions")).get("position")):
                qty = float(p.get("quantity") or 0)
                cost = float(p.get("cost_basis") or 0)
                positions.append({
                    "symbol":        p.get("symbol") or "",
                    "quantity":      qty,
                    # cost_basis is total $; per-contract = cost / (qty*100).
                    "avg_cost":      round(cost / (abs(qty) * 100), 4) if qty else 0,
                    "current_price": 0,
                })
        except Exception as e:
            log.warning("tradier positions fetch failed: %s", e)
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
        log.error("tradier fetch_account_data error: %s", e)
        return _simulate_account_data()


# ── Dry-run / unconfigured simulation (mirrors public_sdk_bridge) ──────────

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
        "cash": 10000.0, "buying_power": 10000.0, "options_buying_power": 10000.0,
        "total_value": 10000.0, "today_pnl": 0.0, "positions_count": 0, "orders": [],
    }


async def fetch_positions() -> list[dict]:
    """Normalized open positions for the reconciler. avg_cost is per share.

    Tradier reports cost_basis as the TOTAL dollars for the lot, so the
    per-share figure is cost / (qty * 100).
    """
    if _DRY_RUN() or not _creds_ok():
        return []
    out: list[dict] = []
    try:
        resp = await _http().get(f"{_base_url()}/accounts/{TRADIER_ACCOUNT_ID}/positions")
        body = _body(resp)
        if resp.status_code >= 400:
            raise RuntimeError(f"Tradier positions HTTP {resp.status_code}: {body}")
        for p in _as_list(_obj(body.get("positions")).get("position")):
            qty = float(p.get("quantity") or 0)
            if qty == 0:
                continue
            cost = float(p.get("cost_basis") or 0)
            out.append({
                "osi_symbol":    (p.get("symbol") or "").replace(" ", ""),
                "qty":           qty,
                "avg_cost":      round(cost / (abs(qty) * 100), 4),
                "current_price": 0.0,
                "sec_type":      "OPT",
            })
    except Exception as e:
        log.warning("tradier fetch_positions failed: %s", e)
    return out
