"""
public_sdk_bridge.py — Thin async wrapper around the Public.com SDK
for batch-fetching option quotes used by the position monitor.

Returns: dict mapping OSI symbol → mid-price (float)
"""

import logging
import os
import configparser

from app.core.config_manager import cfg  # hot-reloadable singleton

log = logging.getLogger(__name__)

# Load API key from environment or config.ini
API_KEY = os.getenv("PUBLIC_API_KEY")
if not API_KEY:
    try:
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
        API_KEY = config.get('public', 'api_key', fallback=None)
    except Exception:
        pass

# Startup-only values (API credentials — not editable from dashboard)
import configparser as _cp
_boot = _cp.ConfigParser(inline_comment_prefixes=(";", "#"))
_boot.read("config.ini", encoding="utf-8-sig")

API_KEY     = os.getenv("PUBLIC_API_KEY",        _boot.get("public", "api_key",        fallback=""))
ACCOUNT_NUM = os.getenv("PUBLIC_ACCOUNT_NUMBER", _boot.get("public", "account_number", fallback=""))

# DRY_RUN is read live on every request so dashboard toggles take effect immediately
def _DRY_RUN() -> bool:
    return cfg.getboolean("trading", "dry_run", fallback=True)

_client = None

# Last seen ask-bid spread % per OSI symbol. Populated by fetch_prices.
# Position monitor reads this to gate SL fires when quotes are wide
# (e.g. pre-market thin books). Stale entries are harmless — SL gate
# only refuses to fire when spread is currently > threshold.
_LAST_SPREAD_PCT: dict[str, float] = {}


def get_last_spread_pct(osi_symbol: str) -> float:
    """Return last observed bid-ask spread % for symbol, or 0 if unseen."""
    return _LAST_SPREAD_PCT.get(osi_symbol, 0.0)


async def _get_client():
    global _client
    if _client is not None:
        return _client

    from public_api_sdk import (
        AsyncPublicApiClient,
        AsyncPublicApiClientConfiguration,
        ApiKeyAuthConfig,
    )
    _client = AsyncPublicApiClient(
        auth_config = ApiKeyAuthConfig(api_secret_key=API_KEY),
        config      = AsyncPublicApiClientConfiguration(default_account_number=ACCOUNT_NUM),
    )
    return _client


async def fetch_index_level(symbol: str = "VIX") -> float | None:
    """Fetch an index level (e.g. VIX) via the Public market-data quotes API.
    Returns None on any failure — callers treat missing data conservatively."""
    if _DRY_RUN() or not API_KEY or API_KEY == "YOUR_PUBLIC_API_KEY_HERE":
        return None
    try:
        from public_api_sdk import OrderInstrument, InstrumentType
        client = await _get_client()
        quotes = await client.get_quotes([OrderInstrument(symbol=symbol, type=InstrumentType.INDEX)])
        for q in quotes:
            last = float(q.last or 0)
            if last > 0:
                return last
            bid, ask = float(q.bid or 0), float(q.ask or 0)
            if bid > 0 and ask > 0:
                return (bid + ask) / 2
        return None
    except Exception as e:
        log.debug("fetch_index_level(%s) failed: %s", symbol, e)
        return None


async def fetch_prices(osi_symbols: list[str]) -> dict[str, float]:
    """
    Fetch current mid-prices for a list of OSI option symbols.

    In dry_run mode, returns simulated prices with ±3% random walk
    so the position monitor can exercise auto-exit logic without real orders.
    """
    import time
    from app.monitors.api_monitor import monitor

    if not osi_symbols:
        return {}

    if _DRY_RUN() or not API_KEY or API_KEY == "YOUR_PUBLIC_API_KEY_HERE":
        return _simulate_prices(osi_symbols)

    start = time.perf_counter()
    status_code = 200
    error_msg = None

    try:
        from public_api_sdk import OrderInstrument, InstrumentType
        client = await _get_client()

        instruments = [
            OrderInstrument(symbol=osi, type=InstrumentType.OPTION)
            for osi in osi_symbols
        ]

        quotes = await client.get_quotes(instruments)

        prices = {}
        for quote in quotes:
            osi  = quote.instrument.symbol
            bid  = float(quote.bid or 0)
            ask  = float(quote.ask or 0)
            last = float(quote.last or 0)

            if bid > 0 and ask > 0:
                mid = (bid + ask) / 2
                prices[osi] = round(mid, 2)
                _LAST_SPREAD_PCT[osi] = ((ask - bid) / mid) * 100.0 if mid > 0 else 0.0
            elif last > 0:
                prices[osi] = round(last, 2)
                # Single-sided quote — flag spread as wide so SL gate defers.
                _LAST_SPREAD_PCT[osi] = 100.0

        return prices

    except Exception as e:
        status_code = 500
        error_msg = str(e)
        log.error("fetch_prices error: %s", e)
        elapsed_ms = (time.perf_counter() - start) * 1000
        monitor.record_call("public_com", "get_quotes", status_code, elapsed_ms, error_msg)
        return {}
    finally:
        # Record metrics for successful calls (errors are handled above)
        if status_code == 200:
            elapsed_ms = (time.perf_counter() - start) * 1000
            monitor.record_call("public_com", "get_quotes", status_code, elapsed_ms, error_msg)


async def fetch_orders() -> list[dict]:
    """
    Fetch all orders from Public.com account.
    Returns list of orders with their status, fills, and P&L.

    The SDK has no get_orders() — orders come from portfolio.orders.
    """
    if not API_KEY or API_KEY == "YOUR_PUBLIC_API_KEY_HERE":
        return []

    try:
        client = await _get_client()
        portfolio = await client.get_portfolio()
        orders = []
        for order in (portfolio.orders or []):
            status_val = getattr(order.status, "value", order.status) if order.status else ""
            orders.append({
                "id":              str(order.order_id),
                "symbol":          order.instrument.symbol if order.instrument else "",
                "side":            str(getattr(order.side, "value", order.side) or ""),
                "status":          str(status_val),
                "quantity":        float(order.quantity) if order.quantity else 0,
                "filled_quantity": float(order.filled_quantity) if order.filled_quantity else 0,
                "avg_fill_price":  float(order.average_price) if order.average_price else 0,
                "created_at":      order.created_at.isoformat() if order.created_at else None,
            })
        return orders
    except Exception as e:
        log.error("fetch_orders error: %s", e)
        return []


async def fetch_account_data() -> dict:
    """
    Fetch comprehensive account data from Public.com including:
    - Balance
    - Positions
    - Today's realized P&L
    - Order history
    """
    if not API_KEY or API_KEY == "YOUR_PUBLIC_API_KEY_HERE":
        return _simulate_account_data()

    try:
        client = await _get_client()
        
        # Get portfolio data
        portfolio = await client.get_portfolio()
        bp = portfolio.buying_power
        equity = portfolio.equity or []
        cash_equity = next((e for e in equity if str(e.type).endswith("CASH")), None)
        cash_val = float(cash_equity.value) if cash_equity else 0.0
        total_val = sum(float(e.value) for e in equity)
        
        # Positions come from portfolio.positions (SDK has no get_positions())
        positions = []
        for pos in (portfolio.positions or []):
            try:
                unit_cost = pos.cost_basis.unit_cost if pos.cost_basis else None
                last_px   = pos.last_price.last_price if pos.last_price else None
                positions.append({
                    "symbol":        pos.instrument.symbol if pos.instrument else "",
                    "quantity":      float(pos.quantity) if pos.quantity else 0,
                    "avg_cost":      float(unit_cost) if unit_cost is not None else 0,
                    "current_price": float(last_px) if last_px is not None else 0,
                })
            except Exception as e:
                log.warning("Could not parse position: %s", e)
        
        orders = await fetch_orders()

        # Broker has no cost basis in the orders payload, so today_pnl cannot be
        # derived here without mis-reporting gross SELL revenue as P&L. Return
        # None and let the frontend use the local DB-sourced realized P&L
        # from /api/stats (which matches each SELL leg to its Position's avg_price).
        return {
            "cash": cash_val,
            "buying_power": float(bp.buying_power or bp.cash_only_buying_power or 0),
            "options_buying_power": float(bp.options_buying_power or 0),
            "total_value": total_val,
            "today_pnl": None,
            "positions_count": len(positions),
            "orders": orders[:50],
        }
    except Exception as e:
        log.error("fetch_account_data error: %s", e)
        return _simulate_account_data()


def _simulate_account_data() -> dict:
    """Simulate account data when API is not available."""
    return {
        "cash": 10000.0,
        "buying_power": 10000.0,
        "options_buying_power": 10000.0,
        "total_value": 10000.0,
        "today_pnl": 0.0,
        "positions_count": 0,
        "orders": [],
    }


async def fetch_account_balance() -> dict:
    """
    Fetch the current account balance from Public.com via get_portfolio().

    Always uses the real API when credentials are available — reading
    the balance is a safe read-only operation regardless of dry_run.
    Falls back to simulation only when no real API key is configured.
    """
    if not API_KEY or API_KEY == "YOUR_PUBLIC_API_KEY_HERE":
        return _simulate_balance()

    try:
        client = await _get_client()
        portfolio = await client.get_portfolio()
        bp = portfolio.buying_power
        equity = portfolio.equity or []
        cash_equity = next((e for e in equity if str(e.type).endswith("CASH")), None)
        cash_val = float(cash_equity.value) if cash_equity else 0.0
        total_val = sum(float(e.value) for e in equity)
        return {
            "cash": cash_val,
            "buying_power": float(bp.buying_power or bp.cash_only_buying_power or 0),
            "options_buying_power": float(bp.options_buying_power or 0),
            "total_value": total_val,
        }
    except Exception as e:
        log.error("fetch_account_balance error: %s", e)
        return _simulate_balance()


_sim_balance = {"cash": 10000.00, "buying_power": 10000.00, "total_value": 10000.00}


def _simulate_balance() -> dict:
    """Return a simulated balance that drifts with simulated positions."""
    import random
    drift = random.gauss(0, 15)
    _sim_balance["total_value"] = round(max(100, _sim_balance["total_value"] + drift), 2)
    pos_value = sum(_sim_cache.values()) * 100 if _sim_cache else 0
    _sim_balance["cash"] = round(max(0, _sim_balance["total_value"] - pos_value), 2)
    _sim_balance["buying_power"] = _sim_balance["cash"]
    return dict(_sim_balance)


# ── Dry-run simulation ────────────────────────────────────────────────────────

import random

_sim_cache: dict[str, float] = {}

def _simulate_prices(osi_symbols: list[str]) -> dict[str, float]:
    """
    Walk existing simulated prices with slight upward drift.
    Seeds initial prices from the strike embedded in the OSI symbol.
    """
    result = {}
    for osi in osi_symbols:
        if osi not in _sim_cache:
            # Seed: extract strike from OSI (last 8 chars / 1000)
            try:
                strike = int(osi[-8:]) / 1000
                _sim_cache[osi] = round(max(0.05, strike * 0.0025), 2)
            except (ValueError, IndexError):
                _sim_cache[osi] = 2.00

        # ±5% random walk, slight upward bias
        delta = random.gauss(0.001, 0.025)
        new_price = max(0.05, _sim_cache[osi] * (1 + delta))
        _sim_cache[osi] = round(new_price, 2)
        result[osi] = _sim_cache[osi]

    return result


async def fetch_positions() -> list[dict]:
    """Normalized open positions for the reconciler. avg_cost is per share.

    Public's SDK has no get_positions(); they ride on the portfolio payload,
    same source reconciler._build_broker_position_map already reads.
    """
    if _DRY_RUN() or not API_KEY:
        return []
    client = await _get_client()
    portfolio = await client.get_portfolio()
    out: list[dict] = []
    for pos in (portfolio.positions or []):
        try:
            qty = float(pos.quantity) if pos.quantity else 0.0
            if qty == 0:
                continue
            unit_cost = pos.cost_basis.unit_cost if pos.cost_basis else None
            last_px = pos.last_price.last_price if pos.last_price else None
            out.append({
                "osi_symbol":    (pos.instrument.symbol if pos.instrument else "") or "",
                "qty":           qty,
                "avg_cost":      float(unit_cost) if unit_cost is not None else 0.0,
                "current_price": float(last_px) if last_px is not None else 0.0,
                "sec_type":      "OPT",
            })
        except Exception as e:
            log.warning("public position parse: %s", e)
    return out
