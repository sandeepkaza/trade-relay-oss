"""
broker_base.py — Vendor-neutral broker / quote-source Protocols.

Type-only. No runtime behaviour. Lets call sites pass either a
PublicExecutor or an IBKRExecutor interchangeably once routed through
broker_router.get_executor().

Default broker stays Public.com — flipping cfg[trading].broker to "ibkr"
swaps the implementation at the factory level without touching call sites.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Broker(Protocol):
    """Order-placement surface every executor must provide.

    PublicExecutor and IBKRExecutor both satisfy this. Methods mirror the
    existing PublicExecutor public API so call-site swaps are import-only.
    """

    name: str  # "public" | "ibkr"

    async def execute(
        self,
        alert: Any,
        alert_db_id: int,
        alert_author: str = "",
        content_hash: str = "",
        _pipeline_timer: Any = None,
    ) -> Any: ...


@runtime_checkable
class QuoteSource(Protocol):
    """Read-only market-data + account surface every vendor must provide."""

    async def fetch_prices(self, osi_symbols: list[str]) -> dict[str, float]: ...
    async def fetch_orders(self) -> list[dict]: ...
    async def fetch_positions(self) -> list[dict]: ...
    """[{osi_symbol, qty, avg_cost, current_price, sec_type}].
    avg_cost is ALWAYS per share — IBKR's per-contract avgCost is divided
    by the multiplier inside its bridge, not here."""
    async def fetch_account_data(self) -> dict: ...
    async def fetch_account_balance(self) -> dict: ...
    def get_last_spread_pct(self, osi_symbol: str) -> float: ...
