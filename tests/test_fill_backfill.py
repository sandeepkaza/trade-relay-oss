"""Recovering unrecorded SELL fills from Public transaction history.

Covers the two things that can silently corrupt P&L: deriving the executed
price from a transaction, and deciding how many contracts are still missing
locally. Both are pure functions of the data, so they test without a broker.
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.monitors.reconciler import _fill_price_from_tx


class Tx:
    """Stand-in for a public_api_sdk history transaction."""
    def __init__(self, quantity=None, principal_amount=None, description=""):
        self.quantity = quantity
        self.principal_amount = principal_amount
        self.description = description


def missing_qty(broker_qty, local_qty):
    """Mirror of the backfill's accounting: what still needs recording."""
    return max(0, broker_qty - local_qty)


def test_price_from_principal():
    # The real 2026-07-27 QQQ fill: SELL 2 @ 2.91, principal 582.00.
    assert _fill_price_from_tx(Tx(-2, 582.00, "SELL 2 QQQ260727C00680000 at 2.91")) == 2.91
    # Both buys from the same position.
    assert _fill_price_from_tx(Tx(1, -269.00, "BUY 1 QQQ260727C00680000 at 2.69")) == 2.69
    assert _fill_price_from_tx(Tx(1, -171.00, "BUY 1 QQQ260727C00680000 at 1.71")) == 1.71
    # Sign of quantity/principal must not leak into the price.
    assert _fill_price_from_tx(Tx(-2, -582.00, "")) == 2.91
    # Sub-penny fills round to 4dp, not to the nearest cent.
    assert _fill_price_from_tx(Tx(-1, 45.01, "")) == 0.4501


def test_price_falls_back_to_description():
    # principal missing -> parse the description rather than return nothing
    assert _fill_price_from_tx(Tx(-2, None, "SELL 2 SPXW260716P07530000 at 3.60")) == 3.60
    assert _fill_price_from_tx(Tx(None, None, "SELL 1 IWM260618P00285000 at 0.05")) == 0.05


def test_price_returns_none_when_underivable():
    # No amount and no parseable description -> None, so the caller skips the
    # transaction instead of writing a zero into realized P&L.
    assert _fill_price_from_tx(Tx(-2, None, "SELL 2 FOO at market")) is None
    assert _fill_price_from_tx(Tx(0, 0, "")) is None
    assert _fill_price_from_tx(Tx(None, None, "")) is None


def test_price_never_zero_or_negative():
    for tx in (Tx(-2, 582.00, ""), Tx(-1, 45.01, ""), Tx(-2, None, "SELL 2 X at 3.60")):
        px = _fill_price_from_tx(tx)
        assert px is not None and px > 0


def test_missing_quantity_accounting():
    # nothing recorded locally -> recover the whole broker quantity
    assert missing_qty(2, 0) == 2
    # already fully recorded -> do nothing (the double-count guard)
    assert missing_qty(2, 2) == 0
    # partial sell recorded -> recover only the remainder
    assert missing_qty(3, 1) == 2
    # local ahead of broker (stale/duplicate row) -> never negative
    assert missing_qty(1, 2) == 0
    assert missing_qty(0, 2) == 0


def test_pnl_matches_broker_for_the_qqq_case():
    entry = 2.20                      # (2.69 + 1.71) / 2
    px = _fill_price_from_tx(Tx(-2, 582.00, "SELL 2 QQQ260727C00680000 at 2.91"))
    assert round((px - entry) * 2 * 100, 2) == 142.00
    # and the broker's own arithmetic agrees
    assert round(582.00 - 269.00 - 171.00, 2) == 142.00


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok — {name}")
    print("all fill-backfill checks passed")
