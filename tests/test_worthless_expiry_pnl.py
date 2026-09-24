"""
A contract that expires worthless realizes at 0.0 — a real price, not a missing one.

_fill_profit tested `not o.fill_price`, and 0.0 is falsy, so every total loss was
classed "unpriceable legacy row" and skipped by both callers: the per-analyst
stats (/api/analysts) and the realized-P&L aggregates (/api/stats). On
relaybot-vm that hid 13 EXPIRED rows worth -$5,703 — the worst trades were
exactly the ones being dropped, so every analyst looked better than they were.

Run: python -m pytest tests/test_worthless_expiry_pnl.py -q
"""
from types import SimpleNamespace as NS

import pytest

from app.core.app import _fill_profit


def sell(fill_price, cost_basis, filled_qty=1, quantity=1):
    return NS(fill_price=fill_price, cost_basis=cost_basis,
              filled_qty=filled_qty, quantity=quantity)


def test_worthless_expiry_is_a_real_full_loss():
    # SPXW260618C07600000: 1 contract @ $8.31 -> expired at $0.00
    pnl, qty = _fill_profit(sell(0.0, 8.31))
    assert qty == 1
    assert pnl == pytest.approx(-831.0)   # was None -> silently dropped


def test_multi_contract_worthless_expiry():
    # SPXW260520P07420000: 8 contracts @ $3.46 -> $0.00, the single biggest hole
    pnl, _ = _fill_profit(sell(0.0, 3.46, filled_qty=8, quantity=8))
    assert pnl == pytest.approx(-2768.0)


def test_normal_exit_unchanged():
    # MU260805C00950000: filled at 0.7601 against a 4.27 basis
    pnl, _ = _fill_profit(sell(0.7601, 4.27))
    assert pnl == pytest.approx(-350.99)


def test_winner_unchanged():
    pnl, _ = _fill_profit(sell(2.50, 1.00, filled_qty=2, quantity=2))
    assert pnl == pytest.approx(300.0)


def test_credit_spread_winner_is_positive():
    """Sold the vertical at $2.05, bought it back at $1.40 → +$65, not -$65."""
    o = sell(1.40, 2.05)
    o.osi_symbol = "SPXW260910C07605000|SPXW260910C07610000"
    pnl, _ = _fill_profit(o)
    assert pnl == pytest.approx(65.0)


@pytest.mark.parametrize("o", [
    sell(None, 4.27),          # never filled — genuinely unpriceable
    sell(0.76, None),          # no cost basis — genuinely unpriceable
    sell(0.76, 4.27, filled_qty=0, quantity=0),   # nothing actually filled
])
def test_genuinely_unpriceable_rows_still_return_none(o):
    pnl, _ = _fill_profit(o)
    assert pnl is None
