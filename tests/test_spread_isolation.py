"""A single-leg SELL must never touch a vertical credit spread.

matae posts in four other forwarded rooms besides his spreads channel, so his
exits reach this instance. Spread rows are stored with expiry=None, which makes
the single-leg matcher's expiry filter a no-op for an undated exit and leaves
symbol + strike + option_type — all three of which a spread row satisfies on its
SHORT leg. The match would send a one-legged SELL against a "short|long" combo
osi_symbol, which is not a tradeable contract.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.db import SessionLocal, init_db               # noqa: E402
from app.core.models import Position                         # noqa: E402
from app.execution.public_executor import PublicExecutor     # noqa: E402
from app.ingest.parser import TradeAlert                     # noqa: E402


@pytest.fixture
def db():
    # init_db, not create_all: an existing positions table predates is_credit
    # and only _ensure_legacy_columns adds it.
    init_db()
    s = SessionLocal()
    s.query(Position).filter(Position.symbol == "SPX").delete()
    s.commit()
    yield s
    s.query(Position).filter(Position.symbol == "SPX").delete()
    s.commit()
    s.close()


def _spread_row():
    return Position(
        osi_symbol="SPXW260811C07790000|SPXW260811C07795000",
        symbol="SPX", expiry=None, strike=7790.0, option_type="C",
        total_contracts=1, remaining=1, avg_price=0.75, current_price=0.75,
        status="OPEN", author="twi_matae", is_credit=True, spread_width=5.0,
    )


def _long_row():
    return Position(
        osi_symbol="SPXW260811C07790000",
        symbol="SPX", expiry=None, strike=7790.0, option_type="C",
        total_contracts=1, remaining=1, avg_price=3.60, current_price=3.60,
        status="OPEN", author="twi_matae", is_credit=False,
    )


def _undated_sell():
    a = TradeAlert(action="SELL", symbol="SPX", expiry=None, strike=7790.0,
                   option_type="C", price=None)
    a._alert_author = "twi_matae"
    return a


def test_an_undated_sell_does_not_match_a_spread(db):
    """Same author, same symbol, same strike, same right — and it must still
    not match, because closing a spread is a two-legged operation."""
    db.add(_spread_row())
    db.commit()
    ex = PublicExecutor.__new__(PublicExecutor)
    assert ex._match_open_positions(db, _undated_sell()) == []


def test_the_same_sell_still_matches_a_real_long_position(db):
    """The guard must be narrow: ordinary single-leg exits are untouched."""
    db.add(_long_row())
    db.commit()
    ex = PublicExecutor.__new__(PublicExecutor)
    rows = ex._match_open_positions(db, _undated_sell())
    assert [p.osi_symbol for p in rows] == ["SPXW260811C07790000"]


def test_a_spread_is_invisible_even_when_both_kinds_are_open(db):
    db.add(_spread_row())
    db.add(_long_row())
    db.commit()
    ex = PublicExecutor.__new__(PublicExecutor)
    rows = ex._match_open_positions(db, _undated_sell())
    assert [p.osi_symbol for p in rows] == ["SPXW260811C07790000"]


def test_an_author_scoped_close_all_skips_spreads(db):
    """His "ALL OUT" in another room must not sweep the spread book either."""
    db.add(_spread_row())
    db.commit()
    a = TradeAlert(action="SELL", symbol="", expiry=None, strike=None,
                   option_type="", price=None, close_all=True)
    a._alert_author = "twi_matae"
    ex = PublicExecutor.__new__(PublicExecutor)
    assert ex._match_open_positions(db, a) == []
