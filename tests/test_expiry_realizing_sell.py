"""
Expired positions must write a realizing SELL order.

/api/calendar, the stats tab and analyst attribution all define a trade as a
FILLED SELL order. Before this, _close_if_expired marked the Position CLOSED
without one, so an expiry was invisible to every P&L view — last week that hid
a real -$1,309 (SPXW260731P07420000, 13.11 -> 0.02).

Run: python -m pytest tests/test_expiry_realizing_sell.py -q
"""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from app.core.db import SessionLocal
from app.core.models import Order, Position
from app.monitors.position_monitor import _close_if_expired


def _mk(db, osi, avg, cur, qty=1, days_ago=1):
    # ET, not the runner's local date. _close_if_expired decides expiry in
    # Eastern ("expired" = before today ET, or today ET past 16:00), so a
    # local "yesterday" on any box east of ET is still *today* in ET and the
    # position is not expired until the close — which is exactly how this
    # passed on one self-hosted runner and failed on another.
    exp = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=days_ago)
    pos = Position(
        osi_symbol=osi, symbol=osi[:4], expiry=exp.isoformat(),
        strike=100.0, option_type="P", total_contracts=qty, remaining=qty,
        avg_price=avg, current_price=cur, status="OPEN",
        open_time=datetime.now(timezone.utc) - timedelta(days=days_ago + 1),
        author="spacemonkey",
    )
    db.add(pos)
    db.commit()
    db.refresh(pos)
    return pos


@pytest.fixture
def db():
    s = SessionLocal()
    yield s
    s.rollback()
    s.close()


def test_expiry_writes_realizing_sell(db):
    pos = _mk(db, "TSTA260101P00100000", avg=13.11, cur=0.02)
    assert _close_if_expired(db, pos) is True
    assert pos.status == "CLOSED" and pos.remaining == 0

    o = db.query(Order).filter(Order.osi_symbol == "TSTA260101P00100000",
                               Order.side == "SELL").one()
    assert o.status == "FILLED"
    assert o.trigger == "EXPIRED"
    assert o.filled_qty == 1
    assert o.fill_price == pytest.approx(0.02)
    assert o.cost_basis == pytest.approx(13.11)   # profit needs BOTH set
    assert o.filled_at is not None                 # calendar buckets on this
    assert o.public_order_id is None               # nothing was ever sent
    assert o.author == "spacemonkey"               # attribution survives
    # The number the calendar was missing.
    assert (o.fill_price - o.cost_basis) * o.filled_qty * 100 == pytest.approx(-1309.0)


def test_no_duplicate_when_real_sell_exists(db):
    """A position closed by a genuine fill must not get a second synthetic leg."""
    pos = _mk(db, "TSTB260101P00100000", avg=2.00, cur=3.00)
    db.add(Order(osi_symbol="TSTB260101P00100000", side="SELL", quantity=1,
                 filled_qty=1, fill_price=3.0, cost_basis=2.0, status="FILLED",
                 trigger="DISCORD", filled_at=datetime.now(timezone.utc)))
    db.commit()

    assert _close_if_expired(db, pos) is True
    assert db.query(Order).filter(Order.osi_symbol == "TSTB260101P00100000",
                                  Order.side == "SELL").count() == 1


def test_unexpired_position_untouched(db):
    pos = _mk(db, "TSTC260101P00100000", avg=1.0, cur=1.2, days_ago=-5)  # expires in future
    assert _close_if_expired(db, pos) is False
    assert pos.status == "OPEN"
    assert db.query(Order).filter(Order.osi_symbol == "TSTC260101P00100000").count() == 0


def test_expired_debit_fly_combo_auto_closes(db):
    """Combo OSI used to miss the sweeper (pipe-separated, expiry NULL), so a
    0DTE fly sat OPEN for days after OCC dropped the legs."""
    exp = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=1)
    yy, mm, dd = exp.year % 100, exp.month, exp.day
    body = f"SPXW{yy:02d}{mm:02d}{dd:02d}P07670000"
    osi = (f"{body}|SPXW{yy:02d}{mm:02d}{dd:02d}P07665000|"
           f"SPXW{yy:02d}{mm:02d}{dd:02d}P07675000")
    pos = _mk(db, osi, avg=1.05, cur=0.05)
    pos.expiry = None
    pos.is_credit = False
    pos.strategy_tag = "SPREAD"
    db.commit()
    assert _close_if_expired(db, pos) is True
    assert pos.status == "CLOSED" and pos.remaining == 0
    o = db.query(Order).filter(Order.osi_symbol == osi, Order.side == "SELL").one()
    assert o.trigger == "EXPIRED"
    assert o.fill_price == pytest.approx(0.05)
    assert (o.fill_price - o.cost_basis) * o.filled_qty * 100 == pytest.approx(-100.0)


@pytest.mark.asyncio
async def test_manual_exit_expired_fly_closes_locally_without_broker(db, monkeypatch):
    """Dashboard Exit on a settled 0DTE fly must not POST a Tradier close —
    that path 5xx's HTML and the UI throws JSON.parse."""
    from app.core import app as app_module

    placed = []

    class _FakeSE:
        async def close_open_position(self, *a, **k):
            placed.append(True)
            raise AssertionError("must not place on an expired combo")

    monkeypatch.setattr(
        "app.execution.broker_router.get_spread_executor", lambda _m: _FakeSE()
    )
    exp = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=1)
    yy, mm, dd = exp.year % 100, exp.month, exp.day
    osi = (f"SPXW{yy:02d}{mm:02d}{dd:02d}P07680000|"
           f"SPXW{yy:02d}{mm:02d}{dd:02d}P07675000|"
           f"SPXW{yy:02d}{mm:02d}{dd:02d}P07685000")
    pos = _mk(db, osi, avg=1.05, cur=0.05)
    pos.expiry = None
    pos.is_credit = False
    pos.strategy_tag = "SPREAD"
    db.commit()

    result = await app_module._fire_manual_exit(pos.id)
    assert placed == []
    assert result["status"] == "ok"
    assert "expired" in result["note"]
    db.expire_all()
    row = db.query(Position).filter(Position.id == pos.id).one()
    assert row.status == "CLOSED"
