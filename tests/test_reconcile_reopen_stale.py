"""
The reconciler must not resurrect a position we just closed (2026-08-04).

The broker's portfolio snapshot lags our own fills. Closing a local row was
already guarded (_MIN_CONSECUTIVE_MISSES) but REOPENING one was not: a single
stale snapshot was enough.

NVDA 215C 8/5, live: manual SELL filled 14:26:24 and closed the row; the
14:29:25 reconcile pass still saw the contract on the broker and REOPENed it;
an analyst ALL OUT at 14:32:32 then sold against that resurrected row and
booked a fill with no broker counterpart. Local ledger ended the day 2 buys /
3 sells on a contract the broker was flat on.
"""
import os, sys, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.db import Base
from app.core.models import Position
import app.monitors.reconciler as rec

_TMP = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMP.close()
_engine = create_engine(f"sqlite:///{_TMP.name}")
_Session = sessionmaker(bind=_engine)
Base.metadata.create_all(_engine)

OSI = "NVDA260805C00215000"


def _closed_row(seconds_ago):
    db = _Session()
    db.query(Position).delete()
    db.add(Position(
        osi_symbol=OSI, symbol="NVDA", expiry="2026-08-05", strike=215.0,
        option_type="C", total_contracts=1, remaining=0, avg_price=0.86,
        current_price=1.01, status="CLOSED", close_price=1.01,
        close_time=datetime.now(timezone.utc) - timedelta(seconds=seconds_ago),
    ))
    db.commit()
    row = db.query(Position).filter_by(osi_symbol=OSI).first()
    return db, row


def _would_reopen(row):
    """The guard as it runs inside reconcile_once's reopen branch."""
    closed_at = rec._aware(getattr(row, "close_time", None))
    if closed_at is None:
        return True
    age = (datetime.now(timezone.utc) - closed_at).total_seconds()
    return not (0 <= age < rec._REOPEN_MIN_CLOSED_AGE_S())


def test_row_closed_seconds_ago_is_not_resurrected():
    db, row = _closed_row(181)      # the real gap: sold 14:26:24, pass at 14:29:25
    try:
        assert _would_reopen(row) is False, "stale snapshot would invent a position"
    finally:
        db.close()


def test_row_closed_long_ago_still_reopens():
    db, row = _closed_row(3600)     # genuine re-buy hours later
    try:
        assert _would_reopen(row) is True
    finally:
        db.close()


def test_boundary_is_the_configured_window():
    window = rec._REOPEN_MIN_CLOSED_AGE_S()
    db, row = _closed_row(window + 5)
    try:
        assert _would_reopen(row) is True
    finally:
        db.close()
    db, row = _closed_row(window - 5)
    try:
        assert _would_reopen(row) is False
    finally:
        db.close()


def test_row_with_no_close_time_is_not_blocked():
    db, row = _closed_row(10)
    try:
        row.close_time = None
        assert _would_reopen(row) is True, "missing close_time must not wedge reconcile"
    finally:
        db.close()


def test_default_window_covers_the_observed_lag():
    assert rec._REOPEN_MIN_CLOSED_AGE_S() >= 181, "default must exceed the observed 181s lag"


if __name__ == "__main__":
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn(); print(f"  [PASS] {name}")
            except AssertionError as exc:
                fails += 1; print(f"  [FAIL] {name} — {exc}")
    print(f"\nReconcile reopen guard: {'OK' if not fails else f'{fails} FAILED'}")
    sys.exit(1 if fails else 0)
