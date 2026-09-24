"""
Reopened positions must belong to whoever opened the NEW cycle (2026-08-04).

Position.osi_symbol is UNIQUE, so re-buying a contract reopens the CLOSED row
in place. That branch reset price/peak/flags but not author or strategy_tag —
and both stamps are write-once ("only if unset"), so the previous cycle's
owner survived. Live case: NVDA 215C 8/5 opened+closed by Sniper Alerts, then
bought hours later by Bdorts, kept author='Sniper Alerts' — Sniper's blank
close_all would have swept Bdorts' position and the wrong exit profile would
resolve.

Runs on its own throwaway engine: the suite shares one module-level
SessionLocal, so seeding/---clearing the global one here would wipe fixtures
other test modules build at import time.
"""
import asyncio, os, sys, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from types import SimpleNamespace as NS
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.execution.public_executor import PublicExecutor
from app.core.db import Base
from app.core.models import Position, Order, Alert

OSI = "NVDA260805C00215000"

_TMP = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMP.close()
_engine = create_engine(f"sqlite:///{_TMP.name}")
_Session = sessionmaker(bind=_engine)
Base.metadata.create_all(_engine)


def _reopen_with(new_author, small_account=False):
    """Seed a CLOSED position owned by Sniper, then re-buy it as new_author."""
    db = _Session()
    try:
        db.query(Position).delete(); db.query(Order).delete(); db.query(Alert).delete()
        db.add(Position(
            osi_symbol=OSI, symbol="NVDA", expiry="2026-08-05", strike=215.0,
            option_type="C", total_contracts=1, remaining=0, avg_price=0.59,
            current_price=0.80, highest_price=0.83, status="CLOSED",
            author="Sniper Alerts", strategy_tag="SMALL_ACCT",
            close_price=0.80, close_time=datetime.now(timezone.utc),
        ))
        alert_row = Alert(action="BUY", osi_symbol=OSI, symbol="NVDA",
                          discord_message_id=f"m-{new_author}", status="PENDING")
        order = Order(side="BUY", osi_symbol=OSI, quantity=1,
                      status="PENDING", filled_qty=0)
        db.add_all([alert_row, order]); db.commit()

        alert = NS(osi_symbol=OSI, symbol="NVDA", expiry="2026-08-05", strike=215.0,
                   option_type="C", _alert_author=new_author,
                   small_account=small_account, strategy_tag=None)

        ex = PublicExecutor.__new__(PublicExecutor)          # skip broker __init__
        ex.ws_manager = NS(broadcast=lambda *a, **k: asyncio.sleep(0))
        asyncio.run(ex._finalize_buy_fill(db, order, alert, alert_row.id, 0.86, 1))

        pos = db.query(Position).filter_by(osi_symbol=OSI).first()
        return NS(author=pos.author, tag=pos.strategy_tag, status=pos.status,
                  remaining=pos.remaining, avg=pos.avg_price)
    finally:
        db.close()


def test_reopen_restamps_new_opener():
    r = _reopen_with("Bdorts")
    assert r.author == "Bdorts", f"stale owner survived: {r.author!r}"
    assert r.tag is None, f"stale strategy_tag survived: {r.tag!r}"


def test_reopen_resets_the_cycle():
    r = _reopen_with("Bdorts")
    assert r.status == "OPEN"
    assert r.remaining == 1
    assert abs(r.avg - 0.86) < 1e-6


def test_reopen_restamps_small_account_from_new_alert():
    assert _reopen_with("Bdorts", small_account=True).tag == "SMALL_ACCT"


def test_same_analyst_reopen_keeps_owner():
    assert _reopen_with("Sniper Alerts").author == "Sniper Alerts"


if __name__ == "__main__":
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn(); print(f"  [PASS] {name}")
            except AssertionError as exc:
                fails += 1; print(f"  [FAIL] {name} — {exc}")
    print(f"\nReopen ownership: {'OK' if not fails else f'{fails} FAILED'}")
    sys.exit(1 if fails else 0)
