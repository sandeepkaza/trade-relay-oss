"""
DB hardening: pragmas (busy_timeout / wal_autocheckpoint), composite perf
indexes, and opt-in retention prune.
"""
import os
import sqlite3
from datetime import datetime, timedelta, timezone

from sqlalchemy import inspect, text

import app.core.db as db
from app.core.models import Alert, DiscordSignal, Position, Order, OrderAudit


def _fresh_db():
    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()


def test_sqlite_pragmas_applied():
    _fresh_db()
    with db.engine.connect() as c:
        assert c.execute(text("PRAGMA busy_timeout")).scalar() == 5000
        assert c.execute(text("PRAGMA wal_autocheckpoint")).scalar() == 400
        assert c.execute(text("PRAGMA journal_mode")).scalar().lower() == "wal"


def test_perf_indexes_created_and_idempotent():
    _fresh_db()
    names = {ix["name"] for ix in inspect(db.engine).get_indexes("orders")} | \
            {ix["name"] for ix in inspect(db.engine).get_indexes("positions")}
    assert "ix_orders_side_status_placed" in names
    assert "ix_positions_status_remaining" in names
    db._ensure_perf_indexes()   # second run must not raise (IF NOT EXISTS)


def test_retention_disabled_is_noop():
    _fresh_db()
    s = db.SessionLocal()
    try:
        s.add(Alert(action="BUY", status="PENDING",
                    timestamp=datetime.now(timezone.utc) - timedelta(days=999)))
        s.commit()
    finally:
        s.close()
    assert db.prune_old_records(0) == {}
    s = db.SessionLocal()
    try:
        assert s.query(Alert).count() == 1   # untouched
    finally:
        s.close()


def test_retention_prunes_old_alerts_keeps_recent_and_trades():
    _fresh_db()
    old = datetime.now(timezone.utc) - timedelta(days=40)
    new = datetime.now(timezone.utc) - timedelta(days=1)
    s = db.SessionLocal()
    try:
        s.add(Alert(action="BUY", status="PENDING", timestamp=old))
        s.add(Alert(action="BUY", status="PENDING", timestamp=new))
        s.add(DiscordSignal(author="x", discord_message_id="m1", content_hash="h1", timestamp=old))
        # Trade history must survive regardless of age.
        s.add(Position(osi_symbol="SPXW260101C06000000", status="CLOSED",
                       total_contracts=1, remaining=0, avg_price=1.0,
                       open_time=old, close_time=old))
        s.add(Order(public_order_id="o1", side="BUY", status="FILLED",
                    placed_at=old))
        s.commit()
    finally:
        s.close()

    deleted = db.prune_old_records(30)
    assert deleted.get("alerts") == 1
    assert deleted.get("discord_signals") == 1

    s = db.SessionLocal()
    try:
        assert s.query(Alert).count() == 1            # recent kept
        assert s.query(DiscordSignal).count() == 0
        assert s.query(Position).count() == 1         # trade history kept
        assert s.query(Order).count() == 1
    finally:
        s.close()


def _wipe_archive():
    p = db.archive_db_path()
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(p + suffix)
        except (OSError, TypeError):
            pass
    return p


def test_archive_disabled_is_noop():
    _fresh_db()
    _wipe_archive()
    assert db.archive_old_trades(0) == {}
    assert db.archive_old_trades(-1) == {}
    assert db.purge_archive(0) == {}


def test_archive_moves_old_terminal_only():
    _fresh_db()
    apath = _wipe_archive()
    old = datetime.now(timezone.utc) - timedelta(days=400)   # past 365d cutoff
    recent = datetime.now(timezone.utc) - timedelta(days=30)  # within 365d
    s = db.SessionLocal()
    try:
        # Should ARCHIVE: terminal + old.
        s.add(Position(osi_symbol="SPXW200101C01000000", status="CLOSED",
                       total_contracts=1, remaining=0, avg_price=1.0,
                       open_time=old, close_time=old))
        s.add(Order(public_order_id="old-filled", side="BUY", status="FILLED",
                    placed_at=old))
        s.add(OrderAudit(action="BUY", osi_symbol="X", verdict="ALLOWED", ts=old))
        # Should STAY: still-open position, even though it's ancient.
        s.add(Position(osi_symbol="SPXW200101C02000000", status="OPEN",
                       total_contracts=1, remaining=1, avg_price=1.0,
                       open_time=old, close_time=None))
        # Should STAY: pending order, ancient but not terminal.
        s.add(Order(public_order_id="old-pending", side="BUY", status="PENDING",
                    placed_at=old))
        # Should STAY: closed but recent.
        s.add(Position(osi_symbol="SPXW250101C03000000", status="CLOSED",
                       total_contracts=1, remaining=0, avg_price=1.0,
                       open_time=recent, close_time=recent))
        s.commit()
    finally:
        s.close()

    moved = db.archive_old_trades(365)
    assert moved == {"positions": 1, "orders": 1, "order_audit": 1}

    s = db.SessionLocal()
    try:
        # Live DB keeps open + pending + recent-closed.
        assert s.query(Position).count() == 2
        assert {p.status for p in s.query(Position).all()} == {"OPEN", "CLOSED"}
        assert s.query(Order).count() == 1
        assert s.query(Order).first().status == "PENDING"
        assert s.query(OrderAudit).count() == 0
    finally:
        s.close()

    # Cold store holds exactly the moved rows and is queryable.
    a = sqlite3.connect(apath)
    try:
        assert a.execute("SELECT count(*) FROM positions").fetchone()[0] == 1
        assert a.execute("SELECT osi_symbol FROM positions").fetchone()[0] == \
            "SPXW200101C01000000"
        assert a.execute("SELECT count(*) FROM orders").fetchone()[0] == 1
        assert a.execute("SELECT count(*) FROM order_audit").fetchone()[0] == 1
    finally:
        a.close()

    # Idempotent: a second run with nothing newly aged moves zero rows.
    assert db.archive_old_trades(365) == {"positions": 0, "orders": 0, "order_audit": 0}
    _wipe_archive()


def test_purge_archive_drops_only_beyond_retention():
    """archive.db keeps `retention_years` of history, then purges. Move uses a
    short window; purge uses the multi-year bound on the cold store only."""
    _fresh_db()
    apath = _wipe_archive()
    ancient = datetime.now(timezone.utc) - timedelta(days=365 * 6)  # past 5y
    midage = datetime.now(timezone.utc) - timedelta(days=400)       # >1y, <5y
    s = db.SessionLocal()
    try:
        s.add(Order(public_order_id="ancient", side="BUY", status="FILLED",
                    placed_at=ancient))
        s.add(Order(public_order_id="midage", side="BUY", status="FILLED",
                    placed_at=midage))
        s.commit()
    finally:
        s.close()

    # Both are >365d old → both move to archive.
    assert db.archive_old_trades(365)["orders"] == 2

    # Purge keeps the mid-age row, drops the 6y-old one.
    purged = db.purge_archive(5)
    assert purged["orders"] == 1
    a = sqlite3.connect(apath)
    try:
        rows = a.execute("SELECT public_order_id FROM orders").fetchall()
        assert [r[0] for r in rows] == ["midage"]
    finally:
        a.close()
    _wipe_archive()
