"""
DB-enforced alert idempotency (GH #8).

A fresh DB must enforce uniqueness on alerts.discord_message_id so a duplicate
delivery of the same Discord message cannot create two alert rows even under a
race. The defensive migration must skip (not crash) when a live DB already holds
duplicate message ids.

Behaviour-based (not index-name-based): fresh DBs get the unique index from
create_all (name `ix_...`); the live-DB migration path adds `ux_...`. Both must
reject a duplicate insert.
"""
import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError


def _fresh_db():
    """Rebuild the alerts schema on the conftest-provided engine. Reusing the
    shared db module keeps Base↔models bound (reloading the module would not)."""
    import app.core.db as db
    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()
    return db


def _has_unique_on_message_id(db) -> bool:
    return any(
        ix.get("unique") and ix.get("column_names") == ["discord_message_id"]
        for ix in inspect(db.engine).get_indexes("alerts")
    )


def test_fresh_db_enforces_unique_message_id():
    db = _fresh_db()
    assert _has_unique_on_message_id(db)


def test_duplicate_message_id_rejected_at_db():
    db = _fresh_db()
    from app.core.models import Alert
    s = db.SessionLocal()
    try:
        s.add(Alert(action="BUY", discord_message_id="msg-1", status="PENDING"))
        s.commit()
        s.add(Alert(action="BUY", discord_message_id="msg-1", status="PENDING"))
        with pytest.raises(IntegrityError):
            s.commit()
        s.rollback()
    finally:
        s.close()


def test_null_message_ids_are_not_deduped():
    """Embeds without an id (NULL) must remain insertable many times."""
    db = _fresh_db()
    from app.core.models import Alert
    s = db.SessionLocal()
    try:
        for _ in range(3):
            s.add(Alert(action="SELL", discord_message_id=None, status="PENDING"))
            s.commit()
        assert s.query(Alert).filter(Alert.discord_message_id.is_(None)).count() == 3
    finally:
        s.close()


def test_migration_skips_when_existing_duplicates():
    """A live DB with pre-existing duplicate ids must not crash init — the index
    is skipped and best-effort dedup stays."""
    db = _fresh_db()
    # Simulate the old live table: drop every index on the column, then seed dupes.
    with db.engine.begin() as c:
        for ix in inspect(db.engine).get_indexes("alerts"):
            if ix.get("column_names") == ["discord_message_id"]:
                c.execute(text(f'DROP INDEX IF EXISTS "{ix["name"]}"'))
        c.execute(text("INSERT INTO alerts (action, discord_message_id, status) VALUES ('BUY','dup','PENDING')"))
        c.execute(text("INSERT INTO alerts (action, discord_message_id, status) VALUES ('BUY','dup','PENDING')"))
    db._ensure_unique_message_id_index()        # must not raise
    assert not _has_unique_on_message_id(db)     # skipped due to dupes
