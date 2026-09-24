"""
Append-only order-decision audit log (provenance).
"""
import app.core.db as db
import app.risk.guardrails as g
from app.core.models import OrderAudit, record_order_decision


def _fresh():
    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()


def test_table_created_on_fresh_db():
    _fresh()
    from sqlalchemy import inspect
    assert "order_audit" in set(inspect(db.engine).get_table_names())


def test_record_order_decision_inserts_row():
    _fresh()
    s = db.SessionLocal()
    try:
        record_order_decision(s, action="BUY", osi_symbol="SPXW260101C06000000",
                              author="sarang", content_hash="h1", verdict="ALLOWED",
                              qty=1, price=1.75)
        row = s.query(OrderAudit).one()
        assert row.verdict == "ALLOWED"
        assert row.author == "sarang"
        assert row.qty == 1 and row.price == 1.75
    finally:
        s.close()


def test_audit_order_decision_writes_row():
    """The executor-side audit helper records the verdict."""
    _fresh()
    g.audit_order_decision(action="BUY", osi_symbol="SPXW260101C06000000",
                           author="x", content_hash="h2", allowed=False,
                           reason="BLOCKED SYMBOL: SPX is in blocked_symbols list",
                           qty=1, price=1.5)
    s = db.SessionLocal()
    try:
        row = s.query(OrderAudit).order_by(OrderAudit.id.desc()).first()
        assert row is not None
        assert row.verdict == "BLOCKED"
        assert "BLOCKED SYMBOL" in row.reason
        assert row.content_hash == "h2"
    finally:
        s.close()


def test_allowed_decision_has_empty_reason():
    _fresh()
    g.audit_order_decision(action="BUY", osi_symbol="X", author="y",
                           content_hash="h", allowed=True, reason="ignored")
    s = db.SessionLocal()
    try:
        row = s.query(OrderAudit).one()
        assert row.verdict == "ALLOWED" and row.reason == ""
    finally:
        s.close()


def test_audit_disabled_via_flag(monkeypatch):
    _fresh()
    import app.core.config_manager as cm
    monkeypatch.setattr(cm.cfg, "getboolean",
                        lambda sec, key, fallback=False: (
                            False if key == "audit_log_enabled" else fallback))
    g.audit_order_decision(action="BUY", osi_symbol="X", author="x",
                           content_hash="h", allowed=False, reason="r")
    s = db.SessionLocal()
    try:
        assert s.query(OrderAudit).count() == 0
    finally:
        s.close()


def test_retention_does_not_prune_audit_rows():
    """order_audit is trade provenance — kept long-term and ARCHIVED, never
    deleted by the short-retention prune (that tier is signal feeds only)."""
    from datetime import datetime, timezone, timedelta
    _fresh()
    s = db.SessionLocal()
    try:
        s.add(OrderAudit(action="BUY", osi_symbol="X", verdict="ALLOWED",
                         ts=datetime.now(timezone.utc) - timedelta(days=40)))
        s.add(OrderAudit(action="BUY", osi_symbol="Y", verdict="ALLOWED",
                         ts=datetime.now(timezone.utc)))
        s.commit()
    finally:
        s.close()
    deleted = db.prune_old_records(30)
    assert "order_audit" not in deleted
    s = db.SessionLocal()
    try:
        assert s.query(OrderAudit).count() == 2  # both kept
    finally:
        s.close()
