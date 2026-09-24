"""
A resubmitted alert must size like the original (2026-08-04).

The override/resubmit path rebuilds the alert from the DB row alone. Alert had
no qty column and _AlertProxy hardcoded `qty = None`, so the analyst's stated
contract count was unrecoverable: Sniper's "BOUGHT INTC 110C 8/7 0.66 - 2 on
small account" was resubmitted by hand and went in as a 1-lot. Persisting qty
(plus re-deriving small_account from raw_text) closes the loop.
"""
import os, sys, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.db import Base
from app.core.models import Alert
from app.ingest.parser import parse_alert, _RE_SMALL_ACCOUNT

_TMP = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMP.close()
_engine = create_engine(f"sqlite:///{_TMP.name}")
_Session = sessionmaker(bind=_engine)
Base.metadata.create_all(_engine)

INTC = "**BOUGHT INTC 110C 8/7 0.66**‼️- 2 on small account @everyone"


def _proxy_fields_for(raw_text, msg_id):
    """Parse -> persist (as the listener does) -> rebuild (as resubmit does)."""
    alert = parse_alert(raw_text)
    db = _Session()
    try:
        row = Alert(
            action=alert.action, osi_symbol=alert.osi_symbol, symbol=alert.symbol,
            raw_text=raw_text, size_tag=alert.size_tag, alert_price=None,
            discord_message_id=msg_id, status="SKIPPED",
            strategy_tag="SMALL_ACCT" if alert.small_account else None,
            qty=alert.qty,
        )
        db.add(row); db.commit(); db.refresh(row)
        # Mirrors _AlertProxy in app.resubmit_alert
        return {
            "qty": row.qty,
            "size_tag": row.size_tag or "",
            "small_account": bool(_RE_SMALL_ACCOUNT.search(row.raw_text or "")),
        }, alert
    finally:
        db.close()


def test_stated_count_survives_the_round_trip():
    proxy, alert = _proxy_fields_for(INTC, "m-intc")
    assert alert.qty == 2, f"parser lost the count: {alert.qty!r}"
    assert proxy["qty"] == 2, f"resubmit would size from tag, not message: {proxy['qty']!r}"


def test_small_account_flag_survives_without_its_own_column():
    proxy, _ = _proxy_fields_for(INTC, "m-intc-2")
    assert proxy["small_account"] is True


def test_alert_without_a_count_stays_none():
    proxy, alert = _proxy_fields_for(
        "**BOUGHT** MSFT 08/05 505C $1.36 [SMALL] @everyone", "m-msft")
    assert alert.qty is None
    assert proxy["qty"] is None, "must fall back to size_default, not invent a count"
    assert proxy["small_account"] is False


def test_qty_is_exposed_on_the_api_row():
    db = _Session()
    try:
        row = db.query(Alert).filter_by(discord_message_id="m-intc").first()
        assert row.to_dict()["qty"] == 2
    finally:
        db.close()


if __name__ == "__main__":
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn(); print(f"  [PASS] {name}")
            except AssertionError as exc:
                fails += 1; print(f"  [FAIL] {name} — {exc}")
    print(f"\nResubmit qty round-trip: {'OK' if not fails else f'{fails} FAILED'}")
    sys.exit(1 if fails else 0)
