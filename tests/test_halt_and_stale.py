"""
Restart-safe daily halt (GH #10) + stale-mark BUY block (GH #11).
"""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import app.core.db as db
import app.risk.guardrails as g
from app.core.models import Position, get_system_state


def _fresh_db():
    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()


def _today_et():
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")


def _reset_halt_cache():
    g._halt_loaded = False
    g._trading_halted = False
    g._halt_date = None


# ── #10 halt persistence ──────────────────────────────────────────────────

def test_halt_persisted_and_restored_after_restart():
    _fresh_db()
    _reset_halt_cache()
    # Simulate a halt set earlier today, then a process restart (cold cache).
    g._persist_halt(_today_et())
    _reset_halt_cache()                 # "new process" — globals back to default

    g._ensure_halt_loaded()
    assert g._trading_halted is True
    assert g._halt_date == _today_et()


def test_stale_halt_from_previous_day_is_cleared_on_load():
    _fresh_db()
    _reset_halt_cache()
    g._persist_halt("2020-01-01")       # an old halt
    _reset_halt_cache()

    g._ensure_halt_loaded()
    assert g._trading_halted is False
    s = db.SessionLocal()
    try:
        assert get_system_state(s, g._HALT_KEY) is None   # row deleted
    finally:
        s.close()


def test_clear_halt_removes_persisted_row():
    _fresh_db()
    g._persist_halt(_today_et())
    g._persist_halt(None)
    s = db.SessionLocal()
    try:
        assert get_system_state(s, g._HALT_KEY) is None
    finally:
        s.close()


# ── #11 stale-mark BUY block ──────────────────────────────────────────────

def _add_position(osi, avg, cur, open_age_s):
    s = db.SessionLocal()
    try:
        s.add(Position(
            osi_symbol=osi, symbol="SPX", status="OPEN",
            total_contracts=1, remaining=1, avg_price=avg, current_price=cur,
            open_time=datetime.now(timezone.utc) - timedelta(seconds=open_age_s),
        ))
        s.commit()
    finally:
        s.close()


def test_unpriced_position_past_grace_blocks_buys():
    _fresh_db()
    _add_position("SPXW260101C06000000", avg=1.5, cur=None, open_age_s=600)
    blocked, reason = g._check_stale_marks()
    assert blocked and "STALE_MARKS" in reason


def test_fresh_unpriced_open_within_grace_is_allowed():
    _fresh_db()
    _add_position("SPXW260101C06000000", avg=1.5, cur=None, open_age_s=5)
    blocked, _ = g._check_stale_marks()
    assert blocked is False


def test_priced_position_is_not_stale():
    _fresh_db()
    _add_position("SPXW260101C06000000", avg=1.5, cur=1.7, open_age_s=600)
    blocked, _ = g._check_stale_marks()
    assert blocked is False


def test_flag_off_disables_block(monkeypatch):
    _fresh_db()
    _add_position("SPXW260101C06000000", avg=1.5, cur=None, open_age_s=600)
    monkeypatch.setattr(g, "_BLOCK_BUYS_ON_STALE_MARKS", lambda: False)
    blocked, _ = g._check_stale_marks()
    assert blocked is False
