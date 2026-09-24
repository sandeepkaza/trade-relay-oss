"""
Conservative stale-mark valuation in the daily-loss cap (GH #11 residual).

Default (haircut=0) keeps legacy behavior: an un-marked position contributes $0.
With a haircut, an un-marked position is valued as a loss so it can't hide a
bleeding blind position from the daily-loss halt.
"""
from datetime import datetime, timezone

import app.core.db as db
import app.risk.guardrails as g
from app.core.models import Position


def _seed_unmarked_position(remaining=2, avg=2.0):
    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()
    s = db.SessionLocal()
    try:
        s.add(Position(osi_symbol="SPXW260101C06000000", symbol="SPX", status="OPEN",
                       total_contracts=remaining, remaining=remaining, avg_price=avg,
                       current_price=None, open_time=datetime.now(timezone.utc)))
        s.commit()
    finally:
        s.close()


def test_default_haircut_zero_values_unmarked_at_zero(monkeypatch):
    _seed_unmarked_position()
    monkeypatch.setattr(g, "_STALE_MARK_HAIRCUT_PCT", lambda: 0.0)
    s = db.SessionLocal()
    try:
        assert g._calculate_daily_pnl(s) == 0.0
    finally:
        s.close()


def test_haircut_values_unmarked_as_loss(monkeypatch):
    # 2 contracts, avg $2.00, 50% haircut → assumed $1.00 → (1-2)*2*100 = -$200.
    _seed_unmarked_position(remaining=2, avg=2.0)
    monkeypatch.setattr(g, "_STALE_MARK_HAIRCUT_PCT", lambda: 50.0)
    s = db.SessionLocal()
    try:
        assert g._calculate_daily_pnl(s) == -200.0
    finally:
        s.close()


def test_haircut_pushes_cap_to_halt(monkeypatch):
    """With a haircut, a blind position can trip the daily-loss halt where $0
    valuation would have let trading continue."""
    _seed_unmarked_position(remaining=2, avg=2.0)
    monkeypatch.setattr(g, "_STALE_MARK_HAIRCUT_PCT", lambda: 50.0)   # -$200 assumed
    monkeypatch.setattr(g, "_MAX_DAILY_LOSS", lambda: 150.0)          # limit -$150
    s = db.SessionLocal()
    try:
        pnl = g._calculate_daily_pnl(s)
        assert pnl <= -g._MAX_DAILY_LOSS()   # -200 <= -150 → would halt
    finally:
        s.close()
