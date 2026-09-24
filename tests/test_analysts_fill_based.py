"""
/api/analysts + /api/stats rewrite onto realized SELL fills (2026-08-03).

The old analysts endpoint matched BUY alerts to "the first CLOSED position
with that OSI" and priced every contract at position close_price — wrong trade
credited on re-traded strikes, multi-leg exits overstated. These tests prove
the new math: per-leg fill × cost_basis, attributed by Order.author, with no
Position row needed at all.
"""

import asyncio
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Point DB at an ephemeral file BEFORE db/models import.
_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp.close()
os.environ["TRADER_DATABASE_URL"] = f"sqlite:///{_tmp.name.replace(chr(92), '/')}"

import app.core.app as app_module  # noqa: E402
from app.core.db import init_db, get_db  # noqa: E402
from app.core.models import Order  # noqa: E402

init_db()

# Two-leg exit (PT1 then PT2) on one position, entered at 1.00:
#   leg 1: 1ct @ 2.00 → +$100      leg 2: 1ct @ 3.00 → +$200
# Old position-based math would have priced BOTH contracts at the last
# leg (3.00) → +$400. Correct realized total is +$300.
# Fills placed ~200 days back so a window-difference check can isolate them
# from anything else in the DB.
_T0 = datetime.now(timezone.utc) - timedelta(days=200)
AUTHOR = "test-analyst-xyz"

_db = next(get_db())
for i, (px, basis) in enumerate([(2.0, 1.0), (3.0, 1.0)]):
    _db.add(Order(
        osi_symbol="SPXW260731C07000000", side="SELL", status="FILLED",
        quantity=1, filled_qty=1, fill_price=px, cost_basis=basis,
        author=AUTHOR, trigger="PT1" if i == 0 else "PT2",
        placed_at=_T0 + timedelta(minutes=i), filled_at=_T0 + timedelta(minutes=i),
    ))
# Legacy row without cost_basis: must appear in the trade log but not the P&L.
_db.add(Order(
    osi_symbol="SPXW260731C07000000", side="SELL", status="FILLED",
    quantity=1, filled_qty=1, fill_price=5.0, cost_basis=None,
    author=AUTHOR, trigger="MANUAL",
    placed_at=_T0 + timedelta(minutes=5), filled_at=_T0 + timedelta(minutes=5),
))
_db.commit()
_db.close()


def _run(coro):
    return asyncio.run(coro)


def _me(days=210):
    r = _run(app_module.get_analysts(days=days))
    return next((a for a in r["analysts"] if a["author"] == AUTHOR), None)


def test_per_leg_pnl_not_last_leg_pricing():
    a = _me()
    assert a is not None
    assert a["total_pnl"] == 300.0          # not 400 (old last-leg math)
    assert a["trades"] == 2                 # priced fills only
    assert a["wins"] == 2 and a["losses"] == 0
    assert a["by_symbol"][0]["symbol"] == "SPX"  # SPXW root normalized


def test_legacy_row_logged_but_not_aggregated():
    a = _me()
    assert len(a["trade_log"]) == 3
    assert sum(1 for t in a["trade_log"] if t["pnl"] is None) == 1


def test_window_excludes_old_fills():
    assert _me(days=190) is None


def test_stats_agrees_with_fills():
    wide = _run(app_module.get_stats(days=210))
    narrow = _run(app_module.get_stats(days=190))
    assert round(wide["total_pnl"] - narrow["total_pnl"], 2) == 300.0
    assert wide["trades"] - narrow["trades"] == 2
    assert "by_weekday" in wide and "hold_buckets" in wide


def test_analyst_new_fields():
    a = _me()
    assert a["avg_win"] == 150.0
    assert a["avg_loss"] == 0.0
    assert a["expectancy"] == 150.0
    assert a["monthly"] and sum(m["pnl"] for m in a["monthly"]) == 300.0
