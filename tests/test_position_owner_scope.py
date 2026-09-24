"""
Owner-scoped positions (2026-08-05 MU 950C incident).

Fluid opened MU260805C00950000 @ $4.27. Sniper — who was flat, his own entry
having been slippage-blocked and its cooldown retry abandoned because "a
position is already open" (Fluid's) — posted "ALL OUT MU 950C 8/5 0.9". The
SELL matched by OSI alone and closed Fluid's contract at $0.76: -$350.99.

Position.osi_symbol is UNIQUE, so the contract is the row identity and author
was only a label. With position_owner_scope=author a position belongs to the
analyst who opened it and only their alerts may act on it. Unowned (NULL
author) rows still match anyone so legacy rows never become unsellable.
position_owner_scope=osi restores the legacy behavior.
"""
import atexit, os, shutil, sys, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TEST_DIR = tempfile.mkdtemp(prefix="trader_ownerscope_test_")
os.environ["TRADER_DATABASE_URL"] = f"sqlite:///{os.path.join(_TEST_DIR, 'test.db')}"
atexit.register(lambda: shutil.rmtree(_TEST_DIR, ignore_errors=True))

from types import SimpleNamespace as NS
from datetime import datetime, timezone, timedelta
import app.execution.public_executor as pe
from app.execution.public_executor import PublicExecutor
from app.core.db import Base, engine, SessionLocal
from app.core.models import Position

Base.metadata.create_all(engine)

PASS = 0; FAIL = 0
def chk(label, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  [PASS] {label}")
    else: FAIL += 1; print(f"  [FAIL] {label}" + (f" — {detail}" if detail else ""))

MU = "MU260805C00950000"

db = SessionLocal()
db.query(Position).delete()
t0 = datetime.now(timezone.utc)
def mkpos(osi, author, i, symbol=None, strike=950.0):
    db.add(Position(osi_symbol=osi, symbol=symbol or osi[:2], expiry="2026-08-05",
                    strike=strike, option_type="C", total_contracts=1, remaining=1,
                    avg_price=4.27, current_price=0.76, status="OPEN", author=author,
                    open_time=t0 + timedelta(seconds=i)))
mkpos(MU, "Fluid Options Alerts", 1, symbol="MU")
mkpos("ZZZZ260805C00100000", None, 2, symbol="ZZZZ", strike=100.0)  # legacy/unowned
db.commit()

def sell(author, osi=MU, symbol=None, close_all=True):
    # A real "ALL OUT MU 950C" carries BOTH symbol and osi — a blank symbol
    # would take the (separately scoped) blank-symbol close_all branch.
    if symbol is None:
        symbol = "MU" if osi else ""
    return NS(close_all=close_all, symbol=symbol, osi_symbol=osi,
              expiry=None, strike=None, option_type=None, _alert_author=author)

def osis(rows):
    return sorted(p.osi_symbol for p in rows)

match = lambda a: PublicExecutor._match_open_positions(None, db, a)

print("\n── Owner-scoped SELL matching ────────────────────────────────────────")
pe._CLOSE_ALL_SCOPE = lambda: "author"
pe._POSITION_OWNER_SCOPE = lambda: "author"

a = sell("Sniper Alerts")
chk("the incident: Sniper's ALL OUT does NOT match Fluid's position",
    match(a) == [], osis(match(a)))
chk("...and reports who does own it", a._owner_scope_dropped == [(MU, "Fluid Options Alerts")],
    repr(getattr(a, "_owner_scope_dropped", None)))

chk("owner's own exit still fills", osis(match(sell("Fluid Options Alerts"))) == [MU])
chk("owner match is case-insensitive", osis(match(sell("fluid options alerts"))) == [MU])

chk("NULL-author position is sellable by anyone",
    osis(match(sell("Sniper Alerts", osi="ZZZZ260805C00100000"))) == ["ZZZZ260805C00100000"])
chk("un-attributed SELL never strands a position",
    osis(match(sell(""))) == [MU])

# Symbol-only match (no OSI parsed) goes down the other branch — scope it too.
chk("symbol-only SELL from a non-owner is also scoped out",
    match(sell("Sniper Alerts", osi="", symbol="MU")) == [])
chk("symbol-only SELL from the owner still matches",
    osis(match(sell("Fluid Options Alerts", osi="", symbol="MU"))) == [MU])

print("\n── Legacy rollback (position_owner_scope=osi) ────────────────────────")
pe._POSITION_OWNER_SCOPE = lambda: "osi"
chk("scope=osi restores the old cross-analyst behavior",
    osis(match(sell("Sniper Alerts"))) == [MU], osis(match(sell("Sniper Alerts"))))

pe._POSITION_OWNER_SCOPE = lambda: "author"

print("\n── Ownership helper ──────────────────────────────────────────────────")
chk("blank alert author matches anything", pe._pos_owned_by(NS(author="Fluid"), "") is True)
chk("blank position author matches anything", pe._pos_owned_by(NS(author=None), "Sniper") is True)
chk("different authors do not match", pe._pos_owned_by(NS(author="Fluid"), "Sniper") is False)
chk("whitespace tolerated", pe._pos_owned_by(NS(author=" Fluid "), "fluid") is True)

total = PASS + FAIL
print(f"\nposition owner scope: {PASS}/{total} passed  {'OK' if FAIL == 0 else 'FAIL'}")
# Module-level so `pytest tests/` fails too, not just `python tests/<this>.py`.
if FAIL:
    raise AssertionError(f"{FAIL}/{total} owner-scope checks failed")
if __name__ == "__main__":
    sys.exit(0)
