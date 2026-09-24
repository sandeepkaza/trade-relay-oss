"""
Author-scoped blank-symbol close_all (2026-06-11 incident #2).

"sell everything" from analyst X must close ONLY positions X opened — never
another analyst's book. NULL-author (legacy/reconciled) positions are
"unowned" and never swept by a scoped close_all. close_all_scope=all restores
the legacy book-wide flatten.
"""
import atexit, os, shutil, sys, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TEST_DIR = tempfile.mkdtemp(prefix="trader_closeall_test_")
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

db = SessionLocal()
db.query(Position).delete()
t0 = datetime.now(timezone.utc)
def mkpos(osi, author, i):
    db.add(Position(osi_symbol=osi, symbol=osi[:4], expiry="2026-06-18", strike=100.0,
                    option_type="C", total_contracts=1, remaining=1, avg_price=1.0,
                    current_price=1.0, status="OPEN", author=author,
                    open_time=t0 + timedelta(seconds=i)))
mkpos("AAAA260618C00100000", "Sarang", 1)
mkpos("BBBB260618C00100000", "Matae", 2)
mkpos("CCCC260618C00100000", "Sarang", 3)
mkpos("DDDD260618C00100000", None, 4)        # legacy/unowned
db.commit()

def blank_close_all(author):
    return NS(close_all=True, symbol="", osi_symbol="", _alert_author=author)

def osis(rows):
    return sorted(p.osi_symbol for p in rows)

print("\n── Author-scoped close_all ───────────────────────────────────────────")

pe._CLOSE_ALL_SCOPE = lambda: "author"
r = PublicExecutor._match_open_positions(None, db, blank_close_all("Sarang"))
chk("Sarang close_all → only Sarang's 2 positions",
    osis(r) == ["AAAA260618C00100000", "CCCC260618C00100000"], osis(r))

r = PublicExecutor._match_open_positions(None, db, blank_close_all("Matae"))
chk("Matae close_all → only Matae's 1 position",
    osis(r) == ["BBBB260618C00100000"], osis(r))

r = PublicExecutor._match_open_positions(None, db, blank_close_all("TC"))
chk("TC (owns nothing) close_all → closes NOTHING", r == [], osis(r))

r = PublicExecutor._match_open_positions(None, db, blank_close_all(""))
chk("No-author close_all → closes NOTHING", r == [], osis(r))

r = PublicExecutor._match_open_positions(None, db, blank_close_all("sarang"))
chk("Author match is case-insensitive", osis(r) == ["AAAA260618C00100000", "CCCC260618C00100000"], osis(r))

r = PublicExecutor._match_open_positions(None, db, blank_close_all("Sarang"))
chk("NULL-author position never swept by scoped close_all",
    "DDDD260618C00100000" not in osis(r))

pe._CLOSE_ALL_SCOPE = lambda: "all"
r = PublicExecutor._match_open_positions(None, db, blank_close_all("Sarang"))
chk("scope=all → legacy book-wide flatten (all 4)", len(r) == 4, osis(r))

total = PASS + FAIL
print(f"\nclose_all scope: {PASS}/{total} passed  {'OK' if FAIL == 0 else 'FAIL'}")
if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
