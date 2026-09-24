"""
Sniper's ticker-less exits + inline small-account qty (2026-08-07 MU 880C).

  11:10 ET  BOUGHT MU 880C 8/7 2.85‼️3. SL is 2. Will hold on.
  11:28 ET  SOLD 2/3 880C 8/7 5.05‼️ - Leaving 1 more. Wil SL at 3.5

The entry filled; the 2/3 profit-take returned None from parse_alert and never
reached the order queue, because the exit names no ticker. The parser refuses
ticker-less lines by design (a bare "SOLD 1/2 SPX 7450C" once matched
ticker="SOLD"), so the symbol has to come from the analyst's own open book.

Replaying 40d of dumps: 1 ticker-less exit and 1 missed inline qty, both his,
both today. Over 2yr: 4 more ticker-less (3 of them exits).

Three behaviors covered here:
  1. "2.85‼️3" yields qty=3           (no "small account" phrase to key off)
  2. a ticker-less exit adopts the symbol of the ONE matching open position,
     and stays unresolved when zero or several match
  3. a malformed price returns None instead of raising through the handler
"""
import atexit, os, shutil, sys, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TEST_DIR = tempfile.mkdtemp(prefix="trader_tickerless_test_")
os.environ["TRADER_DATABASE_URL"] = f"sqlite:///{os.path.join(_TEST_DIR, 'test.db')}"
atexit.register(lambda: shutil.rmtree(_TEST_DIR, ignore_errors=True))

from datetime import date
import app.ingest.parser as P
from app.ingest.discord_listener import _resolve_missing_ticker
from app.core.db import Base, engine, SessionLocal
from app.core.models import Position

Base.metadata.create_all(engine)

PASS = 0; FAIL = 0
def chk(label, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  [PASS] {label}")
    else: FAIL += 1; print(f"  [FAIL] {label}" + (f" — {detail}" if detail else ""))

MARK = "‼️"
BUY  = f"**BOUGHT MU 880C 8/7 2.85**{MARK}3. SL is 2. Will hold on. @everyone"
SELL = f"**SOLD 2/3 880C 8/7 5.05**{MARK} - Leaving 1 more. Wil SL at 3.5 @everyone"

# The dump is replayed against a frozen date so an 8/7 strike is not "expired".
class _FrozenDate(date):
    @classmethod
    def today(cls):
        return cls(2026, 8, 7)
P.date = _FrozenDate

print("\n── 1. inline marker qty ──────────────────────────────────────────────")
a = P.parse_alert(BUY)
chk("entry still parses", a is not None and a.symbol == "MU" and a.strike == 880.0)
chk("price is 2.85, not 2.853", a is not None and str(a.price) == "2.85", str(a and a.price))
chk("qty=3 from '2.85‼️3'", a is not None and a.qty == 3, f"got {a and a.qty}")
chk("small_account still set", a is not None and a.small_account)
chk("'‼️10 on small' → qty=10",
    (P.parse_alert(f"**BOUGHT CRCL 130P 8/22 2.65**{MARK}10 on small @everyone") or
     type("x", (), {"qty": None})).qty == 10)
# The marker is not a qty prefix on its own — these must NOT pick up a count.
chk("'‼️ - Leaving 1 more' does not read as qty=1",
    (P.parse_alert(f"**SOLD 1/2 MU 880C 8/7 5.05**{MARK} - Leaving 1 more.") or
     type("x", (), {"qty": "?"})).qty is None)
chk("'‼️ **BOUGHT IWM ... 3 contracts' still reads 3",
    (P.parse_alert(f"{MARK} **BOUGHT IWM 248C 12/31 1.88** - 3 contracts.") or
     type("x", (), {"qty": None})).qty == 3)

print("\n── 2. ticker-less exit resolution ────────────────────────────────────")
chk("exit alone is unparseable", P.parse_alert(SELL) is None)

db = SessionLocal()
db.query(Position).delete()
db.add(Position(osi_symbol="MU260807C00880000", symbol="MU", expiry="2026-08-07",
                strike=880.0, option_type="C", total_contracts=3, remaining=3,
                avg_price=2.85, current_price=5.05, status="OPEN", author="Sniper"))
db.commit(); db.close()

patched = _resolve_missing_ticker(SELL, "Sniper")
chk("symbol injected", "MU 880C" in patched, repr(patched[:70]))
b = P.parse_alert(patched) if patched else None
chk("resolves to a SELL on MU", b is not None and b.action == "SELL" and b.symbol == "MU")
chk("fraction 2/3 survives", b is not None and round(float(b.fraction), 4) == 0.6667,
    str(b and b.fraction))
chk("price 5.05 survives", b is not None and str(b.price) == "5.05", str(b and b.price))
chk("osi matches the open row", b is not None and b.osi_symbol == "MU260807C00880000")

chk("another analyst's exit does not resolve", _resolve_missing_ticker(SELL, "Fluid") == "")

# Two analysts on the same contract → ambiguous only if both are ownable; the
# real guard is two DIFFERENT symbols at one strike/expiry.
db = SessionLocal()
db.add(Position(osi_symbol="AMD260807C00880000", symbol="AMD", expiry="2026-08-07",
                strike=880.0, option_type="C", total_contracts=1, remaining=1,
                avg_price=1.0, current_price=1.0, status="OPEN", author="Sniper"))
db.commit(); db.close()
chk("two symbols at same strike → refuses to guess",
    _resolve_missing_ticker(SELL, "Sniper") == "")

db = SessionLocal()
db.query(Position).delete(); db.commit(); db.close()
chk("flat book → no resolution", _resolve_missing_ticker(SELL, "Sniper") == "")
chk("a BUY is never resolved this way",
    _resolve_missing_ticker(f"**BOUGHT 880C 8/7 2.85**{MARK}3", "Sniper") == "")

print("\n── 3. malformed price does not raise ─────────────────────────────────")
for bad in ("**SOLD** SNDK 430C $8 1/4 @everyone SNDK 430C $8 1/4 position",
            "**SOLD** SPX 6630C $22 @everyone SPX 6630C $22 holding 2",
            "**SOLD** NVDA 190C $3/4 2.15 @everyone NVDA 190C $3/4 2.15",
            "#ALERT SOLD SPX 6930C $4 ALL OUT RUNNERS"):
    try:
        P.parse_alert(bad)
        chk(f"no raise: {bad[:38]}", True)
    except Exception as e:
        chk(f"no raise: {bad[:38]}", False, f"{type(e).__name__}: {e}")

print(f"\n{'='*70}\n  {PASS} passed, {FAIL} failed\n{'='*70}")
# Module-level so `pytest tests/` fails too, not just `python tests/<this>.py`.
if FAIL:
    raise AssertionError(f"{FAIL}/{PASS + FAIL} ticker-less exit checks failed")
if __name__ == "__main__":
    sys.exit(0)
