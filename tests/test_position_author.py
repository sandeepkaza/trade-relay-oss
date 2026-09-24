"""
Regression: Position.author must be stamped at entry (2026-06-11).

Position.author was declared + indexed but NEVER written, so every position
row had author=NULL. That silently disabled per-author exit profiles and made
author-scoped exits impossible (a blank-symbol close_all could not be limited
to the firing analyst's own book). _stamp_position_author fixes the write;
this locks in the rule, including first-opener preservation on scale-ins.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from types import SimpleNamespace as NS

from app.execution.public_executor import PublicExecutor

PASS = 0; FAIL = 0
def chk(label, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  [PASS] {label}")
    else:
        FAIL += 1; print(f"  [FAIL] {label}" + (f" — {detail}" if detail else ""))

def stamp(pos_author, alert_author):
    pos = NS(author=pos_author)
    alert = NS(_alert_author=alert_author)
    PublicExecutor._stamp_position_author(None, pos, alert)  # self unused
    return pos.author

print("\n── Position.author stamping ──────────────────────────────────────────")
chk("New position gets opening analyst", stamp(None, "Sarang") == "Sarang")
chk("Forwarded author preserved verbatim", stamp(None, "[FWD #analyst-tc] TC") == "[FWD #analyst-tc] TC")
chk("Scale-in preserves first opener", stamp("Sarang", "Matae") == "Sarang", "must not overwrite")
chk("Empty author stays None", stamp(None, "") is None)
chk("Whitespace author stays None", stamp(None, "   ") is None)
chk("Existing author kept when new alert blank", stamp("Sarang", "") == "Sarang")
chk("None pos is a no-op (no crash)", (PublicExecutor._stamp_position_author(None, None, NS(_alert_author="X")) is None))

total = PASS + FAIL
print(f"\nPosition.author: {PASS}/{total} passed  {'OK' if FAIL == 0 else 'FAIL'}")
if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
