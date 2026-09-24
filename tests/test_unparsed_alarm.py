"""
The unparsed-alert alarm must page on lost orders and stay silent otherwise.

An alarm that fires on every "Good Morning Snipers" gets muted within a week,
and a muted alarm is worse than none — so the negative cases below matter more
than the positive ones. Sample texts are real, taken from the 2yr dumps.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import app.ingest.unparsed_alarm as UA
from app.ingest.unparsed_alarm import classify_unparsed

MARK = "‼️"

# Order-shaped and dropped → these cost money silently. Must page.
SHOULD_ALARM = [
    f"**SOLD 2/3 880C 8/7 5.05**{MARK} - Leaving 1 more. Wil SL at 3.5 @everyone",
    "**SOLD 1/4 500C 4/24 5.3** - last 25 left. Will hold till 6.1 and sell 20.",
    "**ALL OUT 460C 1/24 7.45** - this was just 2 contracts. Taking off.",
    f"{MARK} **SOLD 3/5 165C 2/14 2.49** @everyone",
    "**SOLD 1/4 AMZN 272.5C 5/39 1.75** - last 25.",          # analyst typo'd the date
]

# The parser refuses these on purpose, or they were never orders. Must be silent.
SHOULD_BE_SILENT = [
    "Good Morning Snipers, Market flat with DIA IWM stronger pre market.",
    "Good Morning Snipers, another lotto Thursday. Market gapping up 4/16.",
    "**SOLD cash secured PUTS ASML 1500P 7/17 7.5** @everyone",
    "**SOLD covered calls CRWV 100C 7/17 1.25** @everyone",
    "**SOLD covered call AAPL 290C 5/29 1.5**",
    "**SOLD naked put AAPL 280P 5/29 4.20**",
    "**ALL OUT ARM shares 300** - someone early exercised these.",
    "**SOLD 1/2 UNH shares 364.84** - we up almost 100 points.",
    "**BOUGHT USO 100P 7/17 1.75** - roll up. Small @everyone",
    "#alert bought 7410 fly 30w 7/21 $2.85 SMALL",
    "hey guys how is the market today?",
    "No more trades for me Snipers. Swinging 1/4 ORCL. Lotto size on MU.",
    "watching SPX for a setup",
    "",
]

PASS = 0; FAIL = 0
def chk(label, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  [PASS] {label}")
    else: FAIL += 1; print(f"  [FAIL] {label}" + (f" — {detail}" if detail else ""))

print("\n── pages on order-shaped drops ───────────────────────────────────────")
for t in SHOULD_ALARM:
    chk(f"alarm: {t[:56]}", bool(classify_unparsed(t)), "returned silent")

print("\n── silent on intended refusals and chatter ───────────────────────────")
for t in SHOULD_BE_SILENT:
    r = classify_unparsed(t)
    chk(f"silent: {(t or '(empty)')[:56]}", not r, f"would page: {r!r}")

print("\n── throttle ──────────────────────────────────────────────────────────")
UA._recent.clear(); UA._seen.clear()
now = time.time()
chk("first send passes", not UA._throttled("SOLD 2/3 880C 8/7 5.05", now))
chk("identical repeat suppressed", UA._throttled("SOLD 2/3 880C 8/7 5.05", now))

UA._recent.clear(); UA._seen.clear()
for i in range(UA._MAX_PER_HOUR):
    UA._throttled(f"distinct message {i}", now)
chk("hourly cap holds", UA._throttled("one over the cap", now))
chk("cap expires after an hour", not UA._throttled("later message", now + 3601))

total = PASS + FAIL
print(f"\nunparsed alarm: {PASS}/{total} passed  {'OK' if FAIL == 0 else 'FAIL'}")
# Module-level so `pytest tests/` fails too, not just `python tests/<this>.py`.
if FAIL:
    raise AssertionError(f"{FAIL}/{total} unparsed-alarm checks failed")
if __name__ == "__main__":
    sys.exit(0)
