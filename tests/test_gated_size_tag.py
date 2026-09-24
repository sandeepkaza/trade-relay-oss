"""
Gated risk tags (LOTTO / ROLL UP / ROLL DOWN) must reach check_rollup_block.

2026-08-04: Bdorts posted "**BOUGHT** NVDA 08/05 215C $0.85 [SMALL] ... LOTTO
FOR $AMD EARNINGS". rollup_block_enabled=true with an empty whitelist, so that
BUY should have been blocked — instead it filled at $0.8598. Two independent
bugs in the same step let it through:

  1. a bracket match short-circuited the free-text scan, so [SMALL] won and
     "LOTTO" was never looked for;
  2. the free-text scan itself returned the FIRST SIZE_MAP hit in dict order,
     and "small" precedes "lotto", so "very small ... Lotto." lost it too.

Every sample below is a real alert pulled from the production DB.
"""
import os, sys
from datetime import date, timedelta
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.ingest.parser import resolve_size_tag, parse_alert, GATED_TAGS
from app.risk import guardrails

PASS = 0; FAIL = 0
def chk(label, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  [PASS] {label}")
    else: FAIL += 1; print(f"  [FAIL] {label}" + (f" — {detail}" if detail else ""))


print("\n── Gated tag beats size bucket (real production alerts) ──────────────")

# (raw_text, expected_tag) — all previously resolved to the wrong tag.
CASES = [
    ("**BOUGHT** NVDA 08/05 215C $0.85 [SMALL] NVDA 08/05 215C $0.85 [SMALL] "
     "LOTTO FOR $AMD EARNINGS Bdorts", "LOTTO"),
    ("**BOUGHT** SPX 07/28 7440C $1.3 [SMALL] SPX 07/28 7440C $1.3 [SMALL] LOTTO", "LOTTO"),
    ("**BOUGHT** SPX 7/21 7510C $2.10 [SMALL] @everyone End of day lotto", "LOTTO"),
    ("**BOUGHT GOOGL 350P 7/17 1.98** - very small. 5 contracts. Lotto. @everyone", "LOTTO"),
    ("**BOUGHT USO 100P 7/17 1.75** - roll up. Small @everyone", "ROLL UP"),
    ("**BOUGHT MRVL 400C 6/18 7.22** - Small roll up. @everyone", "ROLL UP"),
    ("**BOUGHT AAOI 210C 6/5 1.53** - extremely small with profits. Roll up. "
     "Lotto swing. @everyone", "ROLL UP"),
]
for raw, expected in CASES:
    got = resolve_size_tag(raw)
    chk(f"{expected:<9} ← {raw[:52]}...", got == expected, f"got {got!r}")

print("\n── Plain size buckets still resolve normally ─────────────────────────")
UNCHANGED = [
    ("**BOUGHT** SPX 8/04 7735P $7.15 [ROLL UP] @everyone", "ROLL UP"),
    ("**BOUGHT** IWM 08/05 297C $0.80 [SMALL] Bdorts", "SMALL"),
    ("**BOUGHT** AAPL 08/05 200C $1.20 [MEDIUM]", "MEDIUM"),
    ("BOUGHT TSLA 300C 8/7 1.50 - large position here", "LARGE"),
    ("BOUGHT MSFT 505C 8/5 1.36 @everyone", ""),
]
for raw, expected in UNCHANGED:
    got = resolve_size_tag(raw)
    chk(f"{expected or '(none)':<9} ← {raw[:52]}...", got == expected, f"got {got!r}")

print("\n── End to end: parse_alert → check_rollup_block ──────────────────────")
# The real alert said 08/05, but this leg runs parse_alert, which correctly
# refuses an expiry already in the past — so a literal date turns this into a
# time bomb that took the whole suite (and every deploy behind it) down the day
# after it was written. Same text, expiry rolled forward instead. The samples
# above keep their original dates: they only exercise resolve_size_tag, which
# never looks at the date.
_exp = date.today() + timedelta(days=3)
_mmdd = f"{_exp.month:02d}/{_exp.day:02d}"
NVDA = (f"**BOUGHT** NVDA {_mmdd} 215C $0.85 [SMALL] NVDA {_mmdd} 215C $0.85 [SMALL] "
        "LOTTO FOR $AMD EARNINGS Bdorts")
a = parse_alert(NVDA)
chk("Alert still parses to the right contract",
    a is not None and a.osi_symbol == f"NVDA{_exp:%y%m%d}C00215000",
    f"got {a and a.osi_symbol!r}")
chk("size_tag is LOTTO", a.size_tag == "LOTTO", f"got {a.size_tag!r}")

_orig_enabled, _orig_wl = guardrails._ROLLUP_BLOCK_ENABLED, guardrails._ROLLUP_WHITELIST
try:
    guardrails._ROLLUP_BLOCK_ENABLED = lambda: True
    guardrails._ROLLUP_WHITELIST = lambda: set()
    ok, reason = guardrails.check_rollup_block("Bdorts", a.size_tag)
    chk("Guardrail now BLOCKS the trade that filled", ok is False, f"got ok={ok}")
    chk("Reason names the tag", "LOTTO" in reason, f"got {reason[:70]!r}")

    guardrails._ROLLUP_WHITELIST = lambda: {"bdorts"}
    ok, _ = guardrails.check_rollup_block("Bdorts", a.size_tag)
    chk("Whitelisted author still passes", ok is True)

    guardrails._ROLLUP_BLOCK_ENABLED = lambda: False
    guardrails._ROLLUP_WHITELIST = lambda: set()
    ok, _ = guardrails.check_rollup_block("Bdorts", a.size_tag)
    chk("Toggle off = no-op", ok is True)
finally:
    guardrails._ROLLUP_BLOCK_ENABLED, guardrails._ROLLUP_WHITELIST = _orig_enabled, _orig_wl

chk("GATED_TAGS is the guardrail's own set",
    set(GATED_TAGS) == {"ROLL UP", "ROLL DOWN", "LOTTO"})

total = PASS + FAIL
print(f"\nGated size tags: {PASS}/{total} passed  {'OK' if FAIL == 0 else 'FAIL'}")
if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
