"""
Production-grade parser test suite.
Tests every known alert format including edge cases.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date

from app.ingest import parser as _parser
from app.ingest.parser import parse_alert

# Every sample below carries a literal expiry (4/14 … 8/21). parse_alert
# correctly refuses a date 0–30 days in the past, so as the real calendar moves
# each literal eventually lands in that window and its case fails for a reason
# that has nothing to do with the grammar it exists to test. That is what took
# out "Sniper SELL 1/4 (prefix fraction)" on 2026-08-07: its 7/17 expiry went
# 21 days stale and parse_alert returned None.
#
# Pinning "today" to just before the earliest literal fixes all ~40 at once.
# Rewriting the dates instead (the approach in 6a7ffe2, which had one) is not
# safe here: the fraction tokens 1/2, 1/4, 2/3 are M/D-shaped too, and rolling
# those forward would silently destroy the cases that assert on them.
class _PinnedToday(date):
    @classmethod
    def today(cls):
        return cls(2026, 4, 1)


_parser.date = _PinnedToday

PASS = 0
FAIL = 0


def chk(label, text, exp_action=None, exp_price=None, exp_qty=None,
        exp_frac=None, exp_osi=None, exp_symbol=None, should_none=False):
    global PASS, FAIL
    r = parse_alert(text)

    if should_none:
        if r is None:
            PASS += 1
            print(f"  [PASS] {label}")
        else:
            FAIL += 1
            print(f"  [FAIL] {label} — expected None, got action={r.action} osi={r.osi_symbol}")
        return

    if r is None:
        FAIL += 1
        print(f"  [FAIL] {label} — returned None for: {text[:80]}")
        return

    errors = []
    if exp_action and r.action != exp_action:
        errors.append(f"action={r.action!r} want {exp_action!r}")
    if exp_price is not None and (r.price is None or abs(float(r.price) - exp_price) > 0.015):
        errors.append(f"price={r.price} want {exp_price}")
    if exp_qty is not None and r.qty != exp_qty:
        errors.append(f"qty={r.qty} want {exp_qty}")
    if exp_frac is not None and (r.fraction is None or abs(float(r.fraction) - exp_frac) > 0.02):
        errors.append(f"fraction={r.fraction} want {exp_frac}")
    if exp_osi and r.osi_symbol != exp_osi:
        errors.append(f"osi={r.osi_symbol!r} want {exp_osi!r}")
    if exp_symbol and r.symbol != exp_symbol:
        errors.append(f"symbol={r.symbol!r} want {exp_symbol!r}")

    if errors:
        FAIL += 1
        print(f"  [FAIL] {label}")
        for e in errors:
            print(f"         {e}")
        print(f"         Text: {text[:100]}")
    else:
        PASS += 1
        print(f"  [PASS] {label}")


print("\n── Parser Tests ─────────────────────────────────────────────────────")

# ── Standard BTO ─────────────────────────────────────────────────────────────
chk("Basic BOUGHT", "BOUGHT SPX 4/16 7060C $4.05", "BUY", exp_price=4.05, exp_symbol="SPX")
chk("BOUGHT with @price", "BOUGHT AMD 4/17 285C @ $1.89", "BUY", exp_price=1.89, exp_symbol="AMD")
chk("BTO abbrev", "BTO SPX 4/14 6970C @ 1.75", "BUY", exp_price=1.75)
chk("BUY keyword", "BUY SPY 4/17 560C $2.50", "BUY", exp_price=2.50)

# ── Tags and size ─────────────────────────────────────────────────────────────
chk("SMALL tag", "BOUGHT AMD 4/17 285C $1.89 [SMALL]", "BUY", exp_price=1.89)
chk("MEDIUM tag", "BOUGHT SPX 4/16 7060C $4.05 [MEDIUM]", "BUY", exp_price=4.05)
chk("ROLL UP tag", "BOUGHT AMD 4/17 285C $1.89 [ROLL UP]", "BUY", exp_price=1.89)
chk("FULL tag is NOT close_all", "BOUGHT SPY 4/17 560C $2.50 [FULL]",
    "BUY", exp_frac=1.0)  # fraction=1 on BUY = full size, NOT close_all

# ── Contract qty extraction ───────────────────────────────────────────────────
chk("CONTRACTS: 3", "BOUGHT SPX 4/16 7000P $3.00 [SMALL] CONTRACTS: 3 @ $3.00", "BUY", exp_qty=3, exp_price=3.00)
chk("SmallAccountChallenge", "BOUGHT AMD 4/17 285C $1.89 [ROLL UP] CONTRACTS: 3 @ $1.89 ($567) SmallAccountChallenge spacemonkey",
    "BUY", exp_qty=3, exp_price=1.89)
chk("2 contracts", "BOUGHT SPX 4/16 7060C $4.05 2 contracts", "BUY", exp_qty=2)
# Sniper small-account challenge (2026-07-30 onward) — count precedes the phrase
chk("N on small account", "**BOUGHT CRM 180C 8/21 1.63** ‼️ - 2 on small account. @everyone",
    "BUY", exp_qty=2, exp_price=1.63)
chk("N on small account + % text", "**BOUGHT SKHY 145P 8/21 1.18**‼️ - 1 on small account. We will ride this one for ITM or -50% SL",
    "BUY", exp_qty=1, exp_price=1.18)  # must be 1, not the 50 out of "-50%"
# He writes "in" as often as "on" — live 2026-08-12 CRWV went in as a 1-lot on
# both boxes because only "on" was accepted. He also drops "account" and states
# add-on size as "N more on <TICKER>"; both under-sized real fills to 1.
chk("N in small account", "**BOUGHT CRWV 120C 8/14 0.63**‼️ - 5 in small account. @everyone",
    "BUY", exp_qty=5, exp_price=0.63)
chk("N on small (no 'account')", "**BOUGHT TSLA 330C 8/10 1.77**‼️ - 2 on small @everyone",
    "BUY", exp_qty=2, exp_price=1.77)
chk("N more on TICKER", "**BOUGHT MU 1000C 8/14 2.28**‼️- 2 more on MU. Nice PA @everyone",
    "BUY", exp_qty=2, exp_price=2.28)
# "N more" on an exit means contracts REMAINING — must not become a qty.
# exp_qty=None is not an assertion in chk(), so state it directly.
_leftover = parse_alert("**SOLD 2/3 MSFT 502.5C 8/14 5.05**‼️ - Leaving 1 more. Will SL at 3.5")
assert _leftover is not None and _leftover.action == "SELL", "leftover-SELL case stopped parsing"
assert _leftover.qty is None, f"'Leaving 1 more' must not set qty, got {_leftover.qty}"
chk("N contracts no space", "**BOUGHT AAPL 320C 8/21 0.89**‼️-2 contracts. Swing play.", "BUY", exp_qty=2)
# Unbalanced bold — he typed one closing asterisk instead of two (2026-07-28)
# and the stray `*` fused to BOUGHT, dropping the alert entirely.
chk("unclosed bold marker", "**BOUGHT MU 880C 8/21 4.55* - Small please. Small. @everyone",
    "BUY", exp_symbol="MU", exp_price=4.55)
chk("small account colon count", "BOUGHT SPX 4/16 7060C $4.05 Small Account: 3", "BUY", exp_qty=3)
# Integer premium with a clause AFTER it — the end-anchored rescue can't reach it.
# Live 2026-08-03: this exact alert parsed price=None and was refused by the
# missing-price guard while Sniper filled at 9.05.
chk("int premium mid-message", "**BOUGHT NBIS 200C 8/21 9**‼️ - 1 on small account. WIll make it a spread",
    "BUY", exp_price=9.00, exp_qty=1)
chk("qty after date is not a price", "BOUGHT XYZ 100C 8/21 2 contracts", "BUY", exp_qty=2)
chk("date-first strike is not a price", "BOUGHT SPX 8/21 7060C $4.05", "BUY", exp_price=4.05)

# ── STC / SELL ────────────────────────────────────────────────────────────────
chk("Basic SOLD", "SOLD SPX 4/16 7060C $6.50", "SELL", exp_price=6.50)
chk("STC abbrev", "STC SPX 4/14 6970C $2.70", "SELL", exp_price=2.70)

# ── Fraction / partial exits ──────────────────────────────────────────────────
chk("Sold half", "Sold half SPX 4/16 7060C $5.00", "SELL", exp_frac=0.5)
chk("Sell 1/2", "SOLD SPX 4/16 7060C $2.70 1/2", "SELL", exp_frac=0.5)
chk("Sell 1/3", "Sold 1/3 AMD 4/17 285C $2.20", "SELL", exp_frac=0.333)
chk("Sell 1/4", "Sold 1/4 AMD 4/17 285C $2.20", "SELL", exp_frac=0.25)
chk("Sell 2/3", "Sold 2/3 AMD 4/17 285C $2.20", "SELL", exp_frac=0.667)

# ── Close all ────────────────────────────────────────────────────────────────
chk("ALL OUT", "SOLD SPX 4/16 7060C $6.50 ALL OUT", "SELL", exp_frac=1.0)
chk("all out lowercase", "sold spx 4/16 7060c $6.50 all out", "SELL", exp_frac=1.0)
chk("close all", "SOLD AMD 4/17 285C $3.50 close all", "SELL", exp_frac=1.0)
chk("sold everything", "sold everything spx 4/16 7060c $6.50", "SELL", exp_frac=1.0)
chk("Bare SOLD (no fraction) = close_all", "SOLD SPX 4/14 6970C $2.70", "SELL", exp_frac=1.0)

# ── MonkeyBOT format ─────────────────────────────────────────────────────────
chk("MonkeyBOT LONG (bullish PUT = BUY)",
    "SPX \u25b2 LONG 7032.11 monkeyBOT\u2122 Trade Idea SPX 04/16 7060C @ $4.05",
    "BUY", exp_price=4.05, exp_symbol="SPX")
chk("MonkeyBOT SHORT (bearish = BUY put)",
    "SPX \U0001f53b SHORT 7037.37 monkeyBOT\u2122 Trade Idea SPX 04/16 7020P @ $4.15",
    "BUY", exp_price=4.15, exp_symbol="SPX")

# ── Forwarded messages ───────────────────────────────────────────────────────
chk("FWD prefix BUY", "[FWD #pro-alerts] BOUGHT SPX 4/16 7060C $4.05", "BUY", exp_price=4.05)
chk("FWD prefix SELL", "[FWD #pro-alerts] SOLD AMD 4/17 285C $2.50 all out", "SELL", exp_frac=1.0)
chk("FWD Twinsight Bot",
    "[FWD #pro-alerts] Twinsight Bot BOUGHT SPX 4/16 7000P $3.00 [SMALL]",
    "BUY", exp_price=3.00)

# ── Emoji in messages ────────────────────────────────────────────────────────
chk("Emoji prefix BUY",
    "\U0001f7e2 BOUGHT SPX 4/16 7060C $4.05 @everyone",
    "BUY", exp_price=4.05)

# ── Fluid format: fraction before ticker (reserved-word guard) ──────────────
# Bug: pre-fix matched ticker="SOLD" → bogus OSI SOLD270102C07450000.
chk("Fluid SELL 1/2 SPX",
    "> SOLD 1/2 SPX 7450C 5/29 3.7 @everyone",
    "SELL", exp_price=3.70, exp_symbol="SPX", exp_frac=0.5)
chk("Fluid SELL 1/2 MU",
    "> SOLD 1/2 MU 780C 5/29 4 @everyone",
    "SELL", exp_symbol="MU", exp_frac=0.5)
chk("Fluid SELL 1/4 BLDR",
    "> SOLD 1/4 BLDR 75C 6/18 5.1 @everyone",
    "SELL", exp_price=5.10, exp_symbol="BLDR", exp_frac=0.25)
# 2026-05-27: Sniper "SOLD 1/4 AMZN 300C 7/17 4" parsed fraction=1 because
# alt pattern 4b only inspected the zone AFTER the match — prefix "1/4" was
# missed and the bot closed all 4 contracts instead of 1. Regression guard.
chk("Sniper SELL 1/4 (prefix fraction)",
    "**SOLD 1/4 AMZN 300C 7/17 4** - 30% here. A bit of size here so selling some @everyone",
    "SELL", exp_symbol="AMZN", exp_frac=0.25)
chk("Sniper SELL 1/3 (prefix fraction)",
    "**SOLD 1/3 NVDA 140C 6/06 3.5** - trim",
    "SELL", exp_symbol="NVDA", exp_frac=1/3)
chk("Fluid BOUGHT MU lotto",
    "> BOUGHT MU 780C 5/29 2.4 - Lotto @everyone",
    "BUY", exp_price=2.40, exp_symbol="MU")
chk("Fluid ALL OUT keeps OSI",
    "> ALL OUT INTC 120C 5/29 1.5 @everyone",
    "SELL", exp_symbol="INTC")

# ── Sniper format: asterisks wrap entire line ────────────────────────────────
chk("Sniper full-wrap BOUGHT",
    "**BOUGHT MU 800P 5/29 6.25**",
    "BUY", exp_price=6.25, exp_symbol="MU")
chk("Sniper 'SOLD most of' prefix",
    "**SOLD most of GS 1000C 5/29 7**",
    "SELL", exp_symbol="GS")
chk("Sniper 4-digit strike",
    "**BOUGHT GS 1000C 5/29 1.7**",
    "BUY", exp_price=1.70, exp_symbol="GS")

# ── Equity-strategy alerts: bot doesn't trade these, skip ────────────────────
chk("Skip cash secured put SELL", "**SOLD cash secured put MU 800P 5/29 6.25**", should_none=True)
chk("Skip cash secure puts SELL", "**SOLD cash secure puts LLY 1000P 6/12 1.75**", should_none=True)
chk("Skip cash secured puts BUY",  "**BOUGHT cash secured puts ABC 200P 6/12 5**",  should_none=True)
chk("Skip covered calls SELL", "**SOLD covered calls MSTU 10C 6/12 0.35**", should_none=True)
chk("Skip covered call (singular)", "**SOLD covered call AAPL 290C 5/29 1.5**", should_none=True)
chk("Skip naked put",            "**SOLD naked put AAPL 280P 5/29 4.20**", should_none=True)

# ── Reserved-word ticker guard ───────────────────────────────────────────────
# "SOLD" / "BOUGHT" / "BUY" / "SELL" / "STC" must never be parsed as ticker.
chk("Reserved word 'SOLD' alone not a ticker",
    "SOLD 5/29 100C $1.00", should_none=True)
chk("Reserved word 'BOUGHT' alone not a ticker",
    "BOUGHT 5/29 100C $1.00", should_none=True)
chk("Word head is not a ticker ('against' -> AGAIN)",
    "**BOUGHT MU 900C 8/21 14.25** - I will sell 920C against this one.", "BUY",
    exp_price=14.25)  # must be MU 900C, never AGAIN 920C
chk("STRIKE+OT SYM (4f) still parses", "SOLD 85P ASTS", "SELL")

# ── Should return None (no trade signal) ─────────────────────────────────────
chk("Random chat", "hey guys how is the market today?", should_none=True)
chk("Setup watch", "watching SPX for a setup", should_none=True)
chk("Price level only", "SPX at 7000", should_none=True)
chk("Empty string", "", should_none=True)
chk("@everyone mention only", "@everyone check the market", should_none=True)
chk("Skip equity shares BUY", "> BOUGHT NVDL shares 115.77 @everyone", should_none=True)
chk("Skip equity TE shares BUY", "> BOUGHT TE shares 8.84 @everyone", should_none=True)
chk("Fluid BOIGHT typo (2026-09-23 missed entry)",
    "> BOIGHT AAPL 342.5C 9/23 0.47 - lotto trade. @everyone",
    exp_action="BUY", exp_price=0.47, exp_osi="AAPL260923C00342500")

total = PASS + FAIL
print(f"\nParser: {PASS}/{total} passed  {'✅' if FAIL == 0 else '❌ FAIL'}")
# Module-level so `pytest tests/` fails too, not just `python tests/<this>.py`.
# Without this the suite was collect_ignore'd AND silent, so the 7/17 drift sat
# broken with every gate green.
if FAIL:
    raise AssertionError(f"{FAIL}/{total} parser checks failed")
if __name__ == "__main__":
    sys.exit(0)
