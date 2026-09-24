"""
Production-grade guardrails test suite.
Tests all safety checks using an isolated in-memory SQLite DB.
"""
import atexit, os, shutil, sys, tempfile, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Sandbox the test DB inside a temp dir so the project root stays clean.
_TEST_DIR = tempfile.mkdtemp(prefix="trader_guardrails_test_")
_TEST_DB = os.path.join(_TEST_DIR, "test.db")
os.environ["TRADER_DATABASE_URL"] = f"sqlite:///{_TEST_DB}"
atexit.register(lambda: shutil.rmtree(_TEST_DIR, ignore_errors=True))

# Sandbox the config the same way. Test 8 calls cfg.set_and_save, which
# rewrites the whole file through configparser (normalising every `k=v` to
# `k = v`). Pointed at the tracked tests/test_config.ini that dirties the
# worktree on every run; run standalone on a live box, TRADER_CONFIG_PATH is
# unset and it would rewrite the production config.ini. Copy first, then point
# the loader at the copy — config_manager resolves CONFIG_PATH at import time,
# so this must happen before any app import below.
_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SRC_CFG = os.environ.get("TRADER_CONFIG_PATH") or os.path.join(_REPO, "tests", "test_config.ini")
_TEST_CFG = os.path.join(_TEST_DIR, "config.ini")
shutil.copyfile(_SRC_CFG, _TEST_CFG)
os.makedirs(os.path.join(_TEST_DIR, "logs"), exist_ok=True)  # set_and_save audit log
os.environ["TRADER_CONFIG_PATH"] = _TEST_CFG

import app.risk.guardrails as gr
from app.core.db import Base, engine, SessionLocal
from app.core.models import Alert, Position, Order
from datetime import datetime, timezone
from decimal import Decimal

Base.metadata.create_all(engine)

# ── Test-only config overrides ───────────────────────────────────────────────
# Disable guardrails that depend on wall-clock or broker state so tests are
# deterministic. We patch the accessor funcs rather than _is_market_open
# so real market-hours logic stays exercised if tested elsewhere.
gr._MARKET_HOURS_ONLY = lambda: False
# The new reconcile-required guard would block BUYs when no startup reconcile
# has run — tests don't hit the broker, so short-circuit it to True.
import app.monitors.reconciler as reconciler
reconciler.has_reconciled = lambda: True

PASS = 0; FAIL = 0

def chk(label, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {label}")
    else:
        FAIL += 1
        print(f"  [FAIL] {label}" + (f" — {detail}" if detail else ""))

def reset():
    """Reset in-memory dedup caches between tests."""
    gr._recent_alerts.clear()
    gr._recent_content_hashes.clear()
    gr._last_trade_time.clear()
    gr._trading_halted = False

print("\n── Guardrails Tests ─────────────────────────────────────────────────")

# ── Test 1: First BUY always allowed ─────────────────────────────────────────
reset()
# Signature: (action, osi_symbol, price, qty, alert_author="", content_hash="", alert_db_id=None)
ok, reason = gr.check_guardrails("BUY", "SPXTEST1", None, 1, "author1", "", None)
chk("First BUY passes", ok, reason)

# ── Test 1b: Guardrails disabled allows all orders ───────────────────────────
# NOTE: don't call reset() here — Test 2 below depends on Test 1's recorded BUY
# being present in the dedup cache. With guardrails disabled, check_guardrails
# returns early without recording, so Test 1's record is the only seed.
# Temporarily disable guardrails by patching the accessor (cfg has no in-memory set()).
_orig_guardrails_enabled = gr._GUARDRAILS_ENABLED
gr._GUARDRAILS_ENABLED = lambda: False
ok_disabled, _ = gr.check_guardrails("BUY", "SPXTEST1", Decimal("1000"), 100, "author1", "", None)  # Would exceed max cost
chk("Guardrails disabled allows expensive order", ok_disabled, "")
# Second call - duplicate should also pass when guardrails are disabled
ok_disabled2, _ = gr.check_guardrails("BUY", "SPXTEST1", None, 1, "author1", "", None)
chk("Guardrails disabled allows duplicate", ok_disabled2, "")
gr._GUARDRAILS_ENABLED = _orig_guardrails_enabled  # Restore

# ── Test 2: Duplicate BUY within dedup window is blocked ─────────────────────
# Second call same symbol within cooldown — dedup or cooldown should catch it
ok2, reason2 = gr.check_guardrails("BUY", "SPXTEST1", None, 1, "author1", "", None)
chk("Duplicate BUY blocked", not ok2, reason2)
chk("Duplicate/cooldown reason is informative", any(w in reason2.upper() for w in ("DUPLICATE", "COOLDOWN")), reason2)

# SELL dedup: second identical SELL on same OSI within dedup window IS blocked
# (prevents accidental double-exits)
ok3, r3 = gr.check_guardrails("SELL", "SPXTEST2", None, 1, "author1", "", None)
chk("First SELL passes", ok3, r3)
ok4, r4 = gr.check_guardrails("SELL", "SPXTEST2", None, 1, "author1", "", None)
chk("Duplicate SELL is blocked (prevents double-exit)", not ok4, r4)

# ── Test 4: Different OSI symbols don't conflict ─────────────────────────────
reset()
ok5, _ = gr.check_guardrails("BUY", "SPXA", None, 1, "author", "", None)
ok6, _ = gr.check_guardrails("BUY", "AMDB", None, 1, "author", "", None)
chk("Different symbols don't conflict", ok5 and ok6)

# ── Test 5: get_guardrail_status returns valid dict ───────────────────────────
status = gr.get_guardrail_status()
chk("get_guardrail_status returns dict", isinstance(status, dict))
chk("Has trading_halted", "trading_halted" in status)
chk("trading_halted starts false", status["trading_halted"] == False)
chk("Has open_positions", "open_positions" in status)
chk("Has daily_pnl", "daily_pnl" in status)
chk("Has max_open_positions", "max_open_positions" in status)
chk("Has today_trades", "today_trades" in status)
chk("Has max_daily_trades", "max_daily_trades" in status)
chk("Has dedup_window", "dedup_window" in status)

# ── Test 6: _is_market_open returns bool ─────────────────────────────────────
chk("_is_market_open is bool", isinstance(gr._is_market_open(), bool))

# ── Test 7: _record_alert populates caches ────────────────────────────────────
reset()
gr._record_alert("TESTOSI", "BUY", "testhash123")
chk("_record_alert sets _recent_alerts", ("TESTOSI", "BUY") in gr._recent_alerts)
chk("_record_alert sets _last_trade_time", "TESTOSI" in gr._last_trade_time)
chk("_record_alert sets _recent_content_hashes", "testhash123" in gr._recent_content_hashes)

# ── Test 8: Cooldown blocks rapid repeat trades ───────────────────────────────
from app.core.config_manager import cfg
orig_cooldown = cfg.get("guardrails", "trade_cooldown_seconds", fallback="10")
cfg.set_and_save([{"section": "guardrails", "key": "trade_cooldown_seconds", "value": "60"}])

reset()
ok_first, _ = gr.check_guardrails("BUY", "COOLTEST", None, 1, "a", "", None)
# Second call same symbol within cooldown — dedup should catch it
ok_second, r_second = gr.check_guardrails("BUY", "COOLTEST", None, 1, "a", "", None)
chk("Cooldown/dedup blocks repeat BUY", not ok_second, r_second)

cfg.set_and_save([{"section": "guardrails", "key": "trade_cooldown_seconds", "value": orig_cooldown}])

# ── Test 9: Cross-channel dedup blocks same hash ─────────────────────────────
reset()
import hashlib
content = "BOUGHT SPX 4/16 7060C $4.05"
h = hashlib.sha256(content.encode()).hexdigest()
gr._recent_content_hashes[h] = time.time()
blocked, reason_cc = gr.check_guardrails("BUY", "SPXCC", None, 1, "a", h, None)
chk("Cross-channel dedup blocks same hash", not blocked, reason_cc)
chk("Cross-channel reason text", "CROSS-CHANNEL" in reason_cc.upper() or "DUPLICATE" in reason_cc.upper(), reason_cc)

# ── Test 10-14: blacklist on exits + blank close_all guard (2026-06-11) ───────
# Regression for the incident where BUY-blacklisted 'TC' posted "everything
# selling now" → blank-symbol close_all flattened the whole book.
gr._ANALYST_BLACKLIST = lambda: {"tc", "spacemonkey"}
gr._ANALYST_BLACKLIST_BLOCKS_EXITS = lambda: True
gr._CLOSE_ALL_AUTHOR_WHITELIST = lambda: {"operator"}

reset()
ok_bl, r_bl = gr.check_guardrails("SELL", "TSLA260605C00430000", None, 4, "[FWD #analyst-tc] TC", "", None)
chk("Blacklisted author SELL blocked", not ok_bl, r_bl)
chk("Blacklist reason on SELL", "BLACKLIST" in r_bl.upper(), r_bl)

reset()
ok_ca, r_ca = gr.check_guardrails("SELL", "", None, 1, "TC", "", None)
chk("Blacklisted blank-symbol close_all blocked", not ok_ca, r_ca)

reset()
ok_nw, r_nw = gr.check_guardrails("SELL", "", None, 1, "Sarang", "", None)
chk("Non-whitelisted blank close_all blocked (everyone gate)", not ok_nw, r_nw)
chk("close_all not-authorized reason", "CLOSE_ALL_NOT_AUTHORIZED" in r_nw.upper(), r_nw)

reset()
ok_w, r_w = gr.check_guardrails("SELL", "", None, 1, "Sandeep", "", None)
chk("Whitelisted author blank close_all allowed", ok_w, r_w)

reset()
ok_legit, r_legit = gr.check_guardrails("SELL", "AAPL260618C00200000", None, 4, "Sarang", "", None)
chk("Non-blacklisted author normal SELL still fills", ok_legit, r_legit)

total = PASS + FAIL
print(f"\nGuardrails: {PASS}/{total} passed  {'✅' if FAIL == 0 else '❌ FAIL'}")

# Cleanup
import os
try: os.remove("test_gl.db")
except: pass

if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
