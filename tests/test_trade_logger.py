"""
Trade logger tests: verify persistent HTTP session and config helpers.
"""
import sys, os, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app.analytics.trade_logger as trade_logger

PASS = 0; FAIL = 0

def chk(label, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  [PASS] {label}")
    else:
        FAIL += 1; print(f"  [FAIL] {label}" + (f" — {detail}" if detail else ""))

print("\n── Trade Logger Tests ───────────────────────────────────────────────")

# ── Session management ────────────────────────────────────────────────────────
async def test_session():
    # First call: session should be created
    s1 = trade_logger._get_http_session()
    chk("Session created on first call", s1 is not None)
    chk("Session not closed", not s1.closed)

    # Second call: should return same session
    s2 = trade_logger._get_http_session()
    chk("Same session returned (singleton)", s1 is s2)

    # Close and recreate
    await trade_logger.close_http_session()
    chk("Session closed after close_http_session()", trade_logger._http_session is None)

    s3 = trade_logger._get_http_session()
    chk("New session created after close", s3 is not None and not s3.closed)

    # Cleanup
    await trade_logger.close_http_session()

asyncio.run(test_session())

# ── Config helpers read live values ──────────────────────────────────────────
# Function was renamed from _TRADE_LOG_CHANNEL to _trade_log_channel_id
channel_id = trade_logger._trade_log_channel_id()
chk("_trade_log_channel_id() returns string or empty", isinstance(channel_id, str))

bot_token = trade_logger._bot_token()
chk("_bot_token() returns string", isinstance(bot_token, str))

# ── Color helper ─────────────────────────────────────────────────────────────
chk("BUY color is green-ish", trade_logger._color_for_action("BUY") > 0)
chk("SELL color is red-ish", trade_logger._color_for_action("SELL") > 0)
chk("CANCELLED color exists", trade_logger._color_for_action("CANCELLED") >= 0)
chk("SL color exists", trade_logger._color_for_action("SL") > 0)

total = PASS + FAIL
print(f"\nTradeLogger: {PASS}/{total} passed  {'✅' if FAIL == 0 else '❌'}")
if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
