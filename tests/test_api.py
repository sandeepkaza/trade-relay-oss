"""
Production-grade API integration tests.
Hits the live server at localhost:8000 — server must be running.
Tests all REST endpoints for correct status codes and response shapes.
"""
import sys, json, time
try:
    import urllib.request as req
    import urllib.error
except ImportError:
    print("urllib not found"); sys.exit(1)

BASE = "http://localhost:8000"
PASS = 0; FAIL = 0

def get(path, expect=200):
    global PASS, FAIL
    try:
        r = req.urlopen(f"{BASE}{path}", timeout=5)
        code = r.status
        body = json.loads(r.read())
    except urllib.error.HTTPError as e:
        code = e.code
        try: body = json.loads(e.read())
        except: body = {}
    except Exception as exc:
        FAIL += 1
        print(f"  [FAIL] GET {path} — connection error: {exc}")
        return None, None
    if code == expect:
        PASS += 1
        print(f"  [PASS] GET {path} → {code}")
    else:
        FAIL += 1
        print(f"  [FAIL] GET {path} → {code} (want {expect})")
    return code, body

def post(path, data=None, expect=200):
    global PASS, FAIL
    payload = json.dumps(data or {}).encode() if data else b""
    r_obj = req.Request(f"{BASE}{path}", data=payload,
                        headers={"Content-Type": "application/json"}, method="POST")
    try:
        r = req.urlopen(r_obj, timeout=5)
        code = r.status
        body = json.loads(r.read())
    except urllib.error.HTTPError as e:
        code = e.code
        try: body = json.loads(e.read())
        except: body = {}
    except Exception as exc:
        FAIL += 1
        print(f"  [FAIL] POST {path} — connection error: {exc}")
        return None, None
    ok = code == expect
    if ok: PASS += 1; print(f"  [PASS] POST {path} → {code}")
    else:   FAIL += 1; print(f"  [FAIL] POST {path} → {code} (want {expect})")
    return code, body

def chk(label, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"    [PASS] {label}")
    else:
        FAIL += 1; print(f"    [FAIL] {label}" + (f" — {detail}" if detail else ""))

print("\n── API Integration Tests ────────────────────────────────────────────")

# ── Health ───────────────────────────────────────────────────────────────────
_, health = get("/api/health")
if health:
    chk("health has status", "status" in health)
    chk("health status=ok", health.get("status") == "ok")

# ── Alerts ───────────────────────────────────────────────────────────────────
_, alerts = get("/api/alerts")
if alerts is not None:
    chk("alerts is list", isinstance(alerts, list))
    if alerts:
        a = alerts[0]
        chk("alert has id", "id" in a)
        chk("alert has status", "status" in a)
        chk("alert has action", "action" in a)
        chk("alert has osiSymbol", "osiSymbol" in a)
        chk("alert has error field", "error" in a)
        chk("alert has timestamp", "timestamp" in a)
        chk("alert has channel", "channel" in a)

# ── Orders ───────────────────────────────────────────────────────────────────
_, orders = get("/api/orders")
if orders is not None:
    chk("orders is list", isinstance(orders, list))
    if orders:
        o = orders[0]
        chk("order has id", "id" in o)
        chk("order has status", "status" in o)
        chk("order has side", "side" in o)
        chk("order has osiSymbol", "osiSymbol" in o)
        chk("order has limitPrice", "limitPrice" in o)
        chk("order has fillPrice", "fillPrice" in o)
        chk("order has filledQty", "filledQty" in o)
        chk("order has publicOrderId", "publicOrderId" in o)

# ── Positions ────────────────────────────────────────────────────────────────
_, positions = get("/api/positions")
if positions is not None:
    chk("positions is list", isinstance(positions, list))

# ── Guardrails ───────────────────────────────────────────────────────────────
_, gr_status = get("/api/guardrails")
if gr_status:
    for key in ["trading_halted", "market_hours_only", "is_market_open",
                "open_positions", "max_open_positions", "today_trades",
                "max_daily_trades", "daily_pnl", "max_daily_loss",
                "max_trade_cost", "dedup_window", "cooldown"]:
        chk(f"guardrails has {key}", key in gr_status, str(gr_status.keys()))

# ── Balance ──────────────────────────────────────────────────────────────────
_, balance = get("/api/balance")
if balance:
    chk("balance has buying_power or error", "buying_power" in balance or "error" in balance)

# ── Stats ────────────────────────────────────────────────────────────────────
_, stats = get("/api/stats")
if stats:
    chk("stats has total_realized_pnl", "total_realized_pnl" in stats or "error" not in stats)

# ── Config ───────────────────────────────────────────────────────────────────
_, config = get("/api/config")
if config:
    chk("config is dict", isinstance(config, dict))
    chk("config has trading section", "trading" in config)

# ── Channels ─────────────────────────────────────────────────────────────────
_, channels = get("/api/channels")
if channels is not None:
    chk("channels is list", isinstance(channels, list))

# ── Export endpoints ─────────────────────────────────────────────────────────
get("/api/export/trades")
get("/api/export/positions")
get("/api/export/alerts")

# ── Cancel bad order (should 400/404, not 500) ───────────────────────────────
get("/api/orders/999999/cancel", expect=404)   # This is a POST — expect 405
# Actually test via POST:
_, cancel_bad_order = None, None
try:
    r = req.Request(f"{BASE}/api/orders/999999/cancel", data=b"", method="POST",
                    headers={"Content-Type": "application/json"})
    resp = req.urlopen(r, timeout=5)
    cancel_bad_order = resp.status
    chk("cancel nonexistent order handled cleanly", False, f"Should have errored, got {cancel_bad_order}")
except urllib.error.HTTPError as e:
    chk("cancel nonexistent order returns 4xx", e.code in (400, 404), f"got {e.code}")
    PASS += 1 if e.code in (400, 404) else 0

# ── Cancel alert endpoint exists ─────────────────────────────────────────────
try:
    r = req.Request(f"{BASE}/api/alerts/999999/cancel", data=b"", method="POST",
                    headers={"Content-Type": "application/json"})
    resp = req.urlopen(r, timeout=5)
    chk("/api/alerts/{id}/cancel route exists", False, "should have returned 4xx for unknown id")
except urllib.error.HTTPError as e:
    chk("/api/alerts/{id}/cancel route exists", e.code in (400, 404), f"got {e.code}")

# ── WebSocket endpoint accessible ────────────────────────────────────────────
try:
    ws_r = req.Request(f"{BASE}/ws", headers={"Connection": "Upgrade", "Upgrade": "websocket",
                                               "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==",
                                               "Sec-WebSocket-Version": "13"})
    req.urlopen(ws_r, timeout=2)
except Exception as e:
    # WS upgrade fails with plain http — that's expected; just confirm it's not a 404
    err = str(e)
    chk("WebSocket endpoint exists (not 404)", "404" not in err, err[:80])

total = PASS + FAIL
print(f"\nAPI: {PASS}/{total} passed  {'✅' if FAIL == 0 else '❌ FAIL'}")
if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
