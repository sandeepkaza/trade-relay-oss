"""
Production-grade model + DB test suite.
Validates schema, to_dict(), and P&L calculation correctness.
"""
import atexit, os, shutil, sys, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Sandbox the test DB inside a temp dir so the project root stays clean and
# concurrent test runs don't clobber each other. Cleaned up at process exit.
_TEST_DIR = tempfile.mkdtemp(prefix="trader_models_test_")
_TEST_DB = os.path.join(_TEST_DIR, "test.db")
os.environ["TRADER_DATABASE_URL"] = f"sqlite:///{_TEST_DB}"
atexit.register(lambda: shutil.rmtree(_TEST_DIR, ignore_errors=True))

from app.core.db import Base, engine, SessionLocal
from app.core.models import Alert, Position, Order
from datetime import datetime, timezone

# Drop all tables first to get a clean slate (avoids UNIQUE constraint from previous run)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

PASS = 0; FAIL = 0

def chk(label, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  [PASS] {label}")
    else:
        FAIL += 1; print(f"  [FAIL] {label}" + (f" — {detail}" if detail else ""))

print("\n── Model Tests ──────────────────────────────────────────────────────")

db = SessionLocal()

# ── Alert CRUD ────────────────────────────────────────────────────────────────
a = Alert(
    author="testbot", action="BUY", raw_text="BOUGHT SPX 4/16 7060C $4.05",
    osi_symbol="SPX260416C07060000", symbol="SPX", expiry="04/16/2026",
    strike=7060, option_type="C", alert_price=4.05, status="PENDING",
    channel_name="pro-alerts",
)
db.add(a); db.commit(); db.refresh(a)
chk("Alert saved", a.id is not None)
d = a.to_dict()
chk("Alert to_dict has id", "id" in d)
chk("Alert to_dict has status", d.get("status") == "PENDING")
chk("Alert to_dict has error field", "error" in d)
chk("Alert to_dict has osiSymbol", d.get("osiSymbol") == "SPX260416C07060000")
chk("Alert to_dict has contentHash", "contentHash" in d)
chk("Alert to_dict has channel", d.get("channel") == "pro-alerts")

# ── Position P&L calculations ─────────────────────────────────────────────────
pos = Position(
    osi_symbol="SPX260416C07060000", symbol="SPX", expiry="04/16",
    strike=7060, option_type="C",
    total_contracts=4, remaining=4,
    avg_price=4.05, current_price=6.00,
    status="OPEN",
    open_time=datetime.now(timezone.utc),
)
db.add(pos); db.commit(); db.refresh(pos)

# Unrealized P&L
expected_pct = ((6.00 / 4.05) - 1) * 100
chk("Position pnl_pct OPEN", abs(pos.pnl_pct() - expected_pct) < 0.01,
    f"got {pos.pnl_pct():.2f}% want {expected_pct:.2f}%")

expected_dollar = (6.00 - 4.05) * 4 * 100
chk("Position pnl_dollar OPEN (unrealized)", abs(pos.pnl_dollar() - expected_dollar) < 0.01,
    f"got {pos.pnl_dollar():.2f} want {expected_dollar:.2f}")

# Realized P&L (after close)
pos.status = "CLOSED"
pos.remaining = 0
pos.close_price = 6.00
pos.close_time = datetime.now(timezone.utc)
db.commit()

chk("Position pnl_pct CLOSED uses close_price", abs(pos.pnl_pct() - expected_pct) < 0.01)
expected_realized = (6.00 - 4.05) * 4 * 100
chk("Position pnl_dollar CLOSED uses all contracts",
    abs(pos.pnl_dollar() - expected_realized) < 0.01,
    f"got {pos.pnl_dollar():.2f} want {expected_realized:.2f}")

d2 = pos.to_dict()
chk("Position to_dict has pnlPct", "pnlPct" in d2)
chk("Position to_dict has pnlDollar", "pnlDollar" in d2)
chk("Position to_dict has closePrice", d2.get("closePrice") == 6.00)
chk("Position to_dict has exits dict", "exits" in d2)
chk("Exits has pt1/pt2/pt3/sl/tpt", all(k in d2["exits"] for k in ["pt1","pt2","pt3","sl","tpt"]))

# ── Order CRUD ────────────────────────────────────────────────────────────────
import uuid
ord_ = Order(
    public_order_id=str(uuid.uuid4()),
    osi_symbol="SPX260416C07060000",
    side="BUY", quantity=4, limit_price=4.26,
    status="PENDING", trigger="DISCORD",
    placed_at=datetime.now(timezone.utc),
)
db.add(ord_); db.commit(); db.refresh(ord_)
chk("Order saved", ord_.id is not None)
od = ord_.to_dict()
chk("Order to_dict has publicOrderId", "publicOrderId" in od)
chk("Order to_dict has filledQty", "filledQty" in od)
chk("Order to_dict has placedAt", "placedAt" in od)
chk("Order to_dict has limitPrice", od.get("limitPrice") == 4.26)

# ── Edge cases: avg_price=0 guard ─────────────────────────────────────────────
pos_zero = Position(osi_symbol="ZERO", symbol="X", expiry="01/01", strike=100,
                    option_type="C", total_contracts=1, remaining=1,
                    avg_price=0, current_price=1.0, status="OPEN",
                    open_time=datetime.now(timezone.utc))
db.add(pos_zero); db.commit()
chk("pnl_pct guards avg_price=0", pos_zero.pnl_pct() == 0.0)
chk("pnl_dollar guards avg_price=0", pos_zero.pnl_dollar() == 0.0)

db.close()

# Cleanup
try: os.remove(_TEST_DB)
except: pass

total = PASS + FAIL
print(f"\nModels: {PASS}/{total} passed  {'✅' if FAIL == 0 else '❌'}")
if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
