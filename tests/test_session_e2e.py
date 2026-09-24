"""
End-to-end verification of the 2026-06-11 session changes, no network, no live
broker. Run standalone:  python tests/test_session_e2e.py

Covers:
  A. Parser   — 182.C dot typo, decimal strike intact, chitchat guards, ticker-only blank
  B. Model    — Order.author column + to_dict + migration adds it to a legacy table
  C. Executor — normal BUY stamps Position.author + Order.author
                execute_override stamps author (the override fix)
                targeted SELL Order.author = opener
                blank close_all gate → SKIPPED (requires_symbol=true)
                gate off → author-scoped flatten still works
"""
import os, sys, asyncio, tempfile, atexit, shutil
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_DIR = tempfile.mkdtemp(prefix="trader_e2e_")
os.environ["TRADER_DATABASE_URL"] = f"sqlite:///{os.path.join(_DIR, 'e2e.db').replace(chr(92), '/')}"
os.environ["DRY_RUN"] = "true"
atexit.register(lambda: shutil.rmtree(_DIR, ignore_errors=True))

# ── Neutralize network side-effects (Discord trade-logger) ──────────────
import app.analytics.trade_logger as _tl
async def _noop(*a, **k):
    return None
for _n in dir(_tl):
    if _n.startswith("log_"):
        setattr(_tl, _n, _noop)

import app.execution.public_executor as pe
import app.risk.guardrails as guardrails
import app.risk.ai_scorer as ai_scorer
import app.risk.kelly_sizer as kelly_sizer
import app.monitors.reconciler as reconciler
import app.core.db as db
from app.core.models import Alert, Order, Position
from app.ingest.parser import parse_alert
from app.execution.public_executor import PublicExecutor
from sqlalchemy import inspect, text

pe.DRY_RUN = True
guardrails.MARKET_HOURS_ONLY = False
ai_scorer.SCORER_ENABLED = False
kelly_sizer.KELLY_ENABLED = False
reconciler._reconciled_once = True

# The live accessors read cfg, not module globals — force dry-run + open market
# at the source so BUYs aren't blocked and no real broker/account is touched.
from app.core.config_manager import cfg
_rb = cfg.getboolean
def _gb(section, key, fallback=False):
    if (section, key) == ("trading", "dry_run"):            return True
    if (section, key) == ("guardrails", "market_hours_only"): return False
    return _rb(section, key, fallback=fallback)
cfg.getboolean = _gb

# Pin the two new knobs deterministically regardless of config.ini.
pe._CLOSE_ALL_SCOPE = lambda: "author"
pe._CLOSE_ALL_REQUIRES_SYMBOL = lambda: True
# Real blacklist = analyst-tc,tc,spacemonkey,matae. Sarang/Twinsight are clean;
# Matae is blacklisted (used only to prove execute_override bypasses it).

PASS = 0; FAIL = 0
def chk(label, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  [PASS] {label}")
    else:
        FAIL += 1; print(f"  [FAIL] {label}" + (f" — {detail}" if detail else ""))


class Mgr:
    def __init__(self): self.events = []
    async def broadcast(self, d): self.events.append(d)


def mk_alert_row(a, raw, author):
    s = db.SessionLocal()
    try:
        row = Alert(author=author, action=a.action, raw_text=raw,
                    osi_symbol=a.osi_symbol, symbol=a.symbol,
                    expiry=str(a.expiry) if a.expiry else None, strike=a.strike,
                    option_type=a.option_type,
                    alert_price=float(a.price) if a.price is not None else None,
                    size_tag=a.size_tag, fraction=float(a.fraction), status="PENDING")
        s.add(row); s.commit(); s.refresh(row); return row.id
    finally:
        s.close()


# ════════════════════════ A. PARSER ════════════════════════
def test_parser():
    print("\n── A. Parser ─────────────────────────────────────────────────────────")
    a = parse_alert("#ALERT SOLD NVDA 182.C 8/29 $1.2 ALL OUT")
    chk("182.C dot typo → real OSI", a and a.osi_symbol == "NVDA260829C00182000", a and a.osi_symbol)
    a = parse_alert("#ALERT SOLD NVDA 182.5C 8/29 $1.2 ALL OUT")
    chk("decimal strike 182.5C preserved", a and a.osi_symbol == "NVDA260829C00182500", a and a.osi_symbol)
    for phrase in ["QQQ close to all time high",
                   "preserve capital and call out a day",
                   "they are selling everything again. SPX lotto failed"]:
        chk(f"chitchat dropped → None: {phrase[:32]!r}", parse_alert(phrase) is None)
    a = parse_alert("All out on MRVL")
    chk("ticker-only all-out → blank close_all (symbol='')",
        a and a.action == "SELL" and a.close_all and not (a.symbol or a.osi_symbol),
        a and (a.symbol, a.osi_symbol, a.close_all))
    a = parse_alert("SOLD SPX 8/29 6970C $2.70 1/2")
    chk("normal targeted SELL parses (not blank, not close_all)",
        a and a.symbol == "SPX" and not a.close_all and abs(float(a.fraction) - 0.5) < 1e-6,
        a and (a.symbol, a.close_all, str(a.fraction)))


# ════════════════════════ B. MODEL + MIGRATION ════════════════════════
def test_model_and_migration():
    print("\n── B. Order.author column + migration ────────────────────────────────")
    db.init_db()
    cols = {c["name"] for c in inspect(db.engine).get_columns("orders")}
    chk("orders table has author column after init_db", "author" in cols, cols)
    # to_dict round-trip
    s = db.SessionLocal()
    try:
        o = Order(public_order_id="x1", osi_symbol="MRVL260618C00032000", side="BUY",
                  quantity=4, limit_price=0.32, status="PENDING", trigger="DISCORD", author="Sarang")
        s.add(o); s.commit(); s.refresh(o)
        chk("Order.author persists + to_dict emits it", o.to_dict().get("author") == "Sarang")
    finally:
        s.close()
    # Migration: legacy orders table WITHOUT author → _ensure_columns adds it.
    # Unique per-run table name + explicit teardown so no schema state persists
    # in the on-disk test DB (a fixed name raced across pooled SQLite conns and
    # leaked between suite runs → flaky "column already exists").
    import uuid
    legacy_tbl = f"orders_legacy_{uuid.uuid4().hex[:8]}"
    from app.core.db import _ensure_columns
    try:
        with db.engine.begin() as conn:
            conn.execute(text(f"CREATE TABLE {legacy_tbl} (id INTEGER PRIMARY KEY, side VARCHAR)"))
        _ensure_columns(legacy_tbl, {"author": "VARCHAR"})
        cols2 = {c["name"] for c in inspect(db.engine).get_columns(legacy_tbl)}
        chk("_ensure_columns adds author to a legacy table", "author" in cols2, cols2)
    finally:
        with db.engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {legacy_tbl}"))


# ════════════════════════ C. EXECUTOR E2E ════════════════════════
async def test_executor():
    print("\n── C. Executor end-to-end (dry-run) ──────────────────────────────────")
    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()
    ex = PublicExecutor(Mgr())

    # 1. Normal BUY stamps Position.author + Order.author
    raw = "BOUGHT NVDA 8/29 182C $1.20 [SMALL]"
    a = parse_alert(raw)
    aid = mk_alert_row(a, raw, "Sarang")
    await ex.execute(a, aid, alert_author="Sarang")
    s = db.SessionLocal()
    try:
        pos = s.query(Position).filter_by(osi_symbol="NVDA260829C00182000").first()
        bo = s.query(Order).filter_by(side="BUY").first()
        al = s.get(Alert, aid)
        chk("normal BUY → Position.author=Sarang", pos and pos.author == "Sarang", pos and pos.author)
        chk("normal BUY → Order.author=Sarang", bo and bo.author == "Sarang", bo and bo.author)
        chk("normal BUY → alert FILLED", al and al.status == "FILLED", al and al.status)
    finally:
        s.close()

    # 2. execute_override stamps author (the override fix) — use a blacklist-y name
    raw2 = "BOUGHT MRVL 8/29 65C $1.10 [SMALL]"
    a2 = parse_alert(raw2)
    aid2 = mk_alert_row(a2, raw2, "Matae")
    await ex.execute_override(a2, aid2, alert_author="Matae")
    s = db.SessionLocal()
    try:
        pos2 = s.query(Position).filter_by(osi_symbol="MRVL260829C00065000").first()
        chk("execute_override → Position.author=Matae (was NULL before fix)",
            pos2 and pos2.author == "Matae", pos2 and (pos2.author if pos2 else None))
        bo2 = s.query(Order).filter(Order.osi_symbol == "MRVL260829C00065000", Order.side == "BUY").first()
        chk("execute_override → Order.author=Matae", bo2 and bo2.author == "Matae", bo2 and (bo2.author if bo2 else None))
    finally:
        s.close()

    # 3. Targeted SELL → SELL Order.author = opener (Sarang)
    raw3 = "SOLD NVDA 8/29 182C $1.50 ALL OUT"
    a3 = parse_alert(raw3)
    aid3 = mk_alert_row(a3, raw3, "Sarang")
    await ex.execute(a3, aid3, alert_author="Sarang")
    s = db.SessionLocal()
    try:
        so = s.query(Order).filter_by(side="SELL").order_by(Order.id.desc()).first()
        pos = s.query(Position).filter_by(osi_symbol="NVDA260829C00182000").first()
        chk("targeted SELL → Order.author = opener (Sarang)", so and so.author == "Sarang", so and (so.author if so else None))
        chk("targeted SELL → position CLOSED", pos and pos.status == "CLOSED", pos and pos.status)
    finally:
        s.close()

    # 4. Blank close_all gate (requires_symbol=true) → SKIPPED, nothing touched
    pe._CLOSE_ALL_REQUIRES_SYMBOL = lambda: True
    raw4 = "ALL OUT"
    a4 = parse_alert(raw4)
    aid4 = mk_alert_row(a4, raw4, "Matae")
    before_sells = None
    s = db.SessionLocal()
    try:
        before_sells = s.query(Order).filter_by(side="SELL").count()
        before_open = s.query(Position).filter(Position.status.in_(["OPEN", "PARTIAL"])).count()
    finally:
        s.close()
    await ex.execute(a4, aid4, alert_author="Matae")
    s = db.SessionLocal()
    try:
        al4 = s.get(Alert, aid4)
        after_sells = s.query(Order).filter_by(side="SELL").count()
        after_open = s.query(Position).filter(Position.status.in_(["OPEN", "PARTIAL"])).count()
        chk("blank close_all gate → alert SKIPPED", al4 and al4.status == "SKIPPED", al4 and al4.status)
        chk("blank close_all gate → no new SELL order", after_sells == before_sells, (before_sells, after_sells))
        chk("blank close_all gate → open positions untouched", after_open == before_open, (before_open, after_open))
    finally:
        s.close()

    # 5. Gate OFF → author-scoped flatten works, and respects ownership.
    #    Open a fresh Sarang position; Matae still owns MRVL. Sarang's blank
    #    ALL OUT must close PANW (Sarang's) but NOT MRVL (Matae's).
    rawB = "BOUGHT PANW 8/29 200C $1.30 [SMALL]"
    aB = parse_alert(rawB)
    aidB = mk_alert_row(aB, rawB, "Sarang")
    await ex.execute(aB, aidB, alert_author="Sarang")
    pe._CLOSE_ALL_REQUIRES_SYMBOL = lambda: False
    raw5 = "ALL OUT"
    a5 = parse_alert(raw5)
    aid5 = mk_alert_row(a5, raw5, "Sarang")
    await ex.execute(a5, aid5, alert_author="Sarang")
    s = db.SessionLocal()
    try:
        panw = s.query(Position).filter_by(osi_symbol="PANW260829C00200000").first()
        mrvl = s.query(Position).filter_by(osi_symbol="MRVL260829C00065000").first()
        chk("gate off → Sarang ALL OUT closes Sarang's PANW", panw and panw.status == "CLOSED", panw and (panw.status if panw else None))
        chk("gate off → Sarang ALL OUT does NOT close Matae's MRVL (scope=author)", mrvl and mrvl.status != "CLOSED", mrvl and (mrvl.status if mrvl else None))
    finally:
        s.close()
    pe._CLOSE_ALL_REQUIRES_SYMBOL = lambda: True


def main():
    test_parser()
    test_model_and_migration()
    asyncio.run(test_executor())
    total = PASS + FAIL
    print(f"\n════ session e2e: {PASS}/{total} passed  {'OK' if FAIL == 0 else 'FAIL'} ════")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
