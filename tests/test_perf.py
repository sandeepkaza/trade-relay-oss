"""
Performance / latency tests for the hot path that matters on a trading bot:
alert -> parse -> guardrail -> dry-run order decision.

Manual timing (not wall-clock-of-one-call) with GENEROUS budgets: CI runners are
noisy, so budgets are sized to catch order-of-magnitude regressions, not 2x
jitter. Reads the same dry-run / config / network-stub harness as the rest of
the suite (tests/conftest.py autouse fixtures).

Run locally:  pytest tests/test_perf.py -q -s
"""
import asyncio
import os
import statistics
import tempfile
import time
from decimal import Decimal

# Own temp DB when this file runs alone (the CI perf job runs only this file, so
# no other test module has set the URL). setdefault keeps a value the full-suite
# run already provided. config + dry-run/network-stub come from conftest.
os.environ.setdefault(
    "TRADER_DATABASE_URL",
    "sqlite:///" + os.path.join(tempfile.mkdtemp(prefix="perf_"), "perf.db").replace("\\", "/"),
)

import app.core.db as db
import app.risk.guardrails as guardrails
from app.core.models import Alert, Order, Position
from app.execution.public_executor import PublicExecutor
from app.ingest.parser import parse_alert


def _stats(samples_s):
    """p50/p95/p99/max/avg in ms from a list of per-call seconds."""
    s = sorted(samples_s)
    n = len(s)
    def pct(p):
        return s[min(n - 1, int(p * n))] * 1e3
    return {
        "avg": statistics.mean(s) * 1e3,
        "p50": pct(0.50),
        "p95": pct(0.95),
        "p99": pct(0.99),
        "max": s[-1] * 1e3,
        "n": n,
    }


def _report(name, st, unit="ms"):
    scale = 1e3 if unit == "us" else 1.0
    print(f"[perf] {name:24s}: avg {st['avg']*scale:8.1f}{unit}  p50 {st['p50']*scale:8.1f}  "
          f"p95 {st['p95']*scale:8.1f}  p99 {st['p99']*scale:8.1f}  max {st['max']*scale:8.1f}  "
          f"({st['n']} iters)")


class _M:
    async def broadcast(self, d):
        pass


def _row(a, author):
    s = db.SessionLocal()
    try:
        r = Alert(
            author=author, action=a.action, raw_text="x", osi_symbol=a.osi_symbol,
            symbol=a.symbol, expiry=str(a.expiry) if a.expiry else None, strike=a.strike,
            option_type=a.option_type, alert_price=float(a.price) if a.price is not None else None,
            size_tag=a.size_tag, fraction=float(a.fraction), status="PENDING",
        )
        s.add(r); s.commit(); s.refresh(r); return r.id
    finally:
        s.close()


def test_parse_latency():
    """parse_alert is the per-message CPU cost — must stay well under a tick."""
    txt = "BOUGHT SPX 4/14 6970C $1.75 [SMALL]"
    for _ in range(200):           # warm regex caches
        parse_alert(txt)
    N = 5000
    t0 = time.perf_counter()
    for _ in range(N):
        parse_alert(txt)
    per = (time.perf_counter() - t0) / N
    print(f"\n[perf] parse_alert        : {per * 1e6:8.1f} us/call  ({N} iters)")
    assert per < 0.010, f"parse_alert too slow: {per * 1e3:.2f} ms/call (budget 10ms)"


def test_guardrail_latency():
    """check_guardrails is on every BUY before any network — must be cheap.

    Run against a fresh book so DUPLICATE / max-open guards don't short-circuit
    the path (we want the full-pass cost, the worst case)."""
    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()
    samples = []
    for k in range(2000):
        osi = f"SPXW260414C{6000 + (k % 500):05d}000"
        t0 = time.perf_counter()
        guardrails.check_guardrails(
            "BUY", osi, Decimal("1.75"), 1, alert_author="PerfPilot", size_tag="SMALL",
        )
        samples.append(time.perf_counter() - t0)
    st = _stats(samples)
    _report("check_guardrails", st)
    # Nominal is sub-millisecond; budget sized for order-of-magnitude regressions
    # (e.g. the dedup scan going O(n) on a growing book), not GC/scheduler jitter
    # on a busy box — 5ms flaked ~1/4 runs. A real regression blows well past 15ms.
    assert st["p95"] < 15.0, f"guardrail p95 too slow: {st['p95']:.2f} ms (budget 15ms)"


def test_compute_limit_latency():
    """_compute_limit prices every order (BUY slippage + tick rounding). Pure CPU,
    must be microseconds — it sits between the live quote and placeOrder."""
    ex = PublicExecutor(_M())
    base = Decimal("1.73")
    for _ in range(500):           # warm
        ex._compute_limit(base, "BUY")
    samples = []
    for _ in range(20000):
        t0 = time.perf_counter()
        ex._compute_limit(base, "BUY")
        samples.append(time.perf_counter() - t0)
    st = _stats(samples)
    _report("_compute_limit", st, unit="us")
    assert st["p95"] < 0.100, f"_compute_limit p95 too slow: {st['p95']:.3f} ms (budget 100us)"


def test_db_insert_latency():
    """Alert-row insert is the first persisted hop (discord_listener db_persist).
    SQLite commit cost — a slow disk here delays dispatch_to_executor."""
    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()
    a = parse_alert("BOUGHT SPX 4/14 6970C $1.75 [SMALL]")
    samples = []
    for _ in range(500):
        t0 = time.perf_counter()
        _row(a, "PerfPilot")
        samples.append(time.perf_counter() - t0)
    st = _stats(samples)
    _report("alert_insert+commit", st)
    assert st["p95"] < 50.0, f"db insert p95 too slow: {st['p95']:.1f} ms (budget 50ms)"


def test_order_decision_latency():
    """alert -> parse -> guardrail -> dry-run BUY placement, end to end.

    Unique strike per iter (no DUPLICATE guard) + reset the book each iter (no
    max-open-positions block) so every call exercises the full place path.
    """
    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()
    ex = PublicExecutor(_M())

    async def run():
        lat = []
        for k in range(40):
            s = db.SessionLocal()
            s.query(Position).delete(); s.query(Order).delete(); s.commit(); s.close()
            a = parse_alert(f"BOUGHT SPX 4/14 {6000 + k}C $1.75 [SMALL]")
            aid = _row(a, "PerfPilot")
            t0 = time.perf_counter()
            await ex.execute(a, aid, alert_author="PerfPilot")
            lat.append(time.perf_counter() - t0)
        lat.sort()
        n = len(lat)
        avg = statistics.mean(lat)
        p50 = lat[n // 2]
        p95 = lat[min(n - 1, int(0.95 * n))]
        print(f"[perf] order-decision(dry): avg {avg * 1e3:6.1f}ms  p50 {p50 * 1e3:6.1f}ms  "
              f"p95 {p95 * 1e3:6.1f}ms  ({n} iters)")
        assert p95 < 0.75, f"order-decision p95 too slow: {p95 * 1e3:.1f} ms (budget 750ms)"

    asyncio.run(run())
