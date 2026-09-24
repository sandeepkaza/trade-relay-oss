"""
Load / scaling benchmarks (review §9). Report-only: prints throughput + latency
and asserts only generous, order-of-magnitude budgets so CI jitter never fails.

Concurrency is driven through the real serialized order worker (writes serialize
there) — deliberately NOT 40+ raw concurrent SQLite writers, which thrash the
single writer lock for minutes. This measures the system's true sustained order
throughput and proves the open-position cap holds at scale (GH #9).
"""
import asyncio
import time

import pytest

import app.core.db as db
import app.execution.order_queue as oq
from app.core.models import Position, Order, Alert

CAP = 5


def _open_count(s):
    return s.query(Position).filter(
        Position.status.in_(["OPEN", "PARTIAL"]), Position.remaining > 0,
    ).count()


def _buy_job(i):
    async def job():
        s = db.SessionLocal()
        try:
            if _open_count(s) < CAP:
                await asyncio.sleep(0)   # yield — exercise the await window
                s.add(Position(osi_symbol=f"SPXW260101C{6000+i:06d}", symbol="SPX",
                               status="OPEN", total_contracts=1, remaining=1, avg_price=1.0))
                s.commit()
        finally:
            s.close()
    return job


async def _teardown():
    t = oq._worker_task
    if t is not None:
        t.cancel()
        try:
            await t
        except BaseException:
            pass
    oq._worker_task = None
    oq._queue = None


@pytest.mark.parametrize("n", [100, 500, 1000])
def test_concurrent_dispatch_holds_cap_and_reports_throughput(n):
    async def run():
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()
        oq._worker_task = None
        oq._queue = None
        oq.start_order_worker(maxsize=max(2000, n + 10))
        try:
            t0 = time.perf_counter()
            for i in range(n):
                assert oq.dispatch_execution(_buy_job(i), f"b{i}") is True
            deadline = time.perf_counter() + 30
            while oq.queue_depth() > 0:
                if time.perf_counter() > deadline:
                    raise AssertionError("worker stalled draining load")
                await asyncio.sleep(0.005)
            await asyncio.sleep(0.05)
            dt = time.perf_counter() - t0

            s = db.SessionLocal()
            try:
                opened = _open_count(s)
            finally:
                s.close()
            print(f"\n[load] {n:5d} jobs drained in {dt*1e3:7.1f}ms "
                  f"({n/dt:7.0f} job/s) — open={opened} (cap {CAP})")
            assert opened == CAP, f"cap breached at n={n}: {opened}"
        finally:
            await _teardown()
    asyncio.run(run())


def test_guardrail_latency_vs_db_growth():
    """Seed a large book of history and time the BUY DB-guard path. Validates the
    composite indexes keep it cheap as rows accumulate."""
    from datetime import datetime, timezone
    from decimal import Decimal
    import app.risk.guardrails as g

    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()
    now = datetime.now(timezone.utc)
    s = db.SessionLocal()
    try:
        for i in range(5000):
            s.add(Order(public_order_id=f"o{i}", side="BUY", status="FILLED", placed_at=now))
            s.add(Alert(action="BUY", status="PENDING", timestamp=now,
                        discord_message_id=f"m{i}"))
        # A handful of open positions (the COUNT target).
        for i in range(3):
            s.add(Position(osi_symbol=f"SPXW260101C{7000+i:06d}", symbol="SPX",
                           status="OPEN", total_contracts=1, remaining=1,
                           avg_price=1.0, current_price=1.1))
        s.commit()
    finally:
        s.close()

    N = 200
    t0 = time.perf_counter()
    for _ in range(N):
        g._check_buy_guards("SPXW260101C06000000", Decimal("1.5"), 1)
    per_ms = (time.perf_counter() - t0) / N * 1e3
    print(f"\n[load] _check_buy_guards over 5k orders/5k alerts: {per_ms:.2f} ms/call")
    assert per_ms < 200.0, f"BUY guard too slow at scale: {per_ms:.1f} ms"
