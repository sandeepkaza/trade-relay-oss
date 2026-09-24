"""
Regression guard for the limit-breach race (GH #9).

The hazard: an open-position COUNT and the subsequent INSERT are not one
transaction, so concurrent BUYs can both read open<cap and both insert →
the cap is breached. The serialized order worker (order_queue) makes that
count→insert sequence effectively atomic — only one job runs at a time.

This drives many concurrent "place" jobs (each = the same count→insert the
guardrail+executor perform, with a sleep widening the race window) through the
real worker and asserts the cap holds.
"""
import asyncio

import app.core.db as db
import app.execution.order_queue as oq
from app.core.models import Position

CAP = 5


def _open_count(s):
    return s.query(Position).filter(
        Position.status.in_(["OPEN", "PARTIAL"]),
        Position.remaining > 0,
    ).count()


def _make_buy_job(i):
    """Mimic the guardrail count→insert: read open count, and if under cap,
    open a position. The await between read and write is the race window."""
    async def job():
        s = db.SessionLocal()
        try:
            if _open_count(s) < CAP:
                await asyncio.sleep(0.005)          # widen the window
                s.add(Position(
                    osi_symbol=f"SPXW260101C{6000+i:05d}000", symbol="SPX",
                    status="OPEN", total_contracts=1, remaining=1, avg_price=1.0,
                ))
                s.commit()
        finally:
            s.close()
    return job


async def _drain(timeout=10.0):
    """Wait for the queue to empty, bounded so a misbehaving worker can never
    hang the suite."""
    deadline = asyncio.get_event_loop().time() + timeout
    while oq.queue_depth() > 0:
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError("order queue did not drain — worker stalled")
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.1)   # let the in-flight job commit


async def _teardown_worker():
    t = oq._worker_task
    if t is not None:
        t.cancel()
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass
    oq._worker_task = None
    oq._queue = None


def test_serialized_worker_holds_position_cap_under_concurrency():
    async def run():
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()
        # Fresh worker bound to THIS loop (avoid a stale task from a prior test).
        oq._worker_task = None
        oq._queue = None
        oq.start_order_worker()
        try:
            for i in range(40):                      # 40 concurrent BUYs, cap 5
                assert oq.dispatch_execution(_make_buy_job(i), f"buy-{i}") is True
            await _drain()
            s = db.SessionLocal()
            try:
                n = _open_count(s)
            finally:
                s.close()
            assert n == CAP, f"cap breached: {n} open (limit {CAP})"
        finally:
            await _teardown_worker()

    asyncio.run(run())
