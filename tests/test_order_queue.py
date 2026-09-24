"""
Tests for the serialized order-execution worker (app/execution/order_queue.py).

Covers the GH #9 guarantee (no two executions overlap) plus the legacy-fallback
and fault-isolation contracts dispatch relies on.
"""
import asyncio

import app.execution.order_queue as oq


def test_dispatch_falls_back_when_worker_not_running():
    """With no worker started, dispatch returns False so the caller can use the
    legacy concurrent create_task path."""
    async def run():
        # Fresh module state — no worker.
        oq._worker_task = None
        oq._queue = None
        assert oq.is_running() is False
        assert oq.dispatch_execution(lambda: asyncio.sleep(0), "x") is False

    asyncio.run(run())


def test_executions_run_one_at_a_time_in_order():
    """The single consumer must serialize: no two jobs' [start,end] intervals
    overlap, and they complete FIFO."""
    async def run():
        oq._worker_task = None
        oq._queue = None
        oq.start_order_worker()

        active = 0
        max_active = 0
        order = []

        def make_job(i):
            async def job():
                nonlocal active, max_active
                active += 1
                max_active = max(max_active, active)
                await asyncio.sleep(0.01)   # hold the slot so overlap would show
                order.append(i)
                active -= 1
            return job

        for i in range(20):
            assert oq.dispatch_execution(make_job(i), f"job-{i}") is True

        # Drain.
        while oq.queue_depth() > 0 or active > 0:
            await asyncio.sleep(0.01)

        assert max_active == 1, f"executions overlapped (max concurrent={max_active})"
        assert order == list(range(20)), f"not FIFO: {order}"

        oq._worker_task.cancel()

    asyncio.run(run())


def test_exits_drain_ahead_of_entries():
    """SELL/close_all (PRIORITY_EXIT) must run before queued BUYs, while FIFO
    holds within each lane."""
    async def run():
        oq._worker_task = None
        oq._queue = None
        oq.start_order_worker()

        ran = []

        def make(label):
            async def job():
                await asyncio.sleep(0.005)
                ran.append(label)
            return job

        # Block the worker on the first item so the rest queue up behind it,
        # then enqueue a mix of entries and exits.
        gate = asyncio.Event()

        async def blocker():
            await gate.wait()
        assert oq.dispatch_execution(blocker, "blocker", oq.PRIORITY_ENTRY) is True

        oq.dispatch_execution(make("buy1"), "buy1", oq.PRIORITY_ENTRY)
        oq.dispatch_execution(make("buy2"), "buy2", oq.PRIORITY_ENTRY)
        oq.dispatch_execution(make("sell1"), "sell1", oq.PRIORITY_EXIT)
        oq.dispatch_execution(make("sell2"), "sell2", oq.PRIORITY_EXIT)

        gate.set()  # release the blocker → priority ordering decides the rest
        while oq.queue_depth() > 0 or len(ran) < 4:
            await asyncio.sleep(0.005)

        # Exits first (FIFO within lane), then entries (FIFO within lane).
        assert ran == ["sell1", "sell2", "buy1", "buy2"], ran

        oq._worker_task.cancel()

    asyncio.run(run())


def test_one_failing_job_does_not_kill_worker():
    """A raising job is logged and skipped; subsequent jobs still run."""
    async def run():
        oq._worker_task = None
        oq._queue = None
        oq.start_order_worker()

        ran = []

        async def boom():
            raise RuntimeError("broker exploded")

        async def good():
            ran.append("good")

        assert oq.dispatch_execution(boom, "boom") is True
        assert oq.dispatch_execution(good, "good") is True

        while oq.queue_depth() > 0 or not ran:
            await asyncio.sleep(0.01)

        assert ran == ["good"]
        assert oq.is_running() is True   # survived the exception

        oq._worker_task.cancel()

    asyncio.run(run())
