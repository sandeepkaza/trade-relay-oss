"""
order_queue.py — single-consumer serialization for order execution.

Why this exists
---------------
Ingest dispatches every alert with `asyncio.create_task(executor.execute(...))`
(discord_listener), so N alerts arriving together run their guardrail →
place-order paths *concurrently* on the event loop. `check_guardrails` is
synchronous (atomic per call), but the gap between the guardrail COUNT and the
order-row INSERT is not transactional: two BUYs for different OSIs can both read
`open < max_open_positions` and both place → the cap is breached (GH #9). The
same window threatens max_daily_trades and the daily-loss halt.

Funnelling every execution through ONE worker draining the queue makes the
count→insert sequence effectively atomic (only one `execute` runs at a time) and
gives natural ordering + backpressure during an alert storm. It is also the
foundation the pre-placement idempotency guard (GH #8) builds on.

Exit-priority lane
------------------
The queue is a priority queue: exits (SELL / close_all) drain ahead of entries
(BUY). During a "flatten now" cascade an exit must not wait behind a backlog of
BUYs — closing risk is always more urgent than opening it. Ordering is FIFO
*within* a priority lane (a monotonic sequence breaks ties), so analyst SELL
ladders still fill in order.

Rollback
--------
Gated by [trading] serialized_order_execution (default true). Flip to false to
revert to the legacy concurrent create_task dispatch with no other change —
`dispatch_execution` falls back automatically when the worker isn't running.
"""

import asyncio
import itertools
import logging
from typing import Awaitable, Callable, Optional

log = logging.getLogger(__name__)

# A job is a zero-arg factory returning the execution awaitable. We store the
# factory (not a bare coroutine) so the worker creates the coroutine at run time
# — keeps semantics identical to the legacy `create_task(execute(...))` and
# avoids "coroutine was never awaited" if a job is ever dropped.
Job = Callable[[], Awaitable[None]]

PRIORITY_EXIT = 0    # SELL / close_all — drains first
PRIORITY_ENTRY = 1   # BUY

# Queue item: (priority, seq, factory, name). `seq` is a monotonic tiebreaker so
# items never compare the (uncomparable) factory and FIFO holds within a lane.
_queue: "Optional[asyncio.PriorityQueue[tuple[int, int, Job, str]]]" = None
_worker_task: "Optional[asyncio.Task]" = None
_seq = itertools.count()


def is_running() -> bool:
    return _worker_task is not None and not _worker_task.done()


def start_order_worker(maxsize: int = 1000) -> None:
    """Start the single order-execution consumer. Idempotent — a second call
    while the worker is alive is a no-op. Must be called from inside the running
    event loop (e.g. start_discord_listener)."""
    global _queue, _worker_task
    if is_running():
        return
    _queue = asyncio.PriorityQueue(maxsize=maxsize)
    _worker_task = asyncio.create_task(_worker(), name="order_queue.worker")
    log.info("[ORDER_QUEUE] serialized order worker started (maxsize=%d, exit-priority)", maxsize)


def _job_timeout() -> float:
    """Max seconds a single execution job may hold the worker. A job only PLACES
    the order — fill polling runs as a separate background task — so this bounds
    the place + guardrail path, not the fill wait. 0 disables. Hot-reloadable."""
    try:
        from app.core.config_manager import cfg
        return cfg.getfloat("trading", "order_job_timeout_seconds", fallback=90.0)
    except Exception:
        return 90.0


def _publish_depth() -> None:
    """Track live backlog after a dequeue so an order_queue_depth alert reflects
    the actual (draining) depth, not just the enqueue-time peak."""
    try:
        from app.core import metrics
        metrics.set_gauge("order_queue_depth", _queue.qsize() if _queue else 0)
    except Exception:
        pass


async def _worker() -> None:
    assert _queue is not None
    while True:
        _prio, _s, factory, name = await _queue.get()
        timeout = _job_timeout()
        try:
            if timeout and timeout > 0:
                await asyncio.wait_for(factory(), timeout=timeout)
            else:
                await factory()
        except asyncio.CancelledError:
            # Shutting down — re-raise so the task actually stops.
            _queue.task_done()
            raise
        except asyncio.TimeoutError:
            # A job wedged past the deadline (hung broker POST / quote fetch).
            # wait_for already cancelled the coroutine; abandon it so the queue
            # keeps draining — a stuck ENTRY must never freeze pending EXITS.
            log.error("[ORDER_QUEUE] execution job %r exceeded %.0fs — abandoned "
                      "to keep the queue draining", name, timeout)
            try:
                from app.core import metrics
                metrics.inc("order_queue_timeout_total")
            except Exception:
                pass
            _queue.task_done()
        except Exception:
            # One bad order must never kill the worker; the next alert still runs.
            log.exception("[ORDER_QUEUE] execution job %r failed", name)
            _queue.task_done()
        else:
            _queue.task_done()
        _publish_depth()


def dispatch_execution(factory: Job, name: str, priority: int = PRIORITY_ENTRY) -> bool:
    """Enqueue an execution job on the serial worker.

    priority: PRIORITY_EXIT (0) for SELL/close_all so they jump ahead of BUYs;
    PRIORITY_ENTRY (1) for BUYs (default).

    Returns True if it was queued. Returns False when the worker isn't running
    (legacy mode / startup race) so the caller can fall back to the old
    concurrent create_task path. Also falls back-by-returning-False if the queue
    is full, so a backlog can never silently drop an order — the caller logs and
    runs it concurrently rather than losing it.
    """
    if not is_running() or _queue is None:
        return False
    try:
        _queue.put_nowait((priority, next(_seq), factory, name))
        try:
            from app.core import metrics
            metrics.set_gauge("order_queue_depth", _queue.qsize())
        except Exception:
            pass
        return True
    except asyncio.QueueFull:
        log.error("[ORDER_QUEUE] queue full (%d) — falling back to concurrent dispatch for %s",
                  _queue.maxsize, name)
        try:
            from app.core import metrics
            metrics.inc("order_queue_fallback_total")
        except Exception:
            pass
        return False


def queue_depth() -> int:
    return _queue.qsize() if _queue is not None else 0
