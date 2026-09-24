"""Regression: SELL re-peg must not oversell when a fill races the cancel.

Root cause of the UNH 430C double-sell (2026-07-08): IBKRExecutor.cancel_order
fired ib.cancelOrder() (fire-and-forget) and marked the order CANCELLED locally
without waiting for a terminal state, so the SELL re-peg placed a replacement
while the "cancelled" order was still live on IBKR — both filled. The fix awaits
a terminal state and reports filled_during_cancel so the re-peg skips re-placing.

These cover the terminal-await primitive across its branches.
"""
import asyncio

import app.execution.ibkr_broker as ib


class _OS:
    def __init__(self, status):
        self.status = status


class _FakeTrade:
    """Reports a status sequence; tick() advances it (simulates async updates)."""
    def __init__(self, seq):
        self.seq = list(seq)
        self.i = 0

    @property
    def orderStatus(self):
        return _OS(self.seq[min(self.i, len(self.seq) - 1)])

    def isDone(self):
        return self.orderStatus.status in ("Filled", "Cancelled", "ApiCancelled", "Inactive")

    def tick(self):
        self.i += 1


class _Stub:
    pass


_Stub._await_trade_terminal = ib.IBKRExecutor._await_trade_terminal


async def _drive(trade, ticks=8, dt=0.05):
    for _ in range(ticks):
        await asyncio.sleep(dt)
        trade.tick()


def _run(coro):
    return asyncio.run(coro)


def test_fill_during_cancel_returns_filled():
    async def go():
        t = _FakeTrade(["Submitted", "Submitted", "Filled"])
        asyncio.create_task(_drive(t))
        return await _Stub()._await_trade_terminal(t, 2.0)
    assert _run(go()) == "FILLED"


def test_clean_cancel_returns_cancelled():
    async def go():
        t = _FakeTrade(["Submitted", "PendingCancel", "Cancelled"])
        asyncio.create_task(_drive(t))
        return await _Stub()._await_trade_terminal(t, 2.0)
    assert _run(go()) == "CANCELLED"


def test_timeout_zero_is_legacy_no_wait():
    # timeout<=0 = legacy fire-and-forget: trust the current status only.
    # Non-terminal → UNCONFIRMED (re-peg must hold off); Filled → FILLED.
    assert _run(_Stub()._await_trade_terminal(_FakeTrade(["Submitted"]), 0)) == "UNCONFIRMED"
    assert _run(_Stub()._await_trade_terminal(_FakeTrade(["Filled"]), 0)) == "FILLED"


def test_timeout_expires_returns_unconfirmed():
    # Never goes terminal within the window → UNCONFIRMED (order may still be
    # live and could fill; re-peg must NOT re-place).
    async def go():
        return await _Stub()._await_trade_terminal(_FakeTrade(["Submitted"]), 0.3)
    assert _run(go()) == "UNCONFIRMED"


# ── Public.com cancel terminal-confirm (real-money double-sell guard) ──────────
import app.execution.public_executor as _pe


class _POStatus:
    def __init__(self, v): self.value = v


class _POrder:
    def __init__(self, oid, status, fq=0):
        self.order_id = oid
        self.status = _POStatus(status)
        self.filled_quantity = fq


class _PPortfolio:
    def __init__(self, orders): self.orders = orders


class _PClient:
    """Returns a portfolio snapshot per get_portfolio() call, advancing a sequence."""
    def __init__(self, seq): self.seq = seq; self.i = 0

    async def get_portfolio(self):
        p = self.seq[min(self.i, len(self.seq) - 1)]; self.i += 1
        return _PPortfolio(p)


class _PStub:
    def __init__(self, client): self._c = client
    async def _get_client(self): return self._c


_PStub._await_public_order_terminal = _pe.PublicExecutor._await_public_order_terminal


def test_public_fill_during_cancel_filled():
    c = _PClient([[_POrder("A", "NEW")], [_POrder("A", "FILLED")]])
    assert _run(_PStub(c)._await_public_order_terminal("A", 2.0)) == "FILLED"


def test_public_clean_cancel_cancelled():
    c = _PClient([[_POrder("A", "PENDING_CANCEL")], [_POrder("A", "CANCELLED")]])
    assert _run(_PStub(c)._await_public_order_terminal("A", 2.0)) == "CANCELLED"


def test_public_partial_fill_filled():
    c = _PClient([[_POrder("A", "PARTIALLY_FILLED", fq=1)]])
    assert _run(_PStub(c)._await_public_order_terminal("A", 2.0)) == "FILLED"


def test_public_timeout_unconfirmed():
    c = _PClient([[_POrder("A", "NEW")]])
    assert _run(_PStub(c)._await_public_order_terminal("A", 0.6)) == "UNCONFIRMED"


def test_public_timeout_zero_legacy():
    # timeout<=0 short-circuits to UNCONFIRMED (Public has no get_order; the
    # re-peg holds off rather than risk an unconfirmed re-place).
    assert _run(_PStub(_PClient([[_POrder("A", "FILLED")]]))._await_public_order_terminal("A", 0)) == "UNCONFIRMED"
