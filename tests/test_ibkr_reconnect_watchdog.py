"""
Reconnect watchdog: after a gateway drop the bot must re-establish the session
in the background, instead of leaving the next alert to pay a 20s+ connect on
the money path.

The 2026-08-19 ibkr-oci journal is the case this exists for: IB Gateway was
down ~10 minutes over a missed 2FA push, and nothing on the bot side was trying
to come back.
"""
import asyncio

import pytest

import app.execution.ibkr_sdk_bridge as b


@pytest.fixture(autouse=True)
def _reset_watchdog(monkeypatch):
    monkeypatch.setattr(b, "_ib", None, raising=False)
    monkeypatch.setattr(b, "_reconnect_task", None, raising=False)
    # Keep the test fast: no real backoff sleeps.
    monkeypatch.setattr(b, "_reconnect_max_delay", lambda: 0.01, raising=False)
    monkeypatch.setattr(b, "_reconnect_initial_delay", lambda: 0.01, raising=False)
    yield


async def _drain(task, timeout=2.0):
    if task is not None:
        await asyncio.wait_for(task, timeout=timeout)


async def test_watchdog_retries_until_the_gateway_returns(monkeypatch):
    """A gateway that refuses the first two connects must still be picked up."""
    attempts = {"n": 0}

    async def fake_get_ib():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise ConnectionRefusedError("gateway still restarting")
        b._ib = object()
        return b._ib

    monkeypatch.setattr(b, "_get_ib", fake_get_ib)
    monkeypatch.setattr(b, "_reconnect_enabled", lambda: True)
    monkeypatch.setattr(b, "_post_ibkr_alarm", lambda *a, **k: asyncio.sleep(0))
    monkeypatch.setattr(b, "_set_md_state", lambda *a, **k: None)

    b._on_ib_disconnect()
    await _drain(b._reconnect_task)

    assert attempts["n"] == 3, "watchdog gave up before the gateway came back"
    assert b._ib is not None


async def test_watchdog_is_a_no_op_when_disabled(monkeypatch):
    """The revert knob must restore the old lazy-only behavior exactly."""
    called = {"n": 0}

    async def fake_get_ib():
        called["n"] += 1

    monkeypatch.setattr(b, "_get_ib", fake_get_ib)
    monkeypatch.setattr(b, "_reconnect_enabled", lambda: False)
    monkeypatch.setattr(b, "_post_ibkr_alarm", lambda *a, **k: asyncio.sleep(0))
    monkeypatch.setattr(b, "_set_md_state", lambda *a, **k: None)

    b._on_ib_disconnect()
    await asyncio.sleep(0.05)

    assert b._reconnect_task is None
    assert called["n"] == 0


async def test_only_one_watchdog_runs_at_a_time(monkeypatch):
    """Flapping sockets fire disconnectedEvent repeatedly; that must not stack
    N concurrent reconnect loops all hammering the gateway."""
    async def slow_get_ib():
        await asyncio.sleep(0.2)
        raise ConnectionRefusedError("down")

    monkeypatch.setattr(b, "_get_ib", slow_get_ib)
    monkeypatch.setattr(b, "_reconnect_enabled", lambda: True)
    monkeypatch.setattr(b, "_post_ibkr_alarm", lambda *a, **k: asyncio.sleep(0))
    monkeypatch.setattr(b, "_set_md_state", lambda *a, **k: None)

    b._on_ib_disconnect()
    first = b._reconnect_task
    b._on_ib_disconnect()
    b._on_ib_disconnect()

    assert b._reconnect_task is first, "a second disconnect spawned a second watchdog"
    first.cancel()
    try:
        await first
    except asyncio.CancelledError:
        pass
