"""Market-data farm status — the bridge state machine and the panel line.

The case worth protecting is *unknown*. A failed scrape, a bot that is down, or
a session where IBKR has not yet reported farm state must render as "no answer",
never as "farm broken": a false red on this line invites a gateway restart that
costs a Second Factor Authentication tap and fixes nothing. The inverse matters
too — a genuinely broken farm must not read as healthy, because an open socket
with dead quotes cannot price an order.
"""
import sys
import types

import pytest


@pytest.fixture
def bridge():
    """Fresh farm state per test — it is module-global and per-session."""
    from app.execution import ibkr_sdk_bridge as b
    b._set_md_state("unknown", "test reset")
    b._md_ts = None
    return b


def test_farm_state_starts_unknown_not_broken(bridge):
    # Absent evidence is not evidence of failure: metrics leaves the gauge unset
    # for "unknown", which is what keeps the panel from claiming a dead farm on
    # a box that simply has no IBKR session yet.
    assert bridge.market_data_status()["state"] == "unknown"
    assert bridge.market_data_status()["age_seconds"] is None


@pytest.mark.parametrize("code,expected", [
    (2104, "ok"),          # market data farm connection is OK
    (2106, "ok"),          # HMDS data farm connection is OK
    (2108, "ok"),          # inactive but available on demand — not a failure
    (2103, "broken"),      # market data farm connection is broken
    (2105, "broken"),      # HMDS data farm connection is broken
    (2119, "connecting"),
])
def test_ibkr_farm_codes_map_to_state(bridge, code, expected):
    bridge._on_ib_error(-1, code, f"farm message {code}")
    st = bridge.market_data_status()
    assert st["state"] == expected
    assert st["age_seconds"] is not None


def test_unrelated_error_codes_leave_farm_state_alone(bridge):
    bridge._on_ib_error(-1, 2104, "farm ok")
    bridge._on_ib_error(1, 201, "order rejected")   # a normal API error
    assert bridge.market_data_status()["state"] == "ok"


def test_disconnect_clears_farm_state(bridge):
    bridge._on_ib_error(-1, 2104, "farm ok")
    assert bridge.market_data_status()["state"] == "ok"
    bridge._on_ib_disconnect()
    # Holding "ok" across a dropped socket would show green off a reading from
    # a session that no longer exists.
    assert bridge.market_data_status()["state"] == "unknown"


# ── panel rendering ─────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def control():
    """trader_control calls docker.from_env() and exits without a bot token at
    import time. Stub the daemon and supply env so the module is importable in
    CI, where there is neither."""
    stub = types.ModuleType("docker")
    errors = types.ModuleType("docker.errors")
    for name in ("APIError", "NotFound", "ContainerError"):
        setattr(errors, name, type(name, (Exception,), {}))
    stub.errors = errors
    stub.from_env = lambda *a, **k: object()
    saved = {k: sys.modules.get(k) for k in ("docker", "docker.errors")}
    sys.modules["docker"], sys.modules["docker.errors"] = stub, errors

    import os
    os.environ.setdefault("DISCORD_CONTROL_BOT_TOKEN", "test-token")
    os.environ.setdefault("DISCORD_CONTROL_CHANNEL_ID", "1")
    os.environ.setdefault("DISCORD_AUTHORIZED_USER_ID", "2")
    try:
        from app.analytics import trader_control
        yield trader_control
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v


@pytest.mark.parametrize("raw", ["", "  ", "nan-ish", "None"])
def test_no_answer_renders_unknown_never_broken(control, raw):
    ok, line = control._market_data_line(raw.strip(), "")
    assert ok is None, f"{raw!r} must not assert a broken farm"
    assert "unknown" in line.lower()
    assert "NOT FLOWING" not in line


def test_farm_ok_renders_flowing(control):
    ok, line = control._market_data_line("1", "12")
    assert ok is True
    assert "flowing" in line.lower()
    assert "12s ago" in line


def test_farm_broken_renders_not_flowing(control):
    ok, line = control._market_data_line("0", "300")
    assert ok is False
    assert "NOT FLOWING" in line
    assert "5m ago" in line


def test_connecting_is_not_broken(control):
    ok, line = control._market_data_line("-1", "3")
    assert ok is None          # mid-reconnect must not flash red
    assert "connecting" in line.lower()
