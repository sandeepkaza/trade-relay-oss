"""
Gateway start/stop on the Discord control panel.

The button that matters is Stop: a stopped gateway is not a paused one. Open
positions stop being priced, the exit engine cannot fill, and analyst SELLs are
dropped — and getting back up needs a human 2FA tap, so "stop" is not a toggle.
These pin the two properties that keep that safe: the command allowlist is
closed, and the confirmation tells the truth about the live book.
"""
import sys
import types

import pytest

# trader_control calls docker.from_env() at import time. There is no daemon in
# CI (or on a dev laptop), and the SDK blocks retrying rather than failing fast,
# so importing the module for real hangs the suite. The panel logic under test
# never touches the daemon — every host command goes through _host_exec, which
# the tests stub — so a stand-in client is enough.
_fake_docker = types.ModuleType("docker")
_fake_errors = types.ModuleType("docker.errors")


class _APIError(Exception):
    pass


class _NotFound(Exception):
    pass


class _ContainerError(Exception):
    pass


_fake_errors.APIError = _APIError
_fake_errors.NotFound = _NotFound
_fake_errors.ContainerError = _ContainerError
_fake_docker.errors = _fake_errors
_fake_docker.from_env = lambda *a, **k: types.SimpleNamespace(
    containers=types.SimpleNamespace(run=lambda *a, **k: b"", get=lambda *a, **k: None),
)
sys.modules.setdefault("docker", _fake_docker)
sys.modules.setdefault("docker.errors", _fake_errors)

import app.analytics.trader_control as tc


def test_start_and_stop_are_allowlisted_and_argv_is_fixed():
    """Never build a host command from user input — the panel may only ask for
    a key that is already in the table."""
    for key in ("start", "stop", "restart", "state", "logs", "opens"):
        assert key in tc._IBGW_CMDS, f"{key} missing from the allowlist"
    assert tc._IBGW_CMDS["start"] == ["systemctl", "start", tc.IBGW_UNIT]
    assert tc._IBGW_CMDS["stop"] == ["systemctl", "stop", tc.IBGW_UNIT]


def test_unknown_key_is_refused_not_executed(monkeypatch):
    called = {"n": 0}
    monkeypatch.setattr(tc.dclient.containers, "run",
                        lambda *a, **k: called.__setitem__("n", called["n"] + 1))
    out = tc._host_exec("stop; rm -rf /")
    assert "refused" in out
    assert called["n"] == 0, "a non-allowlisted key reached the host"


def test_stop_warning_names_the_open_position_count(monkeypatch):
    monkeypatch.setattr(tc, "_host_exec", lambda key, timeout=60: "3\n")
    warning = tc._stop_warning()
    assert "3 open position" in warning
    assert "unexitable" in warning


def test_stop_warning_treats_unknown_as_dangerous(monkeypatch):
    """A bot that is down is exactly when you know least about the book —
    unknown must not read like zero."""
    monkeypatch.setattr(tc, "_host_exec", lambda key, timeout=60: "")
    warning = tc._stop_warning()
    assert "assume the book is live" in warning
    assert tc.open_position_count() is None


def test_stop_warning_is_calm_when_the_book_is_flat(monkeypatch):
    monkeypatch.setattr(tc, "_host_exec", lambda key, timeout=60: "0")
    assert "No open positions" in tc._stop_warning()


def test_stop_routes_through_the_confirm_view():
    """Restart is confirmed; stop is strictly worse, so it must be too."""
    import inspect
    src = inspect.getsource(tc.TraderPanel.b_gw_stop)
    assert 'ConfirmView("gw_stop"' in src
    confirm_src = inspect.getsource(tc.ConfirmView.confirm)
    assert 'self.action == "gw_stop"' in confirm_src, "confirming gw_stop would fall through to do_action"


def test_gateway_buttons_disappear_on_a_host_without_a_gateway(monkeypatch):
    """relaybot-vm has no IB Gateway; the panel must keep its old shape there
    rather than showing five dead buttons."""
    import inspect
    src = inspect.getsource(tc.TraderPanel.__init__)
    for name in ("b_gw_status", "b_gw_logs", "b_gw_restart", "b_gw_start", "b_gw_stop"):
        assert name in src, f"{name} is not removed when IBGW_ENABLED is false"
