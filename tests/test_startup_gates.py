"""
Regression tests for bugs found 2026-04-20:

1) app._start_forwarder() must launch when DISCORD_USER_TOKEN is set via env
   even if config.ini [forwarder] user_token is blank. Previously the gate
   only consulted config.ini, so the forwarder silently stayed disabled after
   secrets were rotated to .env — causing missed alerts and lost trades.

2) Alert / Position / Order to_dict() timestamps must be ISO-8601 with a
   trailing 'Z' so the browser parses naive-UTC DB values as UTC, not local.
"""

import asyncio
import os
import sys
import tempfile
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Point DB at an ephemeral file BEFORE db/models import.
_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp.close()
os.environ["TRADER_DATABASE_URL"] = f"sqlite:///{_tmp.name.replace(chr(92), '/')}"

import app.core.app as app_module  # noqa: E402
from app.core.config_manager import cfg  # noqa: E402
from app.core.models import Alert, Order, Position, _iso_utc  # noqa: E402


# ── Forwarder startup gate ───────────────────────────────────────────────────

def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) \
        if sys.platform == "win32" else asyncio.run(coro)


def test_forwarder_disabled_when_token_missing_everywhere(monkeypatch):
    """No env var, blank config → forwarder must NOT start."""
    monkeypatch.delenv("DISCORD_USER_TOKEN", raising=False)
    monkeypatch.setattr(cfg, "get", lambda *a, **kw: "", raising=False)
    monkeypatch.setattr(cfg, "getboolean", lambda *a, **kw: True, raising=False)

    with patch("app.core.app.run_forwarder", new=AsyncMock()) as mock_run:
        asyncio.run(app_module._start_forwarder())
        assert mock_run.await_count == 0, "Forwarder must not launch without a token"


def test_forwarder_starts_when_token_in_env_only(monkeypatch):
    """Env var set, config blank → forwarder MUST start. This is the bug that
    caused today's missed alerts: the gate in app.py only read config.ini."""
    monkeypatch.setenv("DISCORD_USER_TOKEN", "env-token-abc123")

    def _cfg_get(section, key, fallback=""):
        # Simulate blank config for forwarder/trading sections.
        return ""
    monkeypatch.setattr(cfg, "get", _cfg_get, raising=False)
    monkeypatch.setattr(cfg, "getboolean",
                        lambda *a, **kw: True, raising=False)

    with patch("app.core.app.run_forwarder", new=AsyncMock()) as mock_run:
        asyncio.run(app_module._start_forwarder())
        assert mock_run.await_count == 1, \
            "Forwarder gate must honor DISCORD_USER_TOKEN env var"


def test_forwarder_disabled_when_feature_flag_off(monkeypatch):
    """forwarder_enabled=false should override even a valid token."""
    monkeypatch.setenv("DISCORD_USER_TOKEN", "env-token-abc123")
    monkeypatch.setattr(cfg, "get", lambda *a, **kw: "", raising=False)
    monkeypatch.setattr(cfg, "getboolean",
                        lambda *a, **kw: False, raising=False)

    with patch("app.core.app.run_forwarder", new=AsyncMock()) as mock_run:
        asyncio.run(app_module._start_forwarder())
        assert mock_run.await_count == 0


def test_forwarder_rejects_placeholder_token(monkeypatch):
    """'YOUR_DISCORD_TOKEN' placeholder must be treated as unset."""
    monkeypatch.setenv("DISCORD_USER_TOKEN", "YOUR_DISCORD_TOKEN_HERE")
    monkeypatch.setattr(cfg, "get", lambda *a, **kw: "", raising=False)
    monkeypatch.setattr(cfg, "getboolean",
                        lambda *a, **kw: True, raising=False)

    with patch("app.core.app.run_forwarder", new=AsyncMock()) as mock_run:
        asyncio.run(app_module._start_forwarder())
        assert mock_run.await_count == 0


# ── Timestamp serialization ──────────────────────────────────────────────────

def test_iso_utc_appends_z_to_naive():
    """_iso_utc must tag naive DB values as UTC via 'Z' suffix."""
    naive = datetime(2026, 4, 20, 14, 7, 24, 409000)
    assert _iso_utc(naive) == "2026-04-20T14:07:24.409000Z"


def test_iso_utc_preserves_aware_as_z():
    """Timezone-aware datetime must still emit trailing 'Z', never '+00:00'."""
    aware = datetime(2026, 4, 20, 14, 7, 24, tzinfo=timezone.utc)
    out = _iso_utc(aware)
    assert out.endswith("Z")
    assert "+00:00" not in out


def test_iso_utc_none_passthrough():
    assert _iso_utc(None) is None


def test_alert_to_dict_timestamp_is_utc_iso():
    a = Alert(
        timestamp=datetime(2026, 4, 20, 14, 7, 24),
        author="x", action="BUY", raw_text="t",
        osi_symbol="X", symbol="X", expiry=None,
        strike=None, option_type=None, alert_price=0.0,
        size_tag=None, fraction=1.0,
    )
    ts = a.to_dict()["timestamp"]
    assert ts.endswith("Z"), f"expected Z-suffix, got {ts!r}"


def test_position_to_dict_timestamps_are_utc_iso():
    p = Position(
        osi_symbol="X", symbol="X", expiry="2026-04-20",
        strike=100, option_type="C",
        total_contracts=1, remaining=1,
        avg_price=1.0, current_price=1.0,
        open_time=datetime(2026, 4, 20, 14, 7, 24),
        close_time=datetime(2026, 4, 20, 15, 0, 0),
    )
    d = p.to_dict()
    assert d["openTime"].endswith("Z")
    assert d["closeTime"].endswith("Z")


def test_order_to_dict_timestamps_are_utc_iso():
    o = Order(
        osi_symbol="X", side="BUY", quantity=1, limit_price=1.0,
        trigger="DISCORD",
        placed_at=datetime(2026, 4, 20, 14, 7, 24),
        filled_at=None,
    )
    d = o.to_dict()
    assert d["placedAt"].endswith("Z")
    assert d["filledAt"] is None
