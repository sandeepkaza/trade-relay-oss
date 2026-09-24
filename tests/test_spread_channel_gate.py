"""The gate that decides whether a message is parsed as a spread.

This is a safety gate, not a routing convenience: when it fails open, matae's
"CLOSED: CCS SPX 7390/95 $1.20 ALL OUT" reaches the single-leg parser, which
reads it as a symbol-less close_all SELL. On the live IBKR primary that is the
2026-06-11 book-flattening incident.

The two arrival paths carry the source differently, so both are pinned here.
"""
import types

import pytest

from app.core.config_manager import cfg
from app.ingest.discord_listener import _spread_channel

SPREAD_ID = "100000000000017776"
DEST_ID = 100000000000018887          # shared #general the forwarder posts into


def _msg(channel_id, display_name):
    return types.SimpleNamespace(
        channel=types.SimpleNamespace(id=channel_id),
        author=types.SimpleNamespace(display_name=display_name),
    )


@pytest.fixture
def on(monkeypatch):
    def _cfg(section, key, fallback=None, **kw):
        if (section, key) == ("trading", "spread_channel_ids"):
            return f"{SPREAD_ID}, twi-spreads"
        return cfg.get(section, key, fallback=fallback, **kw)

    monkeypatch.setattr(cfg, "get", _cfg)
    monkeypatch.setattr(cfg, "getboolean",
                        lambda s, k, fallback=None, **kw: True if k == "spreads_enabled" else fallback)


def test_directly_watched_source_channel_matches(on):
    assert _spread_channel(_msg(int(SPREAD_ID), "twi_matae")) is True


def test_relayed_message_matches_on_the_forwarder_label(on):
    """The relay lands in the shared destination channel, so the id proves
    nothing — only the [FWD #label] username survives the hop."""
    assert _spread_channel(_msg(DEST_ID, "[FWD #twi-spreads] twi_matae")) is True


def test_a_relayed_message_from_another_room_does_not_match(on):
    assert _spread_channel(_msg(DEST_ID, "[FWD #sniper-alerts] Sniper Alerts")) is False


def test_an_ordinary_message_in_the_destination_channel_does_not_match(on):
    """Everything else in #general must still reach the single-leg parser."""
    assert _spread_channel(_msg(DEST_ID, "Sniper Alerts")) is False


def test_the_gate_is_shut_when_the_feature_is_off(monkeypatch):
    monkeypatch.setattr(cfg, "getboolean", lambda s, k, fallback=None, **kw: False)
    assert _spread_channel(_msg(int(SPREAD_ID), "twi_matae")) is False


def test_an_empty_channel_list_matches_nothing(monkeypatch):
    monkeypatch.setattr(cfg, "get", lambda s, k, fallback=None, **kw: "")
    monkeypatch.setattr(cfg, "getboolean",
                        lambda s, k, fallback=None, **kw: True if k == "spreads_enabled" else fallback)
    assert _spread_channel(_msg(int(SPREAD_ID), "[FWD #twi-spreads] twi_matae")) is False
