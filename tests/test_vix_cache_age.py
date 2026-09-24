"""
VIX cache lifetime.

An index publishes only during the cash session — probed on IBKR at 03:22 ET,
live and delayed both returned bid/ask = -1 and last/close = nan. With the old
hard-coded 300s cache that meant every overnight, every weekend and every brief
feed gap expired the last known VIX, and vix_unavailable_scale silently halved
the next entry.
"""
import time

import app.risk.vix_sizer as vs


def _cache(level, age_seconds):
    return (float(level), time.monotonic() - age_seconds)


def test_overnight_gap_keeps_the_last_known_level(monkeypatch):
    """16:15 close to a 09:30 open is ~17h. That must still size normally."""
    monkeypatch.setattr(vs, "_VIX_CACHE", _cache(15.4, 17 * 3600))
    monkeypatch.setattr(vs.cfg, "get", lambda *a, **k: "")
    assert vs._fetch_vix_level() == 15.4


def test_a_genuinely_stale_level_is_still_refused(monkeypatch):
    """Friday close into Monday pre-market is ~65h — that one really is stale,
    and pretending otherwise is worse than admitting the gap."""
    monkeypatch.setattr(vs, "_VIX_CACHE", _cache(15.4, 65 * 3600))
    monkeypatch.setattr(vs.cfg, "get", lambda *a, **k: "")
    assert vs._fetch_vix_level() is None


def test_max_age_is_configurable(monkeypatch):
    monkeypatch.setattr(vs, "_VIX_CACHE", _cache(21.0, 600))
    monkeypatch.setattr(vs.cfg, "get", lambda *a, **k: "")
    monkeypatch.setattr(vs.cfg, "getfloat",
                        lambda sec, key, fallback=None: 300.0
                        if key == "vix_cache_max_age_seconds" else fallback)
    assert vs._fetch_vix_level() is None, "shorter configured age was ignored"


def test_manual_override_still_wins(monkeypatch):
    """vix_current_level is the portal knob; it must beat any cached value."""
    monkeypatch.setattr(vs, "_VIX_CACHE", _cache(15.4, 10))
    monkeypatch.setattr(vs.cfg, "get",
                        lambda sec, key, fallback=None: "28.5"
                        if key == "vix_current_level" else fallback)
    assert vs._fetch_vix_level() == 28.5


def test_a_carried_level_sizes_normally_instead_of_halving(monkeypatch):
    """The behavior that actually costs money: with the level carried over,
    a calm-VIX entry keeps full size instead of the unavailable-scale cut."""
    monkeypatch.setattr(vs, "_VIX_CACHE", _cache(15.4, 17 * 3600))
    monkeypatch.setattr(vs.cfg, "get", lambda *a, **k: "")
    monkeypatch.setattr(vs, "_VIX_SIZING_ENABLED", lambda: True)
    result = vs.apply_vix_sizing(4, "SPXW261219C06500000")
    assert result.adjusted_qty == 4
    assert result.scale_factor == 1.0
