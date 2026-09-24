"""test_whale_lane.py — whale-tracker auto-trading lane.

Covers the separate-lane invariant: whale positions auto-exit on
[profile:whale] (+15% take-profit, 1 contract, $500 cap) while analyst
positions stay pure relay (global auto_exit_enabled=false, no auto-fire).

Pure-fn / config-resolution tests only — no broker, no network.
"""

import unittest
from datetime import date, timedelta

# Dynamic near-future expiry — a hardcoded date becomes a time-bomb once it
# passes (a past expiry makes _gtd_expiration_for return None/DAY, not GTD).
_FUTURE_EXPIRY = (date.today() + timedelta(days=30)).isoformat()

import app.core.profile_resolver as pr
import app.monitors.position_monitor as pm
from app.core.config_manager import cfg
from app.ingest.parser import parse_alert

# Real whale-tracker layout (SniperTrades), as the bot receives it.
_WHALE_MSG = (
    "🐳 WHALE SPOTTED: `$META` 🟢\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "**Stock**: `$META`  |  **Strike**: `650.0C`  |  **Expiry**: `06/19/2026`  "
    "|  **Entry**: `$3.00`  |  **Action**: `BUY`\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "Data Provide by SniperTrades© • Jun 05, 2026 09:52 AM EST"
)


class WhaleParser(unittest.TestCase):
    def test_parses_structured_whale_alert(self):
        a = parse_alert(_WHALE_MSG, channel_id=100000000000019998)
        self.assertIsNotNone(a)
        self.assertEqual(a.action, "BUY")
        self.assertEqual(a.symbol, "META")
        self.assertEqual(a.option_type, "C")
        self.assertEqual(a.strike, 650.0)
        self.assertEqual(str(a.expiry), "2026-06-19")
        self.assertEqual(str(a.price), "3.00")
        self.assertEqual(a.osi_symbol, "META260619C00650000")

    def test_put_alert(self):
        msg = _WHALE_MSG.replace("650.0C", "850.0P").replace("🟢", "🔴")
        a = parse_alert(msg, channel_id=100000000000019998)
        self.assertEqual(a.option_type, "P")
        self.assertEqual(a.strike, 850.0)

    def test_non_whale_text_unaffected(self):
        # A non-whale message must not be misread as a whale alert.
        self.assertIsNone(parse_alert("just some chart commentary, no trade"))


class _Obj:
    """Duck-typed stand-in for Alert / Position (author / osi / tag / expiry)."""
    def __init__(self, tag="", author="", osi=""):
        self.strategy_tag = tag
        self.author = author
        self.osi_symbol = osi
        self.expiry = None


class WhaleProfileResolution(unittest.TestCase):
    def test_whale_tag_resolves_whale_profile_first(self):
        w = _Obj(tag="WHALE", author="Sniper Market Updates", osi="META260619C00650000")
        # Most specific — whale wins outright.
        self.assertEqual(pr._sections_for(w), ["profile:whale"])

    def test_whale_exit_knobs(self):
        w = _Obj(tag="WHALE", osi="META260619C00650000")
        self.assertTrue(pr.pcfg_bool(w, "auto_exit_enabled", False))
        self.assertEqual(pr.pcfg_float(w, "pt1_pct", 0), 15.0)
        self.assertEqual(pr.pcfg_float(w, "pt1_sell", 0), 1.0)   # sell 100%
        self.assertFalse(pr.pcfg_bool(w, "pt2_enabled", True))
        self.assertFalse(pr.pcfg_bool(w, "pt3_enabled", True))
        self.assertFalse(pr.pcfg_bool(w, "sl_enabled", True))    # no downside stop
        self.assertFalse(pr.pcfg_bool(w, "trailing_sl_enabled", True))

    def test_whale_caps(self):
        w = _Obj(tag="WHALE", osi="META260619C00650000")
        self.assertEqual(pr.pcfg_int(w, "whale_max_contracts", 99), 1)
        self.assertEqual(pr.pcfg_float(w, "max_trade_cost", 9999), 500.0)


class AnalystLaneUnaffected(unittest.TestCase):
    """The whole reason this is code, not config: analyst lanes must NOT
    auto-exit. Global stays false; pure relay preserved."""

    def test_global_auto_exit_still_false(self):
        self.assertFalse(cfg.getboolean("trading", "auto_exit_enabled", fallback=True))

    def test_analyst_position_does_not_auto_exit(self):
        a = _Obj(tag="", author="SARANG", osi="SPXW260101C06000000")
        self.assertFalse(pm._AUTO_EXIT_ENABLED(a))

    def test_no_arg_returns_global(self):
        self.assertFalse(pm._AUTO_EXIT_ENABLED())


class WhaleAutoExitGate(unittest.TestCase):
    def test_whale_position_auto_exits(self):
        w = _Obj(tag="WHALE", author="Sniper Market Updates", osi="META260619C00650000")
        # Profile override flips the global false → true for whale only.
        self.assertTrue(pm._AUTO_EXIT_ENABLED(w))


class _FakeQ:
    def filter(self, *a, **k):
        return self
    def first(self):
        return None

class _FakeDB:
    def query(self, *a, **k):
        return _FakeQ()

class _Pos:
    def __init__(self, tag="WHALE", remaining=1, avg=3.00, expiry=_FUTURE_EXPIRY):
        self.strategy_tag = tag
        self.remaining = remaining
        self.avg_price = avg
        self.expiry = expiry
        self.osi_symbol = "META260619C00650000"


class WhaleRestingTP(unittest.TestCase):
    def setUp(self):
        import app.execution.public_executor as pe
        self.pe = pe
        self.ex = pe.PublicExecutor(ws_manager=None)
        self._orig_flag = pe._whale_resting_tp_enabled

    def tearDown(self):
        self.pe._whale_resting_tp_enabled = self._orig_flag

    def test_flag_default_off(self):
        import app.core.profile_resolver as pr
        self.assertFalse(pr.whale_resting_tp_enabled())

    def test_gtd_future_past_cap(self):
        import datetime
        e = self.ex._gtd_expiration_for(_Pos(expiry=_FUTURE_EXPIRY))
        self.assertEqual(e.time_in_force.value, "GTD")
        self.assertIsNone(self.ex._gtd_expiration_for(_Pos(expiry="2020-01-01")))  # past -> DAY

    def _run_place(self, flag, tag):
        import asyncio
        calls = []
        async def fake_place_sell(db, pos, qty, limit_px, trigger, expiration=None, skip_band_clamp=False):
            calls.append({"qty": qty, "limit_px": limit_px, "trigger": trigger,
                          "skip_band_clamp": skip_band_clamp, "tif": "GTD" if expiration else "DAY"})
        self.ex._place_sell = fake_place_sell
        self.pe._whale_resting_tp_enabled = lambda: flag
        asyncio.run(self.ex._maybe_place_whale_tp(_FakeDB(), _Pos(tag=tag)))
        return calls

    def test_noop_when_flag_off(self):
        self.assertEqual(self._run_place(flag=False, tag="WHALE"), [])

    def test_noop_for_non_whale(self):
        self.assertEqual(self._run_place(flag=True, tag=""), [])

    def test_places_tp_at_plus_15pct_whale_only(self):
        calls = self._run_place(flag=True, tag="WHALE")
        self.assertEqual(len(calls), 1)
        c = calls[0]
        self.assertEqual(c["trigger"], "WHALE_TP")
        self.assertEqual(c["qty"], 1)
        self.assertEqual(str(c["limit_px"]), "3.45")   # 3.00 x 1.15
        self.assertTrue(c["skip_band_clamp"])          # rests above mark
        self.assertEqual(c["tif"], "GTD")

    def test_whale_tp_not_an_auto_trigger(self):
        # MANUAL (portal exit) must be able to displace WHALE_TP. It can only
        # do so if WHALE_TP is NOT in the AUTO trigger set.
        import inspect, app.execution.public_executor as pe
        src = inspect.getsource(pe.PublicExecutor._place_sell)
        self.assertIn("AUTO_TRIGGERS", src)
        # WHALE_TP must not be listed among AUTO_TRIGGERS
        auto_line = [l for l in src.splitlines() if "AUTO_TRIGGERS = {" in l][0]
        self.assertNotIn("WHALE_TP", auto_line)


if __name__ == "__main__":
    unittest.main()
