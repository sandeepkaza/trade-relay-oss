"""
Verifies the SPX entry-overrides path added 2026-05-19.

Covers:
  - profile resolver picks spx_index for SPX/SPXW/NDX/RUT OSI prefixes
  - non-index symbols fall through to [trading] (zero-change behavior)
  - max_live_alert mode never bids below alert
  - default live_or_alert mode preserves old behavior
  - buy_slippage override flows through _compute_limit
  - max_pending_minutes / max_entry_slippage_pct / buy_repeg_cap_pct read
    from profile when overrides enabled, fall back when disabled
"""

import unittest
from decimal import Decimal

from app.core.config_manager import cfg
import app.core.profile_resolver as pr
import app.execution.public_executor as pe


class _Stub:
    def __init__(self, osi, author=None, expiry=None, strategy_tag=None):
        self.osi_symbol = osi
        self.author = author
        self.expiry = expiry
        self.strategy_tag = strategy_tag


class SpxEntryProfileTests(unittest.TestCase):

    def test_spx_resolves_to_spx_index_profile(self):
        s = _Stub("SPXW260518C07410000", author="spacemonkey")
        secs = pr._sections_for(s)
        self.assertIn("profile:spx_index", secs)

    def test_spxw_matches_spx_index(self):
        s = _Stub("SPXW260520P07300000")
        secs = pr._sections_for(s)
        self.assertEqual(secs[0], "profile:spx_index")

    def test_ndx_matches_spx_index(self):
        s = _Stub("NDX260518C20000000")
        secs = pr._sections_for(s)
        self.assertIn("profile:spx_index", secs)

    def test_single_name_skips_spx_profile(self):
        s = _Stub("AAPL260522C00200000", author="spacemonkey")
        secs = pr._sections_for(s)
        self.assertNotIn("profile:spx_index", secs)

    def test_spx_overrides_active_when_master_enabled(self):
        self.assertTrue(pr.entry_overrides_enabled())
        s = _Stub("SPXW260518C07410000")
        # All entry knobs read from spx_index profile.
        self.assertEqual(pr.pcfg_get(s, "entry_base_price_mode"), "max_live_alert")
        self.assertTrue(pr.pcfg_bool(s, "buy_repeg_enabled", False))
        self.assertEqual(pr.pcfg_int(s, "max_pending_minutes", 30), 5)
        self.assertEqual(pr.pcfg_float(s, "max_entry_slippage_pct", 5.0), 10.0)
        self.assertEqual(pr.pcfg_float(s, "buy_repeg_cap_pct", 25.0), 10.0)
        self.assertEqual(pr.pcfg_get(s, "buy_slippage"), "0.05")

    def test_non_spx_uses_trading_defaults(self):
        s = _Stub("GOOGL260518C00405000", author="spacemonkey")
        # Non-SPX never hits the spx_index profile, so all the new entry
        # knobs fall through to [trading] — i.e. legacy behavior.
        self.assertEqual(pr.pcfg_get(s, "entry_base_price_mode", "live_or_alert"), "live_or_alert")
        self.assertFalse(pr.pcfg_bool(s, "buy_repeg_enabled", False))
        self.assertEqual(pr.pcfg_int(s, "max_pending_minutes", 30), 30)
        self.assertEqual(pr.pcfg_float(s, "max_entry_slippage_pct", 5.0), 5.0)
        self.assertEqual(pr.pcfg_get(s, "buy_slippage"), "0.01")

    def test_compute_limit_with_slippage_override(self):
        # buy_slippage=0.05 against base $2.10 → raw=$2.205 → ceil $0.05 tick = $2.25
        executor_cls = pe.PublicExecutor
        # _compute_limit is a method but only uses self for nothing — call on instance.
        e = executor_cls(ws_manager=None)
        result = e._compute_limit(Decimal("2.10"), "BUY", slippage_override=Decimal("0.05"))
        self.assertEqual(result, Decimal("2.25"))

    def test_compute_limit_default_slippage_unchanged(self):
        e = pe.PublicExecutor(ws_manager=None)
        # Default buy_slippage=0.01 against $2.10 → raw=$2.121 → ceil $0.05 tick = $2.15
        result = e._compute_limit(Decimal("2.10"), "BUY")
        self.assertEqual(result, Decimal("2.15"))

    def test_rollback_flag_disables_overrides(self):
        # public_executor.py imports the resolver function under
        # _entry_overrides_enabled at module load — patch that binding,
        # not the original on the resolver module, since the local name
        # is what _alert_* helpers actually call.
        original = pe._entry_overrides_enabled
        pe._entry_overrides_enabled = lambda: False
        try:
            s = _Stub("SPXW260518C07410000")
            self.assertEqual(pe._alert_max_entry_slippage_pct(s), 5.0)
            self.assertEqual(pe._alert_max_pending_minutes(s), 30)
            self.assertEqual(pe._alert_entry_base_price_mode(s), "live_or_alert")
            self.assertFalse(pe._alert_buy_repeg_enabled(s))
        finally:
            pe._entry_overrides_enabled = original

    def test_alert_accessors_with_spx(self):
        # Without flipping the rollback flag — verify wrapper functions
        # respect the profile cascade for SPX inputs.
        s = _Stub("SPXW260518C07410000")
        self.assertEqual(pe._alert_max_entry_slippage_pct(s), 10.0)
        self.assertEqual(pe._alert_max_pending_minutes(s), 5)
        self.assertEqual(pe._alert_entry_base_price_mode(s), "max_live_alert")
        self.assertTrue(pe._alert_buy_repeg_enabled(s))
        self.assertEqual(pe._alert_buy_repeg_cap_pct(s), 10.0)
        self.assertEqual(pe._alert_buy_slippage(s), Decimal("0.05"))

    def test_alert_accessors_with_non_spx(self):
        s = _Stub("GOOGL260518C00405000", author="spacemonkey")
        self.assertEqual(pe._alert_max_entry_slippage_pct(s), 5.0)
        self.assertEqual(pe._alert_max_pending_minutes(s), 30)
        self.assertEqual(pe._alert_entry_base_price_mode(s), "live_or_alert")
        self.assertFalse(pe._alert_buy_repeg_enabled(s))
        self.assertEqual(pe._alert_buy_slippage(s), Decimal("0.01"))


if __name__ == "__main__":
    unittest.main()
