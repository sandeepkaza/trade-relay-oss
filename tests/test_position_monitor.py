"""
Focused tests for position_monitor module-level helpers:
- record_auto_exit / get_recent_auto_exit dedup
- Stale auto-exit cleanup window
- Config accessors return live values

Does NOT spin up the full async monitor loop (that requires broker creds and a
real DB) — focuses on the deterministic helpers that drive auto-exit dedup.

Run: PYTHONIOENCODING=utf-8 .venv/Scripts/python.exe tests/test_position_monitor.py
"""
import os
import sys
import time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tempfile, atexit, shutil
_TEST_DIR = tempfile.mkdtemp(prefix="trader_pm_test_")
_TEST_DB = os.path.join(_TEST_DIR, "test.db")
os.environ["TRADER_DATABASE_URL"] = f"sqlite:///{_TEST_DB}"
atexit.register(lambda: shutil.rmtree(_TEST_DIR, ignore_errors=True))

import app.monitors.position_monitor as pm
from app.monitors.position_monitor import record_auto_exit, get_recent_auto_exit

PASS = 0; FAIL = 0
def chk(label, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {label}")
    else:
        FAIL += 1
        print(f"  [FAIL] {label}" + (f" - {detail}" if detail else ""))


def reset():
    pm._recent_auto_exits.clear()


def test_record_and_retrieve():
    reset()
    record_auto_exit("SPX260423C07000000", "PT1", 5)
    rec = get_recent_auto_exit("SPX260423C07000000")
    chk("Recent auto-exit retrievable", rec is not None)
    chk("Exit type stored", rec[0] == "PT1")
    chk("Qty stored", rec[2] == 5)
    chk("Timestamp recent", time.time() - rec[1] < 2)


def test_no_recent_exit_returns_none():
    reset()
    chk("Unknown symbol returns None", get_recent_auto_exit("NOSUCH") is None)


def test_distinct_symbols_isolated():
    reset()
    record_auto_exit("SPX1", "PT1", 5)
    record_auto_exit("SPX2", "SL", 10)
    chk("SPX1 isolated", get_recent_auto_exit("SPX1")[0] == "PT1")
    chk("SPX2 isolated", get_recent_auto_exit("SPX2")[0] == "SL")


def test_stale_exit_cleaned():
    """Entries older than _RECENT_EXIT_WINDOW_SECONDS get cleaned on next access."""
    reset()
    # Inject a stale entry directly
    pm._recent_auto_exits["STALE"] = ("PT1", time.time() - (pm._RECENT_EXIT_WINDOW_SECONDS + 60), 5)
    pm._recent_auto_exits["FRESH"] = ("SL", time.time(), 5)
    # Trigger cleanup
    fresh = get_recent_auto_exit("FRESH")
    stale = get_recent_auto_exit("STALE")
    chk("Fresh entry survives", fresh is not None)
    chk("Stale entry cleaned", stale is None)


def test_overwrite_same_symbol():
    """Recording a second exit for the same symbol overwrites the prior one."""
    reset()
    record_auto_exit("SPX1", "PT1", 5)
    time.sleep(0.05)
    record_auto_exit("SPX1", "PT2", 3)
    rec = get_recent_auto_exit("SPX1")
    chk("Overwritten exit type", rec[0] == "PT2")
    chk("Overwritten qty", rec[2] == 3)


def test_config_accessors():
    """Verify the SL/PT/TPT accessors read from config (and don't crash)."""
    chk("_AUTO_EXIT_ENABLED bool", isinstance(pm._AUTO_EXIT_ENABLED(), bool))
    chk("_SL_PCT float", isinstance(pm._SL_PCT(), float))
    chk("_PT1_PCT float", isinstance(pm._PT1_PCT(), float))
    chk("_PT1_SELL float", isinstance(pm._PT1_SELL(), float))
    chk("_PT2_PCT float", isinstance(pm._PT2_PCT(), float))
    chk("_PT3_PCT float", isinstance(pm._PT3_PCT(), float))
    chk("_TRAILING_SL_PCT float", isinstance(pm._TRAILING_SL_PCT(), float))
    chk("_TPT_TRAIL_PCT float", isinstance(pm._TPT_TRAIL_PCT(), float))
    chk("_TIME_EXIT_HOUR int", isinstance(pm._TIME_EXIT_HOUR(), int))
    chk("_TIME_EXIT_MINUTE int", isinstance(pm._TIME_EXIT_MINUTE(), int))
    chk("_TIME_EXIT_DAYS str", isinstance(pm._TIME_EXIT_DAYS(), str))


def test_dst_aware_time_exit():
    """Verify the DST fix: position_monitor uses ZoneInfo, not hardcoded UTC-5."""
    import inspect
    src = inspect.getsource(pm)
    chk("ZoneInfo America/New_York imported", 'ZoneInfo("America/New_York")' in src or "ZoneInfo('America/New_York')" in src)
    chk("No hardcoded et_offset = timedelta(hours=5)", "timedelta(hours=5)" not in src)


def main():
    print("\n-- Position Monitor Tests ------------------------------------")
    test_record_and_retrieve()
    test_no_recent_exit_returns_none()
    test_distinct_symbols_isolated()
    test_stale_exit_cleaned()
    test_overwrite_same_symbol()
    test_config_accessors()
    test_dst_aware_time_exit()
    total = PASS + FAIL
    status = "PASS" if FAIL == 0 else "FAIL"
    print(f"\nPosition monitor: {PASS}/{total}  [{status}]")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()


class TestPriceSampling:
    """logs/prices.jsonl sampling — throttle and off-by-default."""

    class _Pos:
        def __init__(self, pid=1):
            self.id = pid
            self.osi_symbol = "AAPL260821C00200000"
            self.author = "Sniper Alerts"
            self.avg_price = 2.00
            self.highest_price = 3.00
            self.lowest_price = 1.50
            self.remaining = 2
            self._last_spread_pct = 4.0

    def _capture(self, monkeypatch):
        import app.monitors.position_monitor as pm
        out = []
        monkeypatch.setattr(pm.price_logger, "info", lambda m: out.append(m))
        pm._last_sample.clear()
        return pm, out

    def test_disable_flag_silences_it(self, monkeypatch):
        pm, out = self._capture(monkeypatch)
        monkeypatch.setattr(pm, "_PRICE_TRACK_ENABLED", lambda: False)
        pm._sample_price(self._Pos(), 2.50)
        assert out == []

    def test_enabled_by_default(self):
        """Tracking is ON with no config key present — the whole point is that
        history accumulates without anyone remembering to switch it on."""
        import app.monitors.position_monitor as pm
        assert pm._PRICE_TRACK_ENABLED() is True
        assert pm._PRICE_TRACK_INTERVAL() == 30.0

    def test_writes_one_json_line(self, monkeypatch):
        import json
        pm, out = self._capture(monkeypatch)
        monkeypatch.setattr(pm, "_PRICE_TRACK_ENABLED", lambda: True)
        monkeypatch.setattr(pm, "_PRICE_TRACK_INTERVAL", lambda: 30.0)
        pm._sample_price(self._Pos(), 2.50)
        assert len(out) == 1
        rec = json.loads(out[0])
        assert rec["osi"] == "AAPL260821C00200000"
        assert rec["px"] == 2.5
        assert rec["pnl_pct"] == 25.0
        assert rec["low"] == 1.5

    def test_throttled_within_interval(self, monkeypatch):
        pm, out = self._capture(monkeypatch)
        monkeypatch.setattr(pm, "_PRICE_TRACK_ENABLED", lambda: True)
        monkeypatch.setattr(pm, "_PRICE_TRACK_INTERVAL", lambda: 30.0)
        pos = self._Pos()
        for _ in range(10):
            pm._sample_price(pos, 2.50)
        assert len(out) == 1, "10 rapid polls must produce one sample, not ten"

    def test_separate_positions_not_throttled_together(self, monkeypatch):
        pm, out = self._capture(monkeypatch)
        monkeypatch.setattr(pm, "_PRICE_TRACK_ENABLED", lambda: True)
        monkeypatch.setattr(pm, "_PRICE_TRACK_INTERVAL", lambda: 30.0)
        pm._sample_price(self._Pos(pid=1), 2.50)
        pm._sample_price(self._Pos(pid=2), 2.50)
        assert len(out) == 2

    def test_bad_price_skipped(self, monkeypatch):
        pm, out = self._capture(monkeypatch)
        monkeypatch.setattr(pm, "_PRICE_TRACK_ENABLED", lambda: True)
        pm._sample_price(self._Pos(), 0.0)
        assert out == []


class TestPriceArchiveRotation:
    """prices.jsonl splits at maxBytes and never deletes a roll."""

    def test_rollover_keeps_every_file(self, tmp_path):
        import logging
        from app.core.log_config import ArchivingRotatingFileHandler

        path = tmp_path / "prices.jsonl"
        h = ArchivingRotatingFileHandler(path, maxBytes=200, encoding="utf-8")
        lg = logging.getLogger("test_price_archive")
        lg.handlers.clear()
        lg.propagate = False
        lg.setLevel(logging.INFO)
        lg.addHandler(h)

        for i in range(200):
            lg.info('{"n":%d,"pad":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}' % i)
        h.close()

        rolls = sorted(p for p in tmp_path.iterdir() if p.name != "prices.jsonl")
        assert rolls, "expected at least one archived roll"

        # Every line ever written must still exist somewhere on disk.
        lines = []
        for p in list(rolls) + [path]:
            lines += [x for x in p.read_text(encoding="utf-8").splitlines() if x.strip()]
        assert len(lines) == 200, f"lost lines: got {len(lines)} of 200"

    def test_default_split_size_is_25mb(self):
        from app.core.log_config import PRICE_MAX_BYTES
        assert PRICE_MAX_BYTES == 25 * 1024 * 1024


class TestAbsurdTickGuard:
    """A single bad print must not latch into highest_price forever.

    Real corruption seen on relaybot-vm: LITE260702C00900000 entered at $2.85
    and recorded a peak of $4938.87 while closing at $9.70.
    """

    class _Pos:
        def __init__(self, avg=2.85):
            self.osi_symbol = "LITE260702C00900000"
            self.avg_price = avg

    def _sane(self, monkeypatch, px, prev, avg=2.85, mult=None):
        import app.monitors.position_monitor as pm
        if mult is not None:
            monkeypatch.setattr(pm, "_MAX_TICK_JUMP", lambda: mult)
        return pm._tick_is_sane(self._Pos(avg), px, prev)

    def test_rejects_the_real_corruption(self, monkeypatch):
        assert self._sane(monkeypatch, 4938.87, 3.0) is False
        assert self._sane(monkeypatch, 1544.87, 3.62, avg=3.20) is False
        assert self._sane(monkeypatch, 535.56, 5.0, avg=4.65) is False

    def test_allows_a_normal_move(self, monkeypatch):
        assert self._sane(monkeypatch, 4.20, 3.0) is True

    def test_allows_the_largest_genuine_mfe(self, monkeypatch):
        """+471% (5.7x) was real — must not be clipped."""
        assert self._sane(monkeypatch, 4.85, 0.85, avg=0.85) is True

    def test_measures_against_higher_of_entry_and_prev(self, monkeypatch):
        """A position already up 15x must not be re-clipped to its entry."""
        assert self._sane(monkeypatch, 2.00, 1.50, avg=0.10) is True

    def test_threshold_is_configurable(self, monkeypatch):
        assert self._sane(monkeypatch, 5.0, 1.0, avg=1.0, mult=2.0) is False

    def test_no_reference_price_passes_through(self, monkeypatch):
        """Nothing to compare against — do not silently drop the tick."""
        assert self._sane(monkeypatch, 9.0, None, avg=0) is True

    def test_evaluate_returns_none_on_bad_tick(self, monkeypatch):
        import asyncio
        import app.monitors.position_monitor as pm

        class P:
            osi_symbol = "LITE260702C00900000"
            avg_price = 2.85
            current_price = 3.0
            highest_price = 3.0
            lowest_price = 2.5
        pos = P()
        assert asyncio.run(pm._evaluate_one_position(None, pos, 4938.87, None, None)) is None
        assert pos.highest_price == 3.0
        assert pos.current_price == 3.0


class TestAbsurdTickGuardLowSide:
    """lowest_price latches exactly like highest_price, so the guard is
    symmetric. A near-zero print must not become the permanent MAE."""

    class _Pos:
        def __init__(self, avg):
            self.osi_symbol = "AAPL260821C00200000"
            self.avg_price = avg

    def _sane(self, px, prev, avg=2.00):
        import app.monitors.position_monitor as pm
        return pm._tick_is_sane(self._Pos(avg), px, prev)

    def test_rejects_near_zero_print(self):
        assert self._sane(0.0001, 1.90) is False

    def test_allows_a_real_collapse(self):
        """MU went 15.90 -> 0.46 (-97%) for real; that must still be recorded."""
        assert self._sane(0.46, 0.58, avg=15.90) is True

    def test_allows_worthless_penny(self):
        """An option dying at $0.01 from a $0.10 mark is real, not corrupt."""
        assert self._sane(0.01, 0.10, avg=0.10) is True

    def test_low_bound_uses_lower_of_entry_and_prev(self):
        """Already down 95% — subsequent small prints are real, not corrupt."""
        assert self._sane(0.04, 0.05, avg=15.90) is True

    def test_zero_multiple_disables_the_guard(self, monkeypatch):
        import app.monitors.position_monitor as pm
        monkeypatch.setattr(pm, "_MAX_TICK_JUMP", lambda: 0.0)
        assert self._sane(9999.0, 1.0) is True


class TestCreditSpreadTickGuard:
    """A credit spread winning toward zero must not be dropped as a bad tick."""

    class _Pos:
        def __init__(self):
            self.osi_symbol = "SPXW260910C07605000|SPXW260910C07610000"
            self.avg_price = 2.05
            self.is_credit = True
            self.spread_width = 5.0

    def test_falling_mark_is_accepted(self):
        import app.monitors.position_monitor as pm
        assert pm._tick_is_sane(self._Pos(), 0.05, 2.05) is True

    def test_mark_beyond_width_is_rejected(self):
        import app.monitors.position_monitor as pm
        assert pm._tick_is_sane(self._Pos(), 9.0, 2.05) is False

    def test_long_premium_low_bound_unchanged(self):
        import app.monitors.position_monitor as pm
        class Long:
            osi_symbol = "AAPL260821C00200000"
            avg_price = 2.00
            is_credit = False
        assert pm._tick_is_sane(Long(), 0.0001, 1.90) is False
