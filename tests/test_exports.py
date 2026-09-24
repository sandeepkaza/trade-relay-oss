"""CSV export contents.

The positions export shipped 13 columns and dropped the analyst, the lane
(SMALL_ACCT / WHALE / RE-ENTRY) and the water marks — so it could not answer
"how is this analyst doing" or "how do small-account trades compare", which
is most of what it gets opened for. These pin the columns so a later edit
cannot quietly drop them again.
"""
import csv, io
import pytest

import app.analytics.trade_logger as tl


class _Pos:
    def __init__(self, **kw):
        d = dict(open_time=None, close_time=None, author="Sniper Alerts",
                 strategy_tag="SMALL_ACCT", symbol="INTC",
                 osi_symbol="INTC260821C00095000", expiry="2026-08-21",
                 strike=95.0, option_type="C", total_contracts=2, remaining=0,
                 avg_price=1.19, current_price=1.04, close_price=1.50,
                 status="CLOSED", realized_pnl=None, highest_price=1.66,
                 lowest_price=1.01, pt1_triggered=False, pt2_triggered=False,
                 pt3_triggered=False, sl_triggered=True,
                 sl_partial_triggered=False, tpt_armed=False,
                 is_credit=False, spread_width=None)
        d.update(kw)
        for k, v in d.items():
            setattr(self, k, v)


class _Order:
    def __init__(self, **kw):
        import datetime as dt
        d = dict(placed_at=dt.datetime(2026, 8, 19, 14, 57, 48),
                 author="Sniper Alerts", strategy_tag="SMALL_ACCT", side="BUY",
                 osi_symbol="MU260821C01050000", quantity=2, filled_qty=2,
                 limit_price=1.65, fill_price=1.54, cost_basis=None,
                 status="FILLED", trigger="DISCORD", is_resting=False,
                 error_text=None)
        d.update(kw)
        for k, v in d.items():
            setattr(self, k, v)


class _Q:
    def __init__(self, rows): self._r = rows
    def filter(self, *a, **k): return self
    def order_by(self, *a, **k): return self
    def all(self): return self._r


class _DB:
    def __init__(self, rows): self._r = rows
    def query(self, *a, **k): return _Q(self._r)
    def close(self): pass


def _csv(monkeypatch, rows, fn):
    monkeypatch.setattr(tl, "SessionLocal", lambda: _DB(rows))
    return list(csv.reader(io.StringIO(fn())))


class TestPositionsExport:
    def _rows(self, monkeypatch, **kw):
        return _csv(monkeypatch, [_Pos(**kw)], tl.export_positions_csv)

    @pytest.mark.parametrize("col", [
        "Analyst", "Lane", "Profile", "Expiry", "Peak", "MFE %",
        "Trough", "MAE %", "Exits Fired", "Realized P&L (db)", "Spread",
    ])
    def test_column_present(self, monkeypatch, col):
        assert col in self._rows(monkeypatch)[0]

    def test_analyst_and_lane_populated(self, monkeypatch):
        head, row = self._rows(monkeypatch)[:2]
        assert row[head.index("Analyst")] == "Sniper Alerts"
        assert row[head.index("Lane")] == "SMALL_ACCT"

    def test_exit_price_uses_close_not_current(self, monkeypatch):
        """Closed rows must report close_price. The old export wrote
        current_price under an 'Exit Price' header."""
        head, row = self._rows(monkeypatch)[:2]
        assert row[head.index("Exit Price")] == "1.50"

    def test_pnl_reconstructed_not_read_from_realized(self, monkeypatch):
        """realized_pnl is NULL on most rows; the money column must not be it."""
        head, row = self._rows(monkeypatch)[:2]
        assert row[head.index("P&L $")] == "+62.00"      # (1.50-1.19)*2*100
        assert row[head.index("Realized P&L (db)")] == ""

    def test_open_row_sizes_pnl_on_remaining(self, monkeypatch):
        head, row = self._rows(monkeypatch, status="OPEN", remaining=1,
                               close_price=None)[:2]
        assert row[head.index("P&L $")] == "-15.00"      # (1.04-1.19)*1*100

    def test_exits_fired_lists_triggers(self, monkeypatch):
        head, row = self._rows(monkeypatch)[:2]
        assert row[head.index("Exits Fired")] == "SL"

    def test_water_marks_carry_percentages(self, monkeypatch):
        head, row = self._rows(monkeypatch)[:2]
        assert row[head.index("MFE %")] == "+39.5%"
        assert row[head.index("MAE %")] == "-15.1%"


class TestTradesExport:
    def _rows(self, monkeypatch, **kw):
        return _csv(monkeypatch, [_Order(**kw)], tl.export_trades_csv)

    def test_ticker_is_the_root_not_six_chars(self, monkeypatch):
        """osi_symbol[:6] produced 'MU2608' and 'SPXW26'."""
        head, row = self._rows(monkeypatch)[:2]
        assert row[head.index("Symbol")] == "MU"

    def test_index_weekly_root(self, monkeypatch):
        head, row = self._rows(monkeypatch, osi_symbol="SPXW260819P07710000")[:2]
        assert row[head.index("Symbol")] == "SPX"

    def test_analyst_and_lane_present(self, monkeypatch):
        head, row = self._rows(monkeypatch)[:2]
        assert row[head.index("Analyst")] == "Sniper Alerts"
        assert row[head.index("Lane")] == "SMALL_ACCT"

    def test_slippage_vs_limit(self, monkeypatch):
        head, row = self._rows(monkeypatch)[:2]
        assert row[head.index("Slip vs Limit %")] == "-6.67%"
