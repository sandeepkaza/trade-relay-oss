"""The single export, and the join it exists for.

Three CSVs with no key between them meant "what did this alert actually
make" was a manual timestamp match across two files. These pin the join.
"""
import io
import datetime as dt
import pytest

import app.analytics.trade_logger as tl


class _A:
    def __init__(self, id, ts, osi="MU260821C01050000", action="BUY", **kw):
        self.id, self.timestamp, self.osi_symbol, self.action = id, ts, osi, action
        d = dict(author="Sniper Alerts", channel_name="#sniper-alerts",
                 strategy_tag="SMALL_ACCT", raw_text="BOUGHT MU 1050C", symbol="MU",
                 strike=1050.0, option_type="C", alert_price=15.90, qty=1,
                 status="FILLED", error_text=None)
        d.update(kw)
        for k, v in d.items():
            setattr(self, k, v)


class _O:
    def __init__(self, id, ts, alert_id=None, **kw):
        self.id, self.placed_at, self.alert_id = id, ts, alert_id
        d = dict(osi_symbol="MU260821C01050000", side="BUY", quantity=1, filled_qty=1,
                 limit_price=16.20, fill_price=15.90, status="FILLED",
                 trigger="DISCORD", is_resting=False, error_text=None,
                 author="Sniper Alerts", strategy_tag="SMALL_ACCT")
        d.update(kw)
        for k, v in d.items():
            setattr(self, k, v)


class _P:
    def __init__(self, **kw):
        d = dict(id=9, osi_symbol="MU260821C01050000", symbol="MU", expiry="2026-08-21",
                 strike=1050.0, option_type="C", status="CLOSED", avg_price=15.90,
                 current_price=0.46, close_price=0.50, total_contracts=1, remaining=0,
                 highest_price=16.50, lowest_price=0.44, open_time=None, close_time=None,
                 pt1_triggered=False, pt2_triggered=False, pt3_triggered=False,
                 sl_triggered=False, is_credit=False, spread_width=None)
        d.update(kw)
        for k, v in d.items():
            setattr(self, k, v)


class _Q:
    def __init__(self, r): self._r = r
    def filter(self, *a, **k): return self
    def order_by(self, *a, **k): return self
    def all(self): return self._r


class _DB:
    def __init__(self, orders, alerts, positions):
        self._m = {"Order": orders, "Alert": alerts, "Position": positions}
    def query(self, model=None, *a, **k):
        return _Q(self._m.get(getattr(model, "__name__", ""), []))
    def close(self): pass


T0 = dt.datetime(2026, 8, 19, 18, 0, 0, tzinfo=dt.timezone.utc)


def _rows(monkeypatch, orders, alerts, positions):
    monkeypatch.setattr(tl, "SessionLocal", lambda: _DB(orders, alerts, positions))
    db = tl.SessionLocal()
    return tl._lifecycle_rows(db, 30)


def _col(name):
    return tl._LIFECYCLE_COLS.index(name)


class TestTheJoin:
    def test_alert_id_links_order_to_alert(self, monkeypatch):
        rows, _ = _rows(monkeypatch, [_O(1, T0, alert_id=7)], [_A(7, T0)], [_P()])
        assert rows[0][_col("Alert ID")] == 7
        assert rows[0][_col("Analyst")] == "Sniper Alerts"

    def test_falls_back_to_timestamp_for_legacy_rows(self, monkeypatch):
        """Orders placed before the alert_id column still map."""
        rows, _ = _rows(monkeypatch, [_O(1, T0, alert_id=None)],
                        [_A(7, T0 - dt.timedelta(seconds=3))], [_P()])
        assert rows[0][_col("Alert ID")] == 7

    def test_far_apart_rows_do_not_get_joined(self, monkeypatch):
        rows, nt = _rows(monkeypatch, [_O(1, T0, alert_id=None)],
                         [_A(7, T0 - dt.timedelta(hours=3))], [_P()])
        assert rows[0][_col("Alert ID")] == ""
        assert len(nt) == 1, "the unmatched alert belongs in Not Taken"

    def test_position_joined_onto_the_row(self, monkeypatch):
        rows, _ = _rows(monkeypatch, [_O(1, T0, alert_id=7)], [_A(7, T0)], [_P()])
        r = rows[0]
        assert r[_col("Position ID")] == 9
        assert r[_col("P&L $")] == -1540.0          # (0.50-15.90)*1*100
        assert r[_col("MAE %")] == -97.2

    def test_reaction_time_and_slippage(self, monkeypatch):
        rows, _ = _rows(monkeypatch, [_O(1, T0, alert_id=7)],
                        [_A(7, T0 - dt.timedelta(milliseconds=800))], [_P()])
        r = rows[0]
        assert r[_col("Reaction (s)")] == 0.8
        assert r[_col("Slip vs Alert %")] == 0.0     # fill 15.90 == alert 15.90
        assert r[_col("Slip vs Limit %")] == -1.85   # 15.90 vs 16.20


class TestNotTakenSheet:
    def test_unordered_alert_lands_there_with_reason(self, monkeypatch):
        a = _A(7, T0, status="SKIPPED", error_text="MANUAL HALT: trading off")
        _, nt = _rows(monkeypatch, [], [a], [])
        assert len(nt) == 1
        assert "MANUAL HALT" in nt[0][tl._NOT_TAKEN_COLS.index("Why Not Taken")]

    def test_taken_alert_is_not_duplicated_there(self, monkeypatch):
        _, nt = _rows(monkeypatch, [_O(1, T0, alert_id=7)], [_A(7, T0)], [_P()])
        assert nt == []


class TestWorkbook:
    def test_sheets_and_it_opens(self, monkeypatch):
        openpyxl = pytest.importorskip("openpyxl")
        monkeypatch.setattr(tl, "SessionLocal",
                            lambda: _DB([_O(1, T0, alert_id=7)], [_A(7, T0)], [_P()]))
        wb = openpyxl.load_workbook(io.BytesIO(tl.export_all_xlsx(30)))
        assert wb.sheetnames == ["Summary", "Round Trips", "Lifecycle", "Not Taken"]
        assert wb["Lifecycle"].max_row == 2          # header + 1
        assert wb["Lifecycle"].freeze_panes == "A2"


class TestFifoMatching:
    """Buy lots matched to sells first-in-first-out, one row per closed lot."""

    def _trips(self, monkeypatch, orders, alerts=(), positions=()):
        rows, _ = _rows(monkeypatch, list(orders), list(alerts), list(positions))
        return tl._round_trips(rows)

    def _rc(self, name):
        return tl._ROUNDTRIP_COLS.index(name)

    def test_scaled_exit_splits_one_entry_into_three_lots(self, monkeypatch):
        buy = _O(1, T0, quantity=10, filled_qty=10, fill_price=2.00)
        sells = [_O(i + 2, T0 + dt.timedelta(minutes=i + 1), side="SELL",
                    quantity=q, filled_qty=q, fill_price=3.00, trigger="DISCORD")
                 for i, q in enumerate((3, 3, 4))]
        trips = self._trips(monkeypatch, [buy] + sells, [], [_P()])
        assert [t[self._rc("Match")] for t in trips] == ["FIFO"] * 3
        assert sorted(t[self._rc("Qty")] for t in trips) == [3, 3, 4]
        assert sum(t[self._rc("P&L $")] for t in trips) == 1000.0   # (3-2)*10*100
        # every row carries its own basis and exit, not a position total
        for t in trips:
            assert t[self._rc("Buy Fill")] == 2.00
            assert t[self._rc("Sell Fill")] == 3.00
            assert t[self._rc("P&L %")] == 50.0
            assert t[self._rc("Buy Order ID")] == 1

    def test_one_sell_consumes_the_oldest_buy_first(self, monkeypatch):
        early = _O(1, T0, quantity=1, filled_qty=1, fill_price=2.00)
        later = _O(2, T0 + dt.timedelta(minutes=5), quantity=1, filled_qty=1, fill_price=4.00)
        sell = _O(3, T0 + dt.timedelta(minutes=9), side="SELL", quantity=1,
                  filled_qty=1, fill_price=5.00, trigger="DISCORD")
        trips = self._trips(monkeypatch, [early, later, sell], [], [_P()])
        matched = [t for t in trips if t[self._rc("Match")] == "FIFO"]
        assert len(matched) == 1
        assert matched[0][self._rc("Buy Order ID")] == 1      # FIFO, not LIFO
        assert matched[0][self._rc("P&L $")] == 300.0         # (5-2)*1*100
        assert [t[self._rc("Buy Order ID")] for t in trips
                if t[self._rc("Match")] == "OPEN"] == [2]

    def test_sell_without_a_buy_in_window_is_flagged_not_priced(self, monkeypatch):
        sell = _O(3, T0, side="SELL", quantity=2, filled_qty=2, fill_price=5.00,
                  trigger="SL")
        trips = self._trips(monkeypatch, [sell], [], [_P()])
        assert [t[self._rc("Match")] for t in trips] == ["SELL W/O BUY"]
        assert trips[0][self._rc("P&L $")] == ""
        assert trips[0][self._rc("Sell Fill")] == 5.00

    def test_cross_analyst_sell_does_not_consume_another_book(self, monkeypatch):
        """position_owner_scope=author — a foreign SELL must not close this lot."""
        buy = _O(1, T0, quantity=1, filled_qty=1, fill_price=2.00, author="Fluid")
        sell = _O(2, T0 + dt.timedelta(minutes=2), side="SELL", quantity=1,
                  filled_qty=1, fill_price=5.00, trigger="DISCORD", author="Sniper")
        trips = self._trips(monkeypatch, [buy, sell], [], [_P()])
        assert sorted(t[self._rc("Match")] for t in trips) == ["OPEN", "SELL W/O BUY"]

    def test_unfilled_orders_never_become_lots(self, monkeypatch):
        rejected = _O(1, T0, status="REJECTED", fill_price=None, filled_qty=0)
        assert self._trips(monkeypatch, [rejected], [], [_P()]) == []
