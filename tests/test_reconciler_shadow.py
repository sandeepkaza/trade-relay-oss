"""Phase 1 per-broker reconciliation: reports divergence, writes nothing.

The Public path is untouched; this only covers the branch that used to be a
bare `return {"skipped": ...}` for ibkr / tradier / lime.
"""
import asyncio
import pytest

import app.monitors.reconciler as rec


class _Pos:
    def __init__(self, osi, remaining, avg):
        self.osi_symbol = osi
        self.remaining = remaining
        self.avg_price = avg
        self.status = "OPEN"


class _Query:
    def __init__(self, rows): self._rows = rows
    def filter(self, *a, **k): return self
    def all(self): return self._rows


class _DB:
    """Returns positions for a Position query and nothing for anything else,
    so the stale-PENDING pass doesn't get handed Position rows."""
    def __init__(self, rows, orders=None):
        self._rows = rows
        self._orders = orders or []
        self.committed = False
    def query(self, model=None, *a, **k):
        name = getattr(model, "__name__", "")
        return _Query(self._orders if name == "Order" else self._rows)
    def commit(self): self.committed = True
    def close(self): pass


def _run(monkeypatch, broker_rows, local_rows, supports=True):
    db = _DB(local_rows)
    monkeypatch.setattr(rec, "SessionLocal", lambda: db)
    import app.execution.broker_router as br
    monkeypatch.setattr(br, "supports_positions", lambda: supports)
    async def _fp(): return broker_rows
    monkeypatch.setattr(br, "fetch_positions", _fp)
    out = asyncio.run(rec._shadow_reconcile("ibkr"))
    return out, db


def _b(osi, qty, avg, sec="OPT"):
    return {"osi_symbol": osi, "qty": qty, "avg_cost": avg,
            "current_price": 0.0, "sec_type": sec}


class TestShadowNeverWrites:
    def test_never_commits(self, monkeypatch):
        out, db = _run(monkeypatch, [_b("AAPL260821C00200000", 5, 1.0)], [])
        assert db.committed is False
        assert out["mode"] == "shadow"


class TestDivergenceDetection:
    def test_clean_when_agreeing(self, monkeypatch):
        out, _ = _run(monkeypatch,
                      [_b("AAPL260821C00200000", 2, 1.50)],
                      [_Pos("AAPL260821C00200000", 2, 1.50)])
        assert out["only_on_broker"] == [] and out["only_in_db"] == []
        assert out["qty_drift"] == [] and out["avg_price_drift"] == []

    def test_position_only_on_broker(self, monkeypatch):
        out, _ = _run(monkeypatch, [_b("AAPL260821C00200000", 2, 1.5)], [])
        assert out["only_on_broker"] == ["AAPL260821C00200000"]

    def test_position_only_in_db(self, monkeypatch):
        out, _ = _run(monkeypatch, [], [_Pos("AAPL260821C00200000", 2, 1.5)])
        assert out["only_in_db"] == ["AAPL260821C00200000"]

    def test_qty_drift(self, monkeypatch):
        out, _ = _run(monkeypatch,
                      [_b("AAPL260821C00200000", 5, 1.5)],
                      [_Pos("AAPL260821C00200000", 2, 1.5)])
        assert "broker=5 local=2" in out["qty_drift"][0]

    def test_avg_price_drift_flags_the_100x_bug(self, monkeypatch):
        """If a bridge ever forgets the multiplier, shadow mode must shout."""
        out, _ = _run(monkeypatch,
                      [_b("AAPL260821C00200000", 2, 150.0)],
                      [_Pos("AAPL260821C00200000", 2, 1.50)])
        assert out["avg_price_drift"], "100x avg_cost must be reported"

    def test_penny_rounding_is_not_drift(self, monkeypatch):
        out, _ = _run(monkeypatch,
                      [_b("AAPL260821C00200000", 2, 1.505)],
                      [_Pos("AAPL260821C00200000", 2, 1.50)])
        assert out["avg_price_drift"] == []


class TestExclusions:
    def test_spread_combo_rows_are_skipped(self, monkeypatch):
        """A vertical is one local row but two broker legs — do not diff it."""
        out, _ = _run(monkeypatch, [],
                      [_Pos("SPXW260819P07710000|SPXW260819P07705000", 1, 0.75)])
        assert out["only_in_db"] == []
        assert out["local_positions"] == 0

    def test_non_option_broker_rows_are_skipped(self, monkeypatch):
        """ibkr-oci holds 1 share of TSLA stock the bot does not manage."""
        out, _ = _run(monkeypatch, [_b("TSLA", 1, 380.70, sec="STK")], [])
        assert out["only_on_broker"] == []
        assert out["broker_positions"] == 0


class TestUnsupportedBroker:
    def test_missing_seam_reports_skip(self, monkeypatch):
        out, _ = _run(monkeypatch, [], [], supports=False)
        assert "no fetch_positions seam" in out["skipped"]


class TestStalePendingDetection:
    """A crash between the DB commit and the broker submit leaves a PENDING
    row for an order that was never placed. Nothing swept those on non-Public
    brokers; shadow mode now at least reports them."""

    class _Order:
        def __init__(self, oid, pubid, minutes_old):
            import datetime as _dt
            self.id = oid
            self.public_order_id = pubid
            self.side = "BUY"
            self.osi_symbol = "AAPL260821C00200000"
            self.status = "PENDING"
            self.placed_at = (_dt.datetime.now(_dt.timezone.utc)
                              - _dt.timedelta(minutes=minutes_old))

    class _OrderQuery:
        def __init__(self, rows): self._rows = rows
        def filter(self, *a, **k): return self
        def all(self): return self._rows

    class _ODB:
        def __init__(self, rows): self._rows = rows
        def query(self, *a, **k):
            return TestStalePendingDetection._OrderQuery(self._rows)

    def _run(self, monkeypatch, local_orders, broker_orders):
        import app.execution.broker_router as br
        async def _fo(): return broker_orders
        monkeypatch.setattr(br, "fetch_orders", _fo)
        return asyncio.run(rec._stale_pending_orders(self._ODB(local_orders), "ibkr"))

    def test_reports_order_broker_never_saw(self, monkeypatch):
        out = self._run(monkeypatch, [self._Order(7, "abc", 45)], [])
        assert out and "#7" in out[0]

    def test_ignores_order_still_live_at_broker(self, monkeypatch):
        out = self._run(monkeypatch, [self._Order(7, "abc", 45)], [{"id": "abc"}])
        assert out == []

    def test_says_nothing_when_broker_fetch_fails(self, monkeypatch):
        """Without the broker list we cannot tell stale from live — stay quiet
        rather than flag every open order every minute."""
        import app.execution.broker_router as br
        async def _boom(): raise RuntimeError("api down")
        monkeypatch.setattr(br, "fetch_orders", _boom)
        out = asyncio.run(rec._stale_pending_orders(
            self._ODB([self._Order(7, "abc", 45)]), "ibkr"))
        assert out == []
