"""
VIX sizing on IBKR.

The bug this covers: vix_sizing_enabled=true on the IBKR box had no data source
(the refresher only fed from Public), so _fetch_vix_level() returned None
forever and vix_unavailable_scale cut every entry to 0.50x. Confirmed live on
ibkr-oci 2026-08-20.
"""
import pytest

import app.execution.broker_router as router


class _FakeTicker:
    """Shape of an ib_async Ticker for an index: no bid/ask, just last/close."""
    def __init__(self, last=None, close=None):
        self.bid = self.ask = float("nan")
        self.last = last if last is not None else float("nan")
        self.close = close if close is not None else float("nan")


class _FakeIB:
    def __init__(self, ticker, qualify=True, delayed_ticker=None):
        self._ticker, self._qualify = ticker, qualify
        self._delayed = delayed_ticker
        self.snapshot_used = None
        self.md_types = []

    def reqMarketDataType(self, t):
        self.md_types.append(t)
        if t == 3 and self._delayed is not None:
            self._ticker = self._delayed

    async def qualifyContractsAsync(self, *contracts):
        if not self._qualify:
            return [None]
        for c in contracts:
            c.conId = 13455763          # real VIX conId shape
        return list(contracts)

    def reqMktData(self, contract, ticks, snapshot, regulatory):
        self.snapshot_used = snapshot
        return self._ticker


@pytest.fixture
def bridge(monkeypatch):
    import app.execution.ibkr_sdk_bridge as b
    monkeypatch.setattr(b, "_DRY_RUN", lambda: False)
    monkeypatch.setattr(b, "_wait_for_quotes", _noop_wait)
    return b


async def test_index_session_is_separate_from_the_trading_session(bridge, monkeypatch):
    """reqMarketDataType is per-connection. Serving VIX off the trading session
    would force delayed option quotes onto the order path (probed live: this
    account gets Error 354 on live VIX, delayed only)."""
    trading_ib = _FakeIB(_FakeTicker(last=99.0))
    monkeypatch.setattr(bridge, "_get_ib", _const(trading_ib))
    monkeypatch.setattr(bridge, "_get_vix_ib", _const(_FakeIB(_FakeTicker(last=17.42))))
    assert await bridge.fetch_index_level("VIX") == 17.42
    assert trading_ib.snapshot_used is None, "VIX quote leaked onto the trading session"


async def test_no_index_session_is_unknown_not_a_crash(bridge, monkeypatch):
    monkeypatch.setattr(bridge, "_get_vix_ib", _const(None))
    assert await bridge.fetch_index_level("VIX") is None


async def _noop_wait(tickers, timeout_ms):
    return None


async def test_reads_the_live_index_level(bridge, monkeypatch):
    ib = _FakeIB(_FakeTicker(last=17.42))
    monkeypatch.setattr(bridge, "_get_vix_ib", _const(ib))
    assert await bridge.fetch_index_level("VIX") == 17.42


async def test_falls_back_to_close_out_of_hours(bridge, monkeypatch):
    """Yesterday's close is a better sizing input than no VIX at all — with no
    close fallback the sizer silently halves every overnight entry."""
    ib = _FakeIB(_FakeTicker(last=None, close=16.08))
    monkeypatch.setattr(bridge, "_get_vix_ib", _const(ib))
    assert await bridge.fetch_index_level("VIX") == 16.08


async def test_uses_a_snapshot_not_a_streaming_line(bridge, monkeypatch):
    """Streaming would burn one of the account's ~100 market-data lines every
    minute, competing with the option ticker cache."""
    ib = _FakeIB(_FakeTicker(last=17.0))
    monkeypatch.setattr(bridge, "_get_vix_ib", _const(ib))
    await bridge.fetch_index_level("VIX")
    assert ib.snapshot_used is True


async def test_unqualified_contract_returns_none_not_an_exception(bridge, monkeypatch):
    """No CBOE index subscription must degrade to 'unknown', never crash the
    refresher loop."""
    ib = _FakeIB(_FakeTicker(), qualify=False)
    monkeypatch.setattr(bridge, "_get_vix_ib", _const(ib))
    assert await bridge.fetch_index_level("VIX") is None


async def test_router_dispatches_to_the_active_broker(monkeypatch):
    called = {}

    class _Stub:
        @staticmethod
        async def fetch_index_level(symbol):
            called["symbol"] = symbol
            return 19.5

    monkeypatch.setattr(router, "_bridge", lambda: _Stub)
    assert await router.fetch_index_level("VIX") == 19.5
    assert called["symbol"] == "VIX"


async def test_router_returns_none_for_a_vendor_without_a_source(monkeypatch):
    """tradier/lime bridges have no fetch_index_level; that is 'unknown', not
    an AttributeError inside the refresher."""
    class _NoSource:
        pass

    monkeypatch.setattr(router, "_bridge", lambda: _NoSource)
    assert await router.fetch_index_level("VIX") is None


def _const(value):
    async def _fn(*a, **k):
        return value
    return _fn


async def test_empty_index_marker_is_not_read_as_a_negative_vix(bridge, monkeypatch):
    """IBKR marks 'no data' on an index as bid/ask = -1 with last/close = nan
    (probed live at 03:22 ET). Reading that as a number gives VIX = -1 and a
    sizer acting on a volatility level that cannot exist."""
    t = _FakeTicker()
    t.bid = t.ask = -1.0
    monkeypatch.setattr(bridge, "_get_vix_ib", _const(_FakeIB(t)))
    assert await bridge.fetch_index_level("VIX") is None


async def test_falls_back_to_delayed_when_live_is_empty(bridge, monkeypatch):
    """Subscription lapsed or wrong hour: delayed's prior close still beats
    'unknown', which costs every entry the 0.50x haircut."""
    empty = _FakeTicker()
    empty.bid = empty.ask = -1.0
    ib = _FakeIB(empty, delayed_ticker=_FakeTicker(close=15.31))
    monkeypatch.setattr(bridge, "_get_vix_ib", _const(ib))
    monkeypatch.setattr(bridge, "_VIX_MD_TYPE", lambda: 1)
    assert await bridge.fetch_index_level("VIX") == 15.31
    assert 3 in ib.md_types, "never tried delayed"
    assert ib.md_types[-1] == 1, "left the session on delayed — poisons later live reads"
