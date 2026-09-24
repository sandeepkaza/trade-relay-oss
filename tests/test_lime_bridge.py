"""Lime bridge: the parts that are wrong silently.

Everything here is pure payload/symbol construction — no network. The point is
the seams where a mistake doesn't raise, it just trades something else:
an unpadded symbol, a fly whose ratio legs got flattened, a credit spread sent
with the side that means debit.
"""
from __future__ import annotations

import asyncio
import json

import pytest

from app.execution import lime_sdk_bridge as bridge
from app.ingest.parser import _build_osi
from datetime import date


# ── Symbol translation ─────────────────────────────────────────────────────

@pytest.mark.parametrize("root,expiry,strike,cp,expected", [
    # Every symbol Lime prints in its own docs, plus the one we actually trade.
    ("TSLA",  date(2024, 9, 20),   35.0, "C", "TSLA  240920C00035000"),
    ("BAC",   date(2025, 1, 17),   32.0, "C", "BAC   250117C00032000"),
    ("CZOO1", date(2023, 8, 18),    3.0, "C", "CZOO1 230818C00003000"),
    ("SPXW",  date(2026, 8, 13), 7800.0, "P", "SPXW  260813P07800000"),
])
def test_osi_to_lime_matches_documented_symbols(root, expiry, strike, cp, expected):
    """_build_osi emits the compact 19-char OSI; Lime wants the root padded to
    six. An unpadded symbol is a rejected order, so this is load-bearing."""
    assert bridge._to_lime(_build_osi(root, expiry, strike, cp)) == expected
    assert len(expected) == 21


def test_lime_symbol_is_idempotent():
    """A symbol that arrives already padded must not get padded twice."""
    padded = "SPXW  260813P07800000"
    assert bridge._to_lime(padded) == padded


def test_from_lime_round_trips_to_app_osi():
    osi = _build_osi("SPXW", date(2026, 8, 13), 7800.0, "P")
    assert bridge._from_lime(bridge._to_lime(osi)) == osi


def test_spx_weeklies_trade_under_the_spx_underlying():
    """SPXW carries its own root but the order's underlying is SPX — same rule
    as the Tradier and IBKR bridges."""
    assert bridge._osi_parts("SPXW260813P07800000")[0] == "SPX"
    assert bridge._osi_parts("SPX260813P07800000")[0] == "SPX"
    assert bridge._osi_parts("AAPL260420C00272500")[0] == "AAPL"


@pytest.mark.parametrize("bad", ["", "SPXW", "260813P07800000", "TOOLONGROOT260813P07800000"])
def test_bad_osi_refused(bad):
    with pytest.raises(ValueError):
        bridge._osi_parts(bad)


# ── Single-leg payload ─────────────────────────────────────────────────────

def test_single_payload_shape(monkeypatch):
    monkeypatch.setattr(bridge, "_account", lambda: "12345678@vision")
    p = bridge._build_single_payload("SPXW260813P07800000", "BUY", 2, 1.234,
                                     order_ref="abc-123-xyz")
    assert p["symbol"] == "SPXW  260813P07800000"
    assert p["side"] == "buy"
    assert p["quantity"] == 2
    assert p["order_type"] == "limit"
    assert p["price"] == 1.23                      # rounded to the penny
    assert p["client_order_id"] == "abc123xyz"     # alnum only, Lime's rule


def test_single_payload_sell():
    p = bridge._build_single_payload("SPXW260813P07800000", "SELL", 1, 2.0)
    assert p["side"] == "sell"
    assert "client_order_id" not in p


def test_client_order_id_capped_at_32():
    p = bridge._build_single_payload("SPXW260813P07800000", "BUY", 1, 1.0,
                                     order_ref="x" * 80)
    assert len(p["client_order_id"]) == 32


# ── Multileg payload — where a fly gets silently flattened ─────────────────

def _fly_legs():
    """Body doubled, wings single — what spread_legs emits for a long fly."""
    return [("SPXW260813P07800000", "sell_to_open", 2),   # body
            ("SPXW260813P07790000", "buy_to_open", 1),    # lower wing
            ("SPXW260813P07810000", "buy_to_open", 1)]    # upper wing


def test_fly_keeps_its_ratio_legs(monkeypatch):
    """Lime multiplies leg ratio by order quantity. Flattening the body to 1
    turns a butterfly into a different position entirely."""
    monkeypatch.setattr(bridge, "_account", lambda: "1@vision")
    p = bridge._build_multileg_payload(_fly_legs(), 1.80, is_credit=False)
    assert [leg["quantity"] for leg in p["legs"]] == [2, 1, 1]
    assert [leg["side"] for leg in p["legs"]] == ["sell", "buy", "buy"]
    assert p["symbol"] == "SPX"          # root is the UNDERLYING, not a leg
    assert p["side"] == "buy"            # a long fly is a DEBIT
    assert p["price"] == 1.80


def test_credit_spread_side_is_sell(monkeypatch):
    """'Use buy if net debit or even. Use sell, if net credit.' Getting this
    backwards asks to pay a price the analyst posted as received."""
    monkeypatch.setattr(bridge, "_account", lambda: "1@vision")
    legs = [("SPXW260813C07800000", "sell_to_open", 1),
            ("SPXW260813C07805000", "buy_to_open", 1)]
    assert bridge._build_multileg_payload(legs, 0.75, is_credit=True)["side"] == "sell"
    assert bridge._build_multileg_payload(legs, 0.75, is_credit=False)["side"] == "buy"


def test_multileg_rejects_malformed():
    one = [("SPXW260813P07800000", "buy_to_open", 1)]
    with pytest.raises(ValueError, match="2-4 legs"):
        bridge._build_multileg_payload(one, 1.0, is_credit=False)

    dupe = [("SPXW260813P07800000", "buy_to_open", 1),
            ("SPXW260813P07800000", "sell_to_open", 1)]
    with pytest.raises(ValueError, match="duplicate"):
        bridge._build_multileg_payload(dupe, 1.0, is_credit=False)

    mixed = [("SPXW260813P07800000", "buy_to_open", 1),
             ("AAPL260813P00100000", "sell_to_open", 1)]
    with pytest.raises(ValueError, match="multiple underlyings"):
        bridge._build_multileg_payload(mixed, 1.0, is_credit=False)

    with pytest.raises(ValueError, match="non-negative"):
        bridge._build_multileg_payload(_fly_legs(), -1.0, is_credit=False)

    zero_ratio = [("SPXW260813P07800000", "buy_to_open", 0),
                  ("SPXW260813P07790000", "sell_to_open", 1)]
    with pytest.raises(ValueError, match="ratio"):
        bridge._build_multileg_payload(zero_ratio, 1.0, is_credit=False)


# ── Status + fill price ────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("new", "PENDING"), ("pending_new", "PENDING"), ("pending_cancel", "PENDING"),
    ("partially_filled", "PARTIALLY_FILLED"), ("filled", "FILLED"),
    ("canceled", "CANCELLED"), ("rejected", "REJECTED"), ("suspended", "REJECTED"),
    ("done_for_day", "EXPIRED"),
    ("", "PENDING"), ("something_new_lime_added", "PENDING"),
])
def test_status_translation(raw, expected):
    """Unknown statuses map to PENDING: keep polling is the safe default —
    guessing TERMINAL would abandon a live order."""
    assert bridge.translate_status(raw) == expected


@pytest.mark.asyncio
async def test_feed_order_event_wakes_waiter():
    """A subscribeOrders frame must unblock wait_order_update immediately."""
    bridge._feed_waiters.clear()
    bridge._feed_snapshots.clear()
    bridge._feed_seq.clear()
    frame = json.dumps({
        "t": "o",
        "data": [{"client_id": "abc123", "order_status": "filled", "executed_quantity": 1}],
    })
    bridge._handle_feed_message(frame)
    snap = await bridge.wait_order_update("abc123", timeout=0.2)
    assert snap is not None
    assert snap["order_status"] == "filled"


def test_feed_position_tick_caches_price():
    bridge._feed_prices.clear()
    bridge._handle_feed_message(json.dumps({
        "t": "p",
        "data": [{"account": "demo-account@demo", "positions": [
            {"symbol": "SPXW  260909C06500000", "current_price": 1.25, "quantity": 1},
        ]}],
    }))
    px, _ts = next(iter(bridge._feed_prices.values()))
    assert px == 1.25


def test_fill_poll_default_is_subsecond():
    """A 2s first-sleep was the fill-detect delay. Default must be sub-second."""
    from app.execution.lime_broker import _FILL_POLL_INTERVAL
    assert 0.05 <= _FILL_POLL_INTERVAL() <= 0.5


def test_http_client_reuses_one_session():
    a = bridge._raw()
    b = bridge._raw()
    assert a is b


@pytest.mark.asyncio
async def test_fetch_prices_chunks_above_quote_batch(monkeypatch):
    """Lime 400s a quotes POST of 20 symbols. Chunking keeps the tail."""
    monkeypatch.setattr(bridge, "_DRY_RUN", lambda: False)
    monkeypatch.setattr(bridge, "_creds_ok", lambda: True)
    seen: list[int] = []

    class _Resp:
        status_code = 200
        def raise_for_status(self):
            return None
        def json(self):
            return []

    async def fake_req(method, path, **kw):
        body = kw.get("json") or []
        seen.append(len(body))
        assert len(body) <= bridge._QUOTE_BATCH
        return _Resp()

    monkeypatch.setattr(bridge, "_req", fake_req)
    osis = [f"SPXW260909P{i:08d}" for i in range(20)]
    await bridge.fetch_prices(osis)
    assert seen == [15, 5]


def test_replaced_is_terminal_not_pending():
    """A replaced order is dead; its replacement carries a new id. Leaving it
    PENDING would poll a corpse until the timeout auto-cancel fires."""
    assert bridge.translate_status("replaced") == "CANCELLED"


def test_net_fill_price_none_until_something_executes():
    assert bridge.net_fill_price({"executed_quantity": 0, "price": 1.5}) is None
    assert bridge.net_fill_price({}) is None


def test_net_fill_price_prefers_executed_price():
    o = {"executed_quantity": 1, "executed_price": 1.82, "price": 2.00}
    assert bridge.net_fill_price(o) == 1.82


def test_net_fill_price_falls_back_to_limit():
    """REST OrderDetails may not carry executed_price (it is documented on the
    streaming event but absent from Lime's own REST model). The limit is exact
    for a fill at the limit and wrong only on price improvement."""
    assert bridge.net_fill_price({"executed_quantity": 1, "price": 2.00}) == 2.00


def test_net_fill_price_is_a_magnitude():
    """A credit multileg can report a negative net; callers carry direction in
    the structure, so a sign here would double-negate the P&L."""
    assert bridge.net_fill_price({"executed_quantity": 1, "executed_price": -0.75}) == 0.75


@pytest.mark.asyncio
@pytest.mark.parametrize("quotes, block, reaches_broker", [
    ({}, True, False),                               # 403 / no quote → refused
    ({"MSFT260925C00510000": 2.41}, True, True),     # quoted → proceeds
    ({}, False, True),                               # knob off → legacy blind bid
])
async def test_buy_without_quote_is_refused(monkeypatch, quotes, block, reaches_broker):
    await _run_gate(monkeypatch, "MSFT260925C00510000", quotes, block, reaches_broker)


@pytest.mark.asyncio
async def test_spx_daily_gate_quotes_spxw(monkeypatch):
    """SPX 7750C 2026-09-23 (a Wednesday): Lime only lists it as SPXW. The gate
    must quote SPXW, or every 0DTE SPX alert is refused as unquotable."""
    await _run_gate(monkeypatch, "SPX260923C07750000",
                    {"SPXW260923C07750000": 2.75}, True, True)


async def _run_gate(monkeypatch, osi, quotes, block, reaches_broker):
    """MSFT 510C 2026-09-23: quotes 403'd (OPRA token not activated) and the
    BUY bid $3.10 blind into a $2.41 mid. No quote must mean no BUY."""
    from types import SimpleNamespace
    from app.execution import lime_broker
    from app.execution.public_executor import PublicExecutor

    monkeypatch.setattr(lime_broker, "_ORDERS_ENABLED", lambda: True)
    monkeypatch.setattr(lime_broker, "_DRY_RUN", lambda: False)
    monkeypatch.setattr(lime_broker, "_BLOCK_BUY_WITHOUT_QUOTE", lambda: block)

    async def fake_prices(osis):
        return quotes
    monkeypatch.setattr(bridge, "fetch_prices", fake_prices)

    async def noop(**kw):
        return None
    monkeypatch.setattr(lime_broker.trade_logger, "log_alert_blocked", noop)

    reached = []

    async def fake_super(self, *a, **kw):
        reached.append(1)
    monkeypatch.setattr(PublicExecutor, "execute", fake_super)

    ex = lime_broker.LimeExecutor.__new__(lime_broker.LimeExecutor)
    alert = SimpleNamespace(action="BUY", osi_symbol=osi)
    await ex.execute(alert, None, "Sniper Alerts")
    assert bool(reached) is reaches_broker


@pytest.mark.asyncio
async def test_place_rejection_carries_lime_reason(monkeypatch):
    """A 400 from /orders/place must surface Lime's message, not just the
    status line — SPXW 7750C 2026-09-23 failed with the reason discarded."""
    monkeypatch.setattr(bridge, "_VALIDATE_FIRST", lambda: False)

    class _Resp:
        status_code = 400
        content = b"x"
        text = '{"code":"bad_request","message":"Insufficient buying power"}'

    async def fake_req(method, path, **kw):
        return _Resp()
    monkeypatch.setattr(bridge, "_req", fake_req)
    with pytest.raises(RuntimeError, match="Insufficient buying power"):
        await bridge._place({}, "BUY 1x X @ 1")
