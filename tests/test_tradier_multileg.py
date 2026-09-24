"""Tests for Tradier multileg (vertical spread) order construction.

The payload builder is pure, so it is tested directly rather than through a
mocked HTTP round-trip — the thing that can silently go wrong here is a leg
being built at the wrong strike or the wrong side, not the POST itself.

Placing a credit spread sells a leg to open. Everything else in this codebase
only ever buys options, so the validation below is the boundary that keeps a
mangled order from becoming a naked short.
"""
from datetime import date

import pytest

from app.execution.tradier_sdk_bridge import _build_multileg_payload, net_fill_price
from app.ingest.spread_parser import parse_spread_alert, spread_legs

SESSION = date(2026, 8, 6)


def _legs(text, qty=2, session=SESSION):
    return spread_legs(parse_spread_alert(text), session, qty)


class TestLegConstruction:
    def test_opening_a_call_credit_spread_sells_the_short_leg(self):
        legs = _legs("OPENED: CCS SPX 7390/95 $1.25 x3", qty=3)
        assert legs == [
            ("SPXW260806C07390000", "sell_to_open", 3),
            ("SPXW260806C07395000", "buy_to_open", 3),
        ]

    def test_opening_a_put_credit_spread_uses_puts(self):
        legs = _legs("OPENED: SPX PCS 7690/85 $1.05 x2", qty=2)
        assert legs == [
            ("SPXW260806P07690000", "sell_to_open", 2),
            ("SPXW260806P07685000", "buy_to_open", 2),
        ]

    def test_closing_reverses_both_legs(self):
        legs = _legs("CLOSED: CCS SPX 7390/95 $0.30 ALL OUT", qty=3)
        assert [side for _osi, side, _q in legs] == ["buy_to_close", "sell_to_close"]

    def test_spx_legs_carry_the_weekly_root(self):
        """SPX 0DTE trades as SPXW; the bridge maps that back to the SPX
        underlying for the order's `symbol` param."""
        assert all(osi.startswith("SPXW") for osi, _s, _q in _legs("OPENED: CCS SPX 7390/95 $1.25"))

    def test_qty_comes_from_sizing_not_the_message(self):
        """The analyst runs a $5k account; his x3 is not automatically ours."""
        legs = _legs("OPENED: CCS SPX 7390/95 $1.25 x3", qty=1)
        assert [q for _o, _s, q in legs] == [1, 1]

    def test_zero_qty_is_refused(self):
        with pytest.raises(ValueError):
            _legs("OPENED: CCS SPX 7390/95 $1.25 x3", qty=0)


class TestPayload:
    def test_credit_spread_payload_matches_the_tradier_schema(self):
        legs = _legs("OPENED: CCS SPX 7390/95 $1.25 x3", qty=3)
        data = _build_multileg_payload(legs, 1.25, order_type="credit", order_ref="twi-1")
        assert data == {
            "class": "multileg",
            "type": "credit",
            "duration": "day",
            "price": "1.25",
            "symbol": "SPX",
            "option_symbol[0]": "SPXW260806C07390000",
            "side[0]": "sell_to_open",
            "quantity[0]": "3",
            "option_symbol[1]": "SPXW260806C07395000",
            "side[1]": "buy_to_open",
            "quantity[1]": "3",
            "tag": "twi-1",
        }

    def test_closing_is_a_debit(self):
        legs = _legs("CLOSED: CCS SPX 7390/95 $0.30 ALL OUT", qty=3)
        data = _build_multileg_payload(legs, 0.30, order_type="debit")
        assert (data["type"], data["price"]) == ("debit", "0.30")

    def test_price_is_the_magnitude_and_type_carries_direction(self):
        """A credit is posted as a positive net; a negative price would be
        Tradier-invalid and almost certainly a sign-convention bug upstream."""
        legs = _legs("OPENED: CCS SPX 7390/95 $1.25")
        with pytest.raises(ValueError, match="non-negative"):
            _build_multileg_payload(legs, -1.25, order_type="credit")


class TestRefusals:
    def test_a_single_leg_is_not_a_spread(self):
        legs = _legs("OPENED: CCS SPX 7390/95 $1.25")[:1]
        with pytest.raises(ValueError, match="2-4 legs"):
            _build_multileg_payload(legs, 1.25, order_type="credit")

    def test_more_than_four_legs_is_refused(self):
        legs = [(f"SPXW260806C0739{i}000", "sell_to_open", 1) for i in range(5)]
        with pytest.raises(ValueError, match="2-4 legs"):
            _build_multileg_payload(legs, 1.25, order_type="credit")

    def test_mixed_underlyings_are_refused(self):
        """One `symbol` param covers the order, so a mixed pair would trade
        entirely under whichever root won."""
        legs = [("SPXW260806C07390000", "sell_to_open", 1),
                ("AAPL260806C00220000", "buy_to_open", 1)]
        with pytest.raises(ValueError, match="multiple underlyings"):
            _build_multileg_payload(legs, 1.25, order_type="credit")

    def test_duplicate_leg_is_refused(self):
        legs = [("SPXW260806C07390000", "sell_to_open", 1),
                ("SPXW260806C07390000", "buy_to_open", 1)]
        with pytest.raises(ValueError, match="duplicate"):
            _build_multileg_payload(legs, 1.25, order_type="credit")

    def test_unknown_side_is_refused(self):
        legs = [("SPXW260806C07390000", "sell", 1),
                ("SPXW260806C07395000", "buy_to_open", 1)]
        with pytest.raises(ValueError, match="bad side"):
            _build_multileg_payload(legs, 1.25, order_type="credit")

    def test_unknown_order_type_is_refused(self):
        legs = _legs("OPENED: CCS SPX 7390/95 $1.25")
        with pytest.raises(ValueError, match="bad multileg type"):
            _build_multileg_payload(legs, 1.25, order_type="limit")

    def test_zero_quantity_leg_is_refused(self):
        legs = [("SPXW260806C07390000", "sell_to_open", 0),
                ("SPXW260806C07395000", "buy_to_open", 1)]
        with pytest.raises(ValueError, match="quantity"):
            _build_multileg_payload(legs, 1.25, order_type="credit")


class TestNetFillPrice:
    def test_credit_spread_fill_nets_to_the_credit_received(self):
        order = {"leg": [
            {"side": "sell_to_open", "avg_fill_price": "12.50", "exec_quantity": "3"},
            {"side": "buy_to_open", "avg_fill_price": "11.25", "exec_quantity": "3"},
        ]}
        assert net_fill_price(order) == pytest.approx(1.25)

    def test_closing_debit_nets_negative(self):
        order = {"leg": [
            {"side": "buy_to_close", "avg_fill_price": "12.50", "exec_quantity": "3"},
            {"side": "sell_to_close", "avg_fill_price": "12.20", "exec_quantity": "3"},
        ]}
        assert net_fill_price(order) == pytest.approx(-0.30)

    def test_a_partially_filled_spread_has_no_net_price_yet(self):
        """Booking a basis off half a spread would record a price that was
        never paid."""
        order = {"leg": [
            {"side": "sell_to_open", "avg_fill_price": "12.50", "exec_quantity": "3"},
            {"side": "buy_to_open", "avg_fill_price": None, "exec_quantity": "0"},
        ]}
        assert net_fill_price(order) is None

    def test_no_legs_means_no_price(self):
        assert net_fill_price({}) is None


class TestErrorBodyNotMasked:
    """A 4xx with an empty body must surface the HTTP status, not a JSON error.

    Regression: Tradier 400'd two matae spreads (2026-08-18, 2026-08-19) and
    the operator saw "Expecting value: line 1 column 1 (char 0)" because
    resp.json() ran before the status was checked.
    """

    class _Resp:
        def __init__(self, status_code, text, payload=None):
            self.status_code = status_code
            self.text = text
            self._payload = payload

        def json(self):
            if self._payload is None:
                raise ValueError("Expecting value: line 1 column 1 (char 0)")
            return self._payload

    def test_empty_body_reports_status(self):
        from app.execution.tradier_sdk_bridge import _body
        with pytest.raises(RuntimeError, match=r"Tradier HTTP 400 with empty body"):
            _body(self._Resp(400, ""))

    def test_html_body_reports_status_and_snippet(self):
        from app.execution.tradier_sdk_bridge import _body
        with pytest.raises(RuntimeError, match=r"Tradier HTTP 502 with non-JSON body"):
            _body(self._Resp(502, "<html>bad gateway</html>"))

    def test_valid_json_passes_through(self):
        from app.execution.tradier_sdk_bridge import _body
        assert _body(self._Resp(200, "{}", {"order": {"id": 7}})) == {"order": {"id": 7}}

    def test_null_json_becomes_empty_dict(self):
        from app.execution.tradier_sdk_bridge import _body
        assert _body(self._Resp(200, "null", {})) == {}
