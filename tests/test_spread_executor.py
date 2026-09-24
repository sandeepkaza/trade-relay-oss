"""Tests for the credit-spread execution path.

Broker IO is stubbed — what matters here is what reaches the broker and what
gets written to the DB, not the HTTP call itself. The cases below are the ones
that cost money if they're wrong: sizing off risk instead of premium, the P&L
sign on a credit, and the gates that must hold when the feature is off.
"""
from datetime import date
from decimal import Decimal

import pytest

from app.core.config_manager import cfg
from app.core.models import Position
from app.execution.spread_executor import SpreadExecutor, combo_key
from app.ingest.spread_parser import parse_spread_alert, spread_legs

SESSION = date(2026, 8, 6)
OPEN_ALERT = "OPENED: CCS SPX 7390/95 $1.25 x3 @everyone"


class _WS:
    def __init__(self):
        self.sent = []

    async def broadcast(self, payload):
        self.sent.append(payload)


@pytest.fixture
def ex():
    return SpreadExecutor(_WS())


@pytest.fixture
def spread_cfg(monkeypatch):
    """Turn the feature on with a Tradier-shaped config."""
    real_get = cfg.get
    real_getboolean = cfg.getboolean
    real_getfloat = cfg.getfloat
    real_getint = cfg.getint
    overrides = {
        ("trading", "spreads_enabled"): True,
        ("trading", "broker"): "tradier",
        ("tradier", "orders_enabled"): True,
        ("trading", "dry_run"): False,
        ("trading", "spread_max_risk_dollars"): 400.0,
        ("trading", "spread_max_contracts"): 1,
    }

    def _pick(kind, section, key, fallback=None, **kw):
        if (section, key) in overrides:
            return overrides[(section, key)]
        return kind(section, key, fallback=fallback, **kw)

    monkeypatch.setattr(cfg, "get", lambda s, k, fallback=None, **kw: _pick(real_get, s, k, fallback, **kw))
    monkeypatch.setattr(cfg, "getboolean", lambda s, k, fallback=None, **kw: _pick(real_getboolean, s, k, fallback, **kw))
    monkeypatch.setattr(cfg, "getfloat", lambda s, k, fallback=None, **kw: _pick(real_getfloat, s, k, fallback, **kw))
    monkeypatch.setattr(cfg, "getint", lambda s, k, fallback=None, **kw: _pick(real_getint, s, k, fallback, **kw))
    return overrides


class TestSizing:
    """Contracts come from the risk budget, never from the analyst's count."""

    def test_one_contract_fits_a_400_budget(self, ex, spread_cfg):
        # 5-wide taken for 1.25 → (5 - 1.25) * 100 = $375 risk.
        assert ex._size(375.0) == 1

    def test_a_spread_costlier_than_the_budget_sizes_to_zero(self, ex, spread_cfg):
        """$470 of risk against a $400 cap is not "round down to one" — it is
        a trade this account cannot take."""
        assert ex._size(470.0) == 0

    def test_the_contract_ceiling_still_applies_when_risk_is_cheap(self, ex, spread_cfg):
        # $50 risk would afford 8, but spread_max_contracts is 1.
        assert ex._size(50.0) == 1

    def test_his_x3_does_not_become_our_x3(self, ex, spread_cfg):
        """He runs a $5k account and posts x3. On a $400 budget that is one."""
        alert = parse_spread_alert(OPEN_ALERT)
        assert alert.qty == 3
        assert ex._size(alert.max_loss_per_contract()) == 1


class TestGates:
    """Every one of these must stop the order on its own."""

    @pytest.mark.asyncio
    async def test_disabled_feature_places_nothing(self, ex, monkeypatch):
        monkeypatch.setattr(cfg, "getboolean",
                            lambda s, k, fallback=None, **kw: False if k == "spreads_enabled" else fallback)
        assert await ex.execute(parse_spread_alert(OPEN_ALERT), None, session=SESSION) is None

    @pytest.mark.asyncio
    async def test_wrong_broker_refuses(self, ex, spread_cfg, monkeypatch):
        """Public and IBKR have no multileg path here — legging in separately
        could leave a naked short if only one leg filled."""
        spread_cfg[("trading", "broker")] = "ibkr"
        calls = []
        monkeypatch.setattr(ex, "_block", lambda aid, reason: calls.append(reason))
        assert await ex.execute(parse_spread_alert(OPEN_ALERT), None, session=SESSION) is None
        assert "SPREAD_WRONG_BROKER" in calls[0]

    @pytest.mark.asyncio
    async def test_tradier_orders_disabled_refuses(self, ex, spread_cfg, monkeypatch):
        spread_cfg[("tradier", "orders_enabled")] = False
        calls = []
        monkeypatch.setattr(ex, "_block", lambda aid, reason: calls.append(reason))
        assert await ex.execute(parse_spread_alert(OPEN_ALERT), None, session=SESSION) is None
        assert "SPREAD_TRADIER_DISABLED" in calls[0]

    @pytest.mark.asyncio
    async def test_a_credit_wider_than_the_spread_is_refused(self, ex, spread_cfg, monkeypatch):
        """No vertical pays more than the distance between its strikes, so a
        $6 credit on a 5-wide is a malformed message, not a free lunch."""
        calls = []
        monkeypatch.setattr(ex, "_block", lambda aid, reason: calls.append(reason))
        alert = parse_spread_alert("OPENED: CCS SPX 7390/95 $6.00 x1")
        assert await ex.execute(alert, None, session=SESSION) is None
        assert "SPREAD_BAD_CREDIT" in calls[0]

    @pytest.mark.asyncio
    async def test_a_spread_over_the_risk_cap_is_refused(self, ex, spread_cfg, monkeypatch):
        """$0.30 credit on a 5-wide risks $470 — over a $400 budget."""
        calls = []
        monkeypatch.setattr(ex, "_block", lambda aid, reason: calls.append(reason))
        alert = parse_spread_alert("OPENED: CCS SPX 7390/95 $0.30 x1")
        assert await ex.execute(alert, None, session=SESSION) is None
        assert "SPREAD_TOO_LARGE" in calls[0]


class TestPositionMath:
    """Position was written for long premium; a credit spread inverts it."""

    def _pos(self, **kw):
        base = dict(osi_symbol=combo_key(spread_legs(parse_spread_alert(OPEN_ALERT), SESSION, 1)),
                    total_contracts=1, remaining=1, avg_price=1.25,
                    is_credit=True, spread_width=5.0, status="OPEN")
        base.update(kw)
        return Position(**base)

    def test_a_falling_mark_is_profit(self):
        assert self._pos(current_price=0.40).pnl_dollar() == pytest.approx(85.0)

    def test_a_rising_mark_is_loss(self):
        assert self._pos(current_price=2.40).pnl_dollar() == pytest.approx(-115.0)

    def test_expiring_worthless_books_the_whole_credit(self):
        """close_price 0.0 is the best possible outcome, not a missing value —
        the long-premium branch's falsy check would have hidden it."""
        pos = self._pos(status="CLOSED", close_price=0.0)
        assert pos.pnl_dollar() == pytest.approx(125.0)
        # The analyst posts this as "(+100%)" — 100% of the maximum profit.
        # pnl_pct means return on capital at risk everywhere else in this app,
        # and keeping that meaning is what makes a spread comparable to a long
        # option on the same dashboard: $125 earned against $375 risked.
        assert pos.pnl_pct() == pytest.approx(33.33, abs=0.01)

    def test_pnl_pct_is_measured_against_risk_not_against_the_credit(self):
        """Buying back at half the credit is not +50%; it is half of the most
        this trade could ever have made, against $375 of risk."""
        assert self._pos(current_price=0.625).pnl_pct() == pytest.approx(16.67, abs=0.01)

    def test_max_loss_is_width_minus_credit(self):
        assert self._pos().max_loss_dollar() == pytest.approx(375.0)

    def test_long_premium_positions_are_untouched(self):
        """The is_credit=False path must behave exactly as it always has."""
        pos = Position(osi_symbol="SPXW260806C07390000", total_contracts=2, remaining=2,
                       avg_price=1.00, current_price=1.50, status="OPEN", is_credit=False)
        assert pos.pnl_dollar() == pytest.approx(100.0)
        assert pos.pnl_pct() == pytest.approx(50.0)
        assert pos.max_loss_dollar() == pytest.approx(200.0)


class TestGuardrailHandoff:
    @pytest.mark.asyncio
    async def test_guardrails_see_risk_not_the_credit(self, ex, spread_cfg, monkeypatch):
        """max_trade_cost has always meant 'dollars at risk'. For a long option
        that equals the premium; for a credit spread it does not, so the risk
        is what gets passed."""
        seen = {}

        def _fake(**kw):
            seen.update(kw)
            return False, "stop here"

        monkeypatch.setattr("app.risk.guardrails.check_guardrails", _fake)
        monkeypatch.setattr(ex, "_block", lambda aid, reason: None)
        await ex.execute(parse_spread_alert(OPEN_ALERT), None, session=SESSION)

        assert seen["action"] == "BUY"
        # $375 of risk, expressed in premium units so the dollar cap matches.
        assert seen["price"] == Decimal("3.75")
        assert seen["osi_symbol"] == "SPXW260806C07390000"   # short leg, a real OSI
        assert seen["qty"] == 1


class TestExpirySweeper:
    """position_monitor._close_if_expired must not invent a P&L for a spread."""

    def _pos(self, **kw):
        base = dict(osi_symbol="SPXW260806C07390000|SPXW260806C07395000",
                    symbol="SPXW", expiry=None, total_contracts=1, remaining=1,
                    avg_price=1.25, current_price=1.25, is_credit=True,
                    spread_width=5.0, status="OPEN")
        base.update(kw)
        return Position(**base)

    def test_an_expired_credit_spread_is_left_open_for_manual_settlement(self):
        """Booking close_price = current_price would report exactly $0 whether
        the spread expired worthless (+$125) or through both strikes (-$375).
        A number that never happened is worse than an open row."""
        from app.monitors.position_monitor import _close_if_expired

        pos = self._pos()
        acted = _close_if_expired(None, pos)     # db unused on this branch
        assert acted is False
        assert pos.status == "OPEN"
        assert pos.close_price is None

    def test_long_premium_expiry_still_auto_closes(self):
        """The credit branch must not disturb the ordinary expiry sweep."""
        from app.monitors.position_monitor import _expiry_date

        pos = Position(osi_symbol="SPXW260806C07390000", is_credit=False,
                       total_contracts=1, remaining=1, avg_price=1.0, status="OPEN")
        assert _expiry_date(pos) == date(2026, 8, 6)

    def test_combo_expiry_is_read_from_the_first_leg(self):
        """Spread rows leave expiry NULL; the date lives in each OSI leg."""
        from app.monitors.position_monitor import _expiry_date

        pos = self._pos(
            osi_symbol="SPXW260911P07670000|SPXW260911P07665000|SPXW260911P07675000",
            is_credit=False,
        )
        assert _expiry_date(pos) == date(2026, 9, 11)


class TestManualSpreadClose:
    """Dashboard Close used TradierExecutor and TypeError'd on _force_plain."""

    def test_tradier_and_lime_accept_force_plain(self):
        import inspect
        from app.execution.tradier_broker import TradierExecutor
        from app.execution.lime_broker import LimeExecutor
        for cls in (TradierExecutor, LimeExecutor):
            params = inspect.signature(cls._place_on_public).parameters
            assert "_force_plain" in params, cls.__name__

    @pytest.mark.asyncio
    async def test_close_open_position_sends_btc_on_short_leg(self, ex, monkeypatch):
        seen = {}

        async def fake_place(legs, net, order_type, oid):
            seen["legs"] = legs
            seen["type"] = order_type
            seen["net"] = net
            return "oid-1"

        async def fake_poll(*a, **k):
            return None

        monkeypatch.setattr(ex, "_place", fake_place)
        monkeypatch.setattr(ex, "_poll_close", fake_poll)
        pos = Position(
            id=1,
            osi_symbol="SPXW260910C07605000|SPXW260910C07610000",
            remaining=1, avg_price=2.05, is_credit=True, status="OPEN",
            author="Matae",
        )
        await ex.close_open_position(pos, 1, 3.60, trigger="MANUAL")
        assert seen["type"] == "debit"
        assert seen["legs"][0] == ("SPXW260910C07605000", "buy_to_close", 1)
        assert seen["legs"][1] == ("SPXW260910C07610000", "sell_to_close", 1)

    @pytest.mark.asyncio
    async def test_close_open_position_marks_order_rejected_when_broker_raises(
            self, ex, monkeypatch):
        async def boom(*a, **k):
            raise RuntimeError("symbol expired")

        monkeypatch.setattr(ex, "_place", boom)
        pos = Position(
            id=1,
            osi_symbol="SPXW260911P07670000|SPXW260911P07665000|SPXW260911P07675000",
            remaining=1, avg_price=1.05, is_credit=False, status="OPEN",
            author="Matae",
        )
        with pytest.raises(RuntimeError, match="expired"):
            await ex.close_open_position(pos, 1, 0.05, trigger="MANUAL")
        from app.core.db import SessionLocal
        from app.core.models import Order
        db = SessionLocal()
        try:
            o = db.query(Order).filter(
                Order.osi_symbol == pos.osi_symbol, Order.trigger == "MANUAL"
            ).order_by(Order.id.desc()).first()
            assert o is not None
            assert o.status == "REJECTED"
            assert "SPREAD_CLOSE_FAILED" in (o.error_text or "")
        finally:
            db.close()


class TestComboMark:
    """fetch_prices cannot quote a short|long key; the monitor stitches legs."""

    def test_vertical_mark_is_short_minus_long(self):
        from app.execution.spread_executor import combo_mark
        mark = combo_mark({
            "SPXW260806C07390000": 2.10,
            "SPXW260806C07395000": 0.40,
        }, "SPXW260806C07390000|SPXW260806C07395000")
        assert mark == pytest.approx(1.70)

    def test_fly_mark_is_wings_minus_two_body(self):
        from app.execution.spread_executor import combo_mark
        assert combo_mark({"A": 1.00, "B": 2.50, "C": 0.80}, "A|B|C") == pytest.approx(1.30)

    def test_missing_leg_returns_none(self):
        from app.execution.spread_executor import combo_mark
        assert combo_mark({"A": 1.0}, "A|B") is None


class TestCreditPnlSign:
    """Winning CCS (sold 2.05, bought back 1.40) must show +$65, not -$65."""

    COMBO = "SPXW260910C07605000|SPXW260910C07610000"

    def test_fill_helpers_invert_credit(self):
        from app.core.models import fill_pnl_dollar, is_credit_combo_osi
        assert is_credit_combo_osi(self.COMBO) is True
        assert is_credit_combo_osi("A|B|C") is False
        assert is_credit_combo_osi("SPXW260910C07605000") is False
        assert fill_pnl_dollar(1.40, 2.05, 1, True) == pytest.approx(65.0)
        assert fill_pnl_dollar(1.40, 2.05, 1, False) == pytest.approx(-65.0)

    def test_order_to_dict_profit_inverts_two_leg_combo(self):
        from app.core.models import Order
        o = Order(osi_symbol=self.COMBO, side="SELL", status="FILLED",
                  fill_price=1.40, cost_basis=2.05, filled_qty=1, quantity=1)
        assert o.to_dict()["profit"] == pytest.approx(65.0)

    def test_long_option_order_profit_unchanged(self):
        from app.core.models import Order
        o = Order(osi_symbol="SPXW260910C07605000", side="SELL", status="FILLED",
                  fill_price=2.50, cost_basis=1.00, filled_qty=2, quantity=2)
        assert o.to_dict()["profit"] == pytest.approx(300.0)

    def test_fill_profit_inverts_two_leg_combo(self):
        from types import SimpleNamespace as NS
        from app.core.app import _fill_profit
        o = NS(fill_price=1.40, cost_basis=2.05, filled_qty=1, quantity=1,
               osi_symbol=self.COMBO)
        pnl, qty = _fill_profit(o)
        assert qty == 1
        assert pnl == pytest.approx(65.0)

    def test_combo_osi_without_is_credit_flag_still_inverts(self):
        pos = Position(osi_symbol=self.COMBO, total_contracts=1, remaining=0,
                       avg_price=2.05, close_price=1.40, status="CLOSED",
                       is_credit=False, spread_width=5.0)
        assert pos.pnl_dollar() == pytest.approx(65.0)
        assert pos.to_dict()["isCredit"] is True

    def test_live_tradier_winners_are_positive(self):
        """Exact fills from the live Tradier book (Show Closed was painting these red)."""
        from app.core.models import Order
        # 2026-08-21 PCS 7665/60: credit 0.85, debit 0.20 → +$65
        pos = Position(osi_symbol="SPXW260821P07665000|SPXW260821P07660000",
                       total_contracts=1, remaining=0, avg_price=0.85,
                       close_price=0.20, status="CLOSED", is_credit=True,
                       spread_width=5.0)
        assert pos.pnl_dollar() == pytest.approx(65.0)
        # 2026-09-04 PCS 7695/90: 1.00 → 0.30 → +$70
        pos2 = Position(osi_symbol="SPXW260904P07695000|SPXW260904P07690000",
                        total_contracts=1, remaining=0, avg_price=1.00,
                        close_price=0.30, status="CLOSED", is_credit=True,
                        spread_width=5.0)
        assert pos2.pnl_dollar() == pytest.approx(70.0)
        # 2026-09-10 CCS 7605/10: 2.05 → 2.50 → -$45 (this one WAS a loss)
        pos3 = Position(osi_symbol="SPXW260910C07605000|SPXW260910C07610000",
                        total_contracts=1, remaining=0, avg_price=2.05,
                        close_price=2.50, status="CLOSED", is_credit=True,
                        spread_width=5.0)
        assert pos3.pnl_dollar() == pytest.approx(-45.0)
        o = Order(osi_symbol=pos.osi_symbol, side="SELL", status="FILLED",
                  fill_price=0.20, cost_basis=0.85, filled_qty=1, quantity=1)
        assert o.to_dict()["profit"] == pytest.approx(65.0)

    def test_fly_keeps_debit_math_from_the_discord_trade(self):
        """2026-08-13: OPENED 10W FLY $1.80, CLOSED $4.15 ALL OUT +130%.

        A fly is a debit. Long-premium math is correct; treating the 3-leg
        combo as a credit vertical would paint this winner as -$235.
        """
        from app.core.models import Order, fill_pnl_dollar, is_credit_combo_osi
        fly = "SPXW260813P07800000|SPXW260813P07790000|SPXW260813P07810000"
        assert is_credit_combo_osi(fly) is False
        pos = Position(osi_symbol=fly, total_contracts=1, remaining=0,
                       avg_price=1.80, close_price=4.15, status="CLOSED",
                       is_credit=False, spread_width=10.0)
        assert pos.pnl_dollar() == pytest.approx(235.0)
        assert pos.pnl_pct() == pytest.approx(130.56, abs=0.01)
        assert pos.to_dict()["isCredit"] is False
        o = Order(osi_symbol=fly, side="SELL", status="FILLED",
                  fill_price=4.15, cost_basis=1.80, filled_qty=1, quantity=1)
        assert o.to_dict()["profit"] == pytest.approx(235.0)
        assert fill_pnl_dollar(4.15, 1.80, 1, False) == pytest.approx(235.0)


def test_err_text_pulls_the_brokers_message_out_of_a_200_rejection():
    """Tradier answers a rejected preview HTTP 200 with no "order" key."""
    from app.execution.tradier_sdk_bridge import _err_text

    body = {"errors": {"error": "Buy order is for more shares than your current "
                                "short position, please review current position "
                                "quantity along with open orders for security. "}}
    assert _err_text(body).startswith("Buy order is for more shares")
    assert "{" not in _err_text(body)
    assert _err_text({"errors": {"error": ["a", "b"]}}) == "a; b"
    assert _err_text({"order": {"status": "ok"}}) == "{'order': {'status': 'ok'}}"
    assert _err_text(None) == "None"


def test_fail_order_keeps_cancelled_apart_from_rejected(monkeypatch):
    """A day order that died at the bell is not a broker rejection."""
    from app.execution import spread_executor as se

    seen = {}

    class Order:
        id = 36
        status = "PENDING"
        error_text = None

    class Q:
        def filter(self, *a):  return self
        def first(self):       return seen["order"]

    class DB:
        def query(self, *a):   return Q()
        def commit(self):      pass
        def close(self):       pass

    monkeypatch.setattr(se, "SessionLocal", lambda: DB())
    ex = se.SpreadExecutor.__new__(se.SpreadExecutor)
    monkeypatch.setattr(ex, "_set_alert", lambda *a, **k: None, raising=False)

    for reason, expected in (
        ("SPREAD_CLOSE_CANCELLED", "CANCELLED"),
        ("SPREAD_OPEN_EXPIRED", "EXPIRED"),
        ("SPREAD_CLOSE_FAILED: Tradier multileg preview not ok: nope", "REJECTED"),
    ):
        seen["order"] = Order()
        ex._fail_order(36, None, reason)
        assert seen["order"].status == expected, reason


def _sizer(monkeypatch, *, mirror, max_contracts, max_risk):
    from app.execution import spread_executor as se
    monkeypatch.setattr(se, "_MIRROR_ANALYST_QTY", lambda: mirror)
    monkeypatch.setattr(se, "_MAX_CONTRACTS", lambda: max_contracts)
    monkeypatch.setattr(se, "_MAX_RISK_DOLLARS", lambda: max_risk)
    return se.SpreadExecutor.__new__(se.SpreadExecutor)._size


def test_mirror_off_keeps_risk_budget_sizing(monkeypatch):
    size = _sizer(monkeypatch, mirror=False, max_contracts=3, max_risk=1000)
    assert size(410.0, 2) == 2       # affordability binds, not his "x2"
    assert size(100.0, 1) == 3       # his x1 is ignored, ceiling wins
    assert size(100.0, None) == 3


def test_mirror_on_takes_his_stated_count(monkeypatch):
    size = _sizer(monkeypatch, mirror=True, max_contracts=3, max_risk=1000)
    assert size(100.0, 2) == 2       # "x2" instead of the ceiling of 3
    assert size(100.0, None) == 3    # no count stated → unchanged behavior


def test_mirror_never_beats_the_caps(monkeypatch):
    # The whole point of the clamp: his size cannot enlarge this account's risk.
    assert _sizer(monkeypatch, mirror=True, max_contracts=1, max_risk=1000)(100.0, 5) == 1
    assert _sizer(monkeypatch, mirror=True, max_contracts=9, max_risk=500)(410.0, 5) == 1
    assert _sizer(monkeypatch, mirror=True, max_contracts=9, max_risk=100)(410.0, 5) == 0
