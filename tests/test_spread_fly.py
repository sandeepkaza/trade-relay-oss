"""Butterfly parsing and leg expansion.

Every OPENED/CLOSED line here is a verbatim message from #twi-spreads on
2026-08-13 — the session whose fly the bot dropped as chatter. The two CLOSE
spellings are both real and both from that same trade, which is the reason the
grammar has to accept strikes on either side of the FLY keyword.
"""
import re
from datetime import date
from decimal import Decimal

import pytest

from app.ingest import spread_parser
from app.ingest.spread_parser import parse_spread_alert, spread_legs

SESSION = date(2026, 8, 13)

OPEN_MSG = "OPENED: SPX 10W FLY 7790/7800/7810 $1.80 @everyone"
CLOSE_PARTIAL = "CLOSED: SPX 10W FLY 7790/7800/7810 $3.0 1/2 POS +62% @everyone"
CLOSE_ALL_BODY_ONLY = "CLOSED: SPX 10W 7800 FLY $4.15 ALL OUT +130% @everyone"


@pytest.fixture(autouse=True)
def _flies_on(monkeypatch):
    """The feature is default-off, so every test here turns it on explicitly."""
    monkeypatch.setattr(spread_parser.cfg, "getboolean",
                        lambda s, k, fallback=None, **kw: True
                        if k == "spread_flies_enabled" else fallback)
    monkeypatch.setattr(spread_parser.cfg, "get",
                        lambda s, k, fallback=None, **kw: "P"
                        if k == "spread_fly_right" else fallback)


def test_open_all_three_strikes():
    a = parse_spread_alert(OPEN_MSG)
    assert a is not None
    assert (a.kind, a.action) == ("FLY", "OPEN")
    assert (a.long_strike, a.short_strike, a.upper_strike) == (7790.0, 7800.0, 7810.0)
    assert a.net_price == Decimal("1.80")
    assert a.width == 10.0                      # wing distance, not total span
    assert a.is_fly and not a.is_credit_structure


def test_body_then_width_then_fly():
    """2026-09-10 live: OPENED: SPX 7600 10W FLY $2.0 x2 — dropped as chatter."""
    a = parse_spread_alert("OPENED: SPX 7600 10W FLY $2.0 x2")
    assert a is not None
    assert (a.kind, a.action) == ("FLY", "OPEN")
    assert (a.long_strike, a.short_strike, a.upper_strike) == (7590.0, 7600.0, 7610.0)
    assert a.net_price == Decimal("2.0")
    assert a.qty == 2
    assert a.width == 10.0
    assert a.is_fly and not a.is_credit_structure


def test_body_with_stated_right():
    """2026-09-17 live: OPENED: SPX 5W FLY 7635P $1.0 x1 — dropped as chatter.
    The `P` broke the strike capture, so `price` swallowed 7635 and the line
    resolved to no strikes at all. A printed right also beats the config knob."""
    a = parse_spread_alert("OPENED: SPX 5W FLY 7635P $1.0  x1 @everyone")
    assert a is not None
    assert (a.long_strike, a.short_strike, a.upper_strike) == (7630.0, 7635.0, 7640.0)
    assert a.net_price == Decimal("1.0")
    assert (a.qty, a.width) == (1, 5.0)
    assert a.option_type == "P"


def test_stated_right_overrides_the_knob(monkeypatch):
    monkeypatch.setattr(spread_parser.cfg, "get",
                        lambda s, k, fallback=None, **kw: "P"
                        if k == "spread_fly_right" else fallback)
    a = parse_spread_alert("OPENED: SPX 5W FLY 7635C $1.0 x1")
    assert a is not None and a.option_type == "C"


def test_vertical_tolerates_stated_right():
    a = parse_spread_alert("OPENED: CCS SPX 7390/95C $1.25 x 3")
    assert a is not None and a.kind == "CCS"
    assert (a.short_strike, a.long_strike) == (7390.0, 7395.0)


def test_close_body_only_spelling_resolves_same_strikes():
    a = parse_spread_alert(CLOSE_ALL_BODY_ONLY)
    assert a is not None
    assert (a.long_strike, a.short_strike, a.upper_strike) == (7790.0, 7800.0, 7810.0)
    assert a.action == "CLOSE" and a.close_all
    assert a.net_price == Decimal("4.15")


def test_close_partial_carries_the_fraction():
    a = parse_spread_alert(CLOSE_PARTIAL)
    assert a is not None
    assert a.fraction == Decimal("1") / Decimal("2")
    assert not a.close_all
    # "7790/7800/7810" must not be mistaken for the "1/2 POS" fraction.
    assert a.short_strike == 7800.0


def test_risk_is_the_debit_not_the_width():
    """The trap: a fly's max loss is what it cost. Reading (width - price) —
    the credit-spread formula — would call this $820 of risk on a $180 trade."""
    assert parse_spread_alert(OPEN_MSG).max_loss_per_contract() == pytest.approx(180.0)


def test_legs_are_one_two_one_and_buy_the_wings():
    legs = spread_legs(parse_spread_alert(OPEN_MSG), SESSION, 2)
    assert len(legs) == 3
    body, lower, upper = legs
    assert body[1:] == ("sell_to_open", 4)      # doubled
    assert lower[1:] == ("buy_to_open", 2)
    assert upper[1:] == ("buy_to_open", 2)
    # Puts, per the analyst's own explainer for this structure.
    assert all(re.search(r"P\d{8}$", osi) for osi, _s, _q in legs)
    assert body[0].startswith("SPXW") and "07800000" in body[0]


def test_close_legs_reverse_every_side():
    legs = spread_legs(parse_spread_alert(CLOSE_ALL_BODY_ONLY), SESSION, 1)
    assert [side for _o, side, _q in legs] == [
        "buy_to_close", "sell_to_close", "sell_to_close"]
    assert [q for _o, _s, q in legs] == [2, 1, 1]


@pytest.mark.parametrize("line", [
    "OPENED: SPX FLY 7800 $1.80",                      # body with no width
    "OPENED: SPX 10W FLY 7790/7800/7815 $1.80",        # wings not symmetric
    "OPENED: SPX 15W FLY 7790/7800/7810 $1.80",        # strikes contradict 15W
    "OPENED: SPX 40W FLY 7760/7800/7840 $1.80",        # wing he doesn't trade
    "i put a fly at 55 15w avg 3.80 tiny",             # chatter, no action verb
    "Anyone holding the 15w fly ?",                    # chatter
])
def test_refuses_rather_than_guesses(line):
    assert parse_spread_alert(line) is None


def test_flag_off_reads_as_chatter(monkeypatch):
    """One-knob revert: with the flag off a fly must parse to nothing, which
    is what stops it reaching the order path at all."""
    monkeypatch.setattr(spread_parser.cfg, "getboolean",
                        lambda s, k, fallback=None, **kw: False
                        if k == "spread_flies_enabled" else fallback)
    assert parse_spread_alert(OPEN_MSG) is None


def test_verticals_still_parse():
    """The fly branch must not have shadowed the grammar it was bolted next to."""
    a = parse_spread_alert("OPENED: CCS SPX 7390/95 $1.25 x 3 @everyone")
    assert a is not None and a.kind == "CCS"
    assert (a.short_strike, a.long_strike, a.qty) == (7390.0, 7395.0, 3)
    assert a.is_credit_structure


def test_net_fill_price_divides_by_spreads_not_mean_leg():
    """A 1-2-1 fill: 2 body @ 30.00 sold, wings @ 35.90/26.30 bought.
    Net per spread = 60.00 - 35.90 - 26.30 = -2.20, a debit. Dividing by the
    mean leg size (4/3) instead would report -1.65."""
    from app.execution.tradier_sdk_bridge import net_fill_price
    order = {"leg": [
        {"side": "sell_to_open", "avg_fill_price": 30.00, "exec_quantity": 2},
        {"side": "buy_to_open",  "avg_fill_price": 35.90, "exec_quantity": 1},
        {"side": "buy_to_open",  "avg_fill_price": 26.30, "exec_quantity": 1},
    ]}
    assert net_fill_price(order) == pytest.approx(-2.20)


def test_net_fill_price_unchanged_for_verticals():
    from app.execution.tradier_sdk_bridge import net_fill_price
    order = {"leg": [
        {"side": "sell_to_open", "avg_fill_price": 2.00, "exec_quantity": 3},
        {"side": "buy_to_open",  "avg_fill_price": 0.75, "exec_quantity": 3},
    ]}
    assert net_fill_price(order) == pytest.approx(1.25)


# ── Bare closes ───────────────────────────────────────────────────────────
#
# "CLOSED 1/2 FLY $3.80" (2026-09-10, verbatim) names no strikes. Refusing it
# strands the open fly with no analyst exit, so the executor resolves it
# against the one open fly — and refuses when there isn't exactly one.

BARE_CLOSE = "CLOSED 1/2 FLY $3.80 @everyone"


def test_bare_close_parses_without_strikes():
    a = parse_spread_alert(BARE_CLOSE)
    assert a is not None
    assert (a.action, a.kind, a.bare) == ("CLOSE", "FLY", True)
    assert a.fraction == Decimal("0.5")          # "1/2" without the POS suffix
    assert a.net_price == Decimal("3.80")
    assert (a.short_strike, a.long_strike, a.upper_strike) == (0.0, 0.0, 0.0)


def test_bare_close_keeps_all_out_and_a_stated_wing():
    a = parse_spread_alert("CLOSED FLY $3.80 ALL OUT @everyone")
    assert a.bare and a.close_all and a.fraction == Decimal("1")
    b = parse_spread_alert("CLOSED 10W FLY $3.80 @everyone")
    assert b.bare and b.stated_wing == 10.0


def test_a_bare_open_is_still_refused():
    """An OPEN has no position to resolve against, so it must not go bare."""
    assert parse_spread_alert("OPENED FLY $3.80 @everyone") is None
    assert parse_spread_alert("OPENED: SPX FLY $1.80 x2 @everyone") is None


def test_spread_legs_refuses_an_unresolved_bare_close():
    a = parse_spread_alert(BARE_CLOSE)
    with pytest.raises(ValueError, match="bare close"):
        spread_legs(a, SESSION, 1)


def _bare_executor(monkeypatch, positions):
    """SpreadExecutor with a fake DB holding `positions` and _block captured."""
    from app.execution import spread_executor as se

    class Q:
        def filter(self, *a, **k):  return self
        def all(self):              return positions

    class DB:
        def query(self, *a):  return Q()
        def close(self):      pass

    monkeypatch.setattr(se, "SessionLocal", lambda: DB())
    monkeypatch.setattr(se, "_OWNER_SCOPE_AUTHOR", lambda: True)
    ex = se.SpreadExecutor.__new__(se.SpreadExecutor)
    blocked = []
    monkeypatch.setattr(ex, "_block", lambda _id, reason: blocked.append(reason), raising=False)
    return ex, blocked


class _Pos:
    def __init__(self, strike, width, right="P"):
        self.strike, self.spread_width, self.option_type = strike, width, right


def test_bare_close_resolves_against_the_one_open_fly(monkeypatch):
    ex, blocked = _bare_executor(monkeypatch, [_Pos(7600.0, 10.0)])
    a = ex._resolve_bare(parse_spread_alert(BARE_CLOSE), 1, "Matae")
    assert blocked == []
    assert (a.long_strike, a.short_strike, a.upper_strike) == (7590.0, 7600.0, 7610.0)
    assert a.bare is False and a.width == 10.0 and a.option_type == "P"
    # And it now expands to the three closing legs it could not build before.
    sides = [side for _o, side, _q in spread_legs(a, SESSION, 2)]
    assert sides == ["buy_to_close", "sell_to_close", "sell_to_close"]


def test_bare_close_refuses_when_two_flies_are_open(monkeypatch):
    """Guessing which one he means is how the wrong position gets flattened."""
    ex, blocked = _bare_executor(monkeypatch, [_Pos(7600.0, 10.0), _Pos(7800.0, 5.0)])
    assert ex._resolve_bare(parse_spread_alert(BARE_CLOSE), 1, "Matae") is None
    assert blocked and "AMBIGUOUS" in blocked[0]


def test_bare_close_refuses_when_nothing_is_open(monkeypatch):
    ex, blocked = _bare_executor(monkeypatch, [])
    assert ex._resolve_bare(parse_spread_alert(BARE_CLOSE), 1, "Matae") is None
    assert blocked and "AMBIGUOUS" in blocked[0]


def test_a_stated_wing_that_contradicts_the_open_fly_refuses(monkeypatch):
    ex, blocked = _bare_executor(monkeypatch, [_Pos(7600.0, 5.0)])
    a = parse_spread_alert("CLOSED 10W FLY $3.80 @everyone")
    assert ex._resolve_bare(a, 1, "Matae") is None
    assert blocked and "WING_MISMATCH" in blocked[0]


def test_alert_row_view_tolerates_a_bare_close():
    """The alert row is written before the strikes are known."""
    from app.execution.spread_executor import AlertRowView
    view = AlertRowView(parse_spread_alert(BARE_CLOSE), SESSION)
    assert view.action == "SELL" and view.osi_symbol == ""
