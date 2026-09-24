"""Tests for the vertical credit-spread parser.

The cases below are verbatim lines from the source channel's full history
(2026-06-26 .. 2026-08-06), chosen to cover every shape that history contains:
ticker before/after the CCS/PCS token or absent, `x 3` / `x1` / qty after the
mention, a price with no `$`, fractional and ALL OUT closes, and the strike
abbreviation rolling over a hundred boundary (`7500/95` → 7495).

The refusal cases matter more than the parses. This channel is the reason the
grammar is anchored: its chatter is what makes the single-leg parser emit
symbol-less close_all SELLs.
"""
from decimal import Decimal

import pytest

from app.ingest.spread_parser import parse_spread_alert


@pytest.mark.parametrize(
    "text, kind, short, long_, price, qty",
    [
        # ticker between the kind token and the strikes
        ("OPENED: CCS SPX 7390/95 $1.25 x 3 @everyone", "CCS", 7390, 7395, "1.25", 3),
        # ticker ahead of the kind token
        ("OPENED: SPX PCS 7435/30 $0.80 x1 @everyone", "PCS", 7435, 7430, "0.80", 1),
        # no ticker at all — defaults to SPX
        ("OPENED: CCS 7500/05 $1.50 x2 @everyone", "CCS", 7500, 7505, "1.50", 2),
        # quantity trailing the mention rather than the price
        ("OPENED: PCS SPX 7345/40 $0.90 @everyone x2", "PCS", 7345, 7340, "0.90", 2),
        # price without the dollar sign
        ("CLOSED: SPX CCS 7575/80 1.80 ALL OUT @everyone", "CCS", 7575, 7580, "1.80", None),
        # single-digit trailing price
        ("CLOSED: SPX PCS 7470/65 $1.0 ALL OUT @everyone", "PCS", 7470, 7465, "1.0", None),
        # CC/PC shorthand — live 2026-09-15: he opened "SPX CCS 7495/00" and
        # closed the same trade "SPX CC 7595/00 $0.30 ALL OUT". The close
        # was dropped as chatter and the spread was never exited.
        ("CLOSED: SPX CC 7595/00 $0.30 ALL OUT @everyone", "CCS", 7595, 7600, "0.30", None),
        ("OPENED: SPX PC 7560/55 $0.75 x1 @everyone", "PCS", 7560, 7555, "0.75", 1),
    ],
)
def test_parses_every_shape_in_the_history(text, kind, short, long_, price, qty):
    alert = parse_spread_alert(text)
    assert alert is not None
    assert (alert.kind, alert.symbol) == (kind, "SPX")
    assert (alert.short_strike, alert.long_strike) == (short, long_)
    assert alert.net_price == Decimal(price)
    assert alert.qty == qty
    assert alert.width == 5.0


def test_put_spread_abbreviation_rolls_below_the_hundred_boundary():
    """`7500/95` is 7495, not 7595 — the short leg of a put credit spread is
    above the long leg, and only the direction disambiguates the digits."""
    alert = parse_spread_alert("OPENED: SPX PCS 7500/95 $0.60 x2 @everyone")
    assert (alert.short_strike, alert.long_strike) == (7500, 7495)


def test_call_spread_abbreviation_rolls_above_the_hundred_boundary():
    alert = parse_spread_alert("OPENED: CCS SPX 7500/05 $1.75 x2 @everyone")
    assert (alert.short_strike, alert.long_strike) == (7500, 7505)


def test_option_type_follows_the_spread_kind():
    assert parse_spread_alert("OPENED: CCS SPX 7390/95 $1.25 x3").option_type == "C"
    assert parse_spread_alert("OPENED: PCS SPX 7345/40 $0.90 x2").option_type == "P"


class TestExits:
    def test_all_out_closes_everything(self):
        alert = parse_spread_alert("CLOSED: CCS SPX 7390/95 $1.20 ALL OUT @everyone")
        assert alert.action == "CLOSE"
        assert alert.close_all is True
        assert alert.fraction == Decimal("1")

    def test_all_out_without_a_space_before_the_mention(self):
        # "ALL OUT@everyone" — posted exactly like this on 2026-06-26.
        alert = parse_spread_alert("CLOSED: CCS SPX 7410/15 $0.30 ALL OUT@everyone")
        assert alert.close_all is True

    def test_fractional_close_is_not_a_full_exit(self):
        alert = parse_spread_alert("CLOSED: CCS SPX 7410/15 $0.70 1/3 POS @everyone")
        assert alert.close_all is False
        assert alert.fraction == Decimal(1) / Decimal(3)

    def test_expiry_worthless_closes_the_position(self):
        alert = parse_spread_alert("EXPIRED: SPX PCS 7500/95 $0.00 @everyone (+100%)")
        assert (alert.expired, alert.close_all) == (True, True)
        assert alert.net_price == Decimal("0.00")


class TestRefusals:
    """Anything not unambiguously an order must return None. A missed spread
    costs one trade; a guessed one opens the wrong strikes."""

    @pytest.mark.parametrize(
        "text",
        [
            # The line the production single-leg parser turns into a blank
            # close_all SELL. It must not survive contact with this one.
            "closed out above us, open to run up now",
            "watching CCS 7435/40",
            "or 40/45, seeing if we can get a run into the peak first",
            "I'd consider adding against 30-40",
            "*the 40/45s is what i took , proper price. typo there",
            "CCS = call credit spread PCS = put call spread @here",
            "@everyone done for now, probably the day. Stay frosty.",
            "",
        ],
    )
    def test_chatter_is_refused(self, text):
        assert parse_spread_alert(text) is None

    def test_action_verb_must_open_the_line(self):
        assert parse_spread_alert("i think we should have CLOSED: CCS SPX 7390/95 $1.25") is None

    def test_width_outside_what_he_trades_is_refused(self):
        # 7390/7398 is 8 wide — not a width in the history, so the line was
        # almost certainly mistyped and must not be guessed at.
        assert parse_spread_alert("OPENED: CCS SPX 7390/98 $1.25 x3") is None

    def test_a_missing_price_is_refused(self):
        assert parse_spread_alert("OPENED: CCS SPX 7390/95 x3 @everyone") is None


def test_max_loss_is_width_minus_credit_not_premium_paid():
    """Sizing reads this, not the net price: a 5-wide taken for $1.25 risks
    $375 a contract, which is what has to clear the per-position cap."""
    alert = parse_spread_alert("OPENED: CCS SPX 7390/95 $1.25 x 3 @everyone")
    assert alert.max_loss_per_contract() == pytest.approx(375.0)


def test_no_expiry_means_same_session():
    """He never states an expiry — every alert is a same-day SPXW 0DTE, and
    the caller has to supply the session date rather than read one here."""
    assert parse_spread_alert("OPENED: CCS SPX 7390/95 $1.25 x3").expiry is None


def test_alert_is_reusable_as_a_position_key():
    a = parse_spread_alert("OPENED: CCS SPX 7390/95 $1.25 x 3 @everyone")
    b = parse_spread_alert("CLOSED: CCS SPX 7390/95 $1.20 ALL OUT @everyone")
    assert (a.kind, a.short_strike, a.long_strike) == (b.kind, b.short_strike, b.long_strike)
