"""
Regression tests: the AI fallback must never invent an expiry.

Bug 2026-08-06: spacemonkey's undated "BOUGHT NVDA 222.50C $2.17 [SMALL]"
came back from Gemini dated today (a Thursday) -> NVDA260806C00222500, which
NVDA does not list (Fridays only). Public.com rejected the order with
    "API Error 400: No match found for this Symbol (NVDA)."
"""

import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.ingest.ai_parser import _build_trade_alert  # noqa: E402

_TODAY = date.today()

# The "already dated" cases need an expiry in the FUTURE. A hardcoded date goes
# stale the day it passes — _build_trade_alert then correctly rolls it forward a
# year and the assertion fails for a reason that has nothing to do with the
# behaviour under test, breaking the deploy gate. Derive it from today instead.
_DATED = _TODAY + timedelta(days=3)
_DATED_OSI = f"NVDA{_DATED:%y%m%d}C00222500"


def _parsed(**over):
    base = {
        "action": "BUY", "symbol": "NVDA",
        "expiry_month": _TODAY.month, "expiry_day": _TODAY.day,
        "expiry_year": _TODAY.year,
        "strike": 222.5, "option_type": "C", "price": 2.17,
        "fraction": 1.0, "size_tag": "SMALL", "qty": None, "close_all": False,
    }
    base.update(over)
    return base


def test_undated_buy_is_dropped():
    assert _build_trade_alert(
        _parsed(), None, src_text="BOUGHT NVDA 222.50C $2.17 [SMALL] @everyone"
    ) is None


def test_undated_sell_keeps_alert_without_expiry():
    a = _build_trade_alert(
        _parsed(action="SELL"), None, src_text="SOLD NVDA 222.50C $3.10 ALL OUT"
    )
    assert a is not None
    assert a.expiry is None
    assert not a.osi_symbol           # symbol+strike+type matching downstream
    assert a.symbol == "NVDA" and a.strike == 222.5


def _dated(**over):
    return _parsed(expiry_month=_DATED.month, expiry_day=_DATED.day,
                   expiry_year=_DATED.year, **over)


def test_dated_alert_is_untouched():
    a = _build_trade_alert(
        _dated(), None,
        src_text=f"BOUGHT NVDA {_DATED.month}/{_DATED.day} 222.50C $2.17 [SMALL]",
    )
    assert a is not None and a.expiry == _DATED
    assert a.osi_symbol == _DATED_OSI


def test_dashed_date_counts_as_dated():
    a = _build_trade_alert(
        _dated(), None,
        src_text=f"BOUGHT NVDA {_DATED.month}-{_DATED.day} 222.50C $2.17",
    )
    assert a is not None and a.expiry == _DATED


def test_missing_src_text_does_not_drop():
    # No source text means the caller didn't supply it, not that the alert was
    # undated — the guard must stay out of the way rather than eat the BUY.
    a = _build_trade_alert(_dated(), 99)
    assert a is not None and a.expiry == _DATED


def test_price_alone_is_not_a_date():
    # "$2.17" must not read as 2/17 and rescue an undated BUY
    assert _build_trade_alert(
        _parsed(), None, src_text="BOUGHT NVDA 222.50C $2.17"
    ) is None
