"""
Regression tests for SPX → SPXW broker symbol translation.

Bug 2026-04-20: SPX260420P07090000 was rejected by Public.com with
    "API Error 400: No match found for this Symbol (SPX)."
because 4/20/2026 is a Monday (weekly/daily expiry). Public routes all
non-3rd-Friday SPX expiries under the 'SPXW' root; alerts come in as 'SPX'.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# public_executor imports SDK/db; set a DB URL so import succeeds.
import tempfile
_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp.close()
os.environ.setdefault("TRADER_DATABASE_URL", f"sqlite:///{_tmp.name.replace(chr(92), '/')}")

from app.execution.public_executor import _osi_for_broker  # noqa: E402


def test_spx_monday_weekly_becomes_spxw():
    # 2026-04-20 is a Monday → weekly → SPXW
    assert _osi_for_broker("SPX260420P07090000") == "SPXW260420P07090000"


def test_spx_third_friday_stays_spx():
    # 2026-04-17 is the 3rd Friday of April 2026 → monthly → SPX
    assert _osi_for_broker("SPX260417C06000000") == "SPX260417C06000000"


def test_spx_non_third_friday_becomes_spxw():
    # 2026-04-24 is a Friday but NOT the 3rd Friday (that's 4/17) → weekly → SPXW
    assert _osi_for_broker("SPX260424P06500000") == "SPXW260424P06500000"


def test_spx_third_friday_march_2026():
    # 2026-03-20 is the 3rd Friday of March → monthly
    assert _osi_for_broker("SPX260320C06000000") == "SPX260320C06000000"


def test_spxw_is_passthrough():
    assert _osi_for_broker("SPXW260420P07090000") == "SPXW260420P07090000"


def test_non_spx_is_passthrough():
    assert _osi_for_broker("AAPL260417C00275000") == "AAPL260417C00275000"
    assert _osi_for_broker("NVDA260515P00800000") == "NVDA260515P00800000"


def test_garbage_is_passthrough():
    assert _osi_for_broker("") == ""
    assert _osi_for_broker("SPXgarbage") == "SPXgarbage"


def test_invalid_date_is_passthrough():
    # month 99 — must not crash, just return original
    assert _osi_for_broker("SPX269920C06000000") == "SPX269920C06000000"
