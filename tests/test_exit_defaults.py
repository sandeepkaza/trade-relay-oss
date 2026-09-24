"""The derived exit defaults are one dict, and they are internally possible.

Two things go wrong with these numbers, and both have happened:

  1. They get copied. position_monitor, Position.exit_plan and the trail-toggle
     endpoint each used to carry their own literal fallback, and they had
     already drifted (arm +50/+60, trail 25/30) — so the dashboard quoted one
     stop and the monitor fired another.
  2. A breakeven floor gets set above its own arm. The sweep's unconstrained
     grid picks exactly that, because the model teleports the exit to the floor
     with no path cost. The market cannot do it.
"""
import re
from pathlib import Path

from app.core.profile_resolver import EXIT_DEFAULTS, xd

_ROOT = Path(__file__).resolve().parents[1]
_CALLERS = [
    _ROOT / "app" / "monitors" / "position_monitor.py",
    _ROOT / "app" / "core" / "models.py",
]
_DASHBOARD = _ROOT / "static" / "dashboard.html"


def test_breakeven_floor_sits_below_its_arm():
    assert xd("breakeven_lock_floor_pct") < xd("breakeven_lock_arm_pct")


def test_the_trail_arms_above_the_stop_it_replaces():
    assert xd("trailing_sl_arm_pct") > 0 > xd("sl_pct")


def test_one_profit_rung_and_it_is_pt1():
    # The rungs are chained: PT2 needs PT1, PT3 needs PT2, TPT needs PT3, and
    # the trail needs PT1. A single rung anywhere but PT1 disables the ladder.
    assert xd("pt1_enabled") is True
    assert xd("pt2_enabled") is False and xd("pt3_enabled") is False


def test_floor_locks_off_so_nothing_sells_at_a_profit_under_an_sl_label():
    # pt2_floor_lock_pct defaults to pt1_pct. With one rung at +50 the lock
    # would ratchet sl_pct to +50 — above the price — and fire instantly.
    assert xd("pt2_floor_lock_enabled") is False
    assert xd("pt3_floor_lock_enabled") is False


def test_callers_read_the_dict_instead_of_their_own_literals():
    stale = []
    for path in _CALLERS:
        src = path.read_text(encoding="utf-8")
        for key in EXIT_DEFAULTS:
            # A fallback for a shared key that is a bare number, not xd(...).
            for m in re.finditer(rf'["\']{key}["\']\s*,\s*(-?\d[\d.]*|True|False)\b', src):
                stale.append(f"{path.name}: {m.group(0)}")
    assert not stale, "exit defaults duplicated as literals: " + "; ".join(stale)


def test_the_dashboard_quotes_the_same_numbers():
    """The settings panel and the position row can't import the dict — no build
    step, it is one static file — so they carry literals and this asserts the
    literals agree. They are what an operator sees before any config loads."""
    src = _DASHBOARD.read_text(encoding="utf-8")
    patterns = [
        # get('trading','pt1_pct','50')  /  getBool('trading','pt2_enabled',false)
        r"""get(?:Bool)?\(\s*['"]trading['"]\s*,\s*['"]{key}['"]\s*,\s*['"]?(-?[\d.]+|true|false)['"]?\s*\)""",
        # configFromWs?.trading?.sl_pct||-20
        r"""trading\?\.{key}\s*\|\|\s*(-?[\d.]+|true|false)""",
    ]
    wrong = []
    for key, want in EXIT_DEFAULTS.items():
        for pat in patterns:
            for m in re.finditer(pat.format(key=re.escape(key)), src):
                got = m.group(1)
                got = got == "true" if got in ("true", "false") else float(got)
                if got != want:
                    wrong.append(f"{key}: dashboard says {m.group(1)}, EXIT_DEFAULTS says {want}")
    assert not wrong, "; ".join(wrong)
