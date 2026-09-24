"""Per-position manual trailing stop (trail_enabled) — arm + fire logic.

Mirrors the TRAIL_MANUAL block in position_monitor._evaluate_one_position:
the block runs OUTSIDE the auto_exit gate, arms on peak >= entry*(1+arm/100),
and exits the remainder at peak*(1-trail/100).
"""


def decide(entry, peak, price, arm_pct, trail_pct, trail_enabled,
           remaining=1, sl_triggered=False, already_fired=False,
           hours_ok=True, spread=0.0, wide_spread_pct=20.0,
           skip_manual=False, grace=False):
    """Returns True when the manual trail should fire this tick."""
    if not (trail_enabled and remaining > 0 and not sl_triggered
            and entry and price > 0):
        return False
    peak = peak or entry or 0
    armed = peak >= entry * (1 + arm_pct / 100.0)
    level = peak * (1 - trail_pct / 100.0)
    return bool(armed and price <= level and hours_ok
                and spread < wide_spread_pct and not already_fired
                and not skip_manual and not grace)


def test():
    A, T = 50.0, 25.0   # arm at +50%, trail 25% off peak

    # off by default — pure relay untouched
    assert not decide(1.00, 2.00, 1.00, A, T, trail_enabled=False)

    # armed but peak never reached the +50% arm threshold
    assert not decide(1.00, 1.40, 1.00, A, T, True), "must not arm below +50%"

    # peak +50% exactly, price still near peak -> hold
    assert not decide(1.00, 1.50, 1.45, A, T, True)

    # peak +50%, price 25% off peak (1.125) -> fire
    assert decide(1.00, 1.50, 1.125, A, T, True)
    assert decide(1.00, 1.50, 1.10, A, T, True)

    # the META260713C00670000 case: entry 1.14, peak 1.75 (+54%), collapsed to
    # 0.08. Trail must fire on the way down instead of riding to -93%.
    assert decide(1.14, 1.75, 1.3125, A, T, True), "META case must fire"
    assert not decide(1.14, 1.75, 1.40, A, T, True), "not yet 25% off peak"

    # guards each independently suppress the fire
    for kw in ({"sl_triggered": True}, {"already_fired": True},
               {"hours_ok": False}, {"spread": 25.0},
               {"skip_manual": True}, {"grace": True}, {"remaining": 0}):
        assert not decide(1.00, 1.50, 1.10, A, T, True, **kw), f"guard {kw} leaked"

    # wider trail holds longer; tighter fires sooner (same peak)
    assert decide(1.00, 2.00, 1.70, A, 10.0, True)       # 10% -> level 1.80
    assert not decide(1.00, 2.00, 1.70, A, 40.0, True)   # 40% -> level 1.20

    # profile arm of 100% (spx_index style) needs a bigger run
    assert not decide(1.00, 1.60, 1.00, 100.0, T, True)
    assert decide(1.00, 2.00, 1.50, 100.0, T, True)

    print("ok — manual trail arm/fire logic")


if __name__ == "__main__":
    test()
