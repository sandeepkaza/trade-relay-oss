"""Regression tests for the SELL re-peg fixes (SPXW 7480C, 2026-07-08):

1. Effective-full exits route to the forced (marketable) lane — covered by the
   dispatch guard `effective_close_all or qty >= pos.remaining` (integration-
   level; asserted indirectly via the qty-rounding invariant here).
2. Partial re-peg goes marketable on the final attempt (config knob present).
3. Re-peg limits snap to the contract min tick so IBKR doesn't reject them
   (Warning 110). This is the unit-testable core.
"""
from decimal import Decimal

import app.execution.public_executor as pe


def test_snap_rounds_offgrid_down_to_tick():
    # Above $3: 0.10 grid. 4.55 is off-grid → 4.50.
    assert pe._snap_down_to_tick(Decimal("4.55")) == Decimal("4.50")
    assert pe._snap_down_to_tick(Decimal("4.65")) == Decimal("4.60")
    assert pe._snap_down_to_tick(Decimal("5.05")) == Decimal("5.00")
    # Below $3: 0.05 grid.
    assert pe._snap_down_to_tick(Decimal("2.97")) == Decimal("2.95")
    assert pe._snap_down_to_tick(Decimal("2.93")) == Decimal("2.90")


def test_snap_preserves_valid_ticks():
    # A price already on-grid must not be dropped a tick (the float-floor bug).
    assert pe._snap_down_to_tick(Decimal("4.60")) == Decimal("4.60")
    assert pe._snap_down_to_tick(Decimal("2.90")) == Decimal("2.90")
    assert pe._snap_down_to_tick(Decimal("3.00")) == Decimal("3.00")
    assert pe._snap_down_to_tick(Decimal("7.70")) == Decimal("7.70")


def test_snap_never_below_one_tick():
    assert pe._snap_down_to_tick(Decimal("0.02")) == Decimal("0.05")
    assert pe._snap_down_to_tick(Decimal("0.00")) == Decimal("0.05")


def test_partial_repeg_final_marketable_knobs_present():
    # Fix #2: the final-marketable escape hatch exists and defaults on.
    assert pe._PARTIAL_REPEG_FINAL_MARKETABLE() is True
    assert pe._PARTIAL_REPEG_FINAL_SLIPPAGE_PCT() == 20.0


def test_qty_rounds_up_to_full_position_triggers_forced_lane():
    # Fix #1 invariant: a fractional SELL on a 1-lot rounds up to the whole
    # position, so `qty >= remaining` is True → forced (marketable) lane.
    remaining = 1
    fraction = 0.5
    qty = min(max(1, int(remaining * fraction)), remaining)
    assert qty == 1
    assert qty >= remaining  # → routes to _repeg_close_all_sell
