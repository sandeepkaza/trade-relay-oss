"""
vix_sizer.py - Volatility-adjusted position sizing.

Scales position sizes based on VIX level:
- VIX < 20: Normal sizing (100%)
- VIX 20-25: Slight reduction (75%)
- VIX 25-30: Moderate reduction (50%)
- VIX 30+: Aggressive reduction (25%)
"""

import logging
from dataclasses import dataclass
from typing import Optional

from app.core.config_manager import cfg

log = logging.getLogger(__name__)


@dataclass
class VixScaleResult:
    """Result of VIX-based position sizing."""
    original_qty: int
    adjusted_qty: int
    scale_factor: float
    vix_level: Optional[float]
    reason: str


def _VIX_SIZING_ENABLED() -> bool:
    return cfg.getboolean("trading", "vix_sizing_enabled", fallback=False)


def _VIX_LOW_THRESHOLD() -> float:
    return cfg.getfloat("trading", "vix_low_threshold", fallback=20.0)


def _VIX_HIGH_THRESHOLD() -> float:
    return cfg.getfloat("trading", "vix_high_threshold", fallback=25.0)


def _VIX_EXTREME_THRESHOLD() -> float:
    return cfg.getfloat("trading", "vix_extreme_threshold", fallback=30.0)


def _VIX_UNAVAILABLE_SCALE() -> float:
    """Scale factor used when VIX feed fails. Default 0.5 (half size) — VIX
    sizing exists to de-risk during volatility, so fail-open to full size
    defeats the feature exactly when it matters most. Configurable for
    operators who want different behavior."""
    return cfg.getfloat("trading", "vix_unavailable_scale", fallback=0.5)


def _get_scale_factor(vix: float) -> float:
    """Determine position scale factor based on VIX level."""
    low = _VIX_LOW_THRESHOLD()
    high = _VIX_HIGH_THRESHOLD()
    extreme = _VIX_EXTREME_THRESHOLD()
    
    if vix < low:
        return 1.0  # Normal sizing
    elif vix < high:
        return 0.75  # Slight reduction
    elif vix < extreme:
        return 0.5  # Moderate reduction
    else:
        return 0.25  # Aggressive reduction


# Live VIX cache, refreshed by the background task in app.py (Public
# INDEX quote every 60s while vix_sizing_enabled). A manual config
# override (vix_current_level) still wins so the knob stays portal-usable.
_VIX_CACHE: tuple[float, float] | None = None  # (level, monotonic ts)
# Default cache lifetime. 24h so an overnight gap in index dissemination
# does not expire the last known level into a silent half-size entry.
# Override with [trading] vix_cache_max_age_seconds.
_VIX_CACHE_MAX_AGE_S = 86400.0


def update_vix_cache(level: float) -> None:
    """Called by the background refresher with a live VIX level."""
    global _VIX_CACHE
    import time
    _VIX_CACHE = (float(level), time.monotonic())


def _vix_cache_max_age() -> float:
    """How long a cached VIX stays usable.

    Was a hard 5 minutes, which only makes sense if the feed never stops. It
    does: an index is disseminated during the cash session and publishes
    NOTHING outside it — probed on IBKR at 03:22 ET, live and delayed alike
    returned bid/ask = -1 and last/close = nan. So every night, every weekend
    and every brief feed gap expired the cache, _fetch_vix_level() returned
    None, and vix_unavailable_scale cut every entry in half. A VIX from
    yesterday's close is the correct input for sizing a position opened before
    today's open; a silent 0.50x is not.

    Default 24h carries overnight. A Friday close into a Monday pre-market
    entry is still older than this, and deliberately so — that is the one case
    where the last print really is stale. Set vix_unavailable_scale = 1.0 if
    you would rather an unknown VIX mean "no adjustment" than "half size".
    """
    return cfg.getfloat("trading", "vix_cache_max_age_seconds",
                        fallback=_VIX_CACHE_MAX_AGE_S)


def _fetch_vix_level() -> Optional[float]:
    """Current VIX: manual config override → cached level (see
    _vix_cache_max_age) → None."""
    try:
        vix_str = cfg.get("trading", "vix_current_level", fallback="")
        if vix_str:
            return float(vix_str)
        if _VIX_CACHE is not None:
            import time
            level, ts = _VIX_CACHE
            if time.monotonic() - ts <= _vix_cache_max_age():
                return level
        return None
    except (ValueError, Exception) as e:
        log.debug("Could not fetch VIX level: %s", e)
        return None


def apply_vix_sizing(qty: int, symbol: str = "") -> VixScaleResult:
    """
    Apply VIX-based position sizing to a trade.
    
    Args:
        qty: Original position size
        symbol: Option symbol (for logging)
        
    Returns:
        VixScaleResult with original and adjusted quantities
    """
    if not _VIX_SIZING_ENABLED():
        return VixScaleResult(
            original_qty=qty,
            adjusted_qty=qty,
            scale_factor=1.0,
            vix_level=None,
            reason="VIX sizing disabled"
        )
    
    vix = _fetch_vix_level()
    if vix is None:
        # Fail-safe: VIX sizing is a risk-reduction feature. If the feed
        # fails, default to a conservative scale (half size) rather than
        # full size — otherwise the feature silently disables itself
        # exactly when volatility may be highest.
        fallback_scale = _VIX_UNAVAILABLE_SCALE()
        adjusted_qty = max(1, int(qty * fallback_scale))
        log.warning("[VIX_SIZER] VIX unavailable — applying fallback scale %.2fx (qty %d → %d) for %s",
                    fallback_scale, qty, adjusted_qty, symbol)
        return VixScaleResult(
            original_qty=qty,
            adjusted_qty=adjusted_qty,
            scale_factor=fallback_scale,
            vix_level=None,
            reason=f"VIX data unavailable — fallback {fallback_scale:.0%} size"
        )
    
    scale_factor = _get_scale_factor(vix)
    adjusted_qty = max(1, int(qty * scale_factor))
    
    # Build reason string — tier by threshold (not float == on the factor).
    if vix < _VIX_LOW_THRESHOLD():
        reason = f"VIX {vix:.1f} < {_VIX_LOW_THRESHOLD()} (normal)"
    elif vix < _VIX_HIGH_THRESHOLD():
        reason = f"VIX {vix:.1f} elevated ({scale_factor:.0%} size)"
    elif vix < _VIX_EXTREME_THRESHOLD():
        reason = f"VIX {vix:.1f} high ({scale_factor:.0%} size)"
    else:
        reason = f"VIX {vix:.1f} extreme ({scale_factor:.0%} size)"
    
    if adjusted_qty != qty:
        log.info("[VIX_SIZER] %s: qty %d → %d (VIX: %.1f, scale: %.0f%%)",
                 symbol, qty, adjusted_qty, vix, scale_factor * 100)
    
    return VixScaleResult(
        original_qty=qty,
        adjusted_qty=adjusted_qty,
        scale_factor=scale_factor,
        vix_level=vix,
        reason=reason
    )


def get_vix_status() -> dict:
    """Return current VIX status for dashboard."""
    vix = _fetch_vix_level()
    return {
        "enabled": _VIX_SIZING_ENABLED(),
        "current_vix": vix,
        "low_threshold": _VIX_LOW_THRESHOLD(),
        "high_threshold": _VIX_HIGH_THRESHOLD(),
        "extreme_threshold": _VIX_EXTREME_THRESHOLD(),
        "scale_factor": _get_scale_factor(vix) if vix else _VIX_UNAVAILABLE_SCALE(),
    }
