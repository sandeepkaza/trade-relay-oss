"""
greeks_monitor.py - Greeks (Delta, Gamma, Theta) tracking for options positions.

Provides:
- Delta exposure tracking (directional risk)
- Gamma exposure (convexity)
- Theta decay (time decay)
- Alert when exposure exceeds thresholds

Note: This is a simplified implementation. For accurate Greeks, 
you would need real-time IV, underlying price, and a pricing model.
"""

import logging
from dataclasses import dataclass
from datetime import date
from typing import Optional

from app.core.config_manager import cfg

log = logging.getLogger(__name__)


@dataclass
class Greeks:
    """Option Greeks for a position."""
    delta: float  # -1 to 1 (per share), -100 to 100 (per contract)
    gamma: float  # Rate of change of delta
    theta: float  # Daily time decay (negative for long options)
    vega: float   # Sensitivity to IV changes
    
    @property
    def total_delta(self) -> float:
        """Total delta for position sizing (contract delta × 100)."""
        return self.delta * 100


class GreeksMonitor:
    """Monitor aggregate Greeks exposure across all positions."""
    
    def __init__(self):
        self.positions_greeks: dict[int, Greeks] = {}  # position_id -> Greeks
    
    def _GREEKS_ENABLED(self) -> bool:
        return cfg.getboolean("trading", "greeks_monitoring_enabled", fallback=False)
    
    def _DELTA_THRESHOLD(self) -> float:
        return cfg.getfloat("trading", "greeks_delta_threshold", fallback=500.0)
    
    def _THETA_THRESHOLD(self) -> float:
        return cfg.getfloat("trading", "greeks_theta_threshold", fallback=-100.0)
    
    def calculate_greeks_approx(
        self,
        option_type: str,  # C or P
        strike: float,
        underlying_price: float,
        days_to_expiry: int,
        implied_vol: float = 0.30,  # Default 30% IV
    ) -> Greeks:
        """
        Calculate approximate Greeks using simplified formulas.
        
        This is a rough estimation for monitoring purposes.
        For production, use a proper options pricing library.
        """
        if not self._GREEKS_ENABLED():
            return Greeks(0, 0, 0, 0)
        
        # Simplified approximations
        is_call = option_type.upper() == "C"
        moneyness = underlying_price / strike if strike > 0 else 1.0
        
        # Approximate Delta
        if is_call:
            # Call delta: ~0.5 at the money, approaches 1 deep ITM, 0 deep OTM
            if moneyness > 1.1:
                delta = 0.85
            elif moneyness > 0.95:
                delta = 0.60
            elif moneyness > 0.90:
                delta = 0.40
            else:
                delta = 0.15
        else:
            # Put delta: ~-0.5 ATM, approaches -1 deep ITM (S<<K), 0 deep OTM
            # (S>>K). Mirror of the call ladder. Old version had ATM (m=1.0)
            # bucketed with deep ITM (-0.60) and slightly-OTM (m=1.04) as
            # -0.60, with the 1.05-1.10 band giving -0.40 (less delta than
            # ATM — backwards).
            if moneyness < 0.90:
                delta = -0.85   # deep ITM
            elif moneyness < 0.95:
                delta = -0.60   # ITM
            elif moneyness < 1.05:
                delta = -0.50   # ATM
            elif moneyness < 1.10:
                delta = -0.40   # slightly OTM
            else:
                delta = -0.15   # deep OTM
        
        # Approximate Gamma (highest near ATM)
        if 0.95 <= moneyness <= 1.05:
            gamma = 0.05
        else:
            gamma = 0.02
        
        # Approximate Theta (negative for long options, worse near expiry)
        theta_decay = -0.02  # Base daily decay
        if days_to_expiry <= 1:
            theta_decay = -0.10  # High decay on expiry day
        elif days_to_expiry <= 5:
            theta_decay = -0.05  # Elevated decay near expiry
        
        # Approximate Vega (higher for longer dated, ATM options)
        vega = implied_vol * 0.1
        if days_to_expiry < 7:
            vega *= 0.3  # Lower for short-dated
        
        return Greeks(delta, gamma, theta_decay, vega)
    
    def update_position_greeks(
        self,
        position_id: int,
        contracts: int,
        option_type: str,
        strike: float,
        underlying_price: float,
        expiry_str: str,
    ) -> Greeks:
        """Calculate and store Greeks for a position."""
        if not self._GREEKS_ENABLED():
            return Greeks(0, 0, 0, 0)
        
        try:
            # Parse expiry — None means we couldn't parse; skip Greeks rather
            # than fabricating numbers from a 30-day default.
            dte = self._days_to_expiry(expiry_str)
            if dte is None:
                log.debug("Greeks skipped for position %d: unparseable expiry %r",
                          position_id, expiry_str)
                return Greeks(0, 0, 0, 0)

            # Calculate Greeks per contract
            greeks = self.calculate_greeks_approx(
                option_type, strike, underlying_price, dte
            )
            
            # Scale by number of contracts
            scaled = Greeks(
                delta=greeks.delta * contracts,
                gamma=greeks.gamma * contracts,
                theta=greeks.theta * contracts,
                vega=greeks.vega * contracts,
            )
            
            self.positions_greeks[position_id] = scaled
            return scaled
            
        except Exception as e:
            log.debug("Failed to calculate Greeks for position %d: %s", position_id, e)
            return Greeks(0, 0, 0, 0)
    
    def _days_to_expiry(self, expiry_str: str) -> Optional[int]:
        """Calculate days to expiry from expiry string. Returns None on
        parse failure so callers skip Greeks for that position rather than
        defaulting to 30 (which silently over-weights theta on long-dated
        positions and under-weights short-dated). Supports YYMMDD (legacy)
        and YYYY-MM-DD (ISO) and YYYYMMDD."""
        if not expiry_str:
            return None
        s = expiry_str.strip()
        try:
            if len(s) == 6 and s.isdigit():       # YYMMDD
                yy, mm, dd = int(s[:2]), int(s[2:4]), int(s[4:6])
                expiry = date(2000 + yy, mm, dd)
            elif len(s) == 8 and s.isdigit():     # YYYYMMDD
                expiry = date(int(s[:4]), int(s[4:6]), int(s[6:8]))
            elif len(s) == 10 and s[4] == "-" and s[7] == "-":  # YYYY-MM-DD
                expiry = date(int(s[:4]), int(s[5:7]), int(s[8:10]))
            else:
                return None
            return max(0, (expiry - date.today()).days)
        except (ValueError, TypeError):
            return None
    
    def get_aggregate_greeks(self) -> dict:
        """Get aggregate Greeks across all positions."""
        total_delta = sum(g.delta for g in self.positions_greeks.values())
        total_gamma = sum(g.gamma for g in self.positions_greeks.values())
        total_theta = sum(g.theta for g in self.positions_greeks.values())
        total_vega = sum(g.vega for g in self.positions_greeks.values())
        
        return {
            "total_delta": round(total_delta, 2),
            "total_gamma": round(total_gamma, 2),
            "total_theta": round(total_theta, 2),
            "total_vega": round(total_vega, 2),
            "position_count": len(self.positions_greeks),
            "delta_exposure": round(total_delta * 100, 0),  # Per contract × 100
        }
    
    def check_thresholds(self) -> list[str]:
        """Check if Greeks exceed configured thresholds and return alerts."""
        if not self._GREEKS_ENABLED():
            return []
        
        alerts = []
        agg = self.get_aggregate_greeks()
        
        delta_threshold = self._DELTA_THRESHOLD()
        theta_threshold = self._THETA_THRESHOLD()
        
        delta_exposure = abs(agg["delta_exposure"])
        if delta_exposure > delta_threshold:
            alerts.append(
                f"HIGH DELTA EXPOSURE: {agg['delta_exposure']:+.0f} (threshold: ±{delta_threshold})"
            )
        
        if agg["total_theta"] < theta_threshold:
            alerts.append(
                f"HIGH THETA DECAY: ${agg['total_theta']:.2f}/day (threshold: ${theta_threshold})"
            )
        
        return alerts


# Global instance
_greeks_monitor: Optional[GreeksMonitor] = None


def get_greeks_monitor() -> GreeksMonitor:
    """Get or create the global Greeks monitor."""
    global _greeks_monitor
    if _greeks_monitor is None:
        _greeks_monitor = GreeksMonitor()
    return _greeks_monitor


def get_greeks_status() -> dict:
    """Get Greeks status for dashboard."""
    monitor = get_greeks_monitor()
    return {
        "enabled": monitor._GREEKS_ENABLED(),
        "aggregate": monitor.get_aggregate_greeks(),
        "alerts": monitor.check_thresholds(),
        "thresholds": {
            "delta": monitor._DELTA_THRESHOLD(),
            "theta": monitor._THETA_THRESHOLD(),
        },
    }
