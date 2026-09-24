"""
kelly_sizer.py - Smart Position Sizing using the Kelly Criterion.

Calculates the optimal number of contracts per trade based on:
  1. Account balance (total portfolio value)
  2. Historical win rate (from filled BUY alerts)
  3. Average win/loss ratio (payoff ratio)
  4. Current option price

Uses Half-Kelly (f*/2) for conservative sizing, plus a hard cap:
  - Never risk more than max_risk_per_trade % of account on a single trade
  - Always returns at least 1 contract (if Kelly says to trade at all)

Kelly Formula: f* = (b*p - q) / b
  where: p = win rate, q = 1-p, b = avg_win / avg_loss

Enable via config.ini:
    [trading]
    kelly_enabled = true
    kelly_max_risk_pct = 2.0
    kelly_lookback_days = 30
"""

import logging
from datetime import datetime, timedelta, timezone

from app.core.db import SessionLocal
from app.core.models import Alert, Position
from app.core.config_manager import cfg  # hot-reloadable singleton

log = logging.getLogger(__name__)

# Live config accessors (read on every call — dashboard changes take effect immediately)
def _KELLY_ENABLED():    return cfg.getboolean("trading", "kelly_enabled", fallback=False)
def _MAX_RISK_PCT():     return cfg.getfloat("trading", "kelly_max_risk_pct", fallback=2.0)
def _LOOKBACK_DAYS():   return cfg.getint("trading", "kelly_lookback_days", fallback=30)


def calculate_kelly_contracts(
    price: float,
    account_balance: float,
    author: str = "",
) -> dict:
    """
    Calculate the max contracts to trade using Half-Kelly Criterion.

    Returns:
        {
            "kelly_fraction": float,    # raw Kelly fraction (before halving)
            "half_kelly_fraction": float,# conservative half-Kelly
            "max_risk_dollars": float,   # hard cap: max_risk_pct% of account
            "kelly_dollars": float,      # Kelly-suggested position size
            "max_contracts": int,        # final contract limit
            "win_rate": float,           # historical win rate used
            "payoff_ratio": float,       # avg_win / avg_loss used
            "trades_analyzed": int,      # number of historical trades
            "reason": str,              # human-readable explanation
        }
    """
    if not _KELLY_ENABLED():
        return _default_result("Kelly sizing disabled")

    if price <= 0 or account_balance <= 0:
        return _default_result("Invalid price or balance")

    cost_per_contract = price * 100  # options = 100 shares per contract

    # Hard cap: never risk more than X% of account
    max_risk_dollars = account_balance * (_MAX_RISK_PCT() / 100)

    # Get historical stats
    stats = _get_historical_stats(author)

    if stats["trades"] < 5:
        # Not enough data — use max risk cap only (no Kelly)
        max_contracts = max(1, int(max_risk_dollars / cost_per_contract))
        return {
            "kelly_fraction": 0,
            "half_kelly_fraction": 0,
            "max_risk_dollars": round(max_risk_dollars, 2),
            "kelly_dollars": 0,
            "max_contracts": max_contracts,
            "win_rate": stats["win_rate"],
            "payoff_ratio": stats["payoff_ratio"],
            "trades_analyzed": stats["trades"],
            "reason": f"<5 trades for {author or 'all'} — using {_MAX_RISK_PCT()}% risk cap only → {max_contracts} contracts",
        }

    # Kelly Criterion: f* = (b*p - q) / b
    p = stats["win_rate"]
    q = 1 - p
    b = stats["payoff_ratio"]

    if b <= 0:
        return _default_result(f"No positive payoff ratio (b={b:.2f})")

    kelly_fraction = (b * p - q) / b

    if kelly_fraction <= 0:
        # Negative Kelly = no edge, shouldn't trade
        return {
            "kelly_fraction": round(kelly_fraction, 4),
            "half_kelly_fraction": 0,
            "max_risk_dollars": round(max_risk_dollars, 2),
            "kelly_dollars": 0,
            "max_contracts": 0,
            "win_rate": round(p, 3),
            "payoff_ratio": round(b, 2),
            "trades_analyzed": stats["trades"],
            "reason": f"Negative Kelly ({kelly_fraction:.3f}) — no edge detected ({p:.0%} win rate, {b:.1f}x payoff)",
        }

    # Use Half-Kelly for conservative sizing
    half_kelly = kelly_fraction / 2

    # Kelly position size in dollars
    kelly_dollars = account_balance * half_kelly

    # Apply the smaller of Kelly sizing vs hard risk cap
    max_risk_dollars = account_balance * (_MAX_RISK_PCT() / 100)
    position_dollars = min(kelly_dollars, max_risk_dollars)
    max_contracts = max(1, int(position_dollars / cost_per_contract))

    which_limit = "Kelly" if kelly_dollars <= max_risk_dollars else f"{_MAX_RISK_PCT()}% risk cap"

    reason = (
        f"½Kelly={half_kelly:.1%} → ${kelly_dollars:.0f}, "
        f"cap={_MAX_RISK_PCT()}%→${max_risk_dollars:.0f}, "
        f"using {which_limit} → {max_contracts} contracts "
        f"(p={p:.0%}, b={b:.1f}x, {stats['trades']} trades)"
    )

    log.info("[KELLY] %s", reason)

    return {
        "kelly_fraction": round(kelly_fraction, 4),
        "half_kelly_fraction": round(half_kelly, 4),
        "max_risk_dollars": round(max_risk_dollars, 2),
        "kelly_dollars": round(kelly_dollars, 2),
        "max_contracts": max_contracts,
        "win_rate": round(p, 3),
        "payoff_ratio": round(b, 2),
        "trades_analyzed": stats["trades"],
        "reason": reason,
    }


def apply_kelly_cap(
    qty: int,
    price: float,
    account_balance: float,
    author: str = "",
) -> tuple[int, dict]:
    """
    Apply Kelly sizing as a CAP on the proposed quantity.
    Returns (capped_qty, kelly_result).

    - If Kelly is disabled: returns qty unchanged.
    - If insufficient history (<5 trades): returns qty unchanged.
      Kelly needs real data to be meaningful; without it we trust the
      configured size_default / size_tag instead of a noisy risk-cap calc.
    - If Kelly says 0 (negative edge): blocks the trade.
    - Otherwise: caps qty at Kelly-calculated max.
    """
    result = calculate_kelly_contracts(price, account_balance, author)

    if not _KELLY_ENABLED():
        return qty, result

    # Not enough Kelly history — fall back to the hard risk cap
    # (max_risk_pct % of account / per-contract cost) so we never bypass the
    # absolute risk gate just because no historical data exists.
    if result["trades_analyzed"] < 5:
        max_risk_dollars = result.get("max_risk_dollars", 0.0)
        per_contract_cost = price * 100 if price > 0 else 0
        if per_contract_cost > 0 and max_risk_dollars > 0:
            hard_cap = max(1, int(max_risk_dollars // per_contract_cost))
            capped = min(qty, hard_cap)
            if capped < qty:
                log.info(
                    "[KELLY] %s has <5 trades — applying hard risk cap %d → %d",
                    author or "all analysts", qty, capped,
                )
            return capped, result
        log.info(
            "[KELLY] %s has <5 trades and no risk cap available — using configured qty=%d",
            author or "all analysts", qty,
        )
        return qty, result

    max_contracts = result["max_contracts"]

    # If Kelly says no edge (negative fraction → 0 contracts): block the trade
    if max_contracts == 0:
        return 0, result

    # Enough data: cap qty at Kelly max
    capped = min(qty, max_contracts)
    if capped < qty:
        log.info("[KELLY] Capped qty %d → %d (%s)", qty, capped, result["reason"])
    return capped, result


def _get_historical_stats(author: str = "") -> dict:
    """
    Calculate win rate and payoff ratio from filled BUY alerts.
    If author is specified, filter to that author; otherwise use all.
    """
    db = SessionLocal()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS())

        query = db.query(Alert).filter(
            Alert.action == "BUY",
            Alert.status == "FILLED",
            Alert.timestamp >= cutoff,
        )
        if author:
            query = query.filter(Alert.author.ilike(f"%{author}%"))

        alerts = query.all()

        wins = 0
        losses = 0
        total_win_pct = 0.0
        total_loss_pct = 0.0

        for alert in alerts:
            if not alert.osi_symbol:
                continue
            # Only count CLOSED positions — open positions have no real exit price
            pos = db.query(Position).filter_by(
                osi_symbol=alert.osi_symbol, status="CLOSED"
            ).first()
            if not pos or not pos.avg_price or pos.avg_price <= 0:
                continue

            # Use close_price for realized P&L accuracy. pos.pnl_pct() inverts
            # credit spreads so a winning CCS is not counted as a loss.
            if pos.close_price is None and pos.current_price is None:
                continue

            pnl_pct = pos.pnl_pct()

            if pnl_pct > 0:
                wins += 1
                total_win_pct += pnl_pct
            else:
                losses += 1
                total_loss_pct += abs(pnl_pct)

        total = wins + losses
        if total == 0:
            return {"trades": 0, "win_rate": 0.5, "payoff_ratio": 1.0}

        win_rate = wins / total
        avg_win = total_win_pct / wins if wins > 0 else 0
        avg_loss = total_loss_pct / losses if losses > 0 else 1
        payoff_ratio = avg_win / avg_loss if avg_loss > 0 else 1.0

        return {
            "trades": total,
            "win_rate": win_rate,
            "payoff_ratio": payoff_ratio,
            "avg_win_pct": avg_win,
            "avg_loss_pct": avg_loss,
        }
    finally:
        db.close()


def _default_result(reason: str) -> dict:
    """Return a pass-through result (no Kelly adjustment)."""
    return {
        "kelly_fraction": 0,
        "half_kelly_fraction": 0,
        "max_risk_dollars": 0,
        "kelly_dollars": 0,
        "max_contracts": 999,  # no cap
        "win_rate": 0,
        "payoff_ratio": 0,
        "trades_analyzed": 0,
        "reason": reason,
    }
