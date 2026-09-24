"""
order_retry.py - Smart order retry logic for failed or partially filled orders.

When an order fails or partially fills, this module:
1. Retries with improved pricing (nudge limit up/down for buy/sell)
2. Retries up to N times with exponential backoff
3. Alerts user after max retries exhausted
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from app.core.config_manager import cfg
from app.core.models import Order

log = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Configuration for order retry behavior."""
    enabled: bool
    max_retries: int
    price_nudge_bps: Decimal  # Basis points to nudge price (1 bps = 0.01%)
    initial_delay_seconds: float
    max_delay_seconds: float


def get_retry_config() -> RetryConfig:
    """Get current retry configuration from config."""
    return RetryConfig(
        enabled=cfg.getboolean("trading", "order_retry_enabled", fallback=False),
        max_retries=cfg.getint("trading", "order_retry_max", fallback=3),
        price_nudge_bps=Decimal(str(cfg.getfloat("trading", "order_retry_nudge_bps", fallback=10.0))),
        initial_delay_seconds=cfg.getfloat("trading", "order_retry_initial_delay", fallback=2.0),
        max_delay_seconds=cfg.getfloat("trading", "order_retry_max_delay", fallback=30.0),
    )


def calculate_retry_price(original_price: Decimal, side: str, nudge_bps: Decimal) -> Decimal:
    """
    Calculate improved price for retry.
    
    For BUY: increase price (make more aggressive) to get filled faster
    For SELL: decrease price (make more aggressive) to get filled faster
    
    Args:
        original_price: Original limit price
        side: "BUY" or "SELL"
        nudge_bps: Basis points to nudge (e.g., 10 = 0.10%)
    
    Returns:
        New limit price with nudge applied
    """
    nudge_factor = nudge_bps / Decimal("10000")  # Convert bps to decimal
    nudge_amount = original_price * nudge_factor
    
    # Round to 2 decimal places for USD
    if side == "BUY":
        # Increase buy price (more aggressive)
        new_price = original_price + nudge_amount
    else:  # SELL
        # Decrease sell price (more aggressive)
        new_price = original_price - nudge_amount
    
    # Ensure minimum price of 0.01
    new_price = max(Decimal("0.01"), new_price)
    
    return Decimal(str(round(new_price, 2)))


def should_retry_order(order: Order, retry_count: int) -> bool:
    """
    Determine if an order should be retried.
    
    Returns True if:
    - Retry is enabled
    - Retry count < max retries
    - Order is in a retryable state (REJECTED, ERROR, PARTIALLY_FILLED with remaining)
    """
    config = get_retry_config()
    
    if not config.enabled:
        return False
    
    if retry_count >= config.max_retries:
        log.warning("[RETRY] Max retries (%d) reached for order #%d", config.max_retries, order.id)
        return False
    
    retryable_statuses = ["REJECTED", "ERROR", "PARTIALLY_FILLED"]
    if order.status not in retryable_statuses:
        return False
    
    # For partial fills, only retry if there's remaining quantity
    if order.status == "PARTIALLY_FILLED":
        filled = order.filled_qty or 0
        if filled >= order.quantity:
            return False  # Fully filled, no need to retry
    
    return True


def calculate_backoff_delay(retry_count: int, config: RetryConfig) -> float:
    """Calculate exponential backoff delay."""
    delay = config.initial_delay_seconds * (2 ** retry_count)
    return min(delay, config.max_delay_seconds)


async def record_retry_attempt(order_id: int, retry_count: int, new_price: Decimal, reason: str):
    """Record a retry attempt in the database for tracking."""
    try:
        from app.core.db import SessionLocal
        db = SessionLocal()
        try:
            order = db.query(Order).filter(Order.id == order_id).first()
            if order:
                # Store retry info in error_text or a dedicated field
                retry_info = f"Retry #{retry_count}: price={new_price} ({reason})"
                if order.error_text:
                    order.error_text = f"{order.error_text}\n{retry_info}"
                else:
                    order.error_text = retry_info
                db.commit()
        finally:
            db.close()
    except Exception as e:
        log.debug("Failed to record retry attempt: %s", e)


async def notify_retry_exhausted(order: Order, retry_count: int, executor):
    """Notify user that all retries have been exhausted."""
    msg = f"Order #{order.id} ({order.side} {order.osi_symbol}) failed after {retry_count} retries"
    log.error("[RETRY_EXHAUSTED] %s", msg)
    
    # Broadcast to dashboard
    try:
        await executor.ws_manager.broadcast({
            "type": "retry_exhausted",
            "data": {
                "order_id": order.id,
                "symbol": order.osi_symbol,
                "side": order.side,
                "retry_count": retry_count,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message": msg,
            }
        })
    except Exception as e:
        log.debug("Failed to broadcast retry exhausted: %s", e)


def get_vix_status() -> dict:
    """Return current retry system status for dashboard."""
    config = get_retry_config()
    return {
        "enabled": config.enabled,
        "max_retries": config.max_retries,
        "nudge_bps": float(config.price_nudge_bps),
        "initial_delay": config.initial_delay_seconds,
        "max_delay": config.max_delay_seconds,
    }
