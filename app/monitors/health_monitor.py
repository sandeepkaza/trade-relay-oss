"""
health_monitor.py - Component health monitoring and status reporting.

Tracks the health of:
- Discord connection
- Public.com API
- Position monitor
- Database connectivity
- Reconciler status
- Forwarder status

Provides dashboard with real-time status and alerts for failures.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


log = logging.getLogger(__name__)


@dataclass
class ComponentStatus:
    """Status of a single component."""
    name: str
    is_healthy: bool
    last_check: float
    last_success: float
    last_error: Optional[str] = None
    error_count: int = 0
    
    @property
    def seconds_since_success(self) -> float:
        return time.time() - self.last_success
    
    @property
    def status_indicator(self) -> str:
        if self.is_healthy:
            return "✅"
        return "❌"


class HealthMonitor:
    """Central health monitor for all bot components."""
    
    def __init__(self):
        self.components: dict[str, ComponentStatus] = {}
        self.start_time = time.time()
        self._lock = asyncio.Lock()
    
    def register_component(self, name: str) -> None:
        """Register a new component for monitoring."""
        now = time.time()
        self.components[name] = ComponentStatus(
            name=name,
            is_healthy=True,
            last_check=now,
            last_success=now,
        )
        log.info("[HEALTH] Registered component: %s", name)
    
    async def update_status(self, name: str, is_healthy: bool, error: Optional[str] = None) -> None:
        """Update the status of a component."""
        async with self._lock:
            if name not in self.components:
                self.register_component(name)
            
            component = self.components[name]
            component.last_check = time.time()
            
            if is_healthy:
                component.is_healthy = True
                component.last_success = time.time()
                component.last_error = None
            else:
                component.is_healthy = False
                component.error_count += 1
                if error:
                    component.last_error = error
                
                # Log degradation
                if component.error_count == 1:
                    log.warning("[HEALTH] %s degraded: %s", name, error or "Unknown error")
                elif component.error_count % 10 == 0:
                    log.error("[HEALTH] %s still degraded (%d errors)", name, component.error_count)
    
    async def heartbeat(self, name: str) -> None:
        """Record a successful heartbeat from a component."""
        await self.update_status(name, is_healthy=True)
    
    def get_status(self, name: str) -> Optional[ComponentStatus]:
        """Get status of a specific component."""
        return self.components.get(name)
    
    def get_all_status(self) -> dict:
        """Get status of all components for dashboard."""
        now = time.time()
        
        # Calculate overall health
        total = len(self.components)
        healthy = sum(1 for c in self.components.values() if c.is_healthy)
        
        result = {
            "overall": {
                "status": "healthy" if healthy == total else "degraded" if healthy > 0 else "down",
                "healthy_components": healthy,
                "total_components": total,
                "uptime_seconds": int(now - self.start_time),
            },
            "components": {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        
        for name, component in self.components.items():
            result["components"][name] = {
                "status": "healthy" if component.is_healthy else "down",
                "indicator": component.status_indicator,
                "seconds_since_check": int(now - component.last_check),
                "seconds_since_success": int(component.seconds_since_success),
                "error_count": component.error_count,
                "last_error": component.last_error,
            }
        
        return result
    
    def get_unhealthy_components(self) -> list[str]:
        """Get list of unhealthy component names."""
        return [name for name, comp in self.components.items() if not comp.is_healthy]
    
    async def check_database(self) -> bool:
        """Check database connectivity."""
        try:
            from app.core.db import SessionLocal
            db = SessionLocal()
            try:
                # Simple query to test connection
                from sqlalchemy import text
                db.execute(text("SELECT 1"))
                await self.update_status("database", True)
                return True
            finally:
                db.close()
        except Exception as e:
            await self.update_status("database", False, str(e))
            return False

    async def check_ibkr(self) -> Optional[bool]:
        """Check IB Gateway connectivity via the existing ib_async singleton.

        Design choices:
          * No-op when active broker isn't ibkr (Public.com stack stays clean).
          * Does NOT trigger a fresh connect — first connect can take 20+ s while
            Gateway streams positions/orders/executions. A health probe that
            forces that cost would make /api/health slow and would also paper
            over real "we never even tried to connect yet" cases. We only probe
            what's already there.
          * If the singleton is None (bot hasn't made an IBKR call yet), the
            check is skipped (returns None, no component registered) — the
            real signal will arrive once trader code actually uses the broker.
          * If a singleton exists, run isConnected() + reqCurrentTimeAsync()
            with a tight 2s timeout. This catches "socket open but session
            dead" — the failure mode the disconnectedEvent handler can't see.
            2s is ample for a Gateway round-trip (healthy is <10ms); a longer
            wait just holds the /api/health request open on a dead session.
        """
        try:
            from app.execution.broker_router import active_broker_name
        except Exception:
            return None
        try:
            if active_broker_name() != "ibkr":
                return None
        except Exception:
            return None

        try:
            from app.execution import ibkr_sdk_bridge  # type: ignore
        except Exception as e:
            await self.update_status("ibkr", False, f"bridge import failed: {e}")
            return False

        ib = getattr(ibkr_sdk_bridge, "_ib", None)
        if ib is None:
            # No connection attempted yet this process — don't trigger one from
            # a health probe. Stay absent until the bot itself uses the broker.
            return None

        try:
            if not ib.isConnected():
                await self.update_status("ibkr", False, "ib.isConnected() == False")
                return False
            await asyncio.wait_for(ib.reqCurrentTimeAsync(), timeout=2.0)
            await self.update_status("ibkr", True)
            return True
        except asyncio.TimeoutError:
            await self.update_status("ibkr", False, "reqCurrentTime timeout >2s (session may be dead)")
            return False
        except Exception as e:
            await self.update_status("ibkr", False, f"{type(e).__name__}: {e}")
            return False

    async def check_stale_heartbeats(self, max_age_seconds: float = 120.0) -> list[str]:
        """Mark components DEGRADED when their heartbeat is older than threshold,
        and post a Discord alarm the FIRST time each component goes stale.

        Returns list of newly-stale component names this tick.

        2026-05-05 incident: forwarder Discord gateway hung for 77 min, no
        heartbeats during outage, but health_monitor showed "healthy" because
        nobody was calling update_status with is_healthy=False. This closes
        that gap by tracking last_check timestamps across all components.

        Alarm-suppression rules (2026-05-06 follow-up):
          - Startup grace: skip alarms for the first 5 minutes after the
            monitor was created. Cold-start components legitimately don't
            beat for the first poll interval and we don't want a deploy
            cascade to ping every restart.
          - Excluded components: discord_listener, public_api, premarket_prep
            don't beat continuously by design (windowed / event-driven).
            They stay in the registration list so dashboards can see them,
            but they don't trip the always-on stale alarm.
        """
        # Lazy-init startup time on first call.
        if not hasattr(self, "_started_at"):
            self._started_at = time.time()
        STARTUP_GRACE = 300.0  # 5 min — covers cold-start before first beat
        EXCLUDED = {"discord_listener", "public_api", "premarket_prep"}

        now = time.time()
        in_grace = (now - self._started_at) < STARTUP_GRACE
        newly_stale: list[str] = []
        async with self._lock:
            for name, comp in self.components.items():
                if name in EXCLUDED:
                    continue
                age = now - comp.last_check
                if age > max_age_seconds:
                    if comp.is_healthy:
                        comp.is_healthy = False
                        comp.error_count += 1
                        comp.last_error = f"No heartbeat for {age:.0f}s"
                        if not in_grace:
                            newly_stale.append(name)
                        log.error(
                            "[HEALTH] %s STALE — no heartbeat for %.0fs (threshold %.0fs)%s",
                            name, age, max_age_seconds,
                            " [in startup grace, alarm suppressed]" if in_grace else "",
                        )

        # Post Discord alarms outside the lock to avoid blocking other heartbeats.
        for name in newly_stale:
            try:
                import app.analytics.trade_logger as _tl
                await _tl.log_health_alarm(
                    component=name,
                    message=f"No heartbeat for >{int(max_age_seconds)}s — process may be hung or disconnected.",
                )
            except Exception as exc:
                log.warning("[HEALTH] could not post Discord alarm for %s: %s", name, exc)
        return newly_stale


# Global singleton instance
_health_monitor: Optional[HealthMonitor] = None


def get_health_monitor() -> HealthMonitor:
    """Get or create the global health monitor instance."""
    global _health_monitor
    if _health_monitor is None:
        _health_monitor = HealthMonitor()
    return _health_monitor


# Convenience functions
def register_component(name: str) -> None:
    """Register a component for monitoring."""
    get_health_monitor().register_component(name)


async def heartbeat(name: str) -> None:
    """Record a successful heartbeat."""
    await get_health_monitor().heartbeat(name)


async def report_error(name: str, error: str) -> None:
    """Report an error for a component."""
    await get_health_monitor().update_status(name, is_healthy=False, error=error)


async def get_health_status() -> dict:
    """Get full health status."""
    monitor = get_health_monitor()
    # Update DB status before returning
    await monitor.check_database()
    await monitor.check_ibkr()
    return monitor.get_all_status()
