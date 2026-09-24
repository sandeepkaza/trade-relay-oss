"""
api_monitor.py - Monitor API usage, rate limits, and failures for Discord and Public.com APIs

Tracks:
- Request counts per API endpoint
- Rate limit hits (HTTP 429)
- Error rates and response codes
- Response times
- Recent failures with timestamps

Exposes: /api/metrics endpoint for dashboard
"""

import json
import os
import tempfile
import time
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timezone
import logging

_PERSIST_PATH = "/app/data/api_monitor_today.json"

log = logging.getLogger(__name__)

# Keep last 100 failures per service
MAX_FAILURE_HISTORY = 100


@dataclass
class APICall:
    timestamp: float
    endpoint: str
    status_code: int
    response_time_ms: float
    error: Optional[str] = None


# Keep last N latency samples per service for percentiles. 1000 samples is
# plenty for stable p95/p99 over a typical bot day, costs ~8KB per service.
LATENCY_SAMPLES_PER_SERVICE = 1000
LATENCY_SAMPLES_PER_ENDPOINT = 200


@dataclass
class ServiceMetrics:
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    rate_limited_count: int = 0
    total_response_time_ms: float = 0.0
    recent_failures: deque = field(default_factory=lambda: deque(maxlen=MAX_FAILURE_HISTORY))
    # Latency samples for p50/p95/p99 (rolling, not all-time)
    latency_samples: deque = field(default_factory=lambda: deque(maxlen=LATENCY_SAMPLES_PER_SERVICE))
    endpoint_breakdown: dict = field(default_factory=lambda: defaultdict(lambda: {
        "count": 0, "errors": 0, "rate_limited": 0,
        "latency_samples": deque(maxlen=LATENCY_SAMPLES_PER_ENDPOINT),
    }))

    @property
    def avg_response_time_ms(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return round(self.total_response_time_ms / self.total_requests, 2)

    @property
    def error_rate_pct(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return round((self.failed_requests / self.total_requests) * 100, 2)

    def percentiles(self) -> dict:
        """Return p50/p95/p99 in ms over the last N samples (or 0 if none)."""
        if not self.latency_samples:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0}
        s = sorted(self.latency_samples)
        n = len(s)
        def pct(p: float) -> float:
            idx = min(n - 1, max(0, int(round(p * (n - 1)))))
            return round(s[idx], 2)
        return {"p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99)}


class APIMonitor:
    """Centralized API monitoring for Discord and Public.com.

    Counter mutations and dict ops happen from sync code paths and from
    concurrent async tasks. The async asyncio.Lock above was declared but
    never acquired — counts could silently desync, and `dict(endpoint_
    breakdown)` could raise RuntimeError if mutation happened mid-iteration.
    Use a threading.Lock that protects every mutation and every snapshot."""

    _instance = None
    _new_lock = threading.Lock()  # protects singleton creation
    _state_lock = threading.Lock()  # protects metrics mutations / snapshots

    def __new__(cls):
        with cls._new_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                # Mark uninitialized only once on creation; subsequent
                # __new__ calls must NOT zero this flag (would re-enter
                # __init__ and erase counters mid-flight).
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self.metrics: dict[str, ServiceMetrics] = {
            "discord": ServiceMetrics(),
            "public_com": ServiceMetrics(),
            "discord_webhook": ServiceMetrics(),
        }
        self._start_time = time.time()
        self._persist_throttle_until = 0.0  # rate-limit disk writes
        self._load_persisted()

    # ── Persistence (JSON file, restored on boot if same ET date) ────────────

    def _today_et(self) -> str:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    def _load_persisted(self) -> None:
        """Load counters from disk if the file is from today (ET)."""
        try:
            if not os.path.exists(_PERSIST_PATH):
                return
            with open(_PERSIST_PATH) as f:
                data = json.load(f)
            if data.get("date_et") != self._today_et():
                return
            for svc_name, svc_data in (data.get("services") or {}).items():
                if svc_name not in self.metrics:
                    self.metrics[svc_name] = ServiceMetrics()
                m = self.metrics[svc_name]
                m.total_requests = svc_data.get("total_requests", 0)
                m.successful_requests = svc_data.get("successful", 0)
                m.failed_requests = svc_data.get("failed", 0)
                m.rate_limited_count = svc_data.get("rate_limited", 0)
                m.total_response_time_ms = svc_data.get("total_response_time_ms", 0.0)
                # Restore latency samples (truncated to maxlen by deque ctor)
                for sample in svc_data.get("latency_samples", []) or []:
                    m.latency_samples.append(float(sample))
                # Restore endpoint breakdown
                for ep, ep_stats in (svc_data.get("endpoint_breakdown") or {}).items():
                    bd = m.endpoint_breakdown[ep]
                    bd["count"] = ep_stats.get("count", 0)
                    bd["errors"] = ep_stats.get("errors", 0)
                    bd["rate_limited"] = ep_stats.get("rate_limited", 0)
                    for s in ep_stats.get("latency_samples", []) or []:
                        bd["latency_samples"].append(float(s))
                # Restore recent failures
                for f in svc_data.get("recent_failures", []) or []:
                    m.recent_failures.append(f)
            log.info("[api_monitor] restored counters from %s", _PERSIST_PATH)
        except Exception as e:
            log.warning("[api_monitor] could not restore counters: %s", e)

    def _persist(self) -> None:
        """Atomic JSON write. Throttled to once per 5 seconds to avoid disk
        churn on every API call (counters update on every call but fsync
        every 5s is plenty)."""
        now = time.time()
        if now < self._persist_throttle_until:
            return
        self._persist_throttle_until = now + 5.0
        try:
            os.makedirs(os.path.dirname(_PERSIST_PATH), exist_ok=True)
            data = {
                "date_et": self._today_et(),
                "services": {},
            }
            for svc_name, m in self.metrics.items():
                data["services"][svc_name] = {
                    "total_requests": m.total_requests,
                    "successful": m.successful_requests,
                    "failed": m.failed_requests,
                    "rate_limited": m.rate_limited_count,
                    "total_response_time_ms": m.total_response_time_ms,
                    "latency_samples": list(m.latency_samples),
                    "endpoint_breakdown": {
                        ep: {
                            "count": stats["count"],
                            "errors": stats["errors"],
                            "rate_limited": stats["rate_limited"],
                            "latency_samples": list(stats["latency_samples"]),
                        }
                        for ep, stats in m.endpoint_breakdown.items()
                    },
                    "recent_failures": list(m.recent_failures),
                }
            d = os.path.dirname(_PERSIST_PATH)
            with tempfile.NamedTemporaryFile("w", dir=d, delete=False) as tf:
                json.dump(data, tf)
                tmp = tf.name
            os.replace(tmp, _PERSIST_PATH)
        except Exception as e:
            log.debug("[api_monitor] persist failed: %s", e)

    def record_call(
        self,
        service: str,
        endpoint: str,
        status_code: int,
        response_time_ms: float,
        error: Optional[str] = None,
    ):
        """Record an API call with its result"""
        with self._state_lock:
            if service not in self.metrics:
                self.metrics[service] = ServiceMetrics()

            metrics = self.metrics[service]
            metrics.total_requests += 1
            metrics.total_response_time_ms += response_time_ms
            metrics.latency_samples.append(response_time_ms)
            metrics.endpoint_breakdown[endpoint]["count"] += 1
            metrics.endpoint_breakdown[endpoint]["latency_samples"].append(response_time_ms)

            if status_code == 429:
                metrics.rate_limited_count += 1
                metrics.endpoint_breakdown[endpoint]["rate_limited"] += 1
                log.warning("[%s] Rate limited on %s", service, endpoint)

            if 200 <= status_code < 300:
                metrics.successful_requests += 1
            else:
                metrics.failed_requests += 1
                metrics.endpoint_breakdown[endpoint]["errors"] += 1
                failure = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "endpoint": endpoint,
                    "status_code": status_code,
                    "response_time_ms": round(response_time_ms, 2),
                    "error": error,
                }
                metrics.recent_failures.append(failure)
                log.error("[%s] API failure on %s: HTTP %s", service, endpoint, status_code)

        # Mirror into Prometheus metrics (low-cardinality: service + outcome
        # only, never endpoint/symbol). Best-effort — must not break recording.
        try:
            from app.core import metrics as _m
            outcome = ("rate_limited" if status_code == 429
                       else "ok" if 200 <= status_code < 300 else "error")
            _m.inc("broker_api_requests_total", service=service, outcome=outcome)
            _m.inc("broker_api_latency_ms_sum", value=response_time_ms, service=service)
            if outcome == "error":
                _m.inc("broker_api_errors_total", service=service)
        except Exception:
            pass

        # Throttled persist (outside the per-call hot path's critical work).
        try:
            self._persist()
        except Exception:
            pass

    def get_metrics(self) -> dict:
        """Get current metrics for all services"""
        uptime_seconds = int(time.time() - self._start_time)

        def _ep_summary(stats: dict) -> dict:
            samples = list(stats.get("latency_samples") or [])
            if samples:
                s = sorted(samples)
                n = len(s)
                p50 = s[min(n - 1, int(round(0.5 * (n - 1))))]
                p95 = s[min(n - 1, int(round(0.95 * (n - 1))))]
                avg = sum(samples) / n
            else:
                p50 = p95 = avg = 0.0
            return {
                "count": stats.get("count", 0),
                "errors": stats.get("errors", 0),
                "rate_limited": stats.get("rate_limited", 0),
                "avg_ms": round(avg, 2),
                "p50_ms": round(p50, 2),
                "p95_ms": round(p95, 2),
            }

        with self._state_lock:
            return {
                "uptime_seconds": uptime_seconds,
                "services": {
                    service: {
                        "total_requests": m.total_requests,
                        "successful": m.successful_requests,
                        "failed": m.failed_requests,
                        "rate_limited": m.rate_limited_count,
                        "error_rate_pct": m.error_rate_pct,
                        "avg_response_time_ms": m.avg_response_time_ms,
                        "latency_ms": m.percentiles(),  # p50/p95/p99
                        "endpoint_breakdown": {
                            ep: _ep_summary(stats)
                            for ep, stats in m.endpoint_breakdown.items()
                        },
                        "recent_failures": list(m.recent_failures)[-10:],
                    }
                    for service, m in self.metrics.items()
                },
            }

    def reset(self):
        """Reset all metrics (useful for testing)"""
        with self._state_lock:
            for m in self.metrics.values():
                m.total_requests = 0
                m.successful_requests = 0
                m.failed_requests = 0
                m.rate_limited_count = 0
                m.total_response_time_ms = 0.0
                m.recent_failures.clear()
                m.endpoint_breakdown.clear()


# Global singleton
monitor = APIMonitor()


# Decorator for automatic monitoring

def monitored(service: str, endpoint: str):
    """Decorator to automatically monitor API calls"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()
            status_code = 200
            error = None
            try:
                result = await func(*args, **kwargs)
                if isinstance(result, tuple) and len(result) >= 2:
                    # Assume (data, status_code) return pattern
                    status_code = result[1] if isinstance(result[1], int) else 200
                return result
            except Exception as e:
                status_code = 500
                error = str(e)
                raise
            finally:
                elapsed_ms = (time.perf_counter() - start) * 1000
                monitor.record_call(service, endpoint, status_code, elapsed_ms, error)
        return wrapper
    return decorator
