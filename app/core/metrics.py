"""
metrics.py — dependency-free in-process metrics for the trading hot path.

No prometheus_client dependency: counters/gauges live in module dicts and render
to the Prometheus text exposition format on demand (served at GET /metrics). A
single threading.Lock guards mutation because the guardrail now runs in a worker
thread (asyncio.to_thread) while the event loop touches gauges — both can write.

Conventions
-----------
- counters: monotonic, suffix _total, increment via inc().
- gauges: point-in-time, set via set_gauge().
- labels kept LOW cardinality (reason category, side, broker) — never raw
  symbols or per-order ids, which would explode the series count.

Event-loop lag
--------------
sample_loop_lag() runs as a background task: it sleeps a fixed interval and
records how late it woke. Rising lag = the event loop is being blocked (the
classic symptom of synchronous DB work on the loop). This is the single most
useful signal for this app.
"""
import asyncio
import threading
import time

_lock = threading.Lock()
_counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
_gauges: dict[str, float] = {}

# Help text + type for rendering (optional; unknown names default to untyped).
_META: dict[str, tuple[str, str]] = {
    "alerts_received_total":        ("counter", "Alerts ingested from Discord"),
    "alert_dup_rejected_total":     ("counter", "Alerts rejected as duplicate (msg-id / content)"),
    "guardrail_pass_total":         ("counter", "Guardrail checks that allowed the order"),
    "guardrail_block_total":        ("counter", "Guardrail blocks by reason category"),
    "orders_placed_total":          ("counter", "Orders submitted to a broker"),
    "orders_rejected_total":        ("counter", "Orders rejected before/at placement"),
    "order_queue_fallback_total":   ("counter", "Serial-queue dispatch fell back to concurrent"),
    "halt_set_total":               ("counter", "Daily-loss halt engaged"),
    "order_queue_depth":            ("gauge",   "Current serialized-execution backlog"),
    "trading_halted":               ("gauge",   "1 if daily-loss halt active, else 0"),
    "open_positions":               ("gauge",   "Open/partial positions"),
    "event_loop_lag_ms":            ("gauge",   "Event-loop wake-up lag (ms)"),
    # ── business / P&L ───────────────────────────────────────────────────
    "pnl_realized_today_usd":       ("gauge",   "Realized P&L today (USD, ET day)"),
    "pnl_unrealized_usd":           ("gauge",   "Unrealized P&L on open positions (USD)"),
    "pnl_total_today_usd":          ("gauge",   "Realized + unrealized P&L today (USD)"),
    "trades_today_total":           ("gauge",   "BUY orders placed today (PENDING+FILLED)"),
    "positions_winning":            ("gauge",   "Open positions currently in profit"),
    "positions_losing":             ("gauge",   "Open positions currently at a loss"),
    # ── liveness / silent-death detection ────────────────────────────────
    "broker_connected":             ("gauge",   "1 if the active broker is connected (IBKR gateway up; REST=1), else 0"),
    "last_price_poll_age_seconds":  ("gauge",   "Seconds since the position monitor last fetched prices (0=fresh)"),
    "market_data_farm_ok":          ("gauge",   "IBKR market-data farm: 1 OK, 0 broken, -1 connecting. Absent = unknown (no IBKR session, or farm not yet reported)"),
    "market_data_farm_age_seconds": ("gauge",   "Seconds since IBKR last reported a market-data farm state change"),
    "lime_opra_ok":                 ("gauge",   "Lime option quotes: 1 OK, 0 = 403 (API token not activated for OPRA this session). Absent = not probed"),
    # ── broker REST health (from api_monitor.record_call) ────────────────
    "broker_api_requests_total":    ("counter", "Broker/API calls by service + outcome"),
    "broker_api_errors_total":      ("counter", "Broker/API calls that returned a non-2xx"),
    "broker_api_latency_ms_sum":    ("counter", "Cumulative broker/API latency ms (÷requests = avg)"),
    # ── execution quality ────────────────────────────────────────────────
    "order_fill_latency_ms_avg":    ("gauge",   "Mean submit→fill latency over fills in the last 15m (0=none)"),
    "order_fill_latency_ms_p95":    ("gauge",   "p95 submit→fill latency over fills in the last 15m (0=none)"),
    "order_fills_recent":           ("gauge",   "Orders filled in the last 15m"),
}


def inc(name: str, value: float = 1.0, **labels: str) -> None:
    """Increment a counter. Never raises — metrics must not break the hot path."""
    try:
        key = (name, tuple(sorted(labels.items())))
        with _lock:
            _counters[key] = _counters.get(key, 0.0) + value
    except Exception:
        pass


def set_gauge(name: str, value: float) -> None:
    try:
        with _lock:
            _gauges[name] = float(value)
    except Exception:
        pass


# Wall-clock of the position monitor's last successful price fetch. Read by the
# business sampler to publish last_price_poll_age_seconds (a stalled monitor =
# frozen P&L and no auto-exits, otherwise silent).
_last_price_poll_ts: float | None = None


def mark_price_poll() -> None:
    """Stamp 'prices were just fetched'. Called by the position monitor each poll."""
    global _last_price_poll_ts
    _last_price_poll_ts = time.time()


def reset() -> None:
    global _last_price_poll_ts
    with _lock:
        _counters.clear()
        _gauges.clear()
    _last_price_poll_ts = None


def snapshot() -> dict:
    """Plain-dict view for tests / JSON. Counters keyed 'name{labels}'."""
    with _lock:
        out: dict[str, float] = {}
        for (name, labels), v in _counters.items():
            lbl = ",".join(f'{k}="{val}"' for k, val in labels)
            out[f"{name}{{{lbl}}}" if lbl else name] = v
        for name, v in _gauges.items():
            out[name] = v
        return out


def render_prometheus() -> str:
    """Render the current metrics in Prometheus text exposition format."""
    with _lock:
        counters = dict(_counters)
        gauges = dict(_gauges)
    lines: list[str] = []
    seen: set[str] = set()

    def _meta(name: str, default_type: str):
        if name in seen:
            return
        seen.add(name)
        typ, helptext = _META.get(name, (default_type, name))
        lines.append(f"# HELP {name} {helptext}")
        lines.append(f"# TYPE {name} {typ}")

    for (name, labels), v in sorted(counters.items()):
        _meta(name, "counter")
        lbl = ",".join(f'{k}="{val}"' for k, val in labels)
        lines.append(f"{name}{{{lbl}}} {v}" if lbl else f"{name} {v}")
    for name, v in sorted(gauges.items()):
        _meta(name, "gauge")
        lines.append(f"{name} {v}")
    return "\n".join(lines) + "\n"


async def sample_loop_lag(interval: float = 1.0) -> None:
    """Background task: measure how late the loop wakes from a fixed sleep and
    publish it as event_loop_lag_ms. Run via asyncio.create_task at startup."""
    while True:
        t0 = time.perf_counter()
        await asyncio.sleep(interval)
        late_ms = (time.perf_counter() - t0 - interval) * 1000.0
        set_gauge("event_loop_lag_ms", max(0.0, late_ms))


def _collect_business() -> None:
    """Read the DB once and publish P&L / trading gauges. Synchronous — call via
    asyncio.to_thread so the queries don't sit on the event loop. Best-effort."""
    from datetime import datetime, timezone
    from app.core.db import SessionLocal
    from app.core.models import Position, Order, realized_pnl_since
    db = SessionLocal()
    try:
        today = datetime.now(timezone.utc).date()
        start = datetime.combine(today, datetime.min.time()).replace(tzinfo=timezone.utc)
        realized = realized_pnl_since(db, start)
        opens = db.query(Position).filter(
            Position.status.in_(["OPEN", "PARTIAL"]), Position.remaining > 0,
        ).all()
        unreal = 0.0
        win = lose = 0
        for p in opens:
            if p.avg_price and p.current_price:
                d = (p.current_price - p.avg_price) * (p.remaining or 0) * 100
                unreal += d
                if d > 0:
                    win += 1
                elif d < 0:
                    lose += 1
        trades = db.query(Order).filter(
            Order.placed_at >= start, Order.side == "BUY",
            Order.status.in_(["PENDING", "FILLED"]),
        ).count()
        set_gauge("pnl_realized_today_usd", round(realized, 2))
        set_gauge("pnl_unrealized_usd", round(unreal, 2))
        set_gauge("pnl_total_today_usd", round(realized + unreal, 2))
        set_gauge("open_positions", len(opens))
        set_gauge("positions_winning", win)
        set_gauge("positions_losing", lose)
        set_gauge("trades_today_total", trades)

        # ── Execution quality: mean submit→fill latency over recent fills ──
        # Broker-agnostic (reads Order rows every vendor writes). 0 when no
        # fills in the window so a stale high value can't false-alarm.
        from datetime import timedelta
        since_fill = datetime.now(timezone.utc) - timedelta(minutes=15)
        recent = db.query(Order).filter(
            Order.status == "FILLED", Order.filled_at != None,  # noqa: E711
            Order.filled_at >= since_fill, Order.placed_at != None,  # noqa: E711
        ).all()
        lats = []
        for o in recent:
            try:
                lats.append((o.filled_at - o.placed_at).total_seconds() * 1000.0)
            except Exception:
                pass
        lats = [x for x in lats if x >= 0]
        set_gauge("order_fills_recent", len(lats))
        set_gauge("order_fill_latency_ms_avg", round(sum(lats) / len(lats), 1) if lats else 0.0)
        if lats:
            _sorted = sorted(lats)
            p95 = _sorted[min(len(_sorted) - 1, int(len(_sorted) * 0.95))]
            set_gauge("order_fill_latency_ms_p95", round(p95, 1))
        else:
            set_gauge("order_fill_latency_ms_p95", 0.0)
    finally:
        db.close()

    # ── Liveness gauges (outside the DB session; best-effort) ──────────────
    if _last_price_poll_ts is not None:
        set_gauge("last_price_poll_age_seconds", round(time.time() - _last_price_poll_ts, 1))
    try:
        from app.execution.broker_router import is_connected as _broker_connected
        set_gauge("broker_connected", 1.0 if _broker_connected() else 0.0)
    except Exception:
        pass

    # Market-data farm (IBKR only). Deliberately leaves both gauges UNSET while
    # the state is unknown — an absent series reads as "no answer", whereas
    # publishing 0 would claim the farm is broken on a box that simply has no
    # IBKR session. Consumers must treat missing as unknown, not as bad.
    try:
        from app.execution.ibkr_sdk_bridge import market_data_status
        _md = market_data_status()
        _code = {"ok": 1.0, "broken": 0.0, "connecting": -1.0}.get(_md["state"])
        if _code is not None:
            set_gauge("market_data_farm_ok", _code)
            if _md["age_seconds"] is not None:
                set_gauge("market_data_farm_age_seconds", round(_md["age_seconds"], 1))
    except Exception:
        pass


async def sample_business_metrics(interval: float = 30.0) -> None:
    """Background task: publish P&L / trading gauges every `interval`s. Run via
    asyncio.create_task at startup. DB work is offloaded; errors are swallowed."""
    while True:
        try:
            await asyncio.to_thread(_collect_business)
        except Exception:
            pass
        await asyncio.sleep(interval)


def guardrail_reason_category(reason: str) -> str:
    """Bucket a guardrail block reason into a low-cardinality label.
    Reasons look like 'DUPLICATE: SPX...', 'MAX POSITIONS: ...', 'STALE_MARKS: ...'
    — take the token before the first ':' (or first few words) and normalize."""
    if not reason:
        return "unknown"
    head = reason.split(":", 1)[0].strip().upper()
    # Collapse whitespace to a single token so 'MAX POSITIONS' → 'MAX_POSITIONS'.
    return "_".join(head.split())[:40] or "unknown"
