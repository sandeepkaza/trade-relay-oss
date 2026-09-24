"""
Metrics module (app/core/metrics.py): counters, gauges, Prometheus rendering,
reason bucketing, loop-lag sampler, and guardrail instrumentation wiring.
"""
import asyncio

import app.core.metrics as m


def setup_function():
    m.reset()


def test_counter_inc_and_labels():
    m.inc("orders_placed_total", side="BUY")
    m.inc("orders_placed_total", side="BUY")
    m.inc("orders_placed_total", side="SELL")
    snap = m.snapshot()
    assert snap['orders_placed_total{side="BUY"}'] == 2
    assert snap['orders_placed_total{side="SELL"}'] == 1


def test_gauge_set_overwrites():
    m.set_gauge("order_queue_depth", 5)
    m.set_gauge("order_queue_depth", 2)
    assert m.snapshot()["order_queue_depth"] == 2


def test_prometheus_render_has_help_type_and_values():
    m.inc("guardrail_block_total", reason="STALE_MARKS")
    m.set_gauge("trading_halted", 1)
    text = m.render_prometheus()
    assert "# TYPE guardrail_block_total counter" in text
    assert 'guardrail_block_total{reason="STALE_MARKS"} 1' in text
    assert "# TYPE trading_halted gauge" in text
    assert "trading_halted 1" in text


def test_reason_category_buckets_low_cardinality():
    assert m.guardrail_reason_category("DUPLICATE: SPXW...x") == "DUPLICATE"
    assert m.guardrail_reason_category("MAX POSITIONS: have 5") == "MAX_POSITIONS"
    assert m.guardrail_reason_category("STALE_MARKS: 2 open ...") == "STALE_MARKS"
    assert m.guardrail_reason_category("") == "unknown"


def test_inc_never_raises_on_bad_input():
    # Must not throw — metrics can't break the hot path.
    m.inc("x", value=float("nan"))
    m.set_gauge("y", 1)


def test_business_metrics_pnl_and_counts():
    """_collect_business publishes correct realized/unrealized P&L + position counts."""
    import app.core.db as db
    from app.core.models import Position
    from datetime import datetime, timezone
    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()
    s = db.SessionLocal()
    try:
        # +$100 winner, -$50 loser → unrealized +$50; no SELL fills → realized 0.
        s.add(Position(osi_symbol="WIN", symbol="SPX", status="OPEN", total_contracts=2,
                       remaining=2, avg_price=2.0, current_price=2.5,
                       open_time=datetime.now(timezone.utc)))
        s.add(Position(osi_symbol="LOSE", symbol="SPX", status="OPEN", total_contracts=1,
                       remaining=1, avg_price=3.0, current_price=2.5,
                       open_time=datetime.now(timezone.utc)))
        s.commit()
    finally:
        s.close()
    m.reset()
    m._collect_business()
    snap = m.snapshot()
    assert snap["pnl_unrealized_usd"] == 50.0
    assert snap["pnl_realized_today_usd"] == 0.0
    assert snap["pnl_total_today_usd"] == 50.0
    assert snap["open_positions"] == 2
    assert snap["positions_winning"] == 1
    assert snap["positions_losing"] == 1


def test_loop_lag_sampler_publishes_gauge():
    async def run():
        t = asyncio.create_task(m.sample_loop_lag(interval=0.01))
        await asyncio.sleep(0.05)
        t.cancel()
        assert "event_loop_lag_ms" in m.snapshot()
    asyncio.run(run())


def test_guardrail_block_increments_metric(monkeypatch):
    """The check_guardrails wrapper records a block by reason category."""
    import app.core.db as db
    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()
    import app.risk.guardrails as g
    m.reset()
    # Force a deterministic, config-free block: blocked symbol.
    monkeypatch.setattr(g, "_BLOCKED_SYMBOLS", lambda: {"SPX"})
    allowed, reason = g.check_guardrails("BUY", "SPXW260101C06000000", None, 1,
                                         alert_author="x")
    assert allowed is False
    snap = m.snapshot()
    assert 'guardrail_block_total{reason="BLOCKED_SYMBOL"}' in snap, snap


def test_guardrail_pass_increments_metric(monkeypatch):
    import app.core.db as db
    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()
    import app.risk.guardrails as g
    m.reset()
    # Disable guardrails so the check passes deterministically (no market-hours
    # / reconcile dependence).
    monkeypatch.setattr(g, "_GUARDRAILS_ENABLED", lambda: False)
    allowed, _ = g.check_guardrails("BUY", "SPXW260101C06000000", None, 1)
    assert allowed is True
    assert m.snapshot().get("guardrail_pass_total") == 1
