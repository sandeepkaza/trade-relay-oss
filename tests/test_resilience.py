"""
Resilience / chaos / failover (review §9).

- Chaos: a broken state store must not crash the guardrail or open the gate.
- Failover: a persisted halt survives a simulated process restart (GH #10).
- Memory: in-memory dedup/cooldown dicts must stay bounded (no unbounded growth
  over a long-running session).
"""
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import app.core.db as db
import app.risk.guardrails as g


def _today_et():
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")


def _reset_halt():
    g._halt_loaded = False
    g._trading_halted = False
    g._halt_date = None


# ── Chaos ────────────────────────────────────────────────────────────────

def test_halt_load_failsafe_when_state_store_raises(monkeypatch):
    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()
    _reset_halt()

    import app.core.models as models
    def boom(*a, **k):
        raise RuntimeError("state store down")
    monkeypatch.setattr(models, "get_system_state", boom)

    # Must not raise, and must NOT halt (fail-safe = assume not halted, never
    # silently block — and never crash the check).
    g._ensure_halt_loaded()
    assert g._trading_halted is False


def test_persist_halt_failure_does_not_crash(monkeypatch):
    import app.core.models as models
    monkeypatch.setattr(models, "set_system_state",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("down")))
    # Should swallow and continue.
    g._persist_halt(_today_et())


# ── Failover ─────────────────────────────────────────────────────────────

def test_failover_restores_persisted_halt():
    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()
    _reset_halt()
    g._persist_halt(_today_et())     # halt engaged earlier today
    _reset_halt()                    # ← simulated restart: cold in-memory state
    g._ensure_halt_loaded()
    assert g._trading_halted is True
    assert g._halt_date == _today_et()


# ── Memory bounds ────────────────────────────────────────────────────────

def test_last_trade_time_prunes_stale_entries():
    # Seed an old entry (>24h) then record a fresh alert; the prune in
    # _record_alert must drop the stale one.
    g._last_trade_time.clear()
    g._last_trade_time["OLD260101C00000000"] = time.time() - 90000   # ~25h ago
    g._record_alert("SPXW260101C06000000", "BUY", content_hash="")
    assert "OLD260101C00000000" not in g._last_trade_time
    assert "SPXW260101C06000000" in g._last_trade_time


def test_content_hash_cache_prunes_stale():
    g._recent_content_hashes.clear()
    g._recent_content_hashes["stalehash"] = time.time() - 10000
    g._recent_content_hashes["freshhash"] = time.time()
    g._clean_stale_hashes(cutoff=time.time() - 120)   # 120s window
    assert "stalehash" not in g._recent_content_hashes
    assert "freshhash" in g._recent_content_hashes
