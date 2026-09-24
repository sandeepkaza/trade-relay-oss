"""
test_e2e_full.py - Comprehensive end-to-end test suite.

Tests the full pipeline: alert parsing (regex + AI), order execution,
position management, auto-exits (PT1/PT2/PT3, fixed SL, trailing SL),
guardrails, and the position monitor.

Usage:
    python test_e2e_full.py
    python test_e2e_full.py -v              (verbose)
    python test_e2e_full.py TestAIParser    (run one class)
"""

import os
import sys
import unittest
import asyncio
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch, AsyncMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Use a throwaway test database and force dry-run mode ─────────────────────
TEST_DB_PATH = os.path.abspath("test_e2e_full.db")
os.environ["TRADER_DATABASE_URL"] = f"sqlite:///{TEST_DB_PATH.replace(chr(92), '/')}"

import app.core.db as db
import app as app_module
import app.monitors.position_monitor as position_monitor
import app.execution.public_sdk_bridge as public_sdk_bridge

# Shim: tests written before the cfg refactor set module-level constants like
# `position_monitor.TRAILING_SL_ENABLED = True`. Production now reads cfg via
# `_pcfg_bool(pos, key, fallback)`. Wrap those readers so they consult the
# module attribute first when present, otherwise fall through to cfg.
_orig_pcfg_bool = position_monitor._pcfg_bool
_orig_pcfg_float = position_monitor._pcfg_float

def _patched_pcfg_bool(pos, key, fallback):
    attr = key.upper()
    val = getattr(position_monitor, attr, None)
    if val is not None and not callable(val):
        return bool(val)
    return _orig_pcfg_bool(pos, key, fallback)

def _patched_pcfg_float(pos, key, fallback):
    attr = key.upper()
    val = getattr(position_monitor, attr, None)
    if val is not None and not callable(val):
        return float(val)
    return _orig_pcfg_float(pos, key, fallback)

position_monitor._pcfg_bool = _patched_pcfg_bool
position_monitor._pcfg_float = _patched_pcfg_float

# Pre-seed module attrs with the values these tests were originally written
# against. config.ini may have aggressive production overrides (e.g. pt1_pct=15
# instead of 30) that would break tests asserting on the documented defaults.
position_monitor.TRAILING_SL_ENABLED = False
position_monitor.TRAILING_SL_PCT = 30.0
position_monitor.PT1_ENABLED = True
position_monitor.PT1_PCT = 30.0
position_monitor.PT1_SELL = 0.50
position_monitor.PT2_ENABLED = True
position_monitor.PT2_PCT = 60.0
position_monitor.PT2_SELL = 0.50
position_monitor.PT3_ENABLED = True
position_monitor.PT3_PCT = 100.0
position_monitor.PT3_SELL = 1.0
position_monitor.SL_ENABLED = True
position_monitor.SL_PCT = -50.0
# Disable per-position profile overrides (spx_index / breakeven lock) for
# default tests; individual tests override as needed.
position_monitor.PTS_DISABLED_DEFAULT = False
position_monitor.BREAKEVEN_LOCK_ENABLED = False
position_monitor.TRAILING_SL_ARM_PCT = 0.0
position_monitor.TPT_ENABLED = False
# Allow SL to fire outside market hours (test environment is not constrained
# to RTH). Production cfg defaults sl_market_hours_only=True.
# These are read via direct function calls, not _pcfg_bool, so they need to
# be overridden as functions.
position_monitor._SL_MARKET_HOURS_ONLY = lambda: False
position_monitor._WIDE_SPREAD_PCT = lambda: 100.0  # disable wide-spread defer
# SL_PARTIAL stages SL into 2 ticks (sells half, then rest). Tests written
# before this feature expect full SL in one tick — disable staging.
position_monitor._SL_STAGE_ENABLED = lambda: False

# Bypass reconcile gate (mirrors pytest _bypass_reconcile_gate fixture).
# BUY guardrail blocks until reconciler runs; tests don't spin up reconciler.
import app.monitors.reconciler as reconciler
reconciler._reconciled_once = True

from app.core.models import Alert, Order, Position
import app.ingest.parser as _parser_mod
import datetime as _dt

# Freeze parser.date so no-year alerts ('4/14', '4/17') resolve to a known
# future date instead of being rejected as 'already expired'. Mirrors the
# pytest _freeze_date fixture in conftest.py — needed here because this file
# runs under unittest __main__, which bypasses conftest.
_FIXED_TODAY = _dt.date(2026, 4, 1)
class _FrozenDate(_dt.date):
    @classmethod
    def today(cls):
        return _FIXED_TODAY
_parser_mod.date = _FrozenDate

from app.ingest.parser import parse_alert, TradeAlert
import app.execution.public_executor as pe_module
from app.execution.public_executor import PublicExecutor

# Force dry-run so tests never hit the real broker API.
# _DRY_RUN is a function in public_executor that reads cfg at call-time, so
# patching a module attribute is not enough — replace the callable itself.
pe_module._DRY_RUN = lambda: True

# Pin size_tag → contracts mapping to documented defaults. Production
# config.ini overrides these (e.g. size_small=4 for current SPX strategy)
# but tests assert against the original convention (SMALL=1, MEDIUM=2, ...).
pe_module._size_contracts = lambda: {
    "XS": 1, "SMALL": 1, "MEDIUM": 2, "LARGE": 3, "FULL": 4, "XL": 5, "": 1,
}

# Disable market hours guard so tests can run anytime
import app.risk.guardrails as guardrails
guardrails.MARKET_HOURS_ONLY = False


def _reset_guardrails():
    """Clear in-memory guardrails state between tests (dedup cache, cooldowns).
    Also clears position_monitor's recent-auto-exit cache so PT/SL fires from
    one test do not block SELL alerts in the next test."""
    guardrails._recent_alerts.clear()
    guardrails._last_trade_time.clear()
    guardrails._trading_halted = False
    guardrails._halt_date = None
    position_monitor._recent_auto_exits.clear()

sys.stdout.reconfigure(encoding="utf-8", errors="replace")


# ── Helpers ──────────────────────────────────────────────────────────────────

class DummyManager:
    """Captures WebSocket broadcasts for assertions."""
    def __init__(self):
        self.events = []

    async def broadcast(self, data):
        self.events.append(data)


def build_alert_row(parsed_alert, raw_text: str, status: str = "PENDING") -> int:
    session = db.SessionLocal()
    try:
        row = Alert(
            author="tester",
            action=parsed_alert.action,
            raw_text=raw_text,
            osi_symbol=parsed_alert.osi_symbol,
            symbol=parsed_alert.symbol,
            expiry=str(parsed_alert.expiry) if parsed_alert.expiry else None,
            strike=parsed_alert.strike,
            option_type=parsed_alert.option_type,
            alert_price=float(parsed_alert.price) if parsed_alert.price is not None else None,
            size_tag=parsed_alert.size_tag,
            fraction=float(parsed_alert.fraction),
            status=status,
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        return row.id
    finally:
        session.close()


def seed_position(**overrides) -> int:
    # Default to AAPL (non-index) so spx_index profile (which disables PTs and
    # tightens SL) does NOT apply. Tests that need SPXW behavior pass it
    # explicitly via overrides.
    defaults = {
        "osi_symbol": "AAPL270417C00220000",
        "symbol": "AAPL",
        "expiry": "2027-04-17",
        "strike": 220,
        "option_type": "C",
        "total_contracts": 2,
        "remaining": 2,
        "avg_price": 1.00,
        "current_price": 1.00,
        "highest_price": 1.00,
        "status": "OPEN",
    }
    defaults.update(overrides)
    session = db.SessionLocal()
    try:
        pos = Position(**defaults)
        session.add(pos)
        session.commit()
        session.refresh(pos)
        return pos.id
    finally:
        session.close()


def get_position(osi_symbol: str = None, position_id: int = None) -> Position:
    session = db.SessionLocal()
    try:
        session.expire_all()
        if position_id:
            pos = session.query(Position).filter(Position.id == position_id).first()
        elif osi_symbol:
            pos = session.query(Position).filter(Position.osi_symbol == osi_symbol).first()
        else:
            pos = session.query(Position).first()
        if pos:
            session.refresh(pos)
        return pos
    finally:
        session.close()


# ══════════════════════════════════════════════════════════════════════════════
# TEST 1: REGEX PARSER
# ══════════════════════════════════════════════════════════════════════════════

class TestRegexParser(unittest.TestCase):
    """Verify the local regex parser handles all known alert formats."""

    def assertParsed(self, raw, **expected):
        alert = parse_alert(raw)
        self.assertIsNotNone(alert, f"Failed to parse: {raw}")
        for key, val in expected.items():
            actual = getattr(alert, key)
            if isinstance(val, Decimal):
                self.assertAlmostEqual(float(actual), float(val), places=2, msg=f"{raw} -> {key}")
            else:
                self.assertEqual(actual, val, f"{raw} -> {key}")

    # ── BUY formats ──────────────────────────────────────────────────────
    def test_bought_standard(self):
        self.assertParsed("BOUGHT SPX 4/14 6970C $1.75 [SMALL]",
                          action="BUY", symbol="SPX", strike=6970,
                          option_type="C", price=Decimal("1.75"), size_tag="SMALL")

    def test_bto(self):
        self.assertParsed("BTO SPX 4/14 6970C @ 1.75",
                          action="BUY", symbol="SPX", strike=6970, price=Decimal("1.75"))

    def test_bot(self):
        self.assertParsed("BOT AAPL 5/16 200C $3.50 [MEDIUM]",
                          action="BUY", symbol="AAPL", strike=200, size_tag="MEDIUM")

    def test_opening(self):
        self.assertParsed("Opening QQQ 4/16 420P $3.20 [MEDIUM]",
                          action="BUY", symbol="QQQ", strike=420, option_type="P")

    def test_buy_with_year(self):
        self.assertParsed("BOUGHT SPX 4/14/25 6970C $1.75",
                          action="BUY", symbol="SPX", strike=6970)

    def test_buy_with_4digit_year(self):
        self.assertParsed("BOUGHT SPX 4/14/2025 6970C $1.75",
                          action="BUY", symbol="SPX", strike=6970)

    def test_emoji_prefix(self):
        self.assertParsed("✅ BTO SPX 4/14 6970C @1.75",
                          action="BUY", symbol="SPX", price=Decimal("1.75"))

    # ── SELL formats ─────────────────────────────────────────────────────
    def test_sold_partial_half(self):
        self.assertParsed("SOLD SPX 4/14 6970C $2.70 1/2",
                          action="SELL", fraction=Decimal("0.5"))

    def test_sold_partial_third(self):
        self.assertParsed("STC NVDA 5/2 900C $5.50 1/3",
                          action="SELL", symbol="NVDA", strike=900)

    def test_sold_full(self):
        self.assertParsed("SOLD SPX 4/14 6970C $2.70 ALL",
                          action="SELL", fraction=Decimal("1"))

    # ── Close-all signals ────────────────────────────────────────────────
    def test_all_out_with_symbol(self):
        self.assertParsed("ALL OUT SPX 6970C $2.70",
                          action="SELL", symbol="SPX", close_all=True)

    def test_all_out_no_symbol(self):
        self.assertParsed("ALL OUT",
                          action="SELL", symbol="", close_all=True)

    def test_close_all(self):
        self.assertParsed("CLOSE ALL",
                          action="SELL", close_all=True)

    # ── Non-trade messages ───────────────────────────────────────────────
    def test_non_trade_returns_none(self):
        self.assertIsNone(parse_alert("SPX is pumping today!"))
        self.assertIsNone(parse_alert("PT1 hit! Great call"))
        self.assertIsNone(parse_alert(""))
        self.assertIsNone(parse_alert("lol"))
        self.assertIsNone(parse_alert("Good morning everyone"))

    # ── OSI symbol construction ──────────────────────────────────────────
    def test_osi_symbol(self):
        alert = parse_alert("BOUGHT SPX 4/14/2025 6970C $1.75")
        self.assertEqual(alert.osi_symbol, "SPX250414C06970000")

    # ── Alternative formats ──────────────────────────────────────────────
    def test_reversed_format(self):
        """SPY 678P 4/14 $3.15 — strike before date."""
        self.assertParsed("BOUGHT SPY 678P 4/14 $3.15",
                          action="BUY", symbol="SPY", strike=678, option_type="P")

    def test_cashtag_symbol(self):
        self.assertParsed("BOUGHT $AAPL 4/17 220C $0.50",
                          action="BUY", symbol="AAPL", strike=220)


# ══════════════════════════════════════════════════════════════════════════════
# TEST 2: AI PARSER
# ══════════════════════════════════════════════════════════════════════════════

class TestAIParser(unittest.IsolatedAsyncioTestCase):
    """Test the Claude Haiku AI fallback parser with real API calls."""

    @classmethod
    def setUpClass(cls):
        # ai_parser switched from Anthropic to Vertex AI / Gemini. Skip the
        # whole class if google-genai isn't installed or Vertex creds aren't
        # configured — these tests need a live LLM, not mockable.
        try:
            from app.ingest.ai_parser import _get_client
        except ImportError:
            raise unittest.SkipTest("ai_parser module not importable")
        if _get_client() is None:
            raise unittest.SkipTest("Vertex AI / google-genai client unavailable (no creds or SDK)")

    async def test_standard_buy(self):
        from app.ingest.ai_parser import ai_parse_alert
        alert = await ai_parse_alert("BOUGHT SPX 4/14 6970C $1.75 [SMALL]")
        self.assertIsNotNone(alert, "AI failed to parse standard BUY")
        self.assertEqual(alert.action, "BUY")
        self.assertEqual(alert.symbol, "SPX")
        self.assertEqual(alert.strike, 6970)
        self.assertEqual(alert.option_type, "C")

    async def test_unusual_buy_phrasing(self):
        """AI should handle slang/unusual phrasing that regex misses."""
        from app.ingest.ai_parser import ai_parse_alert
        alert = await ai_parse_alert("grabbed some SPX 5600 puts expiring 4/18 for about 3.50, small size play")
        self.assertIsNotNone(alert, "AI failed to parse unusual BUY phrasing")
        self.assertEqual(alert.action, "BUY")
        self.assertEqual(alert.symbol, "SPX")
        self.assertEqual(alert.strike, 5600)
        self.assertEqual(alert.option_type, "P")

    async def test_unusual_sell_phrasing(self):
        from app.ingest.ai_parser import ai_parse_alert
        alert = await ai_parse_alert("trimming half my SPX 5700 calls from this morning, taking profits at 2.80")
        self.assertIsNotNone(alert, "AI failed to parse unusual SELL phrasing")
        self.assertEqual(alert.action, "SELL")
        self.assertEqual(alert.symbol, "SPX")
        self.assertEqual(alert.strike, 5700)
        self.assertEqual(alert.option_type, "C")

    async def test_complex_embed_style(self):
        """Simulate a bot embed with complex formatting."""
        from app.ingest.ai_parser import ai_parse_alert
        text = "ALERT | SARANG | Options Play BTO TSLA 250C 4/25 Entry: $4.20 Risk: Medium"
        alert = await ai_parse_alert(text)
        self.assertIsNotNone(alert, "AI failed to parse embed-style alert")
        self.assertEqual(alert.action, "BUY")
        self.assertEqual(alert.symbol, "TSLA")
        self.assertEqual(alert.strike, 250)

    async def test_non_trade_message(self):
        from app.ingest.ai_parser import ai_parse_alert
        alert = await ai_parse_alert("Great day in the market! SPX up 2% today")
        self.assertIsNone(alert, "AI should return None for non-trade messages")

    async def test_all_out_signal(self):
        from app.ingest.ai_parser import ai_parse_alert
        alert = await ai_parse_alert("closing everything, all out of all positions")
        self.assertIsNotNone(alert, "AI failed to parse all-out signal")
        self.assertEqual(alert.action, "SELL")
        self.assertTrue(alert.close_all)

    async def test_partial_sell_fraction(self):
        from app.ingest.ai_parser import ai_parse_alert
        alert = await ai_parse_alert("selling half of my AAPL 4/25 200C at $5.20")
        self.assertIsNotNone(alert, "AI failed to parse partial sell")
        self.assertEqual(alert.action, "SELL")
        self.assertAlmostEqual(float(alert.fraction), 0.5, places=1)


# ══════════════════════════════════════════════════════════════════════════════
# TEST 3: PARSER FALLBACK CHAIN (regex → AI)
# ══════════════════════════════════════════════════════════════════════════════

class TestParserFallbackChain(unittest.IsolatedAsyncioTestCase):
    """Test that the regex → AI fallback logic works correctly."""

    async def test_regex_handles_standard_alert(self):
        """Standard alerts should be handled by regex, never hitting AI."""
        from app.ingest.ai_parser import ai_parse_alert
        raw = "BOUGHT SPX 4/14 6970C $1.75 [SMALL]"

        # Regex should work
        alert = parse_alert(raw)
        self.assertIsNotNone(alert)
        self.assertEqual(alert.action, "BUY")

    async def test_ai_catches_what_regex_misses(self):
        """Messages regex can't parse should fall through to AI."""
        from app.ingest.ai_parser import ai_parse_alert, _get_client
        if _get_client() is None:
            self.skipTest("Vertex AI / google-genai client unavailable (no creds or SDK)")
        raw = "picked up some NVDA 150 calls expiring next Friday around 2 bucks, small"

        # Regex should fail
        regex_result = parse_alert(raw)
        self.assertIsNone(regex_result, "Regex should NOT parse this unusual format")

        # AI should succeed
        ai_result = await ai_parse_alert(raw)
        self.assertIsNotNone(ai_result, "AI should parse what regex missed")
        self.assertEqual(ai_result.action, "BUY")
        self.assertEqual(ai_result.symbol, "NVDA")


# ══════════════════════════════════════════════════════════════════════════════
# TEST 4: BUY → POSITION CREATION
# ══════════════════════════════════════════════════════════════════════════════

class TestBuyExecution(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()
        _reset_guardrails()

    async def test_buy_creates_position_with_highest_price(self):
        """BUY fill should create a position with highest_price initialized."""
        manager = DummyManager()
        executor = PublicExecutor(manager)

        raw = "BOUGHT SPX 4/14/2027 6970C $1.75 [SMALL]"
        alert = parse_alert(raw)
        alert_id = build_alert_row(alert, raw)

        await executor.execute(alert, alert_id)

        pos = get_position(osi_symbol=alert.osi_symbol)
        self.assertIsNotNone(pos)
        self.assertEqual(pos.status, "OPEN")
        self.assertEqual(pos.remaining, 1)  # SMALL = 1 contract
        self.assertAlmostEqual(pos.avg_price, 1.75, places=2)
        self.assertAlmostEqual(pos.highest_price, 1.75, places=2)

    async def test_buy_into_existing_position(self):
        """Second BUY on same OSI should add to the position and update highest_price."""
        manager = DummyManager()
        executor = PublicExecutor(manager)

        raw1 = "BOUGHT SPX 4/14/2027 6970C $1.50 [SMALL]"
        a1 = parse_alert(raw1)
        a1_id = build_alert_row(a1, raw1)
        await executor.execute(a1, a1_id)

        # Clear dedup/cooldown so second BUY on same OSI isn't blocked
        _reset_guardrails()

        raw2 = "BOUGHT SPX 4/14/2027 6970C $2.00 [SMALL]"
        a2 = parse_alert(raw2)
        a2_id = build_alert_row(a2, raw2)
        await executor.execute(a2, a2_id)

        pos = get_position(osi_symbol=a1.osi_symbol)
        self.assertEqual(pos.remaining, 2)
        self.assertAlmostEqual(pos.avg_price, 1.75, places=2)
        self.assertAlmostEqual(pos.highest_price, 2.00, places=2)


# ══════════════════════════════════════════════════════════════════════════════
# TEST 5: SELL FLOW
# ══════════════════════════════════════════════════════════════════════════════

class TestSellExecution(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()
        _reset_guardrails()

    async def test_partial_sell(self):
        seed_position(total_contracts=4, remaining=4)
        manager = DummyManager()
        executor = PublicExecutor(manager)

        raw = "SOLD AAPL 4/17/2027 220C $2.70 1/2"
        alert = parse_alert(raw)
        alert_id = build_alert_row(alert, raw)
        await executor.execute(alert, alert_id)

        pos = get_position()
        self.assertEqual(pos.remaining, 2)
        self.assertEqual(pos.status, "PARTIAL")

    async def test_full_sell(self):
        seed_position(total_contracts=2, remaining=2)
        manager = DummyManager()
        executor = PublicExecutor(manager)

        raw = "SOLD AAPL 4/17/2027 220C $2.70 ALL"
        alert = parse_alert(raw)
        alert_id = build_alert_row(alert, raw)
        await executor.execute(alert, alert_id)

        pos = get_position()
        self.assertEqual(pos.remaining, 0)
        self.assertEqual(pos.status, "CLOSED")

    async def test_all_out_closes_all_positions(self):
        seed_position(osi_symbol="SPXW270414C06970000", symbol="SPXW")
        seed_position(osi_symbol="QQQ270416P00420000", symbol="QQQ",
                      expiry="2027-04-16", strike=420, option_type="P")

        manager = DummyManager()
        executor = PublicExecutor(manager)

        raw = "ALL OUT"
        alert = parse_alert(raw)
        alert_id = build_alert_row(alert, raw)
        await executor.execute(alert, alert_id)

        session = db.SessionLocal()
        try:
            open_count = session.query(Position).filter(
                Position.status.in_(["OPEN", "PARTIAL"])).count()
            self.assertEqual(open_count, 0)
        finally:
            session.close()


# ══════════════════════════════════════════════════════════════════════════════
# TEST 6: POSITION MONITOR — PROFIT TARGETS
# ══════════════════════════════════════════════════════════════════════════════

class TestProfitTargets(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()
        _reset_guardrails()

    async def _run_monitor_with_price(self, price: float) -> DummyManager:
        async def fake_prices(symbols):
            return {s: price for s in symbols}
        original = public_sdk_bridge.fetch_prices
        public_sdk_bridge.fetch_prices = fake_prices
        manager = DummyManager()
        executor = PublicExecutor(manager)
        try:
            await position_monitor._check_all_positions(manager, executor)
        finally:
            public_sdk_bridge.fetch_prices = original
        return manager

    async def test_pt1_sells_half(self):
        """PT1 at +30%: sell 50% of remaining (1 of 2)."""
        seed_position(total_contracts=2, remaining=2, avg_price=1.00, current_price=1.00)
        await self._run_monitor_with_price(1.35)  # +35%

        pos = get_position()
        self.assertTrue(pos.pt1_triggered)
        self.assertEqual(pos.remaining, 1)
        self.assertEqual(pos.status, "PARTIAL")

    async def test_pt2_after_pt1(self):
        """PT2 at +60%: sell 50% of remaining after PT1."""
        seed_position(total_contracts=4, remaining=2, avg_price=1.00,
                      current_price=1.60, pt1_triggered=True, highest_price=1.60)
        await self._run_monitor_with_price(1.65)  # +65%

        pos = get_position()
        self.assertTrue(pos.pt2_triggered)
        self.assertEqual(pos.remaining, 1)

    async def test_pt3_sells_all(self):
        """PT3 at +100%: sell all remaining."""
        seed_position(total_contracts=4, remaining=1, avg_price=1.00,
                      current_price=2.00, pt1_triggered=True, pt2_triggered=True,
                      highest_price=2.00)
        await self._run_monitor_with_price(2.05)  # +105%

        pos = get_position()
        self.assertTrue(pos.pt3_triggered)
        self.assertEqual(pos.remaining, 0)
        self.assertEqual(pos.status, "CLOSED")

    async def test_pt1_does_not_retrigger(self):
        """Once PT1 fires, it should not fire again on the next tick."""
        seed_position(total_contracts=2, remaining=1, avg_price=1.00,
                      current_price=1.40, pt1_triggered=True, highest_price=1.40)
        await self._run_monitor_with_price(1.45)

        pos = get_position()
        self.assertEqual(pos.remaining, 1)  # No additional sell


# ══════════════════════════════════════════════════════════════════════════════
# TEST 7: FIXED STOP LOSS
# ══════════════════════════════════════════════════════════════════════════════

class TestFixedStopLoss(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()
        _reset_guardrails()

    async def _run_monitor_with_price(self, price: float):
        async def fake_prices(symbols):
            return {s: price for s in symbols}
        original = public_sdk_bridge.fetch_prices
        public_sdk_bridge.fetch_prices = fake_prices
        manager = DummyManager()
        executor = PublicExecutor(manager)
        try:
            await position_monitor._check_all_positions(manager, executor)
        finally:
            public_sdk_bridge.fetch_prices = original

    async def test_fixed_sl_triggers_at_minus_50(self):
        """Fixed SL: sell all when price drops 50% (before PT1)."""
        seed_position(total_contracts=2, remaining=2, avg_price=1.00, current_price=1.00)
        await self._run_monitor_with_price(0.48)  # -52%

        pos = get_position()
        self.assertTrue(pos.sl_triggered)
        self.assertEqual(pos.remaining, 0)
        self.assertEqual(pos.status, "CLOSED")

    async def test_fixed_sl_does_not_trigger_above_threshold(self):
        """Fixed SL should NOT trigger at -40% (threshold is -50%)."""
        seed_position(total_contracts=2, remaining=2, avg_price=1.00, current_price=1.00)
        await self._run_monitor_with_price(0.62)  # -38%

        pos = get_position()
        self.assertFalse(pos.sl_triggered)
        self.assertEqual(pos.remaining, 2)


# ══════════════════════════════════════════════════════════════════════════════
# TEST 8: TRAILING STOP LOSS
# ══════════════════════════════════════════════════════════════════════════════

class TestTrailingStopLoss(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()
        _reset_guardrails()
        # Save originals
        self._orig_trailing_enabled = position_monitor.TRAILING_SL_ENABLED
        self._orig_trailing_pct = position_monitor.TRAILING_SL_PCT
        self._orig_fetch = public_sdk_bridge.fetch_prices

    def tearDown(self):
        position_monitor.TRAILING_SL_ENABLED = self._orig_trailing_enabled
        position_monitor.TRAILING_SL_PCT = self._orig_trailing_pct
        public_sdk_bridge.fetch_prices = self._orig_fetch

    async def _run_monitor_with_price(self, price: float):
        async def fake_prices(symbols):
            return {s: price for s in symbols}
        public_sdk_bridge.fetch_prices = fake_prices
        manager = DummyManager()
        executor = PublicExecutor(manager)
        await position_monitor._check_all_positions(manager, executor)
        return manager

    async def test_trailing_sl_triggers_after_pt1(self):
        """After PT1, trailing SL should fire when price drops 30% from peak."""
        position_monitor.TRAILING_SL_ENABLED = True
        position_monitor.TRAILING_SL_PCT = 30.0

        # Position: entered at $1.00, PT1 already triggered, peaked at $1.80
        seed_position(total_contracts=2, remaining=1, avg_price=1.00,
                      current_price=1.80, highest_price=1.80, pt1_triggered=True)

        # Price drops to $1.20 → 33% below $1.80 peak → trailing SL fires
        await self._run_monitor_with_price(1.20)

        pos = get_position()
        self.assertTrue(pos.sl_triggered)
        self.assertEqual(pos.remaining, 0)
        self.assertEqual(pos.status, "CLOSED")

    async def test_trailing_sl_does_not_fire_above_floor(self):
        """Trailing SL should NOT fire if price is above the trailing floor."""
        position_monitor.TRAILING_SL_ENABLED = True
        position_monitor.TRAILING_SL_PCT = 30.0

        # Peak $1.80, trailing floor = $1.80 * 0.70 = $1.26
        seed_position(total_contracts=2, remaining=1, avg_price=1.00,
                      current_price=1.80, highest_price=1.80, pt1_triggered=True)

        # Price dips to $1.30 — still above $1.26 floor
        await self._run_monitor_with_price(1.30)

        pos = get_position()
        self.assertFalse(pos.sl_triggered)
        self.assertEqual(pos.remaining, 1)

    async def test_trailing_sl_ratchets_highest_price(self):
        """highest_price should only go up, never down."""
        position_monitor.TRAILING_SL_ENABLED = True
        position_monitor.TRAILING_SL_PCT = 30.0

        # Mark PT2/PT3 already-triggered so the +100% ratchet move below
        # doesn't fire those exits and close the position before we observe
        # highest_price tracking. Test is about trailing-SL ratchet, not PTs.
        seed_position(total_contracts=2, remaining=1, avg_price=1.00,
                      current_price=1.50, highest_price=1.50,
                      pt1_triggered=True, pt2_triggered=True, pt3_triggered=True)

        # Price goes up to $2.00
        await self._run_monitor_with_price(2.00)
        pos = get_position()
        self.assertAlmostEqual(pos.highest_price, 2.00, places=2)

        # Price dips to $1.80 — highest should stay at $2.00
        await self._run_monitor_with_price(1.80)
        pos = get_position()
        self.assertAlmostEqual(pos.highest_price, 2.00, places=2)
        self.assertFalse(pos.sl_triggered)  # Floor is $1.40, price $1.80 > $1.40

    async def test_trailing_sl_disabled_uses_fixed_sl(self):
        """When trailing SL is disabled, fixed SL should work even after PT1."""
        position_monitor.TRAILING_SL_ENABLED = False

        seed_position(total_contracts=2, remaining=1, avg_price=1.00,
                      current_price=1.40, highest_price=1.40, pt1_triggered=True)

        # Price crashes to $0.45 (-55%) — fixed SL at -50% should fire
        await self._run_monitor_with_price(0.45)

        pos = get_position()
        self.assertTrue(pos.sl_triggered)
        self.assertEqual(pos.remaining, 0)

    async def test_fixed_sl_still_works_before_pt1_with_trailing_enabled(self):
        """Even with trailing enabled, fixed SL protects before PT1."""
        position_monitor.TRAILING_SL_ENABLED = True
        position_monitor.TRAILING_SL_PCT = 30.0

        seed_position(total_contracts=2, remaining=2, avg_price=1.00,
                      current_price=1.00, highest_price=1.00, pt1_triggered=False)

        # Price drops to $0.45 — fixed SL at -50% should fire
        await self._run_monitor_with_price(0.45)

        pos = get_position()
        self.assertTrue(pos.sl_triggered)
        self.assertEqual(pos.remaining, 0)

    async def test_trailing_sl_full_lifecycle(self):
        """Full scenario: buy → PT1 → price rises → dips → trailing SL fires."""
        position_monitor.TRAILING_SL_ENABLED = True
        position_monitor.TRAILING_SL_PCT = 30.0

        # Entry at $1.00, 4 contracts (enough to survive PT1+PT2 and still have remaining)
        seed_position(total_contracts=4, remaining=4, avg_price=1.00,
                      current_price=1.00, highest_price=1.00)

        # Tick 1: price to $1.35 → PT1 fires (sell 50% = 2 of 4)
        await self._run_monitor_with_price(1.35)
        pos = get_position()
        self.assertTrue(pos.pt1_triggered)
        self.assertEqual(pos.remaining, 2)

        _reset_guardrails()

        # Tick 2: price to $1.65 → PT2 fires (sell 50% of remaining = 1 of 2)
        await self._run_monitor_with_price(1.65)
        pos = get_position()
        self.assertTrue(pos.pt2_triggered)
        self.assertEqual(pos.remaining, 1)

        # Tick 3: price rises to $1.80 → no action, highest_price updates
        await self._run_monitor_with_price(1.80)
        pos = get_position()
        self.assertAlmostEqual(pos.highest_price, 1.80, places=2)
        self.assertEqual(pos.remaining, 1)

        # Tick 4: price dips to $1.50 → still above floor ($1.80 * 0.70 = $1.26)
        await self._run_monitor_with_price(1.50)
        pos = get_position()
        self.assertEqual(pos.remaining, 1)
        self.assertFalse(pos.sl_triggered)

        _reset_guardrails()

        # Tick 5: price crashes to $1.20 → below $1.26 floor → TRAILING SL fires
        await self._run_monitor_with_price(1.20)
        pos = get_position()
        self.assertTrue(pos.sl_triggered)
        self.assertEqual(pos.remaining, 0)
        self.assertEqual(pos.status, "CLOSED")


# ══════════════════════════════════════════════════════════════════════════════
# TEST 9: HIGHEST PRICE TRACKING
# ══════════════════════════════════════════════════════════════════════════════

class TestHighestPriceTracking(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()
        _reset_guardrails()

    async def test_highest_price_initializes_from_avg_if_null(self):
        """If highest_price is None (legacy position), it should init from avg_price."""
        session = db.SessionLocal()
        try:
            pos = Position(
                osi_symbol="SPX270414C06970000", symbol="SPX", expiry="2027-04-14",
                strike=6970, option_type="C", total_contracts=2, remaining=2,
                avg_price=1.00, current_price=1.00, highest_price=None, status="OPEN",
            )
            session.add(pos)
            session.commit()
        finally:
            session.close()

        async def fake_prices(symbols):
            return {s: 1.10 for s in symbols}
        original = public_sdk_bridge.fetch_prices
        public_sdk_bridge.fetch_prices = fake_prices
        manager = DummyManager()
        executor = PublicExecutor(manager)
        try:
            await position_monitor._check_all_positions(manager, executor)
        finally:
            public_sdk_bridge.fetch_prices = original

        pos = get_position()
        # max(None or 1.00, 1.10) = 1.10
        self.assertAlmostEqual(pos.highest_price, 1.10, places=2)


# ══════════════════════════════════════════════════════════════════════════════
# TEST 10: MANUAL EXIT
# ══════════════════════════════════════════════════════════════════════════════

class TestManualExit(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()
        _reset_guardrails()

    async def test_manual_exit_all(self):
        seed_position(osi_symbol="SPXW270414C06970000", symbol="SPXW")
        seed_position(osi_symbol="QQQ270416P00420000", symbol="QQQ",
                      expiry="2027-04-16", strike=420, option_type="P")
        app_module.manager = DummyManager()

        result = await app_module.manual_exit_all()

        self.assertEqual(result["closed"], 2)
        session = db.SessionLocal()
        try:
            open_count = session.query(Position).filter(
                Position.status.in_(["OPEN", "PARTIAL"])).count()
            self.assertEqual(open_count, 0)
        finally:
            session.close()


# ══════════════════════════════════════════════════════════════════════════════
# TEST 11: WEBSOCKET BROADCAST EVENTS
# ══════════════════════════════════════════════════════════════════════════════

class TestWebSocketBroadcasts(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()
        _reset_guardrails()

    async def test_pt1_broadcasts_auto_exit_event(self):
        seed_position(total_contracts=2, remaining=2, avg_price=1.00, current_price=1.00)

        async def fake_prices(symbols):
            return {s: 1.35 for s in symbols}
        original = public_sdk_bridge.fetch_prices
        public_sdk_bridge.fetch_prices = fake_prices
        manager = DummyManager()
        executor = PublicExecutor(manager)
        try:
            await position_monitor._check_all_positions(manager, executor)
        finally:
            public_sdk_bridge.fetch_prices = original

        auto_exit_events = [e for e in manager.events if e.get("type") == "auto_exit"]
        self.assertTrue(len(auto_exit_events) > 0, "Should broadcast auto_exit event for PT1")
        self.assertEqual(auto_exit_events[0]["data"]["rule"], "PT1")
        self.assertEqual(auto_exit_events[0]["data"]["severity"], "success")

    async def test_trailing_sl_broadcasts_danger_severity(self):
        self._orig_enabled = position_monitor.TRAILING_SL_ENABLED
        self._orig_pct = position_monitor.TRAILING_SL_PCT
        position_monitor.TRAILING_SL_ENABLED = True
        position_monitor.TRAILING_SL_PCT = 30.0

        seed_position(total_contracts=2, remaining=1, avg_price=1.00,
                      current_price=1.80, highest_price=1.80, pt1_triggered=True)

        async def fake_prices(symbols):
            return {s: 1.20 for s in symbols}
        original = public_sdk_bridge.fetch_prices
        public_sdk_bridge.fetch_prices = fake_prices
        manager = DummyManager()
        executor = PublicExecutor(manager)
        try:
            await position_monitor._check_all_positions(manager, executor)
        finally:
            public_sdk_bridge.fetch_prices = original
            position_monitor.TRAILING_SL_ENABLED = self._orig_enabled
            position_monitor.TRAILING_SL_PCT = self._orig_pct

        auto_exit_events = [e for e in manager.events if e.get("type") == "auto_exit"]
        self.assertTrue(len(auto_exit_events) > 0)
        self.assertEqual(auto_exit_events[0]["data"]["rule"], "TRAILING_SL")
        self.assertEqual(auto_exit_events[0]["data"]["severity"], "danger")


# ══════════════════════════════════════════════════════════════════════════════
# TEST 12: DB MIGRATION (highest_price column)
# ══════════════════════════════════════════════════════════════════════════════

class TestDBMigration(unittest.TestCase):
    def test_positions_table_has_highest_price_column(self):
        """Verify the highest_price column exists after init_db."""
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()
        _reset_guardrails()

        from sqlalchemy import inspect
        inspector = inspect(db.engine)
        columns = {col["name"] for col in inspector.get_columns("positions")}
        self.assertIn("highest_price", columns)


# ══════════════════════════════════════════════════════════════════════════════
# TEST 13: POSITION to_dict INCLUDES NEW FIELDS
# ══════════════════════════════════════════════════════════════════════════════

class TestPositionToDict(unittest.TestCase):
    def setUp(self):
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()
        _reset_guardrails()

    def test_to_dict_includes_highest_price(self):
        pid = seed_position(highest_price=2.50)
        session = db.SessionLocal()
        try:
            pos = session.query(Position).filter(Position.id == pid).first()
            d = pos.to_dict()
            self.assertIn("highestPrice", d)
            self.assertAlmostEqual(d["highestPrice"], 2.50, places=2)
        finally:
            session.close()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Clean up any leftover test DB
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(unittest.TestLoader().loadTestsFromModule(sys.modules[__name__]))

    # Cleanup
    db.engine.dispose()
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)

    sys.exit(0 if result.wasSuccessful() else 1)
