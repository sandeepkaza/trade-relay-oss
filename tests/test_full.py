"""
test_full.py - Comprehensive test suite for the trading bot.

Covers:
  1. Parser (regex) — all BUY/SELL formats, edge cases, OSI generation
  2. AI Parser — response building, error handling
  3. Guardrails — all 8 checks, cooldown fix (SELL bypasses), combined BUY guards
  4. AI Scorer — scoring components, quantity adjustment
  5. Kelly Sizer — formula, edge cases, cap logic
  6. Trade Flow — BUY→SELL, close-all, partial exits
  7. Position Monitor — PT1/PT2/PT3/SL/trailing SL, toggle checks
  8. Trade Logger — CSV exports
  9. Config toggles — every feature can be enabled/disabled
  10. Latency — hot path timing
"""

import os
import sys
import time
import unittest
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo
from unittest.mock import patch, MagicMock, AsyncMock
import asyncio
import contextlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Test DB setup (BEFORE any app imports) ───────────────────────────────────
TEST_DB_PATH = os.path.abspath("test_full_suite.db")
os.environ["TRADER_DATABASE_URL"] = f"sqlite:///{TEST_DB_PATH.replace(chr(92), '/')}"
os.environ["DRY_RUN"] = "true"

import app.execution.public_executor as public_executor
import app.risk.guardrails as guardrails
import app.risk.ai_scorer as ai_scorer
import app.risk.kelly_sizer as kelly_sizer
import app.monitors.position_monitor as position_monitor

# Force safe test defaults
public_executor.DRY_RUN = True
guardrails.MARKET_HOURS_ONLY = False
ai_scorer.SCORER_ENABLED = False
kelly_sizer.KELLY_ENABLED = False

import app.core.db as db
from app.core.models import Alert, Order, Position
from app.ingest.parser import parse_alert, TradeAlert, _build_osi, _parse_date
from app.execution.public_executor import PublicExecutor


# ── Helpers ──────────────────────────────────────────────────────────────────

class DummyManager:
    def __init__(self):
        self.events = []

    async def broadcast(self, data):
        self.events.append(data)


def _reset_db():
    db.Base.metadata.drop_all(bind=db.engine)
    db.init_db()


def _reset_guardrails():
    guardrails._recent_alerts.clear()
    guardrails._recent_content_hashes.clear()
    guardrails._last_trade_time.clear()
    guardrails._trading_halted = False
    guardrails._halt_date = None
    # Any test that fires a real SL leaves a 30-minute per-root cooldown behind
    # in module state that survives _reset_db(), so a later BUY on the same root
    # — in this file or another one — is blocked by SL_COOLDOWN for reasons that
    # have nothing to do with what it is testing.
    guardrails._sl_cooldown.clear()
    guardrails._sl_count_today.clear()


def build_alert_row(parsed_alert, raw_text="test", author="tester", status="PENDING"):
    session = db.SessionLocal()
    try:
        row = Alert(
            author=author,
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


def seed_position(**overrides):
    defaults = {
        "osi_symbol": "SPX270414C06970000",
        "symbol": "SPX",
        "expiry": "2027-04-14",
        "strike": 6970,
        "option_type": "C",
        "total_contracts": 2,
        "remaining": 2,
        "avg_price": 1.75,
        "current_price": 1.75,
        "highest_price": 1.75,
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


def seed_order(osi="SPX270414C06970000", side="BUY", qty=1, status="FILLED", trigger="DISCORD"):
    session = db.SessionLocal()
    try:
        order = Order(
            public_order_id="test-oid",
            osi_symbol=osi,
            side=side,
            quantity=qty,
            limit_price=1.85,
            fill_price=1.75,
            status=status,
            trigger=trigger,
        )
        session.add(order)
        session.commit()
        session.refresh(order)
        return order.id
    finally:
        session.close()


# ═════════════════════════════════════════════════════════════════════════════
# 1. PARSER TESTS
# ═════════════════════════════════════════════════════════════════════════════

class TestParser(unittest.TestCase):
    """Thorough regex parser tests — every known alert format."""

    def assertAlert(self, raw, **expected):
        alert = parse_alert(raw)
        self.assertIsNotNone(alert, f"Failed to parse: {raw}")
        for key, value in expected.items():
            actual = getattr(alert, key)
            if isinstance(value, Decimal):
                self.assertAlmostEqual(float(actual), float(value), places=2, msg=f"{raw} -> {key}")
            else:
                self.assertEqual(actual, value, f"{raw} -> {key}: expected {value}, got {actual}")

    # ── BUY formats ──────────────────────────────────────────────────────
    def test_buy_bought(self):
        self.assertAlert("BOUGHT SPX 4/14 6970C $1.75 [SMALL]",
                         action="BUY", symbol="SPX", strike=6970, option_type="C",
                         price=Decimal("1.75"), size_tag="SMALL")

    def test_buy_bto(self):
        self.assertAlert("BTO SPX 4/14 6970C @ 1.75",
                         action="BUY", symbol="SPX", strike=6970, option_type="C",
                         price=Decimal("1.75"))

    def test_buy_bot(self):
        self.assertAlert("BOT QQQ 4/16 420P @ 3.20 [MEDIUM]",
                         action="BUY", symbol="QQQ", strike=420, option_type="P",
                         price=Decimal("3.20"), size_tag="MEDIUM")

    def test_buy_opening(self):
        self.assertAlert("Opening NVDA 5/2 900C $5.50 [LARGE]",
                         action="BUY", symbol="NVDA", strike=900, option_type="C",
                         price=Decimal("5.50"), size_tag="LARGE")

    def test_buy_with_year_2digit(self):
        self.assertAlert("BOUGHT SPX 4/14/25 6970C $1.75",
                         action="BUY", symbol="SPX", strike=6970, option_type="C",
                         price=Decimal("1.75"))

    def test_buy_with_year_4digit(self):
        alert = parse_alert("BOUGHT SPX 4/14/2027 6970C $1.75")
        self.assertIsNotNone(alert)
        self.assertEqual(alert.expiry, date(2027, 4, 14))

    def test_buy_emoji_prefix(self):
        self.assertAlert("🟢 BOUGHT SPX 4/14 6970C $1.75 [SMALL]",
                         action="BUY", symbol="SPX", strike=6970)

    def test_buy_checkmark_prefix(self):
        self.assertAlert("✅ BTO SPX 4/14 6970C @1.75",
                         action="BUY", symbol="SPX", price=Decimal("1.75"))

    def test_buy_xl_size(self):
        self.assertAlert("BOUGHT TSLA 4/25 250C $2.50 [XL]",
                         action="BUY", symbol="TSLA", strike=250, size_tag="XL")

    def test_buy_full_size(self):
        self.assertAlert("BTO AAPL 5/16 220C $1.20 [FULL]",
                         action="BUY", symbol="AAPL", size_tag="FULL")

    # ── SELL formats — partial ───────────────────────────────────────────
    def test_sell_half(self):
        self.assertAlert("SOLD SPX 4/14 6970C $2.70 1/2",
                         action="SELL", fraction=Decimal("0.5"), close_all=False)

    def test_sell_third(self):
        self.assertAlert("STC NVDA 5/2 900C $5.50 1/3",
                         action="SELL", symbol="NVDA", strike=900,
                         fraction=Decimal("0.333"))

    def test_sell_quarter(self):
        self.assertAlert("SOLD QQQ 4/16 420P $1.85 1/4",
                         action="SELL", fraction=Decimal("0.25"))

    def test_sell_three_quarters(self):
        self.assertAlert("SOLD SPX 4/14 6970C $3.00 3/4",
                         action="SELL", fraction=Decimal("0.75"))

    def test_sell_half_word(self):
        self.assertAlert("SOLD QQQ 4/16 420P $1.85 HALF",
                         action="SELL", fraction=Decimal("0.5"))

    # ── SELL formats — full close ────────────────────────────────────────
    def test_sell_all(self):
        self.assertAlert("SOLD SPX 4/14 6970C $2.70 ALL",
                         action="SELL", fraction=Decimal("1"))

    def test_sell_no_fraction_full_close(self):
        alert = parse_alert("SOLD SPX 4/14 6970C $2.70")
        self.assertIsNotNone(alert)
        self.assertEqual(alert.action, "SELL")

    def test_all_out_with_symbol(self):
        self.assertAlert("ALL OUT SPX 4/14 6970C",
                         action="SELL", symbol="SPX", strike=6970, close_all=True, price=None)

    def test_all_out_generic(self):
        self.assertAlert("ALL OUT",
                         action="SELL", symbol="", close_all=True, price=None)

    def test_all_time(self):
        self.assertAlert("ALL TIME SPX 4/14 6970C $3.50",
                         action="SELL", close_all=True, price=Decimal("3.50"))

    def test_close_all(self):
        self.assertAlert("CLOSE ALL",
                         action="SELL", symbol="", close_all=True)

    def test_sold_everything(self):
        self.assertAlert("SOLD EVERYTHING",
                         action="SELL", symbol="", close_all=True)

    def test_all_out_no_date(self):
        self.assertAlert("ALL OUT SPX 6970C $2.70",
                         action="SELL", symbol="SPX", strike=6970, option_type="C",
                         price=Decimal("2.70"), close_all=True)

    # ── OSI symbol generation ────────────────────────────────────────────
    def test_osi_format(self):
        alert = parse_alert("BOUGHT SPX 4/14/2025 6970C $1.75")
        self.assertIsNotNone(alert)
        self.assertEqual(alert.osi_symbol, "SPX250414C06970000")

    def test_osi_builder(self):
        self.assertEqual(_build_osi("SPX", date(2025, 4, 14), 6970, "C"), "SPX250414C06970000")
        self.assertEqual(_build_osi("AAPL", date(2025, 12, 5), 220, "P"), "AAPL251205P00220000")
        self.assertEqual(_build_osi("QQQ", date(2026, 1, 16), 420, "C"), "QQQ260116C00420000")

    # ── Date parsing ─────────────────────────────────────────────────────
    def test_parse_date_no_year(self):
        d = _parse_date("12", "25", None)
        self.assertIsNotNone(d)
        self.assertEqual(d.month, 12)
        self.assertEqual(d.day, 25)

    def test_parse_date_2digit_year(self):
        d = _parse_date("4", "14", "25")
        self.assertEqual(d, date(2025, 4, 14))

    def test_parse_date_4digit_year(self):
        d = _parse_date("4", "14", "2027")
        self.assertEqual(d, date(2027, 4, 14))

    # ── Non-trade messages ───────────────────────────────────────────────
    def test_non_trade_returns_none(self):
        self.assertIsNone(parse_alert("SPX is pumping today!"))
        self.assertIsNone(parse_alert("PT1 hit! Great call"))
        self.assertIsNone(parse_alert(""))
        self.assertIsNone(parse_alert("lol"))
        self.assertIsNone(parse_alert("hi"))
        self.assertIsNone(parse_alert("good morning"))

    # ── Channel ID pass-through ──────────────────────────────────────────
    def test_channel_id(self):
        alert = parse_alert("BOUGHT SPX 4/14 6970C $1.75", channel_id=12345)
        self.assertEqual(alert.channel_id, 12345)

    # ── Alternative patterns ─────────────────────────────────────────────
    def test_reversed_format(self):
        """SPY 678P 4/14 $3.15"""
        alert = parse_alert("BOUGHT SPY 678P 4/14 $3.15")
        self.assertIsNotNone(alert)
        self.assertEqual(alert.symbol, "SPY")

    def test_cashtag_stripped(self):
        alert = parse_alert("BOUGHT $AAPL 4/14 220C $1.50")
        self.assertIsNotNone(alert)
        self.assertEqual(alert.symbol, "AAPL")


# ═════════════════════════════════════════════════════════════════════════════
# 2. AI PARSER TESTS
# ═════════════════════════════════════════════════════════════════════════════

class TestAIParser(unittest.TestCase):
    """Test the AI parser response builder (no real API calls)."""

    def test_build_trade_alert_buy(self):
        from app.ingest.ai_parser import _build_trade_alert
        parsed = {
            "action": "BUY", "symbol": "SPX",
            "expiry_month": 4, "expiry_day": 14, "expiry_year": 2027,
            "strike": 6970, "option_type": "C",
            "price": 1.75, "fraction": 1.0,
            "size_tag": "SMALL", "close_all": False,
        }
        alert = _build_trade_alert(parsed, channel_id=99)
        self.assertIsNotNone(alert)
        self.assertEqual(alert.action, "BUY")
        self.assertEqual(alert.symbol, "SPX")
        self.assertEqual(alert.strike, 6970)
        self.assertEqual(alert.option_type, "C")
        self.assertEqual(alert.price, Decimal("1.75"))
        self.assertEqual(alert.size_tag, "SMALL")
        self.assertEqual(alert.channel_id, 99)

    def test_build_trade_alert_sell(self):
        from app.ingest.ai_parser import _build_trade_alert
        parsed = {
            "action": "SELL", "symbol": "QQQ",
            "expiry_month": 5, "expiry_day": 16, "expiry_year": 2027,
            "strike": 420, "option_type": "P",
            "price": 2.50, "fraction": 0.5,
            "size_tag": "", "close_all": False,
        }
        alert = _build_trade_alert(parsed, None)
        self.assertIsNotNone(alert)
        self.assertEqual(alert.action, "SELL")
        self.assertEqual(alert.fraction, Decimal("0.5"))

    def test_build_trade_alert_null_action(self):
        from app.ingest.ai_parser import _build_trade_alert
        alert = _build_trade_alert({"action": None}, None)
        self.assertIsNone(alert)

    def test_build_trade_alert_close_all(self):
        from app.ingest.ai_parser import _build_trade_alert
        parsed = {
            "action": "SELL", "symbol": "SPX",
            "expiry_month": 4, "expiry_day": 14,
            "strike": 6970, "option_type": "C",
            "price": None, "fraction": 1.0,
            "size_tag": "", "close_all": True,
        }
        alert = _build_trade_alert(parsed, None)
        self.assertIsNotNone(alert)
        self.assertTrue(alert.close_all)
        self.assertEqual(alert.fraction, Decimal("1"))

    def test_build_trade_alert_bad_action(self):
        from app.ingest.ai_parser import _build_trade_alert
        alert = _build_trade_alert({"action": "HOLD"}, None)
        self.assertIsNone(alert)


# ═════════════════════════════════════════════════════════════════════════════
# 3. GUARDRAILS TESTS
# ═════════════════════════════════════════════════════════════════════════════

class TestGuardrails(unittest.TestCase):

    def setUp(self):
        _reset_db()
        _reset_guardrails()
        # Save and override
        self._orig_market = guardrails.MARKET_HOURS_ONLY
        guardrails.MARKET_HOURS_ONLY = False

    def tearDown(self):
        guardrails.MARKET_HOURS_ONLY = self._orig_market

    def test_first_alert_passes(self):
        ok, reason = guardrails.check_guardrails("BUY", "SPX270414C06970000", Decimal("1.75"), 1)
        self.assertTrue(ok, reason)

    def test_duplicate_blocked(self):
        guardrails.check_guardrails("BUY", "SPX270414C06970000", Decimal("1.75"), 1)
        ok, reason = guardrails.check_guardrails("BUY", "SPX270414C06970000", Decimal("1.75"), 1)
        self.assertFalse(ok)
        self.assertIn("DUPLICATE", reason)

    def test_cooldown_blocks_buy_only(self):
        """Critical fix: cooldown should NOT block SELL orders."""
        guardrails.check_guardrails("BUY", "SPX270414C06970000", Decimal("1.75"), 1)
        # Immediately try to SELL the same symbol — should pass
        ok, reason = guardrails.check_guardrails("SELL", "SPX270414C06970000", Decimal("2.70"), 1)
        # SELL should pass dedup since it's a different action
        self.assertTrue(ok, f"SELL blocked unexpectedly: {reason}")

    def test_cooldown_blocks_rapid_buy(self):
        guardrails.check_guardrails("BUY", "TEST270414C06970000", Decimal("1.75"), 1)
        _reset_guardrails()  # clear dedup but test cooldown logic
        guardrails._last_trade_time["TEST270414C06970000"] = time.time()
        ok, reason = guardrails.check_guardrails("BUY", "TEST270414C06970000", Decimal("1.75"), 1)
        self.assertFalse(ok)
        self.assertIn("COOLDOWN", reason)

    def test_max_trade_cost_blocks(self):
        ok, reason = guardrails.check_guardrails("BUY", "EXPENSIVE00C99999000", Decimal("100.00"), 10)
        self.assertFalse(ok)
        self.assertIn("MAX COST", reason)

    def test_max_positions_blocks(self):
        for i in range(guardrails.MAX_OPEN_POSITIONS):
            seed_position(osi_symbol=f"POS{i}270414C06970000", symbol=f"POS{i}")
        ok, reason = guardrails.check_guardrails("BUY", "NEWPOS270414C06970000", Decimal("1.00"), 1)
        self.assertFalse(ok)
        self.assertIn("MAX POSITIONS", reason)

    def test_cross_channel_dedup(self):
        h = guardrails.compute_content_hash("SPX270414C06970000", "BUY", Decimal("1.75"))
        ok1, _ = guardrails.check_guardrails("BUY", "SPX270414C06970000", Decimal("1.75"), 1, content_hash=h)
        self.assertTrue(ok1)
        # Same hash from a different "channel" — should be blocked
        _reset_guardrails()  # clear simple dedup, keep content hash
        guardrails._recent_content_hashes[h] = time.time()
        ok2, reason = guardrails.check_guardrails("BUY", "SPX270414C06970000", Decimal("1.75"), 1, content_hash=h)
        self.assertFalse(ok2)
        self.assertIn("CROSS-CHANNEL", reason)

    def test_content_hash_deterministic(self):
        h1 = guardrails.compute_content_hash("SPX270414C06970000", "BUY", Decimal("1.75"))
        h2 = guardrails.compute_content_hash("SPX270414C06970000", "BUY", Decimal("1.75"))
        self.assertEqual(h1, h2)

    def test_content_hash_differs_on_price(self):
        h1 = guardrails.compute_content_hash("SPX270414C06970000", "BUY", Decimal("1.75"))
        h2 = guardrails.compute_content_hash("SPX270414C06970000", "BUY", Decimal("2.00"))
        self.assertNotEqual(h1, h2)

    def test_halt_and_reset(self):
        guardrails._trading_halted = True
        guardrails._halt_date = "2020-01-01"  # old date
        ok, reason = guardrails.check_guardrails("BUY", "TEST270414C06970000", Decimal("1.00"), 1)
        # Should auto-reset because it's a new day
        self.assertTrue(ok, reason)

    def test_sell_bypasses_buy_guards(self):
        """SELL should pass even if max positions/trades/cost limits are hit."""
        for i in range(guardrails.MAX_OPEN_POSITIONS):
            seed_position(osi_symbol=f"POS{i}270414C06970000", symbol=f"POS{i}")
        ok, reason = guardrails.check_guardrails("SELL", "POS0270414C06970000", Decimal("5.00"), 10)
        self.assertTrue(ok, f"SELL should bypass BUY guards: {reason}")


# ═════════════════════════════════════════════════════════════════════════════
# 4. AI SCORER TESTS
# ═════════════════════════════════════════════════════════════════════════════

class TestAIScorer(unittest.TestCase):

    def setUp(self):
        _reset_db()

    def test_sell_bypasses_scoring(self):
        result = ai_scorer.score_alert("SELL", "author", "SPX", "SPX270414C06970000", 2.70, "")
        self.assertEqual(result["score"], 100)

    def test_score_unknown_analyst(self):
        result = ai_scorer.score_alert("BUY", "", "SPX", "SPX270414C06970000", 1.75, "SMALL")
        self.assertIn("score", result)
        self.assertGreater(result["score"], 0)

    def test_score_with_data(self):
        """Seed some historical alerts/positions, then score."""
        session = db.SessionLocal()
        try:
            for i in range(5):
                a = Alert(
                    author="TestAnalyst", action="BUY", raw_text="test",
                    osi_symbol=f"TEST{i}270414C06970000", symbol="TEST",
                    status="FILLED", timestamp=datetime.now(timezone.utc) - timedelta(days=i),
                )
                session.add(a)
                p = Position(
                    osi_symbol=f"TEST{i}270414C06970000", symbol="TEST",
                    strike=6970, option_type="C",
                    total_contracts=1, remaining=0,
                    avg_price=1.00, current_price=1.50 if i < 3 else 0.60,
                    status="CLOSED",
                )
                session.add(p)
            session.commit()
        finally:
            session.close()

        result = ai_scorer.score_alert("BUY", "TestAnalyst", "TEST", "TESTNEW270414C06970000", 1.75, "MEDIUM")
        self.assertIn("score", result)
        self.assertIn("breakdown", result)
        self.assertIn("analyst", result["breakdown"])

    def test_quantity_adjustment(self):
        self.assertEqual(ai_scorer.adjust_quantity(2, 20, "MEDIUM"), 0)   # below min → skip
        self.assertEqual(ai_scorer.adjust_quantity(2, 40, "MEDIUM"), 1)   # low → minimum
        self.assertEqual(ai_scorer.adjust_quantity(2, 60, "MEDIUM"), 2)   # moderate → as-is
        self.assertEqual(ai_scorer.adjust_quantity(2, 80, "MEDIUM"), 3)   # high → +1
        self.assertEqual(ai_scorer.adjust_quantity(2, 90, "MEDIUM"), 4)   # very high → +2

    def test_size_tag_scoring(self):
        score_xl, _ = ai_scorer._score_size_tag("XL")
        score_xs, _ = ai_scorer._score_size_tag("XS")
        score_none, _ = ai_scorer._score_size_tag("")
        self.assertGreater(score_xl, score_xs)
        self.assertGreater(score_xl, score_none)


# ═════════════════════════════════════════════════════════════════════════════
# 5. KELLY SIZER TESTS
# ═════════════════════════════════════════════════════════════════════════════

class TestKellySizer(unittest.TestCase):

    def setUp(self):
        _reset_db()

    def test_disabled_returns_passthrough(self):
        kelly_sizer.KELLY_ENABLED = False
        result = kelly_sizer.calculate_kelly_contracts(1.75, 10000.0)
        self.assertEqual(result["max_contracts"], 999)  # no cap
        kelly_sizer.KELLY_ENABLED = False

    def test_zero_price_returns_default(self):
        kelly_sizer.KELLY_ENABLED = True
        result = kelly_sizer.calculate_kelly_contracts(0, 10000.0)
        self.assertIn("Invalid", result["reason"])
        kelly_sizer.KELLY_ENABLED = False

    def test_zero_balance_returns_default(self):
        kelly_sizer.KELLY_ENABLED = True
        result = kelly_sizer.calculate_kelly_contracts(1.75, 0)
        self.assertIn("Invalid", result["reason"])
        kelly_sizer.KELLY_ENABLED = False

    def test_insufficient_data_uses_risk_cap(self):
        kelly_sizer.KELLY_ENABLED = True
        result = kelly_sizer.calculate_kelly_contracts(1.00, 10000.0, author="nobody")
        # Less than 5 trades → should use risk cap only
        self.assertIn("<5 trades", result["reason"])
        self.assertGreater(result["max_contracts"], 0)
        kelly_sizer.KELLY_ENABLED = False

    def test_apply_kelly_cap_disabled(self):
        kelly_sizer.KELLY_ENABLED = False
        capped, result = kelly_sizer.apply_kelly_cap(5, 1.00, 10000.0)
        self.assertEqual(capped, 5)  # unchanged

    def test_apply_kelly_cap_reduces(self):
        kelly_sizer.KELLY_ENABLED = True
        # Seed enough data for Kelly to produce a result
        session = db.SessionLocal()
        try:
            for i in range(10):
                a = Alert(
                    author="kelly_test", action="BUY", raw_text="test",
                    osi_symbol=f"K{i}270414C06970000", symbol="K",
                    status="FILLED", timestamp=datetime.now(timezone.utc) - timedelta(days=i),
                )
                session.add(a)
                p = Position(
                    osi_symbol=f"K{i}270414C06970000", symbol="K",
                    strike=6970, option_type="C",
                    total_contracts=1, remaining=0,
                    avg_price=1.00, current_price=1.30 if i < 6 else 0.70,
                    status="CLOSED",
                )
                session.add(p)
            session.commit()
        finally:
            session.close()

        capped, result = kelly_sizer.apply_kelly_cap(100, 1.00, 5000.0, author="kelly_test")
        # Kelly should cap well below 100 contracts on a $5k account
        self.assertLess(capped, 100)
        kelly_sizer.KELLY_ENABLED = False


# ═════════════════════════════════════════════════════════════════════════════
# 6. TRADE FLOW TESTS (BUY→SELL, close-all, partial exits)
# ═════════════════════════════════════════════════════════════════════════════

class TestTradeFlow(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        _reset_db()
        _reset_guardrails()
        public_executor.DRY_RUN = True
        guardrails.MARKET_HOURS_ONLY = False
        ai_scorer.SCORER_ENABLED = False
        kelly_sizer.KELLY_ENABLED = False

    async def test_buy_creates_position(self):
        manager = DummyManager()
        executor = PublicExecutor(manager)

        raw = "BOUGHT SPX 4/14/2027 6970C $1.75 [MEDIUM]"
        alert = parse_alert(raw)
        alert_id = build_alert_row(alert, raw)
        await executor.execute(alert, alert_id)

        session = db.SessionLocal()
        try:
            pos = session.query(Position).filter_by(osi_symbol=alert.osi_symbol).first()
            self.assertIsNotNone(pos)
            self.assertEqual(pos.status, "OPEN")
            self.assertEqual(pos.total_contracts, 2)  # MEDIUM = 2
            self.assertAlmostEqual(pos.avg_price, 1.75, places=2)
        finally:
            session.close()

    async def test_buy_then_sell_partial(self):
        """BUY→immediate SELL should work (cooldown fix)."""
        manager = DummyManager()
        executor = PublicExecutor(manager)

        buy_raw = "BOUGHT SPX 4/14/2027 6970C $1.75 [MEDIUM]"
        buy_alert = parse_alert(buy_raw)
        buy_id = build_alert_row(buy_alert, buy_raw)
        await executor.execute(buy_alert, buy_id)

        sell_raw = "SOLD SPX 4/14/2027 6970C $2.70 1/2"
        sell_alert = parse_alert(sell_raw)
        sell_id = build_alert_row(sell_alert, sell_raw)
        await executor.execute(sell_alert, sell_id)

        session = db.SessionLocal()
        try:
            pos = session.query(Position).filter_by(osi_symbol=buy_alert.osi_symbol).first()
            self.assertIsNotNone(pos)
            self.assertEqual(pos.remaining, 1)
            self.assertEqual(pos.status, "PARTIAL")

            orders = session.query(Order).filter_by(status="FILLED").count()
            self.assertEqual(orders, 2)  # 1 BUY + 1 SELL
        finally:
            session.close()

    async def test_close_all_no_date(self):
        seed_position()
        manager = DummyManager()
        executor = PublicExecutor(manager)

        raw = "ALL OUT SPX 6970C $2.70"
        alert = parse_alert(raw)
        alert_id = build_alert_row(alert, raw)
        await executor.execute(alert, alert_id)

        session = db.SessionLocal()
        try:
            pos = session.query(Position).one()
            self.assertEqual(pos.remaining, 0)
            self.assertEqual(pos.status, "CLOSED")
        finally:
            session.close()

    async def test_generic_all_out_closes_everything(self):
        seed_position(osi_symbol="SPX270414C06970000", symbol="SPX")
        seed_position(osi_symbol="QQQ270416P00420000", symbol="QQQ",
                      avg_price=3.20, current_price=3.20, highest_price=3.20)

        manager = DummyManager()
        executor = PublicExecutor(manager)

        raw = "ALL OUT"
        alert = parse_alert(raw)
        alert_id = build_alert_row(alert, raw)
        # Bare "ALL OUT" (blank symbol) is gated off by default since 2026-06-11;
        # this test exercises the legacy book-wide flatten, so disable the gate
        # + force scope=all.
        import app.execution.public_executor as _pe
        _sr, _ss = _pe._CLOSE_ALL_REQUIRES_SYMBOL, _pe._CLOSE_ALL_SCOPE
        _pe._CLOSE_ALL_REQUIRES_SYMBOL = lambda: False
        _pe._CLOSE_ALL_SCOPE = lambda: "all"
        try:
            await executor.execute(alert, alert_id)
        finally:
            _pe._CLOSE_ALL_REQUIRES_SYMBOL = _sr
            _pe._CLOSE_ALL_SCOPE = _ss

        session = db.SessionLocal()
        try:
            open_count = session.query(Position).filter(Position.status.in_(["OPEN", "PARTIAL"])).count()
            self.assertEqual(open_count, 0)
        finally:
            session.close()

    async def test_sell_no_matching_position_ignored(self):
        manager = DummyManager()
        executor = PublicExecutor(manager)

        raw = "SOLD AAPL 4/14/2027 220C $5.00"
        alert = parse_alert(raw)
        alert_id = build_alert_row(alert, raw)
        await executor.execute(alert, alert_id)

        session = db.SessionLocal()
        try:
            a = session.query(Alert).filter(Alert.id == alert_id).first()
            # SELL with no matching open position is marked IGNORED by _sell_from_alert
            # or stays PENDING if guardrails block. Either is acceptable — no crash.
            self.assertIn(a.status, ["IGNORED", "PENDING"])
        finally:
            session.close()

    async def test_buy_with_scorer(self):
        """Test that AI scorer integrates without errors when enabled."""
        ai_scorer.SCORER_ENABLED = True
        try:
            manager = DummyManager()
            executor = PublicExecutor(manager)

            # Use a cheap price so MAX COST guardrail doesn't block (LARGE=3, 0.50*3*100=$150 < $500)
            raw = "BOUGHT TSLA 4/14/2027 250C $0.50 [SMALL]"
            alert = parse_alert(raw)
            alert_id = build_alert_row(alert, raw, author="TestAnalyst")
            await executor.execute(alert, alert_id, alert_author="TestAnalyst")

            # Should either fill or skip based on score — no crash
            session = db.SessionLocal()
            try:
                a = session.query(Alert).filter(Alert.id == alert_id).first()
                self.assertIn(a.status, ["FILLED", "SKIPPED"])
            finally:
                session.close()

            # Check WS got a score_update event
            score_events = [e for e in manager.events if e.get("type") == "score_update"]
            self.assertGreater(len(score_events), 0)
        finally:
            ai_scorer.SCORER_ENABLED = False


# ═════════════════════════════════════════════════════════════════════════════
# 7. POSITION MONITOR TESTS
# ═════════════════════════════════════════════════════════════════════════════

class TestPositionMonitor(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        _reset_db()
        public_executor.DRY_RUN = True
        position_monitor.AUTO_EXIT_ENABLED = True
        position_monitor.PT1_ENABLED = True
        position_monitor.PT2_ENABLED = True
        position_monitor.PT3_ENABLED = True
        position_monitor.SL_ENABLED = True

    async def _check_with_price(self, price_map):
        import app.execution.broker_router as broker_router
        orig = broker_router.fetch_prices
        broker_router.fetch_prices = AsyncMock(return_value=price_map)
        manager = DummyManager()
        executor = PublicExecutor(manager)
        try:
            await position_monitor._check_all_positions(manager, executor)
        finally:
            broker_router.fetch_prices = orig
        return manager

    async def test_pt1_triggers_at_30pct(self):
        seed_position(avg_price=1.00, current_price=1.00, total_contracts=4, remaining=4)

        mgr = await self._check_with_price({"SPX270414C06970000": 1.35})

        session = db.SessionLocal()
        try:
            pos = session.query(Position).one()
            self.assertTrue(pos.pt1_triggered)
            self.assertEqual(pos.remaining, 2)  # sold 50% of 4 = 2
            self.assertEqual(pos.status, "PARTIAL")
        finally:
            session.close()

    async def test_pt2_triggers_after_pt1(self):
        seed_position(avg_price=1.00, current_price=1.00, total_contracts=4, remaining=2,
                      pt1_triggered=True, status="PARTIAL")

        mgr = await self._check_with_price({"SPX270414C06970000": 1.65})

        session = db.SessionLocal()
        try:
            pos = session.query(Position).one()
            self.assertTrue(pos.pt2_triggered)
            self.assertEqual(pos.remaining, 1)  # sold 50% of 2 = 1
        finally:
            session.close()

    async def test_pt3_closes_remaining(self):
        seed_position(avg_price=1.00, current_price=1.00, total_contracts=4, remaining=1,
                      pt1_triggered=True, pt2_triggered=True, status="PARTIAL")

        mgr = await self._check_with_price({"SPX270414C06970000": 2.05})

        session = db.SessionLocal()
        try:
            pos = session.query(Position).one()
            self.assertTrue(pos.pt3_triggered)
            self.assertEqual(pos.remaining, 0)
            self.assertEqual(pos.status, "CLOSED")
        finally:
            session.close()

    async def test_sl_triggers_at_minus_50pct(self):
        seed_position(avg_price=2.00, current_price=2.00, total_contracts=2, remaining=2)

        mgr = await self._check_with_price({"SPX270414C06970000": 0.90})

        session = db.SessionLocal()
        try:
            pos = session.query(Position).one()
            self.assertTrue(pos.sl_triggered)
            self.assertEqual(pos.remaining, 0)
            self.assertEqual(pos.status, "CLOSED")
        finally:
            session.close()

    async def test_trailing_sl_after_pt1(self):
        position_monitor.TRAILING_SL_ENABLED = True
        seed_position(avg_price=1.00, current_price=1.50, highest_price=1.80,
                      total_contracts=4, remaining=2,
                      pt1_triggered=True, status="PARTIAL")

        # Price dropped 30%+ from highest (1.80 * 0.70 = 1.26)
        mgr = await self._check_with_price({"SPX270414C06970000": 1.20})

        session = db.SessionLocal()
        try:
            pos = session.query(Position).one()
            self.assertTrue(pos.sl_triggered)
            self.assertEqual(pos.remaining, 0)
            self.assertEqual(pos.status, "CLOSED")
        finally:
            session.close()
        position_monitor.TRAILING_SL_ENABLED = False

    async def test_auto_exit_disabled_skips_all(self):
        position_monitor.AUTO_EXIT_ENABLED = False
        seed_position(avg_price=1.00, current_price=1.00, total_contracts=4, remaining=4)

        mgr = await self._check_with_price({"SPX270414C06970000": 1.50})

        session = db.SessionLocal()
        try:
            pos = session.query(Position).one()
            self.assertFalse(pos.pt1_triggered)
            self.assertEqual(pos.remaining, 4)  # no exit fired
        finally:
            session.close()
        position_monitor.AUTO_EXIT_ENABLED = True

    async def test_pt1_disabled_skips_pt1(self):
        position_monitor.PT1_ENABLED = False
        seed_position(avg_price=1.00, current_price=1.00, total_contracts=4, remaining=4)

        mgr = await self._check_with_price({"SPX270414C06970000": 1.35})

        session = db.SessionLocal()
        try:
            pos = session.query(Position).one()
            self.assertFalse(pos.pt1_triggered)
            self.assertEqual(pos.remaining, 4)
        finally:
            session.close()
        position_monitor.PT1_ENABLED = True

    async def test_sl_disabled_skips_sl(self):
        position_monitor.SL_ENABLED = False
        seed_position(avg_price=2.00, current_price=2.00, total_contracts=2, remaining=2)

        mgr = await self._check_with_price({"SPX270414C06970000": 0.90})

        session = db.SessionLocal()
        try:
            pos = session.query(Position).one()
            self.assertFalse(pos.sl_triggered)
            self.assertEqual(pos.remaining, 2)
        finally:
            session.close()
        position_monitor.SL_ENABLED = True

    async def test_highest_price_tracks(self):
        seed_position(avg_price=1.00, current_price=1.00, highest_price=1.00,
                      total_contracts=2, remaining=2)

        mgr = await self._check_with_price({"SPX270414C06970000": 1.20})

        session = db.SessionLocal()
        try:
            pos = session.query(Position).one()
            self.assertAlmostEqual(pos.highest_price, 1.20)
        finally:
            session.close()


# ═════════════════════════════════════════════════════════════════════════════
# 8. TRADE LOGGER / CSV EXPORT TESTS
# ═════════════════════════════════════════════════════════════════════════════

class TestTradeLogger(unittest.TestCase):

    def setUp(self):
        _reset_db()

    def test_export_trades_csv(self):
        import app.analytics.trade_logger as trade_logger
        seed_order()
        csv = trade_logger.export_trades_csv(days=30)
        self.assertIn("Side", csv)
        self.assertIn("BUY", csv)

    def test_export_positions_csv(self):
        import app.analytics.trade_logger as trade_logger
        seed_position()
        csv = trade_logger.export_positions_csv()
        self.assertIn("Symbol", csv)
        self.assertIn("SPX", csv)

    def test_export_alerts_csv(self):
        import app.analytics.trade_logger as trade_logger
        alert = parse_alert("BOUGHT SPX 4/14/2027 6970C $1.75")
        build_alert_row(alert, "BOUGHT SPX 4/14/2027 6970C $1.75")
        csv = trade_logger.export_alerts_csv(days=30)
        self.assertIn("Author", csv)
        self.assertIn("tester", csv)

    def test_export_empty(self):
        import app.analytics.trade_logger as trade_logger
        csv = trade_logger.export_trades_csv(days=30)
        self.assertIn("Side", csv)  # header still present
        lines = csv.strip().split("\n")
        self.assertEqual(len(lines), 1)  # header only


# ═════════════════════════════════════════════════════════════════════════════
# 9. CONFIG TOGGLE TESTS
# ═════════════════════════════════════════════════════════════════════════════

class TestConfigToggles(unittest.TestCase):
    """Verify that config.ini has all expected toggles and they parse correctly."""

    def setUp(self):
        import configparser, os
        # Read the committed test fixture (conftest sets TRADER_CONFIG_PATH), not
        # the literal "config.ini" — that file is gitignored and absent in CI.
        cfg_path = os.environ.get("TRADER_CONFIG_PATH", "config.ini")
        self.cfg = configparser.ConfigParser(inline_comment_prefixes=(";", "#"))
        self.cfg.read(cfg_path, encoding="utf-8")

    def test_trading_section_toggles(self):
        booleans = [
            "dry_run", "position_monitor_enabled", "auto_exit_enabled",
            "pt1_enabled", "pt2_enabled", "pt3_enabled", "sl_enabled",
            "trailing_sl_enabled", "local_parser_enabled", "ai_parser_enabled",
            "ai_scorer_enabled", "kelly_enabled", "trade_logger_enabled",
            "daily_summary_enabled", "daily_summary_ai_enabled",
            "analyst_tracking_enabled", "forwarder_enabled",
        ]
        for key in booleans:
            val = self.cfg.get("trading", key, fallback=None)
            self.assertIsNotNone(val, f"Missing toggle: [trading] {key}")
            self.assertIn(val.lower(), ("true", "false"), f"Bad value for {key}: {val}")

    def test_guardrails_section(self):
        keys = [
            "dedup_window_seconds", "market_hours_only", "max_daily_loss",
            "max_open_positions", "max_daily_trades", "max_trade_cost",
            "trade_cooldown_seconds", "cross_channel_dedup_seconds",
        ]
        for key in keys:
            val = self.cfg.get("guardrails", key, fallback=None)
            self.assertIsNotNone(val, f"Missing: [guardrails] {key}")

    def test_ai_parser_section(self):
        for key in ["api_key", "model", "timeout_seconds"]:
            val = self.cfg.get("ai_parser", key, fallback=None)
            self.assertIsNotNone(val, f"Missing: [ai_parser] {key}")

    def test_exit_rule_configs(self):
        for key in ["pt1_pct", "pt1_sell", "pt2_pct", "pt2_sell", "pt3_pct",
                     "sl_pct", "trailing_sl_pct"]:
            val = self.cfg.get("trading", key, fallback=None)
            self.assertIsNotNone(val, f"Missing: [trading] {key}")

    def test_size_configs(self):
        for key in ["size_default", "size_xs", "size_small", "size_medium",
                     "size_large", "size_full", "size_xl"]:
            val = self.cfg.getint("trading", key, fallback=None)
            self.assertIsNotNone(val, f"Missing: [trading] {key}")


# ═════════════════════════════════════════════════════════════════════════════
# 10. LATENCY / HOT PATH TESTS
# ═════════════════════════════════════════════════════════════════════════════

class TestLatency(unittest.TestCase):
    """Ensure the hot path (parse + guardrails) is fast enough for trading."""

    def setUp(self):
        _reset_db()
        _reset_guardrails()
        guardrails.MARKET_HOURS_ONLY = False

    def test_parser_latency_under_1ms(self):
        raw = "BOUGHT SPX 4/14/2027 6970C $1.75 [MEDIUM]"
        # Warm up
        parse_alert(raw)

        start = time.perf_counter()
        iterations = 1000
        for _ in range(iterations):
            parse_alert(raw)
        elapsed = time.perf_counter() - start

        avg_us = (elapsed / iterations) * 1_000_000
        print(f"\n  Parser avg latency: {avg_us:.0f} µs ({elapsed*1000:.1f}ms for {iterations} iterations)")
        self.assertLess(avg_us, 1000, f"Parser too slow: {avg_us:.0f} µs (target: <1000 µs)")

    def test_guardrails_latency_under_15ms(self):
        """Guardrails hot path latency, including DB access.

        Budget 15ms (was 5ms): the BUY path now issues an extra DB round-trip
        (the stale-mark guard, GH #11) plus the persisted-halt load + metrics
        wrapper, and this is a deploy-gating test that must pass on variable
        runners (a busy CI box / slow single core swings 5→7ms for identical
        code). 15ms still catches an order-of-magnitude regression, and the
        guardrail runs OFF the event loop (asyncio.to_thread) in production so
        single-call latency here doesn't block ingest regardless."""
        start = time.perf_counter()
        iterations = 100
        for i in range(iterations):
            _reset_guardrails()
            guardrails.check_guardrails("BUY", f"PERF{i}270414C06970000", Decimal("1.75"), 1)
        elapsed = time.perf_counter() - start

        avg_ms = (elapsed / iterations) * 1000
        print(f"\n  Guardrails avg latency: {avg_ms:.1f} ms ({elapsed*1000:.0f}ms for {iterations} iterations)")
        self.assertLess(avg_ms, 15, f"Guardrails too slow: {avg_ms:.1f} ms (target: <15ms)")

    def test_content_hash_latency(self):
        start = time.perf_counter()
        iterations = 10000
        for _ in range(iterations):
            guardrails.compute_content_hash("SPX270414C06970000", "BUY", Decimal("1.75"))
        elapsed = time.perf_counter() - start

        avg_us = (elapsed / iterations) * 1_000_000
        print(f"\n  Content hash avg latency: {avg_us:.1f} µs")
        self.assertLess(avg_us, 100, f"Hash too slow: {avg_us:.1f} µs")


# ═════════════════════════════════════════════════════════════════════════════
# 11. MODELS TESTS
# ═════════════════════════════════════════════════════════════════════════════

class TestModels(unittest.TestCase):

    def setUp(self):
        _reset_db()

    def test_position_pnl_pct(self):
        pid = seed_position(avg_price=1.00, current_price=1.50)
        session = db.SessionLocal()
        try:
            pos = session.query(Position).filter(Position.id == pid).first()
            self.assertAlmostEqual(pos.pnl_pct(), 50.0, places=1)
        finally:
            session.close()

    def test_position_pnl_dollar(self):
        pid = seed_position(avg_price=1.00, current_price=1.50, remaining=2)
        session = db.SessionLocal()
        try:
            pos = session.query(Position).filter(Position.id == pid).first()
            # (1.50 - 1.00) * 2 * 100 = $100
            self.assertAlmostEqual(pos.pnl_dollar(), 100.0, places=1)
        finally:
            session.close()

    def test_position_to_dict(self):
        pid = seed_position()
        session = db.SessionLocal()
        try:
            pos = session.query(Position).filter(Position.id == pid).first()
            d = pos.to_dict()
            self.assertEqual(d["symbol"], "SPX")
            self.assertEqual(d["contracts"], 2)
            self.assertIn("pnlPct", d)
            self.assertIn("exits", d)
        finally:
            session.close()

    def test_alert_to_dict(self):
        alert = parse_alert("BOUGHT SPX 4/14/2027 6970C $1.75")
        aid = build_alert_row(alert, "BOUGHT SPX 4/14/2027 6970C $1.75")
        session = db.SessionLocal()
        try:
            a = session.query(Alert).filter(Alert.id == aid).first()
            d = a.to_dict()
            self.assertEqual(d["action"], "BUY")
            self.assertEqual(d["symbol"], "SPX")
            self.assertIn("osiSymbol", d)
        finally:
            session.close()

    def test_order_to_dict(self):
        oid = seed_order()
        session = db.SessionLocal()
        try:
            o = session.query(Order).filter(Order.id == oid).first()
            d = o.to_dict()
            self.assertEqual(d["side"], "BUY")
            self.assertIn("limitPrice", d)
        finally:
            session.close()


# ═════════════════════════════════════════════════════════════════════════════
# AUTO-EXIT RE-PEG (auto_exit_repeg_enabled)
# ═════════════════════════════════════════════════════════════════════════════

# Near-dated on purpose. A far expiry resolves through [profile:multi_day]
# (sl_pct -40, pts_disabled_default true) and a SPX/NDX/RUT root resolves
# through [profile:spx_index] (sl_enabled false) — neither is the default exit
# ladder these tests are about. Tomorrow's expiry on an equity root lands on
# plain [trading]: sl_enabled true, sl_pct -20.
# ET, not the runner's clock. profile_resolver measures "days to expiry" from
# Eastern, so date.today() on a UTC runner after 20:00 ET is already tomorrow —
# tomorrow+1 then clears multi_day_days_threshold and the contract silently
# resolves to [profile:multi_day], which is the opposite of what these tests set
# up. Cost a red deploy gate on 2026-08-21.
_EQ_EXPIRY = datetime.now(ZoneInfo("America/New_York")).date() + timedelta(days=1)
_EQ_OSI = f"AAPL{_EQ_EXPIRY:%y%m%d}C00220000"


def _stopped(pos) -> bool:
    """The stop fired, by either lane. sl_stage_enabled sells half first and
    latches sl_partial_triggered; only the unstaged path sets sl_triggered."""
    return bool(pos.sl_triggered or pos.sl_partial_triggered)


@contextlib.contextmanager
def _sl_gates_open():
    """Open the gates that stand in front of the stop, so these tests exercise
    the trigger logic rather than the wall clock:

      auto_exit_enabled     — false in [trading] of the fixture, exactly as in
                              production. The other monitor tests dodge this by
                              using the SPX contract, whose [profile:spx_index]
                              turns it on — but that same profile sets
                              sl_enabled=false, so it is no use for stop tests.
      sl_market_hours_only  — otherwise the result depends on when pytest runs.
      sl_post_fill_grace    — every seeded position is brand new, so the 30s
                              grace is always active.
    """
    with patch.object(position_monitor, "_AUTO_EXIT_ENABLED", lambda _pos=None: True), \
         patch.object(position_monitor, "_is_within_market_hours", lambda: True), \
         patch.object(position_monitor, "_SL_POST_FILL_GRACE_S", lambda: 0.0):
        yield


def _eq(**overrides):
    """seed_position kwargs for a plain equity contract, so exit knobs resolve
    through [trading] rather than [profile:spx_index] (which has sl_enabled=false)."""
    d = {"osi_symbol": _EQ_OSI, "symbol": "AAPL", "strike": 220,
         "expiry": _EQ_EXPIRY.isoformat(), "option_type": "C",
         "total_contracts": 2, "remaining": 2}
    d.update(overrides)
    return d


def _seed_sell(status="PENDING", trigger="SL", qty=2, filled_qty=0, age_seconds=0):
    """A SELL order row on the standard test contract, aged into the past."""
    session = db.SessionLocal()
    try:
        o = Order(
            public_order_id=f"test-sell-{trigger}-{status}-{age_seconds}",
            osi_symbol="SPX270414C06970000", side="SELL", quantity=qty,
            limit_price=1.50, status=status, trigger=trigger, filled_qty=filled_qty,
            placed_at=datetime.now(timezone.utc) - timedelta(seconds=age_seconds),
        )
        session.add(o)
        session.commit()
        session.refresh(o)
        return o.id
    finally:
        session.close()


class TestAutoExitRepeg(unittest.IsolatedAsyncioTestCase):
    """The three failures that make a poll-fired stop unreliable in a fast market.

    All three are behind [trading] auto_exit_repeg_enabled, so each behaviour is
    asserted twice: OFF must reproduce the legacy behaviour exactly.
    """

    def setUp(self):
        _reset_db()
        _reset_guardrails()
        public_executor.DRY_RUN = True

    def tearDown(self):
        # These tests fire real SL exits, which record a per-root cooldown in
        # guardrails module state. Leave it set and the next file's BUY on the
        # same root is blocked by SL_COOLDOWN.
        _reset_guardrails()

    # ── 1. stop-class exits get a re-peg watcher, profit targets don't ──────

    async def _fire(self, trigger, flag_on):
        pos_id = seed_position(avg_price=2.00, current_price=1.50)
        session = db.SessionLocal()
        try:
            pos = session.query(Position).filter(Position.id == pos_id).one()
            placed = MagicMock(id=999)
            executor = MagicMock()
            executor._compute_limit.return_value = Decimal("1.45")
            executor._place_sell = AsyncMock(return_value=placed)
            with patch.object(position_monitor, "_AUTO_EXIT_REPEG_ENABLED", lambda: flag_on):
                await position_monitor._fire_exit(
                    executor, session, pos, 2, 1.50, trigger, DummyManager())
            return executor
        finally:
            session.close()

    async def test_stop_exit_starts_repeg_watcher(self):
        executor = await self._fire("SL", flag_on=True)
        executor._track_task.assert_called_once()
        executor._repeg_close_all_sell.assert_called_once()
        # The replacement must keep this exit's own trigger, or the re-pegged
        # order loses its [SL] tag and its priority over DISCORD/MANUAL.
        self.assertEqual(executor._repeg_close_all_sell.call_args.kwargs["trigger"], "SL")

    async def test_profit_target_does_not_chase(self):
        # A PT that never fills is forgone profit, not carried risk — and the
        # ladder's final attempt is a marketable bid − 20%, the wrong price to
        # dump a winner at.
        executor = await self._fire("PT1", flag_on=True)
        executor._track_task.assert_not_called()

    async def test_flag_off_leaves_stop_unchased(self):
        executor = await self._fire("SL", flag_on=False)
        executor._track_task.assert_not_called()

    # ── 2. a stale AUTO order stops blocking every other exit ───────────────

    async def _try_displace(self, flag_on, age_seconds, new_trigger="MANUAL", new_qty=2):
        seed_position(avg_price=2.00, current_price=1.50)
        stale_id = _seed_sell(status="PENDING", trigger="SL", qty=2,
                              age_seconds=age_seconds)
        session = db.SessionLocal()
        try:
            pos = session.query(Position).filter(Position.status == "OPEN").one()
            ex = PublicExecutor(DummyManager())
            ex.cancel_order = AsyncMock(return_value={"status": "ok"})
            with patch.object(public_executor, "_AUTO_EXIT_REPEG_ENABLED", lambda: flag_on):
                out = await ex._place_sell(session, pos, new_qty, Decimal("1.40"), new_trigger)
            return out, stale_id, ex
        finally:
            session.close()

    async def test_stale_auto_is_displaced_by_manual(self):
        out, stale_id, ex = await self._try_displace(flag_on=True, age_seconds=60)
        self.assertIsNotNone(out, "MANUAL exit was still blocked by the dead SL order")
        ex.cancel_order.assert_awaited_once()
        self.assertEqual(ex.cancel_order.await_args.args[0], stale_id)

    async def test_fresh_auto_is_not_displaced(self):
        # Same-tick cascade (PT1 then PT2) must still stack rather than have
        # the second rung cancel the first.
        out, _, ex = await self._try_displace(flag_on=True, age_seconds=0)
        self.assertIsNone(out)
        ex.cancel_order.assert_not_awaited()

    async def test_displacement_never_shrinks_the_exit(self):
        # A 1-lot exit must not cancel a pending 2-lot one and leave a contract
        # unprotected, however stale the pending order is.
        out, _, ex = await self._try_displace(flag_on=True, age_seconds=600, new_qty=1)
        self.assertIsNone(out)
        ex.cancel_order.assert_not_awaited()

    async def test_flag_off_keeps_legacy_blocking(self):
        out, _, ex = await self._try_displace(flag_on=False, age_seconds=600)
        self.assertIsNone(out)
        ex.cancel_order.assert_not_awaited()

    # ── 3. a stop whose order died with no fill re-arms ─────────────────────

    def _rearm(self, **order_kw):
        seed_position(avg_price=2.00, current_price=1.50, sl_triggered=True)
        _seed_sell(**order_kw)
        session = db.SessionLocal()
        try:
            pos = session.query(Position).filter(Position.status == "OPEN").one()
            position_monitor._rearm_dead_stop(session, pos)
            session.commit()
            return pos.sl_triggered
        finally:
            session.close()

    def test_cancelled_stop_rearms(self):
        self.assertFalse(self._rearm(status="CANCELLED", trigger="SL"))

    def test_rejected_stop_rearms(self):
        self.assertFalse(self._rearm(status="REJECTED", trigger="SL"))

    def test_filled_stop_stays_disarmed(self):
        self.assertTrue(self._rearm(status="FILLED", trigger="SL", filled_qty=2))

    def test_partially_filled_stop_stays_disarmed(self):
        # Contracts moved. Re-arming here is the oversell path.
        self.assertTrue(self._rearm(status="CANCELLED", trigger="SL", filled_qty=1))

    # ── 4. the stop measures the bid, not the mid ───────────────────────────

    def test_exit_price_is_the_bid(self):
        # spread_pct = (ask-bid)/mid*100. mid 2.00, spread 25% → bid 1.75.
        self.assertAlmostEqual(position_monitor._exit_price(2.00, 25.0), 1.75, places=4)
        self.assertEqual(position_monitor._exit_price(2.00, 0.0), 2.00)

    def test_exit_price_clamps_broken_quote(self):
        # A spread wider than the mid implies a negative bid. Clamp instead —
        # the wide-spread guard is what should handle a quote like this.
        self.assertAlmostEqual(position_monitor._exit_price(2.00, 400.0), 0.20, places=4)

    async def test_sl_fires_on_bid_before_mid(self):
        # Mid is -15% (above a -20% stop) but the bid is -21.25%.
        import app.execution.broker_router as broker_router
        seed_position(**_eq(avg_price=2.00, current_price=2.00))
        orig = broker_router.fetch_prices
        broker_router.fetch_prices = AsyncMock(return_value={_EQ_OSI: 1.70})
        try:
            with patch.object(position_monitor, "_SL_USE_BID_ENABLED", lambda: True), \
                 patch.object(broker_router, "get_last_spread_pct", lambda _osi: 12.5), \
                 _sl_gates_open():
                await position_monitor._check_all_positions(DummyManager(), PublicExecutor(DummyManager()))
        finally:
            broker_router.fetch_prices = orig
        session = db.SessionLocal()
        try:
            self.assertTrue(_stopped(session.query(Position).one()))
        finally:
            session.close()

    async def test_sl_holds_on_mid_when_flag_off(self):
        import app.execution.broker_router as broker_router
        seed_position(**_eq(avg_price=2.00, current_price=2.00))
        orig = broker_router.fetch_prices
        broker_router.fetch_prices = AsyncMock(return_value={_EQ_OSI: 1.70})
        try:
            with patch.object(position_monitor, "_SL_USE_BID_ENABLED", lambda: False), \
                 patch.object(broker_router, "get_last_spread_pct", lambda _osi: 12.5), \
                 _sl_gates_open():
                await position_monitor._check_all_positions(DummyManager(), PublicExecutor(DummyManager()))
        finally:
            broker_router.fetch_prices = orig
        session = db.SessionLocal()
        try:
            self.assertFalse(_stopped(session.query(Position).one()))
        finally:
            session.close()

    # ── 5. the wide-spread defer is bounded ─────────────────────────────────

    async def _tick_wide_spread(self, cap):
        import app.execution.broker_router as broker_router
        orig = broker_router.fetch_prices
        broker_router.fetch_prices = AsyncMock(return_value={_EQ_OSI: 0.90})
        try:
            with patch.object(position_monitor, "_WIDE_SPREAD_MAX_DEFER_TICKS", lambda: cap), \
                 patch.object(broker_router, "get_last_spread_pct", lambda _osi: 99.0), \
                 _sl_gates_open():
                await position_monitor._check_all_positions(DummyManager(), PublicExecutor(DummyManager()))
        finally:
            broker_router.fetch_prices = orig
        session = db.SessionLocal()
        try:
            return _stopped(session.query(Position).one())
        finally:
            session.close()

    async def test_wide_spread_defers_then_fires(self):
        position_monitor._wide_spread_defers.clear()
        seed_position(**_eq(avg_price=2.00, current_price=2.00))
        self.assertFalse(await self._tick_wide_spread(cap=2), "fired on the first wide tick")
        self.assertTrue(await self._tick_wide_spread(cap=2), "still deferring past the cap")

    async def test_wide_spread_defers_forever_when_cap_zero(self):
        position_monitor._wide_spread_defers.clear()
        seed_position(**_eq(avg_price=2.00, current_price=2.00))
        for _ in range(4):
            self.assertFalse(await self._tick_wide_spread(cap=0))

    # ── 6. a contract that goes dark is reported ────────────────────────────

    async def test_quote_gap_alarms_once(self):
        import app.execution.broker_router as broker_router
        position_monitor._quote_gaps.clear()
        seed_position(**_eq(avg_price=2.00, current_price=2.00))
        orig = broker_router.fetch_prices
        broker_router.fetch_prices = AsyncMock(return_value={})     # symbol went dark
        crit = AsyncMock()
        try:
            with patch.object(position_monitor, "_QUOTE_GAP_ALARM_TICKS", lambda: 2), \
                 patch.object(position_monitor, "_is_within_market_hours", lambda: True), \
                 patch.object(position_monitor.trade_logger, "log_critical", crit):
                for _ in range(4):
                    await position_monitor._check_all_positions(
                        DummyManager(), PublicExecutor(DummyManager()))
                    await asyncio.sleep(0)      # let the fire-and-forget task run
        finally:
            broker_router.fetch_prices = orig
        self.assertEqual(crit.await_count, 1, "alarm should fire once per outage, not per tick")

    async def test_quote_gap_silent_outside_market_hours(self):
        import app.execution.broker_router as broker_router
        position_monitor._quote_gaps.clear()
        seed_position(**_eq(avg_price=2.00, current_price=2.00))
        orig = broker_router.fetch_prices
        broker_router.fetch_prices = AsyncMock(return_value={})
        crit = AsyncMock()
        try:
            with patch.object(position_monitor, "_QUOTE_GAP_ALARM_TICKS", lambda: 1), \
                 patch.object(position_monitor, "_is_within_market_hours", lambda: False), \
                 patch.object(position_monitor.trade_logger, "log_critical", crit):
                for _ in range(3):
                    await position_monitor._check_all_positions(
                        DummyManager(), PublicExecutor(DummyManager()))
                    await asyncio.sleep(0)
        finally:
            broker_router.fetch_prices = orig
        crit.assert_not_awaited()

    # ── IBKR gets every one of the above by inheritance ─────────────────────

    def test_ibkr_inherits_the_exit_path(self):
        """The live box is IBKR. IBKRExecutor overrides placement and fill
        polling only — if it ever overrode these, the fixes above would silently
        apply to Public.com and not to the account that actually trades."""
        from app.execution.ibkr_broker import IBKRExecutor
        for name in ("_place_sell", "_repeg_close_all_sell", "_compute_limit"):
            self.assertIs(
                getattr(IBKRExecutor, name), getattr(PublicExecutor, name),
                f"IBKRExecutor overrides {name} — re-verify the exit fixes against it",
            )
        # cancel_order IS overridden; the re-peg lane depends on it returning
        # the same 3-way contract.
        self.assertIsNot(IBKRExecutor.cancel_order, PublicExecutor.cancel_order)
        import inspect
        self.assertIn("require_confirmed_terminal",
                      inspect.signature(IBKRExecutor.cancel_order).parameters)

    def test_ibkr_terminal_statuses_are_rearmable(self):
        """Every terminal status IBKR can write must be one _rearm_dead_stop
        recognises, or a dead IBKR stop never re-arms."""
        from app.execution.ibkr_broker import _IB_STATUS_MAP
        for status in set(_IB_STATUS_MAP.values()) - {"PENDING", "FILLED", "PARTIALLY_FILLED"}:
            self.assertIn(status, position_monitor._ORDER_DEAD)

    def test_pending_sell_blocks_rearm(self):
        seed_position(avg_price=2.00, current_price=1.50, sl_triggered=True)
        _seed_sell(status="CANCELLED", trigger="SL")
        _seed_sell(status="PENDING", trigger="MANUAL")
        session = db.SessionLocal()
        try:
            pos = session.query(Position).filter(Position.status == "OPEN").one()
            position_monitor._rearm_dead_stop(session, pos)
            self.assertTrue(pos.sl_triggered, "re-armed while a SELL was still working")
        finally:
            session.close()


# ═════════════════════════════════════════════════════════════════════════════
# EXIT PLAN (what the dashboard row promises)
# ═════════════════════════════════════════════════════════════════════════════

class TestExitPlan(unittest.TestCase):
    """The row used to read pt1_pct / sl_pct out of [trading], so a position on
    a profile was shown numbers that could never apply to it. These pin the
    three ways that was wrong."""

    def _pos(self, **kw):
        d = {"osi_symbol": _EQ_OSI, "symbol": "AAPL", "expiry": _EQ_EXPIRY.isoformat(),
             "strike": 220, "option_type": "C", "total_contracts": 2, "remaining": 2,
             "avg_price": 2.00, "current_price": 2.00, "highest_price": 2.00,
             "status": "OPEN"}
        d.update(kw)
        return Position(**d)

    def test_numbers_come_from_the_positions_own_profile(self):
        # A far expiry resolves to [profile:multi_day] (sl_pct -40 in the
        # fixture); a near one falls through to [trading] (-20). Same code path
        # the monitor uses, so the row can no longer disagree with the engine.
        near = self._pos().exit_plan()
        far = self._pos(osi_symbol="AAPL280121C00220000", expiry="2028-01-21").exit_plan()
        self.assertEqual(near["profile"], "trading")
        self.assertEqual(far["profile"], "multi_day")
        self.assertNotEqual(near["stop"]["pct"], far["stop"]["pct"])

    def test_verdict_leads_and_says_nothing_fires(self):
        # The row leads with one sentence about THIS position, not a list of
        # levels. With auto-exit off that sentence has to say so.
        plan = self._pos().exit_plan()
        self.assertFalse(plan["autoExit"])
        self.assertTrue(plan["inert"])
        self.assertEqual(plan["verdict"], plan["summary"][0])
        self.assertIn("auto-exit is OFF", plan["verdict"])

    def test_verdict_measures_distance_from_here_not_from_entry(self):
        # A "+5% stop" on a position sitting at -59% is 159% away and can never
        # fire. Quoting it off the entry price is what made the row useless.
        deep = self._pos(avg_price=3.70, current_price=1.50, highest_price=5.12)
        plan = deep.exit_plan()
        self.assertTrue(plan["inert"])
        self.assertGreater(plan["nearestMovePct"], 100)
        stop = plan["stop"]
        if stop["ratcheted"]:
            self.assertIn("out of reach", " ".join(plan["summary"]))

    def test_chained_rung_is_marked_unreachable(self):
        # PT3 needs PT2, which needs PT1. Turning PT2 off silently kills PT3 —
        # the row now says so instead of drawing it like a live rung.
        import app.core.profile_resolver as pr
        real = pr.pcfg_get
        with patch.object(pr, "pcfg_get",
                          side_effect=lambda o, k, f=None: "false" if k == "pt2_enabled" else real(o, k, f)):
            plan = self._pos().exit_plan()
        pt3 = next(r for r in plan["rungs"] if r["key"] == "PT3")
        self.assertEqual(pt3["state"], "unreachable")
        self.assertEqual(pt3["blockedBy"], "PT2")

    def test_ratcheted_stop_is_reported_as_a_profit_exit(self):
        # peak +40% arms the breakeven lock, so the "stop" now sells in profit
        # while still logging as SL. That is the behaviour that reads as a loss
        # stop in the logs and confused the whole book.
        plan = self._pos(avg_price=2.00, highest_price=2.80).exit_plan()
        st = plan["stop"]
        if st["ratcheted"]:
            self.assertGreater(st["pct"], st["basePct"])
            self.assertTrue(any("ratcheted ABOVE break-even" in s for s in plan["summary"]))

    def test_override_beats_the_profile_default(self):
        # The whole point: one position carries its own numbers without the
        # config, or any other position, changing.
        base = self._pos().exit_plan()
        over = self._pos(sl_pct_override=-30.0, pt_pct_override=35.0).exit_plan()
        self.assertEqual(over["stop"]["basePct"], -30.0)
        self.assertEqual(next(r for r in over["rungs"] if r["key"] == "PT1")["pct"], 35.0)
        self.assertNotEqual(over["stop"]["basePct"], base["stop"]["basePct"])
        self.assertTrue(over["editable"]["stop"]["isOverride"])
        self.assertFalse(base["editable"]["stop"]["isOverride"])

    def test_null_override_keeps_following_the_default(self):
        # "Follow unless overridden": an untouched position must report the
        # config value as its own, so a later config change still reaches it.
        e = self._pos().exit_plan()["editable"]
        self.assertEqual(e["stop"]["pct"], e["stop"]["defaultPct"])
        self.assertEqual(e["target"]["pct"], e["target"]["defaultPct"])
        self.assertFalse(e["target"]["isOverride"])

    def test_auto_exit_override_arms_one_position(self):
        # Global auto_exit is false in the fixture; the override must win, or
        # arming a single runner is impossible without arming the whole book.
        self.assertFalse(self._pos().exit_plan()["autoExit"])
        self.assertTrue(self._pos(auto_exit_override=True).exit_plan()["autoExit"])
        self.assertTrue(
            position_monitor._AUTO_EXIT_ENABLED(self._pos(auto_exit_override=True)))
        self.assertFalse(position_monitor._AUTO_EXIT_ENABLED(self._pos()))

    def test_monitor_and_row_read_the_same_override(self):
        # The row promising -30% while the engine stops at the profile's -40%
        # is the exact class of bug this whole feature exists to kill.
        pos = self._pos(sl_pct_override=-30.0)
        self.assertEqual(position_monitor._override(pos, "sl_pct_override"), -30.0)
        self.assertEqual(pos.exit_plan()["stop"]["basePct"], -30.0)

    def test_hand_armed_trail_is_not_reported_as_inert(self):
        # position_monitor runs the manual trail OUTSIDE the auto-exit gate and
        # without waiting for PT1 — pressing the button IS the arm signal. The
        # row read only the config flag, so arming the trail on a pure-relay
        # position changed nothing on screen: it still said nothing could fire.
        peak = 2.00 * 1.60                      # +60%, well past a +30% arm
        armed = self._pos(avg_price=2.00, current_price=2.90,
                          highest_price=peak, trail_enabled=True)
        plan = armed.exit_plan()
        self.assertFalse(plan["autoExit"])      # fixture keeps auto-exit off
        self.assertTrue(plan["trail"]["manual"])
        self.assertTrue(plan["trail"]["armed"])
        self.assertFalse(plan["inert"])
        self.assertNotIn("Nothing here can fire", plan["verdict"])
        self.assertIn("trailing stop", plan["verdict"].lower())
        # Untouched position on the same numbers still reports inert.
        self.assertTrue(self._pos(avg_price=2.00, current_price=2.90,
                                  highest_price=peak).exit_plan()["inert"])

    def test_hand_armed_trail_below_its_arm_says_so(self):
        # Armed by hand but the peak has not reached the arm yet: the row must
        # not claim it is following anything.
        plan = self._pos(avg_price=2.00, current_price=2.05, highest_price=2.06,
                         trail_enabled=True).exit_plan()
        self.assertTrue(plan["trail"]["manual"])
        self.assertFalse(plan["trail"]["armed"])
        self.assertIn("starts following once the peak passes", plan["verdict"])

    def test_hard_exit_sells_the_whole_position(self):
        # "Take profit at +30%" and "get me all the way out at +30%" are
        # different instructions. The row could only express the first until
        # the sell fraction got its own override, so a hard exit was
        # unreachable from the dashboard.
        e = self._pos(pt_pct_override=30.0, pt_sell_override=1.0).exit_plan()
        self.assertEqual(e["editable"]["target"]["sellFrac"], 1.0)
        self.assertTrue(e["editable"]["target"]["sellIsOverride"])
        self.assertEqual(e["editable"]["target"]["sells"], "everything")
        pt1 = next(r for r in e["rungs"] if r["key"] == "PT1")
        self.assertEqual(pt1["sell"], "the whole position")
        # …and the monitor must agree, or the row promises a full exit while
        # the engine sells half.
        self.assertEqual(
            position_monitor._override(self._pos(pt_sell_override=1.0), "pt_sell_override"), 1.0)

    def test_sell_fraction_follows_the_default_until_set(self):
        e = self._pos().exit_plan()["editable"]["target"]
        self.assertEqual(e["sellFrac"], e["defaultSellFrac"])
        self.assertFalse(e["sellIsOverride"])

    def test_plan_rides_along_in_to_dict(self):
        self.assertIn("exitPlan", self._pos().to_dict())

    def test_plan_never_breaks_the_row(self):
        # to_dict feeds every WS broadcast; a malformed position must degrade to
        # exitPlan=None, not take the dashboard down.
        broken = Position(osi_symbol="???", avg_price=0, remaining=0)
        self.assertIn("exitPlan", broken.to_dict())


# ═════════════════════════════════════════════════════════════════════════════
# CLEANUP
# ═════════════════════════════════════════════════════════════════════════════

def tearDownModule():
    db.engine.dispose()
    if os.path.exists(TEST_DB_PATH):
        try:
            os.remove(TEST_DB_PATH)
        except PermissionError:
            pass


if __name__ == "__main__":
    unittest.main(verbosity=2)


class TestBlacklistedOwnerExit(unittest.TestCase):
    """A blacklisted author must still be able to exit a position they hold.

    2026-09-17 SPCX 9/18 160C: the dashboard's manual resubmit overrode the
    blacklist and opened a position stamped 'spacemonkey'. His own "ALL OUT"
    was then dropped at ingest by that same blacklist, and every other
    analyst's SELL was refused by position_owner_scope=author — nothing
    automated could close it and it was sold by hand at -30.2%.
    """

    OSI = "SPCX260918C00160000"

    def setUp(self):
        _reset_db()
        _reset_guardrails()
        self._orig_market = guardrails.MARKET_HOURS_ONLY
        guardrails.MARKET_HOURS_ONLY = False
        self._orig_bl = guardrails._ANALYST_BLACKLIST
        self._orig_hours = guardrails._MARKET_HOURS_ONLY
        guardrails._ANALYST_BLACKLIST = lambda: {"spacemonkey"}
        guardrails._MARKET_HOURS_ONLY = lambda: False

    def tearDown(self):
        guardrails.MARKET_HOURS_ONLY = self._orig_market
        guardrails._ANALYST_BLACKLIST = self._orig_bl
        guardrails._MARKET_HOURS_ONLY = self._orig_hours

    def _open_position(self, author="spacemonkey"):
        s = db.SessionLocal()
        try:
            s.add(Position(osi_symbol=self.OSI, symbol="SPCX", strike=160.0,
                           option_type="C", total_contracts=1, remaining=1,
                           avg_price=0.96, current_price=0.96, status="OPEN",
                           author=author))
            s.commit()
        finally:
            s.close()

    def test_their_sell_passes_when_they_hold_it(self):
        self._open_position()
        ok, reason = guardrails.check_guardrails(
            "SELL", self.OSI, Decimal("0.57"), 1, "spacemonkey")
        self.assertTrue(ok, reason)

    def test_their_sell_is_still_blocked_with_nothing_open(self):
        ok, reason = guardrails.check_guardrails(
            "SELL", self.OSI, Decimal("0.57"), 1, "spacemonkey")
        self.assertFalse(ok)
        self.assertIn("BLACKLIST", reason.upper())

    def test_they_cannot_exit_someone_elses_position(self):
        self._open_position(author="Sarang")
        ok, reason = guardrails.check_guardrails(
            "SELL", self.OSI, Decimal("0.57"), 1, "spacemonkey")
        self.assertFalse(ok)
        self.assertIn("BLACKLIST", reason.upper())

    def test_blank_symbol_close_all_stays_blocked(self):
        """The nuclear button is what the blacklist was built for."""
        self._open_position()
        ok, reason = guardrails.check_guardrails(
            "SELL", "", Decimal("0.57"), 1, "spacemonkey")
        self.assertFalse(ok)
        self.assertIn("BLACKLIST", reason.upper())

    def test_the_knob_reverts_to_the_old_behaviour(self):
        self._open_position()
        orig = guardrails._BLACKLIST_ALLOWS_OWNER_EXIT
        guardrails._BLACKLIST_ALLOWS_OWNER_EXIT = lambda: False
        try:
            ok, reason = guardrails.check_guardrails(
                "SELL", self.OSI, Decimal("0.57"), 1, "spacemonkey")
        finally:
            guardrails._BLACKLIST_ALLOWS_OWNER_EXIT = orig
        self.assertFalse(ok)
        self.assertIn("BLACKLIST", reason.upper())

    def test_a_buy_from_them_is_still_blacklisted(self):
        """Only exits are carved out — an entry must stay blocked."""
        self._open_position()
        ok, reason = guardrails.check_guardrails(
            "BUY", self.OSI, Decimal("1.00"), 1, "spacemonkey")
        self.assertFalse(ok)
        self.assertIn("BLACKLIST", reason.upper())
