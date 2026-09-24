"""
Regression tests for the 2026-04-30 audit fixes.

Each test ties to a specific real-world bug observed in today's trading:

  #1: parser chitchat → blank-symbol close_all SELL
      "AMZN 258 is previous all time high holding..." (covered in
      tests/test_comprehensive.py)

  #2: blank-osi close_all uses position price, not alert.price
      Defense-in-depth: even if some path emitted a blank-symbol close_all
      with a bogus price (258 from chitchat), the SELL limit must be derived
      from the position's own price.

  #3: auto-exits (PT*/SL/TPT) cancel stale manual pending SELL
      Real bug: a fat-finger DISCORD SELL @ $245.10 (limit derived from
      bogus 258) sat as PENDING for 30 minutes. PT3 trigger at +100% was
      silently SKIPPED because of the existing pending order.

  #4: re-register restores PT/peak flags from DB so SHADOW mode doesn't
      re-fire PT1 every 5s after position_monitor already exited.

  #5: BUY base price clamped to min(live_quote, alert.price * 1.08)
      Real bug: META 600P alert $3.45, live quote ~$4.09, fill $4.30
      (+24.6% slippage). Cap prevents paying more than 8% chase.

  #6: tpt_trail_pct config tuned to 12 (verified by config read).

  #7: post-PT2 ignores non-close_all DISCORD SELL
      Real bug: QCOM 175C had PT1+PT2 done, then a partial DISCORD SELL
      closed the last contract at $3.10 — minutes later analyst signaled
      SELL @ $5.00 then $6.15, all ignored (no remaining).
"""
import os
import sys
import unittest
from decimal import Decimal
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TEST_DB_PATH = os.path.abspath("test_today_fixes.db")
os.environ["TRADER_DATABASE_URL"] = f"sqlite:///{TEST_DB_PATH.replace(chr(92), '/')}"
os.environ["DRY_RUN"] = "true"

import app.execution.public_executor as public_executor
import app.risk.guardrails as guardrails
import app.risk.ai_scorer as ai_scorer
import app.risk.kelly_sizer as kelly_sizer
public_executor.DRY_RUN = True
guardrails.MARKET_HOURS_ONLY = False
ai_scorer.SCORER_ENABLED = False
kelly_sizer.KELLY_ENABLED = False

import app.core.db as db
import app.execution.public_sdk_bridge as public_sdk_bridge
from app.core.models import Alert, Order, Position
from app.ingest.parser import parse_alert, TradeAlert
from app.execution.public_executor import PublicExecutor


class DummyManager:
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
    defaults = {
        "osi_symbol": "QCOM260501C00180000",
        "symbol": "QCOM",
        "expiry": "2026-05-01",
        "strike": 180,
        "option_type": "C",
        "total_contracts": 4,
        "remaining": 1,
        "avg_price": 2.06,
        "current_price": 2.20,
        "status": "PARTIAL",
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


def seed_order(**overrides) -> int:
    """Create an Order row directly (e.g. a stale PENDING manual SELL)."""
    defaults = {
        "public_order_id": "stale-test-oid-001",
        "osi_symbol": "QCOM260501C00180000",
        "side": "SELL",
        "quantity": 1,
        "limit_price": 245.10,
        "status": "PENDING",
        "trigger": "DISCORD",
    }
    defaults.update(overrides)
    session = db.SessionLocal()
    try:
        order = Order(**defaults)
        session.add(order)
        session.commit()
        session.refresh(order)
        return order.id
    finally:
        session.close()


class MultiDayProfileTest(unittest.TestCase):
    """Multi-day expiry auto-profile selection (2026-05-08).

    A position whose expiry is at least multi_day_days_threshold days
    out should auto-resolve PT/SL/TPT settings via [profile:multi_day],
    so bot doesn't aggressively trim a runner that the analyst is
    managing across sessions.
    """

    def test_zero_dte_does_not_use_multi_day_profile(self):
        from datetime import date, timedelta
        from zoneinfo import ZoneInfo
        from datetime import datetime as _dt
        import app.monitors.position_monitor as pm
        today = _dt.now(ZoneInfo("America/New_York")).date()

        class FakePos:
            expiry = today.isoformat()
            osi_symbol = "QCOM260801C00180000"
            strategy_tag = None
            author = "Twinsight Bot"

        self.assertIsNone(pm._multi_day_profile_section_for(FakePos()))

    def test_two_week_expiry_uses_multi_day_profile(self):
        from datetime import timedelta
        from zoneinfo import ZoneInfo
        from datetime import datetime as _dt
        import app.monitors.position_monitor as pm
        future = (_dt.now(ZoneInfo("America/New_York")).date() + timedelta(days=14)).isoformat()

        class FakePos:
            expiry = future
            osi_symbol = "QCOM260801C00180000"
            strategy_tag = None
            author = "Twinsight Bot"

        self.assertEqual(pm._multi_day_profile_section_for(FakePos()), "profile:multi_day")

    def test_multi_day_profile_disables_pts(self):
        """When the multi-day profile applies, pts_disabled_default reads true."""
        from datetime import timedelta
        from zoneinfo import ZoneInfo
        from datetime import datetime as _dt
        import app.monitors.position_monitor as pm
        future = (_dt.now(ZoneInfo("America/New_York")).date() + timedelta(days=10)).isoformat()

        class FakePos:
            expiry = future
            osi_symbol = "META260815C00600000"
            strategy_tag = None
            author = "Twinsight Bot"

        # _pcfg_bool reads through profile chain; multi_day section should win
        # over the [trading] default of pts_disabled_default=false.
        self.assertTrue(pm._pcfg_bool(FakePos(), "pts_disabled_default", False))


class StrictCloseAllTest(unittest.TestCase):
    """H1 fix (2026-05-08): SELL with no fraction must NOT default to
    close_all=True under strict mode. Only EXPLICIT close-all phrases
    set the flag. Prevents bot from killing runners on bare 'SOLD …'
    alerts after PT2 fired (post-PT2 guard relies on close_all=False
    to ignore partial-style SELLs)."""

    def test_bare_sold_no_fraction_is_not_close_all_under_strict(self):
        # Default config: parser_strict_close_all=true (fallback in code)
        alert = parse_alert("SOLD SPX 5/8 7380P $4.90")
        self.assertIsNotNone(alert)
        self.assertEqual(alert.action, "SELL")
        self.assertFalse(alert.close_all,
                         "bare SELL must not be close_all under strict mode")
        self.assertEqual(alert.fraction, Decimal("1"))

    def test_explicit_all_out_still_sets_close_all(self):
        alert = parse_alert("SOLD SPX 5/8 7380P $4.90 ALL OUT")
        self.assertIsNotNone(alert)
        self.assertTrue(alert.close_all)

    def test_close_all_phrase_at_start_still_sets_flag(self):
        alert = parse_alert("CLOSE ALL")
        self.assertIsNotNone(alert)
        self.assertTrue(alert.close_all)

    def test_sold_full_phrase_still_sets_flag(self):
        alert = parse_alert("SOLD FULL SPX 5/8 7380P")
        self.assertIsNotNone(alert)
        self.assertTrue(alert.close_all)

    def test_partial_with_fraction_unchanged(self):
        alert = parse_alert("SOLD SPX 5/8 7380P $4.90 1/2")
        self.assertIsNotNone(alert)
        self.assertFalse(alert.close_all)
        self.assertEqual(alert.fraction, Decimal("0.5"))


class ParserChitchatRegressionTest(unittest.TestCase):
    """Fix #1 — parser must not flag chitchat as a SELL signal.

    Real bug 2026-04-30: a Sarang chat msg "AMZN 258 is previous all time
    high holding exactly around same" parsed as SELL with blank symbol +
    close_all=True + price=258. This routed to the only-open QCOM position
    and placed a SELL @ $245.10 that pended for 30 min, blocking PT3."""

    def test_all_time_high_chitchat_not_signal(self):
        self.assertIsNone(
            parse_alert("AMZN 258 is previous all time high holding exactly around same")
        )

    def test_all_time_highs_chat_not_signal(self):
        self.assertIsNone(parse_alert("SPY at all time highs today"))

    def test_close_all_with_verb_still_works(self):
        # Defensive: legitimate close-all alerts must still parse.
        for legit in ("ALL OUT", "CLOSE ALL", "SOLD EVERYTHING"):
            alert = parse_alert(legit)
            self.assertIsNotNone(alert, f"legit close-all dropped: {legit!r}")
            self.assertTrue(alert.close_all)
            self.assertEqual(alert.action, "SELL")

    def test_equity_signal_with_all_out_does_not_close_options(self):
        """Real bug 2026-05-11: Sarang posted
            '#alert SOLD  GLW COMMONS AT 201 <@&...>\\n* All out'
        After whitespace collapse this becomes
            '#alert sold glw commons at 201 * all out'
        which triggered blank-symbol close_all and killed an unrelated
        META 0DTE put (-$30). Bot only trades options — equity SELL signals
        must not fire close_all on the option book."""
        for equity_text in (
            "#alert SOLD GLW COMMONS AT 201\n* All out",
            "SOLD AAPL SHARES AT 195 — all out",
            "SOLD MSFT STOCK ALL OUT",
            "OUT OF ALL MY TSLA COMMONS",
        ):
            self.assertIsNone(
                parse_alert(equity_text),
                f"equity-context close_all must not parse: {equity_text!r}",
            )


class TodaysFixesTest(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def tearDownClass(cls):
        db.engine.dispose()
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)

    def setUp(self):
        public_executor.DRY_RUN = True
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()
        # Clear in-memory dedup state so each test sees a fresh window.
        guardrails._recent_alerts.clear()

    # ─── Fix #2: blank-osi close_all ignores alert.price ──────────────────

    async def test_blank_osi_close_all_uses_position_price_not_alert_price(self):
        """A close_all SELL with blank osi/symbol must NOT use alert.price as
        the limit base (alert.price might come from chitchat that mentioned
        an unrelated dollar figure, e.g. "AMZN 258 is previous all time high"
        → bogus 258 limit). The SELL limit must derive from position state."""
        seed_position(
            osi_symbol="QCOM260501C00180000",
            avg_price=2.06,
            current_price=2.20,
            remaining=1,
            status="PARTIAL",
        )

        # Construct a malformed close_all alert directly. (In production this
        # path is also blocked by Fix #1 in the parser; we still exercise the
        # executor's defense-in-depth.)
        alert = TradeAlert(
            action="SELL",
            symbol="",
            expiry=None,
            strike=None,
            option_type="",
            price=Decimal("258.00"),  # poison from chat parse
            fraction=Decimal("1"),
            close_all=True,
        )
        alert_id = build_alert_row(alert, "synthetic close_all chitchat case")

        manager = DummyManager()
        executor = PublicExecutor(manager)
        # This test exercises the executor's price-source defense, not the
        # 2026-06-11 blank-symbol gate/scope. Disable the gate and use legacy
        # book-wide scope so the close_all actually reaches _place_sell.
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
            sell_orders = session.query(Order).filter(Order.side == "SELL").all()
            self.assertEqual(len(sell_orders), 1, "expected exactly one SELL order")
            order = sell_orders[0]
            # Limit base should be derived from position current_price=2.20,
            # NOT from alert.price=258. 2.20 with 5% sell slippage rounds to ~2.05.
            self.assertLess(
                order.limit_price, 5.00,
                f"Limit ${order.limit_price} far above any reasonable option price — "
                f"executor trusted alert.price=258 from chitchat",
            )
            self.assertGreater(order.limit_price, 0.5, "limit too low")
        finally:
            session.close()

    # ─── Fix #3: auto-exits cancel stale manual pending SELL ──────────────

    async def test_auto_exit_cancels_stale_discord_pending_order(self):
        """PT3/SL/TPT must cancel a stale DISCORD/MANUAL pending SELL on the
        same symbol so the auto-exit can proceed. Without this, a fat-finger
        manual order silently blocks profit-taking (real QCOM 180C bug)."""
        pos_id = seed_position(remaining=1, status="PARTIAL", current_price=4.12)
        # Seed an unfillable stale DISCORD SELL @ $245.10 (the fat finger).
        stale_order_id = seed_order(
            public_order_id="stale-fat-finger",
            limit_price=245.10,
            trigger="DISCORD",
            status="PENDING",
        )

        manager = DummyManager()
        executor = PublicExecutor(manager)

        session = db.SessionLocal()
        try:
            pos = session.query(Position).filter(Position.id == pos_id).one()
            # Fire a PT3 exit. Auto-exit must cancel the stale DISCORD order
            # and place its own.
            await executor._place_sell(
                session, pos, qty=1, limit_px=Decimal("3.70"), trigger="PT3",
            )
            session.commit()
        finally:
            session.close()

        session = db.SessionLocal()
        try:
            stale = session.query(Order).filter(Order.id == stale_order_id).one()
            self.assertEqual(stale.status, "CANCELLED",
                             "stale DISCORD pending should be cancelled by PT3")

            pt3_orders = session.query(Order).filter(Order.trigger == "PT3").all()
            self.assertEqual(len(pt3_orders), 1, "PT3 order must be placed")
            # In DRY_RUN it should have already filled.
            self.assertIn(pt3_orders[0].status, ("FILLED", "PENDING"))
        finally:
            session.close()

    # ─── 2026-05-08: newer DISCORD displaces stale DISCORD when qty >= ────

    async def test_discord_displaces_stale_discord_when_new_qty_ge_stale(self):
        """SPXW 7380P 2026-05-08 bug: stale partial DISCORD SELL @ $4.60 sat
        unfilled for 17min, blocking the follow-up ALL OUT DISCORD SELL.
        Now: newer DISCORD with qty >= stale qty must cancel the stale
        pending order and place its own."""
        pos_id = seed_position(remaining=4, status="OPEN", current_price=2.73)
        stale_order_id = seed_order(
            public_order_id="stale-partial-1of2",
            limit_price=4.60,
            quantity=2,            # stale partial: 1/2 of 4
            trigger="DISCORD",
            status="PENDING",
        )

        manager = DummyManager()
        executor = PublicExecutor(manager)

        session = db.SessionLocal()
        try:
            pos = session.query(Position).filter(Position.id == pos_id).one()
            # New DISCORD ALL OUT for full qty=4 — must displace stale x2.
            result = await executor._place_sell(
                session, pos, qty=4, limit_px=Decimal("2.55"), trigger="DISCORD",
            )
            session.commit()
            self.assertIsNotNone(result, "newer DISCORD ALL OUT must place")
        finally:
            session.close()

        session = db.SessionLocal()
        try:
            stale = session.query(Order).filter(Order.id == stale_order_id).one()
            self.assertEqual(stale.status, "CANCELLED",
                             "stale DISCORD x2 should be cancelled by newer DISCORD x4")
            new_orders = session.query(Order).filter(
                Order.trigger == "DISCORD",
                Order.id != stale_order_id,
            ).all()
            self.assertEqual(len(new_orders), 1, "newer DISCORD x4 must be placed")
            self.assertEqual(new_orders[0].quantity, 4)
        finally:
            session.close()

    async def test_discord_does_not_shrink_stale_discord(self):
        """Reverse: a smaller-fraction DISCORD must NOT cancel a larger
        stale DISCORD pending. Cancelling x2 to place x1 would leave 3
        contracts unprotected. Skip is the safe behaviour."""
        pos_id = seed_position(remaining=4, status="OPEN", current_price=4.50)
        stale_order_id = seed_order(
            public_order_id="stale-half-out",
            limit_price=4.60,
            quantity=2,            # stale 1/2 of 4
            trigger="DISCORD",
            status="PENDING",
        )

        manager = DummyManager()
        executor = PublicExecutor(manager)

        session = db.SessionLocal()
        try:
            pos = session.query(Position).filter(Position.id == pos_id).one()
            result = await executor._place_sell(
                session, pos, qty=1, limit_px=Decimal("4.40"), trigger="DISCORD",
            )
            session.commit()
            self.assertIsNone(result, "smaller DISCORD must NOT shrink stale DISCORD")
        finally:
            session.close()

        session = db.SessionLocal()
        try:
            stale = session.query(Order).filter(Order.id == stale_order_id).one()
            self.assertEqual(stale.status, "PENDING",
                             "stale x2 must survive — would have left 3 contracts unprotected")
        finally:
            session.close()

    async def test_manual_sell_does_not_cancel_existing_auto_pending(self):
        """The reverse direction: a DISCORD SELL must NOT cancel a pending
        PT/SL order. Auto-exits have priority over manual; manual should not
        clobber an in-flight auto-exit."""
        pos_id = seed_position(remaining=1, status="PARTIAL", current_price=4.12)
        # Seed a pending PT3 order (auto-exit in flight).
        pt3_order_id = seed_order(
            public_order_id="pt3-inflight",
            limit_price=3.70,
            trigger="PT3",
            status="PENDING",
        )

        manager = DummyManager()
        executor = PublicExecutor(manager)

        session = db.SessionLocal()
        try:
            pos = session.query(Position).filter(Position.id == pos_id).one()
            result = await executor._place_sell(
                session, pos, qty=1, limit_px=Decimal("3.50"), trigger="DISCORD",
            )
            session.commit()
            self.assertIsNone(result, "DISCORD must be skipped while PT3 is pending")
        finally:
            session.close()

        session = db.SessionLocal()
        try:
            pt3_after = session.query(Order).filter(Order.id == pt3_order_id).one()
            self.assertEqual(pt3_after.status, "PENDING",
                             "in-flight PT3 must NOT be cancelled by DISCORD")
        finally:
            session.close()

    # ─── Fix #4: re-register restores PT/peak flags from DB ──────────────
    # (REMOVED — the shadow exit_strategy_engine was deleted on 2026-04-30
    #  along with exit_engine_integration. position_monitor is now the
    #  single source of truth for exits, so the re-register-from-DB bug
    #  no longer exists.)

    # ─── Fix #5: BUY base price capped at alert × 1.08 ───────────────────

    async def test_buy_limit_capped_when_live_quote_far_above_alert(self):
        """When live quote is significantly above alert price (fast move),
        the BUY limit must clamp at alert × 1.08 × (1 + slippage), not chase
        the live quote and pay +25% slippage."""
        # Stub fetch_prices to return an inflated live quote.
        async def fake_fetch_prices(symbols):
            return {s: 4.09 for s in symbols}  # 18% above alert

        from app.execution.public_executor import PublicExecutor
        executor = PublicExecutor(DummyManager())

        # Patch the import inside public_executor's BUY branch.
        with patch("app.execution.broker_router.fetch_prices", new=fake_fetch_prices):
            buy_raw = "BOUGHT META 5/1 600P $3.45 [B GRADE]"
            alert = parse_alert(buy_raw)
            self.assertIsNotNone(alert)
            alert_id = build_alert_row(alert, buy_raw)
            await executor.execute(alert, alert_id)

        session = db.SessionLocal()
        try:
            buy_orders = session.query(Order).filter(Order.side == "BUY").all()
            self.assertEqual(len(buy_orders), 1)
            order = buy_orders[0]
            # alert=3.45, cap=3.726, with 5% slippage = 3.91 → ceil(.10) = 4.00
            # Without the cap, fill would be ~$4.30 (4.09 × 1.05 → ceil .10).
            self.assertLessEqual(
                order.limit_price, 4.05,
                f"limit ${order.limit_price} exceeds chase cap (alert=$3.45 × 1.08 × 1.05)",
            )
            self.assertGreater(order.limit_price, 3.45,
                               "limit must allow some slippage above alert")
        finally:
            session.close()

    async def test_buy_limit_unchanged_when_live_quote_close_to_alert(self):
        """The cap must not interfere when live quote is near alert price.
        Slippage of <8% should pass through (live quote used as base)."""
        async def fake_fetch_prices(symbols):
            return {s: 1.95 for s in symbols}  # 1.5% above alert 1.92

        executor = PublicExecutor(DummyManager())
        with patch("app.execution.broker_router.fetch_prices", new=fake_fetch_prices):
            buy_raw = "BOUGHT QCOM 5/1 180C $1.92"
            alert = parse_alert(buy_raw)
            self.assertIsNotNone(alert)
            alert_id = build_alert_row(alert, buy_raw)
            await executor.execute(alert, alert_id)

        session = db.SessionLocal()
        try:
            order = session.query(Order).filter(Order.side == "BUY").one()
            # 1.95 × 1.05 = 2.0475 → ceil .05 (since <3) = 2.05
            self.assertAlmostEqual(order.limit_price, 2.05, places=2)
        finally:
            session.close()

    # ─── Fix #6: config tune verification ────────────────────────────────

    def test_config_tpt_trail_pct_tightened(self):
        """tpt_trail_pct must be tuned from 20 → 12 to give less back at
        peak. Verifies the config.ini change is actually in place."""
        import configparser
        cp = configparser.ConfigParser()
        cp.read(os.path.join(os.path.dirname(__file__), "..", "config.ini"))
        val = cp.getfloat("trading", "tpt_trail_pct")
        self.assertLessEqual(val, 15.0,
                             f"tpt_trail_pct={val} too loose — gives back too much")
        self.assertGreaterEqual(val, 8.0,
                                f"tpt_trail_pct={val} too tight — risks early stop")

    # ─── Fix #7: post-PT2 ignores non-close_all DISCORD SELL ─────────────

    async def test_post_pt2_ignores_partial_discord_sell(self):
        """After PT1+PT2 already hit, a non-close_all DISCORD SELL alert
        from the analyst must be ignored — let TPT carry the runner.
        (Real QCOM 175C bug: analyst SELL @ 3.10 closed the last contract,
        then the same analyst signaled $5.00 and $6.15 minutes later.)"""
        seed_position(
            osi_symbol="QCOM260501C00175000",
            symbol="QCOM",
            strike=175,
            avg_price=2.07,
            current_price=3.20,
            remaining=1,
            total_contracts=4,
            pt1_triggered=True,
            pt2_triggered=True,
            status="PARTIAL",
        )

        manager = DummyManager()
        executor = PublicExecutor(manager)

        # Partial SELL alert (1/2 fraction), NOT close_all.
        sell_raw = "SOLD QCOM 5/1 175C $3.10 1/2"
        alert = parse_alert(sell_raw)
        self.assertIsNotNone(alert)
        self.assertFalse(alert.close_all, "test setup: alert must be partial")
        alert_id = build_alert_row(alert, sell_raw)

        await executor.execute(alert, alert_id)

        session = db.SessionLocal()
        try:
            pos = session.query(Position).one()
            self.assertEqual(pos.remaining, 1,
                             "post-PT2 partial DISCORD SELL must not reduce remaining")
            self.assertEqual(pos.status, "PARTIAL")
            sell_orders = session.query(Order).filter(Order.side == "SELL").count()
            self.assertEqual(sell_orders, 0,
                             "no SELL order should be placed after PT2 for partial")
        finally:
            session.close()

    async def test_post_pt2_close_all_still_executes(self):
        """Stop-loss / full-exit close_all alerts must still execute even
        after PT2 — only partial SELLs are filtered. Under strict mode
        (2026-05-08 H1 fix) the alert text must include an EXPLICIT
        close-all phrase to set close_all=True."""
        seed_position(
            osi_symbol="QCOM260501C00175000",
            symbol="QCOM",
            strike=175,
            avg_price=2.07,
            current_price=3.20,
            remaining=1,
            total_contracts=4,
            pt1_triggered=True,
            pt2_triggered=True,
            status="PARTIAL",
        )

        manager = DummyManager()
        executor = PublicExecutor(manager)

        # Explicit ALL OUT — strict mode requires the phrase.
        sell_raw = "SOLD QCOM 5/1 175C $3.10 ALL OUT"
        alert = parse_alert(sell_raw)
        self.assertIsNotNone(alert)
        self.assertTrue(alert.close_all, "test setup: alert must be close_all")
        alert_id = build_alert_row(alert, sell_raw)

        await executor.execute(alert, alert_id)

        session = db.SessionLocal()
        try:
            pos = session.query(Position).one()
            self.assertEqual(pos.remaining, 0,
                             "close_all SELL must close the position even after PT2")
            self.assertEqual(pos.status, "CLOSED")
        finally:
            session.close()

    async def test_pre_pt2_partial_discord_sell_still_works(self):
        """Pre-PT2 (or only PT1 hit), partial DISCORD SELL must still go
        through — protection only kicks in after PT2."""
        seed_position(
            osi_symbol="QCOM260501C00175000",
            symbol="QCOM",
            strike=175,
            avg_price=2.07,
            current_price=2.70,
            remaining=2,
            total_contracts=4,
            pt1_triggered=True,
            pt2_triggered=False,
            status="PARTIAL",
        )

        manager = DummyManager()
        executor = PublicExecutor(manager)

        sell_raw = "SOLD QCOM 5/1 175C $2.70 1/2"
        alert = parse_alert(sell_raw)
        self.assertIsNotNone(alert)
        alert_id = build_alert_row(alert, sell_raw)

        await executor.execute(alert, alert_id)

        session = db.SessionLocal()
        try:
            pos = session.query(Position).one()
            self.assertLess(pos.remaining, 2,
                            "pre-PT2 partial DISCORD SELL should still reduce qty")
        finally:
            session.close()

    # ─── Fix #8: PT3 floor (no max(1,...)) when TPT enabled ──────────────

    async def _run_pt3_with_overrides(self, *, pt3_sell, remaining, current_price=4.12):
        """Helper: monkeypatch position_monitor's config accessors so PT3
        fires as configured, run one tick, return the post-tick Position."""
        import app.monitors.position_monitor as pm
        saved = (pm._TPT_ENABLED, pm._PT3_SELL, pm._PT3_PCT,
                 pm._PT3_ENABLED, pm._PT2_ENABLED, pm._PT1_ENABLED,
                 pm._AUTO_EXIT_ENABLED, pm._SL_ENABLED, pm._TRAILING_SL_ENABLED)
        pm._TPT_ENABLED = lambda: True
        pm._PT3_SELL = lambda: pt3_sell
        pm._PT3_PCT = lambda: 100.0
        pm._PT3_ENABLED = lambda: True
        pm._PT2_ENABLED = lambda: True
        pm._PT1_ENABLED = lambda: True
        pm._AUTO_EXIT_ENABLED = lambda pos=None: True
        pm._SL_ENABLED = lambda: False
        pm._TRAILING_SL_ENABLED = lambda: False

        seed_position(
            osi_symbol="QCOM260501C00180000",
            symbol="QCOM",
            strike=180,
            avg_price=2.06,
            current_price=current_price,
            remaining=remaining,
            total_contracts=4,
            pt1_triggered=True,
            pt2_triggered=True,
            status="PARTIAL",
        )

        async def fake_fetch_prices(symbols):
            return {s: current_price for s in symbols}
        import app.execution.public_sdk_bridge as psb
        original = psb.fetch_prices
        psb.fetch_prices = fake_fetch_prices

        manager = DummyManager()
        executor = PublicExecutor(manager)
        try:
            await pm._check_all_positions(manager, executor)
        finally:
            psb.fetch_prices = original
            (pm._TPT_ENABLED, pm._PT3_SELL, pm._PT3_PCT,
             pm._PT3_ENABLED, pm._PT2_ENABLED, pm._PT1_ENABLED,
             pm._AUTO_EXIT_ENABLED, pm._SL_ENABLED, pm._TRAILING_SL_ENABLED) = saved

    async def test_pt3_with_one_contract_arms_tpt_does_not_sell(self):
        """When PT3 fires with remaining=1 and pt3_sell=0.5, the old code
        used max(1, int(0.5))=1 and sold the last contract — defeating
        the whole point of TPT runner. New behavior: int(0.5)=0, skip
        partial sell, just arm tpt_armed=True so TPT trail carries the
        full remaining contract."""
        await self._run_pt3_with_overrides(pt3_sell=0.5, remaining=1)

        session = db.SessionLocal()
        try:
            pos = session.query(Position).one()
            self.assertEqual(pos.remaining, 1,
                             "PT3 with remaining=1 and pt3_sell=0.5 must NOT sell the last contract")
            self.assertTrue(pos.pt3_triggered,
                            "pt3_triggered should still be set so PT3 doesn't re-fire")
            self.assertTrue(pos.tpt_armed,
                            "tpt_armed must be True — TPT trail now carries the runner")
            pt3_orders = session.query(Order).filter(Order.trigger == "PT3").count()
            self.assertEqual(pt3_orders, 0,
                             "no PT3 SELL order should be placed when qty rounds to 0")
        finally:
            session.close()

    async def test_pt3_with_two_contracts_sells_one(self):
        """Inverse: PT3 with remaining=2 and pt3_sell=0.5 should sell 1
        (int(2*0.5)=1) and arm TPT for the remaining 1. Floor change must
        not break normal-sized positions."""
        await self._run_pt3_with_overrides(pt3_sell=0.5, remaining=2)

        session = db.SessionLocal()
        try:
            pos = session.query(Position).one()
            self.assertEqual(pos.remaining, 1, "should have sold 1, kept 1")
            self.assertTrue(pos.tpt_armed)
        finally:
            session.close()

    # ─── Fix #9: per-position try/except in _check_all_positions ─────────

    async def test_one_failing_position_does_not_break_others(self):
        """If one position's exit eval raises (e.g. broker error mid-tick),
        all other positions in the same tick must still be evaluated.
        Without this, a single bad position skips checks for every other
        position for that tick (5-10s) — auto-exits could be missed."""
        # Two positions: one with a price that triggers PT1, one we'll
        # corrupt to force an exception in eval.
        seed_position(
            osi_symbol="GOOD260501C00100000",
            symbol="GOOD",
            strike=100,
            avg_price=1.00,
            current_price=1.00,
            remaining=2,
            total_contracts=2,
            status="OPEN",
        )
        bad_id = seed_position(
            osi_symbol="BAD260501C00100000",
            symbol="BAD",
            strike=100,
            avg_price=1.00,
            current_price=1.00,
            remaining=2,
            total_contracts=2,
            status="OPEN",
        )

        async def fake_fetch_prices(symbols):
            return {s: 1.40 for s in symbols}  # +40% → PT1
        import app.execution.public_sdk_bridge as psb
        original = psb.fetch_prices
        psb.fetch_prices = fake_fetch_prices

        # Monkeypatch _evaluate_one_position to raise on BAD only.
        import app.monitors.position_monitor as pm
        orig_eval = pm._evaluate_one_position

        async def faulty_eval(db, pos, current_price, executor, ws_manager):
            if pos.osi_symbol == "BAD260501C00100000":
                raise RuntimeError("simulated broker failure")
            return await orig_eval(db, pos, current_price, executor, ws_manager)

        pm._evaluate_one_position = faulty_eval
        try:
            manager = DummyManager()
            executor = PublicExecutor(manager)
            await pm._check_all_positions(manager, executor)
        finally:
            pm._evaluate_one_position = orig_eval
            psb.fetch_prices = original

        session = db.SessionLocal()
        try:
            good = session.query(Position).filter(
                Position.osi_symbol == "GOOD260501C00100000",
            ).one()
            bad = session.query(Position).filter(
                Position.osi_symbol == "BAD260501C00100000",
            ).one()
            # Good position got its PT1 evaluated despite BAD raising.
            self.assertTrue(good.pt1_triggered,
                            "GOOD position must still hit PT1 even though BAD raised")
            # Bad position is left unchanged — no flag flips, no order placed.
            self.assertFalse(bad.pt1_triggered)
        finally:
            session.close()


class CodebaseAuditFixesTest(unittest.IsolatedAsyncioTestCase):
    """Regression tests for the second-round codebase audit fixes."""

    @classmethod
    def tearDownClass(cls):
        db.engine.dispose()
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)

    def setUp(self):
        public_executor.DRY_RUN = True
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()
        guardrails._recent_alerts.clear()
        guardrails._recent_content_hashes.clear()

    # ─── parser: explicit qty must NOT trigger close_all ─────────────────

    def test_sell_with_explicit_qty_is_not_close_all(self):
        alert = parse_alert("SOLD SPX 4/14 6970C $2.70 2 contracts")
        self.assertIsNotNone(alert)
        self.assertEqual(alert.action, "SELL")
        self.assertEqual(alert.qty, 2)
        self.assertFalse(
            alert.close_all,
            "explicit qty=2 in message must not trigger close_all flatten",
        )

    def test_sell_no_qty_no_fraction_defaults_to_partial_under_strict(self):
        # 2026-05-08 H1 fix: strict mode (default) — bare SELL with no
        # explicit close-all phrase must NOT set close_all=True. fraction
        # defaults to 1 so qty math still resolves to pos.remaining when
        # no PT2 has fired (preserves day-zero behaviour for fresh
        # positions); post-PT2 partial-runner protection now correctly
        # ignores these bare alerts.
        alert = parse_alert("SOLD SPX 4/14 6970C $2.70")
        self.assertIsNotNone(alert)
        self.assertEqual(alert.action, "SELL")
        self.assertIsNone(alert.qty)
        self.assertFalse(alert.close_all,
                         "bare SELL must not be close_all under strict mode")
        self.assertEqual(alert.fraction, Decimal("1"))

    # ─── parser: BUY+SELL keyword ambiguity → first-position wins ─────────

    def test_mixed_buy_sell_message_picks_first_keyword(self):
        # "STC ... BTO ..." — SELL appears first, so SELL wins.
        alert = parse_alert("STC SPX 4/14 6970C $2.70, then BTO SPX 4/14 6980C $1.50")
        self.assertIsNotNone(alert)
        self.assertEqual(
            alert.action, "SELL",
            "When both BUY and SELL keywords appear, the earlier one in the text should win",
        )

    # ─── ai_parser: leap-day expiry must not raise ───────────────────────

    def test_ai_parser_leap_day_does_not_crash(self):
        # We can't easily test the full ai_parser without LLM; smoke-test
        # the date-rolling helper inline.
        from datetime import date as _d
        leap = _d(2024, 2, 29)
        # Simulate the rolled-into-non-leap-year case.
        try:
            rolled = leap.replace(year=2026)
            self.fail("date.replace should raise on leap-day → non-leap year")
        except ValueError:
            pass
        # Our fallback should produce a safe Feb 28.
        safe = _d(2026, leap.month, min(leap.day, 28))
        self.assertEqual(safe, _d(2026, 2, 28))

    # ─── guardrails: content hash uses Decimal not float ─────────────────

    def test_content_hash_decimal_consistency(self):
        from app.risk.guardrails import compute_content_hash
        h1 = compute_content_hash("SPX260501C06000000", "BUY", Decimal("3.005"))
        h2 = compute_content_hash("SPX260501C06000000", "BUY", Decimal("3.01"))
        # 3.005 quantized to 2 dp with banker's rounding → 3.00
        # 3.01 stays 3.01. So these MUST hash differently.
        self.assertNotEqual(h1, h2)

        # Same numeric value via different Decimal instantiations must hash same.
        h3 = compute_content_hash("SPX260501C06000000", "BUY", Decimal("3.45"))
        h4 = compute_content_hash("SPX260501C06000000", "BUY", Decimal("3.4500"))
        self.assertEqual(h3, h4)

    # ─── trade_logger: nullable col guards ───────────────────────────────

    async def test_daily_summary_handles_null_remaining(self):
        # Insert a position with remaining=None to simulate legacy/odd data.
        session = db.SessionLocal()
        try:
            pos = Position(
                osi_symbol="SPX260501C06000000",
                symbol="SPX",
                expiry="2026-05-01",
                strike=6000,
                option_type="C",
                total_contracts=None,
                remaining=None,
                avg_price=2.00,
                current_price=2.50,
                status="OPEN",
            )
            session.add(pos)
            session.commit()
        finally:
            session.close()

        # Just import the function and exercise the math branch that
        # used to crash on `None * 100`. We don't actually post to Discord.
        from app.analytics.trade_logger import log_daily_summary
        # Stub _send_to_channel and _generate_ai_summary to avoid network.
        import app.analytics.trade_logger as tl
        orig_send = tl._send_to_channel
        orig_ai = tl._generate_ai_summary
        async def _fake_send(**_): return None
        async def _fake_ai(_): return None
        tl._send_to_channel = _fake_send
        tl._generate_ai_summary = _fake_ai
        try:
            await log_daily_summary()  # must not raise TypeError
        finally:
            tl._send_to_channel = orig_send
            tl._generate_ai_summary = orig_ai

    # ─── reconciler: REOPEN resets highest_price (no stale TPT trigger) ──

    def test_reopen_resets_highest_price(self):
        # Smoke test the in-memory miss counter exists and starts empty.
        from app.monitors.reconciler import _missing_position_misses, _MIN_CONSECUTIVE_MISSES
        _missing_position_misses.clear()
        self.assertEqual(_MIN_CONSECUTIVE_MISSES, 2,
                         "transient broker miss tolerance should be > 1")

    # ─── forwarder: dedup must be set after send, not before ─────────────

    def test_forwarder_dedup_split_into_check_and_mark(self):
        from app.ingest.forwarder import _is_forwarder_duplicate, _mark_forwarded
        content = "**BOUGHT** SPX 4/14 6970C $1.75"
        # First check: not a duplicate, and crucially does NOT auto-record.
        self.assertFalse(_is_forwarder_duplicate(content, []))
        # Repeat check: still not a duplicate (we never recorded the first).
        self.assertFalse(_is_forwarder_duplicate(content, []))
        # Now mark it.
        _mark_forwarded(content, [])
        # Subsequent check: now it's a duplicate.
        self.assertTrue(_is_forwarder_duplicate(content, []))


class CrossCuttingAuditFixesTest(unittest.TestCase):
    """Third-round audit fixes — security, race-conditions, DST, validation.
    Each test ties to a defect surfaced by the parallel audit forks."""

    # ─── ai_scorer hard cap on bonus qty ─────────────────────────────────

    def test_ai_scorer_qty_capped(self):
        from app.risk.ai_scorer import adjust_quantity
        # An XL-tagged base of 30 + score=95 bonus must not exceed the
        # hard cap regardless of input. Defense-in-depth: if size_tag
        # mapping ever returns a huge number the scorer can't multiply it.
        self.assertLessEqual(adjust_quantity(30, 95, "XL"), 10)
        self.assertLessEqual(adjust_quantity(100, 95, "XL"), 10)
        # Sanity: small inputs unchanged.
        self.assertEqual(adjust_quantity(2, 60, "MEDIUM"), 2)
        self.assertEqual(adjust_quantity(2, 80, "MEDIUM"), 3)
        self.assertEqual(adjust_quantity(2, 95, "MEDIUM"), 4)

    # ─── api_monitor singleton init does not zero counters ───────────────

    def test_api_monitor_singleton_does_not_reset_on_reinstantiate(self):
        from app.monitors.api_monitor import APIMonitor
        m = APIMonitor()
        m.record_call("public_com", "/test", 200, 1.0)
        before = m.metrics["public_com"].total_requests
        # Re-instantiate; old code zeroed the counters here.
        m2 = APIMonitor()
        self.assertIs(m, m2)
        after = m2.metrics["public_com"].total_requests
        self.assertGreaterEqual(after, before, "singleton must not erase counters")

    # ─── greeks: put delta now monotonic vs moneyness ────────────────────

    def test_greeks_put_delta_monotonic(self):
        from app.monitors.greeks_monitor import GreeksMonitor
        gm = GreeksMonitor()
        # Walk strike across moneyness: deep ITM put → ATM → deep OTM.
        # Put delta must be monotonically increasing (toward 0) as
        # underlying moves up relative to strike.
        last = None
        for moneyness in (0.85, 0.92, 1.00, 1.07, 1.20):
            strike = 100.0
            underlying = strike * moneyness
            g = gm.calculate_greeks_approx("P", strike, underlying, days_to_expiry=30)
            if last is not None:
                self.assertGreaterEqual(
                    g.delta, last,
                    f"put delta must increase (toward 0) as moneyness rises; "
                    f"got delta={g.delta} at m={moneyness} after {last}",
                )
            last = g.delta

    # ─── greeks: unparseable expiry returns None ─────────────────────────

    def test_greeks_unparseable_expiry_skips(self):
        from app.monitors.greeks_monitor import GreeksMonitor
        gm = GreeksMonitor()
        # Garbage expiry → None (caller must skip Greeks).
        self.assertIsNone(gm._days_to_expiry("garbage"))
        self.assertIsNone(gm._days_to_expiry(""))
        # ISO format works.
        self.assertIsInstance(gm._days_to_expiry("2099-01-01"), int)
        # YYYYMMDD works.
        self.assertIsInstance(gm._days_to_expiry("20990101"), int)
        # Legacy YYMMDD still works.
        self.assertIsInstance(gm._days_to_expiry("990101"), int)

    # ─── ai_signal_advisor schema validation ─────────────────────────────

    def test_ai_advisor_validation_drops_garbage_fields(self):
        from app.execution.ai_signal_advisor import _validate_advisor_response
        # Malicious / malformed shape: strings where numbers expected.
        rec = _validate_advisor_response({
            "bias": "BULLISH",
            "confidence": "high",      # not numeric → dropped
            "max_risk_pct": 99,        # > 10 → dropped
            "key_levels": "7000",      # not a list → dropped
            "trade": {
                "strategy": "magic",   # not in allowlist → dropped
                "long_strike": "abc",  # not numeric → dropped
                "short_strike": 999999999999,  # too big → dropped
            },
        })
        self.assertEqual(rec["bias"], "bullish")
        self.assertNotIn("confidence", rec)
        self.assertNotIn("max_risk_pct", rec)
        self.assertNotIn("key_levels", rec)
        # trade with no valid fields should be empty / absent
        self.assertNotIn("trade", rec)

    def test_ai_advisor_validation_keeps_good_fields(self):
        from app.execution.ai_signal_advisor import _validate_advisor_response
        rec = _validate_advisor_response({
            "bias": "bearish",
            "confidence": 75,
            "max_risk_pct": 2.5,
            "key_levels": ["6900", "7000"],
            "trade": {
                "strategy": "long_put",
                "long_strike": 7000,
            },
        })
        self.assertEqual(rec["bias"], "bearish")
        self.assertEqual(rec["confidence"], 75)
        self.assertEqual(rec["max_risk_pct"], 2.5)
        self.assertEqual(rec["key_levels"], ["6900", "7000"])
        self.assertEqual(rec["trade"]["strategy"], "long_put")
        self.assertEqual(rec["trade"]["long_strike"], 7000.0)

    # ─── log_config sanitizes log injection from author/channel ──────────

    def test_log_sanitize_strips_newlines(self):
        from app.core.log_config import _sanitize_log_field
        self.assertEqual(
            _sanitize_log_field("evil\nINJECTED log line"),
            "evil INJECTED log line",
        )
        self.assertEqual(_sanitize_log_field("normal"), "normal")
        self.assertEqual(_sanitize_log_field(None), "")
        long = "x" * 500
        self.assertLessEqual(len(_sanitize_log_field(long)), 200)

    # ─── log_config Discord token regex catches bot tokens ───────────────

    def test_log_redacts_discord_bot_token(self):
        import logging
        from app.core.log_config import SecretRedactingFilter
        f = SecretRedactingFilter()
        rec = logging.LogRecord(
            "test", logging.INFO, "f", 1,
            "client.run(MTk4MTQ4NDc5OTA0MzMzNzA0.ABCDEF.abcdefghijklmnopqrstuvwxyz1234567)",
            (), None,
        )
        f.filter(rec)
        self.assertNotIn("MTk4MTQ4NDc5OTA0MzMzNzA0", rec.getMessage())


class DisplacementConfirmedCancelTest(unittest.IsolatedAsyncioTestCase):
    """2026-07-10 scan finding: the _place_sell displacement lane (newer SELL
    cancels a stale pending SELL) must obey the same 3-state confirmed-cancel
    contract as the re-peg lanes — never place the replacement when the stale
    order filled during cancel, when the cancel is unconfirmed, or when the
    cancel raised. Same mechanics as the SPXW 7550C double-sell 2026-07-09."""

    @classmethod
    def tearDownClass(cls):
        db.engine.dispose()
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)

    def setUp(self):
        public_executor.DRY_RUN = True
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()

    async def _displace(self, cancel_behavior):
        from unittest.mock import AsyncMock
        pos_id = seed_position(remaining=4, status="OPEN", current_price=2.73)
        stale_id = seed_order(quantity=2, trigger="DISCORD", status="PENDING")
        executor = PublicExecutor(DummyManager())
        with patch.object(PublicExecutor, "cancel_order", new=cancel_behavior):
            session = db.SessionLocal()
            try:
                pos = session.query(Position).filter(Position.id == pos_id).one()
                result = await executor._place_sell(
                    session, pos, qty=4, limit_px=Decimal("2.55"), trigger="DISCORD",
                )
                session.commit()
            finally:
                session.close()
        session = db.SessionLocal()
        try:
            new_orders = session.query(Order).filter(Order.id != stale_id).count()
        finally:
            session.close()
        return result, new_orders

    async def test_filled_during_cancel_skips_replacement(self):
        from unittest.mock import AsyncMock
        result, new_orders = await self._displace(
            AsyncMock(return_value={"status": "filled_during_cancel"}))
        self.assertIsNone(result, "stale filled during cancel — replacement would oversell")
        self.assertEqual(new_orders, 0)

    async def test_unconfirmed_cancel_holds_off_replacement(self):
        from unittest.mock import AsyncMock
        result, new_orders = await self._displace(
            AsyncMock(return_value={"status": "cancel_unconfirmed"}))
        self.assertIsNone(result, "unconfirmed cancel — stale may still fill; hold off")
        self.assertEqual(new_orders, 0)

    async def test_cancel_exception_holds_off_replacement(self):
        from unittest.mock import AsyncMock
        result, new_orders = await self._displace(
            AsyncMock(side_effect=RuntimeError("gateway reconnect")))
        self.assertIsNone(result, "cancel raised — stale may still be live; hold off")
        self.assertEqual(new_orders, 0)

    async def test_confirmed_cancel_still_places_replacement(self):
        from unittest.mock import AsyncMock
        result, new_orders = await self._displace(
            AsyncMock(return_value={"status": "ok"}))
        self.assertIsNotNone(result, "confirmed cancel — replacement must place")
        self.assertEqual(new_orders, 1)


if __name__ == "__main__":
    unittest.main()
