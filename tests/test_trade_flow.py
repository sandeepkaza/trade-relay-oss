import os
import sys
import unittest
from decimal import Decimal
from unittest.mock import patch, MagicMock, AsyncMock
import asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set test environment BEFORE importing app modules
TEST_DB_PATH = os.path.abspath("test_trader_flow.db")
os.environ["TRADER_DATABASE_URL"] = f"sqlite:///{TEST_DB_PATH.replace('\\', '/')}"
os.environ["DRY_RUN"] = "true"

# Import and patch modules BEFORE they initialize
import app.execution.public_executor as public_executor
import app.risk.guardrails as guardrails
import app.risk.ai_scorer as ai_scorer
import app.risk.kelly_sizer as kelly_sizer
# Force DRY_RUN to True for all tests
public_executor.DRY_RUN = True
# Disable market hours checking for tests
guardrails.MARKET_HOURS_ONLY = False
# Disable AI features for consistent test behavior
ai_scorer.SCORER_ENABLED = False
kelly_sizer.KELLY_ENABLED = False

import app.core.app as app_module   # _fire_manual_exit lives in app.core.app, not the package
import app.core.db as db
import app.monitors.position_monitor as position_monitor
import app.execution.public_sdk_bridge as public_sdk_bridge
from app.core.models import Alert, Order, Position
from app.ingest.parser import parse_alert
from app.execution.public_executor import PublicExecutor

# Ensure DRY_RUN stays True
public_executor.DRY_RUN = True
# Ensure market hours check stays disabled
guardrails.MARKET_HOURS_ONLY = False
# Ensure AI features stay disabled
ai_scorer.SCORER_ENABLED = False
kelly_sizer.KELLY_ENABLED = False


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
        "osi_symbol": "SPX270414C06970000",
        "symbol": "SPX",
        "expiry": "2027-04-14",
        "strike": 6970,
        "option_type": "C",
        "total_contracts": 2,
        "remaining": 2,
        "avg_price": 1.75,
        "current_price": 1.75,
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


class TradeFlowTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def tearDownClass(cls):
        db.engine.dispose()
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)

    def setUp(self):
        # Ensure DRY_RUN is True for every test
        public_executor.DRY_RUN = True
        db.Base.metadata.drop_all(bind=db.engine)
        db.init_db()

    async def test_buy_then_partial_sell_updates_position(self):
        manager = DummyManager()
        executor = PublicExecutor(manager)

        buy_raw = "BOUGHT SPX 4/14/2027 6970C $1.75 [MEDIUM]"
        buy_alert = parse_alert(buy_raw)
        self.assertIsNotNone(buy_alert)
        buy_alert_id = build_alert_row(buy_alert, buy_raw)

        await executor.execute(buy_alert, buy_alert_id)

        sell_raw = "SOLD SPX 4/14/2027 6970C $2.70 1/2"
        sell_alert = parse_alert(sell_raw)
        self.assertIsNotNone(sell_alert)
        sell_alert_id = build_alert_row(sell_alert, sell_raw)

        await executor.execute(sell_alert, sell_alert_id)

        session = db.SessionLocal()
        try:
            position = session.query(Position).filter_by(osi_symbol=buy_alert.osi_symbol).first()
            self.assertIsNotNone(position)
            self.assertEqual(position.total_contracts, 2)
            self.assertEqual(position.remaining, 1)
            self.assertEqual(position.status, "PARTIAL")

            filled_orders = session.query(Order).filter_by(status="FILLED").all()
            self.assertEqual(len(filled_orders), 2)

            buy_status = session.query(Alert).filter(Alert.id == buy_alert_id).first().status
            sell_status = session.query(Alert).filter(Alert.id == sell_alert_id).first().status
            self.assertEqual(buy_status, "FILLED")
            self.assertEqual(sell_status, "FILLED")
        finally:
            session.close()

    async def test_close_all_signal_without_date_matches_open_position(self):
        seed_position()
        manager = DummyManager()
        executor = PublicExecutor(manager)

        raw = "ALL OUT SPX 6970C $2.70"
        alert = parse_alert(raw)
        self.assertIsNotNone(alert)
        alert_id = build_alert_row(alert, raw)

        await executor.execute(alert, alert_id)

        session = db.SessionLocal()
        try:
            position = session.query(Position).one()
            self.assertEqual(position.remaining, 0)
            self.assertEqual(position.status, "CLOSED")
        finally:
            session.close()

    async def test_generic_all_out_is_gated_by_default(self):
        # 2026-06-11: a bare "ALL OUT" (no symbol) no longer flattens the book.
        # close_all_requires_symbol defaults true → it's dropped SKIPPED.
        seed_position(osi_symbol="SPX270414C06970000", symbol="SPX",
                      expiry="2027-04-14", strike=6970, option_type="C")
        seed_position(osi_symbol="QQQ270416P00420000", symbol="QQQ",
                      expiry="2027-04-16", strike=420, option_type="P",
                      avg_price=3.20, current_price=3.20)
        manager = DummyManager()
        executor = PublicExecutor(manager)

        raw = "ALL OUT"
        alert = parse_alert(raw)
        self.assertIsNotNone(alert)
        alert_id = build_alert_row(alert, raw)

        await executor.execute(alert, alert_id)

        session = db.SessionLocal()
        try:
            open_positions = session.query(Position).filter(Position.status.in_(["OPEN", "PARTIAL"])).count()
            sell_orders = session.query(Order).filter(Order.side == "SELL").count()
            self.assertEqual(open_positions, 2)   # untouched
            self.assertEqual(sell_orders, 0)      # nothing placed
            self.assertEqual(session.get(Alert, alert_id).status, "SKIPPED")
        finally:
            session.close()

    async def test_generic_all_out_flattens_when_gate_off_scope_all(self):
        # Legacy behavior is still reachable: gate off + scope=all → book-wide.
        import app.execution.public_executor as _pe
        seed_position(osi_symbol="SPX270414C06970000", symbol="SPX",
                      expiry="2027-04-14", strike=6970, option_type="C")
        seed_position(osi_symbol="QQQ270416P00420000", symbol="QQQ",
                      expiry="2027-04-16", strike=420, option_type="P",
                      avg_price=3.20, current_price=3.20)
        manager = DummyManager()
        executor = PublicExecutor(manager)

        raw = "ALL OUT"
        alert = parse_alert(raw)
        alert_id = build_alert_row(alert, raw)

        saved_req, saved_scope = _pe._CLOSE_ALL_REQUIRES_SYMBOL, _pe._CLOSE_ALL_SCOPE
        _pe._CLOSE_ALL_REQUIRES_SYMBOL = lambda: False
        _pe._CLOSE_ALL_SCOPE = lambda: "all"
        try:
            await executor.execute(alert, alert_id)
        finally:
            _pe._CLOSE_ALL_REQUIRES_SYMBOL = saved_req
            _pe._CLOSE_ALL_SCOPE = saved_scope

        session = db.SessionLocal()
        try:
            open_positions = session.query(Position).filter(Position.status.in_(["OPEN", "PARTIAL"])).count()
            sell_orders = session.query(Order).filter(Order.side == "SELL", Order.status == "FILLED").count()
            self.assertEqual(open_positions, 0)
            self.assertEqual(sell_orders, 2)
        finally:
            session.close()

    async def test_manual_exit_all_endpoint_closes_all_positions(self):
        seed_position()
        seed_position(
            osi_symbol="QQQ270416P00420000",
            symbol="QQQ",
            expiry="2027-04-16",
            strike=420,
            option_type="P",
            avg_price=3.20,
            current_price=2.80,
        )
        # Patch the executor's _place_sell to finalize immediately in DRY_RUN mode
        manager = DummyManager()
        
        # Manually exit each position
        from app.core.models import Position
        db_session = db.SessionLocal()
        try:
            positions = db_session.query(Position).filter(
                Position.status.in_(["OPEN", "PARTIAL"]),
                Position.remaining > 0
            ).all()
            closed_count = 0
            for pos in positions:
                try:
                    result = await app_module._fire_manual_exit(pos.id)
                    if result.get("status") == "ok":
                        closed_count += 1
                except Exception as e:
                    print(f"Exit failed for {pos.osi_symbol}: {e}", file=sys.stderr)
        finally:
            db_session.close()

        session = db.SessionLocal()
        try:
            closed_positions = session.query(Position).filter(Position.status == "CLOSED").count()
            self.assertEqual(closed_count, 2)
            self.assertEqual(closed_positions, 2)
        finally:
            session.close()

    async def test_monitor_pt1_only_reduces_once(self):
        seed_position(
            total_contracts=2,
            remaining=2,
            avg_price=1.00,
            current_price=1.00,
        )

        async def fake_fetch_prices(symbols):
            return {symbols[0]: 1.40}

        original_fetch_prices = public_sdk_bridge.fetch_prices
        public_sdk_bridge.fetch_prices = fake_fetch_prices

        manager = DummyManager()
        executor = PublicExecutor(manager)

        try:
            await position_monitor._check_all_positions(manager, executor)
        finally:
            public_sdk_bridge.fetch_prices = original_fetch_prices

        session = db.SessionLocal()
        try:
            position = session.query(Position).one()
            self.assertEqual(position.remaining, 1)
            self.assertTrue(position.pt1_triggered)
            self.assertEqual(position.status, "PARTIAL")
        finally:
            session.close()


if __name__ == "__main__":
    unittest.main()
