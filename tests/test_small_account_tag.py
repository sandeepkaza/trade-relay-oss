"""
SMALL_ACCT lane tag (2026-08-04): small-account-challenge trades (‼ / "small
account" alerts) are tagged end-to-end — Position.strategy_tag at entry,
Order.strategy_tag on BUY+SELL rows — so the dashboard and calendar journal
can badge and review the challenge separately.
"""

from types import SimpleNamespace

from app.core.models import Order, Position
from app.execution.public_executor import PublicExecutor
from app.ingest.parser import parse_alert

SMALL = "BOUGHT CRM 180C 7/31 1.63 ‼️ - 2 on small account."


def _stamp(pos, alert):
    PublicExecutor._stamp_position_author(None, pos, alert)


def test_small_account_alert_tags_position():
    a = parse_alert(SMALL)
    assert a.small_account is True
    a._alert_author = "Sniper"
    pos = Position(osi_symbol="CRM260731C00180000")
    _stamp(pos, a)
    assert pos.strategy_tag == "SMALL_ACCT"
    assert pos.author == "Sniper"


def test_plain_alert_leaves_tag_empty():
    a = parse_alert("BOUGHT CRM 180C 7/31 1.63")
    a._alert_author = "Sniper"
    pos = Position(osi_symbol="CRM260731C00180000")
    _stamp(pos, a)
    assert pos.strategy_tag is None


def test_existing_lane_tag_not_clobbered():
    pos = Position(osi_symbol="SPXW260804C06400000", strategy_tag="WHALE")
    _stamp(pos, SimpleNamespace(_alert_author="x", small_account=True))
    assert pos.strategy_tag == "WHALE"


def test_cooldown_retry_stub_carries_lane_and_author():
    """A slippage-cooldown retry hands _finalize_buy_fill a detached snapshot of
    the alert row, not the row. Dropping the lane there opened untagged
    positions whose SELL rows showed no badge (2026-08-12 CRWV 120C)."""
    from app.execution.public_executor import _detached_alert_stub

    row = SimpleNamespace(
        osi_symbol="CRWV260814C00120000", symbol="CRWV", expiry="2026-08-14",
        strike=120.0, option_type="C", author=" Sniper Alerts ",
        strategy_tag="SMALL_ACCT",
    )
    stub = _detached_alert_stub(row)
    assert stub.small_account is True, "lane marker lost — position opens untagged"
    assert stub.strategy_tag == "SMALL_ACCT"
    assert stub._alert_author == "Sniper Alerts"

    # and the stamp actually lands off that stub
    pos = Position(osi_symbol=row.osi_symbol)
    _stamp(pos, stub)
    assert pos.strategy_tag == "SMALL_ACCT"
    assert pos.author == "Sniper Alerts"

    # a plain (untagged) alert must stay untagged
    plain = _detached_alert_stub(SimpleNamespace(
        osi_symbol="X", symbol="X", expiry=None, strike=1.0, option_type="C",
        author="Sniper", strategy_tag=None))
    assert plain.small_account is False
    pos2 = Position(osi_symbol="X")
    _stamp(pos2, plain)
    assert pos2.strategy_tag is None


def test_order_to_dict_carries_tag():
    o = Order(osi_symbol="CRM260731C00180000", side="BUY", quantity=2,
              status="PENDING", trigger="DISCORD", strategy_tag="SMALL_ACCT")
    assert o.to_dict()["strategyTag"] == "SMALL_ACCT"
