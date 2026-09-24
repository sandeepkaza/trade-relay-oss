"""
small_account_only (2026-08-04): the Tradier instance on ibkr-oci is dedicated
to Sniper's $5k small-account challenge — it mirrors ONLY the ‼ entries and
ignores every other BUY. Exits must never be blocked, because his SELL posts
("SOLD 1/2") usually carry no marker and the position still has to close.
"""

from app.core.config_manager import cfg
from app.execution.public_executor import _small_account_only_blocks
from app.ingest.parser import parse_alert

SMALL = "BOUGHT CRM 180C 7/31 1.63 ‼️ - 2 on small account."
PLAIN = "BOUGHT CRM 180C 7/31 1.63"
SELL = "SOLD CRM 180C 7/31 2.10 1/2"


def _flag(monkeypatch, on: bool):
    real = cfg.getboolean
    monkeypatch.setattr(cfg, "getboolean", lambda s, k, fallback=False: (
        on if (s, k) == ("trading", "small_account_only") else real(s, k, fallback=fallback)))


def test_flag_off_blocks_nothing():
    assert _small_account_only_blocks(parse_alert(PLAIN)) is False


def test_flag_on_blocks_plain_buy(monkeypatch):
    _flag(monkeypatch, True)
    assert _small_account_only_blocks(parse_alert(PLAIN)) is True


def test_flag_on_passes_marked_buy_and_any_sell(monkeypatch):
    _flag(monkeypatch, True)
    marked = parse_alert(SMALL)
    assert marked.small_account is True
    assert _small_account_only_blocks(marked) is False
    sell = parse_alert(SELL)
    assert sell.action == "SELL"
    assert _small_account_only_blocks(sell) is False
