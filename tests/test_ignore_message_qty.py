"""[trading] ignore_message_qty — analyst's explicit contract count vs configured sizing.

Flag OFF (default/legacy): the count parsed from the message wins.
Flag ON: the count is discarded; sizing comes from size tags / size_default.
Sizes pinned by conftest's _isolate_config: default=1, medium=2, xl=5.
"""
import pytest

from app.core.config_manager import cfg
from app.execution.public_executor import PublicExecutor, _honors_message_qty
from app.ingest.parser import parse_alert


@pytest.fixture
def executor():
    return PublicExecutor(ws_manager=None)


def _set_flag(monkeypatch, value: bool):
    real = cfg.getboolean

    def fake(section, key, fallback=False):
        if (section, key) == ("trading", "ignore_message_qty"):
            return value
        return real(section, key, fallback=fallback)

    monkeypatch.setattr(cfg, "getboolean", fake)


def test_flag_off_message_qty_wins(executor, monkeypatch):
    _set_flag(monkeypatch, False)
    assert executor._contracts_for_size("MEDIUM", 8) == 8


def test_flag_on_uses_size_tag(executor, monkeypatch):
    _set_flag(monkeypatch, True)
    assert executor._contracts_for_size("MEDIUM", 8) == 2


def test_flag_on_no_tag_uses_default(executor, monkeypatch):
    _set_flag(monkeypatch, True)
    assert executor._contracts_for_size("", 8) == 1


def test_flag_on_without_explicit_qty_unchanged(executor, monkeypatch):
    _set_flag(monkeypatch, True)
    assert executor._contracts_for_size("XL", None) == 5


def test_default_is_off():
    # Fresh config without the key → flag must default to False (legacy mirror behavior)
    assert cfg.getboolean("trading", "ignore_message_qty", fallback=False) is False


# ── Sniper small-account challenge exemption ─────────────────────────────────
# ignore_message_qty stays ON globally; alerts carrying the small-account marker
# from an author in [trading] message_qty_authors mirror his stated count anyway.

SMALL = "**BOUGHT CRM 180C 8/21 1.63** ‼️ - 5 on small account. @everyone"
PLAIN = "**BOUGHT AMZN 250C 8/21 1.84** - earnings play. Mid sized. @everyone"


def _set_authors(monkeypatch, value: str):
    real = cfg.get

    def fake(section, key, fallback=None, **kw):
        if (section, key) == ("trading", "message_qty_authors"):
            return value
        return real(section, key, fallback=fallback, **kw)

    monkeypatch.setattr(cfg, "get", fake)


def _alert(text: str, author: str):
    a = parse_alert(text)
    assert a is not None, f"parse returned None for {text!r}"
    a._alert_author = author
    return a


def test_parser_stamps_small_account_marker():
    a = parse_alert(SMALL)
    assert a.small_account is True and a.qty == 5
    assert parse_alert(PLAIN).small_account is False
    # phrase alone (no emoji) and emoji alone both count
    assert parse_alert("BOUGHT CRM 180C 8/21 1.63 - 2 on small account").small_account is True
    assert parse_alert("**BOUGHT AAPL 320C 8/21 0.89**‼️-2 contracts.").small_account is True


def test_small_account_author_honors_qty(monkeypatch):
    _set_authors(monkeypatch, "Sniper")
    assert _honors_message_qty(_alert(SMALL, "Sniper Alerts")) is True


def test_other_author_does_not_honor_qty(monkeypatch):
    _set_authors(monkeypatch, "Sniper")
    assert _honors_message_qty(_alert(SMALL, "Fluid Options Alerts")) is False


def test_same_author_main_account_alert_does_not_honor_qty(monkeypatch):
    _set_authors(monkeypatch, "Sniper")
    assert _honors_message_qty(_alert(PLAIN, "Sniper Alerts")) is False


def test_marker_without_qty_does_not_honor(monkeypatch):
    _set_authors(monkeypatch, "Sniper")
    a = _alert("**BOUGHT SKHY 145P 8/21 1.18**‼️ - riding this one for ITM", "Sniper Alerts")
    assert a.qty is None and _honors_message_qty(a) is False


def test_empty_author_list_disables_feature(monkeypatch):
    _set_authors(monkeypatch, "")
    assert _honors_message_qty(_alert(SMALL, "Sniper Alerts")) is False


def test_honor_qty_beats_flag_on(executor, monkeypatch):
    _set_flag(monkeypatch, True)
    assert executor._contracts_for_size("", 5, None, honor_qty=True) == 5
    assert executor._contracts_for_size("", 5, None, honor_qty=False) == 1
