"""spreads_only dedicates an instance to vertical credit spreads.

The gate exists because an author whitelist cannot express it: the spread
analyst posts single-leg alerts in four other rooms that forward to the same
instance, so whitelisting him for spreads would mirror those too.
"""
import pytest

from app.core.config_manager import cfg
from app.execution.public_executor import _small_account_only_blocks, _spreads_only_blocks
from app.ingest.parser import TradeAlert


def _alert(action="BUY", small_account=False):
    a = TradeAlert(action=action, symbol="NVDA", expiry=None, strike=222.5,
                   option_type="C", price=None)
    a.small_account = small_account
    return a


@pytest.fixture
def on(monkeypatch):
    monkeypatch.setattr(cfg, "getboolean",
                        lambda s, k, fallback=None, **kw: True if k == "spreads_only" else fallback)


@pytest.fixture
def off(monkeypatch):
    monkeypatch.setattr(cfg, "getboolean",
                        lambda s, k, fallback=None, **kw: False if k == "spreads_only" else fallback)


def test_a_single_leg_buy_is_refused(on):
    assert _spreads_only_blocks(_alert("BUY")) is True


def test_the_small_account_marker_does_not_buy_an_exemption(on):
    """small_account_only lets ‼ entries through; this gate is absolute, so a
    marked alert must not slip past it."""
    assert _spreads_only_blocks(_alert("BUY", small_account=True)) is True


def test_sells_are_never_blocked(on):
    """An instance that stops taking entries must still close what it holds."""
    assert _spreads_only_blocks(_alert("SELL")) is False


def test_the_gate_is_off_by_default(off):
    assert _spreads_only_blocks(_alert("BUY")) is False


def test_it_is_independent_of_small_account_only(monkeypatch):
    """The protection must not rest on an unrelated flag: with
    small_account_only OFF, spreads_only alone still refuses the BUY."""
    flags = {"spreads_only": True, "small_account_only": False}
    monkeypatch.setattr(cfg, "getboolean",
                        lambda s, k, fallback=None, **kw: flags.get(k, fallback))
    a = _alert("BUY")
    assert _small_account_only_blocks(a) is False   # would have allowed it
    assert _spreads_only_blocks(a) is True          # this one still stops it
