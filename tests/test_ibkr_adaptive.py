"""Adaptive-algo gating on the IBKR order path.

The asymmetry matters: a BUY that misses costs an opportunity, an analyst
SELL that misses costs the position (2026-05-08, +50% -> -13%). So the two
sides get separate flags, and every re-peg replacement stays a plain limit
regardless of either flag.
"""
import pytest

from app.core.config_manager import EDITABLE_KEYS
import app.execution.ibkr_broker as ib


def _side_uses_adaptive(side: str, force_plain: bool) -> bool:
    """Mirror of the gate in _place_on_public (kept in lockstep by the test
    below that asserts the source still reads this way)."""
    ok = ib._ADAPTIVE_ENTRY_ENABLED() if side == "BUY" else ib._ADAPTIVE_EXIT_ENABLED()
    return (not force_plain) and ok


class TestAdaptiveGating:
    def test_defaults_are_off(self, monkeypatch):
        monkeypatch.setattr(ib.cfg, "getboolean", lambda s, k, fallback=False: fallback)
        assert ib._ADAPTIVE_ENTRY_ENABLED() is False
        assert ib._ADAPTIVE_EXIT_ENABLED() is False

    def test_entry_flag_does_not_arm_sells(self, monkeypatch):
        monkeypatch.setattr(ib, "_ADAPTIVE_ENTRY_ENABLED", lambda: True)
        monkeypatch.setattr(ib, "_ADAPTIVE_EXIT_ENABLED", lambda: False)
        assert _side_uses_adaptive("BUY", False) is True
        assert _side_uses_adaptive("SELL", False) is False

    def test_exit_flag_arms_only_sells(self, monkeypatch):
        monkeypatch.setattr(ib, "_ADAPTIVE_ENTRY_ENABLED", lambda: False)
        monkeypatch.setattr(ib, "_ADAPTIVE_EXIT_ENABLED", lambda: True)
        assert _side_uses_adaptive("SELL", False) is True
        assert _side_uses_adaptive("BUY", False) is False

    @pytest.mark.parametrize("side", ["BUY", "SELL"])
    def test_force_plain_always_wins(self, monkeypatch, side):
        """Re-peg replacements and algo-reject fallbacks must never be Adaptive."""
        monkeypatch.setattr(ib, "_ADAPTIVE_ENTRY_ENABLED", lambda: True)
        monkeypatch.setattr(ib, "_ADAPTIVE_EXIT_ENABLED", lambda: True)
        assert _side_uses_adaptive(side, True) is False


class TestAdaptivePriority:
    @pytest.mark.parametrize("raw,want", [
        ("Normal", "Normal"), ("patient", "Patient"), ("URGENT", "Urgent"),
        ("nonsense", "Urgent"), ("", "Urgent"),
    ])
    def test_priority_normalised(self, monkeypatch, raw, want):
        monkeypatch.setattr(ib.cfg, "get", lambda s, k, fallback="": raw)
        assert ib._ADAPTIVE_PRIORITY() == want


class TestPortalEditable:
    @pytest.mark.parametrize("key", [
        "adaptive_entry_enabled", "adaptive_exit_enabled", "adaptive_priority",
    ])
    def test_key_is_portal_tunable(self, key):
        assert ("ibkr", key) in EDITABLE_KEYS


class TestRepegStaysPlain:
    def test_repeg_lanes_pass_force_plain(self):
        """Both SELL re-peg call sites must ask for a plain limit."""
        src = open("app/execution/public_executor.py", encoding="utf-8").read()
        assert src.count('force_plain=True') >= 2, \
            "SELL re-peg replacements must pass force_plain=True"

    def test_sell_placement_threads_the_flag(self):
        src = open("app/execution/public_executor.py", encoding="utf-8").read()
        assert "_force_plain=force_plain" in src


class _LogEntry:
    def __init__(self, errorCode=0, message=""):
        self.errorCode = errorCode
        self.message = message


class _Trade:
    def __init__(self, *entries):
        self.log = list(entries)


class TestAdaptiveRejectDetection:
    """Live SPXW rejects Adaptive with IBKR error 442; if _is_adaptive_reject
    misses it the order strands CANCELLED instead of re-placing plain
    (2026-08-20: two SPXW 7700C entries lost this way)."""

    @pytest.mark.parametrize("code,msg", [
        (442, "Specified algorithm is invalid."),
        (442, ""),                                   # code alone is enough
        (201, "Algorithmic orders are not supported for this contract"),
        (321, "unsupported algo strategy"),
        (200, "Adaptive algo not available"),
    ])
    def test_reject_recognised(self, code, msg):
        assert ib._is_adaptive_reject(_Trade(_LogEntry(code, msg))) is True

    @pytest.mark.parametrize("code,msg", [
        (0, "Specified algorithm is invalid."),      # no error code -> not a reject
        (202, "Order Canceled - reason: price conformance"),
        (0, ""),
    ])
    def test_non_reject_ignored(self, code, msg):
        assert ib._is_adaptive_reject(_Trade(_LogEntry(code, msg))) is False

    def test_empty_log(self):
        assert ib._is_adaptive_reject(_Trade()) is False


class TestAdaptiveUnsupportedRoots:
    """IBKR 442s Adaptive on index options. Sending it anyway costs a reject
    round-trip per entry before the plain re-place, which on 0DTE is the
    latency that matters — so index roots skip the algo up front."""

    @pytest.fixture(autouse=True)
    def _restore_roots(self):
        original = set(ib._ADAPTIVE_UNSUPPORTED_ROOTS)
        yield
        ib._ADAPTIVE_UNSUPPORTED_ROOTS.clear()
        ib._ADAPTIVE_UNSUPPORTED_ROOTS.update(original)

    @pytest.mark.parametrize("osi", [
        "SPXW260820C07700000",   # the live 2026-08-20 case
        "SPX260820C07700000",
        "NDXP260820C21000000",   # weekly variants collapse to the index root
        "RUTW260820C02300000",
        "VIX260820C00020000",
    ])
    def test_index_roots_skip_adaptive(self, osi):
        assert ib._adaptive_unsupported(osi) is True

    @pytest.mark.parametrize("osi", [
        "SPY260820C00650000",    # ETFs keep Adaptive - the algo works there
        "QQQ260820C00500000",
        "IWM260820C00230000",
        "AAPL260820C00250000",
        "MU260820C00095000",
    ])
    def test_equity_and_etf_roots_keep_adaptive(self, osi):
        assert ib._adaptive_unsupported(osi) is False

    def test_blank_symbol_keeps_adaptive(self):
        assert ib._adaptive_unsupported("") is False

    def test_reject_teaches_a_new_root(self):
        osi = "ZZZZ260820C00100000"
        assert ib._adaptive_unsupported(osi) is False
        assert ib._mark_adaptive_unsupported(osi) == "ZZZZ"
        assert ib._adaptive_unsupported(osi) is True
        # Already known -> returns None so the caller does not re-log.
        assert ib._mark_adaptive_unsupported(osi) is None

    def test_learning_is_per_root_not_per_contract(self):
        ib._mark_adaptive_unsupported("ZZZZ260820C00100000")
        # A different strike/expiry on the same root inherits the lesson.
        assert ib._adaptive_unsupported("ZZZZ261231P00500000") is True
        assert ib._adaptive_unsupported("YYYY260820C00100000") is False
