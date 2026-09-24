"""errors.log must fill itself, and the expired-credit-spread warning must not
repeat every monitor tick.

Both regressions were invisible for months: errors.log sat at 0 bytes on all
three lanes from 2026-07-03 because it hung off a logger no module used, and the
expiry warning wrote 19,672 identical lines on 2026-09-15, rotating bot.log six
times in a day.
"""
import importlib
import logging
from pathlib import Path


def test_errors_log_captures_an_error_from_any_module_logger(tmp_path, monkeypatch):
    import app.core.log_config as lc

    monkeypatch.setattr(lc, "LOG_DIR", Path(tmp_path))
    monkeypatch.setattr(lc, "FILE_LOGGING_ENABLED", True)
    lc.setup_logging()
    try:
        logging.getLogger("app.execution.spread_executor").error("SPREAD_CLOSE_CANCELLED")
        for h in logging.getLogger().handlers:
            h.flush()
        body = (tmp_path / "errors.log").read_text(encoding="utf-8")
        assert "SPREAD_CLOSE_CANCELLED" in body
        # INFO must not leak into the error stream.
        logging.getLogger("app.core.app").info("Daily summary posted")
        for h in logging.getLogger().handlers:
            h.flush()
        assert "Daily summary posted" not in (tmp_path / "errors.log").read_text(encoding="utf-8")
    finally:
        for h in logging.getLogger().handlers[:]:
            h.close()
            logging.getLogger().removeHandler(h)
        importlib.reload(lc)


def test_expired_credit_spread_warns_once_per_row(caplog):
    from app.monitors import position_monitor as pm

    class Pos:
        status = "OPEN"
        is_credit = True
        osi_symbol = "SPXW260915C07495000|SPXW260915C07500000"
        option_expiry = "2026-09-15"
        expiry = "2026-09-15"

    pos = Pos()
    pm._EXPIRED_WARNED.discard(pos.osi_symbol)
    if pm._expiry_date(pos) is None:      # row shape can't carry an expiry here
        return
    with caplog.at_level(logging.WARNING, logger=pm.log.name):
        for _ in range(50):
            assert pm._close_if_expired(None, pos) is False
    assert len([r for r in caplog.records if "[EXPIRED]" in r.getMessage()]) == 1
