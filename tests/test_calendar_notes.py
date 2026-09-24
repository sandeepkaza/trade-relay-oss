"""
Journal notes on the calendar tab (2026-08-03).

Notes live in the SystemState key/value table under `journal:YYYY-MM-DD`.
The round trip that matters: save → the note comes back on /api/calendar even
for a day with no fills, and saving an empty string deletes it rather than
storing a blank row.
"""

import asyncio
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Point DB at an ephemeral file BEFORE db/models import.
_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp.close()
os.environ["TRADER_DATABASE_URL"] = f"sqlite:///{_tmp.name.replace(chr(92), '/')}"

import app.core.app as app_module  # noqa: E402
from app.core.db import init_db  # noqa: E402
from fastapi import HTTPException  # noqa: E402

init_db()


def _run(coro):
    return asyncio.run(coro)


def _save(date, note):
    return _run(app_module.save_calendar_note(
        app_module.JournalNote(date=date, note=note)))


def _notes():
    return _run(app_module.get_calendar(days=3650))["notes"]


def test_note_round_trip_on_a_day_with_no_trades():
    _save("2026-08-03", "  sat out, chop  ")
    assert _notes()["2026-08-03"] == "sat out, chop"  # stripped, and present


def test_empty_note_deletes_rather_than_storing_blank():
    _save("2026-08-04", "typo")
    assert "2026-08-04" in _notes()
    _save("2026-08-04", "   ")
    assert "2026-08-04" not in _notes()


def test_bad_date_rejected():
    with pytest.raises(HTTPException) as e:
        _save("08/05/2026", "nope")
    assert e.value.status_code == 400


def test_note_keys_are_dates_not_prefixed_rows():
    _save("2026-08-06", "keep")
    assert all(not k.startswith("journal:") for k in _notes())
