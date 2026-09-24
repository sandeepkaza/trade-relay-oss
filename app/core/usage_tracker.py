"""
usage_tracker.py — Daily counter for paid API consumption (persistent).

Tracks Vision API (Discord chart OCR) and Vertex AI / Gemini usage.
Counters reset at midnight America/New_York. Snapshot is read by the
daily-summary scheduler at 16:30 ET and posted to Discord.

Persistence: every increment writes the full state to
/app/data/api_usage_today.json (atomic rename). On first record_*()
call after process boot, the file is loaded if its date matches the
current ET date — so deploys mid-day don't zero the counters.
"""

import json
import os
import tempfile
import threading
from collections import defaultdict
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

_lock = threading.Lock()
_loaded_from_disk = False
_PERSIST_PATH = "/app/data/api_usage_today.json"


def _empty_state() -> dict[str, Any]:
    return {
        "date_et": None,
        "vision": {"calls": 0, "errors": 0},
        "vertex": {
            "calls": 0,
            "errors": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "by_model": defaultdict(lambda: {
                "calls": 0, "input_tokens": 0, "output_tokens": 0,
            }),
        },
    }


_state: dict[str, Any] = _empty_state()


def _today_et() -> str:
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")


def _maybe_reset_locked() -> None:
    """Caller must hold _lock. Resets state if the ET date has rolled over."""
    global _loaded_from_disk
    # Lazy-load persisted counts on first call after process boot. Skips if
    # the file is from a previous day (then we drop it and start fresh).
    if not _loaded_from_disk:
        _loaded_from_disk = True
        try:
            if os.path.exists(_PERSIST_PATH):
                with open(_PERSIST_PATH) as f:
                    data = json.load(f)
                if data.get("date_et") == _today_et():
                    # Restore in-place
                    _state["date_et"] = data["date_et"]
                    _state["vision"] = data.get("vision", {"calls": 0, "errors": 0})
                    vx = data.get("vertex", {})
                    bm = defaultdict(lambda: {"calls": 0, "input_tokens": 0, "output_tokens": 0})
                    for k, v in (vx.get("by_model") or {}).items():
                        bm[k] = dict(v)
                    _state["vertex"] = {
                        "calls": vx.get("calls", 0),
                        "errors": vx.get("errors", 0),
                        "input_tokens": vx.get("input_tokens", 0),
                        "output_tokens": vx.get("output_tokens", 0),
                        "by_model": bm,
                    }
        except Exception:
            pass

    today = _today_et()
    if _state["date_et"] != today:
        fresh = _empty_state()
        fresh["date_et"] = today
        _state.clear()
        _state.update(fresh)


def _persist_locked() -> None:
    """Caller must hold _lock. Atomic write of current state to JSON."""
    try:
        os.makedirs(os.path.dirname(_PERSIST_PATH), exist_ok=True)
        snap = {
            "date_et": _state["date_et"],
            "vision": dict(_state["vision"]),
            "vertex": {
                "calls": _state["vertex"]["calls"],
                "errors": _state["vertex"]["errors"],
                "input_tokens": _state["vertex"]["input_tokens"],
                "output_tokens": _state["vertex"]["output_tokens"],
                "by_model": {k: dict(v) for k, v in _state["vertex"]["by_model"].items()},
            },
        }
        # tempfile + rename for atomicity
        d = os.path.dirname(_PERSIST_PATH)
        with tempfile.NamedTemporaryFile("w", dir=d, delete=False) as tf:
            json.dump(snap, tf)
            tmp_path = tf.name
        os.replace(tmp_path, _PERSIST_PATH)
    except Exception:
        # Persistence failure must never break the call site.
        pass


def record_vision(success: bool = True) -> None:
    """Record one Vision API call (typically document_text_detection on a chart)."""
    with _lock:
        _maybe_reset_locked()
        _state["vision"]["calls"] += 1
        if not success:
            _state["vision"]["errors"] += 1
        _persist_locked()


def record_vertex(model: str, input_tokens: int = 0, output_tokens: int = 0,
                  success: bool = True) -> None:
    """Record one Vertex AI / Gemini call. Pass tokens from response.usage_metadata
    when available; pass 0 if the SDK didn't return them (still counts the call)."""
    with _lock:
        _maybe_reset_locked()
        v = _state["vertex"]
        v["calls"] += 1
        if not success:
            v["errors"] += 1
        v["input_tokens"] += int(input_tokens or 0)
        v["output_tokens"] += int(output_tokens or 0)
        bm = v["by_model"][model or "unknown"]
        bm["calls"] += 1
        bm["input_tokens"] += int(input_tokens or 0)
        bm["output_tokens"] += int(output_tokens or 0)
        _persist_locked()


def snapshot() -> dict[str, Any]:
    """Return a JSON-serialisable copy of today's counters."""
    with _lock:
        _maybe_reset_locked()
        return {
            "date_et": _state["date_et"],
            "vision": dict(_state["vision"]),
            "vertex": {
                "calls": _state["vertex"]["calls"],
                "errors": _state["vertex"]["errors"],
                "input_tokens": _state["vertex"]["input_tokens"],
                "output_tokens": _state["vertex"]["output_tokens"],
                "by_model": {
                    k: dict(v) for k, v in _state["vertex"]["by_model"].items()
                },
            },
        }
