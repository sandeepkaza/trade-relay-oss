"""
Tests for signal_intel module — bias parser, level extraction, keyword
matching, dedup hash, recommendation engine, and end-to-end queue worker
(with mocked Vision OCR).

Run: PYTHONIOENCODING=utf-8 .venv/Scripts/python.exe tests/test_signal_intel.py
"""
import os
import sys
import json
import asyncio
import tempfile
import atexit
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TEST_DIR = tempfile.mkdtemp(prefix="trader_signal_test_")
_TEST_DB = os.path.join(_TEST_DIR, "test.db")
os.environ["TRADER_DATABASE_URL"] = f"sqlite:///{_TEST_DB}"
atexit.register(lambda: shutil.rmtree(_TEST_DIR, ignore_errors=True))

import app.core.db as db
db.init_db()

import app.ingest.signal_intel as si
from app.ingest.signal_intel import (
    parse_bias, parse_levels, parse_keywords, parse_signal,
    build_recommendation, content_hash,
)
from app.core.models import DiscordSignal
from app.core.db import SessionLocal

PASS = 0
FAIL = 0


def chk(label, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {label}")
    else:
        FAIL += 1
        print(f"  [FAIL] {label}" + (f" — {detail}" if detail else ""))


# ── Bias detection ───────────────────────────────────────────────────────────

def test_bias_bullish():
    bias, conf = parse_bias("This is bullish, calls into 6500 breakout, gamma squeeze")
    chk("Bullish text → bullish", bias == "bullish", f"got {bias}")
    chk("Bullish confidence > 0", conf > 0)


def test_bias_bearish():
    bias, conf = parse_bias("Bearish below 6400, puts working, breakdown into next support")
    chk("Bearish text → bearish", bias == "bearish", f"got {bias}")
    chk("Bearish confidence > 0", conf > 0)


def test_bias_neutral_no_signal():
    bias, conf = parse_bias("just some random text with no signal words")
    chk("No keywords → neutral", bias == "neutral")
    chk("No keywords → confidence 0", conf == 0.0)


def test_bias_neutral_explicit():
    bias, _ = parse_bias("chop range consolidation sideways")
    chk("Explicit neutral words → neutral", bias == "neutral")


def test_bias_empty():
    bias, conf = parse_bias("")
    chk("Empty text → neutral 0", bias == "neutral" and conf == 0.0)


# ── Level extraction ─────────────────────────────────────────────────────────

def test_levels_basic():
    levels = parse_levels("watching 6400 support and 6500 resistance, target 6600")
    chk("Three levels found", levels == [6400.0, 6500.0, 6600.0], f"got {levels}")


def test_levels_dedup():
    levels = parse_levels("6500 6500 6500 hold above 6500")
    chk("Dedup repeated level", levels == [6500.0])


def test_levels_filters_implausible():
    levels = parse_levels("RSI 50, P/E 25, target 6500")  # 50, 25 too small; 6500 valid
    chk("Filters small/large numbers", levels == [6500.0], f"got {levels}")


def test_levels_decimal():
    levels = parse_levels("strike 580.5 calls, level 632.25")
    chk("Decimal levels parsed", 580.5 in levels and 632.25 in levels)


def test_levels_empty():
    chk("Empty text → no levels", parse_levels("") == [])


# ── Keyword matching ─────────────────────────────────────────────────────────

def test_keywords_match():
    vocab = ["support", "resistance", "gamma", "squeeze"]
    found = parse_keywords("strong support holding, gamma squeeze setup", vocab)
    chk("Keyword matching", set(found) == {"support", "gamma", "squeeze"}, f"got {found}")


def test_keywords_case_insensitive():
    vocab = ["bullish"]
    found = parse_keywords("BULLISH break", vocab)
    chk("Case insensitive", "bullish" in found)


# ── parse_signal combined ────────────────────────────────────────────────────

def test_parse_signal_combines_text_and_ocr():
    out = parse_signal(
        message_text="bullish setup",
        ocr_text="resistance at 6500, support at 6400",
    )
    chk("Combines text+OCR for bias", out["bias"] == "bullish")
    chk("Combines text+OCR for levels", set(out["key_levels"]) == {6400.0, 6500.0})


# ── Recommendation engine ────────────────────────────────────────────────────

def test_rec_bullish_call_spread():
    rec = build_recommendation("bullish", [6400, 6500, 6600], 80.0, min_confidence=50)
    chk("Bullish → call spread", rec and rec["strategy"] == "call_spread")
    chk("Anchor = median", rec and rec["anchor"] == 6500)
    chk("Long < short for calls", rec and rec["long_strike"] < rec["short_strike"])


def test_rec_bearish_put_spread():
    rec = build_recommendation("bearish", [6400, 6500, 6600], 75.0, min_confidence=50)
    chk("Bearish → put spread", rec and rec["strategy"] == "put_spread")
    chk("Long > short for puts", rec and rec["long_strike"] > rec["short_strike"])


def test_rec_neutral_none():
    rec = build_recommendation("neutral", [6500], 99.0, min_confidence=50)
    chk("Neutral → None", rec is None)


def test_rec_low_confidence_none():
    rec = build_recommendation("bullish", [6500], 30.0, min_confidence=50)
    chk("Low confidence → None", rec is None)


def test_rec_no_levels_none():
    rec = build_recommendation("bullish", [], 90.0, min_confidence=50)
    chk("No levels → None", rec is None)


# ── Dedup hash ───────────────────────────────────────────────────────────────

def test_content_hash_stable():
    h1 = content_hash("123", "text", ["url1", "url2"])
    h2 = content_hash("123", "text", ["url2", "url1"])  # url order shouldn't matter
    chk("Hash stable across url order", h1 == h2)


def test_content_hash_distinct():
    h1 = content_hash("123", "text", [])
    h2 = content_hash("124", "text", [])
    chk("Different message_id → different hash", h1 != h2)


# ── End-to-end with worker (mocked OCR) ──────────────────────────────────────

async def _run_e2e():
    # Monkey-patch vision_ocr to avoid network/credential dependency
    import app.ingest.vision_ocr as vision_ocr
    async def fake_ocr(urls):
        if not urls:
            return ""
        return "OCR-extracted: support 6400 resistance 6500 bullish breakout"
    vision_ocr.ocr_image_urls = fake_ocr
    vision_ocr.is_available = lambda: True

    # Force config — accessor monkeypatch (cfg has no in-memory set)
    si._ENABLED = lambda: True
    si._VISION_ENABLED = lambda: True
    si._MIN_CONFIDENCE = lambda: 50.0
    si._KEYWORDS = lambda: ["support", "resistance", "bullish", "breakout"]

    # Process directly (no worker loop) — easier to assert
    sig_id = await si._process_one({
        "message_id": "msg-e2e-1",
        "author": "BacteriaNFA",
        "channel_id": "100000000000016665",
        "text": "Watching 6400 area",
        "image_urls": ["https://example.com/chart.png"],
        "timestamp": None,
    })
    chk("E2E persisted signal", sig_id is not None)

    # Verify DB record
    s = SessionLocal()
    try:
        row = s.query(DiscordSignal).filter(DiscordSignal.id == sig_id).first()
        chk("E2E DB row exists", row is not None)
        chk("E2E bias bullish (from OCR)", row.bias == "bullish", f"got {row.bias}")
        levels = json.loads(row.key_levels or "[]")
        chk("E2E OCR levels parsed", 6400.0 in levels and 6500.0 in levels, f"got {levels}")
        chk("E2E ocr_text persisted", row.ocr_text and "support" in row.ocr_text)
        chk("E2E recommendation generated", row.recommendation is not None)
        rec = json.loads(row.recommendation)
        chk("E2E recommendation strategy=call_spread", rec["strategy"] == "call_spread")
    finally:
        s.close()

    # Dedup: re-process same payload → returns existing id, no second row
    sig_id2 = await si._process_one({
        "message_id": "msg-e2e-1",
        "author": "BacteriaNFA",
        "channel_id": "100000000000016665",
        "text": "Watching 6400 area",
        "image_urls": ["https://example.com/chart.png"],
        "timestamp": None,
    })
    chk("E2E dedup returns same id", sig_id2 == sig_id)
    s = SessionLocal()
    try:
        cnt = s.query(DiscordSignal).filter(DiscordSignal.discord_message_id == "msg-e2e-1").count()
        chk("E2E only one DB row after dedup", cnt == 1, f"got {cnt}")
    finally:
        s.close()


def test_e2e():
    asyncio.run(_run_e2e())


def main():
    print("\n-- Signal Intel Tests ----------------------------------------")
    test_bias_bullish()
    test_bias_bearish()
    test_bias_neutral_no_signal()
    test_bias_neutral_explicit()
    test_bias_empty()
    test_levels_basic()
    test_levels_dedup()
    test_levels_filters_implausible()
    test_levels_decimal()
    test_levels_empty()
    test_keywords_match()
    test_keywords_case_insensitive()
    test_parse_signal_combines_text_and_ocr()
    test_rec_bullish_call_spread()
    test_rec_bearish_put_spread()
    test_rec_neutral_none()
    test_rec_low_confidence_none()
    test_rec_no_levels_none()
    test_content_hash_stable()
    test_content_hash_distinct()
    test_e2e()
    total = PASS + FAIL
    status = "PASS" if FAIL == 0 else "FAIL"
    print(f"\nSignal intel: {PASS}/{total}  [{status}]")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
