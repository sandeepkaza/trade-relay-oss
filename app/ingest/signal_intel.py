"""
signal_intel.py — Discord Signal Intelligence pipeline.

Captures messages from a tracked Discord user (e.g. BacteriaNFA), runs OCR
on attached images via vision_ocr.py, parses bias/levels/keywords, generates
a structured signal record, and persists to the discord_signals table.

Pipeline:
    discord_listener → enqueue_message → _worker → parse → persist

Modules expose:
    start(): kick off background worker (called from app startup)
    stop():  graceful shutdown
    enqueue_message(payload): push a Discord message into the queue
    parse_signal(text, image_text): pure-fn parser (used by tests)
    build_recommendation(bias, levels): pure-fn recommender (used by tests)
"""

import asyncio
import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional

from app.core.config_manager import cfg
from app.core.db import SessionLocal
from app.core.models import DiscordSignal
import app.ingest.vision_ocr as vision_ocr

log = logging.getLogger(__name__)

# ── Config accessors (live values via cfg singleton) ─────────────────────────

def _ENABLED() -> bool:           return cfg.getboolean("signals", "enabled", False)
def _CHANNEL_ID() -> str:         return cfg.get("signals", "channel_id", "").strip()
def _AUTHOR() -> str:             return cfg.get("signals", "author", "").strip()
def _ACTION_MODE() -> str:        return cfg.get("signals", "action_mode", "display").strip().lower()
def _VISION_ENABLED() -> bool:    return cfg.getboolean("signals", "vision_enabled", True)
def _MIN_CONFIDENCE() -> float:   return cfg.getfloat("signals", "min_confidence_for_recommendation", 50.0)

def _KEYWORDS() -> list[str]:
    raw = cfg.get("signals", "keywords", "")
    return [k.strip().lower() for k in raw.split(",") if k.strip()]


# ── Bias lexicon ─────────────────────────────────────────────────────────────

_BULLISH_TERMS = {
    "bullish", "calls", "long", "buy", "breakout", "support hold", "squeeze",
    "rally", "moon", "rip", "pump", "upside", "strength", "gamma squeeze",
    "rip higher", "to the moon", "going up", "higher",
}
_BEARISH_TERMS = {
    "bearish", "puts", "short", "sell", "breakdown", "rejection", "dump",
    "crash", "downside", "weakness", "fade", "fade rally", "going down", "lower",
    "topping", "rolling over",
}
_NEUTRAL_TERMS = {
    "chop", "range", "consolidation", "sideways", "neutral", "wait",
    "no trade", "unclear", "mixed",
}

# Match 3-5 digit numbers, optional decimal — common option strikes / SPX levels
_LEVEL_RE = re.compile(r"\b(\d{3,5}(?:\.\d{1,2})?)\b")


# ── Pure-fn parser (no I/O, fully testable) ───────────────────────────────────

def parse_bias(text: str) -> tuple[str, float]:
    """Return (bias, confidence_pct).
    Confidence = matched_terms / total_terms_in_winning_class * 100, clamped 0..100.
    """
    if not text:
        return "neutral", 0.0
    t = text.lower()
    bull = sum(1 for w in _BULLISH_TERMS if w in t)
    bear = sum(1 for w in _BEARISH_TERMS if w in t)
    neut = sum(1 for w in _NEUTRAL_TERMS if w in t)

    total = bull + bear + neut
    if total == 0:
        return "neutral", 0.0

    if bull > bear and bull > neut:
        return "bullish", min(100.0, (bull / total) * 100 + 20 * min(bull, 3))
    if bear > bull and bear > neut:
        return "bearish", min(100.0, (bear / total) * 100 + 20 * min(bear, 3))
    return "neutral", min(100.0, (max(neut, 1) / total) * 100)


def parse_levels(text: str, max_n: int = 8) -> list[float]:
    """Extract numeric levels (3-5 digit numbers). De-dupes, sorts ascending."""
    if not text:
        return []
    # Strip date tokens (YYYY-MM-DD, YYYY/MM/DD, MM/DD/YYYY) before regex so
    # year/month/day digits aren't swept up as strike candidates. Sniper's
    # whale-tracker embed includes "Expiry: 2026-05-30" — without this strip
    # the year 2026 landed in levels and the median-anchor heuristic in
    # build_recommendation produced a 2026C / 2126C call spread (garbage).
    clean = re.sub(r"\b(?:19|20)\d{2}[-/]\d{1,2}[-/]\d{1,2}\b", " ", text)
    clean = re.sub(r"\b\d{1,2}[-/]\d{1,2}[-/](?:19|20)\d{2}\b", " ", clean)
    raw = _LEVEL_RE.findall(clean)
    seen = set()
    out = []
    for s in raw:
        try:
            v = float(s)
        except ValueError:
            continue
        # Filter implausible: SPX 4xxx-7xxx, equity strikes 100-2000 mostly.
        if v < 100 or v > 99999:
            continue
        # Reject bare year tokens that survived date-stripping (e.g.
        # standalone "2026" in chitchat). No realistic option strike sits
        # in 1900-2099 — TSLA, BRK.B etc. are all <2000 in this band.
        if 1900 <= v <= 2099 and v == int(v):
            continue
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    out.sort()
    return out[:max_n]


def parse_keywords(text: str, vocabulary: Optional[list[str]] = None) -> list[str]:
    """Return matched keywords from `vocabulary` (defaults to config)."""
    if not text:
        return []
    vocab = vocabulary if vocabulary is not None else _KEYWORDS()
    t = text.lower()
    found = []
    for kw in vocab:
        if kw and kw in t:
            found.append(kw)
    return found


def parse_signal(message_text: str, ocr_text: str = "") -> dict[str, Any]:
    """Pure parser — combines message + OCR, returns structured signal fields."""
    combined = (message_text or "") + "\n" + (ocr_text or "")
    bias, confidence = parse_bias(combined)
    levels = parse_levels(combined)
    keywords = parse_keywords(combined)
    return {
        "bias": bias,
        "confidence": round(confidence, 1),
        "key_levels": levels,
        "keywords": keywords,
    }


# ── Recommendation engine (modular for future ML upgrade) ────────────────────

def build_recommendation(
    bias: str,
    levels: list[float],
    confidence: float,
    *,
    min_confidence: float = 50.0,
) -> Optional[dict[str, Any]]:
    """Generate a trade recommendation from parsed signal.

    Bullish → call spread (ATM long, +width short)
    Bearish → put spread (ATM long, -width short)
    Neutral → None

    Returns None if confidence below threshold OR no levels.
    """
    if not levels or bias == "neutral" or confidence < min_confidence:
        return None

    # Use the median level as the anchor — most representative of "current spot"
    sorted_levels = sorted(levels)
    anchor = sorted_levels[len(sorted_levels) // 2]

    # Width: 5% of anchor, rounded to nearest $5
    width = max(5, round((anchor * 0.05) / 5) * 5)

    if bias == "bullish":
        return {
            "strategy": "call_spread",
            "anchor": anchor,
            "long_strike": anchor,
            "short_strike": anchor + width,
            "width": width,
            "notes": f"Bullish bias (conf {confidence:.0f}%). Long {anchor}C / short {anchor + width}C.",
        }
    if bias == "bearish":
        return {
            "strategy": "put_spread",
            "anchor": anchor,
            "long_strike": anchor,
            "short_strike": anchor - width,
            "width": width,
            "notes": f"Bearish bias (conf {confidence:.0f}%). Long {anchor}P / short {anchor - width}P.",
        }
    return None


# ── Dedup ─────────────────────────────────────────────────────────────────────

def content_hash(message_id: str, text: str, image_urls: list[str]) -> str:
    """Stable hash for dedup. Uses Discord message ID as primary key plus
    a hash of text/images so edits don't collide with originals."""
    payload = f"{message_id}|{text or ''}|{','.join(sorted(image_urls or []))}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


# ── Async worker + queue ──────────────────────────────────────────────────────

_queue: Optional[asyncio.Queue] = None
_worker_task: Optional[asyncio.Task] = None
_shutdown = False


def _get_queue() -> asyncio.Queue:
    global _queue
    if _queue is None:
        _queue = asyncio.Queue(maxsize=1000)
    return _queue


async def enqueue_message(payload: dict) -> None:
    """Called by discord_listener when a tracked-author message arrives.
    payload keys: message_id, author, channel_id, text, image_urls, timestamp"""
    if not _ENABLED():
        return
    q = _get_queue()
    try:
        q.put_nowait(payload)
    except asyncio.QueueFull:
        log.warning("[SIGNAL_INTEL] queue full — dropping message %s", payload.get("message_id"))


async def _process_one(payload: dict) -> Optional[int]:
    """Process a single message: OCR, parse, persist. Returns DB id or None."""
    msg_id = str(payload.get("message_id", ""))
    text = payload.get("text", "") or ""
    image_urls = payload.get("image_urls", []) or []
    author = payload.get("author", "")
    channel_id = str(payload.get("channel_id", ""))
    ts = payload.get("timestamp") or datetime.now(timezone.utc)
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            ts = datetime.now(timezone.utc)

    chash = content_hash(msg_id, text, image_urls)

    # Dedup: skip if we've already stored this hash
    db = SessionLocal()
    try:
        existing = db.query(DiscordSignal).filter(DiscordSignal.content_hash == chash).first()
        if existing:
            log.debug("[SIGNAL_INTEL] dedup hit message_id=%s id=%s", msg_id, existing.id)
            return existing.id
    finally:
        db.close()

    # OCR (best-effort)
    ocr_text = ""
    if image_urls and _VISION_ENABLED():
        try:
            ocr_text = await vision_ocr.ocr_image_urls(image_urls)
        except Exception as e:
            log.error("[SIGNAL_INTEL] OCR pipeline error: %s", e)

    parsed = parse_signal(text, ocr_text)
    rec = build_recommendation(
        parsed["bias"], parsed["key_levels"], parsed["confidence"],
        min_confidence=_MIN_CONFIDENCE(),
    )

    # AI multimodal advisor — pass text + OCR + chart image bytes to Gemini
    ai_rec = None
    try:
        import app.execution.ai_signal_advisor as ai_signal_advisor
        if ai_signal_advisor._enabled():
            ai_rec = await ai_signal_advisor.analyze(text, ocr_text, image_urls)
    except Exception as e:
        log.error("[SIGNAL_INTEL] AI advisor error: %s", e)

    # Structured-alert trade evaluator — for clean "Stock|Strike|Expiry|Entry|
    # Action" whale-tracker alerts. Pulls real option data + greeks and scores
    # trade quality. Only fires when text parses as a structured alert; no-op
    # otherwise (returns None), so commentary posts are unaffected.
    trade_eval = None
    try:
        import app.execution.trade_evaluator as trade_evaluator
        if trade_evaluator._enabled():
            trade_eval = await trade_evaluator.evaluate(text)
    except Exception as e:
        log.error("[SIGNAL_INTEL] trade evaluator error: %s", e)

    db = SessionLocal()
    try:
        sig = DiscordSignal(
            timestamp=ts,
            author=author,
            channel_id=channel_id,
            discord_message_id=msg_id,
            raw_text=text,
            ocr_text=ocr_text or None,
            image_urls=json.dumps(image_urls) if image_urls else None,
            bias=parsed["bias"],
            key_levels=json.dumps(parsed["key_levels"]),
            keywords=json.dumps(parsed["keywords"]),
            confidence=parsed["confidence"],
            recommendation=json.dumps(rec) if rec else None,
            ai_recommendation=json.dumps(ai_rec) if ai_rec else None,
            content_hash=chash,
        )
        db.add(sig)
        db.commit()
        db.refresh(sig)
        log.info(
            "[SIGNAL_INTEL] stored id=%s bias=%s conf=%.0f levels=%s rec=%s ai=%s eval=%s",
            sig.id, parsed["bias"], parsed["confidence"], parsed["key_levels"],
            (rec or {}).get("strategy", "none"),
            (ai_rec or {}).get("trade", {}).get("strategy", "none") if ai_rec else "off",
            f"{trade_eval['grade']}/{trade_eval['score']}" if trade_eval else "off",
        )

        # Broadcast to dashboard WebSocket clients (best-effort)
        try:
            from app import manager as ws_manager
            await ws_manager.broadcast({"type": "discord_signal", "data": sig.to_dict()})
        except Exception as e:
            log.debug("[SIGNAL_INTEL] WS broadcast failed: %s", e)

        # Post AI recommendation to Discord webhook if configured
        if ai_rec:
            try:
                import app.execution.ai_signal_advisor as ai_signal_advisor
                if ai_signal_advisor._post_to_webhook():
                    payload = ai_signal_advisor.format_for_discord(ai_rec, sig.id, text)
                    await ai_signal_advisor.post_to_webhook(payload)
            except Exception as e:
                log.error("[SIGNAL_INTEL] webhook post failed: %s", e)

        # Trade-quality verdict for structured alerts — broadcast + webhook.
        if trade_eval:
            try:
                from app import manager as ws_manager
                await ws_manager.broadcast({"type": "trade_eval", "data": trade_eval})
            except Exception as e:
                log.debug("[SIGNAL_INTEL] trade_eval WS broadcast failed: %s", e)
            try:
                import app.execution.trade_evaluator as trade_evaluator
                import app.execution.ai_signal_advisor as ai_signal_advisor
                if trade_evaluator._post_to_webhook():
                    te_payload = trade_evaluator.format_for_discord(trade_eval, sig.id)
                    await ai_signal_advisor.post_to_webhook(te_payload)
            except Exception as e:
                log.error("[SIGNAL_INTEL] trade_eval webhook post failed: %s", e)

        return sig.id
    except Exception as e:
        db.rollback()
        log.error("[SIGNAL_INTEL] DB write failed: %s", e)
        return None
    finally:
        db.close()


async def _worker_loop():
    log.info("[SIGNAL_INTEL] worker started")
    q = _get_queue()
    while not _shutdown:
        try:
            payload = await asyncio.wait_for(q.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        try:
            await _process_one(payload)
        except Exception as e:
            log.error("[SIGNAL_INTEL] worker exception: %s", e, exc_info=True)
        finally:
            q.task_done()
    log.info("[SIGNAL_INTEL] worker stopped")


def start() -> None:
    """Start the background worker. Idempotent."""
    global _worker_task, _shutdown
    if _worker_task and not _worker_task.done():
        return
    _shutdown = False
    loop = asyncio.get_event_loop()
    _worker_task = loop.create_task(_worker_loop(), name="signal_intel_worker")
    # Warm AI advisor client so first real signal doesn't pay cold-start latency
    try:
        import app.execution.ai_signal_advisor as ai_signal_advisor
        if ai_signal_advisor._enabled():
            loop.run_in_executor(None, ai_signal_advisor._get_client)
    except Exception as e:
        log.debug("[SIGNAL_INTEL] AI advisor warm-up skipped: %s", e)
    log.info("[SIGNAL_INTEL] enabled=%s author=%s channel=%s mode=%s vision=%s",
             _ENABLED(), _AUTHOR(), _CHANNEL_ID(), _ACTION_MODE(),
             _VISION_ENABLED() and vision_ocr.is_available())


async def stop() -> None:
    """Graceful shutdown — waits for queue drain."""
    global _shutdown
    _shutdown = True
    if _worker_task:
        try:
            await asyncio.wait_for(_worker_task, timeout=5.0)
        except asyncio.TimeoutError:
            log.warning("[SIGNAL_INTEL] worker did not stop in 5s; cancelling")
            _worker_task.cancel()
