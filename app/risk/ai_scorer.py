"""
ai_scorer.py - AI Trade Confidence Scorer.

Scores each incoming BUY alert (0-100) based on:
  1. Analyst win rate and historical performance (from DB)
  2. Size tag / conviction level from the alert
  3. Time of day (better fills near open, worse near close)
  4. Current market conditions (existing exposure, daily P&L)

The score adjusts position sizing:
  - 0-30:  SKIP (don't trade)
  - 31-50: 1 contract (minimum)
  - 51-70: use size tag as-is
  - 71-85: size tag + 1 contract bonus
  - 86-100: size tag + 2 contract bonus (high conviction)

Enable via config.ini:
    [trading]
    ai_scorer_enabled = true
    ai_scorer_min_score = 30
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from app.core.db import SessionLocal
from app.core.models import Alert, Position
from app.core.config_manager import cfg  # hot-reloadable singleton

log = logging.getLogger(__name__)

# Live config accessors (read on every call — dashboard changes take effect immediately)
def _SCORER_ENABLED(): return cfg.getboolean("trading", "ai_scorer_enabled", fallback=False)
def _MIN_SCORE():      return cfg.getint("trading", "ai_scorer_min_score", fallback=30)
def _PROVIDER():       return (cfg.get("trading", "ai_scorer_provider", fallback="rules") or "rules").strip().lower()


# ── Gemini client (lazy, shares Vertex setup with ai_signal_advisor) ────────
_genai_client = None
_client_init_attempted = False


def _get_gemini_client():
    """Lazy-init Vertex AI client. Same project/location as ai_parser."""
    global _genai_client, _client_init_attempted
    if _genai_client is not None:
        return _genai_client
    if _client_init_attempted:
        return None
    _client_init_attempted = True
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds or not os.path.isfile(creds):
        log.warning("[SCORER_GEMINI] GOOGLE_APPLICATION_CREDENTIALS missing — falling back to rules")
        return None
    try:
        from google import genai  # type: ignore
        project = cfg.get("ai_parser", "project", fallback="your-gcp-project")
        location = cfg.get("ai_parser", "location", fallback="us-east4")
        _genai_client = genai.Client(vertexai=True, project=project, location=location)
        log.info("[SCORER_GEMINI] Vertex client init (project=%s loc=%s)", project, location)
        return _genai_client
    except Exception as e:
        log.error("[SCORER_GEMINI] client init failed: %s — falling back to rules", e)
        return None


_GEMINI_SYSTEM = """You are a risk arbiter for a live options-trading bot. You score each BUY alert 0-100 based on signal quality, analyst track record, market regime, and current portfolio state. Output JSON only — no prose, no markdown.

Schema:
{"score": <int 0-100>, "rationale": "<<= 30 words>"}

Rules:
- Treat analyst rolling 30d win rate as the strongest signal: <30% wr → cap score at 35; 30-50% → cap at 60; 50-70% → up to 80; >70% → up to 95.
- Late-day entries (>14:30 ET) on same-day-expiry options: -10 points.
- Bad day (daily P/L < -$500): -8 points (drawdown discipline).
- Many open positions (>=4): -8 points.
- Same-symbol concentration (>=2 already open in this underlying): -10.
- VIX spike or stressed regime context: -10 (data may indicate).
- High-conviction size tag (XL, FULL): +5; SMALL/XS: -3.
- Score never above 95 unless author win rate >= 70% AND market regime favorable.
- Never recommend below 25 unless data overwhelmingly bearish for this signal."""


def _gemini_score(payload: dict, timeout_s: float = 1.5) -> Optional[dict]:
    client = _get_gemini_client()
    if client is None:
        return None
    try:
        from google.genai import types  # type: ignore
    except ImportError:
        log.error("[SCORER_GEMINI] google-genai missing")
        return None
    model = cfg.get("trading", "ai_scorer_model", fallback="gemini-2.5-flash")
    parts = [types.Part.from_text(text=json.dumps(payload, default=str))]
    cfg_kwargs = dict(
        system_instruction=_GEMINI_SYSTEM,
        temperature=0.1,
        max_output_tokens=512,
        response_mime_type="application/json",
    )
    try:
        cfg_kwargs["thinking_config"] = types.ThinkingConfig(thinking_budget=128)
    except Exception:
        pass

    def _call():
        return client.models.generate_content(
            model=model,
            contents=[types.Content(role="user", parts=parts)],
            config=types.GenerateContentConfig(**cfg_kwargs),
        )

    try:
        # Run synchronously inside a thread; this function is called from a
        # sync context in the BUY pipeline (public_executor.score_alert).
        # Wait at most timeout_s; on overrun, return None and let caller
        # fall back to rules. Don't block the trade.
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_call)
            response = fut.result(timeout=timeout_s)
        try:
            import app.core.usage_tracker as _ut
            um = getattr(response, "usage_metadata", None)
            in_t = int(getattr(um, "prompt_token_count", 0) or 0) if um else 0
            out_t = int(getattr(um, "candidates_token_count", 0) or 0) if um else 0
            _ut.record_vertex(model=model, input_tokens=in_t, output_tokens=out_t, success=True)
        except Exception:
            pass
    except concurrent.futures.TimeoutError:
        log.warning("[SCORER_GEMINI] timeout %.1fs — falling back to rules", timeout_s)
        try:
            import app.core.usage_tracker as _ut
            _ut.record_vertex(model=model, success=False)
        except Exception:
            pass
        return None
    except Exception as e:
        log.error("[SCORER_GEMINI] call failed: %s — falling back to rules", e)
        try:
            import app.core.usage_tracker as _ut
            _ut.record_vertex(model=model, success=False)
        except Exception:
            pass
        return None

    raw = (getattr(response, "text", "") or "").strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    try:
        rec = json.loads(raw)
    except Exception as e:
        log.error("[SCORER_GEMINI] invalid JSON: %s | raw=%s", e, raw[:200])
        return None
    score = rec.get("score")
    if not isinstance(score, (int, float)) or not (0 <= score <= 100):
        return None
    return {"score": int(score), "rationale": str(rec.get("rationale", ""))[:200]}


def score_alert(
    action: str,
    author: str,
    symbol: str,
    osi_symbol: str,
    price: Optional[float],
    size_tag: str,
) -> dict:
    """
    Score a trade alert and return a dict with:
      - score (0-100)
      - adjusted_qty (int or None if unchanged)
      - breakdown (dict of component scores)
      - reason (human-readable explanation)
    """
    if not _SCORER_ENABLED():
        return {"score": 100, "adjusted_qty": None, "breakdown": {}, "reason": "Scorer disabled"}

    if action != "BUY":
        return {"score": 100, "adjusted_qty": None, "breakdown": {}, "reason": "SELL alerts bypass scoring"}

    # ── Gemini provider — Vertex Gemini Flash inference ─────────────────────
    # Falls back to rules-based scoring on timeout, auth failure, or parse
    # failure. Both providers share the same return shape so downstream
    # (adjust_quantity, broadcasts, logging) is identical.
    if _PROVIDER() == "gemini":
        gemini_payload = _build_gemini_payload(author, symbol, osi_symbol, price, size_tag)
        gemini_result = _gemini_score(gemini_payload, timeout_s=1.5)
        if gemini_result is not None:
            total = gemini_result["score"]
            reason = f"[gemini] {gemini_result.get('rationale') or _build_reason(total, {}, author)}"
            log.info("[SCORER_GEMINI] %s score=%d %s", osi_symbol, total, reason)
            return {
                "score": total,
                "adjusted_qty": None,
                "breakdown": {"gemini": {"score": total, "max": 100, "detail": gemini_result.get("rationale") or "—"}},
                "reason": reason,
            }
        # else: fall through to rules

    breakdown = {}

    # ── 1. Analyst performance (0-40 points) ─────────────────────────────
    analyst_score, analyst_detail = _score_analyst(author)
    breakdown["analyst"] = {"score": analyst_score, "max": 40, "detail": analyst_detail}

    # ── 2. Size tag / conviction (0-20 points) ───────────────────────────
    size_score, size_detail = _score_size_tag(size_tag)
    breakdown["conviction"] = {"score": size_score, "max": 20, "detail": size_detail}

    # ── 3. Time of day (0-20 points) ─────────────────────────────────────
    time_score, time_detail = _score_time_of_day()
    breakdown["timing"] = {"score": time_score, "max": 20, "detail": time_detail}

    # ── 4. Market conditions / exposure (0-20 points) ────────────────────
    market_score, market_detail = _score_market_conditions(symbol, price)
    breakdown["market"] = {"score": market_score, "max": 20, "detail": market_detail}

    total = analyst_score + size_score + time_score + market_score
    total = max(0, min(100, total))

    # Build reason
    reason = _build_reason(total, breakdown, author)

    result = {
        "score": total,
        "adjusted_qty": None,
        "breakdown": breakdown,
        "reason": reason,
    }

    log.info(
        "[SCORER] %s alert from %s: score=%d (%s)",
        osi_symbol, author or "unknown", total, reason,
    )

    return result


def adjust_quantity(base_qty: int, score: int, size_tag: str) -> int:
    """
    Adjust position size based on confidence score.
    Returns the adjusted quantity.

    The +1/+2 bonuses are clamped against an absolute hard cap so a malicious
    or misparsed alert (e.g. a rogue "XL" size tag mapping to 20 contracts +2
    bonus = 22) cannot bypass the size config. Guardrails apply downstream
    too, but capping here is defense-in-depth at the scoring layer.
    """
    HARD_CAP = 10  # max contracts the scorer is allowed to recommend
    min_score = _MIN_SCORE()
    if score <= min_score:
        return 0  # Don't trade
    elif score <= 50:
        return min(1, HARD_CAP)
    elif score <= 70:
        return min(base_qty, HARD_CAP)
    elif score <= 85:
        return min(base_qty + 1, HARD_CAP)
    else:
        return min(base_qty + 2, HARD_CAP)


# ── Gemini payload builder ───────────────────────────────────────────────────

def _build_gemini_payload(author: str, symbol: str, osi_symbol: str,
                           price: Optional[float], size_tag: str) -> dict:
    """Assemble the JSON payload for Gemini scoring. Includes analyst track
    record (rolling 30d), open positions, daily P/L, and the alert itself."""
    analyst_score, analyst_detail = _score_analyst(author)

    db = SessionLocal()
    try:
        open_positions = db.query(Position).filter(
            Position.status.in_(["OPEN", "PARTIAL"]),
            Position.remaining > 0,
        ).all()
        same_symbol_open = sum(1 for p in open_positions if p.symbol == symbol)

        from zoneinfo import ZoneInfo as _ZI
        _et_now = datetime.now(_ZI("America/New_York"))
        today_start = _et_now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)

        unrealized_pnl = sum(
            (p.current_price - p.avg_price) * p.remaining * 100
            for p in open_positions
            if p.avg_price and p.current_price
        )
        closed_today = db.query(Position).filter(
            Position.close_time >= today_start,
            Position.status == "CLOSED",
        ).all()
        realized_pnl = sum(
            (p.close_price - p.avg_price) * p.total_contracts * 100
            for p in closed_today
            if p.avg_price and p.close_price
        )
    finally:
        db.close()

    et_now = datetime.now(ZoneInfo("America/New_York"))
    return {
        "alert": {
            "author": author or "unknown",
            "symbol": symbol,
            "osi_symbol": osi_symbol,
            "price": price,
            "size_tag": size_tag or "",
        },
        "analyst_track_record_30d": analyst_detail,
        "analyst_rule_score_40max": analyst_score,
        "portfolio": {
            "open_positions": len(open_positions),
            "same_symbol_open_positions": same_symbol_open,
            "unrealized_pnl_usd": round(unrealized_pnl, 2),
            "realized_pnl_today_usd": round(realized_pnl, 2),
        },
        "regime": {
            "et_time": et_now.strftime("%H:%M"),
            "weekday": et_now.strftime("%A"),
        },
    }


# ── Component scorers ────────────────────────────────────────────────────────

def _score_analyst(author: str) -> tuple[int, str]:
    """
    Score based on analyst's historical win rate.
    Looks at past filled BUY alerts from this author and their outcomes.
    """
    if not author:
        return 20, "Unknown analyst (default score)"

    db = SessionLocal()
    try:
        author_lower = author.lower()
        lookback = datetime.now(timezone.utc) - timedelta(days=30)

        # Find all BUY alerts from this author in the last 30 days
        alerts = db.query(Alert).filter(
            Alert.author.ilike(f"%{author_lower}%"),
            Alert.action == "BUY",
            Alert.status == "FILLED",
            Alert.timestamp >= lookback,
        ).all()

        if len(alerts) < 3:
            return 20, f"{author}: <3 trades in 30d (not enough data)"

        # Match alerts to CLOSED positions only (open positions don't have a real exit price)
        wins = 0
        losses = 0
        total_pnl_pct = 0.0

        for alert in alerts:
            if not alert.osi_symbol:
                continue
            pos = db.query(Position).filter_by(
                osi_symbol=alert.osi_symbol, status="CLOSED"
            ).first()
            if not pos or not pos.avg_price:
                continue

            # Use close_price for realized P&L (current_price is last live tick, not exit price).
            # pos.pnl_pct() inverts credit spreads so a winning CCS is not a loss.
            if pos.close_price is None and pos.current_price is None:
                continue

            pnl_pct = pos.pnl_pct()

            if pnl_pct > 0:
                wins += 1
            else:
                losses += 1
            total_pnl_pct += pnl_pct

        total_trades = wins + losses
        if total_trades == 0:
            return 20, f"{author}: no closed position data available"

        win_rate = wins / total_trades
        avg_pnl = total_pnl_pct / total_trades

        # Score: 60%+ win rate → full points, below 40% → low score
        if win_rate >= 0.7:
            score = 40
        elif win_rate >= 0.6:
            score = 33
        elif win_rate >= 0.5:
            score = 25
        elif win_rate >= 0.4:
            score = 18
        else:
            score = 10

        # Bonus/penalty for avg P&L
        if avg_pnl > 20:
            score = min(40, score + 5)
        elif avg_pnl < -10:
            score = max(0, score - 5)

        detail = f"{author}: {wins}W/{losses}L ({win_rate:.0%} win rate), avg {avg_pnl:+.1f}% over {total_trades} closed trades"
        return score, detail

    finally:
        db.close()


def _score_size_tag(size_tag: str) -> tuple[int, str]:
    """
    Score based on the alert's size tag (proxy for analyst conviction).
    Higher size = higher conviction from the alert source.
    """
    tag = (size_tag or "").upper()
    scores = {
        "XL": (20, "XL position (maximum conviction)"),
        "FULL": (18, "FULL position (very high conviction)"),
        "LARGE": (16, "LARGE position (high conviction)"),
        "MEDIUM": (12, "MEDIUM position (moderate conviction)"),
        "SMALL": (8, "SMALL position (low conviction)"),
        "XS": (5, "XS position (minimal conviction)"),
    }
    return scores.get(tag, (10, "No size tag (default)"))


def _score_time_of_day() -> tuple[int, str]:
    """
    Score based on time of day (ET).
    Best: 9:30-10:30 (opening momentum)
    Good: 10:30-14:00 (midday)
    OK:   14:00-15:00 (afternoon)
    Poor: 15:00-15:30 (late day, risky)
    Bad:  15:30-16:00 (too close to close)
    """
    et = datetime.now(ZoneInfo("America/New_York"))
    hour, minute = et.hour, et.minute
    t = hour + minute / 60

    if et.weekday() >= 5:
        return 5, f"Weekend ({et.strftime('%A')})"

    if t < 9.5:
        return 10, f"Pre-market ({et.strftime('%H:%M ET')})"
    elif t <= 10.5:
        return 20, f"Opening hour ({et.strftime('%H:%M ET')}) - best momentum"
    elif t <= 14.0:
        return 16, f"Midday ({et.strftime('%H:%M ET')}) - stable"
    elif t <= 15.0:
        return 12, f"Afternoon ({et.strftime('%H:%M ET')}) - moderate"
    elif t <= 15.5:
        return 7, f"Late day ({et.strftime('%H:%M ET')}) - risky"
    elif t <= 16.0:
        return 3, f"Near close ({et.strftime('%H:%M ET')}) - avoid new entries"
    else:
        return 5, f"After hours ({et.strftime('%H:%M ET')})"


def _score_market_conditions(symbol: str, price: Optional[float]) -> tuple[int, str]:
    """
    Score based on current portfolio exposure and daily P&L.
    Penalizes adding when already heavily exposed or losing.
    """
    db = SessionLocal()
    try:
        # Check current exposure
        open_positions = db.query(Position).filter(
            Position.status.in_(["OPEN", "PARTIAL"]),
            Position.remaining > 0,
        ).all()

        open_count = len(open_positions)
        same_symbol_count = sum(1 for p in open_positions if p.symbol == symbol)

        # Calculate daily P&L — anchor on ET midnight (the trading day),
        # not UTC midnight. After 8 PM ET the UTC date rolls forward and a
        # UTC-midnight window would empty out today's closed positions →
        # daily_pnl looks artificially good → scorer inflates and the bot
        # over-trades pre-midnight ET.
        from zoneinfo import ZoneInfo as _ZI
        _et = _ZI("America/New_York")
        _et_now = datetime.now(_et)
        _et_midnight = _et_now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start = _et_midnight.astimezone(timezone.utc)

        unrealized_pnl = sum(
            (p.current_price - p.avg_price) * p.remaining * 100
            for p in open_positions
            if p.avg_price and p.current_price
        )

        closed_today = db.query(Position).filter(
            Position.close_time >= today_start,
            Position.status == "CLOSED",
        ).all()
        realized_pnl = sum(
            (p.close_price - p.avg_price) * p.total_contracts * 100
            for p in closed_today
            if p.avg_price and p.close_price  # use close_price for accuracy
        )

        daily_pnl = unrealized_pnl + realized_pnl

        # Start with 20 points
        score = 20
        details = []

        # Penalty for too many open positions
        if open_count >= 4:
            score -= 8
            details.append(f"{open_count} open positions (heavy)")
        elif open_count >= 2:
            score -= 3
            details.append(f"{open_count} open positions")
        else:
            details.append(f"{open_count} open positions (light)")

        # Penalty for same-symbol concentration
        if same_symbol_count >= 2:
            score -= 6
            details.append(f"{same_symbol_count} positions in {symbol} already")
        elif same_symbol_count == 1:
            score -= 2
            details.append(f"1 existing {symbol} position")

        # Penalty/bonus for daily P&L
        if daily_pnl < -100:
            score -= 5
            details.append(f"daily P&L ${daily_pnl:+.0f} (bad day)")
        elif daily_pnl > 100:
            score += 2
            details.append(f"daily P&L ${daily_pnl:+.0f} (good day)")

        score = max(0, min(20, score))
        return score, "; ".join(details)

    finally:
        db.close()


def _build_reason(total: int, breakdown: dict, author: str) -> str:
    """Build a concise human-readable reason string."""
    min_score = _MIN_SCORE()
    if total <= min_score:
        return f"SKIP (score {total} below minimum {min_score})"
    elif total <= 50:
        return f"Low confidence ({total}) - minimum size only"
    elif total <= 70:
        return f"Moderate confidence ({total}) - standard size"
    elif total <= 85:
        return f"High confidence ({total}) - size +1 bonus"
    else:
        return f"Very high confidence ({total}) - size +2 bonus"
