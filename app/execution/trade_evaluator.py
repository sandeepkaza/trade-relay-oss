"""
trade_evaluator.py — quality/profitability check for structured option alerts.

Whale-tracker channels post clean, structured BUY alerts, e.g.:

    Stock: $META | Strike: 630.0C | Expiry: 05/27/2026 | Entry: $2.40 | Action: BUY

This module turns such an alert into an objective, decision-support verdict —
NOT an auto-trade. It pulls REAL option data (bid/ask/spread + greeks + spot)
from the IBKR feed and computes hard metrics the LLM cannot reliably guess:

    - breakeven  + % the underlying must move to reach it
    - moneyness (OTM/ITM %)
    - bid/ask spread % (execution drag)
    - implied vol, delta (~P(ITM)), theta (decay)
    - entry premium vs live mid (are you overpaying?)
    - days to expiry (0DTE = all-or-nothing)

A transparent rule-based scorer grades the setup (A-F) with named flags.
Optionally, Gemini narrates the metrics into a 2-sentence verdict — given the
hard numbers, NOT asked to invent them (so no grounding needed here).

Pure functions (parse_alert / build_osi / compute_metrics / score_trade) have
no I/O and are unit-tested. evaluate() is the async orchestrator.

Config:
    [signals]
    trade_eval_enabled = true
    trade_eval_narrate = true            ; Gemini plain-English verdict
    trade_eval_post_to_webhook = false   ; broadcast embed to Discord
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

from app.core.config_manager import cfg

log = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")


# ── Config accessors ──────────────────────────────────────────────────────

def _enabled() -> bool:
    return cfg.getboolean("signals", "trade_eval_enabled", False)


def _narrate() -> bool:
    return cfg.getboolean("signals", "trade_eval_narrate", False)


def _post_to_webhook() -> bool:
    return cfg.getboolean("signals", "trade_eval_post_to_webhook", False)


# ── Alert parsing (pure) ──────────────────────────────────────────────────

_RIGHT_WORDS = {"C": "C", "CALL": "C", "CALLS": "C", "P": "P", "PUT": "P", "PUTS": "P"}


def _parse_expiry(raw: str) -> Optional[date]:
    """Accept MM/DD/YYYY, M/D/YY, YYYY-MM-DD. Return date or None."""
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y", "%m-%d-%y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def parse_alert(text: str) -> Optional[dict[str, Any]]:
    """Parse a structured option alert into typed fields, or None if it isn't
    one. Tolerant of pipe/newline/comma separators and `$` / spacing noise.

    Required to qualify as structured: symbol + strike + right + expiry.
    `entry` and `action` are optional (entry needed for breakeven math).
    """
    if not text:
        return None

    # Normalize separators → a single delimiter, then split into key:value.
    fields: dict[str, str] = {}
    chunks = re.split(r"[|\n;]+", text)
    for chunk in chunks:
        if ":" not in chunk:
            continue
        key, _, val = chunk.partition(":")
        k = key.strip().lower()
        if k:
            fields[k] = val.strip()

    sym_raw = fields.get("stock") or fields.get("ticker") or fields.get("symbol") or ""
    symbol = sym_raw.replace("$", "").strip().upper()
    symbol = re.sub(r"[^A-Z.]", "", symbol)
    if not symbol:
        return None

    strike_raw = fields.get("strike", "")
    right = None
    strike = None
    m = re.search(r"(\d+(?:\.\d+)?)\s*([A-Za-z]+)?", strike_raw)
    if m:
        try:
            strike = float(m.group(1))
        except ValueError:
            strike = None
        if m.group(2):
            right = _RIGHT_WORDS.get(m.group(2).strip().upper())
    # right may live in its own field (e.g. "Type: CALL" / "Side: PUT")
    if right is None:
        for key in ("right", "type", "side", "cp"):
            rv = fields.get(key, "").strip().upper()
            if rv in _RIGHT_WORDS:
                right = _RIGHT_WORDS[rv]
                break
    if strike is None or right is None:
        return None

    expiry = _parse_expiry(fields.get("expiry", "") or fields.get("expiration", ""))
    if expiry is None:
        return None

    entry = None
    entry_raw = (fields.get("entry") or fields.get("price") or fields.get("fill") or "").replace("$", "").strip()
    em = re.search(r"\d+(?:\.\d+)?", entry_raw)
    if em:
        try:
            entry = float(em.group(0))
        except ValueError:
            entry = None

    action = (fields.get("action") or fields.get("order") or "BUY").strip().upper()
    action = "SELL" if action.startswith("S") else "BUY"

    return {
        "symbol": symbol,
        "strike": strike,
        "right": right,
        "expiry": expiry,
        "entry": entry,
        "action": action,
    }


def build_osi(symbol: str, expiry: date, strike: float, right: str) -> str:
    """Build an OSI option symbol matching ibkr_sdk_bridge._osi_to_ib_option.

    ROOT + YY + MM + DD + C/P + STRIKE(8 digits, *1000).
    META 2026-05-27 630.0 C → 'META260527C00630000'
    """
    yy = expiry.year % 100
    strike_int = int(round(strike * 1000))
    return f"{symbol.upper()}{yy:02d}{expiry.month:02d}{expiry.day:02d}{right.upper()}{strike_int:08d}"


# ── Metrics (pure) ────────────────────────────────────────────────────────

def compute_metrics(alert: dict, quote: dict, *, today: Optional[date] = None) -> dict[str, Any]:
    """Combine parsed alert + live quote into objective metrics. Every field is
    optional — whatever the quote lacks is simply omitted (None-safe)."""
    today = today or datetime.now(_ET).date()
    right = alert["right"]
    strike = alert["strike"]
    entry = alert.get("entry")
    expiry: date = alert["expiry"]

    m: dict[str, Any] = {}
    m["dte"] = (expiry - today).days
    m["expired"] = m["dte"] < 0

    # Breakeven from the alert's own entry premium.
    if entry is not None:
        m["breakeven"] = round(strike + entry, 2) if right == "C" else round(strike - entry, 2)

    spot = quote.get("und_price")
    mid = quote.get("mid")

    if spot:
        # Moneyness: positive = OTM.
        otm = (strike - spot) / spot * 100.0 if right == "C" else (spot - strike) / spot * 100.0
        m["otm_pct"] = round(otm, 2)
        m["spot"] = round(float(spot), 2)
        if "breakeven" in m:
            be = m["breakeven"]
            move = (be - spot) / spot * 100.0 if right == "C" else (spot - be) / spot * 100.0
            m["move_to_be_pct"] = round(move, 2)

    if quote.get("spread_pct") is not None:
        m["spread_pct"] = round(float(quote["spread_pct"]), 2)
    if mid:
        m["mid"] = round(float(mid), 2)
        if entry is not None and mid > 0:
            m["entry_vs_mid_pct"] = round((entry - mid) / mid * 100.0, 2)
    if quote.get("iv") is not None:
        m["iv_pct"] = round(float(quote["iv"]) * 100.0, 1)
    for g in ("delta", "theta", "gamma", "vega"):
        if quote.get(g) is not None:
            m[g] = round(float(quote[g]), 4)

    m["has_market_data"] = bool(spot or mid)
    return m


# ── Scoring (pure) ────────────────────────────────────────────────────────

def _grade(score: int) -> str:
    if score >= 85: return "A"
    if score >= 70: return "B"
    if score >= 55: return "C"
    if score >= 40: return "D"
    return "F"


def score_trade(metrics: dict) -> dict[str, Any]:
    """Transparent rule-based quality score (0-100) + named flags + verdict.

    Each rule is additive/subtractive and labelled so the user sees WHY a
    setup scored low. No ML, no hidden weights.
    """
    flags: list[str] = []
    score = 100

    if metrics.get("expired"):
        return {"score": 0, "grade": "F", "flags": ["EXPIRED — expiry is in the past"], "tradeable": False}

    dte = metrics.get("dte")
    if dte == 0:
        score -= 10
        flags.append("0DTE — theta brutal, all-or-nothing by close")
    elif dte is not None and dte == 1:
        flags.append("1DTE — fast decay")

    spread = metrics.get("spread_pct")
    if spread is not None:
        if spread > 20:
            score -= 30; flags.append(f"Spread {spread:.0f}% — illiquid, heavy execution drag")
        elif spread > 10:
            score -= 15; flags.append(f"Spread {spread:.0f}% — wide")
        elif spread > 5:
            score -= 5; flags.append(f"Spread {spread:.0f}% — moderate")

    overpay = metrics.get("entry_vs_mid_pct")
    if overpay is not None:
        if overpay > 15:
            score -= 20; flags.append(f"Entry {overpay:.0f}% above mid — overpaying")
        elif overpay > 5:
            score -= 10; flags.append(f"Entry {overpay:.0f}% above mid")
        elif overpay < -10:
            flags.append(f"Entry {abs(overpay):.0f}% below mid — alert may be stale")

    delta = metrics.get("delta")
    if delta is not None:
        ad = abs(delta)
        if ad < 0.15:
            score -= 25; flags.append(f"Delta {delta:.2f} — lottery ticket, low P(ITM)")
        elif ad < 0.30:
            score -= 10; flags.append(f"Delta {delta:.2f} — low probability")

    move = metrics.get("move_to_be_pct")
    if move is not None:
        if move <= 0:
            flags.append("Already past breakeven — entry is ITM relative to BE")
        elif dte == 0 and move > 1.0:
            score -= 20; flags.append(f"Needs +{move:.1f}% by close — hard intraday")
        elif dte == 0 and move > 0.5:
            score -= 10; flags.append(f"Needs +{move:.1f}% by close")
        elif move > 5:
            score -= 10; flags.append(f"Needs +{move:.1f}% move to breakeven")

    if not metrics.get("has_market_data"):
        flags.append("No live market data — quality unverified (check IBKR feed/entitlement)")
        score = min(score, 50)

    score = max(0, min(100, score))
    return {
        "score": score,
        "grade": _grade(score),
        "flags": flags,
        "tradeable": score >= 40 and not metrics.get("expired"),
    }


# ── Optional Gemini narration ─────────────────────────────────────────────

async def _narrative(alert: dict, metrics: dict, scored: dict) -> Optional[str]:
    """Ask Gemini for a 2-sentence plain verdict from the hard metrics. No
    grounding — the numbers are already real. Returns None on any failure."""
    try:
        import app.execution.ai_signal_advisor as ai
        client = ai._get_client()
        if client is None:
            return None
        from google.genai import types  # type: ignore
        import asyncio, json
        prompt = (
            "You are an options risk analyst. Given the alert and the REAL "
            "computed metrics below, write a 2-sentence verdict on whether this "
            "is a quality, profitable-odds trade. Be blunt about the biggest "
            "risk. Do NOT invent numbers — use only what is given.\n\n"
            f"ALERT: {json.dumps({k: str(v) for k, v in alert.items()})}\n"
            f"METRICS: {json.dumps(metrics)}\n"
            f"SCORE: {scored['score']}/100 grade {scored['grade']} flags={scored['flags']}"
        )
        model = cfg.get("signals", "ai_advisor_model", fallback="gemini-2.5-flash")

        def _call():
            return client.models.generate_content(
                model=model,
                contents=[types.Content(role="user", parts=[types.Part.from_text(text=prompt)])],
                config=types.GenerateContentConfig(temperature=0.3, max_output_tokens=400),
            )
        resp = await asyncio.wait_for(asyncio.to_thread(_call), timeout=20)
        return (getattr(resp, "text", "") or "").strip() or None
    except Exception as e:
        log.debug("[TRADE_EVAL] narration skipped: %s", e)
        return None


# ── Orchestrator ──────────────────────────────────────────────────────────

async def evaluate(text: str) -> Optional[dict[str, Any]]:
    """Parse → fetch live quote → metrics → score (→ optional narrative).
    Returns None if `text` isn't a structured alert."""
    alert = parse_alert(text)
    if alert is None:
        return None

    osi = build_osi(alert["symbol"], alert["expiry"], alert["strike"], alert["right"])

    quote: dict = {}
    try:
        import app.execution.ibkr_sdk_bridge as ibkr
        quote = await ibkr.fetch_quote_detail(osi)
    except Exception as e:
        log.warning("[TRADE_EVAL] quote fetch failed for %s: %s", osi, e)

    metrics = compute_metrics(alert, quote)
    scored = score_trade(metrics)

    narrative = None
    if _narrate():
        narrative = await _narrative(alert, metrics, scored)

    result = {
        "osi": osi,
        "alert": {**alert, "expiry": alert["expiry"].isoformat()},
        "metrics": metrics,
        "score": scored["score"],
        "grade": scored["grade"],
        "flags": scored["flags"],
        "tradeable": scored["tradeable"],
        "narrative": narrative,
        "quote_raw": quote,
    }
    log.info("[TRADE_EVAL] %s → grade=%s score=%d tradeable=%s flags=%d",
             osi, scored["grade"], scored["score"], scored["tradeable"], len(scored["flags"]))
    return result


# ── Discord embed ─────────────────────────────────────────────────────────

def format_for_discord(result: dict, signal_id: int) -> dict:
    """Build a Discord webhook embed for the evaluation."""
    grade = result.get("grade", "?")
    score = result.get("score", 0)
    color = {"A": 0x10b981, "B": 0x84cc16, "C": 0xeab308,
             "D": 0xf97316, "F": 0xef4444}.get(grade, 0x64748b)
    a = result.get("alert", {})
    m = result.get("metrics", {})

    def _row(label, key, suffix=""):
        return f"**{label}:** {m[key]}{suffix}" if key in m else None

    detail = "\n".join(filter(None, [
        _row("Spot", "spot"),
        _row("Breakeven", "breakeven"),
        _row("Move to BE", "move_to_be_pct", "%"),
        _row("OTM", "otm_pct", "%"),
        _row("Spread", "spread_pct", "%"),
        _row("IV", "iv_pct", "%"),
        _row("Delta", "delta"),
        _row("Mid", "mid"),
        _row("Entry vs mid", "entry_vs_mid_pct", "%"),
        _row("DTE", "dte"),
    ])) or "—"

    fields = [{
        "name": f"{a.get('symbol','?')} {a.get('strike','?')}{a.get('right','?')} {a.get('expiry','?')} @ {a.get('entry','?')}",
        "value": detail,
        "inline": False,
    }]
    if result.get("flags"):
        fields.append({
            "name": "Flags",
            "value": "\n".join(f"• {f}" for f in result["flags"][:8])[:1024],
            "inline": False,
        })

    embed = {
        "title": f"Trade Quality — {grade} ({score}/100)" + ("" if result.get("tradeable") else " ⛔ skip"),
        "description": (result.get("narrative") or "")[:600] or None,
        "color": color,
        "fields": fields,
        "footer": {"text": f"signal #{signal_id} · {result.get('osi','')}"},
    }
    return {"embeds": [embed], "username": "Trade Evaluator"}
