"""
ai_parser.py - AI-powered fallback parser using Google Gemini (Vertex AI).

When the regex parser returns None, this module sends the raw Discord message
to Gemini Flash via Vertex AI to extract structured trade data.

Authentication:
    Uses Google Application Default Credentials. Set GOOGLE_APPLICATION_CREDENTIALS
    to a service account JSON with the `roles/aiplatform.user` role on the project.
    `start.sh` does this automatically using `./keys/google-sa.json`.

Enable/disable via config.ini:
    [trading]
    ai_parser_enabled = true

    [ai_parser]
    provider = vertex_ai          ; vertex_ai | anthropic (legacy)
    model = gemini-2.5-flash      ; cheap + fast for parsing
    project = your-gcp-project
    location = us-east4           ; gemini regions (us-east4 = low latency to us-east-1 VM)
    timeout_seconds = 10
"""

import json
import logging
import os
import re
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional

from app.ingest.parser import (
    TradeAlert, _parse_date, _RE_SMALL_ACCOUNT, GATED_TAGS, resolve_size_tag,
)
from app.core.config_manager import cfg

log = logging.getLogger(__name__)

_genai_client = None
_client_init_attempted = False

# A date the analyst actually typed: "8/7", "8-7", "08/07/26". Slash/dash only —
# a dotted date ("6.18") is indistinguishable from a price ("$2.17") and the
# regex parser already owns that format (pattern 4a).
_DATE_TOKEN = re.compile(r"\b\d{1,2}\s*[/\-]\s*\d{1,2}(?:[/\-]\d{2,4})?\b")


def _get_client():
    """Lazy-init the google-genai client (Vertex AI mode).

    Returns None if SDK is missing or creds are unavailable. Cached after
    the first successful init."""
    global _genai_client, _client_init_attempted
    if _genai_client is not None:
        return _genai_client
    if _client_init_attempted:
        return None
    _client_init_attempted = True

    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds or not os.path.isfile(creds):
        log.warning(
            "[AI_PARSER] GOOGLE_APPLICATION_CREDENTIALS not set or file missing — "
            "AI parser disabled. Set env var to your service-account JSON path."
        )
        return None

    try:
        from google import genai  # type: ignore
        project = cfg.get("ai_parser", "project", fallback="your-gcp-project")
        location = cfg.get("ai_parser", "location", fallback="us-east4")
        _genai_client = genai.Client(vertexai=True, project=project, location=location)
        log.info("[AI_PARSER] Vertex AI client initialized (project=%s location=%s)", project, location)
        return _genai_client
    except ImportError:
        log.error("[AI_PARSER] google-genai not installed. Run: pip install google-genai")
        return None
    except Exception as e:
        log.error("[AI_PARSER] Vertex AI client init failed: %s", e)
        return None


def _build_system_prompt() -> str:
    today = date.today()
    return f"""You are a trade alert parser. You receive raw Discord messages from options trading channels and extract structured trade data.

Today's date is {today.isoformat()} (year={today.year}, month={today.month}, day={today.day}).

Return ONLY valid JSON with these fields (no markdown, no explanation):
{{
  "action": "BUY" or "SELL",
  "symbol": "SPX", "AAPL", etc. (uppercase ticker, no $),
  "expiry_month": <int or null>,
  "expiry_day": <int or null>,
  "expiry_year": <int or null>,
  "strike": <int>,
  "option_type": "C" or "P",
  "price": <float or null>,
  "fraction": 1.0,
  "size_tag": "SMALL", "MEDIUM", "LARGE", "FULL", "XL", or "",
  "qty": null,
  "close_all": false
}}

Rules:
- BUY keywords: bought, bto, bot, buy, opening, entered, entry
- SELL keywords: sold, stc, sell, closing, closed, out, exiting
- "all out", "close all", "sold everything" = SELL with close_all: true
- fraction: "1/2" = 0.5, "1/3" = 0.333, "1/4" = 0.25, no fraction = 1.0
- size_tag: look for [SMALL], [MEDIUM], [LARGE], etc. in brackets
- price: the dollar amount (e.g. $1.75, @1.75, 1.75). null if not present.
- qty: if the message mentions a specific number of contracts (e.g. "2 contracts", "CONTRACTS: 3", "Small Account: 1 contract", "Small Account Challenge: 2"), set qty to that integer. Otherwise null.
- Expiry: only report a date the message actually contains. If the message has no expiry date at all (e.g. "BOUGHT NVDA 222.50C $2.17 [SMALL]"), set expiry_month, expiry_day and expiry_year to null. NEVER substitute today's date for a missing one.
- Year rule: if the year is missing from the date, use {today.year} when the expiry month/day is today or later this year; otherwise use {today.year + 1}. Never emit a year in the past.
- 2-digit years like "4/17/25" mean 20YY (e.g. "25" -> 2025). Only trust them if they are not already in the past — if past, bump to {today.year} or {today.year + 1} per the rule above.
- If you cannot parse the message as a trade alert, return: {{"action": null}}
- strike is always an integer (no decimals)
- option_type is always "C" for calls or "P" for puts"""


async def ai_parse_alert(text: str, channel_id: Optional[int] = None) -> Optional[TradeAlert]:
    """Send raw text to Gemini and parse the response into a TradeAlert.
    Returns None if the model can't parse it or the call fails."""
    client = _get_client()
    if client is None:
        return None

    model = cfg.get("ai_parser", "model", fallback="gemini-2.5-flash")
    timeout = cfg.getint("ai_parser", "timeout_seconds", fallback=10)
    system_prompt = _build_system_prompt()

    try:
        from google.genai import types  # type: ignore
        import asyncio
        import app.core.usage_tracker as _ut
        # Sync SDK call — offload to thread so we don't block the event loop
        def _call():
            return client.models.generate_content(
                model=model,
                contents=text,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    temperature=0.0,
                    max_output_tokens=1024,
                    response_mime_type="application/json",
                ),
            )
        response = await asyncio.wait_for(asyncio.to_thread(_call), timeout=timeout)
        # Record token usage from response.usage_metadata when SDK returns it.
        try:
            um = getattr(response, "usage_metadata", None)
            in_t = int(getattr(um, "prompt_token_count", 0) or 0) if um else 0
            out_t = int(getattr(um, "candidates_token_count", 0) or 0) if um else 0
            _ut.record_vertex(model=model, input_tokens=in_t, output_tokens=out_t, success=True)
        except Exception:
            _ut.record_vertex(model=model, success=True)
    except asyncio.TimeoutError:
        log.warning("[AI_PARSER] Gemini timed out after %ds", timeout)
        try:
            import app.core.usage_tracker as _ut
            _ut.record_vertex(model=model, success=False)
        except Exception:
            pass
        return None
    except Exception as e:
        log.error("[AI_PARSER] Gemini call failed: %s", e)
        try:
            import app.core.usage_tracker as _ut
            _ut.record_vertex(model=model, success=False)
        except Exception:
            pass
        return None

    raw = (getattr(response, "text", "") or "").strip()
    if not raw:
        return None
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        log.error("[AI_PARSER] Gemini returned invalid JSON: %s | raw=%s", e, raw[:200])
        return None

    if not parsed.get("action"):
        log.info("[AI_PARSER] Message not a trade alert")
        return None

    return _build_trade_alert(parsed, channel_id, src_text=text)


def _build_trade_alert(parsed: dict, channel_id: Optional[int], src_text: str = "") -> Optional[TradeAlert]:
    """Convert the model's JSON response into a TradeAlert object."""
    try:
        raw_action = parsed.get("action")
        if not raw_action:
            return None
        action = raw_action.upper()
        if action not in ("BUY", "SELL"):
            return None

        symbol = (parsed.get("symbol") or "").upper().strip()

        # Parse expiry
        expiry = None
        mo = parsed.get("expiry_month")
        day = parsed.get("expiry_day")
        yr = parsed.get("expiry_year")
        if mo and day:
            yr_str = str(yr) if yr else None
            expiry = _parse_date(str(mo), str(day), yr_str)
            if expiry and expiry < date.today():
                today = date.today()
                # Feb 29 of a leap-year expiry, rolled into a non-leap year,
                # raises ValueError. Clamp the day to Feb 28 in that case.
                def _safe_replace(exp, year):
                    try:
                        return exp.replace(year=year)
                    except ValueError:
                        return date(year, exp.month, min(exp.day, 28))
                rolled = _safe_replace(expiry, today.year)
                if rolled < today:
                    rolled = _safe_replace(expiry, today.year + 1)
                log.warning("[AI_PARSER] Past expiry %s; rolling to %s", expiry, rolled)
                expiry = rolled

        # The model is handed today's date, so an UNDATED alert comes back
        # dated today — an expiry the analyst never wrote. 2026-08-06:
        # spacemonkey's undated "BOUGHT NVDA 222.50C $2.17 [SMALL]" became
        # NVDA260806C00222500 — a Thursday, and NVDA lists Fridays only — so
        # the broker rejected it with "API Error 400: No match found for this
        # Symbol (NVDA)". The regex parser deliberately refuses to guess an
        # expiry (parser.py pattern 4h); the AI path must not either.
        #   BUY  -> drop. No expiry means no contract to buy, and guessing one
        #           risks buying the wrong week.
        #   SELL -> keep with expiry=None. The executor then matches the open
        #           position on symbol+strike+type, which is what an undated
        #           SELL means; a SELL that fails to fire is the worse failure.
        # Requires src_text: an empty one means the caller didn't give us the
        # message, NOT that the message was undated. Treating those the same
        # would silently drop every BUY from any caller that omits it.
        if expiry and src_text and not _DATE_TOKEN.search(src_text):
            log.warning(
                "[AI_PARSER] model invented expiry %s for undated text — %s | text=%r",
                expiry, "dropping BUY" if action == "BUY" else "clearing on SELL",
                (src_text or "")[:100],
            )
            if action == "BUY":
                return None
            expiry = None

        strike_raw = parsed.get("strike")
        strike = float(strike_raw) if strike_raw is not None else None

        option_type = (parsed.get("option_type") or "").upper()
        if option_type not in ("C", "P", ""):
            option_type = ""

        price = None
        price_raw = parsed.get("price")
        if price_raw is not None:
            try:
                price = Decimal(str(price_raw)).quantize(Decimal("0.01"))
            except (InvalidOperation, ValueError):
                price = None

        fraction_raw = parsed.get("fraction", 1.0)
        try:
            fraction = Decimal(str(fraction_raw))
        except (InvalidOperation, ValueError):
            fraction = Decimal("1")

        size_tag = (parsed.get("size_tag") or "").upper()
        # The prompt only asks the model for bracketed size buckets, so a
        # free-text gated tag ("... LOTTO FOR $AMD EARNINGS") never comes back
        # and check_rollup_block would pass it. Re-scan the source text and let
        # a gated tag win, exactly as the regex parser does.
        _gated = resolve_size_tag(src_text or "")
        if _gated in GATED_TAGS and size_tag != _gated:
            log.info("[AI_PARSER] gated tag %r found in text (model said %r) — overriding",
                     _gated, size_tag or "")
            size_tag = _gated
        close_all = bool(parsed.get("close_all", False))
        # Layer 4: the AI must never turn a no-ticker phrase into a flatten-the-
        # book order. 2026-06-11: "everything selling now" → SELL close_all with
        # symbol="" → executor closed the entire book. A blank-symbol close_all
        # has no position context the model can be trusted on — reject outright.
        if close_all and not symbol:
            log.warning(
                "[AI_PARSER] rejecting blank-symbol close_all (no ticker) | text=%r",
                src_text[:100],
            )
            return None
        if close_all:
            fraction = Decimal("1")

        qty_raw = parsed.get("qty")
        qty = None
        if qty_raw is not None:
            try:
                n = int(qty_raw)
                if 1 <= n <= 50:
                    qty = n
            except (ValueError, TypeError):
                pass

        alert = TradeAlert(
            action=action,
            symbol=symbol,
            expiry=expiry,
            strike=strike,
            option_type=option_type,
            price=price,
            fraction=fraction,
            size_tag=size_tag,
            close_all=close_all,
            channel_id=channel_id,
            qty=qty,
            # Same marker stamp the regex parser applies. The AI fallback runs on
            # exactly the messages the regex missed — including Sniper's typo'd
            # ones ("BOIGHT GLW 120P") — so without this his small-account sizing
            # would silently fall back to size_default on those.
            small_account=bool(_RE_SMALL_ACCOUNT.search(src_text or "")),
        )
        log.info("[AI_PARSER] Result: %s %s %s %s%s @ %s qty=%s",
                 action, symbol, expiry, strike, option_type, price, qty)
        return alert

    except (KeyError, ValueError, TypeError) as e:
        log.error("[AI_PARSER] Failed to build TradeAlert: %s", e)
        return None
