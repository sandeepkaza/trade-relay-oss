"""
ai_signal_advisor.py - AI-powered options recommendation for Discord signals.

Takes a signal (raw text + OCR text + chart image URLs) and asks Gemini to
produce a structured options recommendation:

    {
      "bias": "bullish" | "bearish" | "neutral",
      "confidence": 0..100,
      "thesis": "short paragraph",
      "trade": {
        "underlying": "SPX",
        "strategy": "long_call" | "long_put" | "call_spread" | "put_spread" | "iron_condor" | "none",
        "expiry": "0DTE" | "1DTE" | "weekly" | "monthly",
        "long_strike": float | null,
        "short_strike": float | null,
        "max_risk_pct": float,
        "rationale": "why this strategy fits the signal"
      },
      "key_levels": [floats],
      "warnings": ["list of risk callouts"]
    }

Multimodal: when chart images are present, they're downloaded and sent inline
so the model can read levels/structure off the chart itself instead of relying
on OCR text alone. Gracefully degrades to text-only if image fetch fails.

Config:
    [signals]
    ai_advisor_enabled = true        ; master switch
    ai_advisor_model = gemini-2.5-flash
    ai_advisor_max_images = 3        ; cap multimodal payload
    ai_advisor_post_to_webhook = true; broadcast formatted recommendation
    ai_advisor_webhook_url =         ; if blank, falls back to forwarder webhook
"""

import asyncio
import json
import logging
import os
from typing import Any, Optional

import httpx

from app.core.config_manager import cfg

log = logging.getLogger(__name__)

_genai_client = None
_client_init_attempted = False


def _enabled() -> bool:
    return cfg.getboolean("signals", "ai_advisor_enabled", False)


def _model() -> str:
    return cfg.get("signals", "ai_advisor_model", fallback="gemini-2.5-flash")


def _max_images() -> int:
    return cfg.getint("signals", "ai_advisor_max_images", fallback=3)


def _timeout() -> int:
    return cfg.getint("signals", "ai_advisor_timeout_seconds", fallback=20)


def _post_to_webhook() -> bool:
    return cfg.getboolean("signals", "ai_advisor_post_to_webhook", False)


def _grounding() -> bool:
    return cfg.getboolean("signals", "ai_advisor_grounding", False)


def _webhook_url() -> str:
    url = cfg.get("signals", "ai_advisor_webhook_url", fallback="").strip()
    if url:
        return url
    return cfg.get("forwarder", "webhook_url", fallback="").strip()


def _get_client():
    """Lazy-init Vertex AI client. Returns None on missing creds/SDK."""
    global _genai_client, _client_init_attempted
    if _genai_client is not None:
        return _genai_client
    if _client_init_attempted:
        return None
    _client_init_attempted = True

    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds or not os.path.isfile(creds):
        log.warning("[AI_ADVISOR] GOOGLE_APPLICATION_CREDENTIALS missing — disabled")
        return None

    try:
        from google import genai  # type: ignore
        project = cfg.get("ai_parser", "project", fallback="your-gcp-project")
        location = cfg.get("ai_parser", "location", fallback="us-east4")
        _genai_client = genai.Client(vertexai=True, project=project, location=location)
        log.info("[AI_ADVISOR] Vertex AI client initialized (project=%s loc=%s)", project, location)
        return _genai_client
    except Exception as e:
        log.error("[AI_ADVISOR] client init failed: %s", e)
        return None


def is_available() -> bool:
    return _get_client() is not None


SYSTEM_PROMPT = """You are an experienced options trading analyst. You receive Discord posts from a whale-flow / options-flow tracker (e.g. "Sniper Market Updates") that contain trading commentary plus screenshots showing unusual options activity, sweeps, block trades, dark-pool prints, dealer positioning (GEX/VEX), gamma walls, and key support/resistance.

Your job: given the post text, the OCR'd text from the chart(s), and the chart image(s) themselves, produce a STRUCTURED options recommendation a retail options trader can act on.

When Google Search grounding is available, FIRST verify the alert against real market data before recommending:
- Look up the current (or most recent) price of the named underlying and recent intraday/market context.
- Check whether the alert's claimed direction, levels, and any cited flow are PLAUSIBLE given that data — e.g. is spot near the claimed entry/level, is the move consistent with today's action, does the alert look stale or fabricated.
- If market data CONTRADICTS the alert or you cannot corroborate it, LOWER confidence and add a "legitimacy" entry to warnings naming what you found. If it corroborates, you may raise confidence and say so in the thesis.

Return ONLY a JSON object — no prose, no markdown fences. Schema:

{
  "bias": "bullish" | "bearish" | "neutral",
  "confidence": <int 0-100>,
  "legit": "verified" | "unverified" | "contradicted",
  "thesis": "<2-3 sentence trade thesis tying signal text + chart structure + market-data check together>",
  "trade": {
    "underlying": "SPX" | "SPY" | "QQQ" | "ES" | "NDX" | "<ticker>",
    "strategy": "long_call" | "long_put" | "call_spread" | "put_spread" | "iron_condor" | "none",
    "expiry": "0DTE" | "1DTE" | "weekly" | "monthly",
    "long_strike": <float or null>,
    "short_strike": <float or null>,
    "entry_zone": "<price range, e.g. 7080-7100>",
    "target": "<price target, e.g. 7150>",
    "stop_loss": "<price level, e.g. 7050>",
    "max_risk_pct": <int 1-10, % of account>,
    "rationale": "<1-2 sentence why this structure>"
  },
  "key_levels": [<floats sorted ascending>],
  "warnings": ["<short risk callouts>"]
}

Rules:
- If post is just chat/banter with no actionable directional signal, set strategy="none", bias="neutral", confidence<=30.
- If chart shows clear gamma/dealer flip levels, name them in key_levels.
- Strikes must be realistic strike values for the underlying (whole-number SPX, $1 SPY, $5 QQQ). Round accordingly.
- For 0DTE recommendations on SPX, prefer ATM-to-1%-OTM strikes; for swing, use closest gamma wall as long strike.
- max_risk_pct should default 2 unless setup is unusually high-conviction (>=80 confidence) where 5 is OK.
- Be conservative on confidence — only >=80 when text, chart, AND verified market data agree directionally.
- Set legit="contradicted" and confidence<=30 if real market data conflicts with the alert.
- Never recommend a trade for an asset class you can't identify.
"""


async def _fetch_image_bytes(url: str, timeout: float = 8.0) -> Optional[tuple[bytes, str]]:
    """Download image, return (bytes, mime). None on failure."""
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                log.debug("[AI_ADVISOR] image fetch %d for %s", resp.status_code, url[:80])
                return None
            mime = resp.headers.get("content-type", "image/png").split(";")[0].strip()
            if not mime.startswith("image/"):
                mime = "image/png"
            return resp.content, mime
    except Exception as e:
        log.debug("[AI_ADVISOR] image fetch exception: %s", e)
        return None


async def analyze(text: str, ocr_text: str, image_urls: list[str]) -> Optional[dict[str, Any]]:
    """Call Gemini with text + OCR + (up to N) chart images. Returns parsed JSON dict or None."""
    if not _enabled():
        return None
    client = _get_client()
    if client is None:
        return None

    try:
        from google.genai import types  # type: ignore
    except ImportError:
        log.error("[AI_ADVISOR] google-genai missing")
        return None

    parts: list[Any] = []
    user_text = (
        f"DISCORD POST TEXT:\n{text or '(empty)'}\n\n"
        f"OCR'D CHART TEXT:\n{ocr_text or '(no images / OCR empty)'}\n\n"
        f"Analyze and return the structured recommendation JSON."
    )
    parts.append(types.Part.from_text(text=user_text))

    image_count = 0
    for url in (image_urls or [])[: _max_images()]:
        fetched = await _fetch_image_bytes(url)
        if fetched is None:
            continue
        img_bytes, mime = fetched
        try:
            parts.append(types.Part.from_bytes(data=img_bytes, mime_type=mime))
            image_count += 1
        except Exception as e:
            log.debug("[AI_ADVISOR] failed to attach image: %s", e)

    log.info("[AI_ADVISOR] calling %s with %d image(s)", _model(), image_count)

    try:
        # Gemini 2.5 reserves part of max_output_tokens for "thinking" before
        # emitting the visible response. With a small budget, JSON gets
        # truncated. Cap thinking_budget low and raise output to 4096.
        gen_config_kwargs = dict(
            system_instruction=SYSTEM_PROMPT,
            temperature=0.2,
            max_output_tokens=4096,
        )
        grounded = _grounding()
        if grounded:
            # Vertex grounding (GoogleSearch) is incompatible with forced JSON
            # output (response_mime_type=application/json) — the API rejects the
            # combination. Drop JSON mode and rely on the prompt + the
            # ```-strip / brace-extract parse below. Give the model more room
            # for the extra search-and-reason step.
            try:
                gen_config_kwargs["tools"] = [types.Tool(google_search=types.GoogleSearch())]
            except Exception as e:
                log.warning("[AI_ADVISOR] GoogleSearch tool unavailable, grounding off: %s", e)
                gen_config_kwargs["response_mime_type"] = "application/json"
        else:
            gen_config_kwargs["response_mime_type"] = "application/json"
        try:
            gen_config_kwargs["thinking_config"] = types.ThinkingConfig(thinking_budget=512)
        except Exception:
            pass  # older SDK without ThinkingConfig — ignore

        def _call():
            return client.models.generate_content(
                model=_model(),
                contents=[types.Content(role="user", parts=parts)],
                config=types.GenerateContentConfig(**gen_config_kwargs),
            )
        response = await asyncio.wait_for(asyncio.to_thread(_call), timeout=_timeout())
        try:
            import app.core.usage_tracker as _ut
            um = getattr(response, "usage_metadata", None)
            in_t = int(getattr(um, "prompt_token_count", 0) or 0) if um else 0
            out_t = int(getattr(um, "candidates_token_count", 0) or 0) if um else 0
            _ut.record_vertex(model=_model(), input_tokens=in_t, output_tokens=out_t, success=True)
        except Exception:
            pass
    except asyncio.TimeoutError:
        log.warning("[AI_ADVISOR] timeout after %ds", _timeout())
        try:
            import app.core.usage_tracker as _ut
            _ut.record_vertex(model=_model(), success=False)
        except Exception:
            pass
        return None
    except Exception as e:
        log.error("[AI_ADVISOR] call failed: %s", e)
        try:
            import app.core.usage_tracker as _ut
            _ut.record_vertex(model=_model(), success=False)
        except Exception:
            pass
        return None

    raw = (getattr(response, "text", "") or "").strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    if not raw:
        log.warning("[AI_ADVISOR] empty response")
        return None

    try:
        rec = json.loads(raw)
    except json.JSONDecodeError:
        # Grounded responses sometimes wrap the JSON in prose / citations.
        # Fall back to extracting the outermost {...} block before giving up.
        start, end = raw.find("{"), raw.rfind("}")
        if start != -1 and end > start:
            try:
                rec = json.loads(raw[start : end + 1])
            except json.JSONDecodeError as e:
                log.error("[AI_ADVISOR] invalid JSON: %s | raw=%s", e, raw[:200])
                return None
        else:
            log.error("[AI_ADVISOR] no JSON object in response | raw=%s", raw[:200])
            return None

    # Validate / sanitize the LLM-returned shape so downstream consumers
    # (and any future auto-trade integration) can't be fed nonsense or
    # malicious values. A free-form string in `long_strike` would otherwise
    # reach numeric paths and either crash or, worse, get coerced into
    # arbitrary qty/strike inputs. Drop fields that fail validation; keep
    # the rest so the human-readable embed still renders.
    rec = _validate_advisor_response(rec)

    rec["_meta"] = {
        "model": _model(),
        "image_count": image_count,
        "url_count": len(image_urls or []),
    }
    return rec


def _validate_advisor_response(rec: dict) -> dict:
    """Coerce LLM JSON into safe, typed fields. Strip anything that doesn't
    pass — never raise (we don't want a malformed advisor call to kill the
    signal worker)."""
    if not isinstance(rec, dict):
        return {"bias": "neutral", "warnings": ["invalid response shape"]}

    out: dict = {}

    bias = rec.get("bias")
    if isinstance(bias, str) and bias.lower() in ("bullish", "bearish", "neutral"):
        out["bias"] = bias.lower()
    else:
        out["bias"] = "neutral"

    conf = rec.get("confidence")
    if isinstance(conf, (int, float)) and 0 <= conf <= 100:
        out["confidence"] = int(conf)

    legit = rec.get("legit")
    if isinstance(legit, str) and legit.lower() in ("verified", "unverified", "contradicted"):
        out["legit"] = legit.lower()

    thesis = rec.get("thesis")
    if isinstance(thesis, str):
        out["thesis"] = thesis[:1000]

    risk = rec.get("max_risk_pct")
    if isinstance(risk, (int, float)) and 0 < risk <= 10:
        out["max_risk_pct"] = float(risk)

    # key_levels / warnings — must be lists of strings or numbers; truncate.
    for k in ("key_levels", "warnings", "support", "resistance"):
        v = rec.get(k)
        if isinstance(v, list):
            out[k] = [
                str(item)[:200] for item in v[:10]
                if isinstance(item, (str, int, float))
            ]

    # trade leg validation
    trade = rec.get("trade")
    if isinstance(trade, dict):
        t_out: dict = {}
        strat = trade.get("strategy")
        ALLOWED_STRATS = {"long_call", "long_put", "call_spread", "put_spread",
                          "iron_condor", "straddle", "strangle", "none", ""}
        if isinstance(strat, str) and strat.lower() in ALLOWED_STRATS:
            t_out["strategy"] = strat.lower()

        for leg_key in ("long_strike", "short_strike"):
            v = trade.get(leg_key)
            if isinstance(v, (int, float)) and 0 < v < 1_000_000:
                t_out[leg_key] = float(v)

        for s_key in ("expiry", "underlying"):
            v = trade.get(s_key)
            if isinstance(v, str) and len(v) <= 32:
                t_out[s_key] = v

        if t_out:
            out["trade"] = t_out

    # Carry through any free-text reasoning (truncated; HTML/markdown stripped
    # at render time by Discord — not here).
    reasoning = rec.get("reasoning")
    if isinstance(reasoning, str):
        out["reasoning"] = reasoning[:2000]

    return out


def format_for_discord(rec: dict, signal_id: int, signal_text: str) -> dict:
    """Build a Discord webhook embed dict for the recommendation."""
    bias = (rec.get("bias") or "neutral").lower()
    color = {"bullish": 0x10b981, "bearish": 0xef4444}.get(bias, 0x64748b)
    trade = rec.get("trade") or {}
    fields: list[dict] = []

    if trade.get("strategy") and trade["strategy"] != "none":
        legs = []
        if trade.get("long_strike") is not None:
            legs.append(f"LONG {trade['long_strike']}")
        if trade.get("short_strike") is not None:
            legs.append(f"SHORT {trade['short_strike']}")
        legs_str = " / ".join(legs) if legs else "—"
        fields.append({
            "name": f"📊 {trade.get('strategy', '?').replace('_', ' ').upper()} — {trade.get('underlying', '?')} ({trade.get('expiry', '?')})",
            "value": f"**Legs:** {legs_str}\n**Entry:** {trade.get('entry_zone', '—')}\n**Target:** {trade.get('target', '—')}\n**Stop:** {trade.get('stop_loss', '—')}\n**Risk:** {trade.get('max_risk_pct', '—')}%",
            "inline": False,
        })
    if trade.get("rationale"):
        fields.append({"name": "Why", "value": trade["rationale"][:1024], "inline": False})

    if rec.get("key_levels"):
        levels = ", ".join(str(level) for level in rec["key_levels"][:10])
        fields.append({"name": "Key Levels", "value": levels, "inline": True})
    if rec.get("warnings"):
        fields.append({
            "name": "⚠ Warnings",
            "value": "\n".join(f"• {w}" for w in rec["warnings"][:5])[:1024],
            "inline": False,
        })

    legit = rec.get("legit")
    legit_badge = {
        "verified": " ✅ verified",
        "unverified": " ❔ unverified",
        "contradicted": " ⛔ contradicted",
    }.get(legit, "")
    embed = {
        "title": f"🤖 AI Recommendation — {bias.upper()} ({rec.get('confidence', 0)}%){legit_badge}",
        "description": (rec.get("thesis", "")[:400] or "—"),
        "color": color,
        "fields": fields,
        "footer": {"text": f"signal #{signal_id} · {rec.get('_meta', {}).get('model', '?')}"},
    }
    return {"embeds": [embed], "username": "AI Advisor"}


async def post_to_webhook(payload: dict) -> bool:
    """Post the embed to the configured Discord webhook. Returns True on 204."""
    url = _webhook_url()
    if not url:
        log.debug("[AI_ADVISOR] no webhook URL configured — skipping post")
        return False
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code in (200, 204):
                return True
            log.warning("[AI_ADVISOR] webhook post %d: %s", resp.status_code, resp.text[:200])
            return False
    except Exception as e:
        log.error("[AI_ADVISOR] webhook post error: %s", e)
        return False
