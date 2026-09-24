"""
Alarm on alerts that look like real orders but failed to parse.

An unparsed alert costs money silently: 2026-08-07 Sniper posted
"SOLD 2/3 880C 8/7 5.05" with no ticker, parse_alert returned None, the 2/3
profit-take never reached the order queue, and the only trace was one DEBUG
line in parser.log that nobody reads. The entry had filled minutes earlier.

Most drops are correct and must stay silent. Replaying 2yr of dumps, Sniper's
512 dropped trade-intent messages break down as: 187 morning commentary, 151
covered-call/CSP income legs, 78 equity share trades, 19 spreads/rolls — all
deliberate refusals — against ~77 that were genuinely order-shaped. So the
filter here is deliberately narrow: verb + strike + C/P + expiry, minus the
classes the parser refuses on purpose. That rates out to roughly one ping per
ten days across all channels, which is rare enough to still mean something.
"""
import logging
import re
import time

log = logging.getLogger(__name__)

# An order line names an action, a contract, and a date. Anything missing one
# of those is commentary ("Good Morning Snipers, chips having a field day")
# and must never page.
_VERB = re.compile(
    r"\b(BOUGHT|BOT|BTO|BUYING|OPENING|ADDING|SOLD|STC|SELLING|CLOSING|CLOSED|"
    r"TRIM(?:MING|MED)?|SCALING|ALL\s*OUT|OUT\s+OF)\b", re.IGNORECASE)
_STRIKE = re.compile(r"\b\d{1,5}(?:\.\d+)?\s*[CP]\b", re.IGNORECASE)
_EXPIRY = re.compile(r"\b\d{1,2}[/.]\d{1,2}\b")

# Refusals the parser makes on purpose — see the guards in parser.py. Silent.
_INTENDED_REFUSAL = (
    ("shares",       re.compile(r"\bshares?\b", re.IGNORECASE)),
    ("covered_call", re.compile(r"\bcovered\s+calls?\b", re.IGNORECASE)),
    ("csp",          re.compile(r"\bcash\s+secure[d]?\s+put|naked\s+put\b", re.IGNORECASE)),
    ("spread",       re.compile(r"\b(spread|debit|credit|condor|butterfly|calendar|"
                                r"diagonal|straddle|strangle|fly|roll(?:ing|ed)?)\b", re.IGNORECASE)),
)


def classify_unparsed(text: str) -> str:
    """Return a short reason to alarm, or "" when this drop is expected.

    "" means the parser was right to refuse — do not page for it.
    """
    if not text:
        return ""
    if not _VERB.search(text):
        return ""
    if not (_STRIKE.search(text) and _EXPIRY.search(text)):
        return ""            # commentary, watchlist, "no more trades today"
    for label, pat in _INTENDED_REFUSAL:
        if pat.search(text):
            return ""        # deliberate guard, not a gap
    return "order-shaped alert did not parse"


# Cheap in-process throttle. A broken parser or a spammy new format must not
# turn one bug into hundreds of Discord posts.
_MAX_PER_HOUR = 10
_recent: list[float] = []
_seen: dict[str, float] = {}


def _throttled(text: str, now: float) -> bool:
    global _recent
    _recent = [t for t in _recent if now - t < 3600]
    for k, ts in list(_seen.items()):
        if now - ts > 3600:
            _seen.pop(k, None)
    key = text[:200]
    if key in _seen:
        return True                      # same message re-delivered
    if len(_recent) >= _MAX_PER_HOUR:
        return True
    _recent.append(now)
    _seen[key] = now
    return False


async def alarm_if_order_shaped(text: str, author: str, channel: str) -> bool:
    """Post a Discord alarm when an order-shaped alert failed to parse.

    Returns True when an alarm was sent. Never raises — an alarm failure must
    not take down the message handler it is reporting on.
    """
    try:
        from app.core.config_manager import cfg
        if not cfg.getboolean("trading", "unparsed_alert_alarm_enabled", fallback=True):
            return False

        reason = classify_unparsed(text)
        if not reason:
            return False
        if _throttled(text, time.time()):
            log.debug("[UNPARSED_ALARM] throttled | %s", text[:80])
            return False

        import app.analytics.trade_logger as _tl
        await _tl.log_health_alarm(
            component="parser-unparsed",
            message=(f"**{reason}** — no order was placed.\n"
                     f"author: `{author}`  channel: `{channel}`\n"
                     f"```{text[:400]}```"),
        )
        log.warning("[UNPARSED_ALARM] %s | author=%s | %s", reason, author, text[:120])
        return True
    except Exception as ex:
        log.warning("[UNPARSED_ALARM] could not post: %s", ex)
        return False
