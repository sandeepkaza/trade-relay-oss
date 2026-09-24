"""
parser.py - Parses Discord alert messages into structured TradeAlert objects.

Handles every real-world format observed in live trading Discord servers:

BUY (open / add to position)
─────────────────────────────
  BOUGHT SPX 4/14 6970C $1.75 [SMALL]
  BOT    SPX 4/14 6970C @ 1.75        ← common typo for BTO
  BOUGHT SPX 4/14 6970C $1.75 [MEDIUM]
  BTO    SPX 4/14 6970C @ 1.75
  BTO    SPX 4/14 6970C @1.75
  Opening SPX 4/14 6970C $1.75
  🟢 BOUGHT SPX 4/14 6970C $1.75 [SMALL]    ← emoji prefix stripped
  ✅ BTO SPX 4/14 6970C @ 1.75
  BOUGHT SPX 4/14/25  6970C $1.75           ← 2-digit year
  BOUGHT SPX 4/14/2025 6970C $1.75          ← 4-digit year

SELL — partial
──────────────
  SOLD    SPX 4/14 6970C $2.70 1/2
  SOLD    SPX 4/14 6970C $3.35 1/4
  STC     SPX 4/14 6970C $2.70 1/2
  CLOSING SPX 4/14 6970C $2.70 1/2
  SOLD    NVDA 5/2 900C $5.50 1/3
  STC     SPX 4/17 6970C $3.50 2/3
  SOLD    SPX 4/14 6970C $2.70 3/4

SELL — full close
──────────────────
  SOLD   SPX 4/14 6970C $2.70          ← no fraction  = close all
  SOLD   SPX 4/14 6970C $2.70 ALL
  SOLD   SPX 4/14 6970C $2.70 FULL
  CLOSED SPX 4/14 6970C $2.70
  SOLD ALL SPX 4/14 6970C $2.70

monkeyBOT™ / "Trade Idea" format
─────────────────────────────────
  SPX ▲ LONG 7032.11 monkeyBOT™ ... Trade Idea SPX 04/16 7060C @ $4.05
  SPX ▼ SHORT 7037.37 monkeyBOT™ ... Trade Idea SPX 04/16 7020P @ $4.15
  (LONG / SHORT are direction words, mapped to BUY — it's always an entry)
"""

import logging
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional

log = logging.getLogger(__name__)


# ─── Data class ───────────────────────────────────────────────────────────────

@dataclass
class TradeAlert:
    action:      str                  # "BUY" | "SELL"
    symbol:      str                  # e.g. "SPX"; empty string if close_all & no symbol
    expiry:      Optional[date]       # None when no date was found
    strike:      Optional[float]      # None when no strike was found; float so half-dollar strikes (272.5) survive
    option_type: str                  # "C" | "P" | "" when unknown
    price:       Optional[Decimal]    # None = market order
    fraction:    Decimal = Decimal("1")
    size_tag:    str     = ""         # SMALL | MEDIUM | LARGE | FULL | XL
    close_all:   bool    = False      # True → exit entire position (or all positions)
    channel_id:  Optional[int] = None
    qty:         Optional[int] = None  # explicit contract count from message (overrides size_tag)
    small_account: bool  = False      # alert carries Sniper's small-account marker (‼ / "small account")
    osi_symbol:  str     = field(init=False, default="")

    def __post_init__(self):
        if self.symbol and self.expiry and self.strike is not None and self.option_type:
            self.osi_symbol = _build_osi(self.symbol, self.expiry, self.strike, self.option_type)
        else:
            self.osi_symbol = ""


# ─── OSI builder ─────────────────────────────────────────────────────────────

def _build_osi(symbol: str, expiry: date, strike: float, option_type: str) -> str:
    """SPX, 2025-04-14, 6970,    C → SPX250414C06970000
       AAPL, 2026-04-20, 272.5, C → AAPL260420C00272500"""
    yy  = str(expiry.year)[-2:]
    mm  = str(expiry.month).zfill(2)
    dd  = str(expiry.day).zfill(2)
    stk = str(int(round(float(strike) * 1000))).zfill(8)  # round guards against FP drift
    return f"{symbol}{yy}{mm}{dd}{option_type}{stk}"


# expose for tests
build_osi = _build_osi


# ─── Lookup tables ────────────────────────────────────────────────────────────

FRACTION_MAP: dict[str, Decimal] = {
    "1/2": Decimal("0.5"),
    "1/3": Decimal("1") / Decimal("3"),
    "1/4": Decimal("0.25"),
    "1/5": Decimal("0.2"),
    "1/6": Decimal("1") / Decimal("6"),
    "2/3": Decimal("2") / Decimal("3"),
    "3/4": Decimal("0.75"),
    "half": Decimal("0.5"),
    "quarter": Decimal("0.25"),
    "all": Decimal("1"),
    "full": Decimal("1"),
}

SIZE_MAP: dict[str, str] = {
    "small": "SMALL", "medium": "MEDIUM", "med": "MEDIUM",
    "large": "LARGE", "lg":    "LARGE",   "full": "FULL",
    "xl":    "XL",    "xs":    "XS",
    "roll up": "ROLL UP", "roll down": "ROLL DOWN",
    "swing": "SWING", "lotto": "LOTTO", "scalp": "SCALP",
}

# Risk classifications that gate execution (guardrails.check_rollup_block).
# These share the size_tag slot with size buckets but are not sizes, and the
# analyst routinely states both — so a gated tag must outrank a bucket or the
# guardrail silently never sees it.
GATED_TAGS: tuple[str, ...] = ("ROLL UP", "ROLL DOWN", "LOTTO")


def resolve_size_tag(text: str) -> str:
    """Resolve the size/risk tag from a message.

    Priority: a gated risk tag anywhere in the text (bracketed or free) beats a
    bracketed size bucket, which beats a free-text size word.

    2026-08-04 (NVDA 215C 8/5, real fill against policy): a bracket match used
    to short-circuit the free-text scan entirely, so Bdorts' "[SMALL] ... LOTTO
    FOR $AMD EARNINGS" resolved to SMALL and walked straight through the LOTTO
    block. The free-text scan had the same class of bug independently — it
    returned the first SIZE_MAP hit in dict order, and "small" precedes "lotto",
    so "very small. 5 contracts. Lotto." also lost the gated tag. 22 of the last
    1200 BUY alerts were mistagged this way.
    """
    lower = (text or "").lower()
    for tag in GATED_TAGS:
        if re.search(rf"\b{re.escape(tag.lower())}\b", lower):
            return tag
    bracket_m = re.search(r"\[(\w[\w\s]*\w|\w)\]", text or "")
    if bracket_m:
        return SIZE_MAP.get(bracket_m.group(1).lower(), bracket_m.group(1).upper())
    for key, val in SIZE_MAP.items():
        if re.search(rf"\b{re.escape(key)}\b", lower):
            return val
    return ""

BUY_KEYWORDS = {
    "bought", "bto", "bot", "buy", "opening", "open", "entered", "entry", "entering",
    "long",   # monkeyBOT direction word: LONG = entering a call position (BUY)
    "short",  # monkeyBOT direction word: SHORT = entering a put position (BUY)
    "added",  # ivtrader scale-in convention: "ADDED 687p ... new avg 1.31"
}
SELL_KEYWORDS = {
    "sold", "stc", "sell", "closing", "close", "closed", "selling",
    "exiting", "exit", "out",
    "stopped",  # stop-loss hit = full exit ("stopped 6910p", "STOPPED ES 6850c")
}

# Reserved words that must never be parsed as a ticker symbol. Without this
# guard, fluid-alerts posts like "> SOLD 1/2 SPX 7450C 5/21 3.7" matched as
# ticker="SOLD", date="1/2", strike=7450 — bot would place an order against
# the bogus OSI SOLD270102C07450000.
_NEVER_TICKER = frozenset({
    "SOLD", "BOUGHT", "BUY", "SELL", "STC", "BTO", "BOT",
    "ALL", "OUT", "FULL", "HALF",
    "CLOSE", "CLOSED", "CLOSING", "OPEN", "OPENING",
    "EXIT", "EXITING", "ENTRY", "ENTERED", "ENTERING",
    "LONG", "SHORT", "ROLL", "ADD", "ADDED", "TRIM", "TRIMMED",
    "MOST", "PUT", "CALL", "PUTS", "CALLS",
    "SHARES", "STOCK", "STOCKS", "COMMON", "COMMONS",
    "LOTTO", "SWING", "SCALP",
})

# Equity-account strategies the bot does not trade. Skip the alert entirely
# so a "SOLD cash secured put MU 800P 5/29 6.25" never reaches the executor.
_NOT_OPTIONS_RE = re.compile(
    r"\b(?:cash\s+secur(?:ed|e)\s+(?:put|call)s?"
    r"|covered\s+(?:call|put)s?"
    r"|csps?|naked\s+(?:put|call)s?)\b",
    re.IGNORECASE,
)

# Channels where the analyst trades a single underlying and routinely omits
# the ticker (e.g. ivtrader's "bought 7515c $1.20" -> implied SPX weekly).
# Bare strike+C/P alerts from these channels default symbol to SPX; every
# other channel still requires an explicit ticker so a stray number is never
# misread as an order.
_IMPLICIT_SPX_CHANNELS = frozenset({
    100000000000021109,  # ivtrader (elhombre20) - SPX/SPXW only, no ticker in alerts
})

# Phrases that mean "close the entire position (or all positions)"
CLOSE_ALL_PHRASES = [
    r"all\s+out", r"all\s+time", r"close\s+all", r"sold\s+everything",
    r"selling\s+everything", r"exiting\s+all", r"out\s+of\s+all",
    r"all\s+gone", r"closed\s+out",
]
# Pre-compiled single OR pattern — replaces per-pattern re.search loop
_CLOSE_ALL_RE = re.compile("|".join(CLOSE_ALL_PHRASES), re.IGNORECASE)
_CLOSE_ALL_RE2 = re.compile(r"\b(sold\s+all|sold\s+full|close(?:d)?\s+all)\b", re.IGNORECASE)

# Regex fragments
_DATE  = r"(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?"
_STRIKE = r"(\d{1,6}(?:\.\d{1,2})?)"
_OT    = r"([CcPp])"
_PRICE = r"((?:[\$@]\s*)?\d+\.\d{1,2}|(?:[\$@]\s*)?\.\d{1,2}|[\$@]\s*\d{1,4})"
# Match an option premium price. Single capture group preserves group
# numbering for compound patterns (rev_pat etc.). Three alternatives:
#   1) Decimal w/ optional prefix:  $1.75, @1.75, 1.75
#   2) .NN w/ optional prefix:      $.96, @.96, .96
#   3) Bare integer ONLY w/ prefix: $3, @3
# A bare integer without `$`/`@` and without a decimal point (e.g. "3"
# in "qty 3") will NOT match — the prior pattern accepted bare \d{1,4}
# which conflated quantity tokens with prices. Captured group(1) may
# include the `$`/`@` prefix; pass through _price_value() to strip it
# before passing to Decimal.
_PRICE_ZONE = _PRICE  # alias — same regex, used in zone searches for clarity


def _price_value(m_or_str) -> str | None:
    """Strip optional `$`/`@` prefix from a captured _PRICE match. Accepts
    either a re.Match (uses group(1)) or a raw string. Returns None when
    no match. Result is safe to pass to Decimal()."""
    import re as _re
    if m_or_str is None:
        return None
    raw = m_or_str.group(1) if hasattr(m_or_str, "group") else m_or_str
    if raw is None:
        return None
    return _re.sub(r"^[\$@]\s*", "", raw)

# Pre-compiled utility patterns (eliminates re-compilation on every parse call)
_RE_BOLD      = re.compile(r"\*\*(.+?)\*\*")
_RE_UNDERLINE = re.compile(r"__(.+?)__")
_RE_STRIKE    = re.compile(r"~~(.+?)~~")
_RE_ITALIC    = re.compile(r"\*(.+?)\*")
_RE_EMOJI_HIGH  = re.compile(r"[\U00010000-\U0010ffff]")   # high-plane emoji
_RE_EMOJI_MISC  = re.compile(r"[\u2600-\u27ff\u2b50-\u2b55\ufe0f]")  # misc symbols
_RE_FRACTION    = re.compile(r"\b(\d{1,2})\/(\d{1,2})\b")
_RE_BRACKET_TAG = re.compile(r"\[(\w[\w\s]*\w|\w)\]")
# Sniper tags every small-account-challenge trade with ‼ (U+203C, usually as
# the ‼️ emoji) and/or the words "small account". Matched on the ORIGINAL text
# because _strip_emoji() deletes the marker. Consumed by the executor to decide
# whether his stated contract count outranks size_default — see
# _honors_message_qty() in public_executor.py.
_RE_SMALL_ACCOUNT = re.compile(r"‼|small[\s_]?account", re.IGNORECASE)

_RE_QTY_PATTERNS = [
    re.compile(r"contracts?:\s*(\d+)", re.IGNORECASE),
    re.compile(r"(\d+)\s*contracts?\b", re.IGNORECASE),
    # Sniper small-account challenge (2026-07-30 onward): the count sits BEFORE
    # the phrase — "BOUGHT CRM 180C 7/31 1.63 ‼️ - 2 on small account."
    # He uses "on" and "in" interchangeably, and drops the word "account" about
    # a third of the time ("- 2 on small"). A 62-day replay of #sniper-alerts
    # (2026-06-11..08-12) found three ‼ BUYs under-sized to 1 contract by the
    # stricter wording: CRWV 120C "5 in small account" (2026-08-12, mirrored 1
    # of 5) and TSLA 330C "2 on small" (2026-08-10). "account" optional, and
    # "in" accepted alongside "on".
    re.compile(r"(\d+)\s+(?:on|in)\s+(?:the\s+)?small\b(?:[\s_]?account)?", re.IGNORECASE),
    # "- 2 more on MU" (2026-08-10 MU 1000C): an add-on states its own count.
    # Requires the trailing on/in so a SELL's "Leaving 1 more" / "holding 2
    # more" — which mean contracts REMAINING, not contracts traded — can't
    # match. qty only ever sizes a BUY (_contracts_for_size), but keep it
    # unambiguous at the source anyway.
    re.compile(r"(\d+)\s+more\s+(?:on|in)\b", re.IGNORECASE),
    # Count AFTER the phrase, but only in the SAME clause: "small account: 2",
    # "small account challenge - 3". The old form allowed an unbounded [^\d]*
    # gap, so "- 1 on small account. We will ride this one for ITM or -50% SL"
    # (Sniper SKHY 2026-07-31) skipped the leading 1 and captured the 50 out of
    # "-50%" → a 1-contract alert would have been mirrored as 50 contracts.
    re.compile(r"small[\s_]?account(?:\s+challenge)?\s*[:\-–—]?\s*(\d+)\b", re.IGNORECASE),
    # Count fused to the marker with no words at all — Sniper 2026-08-07:
    # "BOUGHT MU 880C 8/7 2.85‼️3. SL is 2." and 2026-07-24 "...2.65‼️10 on
    # small" (says "on small", not "on small account", so the phrase patterns
    # above miss it too). Anchored on ‼ and allowing only whitespace before the
    # digit, so "‼️ - Leaving 1 more" and "‼️ **SOLD 3/5 ...**" do NOT match.
    re.compile("‼️?\\s*(\\d{1,2})\\b"),
]



# ─── Helpers ──────────────────────────────────────────────────────────────────

def _strip_markdown(text: str) -> str:
    """Remove Discord markdown formatting: **bold**, *italic*, __underline__, ~~strikethrough~~."""
    text = _RE_BOLD.sub(r"\1", text)       # **bold**
    text = _RE_UNDERLINE.sub(r"\1", text)  # __underline__
    text = _RE_STRIKE.sub(r"\1", text)     # ~~strikethrough~~
    text = _RE_ITALIC.sub(r"\1", text)     # *italic*  (after bold)
    # Whatever asterisk survives all four passes is an unclosed marker, and it
    # fuses to the next token and kills the parse: Sniper typed one `*` instead
    # of two on 2026-07-28 ("**BOUGHT MU 880C 7/31 4.55* - Small please") and
    # the alert dropped. Space, not empty string, so "SOLD*1/2" can't fuse into
    # a single token.
    return text.replace("*", " ")


def _strip_emoji(text: str) -> str:
    """Remove ALL emoji / Unicode symbol characters using pre-compiled patterns."""
    text = _RE_EMOJI_HIGH.sub(" ", text)   # high-plane emoji (😀 🟢 🔴 etc.)
    text = _RE_EMOJI_MISC.sub(" ", text)   # misc symbols/arrows/dingbats
    # Remove remaining symbol-category chars (So/Sm/Sk) — single pass
    text = "".join(" " if unicodedata.category(ch) in ("So", "Sm", "Sk") else ch for ch in text)
    return text.strip()


def _parse_date(mo: str, day: str, yr: Optional[str]) -> Optional[date]:
    try:
        month = int(mo)
        d     = int(day)
        if yr:
            year = int(yr) + 2000 if len(yr) == 2 else int(yr)
            return date(year, month, d)
        today = date.today()
        candidate = date(today.year, month, d)
        delta = (candidate - today).days
        if delta < -30:
            # Far past — likely wraps to next year (e.g. Jan alert posted in Dec)
            return date(today.year + 1, month, d)
        if delta < 0:
            # Recent past — option already expired; reject rather than bump
            return None
        return candidate
    except (ValueError, TypeError):
        return None


def _is_close_all(lower: str) -> bool:
    """Check for 'close all' phrasing using pre-compiled single OR pattern."""
    return bool(_CLOSE_ALL_RE.search(lower))


def _search_valid_sym(pattern, text, sym_group: int = 1, flags=re.IGNORECASE):
    """re.search but skip matches whose ticker capture is a reserved word
    OR is a partial sub-match of a longer word.

    Without this guard, a fluid-alerts post like "> SOLD 1/2 SPX 7450C 5/21"
    matched ticker="SOLD"; a sniper-alerts post like "BOUGHT 5/29 100C $1.00"
    matched ticker="OUGHT" (sub-string of BOUGHT).

    Walks re.finditer and returns the first match where:
      1. The sym capture is not in _NEVER_TICKER
      2. The sym capture does not start in the middle of a longer alphabetic
         token (preceding char must be non-letter)
      3. The sym capture is not a PREFIX of a longer alphabetic token
         (following char must be non-letter). `[A-Z]{1,5}` truncates any
         longer word to its first 5 letters, so alt-pattern 4f read
         "I will sell 920C against this one" (Sniper MU 2026-07-30) as
         ticker="AGAIN" strike=920C.
    """
    for m in re.finditer(pattern, text, flags):
        sym = m.group(sym_group) if m.lastindex and m.lastindex >= sym_group else None
        if not sym or sym.upper() in _NEVER_TICKER:
            continue
        sym_start, sym_end = m.span(sym_group)
        if sym_start > 0 and text[sym_start - 1].isalpha():
            continue  # sym is mid-word — not a real ticker
        if sym_end < len(text) and text[sym_end].isalpha():
            continue  # sym is the head of a longer word — not a real ticker
        return m
    return None


def _extract_fraction_and_size(text: str) -> tuple[Decimal, str]:
    """Extract fraction and size tag from text zone after the main match."""
    lower = text.lower()
    fraction = Decimal("1")

    frac_m = re.search(r"\b(\d{1,2})\/(\d{1,2})\b", text)
    if frac_m:
        frac_key = frac_m.group(0)
        if frac_key in FRACTION_MAP:
            fraction = FRACTION_MAP[frac_key]
        else:
            numer, denom = int(frac_m.group(1)), int(frac_m.group(2))
            if 0 < numer < denom <= 99:
                fraction = Decimal(numer) / Decimal(denom)
    else:
        for key, val in FRACTION_MAP.items():
            if re.search(rf"\b{re.escape(key)}\b", text, re.IGNORECASE):
                fraction = val
                break

    size_tag = resolve_size_tag(text)

    close_all = bool(_CLOSE_ALL_RE.search(lower)) or bool(
        _CLOSE_ALL_RE2.search(lower)
    )
    if close_all:
        fraction = Decimal("1")

    return fraction, size_tag


def _extract_qty(full_text: str) -> Optional[int]:
    """
    Extract an explicit contract count from the message.
    Uses pre-compiled regex patterns for zero-overhead matching.
    Returns None if not found or count > 50 (sanity cap).
    """
    for pat in _RE_QTY_PATTERNS:
        m = pat.search(full_text)
        if m:
            n = int(m.group(1))
            if 1 <= n <= 50:  # sanity cap
                return n
    return None



def _frac_zone(text_clean: str, start: int, end: int) -> str:
    """Return prefix + zone (text outside the matched span) so
    `_extract_fraction_and_size` can pick up fractions like `1/4` that sit
    BEFORE the symbol/strike/date match. The matched span itself is dropped
    so date tokens (`7/17`) inside the match aren't mistaken for fractions.
    Alt patterns 4b–4g used to pass only `text_clean[m.end():]`, which
    missed prefix fractions like `SOLD 1/4 AMZN 300C 7/17 4` → bot sold all
    contracts instead of 25% (Sniper AMZN 2026-05-27, alert id 570: 4
    bought @ 3.15, all 4 sold @ 3.95).
    """
    return text_clean[:start] + " | " + text_clean[end:]


def _try_alternative_patterns(
    text_clean: str, action: str, channel_id: Optional[int]
) -> Optional["TradeAlert"]:
    """Try non-standard alert formats seen in real Discord bot messages."""

    lower = text_clean.lower()

    # ── 4a: terse sniper format — SYM STRIKE+OT M.DD PRICE ────────────────
    #   e.g. "BOUGHT RDDT 185C 6.18 1.79" (sniper-alerts). The date uses a DOT
    #   separator (6.18 = Jun 18), which _DATE's `[\/\-]` misses → NO PARSE,
    #   dropping the signal (2026-06-16, alert lost). Accept dot/slash/dash here
    #   and REQUIRE a trailing price so a bare price like "3.15" (no date) is
    #   never misread as a date; _parse_date still range-validates month/day.
    sniper_pat = (
        r"([A-Z]{1,5})\s+"
        + _STRIKE + _OT + r"\s+"
        + r"(\d{1,2})[\/\-.](\d{1,2})(?:[\/\-.](\d{2,4}))?\s+"
        + _PRICE
    )
    m = _search_valid_sym(sniper_pat, text_clean)
    if m:
        sym, stk_s, ot_s = m.group(1), m.group(2), m.group(3)
        mo, day, yr = m.group(4), m.group(5), (m.group(6) if m.lastindex >= 6 else None)
        expiry = _parse_date(mo, day, yr)
        if expiry:
            price_s = _price_value(m.group(7)) if m.lastindex >= 7 else None
            price = Decimal(price_s).quantize(Decimal("0.01")) if price_s else None
            frac, size = _extract_fraction_and_size(_frac_zone(text_clean, m.start(), m.end()))
            return TradeAlert(
                action=action, symbol=sym.upper(), expiry=expiry,
                strike=float(stk_s), option_type=ot_s.upper(),
                price=price, fraction=frac, size_tag=size,
                close_all=_is_close_all(lower), channel_id=channel_id,
            )

    # ── 4b: reversed — SYM STRIKE+OT [PRICE] DATE [PRICE] ─────────────────
    #   e.g. "SPY 678P 4/14 $3.15"  or  "SPY 682C $.96C 4/10 LOTTO"
    rev_pat = (
        r"([A-Z]{1,5})\s+"
        + _STRIKE + _OT + r"\s+"
        + r"(?:" + _PRICE + r"[A-Za-z]?\s+)?"  # optional price between OT and date
        + _DATE
        + r"(?:.*?" + _PRICE + r")?"            # optional price after date
    )
    m = _search_valid_sym(rev_pat, text_clean)
    if m:
        sym, stk_s, ot_s = m.group(1), m.group(2), m.group(3)
        mid_px = m.group(4)                     # price between strike and date (may be None)
        mo, day, yr = m.group(5), m.group(6), m.group(7)
        end_px = m.group(8) if m.lastindex >= 8 else None  # price after date
        expiry = _parse_date(mo, day, yr)
        if expiry:
            # Prefer the price that looks like a real option price (not the strike)
            price_s = _price_value(end_px) or _price_value(mid_px)
            price = Decimal(price_s).quantize(Decimal("0.01")) if price_s else None
            frac, size = _extract_fraction_and_size(_frac_zone(text_clean, m.start(), m.end()))
            return TradeAlert(
                action=action, symbol=sym.upper(), expiry=expiry,
                strike=float(stk_s), option_type=ot_s.upper(),
                price=price, fraction=frac, size_tag=size,
                close_all=_is_close_all(lower), channel_id=channel_id,
            )

    # ── 4c: OT stuck to date — SYM STRIKE OT+DATE [PRICE] ──────────────────
    #   e.g. "SPX 6790 C4/08 $7.00"
    ot_date = (
        r"([A-Z]{1,5})\s+"
        + _STRIKE + r"\s+"
        + _OT + r"(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?"
    )
    m = _search_valid_sym(ot_date, text_clean)
    if m:
        sym, stk_s, ot_s = m.group(1), m.group(2), m.group(3)
        mo, day, yr = m.group(4), m.group(5), m.group(6) if m.lastindex >= 6 else None
        expiry = _parse_date(mo, day, yr)
        if expiry:
            zone = text_clean[m.end():]
            px_m = re.search(_PRICE, zone)
            price = Decimal(_price_value(px_m)).quantize(Decimal("0.01")) if px_m else None
            frac, size = _extract_fraction_and_size(_frac_zone(text_clean, m.start(), m.end()))
            return TradeAlert(
                action=action, symbol=sym.upper(), expiry=expiry,
                strike=float(stk_s), option_type=ot_s.upper(),
                price=price, fraction=frac, size_tag=size,
                close_all=_is_close_all(lower), channel_id=channel_id,
            )

    # ── 4d: double symbol — SYM DATE SYM STRIKE+OT [PRICE] ─────────────────
    #   e.g. "SPY 4/10 SPY 645P $4.33"
    dbl_sym = (
        r"([A-Z]{1,5})\s+"
        + _DATE + r"\s+"
        + r"[A-Z]{1,5}\s+"
        + _STRIKE + _OT
    )
    m = _search_valid_sym(dbl_sym, text_clean)
    if m:
        sym = m.group(1)
        mo, day, yr = m.group(2), m.group(3), m.group(4)
        stk_s, ot_s = m.group(5), m.group(6)
        expiry = _parse_date(mo, day, yr)
        if expiry:
            zone = text_clean[m.end():]
            px_m = re.search(_PRICE, zone)
            price = Decimal(_price_value(px_m)).quantize(Decimal("0.01")) if px_m else None
            frac, size = _extract_fraction_and_size(_frac_zone(text_clean, m.start(), m.end()))
            return TradeAlert(
                action=action, symbol=sym.upper(), expiry=expiry,
                strike=float(stk_s), option_type=ot_s.upper(),
                price=price, fraction=frac, size_tag=size,
                close_all=_is_close_all(lower), channel_id=channel_id,
            )

    # ── 4e: duplicate date — SYM DATE DATE STRIKE+OT [PRICE] ────────────────
    #   e.g. "SPX 4/09 4/9 6800C $4.15"
    dup_date = (
        r"([A-Z]{1,5})\s+"
        + _DATE + r"\s+"
        + r"\d{1,2}[\/\-]\d{1,2}(?:[\/\-]\d{2,4})?\s+"
        + _STRIKE + _OT
    )
    m = _search_valid_sym(dup_date, text_clean)
    if m:
        sym = m.group(1)
        mo, day, yr = m.group(2), m.group(3), m.group(4)
        stk_s, ot_s = m.group(5), m.group(6)
        expiry = _parse_date(mo, day, yr)
        if expiry:
            zone = text_clean[m.end():]
            px_m = re.search(_PRICE, zone)
            price = Decimal(_price_value(px_m)).quantize(Decimal("0.01")) if px_m else None
            frac, size = _extract_fraction_and_size(_frac_zone(text_clean, m.start(), m.end()))
            return TradeAlert(
                action=action, symbol=sym.upper(), expiry=expiry,
                strike=float(stk_s), option_type=ot_s.upper(),
                price=price, fraction=frac, size_tag=size,
                close_all=_is_close_all(lower), channel_id=channel_id,
            )

    # ── 4f: STRIKE+OT SYMBOL (no date) — "SOLD 85P ASTS" ───────────────────
    strike_sym = (
        r"(?:bought|sold|bto|bot|stc|buy|sell|closing|close(?:d|ing)?|opening|open"
        r"|entering?|selling|exiting?|exit|out|all\s+out|all\s+time)\s+"
        + _STRIKE + _OT + r"\s+"
        + r"([A-Z]{1,5})"
    )
    m = _search_valid_sym(strike_sym, text_clean, sym_group=3)
    if m:
        stk_s, ot_s, sym = m.group(1), m.group(2), m.group(3)
        zone = text_clean[m.end():]
        px_m = re.search(_PRICE, zone)
        price = Decimal(_price_value(px_m)).quantize(Decimal("0.01")) if px_m else None
        return TradeAlert(
            action=action, symbol=sym.upper(), expiry=None,
            strike=float(stk_s), option_type=ot_s.upper(),
            price=price, fraction=Decimal("1"),
            close_all=(action == "SELL"), channel_id=channel_id,
        )

    # ── 4g: KEYWORD SYM DATE STRIKE PRICE — missing C/P, default to Call ───
    #   e.g. "BOUGHT TSLA 5/15 455 $2.44 [SMALL]"
    #   Analyst convention: bare strike with no C/P = call. Require an
    #   explicit "$price" anchor right after the strike so we don't
    #   misparse equity references like "AMZN 258 is previous high" or
    #   chatter that happens to mention a date+number.
    no_ot_pat = (
        r"(?:bought|sold|bto|bot|stc|buy|sell|closing|close(?:d|ing)?|opening|open"
        r"|entering?|selling|exiting?|exit|out|long|short)\s+"
        r"([A-Z]{1,5})\s+"
        + _DATE + r"\s+"
        + r"(\d{1,6}(?:\.\d{1,2})?)"
        + r"\s+\$\s*(\d+(?:\.\d{1,2})?)"
    )
    m = _search_valid_sym(no_ot_pat, text_clean)
    if m:
        sym = m.group(1)
        mo, day, yr = m.group(2), m.group(3), m.group(4)
        stk_s = m.group(5)
        px_s = m.group(6)
        expiry = _parse_date(mo, day, yr)
        if expiry:
            frac, size = _extract_fraction_and_size(_frac_zone(text_clean, m.start(), m.end()))
            return TradeAlert(
                action=action, symbol=sym.upper(), expiry=expiry,
                strike=float(stk_s), option_type="C",
                price=Decimal(px_s).quantize(Decimal("0.01")),
                fraction=frac, size_tag=size,
                close_all=_is_close_all(lower), channel_id=channel_id,
            )

    # ── 4h: implicit-SPX channel — no ticker, STRIKE+OT [DATE] ────────────
    #   e.g. ivtrader "bought 7515c $1.20 lotto" or "bought 7530c $3.05"
    #   (both undated -> 0DTE, his default convention) or "bought 7505c
    #   7/24 $32.40 SMALL" (explicit date). Gated to _IMPLICIT_SPX_CHANNELS
    #   so a bare strike elsewhere still requires a named ticker.
    #   No date given -> assume today (0DTE): ivtrader trades same-day SPX
    #   weeklies by default and only dates a post when it's NOT 0DTE, so an
    #   undated BUY defaulting to a later expiry would be the wrong guess
    #   more often than defaulting to today. An explicit-but-unparseable
    #   date (e.g. already expired) still drops the alert rather than
    #   silently reinterpreting it as today.
    #   SELL is closing an existing position instead (matched by symbol+
    #   strike+type, same as pattern 4f above), so a dateless "sold 7550c
    #   $9.60" still fires — ivtrader almost never dates his SELLs, and a
    #   SELL that silently fails to fire is the worse failure mode (leaves a
    #   live position unmanaged instead of just missing an entry).
    if channel_id in _IMPLICIT_SPX_CHANNELS:
        implicit_pat = (
            r"(?:bought|sold|bto|bot|stc|buy|sell|closing|close(?:d|ing)?|opening|open"
            r"|entering?|selling|exiting?|exit|out|stopped|added)\s+"
            r"(?:(\d{1,2}/\d{1,2})\s+)?"  # optional fraction before strike, e.g. "sold 1/2 7385c"
            + _STRIKE + _OT
            + r"(?:\s+" + _DATE + r")?"
        )
        m = re.search(implicit_pat, text_clean, re.IGNORECASE)
        if m:
            frac_token = m.group(1)
            stk_s, ot_s = m.group(2), m.group(3)
            mo, day, yr = m.group(4), m.group(5), m.group(6)
            expiry = _parse_date(mo, day, yr) if mo else date.today()
            if expiry is not None or action == "SELL":
                zone = text_clean[m.end():]
                px_m = re.search(_PRICE, zone)
                price = Decimal(_price_value(px_m)).quantize(Decimal("0.01")) if px_m else None
                # The fraction (if any) was consumed inside the match span, so
                # _frac_zone alone won't see it — feed it back in explicitly.
                frac_zone = ((frac_token + " ") if frac_token else "") + _frac_zone(text_clean, m.start(), m.end())
                frac, size = _extract_fraction_and_size(frac_zone)
                return TradeAlert(
                    action=action, symbol="SPX", expiry=expiry,
                    strike=float(stk_s), option_type=ot_s.upper(),
                    price=price, fraction=frac, size_tag=size,
                    close_all=_is_close_all(lower), channel_id=channel_id,
                )

    return None


# ─── Main parse function ──────────────────────────────────────────────────────

# ── Whale-tracker (Sniper Market Updates) structured-alert parser ────────────
# The whale-tracker feed is machine-generated by SniperTrades with a fixed
# layout, e.g.:
#   🐳 WHALE SPOTTED: `$MU` 🟢
#   **Stock**: `$MU` | **Strike**: `950.0C` | **Expiry**: `06/05/2026`
#                    | **Entry**: `$8.00` | **Action**: `BUY`
# The general analyst regex doesn't recognise this shape (0% parse rate on a
# 5-day sample), so we parse it deterministically here — far safer than the AI
# fallback for live money. Markdown (**) and code-ticks (`) are tolerated.
_WHALE_MARKER  = re.compile(r"WHALE\s+SPOTTED", re.I)
_WHALE_STOCK   = re.compile(r"Stock\W{0,4}:\s*`?\$?([A-Za-z]{1,6})", re.I)
_WHALE_STRIKE  = re.compile(r"Strike\W{0,4}:\s*`?\$?(\d+(?:\.\d+)?)\s*([CcPp])", re.I)
_WHALE_EXPIRY  = re.compile(r"Expiry\W{0,4}:\s*`?(\d{1,2})/(\d{1,2})/(\d{2,4})", re.I)
_WHALE_ENTRY   = re.compile(r"Entry\W{0,4}:\s*`?\$?(\d+(?:\.\d+)?)", re.I)
_WHALE_ACTION  = re.compile(r"Action\W{0,4}:\s*`?(BUY|SELL)", re.I)


def _parse_whale_alert(text: str, channel_id: Optional[int] = None) -> Optional["TradeAlert"]:
    """Parse the fixed whale-tracker layout. Returns None if it isn't a whale
    alert or any required field is missing (caller then falls through to the
    normal parser / AI)."""
    if not _WHALE_MARKER.search(text):
        return None
    m_stock  = _WHALE_STOCK.search(text)
    m_strike = _WHALE_STRIKE.search(text)
    m_exp    = _WHALE_EXPIRY.search(text)
    m_action = _WHALE_ACTION.search(text)
    if not (m_stock and m_strike and m_exp and m_action):
        return None
    expiry = _parse_date(m_exp.group(1), m_exp.group(2), m_exp.group(3))
    if expiry is None:
        return None
    m_entry = _WHALE_ENTRY.search(text)
    price = (Decimal(m_entry.group(1)).quantize(Decimal("0.01"))
             if m_entry else None)
    return TradeAlert(
        action=m_action.group(1).upper(),
        symbol=m_stock.group(1).upper(),
        expiry=expiry,
        strike=float(m_strike.group(1)),
        option_type=m_strike.group(2).upper(),
        price=price,
        fraction=Decimal("1"),
        channel_id=channel_id,
    )


_BOUGHT_TYPO = re.compile(r"\b(?:BOIGHT|BOUGTH|BOUHGT|BOUGT|BOGHT|BIUGHT|BOUGJT)\b", re.I)


def _clean_text(text: str) -> str:
    """Normalize a raw Discord message for pattern matching: strip markdown/
    emoji, collapse whitespace, drop cashtags and mentions, fix strike-dot-type
    typos. Extracted so parse_alert and its bare-price fallback see identical
    text."""
    text_clean = re.sub(r"\s+", " ", _strip_emoji(_strip_markdown(text.strip()))).strip()
    # Strip cashtag $ from symbols like $ASTS → ASTS
    text_clean = re.sub(r"\$([A-Z]{1,5})\b", r"\1", text_clean)
    # Typo: strike-dot-type "182.C" → "182C". Analysts occasionally drop a dot
    # between the strike and C/P; without this the whole OSI extraction fails and
    # a real targeted exit ("SOLD NVDA 182.C 8/29 ALL OUT") degrades to a blank-
    # symbol close_all. Only fires when the dot sits directly before C/P, so a
    # decimal strike ("182.5C") is untouched.
    text_clean = re.sub(r"(\d)\.([CP])\b", r"\1\2", text_clean, flags=re.I)
    # Typo: misspelled BOUGHT. Fluid's "BOIGHT AAPL 342.5C 9/23 0.47" on
    # 2026-09-23 parsed to nothing and the entry was missed. Exact-word list,
    # not fuzzy matching, so no other word can turn into a BUY.
    # Set parser_verb_typos=false to revert.
    try:
        from app.core.config_manager import cfg as _cfg
        _typos = _cfg.getboolean("trading", "parser_verb_typos", fallback=True)
    except Exception:
        _typos = True
    if _typos:
        text_clean = _BOUGHT_TYPO.sub("BOUGHT", text_clean)
    # Remove @everyone / @here / <@ID>/<@!ID> (user) / <@&ID> (role) / <#ID>
    # (channel) Discord mentions. Role pings (<@&ID>) have a TWO-char prefix
    # ("@&") - a single-char class here would leave "&1234..." in the text.
    text_clean = re.sub(r"@(?:everyone|here)\b", "", text_clean)
    text_clean = re.sub(r"<(?:@[!&]?|#)\d+>", "", text_clean)
    text_clean = re.sub(r"\s+", " ", text_clean).strip()
    return text_clean


# Bare integer at the ABSOLUTE END of the cleaned message. Sniper/Fluid post the
# premium as the trailing number after the date ("BOUGHT DDOG 265C 7/10 5" → $5).
# _PRICE deliberately rejects a bare integer (no decimal, no $/@) to avoid reading
# quantity tokens as prices, which leaves these integer-premium BUYs price=None →
# blocked by the executor's missing-price guard. The lookbehind keeps us from
# slicing a decimal/date/strike ("7/10" → not "10"), and the end-anchor is the
# real safety: a trailing qty like "... 7/17 4 - 30% here" (Sniper AMZN id 570)
# sits mid-message and is NOT rescued, so contracts are never misread as a price.
_TRAILING_BARE_PRICE = re.compile(r"(?<![\d./@$])(\d{1,4})\s*$")

# Same integer premium, but NOT at the end of the message. Since the 2026-07-30
# small-account challenge Sniper appends a clause after the price
# ("BOUGHT NBIS 200C 8/7 9 ‼️ - 1 on small account. WIll make it a spread"),
# which puts the end-anchor above out of reach — that alert parsed price=None
# and was refused by the missing-price guard while he filled at 9.05.
# Anchor on the PRICE SLOT instead: the bare integer immediately after the
# expiry date. That is structural, not positional, so trailing commentary is
# harmless. Guards, in order:
#   (?!\s*(?:contracts?|on\b|x\b))  → "8/7 2 contracts" is a QUANTITY, not a price
#   (?![\d./%-])                    → don't bite into 9.05, 1/2, 30%, or 200-205
#   (?![CcPp])                      → "8/21 7060C" is the STRIKE (date-first
#                                     grammar), not a $7060 premium
# BUY-only at the call site, so a SELL's trailing size token can never become a
# limit price (SELLs stay price=None and fill at market on purpose).
_DATE_THEN_BARE_PRICE = re.compile(
    r"\b\d{1,2}[\/\-.]\d{1,2}(?:[\/\-.]\d{2,4})?\s+"
    r"(\d{1,4})(?!\s*(?:contracts?|on\b|x\b))(?![\d./%-])(?![CcPp])"
)


def parse_alert(text: str, channel_id: Optional[int] = None) -> Optional["TradeAlert"]:
    """
    Parse a raw Discord message and return a TradeAlert, or None if not a trade signal.

    close_all=True + symbol="" → executor exits ALL open positions.
    close_all=True + symbol set → executor exits that specific position fully.
    price=None → place a market / best-available limit order.

    Post-parse, a BUY that came out with no price but whose message ends in a
    bare integer adopts that integer as the entry premium (Sniper/Fluid grammar).
    Restricted to BUY because SELLs intentionally stay price=None so analyst
    exits fill at market rather than pinning to a limit that may not trade.
    """
    # A malformed price token used to escape as decimal.InvalidOperation and
    # kill the whole handler — the raise happened BEFORE discord_listener's AI
    # fallback branch, so an analyst exit was lost outright rather than retried.
    # Four such messages in 2yr of dumps, all SELLs, e.g. pro-alerts 2026-01-15
    # "**SOLD** SNDK 430C $8 1/4" and analyst-tc "#ALERT SOLD SPX 6930C $4 ALL
    # OUT RUNNERS". Returning None keeps the AI parser as the safety net.
    try:
        alert = _parse_alert_impl(text, channel_id)
    except Exception:
        log.exception("[PARSER] regex parse raised — deferring to AI fallback | text=%s", text[:150])
        return None
    if (
        alert is not None
        and alert.action == "BUY"
        and alert.price is None
        and alert.symbol
        and alert.strike is not None
        and alert.option_type
    ):
        _cleaned = _clean_text(text)
        m = _TRAILING_BARE_PRICE.search(_cleaned) or _DATE_THEN_BARE_PRICE.search(_cleaned)
        if m:
            alert.price = Decimal(m.group(1)).quantize(Decimal("0.01"))
    if alert is not None:
        # Stamped here (not in _parse_alert_impl) so all 8+ early-return format
        # branches get it from one place.
        alert.small_account = bool(_RE_SMALL_ACCOUNT.search(text))
    return alert


def _parse_alert_impl(text: str, channel_id: Optional[int] = None) -> Optional["TradeAlert"]:
    """
    Parse a raw Discord message and return a TradeAlert, or None if not a trade signal.

    close_all=True + symbol="" → executor exits ALL open positions.
    close_all=True + symbol set → executor exits that specific position fully.
    price=None → place a market / best-available limit order.
    """
    if not text or len(text) < 5:
        return None

    # Whale-tracker structured alerts first — fixed machine layout, parsed
    # deterministically (the general regex below scores 0% on them).
    _whale = _parse_whale_alert(text, channel_id)
    if _whale is not None:
        return _whale

    # Skip equity-account strategies (cash secured puts, covered calls, etc.)
    # Bot only trades naked long options.
    if _NOT_OPTIONS_RE.search(text):
        return None

    text_clean = _clean_text(text)
    lower      = text_clean.lower()

    # ── Step 1: detect "close all" phrases that don't need a specific symbol ──
    if _is_close_all(lower):
        # Check if a specific symbol is also mentioned; if not → close everything
        # Strike present if explicit C/P after strike, OR bare strike
        # follows a date and has an explicit $price (matches pattern 4g
        # for missing C/P). Without this, "SOLD TSLA 5/15 455 $3.20 ALL
        # OUT" would degenerate to blank-symbol close_all and exit every
        # position.
        has_strike = bool(
            re.search(_STRIKE + _OT, text_clean)
            or re.search(_DATE + r"\s+\d{1,6}(?:\.\d{1,2})?\s+\$", text_clean)
        )
        if not has_strike:
            # Generic "ALL OUT" or "CLOSE ALL" with no specific position.
            # Require an explicit SELL verb to avoid false positives on chitchat
            # like "AMZN 258 is previous all time high holding" (matches the
            # "all time" close-all phrase but is not a trade signal).
            has_sell_verb = any(
                re.search(rf"(?:^|[\s\-_]){re.escape(kw)}(?:[\s\-_$@\[]|$)", lower)
                for kw in SELL_KEYWORDS
            )
            if not has_sell_verb:
                return None
            # Reject equity references — bot only trades options, so an alert
            # referencing "GLW COMMONS" / "AAPL SHARES" / "MSFT STOCK" with a
            # trailing "All out" bullet is the analyst selling stock, not asking
            # us to close every option position.
            # 2026-05-11: Sarang's "SOLD GLW COMMONS AT 201 ... * All out"
            # collapsed to one line by whitespace normalization and triggered
            # blank-symbol close_all → closed an unrelated META 0DTE put
            # (−$30). Bot doesn't track equity, so any equity-context alert
            # should never fire close_all on options.
            if re.search(r"\b(shares?|commons?|common\s+stock|stocks?|equity|equities)\b", lower):
                return None
            # Reject non-signal chatter that trips a close-all phrase but is NOT
            # an exit instruction. 365d sample of blank-symbol close_all was
            # ~half parser noise: "QQQ close to all time high", "preserve capital
            # and call out a day", "they are selling everything again". None are
            # orders; left unguarded they each became a book-wide flatten.
            _NONSIGNAL = (
                "all time high", "all-time high", "alltime high",
                "call out a day", "call it a day", "called out a day",
                "preserve capital", "dont overtrade", "don't overtrade",
                "selling everything", "sell everything",
                "over week end", "over weekend", "over the weekend",
            )
            if any(p in lower for p in _NONSIGNAL):
                return None
            # Still try to extract price if present
            px_m = re.search(_PRICE, text_clean)
            price = Decimal(_price_value(px_m)).quantize(Decimal("0.01")) if px_m else None
            return TradeAlert(
                action="SELL", symbol="", expiry=None, strike=None,
                option_type="", price=price, fraction=Decimal("1"),
                close_all=True, channel_id=channel_id,
            )
        # Has a specific position — fall through to extract full details

    # ── Step 2: detect BUY / SELL keyword ────────────────────────────────────
    # Find the earliest BUY and earliest SELL match, then pick whichever
    # appears first in the text. Without this, "STC half, BTO new" would
    # silently drop the SELL leg (BUY was checked first, won the tie).
    def _first_kw_pos(keywords):
        best = None
        for kw in sorted(keywords, key=len, reverse=True):
            m = re.search(rf"(?:^|[\s\-_]){re.escape(kw)}(?:[\s\-_$@\[]|$)", lower)
            if m and (best is None or m.start() < best):
                best = m.start()
        return best

    buy_pos = _first_kw_pos(BUY_KEYWORDS)
    sell_pos = _first_kw_pos(SELL_KEYWORDS)
    if buy_pos is not None and sell_pos is not None:
        action = "BUY" if buy_pos < sell_pos else "SELL"
    elif buy_pos is not None:
        action = "BUY"
    elif sell_pos is not None:
        action = "SELL"
    else:
        action = None
    if action is None and _is_close_all(lower):
        action = "SELL"
    if action is None:
        return None

    # ── Step 3: primary pattern — KEYWORD SYM DATE STRIKE+OT [PRICE] ─────────
    primary = (
        r"(?:bought|sold|bto|bot|stc|buy|sell|closing|close(?:d|ing)?|opening|open"
        r"|entering?|selling|exiting?|exit|out|long|short|all\s+out|all\s+time)\s+"
        r"([A-Z]{1,5})\s+"
        + _DATE + r"\s+"
        + _STRIKE + _OT
        + r"(?:\s+[\$@]\s*(" + r"\d+\.?\d*" + r"))?"
    )
    m = _search_valid_sym(primary, text_clean)

    # ── Step 4: fallback — sym + date + strike+OT anywhere in string ─────────
    if not m:
        fallback = (
            r"([A-Z]{1,5})\s+"
            + _DATE + r"\s+"
            + _STRIKE + _OT
            + r"(?:.*?" + _PRICE_ZONE + r")?"
        )
        m = _search_valid_sym(fallback, text_clean)

    # ── Step 4b–4f: alternative format fallbacks ───────────────────────────
    # ONLY try alternative patterns if the primary + fallback both failed.
    # This prevents alt patterns from overriding a valid primary match
    # (e.g. AMD 4/17 285C $1.89 being re-matched as price=0.89 by the
    # reversed pattern 4b which grabs the decimal .89 out of $1.89).
    if not m:
        _alt = _try_alternative_patterns(text_clean, action, channel_id)
        if _alt is not None:
            _alt.qty = _extract_qty(text)
            return _alt

    # ── Step 5: no-date pattern — "ALL OUT SPX 6970C [$price]" ───────────────
    if not m:
        no_date_pat = (
            r"(?:all\s+out|all\s+time|out|stc|sold?|close(?:d|ing)?)\s+"
            r"([A-Z]{1,5})\s+"
            + _STRIKE + _OT
            + r"(?:[^A-Za-z0-9]+" + _PRICE + r")?"
        )
        m2 = re.search(no_date_pat, text_clean, re.IGNORECASE)
        if m2:
            sym, stk_s, ot_s = m2.group(1), m2.group(2), m2.group(3)
            px_s = m2.group(4) if m2.lastindex >= 4 else None
            return TradeAlert(
                action="SELL",
                symbol=sym.upper(),
                expiry=None,
                strike=float(stk_s),
                option_type=ot_s.upper(),
                price=Decimal(px_s).quantize(Decimal("0.01")) if px_s else None,
                fraction=Decimal("1"),
                close_all=True,
                channel_id=channel_id,
            )
        return None

    # ── Step 6: unpack groups ─────────────────────────────────────────────────
    g        = m.groups()
    sym      = g[0]
    mo, day, yr = g[1], g[2], g[3]
    strike_s = g[4]
    ot_s     = g[5]
    price_s  = _price_value(g[6]) if len(g) > 6 else None

    expiry = _parse_date(mo, day, yr)
    if expiry is None:
        return None

    symbol      = sym.upper()
    option_type = ot_s.upper()
    strike      = float(strike_s)
    price       = Decimal(price_s).quantize(Decimal("0.01")) if price_s else None

    # ── Step 7: fraction — look AFTER the main match to dodge date artifacts ──
    zone    = text_clean[m.end():]

    # If primary pattern didn't capture a price (e.g. $.96 with leading dot),
    # try to extract it from the zone text using the zone price regex.
    if price is None and zone:
        zone_px_m = re.search(_PRICE_ZONE, zone)
        if zone_px_m:
            px_val = _price_value(zone_px_m)
            # Reject bare integers that look like fractions (e.g. "1" in "1/2")
            if px_val and ("." in px_val or (px_val.startswith(".") and len(px_val) > 1)):
                try:
                    price = Decimal(px_val).quantize(Decimal("0.01"))
                except Exception:
                    pass

    fraction = Decimal("1")

    frac_m  = re.search(r"\b(\d{1,2})\/(\d{1,2})\b", zone)
    if frac_m:
        frac_key = frac_m.group(0)
        if frac_key in FRACTION_MAP:
            fraction = FRACTION_MAP[frac_key]
        else:
            # Compute arbitrary fractions like 1/16, 3/10, etc.
            numer, denom = int(frac_m.group(1)), int(frac_m.group(2))
            if 0 < numer < denom <= 99:
                fraction = Decimal(numer) / Decimal(denom)
    else:
        # First check the zone (after the main match) for keyword fractions
        found_frac = False
        for key, val in FRACTION_MAP.items():
            if re.search(rf"\b{re.escape(key)}\b", zone, re.IGNORECASE):
                fraction = val
                found_frac = True
                break

        # Also search the PREFIX (text before the match, e.g. "Sold half SPX…" or "Sold 1/3 AMD…")
        # Fraction word/ratio may appear between SELL keyword and symbol name.
        if not found_frac:
            prefix = text_clean[: m.start()]
            frac_m_pre = re.search(r"\b(\d{1,2})\/(\d{1,2})\b", prefix)
            if frac_m_pre:
                frac_key = frac_m_pre.group(0)
                if frac_key in FRACTION_MAP:
                    fraction = FRACTION_MAP[frac_key]
                else:
                    numer, denom = int(frac_m_pre.group(1)), int(frac_m_pre.group(2))
                    if 0 < numer < denom <= 99:
                        fraction = Decimal(numer) / Decimal(denom)
            else:
                for key, val in FRACTION_MAP.items():
                    if re.search(rf"\b{re.escape(key)}\b", prefix, re.IGNORECASE):
                        fraction = val
                        break


    # ── Step 8: close_all flag ────────────────────────────────────────────────
    # Also fire if "out" is the very first word (e.g. "OUT SPX 4/14 6970C $2.70")
    # Zone trailing keywords: "SOLD SPX 4/14 6970C $2.70 ALL" → zone contains "ALL"
    # IMPORTANT: only check zone for "all"/"full" on SELL actions — BUY alerts
    # tagged [FULL] or [LARGE] should NOT trigger close_all.
    zone_lower = zone.lower() if zone else ""
    prefix = text_clean[: m.start()]  # text before the main match (where "half", "1/3" etc. live)
    # A fraction found in the prefix counts as an explicit partial-exit fraction
    _prefix_has_frac = bool(re.search(r"\b(\d{1,2})\/(\d{1,2})\b", prefix)) or any(
        re.search(rf"\b{re.escape(k)}\b", prefix, re.IGNORECASE)
        for k in FRACTION_MAP if k not in ("all", "full")
    )
    # If the message has an explicit contract count ("Sold SPX 6970C 2 contracts"),
    # treat that as a partial sell — never close_all, even with no fraction keyword.
    _explicit_qty = _extract_qty(text)

    # Strict close_all: only set close_all=True when the message has an
    # EXPLICIT close-all phrase. The legacy fallback "SELL with no fraction
    # = close_all" misclassified analyst chitchat (e.g. Sarang's bare "all
    # out" in a different ticker thread closed an unrelated NVDA position
    # 2026-05-07: -$260) and bypassed the post-PT2 runner-protection guard
    # in public_executor (a bare "SOLD SPX 7380P $4.90" with no "1/2" tag
    # would kill a runner that PT2 had already trimmed). When strict mode
    # is on (default), bare-fraction SELLs default to fraction=1 with
    # close_all=False — the qty math still resolves to pos.remaining for
    # any non-PT2 case, so day-zero behaviour is preserved; the only
    # behavioural change is post-PT2 ignores partial-style SELLs (correct
    # — analyst's bare SELL after PT2 should NOT clobber the runner).
    # Set parser_strict_close_all=false to revert.
    try:
        from app.core.config_manager import cfg as _cfg
        _strict = _cfg.getboolean("trading", "parser_strict_close_all", fallback=True)
    except Exception:
        _strict = True

    close_all = (
        _is_close_all(lower)
        or bool(re.search(r"\b(close\s+out|all\s+in\s+out)\b", lower))
        or bool(re.match(r"^out\b", lower))
        or bool(re.search(r"\b(sold\s+all|sold\s+full|close(?:d)?\s+all)\b", lower))
        # Zone trailing keywords only apply to SELL — avoids false positive on [FULL] BUY tags
        or (action == "SELL" and bool(re.search(r"\b(all|full)\b", zone_lower)))
        # CLOSED keyword at start (e.g. "CLOSED SPX 4/14 6970C") — SELL action only
        or (action == "SELL" and bool(re.match(r"^closed?\b", lower)))
    )
    if not _strict:
        # Legacy: SELL with no explicit fraction keyword = close all full position.
        # Kept behind the flag for one-knob revert if the strict default
        # over-corrects on some analyst's messaging style.
        close_all = close_all or (
            action == "SELL" and not frac_m and not _prefix_has_frac and not _explicit_qty and not any(
                re.search(rf"\b{re.escape(k)}\b", zone, re.IGNORECASE)
                for k in FRACTION_MAP if k not in ("all", "full")
            )
        )
    if close_all:
        fraction = Decimal("1")



    # ── Step 9: size tag ──────────────────────────────────────────────────────
    # Gated risk tags ([ROLL UP]/[ROLL DOWN]/LOTTO) outrank size buckets — see
    # resolve_size_tag. Both are matched against text_clean, which `lower` is
    # derived from, so this is source-identical to the old inline scan.
    size_tag = resolve_size_tag(text_clean)

    return TradeAlert(
        action      = action,
        symbol      = symbol,
        expiry      = expiry,
        strike      = strike,
        option_type = option_type,
        price       = price,
        fraction    = fraction,
        size_tag    = size_tag,
        close_all   = close_all,
        channel_id  = channel_id,
        qty         = _extract_qty(text),   # use original text (pre-clean) for best pattern coverage
    )
