"""Parser for TWI-style vertical credit-spread and butterfly alerts.

Deliberately a separate module from `parser.py`: this is a different grammar
(one line describes two legs and a net price, not one contract), and the
single-leg parser is money-critical enough that it shouldn't grow a second
language inside it.

The grammar, taken from the full history of the channel (2026-06-26 onward,
123 messages / 52 action lines — see discord-analytics/raw/twi-spreads.txt):

    OPENED: CCS SPX 7390/95 $1.25 x 3 @everyone
    OPENED: SPX PCS 7435/30 $0.80 x1 @everyone
    CLOSED: CCS SPX 7410/15 $0.70 1/3 POS @everyone
    CLOSED: SPX CCS 7575/80 1.80 ALL OUT @everyone
    EXPIRED: SPX PCS 7500/95 $0.00 @everyone (+100%)

  CCS = call credit spread (short leg below long), PCS = put credit spread
  (short leg above long). The short strike is always written first and the
  long leg is abbreviated to its trailing digits — `7390/95` is 7390/7395,
  `7500/95` is 7500/7495. No expiry is ever stated: every one is a same-day
  SPXW 0DTE.

Two properties make this safe to act on, and both are enforced below:

  * The action verb must open the line. Every false positive the single-leg
    parser produces on this channel comes from mid-sentence chatter
    ("closed out above us, open to run up now" → a blank close_all SELL), so
    an unanchored match is not an option here.
  * The abbreviated long strike is reconstructed from the width and direction,
    and the printed digits are then used as a checksum. Over all 28 opens in
    the history the two agree every time, so a disagreement means the message
    is malformed and must be refused rather than guessed at.

Refusing is always the correct failure: an unparsed spread costs a missed
trade, a mis-parsed one opens the wrong strikes.

BUTTERFLIES (added 2026-08-13, behind [trading] spread_flies_enabled)
────────────────────────────────────────────────────────────────────
The same analyst also posts long butterflies, in two spellings that both
appeared in one session (2026-08-13):

    OPENED: SPX 10W FLY 7790/7800/7810 $1.80 @everyone
    CLOSED: SPX 10W FLY 7790/7800/7810 $3.0 1/2 POS +62% @everyone
    CLOSED: SPX 10W 7800 FLY $4.15 ALL OUT +130% @everyone

`10W` is the WING distance, not the total span: 7800 +/- 10. The third line
names the body strike only, so the width token is what reconstructs the wings
— and when all three strikes ARE printed they are used as a checksum, exactly
as the vertical's abbreviated long leg is. Strikes may sit before or after the
FLY keyword; both orderings are live. A strike may also carry its right
("5W FLY 7635P", 2026-09-17) — that spelling dropped the trade as chatter,
because the letter broke the strike capture and `price` then swallowed 7635.

A close may name no strikes at all:

    CLOSED 1/2 FLY $3.80 @everyone

which is `bare`: the fraction sits where the strikes usually do and only the
open position knows which fly he means, so spread_executor resolves it and
refuses unless exactly one fly is open. An OPEN never goes bare — there is
nothing to resolve it against.

A fly inverts the vertical in every direction that matters, which is why the
sign handling below and in spread_executor is branched rather than shared:

  * It is a DEBIT. You pay to open and receive to close — the opposite of the
    credit spread's cash flow, and the opposite Tradier `order_type`.
  * Its risk is the debit paid, in full. Not (width - credit).
  * It is 1-2-1, not 1:1 — the body leg carries twice the wing quantity, which
    is the ratio net_fill_price's per-spread divisor had to learn.

The right (calls or puts) is usually not stated in the alert; when it is,
the printed letter wins. Otherwise the analyst's own
explainer for this structure says puts —

    "you open this by selling 2 7800 puts, buy 1 7790p, buy 1 7810p all at
     once to create the fly"

— so [trading] spread_fly_right defaults to P and is a config knob rather than
a guess baked into the regex. Near the money the two are near-equivalent in
payoff but not in price, so the wrong right mostly costs a no-fill against the
posted limit rather than a wrong position.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional

from app.core.config_manager import cfg
from app.ingest.parser import _build_osi

# SPX options carry the SPXW root for every weekly and 0DTE expiry — which is
# all of them here. tradier_sdk_bridge._osi_parts maps SPXW back to the SPX
# underlying for the order's `symbol` param, so this stays consistent with the
# single-leg path.
_WEEKLY_ROOTS = {"SPX": "SPXW"}

# Widths the analyst actually trades. A reconstructed long strike landing
# outside this set means the line didn't say what we think it said.
ALLOWED_WIDTHS: tuple[float, ...] = (5.0, 10.0, 15.0, 20.0, 25.0)

DEFAULT_SYMBOL = "SPX"

_ACTION = re.compile(
    r"^\W{0,3}(?P<act>OPENED|CLOSED|EXPIRED)\s*:?\s+"        # must open the line
    r"(?:(?P<sym1>[A-Z]{1,6})\s+)?"                          # SPX before...
    r"(?P<kind>(?:CC|PC)S?)\s+"                             # CCS/PCS, or his CC/PC shorthand
    r"(?:(?P<sym2>[A-Z]{1,6})\s+)?"                          # ...or after
    r"(?P<short>\d{2,5})[CP]?\s*/\s*(?P<suffix>\d{1,5})[CP]?\s+"
    r"\$?(?P<price>\d+(?:\.\d+)?|\.\d+)",
    re.IGNORECASE,
)

# Butterflies. Strikes may be written before or after the FLY keyword and the
# width token may sit on either side too, so both positions are optional and
# whichever one matched is used. Requiring the FLY/BUTTERFLY word keeps this
# from ever competing with the vertical grammar above.
_STRIKE = r"\d{3,5}[CP]?"               # he sometimes states the right: 7635P
_STRIKES = rf"{_STRIKE}(?:\s*/\s*{_STRIKE}){{0,2}}"
_FLY = re.compile(
    r"^\W{0,3}(?P<act>OPENED|CLOSED|EXPIRED)\s*:?\s+"        # must open the line
    r"(?:(?P<sym1>[A-Z]{1,6})\s+)?"
    r"(?:(?P<w1>\d{1,3})\s*W\s+)?"                           # 10W before...
    rf"(?:(?P<pre>{_STRIKES})\s+)?"                          # ...strikes before...
    r"(?:(?P<wmid>\d{1,3})\s*W\s+)?"                         # ...or 7600 10W FLY
    r"(?:(?P<frac>\d\s*/\s*\d)\s+)?"                         # "CLOSED 1/2 FLY $3.80"
    r"(?:FLY|BUTTERFLY)\b\s*"
    rf"(?:(?P<post>{_STRIKES})\s+)?"                         # ...or strikes after
    r"(?:(?P<w2>\d{1,3})\s*W\s+)?"                           # ...or 10W after
    r"\$?(?P<price>\d+(?:\.\d+)?|\.\d+)",
    re.IGNORECASE,
)

# Wing distances the analyst actually trades, same role as ALLOWED_WIDTHS.
ALLOWED_FLY_WINGS: tuple[float, ...] = (5.0, 10.0, 15.0, 20.0, 25.0, 30.0)

_QTY = re.compile(r"\bx\s*(\d+)\b", re.IGNORECASE)
_FRACTION = re.compile(r"\b(\d)\s*/\s*(\d)\s*POS\b", re.IGNORECASE)
_ALL_OUT = re.compile(r"\bALL\s*OUT\b", re.IGNORECASE)


@dataclass
class SpreadAlert:
    """One vertical credit spread or one long butterfly.

    `net_price` is always the number the analyst posted, in his sign
    convention: for a vertical it's the credit received on an OPEN and the
    debit paid on a CLOSE; for a fly it's the reverse — the debit paid to
    open, the credit received to close. `is_credit_structure` is what tells
    the two apart downstream, and nothing on the execution path should infer
    the direction from the action alone.

    For a fly, `short_strike` is the body (the doubled leg) and `long_strike`
    is the lower wing, so `width` keeps meaning the wing distance.
    """

    action:       str                     # OPEN | CLOSE
    kind:         str                     # CCS | PCS | FLY
    symbol:       str
    short_strike: float
    long_strike:  float
    net_price:    Decimal
    expiry:       Optional[date] = None   # None → same-session 0DTE
    qty:          Optional[int] = None
    fraction:     Decimal = Decimal("1")
    close_all:    bool = False
    expired:      bool = False
    raw:          str = ""
    upper_strike: Optional[float] = None  # FLY only — the upper wing
    right:        str = ""                # FLY only — resolved from config
    bare:         bool = False            # CLOSE that named no strikes at all
    stated_wing:  Optional[float] = None  # a bare CLOSE that still said "10W"
    width:        float = field(init=False, default=0.0)

    def __post_init__(self):
        self.width = abs(self.long_strike - self.short_strike)

    @property
    def is_fly(self) -> bool:
        return self.kind == "FLY"

    @property
    def is_credit_structure(self) -> bool:
        """True when opening the position RECEIVES cash. Verticals here are
        always credit spreads; a long fly is always a debit."""
        return not self.is_fly

    @property
    def option_type(self) -> str:
        if self.is_fly:
            return self.right or "P"
        return "C" if self.kind == "CCS" else "P"

    def max_loss_per_contract(self) -> float:
        """What one contract can actually lose, in dollars — the number sizing
        and the trade-cost cap both read.

        For a credit spread that's the width minus the credit, never the
        premium. For a long fly it's the whole debit paid: the structure
        expires worthless anywhere outside the wings, and nothing about it can
        lose more than it cost.
        """
        if self.is_fly:
            return float(self.net_price) * 100
        return (self.width - float(self.net_price)) * 100


def _expand_long_strike(kind: str, short: float, suffix: str) -> Optional[float]:
    """Rebuild the abbreviated long strike.

    The suffix replaces the trailing digits of the short strike, and the leg
    sits above the short strike for a call credit spread, below it for a put
    credit spread. `7500/95` is therefore 7495, not 7595 — resolving the
    direction is the whole job, and the printed digits alone can't do it.
    """
    step = 10 ** len(suffix)
    candidate = (short - (short % step)) + int(suffix)
    # Walk to the nearest candidate on the correct side of the short strike.
    if kind == "CCS":
        while candidate <= short:
            candidate += step
    else:
        while candidate >= short:
            candidate -= step
    return float(candidate) if abs(candidate - short) in ALLOWED_WIDTHS else None


def _fly_strikes(strikes: str, wing: Optional[float]) -> Optional[tuple[float, float, float]]:
    """Resolve a fly's (lower, body, upper) from whatever the line printed.

    Two spellings reach here. All three strikes ("7790/7800/7810") are checked
    for symmetry and, when a width token was also given, against it — a fly
    whose wings aren't equidistant isn't a fly, and a printed triple that
    contradicts the stated width means the message is malformed. The body-only
    spelling ("10W 7800 FLY") has nothing to check against, so it REQUIRES the
    width token; a lone strike with no width is refused rather than assumed.
    """
    parts = [float(p.rstrip("CPcp")) for p in re.split(r"\s*/\s*", strikes)] if strikes else []

    if len(parts) == 3:
        lower, body, upper = sorted(parts)
        if (body - lower) != (upper - body):
            return None                   # not symmetric — not a butterfly
        span = body - lower
        if wing is not None and span != wing:
            return None                   # printed strikes contradict "10W"
    elif len(parts) == 1 and wing is not None:
        body, span = parts[0], wing
        lower, upper = body - span, body + span
    else:
        return None                       # body with no width, or no strikes

    if span not in ALLOWED_FLY_WINGS:
        return None
    return lower, body, upper


def _stated_right(strikes: str) -> str:
    """The right the analyst actually printed on the strikes, or "" when he
    didn't. `7635P` says puts outright, which beats the config default — the
    knob exists only because the right is usually unstated."""
    hit = re.search(r"\d\s*([CP])\b", strikes, re.IGNORECASE)
    return hit.group(1).upper() if hit else ""


def _fly_right() -> str:
    """C or P for the fly's legs when the alert didn't print one — see module
    docstring for why this is a knob and why it defaults to puts."""
    right = (cfg.get("trading", "spread_fly_right", fallback="P") or "P").strip().upper()
    return right if right in ("C", "P") else "P"


def _parse_fly(line: str, symbol: str) -> Optional[SpreadAlert]:
    """Parse one butterfly line, or None when it isn't one / is malformed."""
    if not cfg.getboolean("trading", "spread_flies_enabled", fallback=False):
        return None                       # one-knob revert: flies read as chatter
    hit = _FLY.match(line)
    if not hit:
        return None

    act = hit.group("act").upper()
    wing = hit.group("w1") or hit.group("wmid") or hit.group("w2")
    strikes = hit.group("pre") or hit.group("post") or ""
    resolved = _fly_strikes(strikes, float(wing) if wing else None)
    bare = False
    if resolved is None:
        # "CLOSED 1/2 FLY $3.80" names no strikes at all. Only the open
        # position knows which fly he means, so the strikes stay zero here and
        # spread_executor resolves them — refusing outright would strand an
        # open fly with no way for the analyst to exit it. An OPEN has nothing
        # to resolve against, and a line that DID print strikes was refused by
        # _fly_strikes for a reason, so neither may fall through to this.
        if act == "OPENED" or strikes:
            return None
        bare, (lower, body, upper) = True, (0.0, 0.0, 0.0)
    else:
        lower, body, upper = resolved

    try:
        price = Decimal(hit.group("price"))
    except InvalidOperation:
        return None
    if price <= 0 and act != "EXPIRED":
        return None                       # a fly is bought and sold for money

    expired = act == "EXPIRED"
    frac_hit = _FRACTION.search(line)
    fraction = Decimal("1")
    if frac_hit or hit.group("frac"):
        if frac_hit:
            numerator, denominator = int(frac_hit.group(1)), int(frac_hit.group(2))
        else:                             # the "1/2 FLY" spelling
            numerator, denominator = (int(x) for x in re.split(r"\s*/\s*", hit.group("frac")))
        if denominator == 0 or numerator > denominator:
            return None
        fraction = Decimal(numerator) / Decimal(denominator)

    qty_hit = _QTY.search(line)
    return SpreadAlert(
        action="OPEN" if act == "OPENED" else "CLOSE",
        kind="FLY",
        symbol=(hit.group("sym1") or symbol).upper(),
        short_strike=body,                # the doubled leg
        long_strike=lower,
        upper_strike=upper,
        bare=bare,
        stated_wing=float(wing) if (bare and wing) else None,
        right=_stated_right(strikes) or _fly_right(),
        net_price=price,
        qty=int(qty_hit.group(1)) if qty_hit else None,
        fraction=fraction,
        close_all=bool(_ALL_OUT.search(line)) or expired,
        expired=expired,
        raw=line,
    )


def parse_spread_alert(text: str, *, symbol: str = DEFAULT_SYMBOL) -> Optional[SpreadAlert]:
    """Parse one credit-spread alert, or return None when the line isn't one.

    None covers both "this is chatter" and "this is malformed" on purpose —
    every caller's correct response to either is to not trade and to surface
    the message for a human.
    """
    if not text:
        return None
    line = " ".join(text.split())
    hit = _ACTION.match(line)
    if not hit:
        # The two grammars are disjoint — the vertical requires CC[S]/PC[S], the
        # fly requires the FLY keyword — so order here is cosmetic, not a
        # precedence rule.
        return _parse_fly(line, symbol)

    # He writes the structure either way ("SPX CCS 7595/00" and "SPX CC 7595/00"
    # in the same session, open vs close). Normalize so `right` and
    # `_expand_long_strike` see one spelling — a CC read as anything but CCS
    # flips the option right to P and prices a chain he never traded.
    kind = hit.group("kind").upper()
    if not kind.endswith("S"):
        kind += "S"
    short = float(hit.group("short"))
    long_ = _expand_long_strike(kind, short, hit.group("suffix"))
    if long_ is None:
        return None                       # width outside what he trades

    try:
        price = Decimal(hit.group("price"))
    except InvalidOperation:
        return None

    act = hit.group("act").upper()
    expired = act == "EXPIRED"
    frac_hit = _FRACTION.search(line)
    fraction = Decimal("1")
    if frac_hit:
        numerator, denominator = int(frac_hit.group(1)), int(frac_hit.group(2))
        if denominator == 0 or numerator > denominator:
            return None
        fraction = Decimal(numerator) / Decimal(denominator)

    qty_hit = _QTY.search(line)
    return SpreadAlert(
        action="OPEN" if act == "OPENED" else "CLOSE",
        kind=kind,
        symbol=(hit.group("sym1") or hit.group("sym2") or symbol).upper(),
        short_strike=short,
        long_strike=long_,
        net_price=price,
        qty=int(qty_hit.group(1)) if qty_hit else None,
        fraction=fraction,
        # An expiry-worthless line closes the position as surely as ALL OUT.
        close_all=bool(_ALL_OUT.search(line)) or expired,
        expired=expired,
        raw=line,
    )


def spread_legs(alert: SpreadAlert, session: date, qty: int) -> list[tuple[str, str, int]]:
    """Expand a SpreadAlert into `(osi, tradier_side, quantity)` legs, short leg
    first, ready for tradier_sdk_bridge.place_multileg_order.

    Opening a credit spread **sells** the near leg to open — the first time
    anything in this codebase goes short an option. Closing reverses each leg
    individually: the short is bought back, the long is sold.

    `session` is the expiry, which the alert never carries (always same-day);
    `qty` is the contract count, which is sizing's decision, not the message's.

    A butterfly expands to THREE legs at a 1-2-1 ratio — the body carries
    double — and opens by BUYING the wings rather than selling anything to
    open. The body leg still comes first so `combo_key` stays stable across
    both structures.
    """
    if qty < 1:
        raise ValueError(f"qty must be >= 1, got {qty}")
    if alert.bare:
        # Strikes are zero until spread_executor resolves them against the open
        # position; building OSIs here would name a contract at strike 0.
        raise ValueError("bare close must be resolved against the open position first")
    root = _WEEKLY_ROOTS.get(alert.symbol, alert.symbol)
    right = alert.option_type
    short_osi = _build_osi(root, session, alert.short_strike, right)
    long_osi = _build_osi(root, session, alert.long_strike, right)

    if alert.is_fly:
        if alert.upper_strike is None:
            raise ValueError("fly is missing its upper wing")
        upper_osi = _build_osi(root, session, alert.upper_strike, right)
        if alert.action == "OPEN":
            return [(short_osi, "sell_to_open", qty * 2),
                    (long_osi, "buy_to_open", qty),
                    (upper_osi, "buy_to_open", qty)]
        return [(short_osi, "buy_to_close", qty * 2),
                (long_osi, "sell_to_close", qty),
                (upper_osi, "sell_to_close", qty)]

    if alert.action == "OPEN":
        return [(short_osi, "sell_to_open", qty), (long_osi, "buy_to_open", qty)]
    return [(short_osi, "buy_to_close", qty), (long_osi, "sell_to_close", qty)]
