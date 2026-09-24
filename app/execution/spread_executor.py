"""
spread_executor.py — execution path for vertical credit spreads (Tradier).

WHY THIS IS NOT A PublicExecutor SUBCLASS
─────────────────────────────────────────
Every other executor in this codebase trades one long option: it buys premium
to open, sells it to close, sizes off the premium paid, and wins when the mark
rises. A credit spread inverts all four. It *sells* to open, its risk is the
distance between the strikes rather than anything it paid, and it wins as the
mark falls to zero. PublicExecutor.execute encodes the long-premium assumption
in its sizing, its re-entry guard, its whale caps and its position writes;
threading a second instrument through it would mean a branch in each, on the
money path, for one channel. So the broker seams are shared (place, poll,
cancel all go through tradier_sdk_bridge) and the pipeline is its own.

WHAT IS SHARED, DELIBERATELY
────────────────────────────
`guardrails.check_guardrails` runs for every spread entry. Those gates are
account-level — manual halt, daily loss, daily trade count, open-position cap,
analyst blacklist, market hours — and none of them care what instrument is
underneath. The one that needed care is the trade-cost cap: it has always meant
"dollars this order puts at risk", which for a long option happens to equal the
premium. For a credit spread it doesn't, so this module passes the per-contract
RISK where a long-option path would pass the premium, and the existing cap keeps
meaning exactly what it always meant.

POSITION REPRESENTATION
───────────────────────
One Position row per spread, keyed by a combo `osi_symbol`:

    SPXW260806C07390000|SPXW260806C07395000     (short leg first)

`avg_price` is the credit received, `is_credit` is True, `spread_width` is the
strike distance. That keeps positions/orders/alerts on their existing schemas —
no leg table, no migration beyond two columns — and Position.pnl_dollar()
already inverts on the is_credit flag.

SAFETY MODEL
────────────
Four gates, all default-off, any one of which stops everything:
    [trading] spreads_enabled = false      ← master switch, one-knob revert
    [trading] broker           = tradier   ← multileg is Tradier-only here
    [tradier] orders_enabled   = false     ← the existing Tradier master gate
    [trading] manual_halt                  ← via check_guardrails
`dry_run` books a simulated fill and never reaches the broker, matching the
single-leg path.

NOT BUILT (deliberate — see the channel's own history for why)
──────────────────────────────────────────────────────────────
- No auto-exit. The analyst posts a CLOSE for most trades and goes silent on
  the rest; 4 of the 5 silent ones in the first two months were losers that ran
  to max loss. A stop belongs here eventually (he moved to a 2-2.5x-credit stop
  himself), but auto-firing exits contradicts the relay design and is a
  separate decision.
- No 0DTE expiry sweeper. A spread left open past settlement stays OPEN in the
  DB until reconciled by hand.
- No correction window. He posts typo fixes as separate later messages
  ("*7535/30"); this executes the message it is given.
"""

from __future__ import annotations

import asyncio
import logging
import math
import uuid
from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from app.core.config_manager import cfg
from app.core.db import SessionLocal
from app.core.log_config import order_logger
from app.core.models import Alert, Order, Position
from app.execution import tradier_sdk_bridge as bridge
from app.ingest.spread_parser import SpreadAlert, spread_legs
from app.risk import guardrails

log = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
COMBO_SEP = "|"


# ── Config ────────────────────────────────────────────────────────────────

def _SPREADS_ENABLED() -> bool:
    """Master switch. Off = this module never places anything."""
    return cfg.getboolean("trading", "spreads_enabled", fallback=False)


def _MAX_RISK_DOLLARS() -> float:
    """Ceiling on (width - credit) x 100 x contracts for one spread — what the
    position can actually lose, and the knob that decides how much of the
    account a single 5-wide SPX spread may put at stake.

    NOT the same as the margin Tradier reserves. Verified live 2026-08-10: a
    5-wide credit spread holds the FULL $500 width per contract, so a spread
    risking $425 still ties up $500 of option buying power. The margin side is
    enforced by Tradier's own preview (see place_multileg_order), which runs
    unconditionally on this path; this knob caps loss, not collateral.
    """
    return cfg.getfloat("trading", "spread_max_risk_dollars", fallback=400.0)


def _MAX_CONTRACTS() -> int:
    return cfg.getint("trading", "spread_max_contracts", fallback=1)


def _MIRROR_ANALYST_QTY() -> bool:
    """Take the count he posts ("x2") instead of sizing off the risk budget.

    Off by default: the risk-budget path below is the safer one and stays the
    behavior you get by doing nothing. Set false to revert in one knob.
    """
    return cfg.getboolean("trading", "spread_mirror_analyst_qty", fallback=False)


def _FILL_POLL_INTERVAL() -> float:
    return cfg.getfloat("trading", "fill_poll_interval_seconds", fallback=3)


def _MAX_PENDING_MINUTES() -> int:
    return cfg.getint("trading", "max_pending_minutes", fallback=30)


def _DRY_RUN() -> bool:
    return cfg.getboolean("trading", "dry_run", fallback=False)


def _OWNER_SCOPE_AUTHOR() -> bool:
    """Mirrors the single-leg rule: only the analyst who opened a position may
    close it, so one room's exit can never flatten another's book."""
    return cfg.get("trading", "position_owner_scope", fallback="author").lower() == "author"


# ── Combo key ─────────────────────────────────────────────────────────────

def combo_key(legs: list[tuple[str, str, int]]) -> str:
    """`short|long` OSI pair identifying one spread. Order is significant —
    spread_legs always emits the short leg first, and the reverse pair is a
    different position (it would be a debit spread)."""
    return COMBO_SEP.join(osi for osi, _side, _qty in legs)


def is_combo(osi_symbol: str) -> bool:
    return COMBO_SEP in (osi_symbol or "")


def combo_legs(osi_symbol: str) -> list[str]:
    return (osi_symbol or "").split(COMBO_SEP)


def combo_mark(leg_prices: dict, osi_symbol: str) -> float | None:
    """Net mark for a combo key from per-leg mids.

    Vertical (2 legs, short|long): cost to buy back = short − long.
    Fly (3 legs, body|lower|upper at 2:1:1): net value = lower + upper − 2×body.
    Missing any leg → None so the caller keeps the last accepted mark.
    """
    legs = [l for l in combo_legs(osi_symbol) if l]
    if len(legs) < 2:
        return None
    try:
        px = [leg_prices[l] for l in legs]
    except KeyError:
        return None
    if any(v is None for v in px):
        return None
    if len(legs) == 2:
        return round(float(px[0]) - float(px[1]), 2)
    if len(legs) == 3:
        return round(float(px[1]) + float(px[2]) - 2.0 * float(px[0]), 2)
    return None


class AlertRowView:
    """Duck-typed view of a SpreadAlert with the attribute names
    discord_listener._persist_alert reads.

    Reusing that function rather than writing a second insert keeps spreads
    inside the same message-id uniqueness, cross-channel dedup and author
    resolution as every other alert — the parts that stop a duplicate delivery
    from becoming a duplicate trade.
    """

    __slots__ = ("action", "symbol", "expiry", "strike", "option_type", "price",
                 "size_tag", "fraction", "osi_symbol", "strategy_tag", "qty",
                 "small_account", "spread")

    def __init__(self, spread: SpreadAlert, session: date):
        # A bare close ("CLOSED 1/2 FLY $3.80") has no strikes yet, so it has no
        # combo key to write on the alert row; _close resolves it against the
        # open position and the Order/Position rows carry the real key.
        legs = [] if spread.bare else spread_legs(spread, session, 1)
        self.spread = spread
        # The alerts table speaks BUY/SELL; opening a credit spread is still
        # the entry and closing it is still the exit, whatever the leg sides.
        self.action = "BUY" if spread.action == "OPEN" else "SELL"
        self.symbol = spread.symbol
        self.expiry = session
        self.strike = spread.short_strike
        self.option_type = spread.option_type
        self.price = spread.net_price
        self.size_tag = "SPREAD"
        self.fraction = spread.fraction
        self.osi_symbol = combo_key(legs) if legs else ""
        self.strategy_tag = "SPREAD"
        self.qty = spread.qty
        self.small_account = False


# ── Executor ──────────────────────────────────────────────────────────────

class SpreadExecutor:
    """Places and closes vertical credit spreads. One instance, constructed
    alongside the single-leg executor and handed the same ws_manager."""

    name = "spread"

    def __init__(self, ws_manager):
        self.ws_manager = ws_manager

    # ── Entry point ───────────────────────────────────────────────────────
    async def execute(self, alert: SpreadAlert, alert_db_id: int,
                      alert_author: str = "", content_hash: str = "",
                      session: date | None = None):
        """Route one parsed spread alert. Returns the Position id on a placed
        open, or None for anything blocked, closed, or refused."""
        if not _SPREADS_ENABLED():
            log.info("[SPREAD] spreads_enabled=false — ignoring %s %s",
                     alert.action, alert.raw[:80])
            return None

        broker = cfg.get("trading", "broker", fallback="public").lower()
        if broker != "tradier":
            # The other two brokers have no multileg path here; placing the
            # legs separately would risk a naked short if only one filled.
            self._block(alert_db_id, f"SPREAD_WRONG_BROKER: broker={broker}, spreads need tradier")
            return None
        if not cfg.getboolean("tradier", "orders_enabled", fallback=False):
            self._block(alert_db_id, "SPREAD_TRADIER_DISABLED: [tradier] orders_enabled=false")
            return None

        session = session or datetime.now(ET).date()
        if alert.action == "OPEN":
            return await self._open(alert, alert_db_id, alert_author, content_hash, session)
        return await self._close(alert, alert_db_id, alert_author, session)

    # ── Open ──────────────────────────────────────────────────────────────
    async def _open(self, alert: SpreadAlert, alert_db_id: int,
                    alert_author: str, content_hash: str, session: date):
        risk_per_contract = alert.max_loss_per_contract()
        if risk_per_contract <= 0:
            # Credit >= width means the message is malformed: no vertical pays
            # more than the distance between its strikes.
            self._block(alert_db_id,
                        f"SPREAD_BAD_CREDIT: credit {alert.net_price} >= width {alert.width}")
            return None
        if alert.is_fly and float(alert.net_price) >= alert.width:
            # A long fly cannot be worth more than its wing distance — max
            # value at the body IS the wing. Paying more is a typo, and it
            # would be a guaranteed loss.
            self._block(alert_db_id,
                        f"SPREAD_BAD_DEBIT: debit {alert.net_price} >= wing {alert.width}")
            return None

        qty = self._size(risk_per_contract, alert.qty)
        if qty < 1:
            self._block(alert_db_id,
                        f"SPREAD_TOO_LARGE: one contract risks ${risk_per_contract:.0f}, "
                        f"over the ${_MAX_RISK_DOLLARS():.0f} spread_max_risk_dollars cap")
            return None

        legs = spread_legs(alert, session, qty)
        key = combo_key(legs)
        short_osi = legs[0][0]

        # Reuse the account-level gates. `price` carries per-contract RISK in
        # premium units (dollars/100) so max_trade_cost measures what this
        # order can actually lose — see the module docstring.
        allowed, reason = await asyncio.to_thread(
            guardrails.check_guardrails,
            action="BUY",
            osi_symbol=short_osi,
            price=Decimal(str(round(risk_per_contract / 100, 2))),
            qty=qty,
            alert_author=alert_author,
            content_hash=content_hash,
            alert_db_id=alert_db_id,
            size_tag="SPREAD",
        )
        if not allowed:
            self._block(alert_db_id, f"Guardrail: {reason}")
            return None

        db = SessionLocal()
        try:
            if db.query(Position).filter(Position.osi_symbol == key,
                                         Position.status != "CLOSED").first():
                # He adds to a live spread by posting a second OPEN at the same
                # strikes. Averaging into one row would misstate both the basis
                # and the risk, so an add is refused rather than guessed at.
                self._block(alert_db_id, f"SPREAD_ALREADY_OPEN: {key} is already open (add not supported)")
                return None

            oid = uuid.uuid4().hex[:16]
            db_order = Order(
                public_order_id=None, osi_symbol=key, side="BUY", quantity=qty,
                limit_price=float(alert.net_price), status="PENDING",
                trigger="DISCORD", author=alert_author, strategy_tag="SPREAD",
            )
            db.add(db_order)
            db.commit()
            order_db_id = db_order.id
        finally:
            db.close()

        # A vertical opens for a credit, a fly opens for a debit. Sending the
        # wrong one to Tradier inverts the limit: "pay 1.80" asked as "receive
        # 1.80" is an order that can never fill the way it was posted.
        open_type = "credit" if alert.is_credit_structure else "debit"
        try:
            broker_id = await self._place(legs, float(alert.net_price), open_type, oid)
        except Exception as e:
            log.exception("[SPREAD] open failed %s", key)
            self._fail_order(order_db_id, alert_db_id, f"SPREAD_PLACE_FAILED: {e}")
            return None

        order_logger.info("SPREAD_OPEN | %s | %dx %s %.2f | risk $%.0f | author=%s | broker_id=%s",
                          key, qty, open_type, float(alert.net_price), risk_per_contract * qty,
                          alert_author, broker_id)
        asyncio.create_task(self._poll_open(order_db_id, broker_id, alert, alert_db_id,
                                            key, qty, alert_author))
        return order_db_id

    # ── Close ─────────────────────────────────────────────────────────────
    def _resolve_bare(self, alert: SpreadAlert, alert_db_id: int,
                      alert_author: str) -> SpreadAlert | None:
        """Fill in the strikes of a close that named none ("CLOSED 1/2 FLY $3.80").

        The open fly is the only thing that knows which strikes he means, so
        exactly one open fly must exist — two would make the message ambiguous
        and guessing which to close is how the wrong position gets flattened.
        The row carries the body (`strike`) and the wing (`spread_width`),
        which is everything a fly needs; `is_credit=False` is what tells a fly
        from a vertical, and verticals never reach here because their grammar
        requires the strikes.
        """
        db = SessionLocal()
        try:
            q = db.query(Position).filter(Position.strategy_tag == "SPREAD",
                                          Position.is_credit.is_(False),
                                          Position.status != "CLOSED")
            if _OWNER_SCOPE_AUTHOR() and alert_author:
                q = q.filter(Position.author == alert_author)
            open_flies = q.all()
            candidates = [(p.strike, p.spread_width, p.option_type) for p in open_flies]
        finally:
            db.close()

        if len(candidates) != 1:
            self._block(alert_db_id, f"SPREAD_BARE_CLOSE_AMBIGUOUS: {len(candidates)} open "
                                     f"flies for {alert_author!r}, message names no strikes")
            return None
        body, wing, right = candidates[0]
        if not body or not wing:
            self._block(alert_db_id, "SPREAD_BARE_CLOSE_UNKNOWN: open fly has no body/wing on file")
            return None
        if alert.stated_wing is not None and float(alert.stated_wing) != float(wing):
            self._block(alert_db_id, f"SPREAD_BARE_CLOSE_WING_MISMATCH: message says "
                                     f"{alert.stated_wing:g}W, open fly is {wing:g}W")
            return None
        log.info("[SPREAD] bare close resolved to %g/%g/%g %s from the open position",
                 body - wing, body, body + wing, right)
        return replace(alert, short_strike=float(body), long_strike=float(body) - float(wing),
                       upper_strike=float(body) + float(wing), right=right or alert.right,
                       bare=False)

    async def _close(self, alert: SpreadAlert, alert_db_id: int,
                     alert_author: str, session: date):
        if alert.bare:
            alert = self._resolve_bare(alert, alert_db_id, alert_author)
            if alert is None:
                return None
        legs = spread_legs(alert, session, 1)      # qty replaced below
        key = combo_key(legs)

        db = SessionLocal()
        try:
            q = db.query(Position).filter(Position.osi_symbol == key,
                                          Position.status != "CLOSED")
            if _OWNER_SCOPE_AUTHOR() and alert_author:
                q = q.filter(Position.author == alert_author)
            pos = q.first()
            if pos is None:
                self._block(alert_db_id, f"SPREAD_NO_POSITION: nothing open at {key} for {alert_author!r}")
                return None
            open_qty = pos.remaining or 0
            pos_id, credit = pos.id, pos.avg_price
        finally:
            db.close()

        if open_qty < 1:
            self._block(alert_db_id, f"SPREAD_NOTHING_LEFT: {key} has no remaining contracts")
            return None

        qty = open_qty if alert.close_all else max(1, int(open_qty * float(alert.fraction)))
        qty = min(qty, open_qty)
        legs = spread_legs(alert, session, qty)

        db = SessionLocal()
        try:
            db_order = Order(
                public_order_id=None, osi_symbol=key, side="SELL", quantity=qty,
                limit_price=float(alert.net_price), status="PENDING",
                trigger="DISCORD", author=alert_author, strategy_tag="SPREAD",
                cost_basis=credit,
            )
            db.add(db_order)
            db.commit()
            order_db_id = db_order.id
        finally:
            db.close()

        # An expiry-worthless close costs nothing and has nothing to place:
        # the legs simply stopped existing. Book it and skip the broker.
        if alert.expired and float(alert.net_price) == 0.0:
            await self._book_close(order_db_id, pos_id, alert_db_id, qty, 0.0)
            return order_db_id

        # Tradier has a distinct `even` type for a zero net. Sending a 0.00
        # "debit" asks to pay nothing, which is not the same instruction.
        close_type = ("even" if float(alert.net_price) == 0.0
                      else "debit" if alert.is_credit_structure else "credit")
        try:
            broker_id = await self._place(legs, float(alert.net_price), close_type, uuid.uuid4().hex[:16])
        except Exception as e:
            log.exception("[SPREAD] close failed %s", key)
            self._fail_order(order_db_id, alert_db_id, f"SPREAD_CLOSE_FAILED: {e}")
            return None

        order_logger.info("SPREAD_CLOSE | %s | %dx %s %.2f | author=%s | broker_id=%s",
                          key, qty, close_type, float(alert.net_price), alert_author, broker_id)
        asyncio.create_task(self._poll_close(order_db_id, broker_id, pos_id,
                                             alert_db_id, qty))
        return order_db_id

    async def close_open_position(self, pos, qty: int, net_price: float, trigger: str = "MANUAL"):
        """Dashboard / operator close of a live combo. Does not go through
        TradierExecutor (single-leg) — that TypeError'd on `_force_plain` and
        also cannot buy-to-close a short vertical."""
        osis = combo_legs(pos.osi_symbol)
        if len(osis) < 2:
            raise ValueError(f"not a combo: {pos.osi_symbol}")
        qty = min(qty, max(0, pos.remaining or 0))
        if qty < 1:
            raise ValueError("no remaining contracts")
        if len(osis) == 3:
            legs = [(osis[0], "buy_to_close", qty * 2),
                    (osis[1], "sell_to_close", qty),
                    (osis[2], "sell_to_close", qty)]
        else:
            legs = [(osis[0], "buy_to_close", qty), (osis[1], "sell_to_close", qty)]
        close_type = "debit" if pos.is_credit else "credit"
        db = SessionLocal()
        try:
            db_order = Order(
                public_order_id=None, osi_symbol=pos.osi_symbol, side="SELL",
                quantity=qty, limit_price=float(net_price), status="PENDING",
                trigger=trigger, author=pos.author, strategy_tag="SPREAD",
                cost_basis=pos.avg_price,
            )
            db.add(db_order)
            db.commit()
            order_db_id = db_order.id
            pos_id = pos.id
        finally:
            db.close()
        try:
            broker_id = await self._place(legs, float(net_price), close_type, uuid.uuid4().hex[:16])
        except Exception as e:
            log.exception("[SPREAD] dashboard close failed %s", pos.osi_symbol)
            self._fail_order(order_db_id, None, f"SPREAD_CLOSE_FAILED: {e}")
            raise
        order_logger.info("SPREAD_CLOSE | %s | %dx %s %.2f | trigger=%s | broker_id=%s",
                          pos.osi_symbol, qty, close_type, float(net_price), trigger, broker_id)
        asyncio.create_task(self._poll_close(order_db_id, broker_id, pos_id, None, qty))
        return order_db_id

    # ── Broker IO ─────────────────────────────────────────────────────────
    async def _place(self, legs, net_price: float, order_type: str, oid: str) -> str:
        if _DRY_RUN():
            log.info("[SPREAD DRY] %s %s @ %.2f", order_type, combo_key(legs), net_price)
            return f"dry-{oid}"
        return await bridge.place_multileg_order(
            legs, net_price, order_type=order_type, order_ref=oid,
        )

    async def _await_fill(self, broker_id: str, expected_qty: int) -> tuple[str, float | None]:
        """Poll to a terminal state. Returns (status, net_fill_price)."""
        if broker_id.startswith("dry-"):
            return "FILLED", None
        deadline = datetime.now(timezone.utc).timestamp() + _MAX_PENDING_MINUTES() * 60
        while datetime.now(timezone.utc).timestamp() < deadline:
            order = await bridge.get_order(broker_id)
            status = bridge.translate_status(str(order.get("status") or ""))
            if status == "FILLED":
                return status, bridge.net_fill_price(order)
            if status in ("CANCELLED", "REJECTED", "EXPIRED"):
                return status, None
            await asyncio.sleep(_FILL_POLL_INTERVAL())
        return "PENDING", None

    # ── Fill handling ─────────────────────────────────────────────────────
    async def _poll_open(self, order_db_id: int, broker_id: str, alert: SpreadAlert,
                         alert_db_id: int, key: str, qty: int, author: str):
        status, net = await self._await_fill(broker_id, qty)
        if status != "FILLED":
            self._fail_order(order_db_id, alert_db_id, f"SPREAD_OPEN_{status}")
            return
        # net_fill_price returns a signed per-spread figure — negative when the
        # structure cost money. Both directions are stored as a magnitude and
        # `is_credit` carries the direction, so a sign flip here would book a
        # spread as costing what it in fact received.
        basis = abs(net) if net is not None else float(alert.net_price)

        db = SessionLocal()
        try:
            o = db.query(Order).filter(Order.id == order_db_id).first()
            if o:
                o.public_order_id = broker_id
                o.status = "FILLED"
                o.fill_price = basis
                o.filled_qty = qty
                o.filled_at = datetime.now(timezone.utc)
            pos = Position(
                osi_symbol=key, symbol=alert.symbol,
                expiry=None, strike=alert.short_strike,
                option_type=alert.option_type,
                total_contracts=qty, remaining=qty,
                avg_price=basis, current_price=basis,
                status="OPEN", author=author,
                strategy_tag="SPREAD",
                is_credit=alert.is_credit_structure, spread_width=alert.width,
            )
            db.add(pos)
            self._set_alert(db, alert_db_id, "FILLED")
            db.commit()
            payload = pos.to_dict()
        finally:
            db.close()

        max_loss = (basis if alert.is_fly else alert.width - basis) * qty * 100
        order_logger.info("SPREAD_FILLED | %s | %dx %s %.2f | max loss $%.0f",
                          key, qty, "debit" if alert.is_fly else "credit", basis, max_loss)
        await self._broadcast("position", payload)

    async def _poll_close(self, order_db_id: int, broker_id: str, pos_id: int,
                          alert_db_id: int, qty: int):
        status, net = await self._await_fill(broker_id, qty)
        if status != "FILLED":
            self._fail_order(order_db_id, alert_db_id, f"SPREAD_CLOSE_{status}")
            return
        exit_px = abs(net) if net is not None else None
        await self._book_close(order_db_id, pos_id, alert_db_id, qty, exit_px,
                               broker_id=broker_id)

    async def _book_close(self, order_db_id: int, pos_id: int, alert_db_id: int,
                          qty: int, exit_px: float | None, broker_id: str = ""):
        db = SessionLocal()
        try:
            pos = db.query(Position).filter(Position.id == pos_id).first()
            o = db.query(Order).filter(Order.id == order_db_id).first()
            if exit_px is None:
                exit_px = o.limit_price if o else 0.0
            if o:
                o.public_order_id = broker_id or o.public_order_id
                o.status = "FILLED"
                o.fill_price = exit_px
                o.filled_qty = qty
                o.filled_at = datetime.now(timezone.utc)
            payload = None
            if pos:
                # Both prices are magnitudes, so the direction has to come from
                # the structure: a credit spread profits when it costs less to
                # buy back than it paid, a long fly when it sells for more than
                # it cost. Subtracting the same way for both books every
                # winning fly as a loss of exactly the same size.
                realized = ((pos.avg_price - exit_px) if pos.is_credit
                            else (exit_px - pos.avg_price)) * qty * 100
                pos.remaining = max(0, (pos.remaining or 0) - qty)
                pos.current_price = exit_px
                pos.realized_pnl = (pos.realized_pnl or 0) + realized
                if pos.remaining == 0:
                    pos.status = "CLOSED"
                    pos.close_price = exit_px
                    pos.close_time = datetime.now(timezone.utc)
                else:
                    pos.status = "PARTIAL"
                order_logger.info("SPREAD_CLOSED | %s | %dx @ %.2f | realized $%.0f | %s",
                                  pos.osi_symbol, qty, exit_px, realized, pos.status)
            self._set_alert(db, alert_db_id, "FILLED")
            db.commit()
            if pos:
                payload = pos.to_dict()
        finally:
            db.close()
        if payload:
            await self._broadcast("position", payload)

    # ── Sizing ────────────────────────────────────────────────────────────
    def _size(self, risk_per_contract: float, stated_qty: int | None = None) -> int:
        """Contracts to open.

        Default is the risk budget, not the analyst's stated count: he runs a
        $5k account with a 3-contract ceiling, and mirroring that number
        blindly onto a smaller account would stake more than the account holds.

        With `spread_mirror_analyst_qty` on, an alert that states a count ("x2")
        opens that many instead — but still clamped by both caps. Mirroring
        picks the number; it does not buy the right to exceed
        spread_max_contracts or spread_max_risk_dollars, so a lane that wants
        his size has to raise spread_max_contracts to match (it is 1 today, and
        a mirror against a ceiling of 1 changes nothing).
        """
        if risk_per_contract <= 0:
            return 0
        affordable = math.floor(_MAX_RISK_DOLLARS() / risk_per_contract)
        wanted = stated_qty if (_MIRROR_ANALYST_QTY() and stated_qty and stated_qty > 0) else _MAX_CONTRACTS()
        return max(0, min(_MAX_CONTRACTS(), affordable, wanted))

    # ── DB / broadcast helpers ────────────────────────────────────────────
    def _set_alert(self, db, alert_db_id: int | None, status: str, error: str = ""):
        if not alert_db_id:
            return
        row = db.query(Alert).filter(Alert.id == alert_db_id).first()
        if row:
            row.status = status
            if error:
                row.error_text = error[:500]

    def _block(self, alert_db_id: int | None, reason: str):
        log.warning("[SPREAD] BLOCKED %s", reason)
        order_logger.warning("SPREAD_BLOCKED | %s", reason)
        db = SessionLocal()
        try:
            self._set_alert(db, alert_db_id, "SKIPPED", reason)
            db.commit()
        finally:
            db.close()

    def _fail_order(self, order_db_id: int, alert_db_id: int | None, reason: str):
        log.error("[SPREAD] %s", reason)
        order_logger.error("SPREAD_FAILED | order=%s | %s", order_db_id, reason)
        db = SessionLocal()
        try:
            o = db.query(Order).filter(Order.id == order_db_id).first()
            if o:
                # The reason carries the terminal state the broker reported
                # (SPREAD_CLOSE_CANCELLED, SPREAD_OPEN_EXPIRED, …). Stamping
                # every one of them REJECTED made a day order that simply
                # expired at the 16:00 bell read as a broker rejection on the
                # Orders tab — two different outcomes, one label, and the
                # operator can't tell "never filled" from "refused".
                o.status = next(
                    (t for t in ("CANCELLED", "EXPIRED", "PENDING") if reason.endswith(t)),
                    "REJECTED",
                )
                o.error_text = reason[:500]
            self._set_alert(db, alert_db_id, "ERROR", reason)
            db.commit()
        finally:
            db.close()

    async def _broadcast(self, kind: str, data: dict):
        try:
            await self.ws_manager.broadcast({"type": kind, "data": data})
        except Exception:
            log.debug("[SPREAD] broadcast failed", exc_info=True)
