"""
tradier_broker.py — Tradier executor as a thin subclass of PublicExecutor.

ARCHITECTURE
────────────
Same pattern as ibkr_broker.py: inherit PublicExecutor's full pipeline
(guardrails, sizing, AI scoring, re-peg loops, partial-fill recovery,
slippage cooldown, DB writes, fill finalization) and override only the
broker-IO seams:

    _place_on_public                 → POST limit order via Tradier REST
    _poll_order_fill                 → GET order status loop (REST)
    cancel_order                     → DELETE order via Tradier REST
    _cancel_external_blocking_orders → Tradier-side external open-order cleanup

Plus execute() — a safety gate on [tradier].orders_enabled.

SAFETY MODEL
────────────
Two-step opt-in to fire real Tradier orders (mirrors IBKR):
    1. cfg[trading].broker = tradier
    2. cfg[tradier].orders_enabled = true
With step 2 OFF, execute() short-circuits. Read-only paths (quotes, account,
position-monitor) stay functional. Sandbox vs live is a third guard
([tradier].sandbox, default true) so even an enabled bot defaults to paper.

V1 SCOPE / BACKLOG
──────────────────
- Whale OCA is NOT natively bracketed here (unlike IBKR). The inherited
  PublicExecutor._maybe_place_whale_tp rests a TP-only SELL limit through
  this class's _place_on_public — the poll-based +15% exit remains the
  backstop. Native OCO bracket = backlog.
- No restart-resume of resting orders (Tradier is stateless REST; no
  connect event). The poll loop self-heals while the process lives; a
  container restart with a live resting order needs manual reconcile = backlog.
- No IBKR-style Adaptive / price-cap (202) retries — Tradier rejects
  differently; a reject books terminal and the SELL re-peg lane recovers.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from app.core.db import SessionLocal
from app.core.models import Alert, Order, Position
from app.core.config_manager import cfg
from app.core.log_config import order_logger
import app.analytics.trade_logger as trade_logger

from app.execution.public_executor import PublicExecutor
try:
    from app.execution.public_executor import _signal_reject
except ImportError:  # pragma: no cover
    def _signal_reject(order_db_id: int) -> None:
        pass

from app.execution import tradier_sdk_bridge as bridge
from app.core.profile_resolver import (
    pcfg_float as _pcfg_float,
    whale_resting_tp_enabled as _whale_resting_tp_enabled,
)

log = logging.getLogger(__name__)


def _ORDERS_ENABLED() -> bool:
    """Master safety gate. Must be true for TradierExecutor to place any order."""
    return cfg.getboolean("tradier", "orders_enabled", fallback=False)


def _DRY_RUN() -> bool:
    return cfg.getboolean("trading", "dry_run", fallback=False)


def _FILL_POLL_INTERVAL() -> int:
    return cfg.getint("trading", "fill_poll_interval_seconds", fallback=3)


def _MAX_PENDING_MINUTES() -> int:
    return cfg.getint("trading", "max_pending_minutes", fallback=30)


def _CANCEL_CONFIRM_TIMEOUT() -> float:
    """Seconds to await a terminal broker state after a cancel, so a fill racing
    the cancel is detected before the re-peg re-places (would oversell). Shared
    config key with Public/IBKR."""
    return cfg.getfloat("trading", "cancel_confirm_timeout_seconds", fallback=3.0)


def _WHALE_OCO_ENABLED() -> bool:
    return cfg.getboolean("tradier", "whale_oco_enabled", fallback=False)


def _WHALE_OCO_SL_PCT(pos) -> float:
    """Downside-stop % for the whale OCO (negative, e.g. -50). >=0 disables the
    stop leg. Profile-tunable [profile:whale], default -50 (mirrors IBKR)."""
    return _pcfg_float(pos, "whale_oca_sl_pct", -50.0)


class TradierExecutor(PublicExecutor):
    """Tradier-flavoured executor. Inherits PublicExecutor's full pipeline,
    overrides only the broker-IO seams."""

    name: str = "tradier"

    def __init__(self, ws_manager):
        super().__init__(ws_manager)
        # our oid (str) → Tradier broker order id (str).
        self._orders: dict[str, str] = {}
        self._resume_lock = asyncio.Lock()
        # Tradier is stateless REST (no connect event) — kick a one-shot resume
        # scan at construction so a container restart with live resting orders
        # (esp. whale OCO) re-attaches watchers / books outcomes instead of
        # orphaning. Best-effort; skipped if no running loop yet.
        try:
            asyncio.get_running_loop().create_task(self.resume_pending_orders())
        except RuntimeError:
            pass

    # ── Safety gate ──────────────────────────────────────────────────
    async def execute(self, alert: Alert, alert_db_id: int, alert_author: str = "",
                      content_hash: str = "", _pipeline_timer=None):
        if not _ORDERS_ENABLED():
            log.warning(
                "[TRADIER] orders_enabled=false — refusing to %s %s. "
                "Toggle [tradier] orders_enabled in config.ini (or portal) to enable.",
                alert.action, alert.osi_symbol,
            )
            order_logger.warning(
                "TRADIER_DISABLED | %s %s | author=%s",
                alert.action, alert.osi_symbol, alert_author,
            )
            return None
        return await super().execute(alert, alert_db_id, alert_author, content_hash, _pipeline_timer)

    # ── SDK no-ops (the four broker seams below should bypass these) ─
    async def _get_client(self):
        raise NotImplementedError(
            "TradierExecutor._get_client called — a Public-SDK code path was not "
            "overridden for Tradier. Check the broker seams in tradier_broker.py."
        )

    def _load_sdk(self):
        class _NotFound(Exception):
            pass
        return {"NotFoundError": _NotFound}

    # ── Seam 1: place order ──────────────────────────────────────────
    async def _place_on_public(self, oid: str, osi_symbol: str, side: str,
                               quantity: int, limit_price, expiration=None,
                               *, _force_plain: bool = False):
        """Drop-in replacement for PublicExecutor._place_on_public. Places a
        DAY limit option order via Tradier REST; stashes the broker order id
        under our oid so _poll_order_fill can look it up.

        `expiration` accepted for signature parity (Public whale resting-TP/GTD
        path) — Tradier orders here are DAY; GTC wiring is a TODO if needed.
        `_force_plain` is IBKR Adaptive-only; ignored here so a parent re-peg
        / manual exit does not TypeError and leave the position unsellable."""
        if _DRY_RUN():
            log.info("[TRADIER DRY] %s %sx %s @ %s (oid=%s)", side, quantity, osi_symbol, limit_price, oid)
            self._orders[oid] = f"dry-{oid}"
            return

        broker_id = await bridge.place_option_order(
            osi_symbol, side, quantity, float(limit_price), order_ref=oid,
        )
        self._orders[oid] = broker_id
        log.info("[TRADIER] %s placed oid=%s %sx %s @ %s (broker_id=%s)",
                 side, oid, quantity, osi_symbol, limit_price, broker_id)

    # ── Seam 2: poll for fill ────────────────────────────────────────
    async def _poll_order_fill(self, order_db_id: int, public_oid: str, alert,
                               alert_db_id, side: str, qty: int,
                               position_id: int | None = None,
                               max_pending_minutes_override: int | None = None):
        """Re-implementation of PublicExecutor._poll_order_fill against the
        Tradier REST order endpoint. Mirrors parent semantics: FILLED
        finalization, PARTIALLY_FILLED intermediate, terminal handling,
        timeout auto-cancel, consecutive-error breakout."""
        TERMINAL = {"FILLED", "CANCELLED", "REJECTED", "EXPIRED"}
        poll = _FILL_POLL_INTERVAL()
        effective_max_min = (max_pending_minutes_override
                             if max_pending_minutes_override is not None
                             else _MAX_PENDING_MINUTES())
        timeout_s = effective_max_min * 60 if effective_max_min > 0 else 0
        elapsed = 0
        not_found_grace = 15
        consecutive_errors = 0
        MAX_CONSECUTIVE_ERRORS = 10

        log.info("[TRADIER] poll start oid=%s (db=%s, timeout=%sm)",
                 public_oid, order_db_id, effective_max_min or "∞")
        order_logger.info("POLL START | order_id=%s oid=%s | side=%s qty=%d | timeout=%sm",
                          order_db_id, public_oid, side, qty, effective_max_min or "∞")

        # Dry-run: parent-style instant fill at the limit so the pipeline completes.
        if _DRY_RUN():
            await self._dry_fill(order_db_id, public_oid, alert, alert_db_id, side, qty, position_id)
            return

        while True:
            await asyncio.sleep(poll)
            elapsed += poll

            # ── Timeout: auto-cancel stale pending ─────────────
            if timeout_s > 0 and elapsed >= timeout_s:
                log.warning("[TRADIER] order %s timed out after %dm — auto-cancelling",
                            public_oid, effective_max_min)
                broker_id = self._orders.get(public_oid)
                if broker_id:
                    await bridge.cancel_order(broker_id)
                db = SessionLocal()
                try:
                    db_order = db.query(Order).filter(Order.id == order_db_id).first()
                    if db_order and db_order.status == "PENDING":
                        db_order.status = "CANCELLED"
                        db_order.error_text = f"Auto-cancelled: no fill after {effective_max_min} minutes"
                        if alert_db_id:
                            self._mark_alert_status(db, alert_db_id, "CANCELLED", db_order.error_text)
                        db.commit()
                        await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
                        asyncio.create_task(trade_logger.log_order_cancelled(
                            osi=db_order.osi_symbol, side=side,
                            reason=db_order.error_text, order_id=db_order.id,
                        ))
                finally:
                    db.close()
                self._orders.pop(public_oid, None)
                return

            broker_id = self._orders.get(public_oid)
            if not broker_id:
                if elapsed > not_found_grace:
                    log.warning("[TRADIER] order %s missing broker id after %ds", public_oid, elapsed)
                continue

            try:
                o = await bridge.get_order(broker_id)
                if not o:
                    consecutive_errors += 1
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                        self._mark_order_error(order_db_id, alert_db_id,
                                               f"Poll failed: {consecutive_errors} empty reads")
                        self._orders.pop(public_oid, None)
                        return
                    continue
                consecutive_errors = 0

                status_str = bridge.translate_status(str(o.get("status") or ""))
                fill_px = float(o.get("avg_fill_price") or 0) or None
                filled_qty_broker = int(float(o.get("exec_quantity") or 0))

                if status_str == "FILLED":
                    db = SessionLocal()
                    try:
                        db_order = db.query(Order).filter(Order.id == order_db_id).first()
                        if not db_order:
                            return
                        filled_qty_actual = filled_qty_broker if filled_qty_broker > 0 else (db_order.filled_qty or qty)
                        log.info("[TRADIER] order %s FILLED @ $%s (qty=%d/%d)",
                                 public_oid, fill_px, filled_qty_actual, qty)
                        order_logger.info("FILLED | order_id=%s | filled=%d/%d | price=%s",
                                          order_db_id, filled_qty_actual, qty, fill_px)
                        if side == "BUY" and alert:
                            await self._finalize_buy_fill(db, db_order, alert, alert_db_id, fill_px, filled_qty_actual)
                        elif side == "SELL" and position_id:
                            pos = db.query(Position).filter(Position.id == position_id).first()
                            if pos:
                                trigger = db_order.trigger or "DISCORD"
                                await self._finalize_sell_fill(db, db_order, pos, fill_px, filled_qty_actual, trigger)
                        else:
                            db_order.fill_price = fill_px
                            db_order.filled_qty = filled_qty_actual
                            db_order.status = "FILLED"
                            db_order.filled_at = datetime.now(timezone.utc)
                            if alert_db_id:
                                self._mark_alert_status(db, alert_db_id, "FILLED")
                            db.commit()
                            await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
                    finally:
                        db.close()
                    self._orders.pop(public_oid, None)
                    return

                elif status_str == "PARTIALLY_FILLED":
                    if filled_qty_broker > 0 and fill_px and side == "BUY" and alert:
                        db = SessionLocal()
                        try:
                            db_order = db.query(Order).filter(Order.id == order_db_id).first()
                            if db_order and db_order.filled_qty != filled_qty_broker:
                                await self._finalize_partial_buy_fill(
                                    db, db_order, alert, alert_db_id, fill_px, filled_qty_broker)
                        finally:
                            db.close()
                    log.info("[TRADIER] order %s PARTIALLY_FILLED: %d/%d @ $%s — still polling",
                             public_oid, filled_qty_broker, qty, fill_px)

                elif status_str in TERMINAL:
                    reason = o.get("reason_description") or None
                    log.warning("[TRADIER] order %s terminal status: %s | reason: %s",
                                public_oid, status_str, reason)
                    _signal_reject(order_db_id)  # wake SELL re-peg watcher (analyst SELL lane)
                    db = SessionLocal()
                    try:
                        db_order = db.query(Order).filter(Order.id == order_db_id).first()
                        if db_order:
                            db_order.status = status_str
                            db_order.error_text = (f"Order {status_str} on Tradier: {reason}"
                                                   if reason else f"Order {status_str} on Tradier")
                            if alert_db_id:
                                self._mark_alert_status(db, alert_db_id, status_str)
                            db.commit()
                            await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
                    finally:
                        db.close()
                    self._orders.pop(public_oid, None)
                    return
                # else: still PENDING → keep polling

            except Exception as exc:
                consecutive_errors += 1
                log.error("[TRADIER] poll error for %s (%d/%d): %s",
                          public_oid, consecutive_errors, MAX_CONSECUTIVE_ERRORS, exc)
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    self._mark_order_error(order_db_id, alert_db_id,
                                           f"Poll failed after {consecutive_errors} errors: {exc}")
                    self._orders.pop(public_oid, None)
                    return

    async def _dry_fill(self, order_db_id, public_oid, alert, alert_db_id, side, qty, position_id):
        """Instant simulated fill at the order's limit for dry-run."""
        db = SessionLocal()
        try:
            db_order = db.query(Order).filter(Order.id == order_db_id).first()
            if not db_order:
                return
            fill_px = float(db_order.limit_price or 0) or None
            if side == "BUY" and alert:
                await self._finalize_buy_fill(db, db_order, alert, alert_db_id, fill_px, qty)
            elif side == "SELL" and position_id:
                pos = db.query(Position).filter(Position.id == position_id).first()
                if pos:
                    await self._finalize_sell_fill(db, db_order, pos, fill_px, qty, db_order.trigger or "DISCORD")
            else:
                db_order.status = "FILLED"
                db_order.fill_price = fill_px
                db_order.filled_qty = qty
                db_order.filled_at = datetime.now(timezone.utc)
                db.commit()
                await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
        finally:
            db.close()
        self._orders.pop(public_oid, None)

    def _mark_order_error(self, order_db_id: int, alert_db_id, reason: str):
        log.error("[TRADIER] order %s ERROR: %s", order_db_id, reason)
        db = SessionLocal()
        try:
            db_order = db.query(Order).filter(Order.id == order_db_id).first()
            if db_order and db_order.status in ("PENDING", "PARTIALLY_FILLED"):
                db_order.status = "ERROR"
                db_order.error_text = reason
                if alert_db_id:
                    self._mark_alert_status(db, alert_db_id, "ERROR", reason)
                db.commit()
                asyncio.create_task(self.ws_manager.broadcast(
                    {"type": "order_update", "data": db_order.to_dict()}))
        finally:
            db.close()

    # ── Whale resting exit: native Tradier OCO bracket (TP limit + SL stop) ──
    async def _maybe_place_whale_tp(self, db, pos: Position):
        """Tradier override of PublicExecutor._maybe_place_whale_tp.

        WHALE-ONLY. On a whale BUY fill, rest a broker-side OCO bracket so the
        position exits at Tradier even if the bot/VM is down:
            leg A  SELL LIMIT @ avg × (1 + pt1_pct/100)   (take-profit)
            leg B  SELL STOP  @ avg × (1 + sl_pct/100)    (stop-loss, sl_pct<0)
        Whichever fills, Tradier auto-cancels the other (OCO). The TP leg is
        recorded trigger='WHALE_TP' so the EXISTING position_monitor gate
        suppresses the poll-based pt1 — no monitor change needed.

        Gated by whale_resting_tp_enabled AND [tradier] whale_oco_enabled.
        When whale_oco_enabled is off (or sl_pct>=0 → no stop leg), defer to the
        parent (Public TP-only resting limit). Failure is non-fatal: the
        poll-based +15% exit remains the backstop."""
        if not _whale_resting_tp_enabled():
            return
        if not _WHALE_OCO_ENABLED():
            return await super()._maybe_place_whale_tp(db, pos)  # inherit TP-only
        if (getattr(pos, "strategy_tag", "") or "") != "WHALE":
            return
        if pos.remaining <= 0 or not pos.avg_price:
            return
        if _DRY_RUN():
            return await super()._maybe_place_whale_tp(db, pos)  # instant dry fill

        sl_pct = _WHALE_OCO_SL_PCT(pos)
        if sl_pct >= 0:
            # No protective stop → OCO degrades to a lone TP; use the parent's
            # simpler single resting limit instead.
            return await super()._maybe_place_whale_tp(db, pos)

        # Don't double-place if a resting WHALE_TP already exists for this OSI.
        existing = db.query(Order).filter(
            Order.osi_symbol == pos.osi_symbol, Order.side == "SELL",
            Order.status == "PENDING", Order.trigger == "WHALE_TP",
        ).first()
        if existing:
            return

        qty = pos.remaining
        avg = Decimal(str(pos.avg_price))
        tp_pct = Decimal(str(_pcfg_float(pos, "pt1_pct", 15.0)))
        tp_px = float((avg * (Decimal(1) + tp_pct / Decimal(100))).quantize(Decimal("0.01")))
        sl_px = float((avg * (Decimal(1) + Decimal(str(sl_pct)) / Decimal(100))).quantize(Decimal("0.01")))

        try:
            await self._place_whale_oco(db, pos, qty, tp_px, sl_px)
        except Exception as e:
            log.error("[TRADIER WHALE_OCO] place failed for %s: %s — poll exit (pt1) backs it up",
                      pos.osi_symbol, e)

    async def _place_whale_oco(self, db, pos: Position, qty: int, tp_px: float, sl_px: float):
        """POST the OCO bracket, record both legs in the DB against the shared
        parent order id, and spawn a watcher that books whichever leg fills."""
        parent_tag = f"WHALEOCO-{pos.id}-{uuid.uuid4().hex[:8]}"
        parent_id = await bridge.place_oco_bracket(pos.osi_symbol, qty, tp_px, sl_px, tag=parent_tag)

        def _mk_row(px: float, trigger: str) -> Order:
            row = Order(
                public_order_id=parent_id,  # both legs share the OCO parent id
                osi_symbol=pos.osi_symbol, side="SELL", quantity=qty,
                limit_price=px, status="PENDING", trigger=trigger,
            )
            db.add(row)
            return row

        tp_row = _mk_row(tp_px, "WHALE_TP")
        sl_row = _mk_row(sl_px, "WHALE_SL")
        db.commit()
        db.refresh(tp_row)
        db.refresh(sl_row)
        self._orders[parent_tag] = parent_id

        log.info("[TRADIER WHALE_OCO] %s x%d — TP %s / SL %s (parent=%s)",
                 pos.osi_symbol, qty, tp_px, sl_px, parent_id)
        order_logger.info("WHALE_OCO | %s x%d | tp=%s sl=%s | parent=%s",
                          pos.osi_symbol, qty, tp_px, sl_px, parent_id)
        for row in (tp_row, sl_row):
            await self.ws_manager.broadcast({"type": "order_update", "data": row.to_dict()})
            asyncio.create_task(trade_logger.log_order_placed(
                side="SELL", osi=pos.osi_symbol, qty=qty,
                limit_price=row.limit_price, trigger=row.trigger, order_id=row.id))
        self._track_task(self._poll_whale_oco(parent_id, tp_row.id, sl_row.id, pos.id, qty))

    async def _poll_whale_oco(self, parent_id: str, tp_row_id: int, sl_row_id: int,
                              pos_id: int, qty: int):
        """Watch a resting OCO parent. GET returns the parent with a `leg` array;
        when a leg fills, book it against the position (_finalize_sell_fill) and
        mark the OCO-cancelled sibling CANCELLED. Rests indefinitely (no timeout)."""
        poll = _FILL_POLL_INTERVAL()
        errors = 0
        while True:
            await asyncio.sleep(poll)
            try:
                parent = await bridge.get_order(parent_id)
                if not parent:
                    errors += 1
                    if errors >= 20:
                        log.warning("[TRADIER WHALE_OCO] parent %s unreadable — giving up watcher", parent_id)
                        return
                    continue
                errors = 0
                legs = bridge._as_list(parent.get("leg")) or [parent]
                filled_leg = next(
                    (l for l in legs
                     if bridge.translate_status(str(l.get("status") or "")) in ("FILLED", "PARTIALLY_FILLED")
                     and float(l.get("avg_fill_price") or 0) > 0),
                    None,
                )
                if not filled_leg:
                    # All terminal without a fill (both cancelled/expired) → clean up.
                    if all(bridge.translate_status(str(l.get("status") or "")) in ("CANCELLED", "REJECTED", "EXPIRED")
                           for l in legs):
                        for rid in (tp_row_id, sl_row_id):
                            self._mark_order_cancelled(rid, "WHALE_OCO cancelled at Tradier (no fill)")
                        return
                    continue

                # A leg filled → book it. Match it to the right DB row by type.
                is_stop = str(filled_leg.get("type") or "").lower().startswith("stop")
                fill_row_id = sl_row_id if is_stop else tp_row_id
                other_row_id = tp_row_id if is_stop else sl_row_id
                fill_px = float(filled_leg.get("avg_fill_price") or 0) or None
                fqty = int(float(filled_leg.get("exec_quantity") or 0)) or qty

                db = SessionLocal()
                try:
                    db_order = db.query(Order).filter(Order.id == fill_row_id).first()
                    pos = db.query(Position).filter(Position.id == pos_id).first()
                    if db_order and pos and db_order.status in ("PENDING", "PARTIALLY_FILLED"):
                        trigger = db_order.trigger or ("WHALE_SL" if is_stop else "WHALE_TP")
                        await self._finalize_sell_fill(db, db_order, pos, fill_px, fqty, trigger)
                        log.info("[TRADIER WHALE_OCO] %s leg filled @ %s x%d (%s)",
                                 "SL" if is_stop else "TP", fill_px, fqty, trigger)
                finally:
                    db.close()
                # Sibling is OCO-cancelled by Tradier — reflect locally.
                self._mark_order_cancelled(other_row_id, "OCO sibling filled — auto-cancelled")
                return
            except Exception as e:
                errors += 1
                log.error("[TRADIER WHALE_OCO] poll error parent=%s: %s", parent_id, e)
                if errors >= 20:
                    return

    def _mark_order_cancelled(self, order_db_id: int, reason: str):
        db = SessionLocal()
        try:
            db_order = db.query(Order).filter(Order.id == order_db_id).first()
            if db_order and db_order.status in ("PENDING", "PARTIALLY_FILLED"):
                db_order.status = "CANCELLED"
                db_order.error_text = reason
                db.commit()
                asyncio.create_task(self.ws_manager.broadcast(
                    {"type": "order_update", "data": db_order.to_dict()}))
        finally:
            db.close()

    # ── Crash/restart recovery ───────────────────────────────────────
    async def resume_pending_orders(self):
        """One-shot on executor construction. For each DB-PENDING Tradier order
        with a broker id, GET its live state: still working → re-attach a
        watcher; filled while we were down → book it; gone → mark cancelled.

        Closes the orphan gap for resting orders (esp. whale OCO) across a
        container restart. Tradier-only; best-effort, never raises."""
        if self._resume_lock.locked():
            return
        async with self._resume_lock:
            if _DRY_RUN() or not bridge._creds_ok():
                return
            db = SessionLocal()
            try:
                rows = [
                    (o.id, o.public_order_id, o.side, o.quantity, o.osi_symbol, o.trigger)
                    for o in db.query(Order).filter(
                        Order.status.in_(["PENDING", "PARTIALLY_FILLED"]),
                        Order.public_order_id.isnot(None),
                    ).all()
                ]
            finally:
                db.close()
            if not rows:
                return
            log.info("[TRADIER RESUME] scanning %d pending order(s)", len(rows))

            reattached = booked = cancelled = 0
            # Group by broker id so an OCO parent (two rows, same id) is handled once.
            seen_parents: set[str] = set()
            for oid_db, broker_id, side, qty, osi, trigger in rows:
                try:
                    if trigger in ("WHALE_TP", "WHALE_SL"):
                        if broker_id in seen_parents:
                            continue
                        seen_parents.add(broker_id)
                        # Re-attach an OCO watcher for the whole bracket.
                        tp_id = sl_id = None
                        db = SessionLocal()
                        try:
                            for r in db.query(Order).filter(
                                Order.public_order_id == broker_id,
                                Order.status.in_(["PENDING", "PARTIALLY_FILLED"]),
                            ).all():
                                if r.trigger == "WHALE_TP":
                                    tp_id = r.id
                                elif r.trigger == "WHALE_SL":
                                    sl_id = r.id
                        finally:
                            db.close()
                        pos_id = self._pos_id_for_osi(osi)
                        if pos_id and tp_id and sl_id:
                            self._orders[broker_id] = broker_id
                            self._track_task(self._poll_whale_oco(broker_id, tp_id, sl_id, pos_id, qty))
                            reattached += 1
                        continue

                    o = await bridge.get_order(broker_id)
                    status = bridge.translate_status(str(o.get("status") or "")) if o else "CANCELLED"
                    if status in ("PENDING", "PARTIALLY_FILLED"):
                        self._orders[str(oid_db)] = broker_id  # re-key by a stable oid
                        # Re-derive our oid for the poll: reuse broker_id as public_oid key.
                        self._orders[broker_id] = broker_id
                        pos_id = self._pos_id_for_osi(osi) if side == "SELL" else None
                        self._track_task(self._poll_order_fill(
                            order_db_id=oid_db, public_oid=broker_id, alert=None,
                            alert_db_id=None, side=side, qty=qty,
                            position_id=pos_id, max_pending_minutes_override=0))
                        reattached += 1
                    elif status == "FILLED":
                        fill_px = float(o.get("avg_fill_price") or 0) or None
                        fqty = int(float(o.get("exec_quantity") or 0)) or qty
                        await self._book_resumed_fill(oid_db, side, osi, fill_px, fqty, trigger)
                        booked += 1
                    else:
                        self._mark_order_cancelled(oid_db, "Resumed: order absent/terminal at Tradier")
                        cancelled += 1
                except Exception as e:
                    log.error("[TRADIER RESUME] order db=%s resume failed: %s", oid_db, e)

            log.info("[TRADIER RESUME] pending=%d reattached=%d booked=%d cancelled=%d",
                     len(rows), reattached, booked, cancelled)
            order_logger.info("TRADIER_RESUME | pending=%d reattached=%d booked=%d cancelled=%d",
                              len(rows), reattached, booked, cancelled)

    def _pos_id_for_osi(self, osi: str) -> int | None:
        db = SessionLocal()
        try:
            pos = db.query(Position).filter(
                Position.osi_symbol == osi,
                Position.status.in_(["OPEN", "PARTIAL"]),
            ).first()
            return pos.id if pos else None
        finally:
            db.close()

    async def _book_resumed_fill(self, order_db_id: int, side: str, osi: str,
                                 fill_px: float, fqty: int, trigger: str | None):
        """Book a fill that completed while disconnected. SELL → finalize against
        the matching position; BUY → mark FILLED and warn (can't rebuild the
        Position without the original alert)."""
        db = SessionLocal()
        try:
            db_order = db.query(Order).filter(Order.id == order_db_id).first()
            if not db_order or db_order.status not in ("PENDING", "PARTIALLY_FILLED"):
                return
            if side == "SELL":
                pos = db.query(Position).filter(
                    Position.osi_symbol == osi, Position.status.in_(["OPEN", "PARTIAL"]),
                ).first()
                if pos:
                    await self._finalize_sell_fill(db, db_order, pos, fill_px, fqty, trigger or "DISCORD")
                    return
            db_order.status = "FILLED"
            db_order.fill_price = fill_px
            db_order.filled_qty = fqty
            db_order.filled_at = datetime.now(timezone.utc)
            db.commit()
            await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
            if side == "BUY":
                log.warning("[TRADIER RESUME] BUY db=%s %s filled @ %s while down — order booked "
                            "but Position not rebuilt (no alert); verify manually",
                            order_db_id, osi, fill_px)
        finally:
            db.close()

    # ── Seam 3: cancel a single order ────────────────────────────────
    async def _await_tradier_order_terminal(self, broker_id: str, timeout: float) -> str:
        """Poll Tradier until the order is terminal or `timeout` elapses. Returns
        "FILLED" | "CANCELLED" | "UNCONFIRMED" (mirrors the Public/IBKR helpers)."""
        if not broker_id or timeout <= 0:
            return "UNCONFIRMED"
        loop = asyncio.get_event_loop()
        end = loop.time() + timeout
        while loop.time() < end:
            try:
                o = await bridge.get_order(broker_id)
                if o:
                    status = bridge.translate_status(str(o.get("status") or ""))
                    exec_qty = float(o.get("exec_quantity") or 0)
                    if status in ("FILLED", "PARTIALLY_FILLED") or exec_qty > 0:
                        return "FILLED"
                    if status in ("CANCELLED", "REJECTED", "EXPIRED"):
                        return "CANCELLED"
                    # else still working — keep polling
            except Exception as e:
                log.warning("[TRADIER] cancel terminal-confirm poll error for %s: %s", broker_id, e)
            await asyncio.sleep(0.4)
        return "UNCONFIRMED"

    async def cancel_order(self, order_db_id: int, *,
                           require_confirmed_terminal: bool = False) -> dict:
        """Tradier cancel. Mirrors PublicExecutor.cancel_order's contract and
        3-way return (filled_during_cancel | cancel_unconfirmed | ok).

        After the DELETE we AWAIT a terminal state. If a fill raced the cancel we
        do NOT mark it CANCELLED (return filled_during_cancel). If we cannot
        confirm terminal and require_confirmed_terminal is set, leave the order
        live and return cancel_unconfirmed so the re-peg holds off — otherwise it
        oversells (parity with the IBKR/Public fix; Tradier's override previously
        had no terminal guard at all)."""
        db = SessionLocal()
        try:
            db_order = db.query(Order).filter(Order.id == order_db_id).first()
            if not db_order:
                raise ValueError(f"Order {order_db_id} not found in database")
            if db_order.status not in ("PENDING", "PARTIALLY_FILLED"):
                raise ValueError(f"Order #{order_db_id} is already {db_order.status} — nothing to cancel")

            note = ""
            if not _DRY_RUN() and db_order.public_order_id:
                broker_id = self._orders.get(db_order.public_order_id)
                if broker_id:
                    ok = await bridge.cancel_order(broker_id)
                    if ok:
                        log.info("[TRADIER] cancel requested oid=%s (broker_id=%s) — awaiting terminal",
                                 db_order.public_order_id, broker_id)
                    else:
                        note = " [Tradier: cancel not confirmed]"
                    outcome = await self._await_tradier_order_terminal(broker_id, _CANCEL_CONFIRM_TIMEOUT())
                    if outcome == "FILLED":
                        log.warning("[TRADIER] oid=%s FILLED during cancel — not cancelling, "
                                    "signalling caller to skip re-place", db_order.public_order_id)
                        return {
                            "status": "filled_during_cancel",
                            "order_id": db_order.id,
                            "osi": db_order.osi_symbol,
                        }
                    if outcome == "UNCONFIRMED" and require_confirmed_terminal:
                        log.warning("[TRADIER] oid=%s cancel UNCONFIRMED — leaving live, "
                                    "signalling caller to hold off re-place", db_order.public_order_id)
                        return {
                            "status": "cancel_unconfirmed",
                            "order_id": db_order.id,
                            "osi": db_order.osi_symbol,
                        }
                else:
                    log.warning("[TRADIER] no broker id for oid=%s — marking CANCELLED locally",
                                db_order.public_order_id)

            # A poll may have booked the fill while we awaited terminal. Re-read
            # and never clobber a FILLED order to CANCELLED.
            db.refresh(db_order)
            if db_order.status in ("FILLED", "PARTIALLY_FILLED") or (db_order.filled_qty or 0) > 0:
                log.warning("[TRADIER] order #%d filled during cancel — not marking CANCELLED", db_order.id)
                return {
                    "status": "filled_during_cancel",
                    "order_id": db_order.id,
                    "osi": db_order.osi_symbol,
                }

            db_order.status = "CANCELLED"
            db_order.error_text = f"Cancelled by user{note}"
            asyncio.create_task(trade_logger.log_order_cancelled(
                osi=db_order.osi_symbol, side=db_order.side,
                reason=f"Cancelled by user{note}", order_id=db_order.id,
            ))
            db.commit()
            db.refresh(db_order)
            await self.ws_manager.broadcast({"type": "order_update", "data": db_order.to_dict()})
            self._orders.pop(db_order.public_order_id, None)
            return {
                "status": "ok",
                "order_id": db_order.id,
                "osi": db_order.osi_symbol,
                "public_note": note or None,
            }
        finally:
            db.close()

    # ── Seam 4: external blocking-order cleanup ──────────────────────
    async def _cancel_external_blocking_orders(self, broker_symbol: str) -> int:
        """Cancel open Tradier orders on `broker_symbol` that this bot did not
        place (no matching tag/oid in our DB). Same use case as the IBKR/Public
        versions: an operator's manual order is blocking an auto-exit."""
        if _DRY_RUN():
            return 0

        db = SessionLocal()
        try:
            our_oids = {
                o.public_order_id for o in db.query(Order).filter(
                    Order.public_order_id.isnot(None)
                ).all() if o.public_order_id
            }
        finally:
            db.close()

        # SPX/SPXW alerts both route to Tradier underlying 'SPX'.
        underlying = "SPX" if broker_symbol in ("SPX", "SPXW") else broker_symbol
        cancelled = 0
        for o in await bridge.list_account_orders():
            try:
                if bridge.translate_status(str(o.get("status") or "")) not in ("PENDING", "PARTIALLY_FILLED"):
                    continue
                if (o.get("symbol") or "") != underlying:
                    continue
                if (o.get("tag") or "") in our_oids:
                    continue  # our own order
                broker_id = str(o.get("id") or "")
                if not broker_id:
                    continue
                log.warning("[TRADIER EXTERNAL_CANCEL] cancelling external open order on %s (id=%s tag=%s)",
                            broker_symbol, broker_id, o.get("tag") or "(none)")
                if await bridge.cancel_order(broker_id):
                    cancelled += 1
                    asyncio.create_task(trade_logger.log_order_cancelled(
                        osi=broker_symbol, side="EXTERNAL",
                        reason=f"Bot cancelled external Tradier order {broker_id} blocking auto-exit",
                    ))
            except Exception as exc:
                log.error("[TRADIER EXTERNAL_CANCEL] failed for id=%s: %s", o.get("id"), exc)
        return cancelled
