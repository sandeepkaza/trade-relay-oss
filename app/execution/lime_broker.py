"""
lime_broker.py — Lime executor as a thin subclass of PublicExecutor.

ARCHITECTURE
────────────
Same pattern as tradier_broker.py / ibkr_broker.py: inherit PublicExecutor's
full pipeline (guardrails, sizing, AI scoring, re-peg loops, partial-fill
recovery, slippage cooldown, DB writes, fill finalization) and override only
the broker-IO seams:

    _place_on_public   → POST /orders/place  (JSON limit order)
    _poll_order_fill   → GET  /orders/{id}   status loop
    cancel_order       → POST /orders/{id}/cancel, with the terminal-confirm guard

Plus execute() — a safety gate on [lime].orders_enabled.

SAFETY MODEL
────────────
Two-step opt-in to fire real Lime orders (mirrors IBKR/Tradier):
    1. cfg[trading].broker = lime
    2. cfg[lime].orders_enabled = true
With step 2 OFF, execute() short-circuits. Read-only paths (quotes, account,
position-monitor) stay functional.

There is NO third sandbox guard here, and that difference is deliberate rather
than an omission: Lime has no demo base URL. Demo and live accounts are served
by the same https://api.lime.co, so there is no host-level flag that could act
as one. `account_number` is the only thing separating paper from real money —
which is exactly why orders_enabled defaults false.

V1 SCOPE / BACKLOG
──────────────────
- No whale OCO bracket, and this one is a hard API limit rather than a
  deferral: Lime's order_type is market|limit ONLY — no stop, no stop-limit,
  no OCO/OTOCO primitive anywhere in the API. The inherited
  PublicExecutor._maybe_place_whale_tp rests a TP-only SELL limit through
  _place_on_public and the poll-based exit remains the only stop. Do not turn
  on anything that assumes a broker-side stop under this broker.
- No restart-resume of resting orders. The poll loop self-heals while the
  process lives; a restart with a live resting order needs manual reconcile.
- Streaming fills: wss://api.lime.co/accounts (orders+trades) on the app
  loop. REST GET is a watchdog if the feed is silent, not the hot path.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from app.core.db import SessionLocal
from app.core.models import Alert, Order, Position
from app.core.config_manager import cfg
from app.core.log_config import order_logger
import app.analytics.trade_logger as trade_logger

from app.execution.public_executor import PublicExecutor, _osi_for_broker
try:
    from app.execution.public_executor import _signal_reject
except ImportError:  # pragma: no cover
    def _signal_reject(order_db_id: int) -> None:
        pass

from app.execution import lime_sdk_bridge as bridge

log = logging.getLogger(__name__)


def _ORDERS_ENABLED() -> bool:
    return cfg.getboolean("lime", "orders_enabled", fallback=False)


def _DRY_RUN() -> bool:
    return cfg.getboolean("trading", "dry_run", fallback=True)


def _BLOCK_BUY_WITHOUT_QUOTE() -> bool:
    """Refuse a BUY when Lime can't quote the contract. Lime's OPRA data needs
    the API token re-activated in the Cabinet every session; until then quotes
    403 and the entry bids blind off the alert price (MSFT 510C 2026-09-23:
    bid $3.10 into a $2.41 mid). false = previous behavior."""
    return cfg.getboolean("lime", "block_buy_without_quote", fallback=True)


def _FILL_POLL_INTERVAL() -> float:
    # Sub-second on purpose: a 2s first-sleep was the whole fill-detect delay.
    try:
        return max(0.05, cfg.getfloat("lime", "fill_poll_interval_seconds", fallback=0.2))
    except (TypeError, ValueError):
        return 0.2


def _MAX_PENDING_MINUTES() -> int:
    return cfg.getint("trading", "max_pending_minutes", fallback=5)


def _CANCEL_CONFIRM_TIMEOUT() -> float:
    return cfg.getfloat("trading", "cancel_confirm_timeout_seconds", fallback=8.0)


class LimeExecutor(PublicExecutor):
    """Lime-flavoured executor. Inherits PublicExecutor's full pipeline,
    overrides only the broker-IO seams."""

    name: str = "lime"

    def __init__(self, ws_manager):
        super().__init__(ws_manager)
        # our oid (str) → Lime broker order id (str).
        self._orders: dict[str, str] = {}
        try:
            asyncio.get_running_loop().create_task(bridge.warm(), name="lime-warm")
        except RuntimeError:
            pass

    # ── Safety gate ──────────────────────────────────────────────────
    async def execute(self, alert: Alert, alert_db_id: int, alert_author: str = "",
                      content_hash: str = "", _pipeline_timer=None):
        if not _ORDERS_ENABLED():
            log.warning(
                "[LIME] orders_enabled=false — refusing to %s %s. "
                "Toggle [lime] orders_enabled in config.ini (or portal) to enable.",
                alert.action, alert.osi_symbol,
            )
            order_logger.warning(
                "LIME_DISABLED | %s %s | author=%s",
                alert.action, alert.osi_symbol, alert_author,
            )
            return None
        # Quote the symbol the order will actually use: super().execute() turns an
        # SPX daily into SPXW, and Lime has no SPX-root daily (404) — quoting the
        # raw alert symbol refused every 0DTE SPX alert (SPX 7750C 2026-09-23).
        quote_osi = _osi_for_broker(alert.osi_symbol) if alert.osi_symbol else alert.osi_symbol
        if (alert.action == "BUY" and _BLOCK_BUY_WITHOUT_QUOTE() and not _DRY_RUN()
                and not (await bridge.fetch_prices([quote_osi])).get(quote_osi)):
            reason = ("LIME NO QUOTE: Lime returned no price for this contract — BUY refused "
                      "rather than bid blind. Re-activate the API token in the Lime Cabinet "
                      "(OPRA needs it every session).")
            log.warning("[LIME] %s %s", reason, alert.osi_symbol)
            order_logger.warning("LIME_NO_QUOTE | BUY %s | author=%s", alert.osi_symbol, alert_author)
            db = SessionLocal()
            try:
                self._mark_alert_status(db, alert_db_id, "SKIPPED", reason)
                db.commit()
            finally:
                db.close()
            asyncio.create_task(trade_logger.log_alert_blocked(
                action="BUY", osi=alert.osi_symbol, reason=reason,
                author=alert_author, block_type="LIME_NO_QUOTE",
            ))
            return None
        return await super().execute(alert, alert_db_id, alert_author, content_hash, _pipeline_timer)

    # ── SDK no-ops (the seams below should bypass these) ─────────────
    async def _get_client(self):
        raise NotImplementedError(
            "LimeExecutor._get_client called — a Public-SDK code path was not "
            "overridden for Lime. Check the broker seams in lime_broker.py."
        )

    def _load_sdk(self):
        class _NotFound(Exception):
            pass
        return {"NotFoundError": _NotFound}

    # ── Seam 1: place order ──────────────────────────────────────────
    async def _place_on_public(self, oid: str, osi_symbol: str, side: str,
                               quantity: int, limit_price, expiration=None,
                               *, _force_plain: bool = False):
        """Place a DAY limit option order via Lime REST; stash the broker order
        id under our oid so _poll_order_fill can look it up.

        `expiration` is accepted for signature parity with the Public whale
        resting-TP/GTD path. Lime's time_in_force has no GTC — day, ext,
        on-open, on-close, ioc, fok — so a GTD request cannot be honoured and
        the order goes DAY rather than silently resting longer than asked.
        """
        if _DRY_RUN():
            log.info("[LIME DRY] %s %sx %s @ %s (oid=%s)", side, quantity, osi_symbol, limit_price, oid)
            self._orders[oid] = f"dry-{oid}"
            return

        broker_id = await bridge.place_option_order(
            osi_symbol, side, quantity, float(limit_price), order_ref=oid,
        )
        self._orders[oid] = broker_id
        log.info("[LIME] %s placed oid=%s %sx %s @ %s (broker_id=%s)",
                 side, oid, quantity, osi_symbol, limit_price, broker_id)

    # ── Seam 2: poll for fill ────────────────────────────────────────
    async def _poll_order_fill(self, order_db_id: int, public_oid: str, alert,
                               alert_db_id, side: str, qty: int,
                               position_id: int | None = None,
                               max_pending_minutes_override: int | None = None):
        """Wait for a fill on the Lime account websocket; REST GET is the
        watchdog if the feed is silent. Same FILLED / partial / terminal /
        timeout-cancel semantics as PublicExecutor._poll_order_fill."""
        import time as _time
        TERMINAL = {"FILLED", "CANCELLED", "REJECTED", "EXPIRED"}
        poll = _FILL_POLL_INTERVAL()
        effective_max_min = (max_pending_minutes_override
                             if max_pending_minutes_override is not None
                             else _MAX_PENDING_MINUTES())
        timeout_s = effective_max_min * 60 if effective_max_min > 0 else 0
        not_found_grace = 15
        consecutive_errors = 0
        MAX_CONSECUTIVE_ERRORS = 10
        t0 = _time.monotonic()
        seen_seq = -1

        log.info("[LIME] fill wait oid=%s (db=%s, timeout=%sm, feed=%s)",
                 public_oid, order_db_id, effective_max_min or "∞",
                 "up" if bridge.feed_ok() else "down")
        order_logger.info("FILL WAIT | order_id=%s oid=%s | side=%s qty=%d | timeout=%sm feed=%s",
                          order_db_id, public_oid, side, qty, effective_max_min or "∞",
                          "up" if bridge.feed_ok() else "down")

        if _DRY_RUN():
            await self._dry_fill(order_db_id, public_oid, alert, alert_db_id, side, qty, position_id)
            return

        while True:
            elapsed = _time.monotonic() - t0
            remaining = (timeout_s - elapsed) if timeout_s > 0 else 3600.0

            # ── Timeout: auto-cancel stale pending ─────────────
            if timeout_s > 0 and elapsed >= timeout_s:
                log.warning("[LIME] order %s timed out after %dm — auto-cancelling",
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
                    log.warning("[LIME] order %s missing broker id after %ds", public_oid, elapsed)
                if bridge.feed_ok():
                    await bridge.wait_book_event(timeout=min(1.0, max(0.05, remaining)))
                else:
                    await asyncio.sleep(poll)
                continue

            try:
                if bridge.feed_ok():
                    o = await bridge.wait_order_update(
                        broker_id, timeout=max(0.05, remaining), after_seq=seen_seq,
                    )
                    if o:
                        seen_seq = bridge.order_seq(broker_id)
                else:
                    await asyncio.sleep(poll)
                    o = await bridge.get_order(broker_id)
                if not o:
                    if bridge.feed_ok():
                        continue
                    consecutive_errors += 1
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                        self._mark_order_error(order_db_id, alert_db_id,
                                               f"Poll failed: {consecutive_errors} empty reads")
                        self._orders.pop(public_oid, None)
                        return
                    continue
                consecutive_errors = 0

                status_str = bridge.translate_status(str(o.get("order_status") or ""))
                fill_px = bridge.net_fill_price(o)
                filled_qty_broker = int(float(o.get("executed_quantity") or 0))

                if status_str == "FILLED":
                    db = SessionLocal()
                    try:
                        db_order = db.query(Order).filter(Order.id == order_db_id).first()
                        if not db_order:
                            return
                        filled_qty_actual = filled_qty_broker if filled_qty_broker > 0 else (db_order.filled_qty or qty)
                        log.info("[LIME] order %s FILLED @ $%s (qty=%d/%d)",
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
                    log.info("[LIME] order %s PARTIALLY_FILLED: %d/%d @ $%s — still polling",
                             public_oid, filled_qty_broker, qty, fill_px)

                elif status_str in TERMINAL:
                    reason = o.get("reason") or o.get("validation_message") or None
                    log.warning("[LIME] order %s terminal status: %s | reason: %s",
                                public_oid, status_str, reason)
                    _signal_reject(order_db_id)  # wake SELL re-peg watcher (analyst SELL lane)
                    db = SessionLocal()
                    try:
                        db_order = db.query(Order).filter(Order.id == order_db_id).first()
                        if db_order:
                            db_order.status = status_str
                            db_order.error_text = (f"Order {status_str} on Lime: {reason}"
                                                   if reason else f"Order {status_str} on Lime")
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
                log.error("[LIME] poll error for %s (%d/%d): %s",
                          public_oid, consecutive_errors, MAX_CONSECUTIVE_ERRORS, exc)
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    self._mark_order_error(order_db_id, alert_db_id,
                                           f"Poll failed after {consecutive_errors} errors: {exc}")
                    self._orders.pop(public_oid, None)
                    return

    async def _dry_fill(self, order_db_id, public_oid, alert, alert_db_id, side, qty, position_id):
        """Instant simulated fill at the order's limit for dry-run.

        These rows are SIMULATED and land in the same tables as real ones —
        dry_run is a paper simulator, not a stop switch. To halt trading use
        manual_halt.
        """
        db = SessionLocal()
        try:
            db_order = db.query(Order).filter(Order.id == order_db_id).first()
            if not db_order:
                return
            fill_px = float(db_order.limit_price or 0)
            if side == "BUY" and alert:
                await self._finalize_buy_fill(db, db_order, alert, alert_db_id, fill_px, qty)
            elif side == "SELL" and position_id:
                pos = db.query(Position).filter(Position.id == position_id).first()
                if pos:
                    await self._finalize_sell_fill(db, db_order, pos, fill_px, qty,
                                                   db_order.trigger or "DISCORD")
            else:
                db_order.status = "FILLED"
                db_order.fill_price = fill_px
                db_order.filled_qty = qty
                db_order.filled_at = datetime.now(timezone.utc)
                db.commit()
            log.info("[LIME DRY] simulated fill oid=%s @ %s", public_oid, fill_px)
        finally:
            db.close()
        self._orders.pop(public_oid, None)

    def _mark_order_error(self, order_db_id: int, alert_db_id, reason: str):
        db = SessionLocal()
        try:
            db_order = db.query(Order).filter(Order.id == order_db_id).first()
            if db_order and db_order.status == "PENDING":
                db_order.status = "ERROR"
                db_order.error_text = reason[:500]
                if alert_db_id:
                    self._mark_alert_status(db, alert_db_id, "ERROR", reason[:500])
                db.commit()
        finally:
            db.close()

    # ── Seam 3: cancel, with the terminal-confirm guard ──────────────
    async def _await_lime_order_terminal(self, broker_id: str, timeout: float) -> str:
        """Poll Lime until the order is terminal or `timeout` elapses. Returns
        "FILLED" | "CANCELLED" | "UNCONFIRMED" (mirrors the Public/IBKR/Tradier
        helpers)."""
        if not broker_id or timeout <= 0:
            return "UNCONFIRMED"
        loop = asyncio.get_event_loop()
        end = loop.time() + timeout
        while loop.time() < end:
            try:
                o = bridge.latest_order_snapshot(broker_id) or await bridge.get_order(broker_id)
                if o:
                    status = bridge.translate_status(str(o.get("order_status") or ""))
                    exec_qty = float(o.get("executed_quantity") or 0)
                    if status in ("FILLED", "PARTIALLY_FILLED") or exec_qty > 0:
                        return "FILLED"
                    if status in ("CANCELLED", "REJECTED", "EXPIRED"):
                        return "CANCELLED"
            except Exception as e:
                log.warning("[LIME] cancel terminal-confirm poll error for %s: %s", broker_id, e)
            remaining = end - loop.time()
            if remaining <= 0:
                break
            await bridge.wait_order_update(broker_id, timeout=min(0.5, remaining))
        return "UNCONFIRMED"

    async def cancel_order(self, order_db_id: int, *,
                           require_confirmed_terminal: bool = False) -> dict:
        """Lime cancel. Mirrors PublicExecutor.cancel_order's contract and 3-way
        return (filled_during_cancel | cancel_unconfirmed | ok).

        The terminal-confirm await is not boilerplate: a fill that races a
        cancel and is then re-placed is the SPXW double-sell. Every broker on
        this path gates the same way, so a SELL re-peg cannot oversell here
        either.
        """
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
                        log.info("[LIME] cancel requested oid=%s (broker_id=%s) — awaiting terminal",
                                 db_order.public_order_id, broker_id)
                    else:
                        note = " [Lime: cancel not confirmed]"
                    outcome = await self._await_lime_order_terminal(broker_id, _CANCEL_CONFIRM_TIMEOUT())
                    if outcome == "FILLED":
                        log.warning("[LIME] oid=%s FILLED during cancel — not cancelling, "
                                    "signalling caller to skip re-place", db_order.public_order_id)
                        return {
                            "status": "filled_during_cancel",
                            "order_id": db_order.id,
                            "osi": db_order.osi_symbol,
                        }
                    if outcome == "UNCONFIRMED" and require_confirmed_terminal:
                        log.warning("[LIME] oid=%s cancel UNCONFIRMED — leaving live, "
                                    "signalling caller to hold off re-place", db_order.public_order_id)
                        return {
                            "status": "cancel_unconfirmed",
                            "order_id": db_order.id,
                            "osi": db_order.osi_symbol,
                        }
                else:
                    log.warning("[LIME] no broker id for oid=%s — marking CANCELLED locally",
                                db_order.public_order_id)

            # A poll may have booked the fill while we awaited terminal. Re-read
            # and never clobber a FILLED order to CANCELLED.
            db.refresh(db_order)
            if db_order.status in ("FILLED", "PARTIALLY_FILLED") or (db_order.filled_qty or 0) > 0:
                log.warning("[LIME] order #%d filled during cancel — not marking CANCELLED", db_order.id)
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
