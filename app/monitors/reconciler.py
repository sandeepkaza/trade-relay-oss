"""
reconciler.py - Broker-as-source-of-truth reconciliation.

Principle:
    Broker (Public.com) = ground truth
    Local SQLite        = working memory / cache
    Dashboard           = view of working memory

This module periodically pulls broker state (positions + orders) and corrects
the local DB to match. It runs:
  1. Once synchronously on startup BEFORE the Discord listener starts
     accepting new alerts (prevents first-alert-after-restart duplicates).
  2. Continuously every `reconcile_interval_seconds` as a background task.

What it corrects:
  - Positions on broker but not in local DB  → insert (includes trades
    placed manually in the Public.com app outside the bot)
  - Positions in local DB but not on broker → mark CLOSED (missed fill
    event, or manually closed on broker)
  - Qty / avg_price divergence              → overwrite with broker values
  - Orders PENDING / PARTIALLY_FILLED locally but terminal on broker
    → update local status (safety net for the per-order poll loop)

What it does NOT do:
  - Run in dry-run mode (no real broker state to reconcile against)
  - Modify broker state (read-only; never places or cancels orders here)
  - Finalize P&L the way a live fill event does (the poll loop handles
    that when it sees the fill first; reconciler only catches drift)
"""

import asyncio
import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from app.core.db import SessionLocal
from app.core.models import Order, Position
from app.core.config_manager import cfg

log = logging.getLogger(__name__)

# Tracks whether at least one reconcile pass has succeeded. Guardrails check
# this flag to block new BUYs until we know local state matches broker.
_reconciled_once: bool = False

# Counts consecutive reconcile passes where a local OPEN/PARTIAL position is
# missing from the broker's portfolio. We only mark the row CLOSED after
# _MIN_CONSECUTIVE_MISSES misses to avoid false-closing during a transient
# broker fetch hiccup or mid-fill window where the broker briefly returns 0.
_missing_position_misses: dict[str, int] = {}
_MIN_CONSECUTIVE_MISSES = 2

# The mirror-image race, which had no guard at all. The broker's portfolio
# snapshot lags our own fills, so a row we just closed can still appear in it.
# 2026-08-04 NVDA 215C 8/5: sold at 14:26:24, and the 14:29:25 pass still saw
# it on the broker and REOPENed the row — then an analyst ALL OUT at 14:32
# sold against a position that no longer existed, booking a fill with no
# broker counterpart. Never resurrect a row closed more recently than this.
def _REOPEN_MIN_CLOSED_AGE_S() -> float:
    return cfg.getfloat("trading", "reconcile_reopen_min_closed_age_s", fallback=300.0)


def has_reconciled() -> bool:
    """Return True once a reconcile pass has completed without a fatal error."""
    return _reconciled_once


async def _shadow_reconcile(broker: str) -> dict:
    """Phase 1 of per-broker reconciliation: REPORT ONLY, never write.

    Until 2026-08-19 this path was a bare `return {"skipped": ...}`, so on any
    broker other than Public there was no boot reconcile, no divergence
    detection, and — because the skip also set _reconciled_once — the
    require_reconcile_before_buy guardrail was waved through on trust. That
    was defensible while IBKR was paper. It stopped being defensible when
    ibkr-oci went live on 2026-08-05.

    This deliberately makes NO corrections. Applying them needs the Public
    path's full order-history machinery per broker, and pointing a
    half-built corrector at a live account is worse than pointing nothing at
    it. What this does give is the answer to "do the broker and the DB
    actually agree?", logged every reconcile_interval_seconds, which is the
    prerequisite for turning corrections on with any confidence.

    Excluded from the diff, on purpose:
      - spreads: a vertical is one local row under a "SHORT|LONG" combo key
        but two legs at the broker; naively diffing would report both legs
        as unknown and the combo as missing.
      - non-options: ibkr-oci currently holds 1 share of TSLA stock, which
        this bot does not manage.
    """
    from app.execution import broker_router
    from app.core.models import Position

    if not broker_router.supports_positions():
        return {"skipped": f"broker={broker} — no fetch_positions seam yet"}

    try:
        broker_rows = await broker_router.fetch_positions()
    except Exception as e:
        log.error("[RECONCILE_SHADOW] %s position fetch failed: %s", broker, e)
        return {"error": str(e), "broker": broker}

    broker_map = {
        (r.get("osi_symbol") or "").replace(" ", ""): r
        for r in broker_rows
        if (r.get("sec_type") or "OPT") == "OPT"
    }

    db = SessionLocal()
    try:
        local = [
            p for p in db.query(Position)
            .filter(Position.status.in_(["OPEN", "PARTIAL"])).all()
            if "|" not in (p.osi_symbol or "")          # skip spread combos
        ]
        local_map = {p.osi_symbol: p for p in local}

        only_broker = sorted(set(broker_map) - set(local_map))
        only_local = sorted(set(local_map) - set(broker_map))
        qty_drift, px_drift = [], []
        for osi in sorted(set(broker_map) & set(local_map)):
            b, l = broker_map[osi], local_map[osi]
            if int(b["qty"]) != int(l.remaining or 0):
                qty_drift.append(f"{osi} broker={int(b['qty'])} local={l.remaining}")
            bc, lc = float(b.get("avg_cost") or 0), float(l.avg_price or 0)
            if bc > 0 and lc > 0 and abs(bc - lc) / lc > 0.01:
                px_drift.append(f"{osi} broker=${bc:.2f} local=${lc:.2f} "
                                f"({abs(bc-lc)/lc*100:.0f}%)")

        stale_orders = await _stale_pending_orders(db, broker)

        stats = {
            "mode": "shadow",
            "broker": broker,
            "broker_positions": len(broker_map),
            "local_positions": len(local_map),
            "only_on_broker": only_broker,
            "only_in_db": only_local,
            "qty_drift": qty_drift,
            "avg_price_drift": px_drift,
            "stale_pending": stale_orders,
        }
        clean = not (only_broker or only_local or qty_drift or px_drift or stale_orders)
        if clean:
            log.info("[RECONCILE_SHADOW] %s clean — %d position(s) agree",
                     broker, len(broker_map))
        else:
            log.warning(
                "[RECONCILE_SHADOW] %s DIVERGENCE — only_on_broker=%s only_in_db=%s "
                "qty_drift=%s avg_price_drift=%s stale_pending=%s "
                "(report only, nothing changed)",
                broker, only_broker or "-", only_local or "-",
                qty_drift or "-", px_drift or "-", stale_orders or "-",
            )
        return stats
    finally:
        db.close()


async def _stale_pending_orders(db, broker: str) -> list[str]:
    """Local orders stuck PENDING that the broker has no record of.

    The executor commits the Order row BEFORE submitting, which is the right
    ordering — it means a crash can never leave a broker order the DB has
    never heard of. The cost is the mirror case: a crash in that window
    leaves a PENDING row for an order that was never placed, and nothing
    sweeps it. On Public the reconciler cleans it up; on every other broker
    it sits there forever.

    Report-only, like the rest of Phase 1. Cancelling is a write on the live
    order path and belongs with the Phase 2 corrector, where "absent from
    the broker" can be established from order history rather than from a
    single open-orders snapshot -- a filled-and-closed order is also absent
    from that snapshot, and cancelling on that basis would erase a real
    fill.
    """
    from datetime import datetime, timezone, timedelta
    from app.core.models import Order
    from app.execution import broker_router

    grace_min = cfg.getint("trading", "stale_pending_minutes", fallback=10)
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=grace_min)
    try:
        rows = (db.query(Order)
                .filter(Order.status.in_(["PENDING", "PARTIALLY_FILLED"]),
                        Order.placed_at < cutoff)
                .all())
    except Exception as e:
        log.warning("[RECONCILE_SHADOW] stale-pending query failed: %s", e)
        return []
    if not rows:
        return []

    try:
        live = await broker_router.fetch_orders()
    except Exception as e:
        # Without the broker's list we cannot tell stale from live. Say
        # nothing rather than cry wolf on every open order.
        log.warning("[RECONCILE_SHADOW] %s order fetch failed, skipping "
                    "stale-pending check: %s", broker, e)
        return []

    live_ids = {str(o.get("id") or o.get("order_id") or "") for o in (live or [])}
    live_ids |= {str(o.get("public_order_id") or "") for o in (live or [])}
    out = []
    for r in rows:
        if str(r.public_order_id or "") in live_ids:
            continue
        age = (datetime.now(timezone.utc) - r.placed_at).total_seconds() / 60
        out.append(f"#{r.id} {r.side} {r.osi_symbol} {age:.0f}m")
    return out


# ── Live config accessors ────────────────────────────────────────────────────

def _ENABLED() -> bool:
    return cfg.getboolean("trading", "reconciler_enabled", fallback=True)

def _INTERVAL() -> int:
    return cfg.getint("trading", "reconcile_interval_seconds", fallback=60)

def _PHANTOM_PURGE() -> bool:
    return cfg.getboolean("trading", "purge_phantom_closed", fallback=False)

def _FILL_LOOKUP() -> bool:
    """Look the real SELL fill up in broker history before closing a position
    the broker no longer has. Off → legacy behaviour (guess from last quote)."""
    return cfg.getboolean("trading", "reconcile_fill_lookup_enabled", fallback=True)

def _BACKFILL_WRITE() -> bool:
    """Startup sweep writes the fills it finds. Off → log only (default),
    same opt-in shape as purge_phantom_closed."""
    return cfg.getboolean("trading", "startup_fill_backfill_write", fallback=False)

def _BACKFILL_DAYS() -> int:
    return cfg.getint("trading", "startup_fill_backfill_days", fallback=7)


# ── OSI parser (for positions discovered broker-side with no local row) ──────

_OSI_RE = re.compile(r"^([A-Z]{1,6})(\d{6})([CP])(\d{8})$")


def _parse_osi(osi: str) -> dict:
    """SPX250414C06970000 → {symbol, expiry, strike, option_type}"""
    m = _OSI_RE.match(osi or "")
    if not m:
        return {}
    sym, date_s, ot, strike_s = m.groups()
    try:
        yy, mm, dd = int(date_s[:2]), int(date_s[2:4]), int(date_s[4:6])
        exp = date(2000 + yy, mm, dd)
    except ValueError:
        exp = None
    return {
        "symbol": sym,
        "expiry": str(exp) if exp else "",
        "strike": int(int(strike_s) / 1000),
        "option_type": ot,
    }


# ── One-shot reconciliation pass ─────────────────────────────────────────────

async def reconcile_once(ws_manager=None) -> dict:
    """
    Single reconciliation pass. Returns counters of changes made.
    Safe to call from startup or a periodic loop.
    """
    global _reconciled_once
    # Reconciler is Public.com-specific (reads Public SDK object shapes).
    # Skip cleanly when another broker is active to avoid polling the wrong
    # account every minute. Phase 4 work: per-broker reconciler.
    #
    # Mark _reconciled_once=True on skip so the BUY guardrail
    # (require_reconcile_before_buy) doesn't permablock when broker=ibkr.
    # User has explicitly switched broker; can't verify local-vs-broker
    # state without a per-broker reconciler, so we accept the implicit
    # trust on the new broker and let BUYs through.
    from app.execution.broker_router import active_broker_name
    if active_broker_name() != "public":
        _reconciled_once = True
        return await _shadow_reconcile(active_broker_name())

    from app.execution.public_sdk_bridge import _get_client, _DRY_RUN, API_KEY

    stats = {
        "positions_created": 0,
        "positions_closed":  0,
        "positions_updated": 0,
        "orders_updated":    0,
        "errors":            [],
    }

    if _DRY_RUN():
        return {"skipped": "dry_run"}
    if not API_KEY or API_KEY == "YOUR_PUBLIC_API_KEY_HERE":
        return {"skipped": "no_api_key"}

    try:
        client = await _get_client()

        # ── 1. Fetch broker state via portfolio (SDK has no get_positions/get_orders) ──
        # Portfolio is a single snapshot containing both .positions and .orders,
        # so one round-trip gives us everything we need for reconciliation.
        try:
            portfolio = await client.get_portfolio()
            broker_positions = portfolio.positions or []
            broker_orders    = portfolio.orders or []
        except Exception as exc:
            log.error("[RECONCILE] get_portfolio failed: %s", exc)
            stats["errors"].append(f"get_portfolio: {exc}")
            broker_positions = []
            broker_orders    = []

        # ── 2. Build broker position map (options only, long positions) ──
        # PortfolioPosition shape (from public_api_sdk.models.portfolio):
        #   pos.instrument.symbol         → OSI string
        #   pos.quantity                  → Decimal
        #   pos.cost_basis.unit_cost      → Decimal (may be None)
        #   pos.last_price.last_price     → Decimal (may be None)
        broker_pos_map: dict[str, dict] = {}
        for bp in broker_positions:
            inst = getattr(bp, "instrument", None)
            if inst is None:
                continue
            osi = getattr(inst, "symbol", None)
            if not osi or not _OSI_RE.match(osi):
                continue  # only options — skip equities/cash/etc.
            try:
                qty = int(float(bp.quantity)) if bp.quantity else 0
            except (TypeError, ValueError):
                qty = 0
            if qty <= 0:
                continue  # skip flat or short positions

            cost_basis = getattr(bp, "cost_basis", None)
            unit_cost  = getattr(cost_basis, "unit_cost", None) if cost_basis else None
            try:
                avg_cost = float(unit_cost) if unit_cost is not None else 0.0
            except (TypeError, ValueError):
                avg_cost = 0.0

            last_price_obj = getattr(bp, "last_price", None)
            last_px = getattr(last_price_obj, "last_price", None) if last_price_obj else None
            try:
                curr_px = float(last_px) if last_px is not None else 0.0
            except (TypeError, ValueError):
                curr_px = 0.0
            broker_pos_map[osi] = {"qty": qty, "avg_cost": avg_cost, "current_price": curr_px}

        # ── 3. Reconcile positions ───────────────────────────────────────
        changed_positions = []
        db = SessionLocal()
        try:
            local_open = db.query(Position).filter(
                Position.status.in_(["OPEN", "PARTIAL"])
            ).all()
            local_map = {p.osi_symbol: p for p in local_open}

            # (a) Broker has, local doesn't → insert (or reopen closed row)
            # Position.osi_symbol is UNIQUE — if a CLOSED row already exists
            # for this OSI (user re-bought a previously-held contract), we
            # must reopen it instead of inserting a fresh row to avoid
            # violating the unique constraint.
            for osi, bp in broker_pos_map.items():
                if osi in local_map:
                    continue
                existing_closed = db.query(Position).filter(
                    Position.osi_symbol == osi
                ).first()
                avg_px  = bp["avg_cost"] or 0.0
                curr_px = bp["current_price"] or avg_px
                if existing_closed is not None:
                    # Guard the stale-snapshot race: if we closed this row
                    # moments ago, the broker snapshot in hand may simply
                    # predate our SELL. Reopening it invents a position we do
                    # not own, and the next analyst exit sells against thin
                    # air. Wait until the snapshot is unambiguously newer.
                    _closed_at = _aware(getattr(existing_closed, "close_time", None))
                    if _closed_at is not None:
                        _age = (datetime.now(timezone.utc) - _closed_at).total_seconds()
                        if 0 <= _age < _REOPEN_MIN_CLOSED_AGE_S():
                            log.warning(
                                "[RECONCILE] SKIP REOPEN %s — closed locally %.0fs ago "
                                "(< %.0fs); broker snapshot is probably stale, not a real position",
                                osi, _age, _REOPEN_MIN_CLOSED_AGE_S(),
                            )
                            stats.setdefault("reopen_skipped_stale", 0)
                            stats["reopen_skipped_stale"] += 1
                            continue
                    # Reopen the old row with broker's current state.
                    # Reset highest_price to curr_px (NOT max with prior cycle's
                    # peak — a stale $10 high from a closed cycle would arm TPT
                    # at $10*0.88=$8.80 and instantly fire on a $2 reopen).
                    # Reset realized_pnl too — prior cycle's P&L doesn't belong
                    # to this new position.
                    existing_closed.total_contracts = bp["qty"]
                    existing_closed.remaining       = bp["qty"]
                    existing_closed.avg_price       = avg_px
                    existing_closed.current_price   = curr_px
                    existing_closed.highest_price   = curr_px
                    existing_closed.status          = "OPEN"
                    existing_closed.close_time      = None
                    existing_closed.close_price     = None
                    existing_closed.pt1_triggered   = False
                    existing_closed.pt2_triggered   = False
                    existing_closed.pt3_triggered   = False
                    existing_closed.sl_triggered    = False
                    existing_closed.tpt_armed       = False
                    if hasattr(existing_closed, "realized_pnl"):
                        existing_closed.realized_pnl = None
                    changed_positions.append(existing_closed)
                    stats["positions_created"] += 1
                    log.warning(
                        "[RECONCILE] REOPEN %s qty=%d @ $%.2f (broker has it, local row was CLOSED)",
                        osi, bp["qty"], avg_px,
                    )
                    continue

                parsed = _parse_osi(osi)
                new_pos = Position(
                    osi_symbol=osi,
                    symbol=parsed.get("symbol", ""),
                    expiry=parsed.get("expiry", ""),
                    strike=parsed.get("strike"),
                    option_type=parsed.get("option_type", ""),
                    total_contracts=bp["qty"],
                    remaining=bp["qty"],
                    avg_price=avg_px,
                    current_price=curr_px,
                    highest_price=curr_px,
                    status="OPEN",
                )
                db.add(new_pos)
                changed_positions.append(new_pos)
                stats["positions_created"] += 1
                log.warning(
                    "[RECONCILE] INSERT %s qty=%d @ $%.2f (broker has it, local didn't)",
                    osi, bp["qty"], avg_px,
                )

            # (b) Local has, broker doesn't → close, but only after N
            # consecutive misses so a transient broker fetch hiccup or a
            # mid-fill window (broker briefly returns 0 qty) doesn't
            # wrongly close a live position. Reset the miss counter for
            # any local OPEN position the broker IS reporting.
            for osi in list(_missing_position_misses.keys()):
                if osi in broker_pos_map or osi not in local_map:
                    _missing_position_misses.pop(osi, None)

            for osi, lp in local_map.items():
                if osi in broker_pos_map:
                    continue
                _missing_position_misses[osi] = _missing_position_misses.get(osi, 0) + 1
                if _missing_position_misses[osi] < _MIN_CONSECUTIVE_MISSES:
                    log.info(
                        "[RECONCILE] %s missing on broker (miss %d/%d) — waiting before closing",
                        osi, _missing_position_misses[osi], _MIN_CONSECUTIVE_MISSES,
                    )
                    continue
                _missing_position_misses.pop(osi, None)

                # The broker dropped this position, so a SELL filled somewhere
                # we didn't see. Ask the transaction history what it actually
                # went off at before falling back to guessing from the last
                # quote — the guess lands in close_price and drives every P&L
                # readout downstream (2026-07-27 QQQ: guessed $2.92, real
                # $2.91, and the realizing SELL kept fill_price=NULL so Orders
                # and Calendar showed nothing at all).
                adopted = None
                if _FILL_LOOKUP():
                    try:
                        fills = await fetch_broker_sell_fills(lookback_days=2)
                        local_qty = _local_sold_qty(db, lp)
                        for f in sorted(fills.get(osi, []), key=lambda x: str(x["ts"])):
                            if f["qty"] <= local_qty:      # already recorded
                                local_qty -= f["qty"]
                                continue
                            adopted = _adopt_sell_fill(db, lp, f)
                            log.info(
                                "[RECONCILE] %s realizing SELL recovered from broker history "
                                "— %d @ $%.2f (order #%s)", osi, f["qty"], f["price"],
                                getattr(adopted, "id", "new"),
                            )
                            break
                    except Exception as exc:
                        log.warning("[RECONCILE] fill lookup failed for %s: %s "
                                    "— falling back to last quote", osi, exc)

                lp.remaining = 0
                lp.status = "CLOSED"
                if adopted is None:
                    lp.close_time = datetime.now(timezone.utc)
                    if not lp.close_price:
                        lp.close_price = lp.current_price or lp.avg_price or 0.0
                changed_positions.append(lp)
                stats["positions_closed"] += 1
                log.warning(
                    "[RECONCILE] CLOSE %s (local had it, broker doesn't for %d consecutive checks — %s)",
                    osi, _MIN_CONSECUTIVE_MISSES,
                    "fill recovered from broker history" if adopted is not None
                    else "missed fill or manual close; close_price estimated from last quote",
                )

            # (c) Both have → correct qty / avg_price drift
            for osi, bp in broker_pos_map.items():
                lp = local_map.get(osi)
                if lp is None:
                    continue
                dirty = False
                if lp.remaining != bp["qty"]:
                    # Skip qty correction if there's a PENDING SELL for this
                    # position. The broker still sees the full qty (the SELL
                    # is in flight, not yet filled), but our local row was
                    # decremented optimistically. Overwriting back to broker's
                    # qty here causes a double-decrement when the SELL fills.
                    pending_sell = db.query(Order).filter(
                        Order.osi_symbol == osi,
                        Order.side == "SELL",
                        Order.status.in_(["PENDING", "PARTIALLY_FILLED"]),
                    ).first()
                    if pending_sell:
                        log.info(
                            "[RECONCILE] QTY DRIFT %s skipped: PENDING SELL order #%d in flight",
                            osi, pending_sell.id,
                        )
                    else:
                        log.warning(
                            "[RECONCILE] QTY DRIFT %s: local=%d → broker=%d (total_contracts %d)",
                            osi, lp.remaining, bp["qty"], lp.total_contracts or 0,
                        )
                        lp.remaining = bp["qty"]
                        if bp["qty"] > (lp.total_contracts or 0):
                            lp.total_contracts = bp["qty"]
                        # Write DOWN total_contracts when local thinks we hold
                        # MORE than the sum of (broker remaining + already-sold).
                        # 2026-05-06 incident: pos #1 TSLA260508C00400000 had
                        # total_contracts=8 (4 stale phantom + 4 real today)
                        # while broker only ever had 4. On force-close, dashboard
                        # reported +$2064 (8 × $2.58) — actual was +$761.
                        # Truth = broker.remaining + sum(prior FILLED SELLs on
                        # this position's open window).
                        try:
                            from app.core.models import Order as _Order
                            # open_time may be naive (legacy rows). Comparison against
                            # tz-aware datetimes will fail, so coerce both sides.
                            since = lp.open_time
                            if since is None:
                                since = datetime.min
                            elif since.tzinfo is None:
                                # Treat naive as UTC (matches how we write timestamps).
                                since = since.replace(tzinfo=timezone.utc)
                            sold = sum(
                                (o.filled_qty or 0)
                                for o in db.query(_Order).filter(
                                    _Order.osi_symbol == osi,
                                    _Order.side == "SELL",
                                    _Order.status == "FILLED",
                                    _Order.filled_at.isnot(None),
                                ).all()
                                if (
                                    o.filled_at and (
                                        o.filled_at if o.filled_at.tzinfo
                                        else o.filled_at.replace(tzinfo=timezone.utc)
                                    ) >= since
                                )
                            )
                            true_total = bp["qty"] + sold
                            if (lp.total_contracts or 0) > true_total and true_total > 0:
                                log.warning(
                                    "[RECONCILE] TOTAL_CONTRACTS WRITE-DOWN %s: %d → %d "
                                    "(broker_remaining=%d + sold_since_open=%d)",
                                    osi, lp.total_contracts, true_total, bp["qty"], sold,
                                )
                                lp.total_contracts = true_total
                        except Exception as _wd_exc:
                            log.error("[RECONCILE] write-down compute failed for %s: %s", osi, _wd_exc)
                        dirty = True
                if bp["avg_cost"] > 0 and lp.avg_price and abs(lp.avg_price - bp["avg_cost"]) / lp.avg_price > 0.01:
                    # Big-drift guard: when broker avg differs from local by
                    # more than 20%, refuse auto-update. A real spurious case
                    # (2026-05-05 TSLA260508C00400000: local=$5.55 → broker
                    # =$7.12) caused inflated SL trip pre-market and -$227
                    # loss on a position that was actually in profit. Prefer
                    # to alert and require manual reconciliation rather than
                    # silently rewrite cost basis.
                    _drift_pct = abs(lp.avg_price - bp["avg_cost"]) / lp.avg_price
                    if _drift_pct > 0.20:
                        log.warning(
                            "[RECONCILE] AVG PRICE BIG DRIFT %s: local=$%.4f → broker=$%.4f "
                            "(%.0f%%) — refusing auto-update; manual review required",
                            osi, lp.avg_price, bp["avg_cost"], _drift_pct * 100,
                        )
                        try:
                            import asyncio as _asyncio
                            import app.analytics.trade_logger as _tl
                            _asyncio.create_task(_tl.log_reconcile_alert(
                                osi=osi,
                                message=(
                                    f"AVG PRICE BIG DRIFT — local ${lp.avg_price:.2f} "
                                    f"vs broker ${bp['avg_cost']:.2f} ({_drift_pct*100:.0f}%). "
                                    f"Auto-update REFUSED. Manual review required to avoid "
                                    f"false-SL on inflated cost basis."
                                ),
                            ))
                        except Exception:
                            pass
                    else:
                        log.warning(
                            "[RECONCILE] AVG PRICE DRIFT %s: local=$%.4f → broker=$%.4f",
                            osi, lp.avg_price, bp["avg_cost"],
                        )
                        lp.avg_price = bp["avg_cost"]
                        dirty = True
                if dirty:
                    changed_positions.append(lp)
                    stats["positions_updated"] += 1

            db.commit()
            for p in changed_positions:
                try:
                    db.refresh(p)
                except Exception:
                    pass
        finally:
            db.close()

        # ── 4. Broadcast position updates ────────────────────────────────
        if ws_manager:
            for p in changed_positions:
                try:
                    await ws_manager.broadcast({"type": "position_update", "data": p.to_dict()})
                except Exception as exc:
                    log.debug("[RECONCILE] ws broadcast failed: %s", exc)

        # ── 5. Reconcile orders (terminal-state safety net) ──────────────
        # Order shape (from public_api_sdk.models.order):
        #   bo.order_id  → str (NOT .id)
        #   bo.status    → OrderStatus enum (str Enum, use .value)
        broker_order_status: dict[str, str] = {}
        for bo in broker_orders:
            oid = getattr(bo, "order_id", None)
            if oid is None:
                continue
            status = getattr(bo, "status", None)
            if status is None:
                continue
            broker_order_status[str(oid)] = str(getattr(status, "value", status)).upper()

        changed_orders = []
        db = SessionLocal()
        try:
            stale = db.query(Order).filter(
                Order.status.in_(["PENDING", "PARTIALLY_FILLED"])
            ).all()
            for o in stale:
                if not o.public_order_id:
                    continue
                bstatus = broker_order_status.get(o.public_order_id)
                if not bstatus:
                    continue
                # Only act on terminal divergence; let _poll_order_fill
                # do the fill-finalization work when it sees FILLED first.
                if "CANCEL" in bstatus and o.status != "CANCELLED":
                    o.status = "CANCELLED"
                    o.error_text = (o.error_text or "") + " [reconciler: broker=CANCELLED]"
                    changed_orders.append(o)
                    stats["orders_updated"] += 1
                    log.warning("[RECONCILE] ORDER %s: local=PENDING → broker=CANCELLED", o.public_order_id)
                elif "REJECT" in bstatus and o.status != "REJECTED":
                    o.status = "REJECTED"
                    o.error_text = (o.error_text or "") + " [reconciler: broker=REJECTED]"
                    changed_orders.append(o)
                    stats["orders_updated"] += 1
                    log.warning("[RECONCILE] ORDER %s: local=PENDING → broker=REJECTED", o.public_order_id)
                elif bstatus == "EXPIRED" and o.status != "EXPIRED":
                    o.status = "EXPIRED"
                    o.error_text = (o.error_text or "") + " [reconciler: broker=EXPIRED]"
                    changed_orders.append(o)
                    stats["orders_updated"] += 1
                    log.warning("[RECONCILE] ORDER %s: local=PENDING → broker=EXPIRED", o.public_order_id)
                elif bstatus == "REPLACED" and o.status != "REPLACED":
                    o.status = "REPLACED"
                    o.error_text = (o.error_text or "") + " [reconciler: broker=REPLACED]"
                    changed_orders.append(o)
                    stats["orders_updated"] += 1
                    log.warning("[RECONCILE] ORDER %s: local=PENDING → broker=REPLACED", o.public_order_id)
                # FILLED intentionally omitted — the per-order poll loop owns
                # fill finalization (it updates Position avg_price, close_price,
                # status, and broadcasts). Racing here would double-count.
            db.commit()
            for o in changed_orders:
                try:
                    db.refresh(o)
                except Exception:
                    pass
        finally:
            db.close()

        if ws_manager:
            for o in changed_orders:
                try:
                    await ws_manager.broadcast({"type": "order_update", "data": o.to_dict()})
                except Exception as exc:
                    log.debug("[RECONCILE] ws broadcast failed: %s", exc)

    except Exception as exc:
        log.error("[RECONCILE] Fatal error in reconcile_once: %s", exc)
        stats["errors"].append(str(exc))
        return stats

    _reconciled_once = True
    return stats


# ── Broker SELL fills (ground truth for realized P&L) ────────────────────────
#
# Public exposes no get_order(oid); portfolio.orders only carries WORKING
# orders, so a fill that lands after we stop polling is invisible there. The
# transaction history DOES carry it, with the executed price:
#
#   description      "SELL 2 QQQ260727C00680000 at 2.91"
#   principal_amount  582.00     quantity -2     fees 0.07
#
# 2026-07-27 QQQ incident: a re-peg cancel raced a fill. The order was stored
# CANCELLED, the position was closed at the last quote ($2.92 guessed vs $2.91
# actual), and the realizing SELL never got a fill price — so Orders showed no
# P&L and the Calendar counted the day $142 light. Price comes from
# principal/(qty*100), not the description string, because the description is
# free text and the amount is the settled number.

_DESC_PX_RE = re.compile(r"\bat\s+([\d.]+)\s*$")


def _fill_price_from_tx(tx) -> Optional[float]:
    """Executed per-contract price for an option transaction, or None."""
    try:
        qty = abs(float(getattr(tx, "quantity", 0) or 0))
        principal = abs(float(getattr(tx, "principal_amount", 0) or 0))
        if qty and principal:
            return round(principal / (qty * 100.0), 4)
    except (TypeError, ValueError):
        pass
    m = _DESC_PX_RE.search(str(getattr(tx, "description", "") or ""))
    return float(m.group(1)) if m else None


async def fetch_broker_sell_fills(lookback_days: int = 7) -> dict:
    """{osi: [{qty, price, ts, principal, fees, id}, ...]} of option SELLs.

    Public-only (uses the same get_history pagination as phantom detection).
    Returns {} on any failure — callers must treat that as "no data", never
    as "no fills happened".
    """
    from app.execution.broker_router import active_broker_name
    if active_broker_name() != "public":
        return {}

    from public_api_sdk.models.history import HistoryRequest
    from app.execution.public_sdk_bridge import _get_client, _DRY_RUN, API_KEY

    if _DRY_RUN() or not API_KEY or API_KEY == "YOUR_PUBLIC_API_KEY_HERE":
        return {}

    out: dict[str, list] = {}
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    try:
        client = await _get_client()
        token: Optional[str] = None
        while True:
            resp = await client.get_history(HistoryRequest(
                start=cutoff, end=datetime.now(timezone.utc),
                page_size=200, next_token=token,
            ))
            for t in resp.transactions or []:
                sec = str(getattr(t.security_type, "value", t.security_type) or "")
                side = str(getattr(t.side, "value", t.side) or "")
                if sec != "OPTION" or side != "SELL" or not t.symbol:
                    continue
                px = _fill_price_from_tx(t)
                if px is None:
                    log.warning("[FILLS] %s SELL tx has no usable price — skipped", t.symbol)
                    continue
                out.setdefault(t.symbol, []).append({
                    "qty": int(abs(float(t.quantity or 0))),
                    "price": px,
                    "ts": t.timestamp,
                    "principal": float(getattr(t, "principal_amount", 0) or 0),
                    "fees": float(getattr(t, "fees", 0) or 0),
                    "id": str(getattr(t, "id", "") or ""),
                })
            if not resp.next_token:
                break
            token = resp.next_token
    except Exception as exc:
        log.error("[FILLS] get_history failed: %s — callers fall back", exc)
        return {}
    return out


def _local_sold_qty(db, pos) -> int:
    """Contracts already recorded as SOLD locally for this position's lifetime."""
    q = db.query(Order).filter(
        Order.osi_symbol == pos.osi_symbol,
        Order.side == "SELL",
        Order.status == "FILLED",
    )
    if pos.open_time:
        q = q.filter(Order.placed_at >= pos.open_time)
    return sum((o.filled_qty or 0) for o in q.all())


def _adopt_sell_fill(db, pos, fill: dict) -> Optional[Order]:
    """Record a broker SELL fill against `pos`.

    Prefers re-using the dead realizing leg (a CANCELLED/REJECTED SELL inside
    the position's lifetime) so the row the operator already sees in Orders
    becomes correct, rather than leaving a lie next to a new truth. Falls back
    to inserting a RECONCILE order. Returns the row written, or None.
    """
    q = db.query(Order).filter(
        Order.osi_symbol == pos.osi_symbol,
        Order.side == "SELL",
        Order.status.in_(["CANCELLED", "REJECTED", "PENDING"]),
    )
    if pos.open_time:
        q = q.filter(Order.placed_at >= pos.open_time)
    order = q.order_by(Order.placed_at.desc()).first()

    if order is None:
        order = Order(
            osi_symbol=pos.osi_symbol, side="SELL", quantity=fill["qty"],
            limit_price=fill["price"], trigger="RECONCILE",
            placed_at=fill["ts"], author=pos.author,
        )
        db.add(order)

    order.status = "FILLED"
    order.fill_price = fill["price"]
    order.filled_qty = fill["qty"]
    order.filled_at = fill["ts"]
    # cost_basis anchors realized P&L in Order.to_dict() — without it the
    # Orders tab and the Calendar both render this trade as blank/$0.
    if order.cost_basis is None:
        order.cost_basis = pos.avg_price
    if not order.author:
        order.author = pos.author
    order.error_text = (
        f"RECONCILE {datetime.now(timezone.utc):%Y-%m-%d}: fill recovered from Public "
        f"transaction history (SELL {fill['qty']} @ {fill['price']}, "
        f"principal {fill['principal']}, tx {fill['id'][:8]})"
    )

    pos.remaining = max(0, (pos.remaining or 0) - fill["qty"])
    pos.current_price = fill["price"]
    if pos.remaining <= 0:
        pos.remaining = 0
        pos.status = "CLOSED"
        pos.close_price = fill["price"]
        pos.close_time = fill["ts"]
    return order


async def backfill_missing_sell_fills(lookback_days: Optional[int] = None) -> dict:
    """Startup sweep: positions whose realizing SELL was never recorded.

    Catches exits that happened while the app was down or after we stopped
    polling — the overnight/after-hours IWM and SPXW case. Log-only unless
    trading.startup_fill_backfill_write = true.
    """
    days = lookback_days if lookback_days is not None else _BACKFILL_DAYS()
    stats: dict = {"scanned": 0, "repaired": [], "wrote": False, "days": days}

    fills = await fetch_broker_sell_fills(days)
    if not fills:
        stats["skipped"] = "no broker fill data"
        return stats

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    db = SessionLocal()
    try:
        rows = db.query(Position).filter(
            Position.osi_symbol.in_(list(fills.keys())),
        ).all()
        # Only positions whose window overlaps the lookback — the same OSI can
        # be re-listed later at the same strike.
        rows = [p for p in rows if not p.close_time or _aware(p.close_time) >= cutoff]
        stats["scanned"] = len(rows)

        for pos in rows:
            broker_qty = sum(f["qty"] for f in fills[pos.osi_symbol])
            local_qty = _local_sold_qty(db, pos)
            missing = broker_qty - local_qty
            if missing <= 0:
                continue
            # Take the fills that aren't accounted for, newest last.
            for fill in sorted(fills[pos.osi_symbol], key=lambda f: str(f["ts"])):
                if missing <= 0:
                    break
                take = min(missing, fill["qty"])
                entry = dict(fill, qty=take)
                stats["repaired"].append({
                    "osi": pos.osi_symbol, "qty": take, "price": fill["price"],
                    "pnl": round((fill["price"] - (pos.avg_price or 0)) * take * 100, 2),
                })
                log.warning(
                    "[FILL_BACKFILL] %s — broker sold %d, local recorded %d. "
                    "Recovering %d @ $%.2f (entry $%.2f, P&L $%+.2f)%s",
                    pos.osi_symbol, broker_qty, local_qty, take, fill["price"],
                    pos.avg_price or 0,
                    (fill["price"] - (pos.avg_price or 0)) * take * 100,
                    "" if _BACKFILL_WRITE() else "  [LOG ONLY — set trading.startup_fill_backfill_write=true]",
                )
                if _BACKFILL_WRITE():
                    _adopt_sell_fill(db, pos, entry)
                missing -= take

        if _BACKFILL_WRITE() and stats["repaired"]:
            db.commit()
            stats["wrote"] = True
    except Exception as exc:
        db.rollback()
        log.exception("[FILL_BACKFILL] failed: %s", exc)
        stats["error"] = str(exc)
    finally:
        db.close()
    return stats


def _aware(dt):
    """Treat naive DB datetimes as UTC (SQLite drops tzinfo on write).

    None-safe: close_time is nullable, and a raised AttributeError here would
    abort the whole reconcile pass rather than skip one row.
    """
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# ── Phantom CLOSED detection ─────────────────────────────────────────────────
#
# A "phantom CLOSED" is a local Position row marked CLOSED with no matching
# SELL transaction on the broker. Root cause: test data, manual DB edits, or
# stale rows that survived a wipe. The reconciler's main loop only touches
# OPEN/PARTIAL rows, so phantoms accumulate silently and pollute P&L stats.
#
# This runs once at startup (opt-in via config). Default: log only; set
# trading.purge_phantom_closed=true to actually delete them.

async def detect_phantom_closed(lookback_days: int = 90) -> dict:
    # Public-SDK-coupled — skip on other brokers.
    from app.execution.broker_router import active_broker_name
    if active_broker_name() != "public":
        return {"skipped": f"broker={active_broker_name()} — phantom detection is Public-only"}

    from public_api_sdk.models.history import HistoryRequest
    from app.execution.public_sdk_bridge import _get_client, _DRY_RUN, API_KEY

    stats = {"scanned": 0, "phantoms": [], "purged": 0}

    if _DRY_RUN():
        return {"skipped": "dry_run"}
    if not API_KEY or API_KEY == "YOUR_PUBLIC_API_KEY_HERE":
        return {"skipped": "no_api_key"}

    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    try:
        client = await _get_client()

        # Collect all broker SELL OSIs in the window (paginated).
        broker_sold_osis: set[str] = set()
        token: Optional[str] = None
        while True:
            req = HistoryRequest(
                start=cutoff,
                end=datetime.now(timezone.utc),
                page_size=200,
                next_token=token,
            )
            resp = await client.get_history(req)
            for t in resp.transactions or []:
                if (t.security_type and t.security_type.value == "OPTION"
                        and t.side and t.side.value == "SELL"
                        and t.symbol):
                    broker_sold_osis.add(t.symbol)
            if not resp.next_token:
                break
            token = resp.next_token
    except Exception as exc:
        log.error("[PHANTOM] get_history failed: %s", exc)
        return {"error": str(exc)}

    db = SessionLocal()
    try:
        closed_rows = db.query(Position).filter(
            Position.status == "CLOSED",
            Position.close_time >= cutoff,
        ).all()
        stats["scanned"] = len(closed_rows)

        phantom_rows = [p for p in closed_rows if p.osi_symbol not in broker_sold_osis]
        stats["phantoms"] = [p.osi_symbol for p in phantom_rows]

        for p in phantom_rows:
            log.warning(
                "[PHANTOM] CLOSED row %s has no broker SELL in last %dd "
                "(qty=%s avg=$%s close=$%s close_time=%s)",
                p.osi_symbol, lookback_days, p.total_contracts, p.avg_price,
                p.close_price, p.close_time,
            )

        if _PHANTOM_PURGE() and phantom_rows:
            for p in phantom_rows:
                # Scope Order delete to this position's lifetime window only,
                # NOT all-time on the OSI. The same OSI can be reused weeks
                # later (option re-listed at same strike); deleting by OSI
                # alone wipes unrelated historical orders. Use placed_at
                # between open_time and close_time.
                order_q = db.query(Order).filter(Order.osi_symbol == p.osi_symbol)
                if p.open_time:
                    order_q = order_q.filter(Order.placed_at >= p.open_time)
                if p.close_time:
                    order_q = order_q.filter(Order.placed_at <= p.close_time)
                order_q.delete(synchronize_session=False)
                db.delete(p)
                stats["purged"] += 1
            db.commit()
            log.warning("[PHANTOM] Purged %d phantom CLOSED rows", stats["purged"])
    finally:
        db.close()

    return stats


# ── Background loop ──────────────────────────────────────────────────────────

async def start_reconciler(ws_manager):
    """Background task: periodic reconciliation every _INTERVAL() seconds."""
    if not _ENABLED():
        log.info("Reconciler disabled via config (reconciler_enabled=false)")
        return

    # Heartbeat from inside the loop so the health monitor doesn't show
    # this component as forever-healthy after a silent death.
    try:
        from app.monitors.health_monitor import heartbeat as _heartbeat
    except Exception:
        async def _heartbeat(_): return

    while True:
        interval = _INTERVAL()
        await asyncio.sleep(max(10, interval))
        try:
            stats = await reconcile_once(ws_manager)
            if any(stats.get(k) for k in (
                "positions_created", "positions_closed",
                "positions_updated", "orders_updated",
            )):
                log.info("[RECONCILE] periodic: %s", stats)
            await _heartbeat("reconciler")
        except Exception as exc:
            log.error("[RECONCILE] periodic loop error: %s", exc)
