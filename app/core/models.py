"""
models.py - SQLAlchemy ORM models.
"""

from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, Text

from app.core.db import Base


def _iso_utc(dt):
    """Serialize a naive-UTC or aware datetime as ISO8601 with Z suffix.
    DB columns are naive but written as UTC via datetime.now(timezone.utc);
    adding the Z ensures JS `new Date(...)` parses as UTC, not local."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.isoformat() + "Z"
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class Alert(Base):
    __tablename__ = "alerts"

    id          = Column(Integer, primary_key=True, index=True)
    timestamp   = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    author      = Column(String)
    action      = Column(String)          # BUY | SELL
    raw_text    = Column(String)
    osi_symbol  = Column(String)
    symbol      = Column(String)
    expiry      = Column(String)
    strike      = Column(Float)           # float — half-dollar strikes (e.g. 272.5) must survive
    option_type = Column(String)          # C | P
    alert_price = Column(Float)
    size_tag    = Column(String)
    fraction    = Column(Float)
    status      = Column(String, default="PENDING")  # PENDING | FILLED | ERROR | IGNORED | HISTORICAL
    channel_id  = Column(Integer, nullable=True)
    channel_name = Column(String, nullable=True)
    # UNIQUE so the same Discord message can never produce two alert rows even
    # under a concurrent/duplicate delivery (GH #8). NULLs stay distinct in
    # SQLite, so embeds without an id are unaffected. Enforced on fresh DBs via
    # create_all; existing DBs get the index defensively in db._ensure_unique_message_id_index.
    discord_message_id = Column(String, nullable=True, unique=True, index=True)
    is_historical = Column(Boolean, default=False)
    error_text = Column(String, nullable=True)
    content_hash = Column(String, nullable=True, index=True)  # cross-channel dedup hash
    strategy_tag = Column(String, nullable=True)  # e.g., "momentum", "reversal", "breakout"
    # Contract count stated in the message ("2 on small account"). Persisted so
    # a resubmit can size like the original alert — the override path rebuilds
    # from this row alone, and without it every resubmit silently fell back to
    # size_default (2026-08-04: an INTC 2-lot resubmitted as 1).
    qty = Column(Integer, nullable=True)

    def to_dict(self):
        return {
            "id":           self.id,
            "timestamp":    _iso_utc(self.timestamp),
            "author":       self.author,
            "action":       self.action,
            "raw":          self.raw_text,
            "osiSymbol":    self.osi_symbol,
            "symbol":       self.symbol,
            "expiry":       self.expiry,
            "strike":       self.strike,
            "optionType":   self.option_type,
            "price":        self.alert_price,
            "sizeTag":      self.size_tag,
            "fraction":     self.fraction,
            "status":       self.status,
            "channelId":    self.channel_id,
            "channel":      self.channel_name,
            "discordMessageId": self.discord_message_id,
            "isHistorical": self.is_historical,
            "error":        self.error_text,
            "contentHash":  self.content_hash,
            "strategyTag":  self.strategy_tag,
            "qty":          self.qty,
        }


class Position(Base):
    __tablename__ = "positions"

    id            = Column(Integer, primary_key=True, index=True)
    osi_symbol    = Column(String, unique=True, index=True)
    symbol        = Column(String)
    expiry        = Column(String)
    strike        = Column(Float)
    option_type   = Column(String)
    total_contracts = Column(Integer)
    remaining       = Column(Integer)
    avg_price       = Column(Float)
    current_price   = Column(Float)
    close_price     = Column(Float, nullable=True)   # actual exit price (set when CLOSED)
    status          = Column(String, default="OPEN")   # OPEN | PARTIAL | CLOSED
    open_time       = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    close_time      = Column(DateTime, nullable=True)

    # Auto-exit flags
    pt1_triggered   = Column(Boolean, default=False)
    pt2_triggered   = Column(Boolean, default=False)
    pt3_triggered   = Column(Boolean, default=False)
    sl_triggered    = Column(Boolean, default=False)
    sl_partial_triggered = Column(Boolean, default=False)
    last_manual_sell_at  = Column(DateTime, nullable=True)

    # Trailing Profit Target (TPT)
    # Armed when position reaches tpt_arm_pct% gain (default 100%).
    # Once armed, the position is NOT hard-closed — instead the bot
    # watches the high-water mark and exits if price drops tpt_trail_pct%
    # below the peak. This lets winners run past 100% while still auto-exiting
    # on reversals.
    tpt_armed       = Column(Boolean, default=False)

    # High-water mark: highest price seen since entry (used by trailing SL + TPT)
    highest_price   = Column(Float, nullable=True)
    lowest_price    = Column(Float, nullable=True)  # low-water mark -> MAE


    # Per-position PT/TPT disable. When True: PT1/PT2/PT3/TPT skipped for this
    # position; only SL (and manual exits) can close it. Lets a runner ride
    # without auto-trimming.
    pts_disabled    = Column(Boolean, default=False)

    # Per-position breakeven-lock disable. When True: skip the +20%-peak →
    # -1% SL floor logic for this position. Lets a wide-SL swing (e.g.
    # Sarang) keep its full -50% room even after touching +20% on a wick.
    breakeven_lock_disabled = Column(Boolean, default=False)

    # Per-position trailing stop, armed by hand from the dashboard AFTER
    # entry. Opt-in (default False) — unlike the *_disabled flags above,
    # which are opt-outs. When True the monitor trails this position even
    # when [trading] auto_exit_enabled = false, so a pure-relay book can
    # still protect one specific runner without turning on the PT/SL ladder.
    trail_enabled   = Column(Boolean, default=False)
    # ── Per-position exit overrides. NULL = follow the resolved default
    # (profile → [trading]) and keep following it if that default later
    # changes; a value here detaches this one position only. Set from the
    # sliders on the position row; the reset control writes NULL back.
    # Deliberately unclamped — risk knobs are portal-managed and code must not
    # impose floors on them (see CLAUDE.md).
    pt_pct_override = Column(Float, nullable=True)
    # How much of the position the target sells: 0.5 = half, 1.0 = a hard exit
    # that closes the whole thing. Its own override because "take profit at
    # +30%" and "get me all the way out at +30%" are different instructions.
    pt_sell_override = Column(Float, nullable=True)
    sl_pct_override = Column(Float, nullable=True)
    auto_exit_override = Column(Boolean, nullable=True)

    # Strategy tracking for performance analytics
    strategy_tag    = Column(String, nullable=True)  # e.g., "momentum", "reversal"
    realized_pnl    = Column(Float, nullable=True)  # Actual realized P&L when position closed

    # Discord author who triggered this BUY (e.g. "Sarang", "Twinsight Bot").
    # Used by position_monitor to apply per-author exit profile (Sarang =
    # swing/runner with wider SL; Twinsight = scalper with tight PT cascade).
    author          = Column(String, nullable=True, index=True)

    # Pre-placed resting profit-take SELL orders. broker oid + intended limit
    # price + intended qty stored so reconciler/restart can match what the
    # broker is showing. NULL = not pre-placed (legacy reactive path).
    pt1_resting_oid   = Column(String, nullable=True, index=True)
    pt1_resting_price = Column(Float,  nullable=True)
    pt1_resting_qty   = Column(Integer, nullable=True)
    pt2_resting_oid   = Column(String, nullable=True, index=True)
    pt2_resting_price = Column(Float,  nullable=True)
    pt2_resting_qty   = Column(Integer, nullable=True)
    # Pre-placed STOP for SL (only if broker confirms options STOP support).
    # Reserved here so Stage 3+ can light up without another migration.
    sl_resting_oid    = Column(String, nullable=True, index=True)
    sl_resting_price  = Column(Float,  nullable=True)
    sl_resting_qty    = Column(Integer, nullable=True)

    # Vertical credit spread (osi_symbol is a `short|long` combo key). The
    # position was opened for a CREDIT, so every price relation below inverts:
    # avg_price is money received, the mark is what it costs to buy back, and
    # the trade wins as that mark falls toward zero. NULL/False = the ordinary
    # long-premium position every other row is.
    is_credit       = Column(Boolean, default=False, nullable=True)
    # Distance between the legs, in strike points. Max loss is (width - credit),
    # which is the real risk — not avg_price, which is what came in.
    spread_width    = Column(Float, nullable=True)

    def _credit(self) -> bool:
        """True for a credit-spread row. Column first; two-leg combo OSI is
        the fallback so a NULL is_credit on a `short|long` key still inverts."""
        return bool(self.is_credit) or is_credit_combo_osi(self.osi_symbol)

    def max_loss_dollar(self) -> float:
        """Worst case in dollars. For a long option that's the premium paid;
        for a credit spread it's the width minus the credit, which is a
        different and much larger number than anything else on this row."""
        contracts = self.remaining or self.total_contracts or 0
        if self._credit():
            return ((self.spread_width or 0) - (self.avg_price or 0)) * contracts * 100
        return (self.avg_price or 0) * contracts * 100

    def pnl_pct(self) -> float:
        """Return P&L % using close_price for CLOSED, current_price for open."""
        if not self.avg_price or self.avg_price == 0:
            return 0.0
        if self._credit():
            # A closed credit spread's exit price is very often exactly 0.0 —
            # that IS the maximum win, so the CLOSED branch has to test for
            # None, not for truthiness. (2026-08-05: a falsy 0.0 fill_price
            # hid $5,703 of P&L on the single-leg side for the same reason.)
            price = (self.close_price if (self.status == "CLOSED" and self.close_price is not None)
                     else self.current_price)
        else:
            price = self.close_price if (self.status == "CLOSED" and self.close_price) else self.current_price
        if price is None:
            return 0.0
        if self._credit():
            # Credit spreads are measured against what was risked to earn the
            # credit, not against the credit itself: buying back at half the
            # credit is not "+50%", it's half the maximum win. A price of 0
            # (expired worthless) is the full win, so this must not be gated
            # on `price` being truthy the way the long-premium branch is.
            risk = (self.spread_width or 0) - self.avg_price
            if risk <= 0:
                return 0.0
            return ((self.avg_price - price) / risk) * 100
        if not price:
            return 0.0
        return ((price / self.avg_price) - 1) * 100

    def pnl_dollar(self) -> float:
        """Return realized P&L $ for CLOSED positions, unrealized for open/partial."""
        if not self.avg_price:
            return 0.0
        if self._credit():
            # Credit received minus the debit to close, so a mark BELOW the
            # entry is profit. Expiring worthless (0.0) is the full credit —
            # a falsy close_price here is a real outcome, not a missing one.
            if self.status == "CLOSED" and self.close_price is not None:
                return (self.avg_price - self.close_price) * (self.total_contracts or 0) * 100
            if self.current_price is None:
                return 0.0
            return (self.avg_price - self.current_price) * (self.remaining or 0) * 100
        if self.status == "CLOSED" and self.close_price:
            # Realized: use actual exit price × all contracts sold
            return (self.close_price - self.avg_price) * (self.total_contracts or 0) * 100
        # Unrealized: current mark × remaining contracts
        if not self.current_price:
            return 0.0
        return (self.current_price - self.avg_price) * (self.remaining or 0) * 100

    def exit_plan(self):
        """What will actually happen to THIS position, resolved through its profile.

        The dashboard used to read pt1_pct / sl_pct straight out of [trading],
        which is wrong for any position that resolves to a profile — a 4-day
        MRNA lands on [profile:multi_day] and stops at -25%, while the row
        claimed -20%. It also drew PT2/PT3 as live after they were switched off,
        and said nothing at all when auto_exit_enabled was false, which is the
        one fact that determines whether any of it fires.

        Mirrors position_monitor's gates: the PT rungs are chained (PT2 needs
        PT1 to have fired, PT3 needs PT2), the trail needs PT1 plus a peak past
        its arm, and the breakeven / floor locks only ever tighten the stop.
        """
        try:
            from app.core.profile_resolver import _sections_for, pcfg_bool, pcfg_float, xd
        except Exception:
            return None

        try:
            F = lambda k, d: pcfg_float(self, k, d)          # noqa: E731
            B = lambda k, d: pcfg_bool(self, k, d)           # noqa: E731

            auto = (bool(self.auto_exit_override) if self.auto_exit_override is not None
                    else B("auto_exit_enabled", False))
            pts_off = bool(self.pts_disabled) or B("pts_disabled_default", False)
            avg = self.avg_price or 0.0
            peak = self.highest_price or avg
            peak_pct = ((peak / avg) - 1) * 100 if avg else 0.0

            sl_pct = (self.sl_pct_override if self.sl_pct_override is not None
                      else F("sl_pct", xd("sl_pct")))
            sl_on = B("sl_enabled", xd("sl_enabled"))
            base_sl = sl_pct

            # Locks only ratchet upward, and only the ones actually enabled.
            locks = []
            if not self.breakeven_lock_disabled and B("breakeven_lock_enabled", xd("breakeven_lock_enabled")):
                be_arm, be_floor = F("breakeven_lock_arm_pct", xd("breakeven_lock_arm_pct")), F("breakeven_lock_floor_pct", xd("breakeven_lock_floor_pct"))
                if peak_pct >= be_arm:
                    if be_floor > sl_pct:
                        sl_pct = be_floor
                        locks.append(f"breakeven lock (peak passed +{be_arm:.0f}%)")
                elif sl_on:
                    locks.append(f"breakeven lock arms at +{be_arm:.0f}% → floor +{be_floor:.0f}%")
            pt1_pct = (self.pt_pct_override if self.pt_pct_override is not None
                       else F("pt1_pct", xd("pt1_pct")))
            pt1_sell_def = F("pt1_sell", xd("pt1_sell"))
            pt1_sell = (self.pt_sell_override if self.pt_sell_override is not None
                        else pt1_sell_def)
            pt2_pct, pt3_pct = F("pt2_pct", xd("pt2_pct")), F("pt3_pct", xd("pt3_pct"))
            if B("pt2_floor_lock_enabled", xd("pt2_floor_lock_enabled")) and peak_pct >= pt2_pct:
                v = F("pt2_floor_lock_pct", pt1_pct)
                if v > sl_pct:
                    sl_pct, _ = v, locks.append(f"PT2 floor lock (peak passed +{pt2_pct:.0f}%)")
            if B("pt3_floor_lock_enabled", xd("pt3_floor_lock_enabled")) and peak_pct >= pt3_pct:
                v = F("pt3_floor_lock_pct", pt2_pct)
                if v > sl_pct:
                    sl_pct, _ = v, locks.append(f"PT3 floor lock (peak passed +{pt3_pct:.0f}%)")

            cur = self.current_price or avg
            def _move_to(level_pct):
                """% the price must move FROM HERE to reach a level set off entry.

                The row kept quoting levels off the entry price, which says
                nothing once a position has moved: a stop at +5% on a trade
                sitting at -59.5% is not a stop, it is 159% away and can never
                fire. This is the number that answers "so what happens now".
                """
                if not avg or not cur:
                    return None
                target = avg * (1 + level_pct / 100.0)
                return round((target / cur - 1) * 100, 1)

            def _rung(key, pct, sell_label, enabled, fired, blocked_by):
                if pts_off:
                    state = "off"
                elif fired:
                    state = "done"
                elif not enabled:
                    state = "off"
                elif blocked_by:
                    state = "unreachable"
                else:
                    state = "armed"
                return {"key": key, "pct": round(pct, 1), "sell": sell_label,
                        "state": state, "blockedBy": blocked_by,
                        "movePct": _move_to(pct)}

            pt1_en, pt2_en, pt3_en = B("pt1_enabled", xd("pt1_enabled")), B("pt2_enabled", xd("pt2_enabled")), B("pt3_enabled", xd("pt3_enabled"))
            # Chained: a rung whose predecessor can never fire can never fire.
            pt2_blocked = None if (pt1_en and not pts_off) else "PT1"
            pt3_blocked = "PT2" if (pt2_blocked or not pt2_en) else None
            rungs = [
                _rung("PT1", pt1_pct,
                      "the whole position" if pt1_sell >= 1 else f"{pt1_sell * 100:.0f}% of position",
                      pt1_en, self.pt1_triggered, None),
                _rung("PT2", pt2_pct, f"{F('pt2_sell', xd('pt2_sell')) * 100:.0f}% of the rest",
                      pt2_en, self.pt2_triggered, pt2_blocked),
                _rung("PT3", pt3_pct, "the remainder",
                      pt3_en, self.pt3_triggered, pt3_blocked),
            ]

            # Two different trails share these thresholds and nothing else.
            # The CONFIG trail lives inside the auto-exit gate and waits for
            # PT1. The MANUAL one — the row's "Arm trail" button — runs OUTSIDE
            # that gate in position_monitor and does not wait for PT1, because
            # pressing the button IS the arm signal. Reading only the config
            # flag here is what made the row keep saying nothing could fire on
            # a position whose trail was armed by hand.
            manual_trail = bool(self.trail_enabled)
            cfg_trail = B("trailing_sl_enabled", xd("trailing_sl_enabled"))
            trail_en = manual_trail or cfg_trail
            trail_arm, trail_pct = F("trailing_sl_arm_pct", xd("trailing_sl_arm_pct")), F("trailing_sl_pct", xd("trailing_sl_pct"))
            trail_armed = bool(
                (manual_trail or (cfg_trail and self.pt1_triggered and auto))
                and peak_pct >= trail_arm)
            # The trail exits off the PEAK, not off entry, so it needs its own
            # distance-from-here rather than _move_to's entry-relative one.
            trail_level = peak * (1 - trail_pct / 100.0) if peak else None
            trail_move = (round((trail_level / cur - 1) * 100, 1)
                          if (trail_level and cur) else None)
            # What can fire with auto-exit OFF: only the hand-armed trail.
            live_manual = manual_trail and not self.sl_triggered

            # ── The verdict: what happens to THIS position, from where it is now.
            # Everything below used to be a list of levels, which is useless once
            # a trade has moved — a "+5% stop" on a position sitting at -59.5% is
            # 159% away and will never fire. Rank the live rules by how far the
            # price actually has to travel and lead with the nearest one.
            live = [r for r in rungs if r["state"] == "armed"]
            reach = [(r["movePct"], f"{r['key']} at +{r['pct']:.0f}% (sells {r['sell']})")
                     for r in live if r["movePct"] is not None]
            if sl_on:
                stop_label = (f"the stop at {'+' if sl_pct >= 0 else ''}{sl_pct:.0f}%"
                              f" (sells everything)")
                m = _move_to(sl_pct)
                if m is not None:
                    reach.append((m, stop_label))
            if trail_armed and trail_move is not None:
                reach.append((trail_move,
                              f"the trailing stop {trail_pct:.0f}% off the ${peak:.2f} peak "
                              f"(sells everything)"))
            reach.sort(key=lambda t: abs(t[0]))
            nearest = reach[0] if reach else None

            lines = []
            if not auto and not live_manual:
                verdict = ("Nothing here can fire — auto-exit is OFF for this position. "
                           "It exits only when the analyst posts a SELL, or you press Exit.")
            elif not auto:
                # Auto-exit off but the trail was armed from the row. That one
                # runs outside the gate, so it is the ONLY thing that can fire —
                # the nearest PT or stop must not lead the sentence here.
                if trail_armed and trail_move is not None:
                    d = "rise" if trail_move > 0 else "fall"
                    verdict = (f"Next: the trailing stop, {trail_pct:.0f}% off the "
                               f"${peak:.2f} peak (sells everything). Price must {d} "
                               f"{abs(trail_move):.0f}% from here to get there. "
                               f"Nothing else fires — auto-exit is OFF.")
                else:
                    verdict = (f"Auto-exit is OFF; the only thing armed is the trailing stop "
                               f"you set by hand, and it starts following once the peak "
                               f"passes +{trail_arm:.0f}% (peak is +{peak_pct:.0f}%).")
            elif not reach:
                verdict = ("Nothing can fire — there is no stop and no profit target active "
                           "on this position.")
            else:
                mv, what = nearest
                direction = "rise" if mv > 0 else "fall"
                verdict = (f"Next: {what}. Price must {direction} "
                           f"{abs(mv):.0f}% from here to get there.")
                if mv > 100:
                    verdict += " That is effectively out of reach."
            lines.append(verdict)

            if not sl_on:
                lines.append("There is NO stop loss on this position — nothing limits the loss.")
            elif sl_pct >= 0:
                msg = (f"The stop has ratcheted ABOVE break-even to +{sl_pct:.0f}%: it would "
                       f"sell at a profit, not at a loss, and still log as SL. "
                       f"Cause: {', '.join(locks)}.")
                m = _move_to(sl_pct)
                if m is not None and m > 0:
                    msg += (f" It sits {m:.0f}% above the current price"
                            f"{', so it is out of reach' if m > 100 else ''}.")
                lines.append(msg)
            elif not auto:
                m = _move_to(sl_pct)
                lines.append(
                    f"If auto-exit were on, the stop would sell everything at {sl_pct:.0f}%"
                    + (f" — a {abs(m):.0f}% {'fall' if m < 0 else 'rise'} from here." if m is not None else "."))
            for note in locks:
                if sl_pct < 0:
                    lines.append(note[0].upper() + note[1:] + ".")
            if pts_off:
                lines.append("Profit targets are switched off for this position.")
            for r in rungs:
                if r["state"] == "unreachable":
                    lines.append(f"{r['key']} can never fire: {r['blockedBy']} is switched off "
                                 f"and the rungs are chained.")
            if trail_en:
                if trail_armed:
                    where = (f" — exit sits at ${trail_level:.2f}" if trail_level else "")
                    lines.append(f"Trailing stop is ACTIVE, following the peak by "
                                 f"{trail_pct:.0f}%{where}. It sells everything.")
                elif manual_trail:
                    # Armed by hand: no PT1 needed, and it ignores auto-exit.
                    lines.append(f"Trailing stop is armed by hand and starts following once the "
                                 f"peak passes +{trail_arm:.0f}% (peak is +{peak_pct:.0f}%). It "
                                 f"does not need PT1 and it fires with auto-exit off.")
                else:
                    lines.append(f"Trailing stop is idle; it arms once PT1 has fired and the "
                                 f"peak passes +{trail_arm:.0f}%.")
            secs = [s.removeprefix("profile:") for s in _sections_for(self)]
            return {
                "autoExit": auto,
                "profile": secs[0] if secs else "trading",
                "ptsDisabled": pts_off,
                "rungs": rungs,
                "stop": {"pct": round(sl_pct, 1), "basePct": round(base_sl, 1),
                         "enabled": sl_on, "locks": locks,
                         "ratcheted": sl_pct > base_sl,
                         "movePct": _move_to(sl_pct) if sl_on else None},
                "trail": {"enabled": trail_en, "armPct": round(trail_arm, 1),
                          "pct": round(trail_pct, 1), "armed": trail_armed,
                          # True when the row's button armed it, not config.
                          "manual": manual_trail,
                          "level": round(trail_level, 2) if trail_level else None,
                          "movePct": trail_move},
                # Resolved, so the row's tooltips stop quoting stale defaults.
                "breakeven": {
                    "enabled": (not self.breakeven_lock_disabled)
                               and B("breakeven_lock_enabled", xd("breakeven_lock_enabled")),
                    "armPct": round(F("breakeven_lock_arm_pct", xd("breakeven_lock_arm_pct")), 1),
                    "floorPct": round(F("breakeven_lock_floor_pct", xd("breakeven_lock_floor_pct")), 1),
                    "locked": any("breakeven" in n for n in locks),
                },
                "peakPct": round(peak_pct, 1),
                # The two numbers the row lets you edit, each with the default
                # it would fall back to. `isOverride` drives the "reset to
                # default" control; when false the position keeps tracking the
                # config value as it changes.
                "editable": {
                    "target": {
                        "pct": round(pt1_pct, 1),
                        "defaultPct": round(pcfg_float(self, "pt1_pct", xd("pt1_pct")), 1),
                        "isOverride": self.pt_pct_override is not None,
                        "sells": "everything" if pt1_sell >= 1 else f"{pt1_sell * 100:.0f}%",
                        # The slider row's HALF | ALL control. Same NULL rule.
                        "sellFrac": round(pt1_sell, 2),
                        "defaultSellFrac": round(pt1_sell_def, 2),
                        "sellIsOverride": self.pt_sell_override is not None,
                    },
                    "stop": {
                        "pct": round(base_sl, 1),
                        "defaultPct": round(pcfg_float(self, "sl_pct", xd("sl_pct")), 1),
                        "isOverride": self.sl_pct_override is not None,
                        "effectivePct": round(sl_pct, 1),
                    },
                    "autoExit": {
                        "value": auto,
                        "isOverride": self.auto_exit_override is not None,
                        "defaultValue": pcfg_bool(self, "auto_exit_enabled", False),
                    },
                },
                # The one line worth reading. Everything else is detail.
                "verdict": lines[0] if lines else "",
                # True when no rule can realistically fire from here — the row
                # should say so loudly rather than listing levels.
                # A hand-armed trail keeps the row live even with auto-exit off:
                # position_monitor runs that one outside the gate.
                "inert": (not auto and not live_manual) or (not reach) or (abs(nearest[0]) > 100),
                "nearestMovePct": round(nearest[0], 1) if nearest else None,
                "summary": lines,
            }
        except Exception:
            return None

    def to_dict(self):
        try:
            from app.core.profile_resolver import _sections_for
            secs = [s.removeprefix("profile:") for s in _sections_for(self)]
        except Exception:
            secs = []
        return {
            "id":            self.id,
            "osiSymbol":     self.osi_symbol,
            "symbol":        self.symbol,
            "expiry":        self.expiry,
            "strike":        self.strike,
            "optionType":    self.option_type,
            "contracts":     self.total_contracts,
            "remaining":     self.remaining,
            "avgPrice":      self.avg_price,
            "currentPrice":  self.current_price,
            "pnlPct":        round(self.pnl_pct(), 2),
            "pnlDollar":     round(self.pnl_dollar(), 2),
            "status":        self.status,
            "openTime":      _iso_utc(self.open_time),
            "closeTime":     _iso_utc(self.close_time),
            "closePrice":    self.close_price,
            "highestPrice":  self.highest_price,
            "lowestPrice":   self.lowest_price,
            "exits": {
                "pt1": {"triggered": self.pt1_triggered},
                "pt2": {"triggered": self.pt2_triggered},
                "pt3": {"triggered": self.pt3_triggered},
                "sl":  {"triggered": self.sl_triggered},
                "tpt": {"armed": self.tpt_armed},
            },
            "isCredit":      self._credit(),
            "spreadWidth":   self.spread_width,
            "maxLossDollar": round(self.max_loss_dollar(), 2),
            "ptsDisabled":   bool(self.pts_disabled),
            "breakevenLockDisabled": bool(self.breakeven_lock_disabled),
            "trailEnabled":  bool(self.trail_enabled),
            "strategyTag":   self.strategy_tag,
            "realizedPnl":   self.realized_pnl,
            "author":        self.author,
            "profile":       secs[0] if secs else "trading",
            "profileStack":  secs or ["trading"],
            "exitPlan":      self.exit_plan(),
        }


class Order(Base):
    __tablename__ = "orders"

    id            = Column(Integer, primary_key=True, index=True)
    public_order_id = Column(String, nullable=True)
    osi_symbol    = Column(String)
    side          = Column(String)       # BUY | SELL
    quantity      = Column(Integer)
    limit_price   = Column(Float)
    fill_price    = Column(Float, nullable=True)
    filled_qty    = Column(Integer, nullable=True)   # actual number of contracts filled (for partial fills)
    status        = Column(String, default="PENDING")  # PENDING | FILLED | PARTIALLY_FILLED | CANCELLED | REJECTED
    trigger       = Column(String)       # DISCORD | PT1 | PT2 | PT3 | SL | MANUAL | PT1_RESTING | PT2_RESTING | SL_RESTING
    # Resting orders: pre-placed at entry, sit on broker book until fill or cancel.
    # is_resting=True flags an order that was placed AHEAD of the trigger crossing,
    # not REACTIVELY after. Cancel-and-replace updates this row in place.
    is_resting    = Column(Boolean, default=False, nullable=True)
    placed_at     = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    filled_at     = Column(DateTime, nullable=True)
    error_text    = Column(String, nullable=True)
    # Snapshot of pos.avg_price at the moment a SELL filled. Anchors realized P&L
    # against the basis at fill time, immune to later BUY adds / reconciler reopens
    # that mutate the live Position.avg_price.
    cost_basis    = Column(Float, nullable=True)
    # Discord author who triggered this order (entry BUY → opening analyst).
    # Mirrors Position.author so the orders feed can attribute every row.
    author        = Column(String, nullable=True, index=True)
    # Lane tag mirrored from the alert/position (SMALL_ACCT | WHALE | RE-ENTRY),
    # so orders and the calendar journal can badge e.g. small-account-challenge
    # trades without joining back to a Position that may have been reused.
    strategy_tag  = Column(String, nullable=True)
    # The alert this order came from. The executor always knew it — it passed
    # alert_db_id straight into _mark_alert_status — but the link only ever
    # reached the WebSocket payload as linkedOrderId and was never stored, so
    # "which Discord message produced this fill" had to be reconstructed by
    # matching symbol and timestamp. NULL on rows placed before this column
    # existed, and on orders with no alert behind them (reconciler reclaims,
    # dashboard MANUAL exits).
    alert_id      = Column(Integer, nullable=True, index=True)

    def to_dict(self):
        # Realized P&L per SELL row, anchored against the cost_basis snapshot
        # taken when the SELL filled. None for BUYs and for SELLs missing
        # cost_basis (legacy rows or rejected fills). Two-leg combo keys are
        # credit verticals — long-premium (fill - basis) inverts every winner.
        profit = None
        if (
            self.side == "SELL"
            and self.status == "FILLED"
            and self.fill_price is not None
            and self.cost_basis is not None
            and (self.filled_qty or 0) > 0
        ):
            profit = fill_pnl_dollar(
                self.fill_price, self.cost_basis, self.filled_qty,
                is_credit_combo_osi(self.osi_symbol),
            )
        return {
            "id":             self.id,
            "publicOrderId":  self.public_order_id,
            "osiSymbol":      self.osi_symbol,
            "side":           self.side,
            "quantity":       self.quantity,
            "limitPrice":     self.limit_price,
            "fillPrice":      self.fill_price,
            "filledQty":      self.filled_qty,   # actual contracts filled (may differ from quantity on partial)
            "status":         self.status,
            "trigger":        self.trigger,
            "placedAt":       _iso_utc(self.placed_at),
            "filledAt":       _iso_utc(self.filled_at),
            "error":          self.error_text,
            "costBasis":      self.cost_basis,
            "profit":         profit,           # realized $ on SELLs with cost_basis; None otherwise
            "author":         self.author,
            "strategyTag":    self.strategy_tag,
        }


class DiscordSignal(Base):
    """Discord Signal Intelligence — captured message from a tracked user
    (e.g. BacteriaNFA), with OCR-extracted chart text and parsed bias/levels."""
    __tablename__ = "discord_signals"

    id              = Column(Integer, primary_key=True, index=True)
    timestamp       = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    author          = Column(String, index=True)
    channel_id      = Column(String, index=True)
    discord_message_id = Column(String, unique=True, index=True)
    raw_text        = Column(Text)
    ocr_text        = Column(Text, nullable=True)
    image_urls      = Column(Text, nullable=True)        # JSON list of attachment URLs
    bias            = Column(String, nullable=True)      # bullish | bearish | neutral
    key_levels      = Column(Text, nullable=True)        # JSON list of floats
    keywords        = Column(Text, nullable=True)        # JSON list of matched keywords
    confidence      = Column(Float, nullable=True)       # 0..100
    recommendation  = Column(Text, nullable=True)        # JSON {strategy, legs, notes}
    ai_recommendation = Column(Text, nullable=True)      # JSON from ai_signal_advisor (multimodal)
    content_hash    = Column(String, unique=True, index=True)
    outcome_pnl     = Column(Float, nullable=True)       # placeholder for future backtest
    outcome_note    = Column(String, nullable=True)

    def to_dict(self):
        import json
        def _j(s):
            if s is None or s == "":
                return None
            try:
                return json.loads(s)
            except Exception:
                return s
        return {
            "id":               self.id,
            "timestamp":        _iso_utc(self.timestamp),
            "author":           self.author,
            "channelId":        self.channel_id,
            "discordMessageId": self.discord_message_id,
            "rawText":          self.raw_text,
            "ocrText":          self.ocr_text,
            "imageUrls":        _j(self.image_urls) or [],
            "bias":             self.bias,
            "keyLevels":        _j(self.key_levels) or [],
            "keywords":         _j(self.keywords) or [],
            "confidence":       self.confidence,
            "recommendation":   _j(self.recommendation),
            "aiRecommendation": _j(self.ai_recommendation),
            "contentHash":      self.content_hash,
            "outcomePnl":       self.outcome_pnl,
            "outcomeNote":      self.outcome_note,
        }


class SystemState(Base):
    """Tiny key/value store for process state that must survive a restart —
    e.g. the daily trading-halt flag. In-memory module globals (guardrails)
    are lost on restart/redeploy, which let a day that already blew the
    max-daily-loss cap resume trading after a crash (GH #10). Persisting here
    makes that state authoritative across restarts."""
    __tablename__ = "system_state"

    key        = Column(String, primary_key=True)
    value      = Column(String, nullable=True)
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class OrderAudit(Base):
    """Append-only audit trail of every order decision. You trade on others'
    calls — this is the provenance record: who/what drove each allow/block, with
    the dedup hash, so any disputed or surprising trade can be explained. Never
    updated, only inserted; prune by age if it ever grows (it's low-volume)."""
    __tablename__ = "order_audit"

    id           = Column(Integer, primary_key=True)
    ts           = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    action       = Column(String)            # BUY | SELL
    osi_symbol   = Column(String, index=True)
    author       = Column(String, index=True)
    content_hash = Column(String)
    verdict      = Column(String, index=True)  # ALLOWED | BLOCKED
    reason       = Column(String)            # block reason (empty when allowed)
    qty          = Column(Integer, nullable=True)
    price        = Column(Float, nullable=True)

    def to_dict(self):
        return {
            "id": self.id, "ts": _iso_utc(self.ts), "action": self.action,
            "osiSymbol": self.osi_symbol, "author": self.author,
            "contentHash": self.content_hash, "verdict": self.verdict,
            "reason": self.reason, "qty": self.qty, "price": self.price,
        }


def record_order_decision(db, *, action, osi_symbol, author, content_hash,
                          verdict, reason="", qty=None, price=None) -> None:
    """Append one audit row. Best-effort: callers wrap in try/except so the
    audit write can never break the order path."""
    db.add(OrderAudit(
        action=action, osi_symbol=osi_symbol or "", author=author or "",
        content_hash=content_hash or "", verdict=verdict, reason=(reason or "")[:300],
        qty=qty, price=float(price) if price is not None else None,
    ))
    db.commit()


def get_system_state(db, key: str) -> str | None:
    row = db.query(SystemState).filter(SystemState.key == key).first()
    return row.value if row else None


def set_system_state(db, key: str, value: str) -> None:
    row = db.query(SystemState).filter(SystemState.key == key).first()
    if row:
        row.value = value
    else:
        db.add(SystemState(key=key, value=value))
    db.commit()


def delete_system_state(db, key: str) -> None:
    db.query(SystemState).filter(SystemState.key == key).delete()
    db.commit()


def is_credit_combo_osi(osi_symbol: str) -> bool:
    """Two-leg combo keys are credit verticals (`short|long`).

    Three-leg keys are debit flies (body|lower|upper) and keep long-premium
    math. Position.is_credit is the source of truth when a row is in hand;
    this is the Order-side heuristic so calendar / stats / to_dict() agree
    without a join.
    """
    parts = [p for p in (osi_symbol or "").split("|") if p]
    return len(parts) == 2


def fill_pnl_dollar(fill_price, cost_basis, qty, is_credit: bool = False) -> float:
    """Realized $ for one fill. A credit spread profits as the debit falls."""
    if is_credit:
        return (cost_basis - fill_price) * qty * 100
    return (fill_price - cost_basis) * qty * 100


def realized_pnl_since(db, start_time) -> float:
    """Sum realized P&L from FILLED SELL orders since start_time.

    Preferred path: use Order.cost_basis (snapshot of pos.avg_price at SELL
    fill time). This is immune to later BUYs / reconciler reopens that mutate
    the live Position.avg_price.

    Legacy fallback (cost_basis is None — old rows): match the SELL fill to
    the Position that owned that osi_symbol at the time of fill and use its
    current avg_price. This is approximate and may be wrong if the same
    Position row was reopened.

    fill_price is checked against None, not truthiness: a contract that
    expired worthless realizes at exactly 0.0. Two-leg combo SELLs invert
    (basis - fill) so a winning CCS is not booked as a loss.
    """
    from sqlalchemy import or_
    orders = db.query(Order).filter(
        Order.side == "SELL",
        Order.status == "FILLED",
        Order.filled_at.isnot(None),
        Order.filled_at >= start_time,
    ).all()
    total = 0.0
    for o in orders:
        fill_px = o.fill_price
        qty = o.filled_qty or o.quantity
        if fill_px is None or not qty:
            continue
        pos = None
        basis = o.cost_basis
        if not basis:
            pos = (db.query(Position)
                     .filter(Position.osi_symbol == o.osi_symbol)
                     .filter(Position.open_time <= o.filled_at)
                     .filter(or_(Position.close_time.is_(None),
                                 Position.close_time >= o.filled_at))
                     .order_by(Position.open_time.desc())
                     .first())
            if pos is None:
                pos = (db.query(Position)
                         .filter(Position.osi_symbol == o.osi_symbol)
                         .order_by(Position.open_time.desc())
                         .first())
            basis = (pos.avg_price or 0) if pos else 0
        if not basis:
            continue
        if pos is not None:
            credit = bool(pos.is_credit)
        else:
            credit = is_credit_combo_osi(o.osi_symbol)
        total += fill_pnl_dollar(fill_px, basis, qty, credit)
    return total
