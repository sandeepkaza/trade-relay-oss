# Exit engine hardening — open work

Status: **items 1–6 written 2026-08-21, all flag-off by default. Uncommitted, not
deployed.** Item 7 (broker-side protection) not started. Written from a full read of the
exit path plus the P&L forensics in the two artifacts linked at the bottom.

Every fix lands on `PublicExecutor` or `position_monitor`, which is what the **IBKR box
runs** — `IBKRExecutor` subclasses `PublicExecutor` and overrides placement and fill
polling only. Verified by test rather than by reading: `test_ibkr_inherits_the_exit_path`
asserts `_place_sell`, `_repeg_close_all_sell` and `_compute_limit` are still the inherited
objects, and `test_ibkr_terminal_statuses_are_rearmable` asserts every terminal status
`_IB_STATUS_MAP` can produce is one the re-arm recognises. If a future change overrides one
of those on the IBKR side, the suite says so instead of the fix quietly applying to
Public.com only.

| flag | default | turns on |
| --- | --- | --- |
| `auto_exit_repeg_enabled` | `false` | items 1–3 together: the chase, the displacement rule, the re-arm |
| `auto_exit_stale_seconds` | `10` | how long an unfilled AUTO order keeps its priority |
| `sl_use_bid_enabled` | `false` | item 4 — measure the stop against the bid |
| `wide_spread_max_defer_ticks` | `0` | item 5 — bound the wide-spread defer (0 = defer forever, legacy) |
| `quote_gap_alarm_ticks` | `120` | item 6 — alarm on a dark contract (observability only, so default-on) |

All five are in `EDITABLE_KEYS` and documented in `config.example.ini`.

Why it matters now: every finding below is dormant while `auto_exit_enabled = false`.
The forensics say that switch has to be turned on (it is the difference between −$16,687
and roughly +$18,600 over the same 711 trades). **Turning it on is what makes these bugs
real**, so they ship first, not after.

---

## 1. Auto-exits fire once and never chase — DONE

The whole lifecycle of an automated exit is:

```text
position_monitor._fire_exit  →  executor._place_sell  →  executor._poll_order_fill
```

The limit goes out at `mark × (1 - sell_slippage)` (3%) and nothing ever re-pegs it.

Re-peg watchers already exist and work — `_repeg_close_all_sell`
(`public_executor.py:2163`) and `_repeg_partial_sell` (`:2329`), with a real escalation
ladder: bid − 1 tick, repeatedly, then a final decisively-marketable bid − 20%. They are
spawned only at `public_executor.py:1858` and `:1869`, both inside the **DISCORD**
analyst-exit path. No SL / PT / TRAILING_SL / TPT exit ever gets one.

On a fast 0DTE move that limit is stale in seconds.

**Fix:** spawn `_repeg_close_all_sell` from `_fire_exit` for SL-class triggers. Roughly
three lines; the ladder is already written and already tested by the DISCORD lane.

## 2. A stranded AUTO SELL blocks every other way out — DONE

`_place_sell` idempotency ladder (`public_executor.py:1916` onward):

```python
should_cancel = (
    (new_is_auto and not stale_is_auto)
    or (new_is_manual and not stale_is_auto and not stale_is_manual)
    or new_displaces_discord
)
```

Once the stale pending order is AUTO, every branch is `False`:

- a newer AUTO cannot displace it (`not stale_is_auto` fails)
- MANUAL cannot displace it (same clause)
- DISCORD cannot displace it (third clause needs both sides DISCORD)

So while a dead auto limit rests, the dashboard SELL button and the analyst's "ALL OUT"
both log `Skipped: existing pending SELL` and do nothing — for up to
`max_pending_minutes = 30`.

**Fix:** allow a newer AUTO to displace a stale AUTO, and allow MANUAL to displace an AUTO
older than ~10s. Keep the existing 3-state confirmed-cancel contract
(`repeg_confirmed_cancel_only`) on both new paths — that is what prevents the double-sell
this ladder was built to stop.

## 3. The trigger flag never resets, so the stop disarms permanently — DONE

In `position_monitor`, `pos.sl_triggered = True` is set as soon as `_fire_exit` returns —
which is on **submit**, not on fill (`_place_sell` returns `db_order` in PENDING).

The only places any trigger flag is reset are `public_executor.py:1507/1510` and
`:1643/1646`, both on a new BUY (scale-in / re-entry). When `_poll_order_fill` auto-cancels
the unfilled order at 30 minutes, nothing clears the flag. The position then sits open with
its stop permanently disarmed and no log line saying so.

**Fix:** clear `sl_triggered` (and the `pt*_triggered` equivalents) when the order that set
it reaches CANCELLED / REJECTED with zero filled qty.

Items 1–3 are one bug family. Ship together behind a single flag —
`[trading] auto_exit_repeg_enabled` — with a one-knob revert to today's behaviour.

## 4. Everything triggers on mid and fills at the bid — DONE

`fetch_prices` returns `(bid+ask)/2` (`ibkr_sdk_bridge.py:721`, docstring is explicit).
Every PT and SL threshold is measured against that mid, but a SELL fills at or near the
bid. On a $2.00 0DTE SPX contract with a $0.50 spread, a stop that reads "−20%" realises
about −32%.

**Done** (`sl_use_bid_enabled`, default off). `_exit_price(mid, spread_pct)` returns
`mid x (1 - spread_pct/200)` — the bid, since `spread_pct` is `(ask-bid)/mid x 100` — clamped
at 10% of mid so a broken quote cannot manufacture a stop trigger. Only the two fixed-SL
trip conditions and the trailing-SL level read it; `pnl_pct` itself is untouched, so PT
rungs, both floor locks, the breakeven lock, the dashboard and the price sampler all still
see the mid. A PT measured on the bid would fire late on exactly the contracts that are
hardest to sell.

## 5. The wide-spread guard removes the stop exactly when it is needed — DONE

`wide_spread_skip_sl_pct = 20` defers an SL fire whenever the spread exceeds 20% of mid.
The intent is right (a mid-priced stop that fills at the bid is a structural loss), but it
defers **indefinitely**, and wide spreads and fast moves are the same event.

**Done** (`wide_spread_max_defer_ticks`, default 0 = legacy unbounded defer). Counts
consecutive deferred ticks per position in `_wide_spread_defers` and, past the cap, fires
the stop anyway and logs `SL_DEFER_EXPIRED` to `exits.log`. The counter resets the moment
the spread comes back inside the threshold. Only worth turning on together with item 1 —
forcing the exit through a spread that wide is what the re-peg ladder is for.

## 6. A position with no quote is silently unprotected — DONE

`_check_all_positions`: `current_price is None → continue`. No quote means no stop, no
target, no log line, no alarm. The `last_price_poll_age_seconds` metric only catches a
whole-loop stall, not one contract going dark.

**Done** (`quote_gap_alarm_ticks`, default 120 = ~60s at a 0.5s poll). Counts consecutive
misses per OSI and fires one Discord critical plus a `QUOTE_GAP` line in `exits.log`.
Alarms once per outage, not once per tick, and is market-hours gated — without that gate
every open position pages all night, every night. Observability only: it changes no exit
behaviour, which is why this one ships default-on.

## 7. Nothing protects an open position if the process dies

Public.com and Tradier have no broker-side resting stop. Today, if the container dies
mid-session, every open position is naked until it comes back.

IBKR is the one broker that could fix this — the OCA bracket from the
`feat/ibkr-whale-oca` work is built but flag-off and unproven live. Proving it out is its
own piece of work, and it is the reason nothing above should be called "complete" risk
management: items 1–6 make the exit engine reliable *while the process is running*.

---

## Where the settings landed

Decided from the forensics, not yet applied to either box. The two traps are the reason
the obvious version of this is wrong.

**Trap A — the PT rungs are chained.** `PT2` requires `pt1_triggered`, `PT3` requires
`pt2_triggered`, TPT arms only at `PT3`, and `trailing_sl` requires `pt1_triggered`
(`position_monitor.py:757, 770, 673`). Expressing "sell half at +50" by disabling pt1/pt2
and moving `pt3_pct` to 50 silently disables **all** profit-taking and the trail. The single
rung has to sit on PT1.

**Trap B — the floor locks.** `pt2_floor_lock_enabled` / `pt3_floor_lock_enabled` default
to `True` in code and appear in no tracked template. `pt2_floor_lock_pct` defaults to
`pt1_pct` and arms at `pt2_pct`. Move `pt1_pct` to 50 with the lock still on and a peak of
+25% sets `sl_pct` to **+50%** — above the current price — selling instantly under an `SL`
label.

`[trading]`:

```ini
auto_exit_enabled        = true
pt1_pct                  = 50      ; was 15
pt1_sell                 = 0.50    ; was 0.25
pt2_enabled              = false
pt3_enabled              = false
tpt_enabled              = false   ; can never arm once pt3 is off
pt2_floor_lock_enabled   = false   ; trap B
pt3_floor_lock_enabled   = false   ; trap B
trailing_sl_enabled      = true
trailing_sl_arm_pct      = 50      ; was 25
trailing_sl_pct          = 15
sl_enabled               = true
sl_pct                   = -20
breakeven_lock_enabled   = true    ; arm +15 → floor +5, unchanged
```

`[guardrails]`: `max_trade_cost = 1000`

`[profile:*]` — these outrank `[trading]` (resolver order: whale → reentry → multi_day →
spx_index → author → trading), and two of them switch the engine off entirely:

| section | today | change to |
| --- | --- | --- |
| **`spx_index`** | `sl_enabled = false`, `pts_disabled_default = true` — matches every SPX/SPXW/NDX/RUT ticket. **117 lots, $62,580, −$4,668 with no stop and no target.** | `sl_enabled = true`, `sl_pct = -20`, `pts_disabled_default = false` |
| `monkey` | `sl_pct = -50`, `pts_disabled_default = true` | `-20`, `false` |
| `sarang` | `sl_pct = -50`, `pts_disabled_default = true` | `-20`, `false` |
| `tc` | `sl_pct = -30`, own 15/25/40 ladder | `-20`, `pt1_pct = 50`, `pt1_sell = 0.50`, pt2/pt3/tpt off |
| `ivtrader`, `bdorts` | `sl_pct = -50`, `pts_disabled_default = true` | `-20`, `false` |
| `multi_day` | `sl_pct = -40`, `pts_disabled_default = true` | `-25`, `false` (wider on purpose — held overnight) |

Leave `[profile:whale]` alone; it is internally consistent and deliberately runs its own
single-rung scheme.

`[trading]` and `[guardrails]` keys are all in `EDITABLE_KEYS` → `POST /api/config`
hot-reloads them. `[profile:*]` sections are **not** portal-editable: edit `config.ini` with
a real editor (never `sed -i` — new inode, container keeps the old file), then post any
editable key to force the reload.

Only `ibkr-oci`'s `config.ini` was read for the "today" column. The EC2 box was stopped at
the time; its current values are unverified, though the targets are the same.

## Trailing and breakeven, derived (2026-08-21)

These two were inherited guesses — eight different per-profile values, none with
evidence, all sitting in front of the one `[trading]` setting so it reached
almost nothing. Swept against the same 711 lots, base held at half-at-+50 /
stop -20 with a 5% slippage haircut.

| mechanism | worth | evidence |
| --- | --- | --- |
| breakeven lock | **+$7,164** | $14,147 with vs $6,983 without; **86 lots** rescued from round-tripping into a loss |
| trailing stop | **+$10,610** | $24,756 vs $14,147 — the largest contributor after the stop itself |

**Breakeven arm +15% / floor +5% was already correct** and is now validated
rather than changed: it sits at the constrained optimum. The unconstrained grid
picked arm +10% / floor **+15%**, which is impossible — a floor above its own
trigger — and only appeared because the model teleports the exit to the floor
with no path cost. Floors must be swept strictly below the arm.

**The trail width is NOT derivable from this data.** Every grid corners at the
tightest value for the same reason the stop grid did: the model only trails a
lot that *ended* below the trail level, so it is never whipsawed out on an
intermediate dip and tighter always wins. Chosen from a measurement instead —
the median armed winner exits **8% below its peak**, p75 is **20%** — so a 20%
give-back engages only on genuine round-trips, while 15% sits mid-distribution
where whipsaw is real and invisible. Arm +30% matches both the grid (25/30 tie)
and the MFE heuristic (80% of median winner MFE = +28%). Costs ~$1k of modelled
upside against the corner value and buys robustness the model cannot price.

**Per-profile differentiation had no sample behind it**, so all 29 trailing and
breakeven overrides were deleted from `spx_index`, `multi_day`, `sarang`, `tc`,
`twinsight`, `ivtrader`, `bdorts` and `reentry`; every lane now inherits the one
`[trading]` value. `[profile:whale]` is untouched — self-contained scheme, its
trail-off is deliberate. The profiles that had the trail switched off were the
most expensive: SPX/NDX/RUT **+$5,953**, sarang **+$2,530** (bdorts n=15, no
effect; ivtrader n=1, no conclusion).

Applied to ibkr-oci 2026-08-22 00:40 ET. Reproduce with `scripts/ops/exit_sim.py`.

**These are now the built-in defaults** (`app/core/profile_resolver.py`,
`EXIT_DEFAULTS`) rather than only a config edit on one box — half out at +50,
stop -20, trail arm +30 / give back 20, breakeven arm +15 / floor +5, one rung,
both PT floor locks off. The three call sites that each carried their own
literal fallback (`position_monitor`, `Position.exit_plan`, the trail-toggle
endpoint) read that one dict; they had already drifted, so the row quoted a
+50% trail arm while the monitor used +60%. `tests/test_exit_defaults.py` fails
if a literal comes back or if a breakeven floor is set above its own arm.

Nothing about this arms anything: `auto_exit_enabled` still defaults to false,
and any key present in `config.ini` or a `[profile:*]` section still wins. The
box that inherits these is a box whose config is silent on them — which is why
`[profile:whale]` and `exit_engine_v2`'s own ladder are deliberately untouched.

## Modelled effect

Same 711 real lots, FIFO-matched, peaks from `positions.highest_price`:

| | net | $/day |
| --- | --- | --- |
| what actually happened | −16,687 | −204 |
| today's ladder, switch flipped | +11,216 | 137 |
| half at +50, SL −20, breakeven lock | +18,621 | 227 |
| … + `max_trade_cost = 1000` | +20,131 | 252 |
| … with a 5% slippage haircut on every stop | +17,122 | 214 |

Recent regime is thinner: **$89/day since 1 July, $126/day over the last 30 days** (haircut
applied). $200/day average needs roughly 2× current size.

Those figures exclude the trailing stop. Re-run `exit_sim.py` and the trailing rows come
out far higher (~$25k with the haircut) — that model is a ceiling, not a forecast: with no
tick history it only trails a lot that *ended* below the trail level, so it never gets
whipsawed out on an intermediate dip the way a real trail does. Compare no-trail rows to
each other when picking a ladder. The same caveat is why the "no profit target at all" row
appears to beat the proposed one.

Caveat that has not gone away: `lowest_price` was NULL on all 711 historical lots, so a stop
only scores on trades that *finished* below it. Every SL number here is an upper bound, and
it is why −15% was refused even though it scored best. `position_monitor.py:587` now writes
the column; re-run the analysis after ~4 weeks of live rows and settle the stop width from
MAE-vs-final-P&L instead of from a biased grid. The profit-target side does not have this
problem — peaks were always recorded.

## Links

- [Technical report](https://claude.ai/code/artifact/0d70bfc4-37b3-44d2-a03c-2384b56dab6d)
- [Plain-language version](https://claude.ai/code/artifact/9396e926-3f2d-493e-96f4-466407ded92a)
- `scripts/ops/exit_sim.py` — replays a `trader.db` snapshot under alternative exit rules
  and reproduces every table above. This is the tool to re-run once `lowest_price` has real
  data. Lot-level export with analyst attribution: `--csv out.csv` (write it outside the
  repo; it is account data).

```bash
python scripts/ops/exit_sim.py snapshot.db --slip 5
python scripts/ops/exit_sim.py snapshot.db --since 2026-07-01 --cap 1000
python scripts/ops/exit_sim.py --selfcheck
```
