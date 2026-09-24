"""
config_manager.py - Centralized, hot-reloadable configuration singleton.

All modules import from here instead of calling configparser directly.
When /api/config POST is called, `reload()` re-reads config.ini and all
subsequent calls to `get()` / `getboolean()` / `getfloat()` / `getint()`
return the new values — no restart needed.
"""

import configparser
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

# Repo root = parents[2] (this file lives at app/core/config_manager.py).
# Before the restructure config_manager sat next to config.ini at the repo
# root, so `parent / "config.ini"` worked. Now we have to walk back two
# directories. TRADER_CONFIG_PATH env var lets the host override the path
# (used by container deploys where /app/config.ini is a bind mount).
_REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = Path(os.getenv("TRADER_CONFIG_PATH", str(_REPO_ROOT / "config.ini")))

# ── Sensitive keys that must NOT be editable via the API ─────────────────────
SENSITIVE_KEYS: set[tuple[str, str]] = {
    ("discord", "token"),
    ("public", "api_key"),
    ("tradier", "access_token"),
    ("ingest", "shared_secret"),
    ("forwarder", "user_token"),
    ("forwarder", "webhook_url"),
    # Lime authenticates with an OAuth2 password grant, so the account password
    # itself is a config value — all four parts are secrets, not just the token.
    ("lime", "username"),
    ("lime", "password"),
    ("lime", "client_id"),
    ("lime", "client_secret"),
}

# ── Allowlist: only these (section, key) pairs may be written via /api/config ─
# Add a new key here when you add a new setting you want editable from the UI.
EDITABLE_KEYS: set[tuple[str, str]] = {
    # [trading]
    ("trading", "dry_run"),
    ("trading", "manual_halt"),
    ("trading", "first_sell_closes_all"),
    # Vertical credit spreads (Tradier multileg). spreads_enabled is the master
    # switch and the one-knob revert; spread_max_risk_dollars caps (width -
    # credit) x 100 x contracts, which is the real exposure — not the credit.
    ("trading", "spreads_enabled"),
    ("trading", "spreads_only"),          # true = refuse ALL single-leg BUYs on this instance
    ("trading", "spread_channel_ids"),
    ("trading", "spread_max_risk_dollars"),
    ("trading", "spread_max_contracts"),
    ("trading", "spread_mirror_analyst_qty"),
    # Butterflies ride the same lane but are a DEBIT structure: risk is the
    # debit paid, not (width - credit). spread_flies_enabled is their own
    # one-knob revert on top of spreads_enabled; spread_fly_right picks calls
    # or puts, which the alert never states.
    ("trading", "spread_flies_enabled"),
    ("trading", "spread_fly_right"),
    # [lime] — orders_enabled is the second half of the two-step opt-in and the
    # one-knob revert. Lime has no sandbox host, so this flag and the account
    # number are the only things standing between the bot and real money.
    ("lime", "orders_enabled"),
    ("lime", "validate_before_order"),
    ("lime", "fill_poll_interval_seconds"),
    ("lime", "block_buy_without_quote"),  # false = bid blind off the alert price when quotes fail
    ("lime", "feed_wait_max_seconds"),    # max wait for a feed frame before re-quoting over REST; 30 = old behavior
    ("trading", "position_owner_scope"),  # author = only the opener may exit/add; osi = legacy
    ("trading", "resolve_missing_ticker_on_exit"),  # ticker-less SELL adopts the symbol of the analyst's one matching open position
    ("trading", "unparsed_alert_alarm_enabled"),    # page when an order-shaped alert fails to parse
    ("trading", "max_pending_minutes"),
    ("trading", "position_monitor_enabled"),
    ("trading", "poll_interval_seconds"),
    ("trading", "buy_slippage"),
    ("trading", "sell_slippage"),
    ("trading", "fill_poll_interval_seconds"),
    ("trading", "auto_exit_enabled"),
    # Chase the fill on stop-class auto exits (SL/TRAILING_SL/TPT/TIME_EXIT),
    # let anything displace an AUTO order left unfilled past
    # auto_exit_stale_seconds, and re-arm a stop whose order died with no fill.
    # auto_exit_repeg_enabled=false is the one-knob revert to place-once.
    ("trading", "auto_exit_repeg_enabled"),
    ("trading", "auto_exit_stale_seconds"),
    # Measure the stop against the bid instead of the mid (a SELL fills at the
    # bid, so a mid-measured -20% realises more like -32% on a wide 0DTE).
    ("trading", "sl_use_bid_enabled"),
    # Cap on how long wide_spread_skip_sl_pct may keep deferring the stop.
    # 0 = legacy unbounded defer, which removes the stop in fast markets.
    ("trading", "wide_spread_max_defer_ticks"),
    # Alarm after N consecutive polls with no quote for an open position.
    ("trading", "quote_gap_alarm_ticks"),
    ("trading", "pt1_enabled"),
    ("trading", "pt1_pct"),
    ("trading", "pt1_sell"),
    ("trading", "pt2_enabled"),
    ("trading", "pt2_pct"),
    ("trading", "pt2_sell"),
    ("trading", "pt3_enabled"),
    ("trading", "pt3_pct"),
    ("trading", "sl_enabled"),
    ("trading", "sl_pct"),
    ("trading", "trailing_sl_enabled"),
    ("trading", "trailing_sl_pct"),
    ("trading", "trailing_sl_arm_pct"),
    ("trading", "breakeven_lock_enabled"),
    ("trading", "breakeven_lock_arm_pct"),
    ("trading", "breakeven_lock_floor_pct"),
    ("trading", "pt2_floor_lock_enabled"),
    ("trading", "pt2_floor_lock_pct"),
    ("trading", "pt3_floor_lock_enabled"),
    ("trading", "pt3_floor_lock_pct"),
    ("trading", "sl_post_fill_grace_seconds"),
    ("trading", "wide_spread_skip_sl_pct"),
    # Price-history sampling → logs/prices.jsonl. Observability only; changes
    # no exit behavior. Editable so it can be switched on mid-session.
    ("trading", "max_tick_jump_multiple"),  # absurd-tick guard; 0/large = off
    ("trading", "price_track_enabled"),
    ("trading", "price_track_interval_seconds"),
    ("trading", "sl_market_hours_only"),
    ("trading", "manual_sell_priority_seconds"),
    ("trading", "tpt_enabled"),
    ("trading", "pt3_sell"),
    ("trading", "tpt_arm_pct"),
    ("trading", "tpt_trail_pct"),
    ("trading", "size_default"),
    ("trading", "ignore_message_qty"),
    ("trading", "message_qty_authors"),   # authors whose stated ‼ contract count wins
    ("trading", "small_account_only"),    # true = mirror ONLY ‼ small-account entries
    ("trading", "size_xs"),
    ("trading", "size_small"),
    ("trading", "size_medium"),
    ("trading", "size_large"),
    ("trading", "size_full"),
    ("trading", "size_xl"),
    ("trading", "local_parser_enabled"),
    ("trading", "ai_parser_enabled"),
    ("trading", "ai_scorer_enabled"),
    ("trading", "ai_scorer_min_score"),
    ("trading", "kelly_enabled"),
    ("trading", "kelly_max_risk_pct"),
    ("trading", "kelly_lookback_days"),
    ("trading", "trade_logger_enabled"),
    ("trading", "daily_summary_enabled"),
    ("trading", "daily_summary_ai_enabled"),
    ("trading", "pattern_review_enabled"),
    ("trading", "infra_report_enabled"),
    ("trading", "daily_summary_hour"),
    ("trading", "daily_summary_minute"),
    ("trading", "analyst_tracking_enabled"),
    ("trading", "analyst_whitelist"),
    ("trading", "analyst_blacklist"),
    # A blacklisted author may still exit a position they hold — the revert
    # knob for the 2026-09-17 SPCX strand. Portal-editable because the thing it
    # governs is "can this position be closed at all", which is not a knob to
    # go looking for an SSH session over.
    ("trading", "blacklist_allows_owner_exit"),
    ("trading", "symbol_analyst_whitelist"),
    ("trading", "rollup_block_enabled"),
    ("trading", "rollup_whitelist"),
    ("trading", "forwarder_enabled"),
    ("trading", "reconciler_enabled"),
    ("trading", "reconcile_interval_seconds"),
    # [time_exit]
    ("trading", "time_exit_enabled"),
    ("trading", "time_exit_hour"),
    ("trading", "time_exit_minute"),
    ("trading", "time_exit_symbols"),
    ("trading", "time_exit_days"),
    # [vix_sizing]
    ("trading", "vix_sizing_enabled"),
    ("trading", "vix_low_threshold"),
    ("trading", "vix_high_threshold"),
    ("trading", "vix_extreme_threshold"),
    ("trading", "vix_current_level"),
    ("trading", "vix_cache_max_age_seconds"),
    ("trading", "vix_unavailable_scale"),   # size multiplier when VIX is unknown; 1.0 = no haircut
    # [order_retry]
    ("trading", "order_retry_enabled"),
    ("trading", "order_retry_max"),
    ("trading", "order_retry_nudge_bps"),
    ("trading", "order_retry_initial_delay"),
    ("trading", "order_retry_max_delay"),
    # [analytics]
    ("trading", "analytics_enabled"),
    # [greeks]
    ("trading", "greeks_monitoring_enabled"),
    ("trading", "greeks_delta_threshold"),
    ("trading", "greeks_theta_threshold"),
    # [premarket]
    ("trading", "premarket_prep_enabled"),
    ("trading", "premarket_start_hour"),
    ("trading", "premarket_start_minute"),
    ("trading", "premarket_symbols"),
    # [exit_engine]
    ("trading", "exit_engine_mode"),
    # [broker]
    ("trading", "broker"),                 # public | ibkr | tradier — active vendor switch
    ("tradier", "orders_enabled"),         # master gate; must be true for Tradier orders to fire
    ("tradier", "sandbox"),                # true=paper (sandbox.tradier.com), false=live api.tradier.com
    ("tradier", "stream_quotes_enabled"),  # WS streaming quotes (prod-only); off=REST snapshots
    ("tradier", "whale_oco_enabled"),      # native OTOCO/OCO resting bracket on whale fills
    ("tradier", "preview_before_order"),   # pre-flight preview=true buying-power/param check
    # [ingest] — multi-instance HTTP mirror fan-out
    ("trading", "discord_listener_enabled"),  # false = no gateway; HTTP-mirror-fed secondary
    ("ingest",  "http_enabled"),              # accept /api/ingest/forwarded (receiver)
    ("ingest",  "mirror_urls"),               # CSV peer URLs to fan out to (sender)
    ("ibkr",    "orders_enabled"),         # master gate; must be true for IBKR orders to fire
    ("ibkr",    "host"),                   # IB Gateway host (usually 127.0.0.1)
    ("ibkr",    "port"),                   # 4002 paper, 4001 live
    ("ibkr",    "client_id"),              # 0-31 unique connection id
    ("ibkr",    "account"),                # U####### live, DU####### paper
    ("ibkr",    "market_data_type"),       # 1=live, 2=frozen, 3=delayed, 4=delayed-frozen
    ("ibkr",    "quote_wait_ms"),          # wait for first tick on cold subscribe
    ("ibkr",    "ticker_cache_max"),       # LRU cap on streaming subscriptions
    ("ibkr",    "adaptive_entry_enabled"), # BUY entries via IBKR Adaptive algo
    ("ibkr",    "adaptive_exit_enabled"),  # first SELL too; re-pegs stay plain
    ("ibkr",    "adaptive_priority"),      # Patient | Normal | Urgent
    ("ibkr",    "reconnect_watchdog_enabled"),   # retry the session in the background after a drop
    ("ibkr",    "reconnect_max_delay_seconds"),  # backoff ceiling between retries
    ("ibkr",    "reconnect_initial_delay_seconds"),
    ("ibkr",    "vix_client_id"),          # clientId for the index-quote session
    ("ibkr",    "vix_market_data_type"),   # 1=live (needs Cboe Streaming Market Indexes), 3=delayed
    # [guardrails]
    ("guardrails", "guardrails_enabled"),
    ("guardrails", "dedup_window_seconds"),
    ("guardrails", "market_hours_only"),
    ("guardrails", "market_open_hour"),
    ("guardrails", "market_open_minute"),
    ("guardrails", "market_close_hour"),
    ("guardrails", "market_close_minute"),
    ("guardrails", "max_daily_loss"),
    ("guardrails", "max_open_positions"),
    ("guardrails", "max_daily_trades"),
    ("guardrails", "max_trade_cost"),
    ("guardrails", "trade_cooldown_seconds"),
    ("guardrails", "cross_channel_dedup_seconds"),
    ("guardrails", "scale_in_enabled"),
    ("guardrails", "scale_in_min_profit_pct"),
    ("guardrails", "blocked_symbols"),
    ("guardrails", "symbol_max_contracts"),
    ("guardrails", "index_max_contracts"),
    ("guardrails", "correlation_guard_enabled"),
    # [discord]
    ("discord", "channel_ids"),
    ("discord", "no_filter_channel_ids"),
    ("discord", "trade_log_channel_id"),
    ("discord", "daily_summary_channel_id"),
    ("discord", "authors"),
    ("discord", "history_backfill_limit"),
    ("discord", "execute_history"),
    # [ai_parser]
    ("ai_parser", "provider"),
    ("ai_parser", "model"),
    ("ai_parser", "project"),
    ("ai_parser", "location"),
    ("ai_parser", "timeout_seconds"),
    # [signals]
    ("signals", "enabled"),
    ("signals", "channel_id"),
    ("signals", "author"),
    ("signals", "action_mode"),
    ("signals", "vision_enabled"),
    ("signals", "keywords"),
    ("signals", "min_confidence_for_recommendation"),
    ("signals", "ai_advisor_enabled"),
    ("signals", "ai_advisor_model"),
    ("signals", "ai_advisor_max_images"),
    ("signals", "ai_advisor_timeout_seconds"),
    ("signals", "ai_advisor_post_to_webhook"),
    ("signals", "ai_advisor_webhook_url"),
    ("signals", "ai_advisor_grounding"),
    ("signals", "trade_eval_enabled"),
    ("signals", "trade_eval_narrate"),
    ("signals", "trade_eval_post_to_webhook"),
    # [logging]
    ("logging", "file_logging_enabled"),
    ("logging", "log_dir"),
    ("logging", "log_level"),
    ("logging", "max_file_size_mb"),
    ("logging", "backup_count"),
    ("logging", "pipeline_timing_enabled"),
}


class _AppConfig:
    """Thread-safe singleton config reader with hot-reload support."""

    def __init__(self):
        self._lock = threading.RLock()
        self._cfg = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
        self._load()

    def _load(self):
        # interpolation=None  → % signs in Discord tokens/API keys are safe
        # inline_comment_prefixes → strips '; comment' suffixes from values
        self._cfg = configparser.ConfigParser(
            interpolation=None,
            inline_comment_prefixes=(";", "#"),
        )
        self._cfg.read(str(CONFIG_PATH), encoding="utf-8-sig")

    def reload(self):
        """Re-read config.ini into memory. Called after a POST /api/config."""
        with self._lock:
            self._load()
            log.info("[CONFIG] config.ini reloaded into memory")

    # ── Read helpers ──────────────────────────────────────────────────────────

    def get(self, section: str, key: str, fallback: str = "") -> str:
        with self._lock:
            return self._cfg.get(section, key, fallback=fallback)

    def getboolean(self, section: str, key: str, fallback: bool = False) -> bool:
        with self._lock:
            return self._cfg.getboolean(section, key, fallback=fallback)

    def getfloat(self, section: str, key: str, fallback: float = 0.0) -> float:
        with self._lock:
            return self._cfg.getfloat(section, key, fallback=fallback)

    def getint(self, section: str, key: str, fallback: int = 0) -> int:
        with self._lock:
            return self._cfg.getint(section, key, fallback=fallback)

    def has_section(self, section: str) -> bool:
        with self._lock:
            return self._cfg.has_section(section)

    def sections(self) -> list[str]:
        with self._lock:
            return self._cfg.sections()

    def items(self, section: str) -> list[tuple[str, str]]:
        with self._lock:
            try:
                return list(self._cfg.items(section))
            except configparser.NoSectionError:
                return []

    # ── Write helper ──────────────────────────────────────────────────────────

    @staticmethod
    def append_audit(lines: list[str]) -> None:
        """Append rows to logs/config_changes.log — the answer to 'when did this
        knob move'.

        Shared by the portal write path and the reload path. A knob edited on
        disk and reloaded moves money exactly as much as one edited through the
        portal, so both belong in the same trail; on the secondary instances
        every edit arrives this way, which left their history empty.
        """
        if not lines:
            return
        try:
            audit_path = CONFIG_PATH.parent / "logs" / "config_changes.log"
            audit_path.parent.mkdir(parents=True, exist_ok=True)
            with open(str(audit_path), "a", encoding="utf-8") as af:
                for line in lines:
                    af.write(line + "\n")
        except OSError as exc:
            log.warning("[CONFIG] Could not write audit log: %s", exc)

    @staticmethod
    def audit_line(section: str, key: str, old, new, source: str = "") -> str:
        """One audit row. Must stay at exactly three '|' fields — the history
        endpoint parses positionally and drops anything else."""
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        suffix = f" ({source})" if source else ""
        return f"{ts} | {section}.{key} | {old!r} → {new!r}{suffix}"

    def set_and_save(self, updates: list[dict]) -> list[str]:
        """
        Write a list of {section, key, value} updates to config.ini.
        Only EDITABLE_KEYS are allowed. Returns list of error messages.
        Reloads config into memory after writing.
        All changes are appended to logs/config_changes.log for audit.
        """
        errors = []
        with self._lock:
            # interpolation=None + inline_comment_prefixes: same flags as _load
            # so round-tripped values are clean (no stray '; comment' text)
            cfg = configparser.ConfigParser(
                interpolation=None,
                inline_comment_prefixes=(";", "#"),
            )
            cfg.read(str(CONFIG_PATH), encoding="utf-8-sig")

            applied = 0
            audit_lines = []

            for item in updates:
                section = item.get("section", "").lower().strip()
                key = item.get("key", "").lower().strip()
                value = str(item.get("value", "")).strip()

                if not section or not key:
                    errors.append(f"Invalid item (missing section or key): {item}")
                    continue
                if (section, key) in SENSITIVE_KEYS:
                    errors.append(f"{section}.{key}: sensitive key — edit config.ini directly")
                    continue
                if (section, key) not in EDITABLE_KEYS:
                    errors.append(f"{section}.{key}: not in editable allowlist")
                    continue

                # Non-blocking suspicious-edit alarm. User explicitly chose
                # portal-unrestricted edits, so we don't refuse — but we ping
                # Discord so a fat-finger (e.g. sl_pct=1, sl_pct=-1) is visible.
                # 2026-05-06 incident: sl_pct briefly typoed to -1 between
                # 17:15:49 and 18:08:33, fired SL on a fresh SPX position at
                # -1.7% pnl. Without an alarm the operator only noticed after
                # the trade closed.
                if key == "sl_pct" and section.startswith("trading"):
                    try:
                        new_sl = float(value)
                        suspicious = (new_sl >= 0 or abs(new_sl) < 5)
                        if suspicious:
                            try:
                                import asyncio as _aio
                                import app.analytics.trade_logger as _tl
                                _aio.create_task(_tl.log_critical(
                                    title=f"SUSPICIOUS CONFIG EDIT — {section}.{key}",
                                    message=(
                                        f"`{section}.{key}` set to **{new_sl}** via portal. "
                                        f"Tight or positive SL fires on tiny drawdowns. "
                                        f"If this was a typo, revert immediately."
                                    ),
                                    severity="warning",
                                    footer="Config alarm (non-blocking)",
                                ))
                            except Exception:
                                pass
                    except ValueError:
                        pass

                # Read old value for audit log
                try:
                    old_val = cfg.get(section, key) if cfg.has_option(section, key) else "<not set>"
                except Exception:
                    old_val = "<error>"

                if not cfg.has_section(section):
                    cfg.add_section(section)
                cfg.set(section, key, value)
                log.info("[CONFIG] SET %s.%s = %r  (was %r)", section, key, value, old_val)
                # A write that changes nothing is not a change. Auditing it
                # buries the real ones: config_changes.log is the fastest way
                # to find when a knob moved and to what, and no-op rows make
                # that scan noisier for no information. Still counted as
                # applied, so a caller writing an already-correct value gets
                # a success, and still written to config.ini so the file
                # keeps the key.
                if old_val != value:
                    audit_lines.append(self.audit_line(section, key, old_val, value))
                applied += 1

            if applied == 0 and errors:
                log.warning("[CONFIG] No settings applied. Errors: %s", errors)
                return errors

            try:
                with open(str(CONFIG_PATH), "w", encoding="utf-8") as f:
                    cfg.write(f)
                log.info("[CONFIG] config.ini written (%d key(s) updated)", applied)
            except OSError as exc:
                err = f"Failed to write config.ini: {exc}"
                log.error("[CONFIG] %s", err)
                errors.append(err)
                return errors

            # Append to audit log
            self.append_audit(audit_lines)

            self._load()
            log.info("[CONFIG] Reloaded into memory — %d key(s) active immediately", applied)

        return errors

    def as_dict(self) -> dict:
        """
        Return all config as a nested dict, masking sensitive values.
        Forwarder channel sub-section is included separately under
        the key 'forwarder_channels'.
        """
        with self._lock:
            out = {}
            for section in self._cfg.sections():
                if section == "forwarder.channels":
                    continue  # handled separately
                out[section] = {}
                for key, value in self._cfg.items(section):
                    if (section, key) in SENSITIVE_KEYS:
                        out[section][key] = "●●●●●●●●●●"
                    else:
                        out[section][key] = value

            # Forwarder channels as a separate dict
            if self._cfg.has_section("forwarder.channels"):
                out["forwarder_channels"] = dict(self._cfg.items("forwarder.channels"))

            return out


# ── Module-level singleton ────────────────────────────────────────────────────
cfg = _AppConfig()
