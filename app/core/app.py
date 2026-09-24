import asyncio
import hashlib
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.responses import Response, JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.core.db import init_db, get_db
from app.ingest.discord_listener import start_discord_listener
from app.ingest.forwarder import run_forwarder
from app.monitors.position_monitor import start_position_monitor
from app.monitors.reconciler import (backfill_missing_sell_fills, detect_phantom_closed,
                                     reconcile_once, start_reconciler)
from app.core.log_config import setup_logging, startup_logger
from app.core.config_manager import cfg as app_cfg
import app.analytics.trade_logger as trade_logger

# Setup structured logging (files under ./logs/)
setup_logging()
log = logging.getLogger(__name__)


# ── WebSocket connection manager ──────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self.active.add(ws)
        log.info("WS client connected (%d total)", len(self.active))

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            self.active.discard(ws)
        log.info("WS client disconnected (%d remaining)", len(self.active))

    async def broadcast(self, data: dict):
        payload = json.dumps(data, default=str)
        # Snapshot the set to avoid modification during iteration
        async with self._lock:
            clients = list(self.active)
        if not clients:
            return
        # Concurrent sends with a per-client timeout. The position monitor
        # awaits broadcast() inside its poll loop, so one stalled client
        # (backgrounded phone tab, full TCP buffer) must never block price
        # polling / exit checks. Timed-out clients are dropped — they
        # reconnect via the dashboard's auto-retry.
        results = await asyncio.gather(
            *(asyncio.wait_for(ws.send_text(payload), timeout=2.0) for ws in clients),
            return_exceptions=True,
        )
        dead = [ws for ws, r in zip(clients, results) if isinstance(r, BaseException)]
        if dead:
            async with self._lock:
                for ws in dead:
                    self.active.discard(ws)

    async def send_init(self, ws: WebSocket):
        """Push full current state to a newly-connected client."""
        db = next(get_db())
        try:
            from app.core.models import Alert, Position, Order
            from app.execution.broker_router import fetch_account_balance
            alerts    = [a.to_dict() for a in db.query(Alert).order_by(Alert.id.desc()).limit(50).all()]
            # Include OPEN/PARTIAL + recently CLOSED (last 8h) so dashboard shows today's P&L
            open_pos  = db.query(Position).filter(Position.status.in_(["OPEN", "PARTIAL"])).all()
            cutoff_8h = datetime.now(timezone.utc) - timedelta(hours=8)
            closed_pos = db.query(Position).filter(
                Position.status == "CLOSED",
                Position.close_time >= cutoff_8h,
            ).order_by(Position.close_time.desc()).all()
            positions = [p.to_dict() for p in open_pos + closed_pos]
            orders    = [o.to_dict() for o in db.query(Order).order_by(Order.id.desc()).limit(100).all()]
            balance   = await fetch_account_balance()
        finally:
            db.close()
        await ws.send_text(json.dumps(
            {"type": "init", "data": {"alerts": alerts, "positions": positions, "orders": orders, "balance": balance}},
            default=str
        ))


manager = ConnectionManager()
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parents[1]
STATIC_DIR = PROJECT_ROOT / "static"
DEFAULT_DASHBOARD = STATIC_DIR / "dashboard.html"

# In-memory cache for the dashboard shell. The HTML (~184KB) only changes on
# deploy, so re-reading it from disk on every "/" hit needlessly blocks the
# async event loop. Cache the bytes keyed on the file's mtime and expose a
# stable ETag so browser reloads revalidate to a 304 instead of re-pulling the
# full payload through the tunnel. Live data still flows via /api/* + /ws.
_dashboard_cache: dict[str, object] = {"mtime": None, "body": b"", "etag": ""}


def _load_dashboard() -> tuple[bytes, str] | None:
    """Return (body_bytes, etag) for dashboard.html, reading disk only when the
    file's mtime changes. Returns None if the file is missing."""
    try:
        mtime = DEFAULT_DASHBOARD.stat().st_mtime
    except FileNotFoundError:
        return None
    if _dashboard_cache["mtime"] != mtime:
        body = DEFAULT_DASHBOARD.read_bytes()
        _dashboard_cache.update(
            mtime=mtime,
            body=body,
            etag='"' + hashlib.sha1(body, usedforsecurity=False).hexdigest()[:16] + '"',
        )
    return _dashboard_cache["body"], _dashboard_cache["etag"]  # type: ignore[return-value]


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    startup_logger.info("=" * 70)
    _boot_broker = (app_cfg.get("trading", "broker", fallback="public") or "public").strip().lower()
    _boot_label = {"public": "Public.com", "ibkr": "IBKR", "tradier": "Tradier", "lime": "Lime Trader"}.get(_boot_broker, _boot_broker)
    startup_logger.info("APP START  |  Discord → %s", _boot_label)
    startup_logger.info("=" * 70)
    startup_logger.info("Config: dry_run=%s | ai_parser=%s | ai_scorer=%s | kelly=%s",
        app_cfg.get("trading", "dry_run", fallback="?"),
        app_cfg.get("trading", "ai_parser_enabled", fallback="?"),
        app_cfg.get("trading", "ai_scorer_enabled", fallback="?"),
        app_cfg.get("trading", "kelly_enabled", fallback="?"),
    )
    startup_logger.info("Guardrails: market_hours=%s | max_loss=$%s | max_positions=%s | max_trades=%s | max_cost=$%s",
        app_cfg.get("guardrails", "market_hours_only", fallback="?"),
        app_cfg.get("guardrails", "max_daily_loss", fallback="?"),
        app_cfg.get("guardrails", "max_open_positions", fallback="?"),
        app_cfg.get("guardrails", "max_daily_trades", fallback="?"),
        app_cfg.get("guardrails", "max_trade_cost", fallback="?"),
    )
    startup_logger.info("Channels monitored: %s", app_cfg.get("discord", "channel_ids", fallback="(none)"))
    startup_logger.info("Authors filter: %s", app_cfg.get("discord", "authors", fallback="(all)"))
    startup_logger.info("Trade log channel: %s", app_cfg.get("discord", "trade_log_channel_id", fallback="(none)"))

    startup_logger.info("[1/6] Initializing database...")
    init_db()
    startup_logger.info("[1/6] Database ready")

    # ── Startup reconciliation ──────────────────────────────────────────
    # Broker = source of truth. Pull broker state and correct local DB
    # BEFORE the Discord listener starts accepting new alerts, so the
    # first alert after restart cannot duplicate a position the broker
    # already holds (missed fill during downtime, manual trade in app, etc.).
    startup_logger.info("[2/6] Running startup reconciliation (broker → local DB)...")
    try:
        rec_stats = await reconcile_once(manager)
        startup_logger.info("[2/6] Reconciliation complete: %s", rec_stats)
        # Exits that landed while we were down (or after the fill poll gave
        # up) leave the realizing SELL unrecorded — no fill price, no
        # cost_basis, so Orders and the Calendar show the trade as blank.
        # Recover them from the broker's transaction history each boot.
        try:
            fb = await backfill_missing_sell_fills()
            if fb.get("repaired"):
                startup_logger.warning(
                    "[2/6] Fill backfill: %d unrecorded SELL fill(s) found over %dd — %s. %s",
                    len(fb["repaired"]), fb.get("days"),
                    ", ".join(f"{r['osi']} {r['qty']}@${r['price']:.2f} (${r['pnl']:+.0f})"
                              for r in fb["repaired"][:8]),
                    "WRITTEN to DB." if fb.get("wrote")
                    else "LOG ONLY — set trading.startup_fill_backfill_write=true to apply.",
                )
            else:
                startup_logger.info("[2/6] Fill backfill: clean (scanned=%d, %s)",
                                    fb.get("scanned", 0), fb.get("skipped", "all fills recorded"))
        except Exception as exc:
            startup_logger.error("[2/6] Fill backfill failed: %s", exc)

        try:
            phantom_stats = await detect_phantom_closed(lookback_days=90)
            if phantom_stats.get("phantoms"):
                startup_logger.warning(
                    "[2/6] Phantom CLOSED detection: %d local CLOSED rows have "
                    "no matching broker SELL in last 90d — see logs for OSI list "
                    "(set trading.purge_phantom_closed=true to auto-delete)",
                    len(phantom_stats["phantoms"]),
                )
            else:
                startup_logger.info("[2/6] Phantom CLOSED detection: clean (scanned=%d)",
                                    phantom_stats.get("scanned", 0))
        except Exception as exc:
            startup_logger.error("[2/6] Phantom detection failed: %s", exc)
    except Exception as exc:
        startup_logger.error("[2/6] Startup reconciliation failed: %s", exc)

    # Register components for health monitoring
    from app.monitors.health_monitor import register_component
    register_component("discord_listener")
    register_component("position_monitor")
    # forwarder registers only when it will actually run; a token-less
    # container otherwise reports a phantom "down" component forever.
    # An enabled forwarder that never connects (dead token) still alarms:
    # it is registered here and the staleness detector trips on it.
    if _forwarder_configured():
        register_component("forwarder")
    register_component("reconciler")
    register_component("database")
    register_component("public_api")
    register_component("premarket_prep")
    
    startup_logger.info("[2.5/6] Starting pre-market preparation scheduler...")
    t_premarket = asyncio.create_task(_monitored_premarket_scheduler())
    startup_logger.info("[2.5/6] Pre-market prep scheduler started")
    
    # Exit strategy: position_monitor.py is the single source of truth for
    # auto-exits (PT1/PT2/PT3/SL/TPT/TIME_EXIT). The old shadow exit_engine
    # was removed — it was log-only in PARALLEL mode and duplicating the
    # same exit logic in two places caused drift (e.g. PT3 floor bug had to
    # be fixed in both files on 2026-04-30).

    # Discord listener. Always runs so the ingest pipeline (process_message /
    # inject_forwarded) is registered. On a secondary with
    # discord_listener_enabled=false it builds the pipeline but skips the gateway
    # CONNECT (see start_discord_listener) — fed purely by the HTTP mirror.
    startup_logger.info("[4/6] Starting Discord listener...")
    t1 = asyncio.create_task(_monitored_discord_listener(manager))
    startup_logger.info("[4/6] Discord listener task created")

    # Discord Signal Intelligence worker (BacteriaNFA-style chart messages)
    if app_cfg.getboolean("signals", "enabled", fallback=False):
        import app.ingest.signal_intel as signal_intel
        signal_intel.start()
        startup_logger.info("[4/6] Signal intelligence worker started (author=%s channel=%s mode=%s)",
            app_cfg.get("signals", "author", fallback="?"),
            app_cfg.get("signals", "channel_id", fallback="?"),
            app_cfg.get("signals", "action_mode", fallback="display"),
        )

    startup_logger.info("[5/6] Starting position monitor...")
    t2 = asyncio.create_task(_monitored_position_monitor(manager))
    startup_logger.info("[5/6] Position monitor task created (poll_interval=%ss)",
        app_cfg.get("trading", "poll_interval_seconds", fallback="2"))

    startup_logger.info("[5/6] Starting message forwarder...")
    t3 = asyncio.create_task(_monitored_forwarder())
    startup_logger.info("[5/6] Forwarder task created")

    startup_logger.info("[6/6] Starting daily summary scheduler + periodic reconciler...")
    t4 = asyncio.create_task(_daily_summary_scheduler())
    t5 = asyncio.create_task(_monitored_reconciler(manager))
    startup_logger.info("[6/6] Daily summary + reconciler tasks created (reconcile_interval=%ss)",
        app_cfg.get("trading", "reconcile_interval_seconds", fallback="60"))

    asyncio.create_task(_resume_pending_orders())
    startup_logger.info("Pending order monitor queued for startup resume")

    asyncio.create_task(_prewarm_open_positions())
    startup_logger.info("Open-position quote pre-warm queued for startup")

    asyncio.create_task(_stale_heartbeat_checker())
    startup_logger.info("Stale heartbeat checker queued")

    asyncio.create_task(_vix_refresher(), name="vix.refresher")
    startup_logger.info("VIX refresher queued (active only while vix_sizing_enabled, broker=public|ibkr)")

    # Event-loop lag sampler — publishes event_loop_lag_ms for /metrics. Rising
    # lag = the loop is being blocked (the symptom the to_thread offload targets).
    from app.core.metrics import sample_loop_lag, sample_business_metrics
    asyncio.create_task(sample_loop_lag(), name="metrics.loop_lag")
    asyncio.create_task(sample_business_metrics(), name="metrics.business")
    startup_logger.info("Event-loop lag + business (P&L) metric samplers started")

    # Pre-warm dashboard read-path modules. The GET handlers import these lazily
    # (to avoid import cycles at module load), so the FIRST hit per endpoint pays
    # a one-time import cost — measured at 100–270ms cold vs ~2ms warm. Importing
    # them here at startup moves that cost off the first user request; the lazy
    # `from ... import` inside each handler then resolves from sys.modules cache.
    try:
        import importlib
        for _m in (
            "app.analytics.analytics",
            "app.monitors.health_monitor",
            "app.monitors.greeks_monitor",
            "app.monitors.premarket_prep",
            "app.monitors.api_monitor",
            "app.core.metrics",
        ):
            importlib.import_module(_m)
        _load_dashboard()  # prime the 184KB shell cache too
        startup_logger.info("Dashboard read-path modules pre-warmed")
    except Exception as exc:
        startup_logger.warning("Dashboard pre-warm skipped: %s", exc)

    startup_logger.info("ALL SYSTEMS ONLINE — Bot is running")
    log.info("Backend started — Discord listener, position monitor, forwarder, reconciler, and trade logger running")
    yield

    startup_logger.info("APP SHUTDOWN initiated — cancelling background tasks")
    t_premarket.cancel()
    t1.cancel()
    t2.cancel()
    t3.cancel()
    t4.cancel()
    t5.cancel()
    
    # Stop signal intelligence worker
    try:
        import app.ingest.signal_intel as signal_intel
        await signal_intel.stop()
    except Exception as exc:
        startup_logger.warning("signal_intel shutdown error: %s", exc)

    # Cleanly close the shared trade-logger HTTP session to avoid ResourceWarning
    await trade_logger.close_http_session()
    startup_logger.info("APP SHUTDOWN complete")


async def _monitored_discord_listener(ws_manager):
    """Wrap Discord listener with health monitoring."""
    from app.monitors.health_monitor import heartbeat, report_error
    while True:
        try:
            await start_discord_listener(ws_manager)
            await heartbeat("discord_listener")
        except Exception as e:
            await report_error("discord_listener", str(e))
            log.error("Discord listener error: %s", e)
        await asyncio.sleep(5)


async def _monitored_position_monitor(ws_manager):
    """Wrap position monitor with health monitoring + restart on crash.
    Without the while-True restart, a single transient exception killed the
    monitor for the rest of the process — no auto-exits would fire (loses
    money). Mirrors the discord_listener pattern."""
    from app.monitors.health_monitor import heartbeat, report_error
    while True:
        try:
            await start_position_monitor(ws_manager)
            await heartbeat("position_monitor")
        except Exception as e:
            await report_error("position_monitor", str(e))
            log.error("Position monitor error: %s — restarting in 5s", e)
        await asyncio.sleep(5)


async def _stale_heartbeat_checker():
    """Every 30s, scan registered components for stale heartbeats and Discord-ping
    on first transition to stale. Bridges the 2026-05-05 gap where forwarder hung
    77 min with no observable signal.
    """
    from app.monitors.health_monitor import get_health_monitor
    max_age = float(app_cfg.get("trading", "stale_heartbeat_max_age_seconds", fallback="120") or 120)
    while True:
        try:
            await get_health_monitor().check_stale_heartbeats(max_age_seconds=max_age)
        except Exception as exc:
            log.error("[HEALTH] stale heartbeat checker error: %s", exc)
        await asyncio.sleep(30)


def _forwarder_configured() -> bool:
    """True when the forwarder is enabled AND has a usable user token.
    A container deliberately running without a token (secondary broker VM)
    is not a degraded forwarder — it has no forwarder at all."""
    if not app_cfg.getboolean("trading", "forwarder_enabled", fallback=True):
        log.info("Forwarder disabled via config (forwarder_enabled=false)")
        return False
    token = (os.getenv("DISCORD_USER_TOKEN", "").strip()
             or app_cfg.get("forwarder", "user_token", fallback="").strip())
    if not token or "YOUR_DISCORD" in token.upper():
        log.info("Forwarder disabled: set DISCORD_USER_TOKEN env var or user_token in [forwarder] of config.ini")
        return False
    return True


async def _monitored_forwarder():
    """Wrap forwarder with health monitoring."""
    from app.monitors.health_monitor import heartbeat, report_error
    if not _forwarder_configured():
        return
    while True:
        try:
            await run_forwarder()
            await heartbeat("forwarder")
        except Exception as exc:
            await report_error("forwarder", str(exc))
            log.error("Forwarder crashed: %s — restarting in 5s", exc)
        await asyncio.sleep(5)


async def _monitored_reconciler(ws_manager):
    """Wrap reconciler with health monitoring + restart on crash."""
    from app.monitors.health_monitor import heartbeat, report_error
    while True:
        try:
            await start_reconciler(ws_manager)
            await heartbeat("reconciler")
        except Exception as e:
            await report_error("reconciler", str(e))
            log.error("Reconciler error: %s — restarting in 30s", e)
        await asyncio.sleep(30)


async def _monitored_premarket_scheduler():
    """Wrap pre-market prep with health monitoring + restart on crash."""
    from app.monitors.health_monitor import heartbeat, report_error
    from app.monitors.premarket_prep import premarket_scheduler
    while True:
        try:
            await premarket_scheduler()
            await heartbeat("premarket_prep")
        except Exception as e:
            await report_error("premarket_prep", str(e))
            log.error("Pre-market prep error: %s — restarting in 60s", e)
        await asyncio.sleep(60)


async def _start_forwarder():
    """Start the forwarder only if properly configured and enabled."""
    if not _forwarder_configured():
        return
    try:
        await run_forwarder()
    except Exception as exc:
        log.error("Forwarder crashed: %s", exc)


# Strong references to fire-and-forget background tasks. Without these the GC
# can collect the task mid-run (Python 3.11+ doc warning), and any unhandled
# exception in the coroutine is swallowed silently. Done-callback logs errors
# and discards the reference.
_background_tasks: set[asyncio.Task] = set()


def _spawn_tracked(coro, label: str) -> asyncio.Task:
    """asyncio.create_task with a done-callback that logs exceptions and a
    strong reference so the task isn't garbage-collected mid-run."""
    task = asyncio.create_task(coro)
    _background_tasks.add(task)

    def _done(t: asyncio.Task):
        _background_tasks.discard(t)
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            log.error("Background task %s raised: %s", label, exc, exc_info=exc)

    task.add_done_callback(_done)
    return task


async def _resume_pending_orders():
    """On startup, resume background fill polling for any PENDING orders."""
    from app.core.models import Order
    from app.execution.broker_router import get_executor
    from app.core.db import SessionLocal

    await asyncio.sleep(3)  # let Discord and API clients initialize first

    db = SessionLocal()
    try:
        pending = db.query(Order).filter(Order.status == "PENDING").all()
        if not pending:
            return

        log.info("Resuming fill monitoring for %d pending order(s)", len(pending))
        executor = get_executor(manager)

        for order in pending:
            if not order.public_order_id:
                continue
            _spawn_tracked(executor._poll_order_fill(
                order_db_id=order.id,
                public_oid=order.public_order_id,
                alert=None,
                alert_db_id=None,
                side=order.side,
                qty=order.quantity,
            ), label=f"poll_fill[order={order.id}]")
            log.info("  Monitoring order #%d (%s %s)", order.id, order.side, order.osi_symbol)
    finally:
        db.close()


async def _prewarm_open_positions():
    """On startup, open the streaming quote subscription for every OPEN/PARTIAL
    position so the first SL/PT evaluation reads a warm ticker instead of paying
    the cold first-tick wait (~quote_wait_ms + validation). Without this, an SL on
    a contract that was never mentioned in chat this session (e.g. carried from a
    prior run) eats that cold penalty on its first tick — exactly when it must be
    fast. No-op unless broker=ibkr and [ibkr] prewarm_quote_enabled is on."""
    from app.core.models import Position
    from app.execution.broker_router import prewarm_symbol
    from app.core.db import SessionLocal

    await asyncio.sleep(3)  # let the IBKR client connect first

    db = SessionLocal()
    try:
        open_positions = db.query(Position).filter(
            Position.status.in_(["OPEN", "PARTIAL"])
        ).all()
        symbols = [p.osi_symbol for p in open_positions if p.osi_symbol]
        if not symbols:
            return
        log.info("Pre-warming quote subscriptions for %d open position(s)", len(symbols))
        for osi in symbols:
            prewarm_symbol(osi)  # fire-and-forget; internally broker/flag-gated
    finally:
        db.close()


app = FastAPI(title="Discord Trader API", lifespan=lifespan)

# CORS — comma-separated list via CORS_ALLOW_ORIGINS env var. Default keeps
# the prior `*` behavior so existing Cloudflare Tunnel access doesn't break;
# user is encouraged to lock this down by setting CORS_ALLOW_ORIGINS to the
# tunnel hostname only.
_cors_origins = os.getenv("CORS_ALLOW_ORIGINS", "*").strip()
if _cors_origins == "*":
    _origins_list = ["*"]
else:
    _origins_list = [o.strip() for o in _cors_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    # Wildcard default is intentional and env-lockable (CORS_ALLOW_ORIGINS):
    # the app sits behind Cloudflare Access + app-layer JWT/API-key auth, so
    # browser CORS is not the security boundary here.
    allow_origins=_origins_list,  # nosemgrep: wildcard-cors
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── App-layer auth (defense-in-depth behind Cloudflare Access) ────────────────
# Two independent locks inside the app so access never relies on trusting the
# network (the container binds 127.0.0.1:8000 and CF forwards from loopback, so
# a loopback exemption would be a hole — there is none):
#
#   1. Cloudflare Access JWT (preferred, automatic). CF injects a signed token
#      on every proxied request; we verify it here. Zero manual steps, works
#      from any device once the user logs in through Access. When configured
#      (CF_ACCESS_TEAM_DOMAIN + CF_ACCESS_AUD), ALL endpoints are gated.
#   2. Shared API key fallback (X-API-Key / ?key=) for scripts & automation
#      that don't pass through Cloudflare. When this is the ONLY thing
#      configured, only STATE-CHANGING methods are gated so the human dashboard
#      still loads (it's protected by CF Access at the edge).
#
# Both unset → no-op (local dev), with a warning.
import app.core.cf_access as cf_access

_DASH_KEY = os.getenv("DASHBOARD_API_KEY", "").strip()
_PROTECTED_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
# Infra endpoints hit without any credential (container healthcheck, Prometheus
# scrape on loopback) — never gate these.
_AUTH_EXEMPT_PATHS = {"/api/health", "/metrics"}
_AUTH_ENABLED = cf_access.enabled or bool(_DASH_KEY)

if cf_access.enabled:
    log.info("App-layer auth: Cloudflare Access JWT verification ENABLED "
             "(all endpoints gated; API-key fallback %s).",
             "on" if _DASH_KEY else "off")
elif _DASH_KEY:
    log.info("App-layer auth: API-key only (mutating endpoints gated). Set "
             "CF_ACCESS_TEAM_DOMAIN + CF_ACCESS_AUD to gate reads too.")
else:
    log.warning("App-layer auth DISABLED — no DASHBOARD_API_KEY and no CF Access "
                "config. Relying on Cloudflare Access at the edge ONLY.")


@app.middleware("http")
async def require_auth(request: Request, call_next):
    if not _AUTH_ENABLED or request.url.path in _AUTH_EXEMPT_PATHS:
        return await call_next(request)

    # With CF Access on, gate everything (reads + writes). With API key only,
    # gate just the state-changing methods (dashboard reads stay behind CF edge).
    if not cf_access.enabled and request.method not in _PROTECTED_METHODS:
        return await call_next(request)

    # 1) Cloudflare Access JWT — injected automatically by CF on proxied reqs.
    if cf_access.enabled:
        token = (request.headers.get("Cf-Access-Jwt-Assertion")
                 or request.cookies.get("CF_Authorization", ""))
        if await asyncio.to_thread(cf_access.verify, token) is not None:
            return await call_next(request)

    # 2) Shared API key fallback (scripts/automation). Constant-time compare.
    if _DASH_KEY:
        import hmac
        supplied = request.headers.get("X-API-Key") or request.query_params.get("key") or ""
        if hmac.compare_digest(supplied, _DASH_KEY):
            return await call_next(request)

    return JSONResponse({"detail": "unauthorized"}, status_code=401)


# ── WebSocket ─────────────────────────────────────────────────────────────────

async def _ws_authorized(ws: WebSocket) -> bool:
    """Mirror require_auth for the WebSocket handshake. HTTP middleware does not
    run for the websocket ASGI scope, so /ws (which streams full account state)
    must gate itself or the app-layer auth guarantee has a hole for the most
    sensitive endpoint. Same policy as the HTTP path:
      • auth disabled entirely       → allow (local dev)
      • API-key-only mode            → /ws is a read; reads stay behind the CF
                                        edge (mirrors the mutating-only HTTP gate)
      • CF Access enabled            → require a valid CF JWT or the API key
    """
    if not _AUTH_ENABLED:
        return True
    if not cf_access.enabled:
        return True

    token = (ws.headers.get("cf-access-jwt-assertion")
             or ws.cookies.get("CF_Authorization", ""))
    if token and await asyncio.to_thread(cf_access.verify, token) is not None:
        return True

    if _DASH_KEY:
        import hmac
        supplied = ws.headers.get("x-api-key") or ws.query_params.get("key") or ""
        if hmac.compare_digest(supplied, _DASH_KEY):
            return True

    return False


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    # Authenticate BEFORE accept() — an unauthenticated handshake is rejected
    # (HTTP 403) and never receives the send_init account snapshot.
    if not await _ws_authorized(ws):
        await ws.close(code=1008)  # policy violation
        return
    await manager.connect(ws)
    await manager.send_init(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(ws)


# ── Helper: fire a manual exit ────────────────────────────────────────────────

async def _fire_manual_exit(position_id: int) -> dict:
    """
    Place an immediate SELL order for all remaining contracts of a position.
    Uses the last known current_price as the limit, with 5% downside buffer
    so the order is likely to fill quickly.
    """
    from app.core.models import Position
    from app.execution.broker_router import get_executor
    from decimal import Decimal

    db = next(get_db())
    try:
        pos = db.query(Position).filter(
            Position.id == position_id,
            Position.status.in_(["OPEN", "PARTIAL"]),
        ).first()
        if not pos:
            raise HTTPException(status_code=404, detail="Position not found or already closed")
        if pos.remaining <= 0:
            raise HTTPException(status_code=400, detail="No contracts remaining")

        from app.execution.spread_executor import is_combo
        qty = pos.remaining
        base_price = pos.current_price or pos.avg_price or 0.05

        if is_combo(pos.osi_symbol) or getattr(pos, "is_credit", False):
            from zoneinfo import ZoneInfo
            from app.monitors.position_monitor import _close_if_expired, _expiry_date
            exp = _expiry_date(pos)
            now_et = datetime.now(ZoneInfo("America/New_York"))
            expired = bool(exp) and (
                (exp < now_et.date())
                or (exp == now_et.date() and now_et.hour >= 16)
            )
            # 0DTE combo already dropped at the broker. Preview/place on a
            # dead OCC symbol hangs or 5xx's HTML through the proxy, and the
            # dashboard's r.json() then throws JSON.parse. Book it locally.
            if expired:
                if getattr(pos, "is_credit", False):
                    pos.remaining = 0
                    pos.status = "CLOSED"
                    pos.close_time = datetime.now(timezone.utc)
                    pos.close_price = pos.current_price or pos.avg_price or 0.0
                    db.commit()
                    # This branch books real P&L off a mark and places no
                    # broker order, so without a line here the only trace is
                    # the row changing under the operator: position 13 went
                    # CLOSED at -$5 on 2026-09-15 and no stream recorded it.
                    from app.core.log_config import exit_logger
                    pnl = (float(pos.avg_price or 0) - float(pos.close_price or 0)) * 100 * qty
                    exit_logger.info(
                        "SETTLED | %s | %dx credit %.2f → mark %.2f | P&L $%.0f | "
                        "[EXPIRY_LOCAL] no broker order, reconcile against the SPX print",
                        pos.osi_symbol, qty, float(pos.avg_price or 0),
                        float(pos.close_price or 0), pnl,
                    )
                elif not _close_if_expired(db, pos):
                    raise HTTPException(
                        status_code=400,
                        detail="Expired combo could not be closed. Use Force Close.",
                    )
                db.refresh(pos)
                await manager.broadcast(
                    {"type": "position_update", "data": pos.to_dict()}
                )
                return {
                    "status": "ok",
                    "osi": pos.osi_symbol,
                    "qty": qty,
                    "note": "expired — closed locally, no broker order",
                }
            from app.execution.broker_router import get_spread_executor
            se = get_spread_executor(manager)
            width = float(getattr(pos, "spread_width", 0) or 0)
            mark = float(base_price)
            try:
                if pos.is_credit:
                    # Pay up through the mark so a dashboard Close actually fills.
                    # Cap at width - 0.05 (max debit).
                    cap = (width - 0.05) if width > 0.1 else mark * 2
                    debit = min(cap, max(mark * 1.4, mark + 0.25))
                    await se.close_open_position(pos, qty, round(debit, 2), trigger="MANUAL")
                else:
                    credit = max(0.05, mark * 0.6)
                    await se.close_open_position(pos, qty, round(credit, 2), trigger="MANUAL")
            except Exception as exc:
                log.error("Manual spread exit broker error for %s: %s", pos.osi_symbol, exc)
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Broker rejected close for {pos.osi_symbol}: {exc}. "
                        "Use 'Force Close' to remove this position from tracking."
                    ),
                ) from exc
            db.refresh(pos)
            return {"status": "ok", "osi": pos.osi_symbol, "qty": qty, "note": "spread close submitted"}

        executor = get_executor(manager)
        limit_px = executor._compute_limit(Decimal(str(round(base_price, 2))), "SELL")

        try:
            await executor._place_sell(db, pos, qty, limit_px, "MANUAL")
        except Exception as exc:
            msg = str(exc)
            # External-fill recovery: user closed via mobile app / broker UI,
            # broker shows fewer (or zero) contracts than DB. Reconcile then
            # mark CLOSED locally so the UI matches reality. 2026-05-06: this
            # exact 400 from a Public.com mobile-app close required force-close.
            if "exceeds the amount you have available" in msg.lower() or "exceeds the amount" in msg.lower():
                log.warning(
                    "[MANUAL_EXIT] External fill detected for %s (broker has fewer contracts than DB) — reconciling",
                    pos.osi_symbol,
                )
                try:
                    from app.monitors.reconciler import reconcile_once
                    await reconcile_once(manager)
                except Exception as r_exc:
                    log.error("[MANUAL_EXIT] reconcile_once failed: %s", r_exc)
                # Re-read after reconcile
                db.expire(pos)
                pos = db.query(Position).filter(Position.id == position_id).first()
                if pos and (pos.remaining <= 0 or pos.status == "CLOSED"):
                    try:
                        import app.analytics.trade_logger as _tl
                        await _tl.log_critical(
                            title=f"EXTERNAL FILL RECOVERED — {pos.osi_symbol}",
                            message=(
                                f"Manual exit hit `exceeds amount available` — broker shows "
                                f"0 contracts for `{pos.osi_symbol}` (likely closed via mobile "
                                f"app). Reconciler synced local state to match. "
                                f"No broker order placed."
                            ),
                            severity="info",
                            footer="External-fill auto-recovery",
                        )
                    except Exception:
                        pass
                    return {
                        "status": "ok",
                        "osi": pos.osi_symbol,
                        "qty": 0,
                        "note": "Closed externally (mobile app / broker UI) — reconciled locally, no broker order needed",
                    }
                # Reconciler didn't catch it; force-close locally as a last resort
                # (broker is authoritative — qty already 0 there).
                if pos:
                    log.warning(
                        "[MANUAL_EXIT] Reconciler missed it; force-closing #%d (%s) locally",
                        pos.id, pos.osi_symbol,
                    )
                    pos.remaining = 0
                    pos.status = "CLOSED"
                    pos.close_time = datetime.now(timezone.utc)
                    pos.close_price = pos.current_price or pos.avg_price or 0.0
                    db.commit()
                    db.refresh(pos)
                    await manager.broadcast({"type": "position_update", "data": pos.to_dict()})
                    return {
                        "status": "ok",
                        "osi": pos.osi_symbol,
                        "qty": 0,
                        "note": "Broker shows 0 contracts — force-closed locally to match (likely closed via mobile app)",
                    }
            # Real broker rejection (invalid symbol, expired, etc.)
            log.error("Manual exit broker error for %s: %s", pos.osi_symbol, exc)
            raise HTTPException(
                status_code=400,
                detail=f"Broker rejected sell for {pos.osi_symbol}: {exc}. Use 'Force Close' to remove this position from tracking.",
            ) from exc

        await manager.broadcast({
            "type": "auto_exit",
            "data": {
                "message": f"Manual exit: {pos.symbol} {pos.strike}{pos.option_type} - {qty} contracts",
                "severity": "info",
                "osi": pos.osi_symbol,
                "rule": "MANUAL",
            },
        })
        return {"status": "ok", "osi": pos.osi_symbol, "qty": qty}
    finally:
        db.close()


async def _force_close_position(position_id: int) -> dict:
    """
    Force-close a position locally WITHOUT placing a broker order.
    Used for test/bogus positions or when the broker has already delisted the symbol.
    Marks the position CLOSED in DB and broadcasts the update.
    """
    from app.core.models import Position

    db = next(get_db())
    try:
        pos = db.query(Position).filter(
            Position.id == position_id,
            Position.status.in_(["OPEN", "PARTIAL"]),
        ).first()
        if not pos:
            raise HTTPException(status_code=404, detail="Position not found or already closed")

        osi = pos.osi_symbol
        pos.remaining = 0
        pos.status = "CLOSED"
        pos.close_time = datetime.now(timezone.utc)
        # Use current_price as close_price if available, otherwise avg_price
        pos.close_price = pos.current_price or pos.avg_price or 0.0
        db.commit()
        db.refresh(pos)

        await manager.broadcast({"type": "position_update", "data": pos.to_dict()})
        await manager.broadcast({
            "type": "auto_exit",
            "data": {
                "message": f"Force-closed (local only): {pos.symbol} {pos.strike}{pos.option_type} — no broker order placed",
                "severity": "info",
                "osi": osi,
                "rule": "FORCE_CLOSE",
            },
        })
        log.warning("[FORCE_CLOSE] Position #%d (%s) closed locally without broker order", position_id, osi)
        return {"status": "ok", "osi": osi, "note": "Position closed locally — no sell order was placed on the broker"}
    finally:
        db.close()


# ── Config edit request model ─────────────────────────────────────────────────

class ConfigItem(BaseModel):
    section: str
    key: str
    value: str


class JournalNote(BaseModel):
    date: str   # YYYY-MM-DD (ET trading day)
    note: str   # empty string deletes the note


# ── REST endpoints ────────────────────────────────────────────────────────────

@app.get("/api/positions")
async def get_positions(include_closed: bool = False, since_hours: int = 8):
    """
    Return positions for the dashboard.
    By default: OPEN and PARTIAL positions only.
    With ?include_closed=true: also includes CLOSED positions from the
    last `since_hours` hours (default 8h) so you can review today's exits.
    """
    db = next(get_db())
    try:
        from app.core.models import Position
        query = db.query(Position).filter(Position.status.in_(["OPEN", "PARTIAL"]))
        results = list(query.all())

        if include_closed:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
            closed = db.query(Position).filter(
                Position.status == "CLOSED",
                Position.close_time >= cutoff,
            ).order_by(Position.close_time.desc()).all()
            results.extend(closed)

        return [p.to_dict() for p in results]
    finally:
        db.close()


@app.get("/api/orders")
async def get_orders(days: int = 7, limit: int = 500):
    """Return orders placed within the last `days` days (days=0 → no date filter)."""
    db = next(get_db())
    try:
        from app.core.models import Order
        q = db.query(Order)
        if days and days > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            q = q.filter(Order.placed_at >= cutoff)
        return [o.to_dict() for o in q.order_by(Order.id.desc()).limit(limit).all()]
    finally:
        db.close()


@app.get("/api/alerts")
async def get_alerts(days: int = 7, limit: int = 500):
    """Return alerts received within the last `days` days (days=0 → no date filter)."""
    db = next(get_db())
    try:
        from app.core.models import Alert
        q = db.query(Alert)
        if days and days > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            q = q.filter(Alert.timestamp >= cutoff)
        return [a.to_dict() for a in q.order_by(Alert.id.desc()).limit(limit).all()]
    finally:
        db.close()


@app.get("/api/signals/feed")
async def signals_feed(limit: int = 20):
    """Latest discord signals from the tracked author. Newest first."""
    db = next(get_db())
    try:
        from app.core.models import DiscordSignal
        rows = (db.query(DiscordSignal)
                  .order_by(DiscordSignal.id.desc())
                  .limit(max(1, min(200, limit)))
                  .all())
        return [r.to_dict() for r in rows]
    finally:
        db.close()


@app.get("/api/signals/history")
async def signals_history(days: int = 7, bias: str | None = None, limit: int = 500, offset: int = 0):
    """Paginated historical signals with optional bias filter."""
    db = next(get_db())
    try:
        from app.core.models import DiscordSignal
        q = db.query(DiscordSignal)
        if days and days > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            q = q.filter(DiscordSignal.timestamp >= cutoff)
        if bias:
            q = q.filter(DiscordSignal.bias == bias.lower())
        total = q.count()
        rows = (q.order_by(DiscordSignal.timestamp.desc(), DiscordSignal.id.desc())
                 .offset(max(0, offset))
                 .limit(max(1, min(1000, limit)))
                 .all())
        return {"total": total, "items": [r.to_dict() for r in rows]}
    finally:
        db.close()


@app.get("/api/signals/stats")
async def signals_stats(days: int = 30):
    """Aggregate stats: counts by bias, recent confidence avg, top recurring levels."""
    db = next(get_db())
    try:
        from app.core.models import DiscordSignal
        from collections import Counter
        import json as _json
        cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, days))
        rows = db.query(DiscordSignal).filter(DiscordSignal.timestamp >= cutoff).all()
        bias_counts = Counter([(r.bias or "neutral") for r in rows])
        confs = [r.confidence for r in rows if r.confidence is not None]
        avg_conf = round(sum(confs) / len(confs), 1) if confs else 0.0
        level_counter = Counter()
        for r in rows:
            try:
                lvls = _json.loads(r.key_levels) if r.key_levels else []
            except Exception:
                lvls = []
            for v in lvls:
                level_counter[v] += 1
        top_levels = [{"level": lvl, "count": cnt}
                      for lvl, cnt in level_counter.most_common(10)]
        return {
            "period_days": days,
            "total_signals": len(rows),
            "bias_counts": dict(bias_counts),
            "avg_confidence": avg_conf,
            "top_levels": top_levels,
            "vision_available": _vision_status(),
        }
    finally:
        db.close()


def _vision_status() -> bool:
    try:
        import app.ingest.vision_ocr as vision_ocr
        return vision_ocr.is_available()
    except Exception:
        return False


@app.get("/api/signals/{signal_id}")
async def signals_get(signal_id: int):
    db = next(get_db())
    try:
        from app.core.models import DiscordSignal
        r = db.query(DiscordSignal).filter(DiscordSignal.id == signal_id).first()
        if not r:
            return {"error": "not found"}
        return r.to_dict()
    finally:
        db.close()


@app.post("/api/positions/{position_id}/exit")
async def manual_exit_position(position_id: int):
    """
    Manually exit a single open position immediately.
    Places a limit SELL order at current_price × 0.95 (very likely to fill).
    """
    return await _fire_manual_exit(position_id)


@app.post("/api/positions/{position_id}/force-close")
async def force_close_position(position_id: int):
    """
    Force-close a position locally WITHOUT placing a broker sell order.
    Used for test positions, delisted symbols, or positions the broker rejects.
    """
    return await _force_close_position(position_id)


class ExitOverrides(BaseModel):
    """Per-position exit numbers. Omit a field to leave it alone; send null to
    clear it back to the config default."""
    target_pct: float | None = None
    target_sell: float | None = None   # 0.5 = half out at the target, 1.0 = hard exit
    stop_pct: float | None = None
    auto_exit: bool | None = None
    # Sent alongside auto_exit so arming ONE trade is a single write. Arming
    # defaults to stop-only (pts_disabled=true): the stop is what the P&L
    # forensics justify, while the profit target is the analyst's call.
    pts_disabled: bool | None = None
    clear_target: bool = False
    clear_target_sell: bool = False
    clear_stop: bool = False
    clear_auto_exit: bool = False


@app.post("/api/positions/{position_id}/exit-overrides")
async def set_position_exit_overrides(position_id: int, body: ExitOverrides):
    """Set this position's own take-profit %, stop %, and auto-exit switch.

    NULL in the column means "follow the resolved default and keep following
    it" — so a position nobody edited still tracks a later config change, and
    only an explicit value detaches. The clear_* flags write NULL back.

    Deliberately unclamped: risk knobs are portal-managed and code must not
    impose its own floors on them. The UI offers sane steps; this accepts what
    the operator asks for.
    """
    from app.core.models import Position
    db = next(get_db())
    try:
        pos = db.query(Position).filter(
            Position.id == position_id,
            Position.status.in_(["OPEN", "PARTIAL"]),
        ).first()
        if not pos:
            raise HTTPException(status_code=404, detail="Position not found or already closed")

        changed = []
        if body.clear_target:
            pos.pt_pct_override, _ = None, changed.append("target→default")
        elif body.target_pct is not None:
            pos.pt_pct_override, _ = float(body.target_pct), changed.append(f"target={body.target_pct:+g}%")
        if body.clear_target_sell:
            pos.pt_sell_override, _ = None, changed.append("target size→default")
        elif body.target_sell is not None:
            # A fraction, not a percent. Bounded 0-1 because it multiplies a
            # contract count: this is a unit conversion, not a risk floor.
            frac = max(0.01, min(1.0, float(body.target_sell)))
            pos.pt_sell_override, _ = frac, changed.append(
                f"target sells {'everything' if frac >= 1 else f'{frac * 100:.0f}%'}")
        if body.clear_stop:
            pos.sl_pct_override, _ = None, changed.append("stop→default")
        elif body.stop_pct is not None:
            pos.sl_pct_override, _ = float(body.stop_pct), changed.append(f"stop={body.stop_pct:+g}%")
        if body.pts_disabled is not None:
            pos.pts_disabled, _ = bool(body.pts_disabled), changed.append(
                f"targets={'off' if body.pts_disabled else 'on'}")
        if body.clear_auto_exit:
            pos.auto_exit_override, _ = None, changed.append("auto-exit→default")
        elif body.auto_exit is not None:
            pos.auto_exit_override, _ = bool(body.auto_exit), changed.append(
                f"auto-exit={'on' if body.auto_exit else 'off'}")

        if not changed:
            return {"status": "ok", "id": pos.id, "changed": []}

        db.commit()
        db.refresh(pos)
        await manager.broadcast({"type": "position_update", "data": pos.to_dict()})
        await manager.broadcast({"type": "auto_exit", "data": {
            "message": f"Exit settings for {pos.symbol} {pos.strike}{pos.option_type}: {', '.join(changed)}",
            "severity": "info", "osi": pos.osi_symbol, "rule": "EXIT_OVERRIDE",
        }})
        # Same audit trail as a portal config write — a per-position risk change
        # must be as reconstructable as a global one.
        log.info("[EXIT_OVERRIDE] Position #%d (%s): %s", position_id, pos.osi_symbol, "; ".join(changed))
        try:
            from app.core.log_config import exit_logger
            exit_logger.info("OVERRIDE | %s | %s | remaining=%d",
                             pos.osi_symbol, "; ".join(changed), pos.remaining or 0)
        except Exception:
            pass
        return {"status": "ok", "id": pos.id, "osi": pos.osi_symbol, "changed": changed,
                "exitPlan": pos.exit_plan()}
    finally:
        db.close()


@app.post("/api/positions/{position_id}/toggle-pts")
async def toggle_position_pts(position_id: int):
    """
    Toggle pts_disabled flag for a single position. When True, PT1/PT2/PT3/TPT
    are skipped for this position; only SL (and manual exits) can close it.
    """
    from app.core.models import Position
    db = next(get_db())
    try:
        pos = db.query(Position).filter(
            Position.id == position_id,
            Position.status.in_(["OPEN", "PARTIAL"]),
        ).first()
        if not pos:
            raise HTTPException(status_code=404, detail="Position not found or already closed")
        pos.pts_disabled = not bool(pos.pts_disabled)
        db.commit()
        db.refresh(pos)
        await manager.broadcast({"type": "position_update", "data": pos.to_dict()})
        await manager.broadcast({
            "type": "auto_exit",
            "data": {
                "message": f"PTs {'disabled' if pos.pts_disabled else 'enabled'} for {pos.symbol} {pos.strike}{pos.option_type} — SL still active",
                "severity": "info",
                "osi": pos.osi_symbol,
                "rule": "PTS_TOGGLE",
            },
        })
        log.info("[PTS_TOGGLE] Position #%d (%s) pts_disabled=%s", position_id, pos.osi_symbol, pos.pts_disabled)
        return {"status": "ok", "id": pos.id, "osi": pos.osi_symbol, "pts_disabled": pos.pts_disabled}
    finally:
        db.close()


@app.post("/api/positions/{position_id}/toggle-breakeven-lock")
async def toggle_position_breakeven_lock(position_id: int):
    """
    Toggle breakeven_lock_disabled flag for a single position. When True,
    the +20%-peak → -1% SL floor logic is skipped; the position keeps its
    full configured SL (e.g. Sarang's -50% swing room).
    """
    from app.core.models import Position
    db = next(get_db())
    try:
        pos = db.query(Position).filter(
            Position.id == position_id,
            Position.status.in_(["OPEN", "PARTIAL"]),
        ).first()
        if not pos:
            raise HTTPException(status_code=404, detail="Position not found or already closed")
        pos.breakeven_lock_disabled = not bool(pos.breakeven_lock_disabled)
        db.commit()
        db.refresh(pos)
        await manager.broadcast({"type": "position_update", "data": pos.to_dict()})
        await manager.broadcast({
            "type": "auto_exit",
            "data": {
                "message": f"Breakeven lock {'OFF' if pos.breakeven_lock_disabled else 'ON'} for {pos.symbol} {pos.strike}{pos.option_type}",
                "severity": "info",
                "osi": pos.osi_symbol,
                "rule": "BE_LOCK_TOGGLE",
            },
        })
        log.info("[BE_LOCK_TOGGLE] Position #%d (%s) breakeven_lock_disabled=%s",
                 position_id, pos.osi_symbol, pos.breakeven_lock_disabled)
        return {"status": "ok", "id": pos.id, "osi": pos.osi_symbol,
                "breakeven_lock_disabled": pos.breakeven_lock_disabled}
    finally:
        db.close()


@app.post("/api/positions/{position_id}/toggle-trail")
async def toggle_position_trail(position_id: int):
    """
    Toggle trail_enabled for a single position. Opt-in: when True the monitor
    arms a trailing stop on this position once peak >= entry x (1 +
    trailing_sl_arm_pct/100), then exits the remainder trailing_sl_pct off
    that peak. Fires even when [trading] auto_exit_enabled = false, so a
    pure-relay book can protect one runner without enabling the PT/SL ladder.
    """
    from app.core.models import Position
    db = next(get_db())
    try:
        pos = db.query(Position).filter(
            Position.id == position_id,
            Position.status.in_(["OPEN", "PARTIAL"]),
        ).first()
        if not pos:
            raise HTTPException(status_code=404, detail="Position not found or already closed")
        pos.trail_enabled = not bool(pos.trail_enabled)
        db.commit()
        db.refresh(pos)
        from app.core.profile_resolver import xd
        arm = app_cfg.getfloat("trading", "trailing_sl_arm_pct", fallback=xd("trailing_sl_arm_pct"))
        trail = app_cfg.getfloat("trading", "trailing_sl_pct", fallback=xd("trailing_sl_pct"))
        await manager.broadcast({"type": "position_update", "data": pos.to_dict()})
        await manager.broadcast({
            "type": "auto_exit",
            "data": {
                "message": (f"Trailing stop ARMED for {pos.symbol} {pos.strike}{pos.option_type} "
                            f"— arms at +{arm:.0f}%, exits {trail:.0f}% off peak"
                            if pos.trail_enabled else
                            f"Trailing stop OFF for {pos.symbol} {pos.strike}{pos.option_type}"),
                "severity": "info",
                "osi": pos.osi_symbol,
                "rule": "TRAIL_TOGGLE",
            },
        })
        log.info("[TRAIL_TOGGLE] Position #%d (%s) trail_enabled=%s (arm +%.0f%%, trail %.0f%%)",
                 position_id, pos.osi_symbol, pos.trail_enabled, arm, trail)
        return {"status": "ok", "id": pos.id, "osi": pos.osi_symbol,
                "trail_enabled": pos.trail_enabled, "arm_pct": arm, "trail_pct": trail}
    finally:
        db.close()


@app.post("/api/positions/exit-all")
async def manual_exit_all():
    """
    Manually exit ALL open positions immediately.
    Fires individual SELL orders for each open/partial position.
    """
    from app.core.models import Position
    db = next(get_db())
    try:
        positions = db.query(Position).filter(
            Position.status.in_(["OPEN", "PARTIAL"]),
            Position.remaining > 0,
        ).all()
    finally:
        db.close()

    if not positions:
        return {"status": "ok", "closed": 0, "message": "No open positions"}

    results = []
    for pos in positions:
        try:
            r = await _fire_manual_exit(pos.id)
            results.append({"id": pos.id, "osi": pos.osi_symbol, "status": "ok", **r})
        except HTTPException as e:
            results.append({"id": pos.id, "osi": pos.osi_symbol, "status": "error", "detail": e.detail})
        except Exception as e:
            results.append({"id": pos.id, "osi": pos.osi_symbol, "status": "error", "detail": str(e)})

    await manager.broadcast({
        "type": "auto_exit",
        "data": {
            "message": f"Manual EXIT ALL fired - {len(positions)} position(s)",
            "severity": "danger",
            "rule": "MANUAL_ALL",
        },
    })
    closed = sum(1 for result in results if result["status"] == "ok")
    return {"status": "ok", "closed": closed, "requested": len(positions), "results": results}


@app.post("/api/orders/{order_id}/cancel")
async def cancel_order(order_id: int):
    """
    Cancel a pending order on Public.com.
    Only works for orders in PENDING status.
    """
    from app.execution.broker_router import get_executor
    executor = get_executor(manager)
    try:
        result = await executor.cancel_order(order_id)
        await manager.broadcast({
            "type": "auto_exit",
            "data": {
                "message": f"Order #{order_id} cancelled ({result.get('osi', '')})",
                "severity": "info",
                "rule": "MANUAL",
            },
        })
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/ingest/forwarded")
async def ingest_forwarded(request: Request):
    """Peer-mirror receiver. A primary instance POSTs each watched Discord
    message here so THIS instance trades the same signal in parallel (e.g. the
    Tradier secondary). Off by default; enable via [ingest] http_enabled + a
    matching [ingest] shared_secret on both sides. Bridge/loopback-only network
    + shared-secret header. Runs the pipeline off the request so the caller's
    fire-and-forget POST returns immediately (zero latency on the primary)."""
    if not app_cfg.getboolean("ingest", "http_enabled", fallback=False):
        raise HTTPException(status_code=404, detail="ingest disabled")
    secret = app_cfg.get("ingest", "shared_secret", fallback="")
    if not secret or request.headers.get("X-Ingest-Secret") != secret:
        raise HTTPException(status_code=403, detail="bad ingest secret")
    body = await request.json()
    try:
        created = (datetime.fromisoformat(body["created_at"])
                   if body.get("created_at") else datetime.now(timezone.utc))
    except Exception:
        created = datetime.now(timezone.utc)
    from app.ingest.discord_listener import inject_forwarded
    # Strong ref + done-callback: a bare create_task is only weakly held by the
    # loop and can be GC'd mid-await, silently dropping the forwarded signal —
    # fatal on the Tradier secondary where this is the ONLY ingest path.
    _spawn_tracked(inject_forwarded(
        content=body.get("content", ""),
        embeds=body.get("embeds", []),
        author_name=body.get("author_name", ""),
        channel_id=int(body["channel_id"]),
        channel_name=body.get("channel_name", ""),
        message_id=body.get("message_id", ""),
        created_at=created,
    ), "ingest_forwarded")
    return {"ok": True}


@app.post("/api/alerts/{alert_id}/cancel")
async def cancel_alert(alert_id: int):
    """
    Cancel a PENDING alert from the feed.

    Workflow:
      1. Look up the alert by ID.
      2. Find the most recent PENDING order for that alert's OSI symbol.
      3. Cancel it on Public.com (if not dry-run).
      4. Mark the alert as CANCELLED in the DB.
      5. Broadcast alert_update + order_update so the feed updates live.

    If no linked order exists (e.g. dry-run filled instantly, or already gone),
    the alert is just marked CANCELLED so the feed card clears.
    """
    from app.core.models import Alert, Order
    from app.execution.broker_router import get_executor
    executor = get_executor(manager)
    db = next(get_db())
    try:
        # ── 1. Find the alert ─────────────────────────────────────────────
        alert = db.query(Alert).filter(Alert.id == alert_id).first()
        if not alert:
            raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")
        if alert.status not in ("PENDING", "ERROR"):
            raise HTTPException(
                status_code=400,
                detail=f"Alert is {alert.status} — only PENDING or ERROR alerts can be cancelled from the feed"
            )

        cancelled_order = None
        cancel_error = None

        # ── 2. Find most recent PENDING order for this OSI ────────────────
        if alert.osi_symbol:
            pending_order = (
                db.query(Order)
                .filter(
                    Order.osi_symbol == alert.osi_symbol,
                    Order.side == (alert.action or "BUY"),
                    Order.status == "PENDING",
                )
                .order_by(Order.id.desc())
                .first()
            )
            if pending_order:
                # ── 3. Cancel on Public.com ───────────────────────────────
                try:
                    await executor.cancel_order(pending_order.id)
                    cancelled_order = pending_order
                except Exception as e:
                    # Order might already be gone from Public — still mark alert cancelled
                    cancel_error = str(e)
                    log.warning("cancel_alert: order cancel failed: %s", e)
                    pending_order.status = "CANCELLED"
                    pending_order.error_text = f"Force-cancelled from feed: {e}"
                    db.commit()

        # ── 4. Mark alert cancelled ───────────────────────────────────────
        alert.status = "CANCELLED"
        alert.error_text = (
            "Cancelled by user from feed"
            + (f" (order #{cancelled_order.id} cancelled on Public.com)" if cancelled_order else "")
            + (f" [order cancel error: {cancel_error}]" if cancel_error else "")
        )
        db.commit()

        # ── 5. Broadcast live updates ─────────────────────────────────────
        # alert_update → feed card updates immediately
        payload = alert.to_dict()
        if cancelled_order:
            payload["linkedOrderId"] = cancelled_order.id
        await manager.broadcast({"type": "alert_update", "data": payload})

        # Push notification to dashboard
        await manager.broadcast({
            "type": "auto_exit",
            "data": {
                "message": f"Alert #{alert_id} cancelled — {alert.osi_symbol or 'no symbol'}",
                "severity": "info",
                "rule": "MANUAL",
            },
        })

        return {
            "status": "cancelled",
            "alert_id": alert_id,
            "osi": alert.osi_symbol,
            "order_cancelled": cancelled_order.id if cancelled_order else None,
            "note": cancel_error if cancel_error else None,
        }
    finally:
        db.close()


@app.post("/api/alerts/{alert_id}/resubmit")
async def resubmit_alert(alert_id: int):
    """
    Force-resubmit a previously rejected, blocked, or errored alert,
    bypassing guardrails, Kelly Criterion, and AI Scorer entirely.

    Eligible statuses: SKIPPED (guardrail/scorer/kelly blocked), ERROR (API error),
    REJECTED (order rejected by broker).

    Workflow:
      1. Load the alert from DB and validate it can be resubmitted.
      2. Reset alert status → PENDING and clear error_text.
      3. Reconstruct a lightweight alert object from stored fields.
      4. Fire execute_override() which skips all risk filters and goes straight to order.
      5. Broadcast alert_update so the feed card refreshes live.
    """
    from app.core.models import Alert
    from app.execution.broker_router import get_executor
    from app.ingest.parser import _RE_SMALL_ACCOUNT

    db = next(get_db())
    try:
        alert = db.query(Alert).filter(Alert.id == alert_id).first()
        if not alert:
            raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")

        if alert.status not in ("SKIPPED", "ERROR", "REJECTED", "CANCELLED"):
            raise HTTPException(
                status_code=400,
                detail=f"Alert is {alert.status} — only SKIPPED/ERROR/REJECTED/CANCELLED alerts can be resubmitted",
            )

        if not alert.osi_symbol or not alert.action:
            raise HTTPException(
                status_code=400,
                detail="Alert is missing OSI symbol or action — cannot resubmit",
            )

        if alert.action != "BUY":
            raise HTTPException(
                status_code=400,
                detail="Only BUY alerts can be resubmitted — SELL alerts require an open position",
            )

        # ── Spreads take the spread lane, not the single-leg override ───────
        # A vertical's osi_symbol is "SHORT|LONG". execute_override() treats
        # that whole string as one option symbol, so the broker received a
        # pipe-joined symbol and rejected it: Tradier HTTP 400 on both
        # 2026-08-18 (SPXW260818P07690000|…85000) and 2026-08-19
        # (SPXW260819P07710000|…05000), each a matae SPX credit spread the
        # operator resubmitted after SPREAD_TOO_LARGE. Re-parse the original
        # text and dispatch to SpreadExecutor instead.
        #
        # NOTE: unlike the single-leg path this is a RETRY, not a bypass.
        # SpreadExecutor has no override mode, so spread risk caps
        # (spread_max_risk_dollars, credit-vs-width) still apply. That is
        # deliberate — the two failures above were both over-cap orders, and
        # silently waving a $425 spread past a $400 cap is not a bug fix.
        # To take a larger spread, raise spread_max_risk_dollars.
        spread_alert = None
        if "|" in alert.osi_symbol:
            from app.ingest.spread_parser import parse_spread_alert
            spread_alert = parse_spread_alert(alert.raw_text or "")
            if spread_alert is None:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Spread alert could not be re-parsed from its original text — "
                        "refusing to resubmit, because the single-leg path would send "
                        f"the invalid symbol {alert.osi_symbol!r} to the broker."
                    ),
                )

        # ── Reset alert status so the feed card shows it's being retried ──
        original_error = alert.error_text
        alert.status = "PENDING"
        alert.error_text = f"Resubmitted (override) — was: {original_error or alert.status}"
        db.commit()

        # Broadcast updated alert immediately so the feed shows PENDING
        await manager.broadcast({"type": "alert_update", "data": alert.to_dict()})

        # ── Reconstruct a minimal alert-like object from DB fields ─────────
        class _AlertProxy:
            def __init__(self, db_alert):
                self.action      = db_alert.action
                self.osi_symbol  = db_alert.osi_symbol
                self.symbol      = db_alert.symbol
                self.expiry      = db_alert.expiry
                self.strike      = db_alert.strike
                self.option_type = db_alert.option_type
                self.price       = Decimal(str(db_alert.alert_price)) if db_alert.alert_price is not None else None
                self.size_tag    = db_alert.size_tag or ""
                self.fraction    = db_alert.fraction or 1.0
                self.close_all   = False
                # Mirror what the message actually said. Both fields gate
                # _honors_message_qty; hardcoding qty=None here made every
                # resubmit fall back to size_default, so a "2 on small account"
                # alert came back as a 1-lot (INTC 110C 8/7, 2026-08-04).
                # small_account is re-derived from raw_text rather than from
                # strategy_tag, which a WHALE/RE-ENTRY lane tag can occupy.
                self.qty           = db_alert.qty
                self.small_account = bool(_RE_SMALL_ACCOUNT.search(db_alert.raw_text or ""))

        proxy = _AlertProxy(alert)
        author = alert.author or ""

        if spread_alert is not None:
            # Same serial worker the listener uses, so a resubmitted spread
            # can't race a single-leg guardrail count.
            from app.execution.broker_router import get_spread_executor
            from app.execution.order_queue import dispatch_execution, PRIORITY_ENTRY
            from datetime import datetime as _dt
            from zoneinfo import ZoneInfo as _ZI

            spread_executor = get_spread_executor(manager)
            _session = _dt.now(_ZI("America/New_York")).date()
            # Read off the ORM row now — the closure runs after this request's
            # session is closed, and a detached instance would raise.
            _chash = alert.content_hash or ""

            def _factory():
                return spread_executor.execute(
                    spread_alert, alert_id, alert_author=author,
                    content_hash=_chash, session=_session,
                )

            if not dispatch_execution(_factory, name="spread_executor.resubmit",
                                      priority=PRIORITY_ENTRY):
                asyncio.create_task(_factory(), name="spread_executor.resubmit")

            log.info("[RESUBMIT] Alert #%d %s %s — SPREAD lane (risk caps still apply)",
                     alert_id, alert.action, alert.osi_symbol)
        else:
            # ── Fire override execution in background ──────────────────────
            executor = get_executor(manager)
            asyncio.create_task(executor.execute_override(proxy, alert_db_id=alert_id, alert_author=author))

            log.info("[RESUBMIT] Alert #%d %s %s — override execution started", alert_id, alert.action, alert.osi_symbol)

        await manager.broadcast({
            "type": "auto_exit",
            "data": {
                "message": (
                    f"🔁 Resubmit: Alert #{alert_id} {alert.action} {alert.osi_symbol} — "
                    "spread lane, risk caps still apply"
                    if spread_alert is not None else
                    f"🔁 Resubmit override: Alert #{alert_id} {alert.action} {alert.osi_symbol} — guardrails/Kelly/scorer bypassed"
                ),
                "severity": "info",
                "rule": "OVERRIDE",
            },
        })

        return {
            "status": "resubmitting",
            "alert_id": alert_id,
            "osi": alert.osi_symbol,
            "action": alert.action,
            "note": "Order is being placed now with all risk filters bypassed",
        }
    finally:
        db.close()


@app.get("/api/channels")
async def get_channels():
    """Return the list of Discord channels being monitored."""
    raw = app_cfg.get("discord", "channel_ids", fallback="")
    ids = [c.strip() for c in raw.split(",") if c.strip()]
    authors = [a.strip() for a in app_cfg.get("discord", "authors", fallback="").split(",") if a.strip()]
    return {
        "channel_ids": ids,
        "authors": authors,
        "history_backfill_limit": app_cfg.getint("discord", "history_backfill_limit", fallback=0),
        "execute_history": app_cfg.getboolean("discord", "execute_history", fallback=False),
    }


@app.get("/api/balance")
async def get_balance():
    """Return the current broker account balance (or simulated in dry-run mode)."""
    from app.execution.broker_router import fetch_account_balance
    return await fetch_account_balance()


@app.get("/api/health")
async def health():
    """Return comprehensive health status of all bot components."""
    from app.monitors.health_monitor import get_health_status
    return await get_health_status()


@app.get("/api/analytics/summary")
async def analytics_summary():
    """Return high-level analytics summary for dashboard."""
    from app.analytics.analytics import get_analytics_summary
    return get_analytics_summary()


@app.get("/api/analytics/strategies")
async def analytics_strategies(days: int = 30):
    """Return P&L performance by strategy tag."""
    from app.analytics.analytics import get_strategy_performance
    return get_strategy_performance(days=days)


@app.get("/api/analytics/symbols")
async def analytics_symbols(days: int = 30):
    """Return P&L performance by symbol."""
    from app.analytics.analytics import get_symbol_performance
    return get_symbol_performance(days=days)


@app.get("/api/analytics/hourly")
async def analytics_hourly(days: int = 30):
    """Return P&L performance by hour of day."""
    from app.analytics.analytics import get_hourly_performance
    return get_hourly_performance(days=days)


@app.get("/api/greeks")
async def get_greeks():
    """Return aggregate Greeks (Delta, Gamma, Theta) for all positions."""
    from app.monitors.greeks_monitor import get_greeks_status
    return get_greeks_status()


@app.get("/api/premarket")
async def get_premarket_status():
    """Return pre-market preparation status."""
    from app.monitors.premarket_prep import get_premarket_status
    return get_premarket_status()


@app.get("/api/metrics")
async def get_metrics():
    """Return API usage metrics for Discord and Public.com"""
    from app.monitors.api_monitor import monitor
    return monitor.get_metrics()


@app.post("/api/metrics/reset")
async def reset_metrics():
    """Reset API metrics counters (useful for testing)"""
    from app.monitors.api_monitor import monitor
    monitor.reset()
    return {"status": "reset"}


@app.get("/api/audit")
async def order_audit(limit: int = 200, verdict: str | None = None):
    """Append-only order-decision audit trail (provenance). Newest first."""
    from app.core.db import SessionLocal
    from app.core.models import OrderAudit
    db = SessionLocal()
    try:
        q = db.query(OrderAudit)
        if verdict:
            q = q.filter(OrderAudit.verdict == verdict.upper())
        rows = q.order_by(OrderAudit.id.desc()).limit(min(max(limit, 1), 1000)).all()
        return [r.to_dict() for r in rows]
    finally:
        db.close()


@app.get("/metrics")
async def prometheus_metrics():
    """Trading hot-path metrics in Prometheus text exposition format.
    Scrape target for Prometheus/Grafana (distinct from /api/metrics, which is
    Discord/Public API-call counts)."""
    from fastapi.responses import PlainTextResponse
    from app.core import metrics
    return PlainTextResponse(metrics.render_prometheus(),
                             media_type="text/plain; version=0.0.4")


@app.get("/api-dashboard")
async def api_dashboard():
    """Serve the API monitoring dashboard"""
    from fastapi.responses import FileResponse
    return FileResponse(str(STATIC_DIR / "api_dashboard.html"))





@app.get("/api/public-account")
async def get_public_account():
    """
    Fetch comprehensive account data directly from Public.com API.
    Includes: balance, positions, orders, and today's P&L.
    This is the authoritative source for account information.
    """
    from app.execution.broker_router import fetch_account_data
    try:
        # Get full account data
        data = await fetch_account_data()
        return data
    except Exception as e:
        return {"error": str(e), "fallback": "local"}


@app.get("/api/guardrails")
async def get_guardrails():
    """Return current guardrail status."""
    import app.risk.guardrails as guardrails
    return guardrails.get_guardrail_status()


def _realized_sell_fills(db, cutoff):
    """FILLED SELL orders since cutoff, time-ordered. The realized-P&L ground
    truth shared by /api/stats, /api/analysts and /api/calendar: per-leg fills
    against the cost_basis snapshot, so multi-leg PT1/PT2 exits aren't priced
    at the last leg like the old position-close_price math was."""
    from app.core.models import Order
    return db.query(Order).filter(
        Order.side == "SELL",
        Order.status == "FILLED",
        Order.filled_at.isnot(None),
        Order.filled_at >= cutoff,
    ).order_by(Order.filled_at.asc()).all()


def _fill_profit(o, is_credit=None):
    """(profit $, filled qty) for a realized SELL fill; profit None when the
    legacy row is missing cost_basis/fill_price and can't be priced.

    fill_price is checked against None, NOT truthiness: a contract that expired
    worthless realizes at exactly 0.0, which is falsy. Treating that as
    "unpriceable" silently dropped every total loss from the analyst stats and
    the realized-P&L totals — 13 rows / -$5,703 on relaybot-vm alone, i.e. the
    worst trades were the ones being hidden.

    Two-leg combo keys (`short|long`) are credit verticals: profit is
    (basis - fill), so buying back cheaper than the credit received is a win.
    """
    from app.core.models import fill_pnl_dollar, is_credit_combo_osi
    qty = o.filled_qty or o.quantity or 0
    if o.fill_price is None or not qty or o.cost_basis is None:
        return None, qty
    if is_credit is None:
        is_credit = is_credit_combo_osi(getattr(o, "osi_symbol", "") or "")
    return fill_pnl_dollar(o.fill_price, o.cost_basis, qty, bool(is_credit)), qty


@app.get("/api/analysts")
async def get_analysts(days: int = 30):
    """Per-analyst performance from realized SELL fills.

    Rewritten 2026-08-03: attribution comes from Order.author (stamped at
    entry, backfilled through history) instead of matching BUY alerts to the
    *first* CLOSED position with that OSI — which credited the wrong trade
    whenever a strike was traded twice, counted out-of-window trades, and
    priced multi-leg exits at the last leg. A "trade" here is a realizing
    SELL fill — the same unit the calendar counts, so the tabs agree.
    """
    if not app_cfg.getboolean("trading", "analyst_tracking_enabled", fallback=True):
        return {"analysts": [], "period_days": days, "disabled": True}

    from collections import defaultdict
    from zoneinfo import ZoneInfo
    from app.risk.guardrails import extract_root_symbol

    db = next(get_db())
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        et = ZoneInfo("America/New_York")

        analysts = defaultdict(lambda: {
            "trades": 0, "wins": 0, "losses": 0,
            "total_pnl": 0.0,
            "returns": [],            # per-fill % returns, fill-time order (streak walks this)
            "symbols": set(),
            "best_trade_pnl": 0.0, "best_trade_osi": "",
            "worst_trade_pnl": 0.0, "worst_trade_osi": "",
            "first_trade": None, "last_trade": None,
            "by_symbol": {},          # symbol -> {"pnl","trades","wins"}
            "by_month": {},           # "YYYY-MM" (ET) -> pnl
            "trade_log": [],
            "gross_win": 0.0, "gross_loss": 0.0,
        })

        for o in _realized_sell_fills(db, cutoff):
            profit, qty = _fill_profit(o)
            author = o.author or "Unknown"
            sym = extract_root_symbol(o.osi_symbol or "") or "?"
            fa = o.filled_at if o.filled_at.tzinfo else o.filled_at.replace(tzinfo=timezone.utc)

            a = analysts[author]
            a["trades"] += 1
            a["symbols"].add(sym)
            if a["first_trade"] is None:
                a["first_trade"] = fa
            a["last_trade"] = fa

            # cost_basis must be truthy (it is the divisor); fill_price only has
            # to be present — 0.0 is a real exit price (expired worthless, -100%).
            # Credit verticals invert: cheaper buy-back is a positive return.
            if o.cost_basis and o.fill_price is not None:
                from app.core.models import is_credit_combo_osi
                if is_credit_combo_osi(o.osi_symbol):
                    ret_pct = ((o.cost_basis - o.fill_price) / o.cost_basis) * 100
                else:
                    ret_pct = ((o.fill_price / o.cost_basis) - 1) * 100
            else:
                ret_pct = None
            a["trade_log"].append({
                "symbol": sym,
                "osi": o.osi_symbol,
                "entry": round(o.cost_basis, 4) if o.cost_basis is not None else None,
                "exit": round(o.fill_price, 4) if o.fill_price is not None else None,
                "contracts": qty,
                "pnl": round(profit, 2) if profit is not None else None,
                "return_pct": round(ret_pct, 1) if ret_pct is not None else None,
                "closed_at": fa.isoformat(),
            })

            if profit is None:
                continue  # unpriceable legacy row: listed in the log, excluded from aggregates

            a["total_pnl"] += profit
            a["returns"].append(ret_pct)
            month = fa.astimezone(et).strftime("%Y-%m")
            a["by_month"][month] = a["by_month"].get(month, 0.0) + profit

            bs = a["by_symbol"].setdefault(sym, {"symbol": sym, "pnl": 0.0, "trades": 0, "wins": 0})
            bs["pnl"] += profit
            bs["trades"] += 1

            if profit > 0:
                a["wins"] += 1
                a["gross_win"] += profit
                bs["wins"] += 1
            else:
                a["losses"] += 1
                a["gross_loss"] += abs(profit)

            if profit > a["best_trade_pnl"]:
                a["best_trade_pnl"] = profit
                a["best_trade_osi"] = o.osi_symbol
            if profit < a["worst_trade_pnl"]:
                a["worst_trade_pnl"] = profit
                a["worst_trade_osi"] = o.osi_symbol

        result = []
        for name, a in analysts.items():
            total = a["wins"] + a["losses"]
            if total == 0:
                continue

            # Current streak: walk back from the most recent priced fill.
            streak = 0
            streak_type = ""
            for r in reversed(a["returns"]):
                direction = "W" if r > 0 else "L"
                if not streak_type:
                    streak_type = direction
                if direction == streak_type:
                    streak += 1
                else:
                    break

            avg_win = a["gross_win"] / a["wins"] if a["wins"] else 0.0
            avg_loss = a["gross_loss"] / a["losses"] if a["losses"] else 0.0
            wr = a["wins"] / total

            result.append({
                "author": name,
                "trades": total,
                "wins": a["wins"],
                "losses": a["losses"],
                "win_rate": round(wr * 100, 1),
                "avg_return_pct": round(sum(a["returns"]) / total, 1),
                "total_pnl": round(a["total_pnl"], 2),
                "best_trade": {"pnl": round(a["best_trade_pnl"], 2), "osi": a["best_trade_osi"]},
                "worst_trade": {"pnl": round(a["worst_trade_pnl"], 2), "osi": a["worst_trade_osi"]},
                "streak": streak,
                "streak_type": streak_type,
                "symbols": list(a["symbols"]),
                "first_trade": a["first_trade"].isoformat() if a["first_trade"] else None,
                "last_trade": a["last_trade"].isoformat() if a["last_trade"] else None,
                "profit_factor": round(a["gross_win"] / a["gross_loss"], 2) if a["gross_loss"] > 0 else (round(a["gross_win"], 2) if a["gross_win"] > 0 else 0.0),
                "avg_win": round(avg_win, 2),
                "avg_loss": round(avg_loss, 2),
                "expectancy": round(wr * avg_win - (1 - wr) * avg_loss, 2),
                "monthly": [{"month": m, "pnl": round(p, 2)} for m, p in sorted(a["by_month"].items())],
                "by_symbol": sorted(
                    [{**v, "pnl": round(v["pnl"], 2)} for v in a["by_symbol"].values()],
                    key=lambda x: x["pnl"], reverse=True),
                "trade_log": sorted(a["trade_log"], key=lambda t: t["closed_at"] or "", reverse=True),
            })

        result.sort(key=lambda x: x["total_pnl"], reverse=True)
        return {"analysts": result, "period_days": days}
    finally:
        db.close()


@app.get("/")
async def dashboard(request: Request):
    loaded = _load_dashboard()
    if loaded is None:
        raise HTTPException(status_code=404, detail="dashboard.html not found")
    body, etag = loaded
    # Trading UI: never CDN-cache the shell. A 5-minute stale dashboard after a
    # deploy (halt badge, exit control, broker label) is not acceptable while
    # money is live. Live positions/P&L already come from /ws and /api (also
    # uncached). ETag + must-revalidate still 304s an unchanged reload.
    headers = {
        "Cache-Control": "no-cache, must-revalidate",
        "ETag": etag,
        "CDN-Cache-Control": "no-store",
    }
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers=headers)
    return Response(content=body, media_type="text/html", headers=headers)


@app.get("/api/config")
async def get_config():
    """
    Return all non-sensitive config.ini values as nested JSON.
    Sensitive values (tokens, API keys) are masked as ••••••.
    """
    return app_cfg.as_dict()


@app.post("/api/config")
async def update_config(updates: list[ConfigItem]):
    """
    Update config.ini settings in-place and reload without restarting.

    Body: [{"section": str, "key": str, "value": str}, ...]

    Only keys in the EDITABLE_KEYS allowlist are accepted.
    Sensitive keys (tokens, api keys) are always rejected.
    After writing, broadcasts a config_update event via WebSocket.
    """
    if not updates:
        raise HTTPException(status_code=400, detail="No updates provided")

    # Convert Pydantic models to dicts for config_manager
    update_dicts = [{"section": u.section, "key": u.key, "value": u.value} for u in updates]

    try:
        errors = app_cfg.set_and_save(update_dicts)
    except Exception as exc:
        log.error("[CONFIG] set_and_save crashed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Config save failed: {exc}") from exc

    # Propagate reload to guardrails (which uses cfg live, this is a safety call)
    try:
        import app.risk.guardrails as guardrails
        guardrails.reload_config()
    except Exception:
        pass

    # Broadcast to all dashboard clients so their Settings tab refreshes
    await manager.broadcast({"type": "config_update", "data": app_cfg.as_dict()})

    applied = len(updates) - len(errors)
    log.info("[CONFIG] %d/%d setting(s) updated via dashboard", applied, len(updates))

    return {
        "status": "ok" if not errors else "partial",
        "applied": applied,
        "errors": errors,
    }


@app.post("/api/config/reload")
async def reload_config():
    """Re-read config.ini from disk into the live singleton. Writes nothing.

    config.ini is host-mounted and gitignored, so it is sometimes edited on
    the box directly — and the only way to make the running process notice
    used to be POSTing some editable key to the value it already had. That
    works, but it is a lie in the audit trail: config_changes.log is meant to
    answer "when did this knob move", and no-op rows bury the real ones.

    Returns the count of (section, key) pairs the reload changed, so a caller
    can tell a real reload from a no-op.

    The keys a reload actually moved ARE written to config_changes.log, tagged
    'disk edit'. Only no-op rows were ever the lie; a knob edited on disk moves
    money exactly as much as one edited through the portal. On the secondary
    instances every edit arrives this way — setup-instance.sh writes the file —
    so without this their Change History stays empty however much changed.
    """
    before = app_cfg.as_dict()
    app_cfg.reload()
    after = app_cfg.as_dict()
    moved = sorted(
        (s, k)
        for s in set(before) | set(after)
        for k in set(before.get(s, {})) | set(after.get(s, {}))
        if before.get(s, {}).get(k) != after.get(s, {}).get(k)
    )
    changed = [f"{s}.{k}" for s, k in moved]
    app_cfg.append_audit([
        app_cfg.audit_line(s, k, before.get(s, {}).get(k), after.get(s, {}).get(k),
                           source="disk edit")
        for s, k in moved
    ])
    try:
        import app.risk.guardrails as guardrails
        guardrails.reload_config()
    except Exception:
        pass
    await manager.broadcast({"type": "config_update", "data": after})
    log.info("[CONFIG] reload from disk — %d key(s) changed: %s",
             len(changed), ", ".join(changed[:20]) or "none")
    return {"status": "ok", "changed_count": len(changed), "changed": changed[:50]}


@app.get("/api/config/history")
async def get_config_history(limit: int = 100):
    """
    Return the last N lines from logs/config_changes.log as structured records.
    Each record: {ts, key, old, new}
    """
    from pathlib import Path as _Path
    log_path = _Path("logs") / "config_changes.log"
    if not log_path.exists():
        return {"entries": []}

    try:
        lines = log_path.read_text(encoding="utf-8").splitlines()
        entries = []
        for line in reversed(lines[-limit:]):
            # Format: "2026-04-16 10:00:00 UTC | section.key | 'old' → 'new'"
            parts = [p.strip() for p in line.split("|")]
            if len(parts) == 3:
                change = parts[2]
                entries.append({"ts": parts[0], "key": parts[1], "change": change})
        return {"entries": entries}
    except Exception as exc:
        log.error("Failed to read config history: %s", exc)
        return {"entries": []}


@app.get("/api/stats")
async def get_stats(days: int = 30):
    """Overall performance metrics and cumulative P&L history.

    Rewritten 2026-08-03 onto realized SELL fills (same ground truth as the
    calendar and today_pnl) instead of position close_price × total_contracts,
    which overstated multi-leg PT1/PT2 exits by pricing every contract at the
    last leg. Hold-time metrics stay position-based — open→close duration only
    exists on the Position row.
    """
    from app.core.models import Position, Order
    from app.risk.guardrails import extract_root_symbol

    db = next(get_db())
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        wins = 0
        losses = 0
        best_trade = {"pnl": 0.0, "osi": ""}
        worst_trade = {"pnl": 0.0, "osi": ""}
        total_realized_pnl = 0.0

        pnl_history = []
        cumulative = 0.0

        # Richer trade-quality + journaling aggregates (PNLCalendar-style).
        gross_win = 0.0          # sum of winning $ (for profit factor)
        gross_loss = 0.0         # sum of |losing $|
        peak = 0.0               # running peak of cumulative curve (for drawdown)
        max_drawdown = 0.0       # largest peak-to-trough dip in $
        by_symbol: dict = {}     # symbol -> {"pnl":, "trades":, "wins":}
        by_day: dict = {}        # ET date -> daily realized $
        by_weekday: dict = {}    # ET weekday name -> {"pnl","trades","wins"}

        from zoneinfo import ZoneInfo as _ZId
        _etd = _ZId("America/New_York")

        for o in _realized_sell_fills(db, cutoff):
            pnl_dollar, _qty = _fill_profit(o)
            if pnl_dollar is None:
                continue
            fa = o.filled_at if o.filled_at.tzinfo else o.filled_at.replace(tzinfo=timezone.utc)
            fa_et = fa.astimezone(_etd)

            if pnl_dollar > 0:
                wins += 1
                gross_win += pnl_dollar
            else:
                losses += 1
                gross_loss += abs(pnl_dollar)

            total_realized_pnl += pnl_dollar
            cumulative += pnl_dollar

            # Drawdown: track peak of the cumulative curve and the deepest dip.
            if cumulative > peak:
                peak = cumulative
            dd = peak - cumulative
            if dd > max_drawdown:
                max_drawdown = dd

            # Per-symbol roll-up (top/worst tickers).
            sym = extract_root_symbol(o.osi_symbol or "") or "?"
            s = by_symbol.setdefault(sym, {"symbol": sym, "pnl": 0.0, "trades": 0, "wins": 0})
            s["pnl"] += pnl_dollar
            s["trades"] += 1
            if pnl_dollar > 0:
                s["wins"] += 1

            # Per-ET-day + per-ET-weekday roll-ups.
            dkey = fa_et.strftime("%Y-%m-%d")
            by_day[dkey] = round(by_day.get(dkey, 0.0) + pnl_dollar, 2)
            wd = fa_et.strftime("%a")  # Mon..Fri
            w = by_weekday.setdefault(wd, {"day": wd, "pnl": 0.0, "trades": 0, "wins": 0})
            w["pnl"] += pnl_dollar
            w["trades"] += 1
            if pnl_dollar > 0:
                w["wins"] += 1

            if pnl_dollar > best_trade["pnl"]:
                best_trade["pnl"] = pnl_dollar
                best_trade["osi"] = o.osi_symbol

            if pnl_dollar < worst_trade["pnl"]:
                worst_trade["pnl"] = pnl_dollar
                worst_trade["osi"] = o.osi_symbol

            pnl_history.append({
                "t": fa_et.strftime("%m-%d %H:%M"),
                "pnl": round(cumulative, 2),
                "trade_pnl": round(pnl_dollar, 2)
            })

        # Hold-time metrics from CLOSED positions (duration lives there, not on
        # fills). P&L inside the buckets is the position-level approximation —
        # fine for "do my quick flips or my holds make money" reads.
        hold_minutes = []
        hold_buckets = {k: {"bucket": k, "pnl": 0.0, "trades": 0, "wins": 0}
                        for k in ("<15m", "15–60m", "1–4h", ">4h")}
        closed_positions = db.query(Position).filter(
            Position.status == "CLOSED",
            Position.close_time >= cutoff,
        ).all()
        for pos in closed_positions:
            if not (pos.open_time and pos.close_time and pos.avg_price):
                continue
            ot = pos.open_time if pos.open_time.tzinfo else pos.open_time.replace(tzinfo=timezone.utc)
            ct = pos.close_time if pos.close_time.tzinfo else pos.close_time.replace(tzinfo=timezone.utc)
            mins = (ct - ot).total_seconds() / 60.0
            if mins < 0:
                continue
            hold_minutes.append(mins)
            p = pos.pnl_dollar()
            hb = hold_buckets["<15m" if mins < 15 else "15–60m" if mins < 60 else "1–4h" if mins < 240 else ">4h"]
            hb["pnl"] += p
            hb["trades"] += 1
            if p > 0:
                hb["wins"] += 1

        total = wins + losses
        win_rate = (wins / total * 100) if total > 0 else 0.0

        # Derived quality metrics.
        avg_win = (gross_win / wins) if wins else 0.0
        avg_loss = (gross_loss / losses) if losses else 0.0   # positive magnitude
        avg_trade = (total_realized_pnl / total) if total else 0.0
        profit_factor = (gross_win / gross_loss) if gross_loss > 0 else (gross_win if gross_win > 0 else 0.0)
        # Expectancy $/trade = winrate*avgWin - lossrate*avgLoss
        wr = (wins / total) if total else 0.0
        expectancy = wr * avg_win - (1 - wr) * avg_loss
        avg_hold_min = (sum(hold_minutes) / len(hold_minutes)) if hold_minutes else 0.0

        sym_list = sorted(by_symbol.values(), key=lambda x: x["pnl"], reverse=True)
        for s in sym_list:
            s["pnl"] = round(s["pnl"], 2)
        top_symbols = sym_list[:5]
        worst_symbols = [s for s in sym_list[::-1] if s["pnl"] < 0][:5]
        daily_pnl = [{"date": d, "pnl": by_day[d]} for d in sorted(by_day.keys())]

        # Also count current open positions and today's filled orders (for the top cards)
        open_count = db.query(Position).filter(Position.status.in_(["OPEN", "PARTIAL"])).count()

        # "Today" must mean the trading day in ET, not UTC. After 8 PM ET
        # the UTC date has rolled to "tomorrow", so a UTC-midnight window
        # would suddenly show 0 filled orders / $0 P&L for a day's worth of
        # actual trades. Compute ET midnight, then convert to UTC for the
        # query.
        from zoneinfo import ZoneInfo as _ZI
        _et = _ZI("America/New_York")
        et_now = datetime.now(_et)
        et_midnight = et_now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start = et_midnight.astimezone(timezone.utc)
        filled_today = db.query(Order).filter(Order.status == "FILLED", Order.placed_at >= today_start).count()
        auto_exits = db.query(Order).filter(Order.trigger.in_(["PT1", "PT2", "PT3", "SL"]), Order.placed_at >= cutoff).count()
        manual_exits = db.query(Order).filter(Order.trigger.in_(["MANUAL", "MANUAL_ALL"]), Order.placed_at >= cutoff).count()

        # Today-only realized P&L — sum of FILLED SELL orders since ET
        # midnight. Uses per-leg order fills (not position close_price) so
        # multi-leg exits PT1/PT2/PT3 aren't overstated by pricing every
        # contract at the last leg.
        from app.core.models import realized_pnl_since
        today_pnl = realized_pnl_since(db, today_start)

        return {
            "period_days": days,
            "trades": total,
            "wins": wins,
            "losses": losses,
            "win_rate": round(win_rate, 1),
            "total_pnl": round(total_realized_pnl, 2),
            "today_pnl": round(today_pnl, 2),
            "best_trade": {"pnl": round(best_trade["pnl"], 2), "osi": best_trade["osi"]},
            "worst_trade": {"pnl": round(worst_trade["pnl"], 2), "osi": worst_trade["osi"]},
            "pnl_history": pnl_history,
            "open_positions": open_count,
            "filled_orders_today": filled_today,
            "auto_exits_period": auto_exits,
            "manual_exits_period": manual_exits,
            # Trade-quality + journaling metrics (PNLCalendar-style)
            "gross_win": round(gross_win, 2),
            "gross_loss": round(gross_loss, 2),
            "profit_factor": round(profit_factor, 2),
            "expectancy": round(expectancy, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "avg_trade": round(avg_trade, 2),
            "max_drawdown": round(max_drawdown, 2),
            "avg_hold_min": round(avg_hold_min, 1),
            "top_symbols": top_symbols,
            "worst_symbols": worst_symbols,
            "daily_pnl": daily_pnl,
            "by_weekday": [
                {**by_weekday[d], "pnl": round(by_weekday[d]["pnl"], 2)}
                for d in ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun") if d in by_weekday
            ],
            "hold_buckets": [
                {**b, "pnl": round(b["pnl"], 2)} for b in hold_buckets.values() if b["trades"]
            ],
        }
    finally:
        db.close()


@app.get("/api/calendar")
async def get_calendar(days: int = 120):
    """Per-ET-trading-day realized P&L plus the trades behind each day, for the
    journaling calendar.

    A "trade" here is a FILLED SELL order (the realizing leg). Each row carries
    the opening analyst (Order.author, mirrored from the Position) so the
    journal can attribute every exit. Days are bucketed by the ET calendar date
    of the fill — same trading-day definition the rest of the app uses.
    """
    from app.core.models import Order
    from zoneinfo import ZoneInfo

    db = next(get_db())
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        et = ZoneInfo("America/New_York")

        sells = db.query(Order).filter(
            Order.side == "SELL",
            Order.status == "FILLED",
            Order.filled_at.isnot(None),
            Order.filled_at >= cutoff,
        ).order_by(Order.filled_at.asc()).all()

        days_map: dict = {}
        for o in sells:
            d = o.to_dict()
            profit = d.get("profit")  # None for legacy rows missing cost_basis
            fa = o.filled_at
            if fa.tzinfo is None:
                fa = fa.replace(tzinfo=timezone.utc)
            day = fa.astimezone(et).strftime("%Y-%m-%d")

            b = days_map.setdefault(day, {
                "date": day, "pnl": 0.0, "trades": 0,
                "wins": 0, "losses": 0, "analysts": {}, "items": [],
            })
            p = profit or 0.0
            b["pnl"] += p
            b["trades"] += 1
            if profit is not None:
                if p > 0:
                    b["wins"] += 1
                elif p < 0:
                    b["losses"] += 1
            author = d.get("author") or "—"
            b["analysts"][author] = round(b["analysts"].get(author, 0.0) + p, 2)
            b["items"].append({
                "id":        d["id"],
                "osiSymbol": d["osiSymbol"],
                "author":    author,
                "filledQty": d["filledQty"],
                "fillPrice": d["fillPrice"],
                "costBasis": d["costBasis"],
                "profit":    None if profit is None else round(p, 2),
                "trigger":   d["trigger"],
                "filledAt":  d["filledAt"],
                "tag":       d.get("strategyTag"),
            })

        out = []
        for day in sorted(days_map.keys()):
            b = days_map[day]
            b["pnl"] = round(b["pnl"], 2)
            out.append(b)

        # Journal notes ride alongside the days rather than inside them: a note
        # can exist for a day with no fills (a "sat out, chop" entry is worth
        # keeping), so it must not depend on a P&L bucket existing.
        from app.core.models import SystemState
        notes = {
            r.key[len(JOURNAL_PREFIX):]: r.value
            for r in db.query(SystemState).filter(
                SystemState.key.like(f"{JOURNAL_PREFIX}%")
            ).all()
            if r.value
        }
        return {"period_days": days, "days": out, "notes": notes}
    finally:
        db.close()


JOURNAL_PREFIX = "journal:"


@app.post("/api/calendar/note")
async def save_calendar_note(body: JournalNote):
    """Save (or clear, when note is empty) the journal note for one ET day.

    Stored in the SystemState key/value table under ``journal:YYYY-MM-DD`` —
    one note per day is far too little data to earn its own table.
    """
    from app.core.models import set_system_state, delete_system_state

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", body.date):
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")

    note = body.note.strip()[:4000]
    db = next(get_db())
    try:
        key = f"{JOURNAL_PREFIX}{body.date}"
        if note:
            set_system_state(db, key, note)
        else:
            delete_system_state(db, key)
        return {"ok": True, "date": body.date, "note": note}
    finally:
        db.close()


@app.get("/api/stats/symbols")
async def get_symbol_stats(days: int = 30):
    """
    Return per-symbol P&L and performance breakdown.
    Aggregates CLOSED positions by underlying symbol (SPX, AMD, NVDA…)
    for the specified lookback window.
    """
    from app.core.models import Position
    from collections import defaultdict

    db = next(get_db())
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        closed = db.query(Position).filter(
            Position.status == "CLOSED",
            Position.close_time >= cutoff,
        ).order_by(Position.close_time.asc()).all()

        symbols: dict = defaultdict(lambda: {
            "trades": 0, "wins": 0, "losses": 0,
            "total_pnl": 0.0, "total_return_pct": 0.0,
            "total_contracts": 0,
            "best_pnl": 0.0, "worst_pnl": 0.0,
            "best_osi": "", "worst_osi": "",
        })

        for pos in closed:
            if not pos.avg_price:
                continue

            sym = pos.symbol or (pos.osi_symbol[:3] if pos.osi_symbol else "?")
            pnl_dollar = pos.pnl_dollar()
            pnl_pct = pos.pnl_pct()

            s = symbols[sym]
            s["trades"] += 1
            s["total_contracts"] += pos.total_contracts or 0
            s["total_pnl"] += pnl_dollar
            s["total_return_pct"] += pnl_pct
            if pnl_dollar > 0:
                s["wins"] += 1
            else:
                s["losses"] += 1
            if pnl_dollar > s["best_pnl"]:
                s["best_pnl"] = pnl_dollar
                s["best_osi"] = pos.osi_symbol or ""
            if pnl_dollar < s["worst_pnl"]:
                s["worst_pnl"] = pnl_dollar
                s["worst_osi"] = pos.osi_symbol or ""

        result = []
        for sym, s in symbols.items():
            total = s["wins"] + s["losses"]
            result.append({
                "symbol": sym,
                "trades": total,
                "wins": s["wins"],
                "losses": s["losses"],
                "win_rate": round(s["wins"] / total * 100, 1) if total else 0,
                "total_pnl": round(s["total_pnl"], 2),
                "avg_return_pct": round(s["total_return_pct"] / total, 1) if total else 0,
                "total_contracts": s["total_contracts"],
                "best_trade": {"pnl": round(s["best_pnl"], 2), "osi": s["best_osi"]},
                "worst_trade": {"pnl": round(s["worst_pnl"], 2), "osi": s["worst_osi"]},
            })

        result.sort(key=lambda x: x["total_pnl"], reverse=True)
        return {"symbols": result, "period_days": days}
    finally:
        db.close()


# ── CSV Export endpoints ──────────────────────────────────────────────────────────

@app.get("/api/export/trades")
async def export_trades(days: int = 30):
    """Download trade/order history as CSV."""
    csv_data = trade_logger.export_trades_csv(days=days)
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=trades_{datetime.now().strftime('%Y%m%d')}.csv"},
    )


@app.get("/api/export/positions")
async def export_positions():
    """Download all positions (open and closed) as CSV."""
    csv_data = trade_logger.export_positions_csv()
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=positions_{datetime.now().strftime('%Y%m%d')}.csv"},
    )


@app.get("/api/report", response_class=HTMLResponse)
async def trading_report(days: int = 30):
    """Printable trading report. Open it and Ctrl+P → Save as PDF.

    HTML rather than a generated PDF on purpose: the browser is already a
    competent PDF engine, so this needs no weasyprint (cairo/pango system
    libs) or reportlab, and the page stays readable on screen too.
    """
    from app.analytics.report import render_report_html
    # DB reads + string building off the event loop, same as the workbook.
    html = await asyncio.to_thread(render_report_html, days)
    return HTMLResponse(content=html)


@app.get("/api/export/all")
async def export_all(days: int = 30):
    """One workbook replacing the three separate CSVs, with the join done.

    Sheet 1 relates alert -> order -> position on one row; sheet 2 is the
    alerts that produced no order, with the reason; sheet 3 is a header block.
    """
    from zoneinfo import ZoneInfo
    # Workbook build is CPU-bound and touches the DB — off the event loop so a
    # big export cannot stall the monitor poll or the order queue.
    data = await asyncio.to_thread(trade_logger.export_all_xlsx, days)
    stamp = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
    return Response(
        content=data,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename=relaybot_{stamp}.xlsx'},
    )


@app.get("/api/export/alerts")
async def export_alerts(days: int = 30):
    """Download alert history as CSV."""
    csv_data = trade_logger.export_alerts_csv(days=days)
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=alerts_{datetime.now().strftime('%Y%m%d')}.csv"},
    )


@app.post("/api/daily-summary")
async def trigger_daily_summary():
    """Manually trigger a daily P&L summary post to #trade-logs."""
    await trade_logger.log_daily_summary()
    return {"status": "ok", "message": "Daily summary posted to #trade-logs"}


# ── Daily summary scheduler ───────────────────────────────────────────────────────

async def _vix_refresher():
    """Keep vix_sizer's live cache warm from the active broker's INDEX quote.

    Previously the sizer only ever read a hand-typed [trading] vix_current_level
    — enabled without that key it silently sized every trade at the conservative
    fallback scale. Then the Public INDEX quote fed it, but ONLY on Public: the
    IBKR box ran vix_sizing_enabled=true with no source for weeks, so every
    entry was cut to vix_unavailable_scale (0.50x). Both vendors now implement
    fetch_index_level and the router dispatches, so enabling the feature on
    IBKR gets a real VIX instead of a permanent haircut.

    No-op unless vix_sizing_enabled and the active broker has a source."""
    warned = False
    while True:
        try:
            enabled = app_cfg.getboolean("trading", "vix_sizing_enabled", fallback=False)
            broker = app_cfg.get("trading", "broker", fallback="public").lower()
            if enabled and broker in ("public", "ibkr"):
                from app.execution.broker_router import fetch_index_level
                level = await fetch_index_level("VIX")
                if level:
                    from app.risk.vix_sizer import update_vix_cache
                    update_vix_cache(level)
                    warned = False
                elif not warned and not app_cfg.get("trading", "vix_current_level", fallback=""):
                    # A source that exists but returns nothing is the same
                    # silent haircut as no source at all — on IBKR this is
                    # usually a missing CBOE index market-data subscription.
                    warned = True
                    from app.risk.vix_sizer import _VIX_UNAVAILABLE_SCALE
                    log.warning(
                        "[VIX_SIZER] broker=%s returned no VIX level — entries "
                        "scale %.2fx until it does. On IBKR this is normally a "
                        "missing CBOE index market-data subscription.",
                        broker, _VIX_UNAVAILABLE_SCALE(),
                    )
            elif enabled and not app_cfg.get("trading", "vix_current_level", fallback=""):
                # Enabled, but nothing can ever populate the cache on this
                # broker and no manual level is set — so _fetch_vix_level()
                # returns None forever and vix_unavailable_scale is applied to
                # EVERY entry. That is a permanent size haircut wearing a
                # volatility-response costume, and it stayed invisible on
                # ibkr-oci for weeks (every trade silently halved). Say so.
                if not warned:
                    from app.risk.vix_sizer import _VIX_UNAVAILABLE_SCALE
                    log.warning(
                        "[VIX_SIZER] vix_sizing_enabled=true but broker=%s has no VIX "
                        "source and vix_current_level is unset — every entry will be "
                        "scaled %.2fx indefinitely. Set vix_sizing_enabled=false, or "
                        "pin vix_current_level, or set vix_unavailable_scale=1.0.",
                        broker, _VIX_UNAVAILABLE_SCALE(),
                    )
                    warned = True
        except Exception as e:
            log.debug("VIX refresher error: %s", e)
        await asyncio.sleep(60)


async def _daily_summary_scheduler():
    """Post daily P&L + pattern review + infra report at the configured ET
    clock time on weekdays. Default 16:05 ET (5 min after the cash close),
    so each report covers the trading day just finished."""
    from zoneinfo import ZoneInfo
    if not app_cfg.getboolean("trading", "daily_summary_enabled", fallback=True):
        log.info("Daily summary scheduler disabled via config (daily_summary_enabled=false)")
        return

    et = ZoneInfo("America/New_York")
    while True:
        try:
            now = datetime.now(et)
            # Configurable schedule (defaults: 16:05 ET — 5 min after market
            # close, well before the cron-stop at 17:00 ET).
            sum_hour = app_cfg.getint("trading", "daily_summary_hour", fallback=16)
            sum_min  = app_cfg.getint("trading", "daily_summary_minute", fallback=5)
            target = now.replace(hour=sum_hour, minute=sum_min, second=0, microsecond=0)

            if now >= target:
                target += timedelta(days=1)

            # Skip weekends
            while target.weekday() >= 5:  # 5=Sat, 6=Sun
                target += timedelta(days=1)

            # Clamp wait to [60s, 25h]. On the DST fall-back boundary the
            # naive .replace() can produce an ambiguous local time and the
            # delta could come out negative (or off by an hour). Clamping
            # prevents the asyncio.sleep loop from firing twice or busy-
            # spinning. Floor at 60s to keep the trailing 60s anti-retrigger
            # gap meaningful.
            wait_seconds = (target - now).total_seconds()
            wait_seconds = max(60.0, min(wait_seconds, 25 * 3600))
            log.info("Daily summary scheduled for %s (in %.0f minutes)", target.strftime("%Y-%m-%d %H:%M ET"), wait_seconds / 60)
            await asyncio.sleep(wait_seconds)

            await trade_logger.log_daily_summary()
            log.info("Daily summary posted")

            # Pattern review — bot vs analyst leak analysis. Runs immediately
            # after the P&L summary so both land in the daily-summary channel
            # together. Gated by pattern_review_enabled (default on); wrapped in
            # try/except so a review failure can't stop the scheduler.
            if app_cfg.getboolean("trading", "pattern_review_enabled", fallback=True):
                try:
                    import app.analytics.pattern_review as pattern_review
                    await pattern_review.run_and_post()
                except Exception as _pr_exc:
                    log.error("Pattern review failed: %s", _pr_exc)
            else:
                log.info("Pattern review skipped (pattern_review_enabled=false)")

            # Infra report — EC2 / ECR / container / DB / logs / API usage.
            # Gated by infra_report_enabled (default on).
            if app_cfg.getboolean("trading", "infra_report_enabled", fallback=True):
                try:
                    import app.analytics.infra_report as infra_report
                    await infra_report.run_and_post()
                except Exception as _ir_exc:
                    log.error("Infra report failed: %s", _ir_exc)
            else:
                log.info("Infra report skipped (infra_report_enabled=false)")

            # Extended analytics reports — daily breakdown / execution quality /
            # rolling analyst scorecard, plus a Friday-only weekly digest. Each
            # is best-effort (self-guards internally); wrapped here so one can't
            # stop the scheduler. Gated by extended_reports_enabled (default on)
            # and weekly_digest_enabled (default on) inside reports.py.
            try:
                import app.analytics.reports as reports
                from zoneinfo import ZoneInfo as _ZI
                await reports.post_daily_breakdown()
                await reports.post_execution_quality()
                await reports.post_analyst_scorecard()
                # Friday only (ET weekday 4).
                if datetime.now(_ZI("America/New_York")).weekday() == 4:
                    await reports.post_weekly_digest()
            except Exception as _rep_exc:
                log.error("Extended reports failed: %s", _rep_exc)

            # Retention prune — opt-in (retention_days<=0 disables). Bounds the
            # append-only alerts/discord_signals tables so the guardrail dedup
            # scan + tail latency don't drift. Trade history (positions/orders)
            # is never touched. Offloaded so the DELETE can't stall the loop.
            try:
                _rdays = app_cfg.getint("trading", "retention_days", fallback=0)
                if _rdays > 0:
                    from app.core.db import prune_old_records
                    _pruned = await asyncio.to_thread(prune_old_records, _rdays)
                    if _pruned:
                        log.info("Retention prune (>%dd): %s", _rdays, _pruned)
            except Exception as _rt_exc:
                log.error("Retention prune failed: %s", _rt_exc)

            # Cold-storage archive — opt-in (archive_after_days<=0 disables).
            # MOVE terminal trade rows (closed positions / done orders / audit)
            # older than N days out to a sibling archive.db so the active DB
            # working set stays bounded; then PURGE archive rows past the
            # multi-year retention. Trade history is preserved until purge.
            # Offloaded so the INSERT+DELETE can't stall the loop.
            try:
                _adays = app_cfg.getint("trading", "archive_after_days", fallback=0)
                if _adays > 0:
                    from app.core.db import archive_old_trades, ARCHIVE_MIN_FLOOR_DAYS
                    # Never archive inside the dashboard read window, or Calendar/
                    # Stats would silently lose rows (reads don't span archive.db).
                    if _adays < ARCHIVE_MIN_FLOOR_DAYS:
                        log.warning("archive_after_days=%d below read-window floor %d; "
                                    "clamping to protect Calendar/Stats history",
                                    _adays, ARCHIVE_MIN_FLOOR_DAYS)
                        _adays = ARCHIVE_MIN_FLOOR_DAYS
                    _arch = await asyncio.to_thread(archive_old_trades, _adays)
                    if _arch and any(_arch.values()):
                        log.info("Trade archive (>%dd → archive.db): %s", _adays, _arch)
                _ayears = app_cfg.getint("trading", "archive_retention_years", fallback=0)
                if _ayears > 0:
                    from app.core.db import purge_archive
                    _purged = await asyncio.to_thread(purge_archive, _ayears)
                    if _purged and any(_purged.values()):
                        log.info("Archive purge (>%dy): %s", _ayears, _purged)
            except Exception as _ar_exc:
                log.error("Trade archive/purge failed: %s", _ar_exc)

            await asyncio.sleep(60)
        except Exception as exc:
            log.error("Daily summary scheduler error: %s", exc)
            await asyncio.sleep(300)
