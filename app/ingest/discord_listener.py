"""
discord_listener.py - Listens to Discord for BTO/STC alerts and routes them
to the Public.com executor.

Uses an official Discord bot token with message-content intent enabled.
"""

import asyncio
import logging
import os
import re
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import discord
from sqlalchemy.exc import IntegrityError

from app.core.db import SessionLocal
from app.core.models import Alert, Position
from app.ingest.parser import parse_alert
from app.ingest.spread_parser import parse_spread_alert
from app.ingest.ai_parser import ai_parse_alert
from app.ingest.unparsed_alarm import alarm_if_order_shaped
from app.execution.broker_router import get_executor
from app.risk.guardrails import compute_content_hash
from app.core.log_config import pipeline_logger, parser_logger, PipelineTimer
from app.core.config_manager import cfg  # hot-reloadable singleton
from app.ingest.discord_slash import register_slash_commands, sync_commands
import app.analytics.trade_logger as trade_logger

log = logging.getLogger(__name__)


# Sniper writes the ticker on the entry and drops it on the exit:
#   BOUGHT MU 880C 8/7 2.85‼️3        <- has ticker, parses
#   SOLD 2/3 880C 8/7 5.05‼️           <- no ticker, parse_alert returns None
# The parser refuses these by design (parser.py:313 _search_valid_sym — a
# ticker-less "SOLD 1/2 SPX 7450C" once matched ticker="SOLD"), so the symbol
# has to come from state the parser cannot see: the analyst's own open book.
# 2026-08-07 this dropped his 2/3 profit-take on MU 880C; 3 more in 2yr of dumps.
_RE_EXIT_VERB = re.compile(
    r"\b(SOLD|STC|SELLING|CLOSING|CLOSED|TRIM\w*|SCALING|ALL\s*OUT|OUT\s+OF)\b", re.IGNORECASE)
_RE_STRIKE_EXP = re.compile(
    r"\b(\d{1,5}(?:\.\d+)?)\s*([CP])\b\s+(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?", re.IGNORECASE)


def _resolve_missing_ticker(raw_text: str, author: str) -> str:
    """Return `raw_text` with the ticker injected, or "" when it can't be resolved.

    Only fires when EXACTLY ONE open position matches the strike/type/expiry the
    message does name, scoped to positions this analyst owns. Zero matches or an
    ambiguous two both return "" — guessing which book to close is the failure
    mode this whole path exists to avoid.
    """
    if not _RE_EXIT_VERB.search(raw_text):
        return ""
    m = _RE_STRIKE_EXP.search(raw_text)
    if not m:
        return ""

    from app.ingest.parser import _parse_date
    from app.execution.public_executor import _owner_scoped, _pos_owned_by

    strike, opt = float(m.group(1)), m.group(2).upper()
    expiry = _parse_date(m.group(3), m.group(4), m.group(5))
    if expiry is None:
        return ""

    db = SessionLocal()
    try:
        rows = (db.query(Position)
                  .filter(Position.strike == strike,
                          Position.option_type == opt,
                          Position.expiry == str(expiry),
                          Position.status.in_(("OPEN", "PARTIAL")),
                          Position.remaining > 0)
                  .all())
        if _owner_scoped():
            rows = [p for p in rows if _pos_owned_by(p, author)]
        symbols = {(p.symbol or "").upper() for p in rows if p.symbol}
    finally:
        db.close()

    if len(symbols) != 1:
        parser_logger.info(
            "TICKERLESS EXIT unresolved | author=%s | %s%s %s | %d candidate symbol(s) %s",
            author, strike, opt, expiry, len(symbols), sorted(symbols) or "[]")
        return ""

    symbol = symbols.pop()
    # Insert immediately before the strike token so the message keeps its own
    # grammar (fraction, price, ALL OUT) instead of being rebuilt from parts.
    return raw_text[:m.start()] + symbol + " " + raw_text[m.start():]


def _log_task_exception(task: asyncio.Task) -> None:
    """Done-callback that surfaces exceptions from fire-and-forget tasks.

    Without this callback, exceptions raised inside asyncio.create_task() are
    only delivered to the loop's default exception handler — easy to miss in
    a busy log. Attach this so failures in trade_logger or executor surface
    with a readable traceback.
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        log.error("Background task %r failed: %s", task.get_name(), exc, exc_info=exc)


# Strong references to fire-and-forget tasks. asyncio holds only a WEAK ref to a
# bare create_task result, so a pending task with no other reference can be
# garbage-collected mid-await (silently dropping e.g. a mirrored signal). Keep a
# ref here until the task finishes; the done-callbacks discard it and log errors.
_bg_tasks: "set[asyncio.Task]" = set()


def _spawn(coro, name: str) -> asyncio.Task:
    """create_task that won't be GC'd mid-run and surfaces exceptions."""
    task = asyncio.create_task(coro, name=name)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    task.add_done_callback(_log_task_exception)
    return task


def _csv_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _spread_channel(message: discord.Message) -> bool:
    """True when this message came from a channel that posts vertical credit
    spreads and nothing else.

    Opt-in by source rather than by grammar detection: the spread parser
    running on a normal alert channel could only ever cost accuracy there, and
    a matching source has its single-leg parsing switched off entirely. Empty
    list (the default) = no channel is a spread channel.

    Matching has to cover BOTH arrival paths, and they carry the source
    differently. A directly-watched channel is `message.channel.id`. A relayed
    one is not — the forwarder re-posts into the shared #general destination,
    so the destination id says nothing about where the alert came from; the
    only surviving marker is the webhook username, `[FWD #<label>] <author>`,
    where <label> is the key from [forwarder.channels]. Matching the
    destination alone would silently fail open and hand every relayed spread
    message to the single-leg parser — which is the close_all hazard this gate
    exists to prevent. So `spread_channel_ids` accepts channel ids AND
    forwarder labels, and either one matching is enough.
    """
    if not cfg.getboolean("trading", "spreads_enabled", fallback=False):
        return False
    wanted = {s.lower() for s in _csv_list(cfg.get("trading", "spread_channel_ids", fallback=""))}
    if not wanted:
        return False
    if str(getattr(message.channel, "id", "")) in wanted:
        return True
    m = _FWD_PREFIX_RE.match(getattr(message.author, "display_name", "") or "")
    return bool(m) and m.group(1).strip().lower() in wanted


async def _maybe_route_to_signal_intel(message: discord.Message) -> None:
    """If message is from the configured signal author + channel, push it to
    the signal_intel queue. No-op when [signals].enabled=false."""
    if not cfg.getboolean("signals", "enabled", False):
        return
    target_channel = cfg.get("signals", "channel_id", fallback="").strip()
    target_author = cfg.get("signals", "author", fallback="").strip().lower()
    if not target_channel or not target_author:
        return
    if str(message.channel.id) != target_channel:
        return
    author_name = (message.author.display_name or "").lower()
    author_uname = (message.author.name or "").lower()
    if target_author not in author_name and target_author not in author_uname:
        return

    # Lazy import to avoid circular dep at module load
    import app.ingest.signal_intel as signal_intel
    image_urls = []
    for att in getattr(message, "attachments", []) or []:
        url = getattr(att, "url", None)
        if url:
            image_urls.append(url)
    # Forwarded webhook messages carry images as embed.image.url
    # (forwarder converts native attachments to image embeds since webhook
    # endpoints can't re-host the original attachment).
    for emb in getattr(message, "embeds", []) or []:
        img = getattr(emb, "image", None)
        if img and getattr(img, "url", None) and img.url not in image_urls:
            image_urls.append(img.url)
        thumb = getattr(emb, "thumbnail", None)
        if thumb and getattr(thumb, "url", None) and thumb.url not in image_urls:
            image_urls.append(thumb.url)
    payload = {
        "message_id": str(message.id),
        "author": message.author.display_name,
        "channel_id": str(message.channel.id),
        "text": message.content or "",
        "image_urls": image_urls,
        "timestamp": message.created_at.isoformat() if message.created_at else None,
    }
    log.info("[SIGNAL_INTEL] routing msg %s from %s (images=%d)",
             message.id, message.author.display_name, len(image_urls))
    await signal_intel.enqueue_message(payload)


def _is_forwarded(message: discord.Message) -> bool:
    """Check if a message is a Discord forwarded message (flags & 16384)."""
    return bool(getattr(message, "message_snapshots", None))


def _embed_text(embeds) -> list[str]:
    """Extract text from a list of Embed objects."""
    parts = []
    for embed in embeds:
        parts.extend([
            getattr(embed, "title", "") or "",
            getattr(embed, "description", "") or "",
        ])
        for field_obj in getattr(embed, "fields", []):
            parts.extend([
                getattr(field_obj, "name", "") or "",
                getattr(field_obj, "value", "") or "",
            ])
        footer = getattr(embed, "footer", None)
        if footer:
            parts.append(getattr(footer, "text", "") or "")
    return parts


def _message_text(message: discord.Message) -> str:
    # For forwarded messages, try the snapshot first; fall back to normal extraction
    if _is_forwarded(message):
        text = _forwarded_text(message)
        if text:
            return text
        log.debug("Forwarded message snapshot text empty, falling back to normal extraction")

    parts = [message.content or ""]
    parts.extend(_embed_text(message.embeds))
    return " ".join(part for part in parts if part).strip()


def _forwarded_text(message: discord.Message) -> str:
    """Extract text content from a forwarded message's snapshot."""
    parts = []
    for snapshot in message.message_snapshots:
        content = getattr(snapshot, "content", "") or ""
        if content:
            parts.append(content)
        parts.extend(_embed_text(getattr(snapshot, "embeds", [])))

    # Also check regular embeds on the message itself (forwarded embeds)
    parts.extend(_embed_text(message.embeds))
    return " ".join(part for part in parts if part).strip()


def _forwarded_author(message: discord.Message) -> str | None:
    """Try to extract the original author name from a forwarded message.
    Checks: embed author name, embed footer, snapshot content."""
    # Check embeds on the message itself (forwarded messages often have embeds)
    for embed in message.embeds:
        # Check embed author (e.g. "SARANG | 🦉")
        author = getattr(embed, "author", None)
        if author:
            name = getattr(author, "name", None)
            if name:
                return name
        # Check footer
        footer = getattr(embed, "footer", None)
        if footer:
            text = getattr(footer, "text", None)
            if text:
                return text

    # Check snapshots
    for snapshot in getattr(message, "message_snapshots", []):
        for embed in getattr(snapshot, "embeds", []):
            author = getattr(embed, "author", None)
            if author:
                name = getattr(author, "name", None)
                if name:
                    return name
            footer = getattr(embed, "footer", None)
            if footer:
                text = getattr(footer, "text", None)
                if text:
                    return text
    return None


def _author_matches(message: discord.Message, watch_authors: list[str]) -> bool:
    if not watch_authors:
        return True

    candidates = {
        getattr(message.author, "name", "") or "",
        getattr(message.author, "display_name", "") or "",
        getattr(message.author, "global_name", "") or "",
    }
    # For forwarded messages, also check the original author from embed author/footer
    if _is_forwarded(message):
        original = _forwarded_author(message)
        if original:
            candidates.add(original)

    # Also check embed author names directly (for forwarded embeds)
    for embed in message.embeds:
        author = getattr(embed, "author", None)
        if author:
            name = getattr(author, "name", None)
            if name:
                candidates.add(name)
        footer = getattr(embed, "footer", None)
        if footer:
            text = getattr(footer, "text", None)
            if text:
                candidates.add(text)

    lowered = {candidate.lower() for candidate in candidates if candidate}
    log.debug("Author candidates: %s | Watch list: %s", lowered, watch_authors)
    return any(any(author in candidate for candidate in lowered) for author in watch_authors)


def _channel_name(message: discord.Message) -> str:
    name = getattr(message.channel, "name", None)
    return f"#{name}" if name else str(message.channel.id)


_FWD_PREFIX_RE = re.compile(r"^\[FWD #([^\]]+)\]\s+(.+)$")


def _resolve_author(raw_author: str, raw_text: str) -> str:
    """Clean the author for analyst-tab attribution.

    The forwarder relays cross-server alerts via a webhook whose username is
    `[FWD #<source-channel>] <original-author>`. Strip that prefix so the
    analyst tab groups by the underlying author.

    Pro-alerts is a special case: a single bot account (Twinsight Bot) posts
    on behalf of multiple human analysts, with the analyst name appearing as
    a token in the message body (e.g. '...(+170%) spacemonkey'). Re-attribute
    those rows to the named human so each analyst tracks separately.
    """
    m = _FWD_PREFIX_RE.match(raw_author or "")
    if not m:
        return raw_author
    fwd_channel = m.group(1)
    original_author = m.group(2).strip()

    if fwd_channel == "pro-alerts":
        analysts = [
            a.strip()
            for a in cfg.get("forwarder", "pro_alerts_analysts",
                             fallback="spacemonkey,matae").split(",")
            if a.strip()
        ]
        text_lower = (raw_text or "").lower()
        for name in analysts:
            if re.search(rf"\b{re.escape(name.lower())}\b", text_lower):
                return name

    return original_author


# Cross-channel content dedup. The same forwarded signal can reach the listener
# via two watched channels — e.g. this VM's own direct-dispatch target AND a
# second channel another forwarder's webhook posts into — arriving ~200ms apart
# with DIFFERENT discord_message_ids (so the id-dedup below misses it). Drop the
# second copy silently (no alert row, no broadcast, no execution) keyed on
# content_hash within a short window. Process-local + lock-guarded.
_recent_content: dict[str, float] = {}
_recent_content_lock = threading.Lock()


def _seen_content_hash(h: str, window: float) -> bool:
    """True if this content_hash was seen within `window` seconds (records it
    on first sight, so the first/real copy passes and later copies are dropped)."""
    now = time.time()
    with _recent_content_lock:
        for k in [k for k, t in _recent_content.items() if now - t > window]:
            del _recent_content[k]
        if h in _recent_content and now - _recent_content[h] <= window:
            return True
        _recent_content[h] = now
        return False


def _persist_alert(db, message: discord.Message, alert, raw_text: str, historical: bool, content_hash: str = "") -> Alert | None:
    existing = db.query(Alert).filter(
        Alert.discord_message_id == str(message.id)
    ).first()
    if existing:
        _metric("alert_dup_rejected_total")
        return None

    # Cross-channel duplicate (same content via a different channel/message-id).
    # Skipped for historical backfill (replayed in bulk, window meaningless).
    if content_hash and not historical:
        _win = cfg.getfloat("trading", "dedup_content_window_seconds", fallback=90.0)
        if _win > 0 and _seen_content_hash(content_hash, _win):
            parser_logger.info("DEDUP cross-channel | hash=%s | %s %s",
                               content_hash[:8], alert.action, alert.osi_symbol or alert.symbol)
            _metric("alert_dup_rejected_total")
            return None

    author_name = message.author.display_name
    if _is_forwarded(message):
        original = _forwarded_author(message)
        if original:
            author_name = original
    author_name = _resolve_author(author_name, raw_text)

    msg_ts = getattr(message, "created_at", None)
    db_alert = Alert(
        timestamp=msg_ts if msg_ts else None,
        author=author_name,
        action=alert.action,
        raw_text=raw_text or f"[embed] {alert.action} {alert.osi_symbol}",
        osi_symbol=alert.osi_symbol,
        symbol=alert.symbol,
        expiry=str(alert.expiry) if alert.expiry else None,
        strike=alert.strike,
        option_type=alert.option_type,
        alert_price=float(alert.price) if alert.price is not None else None,
        size_tag=alert.size_tag,
        fraction=float(alert.fraction),
        # Persist the lane tag so resubmit/requeue paths (which only see the DB
        # row) keep it. Small-account marker wins only when no lane tag is set.
        strategy_tag=(getattr(alert, "strategy_tag", None)
                      or ("SMALL_ACCT" if getattr(alert, "small_account", False) else None)),
        # Keep the analyst's stated contract count on the row — the resubmit
        # override rebuilds the alert from here and had no way to recover it.
        qty=getattr(alert, "qty", None),
        status="HISTORICAL" if historical else "PENDING",
        channel_id=message.channel.id,
        channel_name=_channel_name(message),
        discord_message_id=str(message.id),
        is_historical=historical,
        content_hash=content_hash,
    )
    db.add(db_alert)
    try:
        db.commit()
    except IntegrityError:
        # Lost the race: another deliver of this same Discord message committed
        # first and the UNIQUE(discord_message_id) index rejected this one. Treat
        # as a duplicate — exactly what the pre-check above intends (GH #8).
        db.rollback()
        _metric("alert_dup_rejected_total")
        return None
    db.refresh(db_alert)
    _metric("alerts_received_total")
    return db_alert


def _metric(name: str) -> None:
    """Best-effort counter increment — metrics must never break ingest."""
    try:
        from app.core import metrics
        metrics.inc(name)
    except Exception:
        pass


# ── Direct-dispatch injection (forwarder → listener, no webhook round-trip) ─────
# The forwarder normally POSTs to a Discord webhook and the bot reads the message
# back (~143ms of POST + Discord redelivery, measured). When direct dispatch is
# enabled it instead hands the message straight to process_message in-process via
# inject_forwarded() below — same parse/guardrail/dispatch path, ~1ms. We rebuild
# a faithful discord.Message-shaped shim so EVERY downstream check (channel
# filter, author resolve, dedup, blacklist, broadcast) behaves identically to the
# webhook path. No new Discord interaction is added (forwarder still only reads).

# process_message is a closure over client/executor/ws_manager; publish a ref so
# the (separate) forwarder module can invoke it without those internals.
_PROCESS_REF: dict = {"fn": None}


class _Obj:
    """Recursive dict→attribute shim for embed objects. Missing keys → None;
    nested dicts/lists wrapped so getattr(embed,'image').url etc. works."""
    __slots__ = ("_d",)

    def __init__(self, d):
        object.__setattr__(self, "_d", d or {})

    def __getattr__(self, k):
        d = object.__getattribute__(self, "_d")
        if k not in d:
            # Raise so getattr(embed, "fields", []) / (..., "") defaults apply,
            # exactly like a real discord.py Embed missing an optional field.
            raise AttributeError(k)
        v = d[k]
        if isinstance(v, dict):
            return _Obj(v)
        if isinstance(v, list):
            return [_Obj(x) if isinstance(x, dict) else x for x in v]
        return v


class _ShimAuthor:
    def __init__(self, name: str):
        self.id = -1               # never equals client.user.id
        self.name = name
        self.display_name = name   # carries "[FWD #ch] author" → _resolve_author strips it
        self.global_name = name
        self.bot = True


class _ShimChannel:
    def __init__(self, cid: int, cname: str):
        self.id = cid
        self.name = cname


class _ShimMessage:
    def __init__(self, *, content, embeds, author_name, channel_id, channel_name,
                 message_id, created_at):
        self.content = content or ""
        self.embeds = [_Obj(e) for e in (embeds or [])]
        self.author = _ShimAuthor(author_name)
        self.channel = _ShimChannel(channel_id, channel_name)
        self.id = message_id
        self.created_at = created_at
        self.attachments = []          # images already passed as image embeds
        self.message_snapshots = None  # not a Discord-forwarded snapshot → _is_forwarded=False


async def inject_forwarded(*, content, embeds, author_name, channel_id, channel_name,
                           message_id, created_at) -> bool:
    """In-process equivalent of the forwarder's webhook POST. Feeds a shim message
    to the live process_message. Returns False if the listener isn't running yet
    (caller should fall back to the webhook)."""
    fn = _PROCESS_REF.get("fn")
    if fn is None:
        log.warning("inject_forwarded: listener not ready; caller should fall back to webhook")
        return False
    try:
        shim = _ShimMessage(
            content=content, embeds=embeds, author_name=author_name,
            channel_id=int(channel_id), channel_name=channel_name,
            message_id=message_id, created_at=created_at,
        )
        await fn(shim)
        return True
    except Exception:
        log.error("inject_forwarded failed", exc_info=True)
        return False


# ── Peer mirror (ultra-low-latency multi-broker fan-out) ───────────────────
# Replicate each watched message to peer instances (e.g. the Tradier secondary)
# so they trade the SAME signal in parallel. Fire-and-forget: create_task, never
# awaited on the hot path → ~0 added latency on THIS (primary) pipeline. No-op
# unless [ingest] mirror_urls is set, so the default single-instance build is
# unchanged. See app.ingest /api/ingest/forwarded on the receiving side.
_MIRROR_SESSION: dict = {"s": None}


def _mirror_targets() -> list[str]:
    return [u.strip() for u in cfg.get("ingest", "mirror_urls", fallback="").split(",") if u.strip()]


async def _post_mirror(url: str, payload: dict, secret: str) -> None:
    try:
        import aiohttp
        s = _MIRROR_SESSION.get("s")
        if s is None or s.closed:
            s = aiohttp.ClientSession()
            _MIRROR_SESSION["s"] = s
        await s.post(url, json=payload, headers={"X-Ingest-Secret": secret},
                     timeout=aiohttp.ClientTimeout(total=3))
    except Exception:
        log.debug("mirror post failed to %s", url, exc_info=True)


def mirror_message(message, historical: bool) -> None:
    """Fan a watched message out to peer instances. Fire-and-forget; never raises
    into process_message. Skips historical backfill (avoids replay storms)."""
    if historical:
        return
    targets = _mirror_targets()
    if not targets:
        return
    try:
        secret = cfg.get("ingest", "shared_secret", fallback="")
        payload = {
            "content": message.content or "",
            "embeds": [e.to_dict() for e in (message.embeds or [])],
            "author_name": message.author.display_name,
            "channel_id": int(message.channel.id),
            "channel_name": _channel_name(message),
            "message_id": str(message.id),
            "created_at": message.created_at.isoformat() if message.created_at else "",
        }
        for url in targets:
            _spawn(_post_mirror(url, payload, secret), f"mirror:{url}")
    except Exception:
        log.debug("mirror_message skipped", exc_info=True)


async def start_discord_listener(ws_manager):
    """Entry point: starts the Discord client and feeds events to the broker."""

    token = os.getenv("DISCORD_TOKEN", cfg.get("discord", "token", fallback="")).strip()
    channel_ids_str = cfg.get("discord", "channel_ids", fallback="")
    channel_ids = [int(x) for x in _csv_list(channel_ids_str)]
    history_backfill_limit = cfg.getint("discord", "history_backfill_limit", fallback=0)
    execute_history = cfg.getboolean("discord", "execute_history", fallback=False)

    # A gateway-disabled secondary (discord_listener_enabled=false) needs NO bot
    # token — it never connects. It still builds the pipeline below so the HTTP
    # mirror can drive process_message via inject_forwarded. Only require a token
    # when we actually intend to connect the gateway.
    gateway_enabled = cfg.getboolean("trading", "discord_listener_enabled", fallback=True)
    if gateway_enabled and (not token or "YOUR_DISCORD" in token.upper()):
        log.warning("Discord listener disabled: configure a real bot token in config.ini or DISCORD_TOKEN")
        return

    if not channel_ids:
        log.warning("Discord listener disabled: no channel IDs configured")
        return

    executor = get_executor(ws_manager)

    async def _handle_spread(message, spread, raw_text: str, historical: bool, timer):
        """Persist and dispatch one vertical credit spread.

        Deliberately walks the same _persist_alert path as single-leg alerts so
        spreads inherit message-id uniqueness, cross-channel dedup and author
        resolution — a duplicate delivery must not become a duplicate spread.
        """
        from app.execution.broker_router import get_spread_executor
        from app.execution.spread_executor import AlertRowView

        session = datetime.now(ZoneInfo("America/New_York")).date()
        view = AlertRowView(spread, session)
        c_hash = compute_content_hash(view.osi_symbol, view.action, view.price)

        db = SessionLocal()
        try:
            db_alert = _persist_alert(db, message, view, raw_text, historical,
                                      content_hash=c_hash)
            if db_alert is None:
                timer.finish("duplicate_message_id")
                return
            alert_db_id, author = db_alert.id, db_alert.author
            if not historical:
                asyncio.create_task(ws_manager.broadcast(
                    {"type": "alert", "data": db_alert.to_dict()}))
        finally:
            db.close()

        pipeline_logger.info(
            "SPREAD %s | %s %s %s/%s | author=%s | net=%s | elapsed=%.1fms",
            "HIST" if historical else "LIVE", spread.action, spread.kind,
            spread.short_strike, spread.long_strike, author, spread.net_price,
            timer.elapsed_ms(),
        )

        if historical and not execute_history:
            timer.finish("historical_no_execute")
            return

        spread_executor = get_spread_executor(ws_manager)

        def _factory():
            return spread_executor.execute(
                spread, alert_db_id, alert_author=author,
                content_hash=c_hash, session=session,
            )

        # Same serial worker as single-leg orders — a spread entry must not race
        # a single-leg guardrail count, and a spread CLOSE belongs in the exit
        # lane so it drains ahead of queued entries.
        from app.execution.order_queue import (
            dispatch_execution, PRIORITY_EXIT, PRIORITY_ENTRY,
        )
        _prio = PRIORITY_EXIT if spread.action == "CLOSE" else PRIORITY_ENTRY
        if not dispatch_execution(_factory, name="spread_executor.execute", priority=_prio):
            asyncio.create_task(_factory(), name="spread_executor.execute").add_done_callback(
                _log_task_exception)
        timer.action = view.action
        timer.osi = view.osi_symbol
        timer.finish("SPREAD_DISPATCHED")

    # Serialize order execution through a single consumer so concurrent alerts
    # can't race the guardrail count→insert window (GH #9). Flag-gated; when off
    # the dispatch below falls back to the legacy concurrent create_task path.
    if cfg.getboolean("trading", "serialized_order_execution", fallback=True):
        from app.execution.order_queue import start_order_worker
        start_order_worker()

    intents = discord.Intents.default()
    intents.message_content = True
    client = discord.Client(intents=intents)
    history_loaded = False

    # Register slash commands (/status, /positions, /stats)
    slash_tree = register_slash_commands(client)

    async def process_message(message: discord.Message, historical: bool = False):
        # ── Signal Intelligence routing (separate channel/author) ────────
        # Intercepts messages from the tracked signal author/channel and
        # routes them to signal_intel for OCR + bias parsing. Does NOT fall
        # through to alert parsing — signal channels are display-only by
        # default and use a distinct DB table.
        try:
            await _maybe_route_to_signal_intel(message)
        except Exception as e:
            log.error("signal_intel routing failed: %s", e, exc_info=True)
        # If this message belongs to the signal channel only (not an alert
        # channel), stop here — alert parser would just no-op and pollute logs.
        # EXCEPTION: whale lane. When whale_lane_enabled, the whale forwarded
        # channel (= signals.channel_id) ALSO falls through to the trading
        # pipeline so structured BUY alerts execute. signal_intel still ran
        # above (dashboard eval); now we let it trade too, tagged WHALE.
        signal_channel = cfg.get("signals", "channel_id", fallback="").strip()
        _whale_lane = cfg.getboolean("trading", "whale_lane_enabled", fallback=False)
        _is_whale_channel = bool(signal_channel) and str(message.channel.id) == signal_channel
        if _is_whale_channel and not _whale_lane:
            return

        # Re-read live config on every message so dashboard changes take effect
        live_channel_ids   = [int(x) for x in _csv_list(cfg.get("discord", "channel_ids", fallback=""))]
        no_filter_ids      = {int(x) for x in _csv_list(cfg.get("discord", "no_filter_channel_ids", fallback=""))}
        watch_authors      = [a.lower() for a in _csv_list(cfg.get("discord", "authors", fallback=""))]
        local_parser_enabled = cfg.getboolean("trading", "local_parser_enabled", fallback=True)
        ai_parser_enabled    = cfg.getboolean("trading", "ai_parser_enabled",    fallback=False)

        if message.channel.id not in live_channel_ids:
            return
        if client.user and message.author.id == client.user.id:
            return

        # Fan out to peer instances (Tradier secondary, etc.) BEFORE the heavy
        # pipeline so both brokers act on the signal near-simultaneously. Fire-
        # and-forget → no latency cost here. No-op unless [ingest] mirror_urls set.
        mirror_message(message, historical)

        # ── Pipeline timer starts here ───────────────────────────────────
        timer = PipelineTimer(
            action="?", osi_symbol="?",
            author=message.author.display_name,
            channel=_channel_name(message),
        )
        timer.mark("msg_received")

        is_fwd = _is_forwarded(message)
        raw_text = _message_text(message)
        timer.mark("text_extracted")

        log.info(
            "MSG [ch=%s] author=%s forwarded=%s text=%s",
            message.channel.id, message.author.display_name, is_fwd,
            raw_text[:150] if raw_text else '[empty]',
        )

        if message.channel.id not in no_filter_ids and not _author_matches(message, watch_authors):
            parser_logger.debug("SKIP author_filter | author=%s | text=%s", message.author.display_name, raw_text[:80])
            timer.mark("author_filtered")
            timer.action = "SKIP"
            timer.finish("author_not_in_watch_list")
            return

        # ── Parse ──────���─────────────────────────────────────────────────
        alert = None
        parser_used = None

        timer.mark("parse_start")

        # Vertical credit spreads get first refusal on configured channels, and
        # must run BEFORE parse_alert rather than as a fallback after it. The
        # single-leg parser does not ignore this grammar — it half-reads it, and
        # turns every "CLOSED: CCS SPX 7390/95 $1.20 ALL OUT" into a
        # symbol-less close_all SELL that would flatten the author's whole book
        # (the 2026-06-11 failure mode). Running second would never get the
        # chance to prevent that.
        if _spread_channel(message):
            spread = parse_spread_alert(raw_text)
            if spread is not None:
                parser_logger.info("SPREAD OK | %s %s %s/%s | text=%s",
                                   spread.action, spread.kind, spread.short_strike,
                                   spread.long_strike, raw_text[:100])
                await _handle_spread(message, spread, raw_text, historical, timer)
                return
            # A spread channel carries nothing the single-leg parser should
            # act on, so an unparsed message stops here instead of falling
            # through to be misread.
            parser_logger.debug("SPREAD NO-PARSE (chatter) | text=%s", raw_text[:100])
            timer.action = "SKIP"
            timer.finish("spread_channel_chatter")
            return

        if local_parser_enabled:
            alert = parse_alert(raw_text, channel_id=message.channel.id)
            if alert:
                parser_used = "regex"
                parser_logger.info("REGEX OK | %s %s %s | text=%s", alert.action, alert.symbol, alert.osi_symbol, raw_text[:100])
        timer.mark("regex_done")

        # Ticker-less exit ("SOLD 2/3 880C 8/7 5.05") — resolve the symbol from
        # the analyst's own open book and re-parse. Runs before the AI fallback
        # because a DB lookup is cheaper and more certain than an LLM guess.
        # One-knob revert: resolve_missing_ticker_on_exit=false.
        if alert is None and local_parser_enabled and cfg.getboolean(
            "trading", "resolve_missing_ticker_on_exit", fallback=False
        ):
            _author = message.author.display_name
            if _is_forwarded(message):
                _author = _forwarded_author(message) or _author
            _author = _resolve_author(_author, raw_text)
            _patched = _resolve_missing_ticker(raw_text, _author)
            if _patched:
                alert = parse_alert(_patched, channel_id=message.channel.id)
                if alert:
                    parser_used = "regex+ticker_resolved"
                    parser_logger.warning(
                        "TICKERLESS EXIT resolved | author=%s | %s %s | text=%s",
                        _author, alert.action, alert.osi_symbol, raw_text[:100])
        timer.mark("ticker_resolve_done")

        if alert is None and ai_parser_enabled:
            parser_logger.info("REGEX MISS → AI fallback | text=%s", raw_text[:100])
            timer.mark("ai_start")
            alert = await ai_parse_alert(raw_text, channel_id=message.channel.id)
            if alert:
                parser_used = "ai"
                parser_logger.info("AI OK | %s %s %s | text=%s", alert.action, alert.symbol, alert.osi_symbol, raw_text[:100])
            else:
                parser_logger.info("AI MISS | text=%s", raw_text[:100])
            timer.mark("ai_done")

        if alert is None:
            parser_logger.debug("NO PARSE | text=%s", raw_text[:100])
            # Page only when the message was shaped like a real order. A silent
            # drop here is how Sniper's 2026-08-07 MU 880C exit was lost.
            asyncio.create_task(alarm_if_order_shaped(
                raw_text, message.author.display_name, _channel_name(message),
            )).add_done_callback(_log_task_exception)
            timer.action = "SKIP"
            timer.finish("unparseable")
            return

        timer.action = alert.action
        timer.osi = alert.osi_symbol or alert.symbol
        timer.mark("parse_complete")

        # On-mention quote warmer: the instant a full OSI is parsed (BUY, SELL, or
        # any re-mention), kick off the IBKR streaming subscription so a later or
        # repeated order on this contract is warm and skips the cold first-tick
        # wait in its hot path. Fired here — the earliest point the OSI is known,
        # before db-persist/dispatch/guardrails — for max head start. Broker- and
        # flag-gated (no-op on Public / when prewarm_quote_enabled is off); never
        # blocks ingest. Skipped for historical backfill.
        if alert.osi_symbol and not historical:
            try:
                from app.execution.broker_router import prewarm_symbol
                prewarm_symbol(alert.osi_symbol)
            except Exception:
                pass

        log.info("  → parsed by %s: %s %s %s", parser_used, alert.action, alert.symbol, alert.osi_symbol)

        # Whale lane: stamp the alert so the executor applies whale caps
        # (1 contract, $500) and the position resolves [profile:whale] for its
        # +15% auto-exit. Only when this is the whale channel + lane enabled.
        if _is_whale_channel and _whale_lane:
            alert.strategy_tag = "WHALE"
            log.info("  → WHALE lane: tagged alert strategy_tag=WHALE")

        # ── Content hash ─────────────────────────────────────────────────
        c_hash = compute_content_hash(alert.osi_symbol, alert.action, alert.price)
        timer.mark("hash_done")
        log.info("  → content hash: %s", c_hash[:8])

        # ── Persist to DB ────��───────────────────────────────────────────
        timer.mark("db_persist_start")
        db = SessionLocal()
        try:
            db_alert = _persist_alert(db, message, alert, raw_text, historical, content_hash=c_hash)
            if db_alert is None:
                timer.finish("duplicate_message_id")
                return

            # ── Layer 1: blacklisted author → block at ingest, never execute ──
            # Stamp BLOCKED *before* the first broadcast so the dashboard never
            # renders a phantom PENDING "awaiting fill" row (server never sends
            # an order). Applies to BUY and SELL/close_all (2026-06-11 incident:
            # blacklisted 'TC'/'Sarang' alerts were dropped correctly but the
            # pre-drop PENDING broadcast made the feed look like a live order).
            if cfg.getboolean("trading", "blacklist_drops_at_ingest", fallback=True):
                from app.risk.guardrails import _ANALYST_BLACKLIST, blacklisted_sell_may_exit
                _bl = _ANALYST_BLACKLIST()
                # An override-opened position is stamped with the blacklisted
                # author, so dropping their exit here strands it — nothing
                # automated can close what only they own (2026-09-17 SPCX).
                _owner_exit = blacklisted_sell_may_exit(
                    alert.action, db_alert.author or "", alert.osi_symbol or "")
                if _bl and not _owner_exit and any(tok in (db_alert.author or "").lower() for tok in _bl):
                    # status SKIPPED (not "BLOCKED") — the dashboard's isBad set
                    # is ['SKIPPED','ERROR','REJECTED'] and renders SKIPPED as the
                    # 🚫 BLOCKED badge + reason. "BLOCKED" is unrecognized.
                    db_alert.status = "SKIPPED"
                    db_alert.error_text = f"ANALYST_BLACKLIST: '{db_alert.author}' dropped at ingest (not executed)"
                    db.commit()
                    # Forget the content hash: a dropped copy must not poison
                    # cross-channel dedup for a later copy from an ALLOWED
                    # author. 2026-08-04 INTC 110C: blacklisted whale-tracker
                    # mirror arrived 77s before the real #sniper-alerts post,
                    # which then died as duplicate_message_id and never traded.
                    if db_alert.content_hash:
                        with _recent_content_lock:
                            _recent_content.pop(db_alert.content_hash, None)
                    parser_logger.info(
                        "INGEST DROP | blacklisted author=%s | %s %s | text=%s",
                        db_alert.author, alert.action, alert.osi_symbol or alert.symbol,
                        raw_text[:80],
                    )
                    if not historical:
                        await ws_manager.broadcast({"type": "alert", "data": db_alert.to_dict()})
                    timer.finish("BLACKLIST_INGEST_DROP")
                    return

            if not historical:
                # Fire-and-forget: executor dispatch must never queue behind
                # dashboard WS I/O (a slow client costs up to the 2s send timeout).
                asyncio.create_task(ws_manager.broadcast({"type": "alert", "data": db_alert.to_dict()}))
                _t = asyncio.create_task(trade_logger.log_alert_received(
                    action=alert.action, symbol=alert.symbol, osi=alert.osi_symbol,
                    price=float(alert.price) if alert.price else 0,
                    author=db_alert.author, channel=db_alert.channel_name,
                    size_tag=alert.size_tag, fraction=float(alert.fraction),
                    is_historical=historical,
                ), name="trade_logger.log_alert_received")
                _t.add_done_callback(_log_task_exception)
        finally:
            db.close()
        timer.mark("db_persist_done")

        pipeline_logger.info(
            "ALERT %s | %s %s | author=%s channel=%s parser=%s price=%s size=%s | elapsed=%.1fms",
            "HIST" if historical else "LIVE",
            alert.action, alert.osi_symbol or alert.symbol,
            db_alert.author, db_alert.channel_name, parser_used,
            alert.price, alert.size_tag, timer.elapsed_ms(),
        )

        # ── Dispatch to executor ─────────────────────────────────────────
        if not historical or execute_history:
            timer.mark("dispatch_to_executor")
            # Capture args now; the factory builds the coroutine when it runs so
            # serialized and legacy paths are semantically identical.
            _exec_alert, _exec_aid = alert, db_alert.id
            _exec_author, _exec_hash, _exec_timer = db_alert.author, c_hash, timer

            def _exec_factory():
                return executor.execute(
                    _exec_alert, _exec_aid,
                    alert_author=_exec_author,
                    content_hash=_exec_hash,
                    _pipeline_timer=_exec_timer,
                )

            # Serial path first; falls back to concurrent create_task when the
            # worker isn't running (flag off) or the queue is saturated. Exits
            # (SELL/close_all) get the priority lane so a flatten cascade never
            # waits behind queued BUYs.
            from app.execution.order_queue import (
                dispatch_execution, PRIORITY_EXIT, PRIORITY_ENTRY,
            )
            _prio = PRIORITY_EXIT if (_exec_alert.action or "").upper() == "SELL" else PRIORITY_ENTRY
            if not dispatch_execution(_exec_factory, name="executor.execute", priority=_prio):
                _exec_t = asyncio.create_task(_exec_factory(), name="executor.execute")
                _exec_t.add_done_callback(_log_task_exception)

    # Publish the closure so the forwarder's direct-dispatch path can invoke the
    # exact same pipeline in-process (see inject_forwarded above).
    _PROCESS_REF["fn"] = process_message

    async def backfill_history():
        nonlocal history_loaded
        if history_loaded or history_backfill_limit <= 0:
            return

        history_loaded = True
        for channel_id in channel_ids:
            try:
                channel = client.get_channel(channel_id) or await client.fetch_channel(channel_id)
                if channel is None:
                    log.warning("Unable to resolve channel %s for history backfill", channel_id)
                    continue

                messages = [message async for message in channel.history(limit=history_backfill_limit, oldest_first=True)]
                for message in messages:
                    await process_message(message, historical=True)
            except discord.Forbidden:
                log.warning("Missing permission to read history for channel %s", channel_id)
            except Exception as exc:
                log.error("History backfill failed for channel %s: %s", channel_id, exc)

    @client.event
    async def on_ready():
        log.info("Discord connected as %s | watching channels %s", client.user, channel_ids)
        # Sync slash command tree with Discord (first run or after code update)
        await sync_commands(slash_tree, client)
        await backfill_history()

    @client.event
    async def on_message(message: discord.Message):
        await process_message(message, historical=False)

    # Secondary (HTTP-mirror-fed) instance: the pipeline is built and
    # _PROCESS_REF is registered above, so inject_forwarded works — but we do
    # NOT connect the gateway (no bot token session). Park the task forever so
    # the monitored wrapper doesn't respawn it in a tight loop.
    if not gateway_enabled:
        log.info("Discord gateway CONNECT skipped (discord_listener_enabled=false) "
                 "— pipeline ready; ingest via HTTP mirror only")
        await asyncio.Event().wait()
        return

    while True:
        try:
            await client.start(token)
            return
        except Exception as exc:
            log.error("Discord client error: %s", exc)
            await asyncio.sleep(15)
