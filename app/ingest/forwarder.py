"""
forwarder.py - Forwards messages from trading-server channels you can see
with your personal Discord account to your own #general channel via a webhook.

How it works:
  1. Connects to Discord Gateway using YOUR personal user token
  2. Watches the source channel IDs (trading server channels)
  3. Forwards every message to a webhook URL on your #general channel
  4. Your existing bot picks up the webhook messages in #general and processes them

Setup:
  1. Get your personal Discord token (see instructions in config.ini)
  2. Create a webhook in your #general channel:
       Server Settings → Integrations → Webhooks → New Webhook → Copy URL
  3. Get the source channel IDs from the trading server
  4. Fill in the [forwarder] section of config.ini
  5. Run:  python forwarder.py
"""

import asyncio
import configparser
import json
import logging
import os
import time

import aiohttp

# Use centralized logging when run as part of the app (log_config already called).
# When run standalone, fall back to basicConfig so the module still works.
try:
    from app.core.log_config import forwarder_logger as log
except ImportError:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [forwarder] %(message)s",
    )
    log = logging.getLogger(__name__)

# Use centralized config_manager for hot-reload support
from app.core.config_manager import cfg as _cfg

def _get_forwarder_config():
    """Get forwarder config with hot-reload support via config_manager."""
    return {
        "user_token": os.getenv("DISCORD_USER_TOKEN", _cfg.get("forwarder", "user_token", fallback="")).strip(),
        "webhook_url": os.getenv("DISCORD_WEBHOOK_URL", _cfg.get("forwarder", "webhook_url", fallback="")).strip(),
        "forward_authors": [
            a.strip().lower()
            for a in _cfg.get("forwarder", "forward_authors", fallback="").split(",")
            if a.strip()
        ],
    }

def _DIRECT_DISPATCH() -> bool:
    """Flag: hand forwarded messages straight to the listener in-process instead
    of round-tripping through a Discord webhook (~143ms saved). OFF by default —
    flipping it back to false is the one-knob rollback to the webhook path."""
    return _cfg.getboolean("forwarder", "direct_dispatch", fallback=False)


def _DIRECT_CHANNEL_ID() -> int:
    """Destination channel id the in-process shim presents — MUST be one the
    listener watches (the same channel the webhook posts to). 0 disables direct
    dispatch (falls back to webhook) so a misconfig can't silently drop signals."""
    return _cfg.getint("forwarder", "direct_dispatch_channel_id", fallback=0)


def _get_channel_names():
    """Get channel names mapping with hot-reload support."""
    channel_names = {}
    if _cfg.has_section("forwarder.channels"):
        for name, channel_id in _cfg.items("forwarder.channels"):
            cid = channel_id.strip()
            if cid.isdigit():
                channel_names[int(cid)] = name
    return channel_names

# Legacy config reading for standalone usage (when imported as module)
config = configparser.ConfigParser(inline_comment_prefixes=(";", "#"))
config.read("config.ini", encoding="utf-8-sig")

# ── Config ────────────────────────────────────────────────────────────────────
# These are module-level defaults; live values come from _get_forwarder_config()

USER_TOKEN = os.getenv("DISCORD_USER_TOKEN",  config.get("forwarder", "user_token",  fallback="")).strip()
WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", config.get("forwarder", "webhook_url", fallback="")).strip()

# Read channels from [forwarder.channels] section (name = id format)
# Falls back to old comma-separated source_channel_ids if section doesn't exist
CHANNEL_NAMES = {}  # id -> name for logging
if config.has_section("forwarder.channels"):
    for name, channel_id in config.items("forwarder.channels"):
        cid = channel_id.strip()
        if cid.isdigit():
            CHANNEL_NAMES[int(cid)] = name
    SOURCE_CHANNEL_IDS = set(CHANNEL_NAMES.keys())
else:
    # Fallback: old comma-separated format
    SOURCE_CHANNEL_IDS = {
        int(x.strip())
        for x in config.get("forwarder", "source_channel_ids", fallback="").split(",")
        if x.strip()
    }

# Per-source-channel webhook overrides (source_channel_id -> webhook_url).
# Channels not in this map use the default WEBHOOK_URL.
CHANNEL_WEBHOOKS: dict[int, str] = {}
if config.has_section("forwarder.webhooks"):
    for src_id, url in config.items("forwarder.webhooks"):
        if src_id.strip().isdigit() and url.strip().startswith("http"):
            CHANNEL_WEBHOOKS[int(src_id.strip())] = url.strip()


def _webhook_for(source_channel_id: int) -> str:
    """Pick the webhook URL for a source channel — per-channel override
    or the default. Empty string means no destination configured."""
    return CHANNEL_WEBHOOKS.get(source_channel_id, WEBHOOK_URL)

# Per-source-channel author allowlist (source_channel_id -> [author substrings]).
# Channels not listed here fall back to global FORWARD_AUTHORS.
CHANNEL_AUTHOR_FILTERS: dict[int, list[str]] = {}
if config.has_section("forwarder.author_filters"):
    for src_id, authors_csv in config.items("forwarder.author_filters"):
        if src_id.strip().isdigit():
            authors = [a.strip().lower() for a in authors_csv.split(",") if a.strip()]
            if authors:
                CHANNEL_AUTHOR_FILTERS[int(src_id.strip())] = authors


def _author_filter_for(source_channel_id: int) -> list[str]:
    """Per-channel author allowlist if any, otherwise the global filter."""
    return CHANNEL_AUTHOR_FILTERS.get(source_channel_id, FORWARD_AUTHORS)

# Optional: only forward messages from specific authors (empty = forward all)
FORWARD_AUTHORS = [
    a.strip().lower()
    for a in config.get("forwarder", "forward_authors", fallback="").split(",")
    if a.strip()
]

GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
HEARTBEAT_INTERVAL = 41.25  # default; overridden by server HELLO

# Cross-channel dedup at forwarder level: track content hashes of recently forwarded messages
# to prevent forwarding the same alert from multiple source channels
import hashlib

FORWARDER_DEDUP_WINDOW = 120  # seconds
_forwarded_hashes: dict[str, float] = {}  # hash -> timestamp


def _content_hash(text: str) -> str:
    """Generate a hash of the message content for dedup (normalized).

    128-bit prefix (32 hex chars) — birthday-collision space is large enough
    that legitimate distinct alerts will not silently dedupe at our message
    rates. 64-bit was the prior value and was within reach of collisions.
    """
    normalized = text.strip().lower()
    return hashlib.sha256(normalized.encode()).hexdigest()[:32]


def _is_forwarder_duplicate(content: str, embeds_raw: list) -> bool:
    """Check if this content was recently forwarded (from another channel)."""
    # Build text from content + embed descriptions for hashing
    parts = [content or ""]
    for e in embeds_raw:
        parts.append(e.get("description", "") or "")
        parts.append(e.get("title", "") or "")
        for f in e.get("fields", []):
            parts.append(f.get("value", "") or "")
    combined = " ".join(p for p in parts if p).strip()
    if not combined:
        return False

    h = _content_hash(combined)
    now = time.time()

    # Clean old entries
    cutoff = now - FORWARDER_DEDUP_WINDOW
    expired = [k for k, v in _forwarded_hashes.items() if v < cutoff]
    for k in expired:
        del _forwarded_hashes[k]

    return h in _forwarded_hashes


def _mark_forwarded(content: str, embeds_raw: list) -> None:
    """Record that this content was successfully forwarded.

    Split from `_is_forwarder_duplicate` so we only record AFTER the webhook
    send succeeds. Recording before send would permanently drop messages on
    transient webhook failures (4xx/5xx/timeout) — the natural retry on the
    next gateway redelivery would hit the dedup cache and be silently
    dropped."""
    parts = [content or ""]
    for e in embeds_raw or []:
        parts.append(e.get("description", "") or "")
        parts.append(e.get("title", "") or "")
        for f in e.get("fields", []):
            parts.append(f.get("value", "") or "")
    combined = " ".join(p for p in parts if p).strip()
    if not combined:
        return
    _forwarded_hashes[_content_hash(combined)] = time.time()


# ── Webhook sender ───────────────────────────────────────────────────────────

async def send_to_webhook(session: aiohttp.ClientSession, content: str,
                          username: str = "Forwarder", embeds: list = None,
                          webhook_url: str = ""):
    """Post a message to a Discord webhook with exponential backoff.
    webhook_url overrides the module-level WEBHOOK_URL (per-source routing)."""
    import time
    from app.monitors.api_monitor import monitor

    target_url = webhook_url or WEBHOOK_URL
    if not target_url:
        log.error("send_to_webhook: no webhook URL configured")
        return False

    payload = {"username": username}
    if content:
        payload["content"] = content[:2000]
    if embeds:
        payload["embeds"] = embeds[:10]

    if "content" not in payload and "embeds" not in payload:
        log.debug("Skipping webhook POST: empty content and embeds")
        return True

    start = time.perf_counter()
    final_status = 0
    error_msg = None

    for attempt in range(3):
        backoff = 2 ** (attempt + 1)  # 2s, 4s, 8s
        try:
            async with session.post(target_url, json=payload) as resp:
                final_status = resp.status
                if resp.status == 204:
                    elapsed_ms = (time.perf_counter() - start) * 1000
                    monitor.record_call("discord_webhook", "webhook_post", 204, elapsed_ms)
                    return True
                if resp.status == 429:  # rate-limited
                    retry_data = await resp.json()
                    wait = retry_data.get("retry_after", backoff)
                    log.warning("Webhook rate limited, waiting %.1fs (attempt %d)", wait, attempt + 1)
                    await asyncio.sleep(wait)
                    continue
                if resp.status >= 500:  # server error — retry with backoff
                    body = await resp.text()
                    log.warning("Webhook server error %d (attempt %d): %s", resp.status, attempt + 1, body[:100])
                    await asyncio.sleep(backoff)
                    continue
                # 4xx client errors — don't retry
                body = await resp.text()
                log.error("Webhook POST returned %d: %s", resp.status, body[:200])
                elapsed_ms = (time.perf_counter() - start) * 1000
                monitor.record_call("discord_webhook", "webhook_post", resp.status, elapsed_ms, body[:100])
                return False
        except Exception as exc:
            log.error("Webhook POST error (attempt %d/%d): %s", attempt + 1, 3, exc)
            error_msg = str(exc)
            await asyncio.sleep(backoff)

    log.error("Webhook POST failed after 3 attempts")
    elapsed_ms = (time.perf_counter() - start) * 1000
    monitor.record_call("discord_webhook", "webhook_post", final_status or 500, elapsed_ms, error_msg or "Max retries exceeded")
    return False


def _format_embeds(embeds_raw: list) -> list:
    """Convert Discord API embed dicts to webhook-compatible embed dicts."""
    result = []
    for e in embeds_raw:
        embed = {}
        if e.get("title"):
            embed["title"] = e["title"]
        if e.get("description"):
            embed["description"] = e["description"]
        if e.get("color"):
            embed["color"] = e["color"]
        if e.get("fields"):
            embed["fields"] = e["fields"]
        if e.get("footer"):
            embed["footer"] = e["footer"]
        if e.get("author"):
            embed["author"] = e["author"]
        if e.get("thumbnail"):
            embed["thumbnail"] = e["thumbnail"]
        if e.get("image"):
            embed["image"] = e["image"]
        if embed:
            result.append(embed)
    return result


# ── Gateway connection ────────────────────────────────────────────────────────

async def _heartbeat(ws, interval: float, sequence: dict, hb_state: dict):
    """Send op-1 heartbeats at the HELLO interval.

    Hardened against the reconnect-churn / "Cannot write to closing transport"
    storm (2026-06-16):
      • Discord ACK tracking — if a heartbeat goes un-ACKed by the next beat the
        link is a zombie; close it (code 4000) so the main loop reconnects and
        RESUMEs instead of silently thrashing.
      • Never raises into a bare task — a send on a closing/closed socket exits
        the loop quietly and lets the main loop own the reconnect.
      • Jittered first beat per the gateway spec."""
    import random
    await asyncio.sleep(interval * random.random())  # spec-mandated startup jitter
    while True:
        # Previous beat never ACKed → zombie connection. Stop beating + close so
        # the outer loop reconnects (was: kept beating into a dead link forever).
        if not hb_state.get("acked", False):
            log.warning("Heartbeat not ACKed within interval — zombie link, forcing reconnect")
            try:
                await ws.close(code=4000)
            except Exception:
                pass
            return
        hb_state["acked"] = False
        try:
            await ws.send_json({"op": 1, "d": sequence.get("s")})
        except (ConnectionResetError, aiohttp.ClientError, RuntimeError):
            # Socket closing/closed — exit quietly; main loop handles reconnect.
            return
        log.debug("Heartbeat sent (seq=%s)", sequence.get("s"))
        await asyncio.sleep(interval)


async def run_forwarder():
    """Main forwarder loop: connect to Discord gateway and relay messages."""

    if not USER_TOKEN:
        log.error("No user_token configured in [forwarder] section of config.ini")
        log.error("See config.ini for instructions on how to get your token")
        return
    if not WEBHOOK_URL:
        log.error("No webhook_url configured in [forwarder] section of config.ini")
        return
    if not SOURCE_CHANNEL_IDS:
        log.error("No source_channel_ids configured in [forwarder] section of config.ini")
        return

    log.info("Starting forwarder...")
    log.info("  Source channels (%d):", len(SOURCE_CHANNEL_IDS))
    for cid in SOURCE_CHANNEL_IDS:
        name = CHANNEL_NAMES.get(cid, str(cid))
        log.info("    • #%s (%s)", name, cid)
    log.info("  Author filter: %s", FORWARD_AUTHORS if FORWARD_AUTHORS else "(all)")
    log.info("  Webhook: %s...%s", WEBHOOK_URL[:50], WEBHOOK_URL[-10:])

    session = aiohttp.ClientSession()
    sequence = {"s": None}
    resume_url = None
    session_id = None
    heartbeat_task = None  # cleaned up on every reconnect, see loop tail

    # Connection-level timeouts. 2026-05-05 incident: DNS resolution failure on
    # Discord gateway hung the websocket connect for 77 minutes before progressing,
    # silently dropping all analyst BUY alerts during pre-market. Cap each
    # ws_connect attempt and HELLO receive at finite seconds so a hung TCP/TLS
    # handshake fails fast and the outer reconnect loop can retry.
    WS_CONNECT_TIMEOUT_S = 15.0
    WS_HELLO_TIMEOUT_S = 15.0

    try:
        while True:
            gateway = resume_url or GATEWAY_URL
            try:
                async with session.ws_connect(
                    gateway,
                    timeout=WS_CONNECT_TIMEOUT_S,
                    receive_timeout=60.0,
                ) as ws:
                    # 1. Receive HELLO (with explicit timeout — ws.receive_timeout
                    #    only applies to the underlying message loop, not the first
                    #    receive after handshake on some aiohttp versions).
                    hello = await asyncio.wait_for(ws.receive_json(), timeout=WS_HELLO_TIMEOUT_S)
                    if hello.get("op") != 10:
                        log.error("Expected HELLO (op=10), got: %s", hello)
                        await asyncio.sleep(5)
                        continue

                    heartbeat_interval = hello["d"]["heartbeat_interval"] / 1000
                    # Fresh ACK state per connection; primed True so the first
                    # beat sends. op-11 flips it back on each ACK.
                    hb_state = {"acked": True}
                    heartbeat_task = asyncio.create_task(
                        _heartbeat(ws, heartbeat_interval, sequence, hb_state)
                    )

                    # 2. Send IDENTIFY (or RESUME)
                    if session_id and sequence["s"] is not None:
                        await ws.send_json({
                            "op": 6,
                            "d": {
                                "token": USER_TOKEN,
                                "session_id": session_id,
                                "seq": sequence["s"],
                            }
                        })
                        log.info("Sent RESUME (session=%s, seq=%s)", session_id, sequence["s"])
                    else:
                        await ws.send_json({
                            "op": 2,
                            "d": {
                                "token": USER_TOKEN,
                                "properties": {
                                    "os": "windows",
                                    "browser": "chrome",
                                    "device": "pc",
                                },
                                "presence": {
                                    "status": "invisible",
                                    "afk": True,
                                },
                            }
                        })
                        log.info("Sent IDENTIFY")

                    # Health heartbeat — fire from INSIDE the running gateway loop
                    # so a hung connection (2026-05-05 incident: 77-min DNS hang)
                    # stops emitting heartbeats and trips the staleness detector.
                    # Throttled to once per 30s to avoid hammering health_monitor.
                    last_hb = 0.0
                    try:
                        from app.monitors.health_monitor import heartbeat as _hb
                    except Exception:
                        async def _hb(_): return

                    # 3. Event loop
                    async for msg in ws:
                        # Heartbeat opportunistically on each message-loop tick.
                        _now = time.time()
                        if _now - last_hb >= 30.0:
                            try:
                                await _hb("forwarder")
                            except Exception:
                                pass
                            last_hb = _now

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except json.JSONDecodeError as exc:
                                # Discord may emit a malformed/truncated frame
                                # under load. Without this guard, JSONDecodeError
                                # propagates out of run_forwarder and the bot
                                # stops receiving alerts until manual restart.
                                log.warning("Skipping non-JSON gateway frame: %s | data=%s",
                                            exc, (msg.data or "")[:120])
                                continue
                            op = data.get("op")
                            t = data.get("t")
                            d = data.get("d", {})

                            if data.get("s"):
                                sequence["s"] = data["s"]

                            # Handle gateway events
                            if op == 0:  # DISPATCH
                                if t == "READY":
                                    session_id = d.get("session_id")
                                    resume_url = d.get("resume_gateway_url")
                                    user = d.get("user", {})
                                    log.info(
                                        "Connected as %s#%s (id: %s)",
                                        user.get("username"), user.get("discriminator"),
                                        user.get("id"),
                                    )
                                    log.info("Watching %d source channel(s)", len(SOURCE_CHANNEL_IDS))

                                elif t == "GUILD_CREATE":
                                    # Discord user accounts must lazy-subscribe to large guilds
                                    # before MESSAGE_CREATE events are delivered. Send op 14 for
                                    # each source channel that belongs to this guild.
                                    guild_id = d.get("id")
                                    guild_channels = {
                                        int(c.get("id", 0)) for c in d.get("channels", [])
                                    }
                                    matching = SOURCE_CHANNEL_IDS & guild_channels
                                    if not matching:
                                        continue
                                    guild_name = d.get("name", "?")
                                    log.info(
                                        "GUILD_CREATE %s (%s) — subscribing to %d source channel(s)",
                                        guild_name, guild_id, len(matching),
                                    )
                                    channels_payload = {str(cid): [[0, 99]] for cid in matching}
                                    await ws.send_json({
                                        "op": 14,
                                        "d": {
                                            "guild_id": guild_id,
                                            "typing": True,
                                            "activities": True,
                                            "threads": True,
                                            "channels": channels_payload,
                                        },
                                    })

                                elif t == "MESSAGE_CREATE":
                                    channel_id = int(d.get("channel_id", 0))
                                    if channel_id not in SOURCE_CHANNEL_IDS:
                                        continue

                                    author = d.get("author", {})
                                    author_name = author.get("username", "unknown")
                                    display_name = (
                                        d.get("member", {}).get("nick")
                                        or author.get("global_name")
                                        or author_name
                                    )

                                    # Author filter — per-channel override falls back to global
                                    author_filter = _author_filter_for(channel_id)
                                    if author_filter:
                                        if not any(
                                            fa in author_name.lower()
                                            or fa in display_name.lower()
                                            for fa in author_filter
                                        ):
                                            log.debug(
                                                "Skipped msg from %s in #%s (not in author filter)",
                                                display_name, CHANNEL_NAMES.get(channel_id, channel_id),
                                            )
                                            continue

                                    content = d.get("content", "")
                                    embeds_raw = d.get("embeds", [])
                                    attachments_raw = d.get("attachments", []) or []

                                    # Convert image attachments → embeds so webhook preserves them.
                                    # Discord webhooks won't re-host attachments, but they will
                                    # render embed.image.url, and downstream listeners can scrape
                                    # those URLs for OCR.
                                    image_embeds = []
                                    image_attachment_urls = []
                                    for att in attachments_raw:
                                        url = att.get("url") or att.get("proxy_url")
                                        if not url:
                                            continue
                                        ctype = (att.get("content_type") or "").lower()
                                        fname = (att.get("filename") or "").lower()
                                        is_image = ctype.startswith("image/") or fname.endswith(
                                            (".png", ".jpg", ".jpeg", ".webp", ".gif")
                                        )
                                        if is_image:
                                            image_embeds.append({"image": {"url": url}})
                                            image_attachment_urls.append(url)

                                    channel_name = CHANNEL_NAMES.get(channel_id, str(channel_id))

                                    # Cross-channel dedup: skip if same content was already forwarded
                                    if _is_forwarder_duplicate(content, embeds_raw):
                                        log.info(
                                            "DEDUP SKIP | #%s | author=%s | content=%s",
                                            channel_name,
                                            display_name,
                                            (content or "[embed]")[:80],
                                        )
                                        continue

                                    log.info(
                                        "FORWARD | #%s | author=%s | content=%s",
                                        channel_name,
                                        display_name,
                                        (content or "[embed]")[:100],
                                    )

                                    # Forward the message — pick per-source webhook override if any
                                    embeds = _format_embeds(embeds_raw) if embeds_raw else []
                                    if image_embeds:
                                        embeds = (embeds or []) + image_embeds
                                    embeds = embeds or None
                                    forward_name = f"[FWD #{channel_name}] {display_name}"

                                    # ── Direct dispatch (flag-gated) ──────────
                                    # Hand the message straight to the listener
                                    # in-process — skips webhook POST + Discord
                                    # redelivery (~143ms measured). Replaces the
                                    # webhook (no double-processing). Falls back to
                                    # the webhook if disabled, no dest channel, or
                                    # the listener isn't ready yet.
                                    if _DIRECT_DISPATCH() and _DIRECT_CHANNEL_ID():
                                        from datetime import datetime, timezone
                                        try:
                                            _ts = d.get("timestamp")
                                            _created = datetime.fromisoformat(_ts) if _ts else datetime.now(timezone.utc)
                                        except Exception:
                                            _created = datetime.now(timezone.utc)
                                        direct_ok = False
                                        try:
                                            from app.ingest.discord_listener import inject_forwarded
                                            direct_ok = await inject_forwarded(
                                                content=content,
                                                embeds=embeds or [],
                                                author_name=forward_name,
                                                channel_id=_DIRECT_CHANNEL_ID(),
                                                channel_name=channel_name,
                                                message_id=str(d.get("id") or ""),
                                                created_at=_created,
                                            )
                                        except Exception:
                                            log.error("DIRECT dispatch error; falling back to webhook", exc_info=True)
                                        if direct_ok:
                                            _mark_forwarded(content, embeds_raw)
                                            log.info("DIRECT OK | #%s | author=%s", channel_name, display_name)
                                            continue  # done; do NOT also webhook

                                    # ── Webhook path (default / fallback) ─────
                                    target_webhook = _webhook_for(channel_id)
                                    ok = await send_to_webhook(
                                        session,
                                        content=content,
                                        username=forward_name,
                                        embeds=embeds,
                                        webhook_url=target_webhook,
                                    )
                                    if ok:
                                        # Only mark as forwarded after a
                                        # successful webhook POST. On failure
                                        # the gateway will redeliver and we
                                        # want the retry to go through.
                                        _mark_forwarded(content, embeds_raw)
                                        log.info(
                                            "WEBHOOK OK | #%s | author=%s",
                                            channel_name, display_name,
                                        )
                                    else:
                                        log.error(
                                            "WEBHOOK FAIL | #%s | author=%s",
                                            channel_name, display_name,
                                        )

                            elif op == 7:  # RECONNECT
                                log.info("Gateway requested reconnect")
                                heartbeat_task.cancel()
                                break

                            elif op == 9:  # INVALID SESSION
                                log.warning("Invalid session, re-identifying in 5s")
                                session_id = None
                                sequence["s"] = None
                                # Clear stale resume URL so the retry connects to the
                                # main gateway. Reusing a dead resume URL kept the
                                # 2026-05-05 outage stuck for 77 minutes.
                                resume_url = None
                                heartbeat_task.cancel()
                                await asyncio.sleep(5)
                                break

                            elif op == 11:  # HEARTBEAT ACK
                                hb_state["acked"] = True
                                log.debug("Heartbeat ACK received")

                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            log.warning("WebSocket closed/error: %s", msg)
                            heartbeat_task.cancel()
                            break

            except asyncio.TimeoutError:
                log.error(
                    "Gateway connect/HELLO timed out after %.0fs — discarding stale "
                    "resume URL and retrying",
                    WS_CONNECT_TIMEOUT_S,
                )
                resume_url = None
                session_id = None
                sequence["s"] = None
            except aiohttp.ClientError as exc:
                log.error("Connection error: %s", exc)

            # Tear down the heartbeat task on EVERY exit path (op7/op9/close,
            # ClientError, TimeoutError, normal break) — cancel AND await so it
            # can never fire a send into the now-closing socket on its next tick.
            # That stray send was the source of the repeating
            # "Cannot write to closing transport" + reconnect churn.
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except BaseException:
                    pass
                heartbeat_task = None

            log.info("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

    finally:
        await session.close()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        asyncio.run(run_forwarder())
    except KeyboardInterrupt:
        log.info("Forwarder stopped by user")
