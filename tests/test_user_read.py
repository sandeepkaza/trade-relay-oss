"""
test_user_read.py - ONE-TIME test script to read messages from Discord channels
using a user token (HTTP API only, no self-bot library).

Reads messages from the last N days, runs them through the parser, and reports
results. For TESTING/VALIDATION only — not for production use.

Usage:
    python test_user_read.py <user_token> [--days 10]
"""

import argparse
import asyncio
import os
import sys
import datetime
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx

from app.ingest.parser import parse_alert

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

API = "https://discord.com/api/v10"
HEADERS = {}

# Target channels from the trading server (from screenshot)
TARGET_CHANNELS = [
    "pro-alerts",
    "commentary",
    "all-alerts",
    "acc-challenge",
    "monkeybot",
    "monkeybot-education",
    "analyst-sarang",
    "analyst-tc",
]


async def api_get(client: httpx.AsyncClient, path: str, params: dict = None):
    """Make a GET request to Discord API with rate-limit handling."""
    for attempt in range(5):
        resp = await client.get(f"{API}{path}", headers=HEADERS, params=params)
        if resp.status_code == 429:
            retry_after = resp.json().get("retry_after", 2)
            print(f"  (rate limited, waiting {retry_after:.1f}s)", flush=True)
            await asyncio.sleep(retry_after + 0.5)
            continue
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"  API error {resp.status_code}: {resp.text[:200]}", flush=True)
            return None
    return None


def extract_text_from_message(msg: dict) -> str:
    """Extract parseable text from a Discord API message object."""
    parts = []

    # Regular content
    content = msg.get("content", "") or ""
    if content:
        parts.append(content)

    # Embeds (bot alerts are usually in embeds)
    for embed in msg.get("embeds", []):
        parts.append(embed.get("title", "") or "")
        parts.append(embed.get("description", "") or "")
        for field in embed.get("fields", []):
            parts.append(field.get("name", "") or "")
            parts.append(field.get("value", "") or "")
        footer = embed.get("footer")
        if footer:
            parts.append(footer.get("text", "") or "")

    # Forwarded message snapshots
    for snapshot in msg.get("message_snapshots", []):
        snap_msg = snapshot.get("message", {})
        snap_content = snap_msg.get("content", "") or ""
        if snap_content:
            parts.append(snap_content)
        for embed in snap_msg.get("embeds", []):
            parts.append(embed.get("title", "") or "")
            parts.append(embed.get("description", "") or "")
            for field in embed.get("fields", []):
                parts.append(field.get("name", "") or "")
                parts.append(field.get("value", "") or "")
            footer = embed.get("footer")
            if footer:
                parts.append(footer.get("text", "") or "")

    return " ".join(p for p in parts if p).strip()


async def fetch_messages(client: httpx.AsyncClient, channel_id: str, after_dt: datetime.datetime):
    """Fetch all messages from a channel after a given datetime."""
    # Discord snowflake from datetime
    after_snowflake = int((after_dt.timestamp() * 1000 - 1420070400000) * 4194304)
    all_messages = []
    after_id = str(after_snowflake)

    while True:
        params = {"after": after_id, "limit": 100}
        data = await api_get(client, f"/channels/{channel_id}/messages", params)
        if not data or len(data) == 0:
            break
        # API returns newest first, reverse for chronological
        data.sort(key=lambda m: m["id"])
        all_messages.extend(data)
        after_id = data[-1]["id"]
        if len(data) < 100:
            break
        await asyncio.sleep(0.5)  # be gentle with rate limits

    return all_messages


async def main():
    parser_arg = argparse.ArgumentParser(description="Test Discord message parsing with user token")
    parser_arg.add_argument("token", help="Your Discord user token")
    parser_arg.add_argument("--days", type=int, default=10, help="Number of days to look back (default: 10)")
    args = parser_arg.parse_args()

    HEADERS["Authorization"] = args.token
    HEADERS["Content-Type"] = "application/json"

    after_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=args.days)

    async with httpx.AsyncClient(timeout=30) as client:
        # Step 1: Get user info
        me = await api_get(client, "/users/@me")
        if not me:
            print("ERROR: Invalid token or cannot reach Discord API.", flush=True)
            return
        print(f"Logged in as: {me['username']}#{me.get('discriminator', '0')}", flush=True)

        # Step 2: Get guilds (servers)
        guilds = await api_get(client, "/users/@me/guilds")
        if not guilds:
            print("ERROR: Cannot fetch guilds.", flush=True)
            return

        print(f"Servers: {len(guilds)}", flush=True)
        for g in guilds:
            print(f"  - {g['name']} ({g['id']})", flush=True)

        # Step 3: Find target channels across all guilds
        target_channels = []
        for guild in guilds:
            channels = await api_get(client, f"/guilds/{guild['id']}/channels")
            if not channels:
                continue
            for ch in channels:
                # Text channels only (type 0 = text, type 5 = announcement)
                if ch.get("type") not in (0, 5):
                    continue
                if ch["name"] in TARGET_CHANNELS:
                    target_channels.append({
                        "id": ch["id"],
                        "name": ch["name"],
                        "guild": guild["name"],
                    })
            await asyncio.sleep(0.3)

        if not target_channels:
            print("\nNo target channels found! Listing all text channels:", flush=True)
            for guild in guilds:
                channels = await api_get(client, f"/guilds/{guild['id']}/channels")
                if not channels:
                    continue
                for ch in channels:
                    if ch.get("type") in (0, 5):
                        print(f"  #{ch['name']} ({ch['id']}) in [{guild['name']}]", flush=True)
            return

        print(f"\nFound {len(target_channels)} target channels:", flush=True)
        for ch in target_channels:
            print(f"  #{ch['name']} in [{ch['guild']}]", flush=True)

        # Step 4: Read messages and parse
        grand_total = 0
        grand_parsed = 0
        grand_failed = 0
        all_parsed = []
        all_failed = []

        for ch in target_channels:
            print(f"\n{'='*80}", flush=True)
            print(f"CHANNEL: #{ch['name']} ({ch['id']}) in [{ch['guild']}]", flush=True)
            print(f"{'='*80}", flush=True)

            messages = await fetch_messages(client, ch["id"], after_dt)
            print(f"  Messages in last {args.days} days: {len(messages)}", flush=True)

            ch_parsed = 0
            ch_failed = 0

            for msg in messages:
                raw = extract_text_from_message(msg)
                if not raw:
                    continue

                grand_total += 1
                ts = msg["timestamp"][:16].replace("T", " ")
                author = msg.get("author", {}).get("username", "?")

                # Check for forwarded
                is_fwd = bool(msg.get("message_snapshots"))
                fwd_tag = "[FWD] " if is_fwd else ""

                alert = parse_alert(raw)

                if alert:
                    grand_parsed += 1
                    ch_parsed += 1
                    all_parsed.append({
                        "ts": ts, "channel": ch["name"], "fwd": fwd_tag,
                        "author": author, "alert": alert, "raw": raw,
                    })
                    print(f"  OK  {ts} {fwd_tag}{author}", flush=True)
                    print(f"      {alert.action} {alert.symbol} {alert.osi_symbol} @ {alert.price} frac={float(alert.fraction):.2f} [{alert.size_tag}]", flush=True)
                else:
                    grand_failed += 1
                    ch_failed += 1
                    all_failed.append({
                        "ts": ts, "channel": ch["name"], "fwd": fwd_tag,
                        "author": author, "raw": raw,
                    })
                    print(f"  --  {ts} {fwd_tag}{author}: {raw[:100]}", flush=True)

            print(f"  Channel total: {ch_parsed} parsed, {ch_failed} not parsed", flush=True)

        # Summary
        print(f"\n\n{'#'*80}", flush=True)
        print(f"GRAND SUMMARY ({args.days}-day lookback)", flush=True)
        print(f"{'#'*80}", flush=True)
        print(f"Total messages with text: {grand_total}", flush=True)
        print(f"Parsed as alerts:        {grand_parsed}", flush=True)
        print(f"Not parsed:              {grand_failed}", flush=True)
        pct = (grand_parsed / grand_total * 100) if grand_total else 0
        print(f"Parse rate:              {pct:.1f}%", flush=True)

        if all_failed:
            print(f"\n--- ALL UNPARSED MESSAGES ---", flush=True)
            for f in all_failed:
                print(f"  [{f['channel']}] {f['ts']} {f['fwd']}{f['author']}: {f['raw'][:120]}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
