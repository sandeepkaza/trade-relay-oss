"""
Test script: Fetch recent messages from trading server alert channels
and forward them via webhook to #general to test the full pipeline.
"""
import asyncio, json, os, sys
import aiohttp

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Secrets come from config.ini or environment — never commit them.
# Prefer env for test scripts so config.ini stays canonical for the running bot.
from app.core.config_manager import cfg

USER_TOKEN  = os.environ.get("DISCORD_USER_TOKEN")  or cfg.get("forwarder", "user_token",  fallback="")
WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL") or cfg.get("forwarder", "webhook_url", fallback="")
API = "https://discord.com/api/v10"

if (not USER_TOKEN or not WEBHOOK_URL) and __name__ == "__main__":
    print("ERROR: set DISCORD_USER_TOKEN and DISCORD_WEBHOOK_URL env vars, "
          "or populate [forwarder] user_token/webhook_url in config.ini.",
          file=sys.stderr)
    sys.exit(2)

# Example alert channels
ALERT_CHANNELS = {
    "100000000000006666": "pro-alerts",
    "100000000000012221": "all-alerts",
    "100000000000013332":  "trade-alerts",
    "100000000000007777": "analyst-sarang",
    "100000000000014443": "monkeybot",
    "100000000000015554": "spacemonkey-alerts",
}


async def main():
    headers = {"Authorization": USER_TOKEN, "Content-Type": "application/json"}

    async with aiohttp.ClientSession() as session:
        print("Fetching latest messages from trading server channels...\n", flush=True)

        forwarded_count = 0

        for channel_id, channel_name in ALERT_CHANNELS.items():
            print(f"--- #{channel_name} (ID: {channel_id}) ---", flush=True)

            async with session.get(
                f"{API}/channels/{channel_id}/messages?limit=3",
                headers=headers
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    print(f"  Cannot read ({resp.status}): {text[:100]}", flush=True)
                    continue

                messages = await resp.json()
                if not messages:
                    print("  (no messages)", flush=True)
                    continue

                for msg in messages:
                    author = msg.get("author", {}).get("username", "?")
                    content = msg.get("content", "")
                    embeds = msg.get("embeds", [])
                    
                    # Build display text
                    display = content[:100] if content else "[embed only]"
                    print(f"  [{author}] {display}", flush=True)

                    # Forward the FIRST message from each channel
                    if forwarded_count < 3:  # Only forward 3 total to avoid spam
                        webhook_payload = {
                            "username": f"[FWD #{channel_name}] {author}",
                        }
                        if content:
                            webhook_payload["content"] = content[:2000]
                        if embeds:
                            webhook_payload["embeds"] = embeds[:10]

                        if content or embeds:
                            async with session.post(WEBHOOK_URL, json=webhook_payload) as wh_resp:
                                if wh_resp.status == 204:
                                    print(f"  >> FORWARDED to #general!", flush=True)
                                    forwarded_count += 1
                                else:
                                    wh_text = await wh_resp.text()
                                    print(f"  >> Forward failed ({wh_resp.status}): {wh_text[:100]}", flush=True)
                            
                            await asyncio.sleep(1)  # Rate limit safety
                            break  # Only forward 1 per channel

            print(flush=True)

        print(f"\nDone! Forwarded {forwarded_count} message(s) to #general.", flush=True)
        print("Check the dashboard at http://127.0.0.1:8000/ to see if they were parsed.", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
