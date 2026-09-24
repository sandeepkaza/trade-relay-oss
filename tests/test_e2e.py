"""Send a test alert via webhook to simulate a forwarded alert from #pro-alerts."""
import asyncio
import os
import sys
import aiohttp

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config_manager import cfg

WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL") or cfg.get("forwarder", "webhook_url", fallback="")

if not WEBHOOK_URL and __name__ == "__main__":
    print("ERROR: set DISCORD_WEBHOOK_URL env var or [forwarder] webhook_url in config.ini",
          file=sys.stderr)
    sys.exit(2)

async def main():
    # Simulate a realistic BUY alert from pro-alerts
    # Using AAPL 4/17 220C at $0.50 - cheap option that should pass guardrails
    payload = {
        "username": "[FWD #pro-alerts] Twinsight Bot",
        "content": "**BOUGHT** AAPL 4/17 220C $0.50 [SMALL]"
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(WEBHOOK_URL, json=payload) as resp:
            if resp.status == 204:
                print("Test alert sent to #general via webhook!", flush=True)
                print("Alert: BOUGHT AAPL 4/17 220C $0.50 [SMALL]", flush=True)
                print("Check dashboard at http://127.0.0.1:8000", flush=True)
            else:
                text = await resp.text()
                print(f"Failed ({resp.status}): {text}", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
