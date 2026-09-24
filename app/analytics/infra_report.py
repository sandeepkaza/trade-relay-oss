"""
infra_report.py — Daily infrastructure snapshot to Discord.

Posts a single embed to daily_summary_channel_id covering everything the
operator should know in one glance:

  - EC2: instance ID, type, AZ, uptime, region (via metadata service)
  - ECR: image count, total size, latest pushed tag
  - Container: image SHA in use, container uptime, memory + CPU (cgroup)
  - SQLite: DB file size, row counts (positions, orders, alerts)
  - Filesystem: log dir size, top-3 largest log files
  - API usage today (Vision + Vertex via usage_tracker)
  - Cron schedule presence

Designed to run alongside the existing log_daily_summary at 16:00 ET.
Best-effort: each section is wrapped in try/except so a single failure
(e.g. ECR creds expired) doesn't kill the whole post.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

log = logging.getLogger(__name__)

REGION = os.getenv("AWS_REGION", "us-east-1")
APP_DIR = "/app"
DATA_DIR = os.path.join(APP_DIR, "data")
LOG_DIR = os.path.join(APP_DIR, "logs")


# ── helpers ───────────────────────────────────────────────────────────────────

def _human_bytes(n: int | float) -> str:
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"


def _fetch_imds(path: str, timeout: float = 1.5) -> str:
    """Fetch from EC2 instance metadata service (IMDSv2). Returns "" on failure."""
    try:
        import urllib.request
        import urllib.error
        token_req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token",
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
        )
        # Fixed IMDSv2 link-local endpoint; http is required (no TLS on 169.254).
        token = urllib.request.urlopen(token_req, timeout=timeout).read().decode().strip()  # nosec  # nosemgrep: dynamic-urllib-use-detected
        req = urllib.request.Request(
            f"http://169.254.169.254/latest/meta-data/{path.lstrip('/')}",
            headers={"X-aws-ec2-metadata-token": token},
        )
        return urllib.request.urlopen(req, timeout=timeout).read().decode().strip()  # nosec  # nosemgrep: dynamic-urllib-use-detected
    except Exception:
        return ""


def _dir_size(path: str) -> int:
    total = 0
    try:
        for root, _, files in os.walk(path):
            for f in files:
                try:
                    total += os.path.getsize(os.path.join(root, f))
                except OSError:
                    pass
    except OSError:
        pass
    return total


# ── sections ──────────────────────────────────────────────────────────────────

def _section_ec2() -> list[str]:
    """EC2 instance details from metadata service (no IAM perms needed)."""
    iid = _fetch_imds("instance-id")
    if not iid:
        return ["**EC2:** metadata service unreachable"]
    itype = _fetch_imds("instance-type") or "?"
    az = _fetch_imds("placement/availability-zone") or "?"
    pub_ip = _fetch_imds("public-ipv4") or "—"
    return [
        f"**EC2:** `{iid}` ({itype}, {az})",
        f"  • public IP: `{pub_ip}` (changes on stop/start; SSM-only access)",
    ]


def _section_ecr() -> list[str]:
    """ECR repo summary via boto3. Reads via instance role (ECR read-only)."""
    try:
        import boto3
        ecr = boto3.client("ecr", region_name=REGION)
        repo = os.getenv("ECR_REPOSITORY", "trader")
        out = ecr.describe_images(repositoryName=repo, maxResults=1000)
        details = out.get("imageDetails", [])
        if not details:
            return [f"**ECR ({repo}):** empty"]
        total_size = sum(d.get("imageSizeInBytes", 0) for d in details)
        latest = max(details, key=lambda d: d.get("imagePushedAt") or 0)
        latest_tags = latest.get("imageTags", []) or []
        latest_tag = next((t for t in latest_tags if t != "latest"), latest_tags[0] if latest_tags else "?")
        return [
            f"**ECR ({repo}):** {len(details)} images, {_human_bytes(total_size)} total",
            f"  • latest: `{latest_tag[:16]}` pushed {_iso(latest.get('imagePushedAt'))}",
        ]
    except Exception as e:
        return [f"**ECR:** lookup failed — `{e!s:.80}`"]


def _iso(dt) -> str:
    if not dt:
        return "?"
    if isinstance(dt, datetime):
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return str(dt)[:19]


def _section_container() -> list[str]:
    """Self-introspection: process uptime + memory via /proc."""
    lines = []
    # Container start (PID 1 cmdline / /proc/1/stat)
    try:
        with open("/proc/uptime") as f:
            host_up = float(f.read().split()[0])
        with open("/proc/1/stat") as f:
            stat = f.read().split()
        # field 22 = starttime in clock ticks since boot
        clk_tck = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        start_secs_since_boot = int(stat[21]) / clk_tck
        cont_up = host_up - start_secs_since_boot
        lines.append(f"**Container:** up {_human_secs(cont_up)}")
    except Exception:
        lines.append("**Container:** uptime unknown")
    # Memory (cgroup v2)
    try:
        with open("/sys/fs/cgroup/memory.current") as f:
            mem_used = int(f.read().strip())
        try:
            with open("/sys/fs/cgroup/memory.max") as f:
                raw = f.read().strip()
            mem_max = float("inf") if raw == "max" else int(raw)
        except OSError:
            mem_max = float("inf")
        if mem_max != float("inf"):
            lines.append(f"  • memory: {_human_bytes(mem_used)} / {_human_bytes(mem_max)}")
        else:
            lines.append(f"  • memory: {_human_bytes(mem_used)}")
    except Exception:
        pass
    # Image SHA (from env set by remote-up.sh)
    img = os.getenv("RELAYBOT_IMAGE") or os.getenv("TRADER_IMAGE", "")
    if img:
        sha = img.rsplit(":", 1)[-1][:12]
        lines.append(f"  • image: `{sha}`")
    return lines


def _human_secs(s: float) -> str:
    s = int(s)
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, _ = divmod(s, 60)
    if d:
        return f"{d}d {h}h"
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


def _section_db() -> list[str]:
    """SQLite DB size + row counts."""
    try:
        import sqlite3
        db_path = os.path.join(DATA_DIR, "trader.db")
        size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
        conn = sqlite3.connect(db_path)
        try:
            counts = {}
            for tbl in ("positions", "orders", "alerts", "discord_signals"):
                try:
                    # tbl is from the hardcoded tuple above — not user input.
                    counts[tbl] = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]  # nosec
                except Exception:
                    counts[tbl] = "?"
        finally:
            conn.close()
        return [
            f"**SQLite:** {_human_bytes(size)} ({counts['positions']} pos, "
            f"{counts['orders']} orders, {counts['alerts']} alerts, "
            f"{counts['discord_signals']} signals)",
        ]
    except Exception as e:
        return [f"**SQLite:** read failed — `{e!s:.60}`"]


def _section_logs() -> list[str]:
    """Log dir size + top 3 largest files."""
    try:
        size = _dir_size(LOG_DIR)
        files = []
        for f in os.listdir(LOG_DIR):
            p = os.path.join(LOG_DIR, f)
            if os.path.isfile(p):
                files.append((f, os.path.getsize(p)))
        files.sort(key=lambda x: -x[1])
        top = ", ".join(f"{n}:{_human_bytes(s)}" for n, s in files[:3])
        return [f"**Logs:** {_human_bytes(size)} total — top: {top}"]
    except Exception as e:
        return [f"**Logs:** read failed — `{e!s:.60}`"]


def _section_api_usage() -> list[str]:
    """Today's Vision + Vertex AI consumption with per-model breakdown."""
    try:
        import app.core.usage_tracker as ut
        s = ut.snapshot()
        v = s["vision"]
        g = s["vertex"]
        out: list[str] = []
        if not (v["calls"] or g["calls"]):
            return ["**API Usage:** none today"]

        out.append(
            f"**API Usage:** Vision {v['calls']} calls"
            + (f" ({v['errors']} err)" if v['errors'] else "")
            + f"; Vertex {g['calls']} calls, "
            + f"{g['input_tokens']/1000:,.1f}k in / {g['output_tokens']/1000:,.1f}k out tokens"
        )
        # Per-model breakdown for Vertex
        bm = g.get("by_model", {})
        if bm:
            for model, mv in sorted(bm.items()):
                out.append(
                    f"  • {model}: {mv['calls']}× "
                    f"({mv['input_tokens']/1000:,.1f}k in / {mv['output_tokens']/1000:,.1f}k out)"
                )
        return out
    except Exception as e:
        return [f"**API Usage:** failed — `{e!s:.60}`"]


# Public Google list prices (USD). Floors are approximate — verify against
# current pricing pages periodically. Prefix-match: "gemini-2.5-flash" matches
# "gemini-2.5-flash-001". Vision DOCUMENT_TEXT_DETECTION = $1.50 per 1000.
_GEMINI_PRICES = {
    # model_prefix: (input $/1M tokens, output $/1M tokens)
    "gemini-2.5-flash":      (0.075, 0.30),
    "gemini-2.5-pro":        (1.25,  5.00),
    "gemini-2.0-flash":      (0.075, 0.30),
    "gemini-1.5-flash":      (0.075, 0.30),
    "gemini-1.5-pro":        (1.25,  5.00),
}
_VISION_PRICE_PER_1K = 1.50  # DOCUMENT_TEXT_DETECTION after free tier


def _gemini_price_for(model: str) -> tuple[float, float] | None:
    """Lookup (input_per_1M, output_per_1M) by prefix match."""
    if not model:
        return None
    m = model.lower()
    for prefix, prices in _GEMINI_PRICES.items():
        if m.startswith(prefix):
            return prices
    return None


def _section_google_cost() -> list[str]:
    """Estimate today's Google Cloud spend from usage_tracker × public list
    prices. APPROXIMATION — does not include free-tier credits (Vision: 1k
    free DOCUMENT_TEXT_DETECTION/month) or per-account discounts. For exact
    numbers, see the GCP Billing Console."""
    try:
        import app.core.usage_tracker as ut
        s = ut.snapshot()
        v = s["vision"]
        g = s["vertex"]

        if not (v["calls"] or g["calls"]):
            return ["**Google Cloud cost (today):** $0.00 (no usage)"]

        # Vision: assume calls beyond free tier are billed. Cap conservatively.
        vision_cost = (v["calls"] / 1000.0) * _VISION_PRICE_PER_1K

        # Vertex: per-model token cost
        vertex_cost = 0.0
        per_model: list[tuple[str, float]] = []
        for model, mv in (g.get("by_model") or {}).items():
            prices = _gemini_price_for(model)
            if not prices:
                # Unknown model — flag with $? but skip math.
                per_model.append((f"{model} (unknown pricing)", 0.0))
                continue
            in_p, out_p = prices
            cost = (mv["input_tokens"] / 1_000_000) * in_p + (mv["output_tokens"] / 1_000_000) * out_p
            vertex_cost += cost
            per_model.append((model, cost))

        total = vision_cost + vertex_cost
        out = [
            f"**Google Cloud cost (today, est.):** ${total:.4f}",
            f"  • Vision OCR: ${vision_cost:.4f} ({v['calls']} calls)",
            f"  • Vertex AI: ${vertex_cost:.4f}",
        ]
        for model, cost in sorted(per_model, key=lambda x: -x[1]):
            out.append(f"     ◦ {model}: ${cost:.4f}")
        out.append("  *list-price estimate; ignores free tier + committed-use discounts*")
        return out
    except Exception as e:
        return [f"**Google Cloud cost:** estimate failed — `{e!s:.60}`"]


def _section_api_health() -> list[str]:
    """Public.com + Discord REST latency / error rate from api_monitor.
    All metrics are since process start (resets on container restart)."""
    try:
        from app.monitors.api_monitor import monitor
        m = monitor.get_metrics()
        out: list[str] = ["**API health (since boot):**"]
        services = m.get("services", {}) or {}
        for svc_name in ("public_com", "discord", "discord_webhook"):
            s = services.get(svc_name)
            if not s or not s.get("total_requests"):
                continue
            lat = s.get("latency_ms", {}) or {}
            rl = s.get("rate_limited") or 0
            rl_part = f", 429×{rl}" if rl else ""
            out.append(
                f"  • **{svc_name}** {s['total_requests']} reqs, "
                f"err {s['error_rate_pct']}%{rl_part} — "
                f"avg {s['avg_response_time_ms']}ms / "
                f"p50 {lat.get('p50', 0)}ms / "
                f"p95 {lat.get('p95', 0)}ms / "
                f"p99 {lat.get('p99', 0)}ms"
            )
            # Top-3 slowest endpoints by p95
            eps = s.get("endpoint_breakdown") or {}
            top = sorted(
                ((ep, st) for ep, st in eps.items() if st.get("count", 0) > 0),
                key=lambda x: -(x[1].get("p95_ms") or 0),
            )[:3]
            for ep, st in top:
                err_n = st.get("errors") or 0
                err_part = f" ({err_n} err)" if err_n else ""
                out.append(
                    f"     ◦ `{ep[:40]}` ×{st['count']} "
                    f"avg {st.get('avg_ms', 0)}ms p95 {st.get('p95_ms', 0)}ms{err_part}"
                )
        if len(out) == 1:
            return ["**API health:** no API traffic since boot"]
        return out
    except Exception as e:
        return [f"**API health:** lookup failed — `{e!s:.60}`"]


def _section_cron() -> list[str]:
    """Cron status read from host_metrics.json (container can't see host /etc)."""
    import json
    path = os.path.join(DATA_DIR, "host_metrics.json")
    state = "unknown (collector pending)"
    if os.path.exists(path):
        try:
            with open(path) as f:
                d = json.load(f)
            state = d.get("cron", "unknown")
        except Exception:
            pass
    if state == "installed":
        return ["**Schedule:** cron installed on host (Mon-Fri 08:00-17:00 ET)"]
    if state == "missing":
        return ["**Schedule:** cron NOT installed on host (manual control only)"]
    return [f"**Schedule:** {state}"]


def _section_host() -> list[str]:
    """Host-level metrics from the JSON dropped by host-metrics-collector.sh.
    Container can't see host /proc, so we rely on the bind-mounted data/
    dir for an out-of-band channel."""
    import json
    path = os.path.join(DATA_DIR, "host_metrics.json")
    if not os.path.exists(path):
        return ["**Host:** metrics not yet collected (host-metrics-collector cron pending)"]
    try:
        with open(path) as f:
            d = json.load(f)
    except Exception as e:
        return [f"**Host:** read failed — `{e!s:.60}`"]
    age_s = 0
    try:
        ts = datetime.strptime(d["collected_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        age_s = (datetime.now(timezone.utc) - ts).total_seconds()
    except Exception:
        pass
    stale = " (stale)" if age_s > 600 else ""
    out = [f"**Host:** uptime {_human_secs(d['uptime_seconds'])}, load {d['load']['1m']}/{d['load']['5m']}/{d['load']['15m']}{stale}"]
    disk = d["disk"]
    out.append(
        f"  • disk: {_human_bytes(disk['used_bytes'])} / {_human_bytes(disk['total_bytes'])} "
        f"({disk['used_pct']}% used, {_human_bytes(disk['free_bytes'])} free)"
    )
    mem = d["memory"]
    out.append(
        f"  • mem (host): {_human_bytes(mem['total_bytes'] - mem['available_bytes'])} / "
        f"{_human_bytes(mem['total_bytes'])} ({mem['used_pct']}% used)"
    )
    return out


def _section_cloudflared() -> list[str]:
    """Cloudflare tunnel state from host JSON."""
    import json
    path = os.path.join(DATA_DIR, "host_metrics.json")
    state = "?"
    if os.path.exists(path):
        try:
            with open(path) as f:
                d = json.load(f)
            state = d.get("cloudflared", "?")
        except Exception:
            pass
    icon = "✅" if state == "running+connected" else ("⚠️" if "running" in state else "❌")
    return [f"**Cloudflared tunnel:** {icon} {state}"]


def _section_cost() -> list[str]:
    """AWS spend yesterday + last-7-day total + simple forecast.
    Requires ce:GetCostAndUsage on the instance role."""
    try:
        import boto3
        from datetime import date, timedelta
        ce = boto3.client("ce", region_name="us-east-1")  # Cost Explorer is global; us-east-1 endpoint
        today = date.today()
        yesterday = today - timedelta(days=1)
        week_start = today - timedelta(days=7)

        # Yesterday's spend by service
        y = ce.get_cost_and_usage(
            TimePeriod={"Start": yesterday.isoformat(), "End": today.isoformat()},
            Granularity="DAILY",
            Metrics=["UnblendedCost"],
            GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
        )
        yest_total = 0.0
        by_svc: list[tuple[str, float]] = []
        for r in y.get("ResultsByTime", []):
            for g in r.get("Groups", []):
                amt = float(g["Metrics"]["UnblendedCost"]["Amount"])
                if amt < 0.005:
                    continue
                svc = g["Keys"][0]
                by_svc.append((svc, amt))
                yest_total += amt
        by_svc.sort(key=lambda x: -x[1])

        # Last 7 days total
        w = ce.get_cost_and_usage(
            TimePeriod={"Start": week_start.isoformat(), "End": today.isoformat()},
            Granularity="DAILY",
            Metrics=["UnblendedCost"],
        )
        week_total = sum(
            float(r["Total"]["UnblendedCost"]["Amount"])
            for r in w.get("ResultsByTime", [])
        )

        out = [
            f"**AWS cost (yesterday {yesterday.isoformat()}):** ${yest_total:.2f}",
            f"  • last 7 days: ${week_total:.2f}  (avg ${week_total/7:.2f}/day)",
        ]
        # Top 4 services yesterday
        for svc, amt in by_svc[:4]:
            out.append(f"  • {svc}: ${amt:.2f}")
        return out
    except Exception as e:
        return [f"**AWS cost:** unavailable — `{e!s:.80}`"]


def _section_github_actions() -> list[str]:
    """Last 5 GitHub Actions workflow runs. Needs GITHUB_PAT env var with
    actions:read on the repo. Falls back gracefully when missing."""
    pat = os.getenv("GITHUB_PAT", "").strip()
    repo = os.getenv("GITHUB_REPO", "your-org/trade-relay").strip()
    if not pat:
        return ["**CI runs:** GITHUB_PAT not set (skip)"]
    try:
        import urllib.request
        import json
        req = urllib.request.Request(
            f"https://api.github.com/repos/{repo}/actions/runs?per_page=5",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {pat}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        # Guard against non-HTTPS schemes (file://, etc.) before opening. The
        # URL is a fixed api.github.com literal, but assert it explicitly so the
        # urlopen below is provably https-only.
        if not req.full_url.lower().startswith("https://"):
            return ["**CI runs:** refused non-HTTPS URL"]
        with urllib.request.urlopen(req, timeout=8) as r:  # nosec  # nosemgrep: dynamic-urllib-use-detected
            data = json.loads(r.read().decode())
        runs = data.get("workflow_runs", [])
        if not runs:
            return ["**CI runs:** no workflow runs visible"]
        out = ["**CI runs (last 5):**"]
        for run in runs[:5]:
            name = run.get("name", "?")[:24]
            concl = run.get("conclusion") or run.get("status") or "?"
            icon = {"success": "✅", "failure": "❌", "cancelled": "⏸", "in_progress": "⏳"}.get(concl, "•")
            sha = (run.get("head_sha") or "")[:7]
            ts = (run.get("updated_at") or "")[:16].replace("T", " ")
            out.append(f"  • {icon} `{sha}` {name} — {concl} @ {ts}")
        return out
    except Exception as e:
        return [f"**CI runs:** lookup failed — `{e!s:.80}`"]


def _section_recent_deploys() -> list[str]:
    """Last few image tags deployed (from host docker images list)."""
    import json
    path = os.path.join(DATA_DIR, "host_metrics.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            d = json.load(f)
        recent = (d.get("recent_images") or "").strip()
        if not recent:
            return ["**Recent deploys:** none on host yet"]
        # JSON stores pipe-delimited "<repo:tag> <created>"
        items = recent.split("|")
        out = ["**Recent deploys (host docker images):**"]
        for line in items[:5]:
            out.append(f"  • `{line.strip()[:80]}`")
        return out
    except Exception as e:
        return [f"**Recent deploys:** read failed — `{e!s:.60}`"]


# ── orchestrator ──────────────────────────────────────────────────────────────

def compose_embed() -> dict:
    """Build the Discord embed."""
    sections: list[str] = []
    for fn in (_section_ec2, _section_host, _section_cloudflared,
               _section_cost, _section_google_cost,
               _section_ecr, _section_recent_deploys,
               _section_github_actions, _section_container,
               _section_db, _section_logs,
               _section_api_health, _section_api_usage, _section_cron):
        try:
            sections.extend(fn())
        except Exception as e:
            sections.append(f"**{fn.__name__}:** crashed — `{e!s:.60}`")
        sections.append("")  # blank line between sections

    desc = "\n".join(sections).strip()
    return {
        "title": "\U0001f3d7️ INFRA REPORT",
        "description": desc[:4000],  # Discord cap
        "color": 0x9B59B6,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Daily infrastructure snapshot"},
    }


async def run_and_post():
    from app.core.config_manager import cfg
    if not cfg.getboolean("trading", "infra_report_enabled", fallback=True):
        log.info("[infra_report] disabled via config")
        return
    embed = compose_embed()
    import app.analytics.trade_logger as tl
    await tl._send_to_channel(embed=embed, channel_id=tl._daily_summary_channel_id())
    log.info("[infra_report] posted")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run_and_post())
