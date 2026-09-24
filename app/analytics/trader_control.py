"""
trader_control.py - Discord bot (Bot B) that posts a button panel in the
configured control channel and runs container start/stop/restart in
response to button clicks. Uses Gateway WebSocket (discord.py), so the
clicks are received directly — no public HTTPS endpoint, no Cloudflare
config.

Requires a SEPARATE bot token from the main relaybot (Bot A). Discord
allows only one Gateway connection per token; sharing would kick Bot A
off the alert channels.

Runs as a docker-compose service (profile: control) using the same image
as relaybot. docker.sock is mounted so this container can issue
start/stop/restart against the main `relaybot-relaybot-1` container.

Env vars (set in .env on VM):
  DISCORD_CONTROL_BOT_TOKEN   - Bot B token (NOT the main bot's).
  DISCORD_CONTROL_CHANNEL_ID  - channel ID where the panel posts and
                                only-this-channel buttons / commands are
                                accepted.
  DISCORD_AUTHORIZED_USER_ID  - your Discord user ID — only this user
                                may click panel buttons or type commands.
  RELAYBOT_CONTAINER_NAME     - default "relaybot-relaybot-1".
  CLOUDFLARED_CONTAINER_NAME  - default "relaybot-cloudflared-1". Start/Stop/
                                Restart also drive this so the dashboard tunnel
                                comes back up with the trader (best-effort).

Panel: 4 buttons (Status / Start / Stop / Restart). Button reply is
posted in the channel referencing the click.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime

import discord
import docker
from discord.ext import commands, tasks
from docker.errors import APIError, NotFound

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("trader_control")

# DISCORD_CONTROL_BOT_TOKEN must be the token of a SECOND Discord
# application (Bot B). Sharing the main bot's token would have two
# Gateway connections on one token, which Discord disallows — the older
# session gets kicked, taking the alert listener offline.
TOKEN = os.environ.get("DISCORD_CONTROL_BOT_TOKEN", "").strip()
CHANNEL_ID = int(os.environ.get("DISCORD_CONTROL_CHANNEL_ID", "0") or 0)
USER_ID = int(os.environ.get("DISCORD_AUTHORIZED_USER_ID", "0") or 0)
CONTAINER_NAME = os.environ.get(
    "RELAYBOT_CONTAINER_NAME",
    os.environ.get("TRADER_CONTAINER_NAME", "relaybot-relaybot-1"),
)
# Cloudflare Tunnel sidecar. Buttons reach this bot over Discord Gateway WS
# (no Cloudflare), so Start can recover the dashboard even when the tunnel is
# down. Lifecycle tied to the trader: Start/Restart bring it up, Stop takes it
# down. Best-effort — if it's absent (local dev, profile not enabled) the
# trader action still succeeds.
CLOUDFLARED_CONTAINER_NAME = os.environ.get(
    "CLOUDFLARED_CONTAINER_NAME", "relaybot-cloudflared-1",
)

# ── IB Gateway control (ibkr-oci only) ──────────────────────────────────────
# IB Gateway is NOT a container — it's a host systemd unit driven by IBC under
# xvfb. So unlike every other button here, these three can't go through the
# docker API against a container; they have to run on the host itself.
#
# Route: spawn a throwaway privileged container that nsenters PID 1's
# namespaces and runs one allowlisted command. This adds no privilege that
# wasn't already granted — a process holding docker.sock is root on the host by
# definition — but the command set is a fixed dict below and no interaction
# input is ever interpolated into it.
#
# Off by default. Set IBGW_CONTROL_ENABLED=1 only on a host that actually runs
# the gateway; elsewhere the three buttons are removed from the panel entirely.
IBGW_ENABLED = os.environ.get("IBGW_CONTROL_ENABLED", "").strip().lower() in ("1", "true", "yes")
IBGW_UNIT = os.environ.get("IBGW_UNIT", "ibgateway").strip() or "ibgateway"
# Image used for the nsenter helper. Same image this bot runs from, so there is
# nothing extra to pull.
IBGW_HELPER_IMAGE = os.environ.get("RELAYBOT_IMAGE", "relaybot:local")
# Where the gateway probe reads market-data health from. The helper runs in the
# host network namespace, so this is the trader's published port on the host —
# not a container name (the control container has no DNS for its siblings).
TRADER_METRICS_URL = os.environ.get(
    "TRADER_METRICS_URL", "http://127.0.0.1:8000/metrics",
)

# Panel identity. Two hosts each run their own copy of this bot (relaybot-vm
# with Bot B, ibkr-oci with Bot C), and on BOTH of them the trader container is
# named relaybot-relaybot-1 — so without a distinct title the two panels are
# indistinguishable and a Stop click could hit the wrong box. Set
# CONTROL_PANEL_TITLE per host.
PANEL_TITLE = os.environ.get("CONTROL_PANEL_TITLE", "").strip() or "Relaybot Control"


async def _idle_forever() -> None:
    """Block instead of crash-loop when not yet configured. Lets the
    container stay up so `docker compose ps` looks normal and a later
    .env paste + container restart picks up the new token."""
    while True:
        await asyncio.sleep(3600)


def _preflight() -> None:
    """Env checks that decide whether this process should run at all.

    Called from __main__, NOT at import: these two blocks used to sit at module
    scope, where the missing-token branch entered asyncio.run(_idle_forever())
    during the import itself. That is correct for the container (the idle loop
    is what keeps it alive without a token) and fatal for anything that merely
    wants to import the module — a test importing it hung forever instead of
    collecting. Same behavior for the daemon, which still reaches both branches
    the moment it starts.
    """
    if not TOKEN:
        log.warning(
            "DISCORD_CONTROL_BOT_TOKEN not set — control bot idle. "
            "Create a SECOND Discord application, copy its bot token, paste "
            "into VM .env, then `docker compose restart relaybot-control`."
        )
        asyncio.run(_idle_forever())
        sys.exit(0)

    if not (CHANNEL_ID and USER_ID):
        log.error(
            "Missing env: DISCORD_CONTROL_CHANNEL_ID=%r, DISCORD_AUTHORIZED_USER_ID=%r",
            CHANNEL_ID, USER_ID,
        )
        sys.exit(1)


dclient = docker.from_env()

PANEL_TAG = "trader-control-panel-v1"


# ── docker helpers ───────────────────────────────────────────────────────────

def container_status() -> str:
    try:
        c = dclient.containers.get(CONTAINER_NAME)
        state = c.attrs.get("State", {}) or {}
        started = (state.get("StartedAt") or "")[:19]
        return (
            f"{state.get('Status', '?')} "
            f"(restarts={state.get('RestartCount', 0)}) "
            f"since {started}"
        )
    except NotFound:
        return "not found"
    except APIError as exc:
        return f"docker error: {exc}"


def cloudflared_status() -> str:
    try:
        cf = dclient.containers.get(CLOUDFLARED_CONTAINER_NAME)
        return (cf.attrs.get("State", {}) or {}).get("Status", "?")
    except NotFound:
        return "not found"
    except APIError as exc:
        return f"docker error: {exc}"


def _drive_cloudflared(action: str) -> str:
    """Best-effort Cloudflare Tunnel control alongside the trader. Returns a
    short status fragment for the reply; never raises (tunnel trouble must not
    block trader start/stop). start/restart -> ensure up; stop -> bring down."""
    nm = CLOUDFLARED_CONTAINER_NAME
    try:
        cf = dclient.containers.get(nm)
    except NotFound:
        return f"{nm}: not found (skipped)"
    except APIError as exc:
        return f"{nm}: docker error: {exc}"
    try:
        if action == "stop":
            if cf.status not in ("running", "restarting"):
                return f"{nm}: already {cf.status}"
            cf.stop(timeout=30)
            return f"{nm}: stopped"
        # start or restart -> ensure tunnel is up so the dashboard is reachable
        if cf.status == "running":
            return f"{nm}: already up"
        cf.start()
        return f"{nm}: started"
    except APIError as exc:
        return f"{nm}: docker error: {exc}"


def do_action(action: str) -> tuple[bool, str]:
    """Blocking — wrap with asyncio.to_thread from async callers."""
    try:
        c = dclient.containers.get(CONTAINER_NAME)
    except NotFound:
        if action == "start":
            return False, (f"{CONTAINER_NAME} not found. Use `docker compose up -d` "
                           "on the VM to create it first.")
        return False, f"{CONTAINER_NAME} not found"
    except APIError as exc:
        return False, f"docker error: {exc}"

    nm = CONTAINER_NAME
    try:
        if action == "stop":
            if c.status not in ("running", "restarting"):
                trader_msg = f"{nm}: already {c.status}"
            else:
                c.stop(timeout=30)
                trader_msg = f"{nm}: stopped"
        elif action == "start":
            if c.status == "running":
                trader_msg = f"{nm}: already running"
            else:
                c.start()
                trader_msg = f"{nm}: started"
        elif action == "restart":
            c.restart(timeout=30)
            trader_msg = f"{nm}: restarted"
        else:
            return False, f"Unknown action: {action}"
    except APIError as exc:
        return False, f"{nm}: docker error: {exc}"

    cf_msg = _drive_cloudflared(action)
    return True, f"{trader_msg} · {cf_msg}"


def fetch_logs(name: str, tail: int = 20) -> str:
    """Last `tail` log lines of a container. Blocking — call via to_thread.
    Trimmed to fit a Discord message (2000-char cap, minus code-fence)."""
    try:
        c = dclient.containers.get(name)
        data = c.logs(tail=tail).decode("utf-8", "replace").rstrip()
    except NotFound:
        return f"{name}: not found"
    except APIError as exc:
        return f"docker error: {exc}"
    if not data:
        return "(no log output)"
    return data[-1800:]


# ── IB Gateway helpers (host systemd via nsenter) ───────────────────────────

# Allowlist. Keys are the only things a button may ask for; values are argv
# lists run on the host. Never build one of these from user input.
# One probe call rather than three: spawning a container costs ~1s, and the
# status button would otherwise pay it three times over and risk the 3s
# interaction ack. `state` prints three labelled lines the parser below reads.
# "unit active" is NOT the same as "logged in" — the 2026-08-06 outage had an
# active unit and a dead API port for 1h51m while IBC looped on 2FA — so the
# probe always reports the socket alongside the unit.
_IBGW_CMDS: dict[str, list[str]] = {
    "state": ["sh", "-c",
              f"echo \"ACTIVE=$(systemctl is-active {IBGW_UNIT})\"; "
              f"echo \"SINCE=$(systemctl show {IBGW_UNIT} -p ActiveEnterTimestamp --value)\"; "
              f"echo \"PORT4002=$(ss -ltn | grep -cE '(127\\.0\\.0\\.1|\\*|0\\.0\\.0\\.0):4002 ')\"; "
              f"echo \"TWOFA30M=$(journalctl -u {IBGW_UNIT} --since '-30 min' --no-pager 2>/dev/null "
              f"| grep -c 'second factor authentication timeout')\"; "
              # Market-data farm, read off the trader's own /metrics. The socket
              # being open says nothing about quotes actually flowing, which is
              # the thing that decides whether an order can be priced. Scraped
              # from the running bot rather than probed directly: a second IBKR
              # client would need its own clientId and can kick the live bot off
              # the gateway. Empty values (curl fails, bot down, gauge absent)
              # are rendered as "unknown", never as broken.
              f"M=$(curl -s --max-time 3 {TRADER_METRICS_URL} 2>/dev/null); "
              f"echo \"MDFARM=$(printf '%s' \"$M\" | awk '/^market_data_farm_ok /{{print $2}}')\"; "
              f"echo \"MDAGE=$(printf '%s' \"$M\" | awk '/^market_data_farm_age_seconds /{{print $2}}')\""],
    "logs":    ["journalctl", "-u", IBGW_UNIT, "-n", "40", "--no-pager"],
    "restart": ["systemctl", "restart", IBGW_UNIT],
    "start":   ["systemctl", "start", IBGW_UNIT],
    "stop":    ["systemctl", "stop", IBGW_UNIT],
    # Open-position count, read off the trader's own /metrics — the number the
    # stop confirmation needs. Same scrape as `state`: never a second IBKR
    # client. Empty (bot down, gauge absent) renders as unknown, and unknown is
    # treated as "assume there are positions" by the caller.
    "opens":   ["sh", "-c",
                f"curl -s --max-time 3 {TRADER_METRICS_URL} 2>/dev/null "
                f"| awk '/^open_positions /{{print $2}}'"],
}


def _host_exec(key: str, timeout: int = 60) -> str:
    """Run one allowlisted host command inside PID 1's namespaces.
    Blocking — call via asyncio.to_thread. Returns combined output text."""
    argv = _IBGW_CMDS.get(key)
    if argv is None:                      # unreachable via buttons; guard anyway
        return f"refused: {key!r} is not an allowlisted command"
    try:
        out = dclient.containers.run(
            image=IBGW_HELPER_IMAGE,
            entrypoint="",
            command=["nsenter", "-t", "1", "-m", "-u", "-i", "-n", "-p", "--", *argv],
            # The image sets USER relaybot (uid 1000). privileged grants the
            # capabilities to root, not to that user, so without this the helper
            # dies on "nsenter: cannot open /proc/1/ns/ipc: Permission denied"
            # and every gateway probe silently reports "?" — a false red on the
            # panel for a gateway that is actually up.
            user="root",
            privileged=True,
            pid_mode="host",
            network_mode="host",
            remove=True,
            stderr=True,
        )
        return out.decode("utf-8", "replace").rstrip() or "(no output)"
    except docker.errors.ContainerError as exc:
        # Non-zero exit — systemctl is-active returns 3 when inactive, which is
        # information, not an error. Surface whatever it printed.
        raw = (exc.stderr or b"").decode("utf-8", "replace").strip()
        return raw or f"exit {exc.exit_status}"
    except APIError as exc:
        return f"docker error: {exc}"


def _market_data_line(raw_farm: str, raw_age: str) -> tuple[bool | None, str]:
    """(ok, rendered line) for the market-data farm.

    ok is True/False/None, where None means "no answer" — the bot is down, the
    metrics scrape failed, or IBKR has not reported farm state on this session.
    Unknown is NOT False: reporting a red farm because a scrape timed out would
    invite a gateway restart (which costs a 2FA tap) for a healthy feed.
    """
    if raw_farm == "":
        return None, "Market data: **unknown** (no answer from the bot)"
    try:
        code = float(raw_farm)
    except ValueError:
        return None, f"Market data: **unknown** (unparsable: {raw_farm!r})"

    age = ""
    try:
        if raw_age:
            secs = int(float(raw_age))
            age = f", last reported {secs // 60}m ago" if secs >= 60 else f", last reported {secs}s ago"
    except ValueError:
        pass

    if code == 1:
        return True, f"Market data: **flowing** (IBKR farm OK{age})"
    if code == -1:
        return None, f"Market data: **connecting**{age}"
    return False, f"Market data: **NOT FLOWING** (IBKR farm broken{age})"


def gateway_status() -> tuple[bool, str]:
    """(healthy, human summary). Healthy means unit active, port 4002 open, and
    market data not known-broken — an open socket with a dead farm cannot price
    an order, so it does not count as healthy."""
    fields = {}
    for line in _host_exec("state").splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            fields[k.strip()] = v.strip()
    active = fields.get("ACTIVE", "?")
    since = fields.get("SINCE", "")
    port_open = fields.get("PORT4002", "0") not in ("0", "")
    twofa = fields.get("TWOFA30M", "0")
    md_ok, md_line = _market_data_line(fields.get("MDFARM", ""), fields.get("MDAGE", ""))
    # Unknown market data does not drag the light red — only a confirmed broken
    # farm does. Otherwise every restart window would flash red while the bot
    # reconnects and IBKR has not re-reported yet.
    healthy = active == "active" and port_open and md_ok is not False

    bits = [
        f"unit `{IBGW_UNIT}`: **{active}**",
        f"API port 4002: **{'listening' if port_open else 'DOWN'}**",
        md_line,
    ]
    if since:
        bits.append(f"since {since}")
    if md_ok is False:
        bits.append(
            "\n:warning: Socket is up but IBKR reports the market-data farm "
            "down — quotes are stale, so entries cannot be priced. Usually "
            "clears on its own; a gateway restart costs a 2FA tap and rarely helps."
        )
    if active == "active" and not port_open:
        bits.append(
            "\n:iphone: Unit is up but the API socket is closed — the gateway "
            "is logged out. Restarting will NOT fix this on its own: IBC needs "
            "a Second Factor Authentication approval tapped in IBKR Mobile."
        )
    if twofa not in ("0", ""):
        bits.append(f"\n:rotating_light: {twofa} 2FA re-login timeout(s) in the last 30 min.")
    return healthy, "\n".join(bits)


def gateway_logs() -> str:
    text = _host_exec("logs")
    return text[-1800:]


def gateway_restart() -> tuple[bool, str]:
    """Restart the gateway unit. NOTE: a cold restart drops the saved session,
    so the re-login needs a fresh 2FA tap — the API can be down for minutes."""
    out = _host_exec("restart", timeout=120)
    ok = out in ("", "(no output)") or "error" not in out.lower()
    _, status = gateway_status()
    return ok, f"restart issued — {out}\n\n{status}"


def gateway_start() -> tuple[bool, str]:
    """Start a stopped gateway. Safe in the sense that nothing is interrupted,
    but it is not instant and not unattended: IBC logs in from cold, which
    sends a Second Factor Authentication push that has to be tapped."""
    out = _host_exec("start", timeout=120)
    ok = out in ("", "(no output)") or "error" not in out.lower()
    _, status = gateway_status()
    return ok, f"start issued — {out}\n\n{status}"


def gateway_stop() -> tuple[bool, str]:
    """Stop the gateway unit. The most destructive control on the panel.

    A stopped gateway is not a paused one: open positions stop being priced,
    the exit engine cannot fill, and an analyst SELL arriving meanwhile is
    dropped. Starting it again is not symmetric either — the cold re-login
    needs a 2FA tap, so 'stop' can easily mean 'down for minutes'.
    """
    out = _host_exec("stop", timeout=120)
    ok = out in ("", "(no output)") or "error" not in out.lower()
    _, status = gateway_status()
    return ok, f"stop issued — {out}\n\n{status}"


def open_position_count() -> int | None:
    """Open positions per the trader's own gauge, or None if unknowable.

    None is not zero. Callers must treat it as "there may be positions" — the
    whole point is to refuse a silent stop on a live book, and a bot that is
    down or a gauge that is missing is exactly when you know least.
    """
    raw = _host_exec("opens", timeout=15).strip()
    try:
        return int(float(raw.splitlines()[0]))
    except (ValueError, IndexError):
        return None


def _stop_warning() -> str:
    """The dynamic half of the stop confirmation: what is actually at risk."""
    opens = open_position_count()
    if opens is None:
        return (":grey_question: Could not read the open-position count "
                "(trader down or /metrics unreachable) — assume the book is live.")
    if opens > 0:
        return (f":rotating_light: **{opens} open position(s) right now.** "
                "Stopping the gateway leaves them unpriced and unexitable — "
                "the exit engine cannot fill and analyst SELLs are dropped "
                "until the gateway is back AND re-logged in.")
    return ":white_check_mark: No open positions — nothing is left unmanaged."


def restart_tunnel() -> tuple[bool, str]:
    """Restart ONLY the Cloudflare tunnel — leaves the trader untouched.
    Fixes a dead dashboard without interrupting trading. Blocking."""
    nm = CLOUDFLARED_CONTAINER_NAME
    try:
        cf = dclient.containers.get(nm)
    except NotFound:
        return False, f"{nm}: not found"
    except APIError as exc:
        return False, f"docker error: {exc}"
    try:
        if cf.status == "running":
            cf.restart(timeout=30)
            return True, f"{nm}: restarted"
        cf.start()
        return True, f"{nm}: started"
    except APIError as exc:
        return False, f"docker error: {exc}"


# ── Button panel View ────────────────────────────────────────────────────────

class TraderPanel(discord.ui.View):
    """Persistent 4-button control panel. custom_ids let Discord route
    clicks back here after a process restart (View.timeout=None)."""

    def __init__(self) -> None:
        super().__init__(timeout=None)
        # Gateway buttons only exist on the host that runs IB Gateway. Removing
        # them (rather than disabling) keeps the panel identical to before on
        # relaybot-vm, so nothing there changes shape.
        if not IBGW_ENABLED:
            for item in (self.b_gw_status, self.b_gw_logs, self.b_gw_restart,
                         self.b_gw_start, self.b_gw_stop):
                self.remove_item(item)

    async def _gate(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != USER_ID:
            await interaction.response.send_message(
                "Not authorized.", ephemeral=True,
            )
            return False
        return True

    async def _refresh_panel(self, interaction: discord.Interaction) -> None:
        """Re-render the pinned panel message in place with current state.
        Embed build hits the (blocking) docker SDK, so do it off the event
        loop — otherwise it can stall the loop and blow interaction acks."""
        try:
            embed = await asyncio.to_thread(_panel_embed)
            await interaction.message.edit(embed=embed, view=self)
        except (discord.HTTPException, AttributeError) as exc:
            log.warning("panel refresh failed: %s", exc)

    async def _run(self, interaction: discord.Interaction, action: str) -> None:
        if not await self._gate(interaction):
            return
        clicker = interaction.user
        log.info("button action=%s from %s (%s)", action, clicker.id, clicker)

        if action == "status":
            # Ack FIRST (docker calls are blocking; acking late = dead button),
            # then refresh the live panel AND reply with the actual state.
            await interaction.response.defer(ephemeral=True)
            await self._refresh_panel(interaction)
            t = await asyncio.to_thread(container_status)
            cf = await asyncio.to_thread(cloudflared_status)
            tup = t.startswith("running")
            cfup = cf == "running"
            await interaction.followup.send(
                f"{_dot(tup)} **Relaybot** `{CONTAINER_NAME}`\n{t}\n\n"
                f"{_dot(cfup)} **Cloudflare** `{CLOUDFLARED_CONTAINER_NAME}`\n{cf}",
                ephemeral=True,
            )
            return

        # mutating action — defer so we have >3s to run docker
        await interaction.response.defer()
        ok, msg = await asyncio.to_thread(do_action, action)
        # Update the pinned panel live so it shows the new state.
        await self._refresh_panel(interaction)
        after = container_status()
        tag = "OK" if ok else "FAIL"
        await interaction.followup.send(
            f"`/trader {action}` by {clicker.mention}\n"
            f"result: {tag} — {msg}\nnow: {after}",
            allowed_mentions=discord.AllowedMentions.none(),
        )

    async def _confirm(self, interaction: discord.Interaction, action: str) -> None:
        """Open an ephemeral Yes/Cancel gate before a disruptive action."""
        if not await self._gate(interaction):
            return
        view = ConfirmView(action, interaction.message)
        await interaction.response.send_message(
            f"⚠️ Confirm **{action}** of `{CONTAINER_NAME}` (+ Cloudflare tunnel)?\n"
            "This interrupts live trading.",
            view=view, ephemeral=True,
        )

    @discord.ui.button(
        label="Status", style=discord.ButtonStyle.primary,
        custom_id="trader:status", emoji="📊", row=0,
    )
    async def b_status(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        await self._run(interaction, "status")

    @discord.ui.button(
        label="Start", style=discord.ButtonStyle.success,
        custom_id="trader:start", emoji="▶️", row=0,
    )
    async def b_start(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        await self._run(interaction, "start")

    @discord.ui.button(
        label="Stop", style=discord.ButtonStyle.danger,
        custom_id="trader:stop", emoji="⏹️", row=0,
    )
    async def b_stop(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        # Disruptive → confirm first.
        await self._confirm(interaction, "stop")

    @discord.ui.button(
        label="Restart", style=discord.ButtonStyle.secondary,
        custom_id="trader:restart", emoji="🔁", row=0,
    )
    async def b_restart(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        # Disruptive → confirm first.
        await self._confirm(interaction, "restart")

    @discord.ui.button(
        label="Logs", style=discord.ButtonStyle.secondary,
        custom_id="trader:logs", emoji="📄", row=1,
    )
    async def b_logs(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        if not await self._gate(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        text = await asyncio.to_thread(fetch_logs, CONTAINER_NAME, 20)
        await interaction.followup.send(
            f"**`{CONTAINER_NAME}`** last lines:\n```\n{text}\n```",
            ephemeral=True,
        )

    @discord.ui.button(
        label="Tunnel", style=discord.ButtonStyle.secondary,
        custom_id="trader:tunnel", emoji="☁️", row=1,
    )
    async def b_tunnel(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        # Restart ONLY the Cloudflare tunnel — no trading interruption, so no
        # confirm gate.
        if not await self._gate(interaction):
            return
        clicker = interaction.user
        log.info("button action=tunnel from %s (%s)", clicker.id, clicker)
        await interaction.response.defer()
        ok, msg = await asyncio.to_thread(restart_tunnel)
        await self._refresh_panel(interaction)
        tag = "OK" if ok else "FAIL"
        await interaction.followup.send(
            f"`/tunnel restart` by {clicker.mention}\nresult: {tag} — {msg}",
            allowed_mentions=discord.AllowedMentions.none(),
        )


    # ── IB Gateway (host systemd, ibkr-oci only) ─────────────────────────
    @discord.ui.button(
        label="GW Status", style=discord.ButtonStyle.primary,
        custom_id="trader:gw_status", emoji="🩺", row=2,
    )
    async def b_gw_status(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        if not await self._gate(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        healthy, text = await asyncio.to_thread(gateway_status)
        await interaction.followup.send(
            f"{_dot(healthy)} **IB Gateway**\n{text}", ephemeral=True,
        )

    @discord.ui.button(
        label="GW Logs", style=discord.ButtonStyle.secondary,
        custom_id="trader:gw_logs", emoji="📜", row=2,
    )
    async def b_gw_logs(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        if not await self._gate(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        text = await asyncio.to_thread(gateway_logs)
        await interaction.followup.send(
            f"**`{IBGW_UNIT}`** last 40 lines:\n```\n{text}\n```", ephemeral=True,
        )

    @discord.ui.button(
        label="GW Restart", style=discord.ButtonStyle.danger,
        custom_id="trader:gw_restart", emoji="🔁", row=2,
    )
    async def b_gw_restart(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        # Most disruptive button on the panel: a cold gateway restart drops the
        # saved session, so the re-login needs a 2FA tap and the API can stay
        # down for minutes (1h51m on 2026-08-06 when nobody tapped). Always
        # confirm, and say so in the prompt.
        if not await self._gate(interaction):
            return
        view = ConfirmView("gw_restart", interaction.message)
        await interaction.response.send_message(
            f"⚠️ Restart **`{IBGW_UNIT}`** on this host?\n"
            "The API drops immediately and comes back only after IBC logs in "
            "again — which needs a **Second Factor Authentication tap in IBKR "
            "Mobile**. Have your phone in hand before confirming.",
            view=view, ephemeral=True,
        )

    @discord.ui.button(
        label="GW Start", style=discord.ButtonStyle.success,
        custom_id="trader:gw_start", emoji="▶️", row=3,
    )
    async def b_gw_start(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        # No confirmation: starting a stopped gateway cannot lose anything, and
        # the case for this button is "it is down and I want it up", usually in
        # a hurry. It still says what happens next, because a cold start sends a
        # 2FA push and an untapped push means it stays down anyway.
        if not await self._gate(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        ok, msg = await asyncio.to_thread(gateway_start)
        await self._refresh_panel(interaction)
        await interaction.followup.send(
            f"{_dot(ok)} `gw_start` — {msg}\n\n"
            ":iphone: Watch for the **Second Factor Authentication** push and "
            "tap it — the gateway stays logged out until you do.",
            ephemeral=True,
        )

    @discord.ui.button(
        label="GW Stop", style=discord.ButtonStyle.danger,
        custom_id="trader:gw_stop", emoji="⏹️", row=3,
    )
    async def b_gw_stop(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        # Worse than restart: a restart at least intends to come back. Stop
        # leaves the book unmanaged for as long as you forget about it, so the
        # prompt reads the live open-position count instead of talking in
        # generalities about risk.
        if not await self._gate(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        warning = await asyncio.to_thread(_stop_warning)
        view = ConfirmView("gw_stop", interaction.message)
        await interaction.followup.send(
            f"⛔ Stop **`{IBGW_UNIT}`** on this host?\n{warning}\n\n"
            "Starting it again needs a **2FA tap in IBKR Mobile**, so this is "
            "not a quick toggle.",
            view=view, ephemeral=True,
        )


class ConfirmView(discord.ui.View):
    """Ephemeral one-shot Yes/Cancel for disruptive actions. Transient
    (30s timeout), so no custom_id persistence needed."""

    def __init__(self, action: str, panel_message: discord.Message) -> None:
        super().__init__(timeout=30)
        self.action = action
        self.panel_message = panel_message

    async def _gate(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != USER_ID:
            await interaction.response.send_message(
                "Not authorized.", ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        if not await self._gate(interaction):
            return
        log.info("confirm %s from %s", self.action, interaction.user.id)
        await interaction.response.defer()
        if self.action == "gw_restart":
            ok, msg = await asyncio.to_thread(gateway_restart)
        elif self.action == "gw_stop":
            ok, msg = await asyncio.to_thread(gateway_stop)
        else:
            ok, msg = await asyncio.to_thread(do_action, self.action)
        # Refresh the live pinned panel.
        try:
            embed = await asyncio.to_thread(_panel_embed)
            await self.panel_message.edit(embed=embed, view=TraderPanel())
        except (discord.HTTPException, AttributeError) as exc:
            log.warning("panel refresh after confirm failed: %s", exc)
        tag = "OK" if ok else "FAIL"
        await interaction.edit_original_response(
            content=f"`{self.action}` {tag} — {msg}", view=None,
        )
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="✖️")
    async def cancel(self, interaction: discord.Interaction, _btn: discord.ui.Button):
        if not await self._gate(interaction):
            return
        await interaction.response.edit_message(content="Cancelled.", view=None)
        self.stop()


def _dot(up: bool) -> str:
    return "🟢" if up else "🔴"


def _panel_embed() -> discord.Embed:
    """Live status panel. Re-rendered on every button press so the pinned
    message itself shows what's running — no need to read the reply text."""
    traw = container_status()
    cf = cloudflared_status()
    trader_up = traw.startswith("running")
    tunnel_up = cf == "running"
    if trader_up and tunnel_up:
        color = 0x2ECC71  # green — all good
    elif trader_up:
        color = 0xE67E22  # orange — trader up but dashboard tunnel down
    else:
        color = 0xE74C3C  # red — trader down
    e = discord.Embed(
        title=PANEL_TITLE,
        description=f"`{os.uname().nodename}` · only the authorized user can use these buttons.",
        color=color,
    )
    e.add_field(
        name="Trader", value=f"{_dot(trader_up)} `{CONTAINER_NAME}` — {traw}",
        inline=False,
    )
    e.add_field(
        name="Cloudflare (dashboard tunnel)",
        value=f"{_dot(tunnel_up)} `{CLOUDFLARED_CONTAINER_NAME}` — {cf}",
        inline=False,
    )
    if IBGW_ENABLED:
        # Rendered on every 30s auto-refresh, so a logged-out gateway is
        # visible on the pinned panel without anyone clicking anything.
        try:
            gw_up, gw_text = gateway_status()
            e.add_field(
                name="IB Gateway (host systemd)",
                value=f"{_dot(gw_up)} {gw_text}", inline=False,
            )
            if not gw_up:
                color = 0xE74C3C
                e.colour = discord.Colour(color)
        except Exception as exc:                     # never break the panel
            log.warning("gateway status for panel failed: %s", exc)
    # TZ=ET on host+container, so naive now() is Eastern.
    e.set_footer(text=f"{PANEL_TAG} · updated {datetime.now():%H:%M:%S ET}")
    return e


# Live reference to the pinned panel message, set in on_ready. The
# auto-refresh loop edits it every 30s so a crash / tunnel drop shows up
# even when nobody clicks a button.
PANEL_MESSAGE: discord.Message | None = None


@tasks.loop(seconds=30)
async def _auto_refresh() -> None:
    if PANEL_MESSAGE is None:
        return
    try:
        embed = await asyncio.to_thread(_panel_embed)
        await PANEL_MESSAGE.edit(embed=embed, view=TraderPanel())
    except discord.HTTPException as exc:
        log.warning("auto-refresh failed: %s", exc)


async def _find_panel(channel: discord.TextChannel) -> discord.Message | None:
    async for m in channel.history(limit=50):
        if m.author.id != channel.guild.me.id:
            continue
        for emb in m.embeds:
            if (emb.footer.text or "").startswith(PANEL_TAG):
                return m
    return None


async def post_fresh_panel(channel: discord.TextChannel) -> discord.Message:
    """Delete any existing panel messages and post a brand-new pinned panel.
    Cure for a panel that's gone stale/dead-click on a Discord client — the
    old cached message is replaced with a fresh interactive one."""
    async for m in channel.history(limit=50):
        if m.author.id != channel.guild.me.id:
            continue
        if any((emb.footer.text or "").startswith(PANEL_TAG) for emb in m.embeds):
            try:
                await m.delete()
            except discord.HTTPException:
                pass
    embed = await asyncio.to_thread(_panel_embed)
    msg = await channel.send(embed=embed, view=TraderPanel())
    try:
        await msg.pin(reason="repost trader control panel")
    except discord.HTTPException as exc:
        log.warning("repost pin failed: %s", exc)
    global PANEL_MESSAGE
    PANEL_MESSAGE = msg
    log.info("fresh panel posted (id=%s)", msg.id)
    return msg


# ── Bot ──────────────────────────────────────────────────────────────────────

intents = discord.Intents.default()
intents.message_content = True  # for legacy /trader text fallback

bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def setup_hook() -> None:
    # Register the View as persistent BEFORE login so clicks on the old
    # panel (from a previous process) route to the new instance.
    bot.add_view(TraderPanel())


@bot.event
async def on_ready() -> None:
    me = bot.user
    log.info("logged in as %s (id=%s)", me, me.id if me else "?")
    channel = bot.get_channel(CHANNEL_ID)
    if channel is None:
        log.error("channel %s not visible — invite bot to that server "
                  "and ensure it can read history", CHANNEL_ID)
        return

    # Track the panel message as soon as it exists. Pinning is cosmetic and is
    # allowed to fail (missing Manage Messages, pin limit); losing the reference
    # because of that is not — it leaves PANEL_MESSAGE None, the 30s refresh
    # loop never starts, and the panel then shows frozen state forever, e.g.
    # "running" for a container that has since died.
    existing = await _find_panel(channel)
    msg = existing
    try:
        if existing:
            await existing.edit(embed=_panel_embed(), view=TraderPanel())
            log.info("panel refreshed (id=%s)", existing.id)
        else:
            msg = await channel.send(embed=_panel_embed(), view=TraderPanel())
            log.info("panel posted (id=%s)", msg.id)
        # Pin so the panel stays reachable at the top — no scrolling the
        # channel to find it. Needs Manage Messages; warn (don't crash) if not.
        if not msg.pinned:
            await msg.pin(reason="keep trader control panel reachable")
            log.info("panel pinned (id=%s)", msg.id)
    except discord.Forbidden:
        log.warning("cannot pin panel — grant the bot Manage Messages in "
                    "channel %s", CHANNEL_ID)
    except discord.HTTPException as exc:
        log.warning("panel upsert/pin failed: %s", exc)

    # Wire the auto-refresh loop to this message. on_ready can fire again on
    # reconnect, so guard against double-start.
    global PANEL_MESSAGE
    PANEL_MESSAGE = msg
    if PANEL_MESSAGE is not None and not _auto_refresh.is_running():
        _auto_refresh.start()
        log.info("auto-refresh loop started (30s)")


@bot.event
async def on_message(message: discord.Message) -> None:
    """Legacy text path — typo-tolerant /trader command. Stays for users
    on mobile who can't see the button panel (e.g. compact view)."""
    if message.author.bot:
        return
    if message.channel.id != CHANNEL_ID:
        return
    raw = (message.content or "").strip().lower()
    # Strip leading slash + any whitespace; accept common typos
    if not raw.startswith("/"):
        return
    head = raw.lstrip("/").lstrip()
    # Accept "trader", "tarder", "tradr", "trade" near-misses
    for prefix in ("trader", "tarder", "tradr", "trade "):
        if head.startswith(prefix):
            rest = head[len(prefix):].strip()
            break
    else:
        return

    if message.author.id != USER_ID:
        await message.reply("Not authorized.", mention_author=False)
        return

    action = (rest.split() or [""])[0]
    if action not in ("status", "start", "stop", "restart", "panel", "repost"):
        await message.reply(
            "Usage: `/trader status|start|stop|restart|panel`",
            mention_author=False,
        )
        return

    if action in ("panel", "repost"):
        # Repost a fresh pinned panel — fixes a stale/dead-click panel.
        new = await post_fresh_panel(message.channel)
        await message.reply(
            f"Fresh control panel posted + pinned (id={new.id}).",
            mention_author=False,
        )
        return

    if action == "status":
        await message.reply(
            f"trader: {container_status()}\ntunnel: {cloudflared_status()}",
            mention_author=False,
        )
        return

    await message.reply(f"trader {action}: working…", mention_author=False)
    ok, msg = await asyncio.to_thread(do_action, action)
    after = container_status()
    tag = "OK" if ok else "FAIL"
    await message.reply(
        f"trader {action} {tag}: {msg}\nnow: {after}", mention_author=False,
    )


if __name__ == "__main__":
    _preflight()
    log.info(
        "starting control bot: channel=%s authorized_user=%s container=%s",
        CHANNEL_ID, USER_ID, CONTAINER_NAME,
    )
    bot.run(TOKEN, log_handler=None)
