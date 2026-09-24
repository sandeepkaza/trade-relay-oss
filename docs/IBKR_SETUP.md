# IBKR Setup

IB Gateway runs as a **systemd unit on the VM host**, not as a docker sidecar.

How the container reaches it depends on the host, and the two are not interchangeable:

| host | container network | `IBKR_HOST` |
|---|---|---|
| generic / EC2 | docker bridge | `host.docker.internal` via the `host-gateway` alias (§A.4) |
| **ibkr-oci** | **`network_mode: host`** (`docker-compose.override.yml`) | **`127.0.0.1`** |

On ibkr-oci the bridged path does not work: the container sits on `172.18.x` while `host.docker.internal` resolves to `docker0` at `172.17.0.1`, and that asymmetry breaks the API handshake (connect-then-drop) even with the container IP in Gateway's TrustedIPs. Host networking puts the trader on loopback, which Gateway always trusts. The cost is that each lane must be given a distinct `--port` in its compose `command`, since nothing is publishing ports any more.

| Why host-install | |
|---|---|
| One fewer layer to wedge | `journalctl -u ibgateway` is the only place to look on a crash. |
| Smaller supply chain | No third-party docker image baking IBC + Xvfb + JVM. |
| Direct systemd watchdog | Crashes restart immediately; no docker daemon dependency. |
| Easier version bumps | `apt`-style: download new installer, swap, `systemctl restart`. |

Tradeoff: ~15 min more setup vs `docker compose up`. Worth it for 24/7 prod.

Bot code is broker-agnostic — `broker_router` dispatches to `IBKRExecutor` whenever `[trading] broker = ibkr` in `config.ini`. Two-step safety gate: also requires `[ibkr] orders_enabled = true`.

---

## 0. IBKR account prerequisites

Apply on `interactivebrokers.com` → Settings → Account Settings → Trading Permissions:

| Permission | Why |
|---|---|
| **Options — Level 3+** | Buy/sell single-leg long calls/puts. |
| **SPX / Index Options** | SPXW weeklies. Separate gate from equity options. |
| **Real-time market data — OPRA + CBOE** | Otherwise quotes are 15-min delayed. ~$1.50/mo OPRA + $5/mo CBOE. |
| **API access** | Tick API permissions ON for the account. Pro account required (not Lite). |

Approvals take 1–3 business days. Validate against the paper account first (`DU#######`, free, mirrors live perms, no 2FA).

---

## §A. Host install on the VM (Ubuntu 22.04)

The `scripts/deploy/bootstrap-ec2.sh` user-data already installs Xvfb + default-jre + downloads IBC. Below covers the manual finish (credentials + IBKR username file + first launch).

### A.1 Install Gateway

Landing page: **https://www.interactivebrokers.com/en/trading/ibgateway-stable.php**

```bash
sudo -u trader -i
cd ~
IBDOWNLOAD=https://download.interactivebrokers.com/installers/ibgateway
curl -o ibgateway-standalone-linux-x64.sh \
  "$IBDOWNLOAD/stable-standalone/ibgateway-stable-standalone-linux-x64.sh"
chmod +x ibgateway-standalone-linux-x64.sh
DISPLAY=:99 ./ibgateway-standalone-linux-x64.sh -q   # silent install via Xvfb
```

Installer drops files in `~/Jts/ibgateway/<version>/`. Note the version (e.g., `1030`) — referenced by `--tws-path` in the systemd unit.

### A.2 IBC (auto-login + daily-restart handler)

```bash
cd /opt
sudo wget https://github.com/IbcAlpha/IBC/releases/download/3.20.0/IBCLinux-3.20.0.zip
sudo unzip IBCLinux-3.20.0.zip -d ibc
sudo chown -R trader:trader /opt/ibc
```

`/opt/ibc/config.ini` (`chmod 600` after writing):

```ini
IbLoginId=YOUR_IBKR_USERNAME
IbPassword=YOUR_IBKR_PASSWORD
TradingMode=paper                      ; "live" once you flip
IbDir=/home/trader/Jts
FIX=no
OverrideTwsApiPort=4002                ; the port you CHOOSE — not a mode indicator
ReadOnlyApi=no
AcceptIncomingConnectionAction=accept
AllowBlindTrading=yes
DismissPasswordExpiryWarning=yes
DismissNSEComplianceNotice=yes
AutoRestartTime=23:55
ClosedownAt=
SaveTwsSettingsAt=
```

```bash
sudo chmod 600 /opt/ibc/config.ini
sudo chown trader:trader /opt/ibc/config.ini
```

⚠️ **The port does not tell you the mode.** IBKR's stock convention is 4001 live / 4002 paper, but `OverrideTwsApiPort` makes the port a free choice, and **on ibkr-oci 4002 is LIVE** — `/opt/ibc/config.ini` there pairs `TradingMode=live` with `OverrideTwsApiPort=4002`. The `[ibkr] port` comment in `config_manager.py` still repeats the stock convention and is wrong for that host. Read `TradingMode` in the IBC config; never infer the mode from the port.

A paper lane therefore needs a **second Gateway process**, not a second port on this one: one Gateway serves exactly one login and one mode. On ibkr-oci that is the `ibgateway-ibkr-paper` unit on 4004, with its own IBC config, its own `--tws-settings-path` (two Gateways sharing one settings dir corrupt each other's `jts.ini` on shutdown), and its own Xvfb display. `scripts/ops/setup-ibgateway-paper.sh` builds it.

⚠️ Plaintext IBKR password on disk. Mode 600 + dedicated `trader` user is the floor. Better: AWS Secrets Manager → fetched into a tmpfs file at boot — backlog item.

### A.3 systemd unit

`/etc/systemd/system/ibgateway.service`:

```ini
[Unit]
Description=IB Gateway via IBC
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=trader
Group=trader
Environment="DISPLAY=:99"
ExecStartPre=/usr/bin/Xvfb :99 -screen 0 1024x768x24 &
ExecStart=/opt/ibc/scripts/ibcstart.sh "stable" --gateway --mode=paper \
  --ibc-path=/opt/ibc --ibc-ini=/opt/ibc/config.ini --tws-path=/home/trader/Jts
Restart=on-failure
RestartSec=30
TimeoutStartSec=180

[Install]
WantedBy=multi-user.target
```

Bind to loopback only — the trader container reaches it through host-gateway DNS; nothing else on the box should:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now ibgateway
journalctl -u ibgateway -f
# Wait for "IBC: Login completed successfully"
```

Verify the port is bound to loopback (NOT 0.0.0.0):

```bash
ss -ltn | grep 4002
# Expected: 127.0.0.1:4002    (NOT 0.0.0.0:4002)
```

### A.4 Trader container → host Gateway wiring

This section is the **bridged** path. On ibkr-oci it does not apply — `docker-compose.override.yml` puts the trader on `network_mode: host` and resets `extra_hosts`, so set `IBKR_HOST=127.0.0.1` there instead. Everything below is for a host that keeps the container on the docker bridge.

`docker-compose.yml` wires the trader's docker DNS so `host.docker.internal` resolves to the host loopback:

```yaml
trader:
  extra_hosts:
    - "host.docker.internal:host-gateway"
```

`.env` on the VM:

```env
IBKR_HOST=host.docker.internal
IBKR_PORT=4002
```

`config.ini` `[ibkr]` block:

```ini
[ibkr]
host = 127.0.0.1              ; overridden by env on VM
port = 4002
client_id = 7
account = DU########          ; paper account number
orders_enabled = false        ; flip ON only after smoke + paper E2E pass
market_data_type = 3          ; 1=live, 3=delayed
```

### A.5 Bring up the stack

```bash
sudo systemctl status ibgateway          # must be Active (running) + listening
cd /home/ubuntu/trader
docker compose up -d trader              # depends on host's 4002, no compose dep needed
docker compose logs -f trader            # expect "IBKR connected: host.docker.internal:4002"
curl -s http://127.0.0.1:8000/api/health | jq .overall.status
```

---

## §B. Going live (after paper validation ≥ 1 week)

1. `config.ini` → `[ibkr] port = 4001`, `account = U#######`.
2. `.env` → `IBKR_PORT=4001`.
3. `/opt/ibc/config.ini` → `TradingMode=live` + `OverrideTwsApiPort=4001`.
4. `sudo systemctl restart ibgateway` — first run prompts a **single 2FA push** on IBKR Mobile. Tap it. IBC keeps the session alive across the daily restart after that.
5. In the IBKR portal verify the API session shows "Live, Logged In".
6. `[ibkr] orders_enabled = true` → `docker compose restart trader`.

Live 2FA caveat: IBKR force-restarts Gateway sessions once per day. The daily restart needs your phone reachable for the 2FA tap. Workaround pinned in `AutoRestartTime` set to a time you're awake.

---

## §C. Local dev (Windows desktop next to running Gateway)

Just leave IB Gateway running in the system tray on the Windows machine.

- `config.ini` `[ibkr] host = 127.0.0.1`
- Don't set `IBKR_HOST` env var
- Smoke: `python scripts/ibkr_smoke.py` (clientId=8)
- E2E: `python scripts/ibkr_e2e_paper.py` (clientId=9)
- Bot itself uses clientId=7 (reserved)

---

## §D. Monitoring

`/etc/cron.d/trader-alarms`:

```cron
*/5 14-20 * * 1-5 trader /usr/local/bin/check_ibgw.sh
```

`/usr/local/bin/check_ibgw.sh`:

```bash
#!/bin/bash
if ! ss -ltn | grep -q ":4002 "; then
  curl -X POST -H "Content-Type: application/json" \
    -d "{\"content\":\"🚨 IB Gateway port 4002 not listening — bot will fail on next IBKR order\"}" \
    "$DISCORD_WEBHOOK_URL"
fi
```

---

## §E. Smoke + E2E validation (any shape)

```bash
# Read-only smoke — clientId=8.
python scripts/ibkr_smoke.py

# Full paper E2E — places real paper orders. clientId=9.
python scripts/ibkr_e2e_paper.py
```

Smoke prints `[1] CONNECT ok` then `[2] BALANCE ok cash=...`. Empty `{}` quotes → OPRA/CBOE subscriptions missing.

E2E places one BUY-cancel, one SPY round-trip, one SPX 0DTE round-trip. Expect terminal `Filled` or `Cancelled` with non-zero `avgFillPrice`.

---

## §F. Rollback to Public.com

`config.ini` → `[trading] broker = public`. Save. `docker compose restart trader`. Position state stays in DB — no migration.

## §G. Known issues / backlog

- `Order.public_order_id` column is a misnomer (holds IBKR oids too); rename deferred.
- Reconciler rebuilds open positions from DB; periodic cross-check against `ib.positions()` not implemented.
- Live-mode 2FA loop is unsolved for fully-unattended ops (§B).
- IBKR password on disk in `/opt/ibc/config.ini` — move to AWS Secrets Manager + tmpfs.
