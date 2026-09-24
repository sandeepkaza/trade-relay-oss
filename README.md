# TradeRelay

Discord → broker automated options trading. Monitors signal channels, scores alerts, sizes positions, places + manages trades.

**Brokers:** set `[trading] broker`.

| Broker | Status |
|---|---|
| `ibkr` | **Supported.** Interactive Brokers via IB Gateway / TWS (`ib_insync`). See [docs/IBKR_SETUP.md](docs/IBKR_SETUP.md). |
| `lime` | **Experimental.** Lime Trader REST. Not proven with live money. It has no broker-side stop orders, so nothing protects a position while the app is down. Use a demo account. |
| `public`, `tradier` | Included, flag-off, not maintained. |

Every vendor executor subclasses `PublicExecutor` (the shared engine: guardrails, scoring, sizing, exits, analyst-sell logic) and overrides only order placement + fill polling. Trading behavior is the same across brokers.

> ⚠️ **This software places real orders with real money.** Options can go to zero in minutes. There is no warranty ([LICENSE](LICENSE)). Nothing here is financial advice. Start on a paper/demo account with `manual_halt = true`, which blocks every BUY at the guardrail, and turn it off only after you have watched it parse and size alerts correctly.

> ⚠️ **Discord self-bot warning.** The optional forwarder (`app/ingest/forwarder.py`) logs in with a *personal user token*, and Discord's Terms of Service forbid that ("self-bots"). Using it can get your Discord account banned. The supported path is a normal **bot** token reading channels in a server where that bot has been invited. Also respect the terms of any paid alert service you relay.

---

## Quick Start

```bash
git clone <your-fork-url> && cd traderelay
cp .env.example .env               # 1. fill DISCORD_TOKEN, BROKER, broker creds
cp config.example.ini config.ini   # 2. set [discord] channel_ids
docker compose up                  # 3. run
```

Then open **http://localhost:8000**. That's it — the app starts the Discord bot, parser, executor, position monitor, and dashboard together.

**Minimum you must provide** (in `.env`): a Discord **bot** token, your `BROKER` choice, and that broker's API key/account. Everything else has a working default. Leave `dry_run = true` in `config.ini` until you've watched it parse a few alerts — then flip to `false` for live trading.

**No Docker?** `pip install -r requirements.txt` then `python -m uvicorn app.core.app:app --host 127.0.0.1 --port 8000`.

Full setting reference is in [Configuration](#configuration-configini) below — every key there is optional and falls back to a default if omitted.

---

## Repo layout

```
app/
  core/         app entry, db, models, config, logging
  ingest/       Discord listener, forwarder, parser, AI parser, OCR
  execution/    shared executor + IBKR / Lime / Public / Tradier bridges, order queue, exit engine
  risk/         guardrails, AI scorer, Kelly + VIX sizing
  monitors/     position, greeks, API, health, reconciler, premarket
  analytics/    trade logger, pattern review, infra report, trader-control daemon
static/         dashboard.html, api_dashboard.html
docs/           architecture, IBKR setup, exit-engine notes
tests/          pytest suite
```

## Daily Startup

```bash
python -m uvicorn app.core.app:app --host 127.0.0.1 --port 8000
```

Open the dashboard at **http://127.0.0.1:8000**

This single command starts **everything**:
- **Discord bot** — watches your `#general` channel for alerts
- **Forwarder** — reads trading server channels with your account and relays them to `#general` via webhook
- **Position monitor** — tracks open positions and triggers auto-exits (PT1/PT2/PT3/TPT/SL/Trailing SL)
- **Trade logger** — posts trade events and daily summaries to `#trade-logs`
- **Dashboard** — live web UI with WebSocket real-time updates

Press `Ctrl+C` to stop.

---

## Architecture: Full End-to-End Flow

```
① forwarder.py         Reads trading server channels via personal user token
                       Cross-channel dedup prevents double-forwarding
                              ↓  webhook POST
② discord_listener.py  Bot receives alert in #general
                       Author filter → Regex parser → AI fallback parser
                       Content hash computed → saved to SQLite DB
                              ↓  alert dispatched
③ public_executor.py   8 guardrail checks → AI scorer (0-100) → Kelly cap
                       Limit order placed on Public.com → fill polling loop
                              ↓  position opened
④ position_monitor.py  Price polled every 2s → PT1/PT2/PT3/TPT/SL auto-exits
                              ↓  fills + P&L
⑤ app.py (FastAPI)     REST API + WebSocket dashboard
   trade_logger.py     Discord #trade-logs embeds + daily summary
   logs/               Rotating structured log files (10 separate streams)
```

---

## Configuration (`config.ini`)

### [discord]
| Key | Description |
|-----|-------------|
| `token` | Official Discord bot token (Message Content intent required) |
| `channel_ids` | Channel IDs the bot monitors (comma-separated) |
| `no_filter_channel_ids` | Channels where ALL messages are treated as alerts (skip author filter) |
| `trade_log_channel_id` | Channel where bot posts trade embeds |
| `authors` | Author names to watch (blank = all messages) |
| `history_backfill_limit` | Past messages to load on startup (0 = disabled) |
| `execute_history` | Set `true` to execute backfilled alerts (use with caution) |

### [public]
| Key | Description |
|-----|-------------|
| `api_key` | Your Public.com API key |
| `account_number` | Your Public.com account number |

### [trading]
| Key | Default | Description |
|-----|---------|-------------|
| `broker` | `public` | Active broker: `public` \| `ibkr` \| `tradier`. Hot-swappable (drops the cached executor, no restart). |
| `dry_run` | `false` | **Paper simulation, TESTING ONLY.** `true` = parse alerts and write *simulated* FILLED rows at fake prices — it does NOT pause trading and it pollutes P&L with phantom fills. To stop trading use `manual_halt`. |
| `manual_halt` | `false` | **The real "stop trading" switch.** `true` = block all new BUY entries at the guardrail (no orders, no rows, no simulation); SELLs still fill so open positions stay exitable. Toggle from dashboard Services → Trade execution → HALTED. Persists across restart; one-flip revert. Shows a red **⛔ TRADING HALTED** header badge. |
| `first_sell_closes_all` | `false` | `true` = the analyst's first SELL on a position (even a `1/4` partial) exits the ENTIRE remaining position, ignoring the fraction; later portions are no-ops. Also closes the post-PT2 runner. Applies to all brokers. Open rows show a **FULL-EXIT** badge when on. |
| `max_pending_minutes` | `30` | Auto-cancel orders unfilled after this many minutes |
| `position_monitor_enabled` | `true` | Enable price polling and auto-exits |
| `poll_interval_seconds` | `2` | How often to poll option prices (seconds) |
| `buy_slippage` | `0.05` | BUY limit = alert_price × 1.05 |
| `sell_slippage` | `0.05` | SELL limit = current_price × 0.95 |
| `fill_poll_interval_seconds` | `3` | How often to check Public.com for fills |
| `auto_exit_enabled` | `true` | Master toggle for all auto-exit rules |
| `pt1_enabled` | `true` | Enable PT1 rule |
| `pt1_pct / pt1_sell` | `30 / 0.50` | Sell 50% when +30% gain |
| `pt2_enabled` | `true` | Enable PT2 rule |
| `pt2_pct / pt2_sell` | `60 / 0.50` | Sell 50% of remaining at +60% |
| `pt3_enabled` | `true` | Enable PT3 rule |
| `pt3_pct` | `100` | Trigger at +100% gain |
| `sl_enabled` | `true` | Enable fixed stop loss |
| `sl_pct` | `-50` | Stop loss: sell all at -50% |
| `trailing_sl_enabled` | `true` | After PT1: trailing stop from peak |
| `trailing_sl_pct` | `30` | Trailing stop % below peak |
| `tpt_enabled` | `true` | Enable Trailing Profit Target (lets winners run past PT3) |
| `pt3_sell` | `0.5` | Fraction to sell at PT3 before arming TPT (1.0 = old behavior) |
| `tpt_arm_pct` | `100` | Arm TPT when position is up this % |
| `tpt_trail_pct` | `25` | TPT exit: sell when price drops this % from peak |
| `size_default / size_xs … size_xl` | `1–5` | Contracts per size tag in alerts |
| `local_parser_enabled` | `true` | Enable fast regex parser |
| `ai_parser_enabled` | `true` | Enable Claude AI fallback parser |
| `ai_scorer_enabled` | `true` | Score each BUY 0–100 and adjust contracts |
| `ai_scorer_min_score` | `30` | Skip trades scoring below this |
| `kelly_enabled` | `true` | Apply Kelly Criterion contract cap |
| `kelly_max_risk_pct` | `2.0` | Max % of account to risk per trade |
| `kelly_lookback_days` | `30` | Days of history for win-rate/payoff calc |
| `trade_logger_enabled` | `true` | Post trade events to #trade-logs |
| `daily_summary_enabled` | `true` | Post P&L summary at 4:15 PM ET weekdays |
| `daily_summary_ai_enabled` | `true` | Add Claude insights to daily summary |
| `analyst_tracking_enabled` | `true` | Enable /api/analysts leaderboard |
| `forwarder_enabled` | `true` | Enable the message forwarder |

### [guardrails] ⚠️ vs Auto-Exits
**Guardrails** = protections that block NEW BUY orders (max cost, positions, daily limits, duplicates)

**Auto-Exits** = protections that close EXISTING positions (PT1/PT2/PT3/SL/TPT in [trading] section)

> **Note:** Disabling `guardrails_enabled` ONLY affects BUY blocking. Auto-exits (profit targets, stop losses) still work to protect open positions.

| Key | Default | Description |
|-----|---------|-------------|
| `guardrails_enabled` | `true` | Master toggle - set `false` to disable ALL guardrails |
| `dedup_window_seconds` | `60` | Block same OSI+action within this window |
| `market_hours_only` | `true` | Only trade Mon-Fri during market hours |
| `market_open_hour` | `9` | Market open hour (ET) |
| `market_open_minute` | `30` | Market open minute (ET) |
| `market_close_hour` | `16` | Market close hour (ET) |
| `market_close_minute` | `0` | Market close minute (ET) |
| `max_daily_loss` | `200` | Halt trading if daily P&L drops below -$200 |
| `max_open_positions` | `5` | Block new BUYs above this count |
| `max_daily_trades` | `20` | Block new BUYs after this many today |
| `max_trade_cost` | `10000` | Block if price × contracts × 100 > $10,000 |
| `trade_cooldown_seconds` | `10` | Cooldown between trades on same symbol |
| `cross_channel_dedup_seconds` | `120` | Block same content hash from different channels |

### [ai_parser]
| Key | Description |
|-----|-------------|
| `api_key` | Claude (Anthropic) API key |
| `model` | Model ID (e.g. `claude-haiku-4-5-20251001`) |
| `timeout_seconds` | Timeout for AI API calls (seconds) |

### [forwarder]
| Key | Description |
|-----|-------------|
| `user_token` | Your personal Discord user token (see the self-bot warning at the top) |
| `webhook_url` | Webhook URL for your #general channel |
| `forward_authors` | Only forward from these authors (blank = all) |

### [forwarder.channels]
Add/remove channels by commenting/uncommenting:
```ini
[forwarder.channels]
alerts-channel = 100000000000001111
# other-channel = 100000000000002222   ← uncomment to enable
```

### [logging]
| Key | Default | Description |
|-----|---------|-------------|
| `file_logging_enabled` | `true` | Write log files (false = console only) |
| `log_dir` | `./logs` | Directory for log files |
| `log_level` | `DEBUG` | Verbosity: DEBUG / INFO / WARNING / ERROR |
| `max_file_size_mb` | `5` | Rotate each file at this size (MB) |
| `backup_count` | `5` | Keep N rotated backup copies per file |
| `pipeline_timing_enabled` | `true` | Enable ms-level timing in pipeline.log |

---

## Auto-Exit Rules

| Rule | Trigger | Action | Condition |
|------|---------|--------|-----------|
| **PT1** | +30% gain | Sell 50% of position | Always (first exit) |
| **PT2** | +60% gain | Sell 50% of remaining | After PT1 |
| **PT3** | +100% gain | Sell `pt3_sell` fraction (default 50%) | After PT2 |
| **TPT** | Price drops `tpt_trail_pct`% from peak | Sell all remaining | Armed after PT3 (if `tpt_enabled`) |
| **SL** | −50% loss | Sell everything | Before PT1 (or if trailing SL disabled) |
| **Trailing SL** | −30% from peak | Sell all remaining | After PT1 (replaces fixed SL; disabled when TPT is armed) |

**TPT example** (default settings, entry at $1.00, 4 contracts):
```
PT1 at +30%  → sell 2 at $1.30
PT2 at +60%  → sell 1 at $1.60
PT3 at +100% → sell 0 (pt3_sell=0.5 of 1 rounds to 0), TPT ARMED
Price runs to $3.50 peak → no exit yet
Price reverses to $2.45 → 30% drop from peak → TPT FIRES, sell remaining at +145%
```

All percentages and rules are configurable in `config.ini → [trading]`. Each rule has an individual `_enabled` toggle.

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Live dashboard |
| `GET` | `/api/health` | Health check |
| `GET` | `/api/positions` | Open positions |
| `GET` | `/api/orders` | Order history (last 200) |
| `GET` | `/api/alerts` | Alert feed (last 200) |
| `POST` | `/api/positions/{id}/exit` | Manually exit one position |
| `POST` | `/api/positions/exit-all` | Exit ALL positions |
| `POST` | `/api/orders/{id}/cancel` | Cancel pending order |
| `GET` | `/api/balance` | Account balance |
| `GET` | `/api/guardrails` | Safety limits status |
| `GET` | `/api/analysts?days=30` | Per-analyst performance leaderboard |
| `GET` | `/api/stats?days=30` | Overall stats + cumulative P&L chart |
| `GET` | `/api/channels` | Configured channels and author filters |
| `GET` | `/api/export/trades?days=30` | Download trade history CSV |
| `GET` | `/api/export/positions` | Download all positions CSV |
| `GET` | `/api/export/alerts?days=30` | Download alert history CSV |
| `POST` | `/api/daily-summary` | Manually trigger daily P&L summary post |
| `GET` | `/metrics` | Prometheus text — trading hot-path metrics (see Reliability) |
| `WS` | `/ws` | Real-time push (positions, orders, alerts, balance, auto-exits) |

---

## Reliability & Observability

Hardening on the order path:

| Concern | Mechanism |
|---|---|
| **Concurrent alerts can't breach caps** | Orders execute through ONE serialized worker (`execution/order_queue.py`) — count→insert is effectively atomic. Flag `[trading] serialized_order_execution` (default true). |
| **Exit-priority** | SELL/close_all jump ahead of queued BUYs during a flatten cascade (FIFO within each lane). |
| **Duplicate alerts** | `UNIQUE(discord_message_id)` + IntegrityError handling — one Discord message can't create two alert rows, even across a restart. |
| **Daily-loss halt survives restart** | Persisted in the `system_state` table; a crash/redeploy on a day that hit the cap does NOT resume trading. |
| **No trading while blind** | New BUYs blocked when an open position has no price mark past `stale_mark_grace_seconds` (default 120). The daily-loss cap can value un-marked positions conservatively via `stale_mark_haircut_pct` (default 0 = off). |
| **Guardrail off the event loop** | `check_guardrails` runs via `asyncio.to_thread` so a slow query / WAL checkpoint never freezes ingest or the monitor. |
| **SQLite resilience** | `busy_timeout=5000` (wait for the writer lock, don't error), `wal_autocheckpoint=400`, composite indexes on `orders(side,status,placed_at)` + `positions(status,remaining)`. |
| **Retention** | `[trading] retention_days` (default 0 = off) prunes only `alerts`/`discord_signals` in the daily job; trade history (positions/orders) is never touched. |
| **Metrics** | `GET /metrics` (Prometheus): `event_loop_lag_ms`, `order_queue_depth`, `order_queue_fallback_total`, `guardrail_block_total{reason}`, `orders_placed_total{side}`, `halt_set_total`, `trading_halted`, … |

New config knobs (all in `config.ini`, portal hot-reloadable):
`serialized_order_execution`, `retention_days`, `[guardrails] block_buys_on_stale_marks`, `stale_mark_grace_seconds`, `stale_mark_haircut_pct`.

Diagrams (architecture / sequence / data-flow): `docs/ARCHITECTURE.md`.

**Known open items:** deterministic broker `orderRef` (retry-dup); durable crash-recovery queue; the reconciler is Public-only, so IBKR and Lime have no orphan-order reclaim; cross-host idempotency (needs Postgres).

---

## Log Files (`./logs/`)

| File | Purpose | Look for |
|------|---------|----------|
| `bot.log` | Master log — everything | Full history |
| `startup.log` | App boot/shutdown lifecycle | `ALL SYSTEMS ONLINE`, `APP SHUTDOWN` |
| `pipeline.log` | Alert timing end-to-end | `PIPELINE [BUY] total=142ms` |
| `guardrails.log` | Every safety check | `PASSED` / `BLOCKED` with reason |
| `orders.log` | Order lifecycle | `PLACED → SUBMITTED → FILLED` |
| `exits.log` | Auto-exit triggers | `PT1 TRIGGER`, `SL TRIGGER`, `TPT` with P&L |
| `scorer.log` | AI + Kelly decisions | `SCORE`, `KELLY` lines |
| `parser.log` | Parse activity | `REGEX OK`, `AI MISS` |
| `forwarder.log` | Forwarding activity | `FORWARD`, `DEDUP SKIP`, `WEBHOOK OK` |
| `errors.log` | Errors only | Read first when debugging |

All files rotate at 5 MB, keeping 5 backups. Toggle file logging via `[logging] file_logging_enabled`.

---

## How It Works

```
Source Discord server
        ↓
  forwarder.py (your Discord account reads alerts)
        ↓
  webhook → your #general channel
        ↓
  discord_listener.py (bot parses BTO/STC alerts)
        ↓
  guardrails → AI scorer → Kelly sizer
        ↓
  public_executor.py → Public.com order
        ↓
  position_monitor.py (price polling, auto-exits)
        ↓
  app.py dashboard (live monitoring via WebSocket)
```

---

## Dashboard Features

- **Live Feed** — Real-time alert stream from Discord
- **Positions** — Open positions with live P&L, highest price, exit status
- **Orders** — Full order history with fill status and triggers
- **Manual Exit** — Click to exit individual positions or all at once
- **Cancel Orders** — Cancel pending orders that haven't filled
- **Stats** — Win rate, total P&L, cumulative P&L chart
- **Analysts** — Per-analyst leaderboard with win rate and streak
- **CSV Export** — Download trades, positions, and alerts
- **WebSocket** — Auto-updates without refreshing

---

## First-Time Setup

### Prerequisites

- Python 3.12+
- A Discord bot token (with Message Content intent enabled)
- A Public.com API key and account number
- Your personal Discord user token (for the forwarder)
- An Anthropic API key (for AI parser and scorer — optional)

### Install dependencies

```bash
python -m pip install -r requirements.txt
```

### Configure `config.ini`

All settings are in `config.ini`. The key sections:

| Section | What it controls |
|---|---|
| `[discord]` | Bot token, watched channel IDs, author filters |
| `[public]` | Public.com API key and account number |
| `[trading]` | Dry run mode, slippage, auto-exit rules, position sizes, AI scorer, Kelly sizer |
| `[guardrails]` | Safety limits (max positions, max loss, dedup, market hours) |
| `[ai_parser]` | Claude API key, model, and timeout |
| `[forwarder]` | Your user token, webhook URL, source channels |
| `[logging]` | File logging toggle, log level, rotation settings |

### Important settings

| Setting | What it does | Default |
|---|---|---|
| `dry_run` | `true` = simulate trades, `false` = real trades | `false` |
| `history_backfill_limit` | Load this many past messages on startup | `50` |
| `execute_history` | Execute backfilled alerts (dangerous!) | `false` |
| `market_hours_only` | Block BUYs outside 9:30 AM – 4:00 PM ET | `true` |

---

## Troubleshooting

| Problem | Where to look | Fix |
|---------|---------------|-----|
| No trades placed | `logs/guardrails.log` | `BLOCKED` line shows exact reason |
| Alerts not received | `logs/startup.log` | Discord token or channel_ids wrong |
| Parser missing alerts | `logs/parser.log` | Add new pattern to `parser.py` or enable `ai_parser_enabled` |
| Orders not filling | `logs/orders.log` | Check `SUBMITTED` → verify broker creds / IB Gateway connection |
| Orders showing `SKIPPED` | `logs/orders.log` | Check `error_text` — may be API key or guardrail |
| Wrong contract count | `logs/scorer.log` | Adjust `kelly_max_risk_pct` / `ai_scorer_min_score` |
| Wrong auto-exit | `logs/exits.log` | Adjust `pt1_pct`, `sl_pct`, etc. in config.ini |
| Duplicate trades | `logs/guardrails.log` | Adjust `dedup_window_seconds` or `cross_channel_dedup_seconds` |
| Forwarder not connecting | `logs/forwarder.log` | Check `user_token` in `[forwarder]` section |
| Bot not parsing alerts | `logs/parser.log` | Check `channel_ids` points to your `#general` channel |
| `InterpolationSyntaxError` | Config parsing | Ensure `%` in inline comments is after `;` or `#` |
| Any crash | `logs/errors.log` | ERROR/CRITICAL lines only |

---

## File Map

| File | Role |
|------|------|
| `app.py` | FastAPI app, lifespan, REST endpoints, WebSocket manager |
| `discord_listener.py` | Discord bot event loop, message parsing, alert dispatch |
| `forwarder.py` | Personal user token gateway client, webhook relay |
| `public_executor.py` | Order placement, fill polling, position book |
| `position_monitor.py` | Price polling, PT1/PT2/PT3/TPT/SL/Trailing SL auto-exit |
| `guardrails.py` | 8-rule safety check system |
| `ai_scorer.py` | 0–100 confidence score for BUY alerts |
| `kelly_sizer.py` | Half-Kelly Criterion contract sizing |
| `parser.py` | Regex-based BTO/STC/STO alert parser |
| `ai_parser.py` | Claude AI fallback parser |
| `trade_logger.py` | Discord #trade-logs embeds, daily summary, CSV exports |
| `log_config.py` | Centralized rotating file logging setup + PipelineTimer |
| `models.py` | SQLAlchemy ORM: Alert, Position, Order |
| `db.py` | SQLite database init, session factory, migration helpers |
| `public_sdk_bridge.py` | Public.com SDK wrapper, batch price fetcher, balance API |
| `dashboard.html` | Web dashboard UI (served at `/`) |
| `config.ini` | **All configuration lives here** |
| `logs/` | Rotating log files (10 streams) |
