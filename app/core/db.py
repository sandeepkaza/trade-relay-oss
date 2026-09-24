"""
db.py - SQLite database initialization and session factory.
"""

import os

from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = os.getenv("TRADER_DATABASE_URL", "sqlite:///./data/trader.db")

if DATABASE_URL.startswith("sqlite"):
    # Ensure the parent directory exists before SQLite tries to open the file —
    # otherwise a fresh checkout / first VM boot fails with "unable to open
    # database file" because ./data/ doesn't exist yet.
    _db_path = DATABASE_URL.split("sqlite:///", 1)[-1].split("?", 1)[0]
    if _db_path and _db_path not in (":memory:",):
        _db_dir = os.path.dirname(_db_path)
        if _db_dir:
            os.makedirs(_db_dir, exist_ok=True)
    connect_args = {"check_same_thread": False}
    engine = create_engine(
        DATABASE_URL,
        connect_args=connect_args,
        poolclass=None,  # Disable pooling for SQLite
        echo=False,
    )
    # Enable WAL mode for better concurrent performance
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        # WAL + NORMAL is durable across an application crash (or SIGKILL),
        # but NOT across an OS crash or power loss — the last commits can
        # roll back. For an order row committed just before the broker
        # submit, that is the one window where the broker could hold an
        # order the DB no longer remembers.
        #
        # Left at NORMAL deliberately: FULL adds an fsync on exactly that hot
        # path, and the broker is the source of truth with a reconciler
        # behind it. Set TRADER_SQLITE_SYNCHRONOUS=FULL in .env to buy the
        # durability back if a host failure ever proves this wrong. Env
        # rather than config.ini because this module is imported before the
        # config singleton exists. Values: OFF | NORMAL | FULL | EXTRA.
        _sync = (os.getenv("TRADER_SQLITE_SYNCHRONOUS", "NORMAL")
                 or "NORMAL").strip().upper()
        if _sync not in ("OFF", "NORMAL", "FULL", "EXTRA"):
            _sync = "NORMAL"
        cursor.execute(f"PRAGMA synchronous={_sync}")
        cursor.execute("PRAGMA cache_size=10000")
        # Wait up to 5s for the single SQLite writer lock instead of throwing
        # 'database is locked' immediately — the position-monitor poll and the
        # order path both write, and a WAL checkpoint can briefly hold the lock.
        cursor.execute("PRAGMA busy_timeout=5000")
        # Checkpoint the WAL more often (default 1000 pages) so it can't grow
        # unbounded under the monitor's per-poll Position rewrites; a smaller
        # interval keeps each checkpoint cheap so one never lands mid-order.
        cursor.execute("PRAGMA wal_autocheckpoint=400")
        cursor.close()
else:
    engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def init_db():
    from app.core.models import Alert, Position, Order, DiscordSignal, SystemState, OrderAudit  # noqa: F401  (ensure models are registered)

    Base.metadata.create_all(bind=engine)
    _ensure_legacy_columns()
    _ensure_unique_message_id_index()
    _ensure_perf_indexes()


def _ensure_legacy_columns():
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())

    if "alerts" in tables:
        _ensure_columns(
            "alerts",
            {
                "channel_id": "INTEGER",
                "channel_name": "VARCHAR",
                "discord_message_id": "VARCHAR",
                "is_historical": "BOOLEAN DEFAULT 0",
                "error_text": "VARCHAR",
                "content_hash": "VARCHAR",
                "strategy_tag": "VARCHAR",  # lane tag (SMALL_ACCT | WHALE | RE-ENTRY)
                "qty": "INTEGER",           # contract count stated in the message
            },
        )

    if "positions" in tables:
        _ensure_columns(
            "positions",
            {
                "highest_price":         "FLOAT",
                "lowest_price":          "FLOAT",   # low-water mark; MAE was unknowable without it
                "tpt_armed":             "BOOLEAN DEFAULT 0",
                "close_price":           "FLOAT",   # actual exit price for realized P&L
                "sl_partial_triggered":  "BOOLEAN DEFAULT 0",
                "last_manual_sell_at":   "DATETIME",
                "pts_disabled":          "BOOLEAN DEFAULT 0",
                "breakeven_lock_disabled": "BOOLEAN DEFAULT 0",
                "trail_enabled":         "BOOLEAN DEFAULT 0",
                # Per-position exit overrides; NULL = follow the resolved default.
                "pt_pct_override":       "FLOAT",
                "pt_sell_override":      "FLOAT",   # 0.5 = half out, 1.0 = hard exit
                "sl_pct_override":       "FLOAT",
                "auto_exit_override":    "BOOLEAN",
                "author":                "VARCHAR",
                # Pre-placed resting exit orders (Stage 3 design).
                # NULL = no resting order; non-NULL = broker oid living on the book.
                "pt1_resting_oid":       "VARCHAR",
                "pt1_resting_price":     "FLOAT",
                "pt1_resting_qty":       "INTEGER",
                "pt2_resting_oid":       "VARCHAR",
                "pt2_resting_price":     "FLOAT",
                "pt2_resting_qty":       "INTEGER",
                "sl_resting_oid":        "VARCHAR",
                "sl_resting_price":      "FLOAT",
                "sl_resting_qty":        "INTEGER",
                # Vertical credit spreads: osi_symbol holds a `short|long` combo
                # key, avg_price is a credit received, and P&L inverts.
                "is_credit":             "BOOLEAN DEFAULT 0",
                "spread_width":          "FLOAT",
            },
        )

    if "orders" in tables:
        _ensure_columns(
            "orders",
            {
                "error_text":  "VARCHAR",
                "filled_qty":  "INTEGER",   # contracts actually filled (partial fill support)
                "cost_basis":  "FLOAT",     # snapshot of pos.avg_price at SELL fill time
                "is_resting":  "BOOLEAN DEFAULT 0",  # pre-placed (vs reactive) sell
                "author":      "VARCHAR",   # Discord author who triggered the order
                "strategy_tag": "VARCHAR",  # lane tag (SMALL_ACCT | WHALE | RE-ENTRY)
                "alert_id":    "INTEGER",   # alert this order came from (NULL = pre-column, or no alert)
            },
        )


def _ensure_unique_message_id_index():
    """Enforce one alert row per Discord message at the DB level (GH #8).

    create_all only adds the UNIQUE index to *fresh* tables; an existing live
    `alerts` table keeps its old non-unique schema. Add the index here, but
    DEFENSIVELY: if the live DB already holds duplicate discord_message_id
    values (pre-dedup history), creating a UNIQUE index would fail and break
    startup — so we detect duplicates first and skip with a loud warning,
    leaving the best-effort query dedup in place. No-op once the index exists.
    """
    if not DATABASE_URL.startswith("sqlite"):
        return
    inspector = inspect(engine)
    if "alerts" not in set(inspector.get_table_names()):
        return
    # Already covered? create_all gives fresh DBs a unique index on the column
    # (any name); only the old live table lacks one.
    for ix in inspector.get_indexes("alerts"):
        if ix.get("unique") and ix.get("column_names") == ["discord_message_id"]:
            return
    with engine.begin() as conn:
        dupes = conn.execute(text(
            "SELECT COUNT(*) FROM (SELECT discord_message_id FROM alerts "
            "WHERE discord_message_id IS NOT NULL "
            "GROUP BY discord_message_id HAVING COUNT(*) > 1)"
        )).scalar()
        if dupes:
            import logging
            logging.getLogger(__name__).warning(
                "[DB] %d duplicate discord_message_id value(s) in alerts — "
                "skipping UNIQUE index; dedup stays best-effort. De-dupe the "
                "table to enable DB-enforced idempotency (GH #8).", dupes,
            )
            return
        conn.execute(text(
            "CREATE UNIQUE INDEX ux_alerts_discord_message_id "
            "ON alerts(discord_message_id)"
        ))


def _ensure_perf_indexes():
    """Composite indexes for the per-BUY guardrail + per-poll monitor queries.

    Every BUY scans orders by (side, status, placed_at) for the daily-trade
    count and positions by (status, remaining) for open-count + daily P&L; the
    position monitor hits the latter every poll. Without these the cost is a
    table scan that worsens as rows accumulate (no retention). CREATE INDEX IF
    NOT EXISTS is idempotent and non-destructive — safe to run on a live DB.
    """
    if not DATABASE_URL.startswith("sqlite"):
        return
    tables = set(inspect(engine).get_table_names())
    stmts = []
    if "orders" in tables:
        stmts.append("CREATE INDEX IF NOT EXISTS ix_orders_side_status_placed "
                     "ON orders(side, status, placed_at)")
    if "positions" in tables:
        stmts.append("CREATE INDEX IF NOT EXISTS ix_positions_status_remaining "
                     "ON positions(status, remaining)")
    if not stmts:
        return
    with engine.begin() as conn:
        for s in stmts:
            conn.execute(text(s))


def _ensure_columns(table_name: str, expected: dict[str, str]):
    inspector = inspect(engine)
    existing = {col["name"] for col in inspector.get_columns(table_name)}

    missing = {name: ddl for name, ddl in expected.items() if name not in existing}
    if not missing:
        return

    with engine.begin() as conn:
        for column_name, ddl in missing.items():
            # table_name/column_name/ddl come from the hardcoded schema dicts in
            # _ensure_legacy_columns — never user input; no injection surface.
            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}"))  # nosemgrep: avoid-sqlalchemy-text


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def prune_old_records(retention_days: int) -> dict[str, int]:
    """Delete high-volume log-like rows older than retention_days. Opt-in
    (retention_days<=0 → no-op) so it never runs unless configured.

    Tier 1 (DELETE — short retention). Prunes ONLY the append-only signal feeds
    `alerts` and `discord_signals`: high-volume, low individual value, never read
    back beyond a recent window. Bounds the table growth that otherwise drives the
    guardrail dedup-fallback scan + tail latency.

    Trade history (`positions`, `orders`, `order_audit`) is NOT touched here — it
    carries realized-P&L + provenance and is kept long-term, then moved to cold
    storage by archive_old_trades(). Returns {table: rows_deleted}.
    """
    if retention_days is None or retention_days <= 0:
        return {}
    from datetime import datetime, timezone, timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    deleted: dict[str, int] = {}
    tables = set(inspect(engine).get_table_names())
    with engine.begin() as conn:
        if "alerts" in tables:
            r = conn.execute(text("DELETE FROM alerts WHERE timestamp < :c"), {"c": cutoff})
            deleted["alerts"] = r.rowcount or 0
        if "discord_signals" in tables:
            r = conn.execute(text("DELETE FROM discord_signals WHERE timestamp < :c"), {"c": cutoff})
            deleted["discord_signals"] = r.rowcount or 0
    return deleted


def archive_db_path() -> str | None:
    """Path to the sibling cold-storage SQLite file (archive.db next to the live
    DB). None for non-SQLite backends."""
    if not DATABASE_URL.startswith("sqlite"):
        return None
    live = DATABASE_URL.split("sqlite:///", 1)[-1].split("?", 1)[0]
    if not live or live == ":memory:":
        return None
    d = os.path.dirname(live) or "."
    return os.path.join(d, "archive.db")


# (table, timestamp column, terminal-state predicate). The predicate guarantees
# we NEVER archive a still-live row regardless of age: an OPEN position or a
# PENDING order stays in the hot DB until it actually closes/terminates, even if
# its open/placed time is older than the cutoff.
_ARCHIVE_SPECS = (
    ("positions",   "close_time", "status = 'CLOSED'"),
    ("orders",      "placed_at",  "status IN ('FILLED','CANCELLED','REJECTED')"),
    ("order_audit", "ts",         "1=1"),
)

# Read paths (dashboard Calendar fetches /api/calendar?days=400, Stats aggregates
# the live position set) query ONLY the hot DB — they never ATTACH archive.db.
# Archiving a row therefore removes it from those views. The scheduler clamps
# archive_after_days up to this floor so the active DB always retains at least the
# longest read window, i.e. nothing visible in the UI is ever silently archived.
# Raise this in lockstep if any read window grows past it (or teach reads to span
# the archive, after which this floor can drop).
ARCHIVE_MIN_FLOOR_DAYS = 400


def archive_old_trades(archive_after_days: int) -> dict[str, int]:
    """Tier 2 (MOVE). Relocate terminal trade rows older than archive_after_days
    out of the hot DB into a sibling `archive.db`, then delete them from the live
    tables. Opt-in (archive_after_days<=0 → no-op).

    Trade history must be RETRIEVABLE for years (taxes, performance review) but
    the dashboard/guardrails only ever query a recent window. Moving aged rows to
    cold storage keeps the active working set — and therefore index size, scan
    cost, and WAL churn — bounded, while the data stays one ATTACH away. Run with
    a short window (e.g. 365d) so the active DB only ever holds the recent slice;
    the archive holds the long tail (purged separately by purge_archive()).

    Atomic per run: INSERT-into-archive + DELETE-from-live commit together, so a
    crash mid-archive can't lose rows (they stay live and move next run). SQLite
    only; returns {table: rows_archived}. ATTACH/DETACH run outside the txn (they
    cannot execute inside one); the INSERT opens the txn the DELETE commits with.
    """
    if not DATABASE_URL.startswith("sqlite") or archive_after_days is None or archive_after_days <= 0:
        return {}
    apath = archive_db_path()
    if not apath:
        return {}
    from datetime import datetime, timezone, timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=archive_after_days)
    # Bind an explicit ISO string (space sep) — same form the now-deprecated
    # sqlite3 default datetime adapter emitted — so comparison vs the stored
    # timestamps is unchanged but no longer relies on the removed adapter.
    cutoff_s = cutoff.isoformat(" ")
    live_tables = set(inspect(engine).get_table_names())
    archived: dict[str, int] = {}
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        # Single-quote escape for the literal path (ATTACH takes no bind params).
        cur.execute(f"ATTACH DATABASE '{apath.replace(chr(39), chr(39) * 2)}' AS arch")
        try:
            for table, tscol, cond in _ARCHIVE_SPECS:
                if table not in live_tables:
                    continue
                # Clone the live schema into the archive on first sight (empty
                # copy via WHERE 0). Columns added to the live table later are
                # reconciled below so INSERT ... SELECT never column-mismatches.
                # `table` comes only from the hardcoded _ARCHIVE_SPECS tuple
                # (never user input) — no injection surface.
                cur.execute(f"CREATE TABLE IF NOT EXISTS arch.{table} AS SELECT * FROM main.{table} WHERE 0")  # nosec
                main_cols = [r[1] for r in cur.execute(
                    f"PRAGMA main.table_info({table})").fetchall()]
                arch_cols = {r[1] for r in cur.execute(
                    f"PRAGMA arch.table_info({table})").fetchall()}
                for c in main_cols:
                    if c not in arch_cols:
                        cur.execute(f"ALTER TABLE arch.{table} ADD COLUMN {c}")
                cols = ",".join(main_cols)
                # table/tscol/cond are from _ARCHIVE_SPECS constants and cols
                # are sqlite PRAGMA-derived column names — never user input.
                # Row values stay parameterized (cutoff_s).
                cur.execute(f"INSERT INTO arch.{table} ({cols}) SELECT {cols} FROM main.{table} WHERE {tscol} < ? AND {cond}", (cutoff_s,))  # nosec
                cur.execute(f"DELETE FROM main.{table} WHERE {tscol} < ? AND {cond}", (cutoff_s,))  # nosec
                archived[table] = cur.rowcount or 0
            raw.commit()
        except Exception:
            raw.rollback()
            raise
        finally:
            cur.execute("DETACH DATABASE arch")
    finally:
        raw.close()
    return archived


def purge_archive(retention_years: int) -> dict[str, int]:
    """Tier 3 (PURGE). Final delete of cold-storage rows older than
    retention_years from `archive.db`. Opt-in (retention_years<=0 → no-op).

    This is the only step that permanently destroys trade history — bounded so
    the archive itself can't grow forever. Operates solely on archive.db; the
    live DB is never touched. No-op (and never creates the file) if the archive
    doesn't exist yet. Returns {table: rows_purged}.
    """
    if not DATABASE_URL.startswith("sqlite") or retention_years is None or retention_years <= 0:
        return {}
    apath = archive_db_path()
    if not apath or not os.path.exists(apath):
        return {}
    import sqlite3
    from datetime import datetime, timezone, timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=365 * retention_years)
    cutoff_s = cutoff.isoformat(" ")  # explicit ISO bind; see archive_old_trades
    purged: dict[str, int] = {}
    conn = sqlite3.connect(apath)
    try:
        present = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        for table, tscol, _cond in _ARCHIVE_SPECS:
            if table not in present:
                continue
            # table/tscol from _ARCHIVE_SPECS constant; value bound.
            r = conn.execute(f"DELETE FROM {table} WHERE {tscol} < ?", (cutoff_s,))  # nosec
            purged[table] = r.rowcount or 0
        conn.commit()
    finally:
        conn.close()
    return purged
