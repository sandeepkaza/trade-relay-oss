"""
log_config.py - Centralized logging configuration for the trading bot.

Creates structured log files under ./logs/ with:
  - bot.log         → master log (everything, DEBUG level)
  - pipeline.log    → alert-to-execution pipeline with timing
  - guardrails.log  → all guardrail checks and blocks
  - orders.log      → order placement, fills, cancels
  - exits.log       → auto-exit triggers (PT1/PT2/PT3/SL)
  - scorer.log      → AI scorer + Kelly sizer decisions
  - parser.log      → parse attempts (regex + AI fallback)
  - forwarder.log   → message forwarding activity
  - errors.log      → ERROR and above only (quick triage)
  - startup.log     → application startup/shutdown lifecycle events
  - prices.jsonl    → per-position price samples, JSON per line (never pruned)

All files rotate at 5 MB, keeping 5 backups — except prices.jsonl, which
splits at 25 MB and keeps every roll forever.
Toggle file logging on/off in config.ini → [logging] section.

Usage:
    from app.core.log_config import setup_logging, pipeline_logger
    setup_logging()  # call once at startup in app.py
"""

import configparser
import logging
import os
import re
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

# ── Read config ──────────────────────────────────────────────────────────────

_config = configparser.ConfigParser(inline_comment_prefixes=(";", "#"))
_config.read("config.ini", encoding="utf-8-sig")

LOG_DIR = Path(_config.get("logging", "log_dir", fallback="./logs"))
FILE_LOGGING_ENABLED = _config.getboolean("logging", "file_logging_enabled", fallback=True)
LOG_LEVEL = _config.get("logging", "log_level", fallback="DEBUG").upper()
MAX_BYTES = int(_config.get("logging", "max_file_size_mb", fallback="5")) * 1024 * 1024
BACKUP_COUNT = int(_config.get("logging", "backup_count", fallback="5"))
# prices.jsonl splits at this size and keeps every roll (no retention).
PRICE_MAX_BYTES = int(_config.get("logging", "price_max_file_size_mb", fallback="25")) * 1024 * 1024
PIPELINE_TIMING = _config.getboolean("logging", "pipeline_timing_enabled", fallback=True)

# ── Log format ───────────────────────────────────────────────────────────────

_CONSOLE_FMT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
_FILE_FMT = "%(asctime)s.%(msecs)03d | %(levelname)-5s | %(name)-18s | %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"

# ── Named loggers (import these from anywhere) ──────────────────────────────

pipeline_logger    = logging.getLogger("pipeline")      # alert → execution timing
guardrail_logger   = logging.getLogger("guardrails")    # guardrail checks
order_logger       = logging.getLogger("orders")        # order lifecycle
exit_logger        = logging.getLogger("exits")         # auto-exit triggers
scorer_logger      = logging.getLogger("scorer")        # AI scorer + Kelly
parser_logger      = logging.getLogger("parser")        # regex + AI parse
forwarder_logger   = logging.getLogger("forwarder")     # message forwarding
error_logger       = logging.getLogger("errors")        # errors only
startup_logger     = logging.getLogger("startup")       # app startup/shutdown lifecycle
price_logger       = logging.getLogger("prices")        # per-position price samples (JSONL)


_SECRET_PATTERNS = [
    re.compile(r'(?i)(authorization\s*[:=]\s*[\'"]?)(bearer\s+)?[A-Za-z0-9._\-]{16,}'),
    re.compile(r'(?i)(api[_-]?key\s*[:=]\s*[\'"]?)[A-Za-z0-9._\-]{16,}'),
    re.compile(r'(?i)(api_secret_key\s*[:=]\s*[\'"]?)[A-Za-z0-9._\-]{16,}'),
    re.compile(r'(?i)(x-api-key\s*[:=]\s*[\'"]?)[A-Za-z0-9._\-]{16,}'),
    re.compile(r'(?i)(token\s*[:=]\s*[\'"]?)[A-Za-z0-9._\-]{20,}'),
    re.compile(r'sk-ant-[A-Za-z0-9_\-]{20,}'),
    re.compile(r'AIza[A-Za-z0-9_\-]{20,}'),
    # Discord bot tokens: 3-segment dotted, starts with M/N/O/Bot prefix.
    # Format: BASE64.TIMESTAMP.HMAC — total ≥ 59 chars typically.
    re.compile(r'(?:Bot\s+)?[MNO][A-Za-z0-9_\-]{23,28}\.[A-Za-z0-9_\-]{6,8}\.[A-Za-z0-9_\-]{27,}'),
]


class ArchivingRotatingFileHandler(RotatingFileHandler):
    """Rotate at maxBytes into a timestamped file that is never deleted.

    Neither stdlib handler can do this. RotatingFileHandler(backupCount=0)
    does not rotate at all — doRollover reopens the same path in append mode,
    so one file grows without bound. Any backupCount > 0 caps the archive and
    starts deleting the oldest roll, which for a price-history stream means
    silently destroying the months of data the analysis needs.

    Rolls are named prices.jsonl.YYYYmmdd-HHMMSS so they sort chronologically
    and can never collide. Nothing here prunes; disk is managed by hand.
    """

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        if os.path.exists(self.baseFilename):
            stamp = time.strftime("%Y%m%d-%H%M%S")
            target = f"{self.baseFilename}.{stamp}"
            n = 1
            while os.path.exists(target):     # same-second rolls
                target = f"{self.baseFilename}.{stamp}.{n}"
                n += 1
            os.rename(self.baseFilename, target)
        if not self.delay:
            self.stream = self._open()


def _sanitize_log_field(s) -> str:
    """Strip newlines / control chars from user-supplied log fields (Discord
    usernames, channel names) to prevent log injection — a username
    containing '\\n' would otherwise inject a fake log line."""
    if s is None:
        return ""
    s = str(s)
    return s.replace("\r", " ").replace("\n", " ")[:200]


class SecretRedactingFilter(logging.Filter):
    """Mask credentials in any log record (defense-in-depth if DEBUG re-enabled)."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            return True
        redacted = msg
        for pat in _SECRET_PATTERNS:
            redacted = pat.sub(lambda m: (m.group(1) if m.lastindex else '') + '***REDACTED***', redacted)
        if redacted != msg:
            record.msg = redacted
            record.args = ()
        return True


def setup_logging():
    """
    Initialize all logging handlers. Call once at app startup.
    Safe to call multiple times (idempotent).
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Root logger: console output
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Filter applied per-handler (Python only invokes logger-level filters on records
    # originating at that logger, not propagated ones — handlers see everything).
    _redactor = SecretRedactingFilter()

    # Remove existing handlers to avoid duplicates on re-init
    for h in root.handlers[:]:
        root.removeHandler(h)

    # Console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(_CONSOLE_FMT, datefmt=_DATE_FMT))
    console.addFilter(_redactor)
    root.addHandler(console)

    if not FILE_LOGGING_ENABLED:
        return

    # ── File handlers ────────────────────────────────────────────────────

    _files = {
        # (logger_name, filename, level)
        "root":       ("bot.log",        LOG_LEVEL),
        "pipeline":   ("pipeline.log",   "DEBUG"),
        "guardrails": ("guardrails.log", "DEBUG"),
        "orders":     ("orders.log",     "DEBUG"),
        "exits":      ("exits.log",      "DEBUG"),
        "scorer":     ("scorer.log",     "DEBUG"),
        "parser":     ("parser.log",     "DEBUG"),
        "forwarder":  ("forwarder.log",  "DEBUG"),
        "errors":     ("errors.log",     "ERROR"),
        "startup":    ("startup.log",    "DEBUG"),
    }

    file_formatter = logging.Formatter(_FILE_FMT, datefmt=_DATE_FMT)

    for logger_name, (filename, level) in _files.items():
        fh = RotatingFileHandler(
            LOG_DIR / filename,
            maxBytes=MAX_BYTES,
            backupCount=BACKUP_COUNT,
            encoding="utf-8",
        )
        fh.setLevel(getattr(logging, level, logging.DEBUG))
        fh.setFormatter(file_formatter)
        fh.addFilter(_redactor)

        if logger_name in ("root", "errors"):
            # errors.log rides ROOT at ERROR level, not a logger named "errors".
            # It was wired to getLogger("errors") and nothing in the app ever
            # logged there, so the file sat at 0 bytes on every lane from
            # 2026-07-03 to 2026-09-15 while real ERRORs went only to bot.log —
            # which rotates 6 files inside a day. Triage advice that says "read
            # the specific stream" is only true if the stream fills itself.
            root.addHandler(fh)
        else:
            target = logging.getLogger(logger_name)
            target.setLevel(logging.DEBUG)
            target.addHandler(fh)
            # Also propagate to root so bot.log captures everything
            target.propagate = True

    # ── prices.jsonl — one JSON object per line, no prefix ──────────────────
    # Deliberately NOT in _files above: this stream is machine-read, so the
    # human formatter would have to be stripped back off before parsing, and
    # propagate=False keeps a 30s-per-position sample rate out of bot.log
    # (which already rotates within hours).
    #
    # NO RETENTION, by explicit request. Splits at price_max_file_size_mb
    # (default 25 MB) into prices.jsonl.YYYYmmdd-HHMMSS and deletes nothing —
    # see ArchivingRotatingFileHandler for why neither stdlib handler does
    # this. The stream exists for multi-month MAE / stop-width analysis, so
    # dropping the oldest roll would destroy exactly the data being collected.
    #
    # Budget: ~200 B/sample, and the monitor loop has NO market-hours gate —
    # it polls every OPEN/PARTIAL row around the clock, so a multi-day
    # position is sampled overnight and at weekends too (verified at the
    # `while True` in position_monitor._monitor_loop). At 30s that is 2,880
    # samples/day/position ≈ 576 KB/day, so 5 open positions ≈ 2.9 MB/day
    # ≈ 1 GB/year and a 25 MB roll lands about every 9 days. Intraday-only
    # books cost roughly a quarter of that. Nothing prunes any of it: watch
    # disk on relaybot-vm, which is already at 77%.
    price_fh = ArchivingRotatingFileHandler(
        LOG_DIR / "prices.jsonl",
        maxBytes=PRICE_MAX_BYTES,
        encoding="utf-8",
    )
    price_fh.setLevel(logging.INFO)
    price_fh.setFormatter(logging.Formatter("%(message)s"))
    price_logger.setLevel(logging.INFO)
    price_logger.addHandler(price_fh)
    price_logger.propagate = False

    logging.info("Logging initialized → %s/ (level=%s, file_logging=%s)", LOG_DIR, LOG_LEVEL, FILE_LOGGING_ENABLED)


# ── Pipeline timer context manager ──────────────────────────────────────────

class PipelineTimer:
    """
    Tracks end-to-end timing through the alert processing pipeline.

    Usage:
        timer = PipelineTimer(osi_symbol="SPX250414C06970000", action="BUY")
        timer.mark("parse_start")
        ... parse ...
        timer.mark("parse_done")
        timer.mark("guardrails_start")
        ... guardrails ...
        timer.mark("guardrails_done")
        timer.finish("FILLED")   # logs the full breakdown
    """

    def __init__(self, osi_symbol: str = "", action: str = "", author: str = "", channel: str = ""):
        self.osi = osi_symbol
        self.action = action
        self.author = author
        self.channel = channel
        self.start = time.perf_counter()
        self.marks: list[tuple[str, float]] = []
        self._enabled = PIPELINE_TIMING

    def mark(self, label: str):
        from app.core.config_manager import cfg as _cfg
        if _cfg.getboolean("logging", "pipeline_timing_enabled", fallback=True):
            self.marks.append((label, time.perf_counter()))

    def finish(self, outcome: str = ""):
        from app.core.config_manager import cfg as _cfg
        if not _cfg.getboolean("logging", "pipeline_timing_enabled", fallback=True):
            return

        total_ms = (time.perf_counter() - self.start) * 1000

        # Build timing breakdown
        segments = []
        for i, (label, ts) in enumerate(self.marks):
            if i == 0:
                delta = (ts - self.start) * 1000
            else:
                delta = (ts - self.marks[i - 1][1]) * 1000
            segments.append(f"{label}={delta:.1f}ms")

        breakdown = " → ".join(segments) if segments else "no marks"

        pipeline_logger.info(
            "PIPELINE [%s] %s %s | total=%.1fms | outcome=%s | author=%s channel=%s | %s",
            self.action, self.osi, _sanitize_log_field(self.author),
            total_ms, outcome, _sanitize_log_field(self.author), _sanitize_log_field(self.channel),
            breakdown,
        )

    def elapsed_ms(self) -> float:
        return (time.perf_counter() - self.start) * 1000
