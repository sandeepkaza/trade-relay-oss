"""
profile_resolver.py - Shared per-author / per-symbol profile resolution.

Used by both position_monitor.py (exit decisions on an open Position) and
public_executor.py (entry decisions on an incoming Alert). Same lookup
order, same fallback to [trading] — so a knob defined only in [trading]
behaves identically regardless of caller.

Accepts any object exposing author / osi_symbol / expiry / strategy_tag
via getattr. Position and Alert both satisfy this duck-typed interface.

Lookup order (most specific first):
  1. [profile:whale]       strategy_tag == "WHALE"  (whale-tracker lane)
  2. [profile:reentry]     strategy_tag == "RE-ENTRY"
  3. [profile:multi_day]   expiry >= today + multi_day_days_threshold
  4. [profile:spx_index]   osi_symbol starts with SPX/SPXW/NDX/RUT
  5. [profile:<author>]    sarang / twinsight / tc / monkey / ivtrader / bdorts substring
  6. [trading]             default
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from app.core.config_manager import cfg


# ── Exit defaults, derived from the 711-lot sweep (2026-08-21) ───────────────
# One copy of every auto-exit number, so position_monitor, Position.exit_plan
# and the API stop quoting three different figures for the same knob. These
# are the values docs/EXIT_ENGINE_HARDENING.md derives; an explicit key in
# config.ini or a [profile:*] section still wins, this is only the fallback.
#
#   half out at +50, one rung          modelled +$18.6k over the same 711 lots
#   stop -20                           the single largest contributor
#   trail arm +30 / give back 20       +$10,610; width from the give-back p75
#   breakeven arm +15 / floor +5       +$7,164; 86 lots rescued
#   pt2/pt3 rungs and their floor locks off — a second rung had no edge, and
#   the floor locks sell at a profit under an "SL" label with every PT off.
EXIT_DEFAULTS: dict[str, float | bool] = {
    "pt1_enabled": True,   "pt1_pct": 50.0,  "pt1_sell": 0.50,
    "pt2_enabled": False,  "pt2_pct": 60.0,  "pt2_sell": 0.50,
    "pt3_enabled": False,  "pt3_pct": 100.0, "pt3_sell": 1.0,
    "sl_enabled": True,    "sl_pct": -20.0,
    "trailing_sl_enabled": True, "trailing_sl_arm_pct": 30.0, "trailing_sl_pct": 20.0,
    "breakeven_lock_enabled": True, "breakeven_lock_arm_pct": 15.0,
    "breakeven_lock_floor_pct": 5.0,
    "pt2_floor_lock_enabled": False, "pt3_floor_lock_enabled": False,
    "tpt_enabled": False, "tpt_arm_pct": 100.0, "tpt_trail_pct": 25.0,
}


def xd(key):
    """Derived default for an exit knob. KeyError on a typo, on purpose."""
    return EXIT_DEFAULTS[key]


_PROFILE_TOKENS = ("sarang", "twinsight", "tc", "monkey", "ivtrader", "bdorts")
_INDEX_PROFILE_PREFIXES = ("SPX", "SPXW", "NDX", "RUT")


def _MULTI_DAY_PROFILE_ENABLED(): return cfg.getboolean("trading", "multi_day_profile_enabled", fallback=True)
def _MULTI_DAY_DAYS_THRESHOLD():  return cfg.getint("trading", "multi_day_days_threshold", fallback=2)


def _profile_section_for(obj) -> str | None:
    """Author-token match (case-insensitive substring on .author)."""
    a = (getattr(obj, "author", None) or "").lower()
    if not a:
        return None
    for tok in _PROFILE_TOKENS:
        if tok in a:
            return f"profile:{tok}"
    return None


def _index_profile_section_for(obj) -> str | None:
    """SPX/NDX/RUT prefix match on .osi_symbol."""
    osi = (getattr(obj, "osi_symbol", None) or "").upper()
    if osi.startswith(_INDEX_PROFILE_PREFIXES):
        return "profile:spx_index"
    return None


def _multi_day_profile_section_for(obj) -> str | None:
    if not _MULTI_DAY_PROFILE_ENABLED():
        return None
    exp = getattr(obj, "expiry", None)
    if not exp:
        return None
    try:
        exp_date = datetime.strptime(str(exp)[:10], "%Y-%m-%d").date()
    except Exception:
        return None
    today_et = datetime.now(ZoneInfo("America/New_York")).date()
    if (exp_date - today_et).days >= _MULTI_DAY_DAYS_THRESHOLD():
        return "profile:multi_day"
    return None


def _sections_for(obj) -> list[str]:
    sections: list[str] = []
    _tag = (getattr(obj, "strategy_tag", "") or "").upper()
    # Whale-tracker lane is its own world: entry-only source, auto-exits on
    # its own PT. Most specific — wins over every other profile + [trading].
    if _tag == "WHALE":
        sections.append("profile:whale")
    if _tag == "RE-ENTRY":
        sections.append("profile:reentry")
    md = _multi_day_profile_section_for(obj)
    if md:
        sections.append(md)
    idx = _index_profile_section_for(obj)
    if idx:
        sections.append(idx)
    auth = _profile_section_for(obj)
    if auth:
        sections.append(auth)
    return sections


def pcfg_get(obj, key, fallback_str: str | None = None) -> str | None:
    """Read string from most specific matching profile, fall through to [trading]."""
    for sec in _sections_for(obj):
        try:
            v = cfg.get(sec, key, fallback=None)
            if v is not None and v != "":
                return v
        except Exception:
            pass
    return cfg.get("trading", key, fallback=fallback_str)


def pcfg_float(obj, key, fallback: float) -> float:
    v = pcfg_get(obj, key)
    if v is None or v == "":
        return fallback
    try:
        return float(v)
    except Exception:
        return fallback


def pcfg_int(obj, key, fallback: int) -> int:
    v = pcfg_get(obj, key)
    if v is None or v == "":
        return fallback
    try:
        return int(v)
    except Exception:
        return fallback


def pcfg_bool(obj, key, fallback: bool) -> bool:
    v = pcfg_get(obj, key)
    if v is None or v == "":
        return fallback
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def entry_overrides_enabled() -> bool:
    """Master switch — when false, entry path ignores per-profile knobs and
    reads everything straight from [trading]. One-knob rollback."""
    return cfg.getboolean("trading", "entry_profile_overrides_enabled", fallback=True)


def whale_lane_enabled() -> bool:
    """Whale-tracker auto-trading lane master switch. When false the whale
    forwarded channel stays display-only (signal_intel eval, no orders) —
    exactly today's behaviour. One-knob rollback for the entire whale lane."""
    return cfg.getboolean("trading", "whale_lane_enabled", fallback=False)


def whale_resting_tp_enabled() -> bool:
    """When true, whale positions get a broker-resting GTD limit take-profit at
    entry (avg x (1+pt1_pct/100)) instead of the poll-based +15% market exit.
    WHALE-ONLY — never touches analyst exits. Default off; flip after a verified
    live test fill. One-knob revert to the poll exit."""
    return cfg.getboolean("trading", "whale_resting_tp_enabled", fallback=False)
