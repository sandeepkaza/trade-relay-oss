"""
Tests for trade_evaluator pure functions — alert parsing, OSI build, metric
computation, and the rule-based scorer. No I/O (no IBKR / Gemini).

Run: PYTHONIOENCODING=utf-8 .venv/Scripts/python.exe tests/test_trade_evaluator.py
"""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.execution.trade_evaluator import (
    parse_alert, build_osi, compute_metrics, score_trade,
)

PASS = 0
FAIL = 0


def chk(label, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {label}")
    else:
        FAIL += 1
        print(f"  [FAIL] {label}" + (f" — {detail}" if detail else ""))


SAMPLE = "Stock: $META | Strike: 630.0C | Expiry: 05/27/2026 | Entry: $2.40 | Action: BUY"


# ── parse_alert ───────────────────────────────────────────────────────────

def test_parse_sample():
    a = parse_alert(SAMPLE)
    chk("sample parses", a is not None)
    chk("symbol META", a and a["symbol"] == "META", f"got {a and a['symbol']}")
    chk("strike 630.0", a and a["strike"] == 630.0)
    chk("right C", a and a["right"] == "C")
    chk("expiry date", a and a["expiry"] == date(2026, 5, 27), f"got {a and a['expiry']}")
    chk("entry 2.40", a and a["entry"] == 2.40)
    chk("action BUY", a and a["action"] == "BUY")


def test_parse_iso_date_and_put_word():
    a = parse_alert("Ticker: SPY | Strike: 580 | Type: PUT | Expiry: 2026-06-01 | Entry: 1.10 | Action: SELL")
    chk("ISO expiry", a and a["expiry"] == date(2026, 6, 1))
    chk("right from Type field", a and a["right"] == "P")
    chk("action SELL", a and a["action"] == "SELL")


def test_parse_missing_entry_ok():
    a = parse_alert("Stock: AAPL | Strike: 210C | Expiry: 06/20/2026")
    chk("parses without entry", a is not None)
    chk("entry None", a and a["entry"] is None)


def test_parse_non_alert_none():
    chk("commentary → None", parse_alert("SPX looking heavy here, watch 6400 for a bounce") is None)
    chk("empty → None", parse_alert("") is None)
    chk("no right → None", parse_alert("Stock: META | Strike: 630 | Expiry: 05/27/2026") is None)


# ── build_osi ─────────────────────────────────────────────────────────────

def test_build_osi():
    chk("META OSI", build_osi("META", date(2026, 5, 27), 630.0, "C") == "META260527C00630000",
        build_osi("META", date(2026, 5, 27), 630.0, "C"))
    chk("SPX fractional strike", build_osi("SPX", date(2026, 1, 16), 7380.0, "P") == "SPX260116P07380000")
    chk("decimal strike *1000", build_osi("SPY", date(2026, 6, 1), 580.5, "C") == "SPY260601C00580500")


# ── compute_metrics ───────────────────────────────────────────────────────

def test_metrics_breakeven_and_move():
    alert = {"symbol": "META", "strike": 630.0, "right": "C",
             "expiry": date(2026, 5, 27), "entry": 2.40, "action": "BUY"}
    quote = {"und_price": 628.0, "mid": 2.10, "spread_pct": 6.0,
             "iv": 0.42, "delta": 0.38, "theta": -1.2}
    m = compute_metrics(alert, quote, today=date(2026, 5, 27))
    chk("dte 0 (0DTE)", m["dte"] == 0)
    chk("breakeven 632.40", m["breakeven"] == 632.40, f"got {m.get('breakeven')}")
    # move to BE = (632.40-628)/628*100 = 0.70%
    chk("move_to_be ~0.70%", abs(m["move_to_be_pct"] - 0.70) < 0.02, f"got {m.get('move_to_be_pct')}")
    # OTM = (630-628)/628*100 = 0.318%
    chk("otm ~0.32%", abs(m["otm_pct"] - 0.32) < 0.02, f"got {m.get('otm_pct')}")
    # entry 2.40 vs mid 2.10 → +14.3% overpay
    chk("entry_vs_mid ~14.3%", abs(m["entry_vs_mid_pct"] - 14.29) < 0.1, f"got {m.get('entry_vs_mid_pct')}")
    chk("iv_pct 42.0", m["iv_pct"] == 42.0)
    chk("delta carried", m["delta"] == 0.38)
    chk("has_market_data", m["has_market_data"] is True)


def test_metrics_no_quote_degrades():
    alert = {"symbol": "META", "strike": 630.0, "right": "C",
             "expiry": date(2026, 5, 28), "entry": 2.40, "action": "BUY"}
    m = compute_metrics(alert, {}, today=date(2026, 5, 27))
    chk("dte 1", m["dte"] == 1)
    chk("breakeven still computed", m["breakeven"] == 632.40)
    chk("no spot → no otm", "otm_pct" not in m)
    chk("has_market_data False", m["has_market_data"] is False)


# ── score_trade ───────────────────────────────────────────────────────────

def test_score_clean_setup():
    # tight spread, fair entry, decent delta, swing expiry, no market-data gap
    m = {"dte": 7, "expired": False, "spread_pct": 3.0, "entry_vs_mid_pct": 1.0,
         "delta": 0.45, "move_to_be_pct": 2.0, "has_market_data": True}
    s = score_trade(m)
    chk("clean setup high grade", s["grade"] in ("A", "B"), f"got {s['grade']} ({s['score']})")
    chk("clean setup tradeable", s["tradeable"] is True)


def test_score_bad_setup():
    # wide spread, overpaying, lottery delta, 0DTE needing big move
    m = {"dte": 0, "expired": False, "spread_pct": 25.0, "entry_vs_mid_pct": 20.0,
         "delta": 0.08, "move_to_be_pct": 1.5, "has_market_data": True}
    s = score_trade(m)
    chk("bad setup low score", s["score"] <= 30, f"got {s['score']}")
    chk("bad setup flags many", len(s["flags"]) >= 4, f"got {s['flags']}")


def test_score_expired():
    s = score_trade({"dte": -3, "expired": True})
    chk("expired → F", s["grade"] == "F" and s["score"] == 0)
    chk("expired not tradeable", s["tradeable"] is False)


def test_score_no_market_data_capped():
    m = {"dte": 5, "expired": False, "has_market_data": False}
    s = score_trade(m)
    chk("no market data capped at 50", s["score"] <= 50, f"got {s['score']}")
    chk("no market data flag present", any("market data" in f for f in s["flags"]))


def main():
    print("\n-- Trade Evaluator Tests -------------------------------------")
    test_parse_sample()
    test_parse_iso_date_and_put_word()
    test_parse_missing_entry_ok()
    test_parse_non_alert_none()
    test_build_osi()
    test_metrics_breakeven_and_move()
    test_metrics_no_quote_degrades()
    test_score_clean_setup()
    test_score_bad_setup()
    test_score_expired()
    test_score_no_market_data_capped()
    total = PASS + FAIL
    status = "PASS" if FAIL == 0 else "FAIL"
    print(f"\nTrade evaluator: {PASS}/{total}  [{status}]")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
