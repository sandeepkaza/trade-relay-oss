"""
Master test runner — runs all test suites and reports results.
Usage:  python tests/run_all.py
"""
import subprocess, sys, os, time

# Force UTF-8 output on Windows to avoid cp1252 encoding errors
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = os.path.join(ROOT, ".venv", "Scripts", "python.exe")
if not os.path.exists(PYTHON):
    PYTHON = sys.executable

tests = [
    ("Parser",        "tests/test_parser.py"),
    ("Models + DB",   "tests/test_models.py"),
    ("Guardrails",    "tests/test_guardrails.py"),
    ("Trade Logger",  "tests/test_trade_logger.py"),
    ("API (live)",    "tests/test_api.py"),
]

results = []
total_start = time.perf_counter()

for name, path in tests:
    print(f"\n{'='*60}")
    print(f"▶  {name}")
    print(f"{'='*60}")
    t0 = time.perf_counter()
    r = subprocess.run([PYTHON, path], cwd=ROOT)
    elapsed = time.perf_counter() - t0
    passed = r.returncode == 0
    results.append((name, passed, elapsed))

# ── Summary ────────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print("FINAL TEST REPORT")
print(f"{'='*60}")
all_pass = True
for name, passed, elapsed in results:
    icon = "✅" if passed else "❌"
    print(f"  {icon}  {name:<20}  ({elapsed:.1f}s)")
    if not passed:
        all_pass = False

total_time = time.perf_counter() - total_start
total_suites = len(results)
passed_suites = sum(1 for _, p, _ in results if p)
print(f"\n  {passed_suites}/{total_suites} test suites passed  ({total_time:.1f}s total)")

sys.exit(0 if all_pass else 1)
