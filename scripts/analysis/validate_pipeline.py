"""
流水线综合验证脚本 — Master Validation Runner
运行所有验证脚本，生成综合评分卡

Usage:
    python scripts/analysis/validate_pipeline.py
    python scripts/analysis/validate_pipeline.py --output results/pipeline_scorecard.json
    python scripts/analysis/validate_pipeline.py --module signals
    python scripts/analysis/validate_pipeline.py --module risk
    python scripts/analysis/validate_pipeline.py --module scorer

输出格式 (JSON):
{
    "score": 97.3,
    "modules": {
        "signals":  {"score": 100.0, "passed": 20, "total": 20},
        "risk":     {"score": 95.2,  "passed": 20, "total": 21},
        "scorer":   {"score": 96.9,  "passed": 31, "total": 32}
    },
    "failures": [...]
}
"""

import sys
import os
import json
import argparse
import subprocess
import time as _time
from pathlib import Path
from datetime import datetime

SCRIPT_DIR  = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]

MODULES = {
    "signals": SCRIPT_DIR / "validate_signals.py",
    "risk":    SCRIPT_DIR / "validate_risk_engine.py",
    "scorer":  SCRIPT_DIR / "validate_bayesian_scorer.py",
}


def _bar(score, width=30):
    filled = int(score / 100 * width)
    bar = "#" * filled + "-" * (width - filled)
    return f"[{bar}] {score:.1f}%"


def run_module(name, script_path, output_dir):
    out_json = output_dir / f"{name}_scorecard.json"
    cmd = [sys.executable, str(script_path), "--output", str(out_json)]

    t0 = _time.time()
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(PROJECT_ROOT))
    elapsed = _time.time() - t0

    stdout = result.stdout.strip()
    stderr = result.stderr.strip()

    # Print the module's output indented
    for line in stdout.splitlines():
        print(f"  {line}")
    if stderr:
        for line in stderr.splitlines():
            if "warning" in line.lower() or "error" in line.lower():
                print(f"  [STDERR] {line}")

    scorecard = {}
    if out_json.exists():
        try:
            scorecard = json.loads(out_json.read_text(encoding="utf-8"))
        except Exception:
            pass

    return {
        "name":     name,
        "score":    scorecard.get("score", 0.0),
        "passed":   scorecard.get("passed", 0),
        "total":    scorecard.get("total", 0),
        "sections": scorecard.get("sections", {}),
        "elapsed":  round(elapsed, 2),
        "exit_code": result.returncode,
    }


def print_header():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"""
+----------------------------------------------------------+
|           Bili_Stock - Pipeline Validation Suite         |
|           {now}                     |
+----------------------------------------------------------+""")


def print_summary(module_results, composite_score):
    print(f"\n{'═'*60}")
    print("  MODULE SUMMARY")
    print(f"{'─'*60}")
    for r in module_results:
        bar = _bar(r["score"])
        flag = "OK" if r["score"] == 100 else ("!!" if r["score"] < 80 else "~~")
        print(f"  {flag} {r['name']:<10} {bar}  ({r['passed']}/{r['total']})  {r['elapsed']}s")

    print(f"{'─'*60}")
    composite_bar = _bar(composite_score)
    flag = "OK" if composite_score == 100 else ("!!" if composite_score < 80 else "~~")
    print(f"  {flag} COMPOSITE  {composite_bar}")
    print(f"{'═'*60}\n")


def collect_all_failures(module_results):
    failures = []
    for r in module_results:
        for sec_name, sec in r.get("sections", {}).items():
            for f in sec.get("failures", []):
                failures.append({
                    "module":  r["name"],
                    "section": sec_name,
                    **f
                })
    return failures


def main():
    parser = argparse.ArgumentParser(description="Master validation runner")
    parser.add_argument("--output", default=None, help="JSON output path for composite scorecard")
    parser.add_argument("--module", default=None, choices=list(MODULES.keys()),
                        help="Run only one module")
    args = parser.parse_args()

    print_header()

    output_dir = PROJECT_ROOT / "data" / "validation_results"
    output_dir.mkdir(parents=True, exist_ok=True)

    modules_to_run = {args.module: MODULES[args.module]} if args.module else MODULES
    module_results = []

    for name, script_path in modules_to_run.items():
        if not script_path.exists():
            print(f"\n[SKIP] {name}: script not found at {script_path}")
            continue

        print(f"\n>> Running: {name}")
        print(f"{'─'*60}")
        r = run_module(name, script_path, output_dir)
        module_results.append(r)

    if not module_results:
        print("[ERROR] No modules ran.")
        sys.exit(1)

    # Composite score (weighted by total cases)
    total_passed = sum(r["passed"] for r in module_results)
    total_cases  = sum(r["total"]  for r in module_results)
    composite_score = 100.0 * total_passed / total_cases if total_cases else 0.0

    print_summary(module_results, composite_score)

    all_failures = collect_all_failures(module_results)
    if all_failures:
        print(f"  FAILURES ({len(all_failures)} total):")
        for f in all_failures[:10]:  # cap display at 10
            mod  = f.get("module", "?")
            sec  = f.get("section", "?")
            lbl  = f.get("label", "?")
            exp  = f.get("expected", "?")
            got  = f.get("got", "?")
            print(f"    [{mod}/{sec}] {lbl!r}: expected={exp}  got={got}")
        if len(all_failures) > 10:
            print(f"    ... and {len(all_failures) - 10} more (see JSON output)")

    composite_scorecard = {
        "timestamp":       datetime.now().isoformat(),
        "score":           round(composite_score, 1),
        "passed":          total_passed,
        "total":           total_cases,
        "modules":         {r["name"]: {
                                "score":    r["score"],
                                "passed":   r["passed"],
                                "total":    r["total"],
                                "elapsed":  r["elapsed"],
                                "sections": r["sections"]
                            } for r in module_results},
        "failures":        all_failures,
    }

    # Always save to default location
    default_out = output_dir / "composite_scorecard.json"
    default_out.write_text(json.dumps(composite_scorecard, ensure_ascii=False, indent=2))
    print(f"  Composite scorecard → {default_out}")

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(composite_scorecard, ensure_ascii=False, indent=2))
        print(f"  Also saved → {args.output}")

    if composite_score < 100:
        sys.exit(1)


if __name__ == "__main__":
    main()
