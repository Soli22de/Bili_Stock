"""
贝叶斯评分器验证脚本 — Bayesian Scorer Validation
验证创作者可信度评分、时间桶权重、后验更新逻辑

Usage:
    python scripts/analysis/validate_bayesian_scorer.py
    python scripts/analysis/validate_bayesian_scorer.py --output results/scorer_validation.json
"""

import sys
import json
import argparse
import tempfile
from datetime import datetime, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.bayesian_scorer import BayesianParams, CreatorCredibilityScorer


def _section(title):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


def _survived(label, total, count):
    pct = 100 * count / total if total else 0
    print(f"  [{label}] survived: {count} / {total}  ({pct:.1f}%)")
    return count


# ── BayesianParams tests ──────────────────────────────────────────────────────

def test_bayesian_params():
    _section("1. BayesianParams.posterior_mean — edge cases")

    cases = [
        # (alpha, beta, expected_range, label)
        (2.0,  2.0,  (0.49, 0.51), "equal priors → 0.5"),
        (0.0,  0.0,  (0.49, 0.51), "both zero → fallback 0.5"),
        (1.0,  0.0,  (0.99, 1.01), "all wins → 1.0"),
        (0.0,  1.0,  (-0.01, 0.01),"all losses → 0.0"),
        (10.0, 2.0,  (0.8, 0.9),   "strong win record"),
        (2.0,  10.0, (0.1, 0.2),   "strong loss record"),
    ]

    total = len(cases)
    passed = 0
    failures = []

    for alpha, beta, (lo, hi), label in cases:
        p = BayesianParams(alpha=alpha, beta=beta)
        mean = p.posterior_mean
        ok = lo <= mean <= hi
        if ok:
            passed += 1
        else:
            failures.append({"label": label, "alpha": alpha, "beta": beta,
                              "expected": f"[{lo},{hi}]", "got": mean})
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {label!r:<35} α={alpha} β={beta} → mean={mean:.4f}")

    _survived("posterior_mean", total, passed)
    return {"passed": passed, "total": total, "failures": failures}


# ── time bucket tests ─────────────────────────────────────────────────────────

def test_time_bucket():
    _section("2. CreatorCredibilityScorer._time_bucket_score() — trading hour weights")

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        tmp_path = f.name

    scorer = CreatorCredibilityScorer(state_path=tmp_path)

    cases = [
        # (hour, minute, expected_score, label)
        (9,  15, 100.0, "pre-market  09:15 → 100"),
        (9,  24, 100.0, "pre-market  09:24 → 100"),
        (9,  25, 85.0,  "morning     09:25 → 85"),
        (11, 29, 85.0,  "morning     11:29 → 85"),
        (11, 30, 85.0,  "morning end 11:30 → 85 (inclusive)"),
        (13,  0, 85.0,  "afternoon   13:00 → 85"),
        (14, 49, 85.0,  "afternoon   14:49 → 85"),
        (14, 50, 85.0,  "afternoon end 14:50 → 85 (inclusive)"),
        (14, 51, 80.0,  "tail        14:51 → 80"),
        (15,  0, 80.0,  "tail end    15:00 → 80 (inclusive)"),
        (15,  1, 40.0,  "after close 15:01 → 40"),
        (20,  0, 40.0,  "evening     20:00 → 40"),
    ]

    total = len(cases)
    passed = 0
    failures = []

    for h, m, expected, label in cases:
        t = time(h, m)
        got = scorer._time_bucket_score(t)
        ok = abs(got - expected) < 0.1
        if ok:
            passed += 1
        else:
            failures.append({"label": label, "time": f"{h:02d}:{m:02d}",
                              "expected": expected, "got": got})
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {label!r:<35} → {got}")

    Path(tmp_path).unlink(missing_ok=True)
    _survived("time_bucket", total, passed)
    return {"passed": passed, "total": total, "failures": failures}


# ── parse_time tests ──────────────────────────────────────────────────────────

def test_parse_time():
    _section("3. CreatorCredibilityScorer._parse_time_from_row() — time field parsing")

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        tmp_path = f.name

    scorer = CreatorCredibilityScorer(state_path=tmp_path)

    cases = [
        # (row_dict, expect_none, label)
        ({"publish_time": "2026-04-10 09:30:00"}, False, "publish_time datetime string"),
        ({"date": "2026-04-10 14:00:00"},          False, "date datetime string"),
        ({"datetime": "2026-04-10 11:00:00"},      False, "datetime field"),
        ({"created_at": "2026-04-10 15:30:00"},    False, "created_at field"),
        ({"publish_time": "nan"},                   True,  "nan string → None"),
        ({"publish_time": ""},                      True,  "empty string → None"),
        ({},                                        True,  "no time fields → None"),
        ({"publish_time": "not-a-date"},            True,  "unparseable → None"),
    ]

    total = len(cases)
    passed = 0
    failures = []

    for row, expect_none, label in cases:
        result = scorer._parse_time_from_row(row)
        ok = (result is None) == expect_none
        if ok:
            passed += 1
        else:
            failures.append({"label": label, "row": row,
                              "expected_none": expect_none, "got": str(result)})
        status = "PASS" if ok else "FAIL"
        parsed = result if result else "None"
        print(f"  [{status}] {label!r:<40} → {parsed}")

    Path(tmp_path).unlink(missing_ok=True)
    _survived("parse_time", total, passed)
    return {"passed": passed, "total": total, "failures": failures}


# ── posterior update tests ────────────────────────────────────────────────────

def test_posterior_update():
    _section("4. CreatorCredibilityScorer.update() — Bayesian updates")

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        tmp_path = f.name

    scorer = CreatorCredibilityScorer(state_path=tmp_path, alpha_prior=2.0, beta_prior=2.0)

    # Start fresh author
    p0 = scorer.get_posterior_params("新作者")
    assert p0.alpha == 2.0 and p0.beta == 2.0, "prior check"

    # Win → alpha increases
    p1 = scorer.update("新作者", success=True, weight=1.0)
    assert p1.alpha == 3.0, f"alpha should be 3.0, got {p1.alpha}"
    assert p1.beta  == 2.0, f"beta should be 2.0, got {p1.beta}"

    # Loss → beta increases
    p2 = scorer.update("新作者", success=False, weight=1.0)
    assert p2.alpha == 3.0
    assert p2.beta  == 3.0

    # Weighted update
    p3 = scorer.update("新作者", success=True, weight=0.5)
    assert abs(p3.alpha - 3.5) < 0.01, f"weighted alpha: {p3.alpha}"

    # Persistence — new scorer same path reads state
    scorer2 = CreatorCredibilityScorer(state_path=tmp_path)
    p4 = scorer2.get_posterior_params("新作者")
    assert abs(p4.alpha - 3.5) < 0.01, f"persistence: {p4.alpha}"

    Path(tmp_path).unlink(missing_ok=True)

    print("  [PASS] initial prior correct")
    print("  [PASS] win → alpha += 1.0")
    print("  [PASS] loss → beta += 1.0")
    print("  [PASS] weighted update: alpha += 0.5")
    print("  [PASS] state persists across instances")
    _survived("posterior_update", 5, 5)
    return {"passed": 5, "total": 5, "failures": []}


# ── score_row integration ─────────────────────────────────────────────────────

def test_score_row():
    _section("5. CreatorCredibilityScorer.score_row() — end-to-end scoring")

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        tmp_path = f.name

    scorer = CreatorCredibilityScorer(state_path=tmp_path)

    cases = [
        # (row, expected_score_range, label)
        (
            {"author_name": "九哥实盘日记", "publish_time": "2026-04-10 09:20:00"},
            (50, 100), "known author, pre-market"
        ),
        (
            {"author_name": "Unknown", "publish_time": "2026-04-10 20:00:00"},
            (0, 60), "unknown author, after-hours → lower score"
        ),
        (
            {"author_name": "测试", "publish_time": "nan"},
            (0, 100), "no valid time → uses default 60.0"
        ),
        (
            {},
            (0, 100), "empty row → no crash"
        ),
    ]

    total = len(cases)
    passed = 0
    failures = []

    for row, (lo, hi), label in cases:
        try:
            score, win_rate = scorer.score_row(row)
            ok = lo <= score <= hi
        except Exception as e:
            ok = False
            score = -1
            failures.append({"label": label, "error": str(e)})

        if ok:
            passed += 1
        else:
            if not any(f.get("label") == label for f in failures):
                failures.append({"label": label, "expected": f"[{lo},{hi}]", "got": score})

        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {label!r:<45} → score={score:.1f}")

    Path(tmp_path).unlink(missing_ok=True)
    _survived("score_row", total, passed)
    return {"passed": passed, "total": total, "failures": failures}


# ── scorecard ─────────────────────────────────────────────────────────────────

def build_scorecard(results):
    total_passed = sum(r["passed"] for r in results.values())
    total_cases  = sum(r["total"]  for r in results.values())
    score = 100.0 * total_passed / total_cases if total_cases else 0.0

    return {
        "module": "bayesian_scorer.CreatorCredibilityScorer",
        "score": round(score, 1),
        "passed": total_passed,
        "total": total_cases,
        "sections": {
            k: {
                "score": round(100.0 * v["passed"] / v["total"], 1) if v["total"] else 0,
                "passed": v["passed"],
                "total": v["total"],
                "failures": v.get("failures", [])
            }
            for k, v in results.items()
        }
    }


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Validate BayesianScorer logic")
    parser.add_argument("--output", default=None, help="JSON output path")
    args = parser.parse_args()

    print("\n══════════════════════════════════════════════════════════")
    print("  Bayesian Scorer Validation")
    print("══════════════════════════════════════════════════════════")

    results = {
        "bayesian_params":   test_bayesian_params(),
        "time_bucket":       test_time_bucket(),
        "parse_time":        test_parse_time(),
        "posterior_update":  test_posterior_update(),
        "score_row":         test_score_row(),
    }

    scorecard = build_scorecard(results)

    print(f"\n{'═'*60}")
    print(f"  OVERALL SCORE: {scorecard['score']:.1f}%  ({scorecard['passed']}/{scorecard['total']} passed)")
    print(f"{'═'*60}\n")

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(scorecard, ensure_ascii=False, indent=2))
        print(f"Scorecard saved → {args.output}")

    failed = [k for k, v in scorecard["sections"].items() if v["score"] < 100]
    if failed:
        print(f"[WARN] Failing sections: {', '.join(failed)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
