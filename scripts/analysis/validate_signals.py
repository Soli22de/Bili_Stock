"""
信号提取验证脚本 — Signal Extraction Validation
跟踪每个过滤阶段的信号存活率，输出诊断报告

Usage:
    python scripts/analysis/validate_signals.py
    python scripts/analysis/validate_signals.py --output results/signal_validation.json
"""

import sys
import os
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.extract_signals import SignalExtractor

# ── helpers ──────────────────────────────────────────────────────────────────

def _survived(label, total, count):
    pct = 100 * count / total if total else 0
    print(f"  [{label}] survived: {count} / {total}  ({pct:.1f}%)")
    return count


def _section(title):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


# ── test cases ────────────────────────────────────────────────────────────────

def test_keyword_matching(extractor):
    _section("1. Keyword Matching — action determination")

    cases = [
        # (segment, expected_action, label)
        ("今天买入茅台，强势突破", "BUY", "clear buy"),
        ("清仓跑路了",               "SELL", "clear sell"),
        ("不买，先观望等待",        "NEUTRAL", "negation overrides buy kw"),
        ("止盈了但还在看",          "NEUTRAL", "sell kw + negative word"),
        ("强势涨停拉板打板",        "BUY", "multiple buy keywords"),
        ("",                        "NEUTRAL", "empty segment"),
        ("随便聊聊今天天气",        "NEUTRAL", "no keywords"),
        ("减仓止盈，但还在加仓",    "NEUTRAL", "mixed, negative wins"),
    ]

    total = len(cases)
    passed = 0
    failures = []

    for seg, expected, label in cases:
        action, kws = extractor.determine_action_in_segment(seg)
        ok = action == expected
        if ok:
            passed += 1
        else:
            failures.append({"label": label, "segment": seg, "expected": expected, "got": action, "keywords": kws})
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {label!r:<40} → {action} (expected {expected})")

    _survived("action_match", total, passed)
    return {"passed": passed, "total": total, "failures": failures}


def test_price_extraction(extractor):
    _section("2. Price Extraction — extract_price()")

    cases = [
        ("买入价格10.5元",          10.5,  "decimal price"),
        ("在12块5的位置建仓",       12.5,  "块 notation"),
        ("目标涨幅30%",             0.0,   "percentage excluded"),
        ("",                        0.0,   "empty string"),
        ("没有价格信息的文字",      0.0,   "no price"),
        ("1.23和4.56两个价格",      1.23,  "first price returned"),
        ("昨天涨了5.2%今天看8.30",  8.30,  "pct excluded, decimal kept"),
    ]

    total = len(cases)
    passed = 0
    failures = []

    for text, expected, label in cases:
        got = extractor.extract_price(text)
        ok = abs(got - expected) < 0.01
        if ok:
            passed += 1
        else:
            failures.append({"label": label, "text": text, "expected": expected, "got": got})
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {label!r:<40} → {got} (expected {expected})")

    _survived("price_match", total, passed)
    return {"passed": passed, "total": total, "failures": failures}


def test_stock_lookup(extractor):
    _section("3. Stock Lookup — find_stocks()")

    total_stocks = len(extractor.stock_map)
    print(f"  Stock map loaded: {total_stocks} entries")

    sample = list(extractor.stock_map.items())[:5]
    print(f"  Sample entries: {sample}")

    cases = [
        ("今天买了000001平安银行",  1, "code match"),
        ("不认识的文字和随机数字",  0, "no match"),
        ("",                        0, "empty"),
    ]

    # add a name-based case if map has entries
    if extractor.stock_map:
        first_name = next(iter(extractor.stock_map))
        cases.append((f"看好{first_name}后市", 1, f"name match ({first_name})"))

    total = len(cases)
    passed = 0
    failures = []

    for text, expected_count, label in cases:
        found = extractor.find_stocks(text)
        ok = len(found) >= expected_count if expected_count > 0 else len(found) == 0
        if ok:
            passed += 1
        else:
            failures.append({"label": label, "text": text, "expected_min": expected_count, "got": len(found)})
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {label!r:<40} → {len(found)} stocks found")

    _survived("stock_lookup", total, passed)
    return {"passed": passed, "total": total, "failures": failures, "map_size": total_stocks}


def test_signal_strength(extractor):
    _section("4. Signal Strength — calculate_signal_strength()")

    cases = [
        # (segment, author, expected_range, label)
        ("买入涨停打板封板龙头连板",  "九哥实盘日记", (0.8, 1.0), "high-weight author + many buy kws"),
        ("观望等待",                   "Unknown",       (0.1, 0.5), "neutral + unknown author"),
        ("买入",                       "Unknown",       (0.3, 0.7), "single buy kw + unknown"),
        ("买入止损",                   "行者实盘",      (0.3, 0.7), "buy + negative word offset"),
    ]

    total = len(cases)
    passed = 0
    failures = []

    for seg, author, (lo, hi), label in cases:
        strength = extractor.calculate_signal_strength(seg, author)
        ok = lo <= strength <= hi
        if ok:
            passed += 1
        else:
            failures.append({"label": label, "strength": strength, "expected": f"[{lo}, {hi}]"})
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {label!r:<45} → {strength:.3f} (expected [{lo}, {hi}])")

    _survived("strength_range", total, passed)
    return {"passed": passed, "total": total, "failures": failures}


def test_negative_word_guard(extractor):
    _section("5. Negative Word Guard — has_negative_words()")

    cases = [
        ("不买这只股票",   True,  "不买"),
        ("谨慎操作",       True,  "谨慎"),
        ("大胆买入",       False, "no negative word"),
        ("",               False, "empty"),
        ("先看等待观望",   True,  "multiple negatives"),
    ]

    total = len(cases)
    passed = 0
    failures = []

    for seg, expected, label in cases:
        got = extractor.has_negative_words(seg)
        ok = got == expected
        if ok:
            passed += 1
        else:
            failures.append({"label": label, "segment": seg, "expected": expected, "got": got})
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {label!r:<30} → {got} (expected {expected})")

    _survived("negative_guard", total, passed)
    return {"passed": passed, "total": total, "failures": failures}


# ── scorecard ─────────────────────────────────────────────────────────────────

def build_scorecard(results):
    total_passed = sum(r["passed"] for r in results.values())
    total_cases  = sum(r["total"]  for r in results.values())

    score = 100.0 * total_passed / total_cases if total_cases else 0.0

    scorecard = {
        "module": "extract_signals.SignalExtractor",
        "score": round(score, 1),
        "passed": total_passed,
        "total":  total_cases,
        "sections": {}
    }

    for name, r in results.items():
        sec_score = 100.0 * r["passed"] / r["total"] if r["total"] else 0.0
        scorecard["sections"][name] = {
            "score": round(sec_score, 1),
            "passed": r["passed"],
            "total":  r["total"],
            "failures": r.get("failures", [])
        }

    return scorecard


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Validate SignalExtractor logic")
    parser.add_argument("--output", default=None, help="JSON output path")
    parser.add_argument("--stock-map", default="data/stock_map_final.json", help="Stock map path")
    args = parser.parse_args()

    print("\n══════════════════════════════════════════════════════════")
    print("  Signal Extractor Validation")
    print("══════════════════════════════════════════════════════════")

    try:
        extractor = SignalExtractor(stock_map_path=args.stock_map)
    except Exception as e:
        print(f"\n[ERROR] Failed to init SignalExtractor: {e}")
        sys.exit(1)

    results = {
        "keyword_matching":    test_keyword_matching(extractor),
        "price_extraction":    test_price_extraction(extractor),
        "stock_lookup":        test_stock_lookup(extractor),
        "signal_strength":     test_signal_strength(extractor),
        "negative_word_guard": test_negative_word_guard(extractor),
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

    failed_sections = [k for k, v in scorecard["sections"].items() if v["score"] < 100]
    if failed_sections:
        print(f"[WARN] Failing sections: {', '.join(failed_sections)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
