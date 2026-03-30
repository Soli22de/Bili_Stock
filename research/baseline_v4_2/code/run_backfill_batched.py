import argparse
import csv
from datetime import datetime
import glob
import os
import re
import subprocess
import sys


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
TARGET = os.path.join(ROOT, "research", "baseline_v4_2", "code", "backfill_stock_data_2019_2021.py")
DATA_DIR = os.path.join(ROOT, "data", "stock_data")


def _run_batch(start_index: int, max_files: int, start_date: str, end_date: str, timeout_sec: int) -> tuple[bool, str]:
    env = os.environ.copy()
    env["START_INDEX"] = str(start_index)
    env["MAX_FILES"] = str(max_files)
    env["BACKFILL_START"] = start_date
    env["BACKFILL_END"] = end_date
    try:
        p = subprocess.run(
            [sys.executable, TARGET],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            errors="replace",
        )
        out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
        ok = p.returncode == 0
        return ok, out
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or "") + ("\n" + e.stderr if e.stderr else "")
        return False, f"TIMEOUT\n{out}"


def _extract_stats(text: str) -> dict[str, int]:
    m = re.search(r"stats\s*\{([^}]*)\}", text)
    if not m:
        return {}
    body = m.group(1)
    out: dict[str, int] = {}
    for part in body.split(","):
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        key = k.strip().strip("'").strip('"')
        val = v.strip()
        try:
            out[key] = int(float(val))
        except Exception:
            continue
    return out


def _extract_detail_stats(text: str) -> dict[str, int]:
    m = re.search(r"detail_stats\s*\{([^}]*)\}", text)
    if not m:
        return {}
    body = m.group(1)
    out: dict[str, int] = {}
    for part in body.split(","):
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        key = k.strip().strip("'").strip('"')
        val = v.strip()
        try:
            out[key] = int(float(val))
        except Exception:
            continue
    return out


def _build_report_paths(report_dir: str) -> tuple[str, str]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_path = os.path.join(report_dir, f"backfill_batch_summary_{ts}.csv")
    failed_path = os.path.join(report_dir, f"backfill_failed_details_{ts}.csv")
    return batch_path, failed_path


def _load_resumed_ok_starts(path: str) -> set[int]:
    done: set[int] = set()
    if (not path) or (not os.path.exists(path)):
        return done
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                status = str(row.get("status", ""))
                s = int(row.get("batch_start", ""))
            except Exception:
                continue
            if status == "ok":
                done.add(s)
    return done


def _write_batch_row(path: str, row: dict):
    exists = os.path.exists(path)
    with open(path, "a", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "batch_start",
                "batch_end",
                "status",
                "backfilled",
                "already_covered",
                "empty_fetch",
                "error",
                "skip_no_date_col",
                "raw_stats",
            ],
        )
        if not exists:
            w.writeheader()
        w.writerow(row)


def _write_failed_row(path: str, row: dict):
    exists = os.path.exists(path)
    with open(path, "a", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["index", "symbol", "status", "error_type"])
        if not exists:
            w.writeheader()
        w.writerow(row)


def _symbol_by_index(files: list[str], idx: int) -> str:
    if idx < 0 or idx >= len(files):
        return ""
    return os.path.splitext(os.path.basename(files[idx]))[0]


def _coverage_check(files: list[str], start_bound: int, end_bound: int, start_date: str, tol_days: int) -> dict[str, int]:
    import pandas as pd

    cutoff = pd.Timestamp(start_date) + pd.Timedelta(days=tol_days)
    out = {"covered": 0, "needs_backfill": 0, "empty_or_bad": 0}
    for i in range(start_bound, end_bound):
        fp = files[i]
        try:
            df = pd.read_csv(fp, usecols=["日期"])
            d = pd.to_datetime(df["日期"], errors="coerce").dropna()
            if d.empty:
                out["empty_or_bad"] += 1
            elif d.min() <= cutoff:
                out["covered"] += 1
            else:
                out["needs_backfill"] += 1
        except Exception:
            out["empty_or_bad"] += 1
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", default="2010-01-01")
    ap.add_argument("--end-date", default="2025-12-31")
    ap.add_argument("--batch-size", type=int, default=20)
    ap.add_argument("--timeout-sec", type=int, default=240)
    ap.add_argument("--single-timeout-sec", type=int, default=120)
    ap.add_argument("--start-index", type=int, default=0)
    ap.add_argument("--end-index", type=int, default=-1)
    ap.add_argument("--report-dir", default=os.path.join(ROOT, "research", "baseline_v4_2", "report"))
    ap.add_argument("--resume-from-report", default="")
    ap.add_argument("--skip-covered-only-check", action="store_true")
    ap.add_argument("--start-tolerance-days", type=int, default=10)
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    total = len(files)
    start_bound = max(0, int(args.start_index))
    end_bound = total if int(args.end_index) < 0 else min(total, int(args.end_index))
    if end_bound <= start_bound:
        print(f"invalid_range start={start_bound} end={end_bound} total={total}")
        return
    os.makedirs(args.report_dir, exist_ok=True)
    batch_report, failed_report = _build_report_paths(args.report_dir)
    print(f"total_files={total}")
    print(f"run_range={start_bound}-{end_bound-1}")
    print(f"batch_report={batch_report}")
    print(f"failed_report={failed_report}")

    if args.skip_covered_only_check:
        cov = _coverage_check(files, start_bound, end_bound, args.start_date, args.start_tolerance_days)
        _write_batch_row(
            batch_report,
            {
                "batch_start": start_bound,
                "batch_end": end_bound - 1,
                "status": "coverage_check",
                "backfilled": 0,
                "already_covered": cov["covered"],
                "empty_fetch": 0,
                "error": 0,
                "skip_no_date_col": cov["empty_or_bad"],
                "raw_stats": str(cov),
            },
        )
        print(f"coverage_check={cov}")
        print("done")
        return

    sum_stats = {"already_covered": 0, "skip_no_date_col": 0, "empty_fetch": 0, "backfilled": 0, "error": 0}
    timeout_starts: list[int] = []
    failed_starts: list[int] = []
    done_starts = _load_resumed_ok_starts(args.resume_from_report)

    for start in range(start_bound, end_bound, args.batch_size):
        size = min(args.batch_size, end_bound - start)
        if start in done_starts:
            print(f"batch={start}-{start+size-1} skipped_from_resume")
            continue
        ok, out = _run_batch(start, size, args.start_date, args.end_date, args.timeout_sec)
        stats = _extract_stats(out)
        detail_stats = _extract_detail_stats(out)
        for k in sum_stats:
            sum_stats[k] += int(stats.get(k, 0))
        flag = "ok"
        if "TIMEOUT" in out:
            timeout_starts.append(start)
            flag = "timeout"
        elif not ok:
            failed_starts.append(start)
            flag = "failed"
        _write_batch_row(
            batch_report,
            {
                "batch_start": start,
                "batch_end": start + size - 1,
                "status": flag,
                "backfilled": int(stats.get("backfilled", 0)),
                "already_covered": int(stats.get("already_covered", 0)),
                "empty_fetch": int(stats.get("empty_fetch", 0)),
                "error": int(stats.get("error", 0)),
                "skip_no_date_col": int(stats.get("skip_no_date_col", 0)),
                "raw_stats": str(detail_stats if detail_stats else stats),
            },
        )
        print(f"batch={start}-{start+size-1} {flag} stats={stats}")

    failed_single: list[int] = []
    if timeout_starts:
        for start in timeout_starts:
            end = min(start + args.batch_size, end_bound)
            for idx in range(start, end):
                ok, out = _run_batch(idx, 1, args.start_date, args.end_date, args.single_timeout_sec)
                stats = _extract_stats(out)
                for k in sum_stats:
                    sum_stats[k] += int(stats.get(k, 0))
                if ("TIMEOUT" in out) or (not ok):
                    failed_single.append(idx)
                    sym = _symbol_by_index(files, idx)
                    err_type = "timeout" if "TIMEOUT" in out else "failed"
                    _write_failed_row(
                        failed_report,
                        {"index": idx, "symbol": sym, "status": "failed", "error_type": err_type},
                    )
                    print(f"single={idx} failed")

    print(f"summary_stats={sum_stats}")
    print(f"timeout_batch_starts={timeout_starts}")
    print(f"failed_batch_starts={failed_starts}")
    print(f"failed_single_indices={failed_single}")
    print("done")


if __name__ == "__main__":
    main()
